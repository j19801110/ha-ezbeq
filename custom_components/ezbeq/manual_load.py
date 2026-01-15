from __future__ import annotations

import csv
import logging
import time
from io import StringIO
from typing import Any, Dict, List, Tuple

from homeassistant.core import HomeAssistant, ServiceCall, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_track_state_change_event  # NEW

from .const import (
    DOMAIN,
    SENSOR_TMDB_IDS,
    SENSOR_TITLES,
    SWITCH_SEARCH_ENABLED,
    SENSOR_DETAILS,
    SENSOR_STATUS,
    SIGNAL_UPDATE_SELECT,
)

_LOGGER = logging.getLogger(__name__)

CATALOG_URL = "https://beqcatalogue.readthedocs.io/en/latest/database.json"
CATALOG_CACHE_TTL = 7 * 24 * 3600  # 1 week
DEFAULT_LIMIT = 10  # How many candidates to expose


# ---------- Helpers ----------
def _signal_name(entry_id: str) -> str:
    return f"{SIGNAL_UPDATE_SELECT}_{entry_id}"


def _utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def _starts_with_any(text: str, prefixes: List[str]) -> bool:
    t = _normalize(text)
    return any(t.startswith(_normalize(p)) for p in prefixes if p)


def _as_list(value: Any) -> List[str]:
    """Coerce value to a list of strings (may split strings into characters for images)."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None]
    if isinstance(value, tuple):
        return [str(v) for v in value if v is not None]
    return [str(value)]


def _as_list_strict(value: Any) -> List[str]:
    """Normalize to a list of strings; never explode a string into characters."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if v is not None and str(v).strip()]
    if isinstance(value, str):
        parsed = _parse_values(value)
        return parsed if parsed else ([value.strip()] if value.strip() else [])
    return [str(value).strip()] if str(value).strip() else []


def _candidate_key(item: Dict[str, Any], audio: str | None) -> str:
    return "|".join(
        [
            str(item.get("theMovieDB", "")).strip(),
            item.get("title", "").strip(),
            item.get("edition", "").strip(),
            (audio or "").strip(),
            item.get("author") if isinstance(item.get("author"), str) else ",".join(item.get("author", []) or []),
        ]
    )


def _first_image(item: Dict[str, Any]) -> Tuple[str | None, str | None]:
    imgs = _as_list(item.get("images"))
    if not imgs:
        return None, None
    if len(imgs) == 1:
        return imgs[0], None
    return imgs[0], imgs[1]


def _is_search_enabled(hass: HomeAssistant) -> bool:
    st = hass.states.get(SWITCH_SEARCH_ENABLED)
    if st is None:
        return True
    return st.state.lower() == "on"


def _parse_values(raw: str | None) -> List[str]:
    if not raw:
        return []
    text = raw.strip()
    if not text:
        return []
    delimiter = ";" if (";" in text and "," not in text) else ","
    reader = csv.reader(StringIO(text), delimiter=delimiter, quotechar='"', skipinitialspace=True)
    values: List[str] = []
    for row in reader:
        for cell in row:
            cell = cell.strip()
            if cell:
                values.append(cell)
    return values


def _clear_manual_state(hass: HomeAssistant, domain_entry: Dict[str, Any], entry_id: str) -> None:
    """Reset candidate lists, selection, and details sensor."""
    domain_entry["candidate_options"] = ["disabled"]
    domain_entry["selected_label"] = "disabled"
    domain_entry["last_candidates"] = {}
    async_dispatcher_send(hass, _signal_name(entry_id))
    _set_sensor(hass, SENSOR_DETAILS, "disabled", last_updated=_utc_timestamp())


# ---------- Entity setters ----------
@callback
def _set_sensor(hass: HomeAssistant, entity_id: str, state: Any, **attrs: Any) -> None:
    hass.states.async_set(entity_id, state, attrs)


@callback
def _set_status(
    hass: HomeAssistant,
    stage: str,
    *,
    reason: str = "",
    candidates: int | None = None,
    selected: str | None = None,
    **extra_attrs: Any,
) -> None:
    attrs: Dict[str, Any] = {
        "stage": stage,
        "reason": reason,
        "last_updated": _utc_timestamp(),
    }
    if candidates is not None:
        attrs["candidates"] = candidates
    if selected is not None:
        attrs["selected"] = selected
    if extra_attrs:
        attrs.update(extra_attrs)
    _set_sensor(hass, SENSOR_STATUS, stage, **attrs)


# ---------- Catalogue fetch ----------
async def _get_catalog_items(hass: HomeAssistant, domain: str) -> list[dict] | None:
    domain_cache = hass.data.setdefault(domain, {})
    cache = domain_cache.get("catalog_cache")
    now = time.time()
    items = None
    if cache and (now - cache["ts"] < CATALOG_CACHE_TTL):
        items = cache["items"]

    if items is None:
        session = async_get_clientsession(hass)
        try:
            async with session.get(CATALOG_URL, timeout=15) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as e:
            _LOGGER.warning("Could not fetch BEQ catalogue: %s", e)
            return None

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("titles") or list(data.values())
        else:
            return None

        domain_cache["catalog_cache"] = {"ts": now, "items": items}
    return items


# ---------- Search + build candidates ----------
def _build_candidates(
    items: List[Dict[str, Any]],
    tmdb_ids: List[str],
    title_prefixes: List[str],
    limit: int,
) -> List[Dict[str, Any]]:
    tmdb_ids_norm = {tid.strip() for tid in tmdb_ids if tid.strip()}
    prefixes_norm = [_normalize(p) for p in title_prefixes if p.strip()]

    results: List[Dict[str, Any]] = []
    seen_keys = set()

    def add_item(item: Dict[str, Any]):
        audio_types_list = _as_list_strict(item.get("audioTypes"))
        if not audio_types_list:
            audio_types_list = [""]
        audio_types_text = ", ".join(audio_types_list)

        genres_list = _as_list_strict(item.get("genres") or item.get("genre"))
        genres_text = ", ".join(genres_list)

        edition_raw = item.get("edition", "") or ""
        edition_display = edition_raw if edition_raw else "—"

        for audio in audio_types_list:
            key = _candidate_key(item, audio)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            img1, img2 = _first_image(item)
            author = item.get("author") or item.get("authors") or ""
            if isinstance(author, list):
                author = ", ".join(a for a in author if a)
            results.append(
                {
                    "key": key,
                    "label": f"{item.get('title','?')} ({item.get('year','?')}) • {edition_display} • {audio or 'Unknown'} • {author or 'n/a'}",
                    "tmdb_id": item.get("theMovieDB", ""),
                    "title": item.get("title", ""),
                    "alt_title": item.get("altTitle", ""),
                    "year": item.get("year"),
                    "edition": edition_raw,
                    "edition_display": edition_display,
                    "audio_type": audio,
                    "audio_types": audio_types_list,
                    "audio_types_text": audio_types_text,
                    "author": author,
                    "mv": item.get("mv"),
                    "warning": item.get("warning", ""),
                    "note": item.get("note", ""),
                    "image1": img1,
                    "image2": img2,
                    "source": item.get("source", ""),
                    "content_type": item.get("content_type", ""),
                    "language": item.get("language", ""),
                    "genres": genres_list,
                    "genres_text": genres_text,
                }
            )

    if tmdb_ids_norm:
        for item in items:
            if str(item.get("theMovieDB", "")).strip() in tmdb_ids_norm:
                add_item(item)

    if prefixes_norm and len(results) < limit:
        for item in items:
            if len(results) >= limit:
                break
            title = item.get("title", "")
            alt_title = item.get("altTitle", "")
            if _starts_with_any(title, prefixes_norm) or _starts_with_any(alt_title, prefixes_norm):
                add_item(item)

    return results[:limit]


# ---------- Services ----------
async def _service_find_candidates(
    hass: HomeAssistant, call: ServiceCall, domain: str, entry_id: str
) -> None:
    domain_entry = hass.data.setdefault(domain, {}).setdefault(
        entry_id, {"candidate_options": ["none"], "selected_label": "none", "last_candidates": {}}
    )

    if not _is_search_enabled(hass):
        _clear_manual_state(hass, domain_entry, entry_id)
        _set_status(hass, "disabled", reason="Search toggle is off")
        return

    tmdb_raw = hass.states.get(SENSOR_TMDB_IDS)
    title_raw = hass.states.get(SENSOR_TITLES)

    tmdb_ids = _parse_values(tmdb_raw.state if tmdb_raw else None)
    titles = _parse_values(title_raw.state if title_raw else None)

    tmdb_found = tmdb_raw is not None
    title_found = title_raw is not None

    if not tmdb_ids and not titles:
        _set_status(
            hass,
            "waiting_for_input",
            reason="No TMDB IDs or titles provided",
            tmdb_sensor_found=tmdb_found,
            title_sensor_found=title_found,
            tmdb_count=0,
            title_count=0,
        )
        domain_entry["candidate_options"] = ["none"]
        domain_entry["selected_label"] = "none"
        async_dispatcher_send(hass, _signal_name(entry_id))
        _set_sensor(hass, SENSOR_DETAILS, "none", last_updated=_utc_timestamp())
        return

    _set_status(
        hass,
        "searching",
        reason="Running candidate search",
        tmdb_sensor_found=tmdb_found,
        title_sensor_found=title_found,
        tmdb_count=len(tmdb_ids),
        title_count=len(titles),
    )

    catalog = await _get_catalog_items(hass, domain)
    if not catalog:
        _set_status(hass, "catalog_unavailable", reason="Failed to fetch BEQ catalogue")
        raise HomeAssistantError("Catalogue unavailable; cannot search.")

    limit = call.data.get("limit", DEFAULT_LIMIT)
    candidates = _build_candidates(catalog, tmdb_ids, titles, limit)

    domain_entry["last_candidates"] = {c["key"]: c for c in candidates}

    if not candidates:
        domain_entry["candidate_options"] = ["none"]
        domain_entry["selected_label"] = "none"
        async_dispatcher_send(hass, _signal_name(entry_id))
        _set_sensor(hass, SENSOR_DETAILS, "none", last_updated=_utc_timestamp())
        _set_status(
            hass,
            "no_candidates",
            reason="No matches for provided TMDB IDs or title prefixes",
            candidates=0,
            tmdb_count=len(tmdb_ids),
            title_count=len(titles),
        )
        return

    options = [c["label"] for c in candidates]
    selected = options[0]

    domain_entry["candidate_options"] = options
    domain_entry["selected_label"] = selected
    async_dispatcher_send(hass, _signal_name(entry_id))

    sel = candidates[0]
    _set_sensor(
        hass,
        SENSOR_DETAILS,
        sel["label"],
        **{k: v for k, v in sel.items() if k not in ("label",)},
        last_updated=_utc_timestamp(),
    )
    _set_status(
        hass,
        "ready",
        reason="Candidates available",
        candidates=len(candidates),
        selected=selected,
        tmdb_count=len(tmdb_ids),
        title_count=len(titles),
    )


async def _service_select_candidate(
    hass: HomeAssistant, call: ServiceCall, domain: str, entry_id: str
) -> None:
    domain_entry = hass.data.setdefault(domain, {}).setdefault(
        entry_id, {"candidate_options": ["none"], "selected_label": "none", "last_candidates": {}}
    )

    if not _is_search_enabled(hass):
        _clear_manual_state(hass, domain_entry, entry_id)
        _set_status(hass, "disabled", reason="Search toggle is off")
        raise HomeAssistantError("Candidate selection blocked: search toggle is off")

    lookup: Dict[str, Dict[str, Any]] = domain_entry.get("last_candidates", {})

    chosen_label = call.data.get("label") or domain_entry.get("selected_label") or "none"
    chosen = next((c for c in lookup.values() if c["label"] == chosen_label), None)
    if not chosen:
        _set_status(hass, "error", reason=f"Candidate '{chosen_label}' not found in last results")
        raise HomeAssistantError(f"Candidate '{chosen_label}' not found in last results")

    _set_sensor(
        hass,
        SENSOR_DETAILS,
        chosen["label"],
        **{k: v for k, v in chosen.items() if k not in ("label",)},
        last_updated=_utc_timestamp(),
    )

    domain_entry["selected_label"] = chosen_label
    async_dispatcher_send(hass, _signal_name(entry_id))
    _set_status(
        hass,
        "ready",
        reason="Candidate selected",
        candidates=len(lookup),
        selected=chosen_label,
    )


async def _service_load_selected_candidate(
    hass: HomeAssistant, call: ServiceCall, domain: str, entry_id: str
) -> None:
    domain_entry = hass.data.setdefault(domain, {}).setdefault(
        entry_id, {"candidate_options": ["none"], "selected_label": "none", "last_candidates": {}}
    )

    if not _is_search_enabled(hass):
        _clear_manual_state(hass, domain_entry, entry_id)
        _set_status(hass, "disabled", reason="Search toggle is off")
        raise HomeAssistantError("Manual load blocked: search toggle is off")

    selected_label = domain_entry.get("selected_label", "none")
    detail_state = hass.states.get(SENSOR_DETAILS)

    if not detail_state or detail_state.state in ("none", "disabled"):
        _set_status(hass, "error", reason="No candidate selected to load")
        raise HomeAssistantError("No candidate selected to load")

    attrs = detail_state.attributes
    if not attrs:
        _set_status(hass, "error", reason="Candidate details missing")
        raise HomeAssistantError("Candidate details missing")

    required = ["tmdb_sensor", "year_sensor", "codec_sensor"]
    missing = [k for k in required if not call.data.get(k)]
    if missing:
        _set_status(
            hass,
            "error",
            reason=f"Missing required service data: {', '.join(missing)}",
            selected=selected_label,
        )
        raise HomeAssistantError(
            "load_selected_candidate requires tmdb_sensor, year_sensor, codec_sensor service data."
        )

    payload = {
        "tmdb_sensor": call.data.get("tmdb_sensor"),
        "year_sensor": call.data.get("year_sensor"),
        "codec_sensor": call.data.get("codec_sensor"),
        "edition_sensor": call.data.get("edition_sensor"),
        "title_sensor": call.data.get("title_sensor"),
        "preferred_author": attrs.get("author", ""),
        "slots": call.data.get("slots") or [1],
        "enable_audio_codec_substitutions": call.data.get("enable_audio_codec_substitutions", False),
        "manual_load": True,
    }

    hass.states.async_set(payload["tmdb_sensor"], attrs.get("tmdb_id", ""))
    hass.states.async_set(payload["year_sensor"], attrs.get("year", 0))
    hass.states.async_set(payload["codec_sensor"], attrs.get("audio_type", ""))
    if payload.get("edition_sensor"):
        hass.states.async_set(payload["edition_sensor"], attrs.get("edition", ""))
    if payload.get("title_sensor"):
        hass.states.async_set(payload["title_sensor"], attrs.get("title", ""))

    await hass.services.async_call(
        domain,
        "load_beq_profile",
        payload,
        blocking=True,
    )
    _set_status(
        hass,
        "loaded",
        reason=f"Candidate '{selected_label}' loaded into BEQ profile",
        selected=selected_label,
    )


# ---------- Setup / teardown ----------
async def async_setup_manual_load(hass: HomeAssistant, coordinator: Any, domain: str) -> None:
    entry_id = coordinator.config_entry.entry_id
    domain_entry = hass.data.setdefault(domain, {}).setdefault(
        entry_id, {"candidate_options": ["none"], "selected_label": "none", "last_candidates": {}}
    )

    domain_entry["candidate_options"] = ["none"]
    domain_entry["selected_label"] = "none"
    async_dispatcher_send(hass, _signal_name(entry_id))

    _set_sensor(hass, SENSOR_DETAILS, "none", last_updated=_utc_timestamp())
    _set_status(
        hass,
        "waiting_for_input",
        reason="Supply candidate TMDB IDs or titles",
        tmdb_sensor_found=False,
        title_sensor_found=False,
        tmdb_count=0,
        title_count=0,
    )

    # Listen for toggle changes; clear state when switched off
    @callback
    def _handle_search_toggle(event) -> None:
        new_state = event.data.get("new_state")
        if not new_state:
            return
        if new_state.state.lower() != "on":
            _clear_manual_state(hass, domain_entry, entry_id)
            _set_status(hass, "disabled", reason="Search toggle is off")

    domain_entry["toggle_unsub"] = async_track_state_change_event(
        hass, [SWITCH_SEARCH_ENABLED], _handle_search_toggle
    )

    # Apply initial toggle state
    if not _is_search_enabled(hass):
        _clear_manual_state(hass, domain_entry, entry_id)
        _set_status(hass, "disabled", reason="Search toggle is off")

    async def handle_find(call: ServiceCall) -> None:
        await _service_find_candidates(hass, call, domain, entry_id)

    async def handle_select(call: ServiceCall) -> None:
        await _service_select_candidate(hass, call, domain, entry_id)

    async def handle_load(call: ServiceCall) -> None:
        await _service_load_selected_candidate(hass, call, domain, entry_id)

    hass.services.async_register(domain, "find_candidates", handle_find)
    hass.services.async_register(domain, "select_candidate", handle_select)
    hass.services.async_register(domain, "load_selected_candidate", handle_load)

    _LOGGER.info("Manual load services registered: find_candidates, select_candidate, load_selected_candidate")


async def async_unload_manual_load(hass: HomeAssistant, domain: str) -> None:
    hass.services.async_remove(domain, "find_candidates")
    hass.services.async_remove(domain, "select_candidate")
    hass.services.async_remove(domain, "load_selected_candidate")

    # Unsubscribe toggle listener if present
    for domain_entry in hass.data.get(domain, {}).values():
        unsub = domain_entry.pop("toggle_unsub", None)
        if unsub:
            unsub()

    _LOGGER.info("Manual load services removed")
