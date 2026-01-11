from __future__ import annotations

import logging
import time  # NEW
from typing import Any, List, Dict

from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from pyezbeq.models import SearchRequest

from .coordinator import EzBEQCoordinator

_LOGGER = logging.getLogger(__name__)

CATALOG_URL = "https://beqcatalogue.readthedocs.io/en/latest/database.json"
CATALOG_CACHE_TTL = 7 * 24 * 3600  # 1 week

# ---------------------------------------------------------------------------
# Substitution rules (ordered). Users can edit these lists directly.
# Each rule: enabled, inputs (incoming codec matches any), outputs (try in order).
# Normalization: lowercase string equality.
# ---------------------------------------------------------------------------
SUBSTITUTION_RULES: List[Dict[str, Any]] = [
    {
        "enabled": True,
        "inputs": ["Atmos"],
        "outputs": ["TrueHD 7.1","TrueHD Atmos" ,"TrueHD 5.1","DD+ Atmos"],
    },
    {
        "enabled": True,
        "inputs": ["dts-hd ma 7.1"],
        "outputs": ["dts-x", "dts:x", "dts-x hr", "dts-hd ma 5.1","dts.hd ma 5.1"],
    },
    {
        "enabled": True,
        "inputs": ["DD+ 5.1", "DD+ 7.1", "DD+ 2.0", "DD+ 2.1"],
        "outputs": ["DD+", "DD+ Atmos", "DD+ 5.1 Atmos"],
    },
    {
        "enabled": True,
        "inputs": ["DTS 5.1", "DTS 6.1"],
        "outputs": ["DTS-HD MA 5.1","DTS-HD MA 7.1", "DTS-ES 5.1", "DTS-ES 6.1", "DTS-EX 5.1"],
    },
    {
        "enabled": True,
        "inputs": ["DTS-HD MA 5.1", "DTS-HD MA 7.1"],
        "outputs": ["DTS-HD MA 5.1","DTS-HD MA 7.1"],
    },
    {
        "enabled": True,
        "inputs": ["PCM"],
        "outputs": ["LPCM 5.1","LPCM 7.1","LPCM 2.0","LPCM 1.0"],
    },
    # Add more rules here as needed.
]


async def async_setup_services(
    hass: HomeAssistant, coordinator: EzBEQCoordinator, domain: str
) -> None:
    """Set up the EzBEQ services."""

    # ---------- Catalogue fetcher (shared) ----------
    async def _get_catalog_items() -> list[dict] | None:
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

    # ---------- Image lookup ----------
    async def fetch_first_image_url(
        tmdb: str, codec: str, edition: str, year: int, title: str
    ) -> str | None:
        """Fetch the first image URL from the BEQ catalogue."""
        items = await _get_catalog_items()
        if not items:
            return None

        tmdb_str = str(tmdb).strip()
        codec_norm = (codec or "").strip().lower()
        edition_norm = (edition or "").strip().lower()
        year_str = str(year).strip()
        title_norm = (title or "").strip().lower()

        def _get_images(item: dict) -> list[str]:
            imgs = item.get("images") or []
            if isinstance(imgs, str):
                imgs = [imgs]
            return imgs

        def _codec_matches(item: dict) -> bool:
            audio_types = item.get("audioTypes") or []
            if isinstance(audio_types, str):
                audio_types = [audio_types]
            return any((a or "").strip().lower() == codec_norm for a in audio_types)

        def _edition_matches(item: dict) -> bool:
            if not edition_norm:
                return True  # ignore edition if empty
            return (item.get("edition", "") or "").strip().lower() == edition_norm

        # Match by TMDB id (field is "theMovieDB" in this catalogue), codec, and edition (if provided)
        for item in items:
            if str(item.get("theMovieDB", "")).strip() == tmdb_str and _codec_matches(item) and _edition_matches(item):
                imgs = _get_images(item)
                if imgs:
                    return imgs[0]

        # Fallback: match by title + year + codec + edition
        for item in items:
            if (
                str(item.get("year", "")).strip() == year_str
                and (item.get("title", "") or "").strip().lower() == title_norm
                and _codec_matches(item)
                and _edition_matches(item)
            ):
                imgs = _get_images(item)
                if imgs:
                    return imgs[0]

        return None

    # ---------- Substitution helpers ----------
    def _normalize_codec(value: str | None) -> str:
        return (value or "").strip().lower()

    def _rule_applies(rule: Dict[str, Any], original_codec_norm: str) -> bool:
        if not rule.get("enabled", False):
            return False
        inputs = [_normalize_codec(x) for x in rule.get("inputs", [])]
        return original_codec_norm in inputs

    def _catalog_has_codec(
        items: list[dict], tmdb: str, edition: str, candidate_codec_norm: str
    ) -> bool:
        tmdb_str = str(tmdb).strip()
        edition_norm = (edition or "").strip().lower()

        def _edition_matches(item: dict) -> bool:
            if not edition_norm:
                return True
            return (item.get("edition", "") or "").strip().lower() == edition_norm

        for item in items:
            if str(item.get("theMovieDB", "")).strip() != tmdb_str:
                continue
            if not _edition_matches(item):
                continue
            audio_types = item.get("audioTypes") or []
            if isinstance(audio_types, str):
                audio_types = [audio_types]
            audio_types_norm = [(a or "").strip().lower() for a in audio_types]
            if candidate_codec_norm in audio_types_norm:
                return True
        return False

    # ---------- Service: load_beq_profile ----------
    async def load_beq_profile(call: ServiceCall) -> None:
        """Load a BEQ profile."""
        enable_audio_codec_subs = bool(call.data.get("enable_audio_codec_substitutions", False))

        def get_sensor_state(entity_id: str) -> Any:
            """Get the state of a sensor entity."""
            state = hass.states.get(entity_id)
            if state is None:
                raise HomeAssistantError(f"Sensor {entity_id} not found")
            return state.state

        try:
            search_request = SearchRequest(
                tmdb=get_sensor_state(call.data["tmdb_sensor"]),
                year=int(get_sensor_state(call.data["year_sensor"])),
                codec=get_sensor_state(call.data["codec_sensor"]),
                preferred_author=call.data.get("preferred_author", ""),
                edition=(
                    get_sensor_state(call.data["edition_sensor"])
                    if "edition_sensor" in call.data
                    else ""
                ),
                slots=call.data.get("slots", [1]),
                title=(
                    get_sensor_state(call.data["title_sensor"])
                    if "title_sensor" in call.data
                    else ""
                ),
            )
        except ValueError as e:
            raise HomeAssistantError(f"Invalid sensor data: {e}") from e

        used_codec = search_request.codec  # Track the codec actually loaded

        try:
            await coordinator.client.load_beq_profile(search_request)
            _LOGGER.info("Successfully loaded BEQ profile")
        except Exception as e:
            _LOGGER.warning("Primary load failed for codec '%s': %s", search_request.codec, e)
            if not enable_audio_codec_subs:
                raise HomeAssistantError(f"Failed to load BEQ profile: {e}") from e

            items = await _get_catalog_items()
            if not items:
                raise HomeAssistantError(f"Failed to load BEQ profile (no catalogue for substitutions): {e}") from e

            original_codec_norm = _normalize_codec(search_request.codec)
            substitute_found = False

            for rule in SUBSTITUTION_RULES:
                if not _rule_applies(rule, original_codec_norm):
                    continue
                outputs = [_normalize_codec(x) for x in rule.get("outputs", [])]
                for cand in outputs:
                    if cand == original_codec_norm:
                        continue  # skip identical
                    if not _catalog_has_codec(items, search_request.tmdb, search_request.edition, cand):
                        continue
                    _LOGGER.info("Retrying load with substitute codec '%s'", cand)
                    search_request.codec = cand
                    used_codec = cand
                    try:
                        await coordinator.client.load_beq_profile(search_request)
                        _LOGGER.info("Successfully loaded BEQ profile after substitution")
                        substitute_found = True
                        break
                    except Exception as e2:
                        _LOGGER.warning("Substitution load with codec '%s' failed: %s", cand, e2)
                        continue
                if substitute_found:
                    break

            if not substitute_found:
                raise HomeAssistantError(f"Failed to load BEQ profile after substitutions: {e}") from e

        # Populate image sensor with first image URL (optional)
        image_sensor = call.data.get("image_sensor")
        if image_sensor:
            image_url = await fetch_first_image_url(
                tmdb=search_request.tmdb,
                codec=used_codec,
                edition=search_request.edition,
                year=search_request.year,
                title=search_request.title or "",
            )
            hass.states.async_set(
                image_sensor,
                image_url or "Not Found",
                {"source": "beq_catalogue", "tmdb": search_request.tmdb},
            )
            if not image_url:
                _LOGGER.info("No image found in BEQ catalogue for tmdb=%s", search_request.tmdb)

    # ---------- Service: unload_beq_profile ----------
    async def unload_beq_profile(call: ServiceCall) -> None:
        """Unload the BEQ profile."""
        try:
            slots = call.data.get("slots", [1])
            search_request = SearchRequest(
                preferred_author="",
                edition="",
                tmdb="",  # These fields are not used for unloading, but are required by the SearchRequest model
                year=0,
                codec="",
                slots=slots,
            )
            await coordinator.client.unload_beq_profile(search_request)
            _LOGGER.info("Successfully unloaded BEQ profile")
        except Exception as e:
            resp = getattr(e, "response", None)
            if resp is not None:
                try:
                    _LOGGER.error(
                        "Failed to unload BEQ profile: %s (status=%s, body=%s)",
                        e,
                        getattr(resp, "status_code", "?"),
                        getattr(resp, "text", "")[:800],
                    )
                except Exception:
                    _LOGGER.error("Failed to unload BEQ profile: %s (response present but unreadable)", e)
            else:
                _LOGGER.error("Failed to unload BEQ profile: %s", e)
            raise HomeAssistantError(f"Failed to unload BEQ profile: {e}") from e
        # NEW: clear the image sensor, if provided
        image_sensor = call.data.get("image_sensor")
        if image_sensor:
            hass.states.async_set(
                image_sensor,
                "",  # empty state to indicate cleared
                {"source": "beq_catalogue", "tmdb": ""},
            )

    hass.services.async_register(domain, "load_beq_profile", load_beq_profile)
    hass.services.async_register(domain, "unload_beq_profile", unload_beq_profile)


async def async_unload_services(hass: HomeAssistant, domain: str) -> None:
    """Unload EzBEQ services."""
    hass.services.async_remove(domain, "load_beq_profile")
