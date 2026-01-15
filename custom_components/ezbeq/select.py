"""Select entity for ezbeq candidate selection."""
from __future__ import annotations

from typing import Any, Callable

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect

from .const import DOMAIN, SIGNAL_UPDATE_SELECT
from .coordinator import EzBEQCoordinator


def _signal_name(entry_id: str) -> str:
    """Build per-entry dispatcher signal."""
    return f"{SIGNAL_UPDATE_SELECT}_{entry_id}"


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities
) -> None:
    """Set up the ezbeq select entity."""
    coordinator: EzBEQCoordinator = entry.runtime_data

    # Ensure per-entry domain data exists
    domain_data = hass.data.setdefault(DOMAIN, {}).setdefault(
        entry.entry_id,
        {"candidate_options": ["none"], "selected_label": "none"},
    )

    async_add_entities(
        [
            EzbeqCandidateSelect(
                hass=hass,
                entry=entry,
                coordinator=coordinator,
                domain_data=domain_data,
            )
        ]
    )


class EzbeqCandidateSelect(SelectEntity):
    """Native select for ezbeq candidates."""

    _attr_icon = "mdi:movie-search"
    _attr_has_entity_name = True
    _attr_translation_key = "candidate_select"

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        coordinator: EzBEQCoordinator,
        domain_data: dict[str, Any],
    ) -> None:
        self.hass = hass
        self.coordinator = coordinator
        self.entry = entry
        self._domain_data = domain_data
        self._attr_name = "EZBEQ Candidate"
        self._attr_unique_id = f"{entry.entry_id}_candidate_select"
        self._attr_options = list(domain_data.get("candidate_options", ["none"]))
        self._attr_current_option = domain_data.get("selected_label", "none")
        self._remove_dispatcher: Callable[[], None] | None = None

    @property
    def available(self) -> bool:
        """Entity is available when coordinator is available."""
        return self.coordinator.last_update_success

    @property
    def current_option(self) -> str | None:
        return self._attr_current_option

    async def async_select_option(self, option: str) -> None:
        """Handle user selection."""
        # Call into manual_load service logic
        await self.hass.services.async_call(
            DOMAIN,
            "select_candidate",
            {"label": option},
            blocking=True,
        )
        # The service will update domain_data and dispatch; we can update locally too
        self._attr_current_option = option
        self._domain_data["selected_label"] = option
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Subscribe to dispatcher updates."""
        self._remove_dispatcher = async_dispatcher_connect(
            self.hass,
            _signal_name(self.entry.entry_id),
            self._handle_update_signal,
        )

    async def async_will_remove_from_hass(self) -> None:
        if self._remove_dispatcher:
            self._remove_dispatcher()
            self._remove_dispatcher = None

    @callback
    def _handle_update_signal(self) -> None:
        """Receive updates from manual_load to refresh options and selection."""
        self._attr_options = list(self._domain_data.get("candidate_options", ["none"]))
        self._attr_current_option = self._domain_data.get("selected_label", "none")
        self.async_write_ha_state()
