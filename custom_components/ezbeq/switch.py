# switch.py
from homeassistant.components.switch import SwitchEntity
from homeassistant.helpers.restore_state import RestoreEntity

SEARCH_SWITCH = "switch.ezbeq_candidate_search_enabled"

async def async_setup_entry(hass, entry, async_add_entities):
    async_add_entities([EzbeqSearchToggle(hass)], update_before_add=False)

class EzbeqSearchToggle(RestoreEntity, SwitchEntity):
    def __init__(self, hass):
        self.hass = hass
        self._attr_name = "ezbeq Candidate Search Enabled"
        self._attr_unique_id = "ezbeq_candidate_search_enabled"
        self._state = True  # default

    async def async_added_to_hass(self):
        if (restored := await self.async_get_last_state()) is not None:
            self._state = restored.state == "on"

    @property
    def is_on(self):
        return self._state

    async def async_turn_on(self, **_):
        self._state = True
        await self.async_update_ha_state()

    async def async_turn_off(self, **_):
        self._state = False
        await self.async_update_ha_state()
