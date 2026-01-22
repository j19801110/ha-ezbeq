# ha-ezbeq
This home assistant integration is used to automate EzBEQ functions, uncluding loading and unloading of BEQ profiles based on the movie currently playing, as well as presentation of rich meta-data on both the dashboards and to other integrations as sensors. (This is an implementation based on the brilliant work at https://github.com/iloveicedgreentea/ha-ezbeq).

Fixes (compared to original forked version):
- BEQ Profiles with no MV changes now load normally
- MV changes are NOT loaded into the MiniDSP by default. However, this is configurable using a variable in _init_.py by setting OVERRIDE_GAINS: bool = False.

New Features (compared to forked version):
  - Ability to pull the BEQ images from the database and dispay them on the dashboard (now part of the load status attributes for both images from v3.0.0).
  - Ability to search the catalogue based on audio codec substitutions defined in services.py IF the primary load fails to find a match. Can be enabled / disabled using enable_audio_codec_substitutions: true in the service call. Read more about this under Configurable Variables heading.
  - Status updates are available by reading the attributes of a new sensor called sensor.ezbeq_load_status. Now (v2.0.0) with quite detailed attributes for the load including the MV volume change. This can be used to set (or limit) the volume on your amplifier through Denon / Marantz or other AVR brand integrations.
  - See the full status of the MiniDSP device you have ezBEQ connected to, through the attributes of sensor.ezbeq_devices. (v2.0.0)
  - A huge update to enable manual search and selection of BEQ profiles by passing a TMDB ID (list) or a (partial) Title (list) using user-created template sensors. This allows for the user to widen the search for BEQ profiles if the automatic search and match doesn't yield results. This might be especially important for TV shows where multiple TMDB IDs might be assigned to the same show in Plex (yay for usability on Plex's part). While this might seem like a re-implementation of the ezBEQ web front-end, the web front-end doesn't allow passing in IDs for automatic searches, which made this a bit of a necessity for a full-featured HA custom component. (v3.0.0) - detailed at the very end of this guide for those that want to use this feature.

## Example Images
### No profile loaded

<img width="1063" height="257" alt="Screenshot 2026-01-09 at 10 19 58 pm" src="https://github.com/user-attachments/assets/faa94932-0c04-40c8-a0a8-7b0fd1d7a99a" />


### Profile loaded

<img width="1066" height="759" alt="Screenshot 2026-01-09 at 10 20 38 pm" src="https://github.com/user-attachments/assets/ac7e135e-903e-4b5b-a3b6-5d8a4a258213" />

### Added Load Status Updates

<img width="515" height="752" alt="Screenshot 2026-01-11 at 2 57 54 pm" src="https://github.com/user-attachments/assets/0c835265-5cd1-438d-b383-7c0bee860947" />

## Installation
1. add this as a repo into HAOS HACS.
2. ezBEQ will appear as an add-on. Download it.
3. Under Settings --> Devices and Services, add ezBEZ as a service and point it to your ezBEQ instance.

### How to pull data into the service

The service call below uses template sensors that need to be created in Home Assistant. Whatever you call your template sensors is what you will need to reference in the service call under Services below.

For example, sensor.apple_tv_tmdb_id needs to be created as a template sensor, and you will need to pull in the TMDB ID from whatever sources you have available in home assistant.
My recommendation would be to do the following:

1. Install Tautulli into a container on the HAOS install using Portainer (but can be run on your QNAP / NAS in container station or using Docker on any PC)
2. Install Tautulli Active Streams integration into HAOS through HACS (GitHub - Richardvaio/Tautulli_Active_Streams: Real time tracking of media details, user activity, playback progress and so much more.) with the Plex token option. It brings through all the media information as attributes which can be used to create the sensors.
3. Feed these into the ezBEQ integration using the template sensors you create.

### Example Template Sensor Definitions

Required template sensors the following template sensors are required. While they can be names anything, they must match what you put in the service call that is defined later on in this guide. These template sensors need to provide the data into the service call to help with matching the correct movie. The TMDB ID, Audio codec and Movie Edition are required for matching. The other sensors are optional, but recommneded for troubleshooting at the very least.

The below definitions are given as exmaples when using Tautulli Active Streams Home Assistant integration connected to your Plex server. However, if you are using a Zidoo integration for example, you can define these derived sensors becased on information (sensors) available from that integration.


#### Template sensor for Title (Name: sensor.ezbeq_tv_title)

```yaml
{% set full_title = state_attr('sensor.plex_session_1_tautulli', 'full_title') | string %}
          {{ full_title.split('[')[0] | trim if full_title is not none else 'Unknown' }}
```

#### TMDB ID:

```yaml
{% set guids = state_attr('sensor.plex_session_1_tautulli', 'guids') %}
          {% if guids is not none %}
            {{ guids | select('match', '^tmdb://') | first | replace('tmdb://', '') | string }}
          {% else %}
            unknown
          {% endif %}
```

#### Movie Edition (when put into the title on Plex with [ ] brackets such as Aliens [Director's Cut])
(Name: sensor.ezbeq_tv_edition)

```yaml
{% set title = state_attr('sensor.plex_session_1_tautulli', 'full_title') %}
          {% set pattern = '\[(.*?)\]' %}
          {% if title is not none and title is search(pattern) %}
            {{ (title | regex_findall(pattern)) | first }}
          {% else %}
            {{ '' }}
          {% endif %}
```
#### Audio Codec (name: sensor.ezbeq_tv_codec)

```yaml
{% set codec_attr = state_attr('sensor.plex_session_1_tautulli', 'audio_codec') %}

{# Check if the sensor has data; if not, return early #}
{% if codec_attr is none or states('sensor.plex_session_1_tautulli') in ['unavailable', 'unknown', 'idle'] %}
  Attribute unavailable
{% else %}
  {% set codec_raw = codec_attr | string | lower %}
  {% set channels = state_attr('sensor.plex_session_1_tautulli', 'audio_channels') | int(0) %}
  {% set layout_raw = state_attr('sensor.plex_session_1_tautulli', 'audio_channel_layout') | string %}

  {# 1. Extract only the numeric part of the layout #}
  {% set pattern = '(\d+\.\d+)' %}
  {% set layout = layout_raw | regex_findall_index(pattern) if layout_raw is search(pattern) else layout_raw %}

  {# 2. Define Codec Mapping #}
  {% set mapper = {
    'dca-ma': 'DTS-HD MA',
    'dca': 'DTS-HD MA',
    'dts': 'DTS',
    'ac3': 'DD',
    'eac3': 'DD+'
  } %}

  {# 3. Logic for Object-Based vs Channel-Based #}
  {% if codec_raw == 'truehd' and channels >= 8 %}
    {% set final_codec = 'Atmos' %}
  {% elif codec_raw == 'dca-ma' and 'x' in state_attr('sensor.plex_session_1_tautulli', 'audio_profile') | string | lower %}
    {% set final_codec = 'DTS:X' %}
  {% elif codec_raw == 'truehd' %}
    {% set final_codec = 'TrueHD' %}
  {% else %}
    {% set final_codec = mapper.get(codec_raw, codec_raw | upper) %}
  {% endif %}

  {# 4. Final Output #}
  {% if final_codec in ['Atmos', 'DTS:X', 'DD+ Atmos'] %}
    {{ final_codec }}
  {% else %}
    {{ (final_codec ~ ' ' ~ (layout if layout != 'None' else '')) | trim }}
  {% endif %}
{% endif %}
```

#### Release Year (name: sensor.ezbeq_tv_year)
```yaml
{{ state_attr('sensor.plex_session_1_tautulli', 'year') | string }}
```

#### EzBEQ Enable Button (name: ezbeq_enable)

This is a button helper (input_boolean.ezbeq_enable) that enables you to switch ezBEQ on or off. If you want to use the example automations on this page, then you will need to create this sensor as well and include it on your dashboard so you can enable / disable BEQ loading right on your dashboard.

## Services

This exposes a service to load a profile. Point it to the right sensors

You can test with the developer tools by calling the service `ezbeq.load_beq_profile`. Title and preferred_author sensors are optional and can be dropped from the service call. The other sensor data is critical to be able to load the correct profile.
You must include the image_sensor part to be able to load the image URL into this sensor. You can then display the image using home assistant (see below).

```yaml
action: ezbeq.load_beq_profile
data:
  tmdb_sensor: sensor.ezbeq_tv_tmdb_id
  year_sensor: sensor.ezbeq_tv_year
  codec_sensor: sensor.ezbeq_tv_codec
  edition_sensor: sensor.ezbeq_tv_edition_title
  title_sensor: sensor.ezbeq_tv_title
  preferred_author: aron7awol
  slots:
    - 1
  dry_run_mode: false
  skip_search: false
```

`unload_beq_profile` does not need any data

## Adding Automations - Examples

You can use the below examples for loading BEQ profiles and unloading them.

### Loading
The reason to use the audio track for executing changes is to simplify the loading code: by detecting change, we unload and then try to load again. This happens when starting a stream, changing audio tracks or stopping a stream.

Please note the following:
- set enable_audio_codec_substitutions: true if you want to allow for codec substitutions. This might be needed for various reasons, but you can read more about this under the Configurable Variables heading in this guide.

```yaml
alias: ezBEQ - Audio Track Change
description: Clears BEQ immediately on audio change
triggers:
  - entity_id: sensor.plex_session_1_tautulli
    attribute: audio_codec
    trigger: state
  - trigger: state
    entity_id:
      - input_boolean.ezbeq_enable
    from:
      - "off"
      - "on"
conditions:
  - condition: template
    value_template: "{{ states('sensor.ezbeq_tv_tmdb_id') | is_number }}"
  - condition: state
    entity_id: input_boolean.ezbeq_enable
    state: "on"
actions:
  - action: ezbeq.unload_beq_profile
  - delay: "00:00:05"
  - data:
      tmdb_sensor: sensor.ezbeq_tv_tmdb_id
      year_sensor: sensor.ezbeq_tv_year
      codec_sensor: sensor.ezbeq_tv_codec
      edition_sensor: sensor.ezbeq_tv_edition_title
      slots:
        - 1
      dry_run_mode: false
      skip_search: true
      enable_audio_codec_substitutions: true
    action: ezbeq.load_beq_profile
  - data:
      entity_id: sensor.master_current_profile
    action: homeassistant.update_entity
mode: restart
```

### Unloading 

```yaml
alias: ezBEQ Unload Profile
description: ""
triggers:
  - trigger: state
    entity_id:
      - sensor.ezbeq_tv_codec
    from: null
    to:
      - Attribute unavailable
      - unavailable
    for:
      hours: 0
      minutes: 0
      seconds: 3
  - trigger: state
    entity_id:
      - input_boolean.ezbeq_enable
    from:
      - "on"
      - "off"
conditions: []
actions:
  - action: ezbeq.unload_beq_profile
    metadata: {}
    data:
      image_sensor: input_text.ezbeq_tv_beq_image_url
  - data:
      entity_id: sensor.master_current_profile
    action: homeassistant.update_entity
mode: single
```
## Dashboard Configuration

I would recommend adding your created template sensors onto the dashboard for testing purposes at the very least. 
Additionally, you can also add the following:

### Displaying the loaded Profile on the Dashboard

Add the sensor 'master_current_profile' to the dashboard
The automations must force an update to this sensor at the end of a successful load or unload otherwise the sensor only updates every 15-30 seconds. This forced refresh is included as part of the automation examples on this page.

### Dispalying the Loading Status on the Dashboard

The integration tracks BEQ loading and unloading status including any reasons for a failure. This can help in tracking what is going on, but also to use automations that kick off due to a failure for example, or simply to send as a payload in a message. The loading status will go through the following stages:

STATE VALUES
- Idle
- Unloading
- Unload Success
- Unload Fail (the reason field will be populated with the reason for the failure)
- Loading – Primary
- Loading – Secondary (codec substitutions)
- Load Success
- Load Fail (the reason field will be populated with the reason for the failure)

The available attributes for the ezbeq_load_status sensor is as follows:

### `sensor.ezbeq_load_status` — attributes (after the update)

| Attribute | Type | Description | When it’s present/updated |
| --- | --- | --- | --- |
| `friendly_name` | string | Entity friendly name (“ezBEQ Load Status”). | Always. |
| `last_changed` | ISO timestamp (UTC) | Time the sensor was last updated. | Every status change. |
| `stage` | string | One of `idle`, `loading_primary`, `loading_secondary`, `load_success`, `load_fail`, `unloading`, `unload_success`, `unload_fail`. | Every status change. |
| `profile` | string | Title supplied in the service call. | Loading stages and load/unload outcomes. |
| `codec` | string | Codec actually used for the load (after any substitutions). | Loading stages and load outcomes. |
| `edition` | string | Edition value from the service call. | Loading stages and load outcomes. |
| `slots` | list[int] | MiniDSP slots targeted. | All load/unload stages. |
| `author` | string | BEQ author (from catalogue, best-effort). | On `load_success`. |
| `reason` | string | Error text. | On `load_fail` / `unload_fail`. |
| `tmdb_id` | string | TheMovieDB ID from catalogue. | On `load_success` (when matched). |
| **New** `title` | string | Catalogue title. | On `load_success` (when matched). |
| **New** `alt_title` | string | Alternate title. | On `load_success` (when matched). |
| **New** `source` | string | Source medium (e.g., Disc). | On `load_success` (when matched). |
| **New** `content_type` | string | Content type (e.g., film). | On `load_success` (when matched). |
| **New** `language` | string | Primary language. | On `load_success` (when matched). |
| **New** `mv_offset` | float or null | Main-volume offset in dB (from catalogue `mv`). | On `load_success` (when matched). |
| **New** `audio_types` | list[string] | Audio types from catalogue (e.g., Atmos, TrueHD 5.1). | On `load_success` (when matched). |
| **New** `warning` | string | Warning text from catalogue. | On `load_success` (when matched). |
| **New** `note` | string | Note text from catalogue. | On `load_success` (when matched). |
| **New** `image1` | string | First image URL (if available). | On `load_success` (when matched). |
| **New** `image2` | string | Second image URL (if available). | On `load_success` (when matched). |
| **New** `runtime_minutes` | int or null | Runtime in minutes. | On `load_success` (when matched). |
| **New** `genres` | list[string] | Genres from catalogue. | On `load_success` (when matched). |
| **New** `created_at` | int or null | Catalogue creation timestamp (epoch seconds). | On `load_success` (when matched). |
| **New** 'manual_load' | bool | whether the load was manual or automatic | When a manual load is sent to the loader. |

You can add the following markdown card onto the dashboard to display the loading status and its attributes. Delete the lines you don't want to see.
Option 1: Simple YAML

```yaml
type: markdown
title: ezBEQ Status
content: |
  **State:** {{ states('sensor.ezbeq_load_status') }}
  **Author:** {{ state_attr('sensor.ezbeq_load_status', 'author') }}
  **Codec:** {{ state_attr('sensor.ezbeq_load_status', 'codec') }}
  **Slots:** {{ state_attr('sensor.ezbeq_load_status', 'slots') }}
  **Reason:** {{ state_attr('sensor.ezbeq_load_status', 'reason') }}
  **Updated:** {{ state_attr('sensor.ezbeq_load_status', 'last_changed')
  **Manual Loal:** {{state_attr('sensor.ezbeq_load_status', 'manual_load') }}
  ```

Option 2: Full list of attributes including newly added ones:

```yaml
{% set entity = states.sensor.ezbeq_load_status %} {% set g = entity.attributes if entity is not none else {} %} {% set s = entity.state if entity is not none else 'unknown' %} {% set stage = g.stage if g.stage is defined else 'n/a' %}
**Stage:** {{ s }} ({{ stage }})
**Last changed (UTC):** {{ g.last_changed if g.last_changed is defined else 'n/a' }} 
**Profile:** {{ g.profile if g.profile is defined else 'n/a' }}
**Codec:** {{ g.codec if g.codec is defined else 'n/a' }}  
**Edition:** {{ g.edition if g.edition is defined else 'n/a' }}  
**Slots:** {{ g.slots if g.slots is defined else 'n/a' }}  
**Author:** {{ g.author if g.author is defined and g.author else 'n/a' }}
{% if g.reason is defined %} **Reason:** {{ g.reason }} {% endif %}

---

**TMDB ID:** {{ g.tmdb_id if g.tmdb_id is defined else 'n/a' }}  
**Title:** {{ g.title if g.title is defined else 'n/a' }}  
**Alt title:** {{ g.alt_title if g.alt_title is defined else 'n/a' }} 
**Source:** {{ g.source if g.source is defined else 'n/a' }}  
**Content type:** {{ g.content_type if g.content_type is defined else 'n/a' }}  
**Language:** {{ g.language if g.language is defined else 'n/a' }}  
**Runtime:** {{ g.runtime_minutes ~ ' min' if g.runtime_minutes is defined and g.runtime_minutes else 'n/a' }}  
**Genres:** {{ ', '.join(g.genres) if g.genres is defined and g.genres else 'n/a' }}
**Audio types:** {{ ', '.join(g.audio_types) if g.audio_types is defined and g.audio_types else 'n/a' }}  
**MV offset:** {{ g.mv_offset ~ ' dB' if g.mv_offset is defined and g.mv_offset is not none else 'n/a' }}  
**Warning:** {{ g.warning if g.warning is defined and g.warning else '—' }}  
**Note:** {{ g.note if g.note is defined and g.note else '—' }}  
**Created at:** {{ g.created_at if g.created_at is defined else 'n/a' }}
{% if g.image1 is defined and g.image1 %} ![Image 1]({{ g.image1 }}) {% endif %} {% if g.image2 is defined and g.image2 %} ![Image 2]({{ g.image2 }}) {% endif %}
```

Optoon3 . Alternatively, you can build the same using an entities card (example only lists initial attrinutes)

```yaml
type: entities
title: ezBEQ Status
entities:
  - entity: sensor.ezbeq_load_status            # shows the state (e.g., load_success)
  - type: attribute
    entity: sensor.ezbeq_load_status
    attribute: author
    name: Author
  - type: attribute
    entity: sensor.ezbeq_load_status
    attribute: reason
    name: Reason
  - type: attribute
    entity: sensor.ezbeq_load_status
    attribute: codec
    name: Codec
  - type: attribute
    entity: sensor.ezbeq_load_status
    attribute: slots
    name: Slots
  - type: attribute
    entity: sensor.ezbeq_load_status
    attribute: last_changed
    name: Last changed
```

### Displaying the BEQ images on the Dashboard
This is done using the load status attributes detailed above. The other method is now deprecated.

### Displaying the status of your MiniDSP device

The sensor sensor.ezbeq_devices exposes detailed attributes that will show the status of your MiniDSP device and its slots. Below is an example markdown you can use on your dashboard to display these. Simply cut the attributes you don't want to display. You can also have a look at Developer Tools to display the more detailed attrinutes for your devices and add these in if required.

```yaml
type: markdown
title: MiniDSP Status
content: >
  **MiniDSP:** `{{ states('sensor.ezbeq_devices') }}`

  **Last refreshed:** {{ state_attr('sensor.ezbeq_devices','last_refreshed') }}


  **Master volume:** {{ state_attr('sensor.ezbeq_devices','master_volume') }}
  dB  

  **Mute:** {{ state_attr('sensor.ezbeq_devices','mute') }}


  **Active slot:** {{ state_attr('sensor.ezbeq_devices','active_slot_id') }} –
  {{ state_attr('sensor.ezbeq_devices','active_slot_title') }}

  * Author: {{ state_attr('sensor.ezbeq_devices','active_slot_author') }}

  * Inputs: {{ state_attr('sensor.ezbeq_devices','active_slot_inputs') }}

  * Outputs: {{ state_attr('sensor.ezbeq_devices','active_slot_outputs') }}

  * Gains: ch1 {{ state_attr('sensor.ezbeq_devices','active_slot_input1_gain')
  }} dB,
           ch2 {{ state_attr('sensor.ezbeq_devices','active_slot_input2_gain') }} dB
  * Mutes: ch1 {{ state_attr('sensor.ezbeq_devices','active_slot_input1_mute')
  }},
           ch2 {{ state_attr('sensor.ezbeq_devices','active_slot_input2_mute') }}

  ---

  **Slot 1:** {{ state_attr('sensor.ezbeq_devices','slot1_title') }} (active: {{
  state_attr('sensor.ezbeq_devices','slot1_active') }})

  * Inputs: {{ state_attr('sensor.ezbeq_devices','slot1_inputs') }}, Outputs: {{
  state_attr('sensor.ezbeq_devices','slot1_outputs') }}

  * Gains: ch1 {{ state_attr('sensor.ezbeq_devices','slot1_input1_gain') }} dB,
           ch2 {{ state_attr('sensor.ezbeq_devices','slot1_input2_gain') }} dB
  * Mutes: ch1 {{ state_attr('sensor.ezbeq_devices','slot1_input1_mute') }},
           ch2 {{ state_attr('sensor.ezbeq_devices','slot1_input2_mute') }}

  **Slot 2:** {{ state_attr('sensor.ezbeq_devices','slot2_title') }} (active: {{
  state_attr('sensor.ezbeq_devices','slot2_active') }})

  * Gains: ch1 {{ state_attr('sensor.ezbeq_devices','slot2_input1_gain') }} dB,
           ch2 {{ state_attr('sensor.ezbeq_devices','slot2_input2_gain') }} dB
  * Mutes: ch1 {{ state_attr('sensor.ezbeq_devices','slot2_input1_mute') }},
           ch2 {{ state_attr('sensor.ezbeq_devices','slot2_input2_mute') }}

  **Slot 3:** {{ state_attr('sensor.ezbeq_devices','slot3_title') }} (active: {{
  state_attr('sensor.ezbeq_devices','slot3_active') }})

  * Gains: ch1 {{ state_attr('sensor.ezbeq_devices','slot3_input1_gain') }} dB,
           ch2 {{ state_attr('sensor.ezbeq_devices','slot3_input2_gain') }} dB
  * Mutes: ch1 {{ state_attr('sensor.ezbeq_devices','slot3_input1_mute') }},
           ch2 {{ state_attr('sensor.ezbeq_devices','slot3_input2_mute') }}

  **Slot 4:** {{ state_attr('sensor.ezbeq_devices','slot4_title') }} (active: {{
  state_attr('sensor.ezbeq_devices','slot4_active') }})

  * Gains: ch1 {{ state_attr('sensor.ezbeq_devices','slot4_input1_gain') }} dB,
           ch2 {{ state_attr('sensor.ezbeq_devices','slot4_input2_gain') }} dB
  * Mutes: ch1 {{ state_attr('sensor.ezbeq_devices','slot4_input1_mute') }},
           ch2 {{ state_attr('sensor.ezbeq_devices','slot4_input2_mute') }}

```

## Configurable variables within the code (in folder integrations/ezbeq)
You can only configure these variables by using a code editor addon within HA or using SSH. There are a number of .py (python) files that the intgeration runs to enable its logic. These files have some variables that are configurable which are listed here.

1. File: init.py variable OVERRIDE_GAINS: by setting this True, MV volume changes are NOT applied to the MiniDSP Input channels. By setting this to false, MV volume changes will be applied. This is enabled by default, which means there will be NO volume changes on the inputs. Make sure you have limiters set on your MiniDSP output channels for safety.
2. File Services.py, variable CATALOG_CACHE_TTL: this is the amount of time in seconds that the BEQ database is cached on HA before it is refreshed. Please note that this only affects the BEQ image currently, but might affect other anscillary data over time if this integration is developed further. It does not affect the main BEQ catalogue used for loading the profiles. The default is one week, but you can change this if you need to. Restarting HA will also reset the cache.
3. File Services.py, variable SUBSTITUTION_RULES. these rules allow you to search the catalogue again for a match using a different / substituted audio codec IF the primary load did not find a match. This allows for substituting audio codec data within the load itself and is useful when the sensors don't provide Atmos, DTS-X, Auro-3D but the database expects those matches. Also, this is useful when the database contains errors or codecs that actually are suitable for a load using the primary audio track. Use this with caution as incorrect matches or lists can result in loading the incorrect data. This is why this can be enabled or disabled within the service call itself using the enable_audio_codec_substitutions: false flag. It is enabled by passing enable_audio_codec_substitutions: true to the service.

# Configuring ezBEQ for manual search and loading of BEQ profiles - GUIDE STILL IN BETA - Report any issues
You might want to configure a manual loading dashboard like the below.

<img width="1059" height="810" alt="Screenshot 2026-01-15 at 7 18 49 pm" src="https://github.com/user-attachments/assets/91456b99-1c65-40e5-9fb6-49974009e2db" />

This allows you to do BEQ profile loading manually using fuzzy search logic with a (list of) TMDB ID(s) and a (list of) Titles or partial title(s) of your choosings that you pass to this module. This allows for some quick searches based on what's playing and loading of an alternate BEQ that wasn't matched. However, you can also enable text-fields as input fields into the sensors if you so wish to allow for any sort of search, but this is not something I give examples of. However, the module is configurable enough that you can pass to the sensors what you wish.

## What the integration provides out‑of‑the‑box
- `switch.ezbeq_candidate_search_enabled` — turns manual search on/off (state is cleared when off).
- `sensor.ezbeq_candidate_status` — shows the current manual-load stage/reason/candidate counts.
- `sensor.ezbeq_candidate_details` — holds the currently highlighted catalogue candidate and its attributes (title, year, edition, audio, author, mv, images, etc.).

These are created by the integration.

## What **you** must provide for searching
The integration **reads** (but does not create) two text-like entities. You can use `input_text`, or a template sensor to drive these.

- `sensor.ezbeq_candidate_tmdb_ids` (`SENSOR_TMDB_IDS`)
  - State: comma or semicolon list of TMDB IDs (e.g., `603,155`).
- `sensor.ezbeq_candidate_titles` (`SENSOR_TITLES`)
  - State: comma or semicolon list of titles or partial titles.

Populate these via your own YAML and / or automations. Examplea are shown below:

sensor.ezbeq_candidate_titles:
```yaml
{% set title = states('sensor.ezbeq_tv_title') | default('', true) | trim %}
{% set safe_title = '"' ~ title ~ '"' if ',' in title else title %}
{{ safe_title[:12] }}
```

sensor.ezbeq_candidate_tmdb_ids
```yaml
{{ states('sensor.ezbeq_tv_tmdb_id') | default('') }}
```

## Entities used when loading (auto-created on first use)
When you call `ezbeq.load_selected_candidate`, you pass entity IDs for the playback metadata. The integration will `async_set` them (so they are created dynamically if they don’t exist):

- `sensor.ezbeq_candidate_tmdb_id`
- `sensor.ezbeq_candidate_year`
- `sensor.ezbeq_candidate_codec`
- `sensor.ezbeq_candidate_edition` (optional)
- `sensor.ezbeq_candidate_title` (optional)

## Service flow
1) Ensure `switch.ezbeq_candidate_search_enabled` is **on**.
2) Set `sensor.ezbeq_candidate_tmdb_ids` (and optionally `sensor.ezbeq_candidate_titles`).
3) Call `ezbeq.find_candidates`.
4) (Optional) Call `ezbeq.select_candidate` with a specific `label`; otherwise the first result is used.
5) Call `ezbeq.load_selected_candidate`, providing the five playback entity IDs above (or your own choices).

## Example dashboard controls

### Button to load the currently selected candidate
```yaml
type: button
name: Load Selected Candidate
icon: mdi:cloud-download
tap_action:
  action: call-service
  service: ezbeq.load_selected_candidate
  service_data:
    tmdb_sensor: sensor.ezbeq_candidate_tmdb_id
    year_sensor: sensor.ezbeq_candidate_year
    codec_sensor: sensor.ezbeq_candidate_codec
    edition_sensor: sensor.ezbeq_candidate_edition
    title_sensor: sensor.ezbeq_candidate_title
    slots: [1]
    enable_audio_codec_substitutions: false
```

### Entities card for search and status
```yaml
type: entities
title: ezBEQ Manual Load
entities:
  - entity: switch.ezbeq_candidate_search_enabled
    name: Enable manual search
  - entity: sensor.ezbeq_candidate_tmdb_ids
    name: TMDB IDs (comma/semicolon)
  - entity: sensor.ezbeq_candidate_titles
    name: Title prefixes
  - type: call-service
    name: Find candidates
    icon: mdi:magnify
    action_name: Run
    service: ezbeq.find_candidates
    data: {}
  - type: call-service
    name: Load selected candidate
    icon: mdi:cloud-download
    action_name: Load
    service: ezbeq.load_selected_candidate
    data:
      tmdb_sensor: sensor.ezbeq_candidate_tmdb_id
      year_sensor: sensor.ezbeq_candidate_year
      codec_sensor: sensor.ezbeq_candidate_codec
      edition_sensor: sensor.ezbeq_candidate_edition
      title_sensor: sensor.ezbeq_candidate_title
      slots: [1]
      enable_audio_codec_substitutions: false
```

### Dropdown list for selecting a BEQ to load

Example below is with mushroom cards:
```yaml
type: vertical-stack
cards:
  - type: custom:mushroom-select-card
    entity: select.ezbeq_candidate
    icon: mdi:movie-search
    fill_container: true
    secondary_info: none
    name: BEQ Profiles Available
grid_options:
  columns: 12
  rows: 2
```

### Markdown card for status/details
```yaml
type: markdown
title: ezBEQ Manual Status
content: |
  **Search enabled:** {{ states('switch.ezbeq_candidate_search_enabled') }}
  **Status:** {{ states('sensor.ezbeq_candidate_status') }}
  - Stage: {{ state_attr('sensor.ezbeq_candidate_status','stage') }}
  - Reason: {{ state_attr('sensor.ezbeq_candidate_status','reason') }}
  - Candidates: {{ state_attr('sensor.ezbeq_candidate_status','candidates') }}
  - Selected: {{ state_attr('sensor.ezbeq_candidate_status','selected') }}
  - Last updated: {{ state_attr('sensor.ezbeq_candidate_status','last_updated') }}

  **Current candidate:** {{ states('sensor.ezbeq_candidate_details') }}
  - Title: {{ state_attr('sensor.ezbeq_candidate_details','title') }} ({{ state_attr('sensor.ezbeq_candidate_details','year') }})
  - Edition: {{ state_attr('sensor.ezbeq_candidate_details','edition_display') }}
  - Audio: {{ state_attr('sensor.ezbeq_candidate_details','audio_types_text') }}
  - Author: {{ state_attr('sensor.ezbeq_candidate_details','author') }}
  - MV offset: {{ state_attr('sensor.ezbeq_candidate_details','mv') }}
  - Note/Warning: {{ state_attr('sensor.ezbeq_candidate_details','note') or '' }} {{ state_attr('sensor.ezbeq_candidate_details','warning') or '' }}
```

### Other example - Full section configuration

You can use the below to re-create the page in the screenshot example

```yaml
- type: sections
    max_columns: 2
    title: ezBEQ Manual Load
    path: ezbeq-manual-load
    icon: mdi:car-shift-pattern
    sections:
      - type: grid
        cards:
          - type: heading
            heading: Candidates
            heading_style: title
            icon: mdi:playlist-star
          - type: tile
            grid_options:
              columns: full
            entity: switch.ezbeq_candidate_search_enabled
            name: ezBEQ Manual Load
            icon: mdi:database-search
            show_entity_picture: false
            vertical: false
            tap_action:
              action: toggle
            icon_tap_action:
              action: toggle
            features_position: bottom
          - show_name: true
            show_icon: true
            type: button
            name: Find Candidates
            icon: mdi:tab-search
            show_state: false
            icon_height: 30px
            grid_options:
              columns: 4
              rows: 2
            tap_action:
              action: perform-action
              perform_action: ezbeq.find_candidates
              target: {}
            hold_action:
              action: perform-action
              perform_action: ezbeq.find_candidates
              target: {}
          - show_name: true
            show_icon: true
            type: button
            name: Load Selected Candidate
            icon: mdi:cloud-download
            icon_height: 35px
            tap_action:
              action: call-service
              service: ezbeq.load_selected_candidate
              service_data:
                tmdb_sensor: sensor.ezbeq_candidate_tmdb_id
                year_sensor: sensor.ezbeq_candidate_year
                codec_sensor: sensor.ezbeq_candidate_codec
                edition_sensor: sensor.ezbeq_candidate_edition
                title_sensor: sensor.ezbeq_candidate_title
                slots:
                  - 1
                enable_audio_codec_substitutions: false
            grid_options:
              columns: 4
              rows: 2
          - show_name: true
            show_icon: true
            type: button
            icon: mdi:cloud-download
            icon_height: 35px
            tap_action:
              action: perform-action
              perform_action: ezbeq.unload_beq_profile
              target: {}
            grid_options:
              columns: 4
              rows: 2
            name: Unload BEQ Profile
          - type: vertical-stack
            cards:
              - type: custom:mushroom-select-card
                entity: select.ezbeq_candidate
                icon: mdi:movie-search
                fill_container: true
                secondary_info: none
                name: BEQ Profiles Available
            grid_options:
              columns: 12
              rows: 2
          - type: markdown
            content: |-
              <h3><b> BEQ Load Status </b> </h3> 

              **State:** {{ states('sensor.ezbeq_load_status') }}
                **Author:** {{ state_attr('sensor.ezbeq_load_status', 'author') }}
                **Codec:** {{ state_attr('sensor.ezbeq_load_status', 'codec') }}
                **Edition:** {{ state_attr('sensor.ezbeq_load_status', 'edition') }}
                **Reason:** {{ state_attr('sensor.ezbeq_load_status', 'reason') }}
                **Updated:** {{ state_attr('sensor.ezbeq_load_status', 'last_changed') }}
                **Manual:** {{ state_attr('sensor.ezbeq_load_status', 'manual_load') }}
      - type: grid
        cards:
          - type: heading
            heading: Details
            heading_style: title
            icon: mdi:movie-check
          - type: markdown
            title: Selected Candidate
            content: >
              {% set ent = 'sensor.ezbeq_candidate_details' %}

              {% set title = states(ent) %}

              {% set attrs = state_attr(ent, 'all') or state_attr(ent, '') or
              state_attr(ent, '_dummy') %}

              {% macro get(name) -%}
                {{ state_attr(ent, name) }}
              {%- endmacro %}


              {% if title in ['none', 'disabled', 'unknown', 'unavailable'] %}

              **No candidate selected.**

              {% else %}

              ### {{ get('title') or title }}

              - **Year:** {{ get('year') or '—' }}

              - **Edition:** {{ get('edition') or '—' }}

              - **Audio type:** {{ get('audio_type') or '—' }}

              **All audio types:** {{
              state_attr('sensor.ezbeq_candidate_details','audio_types_text') }}

              - **Author:** {{ get('author') or '—' }}

              - **TMDB ID:** {{ get('tmdb_id') or '—' }}

              - **Source:** {{ get('source') or '—' }}

              - **Content type:** {{ get('content_type') or '—' }}

              - **Language:** {{ get('language') or '—' }}

              - **MV offset:** {{ get('mv') if get('mv') is not none else '—' }}

              - **Warning:** {{ get('warning') or '—' }}

              - **Note:** {{ get('note') or '—' }}

              - **Runtime (min):** {{ get('runtime_minutes') or '—' }}

              - **Genres:** {{
              state_attr('sensor.ezbeq_candidate_details','genres_text') }}


              {% if get('image1') %}

              ![Poster]({{ get('image1') }})

              {% endif %}

              {% if get('image2') %}

              ![Alt Image]({{ get('image2') }})

              {% endif %}

              {% endif %}
          - type: entities
            entities:
              - entity: sensor.ezbeq_candidate_tmdb_ids
              - entity: sensor.ezbeq_candidate_titles
            grid_options:
              columns: 12
              rows: 1
          - type: markdown
            title: ezbeq Candidate Status
            content: |2-
               **Stage:** {{ states('sensor.ezbeq_candidate_status') or '—' }}
                **Reason:** {{ state_attr('sensor.ezbeq_candidate_status', 'reason') or '—' }}

                **TMDB sensor found:** {{ state_attr('sensor.ezbeq_candidate_status', 'tmdb_sensor_found') }}
                **Title sensor found:** {{ state_attr('sensor.ezbeq_candidate_status', 'title_sensor_found') }}
                **TMDB values count:** {{ state_attr('sensor.ezbeq_candidate_status', 'tmdb_count') or 0 }}
                **Title values count:** {{ state_attr('sensor.ezbeq_candidate_status', 'title_count') or 0 }}

                **Candidates returned:** {{ state_attr('sensor.ezbeq_candidate_status', 'candidates') or 0 }}
                **Selected label:** {{ state_attr('sensor.ezbeq_candidate_status', 'selected') or '—' }}

                **Last updated (UTC):** {{ state_attr('sensor.ezbeq_candidate_status', 'last_updated') or '—' }}
                
               **Select state:** {{ states('select.ezbeq_candidate') or '—' }}
                  **Detail label:** {{ states('sensor.ezbeq_candidate_details') or '—' }}
```
  


