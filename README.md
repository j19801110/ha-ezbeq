# ha-ezbeq
a home assistant integration to automate EzBEQ functions. 
This is a fixed integration based on the brilliant work at https://github.com/iloveicedgreentea/ha-ezbeq.

Fixes:
- BEQ Profiles with no MV changes now load normally
- MV changes are NOT loaded into the MiniDSP by default. However, this is configurable using a variable in _init_.py by setting OVERRIDE_GAINS: bool = False.

New Features:
  - Ability to pull the main BEQ image from the database and dispay it on the dashboard (requires a new text input helper called 'ezbeq_tv_beq_image_url' so it becomes input_text.ezbeq_tv_beq_image_url).
  - Ability to search the catalogue based on audio codec substitutions defined in services.py IF the primary load fails to find a match. Can be enabled / disabled using enable_audio_codec_substitutions: true in the service call. Read more about this under Configurable Variables heading.
  - Status updates are available by reading the attributes of a new sensor called sensor.ezbeq_load_status. Now with quite detailed attributes for the load including the MV volume change. This can be used to set (or limit) the volume on your amplifier through Denon / Marantz or other AVR brand integrations.
  - See the full status of the MiniDSP device you have ezBEQ connected to, through the attributes of sensor.ezbeq_devices.

## Example Images
### No profile loaded

<img width="1063" height="257" alt="Screenshot 2026-01-09 at 10 19 58 pm" src="https://github.com/user-attachments/assets/faa94932-0c04-40c8-a0a8-7b0fd1d7a99a" />


### Profile loaded

<img width="1066" height="759" alt="Screenshot 2026-01-09 at 10 20 38 pm" src="https://github.com/user-attachments/assets/ac7e135e-903e-4b5b-a3b6-5d8a4a258213" />

### Added Load Status Updates

<img width="515" height="752" alt="Screenshot 2026-01-11 at 2 57 54 pm" src="https://github.com/user-attachments/assets/0c835265-5cd1-438d-b383-7c0bee860947" />

## Usage

ezBEQ Integration
Plex integration
Media player integration (if you know how to pull TMDB ID through)
Automations

## Installation
1. add the following as a repo into HAOS HACS: GitHub - iloveicedgreentea/ha-ezbeq: a home assistant integration to automate EzBEQ functions
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

#### BEQ Image URL (name ezbeq_tv_beq_image_url)

Create a text input helper with the name ezbeq_tv_beq_image_url so it becomes input_text.ezbeq_tv_beq_image_url in home assistant. This will have the URL of the BEQ image for the actively loaded profile that you can use to display the image.

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
  image_sensor: input_text.ezbeq_tv_beq_image_url

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
conditions:
  - condition: template
    value_template: "{{ states('sensor.ezbeq_tv_tmdb_id') | is_number }}"
  - condition: state
    entity_id: input_boolean.ezbeq_enable
    state: "on"
actions:
  - data:
      image_sensor: input_text.ezbeq_tv_beq_image_url
    action: ezbeq.unload_beq_profile
  - delay: "00:00:05"
  - data:
      tmdb_sensor: sensor.ezbeq_tv_tmdb_id
      year_sensor: sensor.ezbeq_tv_year
      codec_sensor: sensor.ezbeq_tv_codec
      edition_sensor: sensor.ezbeq_tv_edition_title
      slots:
        - 1
      dry_run_mode: false
      skip_search: false
      image_sensor: input_text.ezbeq_tv_beq_image_url
      enable_audio_codec_substitutions: false
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

### Displaying the BEQ image on the Dashboard
You have two options to do this: 
1. With the new image attributes above.
2. With a sensor that's specified during load. Use the following YAML to add a dashboard tile to display the BEQ profile image on your dashboard. This is helpful if you'd like to know what EQ is being applied at any moment, along with the profile name.

```yaml
type: markdown
content: |
  {% set url = states('input_text.ezbeq_tv_beq_image_url') %}
  <img src="{{ url }}" width="600">
grid_options:
  columns: full
```

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

