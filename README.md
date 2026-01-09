# ha-ezbeq
a home assistant integration to automate EzBEQ functions. 
This is a fixed integration based on the brilliant work at https://github.com/iloveicedgreentea/ha-ezbeq.

Fixes:
- BEQ Profiles with no MV changes now load normally
- MV changes are NOT loaded into the MiniDSP by default. However, this is configurable using a variable in _init_.py by setting OVERRIDE_GAINS: bool = False.

New Features:
  - Ability to pull the main BEQ image from the database and dispay it on the dashboard (requires a new text input helper called 'ezbeq_tv_beq_image_url' so it becomes input_text.ezbeq_tv_beq_image_url)

## Example Images
### No profile loaded

<img width="1063" height="257" alt="Screenshot 2026-01-09 at 10 19 58 pm" src="https://github.com/user-attachments/assets/faa94932-0c04-40c8-a0a8-7b0fd1d7a99a" />


### Profile loaded

<img width="1066" height="759" alt="Screenshot 2026-01-09 at 10 20 38 pm" src="https://github.com/user-attachments/assets/ac7e135e-903e-4b5b-a3b6-5d8a4a258213" />

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

#### Edition (when put into the title with [ ] brackets such as Aliens [Director's Cut])
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
#### Codec (name: sensor.ezbeq_tv_codec)

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

Create a text input helper with the name ezbeq_tv_beq_image_url so it becomes input_text.ezbeq_tv_beq_image_url in home assistant.

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

### Displaying the loaded Profile on the Dashboard

Add the sensor 'master_current_profile' to the dashboard
The automations must force an update to this sensor at the end of a successful load or unload otherwise the sensor only updates every 15-30 seconds.

### Displaying the BEQ image on the dashboard
Use the following YAML to add a dashboard tile to display the BEQ profile image on your dashboard. This is helpful if you'd like to know what EQ is being applied at any moment, along with the profile name.

## Configurable variables within the code (in folder integrations/ezbeq)
You can only configure these variables by using a code editor addon within HA or using SSH. There are a number of .py (python) files that the intgeration runs to enable its logic. These files have some variables that are configurable which are listed here.

1. File: init.py variable OVERRIDE_GAINS: by setting this True, MV volume changes are NOT applied to the MiniDSP Input channels. By setting this to false, MV volume changes will be applied. This is enabled by default, which means there will be NO volume changes on the inputs. Make sure you have limiters set on your MiniDSP output channels for safety.
2. File Services.py, variable CATALOG_CACHE_TTL: this is the amount of time in seconds that the BEQ database is cached on HA before it is refreshed. Please note that this only affects the BEQ image currently, but might affect other anscillary data over time if this integration is developed further. It does not affect the main BEQ catalogue used for loading the profiles. The default is one week, but you can change this if you need to. Restarting HA will also reset the cache.

```yaml
type: markdown
content: |
  {% set url = states('input_text.ezbeq_tv_beq_image_url') %}
  <img src="{{ url }}" width="600">
grid_options:
  columns: full
```

## Blueprints

PLEASE NOTE: the below blueprints have not been updated since ezBEQ integration has been forked so they are now out of date. However, they are not needed for the integration to work. You can create your own automations based on the above code and ignore anything below this line. Blueprints really need to be included as part of the integration but haven't gotten round to integrating them.

### Load Blueprint

You can add these blueprints to use in automations

Load when the media player starts playing
```yaml
blueprint:
  name: Load EzBEQ Profile
  description: Load a BEQ profile using the EzBEQ integration based on sensor data
  domain: automation
  input:
    trigger_entity:
      name: Trigger Entity
      description: The entity that triggers this automation (e.g., media_player.living_room)
      selector:
        entity:
          domain: media_player
    tmdb_sensor:
      name: TMDB ID Sensor
      description: Sensor that provides the TMDB ID
      selector:
        entity:
          domain: sensor
    year_sensor:
      name: Year Sensor
      description: Sensor that provides the release year
      selector:
        entity:
          domain: sensor
    codec_sensor:
      name: Audio Codec Sensor
      description: Sensor that provides the audio codec
      selector:
        entity:
          domain: sensor
    edition_sensor:
      name: Edition Sensor
      description: Sensor that provides the specific edition (optional)
      default: ""
      selector:
        entity:
          domain: sensor
    title_sensor:
      name: Title Sensor
      description: Sensor that provides the title (optional)
      default: ""
      selector:
        entity:
```

### Unload Blueprint
Trigger unload based on a media player

```yaml
alias: "Unload BEQ Profile when Playback Stops"
description: "Unloads the BEQ profile when media playback stops"
use_blueprint:
  path: ezbeq/unload_beq_profile.yaml
  input:
    trigger_entity: media_player.living_room
    slots: [1, 2]
```
