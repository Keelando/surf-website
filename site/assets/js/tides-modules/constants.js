/**
 * Constants for tide display
 * Centralized configuration values
 */

export const PACIFIC_TZ = 'America/Vancouver';

export const STATION_DISPLAY_NAMES = {
  'point_atkinson': 'Point Atkinson',
  'kitsilano': 'Kitsilano',
  'tsawwassen': 'Tsawwassen',
  'whiterock': 'White Rock',
  'crescent_pile': 'Crescent Beach',
  'crescent_beach_ocean': 'Crescent Beach Ocean (Geodetic)',
  'crescent_channel_ocean': 'Crescent Channel Ocean (Geodetic)',
  'nanaimo': 'Nanoose Bay (Nanaimo)',
  'new_westminster': 'New Westminster',
  'campbell_river': 'Campbell River',
  'tofino': 'Tofino',
  'ucluelet': 'Ucluelet',
  'port_renfrew': 'Port Renfrew',
  'victoria_harbor': 'Victoria Harbor'
};

export const GEODETIC_STATIONS = ['crescent_beach_ocean', 'crescent_channel_ocean'];

export const GEODETIC_METHODOLOGIES = {
  'crescent_beach_ocean': 'calibrate_prediction',
  'crescent_channel_ocean': 'calibrate_observation'
};
