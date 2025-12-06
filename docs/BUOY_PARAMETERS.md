# Environment Canada Buoy Parameters

Complete list of all parameters captured from Environment Canada SWOB-ML buoy observations.

Last updated: 2025-12-06

## Parameter Categories

### Wave Metrics - Basic
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| wave_height_sig | sig_wave_hgt_pst20mts | meters | Significant wave height (Hs) - average of highest 1/3 of waves |
| wave_height_peak | pk_wave_hgt_pst20mts | meters | Peak wave height in 20-minute period |
| wave_height_max | max_wave_hgt_pst20mts | meters | Maximum wave height observed |
| wave_height_avg | avg_wave_hgt_pst20mts | meters | Average wave height |
| wave_crest_height_max | max_wave_crst_hgt_abv_avg_wtr_lvl_pst20mts | meters | Maximum wave crest height above average water level |

### Wave Metrics - Statistical (Added 2025-12-06)
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| wave_height_max_avg | avg_max_wave_hgt_pst20mts | meters | Average of maximum waves (1/10 highest) |
| wave_period_max_avg | avg_max_wave_pd_pst20mts | seconds | Period corresponding to maximum waves |

### Wave Metrics - Spectral
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| wave_height_spectral | spetrl_sig_wave_hgt_pst20mts | meters | Spectral significant wave height |
| wave_period_spectral | avg_spetrl_wave_pd_pst20mts | seconds | Average spectral wave period |
| wave_period_energy_spectral | spetrl_wave_enrgy_pd_pst20mts | seconds | Spectral wave energy period |

### Wave Period
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| wave_period_sig | avg_sig_wave_pd_pst20mts | seconds | Significant wave period (average) |
| wave_period_sig_basic | sig_wave_pd_pst20mts | seconds | Significant wave period (basic measurement) - Added 2025-12-06 |
| wave_period_avg | avg_wave_pd_pst20mts | seconds | Average wave period |
| wave_period_peak | pk_wave_pd_pst20mts | seconds | Peak wave period |
| wave_period_max_wave | pd_of_max_wave_hgt_pst20mts | seconds | Period of the maximum wave |

### Wave Direction
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| wave_direction_avg | avg_wave_dir_pst20mts | degrees | Average wave direction (from) |
| wave_direction_peak | avg_pk_wave_dir_pst20mts | degrees | Peak wave direction |
| wave_direction_spread_avg | avg_wave_dir_sprd_pst20mts | degrees | Average directional spread |
| wave_direction_spread_peak | pk_wave_dir_sprd_pst20mts | degrees | Peak directional spread |

### Wind - Primary Sensor
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| wind_speed | avg_wnd_spd_pst10mts | km/h | 10-minute average wind speed |
| wind_gust | max_avg_wnd_spd_pst10mts | km/h | Maximum wind speed (gust) in 10-minute period |
| wind_direction | avg_wnd_dir_pst10mts | degrees | 10-minute average wind direction (from) |
| wind_sensor_height | wnd_snsr_vert_disp | meters | Wind sensor height above sea level |

### Wind - Secondary Sensor (Redundancy)
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| wind_speed_sensor_2 | avg_wnd_spd_pst10mts_2 | km/h | Sensor 2: 10-minute average wind speed |
| wind_gust_sensor_2 | max_wnd_spd_pst10mts_2 | km/h | Sensor 2: Maximum wind speed (gust) |
| wind_direction_sensor_2 | avg_wnd_dir_pst10mts_2 | degrees | Sensor 2: 10-minute average wind direction |
| wind_samples_bad_1 | bad_wnd_smpls_1 | count | Sensor 1: Number of bad samples |
| wind_samples_bad_2 | bad_wnd_smpls_2 | count | Sensor 2: Number of bad samples |

### Temperature
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| air_temp | avg_air_temp_pst10mts | °C | 10-minute average air temperature |
| sea_temp | avg_sea_sfc_temp_pst10mts | °C | 10-minute average sea surface temperature |

### Pressure
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| pressure | avg_stn_pres_pst10mts | hPa | 10-minute average station pressure |
| pressure_msl | avg_mslp_pst10mts | hPa | 10-minute average mean sea level pressure |
| pressure_sensor_2 | avg_stn_pres_pst10mts_2 | hPa | Sensor 2: Station pressure (redundancy) |
| pressure_trend_char | pres_tend_char_pst3hrs | code | 3-hour pressure tendency character |
| pressure_trend_amount | pres_tend_amt_pst3hrs | hPa | 3-hour pressure tendency amount |

### Position & Navigation
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| buoy_lat_current | crnt_buoy_lat | degrees | Current buoy latitude (GPS) |
| buoy_lon_current | crnt_buoy_long | degrees | Current buoy longitude (GPS) |

### System Health & Monitoring (Added 2025-12-06)
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| battery_voltage | avg_batry_volt_pst10mts | volts | 10-minute average battery voltage |
| watchman_boot_count | wtchmn_boot_cnt_pst1hr | count | Number of system reboots in past hour |
| obstruction_lamp_current | avg_obstrn_lamp_crnt_pst10mts | amps | 10-minute average obstruction lamp current |
| compass_heading_1 | avg_cmpss_hdng_pst10mts_1 | degrees | Compass heading from sensor 1 |
| compass_heading_2 | avg_cmpss_hdng_pst10mts_2 | degrees | Compass heading from sensor 2 |

### Solar
| Field Name | XML Source | Unit | Description |
|------------|------------|------|-------------|
| solar_current | avg_solr_panl_crnt_pst10mts | amps | 10-minute average solar panel current (cloudiness indicator) |

## NOAA-Specific Parameters
(For NOAA buoys from .txt format)

| Field Name | Description |
|------------|-------------|
| swell_height | Swell wave height |
| swell_period | Swell wave period |
| swell_direction | Swell direction |
| wind_wave_height | Wind wave height (separate from swell) |
| wind_wave_period | Wind wave period |
| wind_wave_direction | Wind wave direction |

## Parameter Availability by Buoy Type

### Full-Featured Wave Buoys
- **Examples**: 4600131 (Halibut Bank), 4600146 (La Perouse Bank)
- **Available**: All wave, wind, temperature, pressure, system health, and orientation parameters
- **Note**: These buoys have complete wave spectral analysis capabilities

### Standard Wave Buoys
- **Examples**: 4600303 (Southern Georgia Strait), 4600304 (English Bay)
- **Available**: Basic wave metrics, wind, temperature, pressure, GPS, system health
- **Limited**: May not have full spectral wave analysis or compass headings

## Usage Notes

1. **Data Freshness**: All parameters use recent time windows:
   - Wave parameters: Past 20 minutes
   - Wind parameters: Past 10 minutes
   - Pressure trend: Past 3 hours
   - Boot count: Past 1 hour

2. **Missing Values**: Parameters may be `NULL` in database if:
   - Sensor is not present on that buoy
   - Sensor has failed
   - Data is marked as "MSNG" (missing) in XML

3. **Quality Control**: Many parameters include `qa_summary` qualifiers in the XML (not captured but used for filtering)

4. **Dual Sensors**: Wind and pressure have redundant sensors for reliability

5. **New Monitoring Parameters**: Battery voltage, boot count, lamp current, and compass headings enable proactive maintenance and drift monitoring

## References

- Source: Environment Canada SWOB-ML (Surface Weather Observation Markup Language)
- XML Namespace: `http://dms.ec.gc.ca/schema/point-observation/2.0`
- Data Format: Point observations with metadata
- Update Frequency: Varies by buoy (10-60 minutes)
