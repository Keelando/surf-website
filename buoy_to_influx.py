#!/usr/bin/env python3
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime
from influxdb import InfluxDBClient

# Load Influx config
env_path = Path("~/.config/buoy_influx_1.env").expanduser()
creds = {}
for line in env_path.read_text().splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        creds[k.strip()] = v.strip()

client = InfluxDBClient(
    host=creds["INFLUX_HOST"],
    port=int(creds["INFLUX_PORT"]),
    username=creds["INFLUX_USER"],
    password=creds["INFLUX_PASS"],
    database=creds["INFLUX_DB"],
    ssl=False
)

# Track processed files
processed_file = Path("~/.cache/buoy_processed.txt").expanduser()
processed_file.parent.mkdir(parents=True, exist_ok=True)
processed = set(processed_file.read_text().splitlines()
                ) if processed_file.exists() else set()


def parse_and_write_xml(filepath):
    tree = ET.parse(filepath)
    root = tree.getroot()

    time_elem = root.find('.//{http://www.opengis.net/gml}timePosition')
    if time_elem is None:
        return
    timestamp = datetime.fromisoformat(time_elem.text.replace('Z', '+00:00'))

    # Extract buoy ID - PREFER wmo_id_extnd (7 digits) over wmo_synop_id (5 digits)
    buoy_id = None
    synop_id = None
    for e in root.findall('.//{http://dms.ec.gc.ca/schema/point-observation/2.0}element'):
        if e.get('name') == 'wmo_id_extnd':
            buoy_id = e.get('value')
            break
        elif e.get('name') == 'wmo_synop_id':
            synop_id = e.get('value')

    if not buoy_id:
        buoy_id = synop_id

    if not buoy_id:
        print(f"No buoy_id found in {filepath.name}, skipping")
        return

    # Field map
    field_map = {
        'sig_wave_hgt_pst20mts': 'wave_height_sig',
        'avg_sig_wave_hgt_pst20mts': 'wave_height_sig',
        'sig_wave_hgt_pst35mts_10mts_ago': 'wave_height_sig',
        'pk_wave_hgt_pst20mts': 'wave_height_peak',
        'pk_wave_hgt_pst35mts_10mts_ago': 'wave_height_peak',
        'avg_wave_pd_pst20mts': 'wave_period_avg',
        'pk_wave_pd_pst20mts': 'wave_period_peak',
        'pk_wave_pd_pst35mts_10mts_ago': 'wave_period_peak',
        'avg_wave_dir_pst20mts': 'wave_direction_avg',
        'avg_pk_wave_dir_pst20mts': 'wave_direction_peak',
        'avg_wnd_spd_pst10mts': 'wind_speed',
        'avg_wnd_spd_pst10mts_1': 'wind_speed',
        'max_avg_wnd_spd_pst10mts': 'wind_gust',
        'max_avg_wnd_spd_pst10mts_1': 'wind_gust',
        'max_wnd_spd_pst10mts': 'wind_gust',  # Halibut Bank
        'max_avg_wnd_spd_pst10mts': 'wind_gust',  # English Bay, Georgia Strait
        'avg_wnd_dir_pst10mts': 'wind_direction',
        'avg_air_temp_pst10mts': 'air_temp',
        'avg_sea_sfc_temp_pst10mts': 'sea_temp',
        'avg_stn_pres_pst10mts': 'pressure',
    }

    # Parse fields
    fields = {}
    for e in root.findall('.//{http://dms.ec.gc.ca/schema/point-observation/2.0}element'):
        n, v = e.get('name'), e.get('value')
        if n in field_map and v:
            try:
                fields[field_map[n]] = float(v)
            except ValueError:
                pass

    if not fields:
        print(f"No valid fields in {filepath.name}, skipping")
        return

    # Add observation time
    fields['observation_time'] = int(timestamp.timestamp())

    point = {
        "measurement": "buoy_observation",
        "tags": {"buoy_id": buoy_id},
        "time": timestamp.isoformat(),
        "fields": fields,
    }

    try:
        client.write_points([point])
        print(f"Wrote {buoy_id} @ {timestamp} -> {list(fields.keys())}")
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")


# Main processing loop
xml_dir = Path("~/envcan_wave/data/buoy").expanduser()
new_count = 0

for xml_file in sorted(xml_dir.glob("*.xml")):
    file_path = str(xml_file)
    if file_path in processed:
        continue
    try:
        parse_and_write_xml(xml_file)
        processed.add(file_path)
        new_count += 1
    except Exception as e:
        print(f"Error processing {xml_file.name}: {e}")

processed_file.write_text('\n'.join(sorted(processed)))
print(f"\nProcessed {new_count} new files. Total tracked: {len(processed)}")
