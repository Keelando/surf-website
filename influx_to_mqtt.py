#!/usr/bin/env python3
from pathlib import Path
from influxdb import InfluxDBClient
import paho.mqtt.client as mqtt
import json
import math
from datetime import datetime, timezone

env_path = Path("~/.config/buoy_influx_1.env").expanduser()
creds = {}
for line in env_path.read_text().splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        creds[k.strip()] = v.strip()

influx = InfluxDBClient(
    host=creds["INFLUX_HOST"],
    port=int(creds["INFLUX_PORT"]),
    username=creds["INFLUX_USER"],
    password=creds["INFLUX_PASS"],
    database=creds["INFLUX_DB"],
    ssl=False
)

# MQTT configuration from .env file
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(creds["MQTT_USER"], creds["MQTT_PASS"])
mqtt_client.connect(creds["MQTT_HOST"], int(creds["MQTT_PORT"]), 60)

BUOYS = {
    "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait"},
    "4600304": {"name": "English Bay", "location": "Vancouver Harbor"},
    "4600131": {"name": "Sentry Shoal", "location": "Northern Strait of Georgia"}
}

METRICS = {
    "wave_height_sig": {"name": "Significant Wave Height", "unit": "m", "icon": "mdi:wave"},
    "wave_height_peak": {"name": "Peak Wave Height", "unit": "m", "icon": "mdi:wave"},
    "wave_period_peak": {"name": "Peak Wave Period", "unit": "s", "icon": "mdi:timer"},
    "wave_direction_peak": {"name": "Peak Wave Direction", "unit": "°", "icon": "mdi:compass"},
    "wave_direction_peak_cardinal": {"name": "Peak Wave Direction Cardinal", "unit": "", "icon": "mdi:compass"},
    "wind_speed": {"name": "Wind Speed", "unit": "kt", "icon": "mdi:weather-windy"},
    "wind_gust": {"name": "Wind Gust", "unit": "kt", "icon": "mdi:weather-windy"},
    "wind_direction": {"name": "Wind Direction", "unit": "°", "icon": "mdi:compass"},
    "wind_direction_cardinal": {"name": "Wind Direction Cardinal", "unit": "", "icon": "mdi:compass"},
    "air_temp": {"name": "Air Temperature", "unit": "°C", "icon": "mdi:thermometer"},
    "sea_temp": {"name": "Sea Temperature", "unit": "°C", "icon": "mdi:thermometer"},
    "pressure": {"name": "Pressure", "unit": "hPa", "icon": "mdi:gauge"}
}

DIRS_16 = ['N','NNE','NE','ENE','E','ESE','SE','SSE',
           'S','SSW','SW','WSW','W','WNW','NW','NNW']

def degrees_to_cardinal(deg):
    if deg is None:
        return None
    try:
        d = float(deg)
    except (TypeError, ValueError):
        return None
    if math.isnan(d):
        return None
    d = d % 360.0
    idx = int((d + 11.25) // 22.5)
    return DIRS_16[idx % 16]

def kmh_to_knots(kmh):
    if kmh is None:
        return None
    return round(kmh * 0.539957, 1)

def publish_discovery(buoy_id, metric_key, metric_info):
    buoy_name = BUOYS[buoy_id]["name"]
    sensor_id = f"buoy_{buoy_id}_{metric_key}"
    
    config = {
        "name": f"{buoy_name} {metric_info['name']}",
        "unique_id": sensor_id,
        "state_topic": f"buoy/{buoy_id}/{metric_key}",
        "json_attributes_topic": f"buoy/{buoy_id}/{metric_key}/attributes",
        "icon": metric_info["icon"],
        "state_class": "measurement",
        "device": {
            "identifiers": [f"buoy_{buoy_id}"],
            "name": buoy_name,
            "model": "Marine Buoy",
            "manufacturer": "Environment Canada"
        }
    }
    
    if metric_info["unit"]:
        config["unit_of_measurement"] = metric_info["unit"]
    
    # Add device_class for specific sensor types
    if metric_key in ["air_temp", "sea_temp"]:
        config["device_class"] = "temperature"
    elif metric_key == "pressure":
        config["device_class"] = "pressure"
    
    discovery_topic = f"homeassistant/sensor/{sensor_id}/config"
    mqtt_client.publish(discovery_topic, json.dumps(config), retain=True)

def query_and_publish():
    latest_json = {}

    for buoy_id in BUOYS.keys():
        query = f"""
        SELECT last(*) FROM buoy_observation
        WHERE buoy_id = '{buoy_id}'
        AND time > now() - 3h
        """
        result = influx.query(query)
        if not result:
            continue
        points = list(result.get_points())
        if not points:
            continue

        data = points[0]
        obs_time = data.get('last_observation_time')

        buoy_json = {"name": BUOYS[buoy_id]["name"]}

        if obs_time:
            wave_time_str = datetime.fromtimestamp(obs_time, tz=timezone.utc).isoformat()
            buoy_json["observation_time"] = wave_time_str

            mqtt_client.publish(
                f"homeassistant/sensor/buoy_{buoy_id}_last_wave_update/config",
                json.dumps({
                    "name": f"{BUOYS[buoy_id]['name']} Last Wave Update",
                    "unique_id": f"buoy_{buoy_id}_last_wave_update",
                    "state_topic": f"buoy/{buoy_id}/last_wave_update",
                    "device_class": "timestamp",
                    "icon": "mdi:clock-outline",
                    "device": {
                        "identifiers": [f"buoy_{buoy_id}"],
                        "name": BUOYS[buoy_id]["name"],
                        "model": "Marine Buoy",
                        "manufacturer": "Environment Canada"
                    }
                }),
                retain=True
            )
            mqtt_client.publish(f"buoy/{buoy_id}/last_wave_update", wave_time_str, retain=True)
        
        processed_data = {}
        for metric_key, metric_info in METRICS.items():
            field_name = f"last_{metric_key}"
            if metric_key.endswith('_cardinal'):
                continue
            
            if field_name in data and data[field_name] is not None:
                value = data[field_name]
                if metric_key in ['wind_speed', 'wind_gust']:
                    value = kmh_to_knots(value)
                
                processed_data[metric_key] = value
                buoy_json[metric_key] = value

                publish_discovery(buoy_id, metric_key, metric_info)
                mqtt_client.publish(f"buoy/{buoy_id}/{metric_key}", value)
                
                if obs_time:
                    attributes = {
                        "observation_time": datetime.fromtimestamp(obs_time, tz=timezone.utc).isoformat(),
                        "buoy_id": buoy_id
                    }
                    mqtt_client.publish(
                        f"buoy/{buoy_id}/{metric_key}/attributes",
                        json.dumps(attributes)
                    )
        
        if 'wind_direction' in processed_data:
            cardinal = degrees_to_cardinal(processed_data['wind_direction'])
            if cardinal:
                buoy_json["wind_direction_cardinal"] = cardinal
                publish_discovery(buoy_id, 'wind_direction_cardinal', METRICS['wind_direction_cardinal'])
                mqtt_client.publish(f"buoy/{buoy_id}/wind_direction_cardinal", cardinal)
        
        if 'wave_direction_peak' in processed_data:
            cardinal = degrees_to_cardinal(processed_data['wave_direction_peak'])
            if cardinal:
                buoy_json["wave_direction_peak_cardinal"] = cardinal
                publish_discovery(buoy_id, 'wave_direction_peak_cardinal', METRICS['wave_direction_peak_cardinal'])
                mqtt_client.publish(f"buoy/{buoy_id}/wave_direction_peak_cardinal", cardinal)

        latest_json[buoy_id] = buoy_json
        print(f"Published {buoy_id} data")

    # --- NEW: write buoy data to JSON for the website ---
    out_path = Path("~/site/data/latest_buoy.json").expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(latest_json, indent=2))
    print(f"✅ Wrote JSON snapshot to {out_path}")

query_and_publish()
mqtt_client.disconnect()
