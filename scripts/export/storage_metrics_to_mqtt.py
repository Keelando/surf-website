#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
"""
Storage Metrics to MQTT for Home Assistant

Collects metrics from the /mnt/storage drive (where webcam images are stored)
and publishes them to Home Assistant via MQTT Discovery.

Metrics collected:
- Disk usage (total, used, free, percentage)
- Webcam image counts (per camera)
- Webcam storage size (per camera)
- Oldest and newest image timestamps (per camera)

Runs via cron every 5 minutes.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
import paho.mqtt.client as mqtt
from lib.logging_config import setup_logging

logger = setup_logging('storage_metrics')

# MQTT configuration from .env file
env_path = Path("~/.config/buoy_influx_1.env").expanduser()
creds = {}
for line in env_path.read_text().splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        creds[k.strip()] = v.strip()

# Webcam archive directories
WEBCAM_ARCHIVES = {
    "whiterock": {
        "path": Path("/mnt/storage/whiterock_cam"),
        "prefix": "WR",
        "name": "White Rock East Beach"
    },
    "boundarybay": {
        "path": Path("/mnt/storage/boundarybay_cam"),
        "prefix": "BB",
        "name": "Boundary Bay"
    }
}

# Storage mount point
STORAGE_PATH = Path("/mnt/storage")


def get_disk_metrics():
    """Get disk usage metrics for the storage drive"""
    try:
        usage = shutil.disk_usage(STORAGE_PATH)
        return {
            "total_gb": round(usage.total / (1024**3), 2),
            "used_gb": round(usage.used / (1024**3), 2),
            "free_gb": round(usage.free / (1024**3), 2),
            "percent_used": round((usage.used / usage.total) * 100, 1)
        }
    except Exception as e:
        logger.error(f"Failed to get disk metrics: {e}")
        return None


def get_webcam_metrics(cam_config):
    """Get metrics for a specific webcam archive directory"""
    try:
        cam_path = cam_config["path"]
        prefix = cam_config["prefix"]

        if not cam_path.exists():
            logger.warning(f"Webcam path does not exist: {cam_path}")
            return None

        # Get all image files
        images = list(cam_path.glob(f"{prefix}_*.jpg"))

        if not images:
            return {
                "image_count": 0,
                "total_size_mb": 0,
                "oldest_image_age_hours": None,
                "newest_image_age_hours": None,
                "oldest_image_date": None,
                "newest_image_date": None
            }

        # Calculate total size
        total_size_bytes = sum(img.stat().st_size for img in images)
        total_size_mb = round(total_size_bytes / (1024 * 1024), 2)

        # Get oldest and newest images
        images_with_time = [(img, img.stat().st_mtime) for img in images]
        images_with_time.sort(key=lambda x: x[1])

        oldest_img, oldest_time = images_with_time[0]
        newest_img, newest_time = images_with_time[-1]

        now = datetime.now().timestamp()
        oldest_age_hours = round((now - oldest_time) / 3600, 1)
        newest_age_hours = round((now - newest_time) / 3600, 1)

        oldest_date = datetime.fromtimestamp(oldest_time, tz=timezone.utc).isoformat()
        newest_date = datetime.fromtimestamp(newest_time, tz=timezone.utc).isoformat()

        return {
            "image_count": len(images),
            "total_size_mb": total_size_mb,
            "oldest_image_age_hours": oldest_age_hours,
            "newest_image_age_hours": newest_age_hours,
            "oldest_image_date": oldest_date,
            "newest_image_date": newest_date
        }

    except Exception as e:
        logger.error(f"Failed to get webcam metrics for {cam_config['name']}: {e}")
        return None


def publish_disk_discovery(mqtt_client):
    """Publish Home Assistant MQTT Discovery for disk sensors"""

    sensors = {
        "storage_total": {
            "name": "Storage Total",
            "unit": "GB",
            "icon": "mdi:harddisk",
            "device_class": None
        },
        "storage_used": {
            "name": "Storage Used",
            "unit": "GB",
            "icon": "mdi:harddisk",
            "device_class": None
        },
        "storage_free": {
            "name": "Storage Free",
            "unit": "GB",
            "icon": "mdi:harddisk",
            "device_class": None
        },
        "storage_percent_used": {
            "name": "Storage Usage",
            "unit": "%",
            "icon": "mdi:chart-donut",
            "device_class": None
        }
    }

    for sensor_key, sensor_info in sensors.items():
        sensor_id = f"webcam_{sensor_key}"

        config = {
            "name": sensor_info["name"],
            "unique_id": sensor_id,
            "state_topic": f"storage/{sensor_key}",
            "icon": sensor_info["icon"],
            "state_class": "measurement",
            "device": {
                "identifiers": ["webcam_storage"],
                "name": "Surf Server Webcam Storage",
                "model": "External HDD",
                "manufacturer": "Custom"
            }
        }

        if sensor_info["unit"]:
            config["unit_of_measurement"] = sensor_info["unit"]

        if sensor_info["device_class"]:
            config["device_class"] = sensor_info["device_class"]

        discovery_topic = f"homeassistant/sensor/{sensor_id}/config"
        mqtt_client.publish(discovery_topic, json.dumps(config), retain=True)


def publish_webcam_discovery(mqtt_client, cam_id, cam_name):
    """Publish Home Assistant MQTT Discovery for webcam sensors"""

    sensors = {
        "image_count": {
            "name": f"{cam_name} Image Count",
            "unit": "images",
            "icon": "mdi:image-multiple"
        },
        "storage_size": {
            "name": f"{cam_name} Storage Size",
            "unit": "MB",
            "icon": "mdi:database"
        },
        "oldest_image_age": {
            "name": f"{cam_name} Oldest Image Age",
            "unit": "h",
            "icon": "mdi:clock-start"
        },
        "newest_image_age": {
            "name": f"{cam_name} Newest Image Age",
            "unit": "h",
            "icon": "mdi:clock-end"
        },
        "oldest_image_date": {
            "name": f"{cam_name} Oldest Image",
            "unit": None,
            "icon": "mdi:calendar-start",
            "device_class": "timestamp"
        },
        "newest_image_date": {
            "name": f"{cam_name} Latest Image",
            "unit": None,
            "icon": "mdi:calendar-end",
            "device_class": "timestamp"
        }
    }

    for sensor_key, sensor_info in sensors.items():
        sensor_id = f"webcam_{cam_id}_{sensor_key}"

        config = {
            "name": sensor_info["name"],
            "unique_id": sensor_id,
            "state_topic": f"storage/webcam/{cam_id}/{sensor_key}",
            "icon": sensor_info["icon"],
            "device": {
                "identifiers": ["webcam_storage"],
                "name": "Surf Server Webcam Storage",
                "model": "External HDD",
                "manufacturer": "Custom"
            }
        }

        # Only set state_class for numeric measurements (not timestamps)
        if sensor_info.get("device_class") != "timestamp":
            config["state_class"] = "measurement"

        if sensor_info["unit"]:
            config["unit_of_measurement"] = sensor_info["unit"]

        if sensor_info.get("device_class"):
            config["device_class"] = sensor_info["device_class"]

        discovery_topic = f"homeassistant/sensor/{sensor_id}/config"
        mqtt_client.publish(discovery_topic, json.dumps(config), retain=True)


def main():
    """Main execution"""
    logger.info("=== Storage Metrics Collection Started ===")

    # Connect to MQTT
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.username_pw_set(creds["MQTT_USER"], creds["MQTT_PASS"])

    try:
        mqtt_client.connect(creds["MQTT_HOST"], int(creds["MQTT_PORT"]), 60)
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {e}")
        return

    # Publish disk metrics
    logger.info("Collecting disk metrics...")
    disk_metrics = get_disk_metrics()

    if disk_metrics:
        publish_disk_discovery(mqtt_client)

        mqtt_client.publish("storage/storage_total", disk_metrics["total_gb"], retain=True)
        mqtt_client.publish("storage/storage_used", disk_metrics["used_gb"], retain=True)
        mqtt_client.publish("storage/storage_free", disk_metrics["free_gb"], retain=True)
        mqtt_client.publish("storage/storage_percent_used", disk_metrics["percent_used"], retain=True)

        logger.info(f"Disk: {disk_metrics['used_gb']} GB / {disk_metrics['total_gb']} GB ({disk_metrics['percent_used']}%)")

    # Publish webcam metrics
    for cam_id, cam_config in WEBCAM_ARCHIVES.items():
        logger.info(f"Collecting metrics for {cam_config['name']}...")
        cam_metrics = get_webcam_metrics(cam_config)

        if cam_metrics:
            publish_webcam_discovery(mqtt_client, cam_id, cam_config["name"])

            mqtt_client.publish(
                f"storage/webcam/{cam_id}/image_count",
                cam_metrics["image_count"],
                retain=True
            )
            mqtt_client.publish(
                f"storage/webcam/{cam_id}/storage_size",
                cam_metrics["total_size_mb"],
                retain=True
            )

            if cam_metrics["oldest_image_age_hours"] is not None:
                mqtt_client.publish(
                    f"storage/webcam/{cam_id}/oldest_image_age",
                    cam_metrics["oldest_image_age_hours"],
                    retain=True
                )
                mqtt_client.publish(
                    f"storage/webcam/{cam_id}/oldest_image_date",
                    cam_metrics["oldest_image_date"],
                    retain=True
                )

            if cam_metrics["newest_image_age_hours"] is not None:
                mqtt_client.publish(
                    f"storage/webcam/{cam_id}/newest_image_age",
                    cam_metrics["newest_image_age_hours"],
                    retain=True
                )
                mqtt_client.publish(
                    f"storage/webcam/{cam_id}/newest_image_date",
                    cam_metrics["newest_image_date"],
                    retain=True
                )

            logger.info(f"{cam_config['name']}: {cam_metrics['image_count']} images, {cam_metrics['total_size_mb']} MB")

    mqtt_client.disconnect()
    logger.info("=== Storage Metrics Collection Completed ===")


if __name__ == "__main__":
    main()
