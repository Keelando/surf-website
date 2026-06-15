#!/usr/bin/env python3
"""
Storage Metrics to MQTT for Home Assistant

Collects metrics from the /mnt/storage drive (where webcam images are stored)
and publishes them to Home Assistant via MQTT Discovery.

Metrics collected:
- Disk usage (total, used, free, percentage)
- Per-camera: image count, archive size, latest-image timestamp

The camera roster is read from `config/webcams.json` (single source of truth) so
this never drifts from the fetch pipeline.

Runs via cron hourly (crontab: `3 * * * *`).

NOTE: discovery payloads are retained. After changing any sensor's discovery
config (attributes, expire_after) or the sensor roster, set PUBLISH_DISCOVERY
below to True for ONE run so Home Assistant picks up the changes, then set it
back to False.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

from lib.logging_config import setup_logging
from lib.webcam import time_limit

logger = setup_logging("storage_metrics")

REPO_ROOT = Path(__file__).resolve().parents[2]

# MQTT configuration from .env file
env_path = Path("~/.config/buoy_influx_1.env").expanduser()
creds = {}
for line in env_path.read_text().splitlines():
    if "=" in line:
        k, v = line.split("=", 1)
        creds[k.strip()] = v.strip()

# Storage mount point
STORAGE_PATH = Path("/mnt/storage")

# Bound every read against the external SSD so a wedged USB-SATA bridge can't hang
# this hourly cron job (and stack runs). On timeout the read degrades to None and
# the sensor expires to "unavailable" in HA — the same failure signal as a dead run.
STORAGE_READ_TIMEOUT = 20  # seconds

# Mark a sensor unavailable in HA if no fresh value arrives within this window.
# The publisher runs hourly, so 2.5 h tolerates one slow/missed run and flags two.
EXPIRE_AFTER_SECONDS = 9000

# All sensors group under one HA device.
DEVICE = {
    "identifiers": ["webcam_storage"],
    "name": "Surf Server Webcam Storage",
    "model": "External SSD (USB-SATA)",
    "manufacturer": "Custom",
}

# Set to True to (re)publish MQTT discovery for ONE run, then set back to False.
PUBLISH_DISCOVERY = False

# --- One-time discovery cleanup -------------------------------------------------
# Retained discovery configs left on the broker by the previous (rushed) version:
# the mis-keyed "hollyburn" camera (now read as "ambleside" from webcams.json) and
# the redundant age/oldest-date sensors that were trimmed. Publishing an empty
# retained payload to each topic removes the orphaned entity from Home Assistant.
# Runs only when PUBLISH_DISCOVERY is True; safe to delete this list once cleared.
_OLD_WEBCAM_SENSOR_TYPES = [
    "image_count",
    "storage_size",
    "oldest_image_age",
    "newest_image_age",
    "oldest_image_date",
    "newest_image_date",
]
_TRIMMED_SENSOR_TYPES = ["oldest_image_age", "newest_image_age", "oldest_image_date"]
_PREVIOUSLY_PUBLISHED_CAM_IDS = ["whiterock", "boundarybay", "coxbay", "mudbay"]
RETIRED_SENSOR_IDS = [f"webcam_hollyburn_{t}" for t in _OLD_WEBCAM_SENSOR_TYPES] + [
    f"webcam_{cam}_{t}" for cam in _PREVIOUSLY_PUBLISHED_CAM_IDS for t in _TRIMMED_SENSOR_TYPES
]


def load_webcam_archives():
    """Build {cam_id: {path, prefix, name}} from config/webcams.json (single source)."""
    raw = json.loads((REPO_ROOT / "config" / "webcams.json").read_text())
    archives = {}
    for cam_id, cfg in raw.items():
        if cam_id.startswith("_"):
            continue
        archives[cam_id] = {
            "path": Path(cfg["archive_dir"]),
            "prefix": cfg["prefix"],
            "name": cfg["name"],
        }
    return archives


def get_disk_metrics():
    """Get disk usage metrics for the storage drive"""
    try:
        with time_limit(STORAGE_READ_TIMEOUT, "disk usage"):
            usage = shutil.disk_usage(STORAGE_PATH)
        return {
            "total_gb": round(usage.total / (1024**3), 2),
            "used_gb": round(usage.used / (1024**3), 2),
            "free_gb": round(usage.free / (1024**3), 2),
            "percent_used": round((usage.used / usage.total) * 100, 1),
        }
    except Exception as e:
        logger.error(f"Failed to get disk metrics: {e}")
        return None


def get_webcam_metrics(cam_config):
    """Get metrics for a specific webcam archive directory.

    Returns {image_count, total_size_mb, newest_image_date} or None on error.
    """
    try:
        cam_path = cam_config["path"]
        prefix = cam_config["prefix"]

        if not cam_path.exists():
            logger.warning(f"Webcam path does not exist: {cam_path}")
            return None

        # glob + per-file stat() all touch the external SSD; bound the whole scan.
        with time_limit(STORAGE_READ_TIMEOUT, f"scan {cam_config['name']}"):
            images = list(cam_path.glob(f"{prefix}_*.jpg"))
            if not images:
                return {"image_count": 0, "total_size_mb": 0, "newest_image_date": None}

            total_size_mb = round(sum(img.stat().st_size for img in images) / (1024 * 1024), 2)
            newest_time = max(img.stat().st_mtime for img in images)
        newest_date = datetime.fromtimestamp(newest_time, tz=timezone.utc).isoformat()

        return {
            "image_count": len(images),
            "total_size_mb": total_size_mb,
            "newest_image_date": newest_date,
        }

    except Exception as e:
        logger.error(f"Failed to get webcam metrics for {cam_config['name']}: {e}")
        return None


def _publish_discovery(mqtt_client, sensor_id, state_topic, spec):
    """Publish one HA MQTT-discovery config from a sensor spec dict."""
    config = {
        "name": spec["name"],
        "unique_id": sensor_id,
        "state_topic": state_topic,
        "icon": spec["icon"],
        "expire_after": EXPIRE_AFTER_SECONDS,
        "device": DEVICE,
    }
    if spec.get("device_class"):
        config["device_class"] = spec["device_class"]
    if spec.get("state_class"):
        config["state_class"] = spec["state_class"]
    if spec.get("unit"):
        config["unit_of_measurement"] = spec["unit"]
    mqtt_client.publish(f"homeassistant/sensor/{sensor_id}/config", json.dumps(config), retain=True)


def retire_orphaned_discovery(mqtt_client):
    """Clear retained discovery configs for renamed/trimmed sensors (one-time)."""
    for sensor_id in RETIRED_SENSOR_IDS:
        mqtt_client.publish(f"homeassistant/sensor/{sensor_id}/config", "", retain=True)
    logger.info(f"Retired {len(RETIRED_SENSOR_IDS)} orphaned discovery configs")


def publish_disk_discovery(mqtt_client):
    """Publish Home Assistant MQTT Discovery for disk sensors"""
    sensors = {
        "storage_total": {"name": "Storage Total", "unit": "GB", "icon": "mdi:harddisk",
                          "device_class": "data_size", "state_class": "measurement"},
        "storage_used": {"name": "Storage Used", "unit": "GB", "icon": "mdi:harddisk",
                         "device_class": "data_size", "state_class": "measurement"},
        "storage_free": {"name": "Storage Free", "unit": "GB", "icon": "mdi:harddisk",
                         "device_class": "data_size", "state_class": "measurement"},
        "storage_percent_used": {"name": "Storage Usage", "unit": "%", "icon": "mdi:chart-donut",
                                 "device_class": None, "state_class": "measurement"},
    }
    for sensor_key, spec in sensors.items():
        _publish_discovery(mqtt_client, f"webcam_{sensor_key}", f"storage/{sensor_key}", spec)


def publish_webcam_discovery(mqtt_client, cam_id, cam_name):
    """Publish Home Assistant MQTT Discovery for a camera's sensors"""
    sensors = {
        "image_count": {"name": f"{cam_name} Image Count", "unit": "images",
                        "icon": "mdi:image-multiple", "device_class": None, "state_class": "measurement"},
        "storage_size": {"name": f"{cam_name} Storage Size", "unit": "MB", "icon": "mdi:database",
                         "device_class": "data_size", "state_class": "measurement"},
        "newest_image_date": {"name": f"{cam_name} Latest Image", "unit": None,
                              "icon": "mdi:calendar-end", "device_class": "timestamp", "state_class": None},
    }
    for sensor_key, spec in sensors.items():
        _publish_discovery(
            mqtt_client, f"webcam_{cam_id}_{sensor_key}", f"storage/webcam/{cam_id}/{sensor_key}", spec
        )


def main():
    """Main execution"""
    logger.info("=== Storage Metrics Collection Started ===")

    webcam_archives = load_webcam_archives()

    # Connect to MQTT
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.username_pw_set(creds["MQTT_USER"], creds["MQTT_PASS"])

    try:
        mqtt_client.connect(creds["MQTT_HOST"], int(creds["MQTT_PORT"]), 60)
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {e}")
        return

    if PUBLISH_DISCOVERY:
        retire_orphaned_discovery(mqtt_client)

    # Publish disk metrics
    logger.info("Collecting disk metrics...")
    disk_metrics = get_disk_metrics()

    if disk_metrics:
        if PUBLISH_DISCOVERY:
            publish_disk_discovery(mqtt_client)

        mqtt_client.publish("storage/storage_total", disk_metrics["total_gb"], retain=True)
        mqtt_client.publish("storage/storage_used", disk_metrics["used_gb"], retain=True)
        mqtt_client.publish("storage/storage_free", disk_metrics["free_gb"], retain=True)
        mqtt_client.publish("storage/storage_percent_used", disk_metrics["percent_used"], retain=True)

        logger.info(
            f"Disk: {disk_metrics['used_gb']} GB / {disk_metrics['total_gb']} GB " f"({disk_metrics['percent_used']}%)"
        )

    # Publish webcam metrics
    for cam_id, cam_config in webcam_archives.items():
        logger.info(f"Collecting metrics for {cam_config['name']}...")
        cam_metrics = get_webcam_metrics(cam_config)

        if cam_metrics:
            if PUBLISH_DISCOVERY:
                publish_webcam_discovery(mqtt_client, cam_id, cam_config["name"])

            mqtt_client.publish(f"storage/webcam/{cam_id}/image_count", cam_metrics["image_count"], retain=True)
            mqtt_client.publish(f"storage/webcam/{cam_id}/storage_size", cam_metrics["total_size_mb"], retain=True)

            # Left unpublished (so the sensor expires to "unavailable") when the
            # archive is empty — i.e. there is genuinely no latest image.
            if cam_metrics["newest_image_date"] is not None:
                mqtt_client.publish(
                    f"storage/webcam/{cam_id}/newest_image_date", cam_metrics["newest_image_date"], retain=True
                )

            logger.info(f"{cam_config['name']}: {cam_metrics['image_count']} images, {cam_metrics['total_size_mb']} MB")

    mqtt_client.disconnect()
    logger.info("=== Storage Metrics Collection Completed ===")


if __name__ == "__main__":
    main()
