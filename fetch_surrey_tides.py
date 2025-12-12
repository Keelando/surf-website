#!/usr/bin/env python3
"""
Fetch Surrey FlowWorks water level data and store to tide database.

This allows Surrey stations (Crescent Beach Ocean, Crescent Channel Ocean) to
appear in the tide dropdown/charts alongside DFO tide stations.

IMPORTANT DATUM NOTES:
- Surrey data uses CGVD28 GVRD datum (geodetic vertical datum)
- DFO tide data uses Chart Datum (marine/nautical datum)
- These are NOT directly comparable for absolute water levels
- Surrey data is best used for:
  1. Comparing observed vs predicted (tide forecast accuracy)
  2. Calculating tidal residuals (storm surge = observed - predicted)
"""

import requests
import sqlite3
import argparse
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

from config import TIDE_DATABASE
from logging_config import setup_logging

logger = setup_logging('surrey_tide_sync')

# ---- Configuration ----
API_BASE = "https://developers.flowworks.com/fwapi/v2"
USERNAME = "surreyrain"
PASSWORD = "surreyrain"

# Surrey station mapping with channel IDs
SURREY_STATIONS = {
    "crescentpile": {
        "site_id": 20182,
        "station_id": "surrey_crescent_ocean",
        "station_name": "crescent_beach_ocean",
        "display_name": "Crescent Beach Ocean (Surrey)",
        "channels": {
            "water_level_predicted": 2620,  # Tidal_Prediction_CGVD28_GVRD
            "water_level_observed": 2296,   # Anderra - CGVD28 GVRD Stage_10min
        }
    },
    "crescentchannel": {
        "site_id": 20183,
        "station_id": "surrey_crescent_channel",
        "station_name": "crescent_channel_ocean",
        "display_name": "Crescent Channel Ocean (Surrey)",
        "channels": {
            "water_level_predicted": 2621,  # Tidal_Prediction_CGVD28_GVRD
            "water_level_observed": 2279,   # PT - CGVD28 GVRD Stage
        }
    }
}


class FlowWorksAPI:
    def __init__(self, username, password):
        self.base_url = API_BASE
        self.username = username
        self.password = password
        self.token = None
        self.token_expiry = None

    def authenticate(self):
        """Get JWT token from FlowWorks API v2."""
        url = f"{self.base_url}/authenticate"
        payload = {"username": self.username, "password": self.password}

        try:
            response = requests.post(url, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()

            if "Token" in data:
                self.token = data.get("Token")
                expires_str = data.get("Expires")

                if expires_str:
                    self.token_expiry = datetime.fromisoformat(
                        expires_str.replace("Z", "+00:00")
                    )

                logger.info(f"Authenticated - expires {self.token_expiry}")
                return True
            else:
                logger.error(f"Auth failed: {data}")
                return False

        except Exception as e:
            logger.error(f"Auth error: {e}")
            return False

    def _ensure_token(self):
        """Ensure valid token exists."""
        if not self.token or not self.token_expiry:
            return self.authenticate()

        # Refresh if expires in <5 min
        if datetime.now(timezone.utc) >= self.token_expiry - timedelta(minutes=5):
            return self.authenticate()

        return True

    def _get_headers(self):
        """Get auth headers."""
        if not self._ensure_token():
            raise Exception("Failed to get valid token")
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def get_channel_data(self, site_id, channel_id, hours_past=0, hours_future=0):
        """
        Fetch data from specific channel.

        Args:
            hours_past: Hours backward from now (for observations/past predictions)
            hours_future: Hours forward from now (for future predictions)
        """
        url = f"{self.base_url}/sites/{site_id}/channels/{channel_id}/data"

        now = datetime.now(timezone.utc)

        if hours_future > 0:
            # Fetch prediction data (include 2 hours of past data to cover current time)
            # Predictions are on 20-min boundaries, so we need some overlap
            start = now - timedelta(hours=hours_past if hours_past > 0 else 2)
            end = now + timedelta(hours=hours_future)
        else:
            # Fetch PAST data (observations)
            start = now - timedelta(hours=hours_past)
            end = now

        params = {
            "startDateFilter": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "endDateFilter": end.strftime("%Y-%m-%dT%H:%M:%S")
        }

        try:
            response = requests.get(
                url, headers=self._get_headers(), params=params, timeout=15
            )
            response.raise_for_status()
            data = response.json()

            if data.get("ResultCode") == 0:
                return data.get("Resources", [])
            else:
                logger.warning(f"API error: {data.get('ResultMessage')}")
                return []

        except Exception as e:
            logger.error(f"Fetch error: {e}")
            return []


def parse_data_point(point):
    """Extract timestamp and value from FlowWorks data point."""
    timestamp_str = point.get("DataTime")
    value_str = point.get("DataValue")

    if not timestamp_str or value_str is None:
        return None, None

    try:
        # Parse ISO timestamp - Surrey API returns Pacific time (no TZ indicator)
        dt_naive = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

        # If timestamp has no timezone, assume Pacific time
        if dt_naive.tzinfo is None:
            dt = dt_naive.replace(tzinfo=ZoneInfo("America/Vancouver"))
        else:
            dt = dt_naive

        # Convert to UTC epoch for database storage
        return int(dt.timestamp()), float(value_str)
    except (ValueError, TypeError):
        return None, None


def fetch_predictions(api, station_config, tide_conn, hours_future=96):
    """
    Fetch water_level_predicted from Surrey API and store to tide_prediction table.

    Fetches predictions to match DFO pattern:
    - 2 hours PAST to current time (to cover current prediction at 20-min boundaries)
    - Current time + 4 days ahead (96 hours forward)
    - Ensures coverage for: current time + present calendar day + 2 full days + timezone margin
    """
    site_id = station_config["site_id"]
    channel_id = station_config["channels"]["water_level_predicted"]
    station_info = {
        "station_id": station_config["station_id"],
        "station_name": station_config["station_name"]
    }

    # Fetch from API (FUTURE data)
    data_points = api.get_channel_data(site_id, channel_id, hours_future=hours_future)

    if not data_points:
        logger.debug(f"{station_config['display_name']}: No prediction data available")
        return 0

    # Insert into tide_prediction table
    tide_cur = tide_conn.cursor()
    inserted = 0

    for point in data_points:
        timestamp, value = parse_data_point(point)

        if timestamp is None or value is None:
            continue

        try:
            tide_cur.execute("""
                INSERT OR IGNORE INTO tide_prediction
                (station_id, station_name, prediction_time, water_level)
                VALUES (?, ?, ?, ?)
            """, (
                station_info['station_id'],
                station_info['station_name'],
                timestamp,
                value
            ))
            if tide_cur.rowcount > 0:
                inserted += 1
        except sqlite3.Error as e:
            logger.warning(f"Failed to insert prediction: {e}")

    tide_conn.commit()
    logger.info(f"{station_config['display_name']}: Fetched {inserted} prediction points")
    return inserted


def fetch_observations(api, station_config, tide_conn, hours_past=48):
    """
    Fetch water_level_observed from Surrey API and store to tide_observation table.

    Fetches PAST observations (last 48 hours) for incremental updates.
    """
    site_id = station_config["site_id"]
    channel_id = station_config["channels"]["water_level_observed"]
    station_info = {
        "station_id": station_config["station_id"],
        "station_name": station_config["station_name"]
    }

    # Fetch from API (PAST data)
    data_points = api.get_channel_data(site_id, channel_id, hours_past=hours_past)

    if not data_points:
        logger.debug(f"{station_config['display_name']}: No observation data available")
        return 0

    # Insert into tide_observation table
    tide_cur = tide_conn.cursor()
    inserted = 0

    for point in data_points:
        timestamp, value = parse_data_point(point)

        if timestamp is None or value is None:
            continue

        try:
            tide_cur.execute("""
                INSERT OR REPLACE INTO tide_observation
                (station_id, station_name, observation_time, water_level, quality)
                VALUES (?, ?, ?, ?, ?)
            """, (
                station_info['station_id'],
                station_info['station_name'],
                timestamp,
                value,
                "CGVD28"  # Use quality field to indicate datum
            ))
            inserted += 1
        except sqlite3.Error as e:
            logger.warning(f"Failed to insert observation: {e}")

    tide_conn.commit()
    logger.info(f"{station_config['display_name']}: Fetched {inserted} observation points")
    return inserted


def main():
    """
    Fetch Surrey water level data from API and store to tide database.
    """
    parser = argparse.ArgumentParser(description="Fetch Surrey tide data from FlowWorks API")
    parser.add_argument('--predictions', action='store_true',
                       help='Fetch tide predictions (4-day forecast)')
    parser.add_argument('--observations', action='store_true',
                       help='Fetch tide observations (last 2 hours)')
    parser.add_argument('--all', action='store_true',
                       help='Fetch all data types (default)')
    args = parser.parse_args()

    # Default to --all if no flags specified
    if not (args.predictions or args.observations or args.all):
        args.all = True

    logger.info("Fetching Surrey water level data from FlowWorks API...")
    if args.predictions:
        logger.info("Mode: Predictions only")
    elif args.observations:
        logger.info("Mode: Observations only")
    else:
        logger.info("Mode: All data types")

    # Authenticate with Surrey API
    api = FlowWorksAPI(USERNAME, PASSWORD)
    if not api.authenticate():
        logger.error("Failed to authenticate with Surrey API")
        return 1

    # Connect to tide database
    tide_conn = sqlite3.connect(TIDE_DATABASE)

    total_pred = 0
    total_obs = 0

    try:
        for station_key, station_config in SURREY_STATIONS.items():
            logger.info(f"Processing {station_config['display_name']}...")

            # Fetch predictions (4 days forward - matches DFO pattern)
            # Covers: present calendar day + 2 full days + timezone margin
            if args.predictions or args.all:
                pred_count = fetch_predictions(api, station_config, tide_conn, hours_future=96)
                total_pred += pred_count

            # Fetch observations (2 hours backward - running every 20 min)
            if args.observations or args.all:
                obs_count = fetch_observations(api, station_config, tide_conn, hours_past=2)
                total_obs += obs_count

        if args.predictions or args.all:
            logger.info(f"Predictions: {total_pred} points")
        if args.observations or args.all:
            logger.info(f"Observations: {total_obs} points")

    except Exception as e:
        logger.error(f"Fetch failed: {e}", exc_info=True)
        return 1
    finally:
        tide_conn.close()

    return 0


if __name__ == "__main__":
    exit(main())
