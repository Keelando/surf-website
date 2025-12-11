#!/usr/bin/env python3
"""
Sync Surrey FlowWorks water level data from buoy database to tide database.

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

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

from config import BUOY_DATABASE, TIDE_DATABASE
from logging_config import setup_logging

logger = setup_logging('surrey_tide_sync')

# Surrey station mapping (buoy_id -> tide station info)
SURREY_STATIONS = {
    "CRPILE": {
        "station_id": "surrey_crescent_ocean",
        "station_name": "crescent_beach_ocean",
        "display_name": "Crescent Beach Ocean (Surrey)",
        "note": "CGVD28 datum - use for tide comparison and storm surge residuals"
    },
    "CRCHAN": {
        "station_id": "surrey_crescent_channel",
        "station_name": "crescent_channel_ocean",
        "display_name": "Crescent Channel Ocean (Surrey)",
        "note": "CGVD28 datum - use for tide comparison and storm surge residuals"
    }
}


def sync_predictions(buoy_conn, tide_conn, buoy_id, station_info, hours=48):
    """
    Sync water_level_predicted from buoy DB to tide_prediction table.
    """
    buoy_cur = buoy_conn.cursor()
    tide_cur = tide_conn.cursor()

    # Get recent predictions from buoy database
    cutoff = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())

    buoy_cur.execute("""
        SELECT observation_time, water_level_predicted
        FROM buoy_observation
        WHERE buoy_id = ?
          AND observation_time >= ?
          AND water_level_predicted IS NOT NULL
        ORDER BY observation_time
    """, (buoy_id, cutoff))

    rows = buoy_cur.fetchall()
    if not rows:
        logger.debug(f"{station_info['display_name']}: No prediction data to sync")
        return 0

    # Insert into tide_prediction table
    inserted = 0
    for obs_time, water_level in rows:
        try:
            tide_cur.execute("""
                INSERT OR REPLACE INTO tide_prediction
                (station_id, station_name, prediction_time, water_level)
                VALUES (?, ?, ?, ?)
            """, (
                station_info['station_id'],
                station_info['station_name'],
                obs_time,
                water_level
            ))
            inserted += 1
        except sqlite3.Error as e:
            logger.warning(f"Failed to insert prediction: {e}")

    tide_conn.commit()
    logger.info(f"{station_info['display_name']}: Synced {inserted} prediction points")
    return inserted


def sync_observations(buoy_conn, tide_conn, buoy_id, station_info, hours=48):
    """
    Sync water_level_observed from buoy DB to tide_observation table.
    """
    buoy_cur = buoy_conn.cursor()
    tide_cur = tide_conn.cursor()

    # Get recent observations from buoy database
    cutoff = int((datetime.now(timezone.utc) - timedelta(hours=hours)).timestamp())

    buoy_cur.execute("""
        SELECT observation_time, water_level_observed
        FROM buoy_observation
        WHERE buoy_id = ?
          AND observation_time >= ?
          AND water_level_observed IS NOT NULL
        ORDER BY observation_time
    """, (buoy_id, cutoff))

    rows = buoy_cur.fetchall()
    if not rows:
        logger.debug(f"{station_info['display_name']}: No observation data to sync")
        return 0

    # Insert into tide_observation table
    inserted = 0
    for obs_time, water_level in rows:
        try:
            tide_cur.execute("""
                INSERT OR REPLACE INTO tide_observation
                (station_id, station_name, observation_time, water_level, quality)
                VALUES (?, ?, ?, ?, ?)
            """, (
                station_info['station_id'],
                station_info['station_name'],
                obs_time,
                water_level,
                "CGVD28"  # Use quality field to indicate datum
            ))
            inserted += 1
        except sqlite3.Error as e:
            logger.warning(f"Failed to insert observation: {e}")

    tide_conn.commit()
    logger.info(f"{station_info['display_name']}: Synced {inserted} observation points")
    return inserted


def main():
    """
    Sync Surrey water level data from buoy database to tide database.
    """
    logger.info("Syncing Surrey water level data to tide database...")

    # Connect to both databases
    buoy_conn = sqlite3.connect(BUOY_DATABASE)
    tide_conn = sqlite3.connect(TIDE_DATABASE)

    total_pred = 0
    total_obs = 0

    try:
        for buoy_id, station_info in SURREY_STATIONS.items():
            logger.info(f"Processing {station_info['display_name']}...")

            # Sync predictions (48 hours)
            pred_count = sync_predictions(buoy_conn, tide_conn, buoy_id, station_info, hours=48)
            total_pred += pred_count

            # Sync observations (48 hours)
            obs_count = sync_observations(buoy_conn, tide_conn, buoy_id, station_info, hours=48)
            total_obs += obs_count

        logger.info(f"Sync complete: {total_pred} predictions, {total_obs} observations")

    except Exception as e:
        logger.error(f"Sync failed: {e}", exc_info=True)
        return 1
    finally:
        buoy_conn.close()
        tide_conn.close()

    return 0


if __name__ == "__main__":
    exit(main())
