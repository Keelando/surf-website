#!/usr/bin/env python3
"""
Export latest lightstation observations to JSON for website display.

Outputs: ~/site/data/latest_lightstation.json

Format:
{
  "CAPE MUDGE": {
    "region": "STRAIT OF GEORGIA",
    "wind_speed_kt": 27.0,
    "wind_direction": "SOUTHEAST",
    "wind_gusting": true,
    "wind_estimated": true,
    "sea_height_ft": 4.0,
    "sea_condition": "MODERATE",
    "swell_intensity": "LOW",
    "swell_direction": "SOUTHERLY",
    "observation_time": "2025-11-25T18:10:00+00:00",
    "report_time_str": "10 AM Tuesday",
    "stale": false,
    "stale_after_hours": 9.0,
    "bulletins": ["FPCN61"],
    "schedule": {"slots_utc": ["00:10", ...], "reports_per_day": 4, ...}
  },
  ...
}
"""

import sqlite3
from datetime import datetime, timezone

# Shared utilities
from lib.config import EXPORT_DIR, safe_json_write
from lib.config import LIGHTSTATION_DATABASE as DB_PATH
from lib.lightstation_schedule import infer_schedule, staleness_threshold_hours
from lib.logging_config import setup_logging
from lib.stations import get_lightstation_by_report_name

logger = setup_logging("lightstation_json_export")

# ---------- Config ----------
OUT_PATH = EXPORT_DIR / "latest_lightstation.json"

# How much history the publishing-schedule inference gets to look at. Long
# enough for a daily cycle to be obvious, short enough that a station which
# changed its habits is described by what it does now, not last month.
SCHEDULE_LOOKBACK_DAYS = 30

# Which Environment Canada product(s) a station's observations actually arrive
# in, measured over the same window as the schedule rather than declared in a
# table. The page uses this to point each station's "View source" link at
# something that really carries it: SXCN is rendered on EC's public
# Lightstation Reports page, FPCN61 is not published as a page at all.
#
# It has to be measured per station because the two products cover overlapping
# but different rosters — nine stations appear in both, and eleven in only one.
# And it has to be measured over history rather than read off the newest row:
# `report_time_str` names whichever bulletin happened to arrive FIRST for that
# observation (the merge deliberately leaves it alone, see parse_lightstation),
# so for a dual-bulletin station it flips with arrival order. `source_file`
# accumulates every product that contributed, which is the stable answer.
BULLETIN_PRODUCTS = ("SXCN", "FPCN61")


# ---------- Region ----------
#
# `region` is per-observation in the database, and the two bulletin products
# disagree about it: SXCN can only name the area its whole bulletin covers, so
# it files the central-coast lights under HECATE STRAIT and Trial Island under
# STRAIT OF GEORGIA. Whichever bulletin wrote the newest row used to decide
# which group a station appeared in on the page, so nine stations drifted
# between sections as the feeds alternated.
#
# config/stations.json carries a per-station `region` that does not move. It is
# the only thing consulted here; the column stays in the database as a record
# of what each bulletin claimed, and is not exported.
def station_region(station_name, fallback=None):
    """The registry's region for a station, or `fallback` if it is unregistered.

    An unregistered station is a real case — TRIPLE ISLAND arrives on bulletins
    we parse but has never been added to config/stations.json — so say so once
    rather than dropping the station or filing it under nothing.
    """
    metadata = get_lightstation_by_report_name(station_name)
    if metadata and metadata.get("region"):
        return metadata["region"]
    logger.warning(
        f"{station_name} is not in config/stations.json; "
        f"falling back to the region its bulletin claimed ({fallback!r})"
    )
    return fallback


def query_and_export():
    if not DB_PATH.exists():
        logger.warning(f"Lightstation database not found: {DB_PATH}")
        logger.info("No data to export yet. This is normal during initial setup.")
        return

    latest_json = {}

    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        # Enable WAL mode for safe concurrent reads
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get all unique station names
        cur.execute("""
            SELECT DISTINCT station_name
            FROM lightstation_observation
            ORDER BY station_name
        """)
        stations = [row[0] for row in cur.fetchall()]

        logger.info(f"Exporting data for {len(stations)} lightstation(s)")

        for station_name in stations:
            # Get the most recent observation for this station
            cur.execute(
                """
                SELECT *
                FROM lightstation_observation
                WHERE station_name = ?
                ORDER BY observation_time DESC
                LIMIT 1
            """,
                (station_name,),
            )

            row = cur.fetchone()
            if not row:
                continue

            # Check if this observation has null key values
            # If so, fall back to the most recent observation with non-null data
            has_data = (
                row["wind_speed_kt"] is not None
                or row["wind_calm"]
                or row["sea_height_ft"] is not None
                or row["sea_condition"] is not None
            )

            if not has_data:
                # Fall back to last observation with actual data
                cur.execute(
                    """
                    SELECT *
                    FROM lightstation_observation
                    WHERE station_name = ?
                      AND (wind_speed_kt IS NOT NULL
                           OR wind_calm = 1
                           OR sea_height_ft IS NOT NULL
                           OR sea_condition IS NOT NULL)
                    ORDER BY observation_time DESC
                    LIMIT 1
                """,
                    (station_name,),
                )

                fallback_row = cur.fetchone()
                if fallback_row:
                    row = fallback_row

            observation_time = row["observation_time"]
            now_ts = datetime.now(timezone.utc).timestamp()
            age_hours = (now_ts - observation_time) / 3600

            # Publishing schedule, inferred from this station's own history —
            # see lib/lightstation_schedule.py for why it is not read from
            # `update_frequency_hours` in the registry.
            cur.execute(
                """
                SELECT observation_time
                FROM lightstation_observation
                WHERE station_name = ? AND observation_time > ?
            """,
                (station_name, now_ts - SCHEDULE_LOOKBACK_DAYS * 86400),
            )
            schedule = infer_schedule(r[0] for r in cur.fetchall())

            # Staleness is measured against this station's own cadence, not a
            # flat threshold — see staleness_threshold_hours(). Exported so the
            # page can name the number instead of hardcoding one that is wrong
            # for a third of the stations.
            stale_after_hours = staleness_threshold_hours(schedule)
            is_stale = age_hours > stale_after_hours

            # Bulletin membership over the same window (see BULLETIN_PRODUCTS).
            cur.execute(
                """
                SELECT source_file
                FROM lightstation_observation
                WHERE station_name = ? AND observation_time > ?
                  AND source_file IS NOT NULL
            """,
                (station_name, now_ts - SCHEDULE_LOOKBACK_DAYS * 86400),
            )
            # A merged row's source_file is "fileA+fileB", so substring-match
            # each product rather than splitting on a delimiter.
            seen = " ".join(r[0] for r in cur.fetchall())
            bulletins = [p for p in BULLETIN_PRODUCTS if p in seen]

            # Build JSON entry
            station_json = {
                "region": station_region(station_name, row["region"]),
                "wind_speed_kt": row["wind_speed_kt"],
                "wind_direction": row["wind_direction"],
                "wind_gusting": bool(row["wind_gusting"]),
                "wind_calm": bool(row["wind_calm"]),
                "wind_estimated": bool(row["wind_estimated"]),
                "sea_height_ft": row["sea_height_ft"],
                "sea_condition": row["sea_condition"],
                "swell_intensity": row["swell_intensity"],
                "swell_direction": row["swell_direction"],
                "observation_time": datetime.fromtimestamp(observation_time, tz=timezone.utc).isoformat(),
                "report_time_str": row["report_time_str"],
                "stale": is_stale,
                "stale_after_hours": stale_after_hours,
                "bulletins": bulletins,
                "schedule": schedule,
            }

            latest_json[station_name] = station_json

    # Write to file
    safe_json_write(OUT_PATH, latest_json)
    logger.info(f"✓ Exported {len(latest_json)} station(s) to {OUT_PATH}")


def main():
    logger.info("=== Exporting Lightstation JSON ===")
    query_and_export()
    logger.info("=== Export complete ===")


if __name__ == "__main__":
    main()
