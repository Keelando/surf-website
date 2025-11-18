#!/usr/bin/env python3
"""
Fetch NOAA NDBC buoy data and store in local SQLite database.
Uses 5-day feeds for both .txt (meteorological) and .spec (spectral wave) data.
"""

import requests
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Shared utilities
from units import ms_to_kmh
from config import BUOY_DATABASE
from stations import STATIONS as STATION_REGISTRY
from logging_config import setup_logging

logger = setup_logging('noaa')

# ---- Configuration ----
# Get NOAA buoys from the unified station registry
NOAA_BUOYS = STATION_REGISTRY.get_buoys_by_source("NOAA NDBC")
STATIONS = {buoy_id: data["name"] for buoy_id, data in NOAA_BUOYS.items()}

# Set True once to backfill fields into existing rows
UPDATE_EXISTING = False

# ---- Field Mappings ----
FIELD_MAP_TXT = {
    "WSPD": "wind_speed",
    "GST":  "wind_gust",
    "WDIR": "wind_direction",
    "ATMP": "air_temp",
    "WTMP": "sea_temp",
    "PRES": "pressure",
}

FIELD_MAP_SPEC = {
    "WVHT": "wave_height_sig",
    "SwH":  "swell_height",
    "SwP":  "swell_period",
    "SwD":  "swell_direction",
    "WWH":  "wind_wave_height",
    "WWP":  "wind_wave_period",
    "WWD":  "wind_wave_direction",
    "APD":  "wave_period_avg",
    "MWD":  "wave_direction_peak",
}

# ---- Utilities ----
def parse_direction(val):
    """Handle cardinal ('WSW') or numeric degrees ('275')."""
    if val is None or str(val).strip().upper() in ("MM", "M", "NA", ""):
        return None
    try:
        deg = float(val)
        return deg % 360.0
    except ValueError:
        pass
    dir_map = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5,
        'E': 90, 'ESE': 112.5, 'SE': 135, 'SSE': 157.5,
        'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
        'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
    }
    return dir_map.get(str(val).strip().upper())

def is_missing(val, field=None):
    """Return True if a NOAA field value is missing or invalid."""
    if val is None:
        return True
    s = str(val).strip().upper()
    # MM is the standard NOAA missing data indicator
    if s in ("MM", "M", "NA", ""):
        return True
    
    # Direction fields (SwD, WWD, MWD) can be textual; treat as present if not MM
    if field in {"SwD", "WWD", "MWD"}:
        return False
    
    # For numeric fields, just check if parseable (removed 999 check - valid pressure!)
    try:
        float(s)
        return False
    except ValueError:
        return True

# ---- Fetch NOAA data ----
def fetch_noaa_txt(station):
    """Fetch NOAA meteorological data (5-day feed preferred, realtime2 fallback, last 6h)."""
    # Try 5-day feed first (more data)
    urls = [
        f"https://www.ndbc.noaa.gov/data/5day2/{station}_5day.txt",
        f"https://www.ndbc.noaa.gov/data/realtime2/{station}.txt"
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            lines = r.text.strip().splitlines()
            if len(lines) < 3:
                continue
            header = lines[0].split()
            cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
            records = []
            for line in lines[2:]:
                parts = line.split()
                if len(parts) != len(header):
                    continue
                row = dict(zip(header, parts))
                try:
                    ts = datetime.strptime(
                        f"{row['#YY']} {row['MM']} {row['DD']} {row['hh']} {row['mm']}",
                        "%Y %m %d %H %M"
                    ).replace(tzinfo=timezone.utc)
                    if ts >= cutoff:
                        row["timestamp"] = int(ts.timestamp())
                        records.append(row)
                except Exception:
                    continue
            if records:
                feed_type = "5day" if "5day2" in url else "realtime2"
                logger.info(f"Fetched {len(records)} records from .txt ({feed_type}, last 6h)")
                return records
        except Exception as e:
            if url == urls[-1]:  # Last URL failed
                logger.warning(f"Failed to fetch .txt file from all sources: {e}")
            continue
    return []

def fetch_noaa_spec(station):
    """Fetch NOAA spectral wave data (5-day feed, last 6h)."""
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{station}.spec"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        lines = r.text.strip().splitlines()
        header = lines[0].split()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
        records = []
        for line in lines[2:]:
            parts = line.split()
            if len(parts) != len(header):
                continue
            row = dict(zip(header, parts))
            try:
                ts = datetime.strptime(
                    f"{row['#YY']} {row['MM']} {row['DD']} {row['hh']} {row['mm']}",
                    "%Y %m %d %H %M"
                ).replace(tzinfo=timezone.utc)
                if ts >= cutoff:
                    row["timestamp"] = int(ts.timestamp())
                    records.append(row)
            except Exception:
                continue
        logger.info(f"Fetched {len(records)} records from .spec (last 6h)")
        return records
    except Exception as e:
        logger.warning(f"Failed to fetch .spec file: {e}")
        return []

# ---- Merge and insert ----
def merge_records(txt_records, spec_records):
    """Combine both feeds with proper direction and range handling."""
    merged = {}
    RANGES = {
        "WSPD": (0, 200), "GST": (0, 200),
        "ATMP": (-20, 50), "WTMP": (-5, 35), "PRES": (800, 1100),
        "WVHT": (0, 30), "SwH": (0, 30), "WWH": (0, 30),
        "SwP": (1, 30), "WWP": (1, 30), "APD": (1, 30)
    }

    # ---- Meteorological ----
    for row in txt_records:
        ts = row["timestamp"]
        merged.setdefault(ts, {"timestamp": ts})
        for nf, dbf in FIELD_MAP_TXT.items():
            raw = row.get(nf)
            if is_missing(raw, nf):
                continue
            try:
                val = float(raw)
                if nf in ("WSPD", "GST"):
                    val = ms_to_kmh(val)
                if nf in RANGES:
                    lo, hi = RANGES[nf]
                    if not (lo <= val <= hi):
                        continue
                merged[ts][dbf] = round(val, 2)
            except (ValueError, TypeError):
                continue

    # ---- Spectral ----
    for row in spec_records:
        ts = row["timestamp"]
        merged.setdefault(ts, {"timestamp": ts})
        for nf, dbf in FIELD_MAP_SPEC.items():
            raw = row.get(nf)
            if is_missing(raw, nf):
                continue

            # Handle directions (both cardinal text and numeric degrees)
            if nf in {"SwD", "WWD", "MWD"}:
                deg = parse_direction(raw)
                if deg is not None:
                    merged[ts][dbf] = deg
                continue

            # Handle numeric fields
            try:
                val = float(raw)
                if nf in RANGES:
                    lo, hi = RANGES[nf]
                    if not (lo <= val <= hi):
                        continue
                merged[ts][dbf] = round(val, 2)
            except (ValueError, TypeError):
                continue

    # Mirror swell data to wave_* fields for frontend compatibility
    for rec in merged.values():
        if "swell_height" in rec:
            rec["wave_height_peak"] = rec["swell_height"]
        if "swell_period" in rec:
            rec["wave_period_peak"] = rec["swell_period"]
        # Don't mirror swell_direction to wave_direction_peak - use MWD directly

    return list(merged.values())

def ensure_columns_exist(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing = {r[1] for r in cur.fetchall()}
    newcols = [
        "swell_height REAL",
        "swell_period REAL",
        "swell_direction REAL",
        "wind_wave_height REAL",
        "wind_wave_period REAL",
        "wind_wave_direction REAL",
        "wave_direction_peak REAL",
    ]
    for c in newcols:
        name = c.split()[0]
        if name not in existing:
            cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {c};")
            logger.info(f"Added column: {name}")
    conn.commit()

def insert_sqlite(conn, buoy_id, timestamp, fields):
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing_cols = {r[1] for r in cur.fetchall()}
    fcols = [f for f in fields if f in existing_cols and fields[f] is not None]
    if not fcols:
        return

    cols = ["buoy_id", "observation_time"] + fcols + ["source_file"]
    vals = [buoy_id, timestamp] + [fields[c] for c in fcols] + [f"{buoy_id}_5day"]
    placeholders = ",".join("?" * len(cols))

    if UPDATE_EXISTING:
        set_clause = ", ".join([f"{c}=excluded.{c}" for c in fcols])
        sql = f"""
        INSERT INTO buoy_observation ({','.join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(buoy_id, observation_time)
        DO UPDATE SET {set_clause}
        """
    else:
        sql = f"INSERT OR IGNORE INTO buoy_observation ({','.join(cols)}) VALUES ({placeholders})"

    cur.execute(sql, vals)
    conn.commit()

# ---- Main ----
def main():
    conn = sqlite3.connect(BUOY_DATABASE)
    ensure_columns_exist(conn)
    
    total_inserted = 0
    total_ignored = 0
    
    for station_id, station_name in STATIONS.items():
        logger.info(f"Processing {station_name} ({station_id})")

        txt = fetch_noaa_txt(station_id)
        spec = fetch_noaa_spec(station_id)

        if not txt and not spec:
            logger.warning(f"No data retrieved for {station_id}")
            continue

        merged = merge_records(txt, spec)
        if not merged:
            logger.warning(f"No merged records for {station_id}")
            continue

        ins = ign = 0
        for rec in merged:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM buoy_observation WHERE buoy_id=? AND observation_time=?",
                        (station_id, rec["timestamp"]))
            existed = cur.fetchone()[0] > 0
            insert_sqlite(conn, station_id, rec["timestamp"], rec)
            if existed:
                ign += 1
            else:
                ins += 1

        logger.info(f"Inserted {ins}, ignored {ign} duplicates")
        total_inserted += ins
        total_ignored += ign

    conn.close()

    logger.info("=" * 60)
    logger.info(f"Total: Inserted {total_inserted}, ignored {total_ignored} duplicates")
    logger.info(f"Database: {BUOY_DATABASE}")

if __name__ == "__main__":
    main()