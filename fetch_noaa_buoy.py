#!/usr/bin/env python3
"""
Fetch NOAA NDBC buoy data (e.g., Neah Bay 46087) and store in local SQLite database.
Uses 5-day feeds for both .txt (meteorological) and .spec (spectral wave) data.
"""

import requests
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

# ---- Configuration ----
STATION = "46087"  # Neah Bay - 6 NM North of Cape Flattery, WA
URL_TXT = f"https://www.ndbc.noaa.gov/data/5day2/{STATION}_5day.txt"
URL_SPEC = f"https://www.ndbc.noaa.gov/data/5day2/{STATION}_5day.spec"
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()

# Map .txt fields to database columns
FIELD_MAP_TXT = {
    "WSPD": "wind_speed",          # Wind speed (m/s → km/h)
    "GST":  "wind_gust",           # Gust (m/s → km/h)
    "WDIR": "wind_direction",      # Wind dir (°)
    "ATMP": "air_temp",            # Air temp (°C)
    "WTMP": "sea_temp",            # Sea surface temp (°C)
    "PRES": "pressure",            # Pressure (hPa)
}

# Map .spec fields to database columns
FIELD_MAP_SPEC = {
    "WVHT": "wave_height_sig",        # Sig wave height (keep for reference)
    "SwH":  "swell_height",           # Swell height - MAIN wave metric for Neah Bay
    "SwP":  "swell_period",           # Swell period - MAIN period for Neah Bay
    "SwD":  "swell_direction",        # Swell direction - MAIN direction (cardinal → degrees)
    "WWH":  "wind_wave_height",       # Wind wave height - secondary detail
    "WWP":  "wind_wave_period",       # Wind wave period - secondary detail
    "WWD":  "wind_wave_direction",    # Wind wave direction - secondary detail
    "APD":  "wave_period_avg",        # Average wave period
    "MWD":  "wave_direction_peak",    # Mean wave direction - for display consistency
}


def ms_to_kmh(ms):
    """Convert m/s to km/h to match Environment Canada format."""
    return round(ms * 3.6, 2)


def parse_direction(dir_str):
    """Convert cardinal direction (e.g., 'WSW') to degrees, or return None."""
    dir_map = {
        'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5,
        'E': 90, 'ESE': 112.5, 'SE': 135, 'SSE': 157.5,
        'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
        'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
    }
    return dir_map.get(dir_str.upper())


def fetch_noaa_txt():
    """Download and parse the NOAA 5-day .txt file (meteorological data) - last 6 hours only."""
    from datetime import timedelta
    
    try:
        r = requests.get(URL_TXT, timeout=15)
        r.raise_for_status()
        lines = r.text.strip().splitlines()
        if len(lines) < 3:
            print("⚠️  NOAA .txt feed returned too few lines.")
            return []
        
        header = lines[0].split()
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=6)
        
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
                )
                ts = ts.replace(tzinfo=timezone.utc)
                
                if ts >= cutoff_time:
                    row["timestamp"] = int(ts.timestamp())
                    records.append(row)
            except Exception:
                continue
        
        print(f"✅ Fetched {len(records)} records from .txt (last 6h)")
        return records
        
    except Exception as e:
        print(f"❌ Failed to fetch .txt file: {e}")
        return []


def fetch_noaa_spec():
    """Download and parse the NOAA 5-day .spec file (spectral wave data) - last 6 hours only."""
    from datetime import timedelta
    
    try:
        r = requests.get(URL_SPEC, timeout=15)
        r.raise_for_status()
        lines = r.text.strip().splitlines()
        if len(lines) < 3:
            print("⚠️  NOAA .spec feed returned too few lines.")
            return []
        
        header = lines[0].split()
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=6)
        
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
                )
                ts = ts.replace(tzinfo=timezone.utc)
                
                if ts >= cutoff_time:
                    row["timestamp"] = int(ts.timestamp())
                    records.append(row)
            except Exception:
                continue
        
        print(f"✅ Fetched {len(records)} records from .spec (last 6h)")
        return records
        
    except Exception as e:
        print(f"❌ Failed to fetch .spec file: {e}")
        return []


def merge_records(txt_records, spec_records):
    """Merge .txt and .spec records by timestamp, applying NOAA-safe missing value rules."""
    merged = {}

    def is_missing(val):
        """Return True if a NOAA field value is missing or invalid."""
        if val is None:
            return True
        s = str(val).strip().upper()
        if s in ("MM", "M", "NA", ""):
            return True
        try:
            f = float(s)
            return f in (99.0, 999.0, 9999.0)  # NOAA missing markers
        except ValueError:
            return True

    # Sanity ranges for validation
    RANGES = {
        "WSPD": (0, 200), "GST": (0, 200),  # km/h after conversion
        "ATMP": (-20, 50), "WTMP": (-5, 35), "PRES": (800, 1100),
        "WVHT": (0, 30), "SwH": (0, 30), "WWH": (0, 30),
        "SwP": (1, 30), "WWP": (1, 30), "APD": (1, 30)
    }

    # --- Merge meteorological (.txt) data ---
    for row in txt_records:
        ts = row["timestamp"]
        if ts not in merged:
            merged[ts] = {"timestamp": ts}

        for noaa_field, db_field in FIELD_MAP_TXT.items():
            raw_val = row.get(noaa_field)
            if is_missing(raw_val):
                continue
            try:
                val = float(raw_val)
                
                # Convert wind speeds
                if noaa_field in ("WSPD", "GST"):
                    val = ms_to_kmh(val)
                
                # Range check
                if noaa_field in RANGES:
                    min_val, max_val = RANGES[noaa_field]
                    if not (min_val <= val <= max_val):
                        continue
                
                merged[ts][db_field] = round(val, 2)
            except (ValueError, TypeError):
                continue

    # --- Merge spectral (.spec) data ---
    for row in spec_records:
        ts = row["timestamp"]
        if ts not in merged:
            merged[ts] = {"timestamp": ts}

        for noaa_field, db_field in FIELD_MAP_SPEC.items():
            raw_val = row.get(noaa_field)
            if is_missing(raw_val):
                continue

            # Handle cardinal directions
            if noaa_field in ("SwD", "WWD", "MWD"):
                deg = parse_direction(raw_val)
                if deg is not None:
                    merged[ts][db_field] = deg
                continue

            try:
                val = float(raw_val)
                
                # Range check
                if noaa_field in RANGES:
                    min_val, max_val = RANGES[noaa_field]
                    if not (min_val <= val <= max_val):
                        continue
                
                merged[ts][db_field] = round(val, 2)
            except (ValueError, TypeError):
                continue

    # Mirror swell as peak for frontend consistency
    for record in merged.values():
        if "swell_height" in record:
            record["wave_height_peak"] = record["swell_height"]
        if "swell_period" in record:
            record["wave_period_peak"] = record["swell_period"]
        if "swell_direction" in record:
            record["wave_direction_peak"] = record["swell_direction"]

    return list(merged.values())


def insert_sqlite(conn, buoy_id, timestamp, fields):
    """Insert one row safely (unique by buoy_id + observation_time)."""
    cur = conn.cursor()
    
    # Get existing columns
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing_cols = {row[1] for row in cur.fetchall()}
    
    # Only use fields that exist in the table
    field_cols = [c for c in fields if c in existing_cols and fields[c] is not None]
    
    if not field_cols:
        return
    
    cols = ["buoy_id", "observation_time"] + field_cols + ["source_file"]
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT OR IGNORE INTO buoy_observation ({','.join(cols)}) VALUES ({placeholders})"
    vals = [buoy_id, timestamp] + [fields[c] for c in field_cols] + [f"{STATION}_5day"]
    cur.execute(sql, vals)
    conn.commit()


def ensure_columns_exist(conn):
    """Add any missing columns to the database."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing_cols = {row[1] for row in cur.fetchall()}
    
    new_columns = [
        "swell_height REAL",
        "swell_period REAL",
        "swell_direction REAL",
        "wind_wave_height REAL",
        "wind_wave_period REAL",
        "wind_wave_direction REAL",
    ]
    
    for col_def in new_columns:
        col_name = col_def.split()[0]
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {col_def};")
            print(f"ℹ️  Added column: {col_name}")
    
    conn.commit()


def main():
    # Fetch both files independently
    txt_records = fetch_noaa_txt()
    spec_records = fetch_noaa_spec()
    
    if not txt_records and not spec_records:
        print("⚠️  No data retrieved from either source.")
        return
    
    # Merge records by timestamp
    merged_records = merge_records(txt_records, spec_records)
    
    if not merged_records:
        print("⚠️  No merged records to insert.")
        return
    
    # Insert into SQLite
    conn = sqlite3.connect(SQLITE_PATH)
    ensure_columns_exist(conn)
    
    inserted = 0
    ignored = 0
    
    for record in merged_records:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM buoy_observation WHERE buoy_id = ? AND observation_time = ?", 
                    (STATION, record["timestamp"]))
        existed = cur.fetchone()[0] > 0
        
        insert_sqlite(conn, STATION, record["timestamp"], record)
        
        if existed:
            ignored += 1
        else:
            inserted += 1
    
    conn.close()
    
    if ignored > 0:
        print(f"✅ Inserted {inserted} new records, ignored {ignored} duplicates into {SQLITE_PATH}")
    else:
        print(f"✅ Inserted {inserted} new records into {SQLITE_PATH}")


if __name__ == "__main__":
    main()