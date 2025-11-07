#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import json
import math
from datetime import datetime, timezone

# ---------- Config ----------
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
OUT_PATH = Path("~/site/data/latest_buoy_v2.json").expanduser()

# Freshness policy: how old can data be before we stop displaying it?
FRESHNESS_WINDOW = 2 * 3600  # 2 hours in seconds

BUOYS = {
    "4600146": {"name": "Halibut Bank", "location": "Center Strait of Georgia"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait of Georgia"},
    "4600304": {"name": "English Bay", "location": "South of Bowen Island"},
    "4600131": {"name": "Sentry Shoal", "location": "Northern Strait of Georgia"},
    "46087": {"name": "Neah Bay", "location": "Cape Flattery, WA"},
    "46088": {"name": "New Dungeness (Hein Bank)", "location": "Strait of Juan de Fuca, East"},
    "CRPILE": {"name": "Crescent Beach Ocean", "location": "Crescent Beach, Surrey"},
    "CRCHAN": {"name": "Crescent Channel", "location": "Boundary Bay Channel"},
    "COLEB": {"name": "Colebrook", "location": "Colebrook Pump House"},
}

# Fields to query individually (each gets most recent non-null value within 2 hours)
ALL_FIELDS = [
    "wave_height_sig", "wave_height_peak",
    "wave_period_sig", "wave_period_avg", "wave_period_peak",
    "wave_direction_avg", "wave_direction_peak",
    "swell_height", "swell_period", "swell_direction",
    "wind_wave_height", "wind_wave_period", "wind_wave_direction",
    "wind_speed", "wind_gust", "wind_direction",
    "air_temp", "sea_temp", "pressure"
]

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
    try:
        return round(float(kmh) * 0.539957, 1)
    except (TypeError, ValueError):
        return None

def safe_json_write(path: Path, data: dict):
    """Atomic write: temp file + rename to avoid partial writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(path)

def query_and_export():
    latest_json = {}

    with sqlite3.connect(SQLITE_PATH, timeout=5) as conn:
        # Enable WAL mode for safe concurrent reads during ingestion
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Guard against schema drift
        cur.execute("PRAGMA table_info(buoy_observation);")
        existing_cols = {row[1] for row in cur.fetchall()}
        
        available_fields = [f for f in ALL_FIELDS if f in existing_cols]

        if not {"buoy_id", "observation_time"}.issubset(existing_cols):
            print("⚠️  Table buoy_observation missing required columns.")
            return

        for buoy_id in BUOYS.keys():
            buoy_json = {"name": BUOYS[buoy_id]["name"]}

            # Get the most recent observation time (for reference)
            cur.execute("""
                SELECT observation_time
                FROM buoy_observation
                WHERE buoy_id = ?
                ORDER BY observation_time DESC
                LIMIT 1
            """, (buoy_id,))
            latest_row = cur.fetchone()

            if not latest_row:
                continue

            latest_time = latest_row["observation_time"]
            buoy_json["observation_time"] = datetime.fromtimestamp(latest_time, tz=timezone.utc).isoformat()

            # Calculate staleness for UI indicators
            now_ts = datetime.now(timezone.utc).timestamp()
            age_minutes = (now_ts - latest_time) / 60
            buoy_json["stale"] = age_minutes > 120  # >2 hours old

            # Query each field individually - get most recent non-null value within freshness window
            cutoff_time = latest_time - FRESHNESS_WINDOW

            for field in available_fields:
                sql = f"""
                SELECT observation_time, {field}
                FROM buoy_observation
                WHERE buoy_id = ?
                  AND observation_time >= ?
                  AND observation_time <= ?
                  AND {field} IS NOT NULL
                ORDER BY observation_time DESC
                LIMIT 1
                """
                cur.execute(sql, (buoy_id, cutoff_time, latest_time))
                row = cur.fetchone()

                if row:
                    value = row[field]
                    field_time = row["observation_time"]
                    
                    # Convert wind speeds from km/h to knots (future-proof for wind_wave_speed)
                    if "wind" in field and field.endswith(("speed", "gust")):
                        value = kmh_to_knots(value)
                    
                    buoy_json[field] = value
                    
                    # Track individual field timestamps if different from main observation
                    if field_time != latest_time:
                        if "field_times" not in buoy_json:
                            buoy_json["field_times"] = {}
                        buoy_json["field_times"][field] = datetime.fromtimestamp(field_time, tz=timezone.utc).isoformat()

            # Add cardinal directions
            if 'wind_direction' in buoy_json and buoy_json['wind_direction'] is not None:
                cardinal = degrees_to_cardinal(buoy_json['wind_direction'])
                if cardinal:
                    buoy_json["wind_direction_cardinal"] = cardinal

            if 'wave_direction_peak' in buoy_json and buoy_json['wave_direction_peak'] is not None:
                cardinal = degrees_to_cardinal(buoy_json['wave_direction_peak'])
                if cardinal:
                    buoy_json["wave_direction_peak_cardinal"] = cardinal

            if 'swell_direction' in buoy_json and buoy_json['swell_direction'] is not None:
                cardinal = degrees_to_cardinal(buoy_json['swell_direction'])
                if cardinal:
                    buoy_json["swell_direction_cardinal"] = cardinal

            if 'wind_wave_direction' in buoy_json and buoy_json['wind_wave_direction'] is not None:
                cardinal = degrees_to_cardinal(buoy_json['wind_wave_direction'])
                if cardinal:
                    buoy_json["wind_wave_direction_cardinal"] = cardinal

            # Skip buoys with no actual data (only name + observation_time + stale flag)
            if len(buoy_json.keys()) <= 3:
                print(f"⏭️  Skipped {buoy_id} (no data within freshness window)")
                continue

            latest_json[buoy_id] = buoy_json
            print(f"✅ Exported {buoy_id} ({BUOYS[buoy_id]['name']})")

    # Add metadata about this export
    latest_json["_meta"] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "freshness_window_seconds": FRESHNESS_WINDOW,
        "freshness_window_human": f"{FRESHNESS_WINDOW // 3600}h"
    }

    # Atomic write
    safe_json_write(OUT_PATH, latest_json)
    
    # Count actual buoys (exclude _meta)
    buoy_count = len([k for k in latest_json.keys() if k != "_meta"])
    
    print(f"\n✅ Wrote JSON snapshot to {OUT_PATH}")
    print(f"📊 Total buoys: {buoy_count}")

if __name__ == "__main__":
    query_and_export()