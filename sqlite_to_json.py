#!/usr/bin/env python3
from pathlib import Path
import sqlite3
import json
import math
from datetime import datetime, timezone

# ---------- Config ----------
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
OUT_PATH = Path("~/site/data/latest_buoy_v2.json").expanduser()

BUOYS = {
    "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait"},
    "4600304": {"name": "English Bay", "location": "Vancouver Harbor"},
    "4600131": {"name": "Sentry Shoal", "location": "Northern Strait of Georgia"},
}

# Non-wave fields (updated every 10 min)
REALTIME_FIELDS = ["wind_speed", "wind_gust", "wind_direction", "air_temp", "sea_temp", "pressure"]

# Wave fields (updated every 30-60 min)
WAVE_FIELDS = [
    "wave_height_sig", "wave_height_peak",
    "wave_period_sig", "wave_period_avg", "wave_period_peak",
    "wave_direction_avg", "wave_direction_peak"
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

    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Guard against schema drift
        cur.execute("PRAGMA table_info(buoy_observation);")
        existing_cols = {row[1] for row in cur.fetchall()}
        
        available_realtime = [f for f in REALTIME_FIELDS if f in existing_cols]
        available_wave = [f for f in WAVE_FIELDS if f in existing_cols]

        if not {"buoy_id", "observation_time"}.issubset(existing_cols):
            print("⚠️  Table buoy_observation missing required columns.")
            return

        for buoy_id in BUOYS.keys():
            buoy_json = {"name": BUOYS[buoy_id]["name"]}

            # 1. Get the most recent observation (for wind/temp/pressure)
            realtime_cols = ["observation_time"] + available_realtime
            realtime_sql = f"""
            SELECT {", ".join(realtime_cols)}
            FROM buoy_observation
            WHERE buoy_id = ?
            ORDER BY observation_time DESC
            LIMIT 1
            """
            cur.execute(realtime_sql, (buoy_id,))
            realtime_row = cur.fetchone()

            if not realtime_row:
                continue

            # Add main observation time
            obs_time = realtime_row["observation_time"]
            buoy_json["observation_time"] = datetime.fromtimestamp(obs_time, tz=timezone.utc).isoformat()

            # Add realtime fields
            for field in available_realtime:
                value = realtime_row[field]
                if value is not None:
                    if field in ['wind_speed', 'wind_gust']:
                        value = kmh_to_knots(value)
                    buoy_json[field] = value

            # 2. Get the most recent wave observation within the last hour
            one_hour_ago = obs_time - 3600
            wave_cols = ["observation_time"] + available_wave
            wave_sql = f"""
            SELECT {", ".join(wave_cols)}
            FROM buoy_observation
            WHERE buoy_id = ?
              AND observation_time >= ?
              AND observation_time <= ?
              AND wave_height_sig IS NOT NULL
            ORDER BY observation_time DESC
            LIMIT 1
            """
            cur.execute(wave_sql, (buoy_id, one_hour_ago, obs_time))
            wave_row = cur.fetchone()

            if wave_row:
                wave_time = wave_row["observation_time"]
                
                # If wave data is from a different time, note it
                if wave_time != obs_time:
                    buoy_json["wave_observation_time"] = datetime.fromtimestamp(wave_time, tz=timezone.utc).isoformat()

                # Add wave fields
                for field in available_wave:
                    value = wave_row[field]
                    if value is not None:
                        buoy_json[field] = value

            # Add cardinal directions
            if 'wind_direction' in buoy_json and buoy_json['wind_direction'] is not None:
                cardinal = degrees_to_cardinal(buoy_json['wind_direction'])
                if cardinal:
                    buoy_json["wind_direction_cardinal"] = cardinal

            if 'wave_direction_peak' in buoy_json and buoy_json['wave_direction_peak'] is not None:
                cardinal = degrees_to_cardinal(buoy_json['wave_direction_peak'])
                if cardinal:
                    buoy_json["wave_direction_peak_cardinal"] = cardinal

            latest_json[buoy_id] = buoy_json
            
            wave_status = "✅" if wave_row else "❌"
            print(f"{wave_status} Exported {buoy_id} ({BUOYS[buoy_id]['name']})")

    # Atomic write
    safe_json_write(OUT_PATH, latest_json)
    print(f"\n✅ Wrote JSON snapshot to {OUT_PATH}")
    print(f"📊 Total buoys: {len(latest_json)}")

if __name__ == "__main__":
    query_and_export()