#!/usr/bin/env python3
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime
import sqlite3

# ---- Optional Influx sink (soft dependency) ----
class InfluxSink:
    def __init__(self, env_path):
        self.online = False
        self.client = None
        try:
            from influxdb import InfluxDBClient
        except ImportError as e:
            print(f"ℹ️  Influx client not installed ({e}); running SQLite-only.")
            return

        creds = {}
        p = Path(env_path).expanduser()
        if not p.exists():
            print(f"ℹ️  Influx env file not found at {p}; running SQLite-only.")
            return

        for line in p.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                creds[k.strip()] = v.strip()

        try:
            self.client = InfluxDBClient(
                host=creds.get("INFLUX_HOST"),
                port=int(creds.get("INFLUX_PORT", 8086)),
                username=creds.get("INFLUX_USER"),
                password=creds.get("INFLUX_PASS"),
                database=creds.get("INFLUX_DB"),
                ssl=False,
                timeout=5,
            )
            self.client.ping()
            self.online = True
            print("✅ InfluxDB connection established.")
        except Exception as e:
            print(f"⚠️  InfluxDB unavailable ({e}); running SQLite-only.")
            self.online = False

    def write_point(self, measurement, tags, time_iso, fields_dict):
        if not self.online:
            return
        point = {
            "measurement": measurement,
            "tags": tags,
            "time": time_iso,
            "fields": fields_dict,
        }
        try:
            self.client.write_points([point])
        except Exception as e:
            print(f"⚠️  Lost Influx connection: {e}. Disabling Influx for this run.")
            self.online = False


# ---- SQLite setup ----
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)

# Fields we may insert (order is stable for INSERT)
EXPECTED_FIELDS = [
    "wave_height_sig",
    "wave_height_peak",
    "wave_period_sig",
    "wave_period_avg",
    "wave_period_peak",
    "wave_direction_avg",
    "wave_direction_peak",
    "wind_speed",
    "wind_gust",
    "wind_direction",
    "air_temp",
    "sea_temp",
    "pressure",
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS buoy_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buoy_id TEXT NOT NULL,
    observation_time INTEGER NOT NULL,
    wave_height_sig REAL,
    wave_height_peak REAL,
    wave_period_sig REAL,
    wave_period_avg REAL,
    wave_period_peak REAL,
    wave_direction_avg REAL,
    wave_direction_peak REAL,
    wind_speed REAL,
    wind_gust REAL,
    wind_direction REAL,
    air_temp REAL,
    sea_temp REAL,
    pressure REAL,
    source_file TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);
"""

CREATE_INDEXES_SQL = [
    # Fast "latest by buoy" queries
    "CREATE INDEX IF NOT EXISTS idx_buoy_time ON buoy_observation(buoy_id, observation_time DESC);",
    # De-dup safeguard: same buoy_id + timestamp won't double insert
    "CREATE UNIQUE INDEX IF NOT EXISTS uniq_buoy_ts ON buoy_observation(buoy_id, observation_time);",
]

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    for stmt in CREATE_INDEXES_SQL:
        cur.execute(stmt)
    # Auto-add any missing columns from EXPECTED_FIELDS (safe if upgrading)
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing = {row[1] for row in cur.fetchall()}  # column names
    for col in EXPECTED_FIELDS:
        if col not in existing:
            cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {col} REAL;")
            print(f"ℹ️  Added missing column: {col}")
    conn.commit()


# ---- Field mapping from SWOB-ML names -> our columns ----
FIELD_MAP = {
    "sig_wave_hgt_pst20mts": "wave_height_sig",
    "avg_sig_wave_hgt_pst20mts": "wave_height_sig",
    "sig_wave_hgt_pst35mts_10mts_ago": "wave_height_sig",

    "pk_wave_hgt_pst20mts": "wave_height_peak",
    "pk_wave_hgt_pst35mts_10mts_ago": "wave_height_peak",

    "avg_wave_pd_pst20mts": "wave_period_avg",
    "pk_wave_pd_pst20mts": "wave_period_peak",
    "pk_wave_pd_pst35mts_10mts_ago": "wave_period_peak",

    "avg_wave_dir_pst20mts": "wave_direction_avg",
    "avg_pk_wave_dir_pst20mts": "wave_direction_peak",

    "avg_wnd_spd_pst10mts": "wind_speed",
    "avg_wnd_spd_pst10mts_1": "wind_speed",

    "max_avg_wnd_spd_pst10mts": "wind_gust",
    "max_avg_wnd_spd_pst10mts_1": "wind_gust",
    "max_wnd_spd_pst10mts": "wind_gust",

    "avg_wnd_dir_pst10mts": "wind_direction",
    "avg_air_temp_pst10mts": "air_temp",
    "avg_sea_sfc_temp_pst10mts": "sea_temp",
    "avg_stn_pres_pst10mts": "pressure",
}


def parse_and_collect_fields(root):
    """Extract buoy_id, timestamp, and mapped numeric fields."""
    # Timestamp
    t_elem = root.find(".//{http://www.opengis.net/gml}timePosition")
    if t_elem is None or not t_elem.text:
        return None
    timestamp = datetime.fromisoformat(t_elem.text.replace("Z", "+00:00"))

    # Buoy ID (prefer wmo_id_extnd over wmo_synop_id)
    buoy_id = None
    synop_id = None
    for e in root.findall(".//{http://dms.ec.gc.ca/schema/point-observation/2.0}element"):
        name = e.get("name")
        val = e.get("value")
        if name == "wmo_id_extnd" and val:
            buoy_id = val
            break
        if name == "wmo_synop_id" and val:
            synop_id = val
    
    buoy_id = buoy_id or synop_id
    if not buoy_id:
        return None

    # Mapped numeric fields
    fields = {}
    for e in root.findall(".//{http://dms.ec.gc.ca/schema/point-observation/2.0}element"):
        n, v = e.get("name"), e.get("value")
        if n in FIELD_MAP and v is not None:
            try:
                fields[FIELD_MAP[n]] = float(v)
            except ValueError:
                pass

    if not fields:
        return None

    # epoch seconds for fast WHERE clauses
    fields["observation_time"] = int(timestamp.timestamp())
    return buoy_id, timestamp, fields


def insert_sqlite(cur, buoy_id, ts_epoch, field_vals, source_file):
    # only include fields we actually have, in stable order
    field_cols = [c for c in EXPECTED_FIELDS if c in field_vals]
    cols = ["buoy_id", "observation_time"] + field_cols + ["source_file"]
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT OR IGNORE INTO buoy_observation ({','.join(cols)}) VALUES ({placeholders})"
    vals = [buoy_id, ts_epoch] + [field_vals[c] for c in field_cols] + [source_file]
    cur.execute(sql, vals)


def main():
    # Influx (soft)
    influx = InfluxSink("~/.config/buoy_influx_1.env")

    # SQLite
    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()
    ensure_schema(conn)

    # remember processed files
    processed_file = Path("~/.cache/buoy_processed.txt").expanduser()
    processed_file.parent.mkdir(parents=True, exist_ok=True)
    processed = set(processed_file.read_text().splitlines()) if processed_file.exists() else set()

    xml_dir = Path("~/envcan_wave/data/buoy").expanduser()
    new_count = 0
    skipped_count = 0

    for xml_path in sorted(xml_dir.glob("*.xml")):
        fp = str(xml_path)
        if fp in processed:
            continue

        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            parsed = parse_and_collect_fields(root)
            if not parsed:
                print(f"⏭️  Skipping {xml_path.name} (no id/fields/time)")
                processed.add(fp)
                skipped_count += 1
                continue

            buoy_id, timestamp, fields = parsed

            # Try Influx (if online)
            influx.write_point(
                measurement="buoy_observation",
                tags={"buoy_id": buoy_id},
                time_iso=timestamp.isoformat(),
                fields_dict=fields,
            )

            # Always write SQLite
            insert_sqlite(cur, buoy_id, fields["observation_time"], fields, xml_path.name)
            conn.commit()

            new_count += 1
            processed.add(fp)
            field_list = sorted(k for k in fields.keys() if k != 'observation_time')
            print(f"✅ {buoy_id} @ {timestamp.strftime('%Y-%m-%d %H:%M')} UTC -> {field_list}")
        except Exception as e:
            print(f"⚠️  Error processing {xml_path.name}: {e}")

    processed_file.write_text("\n".join(sorted(processed)))
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"✅ Processed {new_count} new files")
    print(f"⏭️  Skipped {skipped_count} invalid files")
    print(f"📊 Total tracked: {len(processed)}")
    print(f"💾 Database: {SQLITE_PATH}")


if __name__ == "__main__":
    main()