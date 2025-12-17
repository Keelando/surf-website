#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
"""
Parse Environment Canada SWOB-ML wind station observations into SQLite.

Similar to buoy_to_influx_sqlite.py but for land-based weather stations.
Extracts wind speed, gusts, direction, temperature, pressure, and rainfall.

Usage:
    python3 wind_to_sqlite.py

Data is stored in ~/.local/share/wind_data.sqlite and optionally synced to InfluxDB.
"""

from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime
import sqlite3

# Shared utilities
from lib.config import WIND_DATABASE
from lib.logging_config import setup_logging

# Disable console logging (runs from cron, file logging only)
logger = setup_logging('wind_parser', console=False)

# ---- Optional Influx sink (soft dependency) ----
class InfluxSink:
    def __init__(self, env_path):
        self.online = False
        self.client = None
        try:
            from influxdb import InfluxDBClient
        except ImportError as e:
            logger.info(f"Influx client not installed ({e}); running SQLite-only")
            return

        creds = {}
        p = Path(env_path).expanduser()
        if not p.exists():
            logger.info(f"Influx env file not found at {p}; running SQLite-only")
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
            logger.info("InfluxDB connection established")
        except Exception as e:
            logger.info(f"InfluxDB unavailable ({e}); running SQLite-only")
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
            logger.warning(f"Lost Influx connection: {e}. Disabling Influx for this run")
            self.online = False


# ---- SQLite setup ----
WIND_DATABASE.parent.mkdir(parents=True, exist_ok=True)

# Fields we may insert (order is stable for INSERT)
EXPECTED_FIELDS = [
    "wind_speed_kmh",
    "wind_gust_kmh",
    "wind_direction_deg",
    "air_temp_c",
    "pressure_hpa",
    "rainfall_1hr_mm",
    "rainfall_6hr_mm",
    # High-value additions (2025-11-18)
    "humidity_percent",
    "dewpoint_c",
    "pressure_mslp_hpa",
    "visibility_km",
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS wind_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id TEXT NOT NULL,
    station_name TEXT,
    observation_time INTEGER NOT NULL,
    wind_speed_kmh REAL,
    wind_gust_kmh REAL,
    wind_direction_deg INTEGER,
    air_temp_c REAL,
    pressure_hpa REAL,
    rainfall_1hr_mm REAL,
    rainfall_6hr_mm REAL,
    source_file TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);
"""

CREATE_INDEXES_SQL = [
    # Fast "latest by station" queries
    "CREATE INDEX IF NOT EXISTS idx_wind_station_time ON wind_observation(station_id, observation_time DESC);",
    # De-dup safeguard: same station_id + timestamp won't double insert
    "CREATE UNIQUE INDEX IF NOT EXISTS uniq_wind_station_ts ON wind_observation(station_id, observation_time);",
]

def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    for stmt in CREATE_INDEXES_SQL:
        cur.execute(stmt)
    # Auto-add any missing columns from EXPECTED_FIELDS (safe if upgrading)
    cur.execute("PRAGMA table_info(wind_observation);")
    existing = {row[1] for row in cur.fetchall()}  # column names
    for col in EXPECTED_FIELDS:
        if col not in existing:
            cur.execute(f"ALTER TABLE wind_observation ADD COLUMN {col} REAL;")
            logger.info(f"Added missing column: {col}")
    conn.commit()


# ---- Field mapping from SWOB-ML names -> our columns ----
FIELD_MAP = {
    # Wind fields (10-minute averages)
    "avg_wnd_spd_pst10mts": "wind_speed_kmh",
    "avg_wnd_spd_pst10mts_1": "wind_speed_kmh",  # alternate naming
    "avg_wnd_spd_10m_pst10mts": "wind_speed_kmh",  # CZBB format (10m sensor height)

    # Wind gust (maximum in past 10 minutes)
    "max_avg_wnd_spd_pst10mts": "wind_gust_kmh",
    "max_avg_wnd_spd_pst10mts_1": "wind_gust_kmh",  # alternate naming
    "max_wnd_spd_pst10mts": "wind_gust_kmh",  # another alternate
    "max_wnd_gst_spd_10m_pst10mts": "wind_gust_kmh",  # CZBB format
    "max_wnd_spd_10m_pst1hr": "wind_gust_kmh",  # CZBB 1-hour max

    # Wind direction (degrees, 10-minute average)
    "avg_wnd_dir_pst10mts": "wind_direction_deg",
    "avg_wnd_dir_pst10mts_1": "wind_direction_deg",
    "avg_wnd_dir_10m_pst10mts": "wind_direction_deg",  # CZBB format

    # Air temperature (10-minute average or instantaneous)
    "avg_air_temp_pst10mts": "air_temp_c",
    "air_temp": "air_temp_c",  # CZBB instantaneous

    # Atmospheric pressure (10-minute average or instantaneous)
    "avg_stn_pres_pst10mts": "pressure_hpa",
    "stn_pres": "pressure_hpa",  # CZBB instantaneous

    # Rainfall
    "pcpn_amt_pst1hr": "rainfall_1hr_mm",
    "pcpn_amt_pst6hrs": "rainfall_6hr_mm",

    # High-value additions (2025-11-18)
    "rel_hum": "humidity_percent",  # Relative humidity
    "dwpt_temp": "dewpoint_c",  # Dewpoint temperature
    "mslp": "pressure_mslp_hpa",  # Mean sea level pressure
    "avg_vis_pst10mts": "visibility_km",  # Visibility (10-min average)
}


def parse_and_collect_fields(root, filename=None):
    """Extract station_id, timestamp, and mapped numeric fields."""
    # Timestamp
    t_elem = root.find(".//{http://www.opengis.net/gml}timePosition")
    if t_elem is None or not t_elem.text:
        return None
    timestamp = datetime.fromisoformat(t_elem.text.replace("Z", "+00:00"))

    # Station ID and name (collect all possible IDs)
    station_ids = {}
    station_name = None
    for e in root.findall(".//{http://dms.ec.gc.ca/schema/point-observation/2.0}element"):
        name = e.get("name")
        val = e.get("value")
        # Skip missing values
        if val == "MSNG" or val is None or val == "":
            continue
        if name in ["icao_stn_id", "wmo_id_extnd", "wmo_synop_id", "tc_id"]:
            station_ids[name] = val
        elif name == "stn_nam":
            station_name = val

    # Try to extract ICAO code from filename
    # Format 1: CWGT-AUTO-*.xml, CYVR-MAN-*.xml (station at start)
    # Format 2: 2025-11-19-2307-CWSB-AUTO-minute-swob.xml (station after date/time)
    station_id = None
    if filename:
        import re
        # Try format with date prefix first (YYYY-MM-DD-HHMM-STATION-)
        match = re.match(r"^\d{4}-\d{2}-\d{2}-\d{4}-([A-Z]{4})-", filename)
        if not match:
            # Try simple format (STATION-AUTO- or STATION-MAN-)
            match = re.match(r"^([A-Z]{4})-(AUTO|MAN)-", filename)
        if match:
            station_id = match.group(1)

    # Fall back to XML fields if filename doesn't have ICAO
    if not station_id:
        station_id = (
            station_ids.get("icao_stn_id") or
            station_ids.get("tc_id") or
            station_ids.get("wmo_id_extnd") or
            station_ids.get("wmo_synop_id")
        )

    if not station_id:
        return None

    # Mapped numeric fields
    fields = {}
    for e in root.findall(".//{http://dms.ec.gc.ca/schema/point-observation/2.0}element"):
        n, v = e.get("name"), e.get("value")

        # Skip missing values
        if v == "MSNG" or v is None:
            continue

        if n in FIELD_MAP:
            try:
                # Wind direction is an integer (degrees)
                if n.startswith("avg_wnd_dir"):
                    fields[FIELD_MAP[n]] = int(float(v))
                else:
                    fields[FIELD_MAP[n]] = float(v)
            except (ValueError, TypeError):
                pass

    if not fields:
        return None

    # epoch seconds for fast WHERE clauses
    fields["observation_time"] = int(timestamp.timestamp())
    return station_id, station_name, timestamp, fields


def insert_sqlite(cur, station_id, station_name, ts_epoch, field_vals, source_file):
    # only include fields we actually have, in stable order
    field_cols = [c for c in EXPECTED_FIELDS if c in field_vals]
    cols = ["station_id", "station_name", "observation_time"] + field_cols + ["source_file"]
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT OR IGNORE INTO wind_observation ({','.join(cols)}) VALUES ({placeholders})"
    vals = [station_id, station_name, ts_epoch] + [field_vals[c] for c in field_cols] + [source_file]
    cur.execute(sql, vals)


def main():
    # Influx (soft)
    influx = InfluxSink("~/.config/buoy_influx_1.env")

    # SQLite
    conn = sqlite3.connect(WIND_DATABASE)
    cur = conn.cursor()
    ensure_schema(conn)

    # remember processed files
    processed_file = Path("~/.cache/wind_processed.txt").expanduser()
    processed_file.parent.mkdir(parents=True, exist_ok=True)
    processed = set(processed_file.read_text().splitlines()) if processed_file.exists() else set()

    xml_dir = Path("~/envcan_wave/data/wind").expanduser()
    if not xml_dir.exists():
        logger.info(f"Wind data directory not found: {xml_dir}")
        logger.info("Creating directory for future use...")
        xml_dir.mkdir(parents=True, exist_ok=True)
        conn.close()
        return

    new_count = 0
    skipped_count = 0

    for xml_path in sorted(xml_dir.glob("*.xml")):
        fp = str(xml_path)
        if fp in processed:
            continue

        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            parsed = parse_and_collect_fields(root, xml_path.name)
            if not parsed:
                logger.info(f"Skipping {xml_path.name} (no id/fields/time)")
                processed.add(fp)
                skipped_count += 1
                continue

            station_id, station_name, timestamp, fields = parsed

            # Try Influx (if online)
            influx.write_point(
                measurement="wind_observation",
                tags={"station_id": station_id, "station_name": station_name or ""},
                time_iso=timestamp.isoformat(),
                fields_dict=fields,
            )

            # Always write SQLite
            insert_sqlite(cur, station_id, station_name, fields["observation_time"], fields, xml_path.name)
            conn.commit()

            new_count += 1
            processed.add(fp)
            field_list = sorted(k for k in fields.keys() if k != 'observation_time')
            name_display = f" ({station_name})" if station_name else ""
            logger.info(f"{station_id}{name_display} @ {timestamp.strftime('%Y-%m-%d %H:%M')} UTC -> {field_list}")
        except Exception as e:
            logger.warning(f"Error processing {xml_path.name}: {e}")

    processed_file.write_text("\n".join(sorted(processed)))
    conn.close()

    logger.info("=" * 60)
    logger.info(f"Processed {new_count} new files")
    logger.info(f"Skipped {skipped_count} invalid files")
    logger.info(f"Total tracked: {len(processed)}")
    logger.info(f"Database: {WIND_DATABASE}")


if __name__ == "__main__":
    main()
