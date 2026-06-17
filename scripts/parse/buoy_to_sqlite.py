#!/usr/bin/env python3
import sqlite3
from datetime import datetime
from pathlib import Path

from defusedxml import ElementTree as ET

# Shared utilities
from lib.config import BUOY_DATABASE, BUOY_RETENTION_DAYS
from lib.logging_config import setup_logging

logger = setup_logging("parser")


# ---- SQLite setup ----
# Database path comes from config.BUOY_DATABASE
BUOY_DATABASE.parent.mkdir(parents=True, exist_ok=True)

# Fields we may insert (order is stable for INSERT)
EXPECTED_FIELDS = [
    # Wave metrics (basic)
    "wave_height_sig",
    "wave_height_peak",
    "wave_height_max",
    "wave_height_avg",
    "wave_period_sig",
    "wave_period_avg",
    "wave_period_peak",
    "wave_period_max_wave",
    "wave_direction_avg",
    "wave_direction_peak",
    "wave_direction_spread_avg",
    "wave_direction_spread_peak",
    "wave_crest_height_max",
    # Wave metrics (spectral)
    "wave_height_spectral",
    "wave_period_spectral",
    "wave_period_energy_spectral",
    # Wind metrics (primary sensor)
    "wind_speed",
    "wind_gust",
    "wind_direction",
    "wind_sensor_height",
    # Wind metrics (secondary sensor)
    "wind_speed_sensor_2",
    "wind_gust_sensor_2",
    "wind_direction_sensor_2",
    "wind_samples_bad_1",
    "wind_samples_bad_2",
    # Temperature
    "air_temp",
    "sea_temp",
    # Pressure
    "pressure",
    "pressure_msl",
    "pressure_sensor_2",
    "pressure_trend_char",
    "pressure_trend_amount",
    # Position (current GPS coordinates)
    "buoy_lat_current",
    "buoy_lon_current",
    # Solar panel current (cloudiness indicator!)
    "solar_current",
    # Wave metrics (additional statistics)
    "wave_period_sig_basic",
    "wave_height_max_avg",
    "wave_period_max_avg",
    # System health & monitoring
    "battery_voltage",
    "watchman_boot_count",
    "obstruction_lamp_current",
    # Orientation (compass headings)
    "compass_heading_1",
    "compass_heading_2",
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
            logger.info(f"Added missing column: {col}")
    conn.commit()


# ---- Field mapping from SWOB-ML names -> our columns ----
FIELD_MAP = {
    # Wave height (multiple variants)
    "sig_wave_hgt_pst20mts": "wave_height_sig",
    "avg_sig_wave_hgt_pst20mts": "wave_height_sig",
    "sig_wave_hgt_pst35mts_10mts_ago": "wave_height_sig",
    "spetrl_sig_wave_hgt_pst20mts": "wave_height_spectral",
    "pk_wave_hgt_pst20mts": "wave_height_peak",
    "pk_wave_hgt_pst35mts_10mts_ago": "wave_height_peak",
    "max_wave_hgt_pst20mts": "wave_height_max",
    "avg_wave_hgt_pst20mts": "wave_height_avg",
    "max_wave_crst_hgt_abv_avg_wtr_lvl_pst20mts": "wave_crest_height_max",
    # Wave period
    "avg_wave_pd_pst20mts": "wave_period_avg",
    # Significant period, wave-buoy SWOB name. NOTE: met buoys publish the same
    # quantity as `sig_wave_pd_pst20mts` -> wave_period_sig_basic (see below).
    # The two go to *separate* columns, so consumers must coalesce
    # wave_period_sig / wave_period_sig_basic to get "the significant period".
    "avg_sig_wave_pd_pst20mts": "wave_period_sig",
    "pk_wave_pd_pst20mts": "wave_period_peak",
    "pk_wave_pd_pst35mts_10mts_ago": "wave_period_peak",
    "pd_of_max_wave_hgt_pst20mts": "wave_period_max_wave",
    "avg_spetrl_wave_pd_pst20mts": "wave_period_spectral",
    "spetrl_wave_enrgy_pd_pst20mts": "wave_period_energy_spectral",
    # Wave direction
    "avg_wave_dir_pst20mts": "wave_direction_avg",
    "avg_pk_wave_dir_pst20mts": "wave_direction_peak",
    "avg_wave_dir_sprd_pst20mts": "wave_direction_spread_avg",
    "pk_wave_dir_sprd_pst20mts": "wave_direction_spread_peak",
    # Wind (primary sensor)
    "avg_wnd_spd_pst10mts": "wind_speed",
    "avg_wnd_spd_pst10mts_1": "wind_speed",
    "max_avg_wnd_spd_pst10mts": "wind_gust",
    "max_avg_wnd_spd_pst10mts_1": "wind_gust",
    "max_wnd_spd_pst10mts": "wind_gust",
    "max_wnd_spd_pst10mts_1": "wind_gust",
    "avg_wnd_dir_pst10mts": "wind_direction",
    "avg_wnd_dir_pst10mts_1": "wind_direction",
    "wnd_snsr_vert_disp": "wind_sensor_height",
    # Wind (secondary sensor)
    "avg_wnd_spd_pst10mts_2": "wind_speed_sensor_2",
    "max_wnd_spd_pst10mts_2": "wind_gust_sensor_2",
    "avg_wnd_dir_pst10mts_2": "wind_direction_sensor_2",
    "bad_wnd_smpls_1": "wind_samples_bad_1",
    "bad_wnd_smpls_2": "wind_samples_bad_2",
    # Temperature
    "avg_air_temp_pst10mts": "air_temp",
    "avg_sea_sfc_temp_pst10mts": "sea_temp",
    # Pressure
    "avg_stn_pres_pst10mts": "pressure",
    "avg_stn_pres_pst10mts_1": "pressure",
    "avg_stn_pres_pst10mts_2": "pressure_sensor_2",
    "avg_mslp_pst10mts": "pressure_msl",
    "pres_tend_char_pst3hrs": "pressure_trend_char",
    "pres_tend_amt_pst3hrs": "pressure_trend_amount",
    # Position
    "crnt_buoy_lat": "buoy_lat_current",
    "crnt_buoy_long": "buoy_lon_current",
    # Solar current (cloudiness indicator)
    "avg_solr_panl_crnt_pst10mts": "solar_current",
    # Wave metrics (additional statistics) - Added 2025-12-06
    # Significant period, met-buoy SWOB name (e.g. English Bay, S. Georgia Strait).
    # Same quantity as avg_sig_wave_pd_pst20mts -> wave_period_sig above, but kept in
    # its own column. Coalesce the two when you want "the significant period".
    "sig_wave_pd_pst20mts": "wave_period_sig_basic",
    "avg_max_wave_hgt_pst20mts": "wave_height_max_avg",
    "avg_max_wave_pd_pst20mts": "wave_period_max_avg",
    # System health & monitoring - Added 2025-12-06
    "avg_batry_volt_pst10mts": "battery_voltage",
    "wtchmn_boot_cnt_pst1hr": "watchman_boot_count",
    "avg_obstrn_lamp_crnt_pst10mts": "obstruction_lamp_current",
    # Orientation (compass headings) - Added 2025-12-06
    "avg_cmpss_hdng_pst10mts_1": "compass_heading_1",
    "avg_cmpss_hdng_pst10mts_2": "compass_heading_2",
}

# Note: The following fields are available in EC buoy XMLs but intentionally not captured:
# - avg_wtr_lvl_snsr_volt_pst10mts (water level sensor voltage - too granular for operational needs)


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
    # SQLite
    conn = sqlite3.connect(BUOY_DATABASE)
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
                logger.info(f"Skipping {xml_path.name} (no id/fields/time)")
                processed.add(fp)
                skipped_count += 1
                continue

            buoy_id, timestamp, fields = parsed

            # Write to SQLite
            insert_sqlite(cur, buoy_id, fields["observation_time"], fields, xml_path.name)
            conn.commit()

            new_count += 1
            processed.add(fp)
            field_list = sorted(k for k in fields.keys() if k != "observation_time")
            logger.info(f"{buoy_id} @ {timestamp.strftime('%Y-%m-%d %H:%M')} UTC -> {field_list}")
        except Exception as e:
            logger.warning(f"Error processing {xml_path.name}: {e}")

    processed_file.write_text("\n".join(sorted(processed)))

    # Purge old data based on retention policy
    import time

    cutoff_timestamp = int(time.time()) - (BUOY_RETENTION_DAYS * 86400)
    cur.execute("DELETE FROM buoy_observation WHERE observation_time < ?", (cutoff_timestamp,))
    deleted = cur.rowcount
    if deleted > 0:
        logger.info(f"Purged {deleted} observations older than {BUOY_RETENTION_DAYS} days")
    conn.commit()
    conn.close()

    logger.info("=" * 60)
    logger.info(f"Processed {new_count} new files")
    logger.info(f"Skipped {skipped_count} invalid files")
    logger.info(f"Total tracked: {len(processed)}")
    logger.info(f"Database: {BUOY_DATABASE}")


if __name__ == "__main__":
    main()
