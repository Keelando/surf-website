#!/usr/bin/env python3
"""
Create lightstation_data.sqlite database with schema.

Run this once to initialize the database.
"""

import sqlite3

from lib.config import LIGHTSTATION_DATABASE as DB_PATH

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS lightstation_observation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_name TEXT NOT NULL,
    region TEXT,
    observation_time INTEGER NOT NULL,
    report_time_str TEXT,
    wind_speed_kt REAL,
    wind_direction TEXT,
    wind_gusting INTEGER DEFAULT 0,
    wind_calm INTEGER DEFAULT 0,
    wind_estimated INTEGER DEFAULT 1,
    sea_height_ft REAL,
    sea_condition TEXT,
    swell_intensity TEXT,
    swell_direction TEXT,
    source_file TEXT,
    recorded_at TEXT DEFAULT (datetime('now'))
);
"""

CREATE_INDEXES_SQL = [
    # Fast "latest by station" queries
    (
        "CREATE INDEX IF NOT EXISTS idx_lightstation_station_time "
        "ON lightstation_observation(station_name, observation_time DESC);"
    ),
    # De-dup safeguard: same station + timestamp won't double insert
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS uniq_lightstation_station_ts "
        "ON lightstation_observation(station_name, observation_time);"
    ),
]


def main():
    # Ensure parent directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect and create schema
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    print(f"Creating database at: {DB_PATH}")

    # Create table
    cur.execute(CREATE_TABLE_SQL)
    print("✓ Created lightstation_observation table")

    # Create indexes
    for idx_sql in CREATE_INDEXES_SQL:
        cur.execute(idx_sql)
    print("✓ Created indexes")

    conn.commit()
    conn.close()

    print("\n✓ Database initialized successfully!")
    print(f"Location: {DB_PATH}")


if __name__ == "__main__":
    main()
