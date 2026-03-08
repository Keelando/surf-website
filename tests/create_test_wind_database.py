#!/usr/bin/env python3
"""
Generate a test wind database with sample data for offline development and testing.

This script creates a pre-populated SQLite database with realistic wind observation data
from Environment Canada weather stations.

Usage:
    python3 tests/create_test_wind_database.py

This creates: tests/databases/wind_data_test.sqlite
"""

import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import wind parser logic
from wind_to_sqlite import EXPECTED_FIELDS, parse_and_collect_fields

# Paths
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures" / "wind"
DB_DIR = SCRIPT_DIR / "databases"
DB_PATH = DB_DIR / "wind_data_test.sqlite"

# Test station mapping
TEST_STATIONS = {
    "CWSB": {"name": "Point Atkinson", "fixture": "sample_CWSB.xml"},
    "CWGT": {"name": "Sisters Island", "fixture": "sample_CWGT.xml"},
    "CVTF": {"name": "Tsawwassen", "fixture": "sample_CVTF.xml"},
}


def create_schema(conn):
    """Create database schema matching production."""
    cur = conn.cursor()

    # Wind observation table
    cur.execute("""
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
        )
    """)

    # Add optional fields from EXPECTED_FIELDS
    cur.execute("PRAGMA table_info(wind_observation);")
    existing_cols = {row[1] for row in cur.fetchall()}

    for col in EXPECTED_FIELDS:
        if col not in existing_cols:
            cur.execute(f"ALTER TABLE wind_observation ADD COLUMN {col} REAL;")
            print(f"  Added column: {col}")

    # Indexes
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wind_station_time
        ON wind_observation(station_id, observation_time DESC)
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_wind_station_ts
        ON wind_observation(station_id, observation_time)
    """)

    # WAL mode for better concurrency
    cur.execute("PRAGMA journal_mode=WAL;")

    conn.commit()
    print("✅ Database schema created")


def populate_station_data(conn, station_id, station_info):
    """
    Populate wind observation data for a station.
    Creates 24 hours of hourly observations with slight variations.
    """
    fixture_path = FIXTURES_DIR / station_info["fixture"]

    if not fixture_path.exists():
        print(f"⚠️  Fixture not found: {fixture_path}")
        return 0

    # Parse the sample XML
    tree = ET.parse(fixture_path)
    root = tree.getroot()

    # Extract base data from fixture
    result = parse_and_collect_fields(root, str(fixture_path))

    if not result:
        print(f"⚠️  Could not parse fixture: {fixture_path}")
        return 0

    station_id_parsed, station_name, timestamp, fields = result

    # Generate 24 hours of data (hourly observations)
    cur = conn.cursor()
    inserted = 0

    # Start from 24 hours ago, go to now
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=24)

    for hour in range(25):  # 0-24 inclusive (25 observations)
        obs_time = start_time + timedelta(hours=hour)
        obs_timestamp = int(obs_time.timestamp())

        # Add slight variations to make data realistic
        import random
        variation = 1 + (random.random() - 0.5) * 0.2  # ±10% variation

        modified_fields = {}
        for key, value in fields.items():
            if value is not None and isinstance(value, (int, float)):
                # Add variation but keep direction in valid range
                if key == "wind_direction_deg":
                    modified_fields[key] = int((value + random.randint(-30, 30)) % 360)
                else:
                    modified_fields[key] = round(value * variation, 1)
            else:
                modified_fields[key] = value

        # Build INSERT statement
        columns = ["station_id", "station_name", "observation_time", "source_file"]
        values = [station_id, station_info["name"], obs_timestamp, f"test_{station_id}_{hour}.xml"]

        for field_name in EXPECTED_FIELDS:
            if field_name in modified_fields:
                columns.append(field_name)
                values.append(modified_fields[field_name])

        placeholders = ", ".join(["?"] * len(values))
        column_names = ", ".join(columns)

        try:
            cur.execute(f"""
                INSERT OR IGNORE INTO wind_observation ({column_names})
                VALUES ({placeholders})
            """, values)

            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            print(f"    ⚠️  Error inserting record: {e}")

    conn.commit()
    return inserted


def print_summary(conn):
    """Print database summary statistics."""
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("Database Summary")
    print("=" * 70)

    # Total observations
    cur.execute("SELECT COUNT(*) FROM wind_observation")
    obs_count = cur.fetchone()[0]
    print(f"Total observations: {obs_count:4d} records")

    # Stations
    print("\nStations:")
    cur.execute("""
        SELECT station_id, station_name, COUNT(*) as obs_count
        FROM wind_observation
        GROUP BY station_id
        ORDER BY station_id
    """)
    for row in cur.fetchall():
        print(f"  {row[0]:6s} - {row[1]:20s} ({row[2]} observations)")

    # Time range
    print("\nTime range:")
    cur.execute("""
        SELECT
            datetime(MIN(observation_time), 'unixepoch') as earliest,
            datetime(MAX(observation_time), 'unixepoch') as latest
        FROM wind_observation
    """)
    earliest, latest = cur.fetchone()
    if earliest:
        print(f"  {earliest} to {latest}")

    # Sample data
    print("\nSample data (latest observation per station):")
    cur.execute("""
        SELECT
            station_id,
            datetime(observation_time, 'unixepoch') as obs_time,
            wind_speed_kmh,
            wind_gust_kmh,
            wind_direction_deg,
            air_temp_c,
            pressure_hpa
        FROM wind_observation
        WHERE (station_id, observation_time) IN (
            SELECT station_id, MAX(observation_time)
            FROM wind_observation
            GROUP BY station_id
        )
        ORDER BY station_id
    """)

    for row in cur.fetchall():
        print(f"  {row[0]:6s} | {row[1]} | Wind: {row[2]:.1f} km/h ({row[3]:.1f} gust) @ {row[4]}° | Temp: {row[5]:.1f}°C | Press: {row[6]:.1f} hPa")

    print("=" * 70)


def main():
    print("💨 Test Wind Database Generator")
    print("=" * 70)

    # Create database directory
    DB_DIR.mkdir(parents=True, exist_ok=True)

    # Remove existing test database
    if DB_PATH.exists():
        print(f"🗑️  Removing existing test database: {DB_PATH}")
        DB_PATH.unlink()

    # Create new database
    print(f"📁 Creating database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    try:
        # Create schema
        create_schema(conn)

        # Populate data for each test station
        for station_id, station_info in TEST_STATIONS.items():
            print(f"\n📍 {station_info['name']} ({station_id})")
            obs_count = populate_station_data(conn, station_id, station_info)
            print(f"   ✅ {obs_count} observations")

        # Print summary
        print_summary(conn)

        print("\n✅ Test database created successfully!")
        print(f"   Location: {DB_PATH}")
        print("\nNext steps:")
        print("  1. Use this database for offline development and testing")
        print("  2. Test export scripts with: export_wind_json.py --test-mode")
        print(f"  3. Verify data: sqlite3 {DB_PATH} 'SELECT * FROM wind_observation LIMIT 5;'")

        return 0

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    exit(main())
