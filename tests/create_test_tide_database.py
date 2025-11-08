#!/usr/bin/env python3
"""
Generate a test tide database with sample data for offline development and testing.

This script creates a pre-populated SQLite database with realistic tide data
including observations, predictions, and high/low events.

Usage:
    python3 tests/create_test_tide_database.py

This creates: tests/databases/tide_data_test.sqlite
"""

import sqlite3
import json
import datetime
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
FIXTURES_DIR = SCRIPT_DIR / "fixtures" / "dfo_iwls"
DB_DIR = SCRIPT_DIR / "databases"
DB_PATH = DB_DIR / "tide_data_test.sqlite"

# Test station mapping (subset of real stations for testing)
TEST_STATIONS = {
    "point_atkinson": {
        "id": "07795",
        "name": "Point Atkinson",
        "has_observations": True
    },
    "kitsilano": {
        "id": "07707",
        "name": "Kitsilano",
        "has_observations": True
    }
}


def create_schema(conn):
    """Create database schema matching production."""
    cur = conn.cursor()

    # Tide observations table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_observation (
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            observation_time INTEGER NOT NULL,
            water_level REAL,
            quality TEXT,
            recorded_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (station_id, observation_time)
        )
    """)

    # Tide predictions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_prediction (
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            prediction_time INTEGER NOT NULL,
            water_level REAL,
            recorded_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (station_id, prediction_time)
        )
    """)

    # Tide high/low events table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_highlow (
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            event_time INTEGER NOT NULL,
            water_level REAL,
            event_type TEXT,
            recorded_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (station_id, event_time)
        )
    """)

    # Tide offset table (for storm surge calculation)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_offset (
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            observation_time INTEGER NOT NULL,
            observed_level REAL,
            predicted_level REAL,
            offset REAL,
            calculated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (station_id, observation_time)
        )
    """)

    # Index for efficient offset queries
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_tide_offset_time
        ON tide_offset(station_id, observation_time DESC)
    """)

    # WAL mode for better concurrency
    cur.execute("PRAGMA journal_mode=WAL;")

    conn.commit()
    print("✅ Database schema created")


def load_fixture_json(filename):
    """Load JSON fixture file and update timestamps to be recent."""
    path = FIXTURES_DIR / filename
    if not path.exists():
        print(f"⚠️  Fixture not found: {filename}")
        return None

    with open(path, 'r') as f:
        data = json.load(f)

    # Calculate time offset to make data "recent" (within last 4 hours)
    if data and "eventDate" in data[0]:
        original_time = datetime.datetime.fromisoformat(data[0]["eventDate"].replace("Z", "+00:00"))
        now = datetime.datetime.now(datetime.timezone.utc)
        # Set first record to 4 hours ago
        target_time = now - datetime.timedelta(hours=4)
        time_delta = target_time - original_time

        # Update all timestamps
        for record in data:
            original = datetime.datetime.fromisoformat(record["eventDate"].replace("Z", "+00:00"))
            new_time = original + time_delta
            record["eventDate"] = new_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    return data


def populate_observations(conn, station_id, station_name):
    """Populate tide observations from fixture."""
    fixture_file = f"{station_id}_observations.json"
    data = load_fixture_json(fixture_file)

    if not data:
        print(f"⚠️  No observation data for {station_id}")
        return 0

    cur = conn.cursor()
    inserted = 0

    for record in data:
        try:
            ts = datetime.datetime.fromisoformat(record["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = record.get("value")
            quality = record.get("qcFlagCode", "1")

            cur.execute("""
                INSERT OR IGNORE INTO tide_observation
                (station_id, station_name, observation_time, water_level, quality)
                VALUES (?, ?, ?, ?, ?)
            """, (station_id, station_name, timestamp, water_level, quality))

            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            print(f"    ⚠️  Skipped observation record: {e}")

    conn.commit()
    return inserted


def populate_predictions(conn, station_id, station_name):
    """Populate tide predictions from fixture."""
    fixture_file = f"{station_id}_predictions.json"
    data = load_fixture_json(fixture_file)

    if not data:
        print(f"⚠️  No prediction data for {station_id}")
        return 0

    cur = conn.cursor()
    inserted = 0

    for record in data:
        try:
            ts = datetime.datetime.fromisoformat(record["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = record.get("value")

            cur.execute("""
                INSERT OR IGNORE INTO tide_prediction
                (station_id, station_name, prediction_time, water_level)
                VALUES (?, ?, ?, ?)
            """, (station_id, station_name, timestamp, water_level))

            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            print(f"    ⚠️  Skipped prediction record: {e}")

    conn.commit()
    return inserted


def populate_highlow(conn, station_id, station_name):
    """Populate tide high/low events from fixture."""
    fixture_file = f"{station_id}_highlow.json"
    data = load_fixture_json(fixture_file)

    if not data:
        print(f"⚠️  No high/low data for {station_id}")
        return 0

    cur = conn.cursor()
    inserted = 0

    # Determine high/low by comparing neighbors
    for i, record in enumerate(data):
        try:
            ts = datetime.datetime.fromisoformat(record["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = record.get("value")

            # Determine event type
            event_type = "unknown"
            if i > 0 and i < len(data) - 1:
                prev_val = data[i-1].get("value")
                next_val = data[i+1].get("value")
                if water_level > prev_val and water_level > next_val:
                    event_type = "high"
                elif water_level < prev_val and water_level < next_val:
                    event_type = "low"
            elif i == 0 and len(data) > 1:
                next_val = data[i+1].get("value")
                event_type = "high" if water_level > next_val else "low"
            elif i == len(data) - 1:
                prev_val = data[i-1].get("value")
                event_type = "high" if water_level > prev_val else "low"

            cur.execute("""
                INSERT OR IGNORE INTO tide_highlow
                (station_id, station_name, event_time, water_level, event_type)
                VALUES (?, ?, ?, ?, ?)
            """, (station_id, station_name, timestamp, water_level, event_type))

            if cur.rowcount > 0:
                inserted += 1
        except Exception as e:
            print(f"    ⚠️  Skipped high/low record: {e}")

    conn.commit()
    return inserted


def print_summary(conn):
    """Print database summary statistics."""
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("Database Summary")
    print("=" * 70)

    # Observations
    cur.execute("SELECT COUNT(*) FROM tide_observation")
    obs_count = cur.fetchone()[0]
    print(f"Observations:      {obs_count:4d} records")

    # Predictions
    cur.execute("SELECT COUNT(*) FROM tide_prediction")
    pred_count = cur.fetchone()[0]
    print(f"Predictions:       {pred_count:4d} records")

    # High/low events
    cur.execute("SELECT COUNT(*) FROM tide_highlow")
    hl_count = cur.fetchone()[0]
    print(f"High/low events:   {hl_count:4d} records")

    # Offsets (should be 0 initially)
    cur.execute("SELECT COUNT(*) FROM tide_offset")
    offset_count = cur.fetchone()[0]
    print(f"Tide offsets:      {offset_count:4d} records")

    print("\nStations:")
    cur.execute("SELECT DISTINCT station_name FROM tide_observation ORDER BY station_name")
    for row in cur.fetchall():
        print(f"  - {row[0]}")

    print("\nTime range:")
    cur.execute("""
        SELECT
            datetime(MIN(observation_time), 'unixepoch') as earliest,
            datetime(MAX(observation_time), 'unixepoch') as latest
        FROM tide_observation
    """)
    earliest, latest = cur.fetchone()
    if earliest:
        print(f"  {earliest} to {latest}")

    print("=" * 70)


def main():
    print("🌊 Test Tide Database Generator")
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
        for station_id, info in TEST_STATIONS.items():
            station_name = info["name"]
            print(f"\n📍 {station_name} ({station_id})")

            if info["has_observations"]:
                obs_count = populate_observations(conn, station_id, station_name)
                print(f"   ✅ {obs_count} observations")

            pred_count = populate_predictions(conn, station_id, station_name)
            print(f"   ✅ {pred_count} predictions")

            hl_count = populate_highlow(conn, station_id, station_name)
            print(f"   ✅ {hl_count} high/low events")

        # Print summary
        print_summary(conn)

        print(f"\n✅ Test database created successfully!")
        print(f"   Location: {DB_PATH}")
        print(f"\nNext steps:")
        print(f"  1. Run calculate_storm_surge_observed.py with --test-mode to populate offsets")
        print(f"  2. Use this database for offline development and testing")

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
