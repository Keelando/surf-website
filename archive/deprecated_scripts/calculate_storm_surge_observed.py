#!/usr/bin/env python3
"""
Calculate observed storm surge from tide observations and predictions.

Storm surge (observed) = Observed water level - Predicted water level

This script:
1. Queries tide observations (accumulated incrementally up to 10 days)
2. Queries tide predictions for matching time periods
3. Calculates offset (observed - predicted) = actual storm surge
4. Stores to tide_offset table for validation against GDSPS forecasts
"""

import sqlite3
import datetime
import time
import argparse
from pathlib import Path

DB_PATH = Path("~/.local/share/tide_data.sqlite").expanduser()
TEST_DB_PATH = Path(__file__).parent / "tests" / "databases" / "tide_data_test.sqlite"
RETENTION_DAYS = 10

# Only stations with real-time observations (PERMANENT stations)
STATIONS_WITH_OBS = {
    "point_atkinson": "Point Atkinson",
    "kitsilano": "Kitsilano",
    "new_westminster": "New Westminster",
    "campbell_river": "Campbell River"
}


def calculate_offsets(conn):
    """
    Calculate tide offsets for all stations with observations.

    Strategy:
    - For each observation timestamp, find nearest prediction (within ±30 minutes)
    - Calculate offset = observed - predicted
    - Store to tide_offset table
    """
    cur = conn.cursor()
    total_calculated = 0

    for station_key, station_display_name in STATIONS_WITH_OBS.items():
        print(f"\n📍 Processing {station_display_name}...")

        # Get all observations for this station (last 11 days)
        # Note: Query by station_name (friendly name), not station_id (DFO ID)
        cur.execute("""
            SELECT observation_time, water_level
            FROM tide_observation
            WHERE station_name = ?
            AND water_level IS NOT NULL
            ORDER BY observation_time ASC
        """, (station_key,))

        observations = cur.fetchall()
        if not observations:
            print(f"   ⚠️  No observations found")
            continue

        print(f"   Found {len(observations)} observations")

        matched = 0
        skipped = 0

        for obs_time, obs_level in observations:
            # Find nearest prediction within ±30 minutes (1800 seconds)
            time_window_start = obs_time - 1800
            time_window_end = obs_time + 1800

            cur.execute("""
                SELECT prediction_time, water_level
                FROM tide_prediction
                WHERE station_name = ?
                AND prediction_time BETWEEN ? AND ?
                AND water_level IS NOT NULL
                ORDER BY ABS(prediction_time - ?) ASC
                LIMIT 1
            """, (station_key, time_window_start, time_window_end, obs_time))

            pred_result = cur.fetchone()

            if pred_result:
                pred_time, pred_level = pred_result
                offset = obs_level - pred_level

                # Insert or update offset
                cur.execute("""
                    INSERT OR REPLACE INTO tide_offset
                    (station_id, station_name, observation_time,
                     observed_level, predicted_level, offset)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (station_key, station_display_name, obs_time,
                      obs_level, pred_level, offset))

                matched += 1
            else:
                skipped += 1

        total_calculated += matched
        print(f"   ✅ Calculated {matched} offsets (skipped {skipped} without matching prediction)")

    return total_calculated


def purge_old_offsets(conn):
    """Remove offsets older than RETENTION_DAYS."""
    cur = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff_timestamp = int((now - datetime.timedelta(days=RETENTION_DAYS)).timestamp())

    cur.execute("DELETE FROM tide_offset WHERE observation_time < ?", (cutoff_timestamp,))
    deleted = cur.rowcount

    if deleted > 0:
        print(f"\n🗑️  Purged {deleted} old offset records (older than {RETENTION_DAYS} days)")

    return deleted


def print_summary(conn):
    """Print summary statistics."""
    cur = conn.cursor()

    print("\n" + "=" * 70)
    print("Summary:")
    print("=" * 70)

    cur.execute("""
        SELECT
            station_name,
            COUNT(*) as records,
            MIN(offset) as min_offset,
            MAX(offset) as max_offset,
            AVG(offset) as avg_offset,
            datetime(MIN(observation_time), 'unixepoch') as earliest,
            datetime(MAX(observation_time), 'unixepoch') as latest
        FROM tide_offset
        GROUP BY station_name
        ORDER BY station_name
    """)

    for row in cur.fetchall():
        station, count, min_off, max_off, avg_off, earliest, latest = row
        print(f"\n{station}:")
        print(f"  Records: {count}")
        print(f"  Time range: {earliest} to {latest}")
        print(f"  Offset range: {min_off:+.3f}m to {max_off:+.3f}m")
        print(f"  Average offset: {avg_off:+.3f}m")


def main():
    parser = argparse.ArgumentParser(description="Calculate observed storm surge from tide data")
    parser.add_argument('--test-mode', action='store_true',
                       help='Use test database instead of production database')
    args = parser.parse_args()

    # Select database path
    db_path = TEST_DB_PATH if args.test_mode else DB_PATH

    start_time = time.time()

    print("🌊 Storm Surge Observed Calculator")
    if args.test_mode:
        print("   [TEST MODE - Using test database]")
    print("=" * 70)

    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        if args.test_mode:
            print(f"   Run: python3 tests/create_test_tide_database.py")
        return 1

    print(f"📁 Database: {db_path}")
    conn = sqlite3.connect(db_path)

    try:
        # Calculate offsets
        total = calculate_offsets(conn)
        conn.commit()

        # Purge old data
        purge_old_offsets(conn)
        conn.commit()

        # Print summary
        print_summary(conn)

        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print(f"✅ Complete! Calculated {total} offsets in {elapsed:.1f}s")

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
