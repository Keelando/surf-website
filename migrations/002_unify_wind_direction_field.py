#!/usr/bin/env python3
"""
Migration 002: Unify wind_direction field in wind_observation table

Background:
- Field unification work completed Dec 2024
- Backend switched to wind_direction_deg (REAL) from wind_direction (INTEGER)
- Frontend updated to use wind_direction_deg
- But historical data still in old wind_direction field

Current state:
- wind_direction (INTEGER, old): 110,050 rows (89% of data)
- wind_direction_deg (REAL, new): 7,891 rows (6.4% - recent data only)

This migration:
1. Backfills wind_direction_deg from wind_direction (cast INTEGER → REAL)
2. Verifies all rows with wind data now have wind_direction_deg
3. Drops the old wind_direction column

See: docs/project/DATABASE_SCHEMA_CLEANUP.md
See: docs/project/NEXT_SESSION.md - Field unification complete
"""

import sys
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.config import WIND_DATABASE

BACKUP_DIR = Path.home() / ".local/share/db_backups"


def create_backup():
    """Create timestamped backup of database."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"wind_data_before_migration_002_{timestamp}.sqlite"

    print(f"Creating backup: {backup_path}")
    shutil.copy2(WIND_DATABASE, backup_path)
    print(f"✓ Backup created successfully")
    return backup_path


def analyze_current_state(conn, include_old_field=True):
    """Analyze current field population."""
    cur = conn.cursor()

    # Total rows
    cur.execute("SELECT COUNT(*) FROM wind_observation")
    total = cur.fetchone()[0]

    # New field (wind_direction_deg)
    cur.execute("SELECT COUNT(*) FROM wind_observation WHERE wind_direction_deg IS NOT NULL")
    new_field = cur.fetchone()[0]

    # Check if old column still exists
    cur.execute("PRAGMA table_info(wind_observation)")
    cols = {row[1] for row in cur.fetchall()}
    has_old_column = 'wind_direction' in cols

    if not has_old_column or not include_old_field:
        # After migration - old column dropped
        return {
            'total': total,
            'old_field': 0,
            'new_field': new_field,
            'both': 0,
            'only_old': 0,
            'only_new': new_field
        }

    # Old field (wind_direction)
    cur.execute("SELECT COUNT(*) FROM wind_observation WHERE wind_direction IS NOT NULL")
    old_field = cur.fetchone()[0]

    # Both fields populated
    cur.execute("SELECT COUNT(*) FROM wind_observation WHERE wind_direction IS NOT NULL AND wind_direction_deg IS NOT NULL")
    both = cur.fetchone()[0]

    # Only old field
    cur.execute("SELECT COUNT(*) FROM wind_observation WHERE wind_direction IS NOT NULL AND wind_direction_deg IS NULL")
    only_old = cur.fetchone()[0]

    # Only new field
    cur.execute("SELECT COUNT(*) FROM wind_observation WHERE wind_direction IS NULL AND wind_direction_deg IS NOT NULL")
    only_new = cur.fetchone()[0]

    return {
        'total': total,
        'old_field': old_field,
        'new_field': new_field,
        'both': both,
        'only_old': only_old,
        'only_new': only_new
    }


def backfill_wind_direction_deg(conn):
    """Backfill wind_direction_deg from wind_direction where NULL."""
    cur = conn.cursor()

    print("\nBackfilling wind_direction_deg from wind_direction...")

    # Update rows that have old field but not new field
    cur.execute("""
        UPDATE wind_observation
        SET wind_direction_deg = CAST(wind_direction AS REAL)
        WHERE wind_direction IS NOT NULL
          AND wind_direction_deg IS NULL
    """)

    rows_updated = cur.rowcount
    conn.commit()

    print(f"✓ Updated {rows_updated:,} rows")
    return rows_updated


def verify_backfill(conn):
    """Verify backfill was successful."""
    cur = conn.cursor()

    # Check if any rows still have only old field
    cur.execute("""
        SELECT COUNT(*) FROM wind_observation
        WHERE wind_direction IS NOT NULL
          AND wind_direction_deg IS NULL
    """)

    remaining = cur.fetchone()[0]

    if remaining > 0:
        raise Exception(f"Backfill incomplete! {remaining} rows still have only old field")

    print("✓ Backfill verified - all old data copied to new field")


def drop_old_column(conn):
    """Drop the old wind_direction column."""
    cur = conn.cursor()

    print("\nDropping old wind_direction column...")

    try:
        cur.execute("ALTER TABLE wind_observation DROP COLUMN wind_direction")
        conn.commit()
        print("✓ Dropped wind_direction column")
    except Exception as e:
        print(f"✗ Failed to drop column: {e}")
        raise


def verify_column_dropped(conn):
    """Verify old column is gone."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(wind_observation)")
    cols = {row[1] for row in cur.fetchall()}

    if 'wind_direction' in cols:
        raise Exception("Migration failed! wind_direction column still exists")

    if 'wind_direction_deg' not in cols:
        raise Exception("Migration failed! wind_direction_deg column missing")

    print("✓ Column schema verified - old field removed, new field present")


def print_summary(stats_before, stats_after, backup_path):
    """Print migration summary."""
    print("\n" + "="*70)
    print("MIGRATION 002 COMPLETE")
    print("="*70)
    print(f"Database: {WIND_DATABASE}")
    print(f"\nBefore migration:")
    print(f"  Total rows: {stats_before['total']:,}")
    print(f"  wind_direction (old): {stats_before['old_field']:,} ({stats_before['old_field']/stats_before['total']*100:.1f}%)")
    print(f"  wind_direction_deg (new): {stats_before['new_field']:,} ({stats_before['new_field']/stats_before['total']*100:.1f}%)")
    print(f"  Only in old field: {stats_before['only_old']:,}")
    print(f"\nAfter migration:")
    print(f"  Total rows: {stats_after['total']:,}")
    print(f"  wind_direction_deg: {stats_after['new_field']:,} ({stats_after['new_field']/stats_after['total']*100:.1f}%)")
    print(f"  Rows backfilled: {stats_before['only_old']:,}")
    print(f"\nBackup location: {backup_path}")
    print("\nRollback instructions (if needed):")
    print(f"  cp {backup_path} {WIND_DATABASE}")
    print("\nNext steps:")
    print("  1. Test exports: python3 scripts/export/export_wind_json.py")
    print("  2. Check station status: python3 scripts/debug_station_status.py")
    print("  3. Verify wind arrows on frontend")
    print("  4. Monitor logs for 24 hours")
    print("="*70 + "\n")


def main():
    """Execute migration."""
    print("\n" + "="*70)
    print("MIGRATION 002: Unify wind_direction field")
    print("="*70 + "\n")

    if not WIND_DATABASE.exists():
        print(f"ERROR: Database not found: {WIND_DATABASE}")
        sys.exit(1)

    # Step 1: Create backup
    backup_path = create_backup()

    # Step 2: Connect and analyze
    conn = sqlite3.connect(WIND_DATABASE)

    print("\nAnalyzing current state...")
    stats_before = analyze_current_state(conn)

    print(f"\nCurrent state:")
    print(f"  Total rows: {stats_before['total']:,}")
    print(f"  wind_direction (old): {stats_before['old_field']:,} ({stats_before['old_field']/stats_before['total']*100:.1f}%)")
    print(f"  wind_direction_deg (new): {stats_before['new_field']:,} ({stats_before['new_field']/stats_before['total']*100:.1f}%)")
    print(f"  Both fields: {stats_before['both']:,}")
    print(f"  Only old: {stats_before['only_old']:,} (will be backfilled)")
    print(f"  Only new: {stats_before['only_new']:,}")

    if stats_before['only_old'] == 0:
        print("\n✓ No backfill needed - all data already in new field!")

        # Still drop old column if it exists
        response = input("\nDrop old wind_direction column? (yes/no): ").strip().lower()
        if response != "yes":
            print("Migration aborted by user")
            conn.close()
            return

        drop_old_column(conn)
        verify_column_dropped(conn)
        conn.close()

        print("\n✓ Migration complete - old column dropped")
        return

    # Step 3: Confirm
    print("\n" + "-"*70)
    print(f"This will:")
    print(f"  1. Backfill {stats_before['only_old']:,} rows (copy old → new field)")
    print(f"  2. Verify all wind data in new field")
    print(f"  3. Drop old wind_direction column")
    print("-"*70)
    response = input("Proceed with migration? (yes/no): ").strip().lower()
    if response != "yes":
        print("Migration aborted by user")
        conn.close()
        return

    # Step 4: Execute migration
    try:
        backfill_wind_direction_deg(conn)
        verify_backfill(conn)
        drop_old_column(conn)
        verify_column_dropped(conn)

        # Step 5: Analyze final state
        stats_after = analyze_current_state(conn)
        conn.close()

        # Step 6: Print summary
        print_summary(stats_before, stats_after, backup_path)

    except Exception as e:
        print(f"\n✗ MIGRATION FAILED: {e}")
        print(f"\nRollback with: cp {backup_path} {WIND_DATABASE}")
        conn.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
