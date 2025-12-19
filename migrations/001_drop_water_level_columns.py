#!/usr/bin/env python3
"""
Migration 001: Drop unused water_level columns from buoy_observation table

Context:
- water_level_predicted and water_level_observed were added during Surrey wave
  integration but caused bugs (mixed predictions with observations)
- Removed from ingestion scripts on Dec 11-12, 2024
- Only 0.3% and 0.6% of rows have these fields populated
- No longer used by any scripts or frontend

See: docs/project/DATABASE_SCHEMA_CLEANUP.md
See: docs/KNOWN_ISSUES.md (Surrey Water Level Integration Bug)

Safety:
- Creates automatic backup before migration
- Verifies columns exist before attempting drop
- Rollback instructions provided if needed
"""

import sys
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from lib.config import BUOY_DATABASE

BACKUP_DIR = Path.home() / ".local/share/db_backups"
COLUMNS_TO_DROP = ["water_level_predicted", "water_level_observed"]


def create_backup():
    """Create timestamped backup of database."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"buoy_data_before_migration_001_{timestamp}.sqlite"

    print(f"Creating backup: {backup_path}")
    shutil.copy2(BUOY_DATABASE, backup_path)
    print(f"✓ Backup created successfully")
    return backup_path


def verify_columns_exist(conn):
    """Check which columns actually exist in the table."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(buoy_observation);")
    existing_cols = {row[1] for row in cur.fetchall()}

    found = []
    missing = []
    for col in COLUMNS_TO_DROP:
        if col in existing_cols:
            found.append(col)
        else:
            missing.append(col)

    return found, missing


def count_non_null_rows(conn, column):
    """Count how many rows have non-null values for this column."""
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM buoy_observation WHERE {column} IS NOT NULL")
    return cur.fetchone()[0]


def drop_columns(conn, columns):
    """Drop specified columns using SQLite's ALTER TABLE."""
    cur = conn.cursor()

    for col in columns:
        print(f"Dropping column: {col}")
        try:
            cur.execute(f"ALTER TABLE buoy_observation DROP COLUMN {col}")
            print(f"  ✓ Dropped {col}")
        except Exception as e:
            print(f"  ✗ Failed to drop {col}: {e}")
            raise

    conn.commit()


def verify_migration(conn):
    """Verify columns were actually dropped."""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(buoy_observation);")
    remaining_cols = {row[1] for row in cur.fetchall()}

    still_present = [col for col in COLUMNS_TO_DROP if col in remaining_cols]

    if still_present:
        raise Exception(f"Migration failed! Columns still present: {still_present}")

    print("✓ Migration verified - columns successfully dropped")


def print_summary(total_rows, backup_path):
    """Print migration summary."""
    print("\n" + "="*70)
    print("MIGRATION 001 COMPLETE")
    print("="*70)
    print(f"Database: {BUOY_DATABASE}")
    print(f"Total rows: {total_rows:,}")
    print(f"Columns dropped: {', '.join(COLUMNS_TO_DROP)}")
    print(f"Backup location: {backup_path}")
    print("\nRollback instructions (if needed):")
    print(f"  cp {backup_path} {BUOY_DATABASE}")
    print("\nNext steps:")
    print("  1. Test exports: python3 scripts/export/sqlite_to_json.py")
    print("  2. Check station status: python3 scripts/debug_station_status.py")
    print("  3. Verify website displays data correctly")
    print("  4. Monitor logs for 24 hours")
    print("="*70 + "\n")


def main():
    """Execute migration."""
    print("\n" + "="*70)
    print("MIGRATION 001: Drop water_level columns from buoy_observation")
    print("="*70 + "\n")

    if not BUOY_DATABASE.exists():
        print(f"ERROR: Database not found: {BUOY_DATABASE}")
        sys.exit(1)

    # Step 1: Create backup
    backup_path = create_backup()

    # Step 2: Connect and analyze
    conn = sqlite3.connect(BUOY_DATABASE)

    print("\nAnalyzing current schema...")
    found_cols, missing_cols = verify_columns_exist(conn)

    if missing_cols:
        print(f"⚠ Columns already dropped: {', '.join(missing_cols)}")

    if not found_cols:
        print("✓ No columns to drop - migration already applied or columns never existed")
        conn.close()
        return

    print(f"Found columns to drop: {', '.join(found_cols)}")

    # Step 3: Show impact
    print("\nData impact assessment:")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM buoy_observation")
    total_rows = cur.fetchone()[0]
    print(f"Total rows in table: {total_rows:,}")

    for col in found_cols:
        non_null = count_non_null_rows(conn, col)
        percentage = (non_null / total_rows * 100) if total_rows > 0 else 0
        print(f"  {col}: {non_null:,} rows ({percentage:.1f}%) have data")

    # Step 4: Confirm
    print("\n" + "-"*70)
    response = input("Proceed with migration? (yes/no): ").strip().lower()
    if response != "yes":
        print("Migration aborted by user")
        conn.close()
        return

    # Step 5: Execute migration
    print("\nExecuting migration...")
    try:
        drop_columns(conn, found_cols)
        verify_migration(conn)
        conn.close()

        # Step 6: Print summary
        print_summary(total_rows, backup_path)

    except Exception as e:
        print(f"\n✗ MIGRATION FAILED: {e}")
        print(f"\nRollback with: cp {backup_path} {BUOY_DATABASE}")
        conn.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
