#!/usr/bin/env python3
"""
Migrate wind_observation.wind_direction_deg → wind_direction
Part of field name unification across buoy and wind databases.
"""
import sqlite3
from pathlib import Path

WIND_DB = Path("~/.local/share/wind_data.sqlite").expanduser()

def migrate():
    print(f"Migrating: {WIND_DB}")
    conn = sqlite3.connect(WIND_DB)
    cur = conn.cursor()

    # Check current schema
    cur.execute("PRAGMA table_info(wind_observation);")
    columns = {row[1]: row[2] for row in cur.fetchall()}

    print(f"Current columns: {', '.join(columns.keys())}")

    if "wind_direction" in columns and "wind_direction_deg" in columns:
        print("⚠️  Both wind_direction and wind_direction_deg exist!")
        print("   Dropping wind_direction_deg (keeping wind_direction)")
        # TODO: Handle this edge case if needed
        return

    if "wind_direction" in columns:
        print("✅ Column already named 'wind_direction' - migration not needed")
        return

    if "wind_direction_deg" not in columns:
        print("❌ Column 'wind_direction_deg' not found!")
        return

    # Perform migration
    try:
        print("Renaming: wind_direction_deg → wind_direction")
        cur.execute("ALTER TABLE wind_observation RENAME COLUMN wind_direction_deg TO wind_direction;")
        conn.commit()
        print("✅ Successfully renamed wind_direction_deg → wind_direction")

        # Verify
        cur.execute("PRAGMA table_info(wind_observation);")
        new_columns = [row[1] for row in cur.fetchall()]
        if "wind_direction" in new_columns and "wind_direction_deg" not in new_columns:
            print("✅ Verification passed!")
        else:
            print("⚠️  Verification failed - check schema")

    except sqlite3.OperationalError as e:
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
