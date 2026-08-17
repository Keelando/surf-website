#!/usr/bin/env python3
"""
Create reporting_lag.sqlite database with schema.

Run this once to initialize the database. Writers call
`lib.reporting_lag.ensure_schema()` themselves, so this is a convenience for
setting up a new host — the schema itself lives in `lib/reporting_lag.py`,
which also documents what the columns mean.
"""

import sqlite3

from lib.config import REPORTING_LAG_DATABASE as DB_PATH
from lib.reporting_lag import ensure_schema


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    print(f"Creating database at: {DB_PATH}")

    ensure_schema(conn)
    print("✓ Created reporting_lag table")
    print("✓ Created indexes")

    conn.close()

    print("\n✓ Database initialized successfully!")
    print(f"Location: {DB_PATH}")


if __name__ == "__main__":
    main()
