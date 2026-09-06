#!/usr/bin/env python3
"""Collapse lightstation observations that were stored twice.

`scripts/parse/parse_lightstation.py` now merges a reading's second copy at
insert time, but rows written before that fix are still doubled: nine stations
appear in both FPCN61 and an SXCN bulletin, and each copy was timestamped from
its own WMO header. This walks the existing pairs and applies the same merge,
so the history behind the inferred publishing schedule stops describing one
observation as two.

A one-off, but safe to re-run: once a pair is collapsed there is nothing left
to match.

Usage:
    python3 -m scripts.utils.dedupe_lightstation_observations [--dry-run]
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.config import LIGHTSTATION_DATABASE as DB_PATH  # noqa: E402
from scripts.parse.parse_lightstation import (  # noqa: E402
    PAIR_OFFSET_MAX_SEC,
    PAIR_OFFSET_MIN_SEC,
    merge_observation,
)

# The columns the merge reads off the FPCN61 row it is folding in.
OBS_FIELDS = (
    "station_name",
    "region",
    "observation_time",
    "wind_speed_kt",
    "wind_direction",
    "wind_gusting",
    "wind_calm",
    "wind_estimated",
    "sea_height_ft",
    "sea_condition",
    "swell_intensity",
    "swell_direction",
    "source_file",
)

# An FPCN61 row that already names an SXCN file has been merged; skip it.
FIND_PAIRS_SQL = """
SELECT f.id AS fpcn_id, s.id AS sxcn_id,
       f.observation_time - s.observation_time AS offset_sec
FROM lightstation_observation f
JOIN lightstation_observation s
  ON s.station_name = f.station_name
 AND f.observation_time - s.observation_time BETWEEN ? AND ?
 AND s.source_file LIKE '%SXCN%'
WHERE f.source_file LIKE '%FPCN61%'
  AND f.source_file NOT LIKE '%SXCN%'
ORDER BY f.observation_time
"""


def collapse_pairs(conn, dry_run=False):
    """Merge every FPCN61 row into its SXCN partner. Returns rows collapsed."""
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(FIND_PAIRS_SQL, (PAIR_OFFSET_MIN_SEC, PAIR_OFFSET_MAX_SEC))
    pairs = cur.fetchall()

    # A band 30 minutes wide against a three-hourly cycle should never pair one
    # row twice, but say so out loud rather than silently merging both ways.
    claimed = set()
    collapsed = 0
    for pair in pairs:
        if pair["fpcn_id"] in claimed or pair["sxcn_id"] in claimed:
            print(f"  ambiguous pair, skipped: FPCN61 row {pair['fpcn_id']} / SXCN row {pair['sxcn_id']}")
            continue
        claimed.add(pair["fpcn_id"])
        claimed.add(pair["sxcn_id"])

        cur.execute("SELECT * FROM lightstation_observation WHERE id = ?", (pair["fpcn_id"],))
        fpcn = cur.fetchone()
        cur.execute("SELECT * FROM lightstation_observation WHERE id = ?", (pair["sxcn_id"],))
        sxcn = cur.fetchone()

        obs = {field: fpcn[field] for field in OBS_FIELDS}
        stamp = datetime.fromtimestamp(sxcn["observation_time"], timezone.utc).strftime("%Y-%m-%d %H:%M")
        if dry_run:
            print(f"  would merge {fpcn['station_name']:<18} {stamp}Z (+{pair['offset_sec'] // 60} min)")
            collapsed += 1
            continue

        merge_observation(cur, sxcn, obs, sxcn["observation_time"])
        cur.execute("DELETE FROM lightstation_observation WHERE id = ?", (fpcn["id"],))
        collapsed += 1

    if not dry_run:
        conn.commit()
    return collapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report what would be merged, change nothing")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found: {DB_PATH}")
        return 1

    with sqlite3.connect(DB_PATH) as conn:
        before = conn.execute("SELECT COUNT(*) FROM lightstation_observation").fetchone()[0]
        collapsed = collapse_pairs(conn, dry_run=args.dry_run)
        after = conn.execute("SELECT COUNT(*) FROM lightstation_observation").fetchone()[0]

    verb = "would collapse" if args.dry_run else "collapsed"
    print(f"{verb} {collapsed} duplicate pair(s); {before} rows -> {after}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
