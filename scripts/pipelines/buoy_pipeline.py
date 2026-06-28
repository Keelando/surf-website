#!/usr/bin/env python3
"""Buoy pipeline orchestrator.

Runs the buoy parse → export chain in a single interpreter so one cron tick is
one ordered process. Replaces the two independent ``*/3`` cron lines
(``parse/buoy_to_sqlite.py`` and ``export/sqlite_to_json.py``), which had no
ordering guarantee between parse and export.

Each stage is isolated in its own try/except: a failure is logged and the
pipeline continues, mirroring the per-job independence the separate cron lines
had (a parse failure should still let the export run on already-present data).
The inner scripts keep writing their own per-stage logs; this module logs only
orchestration messages.
"""

import sys
from pathlib import Path

# The editable install maps `lib` but not `scripts`, so `from scripts...`
# resolves only when cwd is the repo root. Cron doesn't cd there, so put the
# repo root on sys.path explicitly. (scripts/pipelines/this_file → parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.logging_config import setup_logging  # noqa: E402
from scripts.export import sqlite_to_json  # noqa: E402
from scripts.parse import buoy_to_sqlite  # noqa: E402

logger = setup_logging("buoy_pipeline")

# Ordered (stage name, callable) pairs. Order matters: parse before export.
STAGES = [
    ("parse:buoy_to_sqlite", buoy_to_sqlite.main),
    ("export:sqlite_to_json", sqlite_to_json.query_and_export),
]


def main():
    logger.info("=== Buoy pipeline start ===")
    failures = 0
    for name, stage in STAGES:
        try:
            logger.info("→ %s", name)
            stage()
        except Exception:
            failures += 1
            logger.exception("Stage failed: %s (continuing)", name)
    logger.info("=== Buoy pipeline complete (%d stage failure(s)) ===", failures)


if __name__ == "__main__":
    main()
