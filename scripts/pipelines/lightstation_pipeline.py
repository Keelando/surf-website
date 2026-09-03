#!/usr/bin/env python3
"""Lightstation pipeline orchestrator.

Runs the lightstation parse → export(json) → export(timeseries) chain in a
single interpreter so one cron tick is one ordered process. Replaces the four
hourly cron lines previously staggered at :05/:10/:15/:18, which ordered
themselves only by hoping the clock gaps were wide enough.

There is no fetch stage: sr3/AMQP delivers both bulletin families into
data/lightstation_bulletins as they are published, so this chain only reads.

Each stage is isolated in its own try/except: a failure is logged and the
pipeline continues, mirroring the per-job independence the separate cron lines
had (a parse failure should still let downstream stages run on already-present
data). The inner scripts keep writing their own per-stage logs; this module logs
only orchestration messages.
"""

import sys
from pathlib import Path

# The editable install maps `lib` but not `scripts`, so `from scripts...`
# resolves only when cwd is the repo root. Cron doesn't cd there, so put the
# repo root on sys.path explicitly. (scripts/pipelines/this_file → parents[2])
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from lib.logging_config import setup_logging  # noqa: E402
from scripts.export import export_lightstation_json, export_lightstation_timeseries  # noqa: E402
from scripts.parse import parse_lightstation  # noqa: E402

logger = setup_logging("lightstation_pipeline")

# Ordered (stage name, callable) pairs. Order matters: parse → export. There is
# no fetch stage: sr3/AMQP pushes the bulletins into data/lightstation_bulletins
# as they are published, so this chain only has to read what is already there.
STAGES = [
    ("parse:parse_lightstation", parse_lightstation.main),
    ("export:export_lightstation_json", export_lightstation_json.main),
    ("export:export_lightstation_timeseries", export_lightstation_timeseries.main),
]


def main():
    logger.info("=== Lightstation pipeline start ===")
    failures = 0
    for name, stage in STAGES:
        try:
            logger.info("→ %s", name)
            stage()
        except Exception:
            failures += 1
            logger.exception("Stage failed: %s (continuing)", name)
    logger.info("=== Lightstation pipeline complete (%d stage failure(s)) ===", failures)


if __name__ == "__main__":
    main()
