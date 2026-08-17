#!/usr/bin/env python3
"""
Daily digest: parse last 24h of logs to produce a status snapshot.

Outputs a summary of:
- Which data sources reported errors vs worked normally
- Webcam capture status
- Any cascade-risk patterns (export failures, DB errors, cron gaps)

Usage:
    python3 daily_digest.py [--verbose]

Output: site/data/daily_digest.json + log summary
"""

import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lib.config import EXPORT_DIR
from lib.logging_config import setup_logging

logger = setup_logging("daily_digest", console=False)

LOGS_DIR = Path.home() / "envcan_wave" / "logs"
OUTPUT_FILE = EXPORT_DIR / "daily_digest.json"
VERBOSE = False


def log(msg: str):
    logger.debug(msg)
    if VERBOSE:
        print(msg)


def parse_log_24h(log_path: Path) -> dict:
    """Parse a log file for entries in the last 24 hours.

    Returns counts of INFO, WARNING, ERROR lines and any error messages.
    """
    now = datetime.now()
    cutoff = now - timedelta(hours=24)
    counts = Counter()
    errors = []
    last_timestamp = None

    if not log_path.exists():
        return {"exists": False}

    try:
        with open(log_path, "r", errors="replace") as f:
            for line in f:
                # Match standard log format: 2026-03-10 21:30:53 - name - LEVEL - msg
                m = re.match(
                    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - \S+ - (\w+) - (.+)",
                    line,
                )
                if not m:
                    continue

                try:
                    ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

                if ts < cutoff:
                    continue

                level = m.group(2)
                msg = m.group(3).strip()
                counts[level] += 1
                last_timestamp = ts

                if level == "ERROR":
                    # Deduplicate: keep unique error messages
                    if msg not in errors:
                        errors.append(msg)

    except Exception:
        logger.exception(f"Failed to parse {log_path}")
        return {"exists": True, "status": "parse_error"}

    # The error text stays out of the return value: this dict is published to
    # site/data/daily_digest.json, which Caddy serves to anyone. Raw log lines
    # name internal tooling, exit codes and absolute paths. The count says
    # whether something is wrong; the log says what.
    if errors:
        logger.warning(f"{log_path.name}: {len(errors)} unique error(s) in 24h")
        for msg in errors[:10]:
            logger.warning(f"  {log_path.name}: {msg}")

    return {
        "exists": True,
        "info": counts.get("INFO", 0),
        "warning": counts.get("WARNING", 0),
        "error": counts.get("ERROR", 0),
        "last_activity": last_timestamp.isoformat() if last_timestamp else None,
        "unique_error_count": len(errors),
    }


def check_cron_gaps() -> dict:
    """Detect if key cron jobs stopped running (gap > 2x expected interval)."""
    results = {}

    # Map log files to expected intervals (minutes)
    critical_jobs = {
        "parser.log": ("EC buoy parser", 1),
        "json_export.log": ("JSON export", 1),
        "noaa.log": ("NOAA fetch", 20),
        "wind_parser.log": ("Wind parser", 1),
        "tide_export.log": ("Tide export", 1),
    }

    now = datetime.now()
    cutoff = now - timedelta(hours=24)

    for log_file, (name, expected_interval) in critical_jobs.items():
        log_path = LOGS_DIR / log_file
        if not log_path.exists():
            results[name] = {"status": "missing", "message": "Log file not found"}
            continue

        # Collect timestamps from last 24h
        timestamps = []
        try:
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
                    if not m:
                        continue
                    try:
                        ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        continue
                    if ts >= cutoff:
                        timestamps.append(ts)
        except Exception:
            results[name] = {"status": "error", "message": "Could not parse log"}
            continue

        if not timestamps:
            results[name] = {
                "status": "error",
                "message": "No activity in last 24h",
            }
            continue

        # Find largest gap
        timestamps.sort()
        max_gap_minutes = 0
        for i in range(1, len(timestamps)):
            gap = (timestamps[i] - timestamps[i - 1]).total_seconds() / 60
            max_gap_minutes = max(max_gap_minutes, gap)

        # Also check gap from last entry to now
        since_last = (now - timestamps[-1]).total_seconds() / 60
        max_gap_minutes = max(max_gap_minutes, since_last)

        threshold = expected_interval * 3  # 3x interval = concern
        if max_gap_minutes > threshold:
            results[name] = {
                "status": "warning",
                "max_gap_minutes": round(max_gap_minutes, 1),
                "expected_interval": expected_interval,
                "message": f"Gap of {max_gap_minutes:.0f}min (expected every {expected_interval}min)",
            }
        else:
            results[name] = {
                "status": "ok",
                "max_gap_minutes": round(max_gap_minutes, 1),
                "runs_24h": len(timestamps),
            }

    return results


def check_cascade_risks() -> list:
    """Detect patterns that could indicate systemic failures."""
    risks = []

    # 1. Check if export files are being written (the final output)
    critical_exports = [
        ("latest_buoy_v2.json", 10),  # Should update every minute
        ("buoy_timeseries_48h.json", 30),  # Every 5 min
        ("tide-latest.json", 10),  # Every minute
        ("latest_wind.json", 10),  # Every minute
    ]

    now = datetime.now(timezone.utc)
    for filename, max_age_minutes in critical_exports:
        path = EXPORT_DIR / filename
        if not path.exists():
            risks.append({
                "type": "export_missing",
                "severity": "error",
                "message": f"Critical export missing: {filename}",
            })
            continue

        age_min = (now - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)).total_seconds() / 60
        if age_min > max_age_minutes:
            risks.append({
                "type": "export_stale",
                "severity": "warning",
                "message": f"{filename} is {age_min:.0f}min old (expected <{max_age_minutes}min)",
            })

    # 2. Check if databases are locked or inaccessible
    import sqlite3

    db_dir = Path.home() / ".local" / "share"
    for db_name in ["buoy_data.sqlite", "tide_data.sqlite", "wind_data.sqlite"]:
        db_path = db_dir / db_name
        if not db_path.exists():
            risks.append({
                "type": "db_missing",
                "severity": "error",
                "message": f"Database missing: {db_name}",
            })
            continue

        try:
            conn = sqlite3.connect(db_path, timeout=5)
            conn.execute("SELECT 1")
            conn.close()
        except Exception:
            # Published surface: sqlite3 errors carry the absolute database
            # path. Name the database, not the exception.
            logger.exception(f"Cascade check failed to open {db_name}")
            risks.append({
                "type": "db_locked",
                "severity": "error",
                "message": f"Database unavailable: {db_name}",
            })

    return risks


def summarize_data_sources() -> dict:
    """Summarize status of each data source category from logs."""
    sources = defaultdict(lambda: {"status": "unknown", "details": {}})

    log_mapping = {
        "buoys_ec": [("parser.log", "EC Buoy Parser")],
        "buoys_noaa": [("noaa.log", "NOAA Fetch")],
        "wind": [("wind_parser.log", "Wind Parser")],
        "tides": [("tide_obs.log", "Tide Observations")],
        "lightstations": [("lightstation_parse.log", "Lightstation Parser")],
        "webcams": [
            ("webcam_whiterock.log", "White Rock"),
            ("webcam_coxbay.log", "Cox Bay"),
            ("webcam_mudbay.log", "Mud Bay"),
            ("webcam_ambleside.log", "Ambleside"),
        ],
        "forecasts": [("marine_forecast.log", "Marine Forecast")],
        "storm_surge": [("storm_surge.log", "Storm Surge")],
    }

    for source_name, log_files in log_mapping.items():
        source_status = "ok"
        details = {}

        for log_file, label in log_files:
            result = parse_log_24h(LOGS_DIR / log_file)
            details[label] = result

            if not result.get("exists"):
                source_status = "warning"
            elif result.get("error", 0) > 0 and result.get("info", 0) == 0:
                # All errors, no successful runs = down
                source_status = "error"
            elif result.get("error", 0) > 0:
                # Some errors but also some successes = degraded
                if source_status != "error":
                    source_status = "warning"
            elif not result.get("last_activity"):
                source_status = "warning"

        sources[source_name] = {"status": source_status, "logs": details}

    return dict(sources)


def main():
    global VERBOSE
    if "--verbose" in sys.argv:
        VERBOSE = True

    logger.info("Daily digest started")
    log("Daily Digest - Last 24 Hours")

    # Gather all checks
    data_sources = summarize_data_sources()
    cron_gaps = check_cron_gaps()
    cascade_risks = check_cascade_risks()

    # Count statuses
    source_statuses = [s["status"] for s in data_sources.values()]
    ok_count = source_statuses.count("ok")
    warn_count = source_statuses.count("warning")
    error_count = source_statuses.count("error")

    overall = "ok"
    if cascade_risks:
        overall = "warning"
    if error_count > 0 or any(r["severity"] == "error" for r in cascade_risks):
        overall = "error"

    digest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall,
        "summary": {
            "sources_ok": ok_count,
            "sources_warning": warn_count,
            "sources_error": error_count,
            "sources_total": len(data_sources),
        },
        "data_sources": data_sources,
        "cron_health": cron_gaps,
        "cascade_risks": cascade_risks,
    }

    # Atomic write
    from lib.config import safe_json_write

    safe_json_write(OUTPUT_FILE, digest)

    # Log summary
    logger.info(
        f"Daily digest: {overall.upper()} | "
        f"{ok_count} ok, {warn_count} warn, {error_count} error / {len(data_sources)} sources | "
        f"{len(cascade_risks)} cascade risks"
    )

    if cascade_risks:
        for risk in cascade_risks:
            logger.warning(f"Cascade risk: {risk['message']}")

    # Print summary table if verbose
    if VERBOSE:
        print(f"\n{'Source':<20} {'Status':<10} {'Errors':<8} {'Last Activity'}")
        print("-" * 65)
        for name, info in sorted(data_sources.items()):
            total_errors = sum(
                log_info.get("error", 0) for log_info in info["logs"].values()
            )
            last = "n/a"
            for log_info in info["logs"].values():
                if log_info.get("last_activity"):
                    last = log_info["last_activity"]
            print(f"{name:<20} {info['status']:<10} {total_errors:<8} {last}")

        print(f"\nCascade risks: {len(cascade_risks)}")
        for risk in cascade_risks:
            print(f"  [{risk['severity']}] {risk['message']}")

    log(f"\nDigest saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
