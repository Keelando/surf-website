#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
"""
Fetch BC Lightstation Reports from Environment Canada.

Reports are published every 3 hours (00, 03, 06, 09, 12, 15, 18, 21 UTC).
Format: FPCN61 (text-based period-delimited format).

URL pattern: https://dd.weather.gc.ca/today/bulletins/alphanumeric/YYYYMMDD/FP/CWVR/HH/
Files: FPCN61_CWVR_DDHHMM___<id>

Usage:
    python3 fetch_lightstation.py
"""

import requests
import re
from datetime import datetime, timezone
from pathlib import Path
from lib.logging_config import setup_logging

# Disable console logging (runs from cron, file logging only)
logger = setup_logging('lightstation_fetch', console=False)

# Configuration
BASE_URL = "https://dd.weather.gc.ca/today/bulletins/alphanumeric"
DOWNLOAD_DIR = Path.home() / "envcan_wave" / "data" / "lightstation"

# Reports are issued every 3 hours
REPORT_HOURS = [0, 3, 6, 9, 12, 15, 18, 21]


def get_latest_report_url():
    """
    Determine the most likely URL for the latest report.
    Reports are published every 3 hours, but may have a delay.
    Try the last two possible report times.
    """
    now_utc = datetime.now(timezone.utc)
    current_date = now_utc.strftime("%Y%m%d")
    current_hour = now_utc.hour

    # Find the most recent report hour(s) to try
    candidate_hours = []
    for h in reversed(REPORT_HOURS):
        if h <= current_hour:
            candidate_hours.append(h)
        if len(candidate_hours) >= 2:
            break

    # If we're early in the day, also try the last hour from previous period
    if len(candidate_hours) < 2:
        candidate_hours.append(REPORT_HOURS[-1])  # 21 from previous day

    logger.info(f"Current time: {now_utc.strftime('%Y-%m-%d %H:%M')} UTC")
    logger.info(f"Will try report hours: {candidate_hours}")

    for hour in candidate_hours:
        dir_url = f"{BASE_URL}/{current_date}/FP/CWVR/{hour:02d}/"

        try:
            # Fetch the directory listing
            response = requests.get(dir_url, timeout=10)

            if response.status_code == 200:
                # Parse HTML to find FPCN61 files
                matches = re.findall(r'href="(FPCN61_CWVR_\d{6}___\d+)"', response.text)

                if matches:
                    # Get the most recent file (usually only one per hour)
                    latest_file = matches[-1]
                    file_url = dir_url + latest_file
                    logger.info(f"Found report: {latest_file}")
                    return file_url, latest_file

                logger.warning(f"No FPCN61 files found in {dir_url}")
            else:
                logger.info(f"Hour {hour:02d} directory not available (HTTP {response.status_code})")

        except requests.RequestException as e:
            logger.warning(f"Failed to check hour {hour:02d}: {e}")

    return None, None


def download_report(url, filename):
    """Download the report file to the local directory."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    local_path = DOWNLOAD_DIR / filename

    # Check if already downloaded
    if local_path.exists():
        logger.info(f"Report already exists: {filename}")
        return local_path

    try:
        logger.info(f"Downloading: {url}")
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        # Save to file
        local_path.write_text(response.text)
        logger.info(f"✓ Downloaded to: {local_path}")
        return local_path

    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        return None


def main():
    logger.info("=== Fetching BC Lightstation Reports ===")

    # Find and download the latest report
    url, filename = get_latest_report_url()

    if url:
        local_path = download_report(url, filename)
        if local_path:
            logger.info(f"✓ Success! Report saved to: {local_path}")
        else:
            logger.error("Failed to download report")
    else:
        logger.warning("No recent reports found. This may be normal if reports are delayed.")

    logger.info("=== Fetch complete ===")


if __name__ == "__main__":
    main()
