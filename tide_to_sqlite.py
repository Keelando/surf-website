#!/usr/bin/env python3
"""
Comprehensive Tide Data Fetcher for DFO IWLS API

Handles three data types:
  1. Tide Observations (wlo) - actual measurements, 11-day retention
  2. Tide Predictions (wlp) - continuous graph data, 3-day retention
  3. High/Low Predictions (wlp-hilo) - discrete events, 3-day retention

All continuous data downsampled to 15-minute intervals.

Usage:
  tide_to_sqlite.py --observations  # Fetch recent observations (run every 30 min)
  tide_to_sqlite.py --predictions   # Fetch today's predictions (run daily)
  tide_to_sqlite.py --all           # Fetch everything (manual runs)
"""

import requests
import sqlite3
import datetime
import json
import time
import argparse
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
STATION_FILE = Path("~/envcan_wave/tide_stations.json").expanduser()
BASE_URL = "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations"
HEADERS = {"User-Agent": "keelan_w@hotmail.com"}

# Retention policies (in days)
OBSERVATION_RETENTION_DAYS = 11
PREDICTION_RETENTION_DAYS = 3

# Observation fetch window (how far back to look)
OBSERVATION_LOOKBACK_HOURS = 2


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def load_stations():
    """Load tide stations from external JSON file."""
    if not STATION_FILE.exists():
        raise FileNotFoundError(f"Missing {STATION_FILE}")
    with open(STATION_FILE, "r") as f:
        data = json.load(f)
    return {k: v["id"] for k, v in data.items()}


def safe_get(url, params=None):
    """Make HTTP request with error handling."""
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"  ?  {e}")
        return None


def detect_available_codes(station_id):
    """Query station metadata to find available time series codes."""
    meta = safe_get(f"{BASE_URL}/{station_id}")
    if not meta or "timeSeries" not in meta:
        return []
    return [ts["code"] for ts in meta.get("timeSeries", [])]


def downsample_to_15min(data_rows):
    """
    Downsample tide data to 15-minute intervals.
    
    Args:
        data_rows: List of dicts with 'eventDate', 'value', 'qcFlagCode'
    
    Returns:
        Filtered list keeping only :00, :15, :30, :45 timestamps
    """
    downsampled = []
    for row in data_rows:
        try:
            ts = datetime.datetime.fromisoformat(row["eventDate"].replace("Z", "+00:00"))
            # Only keep if minute is 0, 15, 30, or 45 and second is 0
            if ts.minute in (0, 15, 30, 45) and ts.second == 0:
                downsampled.append(row)
        except Exception:
            continue
    return downsampled


# ============================================================================
# DATABASE SETUP
# ============================================================================

def ensure_db():
    """Create or update database schema safely."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tide_observation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT NOT NULL,
            station_name TEXT NOT NULL,
            timestamp_utc INTEGER NOT NULL,
            water_level REAL,
            quality TEXT,
            series_code TEXT,
            event_type TEXT,
            recorded_at TEXT DEFAULT (datetime('now'))
        )
    """)
    
    # Create indexes
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uniq_tide_reading
        ON tide_observation(station_id, timestamp_utc, series_code)
    """)
    
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_tide_series
        ON tide_observation(series_code, timestamp_utc)
    """)
    
    # Check if event_type column exists, add if missing
    cur.execute("PRAGMA table_info(tide_observation)")
    columns = {row[1] for row in cur.fetchall()}
    if "event_type" not in columns:
        cur.execute("ALTER TABLE tide_observation ADD COLUMN event_type TEXT")
        print("??  Added event_type column to tide_observation table")
    
    conn.commit()
    return conn


# ============================================================================
# DATA INSERTION
# ============================================================================

def insert_tide_data(cur, station_key, station_id, series_code, data, event_type=None):
    """
    Insert tide data rows, skipping duplicates.
    
    Args:
        cur: Database cursor
        station_key: Human-readable station name
        station_id: DFO station ID
        series_code: wlo, wlp, or wlp-hilo
        data: List of data rows from API
        event_type: Optional event type for wlp-hilo (e.g., 'high', 'low')
    
    Returns:
        Number of rows added
    """
    added = 0
    for row in data:
        try:
            ts = datetime.datetime.fromisoformat(row["eventDate"].replace("Z", "+00:00"))
            timestamp = int(ts.timestamp())
            water_level = row.get("value")
            qc = row.get("qcFlagCode")
            
            # For wlp-hilo, extract event type from the data if available
            row_event_type = event_type
            if series_code == "wlp-hilo" and "eventType" in row:
                row_event_type = row["eventType"]
            
            cur.execute("""
                INSERT OR IGNORE INTO tide_observation 
                (station_id, station_name, timestamp_utc, water_level, quality, series_code, event_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (station_id, station_key, timestamp, water_level, qc, series_code, row_event_type))
            
            if cur.rowcount > 0:
                added += 1
        except Exception as e:
            print(f"    ??  Skipped bad row: {e}")
    
    return added


# ============================================================================
# OBSERVATION FETCHING (wlo)
# ============================================================================

def fetch_observations(conn, stations):
    """
    Fetch recent tide observations for all stations.
    Downsamples to 15-minute intervals.
    
    Returns:
        Number of new rows added
    """
    cur = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc)
    start = now - datetime.timedelta(hours=OBSERVATION_LOOKBACK_HOURS)
    
    total_added = 0
    observation_count = 0
    
    print(f"?? Fetching observations (last {OBSERVATION_LOOKBACK_HOURS}h)")
    print("=" * 70)
    
    for key, sid in stations.items():
        print(f"? {key}")
        
        # Check if station has observations available
        codes = detect_available_codes(sid)
        if "wlo" not in codes:
            print(f"  ??  No observations available")
            continue
        
        # Fetch observations
        url = f"{BASE_URL}/{sid}/data"
        params = {
            "time-series-code": "wlo",
            "from": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": now.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        data = safe_get(url, params)
        if not data:
            print(f"  ?  No response")
            continue
        
        values = data if isinstance(data, list) else data.get("values", [])
        if not values:
            print(f"  ??  Empty dataset")
            continue
        
        # Downsample to 15-minute intervals
        before_count = len(values)
        downsampled = downsample_to_15min(values)
        after_count = len(downsampled)
        
        # Insert into database
        added = insert_tide_data(cur, key, sid, "wlo", downsampled)
        total_added += added
        observation_count += 1
        
        print(f"  ?  {added} new rows (downsampled: {before_count} ? {after_count})")
        time.sleep(2.1)  # Rate limiting
    
    conn.commit()
    return total_added, observation_count


# ============================================================================
# PREDICTION FETCHING (wlp, wlp-hilo)
# ============================================================================

def fetch_predictions(conn, stations):
    """
    Fetch today's tide predictions (both continuous and high/low).
    - wlp: Continuous predictions, downsampled to 15-minute intervals
    - wlp-hilo: High/low events, kept at original timestamps
    
    Returns:
        Tuple of (total_rows_added, stations_processed)
    """
    cur = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc)
    
    # Calculate today's date range in UTC
    # Get start of today (00:00:00 UTC)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # Get end of today (23:59:59 UTC)
    today_end = today_start + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    
    total_added = 0
    stations_processed = 0
    
    print(f"?? Fetching predictions for {today_start.strftime('%Y-%m-%d')}")
    print("=" * 70)
    
    for key, sid in stations.items():
        print(f"? {key}")
        
        codes = detect_available_codes(sid)
        if not codes:
            print(f"  ??  No timeSeries metadata")
            continue
        
        station_added = 0
        
        # ---- Fetch continuous predictions (wlp) ----
        if "wlp" in codes:
            url = f"{BASE_URL}/{sid}/data"
            params = {
                "time-series-code": "wlp",
                "from": today_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": today_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            
            data = safe_get(url, params)
            if data:
                values = data if isinstance(data, list) else data.get("values", [])
                if values:
                    before_count = len(values)
                    downsampled = downsample_to_15min(values)
                    after_count = len(downsampled)
                    
                    added = insert_tide_data(cur, key, sid, "wlp", downsampled)
                    station_added += added
                    print(f"  ?? wlp: {added} rows (downsampled: {before_count} ? {after_count})")
            
            time.sleep(2.1)  # Rate limiting
        
        # ---- Fetch high/low predictions (wlp-hilo) ----
        if "wlp-hilo" in codes:
            url = f"{BASE_URL}/{sid}/data"
            params = {
                "time-series-code": "wlp-hilo",
                "from": today_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": today_end.strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            
            data = safe_get(url, params)
            if data:
                values = data if isinstance(data, list) else data.get("values", [])
                if values:
                    # No downsampling for high/low events - keep original timestamps
                    added = insert_tide_data(cur, key, sid, "wlp-hilo", values)
                    station_added += added
                    print(f"  ?? wlp-hilo: {added} events")
            
            time.sleep(2.1)  # Rate limiting
        
        if station_added > 0:
            stations_processed += 1
            total_added += station_added
            print(f"  ?  Total: {station_added} rows")
        else:
            print(f"  ??  No new data")
    
    conn.commit()
    return total_added, stations_processed


# ============================================================================
# CLEANUP FUNCTIONS
# ============================================================================

def cleanup_old_data(conn):
    """
    Remove stale data based on retention policies:
    - Observations (wlo): older than 11 days
    - Predictions (wlp, wlp-hilo): older than 3 days
    """
    cur = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc)
    
    # Calculate cutoff timestamps
    obs_cutoff = int((now - datetime.timedelta(days=OBSERVATION_RETENTION_DAYS)).timestamp())
    pred_cutoff = int((now - datetime.timedelta(days=PREDICTION_RETENTION_DAYS)).timestamp())
    
    print("\n?? Cleaning up old data...")
    
    # Delete old observations
    cur.execute("""
        DELETE FROM tide_observation 
        WHERE series_code = 'wlo' 
        AND timestamp_utc < ?
    """, (obs_cutoff,))
    obs_deleted = cur.rowcount
    
    # Delete old predictions (both types)
    cur.execute("""
        DELETE FROM tide_observation 
        WHERE series_code IN ('wlp', 'wlp-hilo') 
        AND timestamp_utc < ?
    """, (pred_cutoff,))
    pred_deleted = cur.rowcount
    
    conn.commit()
    
    print(f"  ???  Deleted {obs_deleted} old observations (>{OBSERVATION_RETENTION_DAYS} days)")
    print(f"  ???  Deleted {pred_deleted} old predictions (>{PREDICTION_RETENTION_DAYS} days)")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Fetch and store tide data from DFO IWLS API"
    )
    parser.add_argument(
        "--observations",
        action="store_true",
        help="Fetch recent observations only (run every 30 min)"
    )
    parser.add_argument(
        "--predictions",
        action="store_true",
        help="Fetch today's predictions only (run daily)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch everything (observations + predictions)"
    )
    
    args = parser.parse_args()
    
    # Default to --all if no mode specified
    if not (args.observations or args.predictions or args.all):
        args.all = True
    
    start_time = time.time()
    
    # Load configuration
    try:
        stations = load_stations()
    except FileNotFoundError as e:
        print(f"? {e}")
        return 1
    
    # Initialize database
    conn = ensure_db()
    
    print(f"?? Tide Data Fetcher")
    print(f"?? {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)
    
    total_added = 0
    
    try:
        # Fetch observations
        if args.observations or args.all:
            obs_added, obs_count = fetch_observations(conn, stations)
            total_added += obs_added
            print(f"\n? Observations: {obs_added} rows from {obs_count} stations")
        
        # Fetch predictions
        if args.predictions or args.all:
            pred_added, pred_count = fetch_predictions(conn, stations)
            total_added += pred_added
            print(f"\n? Predictions: {pred_added} rows from {pred_count} stations")
        
        # Cleanup old data
        cleanup_old_data(conn)
        
        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print(f"?? Complete in {elapsed:.1f}s | Total new rows: {total_added}")
        print(f"?? Database: {DB_PATH}")
        
        return 0
        
    except Exception as e:
        print(f"\n? Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        conn.close()


if __name__ == "__main__":
    exit(main())