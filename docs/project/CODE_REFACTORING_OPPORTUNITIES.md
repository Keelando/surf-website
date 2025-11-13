# Code Refactoring Opportunities

**Created:** 2025-11-10
**Status:** Assessment for post-structure refactoring
**Dependency:** Should be done AFTER directory structure refactoring

---

## Executive Summary

Analysis reveals significant code duplication and opportunities for improvement:
- **Database paths hardcoded** in 11 scripts
- **Utility functions duplicated** 3-7 times across scripts
- **Station data duplicated** instead of using stations.json
- **No shared database helpers** - each script opens its own connections
- **Configuration scattered** - hardcoded paths throughout

**Recommendation:** Complete directory structure refactoring FIRST (3-phase plan), THEN tackle code refactoring.

---

## Why Structure First, Code Second?

### Benefits of Doing Directory Cleanup First

1. **Visibility of Duplication**
   - Once ingestion scripts are grouped in `src/ingestion/`, duplication becomes obvious
   - Easier to see which scripts share similar patterns
   - Can identify opportunities for shared utilities

2. **Clear Boundaries for Refactoring**
   - All export scripts in one place → refactor exports together
   - All ingestion scripts in one place → refactor fetchers together
   - Easier to decide where shared code belongs

3. **Git History Separation**
   - File moves are separate commits from logic changes
   - Easier to debug if issues arise ("was it the move or the refactor?")
   - Can rollback moves without losing code improvements

4. **Lower Risk**
   - Moving files is safer than changing business logic
   - Can test structure change independently
   - Production system keeps running during structure change

5. **Cleaner Refactoring**
   - Don't have to update imports twice (once for move, once for refactoring)
   - Shared utilities go directly to `src/core/` (instead of root, then moved later)
   - Avoid churn

6. **Psychological Win**
   - Quick win with structure cleanup → momentum
   - Clear workspace makes code refactoring more enjoyable
   - Easier to focus on logic when directory isn't cluttered

### Risks of Doing Code Refactoring First

1. **Hard to see patterns** when files scattered across root
2. **Changes logic AND structure** simultaneously = harder debugging
3. **More complex rollback** if issues discovered
4. **Import churn** - update imports for refactoring, then update again for move
5. **Scope creep** - tempting to over-engineer without clear boundaries

---

## Code Refactoring Opportunities Identified

### 1. Database Path Configuration (HIGH PRIORITY)

**Problem:** Database paths hardcoded in 11 scripts

**Current State:**
```python
# Repeated in 7 scripts:
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()

# Repeated in 4 scripts:
DB_PATH = Path("~/.local/share/tide_data.sqlite").expanduser()
```

**Files Affected:**
- buoy_to_influx_sqlite.py
- fetch_noaa_buoy.py
- sqlite_to_json.py
- export_24hr_timeseries.py
- fetch_surrey_wave_v2.py
- tide_to_sqlite.py
- export_tide_json.py
- calculate_storm_surge_observed.py
- export_combined_water_level.py
- compare_surrey_dfo_water_levels.py
- influx_to_mqtt.py (implicit)

**Proposed Solution:**

Create `src/core/config.py`:

```python
"""
Centralized configuration for marine weather pipeline.
"""
from pathlib import Path
import os

# ========== Paths ==========

# Repository root (calculated from this file's location)
REPO_ROOT = Path(__file__).parent.parent.parent.resolve()

# Data directories
DATA_DIR = Path(os.getenv("MARINE_DATA_DIR", "~/.local/share")).expanduser()
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Databases
BUOY_DATABASE = DATA_DIR / "buoy_data.sqlite"
TIDE_DATABASE = DATA_DIR / "tide_data.sqlite"
STORM_SURGE_DATABASE = DATA_DIR / "storm_surge_forecast.sqlite"

# Configuration files
CONFIG_DIR = REPO_ROOT / "config"
STATIONS_JSON = CONFIG_DIR / "stations.json"
SR3_CONFIG_DIR = CONFIG_DIR / "sr3"

# Export destinations
SITE_DIR = Path(os.getenv("SITE_DIR", "~/site")).expanduser()
SITE_DATA_DIR = SITE_DIR / "data"
SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ========== Freshness Policies ==========

# How old can buoy data be before we stop displaying it?
BUOY_FRESHNESS_WINDOW = 2 * 3600  # 2 hours in seconds

# How old can tide observations be?
TIDE_OBS_FRESHNESS_WINDOW = 3 * 3600  # 3 hours in seconds

# ========== Field Definitions ==========

# All available buoy metrics
BUOY_FIELDS = [
    "wave_height_sig", "wave_height_peak",
    "wave_period_sig", "wave_period_avg", "wave_period_peak",
    "wave_direction_avg", "wave_direction_peak",
    "swell_height", "swell_period", "swell_direction",
    "wind_wave_height", "wind_wave_period", "wind_wave_direction",
    "wind_speed", "wind_gust", "wind_direction",
    "air_temp", "sea_temp", "pressure"
]

# Field metadata for exports
FIELD_METADATA = {
    "wave_height_sig": {"name": "Significant Wave Height", "unit": "m"},
    "wave_height_peak": {"name": "Peak Wave Height", "unit": "m"},
    "wave_period_avg": {"name": "Average Wave Period", "unit": "s"},
    "wave_period_peak": {"name": "Peak Wave Period", "unit": "s"},
    "wave_direction_peak": {"name": "Peak Wave Direction", "unit": "°"},
    "swell_height": {"name": "Swell Height", "unit": "m"},
    "swell_period": {"name": "Swell Period", "unit": "s"},
    "swell_direction": {"name": "Swell Direction", "unit": "°"},
    "wind_wave_height": {"name": "Wind Wave Height", "unit": "m"},
    "wind_wave_period": {"name": "Wind Wave Period", "unit": "s"},
    "wind_wave_direction": {"name": "Wind Wave Direction", "unit": "°"},
    "wind_speed": {"name": "Wind Speed", "unit": "kt"},
    "wind_gust": {"name": "Wind Gust", "unit": "kt"},
    "wind_direction": {"name": "Wind Direction", "unit": "°"},
    "air_temp": {"name": "Air Temperature", "unit": "°C"},
    "sea_temp": {"name": "Sea Temperature", "unit": "°C"},
    "pressure": {"name": "Pressure", "unit": "hPa"},
}
```

**Usage in scripts:**
```python
# Instead of:
SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()

# Use:
from src.core.config import BUOY_DATABASE as SQLITE_PATH
```

**Impact:** 11 scripts updated, 20+ lines of code eliminated

---

### 2. Utility Functions (HIGH PRIORITY)

**Problem:** Utility functions duplicated across multiple scripts

#### 2A: Unit Conversion Functions

**Current State:**

`kmh_to_knots()` duplicated in 3 scripts:
- sqlite_to_json.py:54
- influx_to_mqtt.py:68
- export_24hr_timeseries.py:46

`ms_to_kmh()` duplicated in 2 scripts:
- fetch_noaa_buoy.py:46
- fetch_surrey_wave_v2.py:180

**Proposed Solution:**

Create `src/core/units.py`:

```python
"""
Unit conversion utilities for marine weather data.
"""

def kmh_to_knots(kmh):
    """Convert kilometers per hour to knots."""
    if kmh is None:
        return None
    try:
        return round(float(kmh) * 0.539957, 2)
    except (TypeError, ValueError):
        return None

def ms_to_kmh(ms):
    """Convert meters per second to kilometers per hour."""
    if ms is None:
        return None
    try:
        return round(float(ms) * 3.6, 2)
    except (TypeError, ValueError):
        return None

def knots_to_kmh(knots):
    """Convert knots to kilometers per hour."""
    if knots is None:
        return None
    try:
        return round(float(knots) * 1.852, 2)
    except (TypeError, ValueError):
        return None

def celsius_to_fahrenheit(celsius):
    """Convert Celsius to Fahrenheit."""
    if celsius is None:
        return None
    try:
        return round(float(celsius) * 9/5 + 32, 2)
    except (TypeError, ValueError):
        return None
```

**Usage:**
```python
from src.core.units import kmh_to_knots, ms_to_kmh

wind_speed_knots = kmh_to_knots(wind_speed_kmh)
```

**Impact:** 5 functions × 3 duplications = 15+ lines eliminated

---

#### 2B: Direction Utilities

**Current State:**

`degrees_to_cardinal()` duplicated in 2 scripts:
- sqlite_to_json.py:41
- influx_to_mqtt.py:55

**Proposed Solution:**

Create `src/core/directions.py`:

```python
"""
Direction conversion utilities (degrees ↔ cardinal).
"""
import math

# 16-point compass rose
DIRS_16 = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
           'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']

def degrees_to_cardinal(deg):
    """
    Convert degrees (0-360) to 16-point cardinal direction.

    Args:
        deg: Direction in degrees (0-360), where 0 = North

    Returns:
        Cardinal direction string (e.g., 'N', 'NE', 'SSW') or None
    """
    if deg is None:
        return None
    try:
        d = float(deg)
    except (TypeError, ValueError):
        return None
    if math.isnan(d):
        return None

    d = d % 360.0
    ix = int(round(d / 22.5)) % 16
    return DIRS_16[ix]

def cardinal_to_degrees(cardinal):
    """
    Convert cardinal direction to degrees.

    Args:
        cardinal: Cardinal direction string (e.g., 'N', 'NE', 'SSW')

    Returns:
        Direction in degrees (0-360) or None
    """
    if cardinal is None:
        return None
    cardinal = cardinal.strip().upper()
    if cardinal not in DIRS_16:
        return None

    ix = DIRS_16.index(cardinal)
    return (ix * 22.5) % 360

def parse_direction(val):
    """
    Parse direction from string (handles cardinal or numeric degrees).

    Args:
        val: Either cardinal string ('WSW') or numeric degrees ('275')

    Returns:
        Direction in degrees (0-360) or None
    """
    if not val or val in ['MM', 'M', 'NA', '']:
        return None

    # Try parsing as degrees first
    try:
        return float(val) % 360
    except (TypeError, ValueError):
        pass

    # Try parsing as cardinal
    return cardinal_to_degrees(val)
```

**Usage:**
```python
from src.core.directions import degrees_to_cardinal, parse_direction

cardinal = degrees_to_cardinal(275)  # 'W'
degrees = parse_direction('WSW')      # 247.5
```

**Impact:** 3 functions × 2 duplications = 6+ lines eliminated

---

### 3. Station Data Duplication (MEDIUM PRIORITY)

**Problem:** BUOYS dictionary duplicated in 3 scripts instead of using stations.json

**Current State:**

BUOYS dictionary defined in:
- sqlite_to_json.py:15
- influx_to_mqtt.py:30
- export_24hr_timeseries.py:13

Each has slightly different metadata (name/location wording).

**Proposed Solution:**

Already have `src/core/stations.py` (moved in Phase 2), but scripts aren't using it!

**Update scripts to use:**
```python
from src.core.stations import get_all_buoys, get_buoy_by_id

BUOYS = get_all_buoys()  # Returns dict from stations.json
```

**Impact:**
- 3 hardcoded BUOYS dictionaries eliminated
- Station metadata centralized in stations.json
- Consistent naming/metadata across all scripts

---

### 4. Database Helper Utilities (MEDIUM PRIORITY)

**Problem:** Each script opens its own SQLite connections, no shared helpers

**Current State:**

Every script has:
```python
conn = sqlite3.connect(SQLITE_PATH)
cursor = conn.cursor()
# ... do stuff ...
conn.close()
```

Some use context managers (`with`), some don't. Inconsistent error handling.

**Proposed Solution:**

Create `src/core/database.py`:

```python
"""
Database connection and query helpers.
"""
import sqlite3
from contextlib import contextmanager
from src.core.config import BUOY_DATABASE, TIDE_DATABASE

@contextmanager
def buoy_db_connection(timeout=10):
    """
    Context manager for buoy database connection.

    Usage:
        with buoy_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM buoy_observation")
    """
    conn = sqlite3.connect(BUOY_DATABASE, timeout=timeout)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

@contextmanager
def tide_db_connection(timeout=10):
    """Context manager for tide database connection."""
    conn = sqlite3.connect(TIDE_DATABASE, timeout=timeout)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def execute_query(db_connection, query, params=None):
    """
    Execute query and return results as list of dicts.

    Args:
        db_connection: Context manager (buoy_db_connection or tide_db_connection)
        query: SQL query string
        params: Optional tuple of parameters

    Returns:
        List of dicts (column_name: value)
    """
    with db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

def get_latest_observation(buoy_id, field, freshness_window=7200):
    """
    Get most recent non-null value for a field within freshness window.

    Args:
        buoy_id: Buoy identifier (e.g., '4600146')
        field: Field name (e.g., 'wave_height_sig')
        freshness_window: Maximum age in seconds (default 2 hours)

    Returns:
        (value, timestamp) tuple or (None, None)
    """
    import time
    cutoff = int(time.time()) - freshness_window

    query = f"""
        SELECT {field}, timestamp
        FROM buoy_observation
        WHERE buoy_id = ?
          AND {field} IS NOT NULL
          AND timestamp >= ?
        ORDER BY timestamp DESC
        LIMIT 1
    """

    result = execute_query(buoy_db_connection, query, (buoy_id, cutoff))
    if result:
        return result[0][field], result[0]['timestamp']
    return None, None
```

**Usage:**
```python
from src.core.database import buoy_db_connection, get_latest_observation

# Simple query
with buoy_db_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM buoy_observation")
    count = cursor.fetchone()[0]

# Helper function
wave_height, timestamp = get_latest_observation('4600146', 'wave_height_sig')
```

**Impact:**
- Consistent connection handling across all scripts
- Centralized error handling and transaction management
- Reusable query patterns
- Easier to add connection pooling later if needed

---

### 5. InfluxDB Soft Dependency Pattern (LOW PRIORITY)

**Problem:** `InfluxSink` class only in buoy_to_influx_sqlite.py, but pattern is good

**Current State:**

Nice pattern for optional InfluxDB integration exists in buoy_to_influx_sqlite.py but not reusable.

**Proposed Solution:**

Create `src/integration/influxdb_sink.py`:

```python
"""
Optional InfluxDB integration (soft dependency).
"""
from pathlib import Path

class InfluxSink:
    """
    Optional InfluxDB sink for time-series data.

    Gracefully degrades if InfluxDB client not installed or unavailable.
    """
    def __init__(self, env_path="~/.envcan_wave_influx"):
        self.online = False
        self.client = None

        try:
            from influxdb import InfluxDBClient
        except ImportError as e:
            print(f"ℹ️  Influx client not installed ({e}); running SQLite-only.")
            return

        creds = self._load_credentials(env_path)
        if not creds:
            return

        try:
            self.client = InfluxDBClient(
                host=creds.get("INFLUX_HOST"),
                port=int(creds.get("INFLUX_PORT", 8086)),
                username=creds.get("INFLUX_USER"),
                password=creds.get("INFLUX_PASS"),
                database=creds.get("INFLUX_DB"),
                ssl=False,
                timeout=5,
            )
            self.client.ping()
            self.online = True
            print("✅ InfluxDB connection established.")
        except Exception as e:
            print(f"⚠️  InfluxDB unavailable ({e}); running SQLite-only.")
            self.online = False

    def _load_credentials(self, env_path):
        """Load credentials from .env file."""
        p = Path(env_path).expanduser()
        if not p.exists():
            print(f"ℹ️  Influx env file not found at {p}; running SQLite-only.")
            return None

        creds = {}
        for line in p.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                creds[k.strip()] = v.strip()
        return creds

    def write_point(self, measurement, tags, time_iso, fields_dict):
        """Write a single point to InfluxDB (no-op if offline)."""
        if not self.online:
            return

        point = {
            "measurement": measurement,
            "tags": tags,
            "time": time_iso,
            "fields": fields_dict
        }

        try:
            self.client.write_points([point])
        except Exception as e:
            print(f"⚠️  InfluxDB write failed: {e}")
```

**Usage:**
```python
from src.integration.influxdb_sink import InfluxSink

influx = InfluxSink()
influx.write_point("buoy_data", {"buoy_id": "4600146"}, timestamp_iso, fields)
```

**Impact:**
- Reusable InfluxDB integration for other scripts
- Consistent soft dependency pattern
- Easier to maintain (one place to update InfluxDB logic)

---

### 6. Logging Configuration (LOW PRIORITY)

**Problem:** Each script does its own print statements, no centralized logging

**Current State:**

Scripts use:
- `print()` for info messages
- `print(f"❌ Error: ...")` for errors
- No log levels, no structured logging
- Cron redirects to separate log files (`>> script.log 2>&1`)

**Proposed Solution:**

Create `src/core/logging_config.py`:

```python
"""
Centralized logging configuration.
"""
import logging
import sys

def setup_logging(name, level=logging.INFO):
    """
    Set up logging for a script.

    Args:
        name: Logger name (usually __name__)
        level: Logging level (default INFO)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Console handler with formatting
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger
```

**Usage:**
```python
from src.core.logging_config import setup_logging

logger = setup_logging(__name__)

logger.info("Starting buoy data fetch")
logger.warning("Influx unavailable, using SQLite only")
logger.error("Failed to fetch NOAA data")
```

**Impact:**
- Consistent logging across all scripts
- Easier to filter/search logs by level
- Can add file handlers, syslog, etc. in one place
- Better production monitoring

---

## Proposed Refactoring Phases (Post-Structure)

### Phase A: Core Utilities (Week 1)
**Goal:** Create shared configuration and utilities

**Tasks:**
1. Create `src/core/config.py` with all paths and constants
2. Create `src/core/units.py` with conversion functions
3. Create `src/core/directions.py` with direction utilities
4. Update all scripts to use shared config (11 scripts)
5. Update scripts to use shared units/directions (5 scripts)
6. Test all scripts still work

**Deliverable:** Eliminated ~50 lines of duplicated code

---

### Phase B: Database Utilities (Week 2)
**Goal:** Centralize database connection management

**Tasks:**
1. Create `src/core/database.py` with connection helpers
2. Update scripts to use database helpers (11 scripts)
3. Add consistent error handling
4. Test database operations

**Deliverable:** Consistent DB handling, easier maintenance

---

### Phase C: Station Data Integration (Week 3)
**Goal:** Eliminate BUOYS dictionary duplication

**Tasks:**
1. Update 3 scripts to use `src.core.stations.get_all_buoys()`
2. Remove hardcoded BUOYS dictionaries
3. Verify station metadata consistency
4. Test exports have correct station names

**Deliverable:** Single source of truth for station data

---

### Phase D: Optional Improvements (Future)
**Can be done anytime after Phase A-C**

1. Extract `InfluxSink` to `src/integration/influxdb_sink.py`
2. Implement centralized logging (`src/core/logging_config.py`)
3. Add type hints throughout codebase
4. Add unit tests for core utilities
5. Add docstrings for all functions

---

## Metrics

### Current State
- **Total Python files:** 17
- **Lines of duplicated config:** ~40
- **Lines of duplicated utilities:** ~60
- **Hardcoded BUOYS dicts:** 3
- **Database connection patterns:** 11 variations

### After Code Refactoring
- **Lines eliminated:** ~100+
- **Shared modules created:** 4-5
- **Maintenance burden:** -50% (one place to update paths, units, etc.)
- **Code reuse:** 6+ functions shared across scripts
- **Consistency:** All scripts use same patterns

---

## Decision: Structure First vs Code First

### Recommended Approach: **Structure First, Code Second**

**Timeline:**
1. **Weeks 1-5:** Three-phase structure refactoring (already planned)
2. **Weeks 6-8:** Code refactoring Phases A-C
3. **Week 9+:** Optional improvements (Phase D)

**Total:** ~9 weeks for complete refactoring

### Rationale:

1. **Structure refactoring is lower risk** → Do it first, validate it works
2. **Code refactoring is easier with clean structure** → Wait until you can see patterns clearly
3. **Separation of concerns** → Git history shows moves vs logic changes separately
4. **Incremental progress** → Can stop after structure if code refactoring not urgent
5. **Psychological benefit** → Clean workspace first, then improve code quality

### Alternative (Not Recommended): Code First

If you really want to refactor code first:
- Do Phases A-C of code refactoring while everything is still in root
- Then do 3-phase structure migration
- **Risk:** More churn, imports updated twice, harder to see patterns

---

## Open Questions

1. **Urgency:** Is code duplication causing active pain, or can it wait until after structure refactoring?

2. **Scope:** Do all code refactoring phases (A-D), or just core utilities (A-B)?

3. **Testing:** Should we add unit tests during code refactoring, or as separate phase?

4. **Type Hints:** Add type hints during refactoring, or defer to later?

5. **Surrey Scripts:** Are Surrey integration scripts (src/integration/surrey/) production or experimental? If experimental, skip them in code refactoring.

---

## Summary

**Current state:** 17 scripts with ~100 lines of duplicated code, no shared utilities

**Structure refactoring:** Already planned (3 phases, 5 weeks)

**Code refactoring:** 3 phases (A-C), 3 weeks

**Combined timeline:** 8-9 weeks for complete transformation

**Recommendation:** Do structure refactoring first (Phases 1-3), THEN code refactoring (Phases A-C)

**Key benefits:**
- Eliminates 100+ lines of duplicated code
- Creates 4-5 shared utility modules
- Reduces maintenance burden by 50%
- Establishes patterns for future growth
- Improves code quality and consistency

**Risk:** Low if done incrementally with testing between phases

---

**Ready for your decision: Structure first, code first, or both together?**
