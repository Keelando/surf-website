# Phase 3: Final Data Pipeline Cleanup

**Date Created:** 2025-11-13
**Estimated Time:** 2-3 hours
**Prerequisites:** Phase 2A and 2B refactoring complete

## Overview

Phase 3 completes the data pipeline refactoring by:
1. **Phase 3A:** Centralizing logging infrastructure (eliminate 64+ print statements)
2. **Phase 3B:** Extracting `safe_json_write()` to shared utilities (eliminate ~30 lines duplication)

**Expected Results:**
- ~250-300 additional lines of duplicate code eliminated
- Consistent logging across all 11+ production scripts
- Better debugging and monitoring capabilities
- More maintainable codebase

---

## Phase 3A: Centralize Logging

### Current State
- Only `parse_marine_forecast.py` uses proper logging
- 64+ `print()` statements across fetch scripts:
  - `fetch_storm_surge.py`: 38 occurrences
  - `fetch_surrey_wave_v2.py`: 14 occurrences
  - `fetch_noaa_buoy.py`: 12 occurrences
- No log rotation or persistence
- Debugging production issues requires manual inspection

### Implementation Steps

#### Step 1: Create Logging Infrastructure

**File:** `/home/user/surf-website/logging_utils.py`

```python
#!/usr/bin/env python3
"""
Centralized logging configuration for marine weather monitoring system.

Provides consistent logging setup across all data processing scripts with:
- Rotating file handlers (prevent disk fill)
- Console output for development
- Standardized formatting
- Per-script log files

Usage:
    from logging_utils import get_logger

    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.warning("Missing data for station X")
    logger.error("API request failed", exc_info=True)
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from config import PROJECT_ROOT

# Log directory
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Default log format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Log levels
DEFAULT_LEVEL = logging.INFO
FILE_LEVEL = logging.DEBUG
CONSOLE_LEVEL = logging.INFO


def get_logger(name: str, log_file: str = None, console: bool = True) -> logging.Logger:
    """
    Get a configured logger instance for a script.

    Args:
        name: Logger name (typically __name__ from calling script)
        log_file: Optional custom log filename (defaults to script name)
        console: Whether to also log to console (default True)

    Returns:
        Configured logger instance

    Example:
        logger = get_logger(__name__)
        logger.info("Script started")
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured
    if logger.handlers:
        return logger

    logger.setLevel(DEFAULT_LEVEL)
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # File handler with rotation (5MB max, keep 3 backups)
    if log_file is None:
        # Extract script name from logger name
        script_name = name.split('.')[-1] if '.' in name else name
        log_file = f"{script_name}.log"

    file_handler = RotatingFileHandler(
        LOG_DIR / log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
        encoding='utf-8'
    )
    file_handler.setLevel(FILE_LEVEL)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler for development/debugging
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(CONSOLE_LEVEL)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def log_script_start(logger: logging.Logger, script_name: str, **kwargs):
    """
    Log standard script start message with optional context.

    Args:
        logger: Logger instance
        script_name: Name of script starting
        **kwargs: Additional context to log (e.g., stations, time_range)
    """
    logger.info(f"{'=' * 60}")
    logger.info(f"Starting {script_name}")
    for key, value in kwargs.items():
        logger.info(f"  {key}: {value}")
    logger.info(f"{'=' * 60}")


def log_script_end(logger: logging.Logger, script_name: str, success: bool = True, **kwargs):
    """
    Log standard script end message with optional metrics.

    Args:
        logger: Logger instance
        script_name: Name of script ending
        success: Whether script completed successfully
        **kwargs: Additional metrics to log (e.g., records_processed, errors)
    """
    status = "✓ COMPLETED" if success else "✗ FAILED"
    logger.info(f"{'=' * 60}")
    logger.info(f"{status}: {script_name}")
    for key, value in kwargs.items():
        logger.info(f"  {key}: {value}")
    logger.info(f"{'=' * 60}")


# Test/demonstration code
if __name__ == "__main__":
    # Example usage
    logger = get_logger(__name__)

    log_script_start(logger, "test_script", mode="testing", stations=5)

    logger.info("This is an info message")
    logger.warning("This is a warning")
    logger.debug("This is a debug message (only in file)")

    try:
        raise ValueError("Example error")
    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)

    log_script_end(logger, "test_script", success=True, records_processed=100)

    print(f"\nLog file created at: {LOG_DIR / 'logging_utils.log'}")
```

#### Step 2: Create logs/.gitkeep

**File:** `/home/user/surf-website/logs/.gitkeep`

```
# Keep logs directory in git but ignore log files
```

#### Step 3: Update .gitignore

Add to `/home/user/surf-website/.gitignore`:

```gitignore
# Logs (keep directory, ignore files)
logs/*.log
logs/*.log.*
```

#### Step 4: Migration Priority Order

Migrate scripts in batches based on complexity:

**Batch 1 - Simple (Test logging setup):**
1. `parse_marine_forecast.py` - Already uses logging, just switch to logging_utils
2. `sqlite_to_json.py` - Simple export script, minimal logging

**Batch 2 - Medium (Fetch scripts):**
3. `fetch_surrey_wave_v2.py` - 14 print statements
4. `fetch_noaa_buoy.py` - 12 print statements
5. `buoy_to_influx_sqlite.py` - Main buoy ingestion

**Batch 3 - Complex (Multi-stage scripts):**
6. `fetch_storm_surge.py` - 38 print statements, complex WMS logic
7. `tide_to_sqlite.py` - Tide fetching with multiple API calls
8. `export_tide_json.py` - Multi-file export with downsampling

**Batch 4 - Remaining exports:**
9. `export_24hr_timeseries.py`
10. `export_combined_water_level.py`
11. `export_observed_storm_surge.py`
12. `export_hindcast_json.py`
13. `influx_to_mqtt.py`

### Migration Pattern (Example)

**Before (fetch_surrey_wave_v2.py):**
```python
#!/usr/bin/env python3
import requests
import json

def main():
    print("Fetching Surrey wave data...")

    try:
        response = requests.get(API_URL)
        print(f"Status code: {response.status_code}")
        data = response.json()
        print(f"Found {len(data)} records")
    except Exception as e:
        print(f"Error: {e}")
        return

    print("✓ Done")

if __name__ == "__main__":
    main()
```

**After:**
```python
#!/usr/bin/env python3
import requests
import json
from logging_utils import get_logger, log_script_start, log_script_end

logger = get_logger(__name__)

def main():
    log_script_start(logger, "Surrey Wave Fetcher", source="Surrey API")

    try:
        logger.info(f"Requesting data from {API_URL}")
        response = requests.get(API_URL)
        logger.debug(f"Status code: {response.status_code}")

        data = response.json()
        logger.info(f"Found {len(data)} records")

        # ... processing ...

        log_script_end(logger, "Surrey Wave Fetcher", success=True,
                       records_processed=len(data))
    except Exception as e:
        logger.error(f"Failed to fetch Surrey data: {e}", exc_info=True)
        log_script_end(logger, "Surrey Wave Fetcher", success=False)
        return

if __name__ == "__main__":
    main()
```

### Logging Level Guidelines

- `logger.debug()` - Detailed diagnostic info (SQL queries, response bodies)
- `logger.info()` - Normal operational events (script start/end, record counts)
- `logger.warning()` - Unexpected but handled (missing optional fields, stale data)
- `logger.error()` - Errors requiring attention (API failures, database errors)

---

## Phase 3B: Extract safe_json_write()

### Current State

Function duplicated in **6 files** with minor variations:
- `sqlite_to_json.py:28`
- `export_tide_json.py:37` (adds `sort_keys=True`)
- `export_24hr_timeseries.py:73` (adds `sort_keys=True`)
- `export_combined_water_level.py` (similar)
- `export_hindcast_json.py` (similar)
- `fetch_storm_surge.py` (similar pattern)

### Implementation Steps

#### Step 1: Add to Shared Utilities

**Option A: Add to existing `config.py`** (recommended - keeps I/O helpers together)

Add to `/home/user/surf-website/config.py` (after existing helper functions):

```python
# =============================================================================
# File I/O Helpers
# =============================================================================

def safe_json_write(path: Path, data: dict, sort_keys: bool = False, indent: int = 2):
    """
    Atomic JSON write: temp file + rename to avoid partial writes.

    This prevents race conditions where a reader might get incomplete JSON
    during the write operation. The temp file is written completely, then
    atomically renamed to the target path.

    Args:
        path: Target file path (will be created/overwritten)
        data: Dictionary to serialize as JSON
        sort_keys: Whether to sort dictionary keys (default False)
        indent: JSON indentation spaces (default 2)

    Example:
        from config import safe_json_write, EXPORT_DIR

        data = {"station": "Point_Atkinson", "wave_height": 1.5}
        safe_json_write(EXPORT_DIR / "latest.json", data)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=indent, sort_keys=sort_keys))
    tmp.replace(path)
```

Add to imports at top of config.py:
```python
import json
```

**Option B: Create new `file_utils.py`** (if config.py getting too large)

#### Step 2: Update All Scripts Using safe_json_write()

**Scripts to update:**

1. **sqlite_to_json.py**
   - Remove lines 28-33 (function definition)
   - Update import: `from config import BUOY_DATABASE, EXPORT_DIR, BUOY_FRESHNESS_WINDOW, safe_json_write`

2. **export_tide_json.py**
   - Remove lines 37-42
   - Update import: `from config import TIDE_DATABASE, EXPORT_DIR, safe_json_write`
   - Update calls to include `sort_keys=True` parameter

3. **export_24hr_timeseries.py**
   - Remove lines 73-78
   - Update import: `from config import BUOY_DATABASE, EXPORT_DIR, safe_json_write`
   - Update calls to include `sort_keys=True` parameter

4. **export_combined_water_level.py**
   - Update similar pattern

5. **export_hindcast_json.py**
   - Update similar pattern

6. **fetch_storm_surge.py**
   - Update similar pattern

---

## Testing Plan

### Phase 3A Testing

After migrating each script:

1. **Manual test:**
   ```bash
   python3 script_name.py
   ```

2. **Verify log file created:**
   ```bash
   ls -lh logs/
   tail -n 50 logs/script_name.log
   ```

3. **Check log rotation:**
   ```bash
   # Should create .log.1, .log.2, .log.3 after 5MB
   ls -lh logs/*.log*
   ```

4. **Verify functionality unchanged:**
   - Check JSON exports still generated
   - Verify database updates still work
   - Ensure no regressions in data quality

### Phase 3B Testing

After updating each export script:

1. **Verify imports work:**
   ```bash
   python3 -c "from config import safe_json_write; print('OK')"
   ```

2. **Test atomic write behavior:**
   ```bash
   # Run export script
   python3 sqlite_to_json.py

   # Verify no .tmp files left behind
   ls ~/site/data/*.tmp  # Should be empty

   # Verify JSON valid
   python3 -c "import json; json.load(open('~/site/data/latest_buoy_v2.json'))"
   ```

3. **Compare before/after JSON output:**
   ```bash
   # Should be byte-for-byte identical (except scripts using sort_keys)
   diff old_output.json new_output.json
   ```

---

## Rollback Plan

If issues arise:

### Rollback Phase 3A (Logging)
```bash
# Revert to previous commit
git log --oneline -10
git revert <commit_hash>

# Or manual cleanup
git checkout HEAD -- logging_utils.py
# Restore individual scripts from git history
```

### Rollback Phase 3B (safe_json_write)
```bash
# Revert config.py changes
git checkout HEAD -- config.py

# Restore individual scripts
git checkout HEAD -- sqlite_to_json.py export_tide_json.py ...
```

---

## Success Criteria

### Phase 3A Complete When:
- [ ] `logging_utils.py` created and tested
- [ ] `logs/` directory created with `.gitkeep`
- [ ] `.gitignore` updated
- [ ] All 13 production scripts migrated from print() to logging
- [ ] Log files rotating correctly (max 5MB × 3 backups per script)
- [ ] All scripts produce expected output (no functionality regressions)
- [ ] Logs contain useful debugging information

### Phase 3B Complete When:
- [ ] `safe_json_write()` added to shared utilities
- [ ] All 6 duplicate implementations removed
- [ ] All JSON exports still work correctly
- [ ] No `.tmp` files left behind after exports
- [ ] Import statements updated in all affected scripts

### Overall Phase 3 Complete When:
- [ ] ~250-300 lines of duplicate code eliminated
- [ ] Consistent logging across entire pipeline
- [ ] All tests pass (manual verification)
- [ ] Git commit with clear message documenting changes
- [ ] Ready for production deployment

---

## Git Commit Strategy

Commit in logical chunks for easy rollback:

```bash
# Phase 3A - Infrastructure
git add logging_utils.py logs/.gitkeep .gitignore
git commit -m "Phase 3A: Add centralized logging infrastructure"

# Phase 3A - Batch 1
git add parse_marine_forecast.py sqlite_to_json.py
git commit -m "Phase 3A: Migrate batch 1 scripts to logging_utils (2 scripts)"

# Phase 3A - Batch 2
git add fetch_surrey_wave_v2.py fetch_noaa_buoy.py buoy_to_influx_sqlite.py
git commit -m "Phase 3A: Migrate batch 2 fetch scripts to logging_utils (3 scripts)"

# ... etc

# Phase 3B
git add config.py sqlite_to_json.py export_tide_json.py export_24hr_timeseries.py \
        export_combined_water_level.py export_hindcast_json.py fetch_storm_surge.py
git commit -m "Phase 3B: Extract safe_json_write() to shared utilities (6 scripts)"

# Final commit
git commit -m "Phase 3 complete: Centralized logging + safe_json_write extraction"
```

---

## Estimated Timeline

| Task | Time | Notes |
|------|------|-------|
| Create logging_utils.py | 20 min | Includes testing |
| Update .gitignore | 2 min | Quick add |
| Migrate batch 1 (2 scripts) | 15 min | Proof of concept |
| Migrate batch 2 (3 scripts) | 30 min | Fetch scripts |
| Migrate batch 3 (3 scripts) | 45 min | Complex scripts |
| Migrate batch 4 (5 scripts) | 30 min | Export scripts |
| Extract safe_json_write() | 20 min | Add to config.py |
| Update 6 scripts to use it | 20 min | Import changes |
| Testing & verification | 30 min | Run all scripts |
| **Total** | **~3 hours** | Can be split across sessions |

---

## Notes

- **Backward compatible:** Logging changes don't affect output/functionality
- **Low risk:** Each script can be migrated independently
- **Immediate value:** Better debugging starts with first migrated script
- **No external dependencies:** Uses Python stdlib logging
- **Production ready:** RotatingFileHandler prevents disk space issues

---

## Questions?

Before starting tomorrow, consider:

1. **Log retention:** 5MB × 3 backups = 15MB max per script OK?
2. **Console output:** Keep console logging during migration? (Can disable later)
3. **Log location:** `~/surf-website/logs/` OK or prefer `/var/log/`?
4. **safe_json_write() location:** Add to `config.py` or new `file_utils.py`?

---

**Ready to execute Phase 3 tomorrow!**
