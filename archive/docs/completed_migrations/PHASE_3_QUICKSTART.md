# Phase 3 Quick Start (No Over-Engineering!)

**Estimated time:** 1-2 hours max
**Goal:** Clean up the last bits of duplication without going crazy

## What We're Actually Doing

1. Add basic logging (replace print statements)
2. Extract duplicate `safe_json_write()` function
3. **That's it.** No fancy features, no over-engineering.

---

## Part 1: Basic Logging (30-45 min)

### Step 1: Create dead-simple logging helper

Create `logging_utils.py`:
```python
#!/usr/bin/env python3
"""Simple logging setup - nothing fancy."""
import logging
from pathlib import Path

LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

def get_logger(name):
    """Get a logger. That's it."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # Write to file
    fh = logging.FileHandler(LOG_DIR / f"{name}.log")
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)

    # Also print to console
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
    logger.addHandler(ch)

    return logger
```

### Step 2: Update .gitignore

Add one line:
```
logs/*.log
```

### Step 3: Migrate scripts (pick 3-4 important ones)

**Don't migrate everything!** Just the scripts that actually matter for debugging:

Priority:
- `fetch_storm_surge.py` (complex, 38 print statements)
- `fetch_noaa_buoy.py` (external API calls)
- `tide_to_sqlite.py` (external API calls)

Pattern:
```python
# Add at top
from logging_utils import get_logger
logger = get_logger(__name__)

# Replace this:
print("Fetching data...")

# With this:
logger.info("Fetching data...")

# Replace this:
print(f"Error: {e}")

# With this:
logger.error(f"Error: {e}")
```

**Leave the rest alone.** If print() works fine, don't touch it.

---

## Part 2: Extract safe_json_write() (15-20 min)

### Step 1: Add to config.py

At the top, add import:
```python
import json
```

At the bottom (before `if __name__`), add:
```python
def safe_json_write(path: Path, data: dict, sort_keys: bool = False):
    """Atomic write to prevent partial JSON files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=sort_keys))
    tmp.replace(path)
```

### Step 2: Update the 6 scripts that duplicate it

In each script:

**Remove the function** (usually ~5 lines)

**Update import:**
```python
from config import EXPORT_DIR, safe_json_write  # Add safe_json_write
```

**Files to update:**
1. `sqlite_to_json.py`
2. `export_tide_json.py` (use `sort_keys=True`)
3. `export_24hr_timeseries.py` (use `sort_keys=True`)
4. `export_combined_water_level.py`
5. `export_hindcast_json.py`
6. `fetch_storm_surge.py`

---

## Testing

```bash
# Test logging
python3 fetch_storm_surge.py
ls logs/  # Should see fetch_storm_surge.log

# Test safe_json_write
python3 sqlite_to_json.py
ls ~/site/data/*.tmp  # Should be empty (tmp files cleaned up)
```

---

## Done!

That's it. No log rotation, no fancy error handlers, no over-engineering.

**Wins:**
- ~200 lines of duplicated code removed
- Better debugging when things break
- More maintainable

**What we're NOT doing:**
- Log rotation (if logs get big, we'll deal with it then)
- Complex error handling decorators
- Centralized monitoring dashboards
- Any other fancy stuff

Keep it simple!
