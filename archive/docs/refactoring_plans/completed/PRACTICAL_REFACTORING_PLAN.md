# Practical Refactoring Plan
**Created:** 2025-01-13
**Approach:** Manual, incremental, low-risk
**Goal:** Declutter directories, improve script logic, consolidate codebase

---

## Current State Analysis

### Issues Identified

**1. Directory Clutter (16 non-code files in root)**
- 10+ markdown files scattered in root
- Config files not organized
- Old backups and migration artifacts
- Deploy scripts mixed with data scripts

**2. Code Duplication**
- **5 scripts** duplicate unit conversion functions (kmh_to_knots, ms_to_kmh, degrees_to_cardinal)
- **13 scripts** hardcode database paths
- Tide calculation logic duplicated across 2 scripts

**3. Deprecated/Unused Scripts**
- `calculate_storm_surge_observed.py` - NOT in cron, likely replaced by `export_observed_storm_surge.py`
- `update_exports_for_surrey.py` - One-time migration script
- `tide_stations.json` - Old format, replaced by unified `stations.json`

**4. Missing Cron Jobs (FIXED!)**
- ✅ `export_hindcast_json.py` - Added (runs daily at 2 AM)
- ✅ `export_observed_storm_surge.py` - Added (runs every 5 min)

**5. Tide/Storm Surge Interrelationships**

```
Data Flow:
┌─────────────────────────────────────────────────────────────┐
│ DFO IWLS API                                                 │
└──────────────┬──────────────────────────────────────────────┘
               │
               ▼
   tide_to_sqlite.py (3 modes: obs/pred/highlow)
               │
               ▼
   ~/.local/share/tide_data.sqlite
               │
      ┌────────┴────────┐
      │                 │
      ▼                 ▼
export_tide_json.py   export_observed_storm_surge.py
      │                 │ (obs - pred = surge)
      │                 │
      │                 ▼
      │         observed_surge.json
      │                 │
      ▼                 │
tide JSONs              │
      │                 │
      └────────┬────────┘
               │
               ▼
   export_combined_water_level.py
               │ (tide + storm surge forecast)
               ▼
   combined_water_level.json
```

**Key insight:** Tide and storm surge are tightly coupled. The observed surge calculation (obs - pred) is reused for hindcast validation.

---

## Refactoring Strategy: 3 Phases

### Phase 1: Directory Cleanup (LOW RISK, HIGH IMPACT)
**Time:** 1-2 hours
**Risk:** Very low (no code changes)
**Status:** Ready to execute

### Phase 2: Code Consolidation (MEDIUM RISK, MEDIUM IMPACT)
**Time:** 3-4 hours (manual, incremental)
**Risk:** Low (if done incrementally with testing)
**Status:** Plan complete, ready after Phase 1

### Phase 3: Script Organization (MEDIUM-HIGH RISK, OPTIONAL)
**Time:** 4-6 hours
**Risk:** Medium (import changes, cron updates)
**Status:** Optional - evaluate after Phase 1 & 2

---

## Phase 1: Directory Cleanup

### Goals
1. Move all documentation to `docs/` subdirectories
2. Move configuration to `config/`
3. Archive deprecated files
4. Clean root directory to only Python scripts + essentials

### Structure (After Cleanup)

```
envcan_wave/
├── README.md                          # Keep in root
├── requirements.txt                   # Keep in root
├── *.py (17 scripts)                  # Keep in root (cron uses these paths)
│
├── config/                            # NEW
│   ├── stations.json                  # Master registry (unified)
│   └── tide_stations.json             # OLD format (archive candidate)
│
├── docs/                              # Reorganized
│   ├── README.md                      # NEW: Documentation index
│   ├── project/                       # NEW: Project docs
│   │   ├── CLAUDE.md
│   │   ├── TODO.md
│   │   ├── PRACTICAL_REFACTORING_PLAN.md (this file)
│   │   └── [other refactoring docs]
│   ├── deployment/                    # NEW: Deployment docs
│   │   ├── cron.txt
│   │   ├── DEPLOYMENT.md
│   │   ├── STORM_SURGE_SETUP.md
│   │   └── TROUBLESHOOTING.md
│   ├── integrations/                  # NEW: Integration guides
│   │   ├── SURREY_*.md (3 files)
│   │   └── deploy_surrey_integration.sh
│   ├── frontend/                      # NEW: Frontend docs
│   │   └── MARINE_FORECAST_FRONTEND_SUMMARY.md
│   └── [existing docs remain]
│
├── archive/                           # NEW: Deprecated files
│   ├── backups/
│   │   └── CLAUDE.md.backup-2025-11-05
│   ├── migrations/
│   │   ├── stations_migration/
│   │   └── phase0_scaffolding/
│   └── deprecated_scripts/
│       ├── calculate_storm_surge_observed.py
│       ├── update_exports_for_surrey.py
│       └── tide_stations.json
│
├── scripts/                           # Utility scripts
│   └── generate_pr_description.sh
│
├── examples/                          # Unchanged
├── tests/                             # Unchanged
└── data/                              # Local data (unchanged)
```

### Execution Checklist

**Step 1: Create directories**
```bash
mkdir -p config
mkdir -p docs/{project,deployment,integrations,frontend}
mkdir -p archive/{backups,migrations,deprecated_scripts}
```

**Step 2: Move documentation** (using `git mv`)
```bash
# Project docs
git mv CLAUDE.md docs/project/
git mv TODO.md docs/project/
git mv PRACTICAL_REFACTORING_PLAN.md docs/project/
git mv CODE_REFACTORING_OPPORTUNITIES.md docs/project/
git mv HYBRID_REFACTORING_STRATEGY.md docs/project/
git mv PHASE0_READY_TO_EXECUTE.md docs/project/
git mv REFACTORING_*.md docs/project/

# Deployment docs
git mv cron.txt docs/deployment/

# Integration docs
git mv SURREY_*.md docs/integrations/
git mv deploy_surrey_integration.sh docs/integrations/

# Frontend docs
git mv MARINE_FORECAST_FRONTEND_SUMMARY.md docs/frontend/
```

**Step 3: Move configuration**
```bash
git mv stations.json config/
git mv tide_stations.json config/  # Will archive later
```

**Step 4: Archive obsolete files**
```bash
# Old backups
git mv CLAUDE.md.backup-* archive/backups/ 2>/dev/null || true

# Old migration directories
git mv stations_migration archive/migrations/
git mv phase0_scaffolding archive/migrations/

# Deprecated scripts (NOT in cron)
git mv calculate_storm_surge_observed.py archive/deprecated_scripts/
git mv update_exports_for_surrey.py archive/deprecated_scripts/
git mv compare_surrey_dfo_water_levels.py archive/deprecated_scripts/  # One-time validation
```

**Step 5: Update script references**

Only 2 files need path updates:
- `stations.py` line ~10: Change to `config/stations.json`
- `validate_stations.py` line ~10: Change to `config/stations.json`

**Step 6: Test**
```bash
# Test imports work
python3 -c "from stations import get_all_buoys; print(len(get_all_buoys()))"

# Test a few scripts
python3 validate_stations.py
python3 sqlite_to_json.py
```

**Step 7: Commit**
```bash
git add -A
git commit -m "Phase 1: Organize directory structure

- Move docs to docs/ subdirectories
- Move config to config/
- Archive deprecated scripts and old migrations
- Update 2 scripts to reference config/stations.json

Result: Clean root with only Python scripts + README/requirements"
git push origin main
```

---

## Phase 2: Code Consolidation

### Goals
1. Extract shared utilities (units, directions, config)
2. Update scripts to use shared code
3. Remove deprecated scripts
4. Consolidate tide/storm surge calculation logic

### Step 2A: Create Utility Modules

**File: `utils/units.py`**
```python
"""Unit conversion utilities used across data processing scripts."""

def kmh_to_knots(kmh):
    """Convert km/h to knots. Returns None if input is None."""
    if kmh is None:
        return None
    try:
        return round(float(kmh) * 0.539957, 1)
    except (TypeError, ValueError):
        return None

def ms_to_kmh(ms):
    """Convert m/s to km/h. Returns None if input is None."""
    if ms is None:
        return None
    try:
        return round(float(ms) * 3.6, 1)
    except (TypeError, ValueError):
        return None

def meters_to_feet(meters):
    """Convert meters to feet. Returns None if input is None."""
    if meters is None:
        return None
    try:
        return round(float(meters) * 3.28084, 1)
    except (TypeError, ValueError):
        return None
```

**File: `utils/directions.py`**
```python
"""Direction/heading utilities for wind and wave data."""

# 16-point compass directions
DIRS_16 = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
           "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]

def degrees_to_cardinal(degrees, points=16):
    """
    Convert degrees (0-360) to cardinal direction.

    Args:
        degrees: Heading in degrees (0-360)
        points: Number of compass points (8 or 16)

    Returns:
        Cardinal direction string (e.g., "NW", "SSE")
        Returns None if degrees is None
    """
    if degrees is None:
        return None

    try:
        degrees = float(degrees)
    except (TypeError, ValueError):
        return None

    # Normalize to 0-360
    degrees = degrees % 360

    if points == 8:
        dirs = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        index = int((degrees + 22.5) / 45) % 8
    else:  # 16 points
        index = int((degrees + 11.25) / 22.5) % 16
        dirs = DIRS_16

    return dirs[index]
```

**File: `utils/config.py`**
```python
"""Centralized configuration for database paths and constants."""
from pathlib import Path

# Database paths
DATA_DIR = Path("~/.local/share").expanduser()

BUOY_DATABASE = DATA_DIR / "buoy_data.sqlite"
TIDE_DATABASE = DATA_DIR / "tide_data.sqlite"
STORM_SURGE_DATABASE = DATA_DIR / "storm_surge_forecast.sqlite"

# Station metadata
STATIONS_FILE = Path(__file__).parent.parent / "config" / "stations.json"

# Export paths
EXPORT_DIR = Path("~/site/data").expanduser()

# Retention periods (days)
BUOY_RETENTION_DAYS = 2
TIDE_OBS_RETENTION_DAYS = 11
TIDE_PRED_RETENTION_DAYS = 3
STORM_SURGE_RETENTION_DAYS = 30

# Freshness window (seconds)
BUOY_FRESHNESS_WINDOW = 7200  # 2 hours
TIDE_FRESHNESS_WINDOW = 7200   # 2 hours
```

**File: `utils/tide_surge.py`** (NEW - consolidates tide calculation logic)
```python
"""Tide and storm surge calculation utilities."""

def calculate_surge_offset(observation, prediction):
    """
    Calculate storm surge offset: observation - prediction.

    Args:
        observation: Observed water level (meters)
        prediction: Predicted tide level (meters)

    Returns:
        Storm surge offset in meters (can be negative)
        Returns None if either input is None
    """
    if observation is None or prediction is None:
        return None

    try:
        return round(float(observation) - float(prediction), 4)
    except (TypeError, ValueError):
        return None

def match_prediction_to_observation(obs_time, predictions_dict, window_seconds=1800):
    """
    Find the closest prediction to an observation timestamp.

    Args:
        obs_time: Observation timestamp (unix epoch)
        predictions_dict: Dict of {timestamp: water_level}
        window_seconds: Maximum time difference allowed (default 30 min)

    Returns:
        (pred_time, pred_level) tuple or (None, None) if no match
    """
    best_match = None
    best_diff = window_seconds + 1

    for pred_time, pred_level in predictions_dict.items():
        diff = abs(pred_time - obs_time)
        if diff < best_diff:
            best_diff = diff
            best_match = (pred_time, pred_level)

    return best_match if best_match else (None, None)
```

### Step 2B: Update Scripts (Manual, One at a Time)

**Priority order** (start with simplest):
1. ✅ `sqlite_to_json.py` - Uses units, directions
2. ✅ `influx_to_mqtt.py` - Uses units, directions
3. ✅ `export_24hr_timeseries.py` - Uses units
4. ✅ `fetch_noaa_buoy.py` - Uses units
5. ✅ `fetch_surrey_wave_v2.py` - Uses units
6. ✅ `export_observed_storm_surge.py` - Use tide_surge utils + config
7. ✅ `export_combined_water_level.py` - Use config paths
8. ✅ `export_hindcast_json.py` - Use config paths
9. ✅ `fetch_storm_surge.py` - Use config paths
10. ✅ `tide_to_sqlite.py` - Use config paths
11. ✅ `export_tide_json.py` - Use config paths

**Template for each script:**
```python
# Add imports at top (after existing imports)
from utils.units import kmh_to_knots, ms_to_kmh
from utils.directions import degrees_to_cardinal, DIRS_16
from utils.config import BUOY_DATABASE, TIDE_DATABASE

# Remove duplicated function definitions
# (Delete def kmh_to_knots, def ms_to_kmh, def degrees_to_cardinal)

# Replace hardcoded paths
# OLD: SQLITE_PATH = Path("~/.local/share/buoy_data.sqlite").expanduser()
# NEW: (just use BUOY_DATABASE from config)
```

**Testing strategy:**
- Update one script
- Test it manually
- Check log file for errors
- Commit with message: `"Refactor: Migrate <script> to shared utilities"`
- Move to next script

### Step 2C: Remove Deprecated Files

After confirming `export_observed_storm_surge.py` works:
```bash
# Confirm calculate_storm_surge_observed.py is NOT in cron
crontab -l | grep calculate_storm_surge

# If not found, safe to delete (already archived in Phase 1)
```

---

## Phase 3: Script Organization (OPTIONAL)

**Only do this if you need clearer functional grouping.**

### Proposed Structure
```
envcan_wave/
├── src/
│   ├── ingestion/          # Data fetching
│   │   ├── buoy_to_influx_sqlite.py
│   │   ├── fetch_noaa_buoy.py
│   │   ├── fetch_surrey_wave_v2.py
│   │   ├── fetch_storm_surge.py
│   │   ├── tide_to_sqlite.py
│   │   └── parse_marine_forecast.py
│   ├── export/             # JSON exports
│   │   ├── sqlite_to_json.py
│   │   ├── export_24hr_timeseries.py
│   │   ├── export_tide_json.py
│   │   ├── export_hindcast_json.py
│   │   ├── export_observed_storm_surge.py
│   │   └── export_combined_water_level.py
│   ├── integration/        # External integrations
│   │   └── influx_to_mqtt.py
│   └── utils/              # Shared utilities
│       ├── units.py
│       ├── directions.py
│       ├── config.py
│       ├── tide_surge.py
│       └── stations.py
├── config/                 # Configuration files
├── docs/                   # Documentation
├── tests/                  # Tests
└── ... (others)
```

### Trade-offs

**Pros:**
- Very organized
- Clear functional grouping
- Professional structure
- Easier to navigate

**Cons:**
- **ALL cron jobs need updating** (17 paths)
- **ALL imports need updating** (cross-script imports)
- Higher risk of breakage
- More complex deployment
- May not be worth it for 17 scripts

**Recommendation:** Skip Phase 3 unless you're planning significant growth (wind database, more data sources, etc.). The Phase 1 + 2 cleanup is sufficient for maintainability.

---

## Success Criteria

### Phase 1 Complete When:
- [ ] Root directory has only .py files + README + requirements
- [ ] All docs organized in docs/ subdirectories
- [ ] Config files in config/
- [ ] Deprecated files archived
- [ ] All scripts still run without errors
- [ ] Committed and pushed

### Phase 2 Complete When:
- [ ] `utils/` directory created with 4 modules
- [ ] All 11 scripts updated to use shared utilities
- [ ] No duplicated utility functions remain
- [ ] All database paths use config module
- [ ] All scripts tested and working
- [ ] Committed (one commit per script or small batches)

### Phase 3 (Optional):
- Evaluate need after Phase 1 & 2
- Only proceed if benefits outweigh complexity

---

## Timeline Estimate

| Phase | Task | Time | Risk |
|-------|------|------|------|
| **Phase 1** | Directory cleanup | 1-2 hours | Very Low |
| **Phase 2** | Code consolidation | 3-4 hours | Low |
| **Phase 3** | Script organization | 4-6 hours | Medium |
| **Total (Phase 1-2)** | Recommended scope | **4-6 hours** | **Low** |

**Recommended approach:**
1. Do Phase 1 in one sitting (1-2 hours)
2. Do Phase 2 incrementally over a weekend (30 min per script × 11 scripts)
3. Skip Phase 3 unless you really need it

---

## Risk Mitigation

### Before Starting
- [ ] All cron jobs running normally
- [ ] Recent backup of repository
- [ ] Recent backup of databases

### During Execution
- Test each script after modification
- Commit frequently (after each successful change)
- Monitor log files for errors
- Keep terminal open with `tail -f *.log`

### Rollback Options
- **Phase 1:** `git reset --hard HEAD~1` (instant rollback)
- **Phase 2:** `git revert <commit>` for specific script
- **Both:** All changes in git history, easy to undo

---

## Next Steps

Ready to start Phase 1? Here's the quick version:

```bash
# 1. Create directories
mkdir -p config docs/{project,deployment,integrations,frontend} archive/{backups,migrations,deprecated_scripts}

# 2. Move files (use commands from Step 2-4 above)
git mv CLAUDE.md docs/project/
git mv stations.json config/
# ... (full list in Phase 1 section)

# 3. Update 2 scripts (stations.py, validate_stations.py)
# Edit path to point to config/stations.json

# 4. Test
python3 validate_stations.py
python3 sqlite_to_json.py

# 5. Commit
git add -A
git commit -m "Phase 1: Organize directory structure"
git push origin main
```

After Phase 1 is complete and stable, come back for Phase 2 guidance!

---

## Questions?

- Want to adjust the directory structure?
- Concerned about any specific scripts?
- Want to do a dry-run first?
- Need help with any step?

Let's make this codebase clean and maintainable! 🚀
