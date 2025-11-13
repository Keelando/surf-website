# Hybrid Refactoring Strategy

**Approach:** Light Code Refactoring → Structure Move → Additional Polish
**Rationale:** Create shared utilities BEFORE moving files to reduce duplication and simplify migration

---

## The Insight

**Your observation:** "Code refactoring now might make the directory cleanup less painful"

**You're right!** Here's why:

1. **Less code to move** - If we consolidate duplicated functions first, we move fewer files
2. **Cleaner imports** - Scripts import from shared modules, then we move those modules once
3. **Better testing** - Test shared utilities in root, then just move them (less re-testing)
4. **Smaller diffs** - Directory move PRs are smaller if duplication already eliminated

---

## Revised Strategy: 4-Phase Hybrid Approach

### Phase 0: Light Code Refactoring (BEFORE structure move)
**Goal:** Create shared utilities in root, eliminate obvious duplication
**Duration:** 1 week
**Risk:** LOW (no file moves, just extraction)

### Phase 1: Documentation & Config (Structure)
**Goal:** Clean up docs and configs
**Duration:** 1 week
**Risk:** LOW

### Phase 2: Module Structure + Code Migration (Structure)
**Goal:** Move Python scripts to src/, including utilities from Phase 0
**Duration:** 2 weeks
**Risk:** MEDIUM

### Phase 3: Additional Code Polish (AFTER structure move)
**Goal:** Database helpers, logging, type hints
**Duration:** 1-2 weeks (optional)
**Risk:** LOW

**Total:** 5-6 weeks

---

## Phase 0: Light Code Refactoring (NEW!)

### What to Refactor in Root (Before Moving)

#### 0.1 Create units.py
**Why now?** Used by 5 scripts - easier to move 1 file than update imports 5 times

```bash
# Create units.py in root
cat > units.py << 'EOF'
"""Unit conversion utilities."""

def kmh_to_knots(kmh):
    if kmh is None:
        return None
    try:
        return round(float(kmh) * 0.539957, 2)
    except (TypeError, ValueError):
        return None

def ms_to_kmh(ms):
    if ms is None:
        return None
    try:
        return round(float(ms) * 3.6, 2)
    except (TypeError, ValueError):
        return None
EOF

# Update 5 scripts to import from units instead of defining locally
# - sqlite_to_json.py
# - influx_to_mqtt.py
# - export_24hr_timeseries.py
# - fetch_noaa_buoy.py
# - fetch_surrey_wave_v2.py
```

**Result:**
- 1 new file (units.py)
- 5 scripts simplified
- ~25 lines of duplication eliminated
- Easier to move (1 file to src/core/ instead of updating 5 imports)

---

#### 0.2 Create directions.py
**Why now?** Used by 2 scripts, useful for others

```bash
cat > directions.py << 'EOF'
"""Direction conversion utilities."""
import math

DIRS_16 = ['N','NNE','NE','ENE','E','ESE','SE','SSE',
           'S','SSW','SW','WSW','W','WNW','NW','NNW']

def degrees_to_cardinal(deg):
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

def parse_direction(val):
    if not val or val in ['MM', 'M', 'NA', '']:
        return None
    try:
        return float(val) % 360
    except (TypeError, ValueError):
        pass
    # Could be cardinal
    return None  # Add cardinal_to_degrees if needed
EOF

# Update 2 scripts
# - sqlite_to_json.py
# - influx_to_mqtt.py
```

**Result:**
- 1 new file (directions.py)
- 2 scripts simplified
- ~20 lines eliminated

---

#### 0.3 Update Scripts to Use stations.py
**Why now?** stations.py already exists! 3 scripts have hardcoded BUOYS dictionaries

```python
# In sqlite_to_json.py, influx_to_mqtt.py, export_24hr_timeseries.py
# DELETE the hardcoded BUOYS = {...} dictionary

# REPLACE with:
from stations import get_all_buoys
BUOYS = get_all_buoys()
```

**Result:**
- 0 new files (stations.py exists)
- 3 scripts simplified
- ~30 lines eliminated
- Station data now from single source (stations.json)

---

#### 0.4 Create config.py (Database Paths Only)
**Why now?** 11 scripts hardcode database paths - consolidate now

```bash
cat > config.py << 'EOF'
"""Centralized configuration."""
from pathlib import Path

# Database paths
DATA_DIR = Path("~/.local/share").expanduser()
BUOY_DATABASE = DATA_DIR / "buoy_data.sqlite"
TIDE_DATABASE = DATA_DIR / "tide_data.sqlite"
STORM_SURGE_DATABASE = DATA_DIR / "storm_surge_forecast.sqlite"

# Ensure data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)
EOF

# Update 11 scripts to use:
# from config import BUOY_DATABASE as SQLITE_PATH
# from config import TIDE_DATABASE as DB_PATH
```

**Result:**
- 1 new file (config.py)
- 11 scripts simplified
- ~22 lines eliminated (2 lines per script)
- Easy to change database locations later

---

### Phase 0 Summary

**New files created:**
- `units.py` (60 lines)
- `directions.py` (40 lines)
- `config.py` (15 lines)
- Total: ~115 lines

**Lines eliminated from scripts:**
- Duplicated utilities: ~45 lines
- Hardcoded BUOYS: ~30 lines
- Hardcoded paths: ~22 lines
- Total: ~97 lines eliminated

**Net change:** +18 lines, but in 3 shared modules instead of scattered across 11 scripts

**Scripts updated:** 11 out of 17 scripts (65%)

**Test:** Run all scripts, verify they still work with shared utilities

**Commit:**
```bash
git add units.py directions.py config.py
git add <all modified scripts>
git commit -m "refactor: extract shared utilities before structure migration

Create shared utility modules:
- units.py: Unit conversion functions (kmh_to_knots, ms_to_kmh)
- directions.py: Direction utilities (degrees_to_cardinal, parse_direction)
- config.py: Centralized database paths

Update 11 scripts to use shared utilities instead of duplicated code.
Eliminate BUOYS dictionary duplication by using stations.py.

Prepares codebase for directory structure migration (Phase 1-2).
Net: Eliminated ~97 lines of duplication across scripts."
```

---

## Updated Phase 1: Documentation & Config

**Changes from original plan:**
- config.py already created in Phase 0 (just move it)
- Less clutter in root already (3 new utility files, but scripts cleaner)

**Tasks:** Same as original (move docs, move configs, archive old files)

---

## Updated Phase 2: Module Structure + Code Migration

**Easier now because:**
1. **Only 4 shared modules to move** instead of extracting from 11 scripts:
   - `stations.py` → `src/core/stations.py`
   - `units.py` → `src/core/units.py`
   - `directions.py` → `src/core/directions.py`
   - `config.py` → `src/core/config.py`

2. **Scripts already import from shared modules**, so import updates are simpler:
   ```python
   # OLD (after Phase 0)
   from units import kmh_to_knots

   # NEW (after Phase 2)
   from src.core.units import kmh_to_knots
   ```

3. **Less duplication** means smaller diffs in Phase 2 PR

---

## Updated Phase 3: Additional Code Polish

**What's left for Phase 3:**
- Database helper utilities (`src/core/database.py`)
- Logging configuration (`src/core/logging_config.py`)
- Type hints (optional)
- Unit tests (optional)

**Not urgent** - Can be done anytime after Phase 2

---

## Comparison: Original vs Hybrid

| Aspect | Original (Structure First) | Hybrid (Light Code First) |
|--------|---------------------------|---------------------------|
| **Phase 0** | N/A | Extract utilities (1 week) |
| **Phase 1** | Docs/config (1 week) | Docs/config (1 week) |
| **Phase 2** | Module structure (2 weeks) | Move everything (2 weeks) |
| **Phase 3** | Code migration (2 weeks) | Optional polish (1 week) |
| **Total** | 5 weeks | 5 weeks (same!) |
| **Duplication in Phase 2** | High (extract + move) | Low (just move) |
| **Phase 2 PR size** | Large | Medium |
| **Import updates** | Complex | Simple |
| **Testing effort** | Higher | Lower |

---

## Why Hybrid is Better

### 1. Smaller Phase 2 Diffs
**Original approach:**
- Phase 2 PR has file moves PLUS utility extraction PLUS import updates
- Hard to review, hard to debug if something breaks

**Hybrid approach:**
- Phase 0 PR: Just utility extraction (easy to review)
- Phase 2 PR: Just file moves (easy to review)
- Clear separation of concerns

### 2. Less Import Churn
**Original approach:**
```python
# Initial
def kmh_to_knots(kmh):  # Defined in script
    ...

# After Phase 2 (extract + move together)
from src.core.units import kmh_to_knots

# One big jump
```

**Hybrid approach:**
```python
# Initial
def kmh_to_knots(kmh):  # Defined in script
    ...

# After Phase 0 (extract)
from units import kmh_to_knots

# After Phase 2 (move)
from src.core.units import kmh_to_knots

# Two smaller jumps, easier to debug
```

### 3. Better Testing
**Hybrid approach:**
- Phase 0: Test shared utilities work in root (easy, no PYTHONPATH needed)
- Phase 2: Test file moves (imports update, but logic unchanged)
- Clear point of failure if issues arise

**Original approach:**
- Phase 2: Test extraction + move together (harder to isolate failures)

### 4. Incremental Value
**Hybrid approach:**
- After Phase 0: Already have cleaner code (even without structure move)
- Can pause after Phase 0 if needed
- Each phase delivers value independently

**Original approach:**
- No value until Phase 2 complete (all-or-nothing)

---

## Decision Matrix

| Scenario | Recommended Approach |
|----------|---------------------|
| **Want it done fast** | Hybrid (same timeline, less risk) |
| **Low risk tolerance** | Hybrid (smaller phases, easier to debug) |
| **Want to see progress** | Hybrid (incremental value) |
| **Large team reviewing PRs** | Hybrid (smaller, focused PRs) |
| **Solo developer** | Either works, but Hybrid slightly better |
| **Already have shared utilities** | Original (nothing to extract) |
| **Minimal duplication** | Original (extraction not worth it) |

**For your project:** Hybrid is better (significant duplication exists)

---

## Revised Timeline

### Week 1: Phase 0 - Light Code Refactoring
- Create units.py, directions.py, config.py
- Update 11 scripts to use shared modules
- Eliminate BUOYS duplication
- Test everything still works
- **Deliverable:** Cleaner code, 3 shared utilities, ~97 lines eliminated

### Week 2: Phase 1 - Documentation & Config
- Move all docs to docs/
- Move configs to config/
- Archive old files
- **Deliverable:** Clean root directory (docs gone)

### Weeks 3-4: Phase 2 - Module Structure + Code Migration
- Create src/ structure
- Move 4 shared modules (stations, units, directions, config) to src/core/
- Move 13 pipeline scripts to src/ingestion/, src/export/, src/processing/, src/integration/
- Update imports (simple: add `src.core.` prefix)
- Update cron.txt
- **Deliverable:** Complete structure refactoring

### Week 5 (Optional): Phase 3 - Additional Polish
- Database helpers (database.py)
- Logging config (logging_config.py)
- Type hints
- Unit tests
- **Deliverable:** Production-grade code quality

---

## Next Steps

**If you approve hybrid approach:**

1. I'll start with Phase 0 right now:
   - Create units.py
   - Create directions.py
   - Create config.py (minimal version)
   - Update scripts to use them
   - Test everything
   - Commit & push

2. Then proceed to Phase 1 (docs reorganization)

3. Then Phase 2 (structure migration) - which will now be easier!

**If you prefer original approach:**

1. Skip Phase 0
2. Start directly with Phase 1 (docs)
3. Tackle code + structure together in Phase 2

**Your call - what sounds better to you?** The hybrid approach with Phase 0 first, or stick with the original plan?

