# Three-Phase Refactoring Execution Guide

**Selected Strategy:** Option 3 - Three-Phase Conservative Approach
**Timeline:** 5 weeks (1 week + 2 weeks + 2 weeks)
**Risk Profile:** Low → Medium → High (incremental)
**Downtime Acceptable:** Yes (1-3 hours for Phase 3)

---

## Overview

| Phase | Focus | Duration | Risk | Downtime |
|-------|-------|----------|------|----------|
| **1** | Documentation & Configuration | 1 week | LOW | None |
| **2** | Module Structure + Shared Code | 2 weeks | MEDIUM | None |
| **3** | All Pipeline Scripts Migration | 2 weeks | HIGH | 1-3 hours |

**Total:** 5 weeks from start to production deployment

---

## Pre-Flight Checklist

Before starting Phase 1, ensure:

- [ ] This execution guide reviewed and approved
- [ ] Git branch created: `refactor/three-phase-restructure`
- [ ] Backup of production databases taken
- [ ] Current cron.txt backed up
- [ ] Stakeholders notified of upcoming changes
- [ ] Test environment available (or acceptable to test in prod with downtime)
- [ ] Rollback plan documented (can revert git commit + restore cron.txt)

---

# Phase 1: Documentation & Configuration

**Goal:** Clean up root directory of all non-code files
**Duration:** 1 week
**Risk:** LOW (no code execution changes)
**Downtime:** None

## Phase 1 Tasks

### 1.1 Create New Directory Structure

```bash
# Create all new directories
mkdir -p docs/{project,architecture,deployment,operations,integrations,frontend,development}
mkdir -p config/sr3
mkdir -p tools/{migration/archive,diagnostics,deployment}
mkdir -p archive/backups
mkdir -p src/{core,ingestion,processing,export,integration/surrey,validation}

# Create __init__.py files (prepare for Phase 2)
find src -type d -exec touch {}/__init__.py \;
```

**Validation:**
```bash
# Check directory structure created
ls -la docs/
ls -la config/
ls -la tools/
ls -la archive/
ls -la src/
```

### 1.2 Move Documentation Files

```bash
# Project-level documentation
git mv CLAUDE.md docs/project/
git mv TODO.md docs/project/

# Frontend documentation
git mv MARINE_FORECAST_FRONTEND_SUMMARY.md docs/frontend/
git mv FIREFOX_ARROW_BUG.md docs/frontend/

# Integration documentation
git mv SURREY_INTEGRATION_GUIDE.md docs/integrations/
git mv SURREY_DEPLOYMENT.md docs/integrations/
git mv SURREY_FRONTEND_GUIDE.md docs/integrations/

# Deployment documentation
git mv cron.txt docs/deployment/

# Keep refactoring docs in root for now (will move after completion)
# REFACTORING_PLAN.md - stays in root
# REFACTORING_EXECUTION_GUIDE.md - stays in root
```

**Validation:**
```bash
# Check old files gone from root
ls -la *.md | grep -v -E "(README|REFACTORING)"
# Should return nothing (or only README.md, REFACTORING_*.md)

# Check new locations
ls -la docs/project/
ls -la docs/frontend/
ls -la docs/integrations/
ls -la docs/deployment/
```

### 1.3 Move Configuration Files

```bash
# Move station registry
git mv stations.json config/

# Move sr3 config
git mv marine_forecast.conf config/sr3/
```

**Note:** Scripts still reference old paths. We'll update in Phase 3.

**Validation:**
```bash
ls -la config/
ls -la config/sr3/
```

### 1.4 Archive Old Files

```bash
# Archive completed migration
git mv stations_migration tools/migration/archive/stations_migration_2025-11

# Archive backup files
git mv CLAUDE.md.backup-2025-11-05 archive/backups/

# Archive old artifacts
git mv "surrey wave integration.zip" archive/

# Move deployment script to tools
git mv deploy_surrey_integration.sh tools/deployment/
```

**Validation:**
```bash
ls -la archive/
ls -la archive/backups/
ls -la tools/migration/archive/
ls -la tools/deployment/

# Root should now have only Python scripts + requirements.txt + README.md
ls -la | wc -l
# Should be much smaller (< 25 items)
```

### 1.5 Update Documentation Cross-References

Need to update these files to reflect new paths:

1. **docs/project/CLAUDE.md** - Update all file path references
   - Change `COMMANDS.md` → `docs/operations/COMMANDS.md`
   - Change `DEPLOYMENT.md` → `docs/deployment/DEPLOYMENT.md`
   - Change `stations.json` → `config/stations.json`
   - Update "Documentation Structure" section

2. **README.md** - Update documentation links
   - Change links to documentation files

3. **docs/deployment/DEPLOYMENT.md** - Update paths
   - References to cron.txt (now in same directory)
   - References to stations.json

4. **All docs in docs/** - Update cross-references between docs

**Commands:**
```bash
# Search for broken links (manual review)
grep -r "CLAUDE.md" docs/ --include="*.md"
grep -r "stations.json" docs/ --include="*.md"
grep -r "\.\./\.\." docs/ --include="*.md"  # Look for relative paths

# After manual updates, validate no broken links remain
```

### 1.6 Create Documentation Index

Create `docs/README.md`:

```markdown
# Documentation Index

All project documentation is organized here.

## Quick Links

### Getting Started
- [Project Overview](../README.md) - Main project README
- [Claude Instructions](project/CLAUDE.md) - AI assistant guidance
- [TODO List](project/TODO.md) - Current and completed tasks

### For Operators
- [Commands Reference](operations/COMMANDS.md) - Common commands and queries
- [Deployment Guide](deployment/DEPLOYMENT.md) - Cron schedules, configs
- [Troubleshooting](operations/TROUBLESHOOTING.md) - Debugging guide

### For Developers
- [Architecture Details](architecture/ARCHITECTURE_DETAILED.md) - Database schemas, script details
- [Storm Surge Setup](deployment/STORM_SURGE_SETUP.md) - GDSPS integration guide
- [PR Automation](development/PR_AUTOMATION_GUIDE.md) - GitHub automation

### Integrations
- [Surrey Integration Guide](integrations/SURREY_INTEGRATION_GUIDE.md)
- [Surrey Deployment](integrations/SURREY_DEPLOYMENT.md)
- [Surrey Frontend Guide](integrations/SURREY_FRONTEND_GUIDE.md)

### Frontend
- [Marine Forecast Summary](frontend/MARINE_FORECAST_FRONTEND_SUMMARY.md)
- [Firefox Arrow Bug](frontend/FIREFOX_ARROW_BUG.md)

## Directory Structure

- `project/` - Project-level documentation (CLAUDE.md, TODO.md)
- `architecture/` - System design and architecture docs
- `deployment/` - Deployment guides and configs
- `operations/` - Operational guides (commands, troubleshooting)
- `integrations/` - Third-party integration docs
- `frontend/` - Frontend-specific documentation
- `development/` - Developer workflows and tooling
```

**Command:**
```bash
# Create the index (or use Write tool)
cat > docs/README.md << 'EOF'
[paste content above]
EOF
```

### 1.7 Phase 1 Testing & Validation

```bash
# 1. Check git status
git status
# Should show all moves, no deletions

# 2. Verify Python scripts still in root
ls -1 *.py | wc -l
# Should be 17 (all scripts still there)

# 3. Verify new structure
tree docs -L 2
tree config -L 2
tree tools -L 2
tree archive -L 2

# 4. Test that scripts still run (paths not updated yet, but should work)
python3 buoy_to_influx_sqlite.py --help || echo "Script doesn't have --help, that's ok"
python3 -c "import stations; print('stations.py still imports ok')"

# 5. Verify no broken markdown links (manual review)
# Open docs/project/CLAUDE.md and click through links
```

### 1.8 Phase 1 Commit & PR

```bash
# Stage all changes
git add -A

# Commit with detailed message
git commit -m "$(cat <<'EOF'
Phase 1: Reorganize documentation and configuration

Move all documentation to docs/ subdirectories:
- Project docs (CLAUDE.md, TODO.md) → docs/project/
- Frontend docs → docs/frontend/
- Integration docs → docs/integrations/
- Deployment docs (cron.txt) → docs/deployment/

Move configuration to config/:
- stations.json → config/
- marine_forecast.conf → config/sr3/

Archive old files:
- stations_migration/ → tools/migration/archive/
- Backup files → archive/backups/
- Old artifacts → archive/

Create organized directory structure:
- docs/ with 7 subdirectories for different doc types
- config/ for all configuration files
- tools/ for utilities and migrations
- archive/ for deprecated code
- src/ structure (empty, prepared for Phase 2)

Update documentation cross-references to reflect new paths.

No functional changes - all Python scripts remain in root and unchanged.
Root directory reduced from 33 to ~20 files.

Phase 1 of 3-phase refactoring (see REFACTORING_EXECUTION_GUIDE.md)
EOF
)"

# Push to remote
git push origin refactor/three-phase-restructure

# Create PR (if using GitHub workflow)
gh pr create --title "Phase 1: Reorganize documentation and configuration" \
  --body "First phase of three-phase refactoring. Moves all docs and configs to organized structure. No functional changes to code."
```

### 1.9 Phase 1 Sign-Off

Before proceeding to Phase 2:

- [ ] All documentation files moved successfully
- [ ] All configuration files moved successfully
- [ ] Archive created with old files
- [ ] Documentation cross-references updated
- [ ] No broken links in documentation
- [ ] Python scripts still run (import checks pass)
- [ ] Git commit shows moves (not deletes + adds)
- [ ] PR reviewed and approved (if applicable)
- [ ] Changes merged to main branch (if applicable)

**Phase 1 Complete! ✅**

Root directory now clean of documentation clutter. Ready for Phase 2.

---

# Phase 2: Module Structure + Shared Code

**Goal:** Create Python module structure and migrate shared utilities
**Duration:** 2 weeks
**Risk:** MEDIUM (changes imports across multiple scripts)
**Downtime:** None (if careful)

## Phase 2 Overview

This phase establishes the `src/` module structure and migrates `stations.py` (used by ~8 scripts). This validates our approach before the big Phase 3 migration.

## Phase 2 Tasks

### 2.1 Install Project as Editable Package

To make imports work cleanly, install the project in editable mode.

**Option A: Simple PYTHONPATH (Recommended for cron)**

Create `src/__init__.py` and add to PYTHONPATH:

```bash
# In each cron job or script wrapper, add:
export PYTHONPATH="/home/keelando/envcan_wave:$PYTHONPATH"
```

**Option B: Install as editable package**

Create `setup.py`:

```python
from setuptools import setup, find_packages

setup(
    name="marine-weather-pipeline",
    version="2.0.0",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        # Copy from requirements.txt
    ],
)
```

Install:
```bash
pip install -e .
```

**Decision:** For simplicity with cron, use **Option A** (PYTHONPATH). We can upgrade to Option B later.

### 2.2 Move stations.py to src/core/

```bash
# Move stations.py
git mv stations.py src/core/stations.py

# Verify __init__.py exists
ls -la src/core/__init__.py
```

### 2.3 Update All Imports of stations.py

Scripts that import `stations`:

1. `buoy_to_influx_sqlite.py`
2. `fetch_noaa_buoy.py`
3. `sqlite_to_json.py`
4. `export_24hr_timeseries.py`
5. `fetch_storm_surge.py`
6. `influx_to_mqtt.py`
7. `export_tide_json.py`
8. `export_combined_water_level.py`
9. `validate_stations.py` (will move separately)

**For each script**, change:
```python
# OLD
from stations import get_all_buoys, get_tide_station, BUOYS

# NEW
from src.core.stations import get_all_buoys, get_tide_station, BUOYS
```

**Commands:**
```bash
# Find all files importing stations
grep -l "from stations import\|import stations" *.py

# For each file, edit imports
# (Use Edit tool or manual editing)
```

**Example edit for buoy_to_influx_sqlite.py:**

```bash
# Before
from stations import get_all_buoys

# After
from src.core.stations import get_all_buoys
```

### 2.4 Move validate_stations.py

```bash
# Move to validation module
git mv validate_stations.py src/validation/stations.py

# Update its own import
# Edit src/validation/stations.py:
# OLD: from stations import ...
# NEW: from src.core.stations import ...
```

### 2.5 Update config/stations.json Path References

Some scripts may hard-code the path to `stations.json`. Update to:

```python
# OLD
STATIONS_PATH = "stations.json"

# NEW
STATIONS_PATH = "config/stations.json"
```

**Check which scripts reference stations.json:**
```bash
grep -n "stations.json" *.py
```

**Likely candidates:**
- `src/core/stations.py` itself
- `src/validation/stations.py`

**Edit src/core/stations.py:**

```python
# Find the line that loads stations.json
# Change from:
with open("stations.json") as f:

# To:
import os
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STATIONS_JSON = os.path.join(REPO_ROOT, "config", "stations.json")
with open(STATIONS_JSON) as f:
```

### 2.6 Phase 2 Testing (Critical!)

**Test each script individually:**

```bash
# Set PYTHONPATH
export PYTHONPATH="/home/keelando/envcan_wave:$PYTHONPATH"

# Test imports work
python3 -c "from src.core.stations import get_all_buoys; print(get_all_buoys())"

# Test each script that uses stations
python3 buoy_to_influx_sqlite.py  # Should run without import errors
python3 fetch_noaa_buoy.py
python3 sqlite_to_json.py
python3 export_24hr_timeseries.py
python3 influx_to_mqtt.py
python3 export_tide_json.py
python3 export_combined_water_level.py
python3 fetch_storm_surge.py

# Test validation script
python3 src/validation/stations.py
```

**If any errors:**
- Check PYTHONPATH is set
- Verify import statements updated correctly
- Check config/stations.json path in stations.py

### 2.7 Update Cron Jobs (Add PYTHONPATH)

Edit `docs/deployment/cron.txt` to add PYTHONPATH to each job:

```bash
# Before
* * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/buoy_to_influx_sqlite.py >> /home/keelando/envcan_wave/parser.log 2>&1

# After
* * * * * export PYTHONPATH="/home/keelando/envcan_wave:$PYTHONPATH" && /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/buoy_to_influx_sqlite.py >> /home/keelando/envcan_wave/parser.log 2>&1
```

**Or create wrapper scripts:**

```bash
# Create scripts/wrapper.sh
#!/bin/bash
export PYTHONPATH="/home/keelando/envcan_wave:$PYTHONPATH"
source /home/keelando/envcan_wave/.venv/bin/activate
exec "$@"

# In cron:
* * * * * /home/keelando/envcan_wave/scripts/wrapper.sh python3 /home/keelando/envcan_wave/buoy_to_influx_sqlite.py >> /home/keelando/envcan_wave/parser.log 2>&1
```

**Decision:** For Phase 2, **don't update production cron yet**. Just document the change needed for Phase 3.

### 2.8 Phase 2 Commit & PR

```bash
git add -A

git commit -m "$(cat <<'EOF'
Phase 2: Create module structure and migrate shared code

Create src/ module structure:
- src/core/ for shared utilities
- src/validation/ for validation tools
- All packages have __init__.py files

Move shared modules:
- stations.py → src/core/stations.py
- validate_stations.py → src/validation/stations.py

Update imports in 8 scripts:
- buoy_to_influx_sqlite.py
- fetch_noaa_buoy.py
- sqlite_to_json.py
- export_24hr_timeseries.py
- fetch_storm_surge.py
- influx_to_mqtt.py
- export_tide_json.py
- export_combined_water_level.py

Update config file path:
- src/core/stations.py now loads from config/stations.json

All scripts tested and working with new import structure.
Requires PYTHONPATH="/path/to/repo:$PYTHONPATH" in environment.

Phase 2 of 3-phase refactoring.
EOF
)"

git push origin refactor/three-phase-restructure

gh pr create --title "Phase 2: Create module structure and migrate shared code" \
  --body "Second phase establishes src/ structure and migrates stations.py. All imports updated. Tested and working."
```

### 2.9 Phase 2 Sign-Off

Before proceeding to Phase 3:

- [ ] src/ structure created with all subdirectories
- [ ] stations.py moved to src/core/
- [ ] validate_stations.py moved to src/validation/
- [ ] All 8 scripts updated with new imports
- [ ] config/stations.json path updated in stations.py
- [ ] PYTHONPATH approach documented
- [ ] All scripts tested manually and working
- [ ] No import errors when running scripts
- [ ] PR reviewed and approved
- [ ] Changes merged to main

**Phase 2 Complete! ✅**

Module structure established and validated. Ready for the big Phase 3 migration.

---

# Phase 3: Pipeline Scripts Migration

**Goal:** Move all remaining Python scripts to src/ and update production cron jobs
**Duration:** 2 weeks (1 week prep/testing, 1 week deployment/monitoring)
**Risk:** HIGH (all pipeline scripts move, production cron changes)
**Downtime:** 1-3 hours acceptable

## Phase 3 Overview

This is the big one. We'll move all 17 Python scripts to their proper locations in src/, update cron.txt with new paths, and deploy to production. Because downtime is acceptable, we can be aggressive with testing.

## Phase 3 Tasks

### 3.1 Pre-Migration Checklist

- [ ] Phase 2 tested and stable in production (if deployed)
- [ ] Production database backup completed
- [ ] Current cron.txt backed up
- [ ] All stakeholders notified of upcoming downtime window
- [ ] Rollback plan documented and ready
- [ ] Test environment available (or production downtime scheduled)

### 3.2 Move All Python Scripts

**Ingestion scripts:**
```bash
git mv buoy_to_influx_sqlite.py src/ingestion/buoy_ec.py
git mv fetch_noaa_buoy.py src/ingestion/buoy_noaa.py
git mv tide_to_sqlite.py src/ingestion/tide_dfo.py
git mv fetch_storm_surge.py src/ingestion/storm_surge_gdsps.py
git mv parse_marine_forecast.py src/ingestion/marine_forecast_ec.py
```

**Processing scripts:**
```bash
git mv calculate_storm_surge_observed.py src/processing/storm_surge_observed.py
```

**Export scripts:**
```bash
git mv sqlite_to_json.py src/export/buoy_latest.py
git mv export_24hr_timeseries.py src/export/buoy_timeseries.py
git mv export_tide_json.py src/export/tide.py
git mv export_hindcast_json.py src/export/hindcast.py
git mv export_combined_water_level.py src/export/combined_water_level.py
```

**Integration scripts:**
```bash
git mv influx_to_mqtt.py src/integration/mqtt_publisher.py

# Surrey integration
git mv fetch_surrey_wave_v2.py src/integration/surrey/fetch_wave.py
git mv update_exports_for_surrey.py src/integration/surrey/update_exports.py
git mv compare_surrey_dfo_water_levels.py src/integration/surrey/compare_water_levels.py
```

**Validation:**
```bash
# Verify root is clean (only requirements.txt, README.md, refactoring docs remain)
ls -la *.py
# Should show: No such file or directory

# Verify all scripts in src/
find src -name "*.py" -type f | grep -v __init__ | wc -l
# Should be 17

# Check structure
tree src -L 2
```

### 3.3 Update Internal Imports (If Any)

Some scripts may import each other. Check for cross-imports:

```bash
# Check if any scripts import other pipeline scripts
grep -h "^import \|^from " src/**/*.py | grep -v "^from src\." | sort -u
```

**Most likely:** No cross-imports (scripts are independent). But if found, update to:

```python
# If buoy_latest.py imported something from buoy_ec.py
from src.ingestion.buoy_ec import some_function
```

### 3.4 Update Configuration File Paths

Scripts may reference config files. Update all references:

```bash
# Search for hardcoded config paths
grep -rn "marine_forecast.conf" src/
grep -rn "stations.json" src/

# Update to:
# config/marine_forecast.conf
# config/stations.json
```

**Most likely locations:**
- `src/ingestion/marine_forecast_ec.py` - may reference marine_forecast.conf
- Already updated `src/core/stations.py` in Phase 2

### 3.5 Create Updated cron.txt

Create `docs/deployment/cron_v2.txt` with all new paths:

```bash
# =============================================================================
# CRON SCHEDULE FOR MARINE WEATHER MONITORING SYSTEM (v2.0 - Refactored)
# =============================================================================

# Export PYTHONPATH for all jobs
PYTHONPATH=/home/keelando/envcan_wave:$PYTHONPATH

# system telemetry over mqtt to haos (unchanged)
*/2 * * * * /home/keelando/.sys-venv/bin/python /home/keelando/sys_stats_mqtt.py

# Parse EC buoy XMLs (every minute)
* * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/ingestion/buoy_ec.py >> /home/keelando/envcan_wave/parser.log 2>&1

# Purge XML files older than 2 days
0 * * * * find /home/keelando/envcan_wave/data/buoy -name "*.xml" -mtime +2 -delete

# Push latest data over MQTT (every minute)
* * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/integration/mqtt_publisher.py >> /home/keelando/envcan_wave/mqtt.log 2>&1

# Export latest snapshot to JSON (every minute)
* * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/export/buoy_latest.py >> /home/keelando/envcan_wave/json_export.log 2>&1

# Export 24h timeseries (every 5 minutes)
*/5 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/export/buoy_timeseries.py >> /home/keelando/envcan_wave/timeseries_export.log 2>&1

# Fetch storm surge forecast (every 6 hours at :30)
30 1,7,13,19 * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/ingestion/storm_surge_gdsps.py >> /home/keelando/envcan_wave/storm_surge.log 2>&1

# Export hindcast data (daily at 14:00 UTC)
0 14 * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/export/hindcast.py >> /home/keelando/envcan_wave/hindcast_export.log 2>&1

# Fetch NOAA buoy data (every 20 minutes at 5, 25, 45)
5,25,45 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/ingestion/buoy_noaa.py >> /home/keelando/envcan_wave/noaa.log 2>&1

# ========== TIDE DATA ==========

# Fetch tide observations (every 30 minutes)
*/30 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/ingestion/tide_dfo.py --observations >> /home/keelando/envcan_wave/tide_obs.log 2>&1

# Fetch tide predictions (daily at 00:10)
10 0 * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/ingestion/tide_dfo.py --predictions >> /home/keelando/envcan_wave/tide_pred.log 2>&1

# Fetch high/low events (daily at 00:15)
15 0 * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/ingestion/tide_dfo.py --highlow >> /home/keelando/envcan_wave/tide_highlow.log 2>&1

# Export tide JSON (every minute)
* * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/export/tide.py >> /home/keelando/envcan_wave/tide_export.log 2>&1

# Calculate observed storm surge (every hour at :05)
5 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/processing/storm_surge_observed.py >> /home/keelando/envcan_wave/tide_offset.log 2>&1

# ========== COMBINED WATER LEVEL ==========

# Export combined water level (10 minutes after storm surge fetch)
40 1,7,13,19 * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/src/export/combined_water_level.py >> /home/keelando/envcan_wave/combined_water_level.log 2>&1

# ========== MAINTENANCE ==========

# Auto-backup git repo (nightly at 23:03)
3 23 * * * cd /home/keelando/envcan_wave && /usr/bin/git add -A && /usr/bin/git diff --staged --quiet || (/usr/bin/git commit -m "Auto-backup $(date +\%Y-\%m-\%d)" && /usr/bin/git push origin main) >> /home/keelando/envcan_wave/git_backup.log 2>&1

# Auto-backup website frontend (nightly at 23:04)
4 23 * * * cd /home/keelando/site && /usr/bin/git add -A && /usr/bin/git diff --staged --quiet || (/usr/bin/git commit -m "Auto-backup $(date +\%Y-\%m-\%d)" && /usr/bin/git push origin main) >> /home/keelando/site/git_backup.log 2>&1

# Purge old logs (daily at 00:00)
0 0 * * * find /home/keelando/envcan_wave -name "*.log" -type f -mtime +7 -delete
```

**Key changes:**
- Added `PYTHONPATH` at top (applies to all cron jobs)
- Updated all script paths from `scriptname.py` → `src/category/scriptname.py`
- Renamed scripts for clarity (e.g., `buoy_ec.py` instead of `buoy_to_influx_sqlite.py`)

### 3.6 Test All Scripts Manually (Pre-Deployment)

```bash
# Set environment
export PYTHONPATH="/home/keelando/envcan_wave:$PYTHONPATH"
cd /home/keelando/envcan_wave
source .venv/bin/activate

# Test each script individually (in order of pipeline)
python3 src/ingestion/buoy_ec.py
python3 src/ingestion/buoy_noaa.py
python3 src/ingestion/tide_dfo.py --observations
python3 src/ingestion/tide_dfo.py --predictions
python3 src/ingestion/tide_dfo.py --highlow
python3 src/ingestion/storm_surge_gdsps.py
python3 src/ingestion/marine_forecast_ec.py

# Test processing
python3 src/processing/storm_surge_observed.py

# Test exports
python3 src/export/buoy_latest.py
python3 src/export/buoy_timeseries.py
python3 src/export/tide.py
python3 src/export/hindcast.py
python3 src/export/combined_water_level.py

# Test integration
python3 src/integration/mqtt_publisher.py

# Test Surrey (if used)
python3 src/integration/surrey/fetch_wave.py
python3 src/integration/surrey/update_exports.py

# Test validation
python3 src/validation/stations.py
```

**For each script:**
- [ ] No import errors
- [ ] No file not found errors (config paths work)
- [ ] Executes successfully (or expected errors documented)
- [ ] Check logs for any warnings

**If any errors:**
- Fix import paths
- Fix config file paths
- Update script and re-test

### 3.7 Validate Output Data

After running all scripts:

```bash
# Check JSON exports created
ls -lh ~/site/data/*.json
ls -lh ~/site/data/tide/*.json
ls -lh ~/site/data/storm_surge/*.json

# Check databases updated
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT COUNT(*) FROM buoy_observation;"
sqlite3 ~/.local/share/tide_data.sqlite "SELECT COUNT(*) FROM tide_observation;"

# Compare with backups (should be same or newer)
diff <(cat ~/site/data/latest_buoy_v2.json | jq -S .) <(cat ~/site/data/latest_buoy_v2.json.backup | jq -S .) || echo "Data differs (expected if new data collected)"
```

### 3.8 Update Documentation

Update all documentation to reflect new structure:

**1. Update docs/project/CLAUDE.md:**
- Section "Key Scripts" → update all paths to src/
- Section "Data Flow Pipeline" → update script names
- Section "Adding a New Buoy" → update script paths
- All command examples with script paths

**2. Update docs/operations/COMMANDS.md:**
- All python3 script examples → src/ paths

**3. Update docs/deployment/DEPLOYMENT.md:**
- Replace all script paths with src/ paths
- Update cron examples

**4. Update README.md:**
- Update "Getting Started" script paths

**5. Create docs/deployment/MIGRATION_NOTES.md:**

```markdown
# Migration Notes: v1.0 → v2.0 (Refactored Structure)

**Date:** 2025-11-XX
**Migration Type:** Three-phase directory restructure

## What Changed

### Directory Structure
- All Python scripts moved from root → `src/` subdirectories
- All documentation moved to `docs/` subdirectories
- Configuration moved to `config/`
- Old migrations archived in `tools/migration/archive/`

### Script Paths
See mapping in REFACTORING_PLAN.md Appendix A

### Cron Jobs
All cron job paths updated. See `docs/deployment/cron_v2.txt` for new paths.

### Import Changes
- `from stations import ...` → `from src.core.stations import ...`
- Requires `PYTHONPATH=/path/to/repo:$PYTHONPATH`

### Configuration Paths
- `stations.json` → `config/stations.json`
- `marine_forecast.conf` → `config/sr3/marine_forecast.conf`

## Rollback Procedure

If critical issues discovered:

1. Restore crontab from backup:
   ```bash
   crontab < ~/envcan_wave/cron.txt.backup
   ```

2. Revert git commit:
   ```bash
   cd /home/keelando/envcan_wave
   git revert HEAD
   git push origin main
   ```

3. Restart sr3:
   ```bash
   sr3 restart
   ```

4. Monitor logs for 30 minutes

## Validation Checklist

After deployment:
- [ ] All cron jobs running (check cron logs)
- [ ] JSON files updating (check timestamps)
- [ ] Database writes working (check row counts)
- [ ] MQTT messages publishing (check HA entities)
- [ ] Website displaying data correctly
- [ ] No errors in logs for 24 hours

## Known Issues

(None expected, but document any discovered during deployment)
```

### 3.9 Phase 3 Commit

```bash
git add -A

git commit -m "$(cat <<'EOF'
Phase 3: Migrate all pipeline scripts to src/ structure

Move all Python scripts to organized src/ structure:

Ingestion (5 scripts):
- buoy_to_influx_sqlite.py → src/ingestion/buoy_ec.py
- fetch_noaa_buoy.py → src/ingestion/buoy_noaa.py
- tide_to_sqlite.py → src/ingestion/tide_dfo.py
- fetch_storm_surge.py → src/ingestion/storm_surge_gdsps.py
- parse_marine_forecast.py → src/ingestion/marine_forecast_ec.py

Processing (1 script):
- calculate_storm_surge_observed.py → src/processing/storm_surge_observed.py

Export (5 scripts):
- sqlite_to_json.py → src/export/buoy_latest.py
- export_24hr_timeseries.py → src/export/buoy_timeseries.py
- export_tide_json.py → src/export/tide.py
- export_hindcast_json.py → src/export/hindcast.py
- export_combined_water_level.py → src/export/combined_water_level.py

Integration (4 scripts):
- influx_to_mqtt.py → src/integration/mqtt_publisher.py
- Surrey scripts → src/integration/surrey/

Create cron_v2.txt with updated paths for all jobs.
Update all documentation with new script paths.
Create migration notes with rollback procedure.

Root directory now contains only:
- README.md
- requirements.txt
- Refactoring documentation
- Directory structure (src/, docs/, config/, tests/, tools/, etc.)

All scripts tested and validated. Ready for production deployment.

BREAKING CHANGE: Requires crontab update (see docs/deployment/cron_v2.txt)

Phase 3 of 3-phase refactoring - COMPLETE.
EOF
)"

git push origin refactor/three-phase-restructure
```

### 3.10 Production Deployment

**Deployment Window:** Schedule 1-3 hour maintenance window

#### Pre-Deployment

```bash
# 1. Backup current crontab
crontab -l > ~/cron.txt.backup.$(date +%Y%m%d)

# 2. Backup production databases
cp ~/.local/share/buoy_data.sqlite ~/.local/share/buoy_data.sqlite.backup.$(date +%Y%m%d)
cp ~/.local/share/tide_data.sqlite ~/.local/share/tide_data.sqlite.backup.$(date +%Y%m%d)

# 3. Backup JSON exports
tar -czf ~/site/data_backup_$(date +%Y%m%d).tar.gz ~/site/data/

# 4. Stop cron (optional, if want clean cutover)
# Not needed if downtime acceptable - just update crontab
```

#### Deployment

```bash
# 1. Pull latest code (assumes Phase 3 merged to main)
cd /home/keelando/envcan_wave
git pull origin main

# 2. Update crontab with new paths
crontab -e
# Replace contents with docs/deployment/cron_v2.txt
# Or:
crontab docs/deployment/cron_v2.txt

# 3. Verify crontab updated
crontab -l | head -20

# 4. Restart sr3 (if marine_forecast.conf path changed)
sr3 restart
sr3 status

# 5. Test one script manually
export PYTHONPATH="/home/keelando/envcan_wave:$PYTHONPATH"
source .venv/bin/activate
python3 src/ingestion/buoy_ec.py
```

#### Post-Deployment Monitoring (First Hour)

```bash
# Watch cron logs (wait for jobs to trigger)
tail -f ~/envcan_wave/parser.log
tail -f ~/envcan_wave/json_export.log
tail -f ~/envcan_wave/mqtt.log

# Check JSON files updating
watch -n 10 'ls -lh ~/site/data/latest_buoy_v2.json'

# Check for errors
grep -i error ~/envcan_wave/*.log | tail -20

# Check database writes
watch -n 30 'sqlite3 ~/.local/share/buoy_data.sqlite "SELECT COUNT(*) FROM buoy_observation;"'
```

#### Validation (First 24 Hours)

- [ ] All cron jobs executed at least once (check logs)
- [ ] JSON exports updating every minute/5 minutes (check timestamps)
- [ ] Database row counts increasing (observations coming in)
- [ ] MQTT messages publishing (check Home Assistant)
- [ ] Website displaying fresh data (check halibur.ca)
- [ ] No import errors in any logs
- [ ] No file not found errors in logs
- [ ] Storm surge forecast updated (next 6-hour window)
- [ ] Tide predictions/high-low updated (next daily window)

#### Rollback (If Needed)

```bash
# 1. Restore old crontab
crontab < ~/cron.txt.backup.YYYYMMDD

# 2. Revert git
cd /home/keelando/envcan_wave
git revert HEAD~3..HEAD  # Revert all 3 phases
git push origin main

# 3. Pull reverted code
git pull origin main

# 4. Restart sr3
sr3 restart

# 5. Monitor for 30 minutes
tail -f ~/envcan_wave/*.log
```

### 3.11 Phase 3 Sign-Off

After 24 hours of production stability:

- [ ] All scripts running successfully for 24 hours
- [ ] No errors in logs
- [ ] Data pipeline functioning normally
- [ ] Website displaying correct data
- [ ] JSON exports validated
- [ ] Database writes confirmed
- [ ] MQTT integration working
- [ ] Cron jobs executing on schedule
- [ ] Documentation updated
- [ ] Migration notes created
- [ ] Rollback procedure documented (but not needed)

**Phase 3 Complete! ✅**

**THREE-PHASE REFACTORING COMPLETE! 🎉**

---

# Post-Refactoring Tasks

After all 3 phases complete and stable:

### 1. Clean Up Refactoring Docs

```bash
# Move refactoring docs to archive
git mv REFACTORING_PLAN.md docs/project/archive/
git mv REFACTORING_EXECUTION_GUIDE.md docs/project/archive/
git commit -m "docs: archive refactoring plans (completed)"
```

### 2. Update TODO.md

Add to docs/project/TODO.md:

```markdown
## Completed (2025-11-XX)

✅ **Three-Phase Directory Refactoring**
  - Phase 1: Reorganized documentation and configuration
  - Phase 2: Created module structure, migrated shared code
  - Phase 3: Migrated all pipeline scripts to src/
  - Root directory reduced from 33 to ~5 files
  - Established scalable structure for future growth
  - All cron jobs updated and validated
  - 24-hour production stability confirmed
```

### 3. Create Git Tag

```bash
git tag -a v2.0.0 -m "Major refactoring: organized directory structure"
git push origin v2.0.0
```

### 4. Update README.md

Add "Recent Changes" section:

```markdown
## Recent Changes

**v2.0.0 (November 2025)** - Major directory structure refactoring
- Organized all scripts into `src/` with functional grouping
- Centralized documentation in `docs/` subdirectories
- Moved configuration to `config/`
- Root directory significantly cleaned up
- Improved discoverability and maintainability
```

### 5. Consider Future Improvements

Now that structure is clean, consider:
- [ ] Add unit tests (easier with modular code)
- [ ] Set up CI/CD pipeline
- [ ] Add type hints throughout
- [ ] Create developer onboarding guide
- [ ] Implement centralized logging
- [ ] Convert to installable package (setup.py)

---

# Summary

## What Was Accomplished

- **33 files in root** → **~5 files in root** (83% reduction)
- **17 scattered Python scripts** → **Organized into 5 functional categories**
- **22 documentation files** → **Organized into 7 subdirectories**
- **Zero-downtime** (Phases 1-2) + **1-3 hour maintenance window** (Phase 3)
- **No data loss**, **No functionality regressions**

## Final Directory Structure

```
surf-website/
├── README.md
├── requirements.txt
├── src/               # All production code (17 scripts)
│   ├── core/          # Shared utilities (1)
│   ├── ingestion/     # Data fetching (5)
│   ├── processing/    # Calculations (1)
│   ├── export/        # JSON exporters (5)
│   ├── integration/   # MQTT, Surrey (4+)
│   └── validation/    # Validators (1)
├── config/            # All configuration
│   ├── stations.json
│   └── sr3/
├── docs/              # All documentation (organized)
│   ├── project/
│   ├── architecture/
│   ├── deployment/
│   ├── operations/
│   ├── integrations/
│   ├── frontend/
│   └── development/
├── tests/             # Testing infrastructure
├── tools/             # Developer utilities
├── archive/           # Deprecated code
└── examples/          # Sample data
```

## Key Metrics

- **Phases Completed:** 3/3
- **Scripts Migrated:** 17/17
- **Cron Jobs Updated:** ~15
- **Documentation Files Organized:** 22
- **Production Downtime:** <3 hours (Phase 3 deployment)
- **Data Loss:** 0
- **Bugs Introduced:** 0 (if testing thorough)

---

**Refactoring Complete! The data pipeline is now organized for sustainable growth. 🚀**
