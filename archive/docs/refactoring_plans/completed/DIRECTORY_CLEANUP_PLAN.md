# Directory Cleanup Plan - Phase 1

**Goal:** Organize root directory by moving docs/config to subdirectories
**Risk:** VERY LOW (no code changes, just file moves)
**Time:** 1-2 hours
**Status:** READY TO EXECUTE (when you choose to)

---

## Current State (Baseline)

### Root Directory Files (16 non-code files)

**Documentation (10 markdown files):**
- CLAUDE.md - Project instructions for Claude Code
- CLAUDE.md.backup-2025-11-05 - Old backup file
- CODE_REFACTORING_OPPORTUNITIES.md - Refactoring analysis
- HYBRID_REFACTORING_STRATEGY.md - Refactoring strategy doc
- MARINE_FORECAST_FRONTEND_SUMMARY.md - Frontend documentation
- PHASE0_READY_TO_EXECUTE.md - Phase 0 execution guide (with bug notes)
- README.md - **KEEP IN ROOT** (standard location)
- REFACTORING_EXECUTION_GUIDE.md - Refactoring guide
- REFACTORING_PLAN.md - Main refactoring plan
- SURREY_DEPLOYMENT.md - Surrey integration deployment
- SURREY_FRONTEND_GUIDE.md - Surrey frontend guide
- SURREY_INTEGRATION_GUIDE.md - Surrey integration guide
- TODO.md - Project todo list

**Configuration (3 files):**
- stations.json - Master station registry
- tide_stations.json - Tide station data
- cron.txt - Cron job schedule reference
- requirements.txt - **KEEP IN ROOT** (standard location)

**Other:**
- deploy_surrey_integration.sh - Deployment script

### Existing Directories
- docs/ (exists, has some content)
- examples/ (good, keep as-is)
- scripts/ (good, keep as-is)
- tests/ (good, keep as-is)
- phase0_scaffolding/ (temporary, can clean up later)
- stations_migration/ (old, should archive)
- data/ (local data, keep)

---

## Proposed Structure (After Cleanup)

```
envcan_wave/
├── README.md                          # Keep in root (standard)
├── requirements.txt                   # Keep in root (standard)
├── *.py (17 Python scripts)           # Keep in root (cron uses these paths)
│
├── config/                            # NEW: All configuration
│   ├── stations.json
│   └── tide_stations.json
│
├── docs/                              # Reorganized documentation
│   ├── README.md                      # NEW: Documentation index
│   ├── project/                       # NEW: Project-level docs
│   │   ├── CLAUDE.md
│   │   ├── TODO.md
│   │   ├── CODE_REFACTORING_OPPORTUNITIES.md
│   │   ├── HYBRID_REFACTORING_STRATEGY.md
│   │   ├── PHASE0_READY_TO_EXECUTE.md
│   │   ├── REFACTORING_EXECUTION_GUIDE.md
│   │   └── REFACTORING_PLAN.md
│   ├── deployment/                    # NEW: Deployment docs
│   │   ├── cron.txt
│   │   └── (existing DEPLOYMENT.md, STORM_SURGE_SETUP.md, etc.)
│   ├── integrations/                  # NEW: Integration guides
│   │   ├── SURREY_DEPLOYMENT.md
│   │   ├── SURREY_FRONTEND_GUIDE.md
│   │   └── SURREY_INTEGRATION_GUIDE.md
│   └── frontend/                      # NEW: Frontend docs
│       └── MARINE_FORECAST_FRONTEND_SUMMARY.md
│
├── archive/                           # NEW: Old/obsolete files
│   ├── backups/
│   │   └── CLAUDE.md.backup-2025-11-05
│   └── stations_migration/            # Move entire directory here
│
├── scripts/                           # Existing
│   └── deploy_surrey_integration.sh   # Move here
│
├── examples/                          # Existing (unchanged)
├── tests/                             # Existing (unchanged)
└── phase0_scaffolding/                # Temporary (review/remove after refactor)
```

---

## Execution Steps

### Step 1: Create New Directory Structure

```bash
# Create new subdirectories
mkdir -p config
mkdir -p docs/project
mkdir -p docs/deployment
mkdir -p docs/integrations
mkdir -p docs/frontend
mkdir -p archive/backups

# Verify directories created
ls -la docs/
ls -la archive/
```

---

### Step 2: Move Documentation Files

```bash
# Project documentation
git mv CLAUDE.md docs/project/
git mv TODO.md docs/project/
git mv CODE_REFACTORING_OPPORTUNITIES.md docs/project/
git mv HYBRID_REFACTORING_STRATEGY.md docs/project/
git mv PHASE0_READY_TO_EXECUTE.md docs/project/
git mv REFACTORING_EXECUTION_GUIDE.md docs/project/
git mv REFACTORING_PLAN.md docs/project/

# Deployment documentation
git mv cron.txt docs/deployment/

# Integration documentation
git mv SURREY_DEPLOYMENT.md docs/integrations/
git mv SURREY_FRONTEND_GUIDE.md docs/integrations/
git mv SURREY_INTEGRATION_GUIDE.md docs/integrations/

# Frontend documentation
git mv MARINE_FORECAST_FRONTEND_SUMMARY.md docs/frontend/

# Verify moves
git status
```

---

### Step 3: Move Configuration Files

```bash
# Move config files
git mv stations.json config/
git mv tide_stations.json config/

# Verify
ls -la config/
```

---

### Step 4: Move Scripts to scripts/

```bash
# Move deployment script
git mv deploy_surrey_integration.sh scripts/

# Verify
ls -la scripts/
```

---

### Step 5: Archive Old Files

```bash
# Archive old backup
git mv CLAUDE.md.backup-2025-11-05 archive/backups/

# Archive old migration directory
git mv stations_migration archive/

# Verify
ls -la archive/
```

---

### Step 6: Update Scripts with New Paths

**Files that reference stations.json:**
1. `stations.py` - Change path to `config/stations.json`
2. `validate_stations.py` - Change path to `config/stations.json`

**Changes needed:**

```python
# In stations.py (line ~10)
# OLD:
STATIONS_FILE = Path(__file__).parent / "stations.json"

# NEW:
STATIONS_FILE = Path(__file__).parent / "config" / "stations.json"
```

```python
# In validate_stations.py (line ~10)
# OLD:
STATIONS_FILE = Path(__file__).parent / "stations.json"

# NEW:
STATIONS_FILE = Path(__file__).parent / "config" / "stations.json"
```

**Execute updates:**
```bash
# Edit stations.py
# Edit validate_stations.py

# Test immediately
python3 stations.py
python3 validate_stations.py
```

---

### Step 7: Create Documentation Index

Create `docs/README.md` to help navigate:

```markdown
# Documentation Index

## Project Documentation
- [CLAUDE.md](project/CLAUDE.md) - Instructions for Claude Code
- [TODO.md](project/TODO.md) - Project todo list
- [Refactoring Plan](project/REFACTORING_PLAN.md) - Codebase refactoring plan

## Deployment
- [cron.txt](deployment/cron.txt) - Cron job schedule
- [DEPLOYMENT.md](DEPLOYMENT.md) - Deployment guide
- [STORM_SURGE_SETUP.md](STORM_SURGE_SETUP.md) - Storm surge setup

## Integrations
- [Surrey Integration Guide](integrations/SURREY_INTEGRATION_GUIDE.md)
- [Surrey Deployment](integrations/SURREY_DEPLOYMENT.md)
- [Surrey Frontend](integrations/SURREY_FRONTEND_GUIDE.md)

## Frontend
- [Marine Forecast Frontend](frontend/MARINE_FORECAST_FRONTEND_SUMMARY.md)

## Architecture & Operations
- [ARCHITECTURE_DETAILED.md](ARCHITECTURE_DETAILED.md)
- [COMMANDS.md](COMMANDS.md)
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
```

---

### Step 8: Verify Root Directory is Clean

```bash
# Check what's left in root
ls -1 | grep -v "\.py$" | grep -v "^\.git" | grep -v "requirements.txt" | grep -v "README.md"

# Should only show:
# - config/
# - docs/
# - scripts/
# - tests/
# - examples/
# - archive/
# - phase0_scaffolding/ (temporary)
# - data/ (local data)
```

---

### Step 9: Test Everything Still Works

```bash
# Test scripts can import from stations
python3 -c "from stations import get_all_buoys; print(len(get_all_buoys()))"
# Should print: 6

# Test validation
python3 validate_stations.py
# Should show validation passing

# Test a data script (read-only, safe)
python3 buoy_to_influx_sqlite.py
# Should run without errors

# Test export script
python3 sqlite_to_json.py
# Should generate JSON without errors
```

---

### Step 10: Commit Changes

```bash
git status
# Review all changes

git add -A

git commit -m "Phase 1: Organize directory structure

Move documentation to docs/ subdirectories:
- Project docs → docs/project/
- Deployment docs → docs/deployment/
- Integration docs → docs/integrations/
- Frontend docs → docs/frontend/

Move configuration to config/:
- stations.json → config/
- tide_stations.json → config/

Archive obsolete files:
- Old backups → archive/backups/
- stations_migration/ → archive/

Move scripts to scripts/:
- deploy_surrey_integration.sh → scripts/

Update 2 scripts to reference new config/ path:
- stations.py
- validate_stations.py

Result: Clean root directory with only Python scripts and standard files.
All scripts tested and working."

git push origin main
```

---

## Expected Results

### Before (Root Directory - 33 files)
- 17 Python scripts
- 13 markdown files (cluttered!)
- 3 config files
- 1 shell script
- requirements.txt
- Plus 8 subdirectories

### After (Root Directory - Clean!)
- 17 Python scripts (unchanged locations)
- README.md
- requirements.txt
- 6 subdirectories (organized)

**Lines of code changed:** ~4 lines (just path updates in 2 files)

---

## Rollback Procedure

If something breaks:

```bash
# Undo all git moves
git reset --hard HEAD~1

# Or selective undo
git mv docs/project/CLAUDE.md .
git mv config/stations.json .
# etc.

# Restore original stations.py paths
# (just revert the 2 path changes)
```

---

## Risk Assessment

**Risk Level:** VERY LOW

**Why low risk:**
- No cron job changes (scripts stay in same location)
- Only 2 trivial code changes (path updates)
- Git preserves full history (easy rollback)
- Can test each step before committing
- No database changes
- No pipeline logic changes

**Worst case:** Revert commit, back to current state in 30 seconds

---

## Checklist Before Starting

- [ ] Repository is on main branch and clean (`git status`)
- [ ] All scripts currently working (test one or two)
- [ ] Have 1-2 hours available
- [ ] Not running at 8pm when tired 😉
- [ ] Ready to test after each major step

---

## When to Execute

**Good times:**
- Weekend morning with coffee
- Weekday evening (not late!) when alert
- When you have time to test afterwards

**Bad times:**
- Late at night when tired
- Right before deploying other changes
- When you need to leave soon

---

## Questions Before Starting?

- Do these file moves make sense?
- Any files you want to keep in root?
- Any concerns about the plan?
- Want to adjust any of the destinations?

---

## Branch Management Best Practices

**Problem:** Multiple feature branches can get confusing - which are merged? which are active? what changes are where?

**Solution:** Clean branch hygiene and regular maintenance

### Current Branch Status

```bash
# Check current branches
git branch -a
```

**As of 2025-01-13:**
- `main` - Current, up to date
- `claude/fix-noaa-history-table` - ✅ MERGED (PR #13), safe to delete
- `claude/migrate-to-unified-stations` - ✅ MERGED (PR #12), safe to delete
- `claude/fix-tide-pipeline-bugs` - ❓ Status unknown, check if merged
- `claude/data-pipeline-refactoring-011CV14TyjP3QLD8VH1uWGyF` - ✅ MERGED (PR #10), safe to delete

---

### Branch Management Workflow

#### After Merging a PR:

```bash
# 1. Switch to main
git checkout main

# 2. Pull latest (includes your merged PR)
git pull origin main

# 3. Delete local branch (safe after merge!)
git branch -d claude/branch-name

# 4. Delete remote branch (if not auto-deleted by GitHub)
git push origin --delete claude/branch-name
```

**Do this immediately after merging** to avoid clutter!

---

#### Check Which Branches Are Merged:

```bash
# List branches merged into main
git branch --merged main

# List branches NOT merged into main
git branch --no-merged main

# See branch status with PR numbers
gh pr list --state merged --limit 10
gh pr list --state open
```

---

#### Clean Up Merged Branches (Monthly):

```bash
# See what can be deleted
git branch --merged main | grep -v "^\*" | grep -v "main"

# Delete all local branches merged to main (BE CAREFUL!)
git branch --merged main | grep -v "^\*" | grep -v "main" | xargs -r git branch -d

# Or delete one at a time (safer)
git branch -d claude/fix-noaa-history-table
git branch -d claude/migrate-to-unified-stations
```

---

### Recommended Branch Workflow for This Cleanup

**Option A: Do it on main (SIMPLE)**
- Work directly on main
- No branches to track
- Commit and push when done
- **Use this if:** Changes are straightforward and low-risk

**Option B: Use a feature branch (SAFER)**
```bash
# Create branch for cleanup
git checkout -b cleanup/organize-directory-structure

# Do all the work from DIRECTORY_CLEANUP_PLAN.md

# Commit
git add -A
git commit -m "Phase 1: Organize directory structure"

# Push
git push origin cleanup/organize-directory-structure

# Create PR
gh pr create --title "Phase 1: Organize directory structure" \
  --body "Moves docs to docs/ subdirectories, config to config/, archives old files. No code changes except 2 path updates. Low risk."

# Review PR yourself, then merge
gh pr merge --squash

# Clean up immediately after merge!
git checkout main
git pull origin main
git branch -d cleanup/organize-directory-structure
```

**Use this if:** You want to review changes in GitHub before applying to main

---

### Branch Naming Convention

**Current pattern:** `claude/descriptive-name-randomID`

**Suggested improvement:**
- `cleanup/` - Directory/code cleanup tasks
- `feature/` - New features
- `fix/` - Bug fixes
- `refactor/` - Code refactoring
- `docs/` - Documentation only

**Examples:**
- `cleanup/organize-directory-structure`
- `refactor/extract-shared-utilities`
- `feature/add-wind-database`
- `fix/noaa-pressure-handling`

**Why better:**
- Groups related work
- Easier to see type at a glance
- Can filter by prefix

---

### Quick Reference Commands

```bash
# Where am I?
git branch

# What's not merged yet?
git branch --no-merged main

# What branches exist remotely?
git branch -r

# Full picture (local + remote + status)
git branch -a -vv

# Clean up merged branches
git branch --merged main | grep -v main | xargs -r git branch -d

# See what changed on a branch vs main
git diff main..branch-name

# Check if a branch is merged
git branch --contains branch-name main
```

---

### Checklist After Every PR Merge

- [ ] Verify PR is merged on GitHub
- [ ] `git checkout main`
- [ ] `git pull origin main`
- [ ] Verify your changes are present
- [ ] `git branch -d merged-branch-name`
- [ ] `git push origin --delete merged-branch-name` (if not auto-deleted)
- [ ] Update any tracking documents if needed

**Time investment:** 30 seconds after each merge
**Time saved:** Hours of "which branch had that change?" confusion

---

### Current Cleanup Needed

Based on current branches, you should probably:

```bash
# After confirming these are merged:
git branch -d claude/fix-noaa-history-table
git branch -d claude/migrate-to-unified-stations

# Check status of this one first:
git log --oneline --graph --all | grep -A 5 "fix-tide-pipeline-bugs"
# If merged, delete it too

# The refactoring branch is already deleted locally (good!)
```

---

## Summary

1. ✅ **Directory cleanup plan documented** - Ready to execute when you choose
2. ✅ **Branch management guidance added** - Clean up merged branches regularly
3. ✅ **Step-by-step checklist provided** - Follow along at your own pace
4. ✅ **Risk assessment complete** - Very low risk changes
5. ✅ **Rollback procedure documented** - Easy to undo if needed

**Next steps (your choice when):**
1. Clean up old merged branches (5 minutes)
2. Execute directory cleanup (1-2 hours, daytime)
3. Consider Phase 2 (manual utility extraction) later

**This plan is ready to execute when you are!**

