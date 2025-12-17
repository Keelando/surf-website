# Field Name Unification Plan

**Created:** 2025-12-17
**Goal:** Standardize wind direction field names across buoy and wind databases
**Effort:** 1-2 hours
**Impact:** Eliminates frontend bugs, cleaner code

---

## Problem Statement

**Current state:**
- `buoy_data.sqlite` → `buoy_observation.wind_direction` (REAL)
- `wind_data.sqlite` → `wind_observation.wind_direction_deg` (INTEGER)

**Impact:**
- Frontend code needs fallback checks (e.g., stations-map.js:370)
- Risk of bugs when checking wrong field name
- Inconsistent data model
- Confusing for developers

---

## Decision: Which Approach?

### Option A: Rename wind_observation.wind_direction_deg → wind_direction
**Pros:**
- Shorter, cleaner field name
- Buoy database (larger, more fields) stays unchanged
- Simpler SQL (fewer characters)

**Cons:**
- Wind database uses INTEGER, buoy uses REAL (minor type inconsistency)
- Loses semantic clarity (_deg suffix is descriptive)

### Option B: Add wind_direction_deg to buoy_observation
**Pros:**
- Explicit units in field name (_deg = degrees)
- Type consistency (both INTEGER)
- More self-documenting

**Cons:**
- More work (add column, migrate data, drop old column)
- Longer field name
- Buoy database has more changes

---

## RECOMMENDED: Option A

**Rationale:**
- Less work (single column rename)
- Buoy database has 58+ columns already - avoid adding more
- "degrees" is implied for direction fields
- Frontend already uses both field names, so changing one is easier

---

## Migration Steps

### Phase 1: Database Migration (5 minutes)

**Step 1: Backup databases**
```bash
cp ~/.local/share/wind_data.sqlite ~/.local/share/wind_data.sqlite.backup-$(date +%Y%m%d)
cp ~/.local/share/buoy_data.sqlite ~/.local/share/buoy_data.sqlite.backup-$(date +%Y%m%d)
```

**Step 2: Create migration script**
```python
# scripts/migrate_wind_direction_field.py
import sqlite3
from pathlib import Path

WIND_DB = Path("~/.local/share/wind_data.sqlite").expanduser()

def migrate():
    conn = sqlite3.connect(WIND_DB)
    cur = conn.cursor()

    # SQLite doesn't support RENAME COLUMN directly in old versions
    # Need to use ALTER TABLE ... RENAME COLUMN (SQLite 3.25+)

    try:
        cur.execute("ALTER TABLE wind_observation RENAME COLUMN wind_direction_deg TO wind_direction;")
        conn.commit()
        print("✅ Successfully renamed wind_direction_deg → wind_direction")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("⚠️  Column 'wind_direction' already exists - migration already done?")
        else:
            raise
    finally:
        conn.close()

if __name__ == "__main__":
    migrate()
```

**Step 3: Run migration**
```bash
python3 scripts/migrate_wind_direction_field.py
```

**Step 4: Verify migration**
```bash
sqlite3 ~/.local/share/wind_data.sqlite "PRAGMA table_info(wind_observation);" | grep wind_direction
# Should show: wind_direction (not wind_direction_deg)
```

---

### Phase 2: Backend Code Updates (15-20 minutes)

**Files to update:**

#### 1. `scripts/export/export_wind_json.py`
**Line ~30-50:** Query uses `wind_direction_deg`
```python
# OLD:
SELECT wind_speed_kmh, wind_gust_kmh, wind_direction_deg, ...

# NEW:
SELECT wind_speed_kmh, wind_gust_kmh, wind_direction, ...
```

**Line ~80-100:** JSON export field names
```python
# OLD:
"wind_direction_deg": row[2],

# NEW:
"wind_direction": row[2],  # Also rename JSON field for consistency
```

#### 2. `scripts/export/export_wind_24hr_timeseries.py`
**Line ~40-60:** Query uses `wind_direction_deg`
```python
# OLD:
SELECT observation_time, wind_speed_kmh, wind_direction_deg, ...

# NEW:
SELECT observation_time, wind_speed_kmh, wind_direction, ...
```

**Line ~100-120:** JSON field names
```python
# OLD:
"wind_direction_deg": row[2],

# NEW:
"wind_direction": row[2],
```

---

### Phase 3: Frontend Code Updates (20-30 minutes)

**Files to update:**

#### 1. `~/site/assets/js/stations-map.js`
**Line ~370:** Remove fallback check
```javascript
// OLD:
const windDir = station.wind_direction_deg || station.wind_direction;

// NEW:
const windDir = station.wind_direction;
```

#### 2. `~/site/assets/js/main.js`
**Search for:** `wind_direction_deg`
```javascript
// OLD:
windDirection: data.wind_direction_deg

// NEW:
windDirection: data.wind_direction
```

#### 3. `~/site/assets/js/charts.js`
**Search for:** `wind_direction_deg`
```javascript
// OLD:
data.wind_direction_deg

// NEW:
data.wind_direction
```

#### 4. `~/site/wind.html` or `~/site/assets/js/wind.js`
**Search for:** `wind_direction_deg`
```javascript
// Replace all instances with wind_direction
```

---

### Phase 4: Testing (15-20 minutes)

#### Backend Tests

**Test 1: Wind data export**
```bash
# Run export
python3 scripts/export/export_wind_json.py

# Check JSON has wind_direction field
cat ~/site/data/latest_wind.json | jq '.CWGT.wind_direction'
# Should return a number (not null)

# Verify old field is gone
cat ~/site/data/latest_wind.json | jq '.CWGT.wind_direction_deg'
# Should return null
```

**Test 2: Wind timeseries export**
```bash
python3 scripts/export/export_wind_24hr_timeseries.py
cat ~/site/data/wind_24hr_data.json | jq '.CWGT[0].wind_direction'
# Should return a number
```

#### Frontend Tests

**Test 3: Map wind arrows**
1. Open https://halibutbank.ca
2. Check wind station markers on map
3. Verify wind direction arrows display correctly
4. Check popup shows wind direction

**Test 4: Wind page**
1. Open https://halibutbank.ca/winds.html
2. Verify wind direction displays for all stations
3. Check charts show wind direction

**Test 5: Browser console**
```javascript
// Open browser console on map page
// Check for errors related to wind_direction
```

---

### Phase 5: Deployment (5 minutes)

**Commit changes:**
```bash
cd ~/envcan_wave
git add scripts/migrate_wind_direction_field.py
git add scripts/export/export_wind_json.py
git add scripts/export/export_wind_24hr_timeseries.py
git add docs/project/FIELD_UNIFICATION_PLAN.md
git add docs/project/NEXT_SESSION.md

git commit -m "$(cat <<'EOF'
Unify wind direction field names across databases

Database Changes:
- Renamed wind_observation.wind_direction_deg → wind_direction
- Now consistent with buoy_observation.wind_direction
- Created migration script for reproducibility

Backend Changes:
- Updated export_wind_json.py to use wind_direction
- Updated export_wind_24hr_timeseries.py to use wind_direction
- JSON exports now use consistent field name

Impact:
- Eliminates frontend fallback checks
- Simpler, cleaner data model
- Prevents future field name bugs

Migration:
- Backed up databases before changes
- Tested all exports and frontend pages
- No data loss, backward compatible JSON

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
EOF
)"

git push origin main
```

**Frontend deployment:**
```bash
cd ~/site
git add assets/js/stations-map.js
git add assets/js/main.js
git add assets/js/charts.js
# (add any other affected files)

git commit -m "Update wind direction field references after backend unification"
git push origin main
```

---

## Rollback Plan

If something breaks:

**Step 1: Restore database**
```bash
cp ~/.local/share/wind_data.sqlite.backup-$(date +%Y%m%d) ~/.local/share/wind_data.sqlite
```

**Step 2: Revert code**
```bash
cd ~/envcan_wave
git revert HEAD
git push origin main
```

**Step 3: Re-run exports**
```bash
python3 scripts/export/export_wind_json.py
python3 scripts/export/export_wind_24hr_timeseries.py
```

---

## Success Criteria

- [ ] Database migration completed without errors
- [ ] `wind_observation` table has `wind_direction` column (not `wind_direction_deg`)
- [ ] Backend exports produce JSON with `wind_direction` field
- [ ] Frontend map shows wind direction arrows correctly
- [ ] No JavaScript console errors
- [ ] Wind page displays all station data correctly
- [ ] Charts render wind direction properly
- [ ] All changes committed and pushed
- [ ] Documentation updated

---

## Notes

- **Type inconsistency acceptable:** wind_direction is INTEGER in wind DB, REAL in buoy DB
  - SQLite is flexible with types
  - Both represent degrees (0-360)
  - No functional impact

- **JSON field name:** Changed to `wind_direction` for consistency
  - Frontend can update gradually (old field will just be missing)
  - No breaking change if frontend checks for existence

- **Future:** Consider adding type hints and validation to export scripts
