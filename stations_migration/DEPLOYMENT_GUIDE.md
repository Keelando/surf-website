# Unified Stations System - Deployment Guide

## 📦 What You're Getting

1. **`stations.json`** - Master metadata file (5 buoys + 8 tide stations)
2. **`stations.py`** - Python utility module for easy access
3. **`validate_stations.py`** - Validation script to check integrity
4. **`migration_guide.py`** - Code examples for updating scripts
5. **`STATIONS_README.md`** - Complete documentation
6. **`deploy_stations.sh`** - Deployment helper script

## 🚀 Deployment Steps

### Step 1: Backup Everything
```bash
cd ~/envcan_wave
mkdir -p backups/$(date +%Y%m%d_%H%M%S)
cp tide_stations.json backups/*/  # If exists
cp *.py backups/*/  # Backup all Python scripts
```

### Step 2: Deploy New Files
```bash
# Copy from wherever Claude saved them (likely ~/claude/outputs/ or similar)
cp /path/to/stations.json ~/envcan_wave/
cp /path/to/stations.py ~/envcan_wave/
cp /path/to/validate_stations.py ~/envcan_wave/
```

### Step 3: Validate Installation
```bash
cd ~/envcan_wave
python3 validate_stations.py
```

Expected output:
```
🔍 Validating stations.json...
======================================================================
✅ JSON syntax is valid

📊 Validating 5 buoy stations...
  ✅ All buoy stations validated

🌊 Validating 8 tide stations...
  ✅ All tide stations validated

📋 Validating metadata...
  ✅ Metadata complete

======================================================================
📊 VALIDATION SUMMARY
======================================================================
Total stations: 13
  • Buoys: 5
  • Tide stations: 8

✅ All validations passed!
```

### Step 4: Test the Module
```bash
cd ~/envcan_wave
python3 stations.py
```

Should display all stations and test lookups.

### Step 5: Update Scripts (One at a Time)

#### A. Update `sqlite_to_json.py`

**Find this section (around line 15):**
```python
BUOYS = {
    "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait"},
    "4600304": {"name": "English Bay", "location": "Vancouver Harbor"},
    "4600131": {"name": "Sentry Shoal", "location": "Northern Strait of Georgia"},
    "46087": {"name": "Neah Bay", "location": "Cape Flattery, WA"}
}
```

**Replace with:**
```python
from stations import get_all_buoys

BUOYS = get_all_buoys()
```

**Test it:**
```bash
python3 sqlite_to_json.py
# Check output JSON matches previous format
cat ~/site/data/latest_buoy_v2.json | jq .
```

#### B. Update `export_24hr_timeseries.py`

**Find this section (around line 13):**
```python
BUOYS = {
    "4600146": {"name": "Halibut Bank", "location": "Off Vancouver"},
    "4600303": {"name": "Southern Georgia Strait", "location": "Southern Strait"},
    "4600304": {"name": "English Bay", "location": "Vancouver Harbor"},
    "4600131": {"name": "Sentry Shoal", "location": "Northern Strait of Georgia"},
    "46087": {"name": "Neah Bay", "location": "Cape Flattery, WA"}
}
```

**Replace with:**
```python
from stations import get_all_buoys

BUOYS = get_all_buoys()
```

**Test it:**
```bash
python3 export_24hr_timeseries.py
cat ~/site/data/buoy_timeseries_24h.json | jq '._meta'
```

#### C. Update `tide_to_sqlite.py`

**Find this section (around line 13-20):**
```python
def load_stations():
    """Load tide stations from external JSON file."""
    if not STATION_FILE.exists():
        raise FileNotFoundError(f"Missing {STATION_FILE}")
    with open(STATION_FILE, "r") as f:
        data = json.load(f)
    return {k: v["id"] for k, v in data.items()}
```

**Replace with:**
```python
from stations import get_all_tides

def load_stations():
    """Load tide stations from unified stations.json."""
    return {k: v["id"] for k, v in get_all_tides().items()}
```

**Also update the STATION_FILE constant (around line 9):**
```python
# OLD:
STATION_FILE = Path("~/envcan_wave/tide_stations.json").expanduser()

# NEW:
STATION_FILE = Path("~/envcan_wave/stations.json").expanduser()
```

**Test it:**
```bash
python3 tide_to_sqlite.py
# Should fetch data normally
```

#### D. Update `export_tide_json.py`

**Find this section (around line 18-27):**
```python
def load_station_metadata():
    """Load station names and metadata from external JSON file."""
    if not STATION_FILE.exists():
        print(f"⚠️  Station file not found: {STATION_FILE}")
        return {}
    
    with open(STATION_FILE, "r") as f:
        return json.load(f)
```

**Replace with:**
```python
from stations import get_all_tides

def load_station_metadata():
    """Load station names and metadata from unified stations.json."""
    return get_all_tides()
```

**Test it:**
```bash
python3 export_tide_json.py
cat ~/site/data/tide_current.json | jq '._meta'
```

### Step 6: Verify Everything Still Works

```bash
# Test buoy data pipeline
python3 buoy_to_influx_sqlite.py
python3 sqlite_to_json.py
python3 export_24hr_timeseries.py

# Test tide data pipeline
python3 tide_to_sqlite.py
python3 export_tide_json.py

# Check output files
ls -lh ~/site/data/*.json
```

### Step 7: Clean Up (Optional)

```bash
# Remove old tide_stations.json (now redundant)
mv ~/envcan_wave/tide_stations.json ~/envcan_wave/backups/
```

### Step 8: Update Cron (If Needed)

Your cron jobs should work unchanged since we only modified internal code, not the script interfaces.

But verify:
```bash
crontab -l | grep envcan
```

## 🔍 Verification Checklist

- [ ] `validate_stations.py` passes with no errors
- [ ] `stations.py` runs and displays all stations
- [ ] `sqlite_to_json.py` produces same output as before
- [ ] `export_24hr_timeseries.py` produces same output
- [ ] `tide_to_sqlite.py` fetches data successfully
- [ ] `export_tide_json.py` produces valid JSON
- [ ] Website still displays buoy data correctly
- [ ] Website still displays tide data correctly
- [ ] All cron jobs run without errors
- [ ] Old `tide_stations.json` backed up

## 🆘 Troubleshooting

### "Module 'stations' not found"
```bash
# Make sure stations.py is in the same directory as your scripts
cd ~/envcan_wave
ls -l stations.py

# Or check your Python path
python3 -c "import sys; print('\n'.join(sys.path))"
```

### "Stations file not found"
```bash
# Verify stations.json exists and path is correct
ls -l ~/envcan_wave/stations.json

# Check the path in stations.py (line 14)
grep "STATIONS_FILE = " ~/envcan_wave/stations.py
```

### "Validation warnings"
```bash
# Run validation to see specific issues
python3 validate_stations.py

# Warnings are usually non-critical
# Errors must be fixed
```

### Scripts produce different output
```bash
# Compare before/after
diff <(jq -S . ~/site/data/latest_buoy_v2.json.backup) \
     <(jq -S . ~/site/data/latest_buoy_v2.json)

# If only metadata changed (timestamps, etc) that's OK
# If actual data is different, check your migration
```

## 📊 Benefits After Migration

1. **Easier Updates**: Add new stations by editing one JSON file
2. **Better Documentation**: All metadata in one place
3. **Consistent Data**: No more copy-paste errors across scripts
4. **More Features**: Can now query by source, type, capabilities
5. **Validation**: Can check data integrity anytime
6. **Future-Proof**: Easy to add more station types (weather, etc.)

## 🎯 Success Criteria

After deployment, you should be able to:

```python
# In any script
from stations import get_buoy, get_all_tides

# Get buoy info
buoy = get_buoy("4600146")
print(f"{buoy['name']}: {buoy['lat']}, {buoy['lon']}")

# Get tide stations with observations
from stations import STATIONS
obs_tides = STATIONS.get_tide_stations(has_observations=True)
print(f"Found {len(obs_tides)} tide stations with real-time data")
```

## 📝 Notes

- Keep backups until you're confident everything works
- Old `tide_stations.json` can be deleted after successful migration
- Consider adding this to your git repo: `git add stations.json stations.py`
- Update frequency info in stations.json is for documentation only (doesn't affect cron)

## 🎓 Further Reading

- `STATIONS_README.md` - Full documentation
- `migration_guide.py` - More code examples
- `stations.py` - Module docstrings and usage examples

---

**Questions?** Check the migration_guide.py for more examples!
