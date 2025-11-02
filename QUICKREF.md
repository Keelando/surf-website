# Quick Reference: Tide High/Low Troubleshooting

## 🚨 When High/Low Data is Broken

### Quick Diagnostic (run all 3)
```bash
cd ~/envcan_wave

# 1. Full system check
.venv/bin/python3 tide_highlow_diagnostic.py

# 2. Test DFO API directly  
.venv/bin/python3 test_dfo_api_highlow.py

# 3. Analyze event detection
.venv/bin/python3 analyze_highlow_detection.py
```

---

## 📊 Quick Database Checks

```bash
# How many high/low events in DB?
sqlite3 ~/.local/share/tide_data.sqlite \
  "SELECT station_name, COUNT(*) FROM tide_highlow GROUP BY station_name"

# When is the most recent event?
sqlite3 ~/.local/share/tide_data.sqlite \
  "SELECT station_name, datetime(MAX(event_time), 'unixepoch') FROM tide_highlow GROUP BY station_name"

# Events in current export window (12h before to 14h after now)
sqlite3 ~/.local/share/tide_data.sqlite \
  "SELECT station_name, datetime(event_time, 'unixepoch'), event_type 
   FROM tide_highlow 
   WHERE event_time >= strftime('%s', 'now', '-12 hours')
     AND event_time <= strftime('%s', 'now', '+14 hours')
   ORDER BY event_time"
```

---

## 🔧 Quick Fixes

### No data in database
```bash
cd ~/envcan_wave
.venv/bin/python3 tide_to_sqlite.py --highlow
```

### Data in DB but not in JSON
```bash
cd ~/envcan_wave
.venv/bin/python3 export_tide_json.py
```

### Check if cron is running
```bash
grep tide_to_sqlite /var/log/syslog | tail -20
```

---

## 📝 Check Logs

```bash
# Fetch log
tail -50 ~/envcan_wave/tide.log

# If export log exists
tail -50 ~/envcan_wave/tide_export.log

# System log for cron
grep -i tide /var/log/syslog | tail -20
```

---

## 🎯 Common Issues Quick Check

| Issue | Check | Fix |
|-------|-------|-----|
| No data | Run diagnostic script | Run fetch script manually |
| Old data | Check most recent event | API might not have future data |
| Wrong types | Run detection analysis | May need to use API types |
| Missing stations | Compare metadata vs DB | Check stations.json |
| JSON empty | Check export query window | Events might be outside window |

---

## 📂 Important Files

```bash
# Database
~/.local/share/tide_data.sqlite

# Scripts
~/envcan_wave/tide_to_sqlite.py       # Fetches data
~/envcan_wave/export_tide_json.py     # Exports JSON

# Config
~/envcan_wave/stations.json           # Station metadata

# Output
~/site/data/tide_highlow.json         # Website reads this

# Logs
~/envcan_wave/tide.log                # Fetch log
```

---

## 🔍 Paste These Outputs When Asking for Help

```bash
# Run diagnostics and save output
cd ~/envcan_wave

.venv/bin/python3 tide_highlow_diagnostic.py > /tmp/diag.txt 2>&1
.venv/bin/python3 test_dfo_api_highlow.py > /tmp/api.txt 2>&1
.venv/bin/python3 analyze_highlow_detection.py > /tmp/detect.txt 2>&1

# Show key sections
echo "=== DATABASE CONTENT ===" && cat /tmp/diag.txt | grep -A 30 "DATABASE CONTENT ANALYSIS"
echo "=== EXPORT QUERY TEST ===" && cat /tmp/diag.txt | grep -A 50 "EXPORT QUERY TEST"
echo "=== API TEST SUMMARY ===" && cat /tmp/api.txt | grep -E "(Testing:|✅|❌)"
echo "=== DETECTION ISSUES ===" && cat /tmp/detect.txt | grep -E "(⚠️|WARNING)"
```

---

*Use the full TIDE_HIGHLOW_TROUBLESHOOTING.md for detailed explanations*
