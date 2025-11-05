# Troubleshooting Guide

Common issues and debugging steps for the marine weather monitoring system.

## Quick Diagnostics

### Health Check Script

```bash
# Run comprehensive health check
cd ~/envcan_wave
source .venv/bin/activate

echo "=== sr3 Status ==="
sr3 status

echo -e "\n=== Latest XML Files ==="
ls -lt ~/envcan_wave/data/buoy/*.xml 2>/dev/null | head -3
ls -lt ~/envcan_wave/data/marine_forecast/*.xml 2>/dev/null | head -3

echo -e "\n=== Database Status ==="
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT buoy_id, datetime(MAX(observation_time), 'unixepoch'), (strftime('%s','now') - MAX(observation_time))/3600.0 as hours_ago FROM buoy_observation GROUP BY buoy_id;"

echo -e "\n=== JSON Exports ==="
ls -lh ~/site/data/latest_buoy_v2.json ~/site/data/tide-latest.json ~/site/data/marine_forecast.json

echo -e "\n=== Recent Errors in Logs ==="
tail -100 ~/envcan_wave/*.log | grep -i error | tail -10
```

---

## Common Issues

### 1. sr3 Not Running

**Symptoms:**
- No new XML files in `~/envcan_wave/data/buoy/` or `~/envcan_wave/data/marine_forecast/`
- Buoy data becomes stale (more than 2 hours old)

**Diagnosis:**
```bash
# Check if sr3 processes are running
sr3 status
ps aux | grep sr3

# Check when last XML was downloaded
ls -lt ~/envcan_wave/data/buoy/*.xml | head -1
ls -lt ~/envcan_wave/data/marine_forecast/*.xml | head -1
```

**Fix:**
```bash
# Start sr3 subscriptions
sr3 start subscribe/bc_buoys
sr3 start subscribe/marine_forecast

# Verify they're running
sr3 status

# Check logs for errors
tail -50 ~/.cache/sr3/log/subscribe_bc_buoys_*.log
tail -50 ~/.cache/sr3/log/subscribe_marine_forecast_*.log
```

**Prevent:**
- Create systemd service for sr3 (see `DEPLOYMENT.md`)
- Add monitoring cron job to alert if sr3 stops

---

### 2. Stale Data on Website

**Symptoms:**
- Website shows "Last updated: X hours ago" with large X
- No recent observations in database

**Diagnosis:**
```bash
# Check cron jobs are running
crontab -l

# Check if scripts can run manually
cd ~/envcan_wave
source .venv/bin/activate
python3 buoy_to_influx_sqlite.py
python3 sqlite_to_json.py

# Check logs for errors
tail -50 ~/envcan_wave/buoy_sqlite.log
tail -50 ~/envcan_wave/noaa.log
```

**Common causes:**
1. **sr3 stopped** → No new XMLs → Scripts parse old data
2. **Cron not running** → Scripts don't execute on schedule
3. **Script errors** → Check logs for exceptions
4. **Database locked** → SQLite lock from concurrent access
5. **Disk full** → Can't write to database or logs

**Fix based on cause:**

**If sr3 stopped:**
```bash
sr3 start subscribe/bc_buoys
sr3 start subscribe/marine_forecast
```

**If cron not running:**
```bash
sudo systemctl status cron
sudo systemctl start cron
```

**If script errors:**
```bash
# Check logs for Python tracebacks
tail -100 ~/envcan_wave/*.log | grep -A 10 "Traceback"

# Run script manually to see error
python3 buoy_to_influx_sqlite.py
```

**If database locked:**
```bash
# Check for stale lock
fuser ~/.local/share/buoy_data.sqlite

# Kill processes holding lock (use caution!)
fuser -k ~/.local/share/buoy_data.sqlite

# Or wait for lock to clear (usually <5 seconds)
```

**If disk full:**
```bash
df -h

# Clean old log files
find ~/envcan_wave -name "*.log" -mtime +30 -delete

# Clean old XML files
find ~/envcan_wave/data/buoy -name "*.xml" -mtime +2 -delete
find ~/envcan_wave/data/marine_forecast -name "*.xml" -mtime +2 -delete
```

---

### 3. Missing Spectral Data for EC Buoys

**Symptoms:**
- Swell height/period/direction show as null
- Wind wave height/period/direction show as null
- Only affects EC buoys (4600146, 4600303, 4600304, 4600131)

**Status:** **EXPECTED BEHAVIOR**

EC buoys do NOT provide spectral wave data. Only NOAA stations (46087, 46088) provide swell vs wind wave separation.

**No fix needed** - This is by design.

---

### 4. Wind Direction Shows as Null

**Symptoms:**
- Wind direction field is null despite wind speed data

**Status:** **EXPECTED in some conditions**

**Possible causes:**
1. **Calm winds** - NOAA reports `MM` (missing) when wind < 1 knot
2. **Sensor failure** - Anemometer working but vane broken
3. **Recent station deployment** - Not all sensors operational

**Diagnosis:**
```bash
# Check raw NOAA data
curl -s https://www.ndbc.noaa.gov/data/realtime2/46087.txt | head -10

# Check database
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT datetime(observation_time, 'unixepoch'),
         wind_speed, wind_direction
  FROM buoy_observation
  WHERE buoy_id='46087'
  ORDER BY observation_time DESC
  LIMIT 10;"
```

**Fix:**
- If NOAA source data shows `MM`, this is expected - no fix possible
- If NOAA has data but database doesn't, check parsing logic in `fetch_noaa_buoy.py`

---

### 5. Missing Tide Predictions or High/Low Events

**Symptoms:**
- Tide page shows "No prediction data available"
- High/low tide table is empty

**Diagnosis:**
```bash
# Check if tide database exists
ls -lh ~/.local/share/tide_data.sqlite

# Check tide prediction data
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT COUNT(*) as prediction_count,
         datetime(MIN(prediction_time), 'unixepoch') as oldest,
         datetime(MAX(prediction_time), 'unixepoch') as newest
  FROM tide_prediction;"

# Check high/low data
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT COUNT(*) as highlow_count,
         datetime(MIN(event_time), 'unixepoch') as oldest,
         datetime(MAX(event_time), 'unixepoch') as newest
  FROM tide_highlow;"

# Check logs
tail -50 ~/envcan_wave/tide_pred.log
tail -50 ~/envcan_wave/tide_highlow.log
```

**Common causes:**

1. **Predictions haven't been fetched today**
   - Predictions fetch once daily at 12:05 AM
   - High/low fetch twice daily at 12:10 AM & PM
   - If current time < first fetch, no data yet

2. **Fetch failed due to API error**
   - Check logs for HTTP errors (429 rate limit, 500 server error)
   - DFO API may be temporarily down

3. **Data is stale (>24 hours old)**
   - Predictions use 48-hour rolling window
   - If predictions weren't fetched yesterday, they're outside export window

**Fix:**

**Manually trigger fetch:**
```bash
cd ~/envcan_wave
source .venv/bin/activate

# Fetch predictions
python3 tide_to_sqlite.py --predictions

# Fetch high/low events
python3 tide_to_sqlite.py --highlow

# Export JSONs
python3 export_tide_json.py

# Check if data now appears
cat ~/site/data/tide-hi-low.json | jq .
```

**If API returns errors:**
```bash
# Check DFO API status manually
curl -s "https://api-iwls.dfo-mpo.gc.ca/api/v1/stations/5cebf1de3d0f4a073c4bb94c/data?time-series-code=wlp&from=$(date -u +%Y-%m-%dT%H:%M:%SZ)&to=$(date -u -d '+2 days' +%Y-%m-%dT%H:%M:%SZ)" | jq .

# If API is down, wait and retry later
```

**Prevent:**
- Ensure cron jobs for predictions and high/low are running
- Consider adding redundancy (fetch every 12 hours instead of daily)

---

### 6. "Influx Unavailable" Warnings in Logs

**Symptoms:**
- Logs show "InfluxDB unavailable, writing to SQLite only"

**Status:** **EXPECTED if not using InfluxDB**

Scripts have soft dependency on InfluxDB. If InfluxDB is not running or configured, they gracefully degrade to SQLite-only mode.

**If you don't use InfluxDB:** No fix needed - This is normal

**If you do use InfluxDB:**
```bash
# Check InfluxDB is running
sudo systemctl status influxdb

# Check credentials in config
cat ~/.config/buoy_influx_1.env

# Test connection
curl -I http://192.168.1.98:8086/ping

# Start InfluxDB if stopped
sudo systemctl start influxdb
```

---

### 7. Website Returns 404 or Not Loading

**Symptoms:**
- Visiting http://localhost:8090 returns 404 or connection refused
- Pages load but JSON data fails to load

**Diagnosis:**
```bash
# Check Caddy is running
sudo systemctl status caddy

# Check Caddy logs
sudo journalctl -u caddy -n 50

# Test from command line
curl -I http://localhost:8090
curl http://localhost:8090/data/latest_buoy_v2.json | jq . | head
```

**Common causes:**

1. **Caddy not running**
```bash
sudo systemctl start caddy
sudo systemctl enable caddy  # Start on boot
```

2. **Caddyfile syntax error**
```bash
# Validate Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile

# If errors, fix and reload
sudo caddy reload --config /etc/caddy/Caddyfile
```

3. **Wrong document root**
```bash
# Verify path in Caddyfile
grep "root" /etc/caddy/Caddyfile

# Should show: root * /home/keelando/site
```

4. **File permissions**
```bash
# Ensure Caddy can read site files
sudo chmod 755 /home/keelando/site
sudo chmod 644 /home/keelando/site/index.html
sudo chmod 644 /home/keelando/site/data/*.json
```

---

### 8. Marine Warnings Not Appearing on Website

**Symptoms:**
- DFO/EC issued warning but nothing shows on site

**Diagnosis:**
```bash
# Check if marine forecast JSON has warnings
cat ~/site/data/marine_forecast.json | jq '.locations | .[] | .warnings'

# Check if warnings are active
cat ~/site/data/marine_forecast.json | jq '[.locations | .[] | .warnings | .[] | select(.status == "IN EFFECT")]'

# Check warning banner script loaded
curl -s http://localhost:8090 | grep "warning-banner.js"

# Check browser console for JavaScript errors
# Open browser DevTools (F12) → Console tab
```

**Common causes:**

1. **No warnings in source data**
   - Check Environment Canada website directly
   - Warnings may have been issued for different zone

2. **Marine forecast XML not downloaded**
```bash
# Check latest marine forecast XML
ls -lt ~/envcan_wave/data/marine_forecast/*.xml | head -1

# If old (>6 hours), check sr3
sr3 status | grep marine_forecast
sr3 start subscribe/marine_forecast
```

3. **Parser not extracting warnings**
```bash
# Run parser manually
python3 parse_marine_forecast.py

# Check logs
tail -50 ~/envcan_wave/marine_forecast.log
```

4. **Warning dismissed by user**
```bash
# Check browser localStorage (F12 → Application → Local Storage)
# Key: dismissed_marine_warnings
# Delete key to reset dismissals
```

---

### 9. Charts Not Displaying on Website

**Symptoms:**
- Buoy or tide charts show blank area or spinner indefinitely

**Diagnosis:**
```bash
# Check if timeseries JSON files exist
ls -lh ~/site/data/timeseries_*.json
ls -lh ~/site/data/tide-timeseries.json

# Check JSON structure
cat ~/site/data/timeseries_46087.json | jq '.data | length'

# Check browser console for errors
# F12 → Console → Look for ECharts errors or JSON parse errors
```

**Common causes:**

1. **Timeseries JSON not generated**
```bash
# Generate timeseries
cd ~/envcan_wave
source .venv/bin/activate
python3 export_24hr_timeseries.py
python3 export_tide_json.py
```

2. **ECharts library not loading**
   - Check network tab in browser DevTools
   - Verify CDN links in HTML are valid

3. **Insufficient data points**
   - Charts need minimum 2 data points
   - Check if buoy has been reporting for at least 2 hours

---

### 10. NOAA Pressure Shows as Missing But Should Be Valid

**Symptoms:**
- Pressure field is null despite NOAA source showing data

**IMPORTANT:** Pressure around 999 hPa is VALID (low-pressure systems)

**Check parsing logic:**
```python
# In fetch_noaa_buoy.py, ensure 999 is NOT treated as missing
# Only these are missing: 'MM', 'M', 'NA', '' (empty)

if pres in ['MM', 'M', 'NA', ''] or pres.strip() == '':
    pres = None  # Missing
else:
    pres = float(pres)  # 999 is valid!
```

**If pressure is wrongly treated as missing:**
- Update parsing logic to allow 999
- Valid range: ~950-1050 hPa

---

### 11. Database Locked Errors

**Symptoms:**
- Logs show "database is locked" errors
- Scripts hang indefinitely

**Cause:**
SQLite allows one writer at a time. If multiple scripts try to write simultaneously, later ones wait.

**Quick fix:**
```bash
# Wait 10 seconds for lock to clear (usually <5 sec)
sleep 10

# If still locked, check what's holding it
fuser ~/.local/share/buoy_data.sqlite
```

**Long-term fix:**
- Ensure cron jobs don't overlap
- Add retry logic with timeout to scripts
- Consider using WAL mode:
```bash
sqlite3 ~/.local/share/buoy_data.sqlite "PRAGMA journal_mode=WAL;"
sqlite3 ~/.local/share/tide_data.sqlite "PRAGMA journal_mode=WAL;"
```

---

### 12. Git Auto-Backup Failing

**Symptoms:**
- Daily git commits not appearing
- Log shows authentication errors

**Diagnosis:**
```bash
# Check git backup logs
tail -50 ~/envcan_wave/git_backup.log
tail -50 ~/site/git_backup.log

# Test git push manually
cd ~/envcan_wave
git status
git push
```

**Common causes:**

1. **SSH key not set up for cron**
```bash
# Ensure SSH key is loaded
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa

# Test GitHub connection
ssh -T git@github.com
```

2. **Git credentials expired**
```bash
# Re-authenticate if using HTTPS
git config --global credential.helper store
git push  # Will prompt for credentials
```

3. **No changes to commit**
   - If no files changed, commit fails (this is normal)
   - Cron will succeed on days with changes

---

## Debugging Tools

### Enable Verbose Logging

Add to top of Python scripts for more debug output:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Watch Logs in Real-Time

```bash
# Monitor all logs simultaneously
tail -f ~/envcan_wave/*.log
```

### Interactive SQLite Queries

```bash
# Open SQLite interactive shell
sqlite3 ~/.local/share/buoy_data.sqlite

# Useful commands
.tables                    # List tables
.schema buoy_observation   # Show table schema
.headers on                # Show column headers
.mode column               # Pretty print columns
```

### Test Scripts in Isolation

```bash
# Run with Python's verbose mode
python3 -v buoy_to_influx_sqlite.py

# Run with error tracebacks
python3 -u buoy_to_influx_sqlite.py 2>&1 | tee debug.log
```

---

## When to Ask for Help

If you've tried the above and still have issues:

1. **Gather information:**
   ```bash
   # Collect logs
   tar -czf debug_logs_$(date +%Y%m%d).tar.gz ~/envcan_wave/*.log

   # Collect database stats
   sqlite3 ~/.local/share/buoy_data.sqlite ".schema" > db_schema.txt
   sqlite3 ~/.local/share/buoy_data.sqlite "SELECT COUNT(*) FROM buoy_observation;" >> db_schema.txt
   ```

2. **Document the problem:**
   - What are you trying to do?
   - What actually happens?
   - What error messages do you see?
   - When did it start happening?
   - What changed before it broke?

3. **Check GitHub issues:**
   - Look for similar problems
   - Check closed issues for solutions

---

For command examples, see `COMMANDS.md`.
For deployment configuration, see `DEPLOYMENT.md`.
