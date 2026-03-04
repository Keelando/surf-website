# Command Reference

Detailed command examples for working with the marine weather monitoring system.

## Setup Commands

### Initial Setup

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start sr3 to begin downloading Environment Canada XML files
sr3 start subscribe/bc_buoys
sr3 start subscribe/bc_wind_stations
sr3 start subscribe/marine_forecast
```

### Activate Virtual Environment

```bash
source .venv/bin/activate
```

---

## Sarracenia (sr3) Commands

### Check Status

```bash
# Check if sr3 is running
ps aux | grep sr3

# Check sr3 status
sr3 status

# Check specific subscription
sr3 status | grep bc_buoys
sr3 status | grep marine_forecast
```

### Start/Stop Subscriptions

```bash
# Start buoy subscription (as daemon)
sr3 start subscribe/bc_buoys

# Start marine forecast subscription
sr3 start subscribe/marine_forecast

# Run in foreground (for debugging)
sr3 foreground subscribe/bc_buoys

# Stop subscriptions
sr3 stop subscribe/bc_buoys
sr3 stop subscribe/marine_forecast
```

### View Recent Downloads

```bash
# View recent buoy XMLs
ls -lth ~/envcan_wave/data/buoy/*.xml | head

# View recent marine forecast XMLs
ls -lth ~/envcan_wave/data/marine_forecast/*.xml | head

# Count XML files
find ~/envcan_wave/data/buoy -name "*.xml" | wc -l
```

### Monitor sr3 Logs

```bash
# Tail buoy subscription logs
tail -f ~/.cache/sr3/log/subscribe_bc_buoys_*.log

# Tail marine forecast logs
tail -f ~/.cache/sr3/log/subscribe_marine_forecast_*.log

# View all sr3 logs
ls -lth ~/.cache/sr3/log/
```

---

## Manual Script Execution

### Buoy Data Pipeline

```bash
# Activate venv first
source .venv/bin/activate

# Fetch Environment Canada data (parses XMLs in data/buoy/)
python3 buoy_to_influx_sqlite.py

# Fetch NOAA 5-day feeds (meteorological + spectral)
python3 fetch_noaa_buoy.py

# Export latest snapshot to JSON
python3 sqlite_to_json.py

# Export 24-hour timeseries
python3 export_24hr_timeseries.py

# Push to Home Assistant via MQTT
python3 influx_to_mqtt.py
```

### Tide Data Pipeline

```bash
# Fetch tide data - all types
python3 tide_to_sqlite.py --all

# Fetch only observations (real-time water levels)
python3 tide_to_sqlite.py --observations

# Fetch only predictions (astronomical forecasts)
python3 tide_to_sqlite.py --predictions

# Fetch only high/low events
python3 tide_to_sqlite.py --highlow

# Export tide JSON files (latest, timeseries, high/low)
python3 export_tide_json.py
```

### Marine Forecasts

```bash
# Parse marine forecast XMLs
python3 parse_marine_forecast.py
```

### Storm Surge Forecasts

```bash
# Fetch storm surge forecast from GeoMet
python3 fetch_storm_surge.py
```

---

## Database Inspection

### Buoy Database Queries

#### Check Latest Observations Per Buoy

```bash
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT buoy_id,
         datetime(observation_time, 'unixepoch') AS last_obs,
         (strftime('%s','now') - observation_time)/3600.0 AS hours_ago
  FROM buoy_observation
  WHERE observation_time IN (
    SELECT MAX(observation_time) FROM buoy_observation GROUP BY buoy_id
  )
  ORDER BY buoy_id;"
```

#### View Recent Records for Specific Buoy

```bash
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT datetime(observation_time, 'unixepoch') AS time,
         wave_height_sig,
         wind_speed,
         wind_direction,
         air_temp,
         sea_temp
  FROM buoy_observation
  WHERE buoy_id='46087'
  ORDER BY observation_time DESC
  LIMIT 10;"
```

#### Check Spectral Data (NOAA Only)

```bash
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT datetime(observation_time, 'unixepoch') AS time,
         swell_height,
         swell_period,
         swell_direction,
         wind_wave_height,
         wind_wave_period,
         wind_wave_direction
  FROM buoy_observation
  WHERE buoy_id='46087'
    AND swell_height IS NOT NULL
  ORDER BY observation_time DESC
  LIMIT 10;"
```

#### Count Records by Buoy

```bash
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT buoy_id, COUNT(*) as record_count
  FROM buoy_observation
  GROUP BY buoy_id
  ORDER BY buoy_id;"
```

#### Check Database Size

```bash
sqlite3 ~/.local/share/buoy_data.sqlite "
  SELECT
    'buoy_observation' as table_name,
    COUNT(*) as row_count,
    MIN(datetime(observation_time, 'unixepoch')) as oldest_record,
    MAX(datetime(observation_time, 'unixepoch')) as newest_record
  FROM buoy_observation;"
```

### Tide Database Queries

#### Check Table Counts

```bash
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT 'tide_observation' as table_name, COUNT(*) as count
  FROM tide_observation
  UNION ALL
  SELECT 'tide_prediction', COUNT(*)
  FROM tide_prediction
  UNION ALL
  SELECT 'tide_highlow', COUNT(*)
  FROM tide_highlow;"
```

#### Check Latest Observations Per Station

```bash
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT station_name,
         datetime(observation_time, 'unixepoch') AS last_obs,
         water_level,
         quality,
         (strftime('%s','now') - observation_time)/60.0 AS minutes_ago
  FROM tide_observation
  WHERE observation_time IN (
    SELECT MAX(observation_time)
    FROM tide_observation
    GROUP BY station_id
  )
  ORDER BY station_name;"
```

#### Check Today's High/Low Tide Events

```bash
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT station_name,
         datetime(event_time, 'unixepoch', 'localtime') AS event_time,
         event_type,
         water_level
  FROM tide_highlow
  WHERE event_time >= strftime('%s', 'now', 'start of day')
    AND event_time < strftime('%s', 'now', '+1 day', 'start of day')
  ORDER BY station_name, event_time;"
```

#### View Tide Prediction vs Observation for Station

```bash
# Point Atkinson for last 6 hours
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT
    datetime(observation_time, 'unixepoch', 'localtime') AS time,
    'observation' as type,
    water_level
  FROM tide_observation
  WHERE station_name = 'point_atkinson'
    AND observation_time > strftime('%s', 'now', '-6 hours')
  UNION ALL
  SELECT
    datetime(prediction_time, 'unixepoch', 'localtime') AS time,
    'prediction' as type,
    water_level
  FROM tide_prediction
  WHERE station_name = 'point_atkinson'
    AND prediction_time > strftime('%s', 'now', '-6 hours')
  ORDER BY time;"
```

#### Check Prediction/High-Low Staleness

```bash
sqlite3 ~/.local/share/tide_data.sqlite "
  SELECT
    'predictions' as type,
    MAX(datetime(prediction_time, 'unixepoch')) as newest_data,
    (strftime('%s', 'now') - MAX(prediction_time))/3600.0 as hours_old
  FROM tide_prediction
  UNION ALL
  SELECT
    'high/low' as type,
    MAX(datetime(event_time, 'unixepoch')) as newest_data,
    (strftime('%s', 'now') - MAX(event_time))/3600.0 as hours_old
  FROM tide_highlow;"
```

---

## JSON Export Verification

### Check Buoy JSON Exports

```bash
# View latest buoy snapshot
cat site/data/latest_buoy_v2.json | jq .

# Check specific buoy
cat site/data/latest_buoy_v2.json | jq '.["4600146"]'

# List all buoy IDs in export
cat site/data/latest_buoy_v2.json | jq 'keys'

# Check if buoy has data
cat site/data/latest_buoy_v2.json | jq '.["46087"] | has("wave_height_sig")'

# View timeseries for specific buoy
cat site/data/timeseries_46087.json | jq '.data | length'
```

### Check Tide JSON Exports

```bash
# View tide latest snapshot
cat site/data/tide-latest.json | jq .

# Check specific station
cat site/data/tide-latest.json | jq '.point_atkinson'

# View tide timeseries data points
cat site/data/tide-timeseries.json | jq '.point_atkinson.observations | length'
cat site/data/tide-timeseries.json | jq '.point_atkinson.predictions | length'

# View today's high/low tides
cat site/data/tide-hi-low.json | jq '.point_atkinson'
```

### Check Marine Forecast JSON

```bash
# View marine forecast
cat site/data/marine_forecast.json | jq .

# Check active warnings
cat site/data/marine_forecast.json | jq '.locations | to_entries | .[] | select(.value.warnings | length > 0) | {zone: .key, warnings: .value.warnings}'

# List all warnings currently in effect
cat site/data/marine_forecast.json | jq '[.locations | .[] | .warnings | .[] | select(.status == "IN EFFECT") | .type] | unique'
```

---

## Log Monitoring

### View All Project Logs

```bash
# Tail all logs in real-time
tail -f ~/envcan_wave/logs/*.log

# View specific log
tail -f ~/envcan_wave/logs/buoy_sqlite.log
tail -f ~/envcan_wave/logs/noaa.log
tail -f ~/envcan_wave/logs/tide_obs.log
tail -f ~/envcan_wave/logs/tide_pred.log
tail -f ~/envcan_wave/logs/tide_highlow.log
tail -f ~/envcan_wave/logs/marine_forecast.log
```

### List Recent Log Activity

```bash
# Show last 20 lines of all logs
find ~/envcan_wave/logs -name "*.log" -exec echo "=== {} ===" \; -exec tail -20 {} \;

# Check log file sizes
ls -lh ~/envcan_wave/logs/*.log
```

---

## Cron Management

### View Current Cron Schedule

```bash
crontab -l
```

### Edit Cron Schedule

```bash
crontab -e
```

### Test Cron Job Manually

```bash
# Test buoy data fetch (runs every minute in cron)
cd ~/envcan_wave && source .venv/bin/activate && python3 buoy_to_influx_sqlite.py && python3 sqlite_to_json.py && python3 influx_to_mqtt.py

# Test NOAA fetch (runs every 20 min in cron)
cd ~/envcan_wave && source .venv/bin/activate && python3 fetch_noaa_buoy.py

# Test tide observations fetch (runs every 30 min)
cd ~/envcan_wave && source .venv/bin/activate && python3 tide_to_sqlite.py --observations

# Test tide predictions fetch (runs daily)
cd ~/envcan_wave && source .venv/bin/activate && python3 tide_to_sqlite.py --predictions
```

---

## Data Cleanup

### Purge Old XML Files

```bash
# Remove XMLs older than 2 days (this runs hourly via cron)
find ~/envcan_wave/data/buoy -name "*.xml" -mtime +2 -delete
find ~/envcan_wave/data/marine_forecast -name "*.xml" -mtime +2 -delete

# Count remaining XML files
find ~/envcan_wave/data/buoy -name "*.xml" | wc -l
find ~/envcan_wave/data/marine_forecast -name "*.xml" | wc -l
```

### Vacuum SQLite Databases

```bash
# Vacuum buoy database (reclaim space)
sqlite3 ~/.local/share/buoy_data.sqlite "VACUUM;"

# Vacuum tide database
sqlite3 ~/.local/share/tide_data.sqlite "VACUUM;"

# Check database sizes
ls -lh ~/.local/share/*.sqlite
```

---

## Testing & Validation

### Full Pipeline Test

```bash
# Activate environment
source .venv/bin/activate

# 1. Check sr3 is running
sr3 status

# 2. Run buoy pipeline
python3 buoy_to_influx_sqlite.py
python3 fetch_noaa_buoy.py
python3 sqlite_to_json.py

# 3. Run tide pipeline
python3 tide_to_sqlite.py --all
python3 export_tide_json.py

# 4. Run marine forecast
python3 parse_marine_forecast.py

# 5. Verify data in database
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT COUNT(*) FROM buoy_observation;"
sqlite3 ~/.local/share/tide_data.sqlite "SELECT COUNT(*) FROM tide_observation;"

# 6. Verify JSON exports exist
ls -lh site/data/latest_buoy_v2.json
ls -lh site/data/tide-latest.json
ls -lh site/data/marine_forecast.json

# 7. Check for errors in logs
tail -50 ~/envcan_wave/*.log | grep -i error
```

### Validate Station Registry

```bash
# Run station validation script
python3 validate_stations.py

# Check stations.json is valid JSON
cat stations.json | jq . > /dev/null && echo "Valid JSON" || echo "Invalid JSON"

# Count stations
cat stations.json | jq '[.buoys, .tide_stations] | add | length'
```

---

## Git Operations

### Check Status

```bash
cd ~/envcan_wave
git status

cd site/
git status
```

### Commit and Push

```bash
# Backend repo
cd ~/envcan_wave
git add .
git commit -m "Description of changes"
git push

# Frontend repo
cd site/
git add .
git commit -m "Description of changes"
git push
```

### View Recent Commits

```bash
git log --oneline -10
```

---

## Caddy Web Server

### Reload Configuration

```bash
sudo caddy reload --config /etc/caddy/Caddyfile
```

### Check Caddy Status

```bash
sudo systemctl status caddy
```

### View Caddy Logs

```bash
sudo journalctl -u caddy -f
```

### Test Website Access

```bash
# Test from command line
curl -I http://localhost:8090

# Check if JSON files are accessible
curl -s http://localhost:8090/data/latest_buoy_v2.json | jq . | head
```

---

## Troubleshooting Commands

### Check for Stale Data

```bash
# Check if sr3 is running
sr3 status

# Check when last XML was downloaded
ls -lt ~/envcan_wave/data/buoy/*.xml | head -1
ls -lt ~/envcan_wave/data/marine_forecast/*.xml | head -1

# Check when scripts last ran (via logs)
ls -lt ~/envcan_wave/*.log

# Check cron jobs
crontab -l
```

### Restart Everything

```bash
# Stop sr3
sr3 stop subscribe/bc_buoys
sr3 stop subscribe/marine_forecast

# Wait a moment
sleep 5

# Start sr3
sr3 start subscribe/bc_buoys
sr3 start subscribe/marine_forecast

# Verify running
sr3 status
ps aux | grep sr3
```

### Check Disk Space

```bash
# Check overall disk space
df -h

# Check project directories
du -sh ~/envcan_wave
du -sh site/
du -sh ~/.local/share/*.sqlite

# Check XML data directories
du -sh ~/envcan_wave/data/buoy
du -sh ~/envcan_wave/data/marine_forecast
```

---

## Performance Monitoring

### Database Query Performance

```bash
# Explain query plan for latest observation query
sqlite3 ~/.local/share/buoy_data.sqlite "EXPLAIN QUERY PLAN
  SELECT * FROM buoy_observation
  WHERE buoy_id='46087'
  ORDER BY observation_time DESC LIMIT 1;"

# Check index usage
sqlite3 ~/.local/share/buoy_data.sqlite ".indexes"
```

### Check Script Execution Times

```bash
# Time buoy XML parsing
time python3 buoy_to_influx_sqlite.py

# Time NOAA fetch
time python3 fetch_noaa_buoy.py

# Time JSON export
time python3 sqlite_to_json.py
```

---

For troubleshooting guidance, see `TROUBLESHOOTING.md`.
For deployment and configuration details, see `DEPLOYMENT.md`.
