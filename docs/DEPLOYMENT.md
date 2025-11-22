# Deployment Guide

Production deployment configuration for the marine weather monitoring system.

## Cron Schedule

Production system runs on cron. See `cron.txt` for the actual crontab file.

### Buoy Data

```bash
# Every minute: Parse EC XMLs, export JSON, push MQTT
* * * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 buoy_to_influx_sqlite.py && python3 sqlite_to_json.py && python3 influx_to_mqtt.py >> ~/envcan_wave/buoy_sqlite.log 2>&1

# Every 5 minutes: Export 24h timeseries
*/5 * * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 export_24hr_timeseries.py >> ~/envcan_wave/timeseries.log 2>&1

# Every 20 minutes (at 5, 25, 45): Fetch NOAA data
5,25,45 * * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 fetch_noaa_buoy.py >> ~/envcan_wave/noaa.log 2>&1
```

### Station Metadata

```bash
# Hourly: Export stations.json from backend to frontend
# Backend source: ~/envcan_wave/config/stations.json
# Frontend output: ~/site/data/stations.json
# Contains coordinates, names, types for all buoys, tide stations, and wind stations
0 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/export_stations_json.py >> /home/keelando/envcan_wave/logs/stations_export.log 2>&1
```

**Why this matters:**
- Backend `config/stations.json` is the **source of truth**
- Frontend `data/stations.json` is auto-synced every hour
- Never edit the frontend copy - changes will be overwritten
- Edit backend version only, then export will sync it

### Tide Data

```bash
# Every 5 minutes: Export tide JSONs
*/5 * * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 export_tide_json.py >> ~/envcan_wave/tide_export.log 2>&1

# Every 30 minutes: Fetch tide observations (real-time)
*/30 * * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 tide_to_sqlite.py --observations >> ~/envcan_wave/tide_obs.log 2>&1

# Daily 12:05 AM: Fetch tide predictions (48-hour forecasts)
5 0 * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 tide_to_sqlite.py --predictions >> ~/envcan_wave/tide_pred.log 2>&1

# Twice daily (12:10 AM & 12:10 PM): Fetch tide high/low events (redundancy)
10 0,12 * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 tide_to_sqlite.py --highlow >> ~/envcan_wave/tide_highlow.log 2>&1
```

### Marine Forecasts

```bash
# Every 30 minutes: Parse marine forecast XMLs
*/30 * * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 parse_marine_forecast.py >> ~/envcan_wave/marine_forecast.log 2>&1
```

### Storm Surge Forecasts

```bash
# Every 6 hours at :30 (1:30, 7:30, 13:30, 19:30 UTC): Fetch storm surge forecast
# 19:30 run stores 18Z forecast to database for hindcast analysis (closest to noon Pacific)
30 1,7,13,19 * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 fetch_storm_surge.py >> ~/envcan_wave/storm_surge.log 2>&1

# Daily 2 PM UTC: Export hindcast data (hours 38-61 / full Pacific calendar day 2 days ahead)
0 14 * * * cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 export_hindcast_json.py >> ~/envcan_wave/hindcast_export.log 2>&1
```

### Maintenance

```bash

# Hourly: Purge XML files older than 2 days
0 * * * * find /home/keelando/envcan_wave/data/buoy -name "*.xml" -mtime +2 -delete
5 * * * * find /home/keelando/envcan_wave/data/marine_forecast -name "*.xml" -mtime +2 -delete

# Daily 11 PM: Auto-commit and push to git (backend)
0 23 * * * cd /home/keelando/envcan_wave && git add . && git commit -m "Auto-backup $(date +\%Y-\%m-\%d)" && git push >> ~/envcan_wave/git_backup.log 2>&1

# Daily 11:05 PM: Auto-commit and push to git (frontend)
5 23 * * * cd /home/keelando/site && git add . && git commit -m "Auto-backup $(date +\%Y-\%m-\%d)" && git push >> ~/site/git_backup.log 2>&1
```

### Install Cron Schedule

```bash
crontab -e
# Paste the contents above, or:
crontab < cron.txt
```

---

## Sarracenia (sr3) Configuration

### Buoy Observation Subscription

**File:** `~/.config/sr3/subscribe/bc_buoys.conf`

```conf
broker amqps://dd.weather.gc.ca
topicPrefix v02.post

directory /home/keelando/envcan_wave/data/buoy

instances 1
batch 50
logLevel info

# Subscribe to specific buoy IDs
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600146.#  # Halibut Bank
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600303.#  # Southern Strait
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600304.#  # English Bay
subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.4600131.#  # Sentry Shoal
```

**How it works:**
- Connects to Environment Canada's public AMQP broker (no authentication required)
- Subscribes to SWOB-ML (Standard Weather Observation - Markup Language) topics
- Downloads XML files as they're published (approximately hourly)
- Files saved to `~/envcan_wave/data/buoy/`

**Start subscription:**
```bash
sr3 start subscribe/bc_buoys
```

**Logs:** `~/.cache/sr3/log/subscribe_bc_buoys_*.log`

### Marine Forecast Subscription

**File:** `~/.config/sr3/subscribe/marine_forecast.conf`

```conf
broker amqps://dd.weather.gc.ca
topicPrefix v02.post

directory /home/keelando/envcan_wave/data/marine_forecast

instances 1
batch 50
logLevel info

# Marine weather forecasts for Strait of Georgia
# m0000028 covers BOTH north and south of Nanaimo zones
subtopic *.WXO-DD.marine_weather.*.*.m0000028.#
```

**What it downloads:**
- Marine weather forecast XMLs for Strait of Georgia (north + south zones)
- Updates 2-4 times daily (typically 05h, 11h, 18h UTC)
- Files include warnings, wind/weather forecasts, extended outlook

**Start subscription:**
```bash
sr3 start subscribe/marine_forecast
```

**Logs:** `~/.cache/sr3/log/subscribe_marine_forecast_*.log`

### Verify sr3 Is Running

```bash
sr3 status
ps aux | grep sr3
```

**IMPORTANT:** sr3 must run continuously as daemon. Without it, no new XML files are downloaded.

---

## Configuration Files

### InfluxDB + MQTT Credentials

**File:** `~/.config/buoy_influx_1.env`

```bash
INFLUX_HOST=192.168.1.98
INFLUX_PORT=8086
INFLUX_USER=your_user
INFLUX_PASS=your_password
INFLUX_DB=buoy_data
MQTT_HOST=192.168.1.98
MQTT_PORT=1883
MQTT_USER=your_user
MQTT_PASS=your_password
```

**Security:**
- Never commit `.env` files to git
- Use `chmod 600 ~/.config/buoy_influx_1.env` to restrict access
- Keep credentials separate from code

---

## Caddy Web Server

### Installation

```bash
# Install Caddy (if not already installed)
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install caddy
```

### Configuration

**File:** `/etc/caddy/Caddyfile`

```caddy
:8090 {
    root * /home/keelando/site
    file_server

    # Cache images for 1 month (includes banner)
    @images {
        path *.jpg *.jpeg *.png *.gif *.webp *.svg
    }
    header @images Cache-Control "public, max-age=2592000, immutable"

    # No caching for everything else (HTML/CSS/JS/data)
    @nocache {
        not path *.jpg *.jpeg *.png *.gif *.webp *.svg
    }
    header @nocache Cache-Control "no-store, no-cache, must-revalidate"

    # Enable compression
    encode gzip zstd
}
```

**Cache strategy:**
- **Images:** Cached for 1 month (immutable)
- **HTML/CSS/JS/JSON:** No caching (allows immediate updates)

**Why no caching for code:** Allows rapid development iteration without cache invalidation issues

### Caddy Commands

```bash
# Reload configuration after changes
sudo caddy reload --config /etc/caddy/Caddyfile

# Check Caddy status
sudo systemctl status caddy

# Start/stop/restart Caddy
sudo systemctl start caddy
sudo systemctl stop caddy
sudo systemctl restart caddy

# Enable Caddy at boot
sudo systemctl enable caddy

# View Caddy logs
sudo journalctl -u caddy -f
```

### Test Website

```bash
# Test from localhost
curl -I http://localhost:8090

# Check JSON endpoints
curl http://localhost:8090/data/latest_buoy_v2.json | jq . | head
curl http://localhost:8090/data/tide-latest.json | jq . | head
curl http://localhost:8090/data/marine_forecast.json | jq . | head
```

---

## Browser Cache Busting

### Problem

Even with `Cache-Control: no-store`, browsers (especially Firefox) aggressively cache CSS/JS files, causing stale styles after updates.

### Solution: CSS Versioning

**CSS files use version numbers in filenames:**
- `style-v3.css` - Main site styles
- `nav-tide-styles-v3.css` - Navigation and tide page styles
- `stations-map-v3.css` - Map component styles
- `warning-banner-v3.css` - Warning banner styles

**HTML references:**
```html
<!-- index.html -->
<link rel="stylesheet" href="/assets/css/style-v3.css" />
<link rel="stylesheet" href="/assets/css/nav-tide-styles-v3.css" />
<link rel="stylesheet" href="/assets/css/stations-map-v3.css" />
<link rel="stylesheet" href="/assets/css/warning-banner-v3.css" />
```

### When to Increment Versions

- After making CSS changes that don't appear in browser
- When users report visual inconsistencies
- After any significant UI/UX changes

### How to Bust Cache

```bash
cd ~/site/assets/css

# Increment version numbers (v3 → v4)
mv style-v3.css style-v4.css
mv nav-tide-styles-v3.css nav-tide-styles-v4.css
mv stations-map-v3.css stations-map-v4.css
mv warning-banner-v3.css warning-banner-v4.css

# Update HTML files to reference v4
# Edit ~/site/index.html, tides.html, forecasts.html

# Optional: Delete old versions once confirmed working
rm *-v3.css
```

**Why this works:**
- Browser sees completely new file path = guaranteed cache bust
- More reliable than query parameters (`style.css?v=4`)
- Prevents mixed old/new assets causing broken UI

---

## File Permissions

### Required Permissions

```bash
# Ensure JSON files are world-readable for web server
chmod 644 ~/site/data/*.json

# Note: stations.json is automatically synced by export_stations_json.py (runs hourly)
# Backend source: ~/envcan_wave/config/stations.json
# Frontend copy: ~/site/data/stations.json (auto-updated, do not edit manually)
chmod 644 ~/envcan_wave/config/stations.json

# Protect credentials
chmod 600 ~/.config/buoy_influx_1.env

# Ensure log directory is writable
chmod 755 ~/envcan_wave

# Ensure data directories exist and are writable
mkdir -p ~/envcan_wave/data/buoy
mkdir -p ~/envcan_wave/data/marine_forecast
chmod 755 ~/envcan_wave/data/buoy
chmod 755 ~/envcan_wave/data/marine_forecast
```

### Ownership

```bash
# Ensure user owns all project files
sudo chown -R keelando:keelando ~/envcan_wave
sudo chown -R keelando:keelando ~/site
```

---

## System Services

### Create Systemd Service for sr3 (Optional)

If you want sr3 to auto-start on boot:

**File:** `/etc/systemd/system/sr3-buoys.service`

```ini
[Unit]
Description=Sarracenia Buoy Data Subscription
After=network.target

[Service]
Type=simple
User=keelando
WorkingDirectory=/home/keelando/envcan_wave
ExecStart=/home/keelando/envcan_wave/.venv/bin/sr3 foreground subscribe/bc_buoys
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Enable and start:**
```bash
sudo systemctl daemon-reload
sudo systemctl enable sr3-buoys
sudo systemctl start sr3-buoys
sudo systemctl status sr3-buoys
```

**Note:** Currently sr3 runs manually via `sr3 start subscribe/bc_buoys`. Creating a systemd service is optional but recommended for production.

---

## Monitoring & Health Checks

### Check Data Freshness

```bash
# Script to check if all components are working
#!/bin/bash

echo "=== Data Freshness Check ==="

# Check sr3 status
echo "sr3 status:"
sr3 status | grep -E '(bc_buoys|marine_forecast)'

# Check latest XML files
echo -e "\nLatest buoy XML:"
ls -lt ~/envcan_wave/data/buoy/*.xml 2>/dev/null | head -1

echo -e "\nLatest marine forecast XML:"
ls -lt ~/envcan_wave/data/marine_forecast/*.xml 2>/dev/null | head -1

# Check database
echo -e "\nLatest buoy observation:"
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT buoy_id, datetime(MAX(observation_time), 'unixepoch') FROM buoy_observation GROUP BY buoy_id ORDER BY buoy_id;"

echo -e "\nLatest tide observation:"
sqlite3 ~/.local/share/tide_data.sqlite "SELECT station_name, datetime(MAX(observation_time), 'unixepoch') FROM tide_observation GROUP BY station_name;"

# Check JSON exports
echo -e "\nJSON export timestamps:"
ls -lh ~/site/data/latest_buoy_v2.json ~/site/data/tide-latest.json ~/site/data/marine_forecast.json
```

**Save as:** `~/envcan_wave/check_health.sh`

```bash
chmod +x ~/envcan_wave/check_health.sh
~/envcan_wave/check_health.sh
```

### Set Up Monitoring (Optional)

Add to crontab for daily health check:

```bash
# Daily 8 AM: Email health check report
0 8 * * * ~/envcan_wave/check_health.sh | mail -s "Marine Weather System Health" your@email.com
```

---

## Backup Strategy

### Automated Git Backups

Cron jobs automatically commit and push changes daily at 11 PM (see Cron Schedule section).

**Backend repo:** `~/envcan_wave`
**Frontend repo:** `~/site`

### Manual Backup

```bash
# Backup databases
cp ~/.local/share/buoy_data.sqlite ~/backups/buoy_data_$(date +%Y%m%d).sqlite
cp ~/.local/share/tide_data.sqlite ~/backups/tide_data_$(date +%Y%m%d).sqlite

# Backup configuration
cp -r ~/.config/sr3 ~/backups/sr3_config_$(date +%Y%m%d)
cp ~/.config/buoy_influx_1.env ~/backups/

# Backup Caddyfile
sudo cp /etc/caddy/Caddyfile ~/backups/Caddyfile_$(date +%Y%m%d)
```

### Restore from Backup

```bash
# Restore database
cp ~/backups/buoy_data_20251105.sqlite ~/.local/share/buoy_data.sqlite

# Restore sr3 config
cp -r ~/backups/sr3_config_20251105/* ~/.config/sr3/
sr3 stop subscribe/bc_buoys
sr3 start subscribe/bc_buoys
```

---

## Firewall Configuration

If using UFW firewall:

```bash
# Allow web traffic on port 8090
sudo ufw allow 8090/tcp

# Allow SSH (if not already allowed)
sudo ufw allow 22/tcp

# Enable firewall
sudo ufw enable

# Check status
sudo ufw status
```

---

## Troubleshooting Deployment

### sr3 Not Downloading Files

```bash
# Check sr3 status
sr3 status

# Check sr3 logs for errors
tail -50 ~/.cache/sr3/log/subscribe_bc_buoys_*.log | grep -i error

# Restart sr3
sr3 stop subscribe/bc_buoys
sr3 start subscribe/bc_buoys
```

### Cron Jobs Not Running

```bash
# Check cron service is running
sudo systemctl status cron

# Check cron logs
tail -f /var/log/syslog | grep CRON

# Test cron command manually
cd /home/keelando/envcan_wave && source .venv/bin/activate && python3 buoy_to_influx_sqlite.py
```

### Website Not Serving Files

```bash
# Check Caddy is running
sudo systemctl status caddy

# Check Caddy logs
sudo journalctl -u caddy -n 50

# Restart Caddy
sudo systemctl restart caddy

# Test from localhost
curl -I http://localhost:8090
```

### Permissions Issues

```bash
# Fix ownership
sudo chown -R keelando:keelando ~/envcan_wave ~/site

# Fix permissions
chmod 755 ~/envcan_wave ~/site
chmod 644 ~/site/data/*.json
chmod 600 ~/.config/buoy_influx_1.env
```

---

For troubleshooting guidance, see `TROUBLESHOOTING.md`.
For command examples, see `COMMANDS.md`.
