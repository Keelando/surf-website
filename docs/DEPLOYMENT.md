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

### Maintenance & Backups

```bash
# Hourly: Purge XML files older than 2 days
0 * * * * find /home/keelando/envcan_wave/data/buoy -name "*.xml" -mtime +2 -delete
5 * * * * find /home/keelando/envcan_wave/data/marine_forecast -name "*.xml" -mtime +2 -delete

# Weekly: Purge logs older than 7 days
0 0 * * * find /home/keelando/envcan_wave/logs -name "*.log" -type f -mtime +7 -delete

# Daily 11:02 PM: Backup crontab to git repo (runs before git backup)
2 23 * * * crontab -l > /home/keelando/envcan_wave/config/crontab.txt 2>&1

# Daily 11:03 PM: Auto-commit and push backend repo to git
3 23 * * * /usr/bin/git add -A && /usr/bin/git diff --staged --quiet || (/usr/bin/git commit -m "Auto-backup $(date +\%Y-\%m-\%d)" && /usr/bin/git push origin main) >> /home/keelando/envcan_wave/logs/git_backup.log 2>&1

# Daily 11:04 PM: Auto-commit and push frontend repo to git
4 23 * * * cd /home/keelando/site && /usr/bin/git add -A && /usr/bin/git diff --staged --quiet || (/usr/bin/git commit -m "Auto-backup $(date +\%Y-\%m-\%d)" && /usr/bin/git push origin main) >> /home/keelando/site/git_backup.log 2>&1
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

### Automated Nightly Backups

The system performs automatic backups every night at 11 PM via cron jobs:

**Backup sequence (runs in order):**
1. **11:02 PM** - Export crontab to `~/envcan_wave/config/crontab.txt`
2. **11:03 PM** - Commit and push backend repo (`~/envcan_wave`) to GitHub
3. **11:04 PM** - Commit and push frontend repo (`~/site`) to GitHub

**What gets backed up:**
- **Backend repo** (`~/envcan_wave`):
  - All Python scripts (parsers, fetchers, exporters)
  - Configuration files (`stations.json`, `tide_stations.json`)
  - **Crontab** (`config/crontab.txt`) - Automatically saved before git push
  - Documentation (README, CLAUDE.md, docs/)
  - SQLite databases (via git-lfs if configured, otherwise excluded)

- **Frontend repo** (`~/site`):
  - HTML, CSS, JavaScript files
  - Static assets (images, icons)
  - JSON data exports (latest buoy/tide/wind/forecast data)
  - Analytics reports

**Not backed up to git:**
- SQLite databases (`~/.local/share/*.sqlite`) - use manual backup below
- sr3 configuration (`~/.config/sr3/`) - use manual backup below
- Credentials (`~/.config/buoy_influx_1.env`) - NEVER commit to git
- Raw XML data (`~/envcan_wave/data/`) - transient, auto-purged after 2 days

**Backup logs:**
- Backend: `~/envcan_wave/logs/git_backup.log`
- Frontend: `~/site/git_backup.log`

**Check backup status:**
```bash
# View backend backup log
tail -20 ~/envcan_wave/logs/git_backup.log

# View frontend backup log
tail -20 ~/site/git_backup.log

# Check if backup ran today
ls -lh ~/envcan_wave/config/crontab.txt
```

### Manual Database Backups

SQLite databases should be backed up manually or via separate cron job (not included in git due to size):

```bash
# Create backup directory
mkdir -p ~/backups/databases

# Backup all databases
cp ~/.local/share/buoy_data.sqlite ~/backups/databases/buoy_data_$(date +%Y%m%d).sqlite
cp ~/.local/share/tide_data.sqlite ~/backups/databases/tide_data_$(date +%Y%m%d).sqlite
cp ~/.local/share/wind_data.sqlite ~/backups/databases/wind_data_$(date +%Y%m%d).sqlite
cp ~/.local/share/storm_surge_forecast.sqlite ~/backups/databases/storm_surge_$(date +%Y%m%d).sqlite

# Optional: Compress backups
tar -czf ~/backups/databases_$(date +%Y%m%d).tar.gz ~/backups/databases/*_$(date +%Y%m%d).sqlite
```

**Add to crontab for weekly database backups:**
```bash
# Weekly Sunday 3 AM: Backup databases
0 3 * * 0 mkdir -p ~/backups/databases && cp ~/.local/share/buoy_data.sqlite ~/backups/databases/buoy_data_$(date +\%Y\%m\%d).sqlite && cp ~/.local/share/tide_data.sqlite ~/backups/databases/tide_data_$(date +\%Y\%m\%d).sqlite && cp ~/.local/share/wind_data.sqlite ~/backups/databases/wind_data_$(date +\%Y\%m\%d).sqlite
```

### Manual Configuration Backups

**Critical configuration files (backup before making changes):**

```bash
# Create config backup directory
mkdir -p ~/backups/config

# Backup sr3 subscription configs
cp -r ~/.config/sr3 ~/backups/config/sr3_$(date +%Y%m%d)

# Backup credentials (keep secure!)
cp ~/.config/buoy_influx_1.env ~/backups/config/buoy_influx_1_$(date +%Y%m%d).env
chmod 600 ~/backups/config/buoy_influx_1_*.env

# Backup Caddyfile
sudo cp /etc/caddy/Caddyfile ~/backups/config/Caddyfile_$(date +%Y%m%d)

# Backup current crontab (redundant with automated backup, but useful before manual edits)
crontab -l > ~/backups/config/crontab_$(date +%Y%m%d).txt
```

---

## System Backups with Restic

The system uses [restic](https://restic.net/) for automated, incremental, deduplicated backups to `/mnt/storage/restic-backup`.

### Automated Backup Schedule

Daily backups run at **2:30 AM** via cron (`~/backup_surf.sh`):

**What's backed up:**
- `/home` (excluding cache, trash, logs, `.venv`, `__pycache__`)
- `/etc` (system configs)
- `/var/lib` (databases, application data)
- `/srv`, `/opt`, `/usr/local`
- `/root`
- Package list, crontab, enabled services

**Retention policy:**
- Keep 7 daily snapshots
- Keep 4 weekly snapshots
- Automatic pruning of old snapshots

### Manual Backup Operations

**Run backup manually:**
```bash
sudo /home/keelando/backup_surf.sh
```

**List all snapshots:**
```bash
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup snapshots
```

**Check repository health:**
```bash
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup check
```

**View backup statistics:**
```bash
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup stats
```

### Restore from Restic

**List files in latest snapshot:**
```bash
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup ls latest
```

**Restore specific file:**
```bash
# Restore to original location
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup restore latest \
  --target / \
  --include /home/keelando/.config/buoy_influx_1.env

# Or restore to temporary location for inspection
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup restore latest \
  --target /tmp/restore \
  --include /home/keelando/envcan_wave/config/crontab.txt
```

**Restore entire home directory:**
```bash
# Restore to temporary location first (recommended)
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup restore latest \
  --target /tmp/restore \
  --include /home/keelando

# Then copy what you need
cp -r /tmp/restore/home/keelando/envcan_wave ~/envcan_wave_restored
```

**Restore from specific snapshot:**
```bash
# List snapshots with IDs
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup snapshots

# Restore using snapshot ID
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup restore a1b2c3d4 \
  --target /tmp/restore
```

### Initialize New Restic Repository

If setting up on a new drive:

```bash
# Initialize repository
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup init

# Run first backup
sudo /home/keelando/backup_surf.sh
```

---

### Restore from Backup

**Restore crontab:**
```bash
# Restore from git repo (automated backup)
crontab ~/envcan_wave/config/crontab.txt

# OR restore from manual backup
crontab ~/backups/config/crontab_20251130.txt

# Verify restoration
crontab -l | head -20
```

**Restore databases:**
```bash
# Stop any running processes that might be writing to the database
# (or wait for cron cycle to complete)

# Restore database
cp ~/backups/databases/buoy_data_20251130.sqlite ~/.local/share/buoy_data.sqlite

# Verify restoration
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT COUNT(*) FROM buoy_observation;"
```

**Restore sr3 configuration:**
```bash
# Stop sr3 subscriptions
sr3 stop subscribe/bc_buoys
sr3 stop subscribe/bc_wind_stations
sr3 stop subscribe/marine_forecast

# Restore config
cp -r ~/backups/config/sr3_20251130/* ~/.config/sr3/

# Restart sr3
sr3 start subscribe/bc_buoys
sr3 start subscribe/bc_wind_stations
sr3 start subscribe/marine_forecast

# Verify
sr3 status
```

**Restore credentials:**
```bash
# Restore environment file
cp ~/backups/config/buoy_influx_1_20251130.env ~/.config/buoy_influx_1.env
chmod 600 ~/.config/buoy_influx_1.env

# Verify (check permissions)
ls -la ~/.config/buoy_influx_1.env
```

### Disaster Recovery Checklist

If rebuilding system from scratch:

1. **Clone repositories:**
   ```bash
   git clone https://github.com/yourusername/envcan_wave.git ~/envcan_wave
   git clone https://github.com/yourusername/site.git ~/site
   ```

2. **Restore crontab:**
   ```bash
   crontab ~/envcan_wave/config/crontab.txt
   ```

3. **Restore credentials:**
   ```bash
   mkdir -p ~/.config
   cp ~/backups/config/buoy_influx_1.env ~/.config/
   chmod 600 ~/.config/buoy_influx_1.env
   ```

4. **Restore sr3 config:**
   ```bash
   cp -r ~/backups/config/sr3_latest/* ~/.config/sr3/
   ```

5. **Restore databases (optional - they will rebuild from APIs):**
   ```bash
   mkdir -p ~/.local/share
   cp ~/backups/databases/buoy_data_latest.sqlite ~/.local/share/
   cp ~/backups/databases/tide_data_latest.sqlite ~/.local/share/
   cp ~/backups/databases/wind_data_latest.sqlite ~/.local/share/
   ```

6. **Install dependencies and start services:**
   ```bash
   cd ~/envcan_wave
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt

   # Start sr3
   sr3 start subscribe/bc_buoys
   sr3 start subscribe/bc_wind_stations
   sr3 start subscribe/marine_forecast

   # Verify
   sr3 status
   ```

7. **Restore Caddy config:**
   ```bash
   sudo cp ~/backups/config/Caddyfile_latest /etc/caddy/Caddyfile
   sudo caddy reload --config /etc/caddy/Caddyfile
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
