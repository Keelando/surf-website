# Migration Guide - halibutbank.ca

Complete guide for migrating the Salish Sea wave monitoring system to a new machine.

## Overview

The system consists of:
- **Backend** (`envcan_wave`) - Python data pipelines, cron jobs
- **Frontend** (`site`) - Static HTML/CSS/JS served by Caddy
- **Databases** - SQLite files in `~/.local/share/`
- **External services** - Cloudflare, Environment Canada AMQP, NOAA, DFO

---

## Phase 1: System Setup

### Install Ubuntu 22.04+ LTS

### Install Required Packages
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-dev sqlite3 ffmpeg git curl

# Install Caddy
sudo apt install -y debian-keyring debian-archive-keyring apt-transport-https
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | sudo gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' | sudo tee /etc/apt/sources.list.d/caddy-stable.list
sudo apt update
sudo apt install caddy
```

### Create User (if needed)
```bash
sudo adduser keelando
sudo usermod -aG sudo keelando
```

---

## Phase 2: Clone Repositories

```bash
cd ~
git clone git@github.com:Keelando/surf-website.git envcan_wave
git clone git@github.com:Keelando/surf-website-front-end.git site
```

---

## Phase 3: Python Environment

```bash
cd ~/envcan_wave
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Phase 4: Copy Data from Old Machine

### Databases (~155 MB total)
```bash
# On old machine
tar -czf databases.tar.gz ~/.local/share/*.sqlite

# Transfer to new machine
scp databases.tar.gz newmachine:~/

# On new machine
mkdir -p ~/.local/share
tar -xzf databases.tar.gz -C /
```

### Databases included:
| Database | Size | Purpose |
|----------|------|---------|
| `buoy_data.sqlite` | ~7 MB | Wave buoy observations |
| `wind_data.sqlite` | ~64 MB | Wind station observations |
| `tide_data.sqlite` | ~71 MB | Tide observations & predictions |
| `storm_surge_forecast.sqlite` | ~9 MB | Storm surge forecasts |
| `lightstation_data.sqlite` | ~2 MB | Lightstation reports |

**Note:** Databases will auto-recreate if missing (empty), but you'll lose historical data.

### Credentials File
```bash
# On old machine
cat ~/.config/buoy_influx_1.env

# On new machine
mkdir -p ~/.config
nano ~/.config/buoy_influx_1.env
chmod 600 ~/.config/buoy_influx_1.env
```

Contents (update IPs for your network):
```
INFLUX_HOST=<YOUR_INFLUX_HOST>
INFLUX_PORT=8086
INFLUX_USER=<YOUR_USER>
INFLUX_PASS=<YOUR_PASS>
INFLUX_DB=weather_data

MQTT_HOST=<YOUR_MQTT_HOST>
MQTT_PORT=1883
MQTT_USER=<YOUR_USER>
MQTT_PASS=<YOUR_PASS>
```

**Note:** If you don't have InfluxDB/MQTT (Home Assistant), the scripts will still work - they'll just skip those exports.

---

## Phase 5: Configure Caddy

Create `/etc/caddy/Caddyfile`:
```caddy
:8090 {
    root * /home/keelando/site
    file_server

    # Cache images for 1 month
    @images {
        path *.jpg *.jpeg *.png *.gif *.webp *.svg
    }
    header @images Cache-Control "public, max-age=2592000, immutable"

    # No caching for data files
    @nocache {
        not path *.jpg *.jpeg *.png *.gif *.webp *.svg
    }
    header @nocache Cache-Control "no-store, no-cache, must-revalidate"

    encode gzip zstd

    log {
        output file /var/log/caddy/halibutbank-access.log {
            roll_size 10mb
            roll_keep 5
        }
        format json
    }
}
```

```bash
sudo systemctl enable caddy
sudo systemctl restart caddy
```

---

## Phase 6: Install Cron Jobs

### Set Environment Variables
Edit crontab and add at the top:
```bash
WINDY_API_KEY=<YOUR_WINDY_JWT_TOKEN>
SURREY_API_USERNAME=surreyrain
SURREY_API_PASSWORD=surreyrain
```

### Install Crontab
```bash
# Review first
cat ~/envcan_wave/config/crontab.txt

# Install (this will overwrite existing crontab!)
crontab ~/envcan_wave/config/crontab.txt

# Verify
crontab -l
```

---

## Phase 7: Start SR3 (Sarracenia)

SR3 subscribes to Environment Canada's AMQP feed for real-time XML data.

```bash
# Copy SR3 configs (already in repo)
mkdir -p ~/.config/sr3/subscribe
cp ~/envcan_wave/config/sr3/subscribe/*.conf ~/.config/sr3/subscribe/

# Start subscriptions
sr3 start subscribe/bc_buoys
sr3 start subscribe/bc_wind_stations
sr3 start subscribe/marine_forecast

# Verify running
sr3 status
```

---

## Phase 8: Initialize Directories

```bash
# Create log directory
mkdir -p ~/envcan_wave/logs

# Create data directories (SR3 will populate these)
mkdir -p ~/envcan_wave/data/buoy
mkdir -p ~/envcan_wave/data/wind
mkdir -p ~/envcan_wave/data/marine_forecast
mkdir -p ~/envcan_wave/data/lightstation

# Frontend data directory (scripts will populate)
mkdir -p ~/site/data
```

---

## Phase 9: Update Cloudflare DNS

1. Log into Cloudflare dashboard
2. Select `halibutbank.ca` domain
3. Go to DNS settings
4. Update A record to point to new server's public IP
5. Ensure proxy status is "Proxied" (orange cloud)
6. SSL/TLS mode should be "Full (strict)"

**Port mapping:** Cloudflare 443 (HTTPS) → Your server 8090 (HTTP)

---

## Phase 10: Verify Everything Works

### Test Individual Scripts
```bash
cd ~/envcan_wave
source .venv/bin/activate

# Test buoy parser
python3 scripts/parse/buoy_to_influx_sqlite.py

# Test JSON export
python3 scripts/export/sqlite_to_json.py

# Check output
cat ~/site/data/latest_buoy_v2.json | head -20
```

### Check SR3 is Receiving Data
```bash
sr3 status
ls -la ~/envcan_wave/data/buoy/
```

### Check Website
```bash
curl -I http://localhost:8090
curl http://localhost:8090/data/latest_buoy_v2.json | head
```

### Monitor Logs
```bash
tail -f ~/envcan_wave/logs/*.log
```

---

## Troubleshooting

### SR3 Not Receiving Data
```bash
# Check status
sr3 status

# Check logs
tail -50 ~/.cache/sr3/log/subscribe_bc_buoys_*.log

# Restart
sr3 restart subscribe/bc_buoys
```

### Cron Jobs Not Running
```bash
# Check cron service
sudo systemctl status cron

# Check cron logs
grep CRON /var/log/syslog | tail -20

# Test a job manually
cd ~/envcan_wave && source .venv/bin/activate && python3 scripts/parse/buoy_to_influx_sqlite.py
```

### Website Not Loading
```bash
# Check Caddy
sudo systemctl status caddy
sudo journalctl -u caddy -n 50

# Check if port 8090 is listening
ss -tlnp | grep 8090
```

### Database Errors
```bash
# Check database integrity
sqlite3 ~/.local/share/buoy_data.sqlite "PRAGMA integrity_check;"

# Check recent data
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT buoy_id, datetime(MAX(observation_time), 'unixepoch') FROM buoy_observation GROUP BY buoy_id;"
```

---

## Optional: Restic Backups

If you want system backups on the new machine:

```bash
# Install restic
sudo apt install restic

# Create password file
echo "your-backup-password" | sudo tee /root/.restic_pw
sudo chmod 600 /root/.restic_pw

# Initialize repository (adjust path for your backup drive)
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/backup/restic init

# Copy backup script
sudo cp ~/envcan_wave/backup_surf.sh /home/keelando/
sudo chmod +x /home/keelando/backup_surf.sh

# Add to crontab (already in crontab.txt)
# 30 2 * * * /home/keelando/backup_surf.sh
```

---

## Data Retention Notes

The system auto-manages data retention:
- **XML files:** Purged after 2 days (hourly cron)
- **Log files:** Purged after 7 days (daily cron)
- **Database observations:** 30 days (script-managed)
- **Tide predictions:** 3 days
- **Webcam archives:** 30-day rolling window

---

## External Services Reference

| Service | URL | Auth | Purpose |
|---------|-----|------|---------|
| Environment Canada | `amqps://dd.weather.gc.ca` | None (public) | Buoy/wind XML feeds |
| NOAA NDBC | `ndbc.noaa.gov` | None | US buoy data |
| DFO IWLS | `api-iwls.dfo-mpo.gc.ca` | None | Tide data |
| GeoMet | `geo.weather.gc.ca` | None | Storm surge forecasts |
| Surrey FlowWorks | `flowworks.com` | `surreyrain/surreyrain` | Wave data |
| Windy PWS | `stations.windy.com` | API key (JWT) | Push forecasts |

---

## Quick Reference

```bash
# Start everything after reboot
sr3 start subscribe/bc_buoys subscribe/bc_wind_stations subscribe/marine_forecast
sudo systemctl start caddy

# Check system health
~/envcan_wave/scripts/monitoring/health_check.py

# Manual backup
sudo /home/keelando/backup_surf.sh

# View recent data
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT * FROM buoy_observation ORDER BY observation_time DESC LIMIT 5;"
```
