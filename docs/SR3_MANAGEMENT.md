# SR3 Service Management Guide

## Overview
SR3 subscriptions run as systemd services that auto-start on boot and auto-restart on failure.

## Service Names
- `sr3-bc-buoys.service` — Environment Canada buoys
- `sr3-bc-wind-stations.service` — Environment Canada wind stations
- `sr3-marine-forecast.service` — Marine weather forecasts
- `sr3-bc-lightstation-obs.service` — Lightstation FICN bulletins (pending deployment)

## Common Commands

### Check Status
```bash
# All services
sudo systemctl status sr3-bc-buoys sr3-bc-wind-stations sr3-marine-forecast

# Individual service
sudo systemctl status sr3-bc-buoys

# sr3's view (shows data rates)
/home/keelando/envcan_wave/.venv/bin/sr3 status
```

### Restart a Service
**When to restart:** After adding new buoys/stations to `~/.config/sr3/subscribe/*.conf`

```bash
# Restart buoys (e.g., after adding La Perouse Bank)
sudo systemctl restart sr3-bc-buoys

# Check it's running
sudo systemctl status sr3-bc-buoys
```

### View Logs
```bash
# Recent logs
sudo journalctl -u sr3-bc-buoys --since "1 hour ago"

# Follow live logs
sudo journalctl -u sr3-bc-buoys -f

# Show errors only
sudo journalctl -u sr3-bc-buoys -p err
```

### Stop/Start
```bash
# Stop
sudo systemctl stop sr3-bc-buoys

# Start
sudo systemctl start sr3-bc-buoys
```

## Config Files

**Source of truth:** `config/sr3/*.conf` (in the repo — edit here first)
**Deployed to:** `~/.config/sr3/subscribe/*.conf`
**Systemd services:** `/etc/systemd/system/sr3-*.service`
**Downloaded data:** `data/{buoy,wind,marine_forecast,lightstation_ficn}/`

## Adding New Stations

### Example: Adding a new buoy

1. **Add to stations.json:**
   ```bash
   vim ~/envcan_wave/config/stations.json
   # Add the buoy definition
   ```

2. **Edit sr3 config in the repo (source of truth):**
   ```bash
   vim config/sr3/bc_buoys.conf
   # Add: subtopic *.WXO-DD.observations.swob-ml.marine.moored-buoys.*.BUOY_ID.#
   ```

3. **Deploy to backend and restart:**
   ```bash
   cp config/sr3/bc_buoys.conf ~/.config/sr3/subscribe/
   sudo systemctl restart sr3-bc-buoys
   ```

4. **Verify it's working:**
   ```bash
   sudo systemctl status sr3-bc-buoys
   ls -lt ~/envcan_wave/data/buoy/ | head -20
   sudo journalctl -u sr3-bc-buoys --since "5 minutes ago"
   ```

## Troubleshooting

### Service won't start
```bash
# Check systemd logs
sudo journalctl -u sr3-bc-buoys --since "10 minutes ago" -p err

# Clean sr3 state
rm -f ~/.cache/sr3/subscribe/bc_buoys/*.pid
sudo systemctl restart sr3-bc-buoys
```

### No data downloading
```bash
# Check AMQP connection in logs
sudo journalctl -u sr3-bc-buoys | grep -i "amqp\|connected\|queue"

# Verify sr3 config syntax
/home/keelando/envcan_wave/.venv/bin/sr3 sanity subscribe/bc_buoys
```

### Old process stuck
```bash
# Find PIDs
ps aux | grep "sr3 foreground"

# Clean up
sudo systemctl stop sr3-bc-buoys
kill -9 <PID>
rm -f ~/.cache/sr3/subscribe/bc_buoys/*.pid
sudo systemctl start sr3-bc-buoys
```

## Why Systemd?

**Before:** Orphaned foreground processes, hard to restart, manual recovery after crashes  
**After:** Clean lifecycle management, easy restarts, auto-recovery, centralized logging

That's how sr3 survived reboots - the systemd services are enabled:
```bash
$ sudo systemctl is-enabled sr3-bc-buoys
enabled
```
