# Sarracenia (sr3) Configuration Backups

**⚠️ IMPORTANT: These are BACKUP COPIES for reference only!**

## Active Configuration Location

The **actual running configs** are located at:
```
~/.config/sr3/subscribe/bc_buoys.conf
~/.config/sr3/subscribe/marine_forecast.conf
~/.config/sr3/credentials.conf
```

## What's in this directory

- `bc_buoys.conf` - Backup copy of buoy subscription config
- `marine_forecast.conf` - Backup copy of marine forecast subscription config
- `bc_wind_stations.conf` - Wind station subscription config (9 coastal weather stations)

## Credentials

**Credentials are stored separately** and should **NEVER** be committed to the repo:
- Location: `~/.config/sr3/credentials.conf`
- Contains: AMQP username/password for dd.weather.gc.ca
- **DO NOT** copy this file into the repo

## Deployment Notes

When setting up a new server:
1. Install sr3: `pip install metpx-sr3`
2. Create `~/.config/sr3/subscribe/` directory
3. Copy these configs to `~/.config/sr3/subscribe/`
4. Create `~/.config/sr3/credentials.conf` separately (not in repo)
5. Start subscriptions: `sr3 start`

## Updating Active Configs

If you modify files here, remember to:
1. Copy changes to `~/.config/sr3/subscribe/`
2. Restart sr3: `sr3 restart`
3. Verify: `sr3 status`
