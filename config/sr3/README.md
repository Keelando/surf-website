# sr3 Config (Source of Truth)

These are the **source configs** for sr3 (Sarracenia) AMQP subscriptions.
Edit here, then deploy to the backend.

## Workflow

1. Edit the config file in this directory
2. Copy to backend: `cp config/sr3/<file>.conf ~/.config/sr3/subscribe/`
3. Restart sr3: `sr3 restart subscribe/<name>`
4. Verify: `sr3 status`

## Files

- `bc_buoys.conf` — Buoy SWOB-ML subscriptions
- `bc_wind_stations.conf` — Coastal weather station subscriptions
- `marine_forecast.conf` — Marine weather forecast subscriptions

## Credentials

Stored separately at `~/.config/sr3/credentials.conf` — **never** commit to repo.
