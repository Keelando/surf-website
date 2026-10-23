# Surrey FlowWorks Integration

Complete guide for integrating Surrey FlowWorks wave and tide data into the Salish Sea monitoring system.

**Quick Links:**
- [Setup & Integration](#setup--integration)
- [Deployment](#deployment)
- [Frontend Integration](#frontend-integration)
- [Channel Reference](#channel-reference)

---

## Setup & Integration

### Overview

Surrey FlowWorks provides real-time wave and tide data from Boundary Bay via API.

**Stations:**
- Crescent Beach Ocean (CRPILE) - Wave buoy + geodetic tide
- Crescent Channel Ocean (CRCHAN) - Wave buoy + geodetic tide

**Data types:**
- Wave metrics (height, period, direction)
- Wind data
- Water level (geodetic datum - CGVD28)
- Tide predictions

### API Configuration

**File:** `fetch_surrey_wave_v2.py`

```python
SURREY_API_USERNAME = os.getenv('SURREY_API_USERNAME')
SURREY_API_PASSWORD = os.getenv('SURREY_API_PASSWORD')
```

Set in crontab or `.env` file.

### Database Storage

**Wave/Wind data:** → `buoy_data.sqlite`
**Tide data:** → `tide_data.sqlite` (separate pipeline)

---

## Deployment

### 1. Install Dependencies

```bash
pip install requests python-dotenv
```

### 2. Configure Credentials

Put them in `config/.env` (gitignored) — **never** in `config/crontab.txt`,
which is tracked and public:

```bash
SURREY_API_USERNAME=<username>
SURREY_API_PASSWORD=<password>
```

`lib/env.py` reads this file, so the fetch scripts pick the values up under
cron without the crontab exporting anything. `os.environ` still takes
precedence if you'd rather export them another way.

### 3. Add Cron Jobs

**Wave/wind data (every 20 min):**
```bash
*/20 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_surrey_wave_v2.py >> /home/keelando/envcan_wave/logs/surrey.log 2>&1
```

**Tide observations (every 20 min):**
```bash
*/20 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_surrey_tides.py --observations >> /home/keelando/envcan_wave/logs/surrey_tide_sync.log 2>&1
```

**Tide predictions (daily at 2am):**
```bash
0 2 * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_surrey_tides.py --predictions >> /home/keelando/envcan_wave/logs/surrey_tide_sync.log 2>&1
```

### 4. Verify

```bash
# Check logs
tail -f /home/keelando/envcan_wave/logs/surrey.log

# Query database
sqlite3 ~/.local/share/buoy_data.sqlite "SELECT * FROM buoy_observation WHERE buoy_id='CRPILE' ORDER BY observation_time DESC LIMIT 5;"
```

---

## Frontend Integration

### Buoy Cards

Surrey stations appear as wave buoys with special attribution:

```javascript
// Display: "Crescent Beach Ocean (Surrey)"
if (station.id === 'CRPILE' || station.id === 'CRCHAN') {
  attribution = '(Surrey)';
}
```

### Geodetic Tides

Surrey tide data is in **CGVD28 geodetic datum**, not chart datum.

**Key differences:**
- Chart datum: 0.0m = lowest astronomical tide
- CGVD28: 0.0m = geodetic reference (higher)

**Frontend handling:**
- Tide charts show geodetic water levels
- No conversion to chart datum
- Display "Geodetic Datum (CGVD28)" label

### Map Integration

Stations added to `stations.json`:

```json
"CRPILE": {
  "name": "Crescent Beach Ocean",
  "lat": 49.054,
  "lon": -122.897,
  "source": "Surrey FlowWorks",
  "type": "buoy"
}
```

---

## Channel Reference

**Source of truth:** All Surrey FlowWorks site IDs and channel IDs live in
`config/stations.json` under the `buoys` (CRPILE, CRCHAN) and `wind` (COLEB)
entries. Both fetchers (`fetch_surrey_wave_v2.py`, `fetch_surrey_tides.py`) read
channel IDs from there via `lib/stations.py` — do **not** hardcode channel IDs
in the scripts.

Each station's `channels` is a flat `{field_name: channel_id}` map. Fields are
partitioned by which fetcher owns them:

- **Wave fetcher** (`buoy_data.sqlite` / `wind_data.sqlite`): `wind_*`,
  `wave_*`, `sea_temp`, `air_temp` (see `BUOY_FIELDS` / `WIND_FIELDS`).
- **Tide fetcher** (`tide_data.sqlite`): `water_level_*`, `tidal_residual`,
  `geodiff_*` (see `TIDE_FIELDS`).

### Wave sensors in priority order

CRPILE has three wave sensors on the pile, listed in `wave_sensors` in
stations.json in the order they are trusted: the Anderaa (Hs_Anderra,
Hmax_Anderra, Tmean_Anderra, Tpeak_Anderra), then the radar (Hm0_RADAR,
Tp_RADAR), then the Keller pressure sensor (Hs_KellerPT, Tp_Keller). Its wave
fields live there and NOT in `channels` (a test enforces one home per field).

`resolve_wave_readings()` in `fetch_surrey_wave_v2.py` takes each timestamp's
reading **whole** from the first sensor with a real Hs there, so a height and
period shown together always come from one sensor. A lower sensor is only
asked when the ones above left a timestamp without a reading, so a healthy
Anderaa costs no extra requests. A wave height or period of exactly 0 counts
as no reading (`NO_ZERO_FIELDS`): a sensor with no signal (dead, or dry at low
tide) reports zeros, not nulls. From 2026-10-21 the Anderaa sent only zeros.

The `wave_sensor` column records the backup that supplied a reading (NULL =
the first choice). The exports carry it as `wave_sensor` on the latest reading
and `sensor` on wave-height timeseries points; the site tags the card ("via
radar"), stars history rows, and shades the chart. Sensors read slightly
differently, so a step at a switch may not be the sea.

To discover available channel IDs at a site, use
`scripts/query_surrey_channels.py <site_id>`.

### Looking up current channel IDs

```bash
# All channels for a site, live from the API
python3 scripts/query_surrey_channels.py 20182   # Crescent Beach Ocean
```

### API Time Zone

**CRITICAL:** Surrey API expects **Pacific time** (America/Vancouver), NOT UTC!

```python
now = datetime.now(ZoneInfo('America/Vancouver'))
params = {
    'startDateFilter': now.strftime('%Y-%m-%dT%H:%M:%S'),  # No timezone indicator
}
```

### Data Retention

- **Observations:** 48 hours lookback
- **Predictions:** 96 hours forward (4 days)

---

## Troubleshooting

### No Data Returned

**Issue:** API returns 0 points

**Fix:** Check timezone - must use Pacific time, not UTC

```python
# WRONG
now = datetime.now(timezone.utc)

# CORRECT
now = datetime.now(ZoneInfo('America/Vancouver'))
```

### Stale Tide Data

**Issue:** Observations showing as >2h old

**Check:**
1. Cron job running? `crontab -l | grep surrey`
2. API credentials set? `env | grep SURREY`
3. Logs: `tail /home/keelando/envcan_wave/logs/surrey_tide_sync.log`

### Wave Data Missing

**Issue:** Buoys not appearing on map

**Check:**
1. Database: `sqlite3 ~/.local/share/buoy_data.sqlite "SELECT DISTINCT buoy_id FROM buoy_observation;"`
2. Export: `cat /home/keelando/envcan_wave/site/data/latest_buoy_v2.json | jq '.CRPILE'`

---

## Migration Notes

**Dec 11-12, 2024:** Separated tide data from buoy database
- Tide predictions now go to `tide_data.sqlite`
- Wave/wind observations stay in `buoy_data.sqlite`
- Prevents future timestamp conflicts

**Dec 12, 2024:** Fixed timezone issue
- API now uses Pacific time instead of UTC
- Resolved "no data" issue
