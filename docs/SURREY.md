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

Add to crontab:
```bash
SURREY_API_USERNAME=surreyrain
SURREY_API_PASSWORD=surreyrain
```

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

### Site IDs

- **Crescent Beach Ocean:** `8f8c62d5-5096-4b3c-bf56-5e6ba25e08ac`
- **Crescent Channel Ocean:** `bb4f8b3d-7df2-4d37-b6dd-afc46ffb3097`

### Wave Channels

| Channel ID | Parameter | Unit | Update Frequency |
|------------|-----------|------|------------------|
| `a73636ac-...` | Significant Wave Height | m | 10 min |
| `4f4a2e62-...` | Peak Wave Period | s | 10 min |
| `8e9ddb87-...` | Average Wave Period | s | 10 min |
| `7bc9ad14-...` | Wave Direction | degrees | 10 min |

### Wind Channels

| Channel ID | Parameter | Unit | Update Frequency |
|------------|-----------|------|------------------|
| `f3a9b121-...` | Wind Speed | m/s | 10 min |
| `2d8f4c91-...` | Wind Gust | m/s | 10 min |
| `c5e7a832-...` | Wind Direction | degrees | 10 min |

### Tide Channels

| Channel ID | Parameter | Unit | Update Frequency |
|------------|-----------|------|------------------|
| `d4b2f891-...` | Water Level (Observed) | m CGVD28 | 5 min |
| `9a3c5e17-...` | Water Level (Predicted) | m CGVD28 | 5 min |

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
2. Export: `cat /home/keelando/site/data/latest_buoy_v2.json | jq '.CRPILE'`

---

## Migration Notes

**Dec 11-12, 2024:** Separated tide data from buoy database
- Tide predictions now go to `tide_data.sqlite`
- Wave/wind observations stay in `buoy_data.sqlite`
- Prevents future timestamp conflicts

**Dec 12, 2024:** Fixed timezone issue
- API now uses Pacific time instead of UTC
- Resolved "no data" issue
