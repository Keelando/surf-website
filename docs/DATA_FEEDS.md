# External Data Feeds

All external APIs and data sources consumed by this system. A good starting point
for new developers getting oriented.

## Delivery Methods

Data arrives via two mechanisms:

- **AMQP push (sr3/Sarracenia)** — Environment Canada data only. Files are
  pushed to us as soon as EC publishes them. Configs live in `config/sr3/`
  (source of truth); deployed to `~/.config/sr3/subscribe/` and run as systemd
  services. See `docs/SR3_MANAGEMENT.md` for operational details.

- **HTTP polling (cron)** — Everything else: NOAA, DFO tides, Surrey FlowWorks,
  Jericho, White Rock, webcams, and (currently) lightstation FPCN61 bulletins.
  Fetch scripts in `scripts/fetch/`, scheduled via crontab.

---

## MSC Datamart — dd.weather.gc.ca

Environment Canada data delivered via **sr3 AMQP push**. Configs in `config/sr3/`
(source of truth — edit there, then deploy to `~/.config/sr3/subscribe/`).

### EC Wave Buoys — SWOB-ML XML

| Station | ID | Location |
|---------|----|----------|
| Halibut Bank | `4600146` | 49.337°N 123.731°W |
| Southern Strait of Georgia | `4600303` | 49.03°N 123.43°W |
| English Bay | `4600304` | 49.3°N 123.36°W |
| Sentry Shoal | `4600131` | 49.917°N 124.917°W |

**Format:** SWOB-ML XML
**Config:** `config/sr3/bc_buoys.conf`
**Parsed by:** `scripts/parse/buoy_to_influx_sqlite.py`
**Stored in:** `buoy_data.sqlite`
**AMQP subtopic pattern:** `*.WXO-DD.observations.swob-ml.marine.moored-buoys.*.{ID}.#`

### EC Wind Stations — SWOB-ML XML

| Station | ID | Location |
|---------|----|----------|
| Sisters Islets | `CWGT` | Strait of Georgia |
| Ballenas | `CWGB` | Strait of Georgia |
| Entrance Island | `CWEL` | Nanaimo area |
| Point Atkinson | `CWSB` | West Vancouver |
| Tsawwassen | `CVTF` | Delta |
| Sand Heads | `CWVF` | Fraser River mouth |
| Saturna | `CWEZ` | Gulf Islands |
| Race Rocks | `CWQK` | Juan de Fuca Strait |
| Pam Rocks | `CWAS` | Howe Sound |
| YVR Airport | `CYVR` | Richmond (manual obs) |
| Boundary Bay Airport | `CZBB` | Delta |
| Tofino Airport | `CYAZ` | West Coast Vancouver Island |
| Kelp Reefs | `CWZO` | Haro Strait (wind only — no temp/pressure) |
| Discovery Island | `CWDR` | Haro Strait |
| Victoria Gonzales | `CWLM` | Victoria (hilltop, 65 m — see note below) |

`config/stations.json` is the source of truth for this list; the table above
is a reading aid, so check the registry before trusting it.

**Elevation caveat:** `CWLM` sits 65 m up on Gonzales Heights. Its
`pressure_hpa` (station pressure) runs ~8 hPa below the sea-level sites — the
SWOB feed also carries `mslp`, which we store as `pressure_mslp_hpa`.

**Config:** `config/sr3/bc_wind_stations.conf`
**Parsed by:** `scripts/parse/wind_to_sqlite.py`
**Stored in:** `wind_data.sqlite`
**AMQP subtopic pattern:** `*.observations.swob-ml.*.{ID}-AUTO*.#`

### Marine Weather Forecasts — XML

**Zone:** `m0000028` — Strait of Georgia (covers both north and south of Nanaimo zones)
**Config:** `config/sr3/marine_forecast.conf`
**Parsed by:** `scripts/parse/parse_marine_forecast.py`
**AMQP subtopic:** `*.WXO-DD.marine_weather.pacific.#`

### Lightstation Bulletins

Marine lightstation observations issued every 3 hours. Two bulletin families:

**FPCN61 (current observations)** — HTTP polled (not yet on sr3)
```
https://dd.weather.gc.ca/today/bulletins/alphanumeric/YYYYMMDD/FP/CWVR/HH/
```
**Fetched by:** `scripts/fetch/fetch_lightstation.py` (cron, hourly)
**Parsed by:** `scripts/parse/parse_lightstation.py`
**Stored in:** `lightstation_data.sqlite`
**Covers:** 19 stations (Strait of Georgia, Central Coast, Hecate Strait, north WCVI)

**FICN31/32/33 (regional observations)** — sr3 AMQP (new, pending parser)
| Bulletin | Region | Key Stations |
|----------|--------|--------------|
| FICN31 | North & Central Coast | Langara, Bonilla, McInnes, Cape Scott |
| FICN32 | Georgia Strait / South Coast | Chrome Island, Merry Island, Trial Island |
| FICN33 | WCVI South | Lennard Island, Estevan Point, Cape Beale |

**Config:** `config/sr3/bc_lightstation_obs.conf`
**AMQP subtopic:** `*.WXO-DD.bulletins.alphanumeric.*.FI.CWVR.#`
**Data dir:** `data/lightstation_ficn/`
**Status:** Subscription config created, awaiting deployment and parser integration.
FICN33 is the key bulletin — it contains the west coast VI stations missing from FPCN61.

---

## EC GeoMet — geo.weather.gc.ca

### GDSPS Storm Surge Forecast

**URL:** `https://geo.weather.gc.ca/geomet`
**Layer:** `GDSPS_15km_StormSurge`
**Protocol:** WMS 1.3.0 via OWSLib
**Fetched by:** `scripts/fetch/fetch_storm_surge.py`
**Schedule:** Every 6 hours, after 00Z and 12Z model runs (cron at 1, 7, 13, 19 UTC)
**Stored in:** `storm_surge_forecast.sqlite` + exported JSON to `site/data/storm_surge/`

---

## NOAA NDBC — ndbc.noaa.gov

| Station | ID | Location | Data |
|---------|-----|----------|------|
| Neah Bay | `46087` | 48.495°N 124.728°W | Met + spectral wave (swell/wind-wave separation) |
| New Dungeness / Hein Bank | `46088` | 48.333°N 123.167°W | Met + spectral wave |

**Fetched by:** `scripts/fetch/fetch_noaa_buoy.py`
**Schedule:** Every 20 minutes
**Stored in:** `buoy_data.sqlite`

**URLs:**
```
Met (5-day):   https://www.ndbc.noaa.gov/data/5day2/{ID}_5day.txt
Met (realtime): https://www.ndbc.noaa.gov/data/realtime2/{ID}.txt
Spectral:       https://www.ndbc.noaa.gov/data/realtime2/{ID}.spec
```

---

## DFO IWLS — api-iwls.dfo-mpo.gc.ca

Tide observations and predictions for DFO water level stations.

**Base URL:** `https://api-iwls.dfo-mpo.gc.ca/api/v1`
**Auth:** None required (public API)
**Fetched by:** `scripts/parse/tide_to_sqlite.py` (fetches and stores)
**Stored in:** `tide_data.sqlite`
**Station list:** `config/stations.json` (`tides` section, via `lib/stations.py`)

**Series codes:**
| Code | Description | Fetch schedule |
|------|-------------|----------------|
| `wlo` | Real-time observations (6-min intervals) | Every 30 min |
| `wlp` | Astronomical tide predictions | Daily at 00:10 UTC |
| `wlp-hilo` | Pre-calculated high/low events | Daily at 00:15 UTC |

---

## Surrey FlowWorks API

Wave and geodetic tide data from Boundary Bay instrument sites.

**Auth:** `SURREY_API_USERNAME` / `SURREY_API_PASSWORD` — in `config/.env`
(gitignored), read via `lib/env.py`. Never in `config/crontab.txt`; see
`docs/SECRETS.md`.
**Timezone:** API expects **Pacific time** (not UTC) — see `docs/SURREY.md`
**Channel reference:** `docs/SURREY_CHANNELS.md`

| Station | Site ID | Buoy ID | Data |
|---------|---------|---------|------|
| Crescent Beach Ocean | `20182` | `CRPILE` | Wave, wind, geodetic tide |
| Crescent Channel Ocean | `20183` | `CRCHAN` | Wave, geodetic tide |

**Scripts:**
- Wave/wind: `scripts/fetch/fetch_surrey_wave_v2.py` (every 20 min)
- Tides: `scripts/fetch/fetch_surrey_tides.py` (obs every 20 min, predictions daily)

### Air temperature runs hot in daylight (solar loading)

**Do not treat FlowWorks `air_temp` as a shaded air temperature.** The three
FlowWorks sensors (`COLEB`, `CRPILE`, `CRCHAN`) appear to sit in unshielded
enclosures, so they read high whenever the sun is on them. This is not a
constant offset that can be subtracted — it is near zero at night and peaks
mid-afternoon.

Measured over the 10 days to 2026-08-13, against Sand Heads (`CWVF`, a
marine EC station, the fair reference for over-water siting):

| Station | Overnight bias | Midday bias | Daytime excess | Peak in record |
|---------|---------------|-------------|----------------|----------------|
| `COLEB` | −0.3 °C | +18.8 °C | **+19.1 °C** | 42.6 °C |
| `CRPILE` | +0.6 °C | +7.6 °C | +7.0 °C | 33.5 °C |
| `CRCHAN` | +1.0 °C | +6.5 °C | +5.5 °C | 28.8 °C |

All three **agree with the marine reference to within ~1 °C overnight** and
diverge only in daylight — the signature of solar heating rather than a
miscalibrated or genuinely warmer site. `COLEB` is the extreme case: a mean
of 38.6 °C at 16:00 PDT and a 22.6 °C diurnal swing, versus 3.0 °C at Sand
Heads. Its 42.6 °C peak is not an air temperature.

Two caveats on the numbers above: `CRPILE`/`CRCHAN` sit near the beach, so
some of their +6–7 °C is genuine daytime land warming rather than the
enclosure; and comparing against a land station (`CZBB`) instead hides the
effect for the Crescent pair, because that reference has its own large
diurnal cycle. Sea temperature at `CRPILE` is unaffected (2.4 °C swing).

Reproduce with the hour-of-day query in `docs/COMMANDS.md`; wind, wave and
water-level channels from these sites show no such bias.

**Consequence for the Windy push:** `push_to_windy` deliberately sends no
`temp` parameter. On our own pages the reading carries a footnote; on Windy
it would be a bare public observation with nothing to qualify it. Wind is
the trustworthy signal from these stations, so wind is all we publish. A
time-of-day gate was considered and rejected — it would still publish a
number nobody should trust, just less often.

---

## Jericho Sailing Centre

Wind observations from the Jericho Sailing Centre anemometer (English Bay area).

**URL:** `https://jsca.bc.ca/main/downld02.txt`
**Format:** Tab-delimited text
**Auth:** None
**Fetched by:** `scripts/fetch/fetch_jericho_wind.py`
**Stored in:** `wind_data.sqlite`
