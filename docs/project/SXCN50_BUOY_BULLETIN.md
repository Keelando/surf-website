# SXCN Bulletin Family — Feature Planning

**Status as of 2026-09-07:** partly shipped. SXCN23/25/26 have been subscribed
and parsed since 2026-08; SXCN24 was added 2026-09-07 after the "skip" decision
below turned out to be wrong. What remains undone is the interesting half —
the extra fields and the buoy bulletin:

| Step | State |
|---|---|
| Subscribe SXCN23/25/26 | done |
| Subscribe SXCN24 | done 2026-09-07 — see the correction below |
| Lightstation SXCN parser (wind, sea, swell) | done — `parse_lightstation.py` |
| **Extra fields: visibility, cloud, pressure, sea temp** | **not done** — parsed past and dropped; needs columns on `lightstation_observation` |
| **Add Triple Island to `stations.json`** | **not done** — it reports on SXCN23 and lands on the page in a trailing region section |
| **SXCN50 buoy parser** | **not done** — files collected, nothing reads them |
| **Decide which of the 12 extra buoys to add** | **not done** |

The live description of what we ingest is in `docs/DATA_FEEDS.md`; this file is
the planning record and the format reference.

## Discovery (2026-04-11)

Found a family of EC text bulletins from CWVR under the `SX` topic prefix containing lightstation observations and buoy summaries.

```
URL pattern: https://dd.weather.gc.ca/today/bulletins/alphanumeric/YYYYMMDD/SX/CWVR/HH/<bulletin>_CWVR_DDHHMM___<id>
Topic prefix: SX (not FI as originally assumed for lightstation sr3 work)
Source station: CWVR (Vancouver)
Published: hourly directories, multiple times per day
```

## Bulletin Inventory

| Bulletin | Region | Stations | Action |
|----------|--------|----------|--------|
| **SXCN23** | North Coast / Hecate | Green, **Triple (new!)**, Bonilla, Langara, Boat Bluff, McInnes, Ivory, Dryad, Addenbroke | **Subscribe** — adds Triple Island |
| **SXCN24** | Central Coast / N. Island | Chatham, Scarlett, Pine Island, Egg Island, Cape Scott, Quatsino | ~~Skip — all already covered by FPCN61~~ → **Subscribed 2026-09-07.** The premise was wrong: FPCN61 does *not* carry Egg Island, which is why that station had never produced a single observation, and Chatham Point reached us only sparsely. |
| **SXCN25** | WCVI South (Tofino area) | Nootka, Estevan, Lennard, Cape Beale | **Subscribe** — supplements with visibility, cloud, pressure, sea temp |
| **SXCN26** | Georgia Strait / S. Coast | Chrome, Merry, Entrance, Trial Island | **Subscribe** — supplements with visibility, cloud cover |
| **SXCN50** | All BC buoys | 17 buoys (Nomads, Dixon, Hecate, Moresby, Dellwood, plus our existing 5) | **Subscribe** — huge expansion potential |

## Strategy

**Keep FPCN61 HTTP polling as the backbone** — it works, provides core wind/sea/swell data for all 19 lightstations.

**Use SXCN bulletins to supplement:**
1. Extra fields not in FPCN61 (visibility, cloud cover, pressure, sea water temp, weather remarks)
2. New stations (Triple Island via SXCN23)
3. All-BC buoy summary (SXCN50) — potential to add 13 buoys we don't currently track

## Sample Bulletin Formats

### SXCN23 — Lightstation Observations (North Coast)
```
SXCN23 CWVR 110530
VAJ

GREEN         OVC 15 NW14E 3FT MDT LO S
TRIPLE        N/A
BONILLA       CLDY 15 NW14E 3FT MDT LO S
LANGARA       PC 15 NW19 3FT MDT LO W
BOAT BLUFF    PC 15 CLM RPLD
MCINNES       PC 15 NW02E RPLD LO W
IVORY         PC 15 CLM RPLD LO SW
DRYAD         PC 15 CLM SMTH
ADDENBROKE    PC 15 NW09E 2FT CHP LO W
```

### SXCN25 — Lightstation Observations (WCVI / Tofino area)
```
SXCN25 CWVR 110540
VAE

NOOTKA        OVC 12 NE11E 2FT CHP LO SW OCNL RW-
ESTEVAN       CLDY 15 NW03E 1FT CHP LO SW 1006.8F
LENNARD       CLDY 15 N04E 1FT CHP LO SW
CAPE BEALE    CLDY 15 NW02E 1FT CHP LO SW
```

### SXCN26 — Lightstation Observations (Georgia Strait)
```
SXCN26 CWVR 110540
VAI

CHROME        OVC 15 NW02E RPLD
MERRY         OVC 15 NW04E RPLD
ENTRANCE      NOT AVAILABLE
TRIAL IS      CLDY 15 SW06E RPLD
```

### SXCN50 — All BC Buoys Summary
```
NAME OF BUOY              HHMM  WIND PK  TEMP PRESS  T HW(m) S TEMP
------------------------- ----  ---- --- ---- ------ - ----- ------
NORTH NOMAD               2000  NW18 022   6  1017.1 F  1.8    5.8
MIDDLE NOMAD              2000  N 20G025   9  1015.4 R  3.1    7.0
...
HALIBUT BANK              2000  E 12 014  11  1005.8 R  0.2   10.7
SENTRY SHOAL              2000  SE10 012  11  1004.3 R  0.1   10.7
GEORGIA STRAIT            2000  E 08 010  11  1006.1 R  0.1   10.4
```

## Lightstation SXCN Format Reference

```
STATION       CLOUD VIS WIND SEA SWELL [PRESSURE] [WEATHER]
```

| Field | Examples | Notes |
|-------|----------|-------|
| Cloud cover | `OVC`, `CLDY`, `PC` | Overcast, Cloudy, Partly Cloudy |
| Visibility (nm) | `15`, `12`, `10` | Nautical miles |
| Wind | `NW14E`, `CLM`, `SW06E` | Dir + speed (kt), E=estimated, CLM=calm |
| Sea state | `3FT MDT`, `RPLD`, `SMTH`, `1FT CHP` | Height + condition (RIPPLED, SMOOTH, CHOPPY, MODERATE) |
| Swell | `LO S`, `LO SW`, `LO W` | Intensity + direction (LO=low, MDT=moderate) |
| Pressure | `1006.8F` | hPa + tendency (F/R/M) — not always present |
| Sea water temp | `SWT 8.8` | °C — rare, seen on Pine Island |
| Weather | `OCNL RW-` | Occasional light rain showers |
| Unavailable | `N/A`, `NOT AVAILABLE` | Station not reporting |

## SXCN50 Buoy Format Reference

| Column | Description | Notes |
|--------|-------------|-------|
| NAME OF BUOY | Buoy name | Fixed-width text |
| HHMM | Observation time (UTC) | May differ from bulletin time |
| WIND | Direction + speed (knots) | e.g. `NW18`, `N 20G025` (G=gust) |
| PK | Peak wind (knots) | |
| TEMP | Air temperature (°C) | `mm` = missing |
| PRESS | Pressure (hPa) | |
| T | Tendency (F=falling, R=rising, M=mixed) | |
| HW(m) | Wave height (metres) | |
| S TEMP | Sea surface temperature (°C) | |

## Buoys NOT Currently Tracked (SXCN50)

These buoys appear in SXCN50 but are not in our system:

- **North Nomad** — North Pacific
- **Middle Nomad** — North Pacific
- **South Nomad** — North Pacific
- **West Dixon Entrance** — Northern BC
- **Central Dixon Entrance** — Northern BC
- **Nanakwa Shoal** — Central BC coast
- **North Hecate Strait** — Hecate Strait
- **South Hecate Strait** — Hecate Strait
- **West Sea Otter** — Central coast
- **West Moresby** — Haida Gwaii
- **South Moresby** — Haida Gwaii
- **East Dellwood** — Central coast
- **South Brooks** — West coast Vancouver Island

## sr3 Subscription

The existing `bc_lightstation_obs.conf` uses topic prefix `FI` and gets 0 messages.
Needs to be updated to use `SX`:

```conf
subtopic *.WXO-DD.bulletins.alphanumeric.*.SX.CWVR.#

accept .*SXCN23.*
accept .*SXCN24.*
accept .*SXCN25.*
accept .*SXCN26.*
accept .*SXCN50.*
reject .*
```

Or fetch via HTTP polling (like FPCN61) if sr3 topic doesn't work.

## Implementation Steps

1. ~~**Fix sr3 config** — change topic from `FI` to `SX`, add accept/reject filters~~ *Done.*
2. ~~**Verify messages arrive** — restart service, check logs~~ *Done.*
3. **Write SXCN lightstation parser** — *partly done*: wind, sea and swell are
   extracted; cloud, visibility, pressure and weather are parsed past and
   dropped for want of anywhere to put them (step 5).
4. **Write SXCN50 buoy parser** — not started. Files are collected.
5. **Extend lightstation DB schema** — add columns for visibility, cloud_cover,
   pressure, sea_water_temp, weather. This is the blocker for step 3's
   remainder, and the reason to prefer SXCN over chasing FICN: the fields are
   already arriving.
6. **Add Triple Island** to stations.json — it has been reporting on SXCN23 the
   whole time. Until it is registered it renders in a trailing region section
   and both lightstation exports log a warning naming it.
7. **Decide which new buoys to add** from SXCN50
