# Pressure & Frontal Passage Page — Planning

**Status:** Brainstorm / planning phase
**Started:** 2026-04-13

---

## Concept

A new page that visualizes frontal passages using real-time pressure readings and tendencies across the station network. The "local edge" on weather forecasts — see a front approaching hours before it arrives by watching pressure fall propagate across stations from west to east.

## Approach: Isallobaric Analysis

Based on how meteorologists actually do this (not reinventing the wheel):

**Isallobars** are contours of equal pressure *change* drawn spatially on a map. A front shows up as a dipole: **katallobars** (falling pressure) ahead of the front, **anallobars** (rising pressure) behind it. The front sits between them.

### Primary View: Map

A Leaflet map of stations, each colored/sized by its current 3-hour pressure tendency:
- Red = falling (katallobar), intensity by magnitude
- Blue = rising (anallobar), intensity by magnitude
- Grey/neutral = steady

A front propagating W→E would appear as a red-to-blue gradient sweeping across the map.

### Secondary Views (to explore)

- **Multi-station pressure overlay chart** — all traces on one time axis, 48h window
- **User-selectable frontal axis** — let user pick a direction vector, project stations onto it, reorder/time-shift traces to see if they collapse (confirms frontal direction + speed). Default to W→E.
- **Pressure tendency heatmap** — station rows ordered by longitude, time on X axis, color = tendency. A front appears as a diagonal colored band.

### Key Signatures to Surface

- **Pressure tendency**: 3h change (EC buoys report directly; compute for wind stations)
- **Rate of fall**: > 1 hPa/hr = strong front approaching; > 3.5 hPa/3h = "rapidly falling"
- **Wind shift**: overlay wind direction — cold fronts show pressure trough + veer SW→NW
- **Temperature drop**: correlated with frontal passage

---

## Data Inventory

### What We Already Have

**Buoy timeseries (48h, hourly, in `buoy_timeseries_48h.json`):**
- `pressure` — station pressure (hPa)
- `pressure_msl` — mean sea level pressure (hPa)
- `pressure_trend_amount` — 3h pressure change (hPa) — EC buoys only
- `pressure_trend_char` — WMO tendency code (0-8) — EC buoys only

**Wind timeseries (48h, 10-min, in `wind_timeseries_48hr.json`):**
- `pressure` — station pressure (hPa)

**Latest snapshots** also have pressure in `latest_buoy_v2.json` and `latest_wind.json`.

### Current Stations With Pressure (~22)

**Buoys (7):**
| ID | Name | Lat | Lon |
|----|------|-----|-----|
| 4600146 | Halibut Bank | 49.34 | -123.73 |
| 4600303 | S. Georgia Strait | 49.03 | -123.43 |
| 4600304 | English Bay | 49.30 | -123.36 |
| 4600131 | Sentry Shoal | 49.92 | -124.92 |
| 4600206 | La Perouse Bank | 48.83 | -126.00 |
| 46087 | Neah Bay | 48.50 | -124.73 |
| 46088 | New Dungeness | 48.33 | -123.17 |

**Wind stations (15):**
| ID | Name | Lat | Lon |
|----|------|-----|-----|
| CWGT | Sisters Islets | 49.49 | -124.43 |
| CWGB | Ballenas | 49.35 | -124.16 |
| CWEL | Entrance Island | 49.21 | -123.81 |
| CWAS | Pam Rocks | 49.49 | -123.30 |
| CWSB | Point Atkinson | 49.33 | -123.27 |
| CVTF | Tsawwassen | 49.01 | -123.18 |
| CWVF | Sand Heads | 49.11 | -123.30 |
| CWEZ | Saturna Island | 48.78 | -123.05 |
| CWQK | Race Rocks | 48.30 | -123.53 |
| CYVR | YVR Airport | 49.19 | -123.18 |
| CZBB | Boundary Bay Airport | 49.07 | -123.01 |
| JERICHO | Jericho Sailing Centre | 49.28 | -123.20 |
| KBLI | Bellingham Airport | 48.80 | -122.53 |
| KORS | Orcas Island Airport | 48.71 | -122.91 |
| whiterock_east | White Rock East Beach | 49.02 | -122.79 |
| SISW1 | Smith Island | 48.32 | -122.83 |

### Spatial Coverage Problem

Current stations cluster heavily around Vancouver (-123.0 to -123.4°). For isallobar analysis, we need stations spread across the frontal approach path. Effective "independent" spatial groups are roughly:
- Outer ocean (La Perouse only, ~-126°)
- Outer strait (Neah Bay, Sentry Shoal, ~-124.7 to -124.9°)
- Mid strait (Halibut, Entrance Is, Ballenas, ~-123.7 to -124.4°)
- Inner/urban (the Vancouver blob, ~-123.0 to -123.4°)
- US side (Bellingham, Orcas, Smith Is, New Dungeness, ~-122.5 to -123.2°)

---

## Stations To Add

### Priority 1: NOAA C-MAN / NDBC (plug-and-play with existing `fetch_noaa_land.py`)

| Station | Name | Lat | Lon | Why |
|---------|------|-----|-----|-----|
| DESW1 | Destruction Island | 47.68 | -124.49 | Outer WA coast — early warning |
| LAPW1 | La Push | 47.91 | -124.64 | Outer WA coast |
| WPOW1 | West Point, Seattle | 47.66 | -122.44 | South Puget Sound extent |
| FRDW1 | Friday Harbor | 48.55 | -123.01 | San Juan Islands mid-point |
| 46041 | Cape Elizabeth | 47.35 | -124.74 | Far offshore — earliest warning, hours of lead time |
| 46029 | Columbia River Bar | 46.16 | -124.49 | Southern approach tracking |

### Priority 2: EC SWOB-ML (need to add sr3 subscription or HTTP polling)

| Station | Name | Lat | Lon | Why |
|---------|------|-----|-----|-----|
| CXFA | Fanny Island | 50.45 | -125.99 | North outer coast, fills huge gap |
| TBD | Comox (CYQQ) | ~49.67 | ~-124.93 | Up-island coverage |
| TBD | Campbell River area | ~50.04 | ~-125.25 | Northern strait |

### Priority 3: Lightstations (3h resolution — coarser but fills outer coast)

Already in the system, just need to confirm pressure is being parsed/exported:
- Chrome Island, Merry Island (mid-strait west)
- Cape Beale, Lennard Island, Estevan Point (WCVI)
- Cape Scott, Quatsino (north island)

### Note

46267 (Angeles Point / Port Angeles) is already in our system but currently down. Would be valuable when it comes back — right in the strait.

---

## Technical Considerations

- **MSLP normalization**: Buoys already report MSLP. Wind stations report station pressure — near sea level so close, but should normalize for accuracy.
- **Tendency computation**: EC buoys report `pressure_trend_amount` directly. For wind stations and NOAA, compute from timeseries (current - 3h ago).
- **48h window** is decent for seeing a single front. 5-7 day window would show full synoptic cycles — would need a new export script for longer history.
- **3h lightstation data** is coarse but still useful for the map view (color by tendency). Less useful for detailed time-series charts.
- **No new databases needed** — pressure lives in existing buoy/wind/lightstation SQLite DBs.
- **New JSON export** likely needed — a combined pressure-focused export pulling from all three DBs.

---

## Open Questions

1. Best way to handle mixed temporal resolution (10-min wind vs 1h buoy vs 3h lightstation) on the same map?
2. Should tendency be computed server-side (in export) or client-side (from timeseries)?
3. How to present the "frontal axis selector" UX — compass dial? drag on map? presets (W→E, SW→NE)?
4. Is 48h enough history, or do we need a longer export for the overlay chart?
5. Should we include temperature and wind direction shift indicators alongside pressure?
