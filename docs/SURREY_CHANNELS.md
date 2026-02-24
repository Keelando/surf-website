# Surrey FlowWorks — Active Channel Reference

Channels actively used by the system. For the full list of all available channels
(88 for Crescent Beach, 79 for Crescent Channel), see
`archive/docs/SURREY_CHANNELS_REFERENCE.md`.

---

## Crescent Beach Ocean — Site 20182 (CRPILE)

| Channel | Name | Unit | Role |
|---------|------|------|------|
| `2296` | Anderra - CGVD28 GVRD Stage_10min | m | **Observation** — 10-min averaged Anderra stage |
| `2620` | Tidal_Prediction_CGVD28_GVRD | m | **Prediction** — astronomical tide |
| `2414` | Tidal Residual | m | **Storm surge** — observed minus predicted |

## Crescent Channel Ocean — Site 20183 (CRCHAN)

| Channel | Name | Unit | Role |
|---------|------|------|------|
| `2279` | PT - CGVD28 GVRD Stage | m | **Observation** — pressure transducer stage |
| `2621` | Tidal_Prediction_CGVD28_GVRD | m | **Prediction** — astronomical tide |
| `3660` | Tidal Residual | m | **Storm surge** — observed minus predicted (added 2026-01-12) |

---

## Datum Notes

All channels use **CGVD28 GVRD** (Canadian Geodetic Vertical Datum 1928, Greater Vancouver Regional District).

- **Not directly comparable to DFO Chart Datum** — the offset varies by location (~1–3 m)
- Best use: comparing observed vs predicted to derive the tidal residual (storm surge)
- Tidal residual > 0 = water higher than predicted (positive surge)
- Tidal residual < 0 = water lower than predicted (inverse surge)
- Do **not** display absolute Surrey water levels alongside DFO water levels without a datum correction

## Channel Selection Notes

**2296 vs 2004** (Crescent Beach observation): Channel 2296 is the 10-minute averaged
Anderra stage in CGVD28 GVRD. Channel 2004 (`TideLevel_Anderra`) was originally tested
but showed inconsistent offsets and was deprecated. See
`archive/deprecated_scripts/compare_surrey_dfo_water_levels.py`.

**2279** (Crescent Channel observation): Pressure transducer stage in CGVD28 GVRD.
Consistent and well-behaved; preferred over the radar-based alternatives.
