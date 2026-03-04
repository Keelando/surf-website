# Tide Display Module TODO

## High Priority

(No pending items)

---

## Completed Items

### Fix "Now" Dot Interpolation on Charts (2026-01-22)
- ✅ Simplified "Now" dot logic for geodetic stations (Surrey)
- ✅ Geodetic stations now use straightforward interpolation without residual calculation
- ✅ Non-geodetic stations (DFO) still calculate residual from latest observation
- ✅ Added `isGeodetic` check to skip unnecessary residual computation for Surrey data

### Storm Surge Card Alignment (2026-01-12)
- ✅ Fixed geodetic station labels to show "Residual" instead of misleading labels
- ✅ Separated geodetic (Surrey) and DFO data paths completely
- ✅ Added "Today's Peak Tide Forecast" to all applicable stations
- ✅ Created reusable `generatePeakForecastHtml()` helper function
