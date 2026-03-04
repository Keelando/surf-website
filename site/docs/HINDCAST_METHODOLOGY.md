# Storm Surge Hindcast Analysis Methodology

## Purpose

Hindcast analysis evaluates forecast skill by comparing historical predictions against actual observations. For storm surge forecasting, this reveals model accuracy at the critical ~48-hour lead time when maritime operators, emergency managers, and coastal communities make operational decisions.

## Data Windows

### Forecast Data: 12 days (today + 11 days back)

**What it shows:** Storm surge predictions made 38-61 hours in advance
- Based on Environment Canada's GDSPS 12Z model run
- Corresponds to hours 50-73 from midnight UTC (the 38-61 hour window from the 12:00 UTC run)
- Includes forecast runs from ~14 days ago to capture predictions for the full 12-day valid time range

**Example:** To display predictions FOR Nov 12-23, we include forecast runs FROM Nov 10-21. The ~2-day offset accounts for the forecast lead time.

### Observed Data: 10 days (today + 9 days back)

**What it shows:** Actual storm surge calculated from tide observations

**Calculation:**
```
observed_surge = tide_observation - astronomical_tide_prediction
```

This represents the actual water level anomaly caused by meteorological forcing (wind stress, atmospheric pressure).

**Why shorter than forecasts?** Focuses visualization on recent model performance without excessive clutter. Ten days provides sufficient statistical context while maintaining readability.

## Time Alignment

- **All windows:** Aligned to Pacific timezone midnight boundaries (00:00 PT to 23:59 PT)
- **Future data:** No predictions shown beyond 23:59 PT today (prevents displaying unverified forecasts)
- **Timezone handling:** Database stores UTC; exports and frontend convert to Pacific for display

## Scientific Rationale

**48-hour lead time:**
- Balances actionable forecast range with model skill
- Critical decision window for maritime operations, flood preparedness, and coastal infrastructure protection
- Beyond 72 hours, atmospheric model uncertainty degrades surge prediction accuracy

**12-day window:**
- Captures 1-2 complete synoptic weather pattern cycles (typical 3-7 day periodicity)
- Sufficient sample size to identify systematic forecast biases vs. random errors
- Long enough to observe multi-day surge events (e.g., extended onshore wind patterns)

**10-day observed window:**
- Maintains temporal overlap with forecast data for comparison
- Reduces chart complexity while preserving statistical relevance
- Aligns with operational forecast verification standards

## Visualization Interpretation

**Chart elements:**
- **Colored lines:** Each represents predictions from a different forecast run date
- **Black line:** Calculated observed surge (ground truth)
- **Convergence:** Multiple forecast runs agreeing suggests high confidence
- **Divergence:** Forecast spread indicates uncertainty or regime change (e.g., approaching storm)
- **Vertical gridlines:** Pacific timezone midnight boundaries for day separation

**Performance indicators:**
- Colored lines tracking close to black line = good forecast skill
- Systematic offset = potential model bias
- High variability = challenging forecast scenario (rapidly evolving conditions)

## Technical Implementation Notes

**Database filtering:**
- SQL queries filter by ISO 8601 format (`YYYY-MM-DDTHH:MM:SSZ`) to match stored format
- Timezone conversions use `pytz` for Pacific timezone handling including DST transitions
- Forecast run dates stored as date-only (`YYYY-MM-DD`), valid times as full timestamps

**Frontend filtering:**
- JavaScript applies same date boundaries as backend (failsafe for stale/cached data)
- Dynamic calculation ensures correct window as time advances
- ECharts library handles visualization with automatic axis scaling

---

**Last updated:** 2025-11-23
**Applies to:** Storm surge hindcast visualization (`storm_surge.html`, hindcast section)
