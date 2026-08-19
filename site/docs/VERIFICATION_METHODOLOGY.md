# Storm Surge Forecast Verification Methodology

## Purpose

Forecast verification evaluates model skill by comparing predictions that were
issued ahead of time against what was later observed. For storm surge this
reveals model accuracy at the two-to-three-day lead time when maritime
operators, emergency managers, and coastal communities make operational
decisions.

## Verification, not hindcast (renamed 2026-08-19)

This was called "hindcast analysis" until 2026-08-19. That was the wrong word,
and the distinction matters:

- A **hindcast** re-runs a model over past dates, usually with better forcing
  (analysed winds and pressures) than any real forecast had. It answers "how
  good is the model physics?"
- **Verification** compares forecasts that were genuinely *issued in advance*
  and archived at the time. It answers "how good was the forecast you could
  actually have acted on?"

Everything here is the second kind: `fetch_storm_surge.py` archives each 00Z
run as it arrives, and nothing is ever re-run. "Backcast" is not a term of art
in this field and is not used. The old name survives in historical documents
(changelogs, worklogs), where it is a record of what the file used to be
called, and in nothing that describes current behaviour.

## Data Windows

### Forecast Data: 12 days (today + 11 days back)

**What it shows:** Storm surge predictions as issued **56-79 hours ahead**
- Based on Environment Canada's GDSPS **00Z** model run
- `forecast_archive` stores `forecast_run_time` as a bare date, so
  `valid_time - forecast_run_time` counts hours from midnight UTC. Because the
  archived run is 00Z, midnight *is* the run instant and those hours are
  genuine lead times — no offset to remember.
- Includes forecast runs from ~14 days ago to capture predictions for the full 12-day valid time range

> **Corrected 2026-08-19.** Three different lead figures were in circulation
> and none matched the query, which has always used `hours_ahead BETWEEN 56 AND
> 79`. This doc said "38-61 hours / 12Z run / hours 50-73 from midnight"; the
> exported JSON declared `forecast_horizon_hours: "38-61"` and an 18Z run; the
> page told readers "48 hours in advance" from "the 12Z model run". The 56-79 h
> figure above is read off the code and confirmed against the exported data.

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

**Last updated:** 2026-08-19
**Applies to:** Storm surge forecast verification (`storm_surge.html`, verification section)
