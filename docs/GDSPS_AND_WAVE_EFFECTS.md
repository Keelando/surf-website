# Storm Surge and Wave Effects: Technical Explanation

## What This System Predicts

The combined water level predictions provided by `export_combined_water_level.py` calculate:

```
Total Water Level = Astronomical Tide + Storm Surge (GDSPS)
```

**This is NOT the complete picture for coastal flooding risk.**

---

## Components of Coastal Water Level During Storms

### 1. Astronomical Tide (✅ Included)
**Source:** DFO IWLS (Integrated Water Level System)

- Predictable gravitational forcing from sun and moon
- Deterministic - known years in advance
- Typically 0-5m range in Salish Sea
- Independent of weather

### 2. Storm Surge (✅ Included)
**Source:** Environment Canada GDSPS (Global Deterministic Storm Surge Prediction System)

**What GDSPS predicts:**
- Static water level elevation caused by meteorological forcing:
  - **Wind stress** (wind blowing water toward/away from shore)
  - **Atmospheric pressure** (inverse barometer effect: low pressure = higher water)

**How it works:**
- GDSPS uses ocean circulation models (15km horizontal resolution)
- Driven by weather forecast inputs (wind, pressure fields)
- Simulates large-scale water movement
- Updates 2x daily (00Z, 12Z)
- Provides 10-day hourly forecasts

**Typical magnitudes (Salish Sea):**
- Calm conditions: -0.1 to +0.1m
- Moderate weather: -0.2 to +0.2m
- Significant storm: +0.3 to +0.5m
- Major storm event: +0.5 to +1.0m

**What GDSPS does NOT include:**
- ❌ Wave setup
- ❌ Wave runup
- ❌ Wave overtopping
- ❌ Localized wind setup in small bays
- ❌ Freshwater discharge effects

---

### 3. Wave Setup (❌ NOT Included - Critical Gap!)

**Definition:** Super-elevation of mean water level near the shore caused by breaking waves.

**Physical mechanism:**
- As waves approach shore and break, they transfer momentum to water column
- This creates a pressure gradient that elevates water level
- Accumulated effect of many breaking waves

**Typical magnitudes:**
- Protected areas (Vancouver Harbour): 0.05-0.10m
- Moderately exposed (Point Atkinson): 0.10-0.30m
- Exposed coasts (Tofino, Neah Bay): 0.20-0.50m
- Extreme storms on open coast: 0.50-1.0m

**Depends on:**
- Significant wave height (Hs)
- Beach/reef slope
- Offshore bathymetry
- Wave period

**Empirical formula (Stockdon et al. 2006):**
```
Wave Setup ≈ 0.35 × √(Hs × wavelength)
```

For example, 4m waves with 10s period → ~0.4m wave setup

---

### 4. Wave Runup (❌ NOT Included - Often Dominant!)

**Definition:** Maximum vertical elevation reached by individual wave swashes running up the beach/structure.

**Physical mechanism:**
- Individual waves rush up beach face
- Kinetic energy of breaking waves converted to potential energy
- Highly variable (statistical distribution)

**Typical magnitudes:**
- Protected areas: 0.1-0.5m
- Moderately exposed: 0.5-1.5m
- Exposed coasts during storms: 1.5-3.0m
- Major storm events: 3.0-6.0m+

**R2% (runup exceeded by 2% of waves):**
```
R2% ≈ 1.1 × (Wave Setup + Swash)
Swash ≈ √(Hs × wavelength) × (offshore slope)
```

**Depends critically on:**
- Wave height and period
- Beach/structure slope (steeper = higher runup)
- Roughness (rocks reduce runup vs. smooth surfaces)
- Wave direction relative to shore

---

### 5. Wave Overtopping (❌ NOT Included)

Volume of water splashing over coastal structures (dikes, seawalls) when waves exceed crest elevation.

**Key factors:**
- Structure height relative to water level
- Wave height
- Structure slope and roughness

---

## Total Coastal Flooding Risk

### Complete Formula

```
Actual Water Level at Shore =
    Astronomical Tide (✅)
  + Storm Surge (✅)
  + Wave Setup (❌)
  + Wave Runup (❌)
```

### Example Scenarios

#### Scenario 1: Protected Inner Harbour (Vancouver Harbour, Point Atkinson)

**During moderate storm (wind 30 knots, 1.5m waves):**
```
Astronomical Tide:  +4.0m  (high tide)
Storm Surge:        +0.3m  (GDSPS forecast)
Wave Setup:         +0.1m  (protected location)
Wave Runup:         +0.3m  (small waves)
────────────────────────────
TOTAL:              +4.7m

Our system predicts: 4.3m (missing 0.4m from waves)
Error: ~9%
```

**Risk:** Moderate - wave effects are secondary

#### Scenario 2: Exposed Outer Coast (Tofino, Neah Bay)

**During major storm (wind 50 knots, 6m waves):**
```
Astronomical Tide:  +2.5m  (high tide)
Storm Surge:        +0.8m  (GDSPS forecast)
Wave Setup:         +0.5m  (large breaking waves)
Wave Runup:         +3.0m  (R2% for 6m waves)
────────────────────────────
TOTAL:              +6.8m

Our system predicts: 3.3m (missing 3.5m from waves!)
Error: ~106%
```

**Risk:** CRITICAL - wave effects dominate, our predictions severely underestimate flooding risk!

---

## Why Separate Storm Surge from Wave Effects?

### Different Physical Processes

1. **Storm Surge** (what GDSPS models):
   - Large-scale ocean circulation
   - Driven by wind stress field and atmospheric pressure
   - Relatively smooth spatial/temporal variation
   - Predictable from weather forecasts
   - Modeled with ocean circulation models (NEMO, etc.)

2. **Wave Effects** (NOT in GDSPS):
   - Local wave transformation as waves approach shore
   - Shoaling, refraction, breaking
   - Highly variable in space (depends on local bathymetry)
   - Highly variable in time (individual wave events)
   - Requires wave models (SWAN, WaveWatch III) + runup formulas

### Computational Complexity

- **GDSPS resolution:** 15km horizontal
- **Wave setup modeling:** Needs <100m resolution near shore
- **Wave runup:** Needs detailed local bathymetry + structure geometry

Including wave effects in GDSPS would require:
- 100x finer grid resolution
- Coupled ocean-wave models
- Site-specific topography/bathymetry
- Orders of magnitude more computation

---

## Implications for Our System

### What We Provide (Good For)

✅ **Inland/protected areas:** Vancouver Harbour, Burrard Inlet, Fraser River
- Wave effects are minimal (<0.2m typically)
- Our predictions are reasonably accurate

✅ **General awareness:** Understanding when conditions are worse than normal
- Seeing when storm surge adds to high tide

✅ **Relative comparisons:** Comparing different days/times
- Even if absolute magnitude is wrong, trends are correct

### What We're Missing (Caution Required)

⚠️ **Exposed coasts:** Tofino, Neah Bay, outer coast locations
- Wave effects can be 2-3x larger than storm surge
- Our predictions will significantly underestimate actual water levels

⚠️ **Coastal flooding risk assessment:**
- Should NOT be used as sole input for flood warnings
- Should NOT be used for engineering design

⚠️ **Storm events with large waves:**
- The bigger the waves, the larger our error
- Most critical events (major storms) are where we're least accurate

---

## Recommendations

### For Frontend Display

1. **Show all components separately:**
   ```
   Predicted Water Level: 4.3m
   ├─ Astronomical Tide: 4.0m
   └─ Storm Surge:       0.3m

   ⚠️ Wave effects NOT included
   During storms, waves can add 0.5-3m+ depending on location
   ```

2. **Location-specific warnings:**
   - Protected areas: "Wave effects minimal (~0.1-0.2m)"
   - Exposed areas: "⚠️ Wave runup can add 1-3m+ during storms. This prediction does not include wave effects."

3. **Link to wave buoy data:**
   - Show current wave height from buoys
   - Qualitative guidance: "2m waves = add ~0.5m, 5m waves = add ~2m"

### For Future Enhancements

**Option 1: Add Empirical Wave Correction**
```python
# Simple correction based on buoy wave height
wave_height_significant = get_buoy_wave_height(station)
if wave_height_significant > 2.0:
    wave_setup_estimate = 0.35 * sqrt(wave_height_significant * 100)  # Rough approximation
    warning = "⚠️ Large waves present - add ~{wave_setup_estimate:.1f}m for wave effects"
```

**Option 2: Fetch Wave Model Predictions**
- NOAA WaveWatch III provides wave forecasts
- Could fetch Hs forecasts for each location
- Apply empirical runup formulas (Stockdon, Nielsen, etc.)
- Requires knowing local beach slope

**Option 3: Conservative Safety Factor**
```
For flood warnings, add safety margin:
- Protected areas: +0.3m
- Moderately exposed: +0.5m
- Exposed coasts: +1.0m
```

---

## Technical References

### GDSPS Documentation
- [Environment Canada GDSPS Product Guide](https://eccc-msc.github.io/open-data/msc-data/nwp_gdsps/readme_gdsps_en/)
- Model: NEMO ocean model (15km resolution)
- Physics: Barotropic ocean dynamics + meteorological forcing

### Wave Setup & Runup
- Stockdon et al. (2006): "Empirical parameterization of setup, swash, and runup"
- Nielsen & Hanslow (1991): "Wave runup distributions on natural beaches"
- Battjes (1974): "Surf similarity parameter"

### Key Equations

**Wave Setup (Stockdon et al.):**
```
η_setup = 0.35 × β × √(Hs × L0)

where:
  β = beach slope (rise/run)
  Hs = significant wave height
  L0 = deep water wavelength = g × T² / (2π)
  T = wave period
```

**Wave Runup R2% (Stockdon et al.):**
```
R2% = 1.1 × (η_setup + √(η_setup² + 0.5 × Hs × L0 × β²))
```

**For steep slopes (β > 0.3):**
```
R2% ≈ 1.5 × Hs
```

---

## Summary

Our system provides **half the picture**:
- ✅ Astronomical tide (predictable, accurate)
- ✅ Storm surge (weather-driven, GDSPS forecast)
- ❌ Wave setup (can add 0.2-0.5m)
- ❌ Wave runup (can add 1-3m+)

**This is valuable** for protected inland areas where wave effects are small.

**This is incomplete** for exposed coasts where wave effects dominate.

**Always caveat predictions** with clear statements about wave effects not being included, especially during storm conditions or at exposed locations.

The ideal solution would combine:
1. Our current system (tide + surge)
2. Wave model forecasts (WaveWatch III, SWAN)
3. Site-specific runup calculations
4. Real-time wave buoy observations

But that's a much larger undertaking requiring wave modeling expertise.
