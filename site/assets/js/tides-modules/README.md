# Tide Page Refactoring - Modular Structure

This directory contains the refactored tide display logic, broken down from the original 1937-line `tides.js` monolith into maintainable, testable modules.

## Module Structure

### ✅ Completed Modules

#### 1. **constants.js**
- Station display names
- Geodetic station identifiers
- Geodetic methodology mappings
- **Lines extracted:** ~30
- **Purpose:** Centralized configuration

#### 2. **geodetic.js**
- Geodetic station detection
- Calibration methodology selection
- Offset value retrieval
- Residual storage
- **Lines extracted:** ~70
- **Purpose:** Isolates geodetic-specific logic from regular tide stations

#### 3. **data-loader.js**
- TideDataStore class for managing all tide data
- Methods for loading JSON endpoints
- Accessors for observations, predictions, timeseries, high/low
- Current station and day offset management
- **Lines extracted:** ~200
- **Purpose:** Centralized data management with clean API

#### 4. **utils.js**
- Time formatting (Pacific timezone)
- Age string calculation
- Timestamp updates
- Error display
- Map navigation helper
- **Lines extracted:** ~100
- **Purpose:** Common utilities used across modules

#### 5. **sunlight.js**
- SunlightDataStore class
- Sunlight times loading and display
- Sunrise/sunset/civil twilight cards
- Day navigation for sunlight widget
- **Lines extracted:** ~200
- **Purpose:** Self-contained sunlight times feature

#### 6. **ui-controls.js**
- Station dropdown population
- Day navigation setup
- Button state management
- URL parameter handling
- **Lines extracted:** ~240
- **Purpose:** All UI control logic in one place

### 🚧 Remaining Modules

#### 7. **display.js** (To Do)
Should contain:
- displayStationMetadata()
- displayCurrentObservation()
- displayCurrentPrediction()
- displayStormSurge()
- displayHighLowTable()
- **Estimated lines:** ~400-500

#### 8. **chart-renderer.js** (To Do)
Should contain:
- displayTideChart() - Main chart rendering
- Series configuration (predictions, observations, storm surge, combined water level)
- Mark lines (current time indicator, high/low events)
- Chart options and styling
- Geodetic calibration application to chart data
- **Estimated lines:** ~600-700 (most complex module)

## Integration Plan

### Phase 1: Create Core Integration Layer
Create `tides-refactored.js` that:
1. Imports all modules
2. Initializes data stores
3. Wires up callbacks between modules
4. Exposes initialization function

### Phase 2: Side-by-Side Testing
1. Add `tides-refactored.js` to tides.html alongside original
2. Add feature flag/toggle to switch between implementations
3. Verify identical behavior

### Phase 3: Replace Original
1. Remove original tides.js
2. Update script tag in tides.html
3. Monitor for issues

## Benefits of Refactoring

1. **Maintainability** - Each module has single responsibility
2. **Testability** - Modules can be unit tested independently
3. **Readability** - Much easier to find and understand specific features
4. **Extensibility** - Adding new features doesn't require touching 2000-line file
5. **Debugging** - Easier to isolate issues to specific modules

## Module Dependencies

```
constants.js (no dependencies)
    ↓
geodetic.js (imports constants)
    ↓
utils.js (no module dependencies, uses DOM)
    ↓
data-loader.js (no module dependencies, uses fetch)
    ↓
sunlight.js (no module dependencies, uses DOM)
    ↓
ui-controls.js (imports constants)
    ↓
display.js (imports utils, geodetic, constants)
    ↓
chart-renderer.js (imports geodetic, utils, display)
    ↓
tides-refactored.js (imports ALL, wires together)
```

## Current Status

- ✅ 6/8 modules created (~840 lines refactored)
- 🚧 2/8 modules remaining (~1000-1200 lines to refactor)
- 📊 ~43% complete

## Next Steps

1. Create display.js module
2. Create chart-renderer.js module
3. Create tides-refactored.js integration layer
4. Test alongside original implementation
5. Replace original tides.js
