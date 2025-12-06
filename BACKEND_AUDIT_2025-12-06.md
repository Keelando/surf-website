# Backend Audit Report - 2025-12-06

## Executive Summary

This audit examined two critical areas:
1. **Environment Canada Buoy XML Feed Analysis**: Identified 10 data fields available in XML feeds but not currently captured
2. **SQL Injection & Code Quality Review**: Found several instances of f-string SQL construction (low-risk but code smell)

---

## Part 1: Environment Canada Buoy XML Feed Analysis

### Objective
Review XML feed structure to identify parameters we're glossing over or not capturing.

### Methodology
- Analyzed actual XML samples from `/home/keelando/envcan_wave/data/buoy/`
- Extracted all unique field names from recent observations
- Compared against `FIELD_MAP` in `buoy_to_influx_sqlite.py`
- Filtered out metadata fields (station IDs, names, etc.)

### Findings: Missing Fields (10 total)

#### High-Value Fields (Recommended to Capture)

**1. `sig_wave_pd_pst20mts` - Significant Wave Period**
- **Type**: Wave metric (basic)
- **Unit**: seconds
- **Value**: PRIMARY wave period metric (different from avg_sig_wave_pd_pst20mts which we DO capture)
- **Recommendation**: HIGH PRIORITY - This is a fundamental wave parameter
- **Usage**: Important for wave forecasting and maritime operations

**2. `avg_max_wave_hgt_pst20mts` - Average Maximum Wave Height**
- **Type**: Wave metric (statistical)
- **Unit**: meters
- **Value**: Average of the maximum waves (1/10 highest waves)
- **Recommendation**: MEDIUM PRIORITY - Useful for understanding wave climate
- **Usage**: Complements existing max_wave_hgt_pst20mts

**3. `avg_max_wave_pd_pst20mts` - Average Maximum Wave Period**
- **Type**: Wave metric (statistical)
- **Unit**: seconds
- **Value**: Period corresponding to maximum waves
- **Recommendation**: MEDIUM PRIORITY - Pairs with avg_max_wave_hgt_pst20mts
- **Usage**: Wave period analysis

**4. `max_avg_wnd_spd_pst10mts_2` - Wind Gust (Sensor 2)**
- **Type**: Wind metric (secondary sensor)
- **Unit**: km/h
- **Value**: Maximum wind speed from redundant sensor
- **Recommendation**: MEDIUM PRIORITY - We capture sensor 2 average speed but not gusts
- **Usage**: Complete dual-sensor wind redundancy
- **Note**: We already capture `max_avg_wnd_spd_pst10mts` and `max_avg_wnd_spd_pst10mts_1` for primary sensor

#### System Health & Monitoring Fields (Recommended)

**5. `avg_batry_volt_pst10mts` - Battery Voltage**
- **Type**: System health
- **Unit**: Volts
- **Value**: Buoy battery voltage
- **Recommendation**: MEDIUM PRIORITY - Useful for operations
- **Usage**: Predict transmission failures, monitor buoy health over time
- **Why capture**: Early warning system for buoy maintenance needs
- **Note**: Already mentioned in code comments (line 232)

**6. `wtchmn_boot_cnt_pst1hr` - Watchman Boot Count**
- **Type**: System health
- **Unit**: count
- **Value**: Number of system reboots in past hour
- **Recommendation**: MEDIUM PRIORITY - Useful for operations
- **Usage**: Detect buoy instability/malfunctions, correlate with weather events
- **Why capture**: Indicator of hardware issues or severe conditions
- **Note**: Already mentioned in code comments (line 233)

**7. `avg_obstrn_lamp_crnt_pst10mts` - Obstruction Lamp Current**
- **Type**: System health
- **Unit**: Amps
- **Value**: Navigation light current draw
- **Recommendation**: MEDIUM PRIORITY - Useful for operations
- **Usage**: Verify navigation light functionality, detect lamp failures
- **Why capture**: Maritime safety compliance, remote diagnostics
- **Note**: Already mentioned in code comments (line 236)

#### Orientation/Navigation Fields (Recommended)

**8. `avg_cmpss_hdng_pst10mts_1` - Compass Heading (Sensor 1)**
- **Type**: Orientation
- **Unit**: degrees
- **Value**: Buoy heading from compass sensor 1
- **Recommendation**: MEDIUM PRIORITY - Useful for analysis
- **Usage**: Detect buoy rotation, understand wind/current interaction
- **Why capture**: Helps interpret directional wave/wind data
- **Note**: Already mentioned in code comments (line 234)

**9. `avg_cmpss_hdng_pst10mts_2` - Compass Heading (Sensor 2)**
- **Type**: Orientation
- **Unit**: degrees
- **Value**: Buoy heading from compass sensor 2 (redundant)
- **Recommendation**: MEDIUM PRIORITY - Useful for analysis
- **Usage**: Dual-sensor compass redundancy, validate heading accuracy
- **Why capture**: Pairs with sensor 1 for data quality
- **Note**: Already mentioned in code comments (line 234)

#### Fields NOT Recommended

**10. `avg_wtr_lvl_snsr_volt_pst10mts` - Water Level Sensor Voltage**
- **Type**: System diagnostic
- **Unit**: Volts
- **Value**: Wave measurement sensor voltage
- **Recommendation**: SKIP - Too granular for operational needs
- **Note**: Already mentioned in code comments (line 235)

#### Already Captured (GPS Drift Monitoring) ✅

**Position Data** - ALREADY CAPTURED:
- `crnt_buoy_lat` → `buoy_lat_current` (FIELD_MAP line 224)
- `crnt_buoy_long` → `buoy_lon_current` (FIELD_MAP line 225)
- **Status**: ✅ Already in database and exported to JSON
- **Usage**: Real-time drift monitoring, mooring status
- **Location**: See `buoy_to_influx_sqlite.py` lines 113-114, 224-225

### Recommendations: Environment Canada Buoy Fields

#### Immediate Actions (High Priority)
1. **Add `sig_wave_pd_pst20mts` to FIELD_MAP**
   ```python
   "sig_wave_pd_pst20mts": "wave_period_sig_basic",  # Distinguish from avg_sig_wave_pd_pst20mts
   ```
   - This is a fundamental wave metric that should be captured
   - Use different column name to avoid collision with existing field

#### Secondary Actions (Medium Priority)

2. **Add wave statistics fields**:
   ```python
   "avg_max_wave_hgt_pst20mts": "wave_height_max_avg",
   "avg_max_wave_pd_pst20mts": "wave_period_max_avg",
   ```

3. **Complete dual-sensor wind coverage**:
   ```python
   "max_avg_wnd_spd_pst10mts_2": "wind_gust_sensor_2",
   ```

4. **Add operational monitoring fields**:
   ```python
   # System health
   "avg_batry_volt_pst10mts": "battery_voltage",
   "wtchmn_boot_cnt_pst1hr": "watchman_boot_count",
   "avg_obstrn_lamp_crnt_pst10mts": "obstruction_lamp_current",

   # Orientation/drift analysis
   "avg_cmpss_hdng_pst10mts_1": "compass_heading_1",
   "avg_cmpss_hdng_pst10mts_2": "compass_heading_2",
   ```
   - **Why**: Enables proactive buoy maintenance and operational insights
   - **Benefits**:
     - Battery voltage: Predict power failures before they occur
     - Boot count: Identify unstable buoys or severe weather impacts
     - Lamp current: Ensure maritime safety compliance
     - Compass heading: Understand buoy behavior, validate directional data

#### Fields to Skip
5. **Do NOT add**:
   ```python
   # "avg_wtr_lvl_snsr_volt_pst10mts": "water_level_sensor_voltage"  # Too granular
   ```

---

## Part 2: SQL Injection & Code Quality Review

### Objective
Identify SQL injection vulnerabilities and code quality issues in database operations.

### Methodology
- Reviewed all Python scripts that interact with SQLite
- Searched for SQL query construction patterns
- Analyzed parameterization usage
- Checked error handling and resource management

### Findings: SQL Query Construction

#### ✅ GOOD: Parameterized Queries (No Risk)
Most database operations correctly use parameterized queries with `?` placeholders:

**Examples**:
- `tide_to_sqlite.py:141-145` - INSERT with parameters
- `tide_to_sqlite.py:335` - DELETE with parameters
- `wind_to_sqlite.py:261` - INSERT with parameters
- `sqlite_to_json.py:107` - SELECT with parameters

#### ⚠️ CODE SMELL: F-String SQL Construction (Low Risk)

Found 7 instances of f-string usage in SQL queries. These are **technically safe** because values come from hardcoded constants, NOT user input, but violate best practices:

**File**: `buoy_to_influx_sqlite.py`
- **Line 159**: `ALTER TABLE ... ADD COLUMN {col} REAL`
  - `col` comes from `EXPECTED_FIELDS` (hardcoded list)
  - Risk: NONE (hardcoded constant)
  - Issue: Code smell - violates SQL best practices

- **Line 286**: `INSERT OR IGNORE INTO buoy_observation ({','.join(cols)}) VALUES ({placeholders})`
  - `cols` built from `EXPECTED_FIELDS` (hardcoded list)
  - Risk: NONE (hardcoded constant)
  - Issue: Code smell

**File**: `wind_to_sqlite.py`
- **Line 135**: `ALTER TABLE wind_observation ADD COLUMN {col} REAL`
  - `col` from `EXPECTED_FIELDS` (hardcoded)
  - Risk: NONE

- **Line 259**: `INSERT OR IGNORE INTO wind_observation ({','.join(cols)}) VALUES ({placeholders})`
  - `cols` from `EXPECTED_FIELDS` (hardcoded)
  - Risk: NONE

**File**: `fetch_surrey_wave_v2.py`
- **Line 210**: `ALTER TABLE buoy_observation ADD COLUMN {col} REAL`
  - `col` from hardcoded `required` set
  - Risk: NONE

- **Line 258-262**: `UPDATE buoy_observation SET {field} = ? WHERE ...`
  - `field` from hardcoded `STATIONS` dict keys
  - Risk: NONE

- **Line 265-269**: `INSERT INTO buoy_observation (..., {field}, ...) VALUES ...`
  - `field` from hardcoded `STATIONS` dict keys
  - Risk: NONE

**File**: `fetch_noaa_buoy.py`
- **Line 248**: `ALTER TABLE buoy_observation ADD COLUMN {c}`
  - `c` from `newcols` list (hardcoded)
  - Risk: NONE

- **Lines 266-273**: INSERT with ON CONFLICT
  - `fcols` filtered from hardcoded field list
  - Risk: NONE

**File**: `sqlite_to_json.py`
- **Lines 97-106**: `SELECT observation_time, {field} FROM buoy_observation WHERE ...`
  - `field` comes from `available_fields` which is filtered from `ALL_FIELDS` constant AND validated against actual DB schema
  - Risk: NONE (doubly validated)
  - Issue: Most concerning pattern but still safe

### Risk Assessment

**Overall Risk Level**: 🟢 **LOW**

- ✅ No actual SQL injection vulnerabilities found
- ✅ All user-facing inputs use parameterized queries
- ✅ All f-string SQL uses hardcoded constants
- ⚠️ Code style violates SQL best practices
- ⚠️ Future developers might copy these patterns unsafely

### Recommendations: SQL Code Quality

#### 1. Refactor ALTER TABLE Statements (Low Priority)
While safe, consider refactoring for consistency:

**Current** (lines 159, 135, 210, etc.):
```python
cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {col} REAL;")
```

**Better** (if SQLite supported it - but it doesn't):
```python
# SQLite doesn't support parameterized column names in DDL
# Current approach is acceptable with validation
```

**Note**: SQLite doesn't support parameterized identifiers (table/column names) in DDL statements, so f-strings are actually the correct approach here. The key is ensuring `col` comes from a trusted source (hardcoded list), which it does.

#### 2. Add Validation Comments
Add comments to document why f-strings are safe:

```python
# Safe: col comes from EXPECTED_FIELDS constant, not user input
cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {col} REAL;")
```

#### 3. Defensive Validation (Optional)
Add assertion to make intent explicit:

```python
assert col in EXPECTED_FIELDS, f"Unsafe column name: {col}"
cur.execute(f"ALTER TABLE buoy_observation ADD COLUMN {col} REAL;")
```

#### 4. sqlite_to_json.py Enhancement
The dynamic field selection (lines 97-106) is safe but could be more explicit:

```python
# Current: Safe because field is validated against DB schema
available_fields = [f for f in ALL_FIELDS if f in existing_cols]

# Enhancement: Add assertion for clarity
for field in available_fields:
    assert field in ALL_FIELDS, f"Unexpected field: {field}"
    sql = f"SELECT observation_time, {field} FROM buoy_observation WHERE ..."
```

---

## Part 3: Additional Code Quality Observations

### Resource Management ✅

**Good Practices Found**:
- `tide_to_sqlite.py:350` - Explicit `conn.close()`
- Context managers used in `sqlite_to_json.py:53` - `with sqlite3.connect(...)`
- Proper WAL mode usage for concurrent access

### Error Handling ✅

**Good Practices Found**:
- Try-except blocks around HTTP requests (`tide_to_sqlite.py:101-108`)
- Graceful degradation when InfluxDB unavailable
- Specific exception handling with logging

### Configuration Management ⚠️

**Observation**:
- Credentials loaded from environment files (`~/.config/buoy_influx_1.env`)
- **Good**: Credentials not hardcoded
- **Concern**: One exception found:

**File**: `fetch_surrey_wave_v2.py:29-30`
```python
USERNAME = "surreyrain"
PASSWORD = "surreyrain"
```

**Assessment**: Public API credentials (appears to be read-only public access), low risk

**Recommendation**: Move to environment variable for consistency:
```python
USERNAME = os.environ.get("SURREY_USERNAME", "surreyrain")
PASSWORD = os.environ.get("SURREY_PASSWORD", "surreyrain")
```

### Logging - Sensitive Data ✅

**Good Practices Found**:
- No passwords logged
- API keys accessed via environment variables
- Structured logging with appropriate levels

---

## Summary & Priority Matrix

### High Priority
1. ✅ Add `sig_wave_pd_pst20mts` to buoy FIELD_MAP (fundamental wave metric)
2. 📝 Document why f-string SQL is safe in these contexts

### Medium Priority
3. ➕ Add wave statistics fields (avg_max_wave_hgt_pst20mts, avg_max_wave_pd_pst20mts)
4. ➕ Complete dual-sensor wind coverage (max_avg_wnd_spd_pst10mts_2)
5. 📊 **Add operational monitoring fields**:
   - Battery voltage (predict failures)
   - Boot count (detect instability)
   - Obstruction lamp current (safety compliance)
   - Compass headings (drift/orientation analysis)
6. 🔧 Move Surrey credentials to environment variables

### Low Priority
7. 💬 Add validation comments to f-string SQL statements
8. 🛡️ Add defensive assertions (optional)

### Already Implemented ✅
- GPS coordinates for drift monitoring (`buoy_lat_current`, `buoy_lon_current`)

---

## Conclusion

The codebase demonstrates solid security practices with proper parameterized queries for all user-facing data. The f-string SQL patterns, while technically safe, should be documented to prevent future unsafe copying.

The Environment Canada buoy XML feed contains 10 additional fields not currently captured:

**Wave Data**:
- `sig_wave_pd_pst20mts` (HIGH priority) - fundamental wave metric
- Wave statistics (MEDIUM priority) - avg max height/period

**Operational Monitoring** (MEDIUM priority):
- Battery voltage, boot counts, lamp current - Enable proactive maintenance
- Compass headings - Drift/orientation analysis

**Already Captured**:
- GPS coordinates for drift monitoring ✅

**Skip**:
- Water level sensor voltage (too granular)

No critical security vulnerabilities were identified. The system demonstrates good practices in error handling, resource management, and credential management.
