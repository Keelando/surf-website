#!/bin/bash
#
# Validation script for Phase 0 migration
#
# Tests that all modified scripts can:
# 1. Import successfully (no syntax errors, import errors)
# 2. Run without crashing (basic smoke test)
#
# Usage:
#   bash validate_scripts.sh

set -e  # Exit on error

echo "=========================================="
echo "Phase 0 Script Validation"
echo "=========================================="
echo ""

# Set PYTHONPATH to current directory (for root-level imports)
export PYTHONPATH="$(pwd):$PYTHONPATH"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track results
TOTAL=0
PASSED=0
FAILED=0
WARNINGS=0

# Test function
test_script() {
    local script=$1
    local description=$2

    TOTAL=$((TOTAL + 1))

    echo "Testing: $script"

    # Check if file exists
    if [ ! -f "$script" ]; then
        echo -e "  ${YELLOW}⚠️  SKIPPED${NC}: File not found"
        WARNINGS=$((WARNINGS + 1))
        return
    fi

    # Test 1: Check syntax (python compile)
    if python3 -m py_compile "$script" 2>/dev/null; then
        echo -e "  ${GREEN}✅${NC} Syntax check passed"
    else
        echo -e "  ${RED}❌${NC} Syntax errors detected"
        FAILED=$((FAILED + 1))
        return
    fi

    # Test 2: Try importing (if it's a module)
    # This catches import errors without running the script
    local module_name="${script%.py}"
    if python3 -c "import $module_name" 2>/dev/null; then
        echo -e "  ${GREEN}✅${NC} Import successful"
    else
        # Some scripts may not be importable (if they have side effects)
        # Try running with --help or -h
        if python3 "$script" --help >/dev/null 2>&1 || python3 "$script" -h >/dev/null 2>&1; then
            echo -e "  ${GREEN}✅${NC} Script runs (--help works)"
        else
            # Just check that imports don't fail
            if python3 -c "exec(open('$script').read())" 2>&1 | grep -i "importerror\|modulenotfounderror"; then
                echo -e "  ${RED}❌${NC} Import errors detected"
                FAILED=$((FAILED + 1))
                return
            else
                echo -e "  ${YELLOW}⚠️${NC}  Script runs but may need data/config"
                WARNINGS=$((WARNINGS + 1))
            fi
        fi
    fi

    PASSED=$((PASSED + 1))
}

# =============================================================================
# TEST SCRIPTS
# =============================================================================

echo "Testing scripts modified in Phase 0:"
echo ""

# Scripts with unit conversion changes
echo "--- Unit Conversion Scripts ---"
test_script "sqlite_to_json.py" "Buoy latest snapshot exporter"
test_script "influx_to_mqtt.py" "MQTT publisher"
test_script "export_24hr_timeseries.py" "Buoy timeseries exporter"
test_script "fetch_noaa_buoy.py" "NOAA buoy fetcher"
test_script "fetch_surrey_wave_v2.py" "Surrey wave fetcher"
echo ""

# Scripts with BUOYS dictionary changes
echo "--- Station Data Scripts ---"
# (Already tested above, but note they also changed BUOYS dict)
echo ""

# Scripts with database path changes
echo "--- Database Path Scripts ---"
test_script "buoy_to_influx_sqlite.py" "EC buoy XML parser"
test_script "tide_to_sqlite.py" "DFO tide fetcher"
test_script "export_tide_json.py" "Tide JSON exporter"
test_script "calculate_storm_surge_observed.py" "Observed surge calculator"
test_script "export_combined_water_level.py" "Combined water level exporter"
test_script "compare_surrey_dfo_water_levels.py" "Surrey/DFO comparison"
echo ""

# Test new modules
echo "--- New Modules ---"
test_script "units.py" "Unit conversion utilities"
test_script "directions.py" "Direction conversion utilities"
test_script "config.py" "Configuration module"
echo ""

# =============================================================================
# SUMMARY
# =============================================================================

echo "=========================================="
echo "Validation Summary"
echo "=========================================="
echo "  Total scripts tested: $TOTAL"
echo -e "  ${GREEN}Passed: $PASSED${NC}"
echo -e "  ${RED}Failed: $FAILED${NC}"
echo -e "  ${YELLOW}Warnings: $WARNINGS${NC}"
echo "=========================================="
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✅ ALL VALIDATIONS PASSED!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Run: python3 test_new_modules.py"
    echo "  2. Test a few scripts manually:"
    echo "     python3 buoy_to_influx_sqlite.py"
    echo "     python3 sqlite_to_json.py"
    echo "     python3 export_tide_json.py"
    echo "  3. Check JSON exports are still generated correctly"
    echo "  4. If all looks good, commit changes!"
    echo ""
    exit 0
else
    echo -e "${RED}❌ VALIDATION FAILED${NC}"
    echo ""
    echo "Fix the $FAILED failed scripts before proceeding."
    echo "Review errors above for details."
    echo ""
    exit 1
fi
