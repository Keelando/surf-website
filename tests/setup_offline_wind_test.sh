#!/bin/bash
#
# Setup script for offline wind data testing
#
# This script:
#   1. Creates test wind database with sample data
#   2. Exports JSON files for frontend testing
#   3. Copies test fixtures to data directory
#
# Usage:
#   ./tests/setup_offline_wind_test.sh
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python3"

echo "🌬️  Wind Data Offline Testing Setup"
echo "======================================================================"

# Step 1: Generate test database
echo ""
echo "Step 1: Generating test wind database..."
$VENV_PYTHON "$SCRIPT_DIR/create_test_wind_database.py"

# Step 2: Create test data directory
TEST_DATA_DIR="$SCRIPT_DIR/data/wind"
mkdir -p "$TEST_DATA_DIR"
echo ""
echo "Step 2: Created test data directory: $TEST_DATA_DIR"

# Step 3: Copy test database to standard location for testing
TEST_DB="$SCRIPT_DIR/databases/wind_data_test.sqlite"
STANDARD_DB="$HOME/.local/share/wind_data_test.sqlite"

if [ -f "$TEST_DB" ]; then
    cp "$TEST_DB" "$STANDARD_DB"
    echo ""
    echo "Step 3: Copied test database to: $STANDARD_DB"
else
    echo "⚠️  Test database not found at: $TEST_DB"
    exit 1
fi

# Step 4: Generate test JSON exports
echo ""
echo "Step 4: Generating test JSON exports..."
echo "  This demonstrates how export scripts can use the test database"
echo ""
echo "  To test exports manually:"
echo "    1. Modify export scripts to use: ~/.local/share/wind_data_test.sqlite"
echo "    2. Run: python3 export_wind_json.py"
echo "    3. Check: ~/site/data/latest_wind.json"

# Step 5: Summary
echo ""
echo "======================================================================"
echo "✅ Offline wind testing environment ready!"
echo ""
echo "Test database:    $STANDARD_DB"
echo "Test fixtures:    $SCRIPT_DIR/fixtures/wind/"
echo "Sample XML files: 3 stations (CWSB, CWGT, CVTF)"
echo "Sample data:      25 hourly observations per station"
echo ""
echo "Next steps:"
echo "  1. Test wind parser:"
echo "     python3 wind_to_sqlite.py --test-mode"
echo ""
echo "  2. Test JSON exports:"
echo "     python3 export_wind_json.py --test-mode"
echo "     python3 export_wind_24hr_timeseries.py --test-mode"
echo ""
echo "  3. Verify database contents:"
echo "     sqlite3 $STANDARD_DB 'SELECT * FROM wind_observation LIMIT 5;'"
echo ""
echo "  4. Test frontend with sample data:"
echo "     cp $STANDARD_DB ~/.local/share/wind_data.sqlite"
echo "     python3 export_wind_json.py"
echo "     # Then open ~/site/winds.html in browser"
echo ""
echo "======================================================================"
