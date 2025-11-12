#!/bin/bash
# Test workflow for tide offset development
# This script demonstrates the complete offline testing workflow

set -e  # Exit on error

# Change to repo root (parent of tests/ directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

echo "🌊 Tide Offset Test Workflow"
echo "======================================================================"
echo "Working directory: $(pwd)"
echo ""

# Step 1: Generate test database
echo "Step 1: Generating test database..."
python3 create_test_tide_database.py
echo ""

# Step 2: Calculate tide offsets
echo "Step 2: Calculating tide offsets (observed storm surge)..."
python3 ../calculate_storm_surge_observed.py --test-mode
echo ""

echo "======================================================================"
echo "✅ Test workflow complete!"
echo ""
echo "You can now:"
echo "  - Inspect the test database: tests/databases/tide_data_test.sqlite"
echo "  - Query offsets directly:"
echo "    python3 -c 'import sqlite3; conn = sqlite3.connect(\"tests/databases/tide_data_test.sqlite\"); cur = conn.cursor(); cur.execute(\"SELECT * FROM tide_offset LIMIT 5\"); print(cur.fetchall())'"
echo "  - Modify calculate_storm_surge_observed.py and re-run this script"
echo ""
