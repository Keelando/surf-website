#!/bin/bash
# Quick Deployment Script for Unified Stations System
# Copy this to ~/envcan_wave/deploy_stations.sh and run it

set -e  # Exit on any error

echo "🌊 Deploying Unified Stations System"
echo "======================================"

# Paths
PROJECT_DIR="$HOME/envcan_wave"
BACKUP_DIR="$PROJECT_DIR/backups/$(date +%Y%m%d_%H%M%S)"

echo ""
echo "📁 Working directory: $PROJECT_DIR"
echo "💾 Backup directory: $BACKUP_DIR"

# Create backup directory
mkdir -p "$BACKUP_DIR"
echo ""
echo "✅ Created backup directory"

# Backup old files if they exist
echo ""
echo "📦 Backing up old files..."

if [ -f "$PROJECT_DIR/tide_stations.json" ]; then
    cp "$PROJECT_DIR/tide_stations.json" "$BACKUP_DIR/"
    echo "  ✅ Backed up tide_stations.json"
fi

# Backup scripts that will be modified
for script in sqlite_to_json.py export_24hr_timeseries.py tide_to_sqlite.py export_tide_json.py; do
    if [ -f "$PROJECT_DIR/$script" ]; then
        cp "$PROJECT_DIR/$script" "$BACKUP_DIR/"
        echo "  ✅ Backed up $script"
    fi
done

# Copy new files
echo ""
echo "📝 Deploying new files..."

# You'll need to manually copy these from ~/claude/outputs/
# Uncomment and adjust paths as needed:
# cp ~/claude/outputs/stations.json "$PROJECT_DIR/"
# cp ~/claude/outputs/stations.py "$PROJECT_DIR/"
# cp ~/claude/outputs/validate_stations.py "$PROJECT_DIR/"

echo "  ⚠️  Please manually copy these files:"
echo "     • stations.json → $PROJECT_DIR/"
echo "     • stations.py → $PROJECT_DIR/"
echo "     • validate_stations.py → $PROJECT_DIR/"
echo ""
echo "  📖 See DEPLOYMENT_GUIDE.md for details"

# Validate
echo ""
echo "🧪 To validate after copying files, run:"
echo "   cd $PROJECT_DIR"
echo "   python3 validate_stations.py"

echo ""
echo "======================================"
echo "✅ Deployment preparation complete!"
echo "======================================"
echo ""
echo "Next steps:"
echo "  1. Copy the three files mentioned above"
echo "  2. Run validate_stations.py to check"
echo "  3. Update your scripts using migration_guide.py examples"
echo "  4. Test each script individually"
echo "  5. Update cron jobs if needed"
echo ""
echo "Backups saved to: $BACKUP_DIR"
