#!/bin/bash
# Surrey FlowWorks Integration - Deployment Script
# Run this on your server after pulling the latest changes

set -e  # Exit on error

echo "🌊 Surrey FlowWorks Integration - Deployment Script"
echo "======================================================================="

# Detect script location
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ENVCAN_DIR="${HOME}/envcan_wave"

echo "📂 Repository: $SCRIPT_DIR"
echo "📂 Target: $ENVCAN_DIR"
echo ""

# Check if envcan_wave directory exists
if [ ! -d "$ENVCAN_DIR" ]; then
    echo "❌ Error: $ENVCAN_DIR not found!"
    echo "   Please create this directory or update ENVCAN_DIR in this script."
    exit 1
fi

echo "📋 Deployment steps:"
echo ""

# 1. Copy fetcher script
echo "1️⃣  Copying fetch_surrey_wave_v2.py..."
cp -v "$SCRIPT_DIR/fetch_surrey_wave_v2.py" "$ENVCAN_DIR/"
chmod +x "$ENVCAN_DIR/fetch_surrey_wave_v2.py"
echo "   ✅ Done"
echo ""

# 2. Update stations.json
echo "2️⃣  Updating stations.json..."
if [ -f "$ENVCAN_DIR/stations.json" ]; then
    echo "   📦 Backing up existing stations.json..."
    cp -v "$ENVCAN_DIR/stations.json" "$ENVCAN_DIR/stations.json.backup.$(date +%Y%m%d-%H%M%S)"
fi
cp -v "$SCRIPT_DIR/stations_updated.json" "$ENVCAN_DIR/stations.json"
echo "   ✅ Done"
echo ""

# 3. Backup and update export scripts
echo "3️⃣  Updating export scripts..."
for script in sqlite_to_json.py export_24hr_timeseries.py; do
    if [ -f "$ENVCAN_DIR/$script" ]; then
        echo "   📦 Backing up $script..."
        cp -v "$ENVCAN_DIR/$script" "$ENVCAN_DIR/${script}.backup.$(date +%Y%m%d-%H%M%S)"
    fi
    cp -v "$SCRIPT_DIR/$script" "$ENVCAN_DIR/"
done
echo "   ✅ Done"
echo ""

# 4. Copy helper script
echo "4️⃣  Copying helper script..."
cp -v "$SCRIPT_DIR/update_exports_for_surrey.py" "$ENVCAN_DIR/"
echo "   ✅ Done"
echo ""

echo "======================================================================="
echo "✅ Deployment complete!"
echo ""
echo "📋 Next steps:"
echo ""
echo "1. Test the fetcher:"
echo "   cd $ENVCAN_DIR"
echo "   python3 fetch_surrey_wave_v2.py"
echo ""
echo "2. Add to crontab (fetch every 20 minutes):"
echo "   crontab -e"
echo "   # Add this line:"
echo "   */20 * * * * $ENVCAN_DIR/.venv/bin/python3 $ENVCAN_DIR/fetch_surrey_wave_v2.py >> $ENVCAN_DIR/surrey.log 2>&1"
echo ""
echo "3. Test exports:"
echo "   python3 $ENVCAN_DIR/sqlite_to_json.py"
echo "   python3 $ENVCAN_DIR/export_24hr_timeseries.py"
echo ""
echo "📚 For detailed instructions, see:"
echo "   - SURREY_DEPLOYMENT.md"
echo "   - SURREY_FRONTEND_GUIDE.md"
echo ""
