#!/bin/bash
# Setup Offline Testing Environment for Storm Surge Hindcast
set -e

echo "🧪 Setting up offline testing environment..."
echo "=" * 60

# Create necessary directories
echo "📁 Creating directory structure..."
mkdir -p ~/site/data/storm_surge
mkdir -p ~/.local/share

# Copy test fixtures to output location
echo "📋 Copying test fixtures to data directory..."
cp tests/fixtures/storm_surge/*.json ~/site/data/storm_surge/

# List what we have
echo ""
echo "✅ Test data files ready:"
ls -lh ~/site/data/storm_surge/

echo ""
echo "📊 Checking fixture timestamps..."
for file in ~/site/data/storm_surge/*.json; do
    filename=$(basename "$file")
    echo ""
    echo "📄 $filename:"
    # Extract first and last timestamp from forecast data
    python3 -c "
import json
with open('$file') as f:
    data = json.load(f)
    if 'forecast' in data:
        times = list(data['forecast'].keys())
        if times:
            print(f'  First: {times[0]}')
            print(f'  Last:  {times[-1]}')
            print(f'  Count: {len(times)} hours')
    elif 'hindcast' in data:
        items = data['hindcast']
        if items:
            print(f'  First: {items[0][\"time\"]}')
            print(f'  Last:  {items[-1][\"time\"]}')
            print(f'  Count: {len(items)} predictions')
"
done

echo ""
echo "🎉 Offline test environment ready!"
echo ""
echo "Next steps:"
echo "  1. Open ~/site/index.html in browser"
echo "  2. Navigate to storm surge/hindcast charts"
echo "  3. Verify timestamps display correctly"
echo "  4. Check that day boundaries align properly"
