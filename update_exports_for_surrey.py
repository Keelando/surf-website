#!/usr/bin/env python3
"""
Helper: Update export scripts to include Surrey FlowWorks stations.
Run this once to add Surrey stations to your BUOYS dict in export scripts.
"""

from pathlib import Path

# Surrey stations to add
SURREY_STATIONS = {
    "CRPILE": {"name": "Crescent Pile", "location": "Crescent Beach, Surrey"},
    "CRCHAN": {"name": "Crescent Channel", "location": "Boundary Bay Channel"},
    "COLEB": {"name": "Colebrook", "location": "Colebrook Pump House"},
}

def update_export_script(script_path):
    """Add Surrey stations to BUOYS dict in export script."""
    script_path = Path(script_path).expanduser()
    
    if not script_path.exists():
        print(f"⚠️  Script not found: {script_path}")
        return False
    
    content = script_path.read_text()
    
    # Find the BUOYS dict
    if 'BUOYS = {' not in content:
        print(f"⚠️  No BUOYS dict found in {script_path.name}")
        return False
    
    # Check if Surrey stations already added
    if '"CRPILE"' in content:
        print(f"✅ {script_path.name} already has Surrey stations")
        return True
    
    # Find the closing brace of BUOYS dict
    start_idx = content.find('BUOYS = {')
    brace_count = 0
    in_buoys = False
    end_idx = -1
    
    for i in range(start_idx, len(content)):
        if content[i] == '{':
            brace_count += 1
            in_buoys = True
        elif content[i] == '}':
            brace_count -= 1
            if in_buoys and brace_count == 0:
                end_idx = i
                break
    
    if end_idx == -1:
        print(f"⚠️  Could not find end of BUOYS dict in {script_path.name}")
        return False
    
    # Build Surrey entries
    surrey_entries = []
    for buoy_id, info in SURREY_STATIONS.items():
        entry = f'''    "{buoy_id}": {{"name": "{info['name']}", "location": "{info['location']}"}},'''
        surrey_entries.append(entry)
    
    # Insert before closing brace
    new_content = (
        content[:end_idx] +
        '\n' +
        '\n'.join(surrey_entries) +
        '\n' +
        content[end_idx:]
    )
    
    # Backup original
    backup_path = script_path.with_suffix('.py.bak')
    script_path.rename(backup_path)
    print(f"📦 Backed up to {backup_path.name}")
    
    # Write updated version
    script_path.write_text(new_content)
    print(f"✅ Updated {script_path.name}")
    
    return True


def main():
    print("🔧 Surrey FlowWorks Integration Helper")
    print("=" * 70)
    
    # Scripts to update
    scripts_to_update = [
        "~/envcan_wave/sqlite_to_json.py",
        "~/envcan_wave/export_24hr_timeseries.py",
    ]
    
    for script_path in scripts_to_update:
        update_export_script(script_path)
        print()
    
    print("=" * 70)
    print("✅ Complete!")
    print("\nNext steps:")
    print("1. Review the .bak files to ensure changes look correct")
    print("2. Test the updated export scripts")
    print("3. Add fetch_surrey_wave_v2.py to cron:")
    print("   */10 * * * * ~/envcan_wave/.venv/bin/python3 ~/envcan_wave/fetch_surrey_wave_v2.py >> ~/envcan_wave/surrey.log 2>&1")
    print("4. Update ~/envcan_wave/stations.json with the new version")


if __name__ == "__main__":
    main()
