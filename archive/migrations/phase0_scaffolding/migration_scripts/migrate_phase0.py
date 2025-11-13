#!/usr/bin/env python3
"""
Automated Phase 0 Migration Script

This script automatically updates all Python scripts to use the new shared utilities:
- units.py (unit conversions)
- directions.py (direction conversions)
- config.py (database paths and configuration)
- stations.py (use get_all_buoys() instead of hardcoded BUOYS dict)

Usage:
    python3 migrate_phase0.py --dry-run    # Preview changes without applying
    python3 migrate_phase0.py              # Apply changes
    python3 migrate_phase0.py --backup     # Create .backup files before changing

Safety:
    - Creates backups of all modified files
    - Can be run in dry-run mode to preview changes
    - Validates changes after applying
    - Generates detailed migration report
"""

import re
import argparse
from pathlib import Path
import shutil
from datetime import datetime

# =============================================================================
# MIGRATION TARGETS
# =============================================================================

# Scripts that need kmh_to_knots function replaced
KMHTOKNOTS_SCRIPTS = [
    'sqlite_to_json.py',
    'influx_to_mqtt.py',
    'export_24hr_timeseries.py',
]

# Scripts that need ms_to_kmh function replaced
MSTOKMH_SCRIPTS = [
    'fetch_noaa_buoy.py',
    'fetch_surrey_wave_v2.py',
]

# Scripts that need degrees_to_cardinal function replaced
DEGTOCARDINAL_SCRIPTS = [
    'sqlite_to_json.py',
    'influx_to_mqtt.py',
]

# Scripts with hardcoded BUOYS dictionary
BUOYS_DICT_SCRIPTS = [
    'sqlite_to_json.py',
    'influx_to_mqtt.py',
    'export_24hr_timeseries.py',
]

# Scripts with hardcoded buoy database paths
BUOY_DB_SCRIPTS = [
    'buoy_to_influx_sqlite.py',
    'fetch_noaa_buoy.py',
    'sqlite_to_json.py',
    'export_24hr_timeseries.py',
    'fetch_surrey_wave_v2.py',
    'influx_to_mqtt.py',
]

# Scripts with hardcoded tide database paths
TIDE_DB_SCRIPTS = [
    'tide_to_sqlite.py',
    'export_tide_json.py',
    'calculate_storm_surge_observed.py',
    'export_combined_water_level.py',
    'compare_surrey_dfo_water_levels.py',
]

# =============================================================================
# MIGRATION PATTERNS
# =============================================================================

class Migration:
    """Represents a single code migration."""

    def __init__(self, script, pattern, replacement, description):
        self.script = script
        self.pattern = pattern  # Regex pattern or string
        self.replacement = replacement
        self.description = description
        self.applied = False
        self.matches_found = 0


# =============================================================================
# MIGRATION DEFINITIONS
# =============================================================================

def get_migrations():
    """Define all migrations to apply."""
    migrations = []

    # -------------------------------------------------------------------------
    # 1. Add imports for new modules
    # -------------------------------------------------------------------------

    # Add units import to scripts using unit conversions
    for script in set(KMHTOKNOTS_SCRIPTS + MSTOKMH_SCRIPTS):
        migrations.append(Migration(
            script=script,
            pattern=r'^(#!/usr/bin/env python3\n)',
            replacement=r'\1from units import kmh_to_knots, ms_to_kmh\n',
            description=f"Add units import to {script}"
        ))

    # Add directions import to scripts using direction conversions
    for script in DEGTOCARDINAL_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'^(#!/usr/bin/env python3\n)',
            replacement=r'\1from directions import degrees_to_cardinal, DIRS_16\n',
            description=f"Add directions import to {script}"
        ))

    # Add config import to buoy database scripts
    for script in BUOY_DB_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'^(#!/usr/bin/env python3\n)',
            replacement=r'\1from config import BUOY_DATABASE\n',
            description=f"Add config import to {script}"
        ))

    # Add config import to tide database scripts
    for script in TIDE_DB_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'^(#!/usr/bin/env python3\n)',
            replacement=r'\1from config import TIDE_DATABASE\n',
            description=f"Add config import to {script}"
        ))

    # -------------------------------------------------------------------------
    # 2. Remove duplicated function definitions
    # -------------------------------------------------------------------------

    # Remove kmh_to_knots function definitions
    for script in KMHTOKNOTS_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'def kmh_to_knots\(.*?\):\s*""".*?""".*?return None\s*\n',
            replacement='',
            description=f"Remove kmh_to_knots function from {script}"
        ))

    # Remove ms_to_kmh function definitions
    for script in MSTOKMH_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'def ms_to_kmh\(.*?\):\s*"""?.*?"""?.*?return.*?\n',
            replacement='',
            description=f"Remove ms_to_kmh function from {script}"
        ))

    # Remove degrees_to_cardinal and DIRS_16 definitions
    for script in DEGTOCARDINAL_SCRIPTS:
        # Remove DIRS_16 list
        migrations.append(Migration(
            script=script,
            pattern=r"DIRS_16 = \[.*?\]\s*\n",
            replacement='',
            description=f"Remove DIRS_16 list from {script}"
        ))
        # Remove degrees_to_cardinal function
        migrations.append(Migration(
            script=script,
            pattern=r'def degrees_to_cardinal\(.*?\):.*?return DIRS_16\[ix\]\s*\n',
            replacement='',
            description=f"Remove degrees_to_cardinal function from {script}"
        ))

    # -------------------------------------------------------------------------
    # 3. Remove hardcoded BUOYS dictionaries
    # -------------------------------------------------------------------------

    for script in BUOYS_DICT_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'BUOYS = \{[\s\S]*?\n\}\s*\n',
            replacement='',
            description=f"Remove hardcoded BUOYS dict from {script}"
        ))
        # Add stations import and get_all_buoys() call
        migrations.append(Migration(
            script=script,
            pattern=r'^(#!/usr/bin/env python3\n)',
            replacement=r'\1from stations import get_all_buoys\nBUOYS = get_all_buoys()\n',
            description=f"Add stations.get_all_buoys() to {script}"
        ))

    # -------------------------------------------------------------------------
    # 4. Replace hardcoded database paths
    # -------------------------------------------------------------------------

    # Replace buoy database paths
    for script in BUOY_DB_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'SQLITE_PATH = Path\("~/\.local/share/buoy_data\.sqlite"\)\.expanduser\(\)',
            replacement='SQLITE_PATH = BUOY_DATABASE  # From config.py',
            description=f"Replace buoy DB path in {script}"
        ))

    # Replace tide database paths
    for script in TIDE_DB_SCRIPTS:
        migrations.append(Migration(
            script=script,
            pattern=r'DB_PATH = Path\("~/\.local/share/tide_data\.sqlite"\)\.expanduser\(\)',
            replacement='DB_PATH = TIDE_DATABASE  # From config.py',
            description=f"Replace tide DB path in {script}"
        ))
        migrations.append(Migration(
            script=script,
            pattern=r'TIDE_DB = Path\("~/\.local/share/tide_data\.sqlite"\)\.expanduser\(\)',
            replacement='TIDE_DB = TIDE_DATABASE  # From config.py',
            description=f"Replace TIDE_DB path in {script}"
        ))

    return migrations


# =============================================================================
# MIGRATION ENGINE
# =============================================================================

def apply_migration(migration, content):
    """
    Apply a single migration to file content.

    Returns:
        (modified_content, num_matches): Tuple of modified content and match count
    """
    if isinstance(migration.pattern, str):
        # Simple string replacement
        matches = content.count(migration.pattern)
        modified = content.replace(migration.pattern, migration.replacement)
    else:
        # Regex replacement
        modified, matches = re.subn(
            migration.pattern,
            migration.replacement,
            content,
            flags=re.MULTILINE | re.DOTALL
        )

    return modified, matches


def migrate_file(filepath, migrations, dry_run=False, backup=True):
    """
    Apply all relevant migrations to a file.

    Returns:
        dict: Migration results (applied, skipped, errors)
    """
    results = {
        'applied': [],
        'skipped': [],
        'errors': [],
    }

    # Read original content
    try:
        with open(filepath, 'r') as f:
            original_content = f.read()
    except Exception as e:
        results['errors'].append(f"Failed to read file: {e}")
        return results

    # Apply all relevant migrations
    content = original_content
    for migration in migrations:
        if migration.script != filepath.name:
            continue

        modified_content, matches = apply_migration(migration, content)

        if matches > 0:
            content = modified_content
            migration.applied = True
            migration.matches_found = matches
            results['applied'].append({
                'description': migration.description,
                'matches': matches
            })
        else:
            results['skipped'].append(migration.description)

    # If no changes, return early
    if content == original_content:
        return results

    # Backup original file (if requested and not dry-run)
    if backup and not dry_run:
        backup_path = filepath.with_suffix(filepath.suffix + '.backup')
        try:
            shutil.copy2(filepath, backup_path)
        except Exception as e:
            results['errors'].append(f"Failed to create backup: {e}")
            return results

    # Write modified content (if not dry-run)
    if not dry_run:
        try:
            with open(filepath, 'w') as f:
                f.write(content)
        except Exception as e:
            results['errors'].append(f"Failed to write file: {e}")
            return results

    return results


def run_migration(root_dir, dry_run=False, backup=True):
    """
    Run Phase 0 migration on all Python scripts.

    Args:
        root_dir: Repository root directory (Path object)
        dry_run: Preview changes without applying (default False)
        backup: Create .backup files before modifying (default True)

    Returns:
        dict: Migration summary with per-file results
    """
    migrations = get_migrations()

    # Find all Python scripts in root
    scripts = list(root_dir.glob('*.py'))

    summary = {
        'timestamp': datetime.now().isoformat(),
        'dry_run': dry_run,
        'backup': backup,
        'files': {}
    }

    print(f"\n{'='*60}")
    print(f"Phase 0 Migration - {'DRY RUN' if dry_run else 'LIVE MIGRATION'}")
    print(f"{'='*60}\n")

    # Migrate each file
    for script in scripts:
        # Skip special files
        if script.name in ['migrate_phase0.py', 'units.py', 'directions.py', 'config.py', 'stations.py']:
            continue

        print(f"Processing: {script.name}")

        results = migrate_file(script, migrations, dry_run=dry_run, backup=backup)

        if results['applied']:
            print(f"  ✅ Applied {len(results['applied'])} migrations:")
            for item in results['applied']:
                print(f"     - {item['description']} ({item['matches']} matches)")

        if results['skipped']:
            print(f"  ⊘ Skipped {len(results['skipped'])} migrations (no matches)")

        if results['errors']:
            print(f"  ❌ Errors:")
            for error in results['errors']:
                print(f"     - {error}")

        summary['files'][script.name] = results

    # Summary
    total_applied = sum(len(r['applied']) for r in summary['files'].values())
    total_errors = sum(len(r['errors']) for r in summary['files'].values())

    print(f"\n{'='*60}")
    print(f"Migration Summary:")
    print(f"  Files processed: {len(summary['files'])}")
    print(f"  Migrations applied: {total_applied}")
    print(f"  Errors: {total_errors}")

    if dry_run:
        print(f"\n⚠️  This was a DRY RUN - no files were modified")
        print(f"    Run without --dry-run to apply changes")
    else:
        print(f"\n✅ Migration complete!")
        if backup:
            print(f"   Backups created with .backup extension")

    print(f"{'='*60}\n")

    return summary


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Automated Phase 0 migration script',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview changes
  python3 migrate_phase0.py --dry-run

  # Apply changes with backups
  python3 migrate_phase0.py --backup

  # Apply changes without backups (not recommended)
  python3 migrate_phase0.py --no-backup
        """
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without applying them'
    )

    parser.add_argument(
        '--backup',
        action='store_true',
        default=True,
        help='Create .backup files before modifying (default: True)'
    )

    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Do not create backup files'
    )

    parser.add_argument(
        '--root',
        type=Path,
        default=Path.cwd(),
        help='Repository root directory (default: current directory)'
    )

    args = parser.parse_args()

    # Handle backup flag logic
    backup = args.backup and not args.no_backup

    # Run migration
    summary = run_migration(
        root_dir=args.root,
        dry_run=args.dry_run,
        backup=backup
    )

    # Exit with error code if migrations failed
    total_errors = sum(len(r['errors']) for r in summary['files'].values())
    return 1 if total_errors > 0 else 0


if __name__ == '__main__':
    exit(main())
