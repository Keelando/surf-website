# Backup Status - Updated 2025-12-08

## Current Backup Configuration

### Git Auto-Backups (Nightly at 11 PM)
**Status:** ✅ **FIXED**

- **Backend repo** (`~/envcan_wave`): 23:03 nightly
- **Frontend repo** (`~/site`): 23:04 nightly  
- **Destination:** GitHub (keelando/envcan_wave, keelando/site)
- **Issue fixed:** Added `cd` command to cron job (was failing with "not a git repository")

### Restic System Backups (Daily at 2:30 AM)
**Status:** ✅ **CONFIGURED**

- **Location:** `/mnt/storage/restic-backup` (2nd internal drive - 240GB Kingston SATA SSD)
- **Repository:** Initialized Dec 8, 2025
- **Retention:** 7 daily + 4 weekly snapshots
- **What's backed up:**
  - `/home` (user data, projects, configs)
  - `/etc` (system configuration)
  - `/var/lib` (application data)
  - `/root` (root user data)
  - Package lists, crontabs, systemd services

- **Exclusions:**
  - `.cache`, `.venv`, `__pycache__`
  - `*.log`, `*.tmp` files
  - XML files in `~/envcan_wave/data/buoy/`

## Drive Layout

```
┌─────────────────────────────────────────┐
│  Drive 1: 256GB NVMe (nvme0n1)          │
│  ─────────────────────────────          │
│  • OS / root filesystem                 │
│  • /home/keelando/envcan_wave (1.3GB)  │
│  • /home/keelando/site (21MB)           │
│  • Databases (~395MB in .local/share)  │
│  • 204GB free                           │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│  Drive 2: 240GB Kingston SATA (sda)     │
│  ────────────────────────────────────   │
│  Mounted: /mnt/storage                  │
│  • boundarybay_cam/ (48MB)             │
│  • whiterock_cam/ (60MB)               │
│  • restic-backup/ (system backups)     │
│  • 208GB free                           │
└─────────────────────────────────────────┘
```

## Testing Backups

### Test Git Backup Manually
```bash
cd ~/envcan_wave
git add -A && git diff --staged --quiet || \
  (git commit -m "Test backup $(date +%Y-%m-%d)" && git push origin main)
```

### Test Restic Backup Manually
```bash
sudo /home/keelando/backup_surf.sh
```

### View Restic Snapshots
```bash
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" \
  restic -r /mnt/storage/restic-backup snapshots
```

### Restore from Restic
```bash
# List files in latest snapshot
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" \
  restic -r /mnt/storage/restic-backup ls latest

# Restore specific file
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" \
  restic -r /mnt/storage/restic-backup restore latest \
  --target /tmp/restore --include /path/to/file
```

## Backup Schedule

```
23:02 - Export crontab to git repo
23:03 - Git backup: envcan_wave repo
23:04 - Git backup: site repo  
02:30 - Restic system backup (full system snapshot)
```

## Important Notes

- **Password file:** `/root/.restic_pw` (contains: "surfboard")
- **Restic repo initialized:** Dec 8, 2025 (repository ID: e8992cdcdd)
- **Previous issue:** Restic was configured for external USB, but that USB is no longer connected
- **Current setup:** Uses 2nd internal SATA SSD for all backups (separate from system drive)
