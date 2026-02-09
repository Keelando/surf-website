# Webcam Pipeline Documentation

Complete documentation for the webcam capture, storage, and display system.

## Overview

The system captures images from 5 webcam sources (YouTube livestreams, direct URLs, and Yawcam servers), archives them to external storage, and serves them to the website with slideshow functionality.

## Webcam Sources

### 1. White Rock Pier (`whiterock`)
- **Source:** YouTube livestream (4MK3E9EWDSY)
- **Location:** White Rock Pier, Semiahmoo Bay, BC (49.0253, -122.8031)
- **Resolution:** 720p (cropped to remove street, keep pier/ocean)
- **Capture interval:** Every 10 minutes (24/7)
- **Archive:** `/mnt/storage/whiterock_cam/`
- **Website:** `~/site/data/wrcam/`
- **Cron schedule:** `:00, :10, :20, :30, :40, :50` (every hour)

### 2. White Rock East Beach (`boundarybay`)
- **Source:** YouTube livestream (O8RsAq9RUlA)
- **Location:** East Beach, White Rock, BC (49.0042, -123.0128)
- **Resolution:** 480p (full frame)
- **Capture interval:** Every 10 minutes (24/7)
- **Archive:** `/mnt/storage/boundarybay_cam/`
- **Website:** `~/site/data/bbcam/`
- **Cron schedule:** Currently disabled in cron

### 3. Cox Bay (`coxbay`)
- **Source:** YouTube livestream (LqaP8m2OIqM) - Pacific Sands Beach Resort
- **Location:** Cox Bay, Tofino, BC (49.1167, -125.9000)
- **Resolution:** 720p (full frame)
- **Capture interval:** Every 15 minutes (daylight only)
- **Daylight margin:** ±75 minutes from sunrise/sunset
- **Archive:** `/mnt/storage/coxbay_cam/`
- **Website:** `~/site/data/coxbay/`
- **Cron schedule:** `:04, :19, :34, :49` (every hour)

### 4. Mud Bay HD (`mudbay`)
- **Source:** Direct image URL (OxBlue construction cam)
- **Location:** Mud Bay, BC (49.0714, -122.9554)
- **Resolution:** 1024x768 (fetched as-is)
- **Capture interval:** Every 30 minutes (daylight only)
- **Daylight margin:** ±75 minutes from sunrise/sunset
- **Archive:** `/mnt/storage/mudbay_cam/`
- **Website:** `~/site/data/mudbay/`
- **Timestamp annotation:** Enabled
- **Cron schedule:** `:06, :36` (every hour)

### 5. Ambleside - Hollyburn Sailing Club (`ambleside`)
- **Source:** Yawcam server (onsite.hollyburnsailingclub.ca:8081)
- **Location:** Hollyburn Sailing Club, West Vancouver, BC (49.3266, -123.1529)
- **Quality:** 50 (Yawcam quality setting)
- **Capture interval:** Every 20 minutes (daylight only)
- **Daylight margin:** ±60 minutes from sunrise/sunset
- **Archive:** `/mnt/storage/ambleside_cam/`
- **Website:** `~/site/data/ambleside/`
- **Cron schedule:** `:08, :28, :48` (every hour)
- **Permission:** Approval granted to halibutbank.ca by Hollyburn Sailing Club (Jan 2026)
  - **IMPORTANT:** If you fork this project, you MUST obtain your own permission from Hollyburn Sailing Club

## Architecture

### Storage Structure

**Archive directory:** `/mnt/storage/`
- Mounted from external USB SATA drive (223.6GB)
- Contains timestamped archive images for all webcams
- Automatic cleanup when disk usage exceeds 80%

**Website directory:** `~/site/data/`
- Contains latest images and slideshow for each webcam
- Served by Caddy web server on port 8090

### File Naming Convention

**Archive files:**
```
[PREFIX]_[YYYYMMDD]_[HHMMSS]_[UNIX_TIMESTAMP].jpg

Example:
WR_20260208_231218_1770592338.jpg
│  │              │         └─ Unix timestamp (for sorting)
│  │              └─ Time (23:12:18 UTC)
│  └─ Date (Feb 8, 2026)
└─ Prefix (WR = White Rock)
```

**Prefixes:**
- `WR` - White Rock Pier
- `BB` - Boundary Bay (East Beach)
- `CB` - Cox Bay
- `MB` - Mud Bay
- `AB` - Ambleside

**Website files:**
- `latest.jpg` - Most recent capture (atomically updated)
- `latest.json` - Metadata (timestamp, source, URL)
- `slideshow/img_[UNIX_TIMESTAMP].jpg` - Slideshow images (last 7 captures)
- `slideshow_manifest.json` - Index of slideshow images

## Data Flow

```
YouTube Livestream / Direct URL / Yawcam Server
         ↓
fetch_webcam.py (yt-dlp + ffmpeg / HTTP GET / Yawcam API)
         ↓
/mnt/storage/[webcam]_cam/[PREFIX]_[TIMESTAMP].jpg (archive)
         ↓ (atomic copy)
~/site/data/[webcam]/latest.jpg (website)
         ↓ (slideshow management)
~/site/data/[webcam]/slideshow/img_[TIMESTAMP].jpg (last 7 images)
         ↓
Website (webcams.html) displays latest + slideshow carousel
```

## Capture Methods

### YouTube Livestream Capture
**Tools:** yt-dlp + Deno

**Process:**
1. yt-dlp downloads video segment (format selection by max_height)
2. Deno extracts frame from segment
3. ffmpeg crops image (if crop parameter specified)
4. Image saved to archive directory

**Format selection:**
- `max_height: 720` → 720p stream
- `max_height: 480` → 480p stream (default)

**Cropping:**
- White Rock: `in_w*0.75:in_h:in_w*0.25:0` (crops left 25%, keeps right 75%)
- Others: `in_w:in_h:0:0` (full frame)

### Direct Image URL Capture
**Tool:** HTTP GET with requests library

**Process:**
1. Download image from URL
2. Optionally annotate with timestamp (ImageMagick)
3. Save to archive directory

**Example:** Mud Bay HD (OxBlue construction cam)

### Yawcam Server Capture
**Protocol:** HTTP GET with Yawcam-specific parameters

**Process:**
1. Request image from Yawcam server (`http://server:8081/out.jpg?quality=50`)
2. Save to archive directory

**Quality parameter:** 1-100 (50 = balanced quality/size)

**Example:** Ambleside (Hollyburn Sailing Club)

## Daylight Detection

Some webcams only capture during daylight hours to avoid useless nighttime images.

**Daylight-aware webcams:**
- Cox Bay (±75 min margin)
- Mud Bay (±75 min margin)
- Ambleside (±60 min margin)

**24/7 webcams:**
- White Rock Pier (urban area with lighting)
- White Rock East Beach

**How it works:**
1. Script calls `is_daylight(lat, lon, margin_minutes)` from `scripts/utils/daylight.py`
2. Uses Skyfield library to calculate sunrise/sunset for location
3. Adds margin (e.g., ±75 min) to capture golden hour
4. Exits gracefully if outside daylight window

**Rationale:** Saves bandwidth, storage, and reduces meaningless nighttime captures

## Storage Management

### Archive Cleanup

**Trigger:** Disk usage exceeds 80%

**Cleanup process:**
1. Check disk usage of `/mnt/storage/`
2. If > 80%, delete oldest images until usage reaches 75%
3. Always keep at least 24 hours of recent images
4. Log deletion count and freed space

**Implementation:** `cleanup_old_archives()` in `lib/webcam/storage.py`

### Slideshow Management

**Slideshow images:** Last 7 captures per webcam

**Process:**
1. Copy latest capture to `slideshow/img_[UNIX_TIMESTAMP].jpg`
2. Get list of all slideshow images, sorted by timestamp (newest first)
3. Keep only the 7 most recent
4. Delete older images
5. Generate `slideshow_manifest.json` with metadata

**Manifest structure:**
```json
[
  {
    "filename": "img_1770592338.jpg",
    "timestamp": "2026-02-08T23:12:18+00:00",
    "path": "slideshow/img_1770592338.jpg"
  },
  ...
]
```

**Implementation:** `manage_slideshow_images()` in `lib/webcam/storage.py`

## Cron Schedule

**From:** `~/envcan_wave/config/crontab.txt`

```bash
# ==================== WEBCAMS ====================

# White Rock Pier: Every 10 minutes (24/7)
0,10,20,30,40,50 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_webcam.py whiterock >> /home/keelando/envcan_wave/logs/webcam_whiterock.log 2>&1

# Boundary Bay (East Beach): Every 10 minutes (DISABLED)
# 2,12,22,32,42,52 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_webcam.py boundarybay >> /home/keelando/envcan_wave/logs/webcam_boundarybay.log 2>&1

# Cox Bay: Every 15 minutes at :04, :19, :34, :49 (daylight only)
4,19,34,49 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_webcam.py coxbay >> /home/keelando/envcan_wave/logs/webcam_coxbay.log 2>&1

# Mud Bay HD: Every 30 minutes at :06, :36 (daylight only)
6,36 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_webcam.py mudbay >> /home/keelando/envcan_wave/logs/webcam_mudbay.log 2>&1

# Ambleside (Hollyburn Sailing Club): Every 20 minutes at :08, :28, :48 (daylight only)
8,28,48 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_webcam.py ambleside >> /home/keelando/envcan_wave/logs/webcam_ambleside.log 2>&1
```

**Staggered execution:**
- Prevents multiple webcams from capturing simultaneously
- Reduces CPU/network spikes
- Each webcam has unique minute offsets

## Script Reference

### Main Script: `scripts/fetch/fetch_webcam.py`

**Usage:**
```bash
python3 fetch_webcam.py <config_name>

# Examples:
python3 fetch_webcam.py whiterock
python3 fetch_webcam.py coxbay
python3 fetch_webcam.py mudbay
python3 fetch_webcam.py ambleside
```

**Configuration:** All webcam configs defined in `WEBCAM_CONFIGS` dict in the script

**Logging:** Centralized via `lib/logging_config.py`
- Log file: `~/envcan_wave/logs/webcam_<config_name>.log`
- Format: `YYYY-MM-DD HH:MM:SS - webcam_<config> - LEVEL - message`

### Library Modules

**`lib/webcam/__init__.py`** - Public API exports
- `capture_youtube_frame()`
- `download_image()`
- `capture_yawcam_image()`
- `annotate_image()`
- `cleanup_old_archives()`
- `manage_slideshow_images()`

**`lib/webcam/youtube.py`** - YouTube livestream capture
- Handles yt-dlp + Deno frame extraction
- Format selection by max_height
- Error handling and logging

**`lib/webcam/sources.py`** - Direct URL and Yawcam capture
- HTTP GET for direct image URLs
- Yawcam API integration
- Timestamp annotation with ImageMagick

**`lib/webcam/storage.py`** - Archive and slideshow management
- Disk usage monitoring and cleanup
- Slideshow image rotation
- Manifest generation

## Dependencies

**System packages:**
- `yt-dlp` - YouTube stream download
- `ffmpeg` - Video processing and cropping
- `imagemagick` - Image annotation (for timestamp overlays)
- `deno` - JavaScript runtime for frame extraction

**Python packages:**
- `requests` - HTTP client for direct URLs
- `Pillow` - Image processing (if needed)
- `skyfield` - Astronomical calculations for daylight detection

**Install:**
```bash
# System packages
sudo apt install yt-dlp ffmpeg imagemagick

# Deno (if not installed)
curl -fsSL https://deno.land/install.sh | sh

# Python packages (in venv)
source ~/envcan_wave/.venv/bin/activate
pip install requests pillow skyfield
```

## Monitoring & Troubleshooting

### Check Webcam Status

```bash
# View recent logs
tail -50 ~/envcan_wave/logs/webcam_whiterock.log

# Check archive directory
ls -lh /mnt/storage/whiterock_cam/ | tail -10

# Check website directory
ls -lh ~/site/data/wrcam/

# View latest metadata
cat ~/site/data/wrcam/latest.json | jq .
```

### Test Webcam Capture Manually

```bash
cd ~/envcan_wave
source .venv/bin/activate

# Test capture
python3 scripts/fetch/fetch_webcam.py whiterock

# Expected output:
# === White Rock Pier Cam Webcam Capture Started ===
# Capturing frame from YouTube: https://www.youtube.com/watch?v=4MK3E9EWDSY
# Deno capture successful (165.2 KB)
# Atomically updated: /home/keelando/site/data/wrcam/latest.jpg
# === White Rock Pier Cam Webcam Capture Completed Successfully ===
```

### Common Issues

**1. YouTube stream offline**
```
Error: Video unavailable
```
**Solution:** Check if YouTube stream is live. If down permanently, update video_id in config.

**2. Disk full**
```
Error: No space left on device
```
**Solution:** Run cleanup manually or lower threshold in config.

**3. yt-dlp outdated**
```
Error: Unable to extract video info
```
**Solution:**
```bash
sudo apt update && sudo apt install yt-dlp
# Or: pip install -U yt-dlp
```

**4. Daylight check preventing capture**
```
Skipping capture - it's nighttime
```
**Solution:** This is normal. Check logs during daylight hours, or disable `check_daylight` in config.

**5. Permission denied on Yawcam**
```
Error: HTTP 403 Forbidden
```
**Solution:** Verify Yawcam server URL and permissions. Contact webcam owner if needed.

## Storage Hardware

**Primary storage:** `/mnt/storage/` on external USB SATA drive
- **Device:** `/dev/sda1`
- **UUID:** `85af7264-6ebb-446c-81e0-94eec769b5d8`
- **Filesystem:** ext4
- **Capacity:** 223.6GB
- **Auto-mount:** Yes (via `/etc/fstab`)

**Mount configuration:**
```bash
# /etc/fstab entry:
UUID=85af7264-6ebb-446c-81e0-94eec769b5d8  /mnt/storage  ext4  defaults  0  2
```

**Verify mount:**
```bash
df -h /mnt/storage
mount | grep /mnt/storage
```

## Frontend Integration

**Website:** `~/site/webcams.html`

**JavaScript:** `~/site/assets/js/webcams-v4.js`

**Features:**
- Display latest webcam images
- Slideshow carousel (last 7 captures)
- Timestamp display (Pacific time)
- Auto-refresh
- Source attribution

**API endpoints:**
- `GET /data/wrcam/latest.jpg` - Latest White Rock image
- `GET /data/wrcam/latest.json` - Metadata
- `GET /data/wrcam/slideshow_manifest.json` - Slideshow index
- `GET /data/wrcam/slideshow/img_[TIMESTAMP].jpg` - Slideshow images

## Backup

**Webcam images:** Archived to `/mnt/storage/` (backed up by restic)

**Restic backup includes:**
- Archive images in `/mnt/storage/`
- Restic repository: `/mnt/storage/restic-backup`
- Daily automated backup at 2:30 AM
- Retention: 7 daily + 4 weekly snapshots

**Restic restore:**
```bash
# List snapshots
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup snapshots

# Restore webcam archives
sudo RESTIC_PASSWORD_FILE="/root/.restic_pw" restic -r /mnt/storage/restic-backup restore latest \
  --target /tmp/restore \
  --include /mnt/storage/whiterock_cam
```

**Website files:** Auto-backed up to git nightly at 11:04 PM
- Repository: `~/site`
- Includes: `latest.jpg`, `slideshow/`, `latest.json`, etc.

## Adding a New Webcam

1. **Add configuration** to `WEBCAM_CONFIGS` in `scripts/fetch/fetch_webcam.py`:
```python
"newcam": {
    "name": "New Camera Name",
    "youtube_url": "https://www.youtube.com/watch?v=VIDEO_ID",  # or image_url or yawcam_url
    "video_id": "VIDEO_ID",
    "archive_dir": Path("/mnt/storage/newcam_cam"),
    "website_dir": Path.home() / "site" / "data" / "newcam",
    "prefix": "NC",
    "crop": "in_w:in_h:0:0",  # Full frame
    "source_text": "New Camera - Source Attribution",
    "lat": 49.0000,
    "lon": -123.0000,
    "max_height": 720,
    "check_daylight": True,
    "daylight_margin_minutes": 75,
    "interval_minutes": 15,
    "cron_offset": 10  # Unique offset to avoid conflicts
}
```

2. **Add cron job**:
```bash
crontab -e

# Add line (adjust minute offsets based on interval_minutes and cron_offset):
10,25,40,55 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/scripts/fetch/fetch_webcam.py newcam >> /home/keelando/envcan_wave/logs/webcam_newcam.log 2>&1
```

3. **Test manually**:
```bash
cd ~/envcan_wave
source .venv/bin/activate
python3 scripts/fetch/fetch_webcam.py newcam
```

4. **Verify storage created**:
```bash
ls -lh /mnt/storage/newcam_cam/
ls -lh ~/site/data/newcam/
```

5. **Add to frontend** (`~/site/webcams.html`)

---

**Last updated:** February 2026
**Maintainer:** Keelando
**Live site:** [halibutbank.ca/webcams.html](https://halibutbank.ca/webcams.html)
