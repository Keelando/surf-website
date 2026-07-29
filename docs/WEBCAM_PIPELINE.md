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
- **Website:** `site/data/wrcam/`
- **Cron schedule:** `:00, :10, :20, :30, :40, :50` (every hour)

### 2. White Rock East Beach (`boundarybay`)
- **Source:** YouTube livestream (O8RsAq9RUlA)
- **Location:** East Beach, White Rock, BC (49.0042, -123.0128)
- **Resolution:** 480p (full frame)
- **Capture interval:** Every 10 minutes (24/7)
- **Archive:** `/mnt/storage/boundarybay_cam/`
- **Website:** `site/data/bbcam/`
- **Cron schedule:** Currently disabled in cron

### 3. Cox Bay (`coxbay`)
- **Source:** YouTube livestream (LqaP8m2OIqM) - Pacific Sands Beach Resort
- **Location:** Cox Bay, Tofino, BC (49.1167, -125.9000)
- **Resolution:** 720p (full frame)
- **Capture interval:** Every 15 minutes (daylight only)
- **Daylight margin:** ±75 minutes from sunrise/sunset
- **Archive:** `/mnt/storage/coxbay_cam/`
- **Website:** `site/data/coxbay/`
- **Cron schedule:** `:04, :19, :34, :49` (every hour)

### 4. Mud Bay HD (`mudbay`)
- **Source:** Direct image URL (OxBlue construction cam)
- **Location:** Mud Bay, BC (49.0714, -122.9554)
- **Resolution:** 1024x768 (fetched as-is)
- **Capture interval:** Every 30 minutes (daylight only)
- **Daylight margin:** ±75 minutes from sunrise/sunset
- **Archive:** `/mnt/storage/mudbay_cam/`
- **Website:** `site/data/mudbay/`
- **Timestamp annotation:** Enabled
- **Cron schedule:** `:06, :36` (every hour)

### 5. Ambleside - Hollyburn Sailing Club (`ambleside`)
- **Source:** Direct image URL (Hollyburn Sailing Club webcam server). Endpoint and referer live in the gitignored `config/webcams.json` — see permission note below before using.
- **Location:** Hollyburn Sailing Club, West Vancouver, BC (49.3266, -123.1529)
- **Capture interval:** Every 20 minutes (daylight only)
- **Daylight margin:** ±60 minutes from sunrise/sunset
- **Archive:** `/mnt/storage/ambleside_cam/`
- **Website:** `site/data/ambleside/`
- **Cron schedule:** `:08, :28, :48` (every hour)
- **Permission:** Approval granted to halibutbank.ca by Hollyburn Sailing Club (Jan 2026)
  - **IMPORTANT:** If you fork this project, you MUST obtain your own permission from Hollyburn Sailing Club
- **Known issue:** Server intermittently returns HTTP 404 even during daylight hours. This appears to be upstream flakiness (the file is sometimes absent on their server). Failed fetches are logged and the next scheduled run recovers automatically.

## Architecture

### Storage Structure

**Archive directory:** `/mnt/storage/`
- Mounted from external USB SATA drive (223.6GB)
- Contains timestamped archive images for all webcams
- Automatic cleanup when disk usage exceeds 80%

**Website directory:** `site/data/`
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
site/data/[webcam]/latest.jpg (website)
         ↓ (slideshow management)
site/data/[webcam]/slideshow/img_[TIMESTAMP].jpg (last 7 images)
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
**Tool:** curl

**Process:**
1. Download image from URL
2. Optionally annotate with timestamp (ImageMagick)
3. Save to archive directory

**Optional request headers** (set in `config/webcams.json`):
- `image_referer` — `Referer:` header; required by some hosts to serve the image (e.g. Ambleside/Hollyburn)
- `image_user_agent` — `User-Agent:` override (e.g. `HalibutBank/1.0 (+https://halibutbank.ca)`)
- `image_from` — `From:` header with operator contact email; good practice for polite bot identification

**Examples:** Mud Bay HD (OxBlue), Ambleside (Hollyburn Sailing Club)

### Yawcam Server Capture
**Protocol:** HTTP GET with Yawcam-specific parameters

**Process:**
1. Request image from Yawcam server (`http://server:8081/out.jpg?quality=50`)
2. Save to archive directory

**Quality parameter:** 1-100 (50 = balanced quality/size)

**Example:** Previously used for Ambleside (Hollyburn Sailing Club) — now uses direct image URL instead

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
1. Script calls `is_daylight(lat, lon, margin_minutes)` from `lib/daylight.py`
2. Uses the astral library to calculate sunrise/sunset for location
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

**Configuration:** Webcam configs live in `config/webcams.json` (gitignored — keeps endpoints, referers, and permission-gated feeds out of the public repo). The script loads them at startup via `load_webcam_configs()`. See `config/webcams.example.json` for the schema and a sanitized template.

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
ls -lh site/data/wrcam/

# View latest metadata
cat site/data/wrcam/latest.json | jq .
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
# Atomically updated: /home/keelando/envcan_wave/site/data/wrcam/latest.jpg
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

**6. Ambleside intermittent 404s**
```
curl: (22) The requested URL returned error: 404
```
**Solution:** Upstream server flakiness — the Hollyburn Sailing Club server sometimes returns 404 even during daylight. No action needed; the next scheduled run will recover. If 404s persist for several hours during daylight, check manually whether the cam endpoint has changed.

**7. Archive drive went read-only / "Input/output error" on write**
```
touch: cannot touch '/mnt/storage/...': Input/output error
# ...even though `mount` reports the filesystem as rw
```
**Tell:** On the headless server, the *first human-noticeable sign* is often the
**screen turning on by itself** — the flaky USB-SATA bridge throws UAS abort/reset
messages that the kernel prints to the console, which defeats `consoleblank`. The
archive **and** the website used to stop together; after the 2026-06 hardening the
website keeps updating and the logs show loud `ARCHIVE DEGRADED` lines instead.
**Solution:** See **Storage Hardware → Failure modes & recovery** below.

## Storage Hardware

**Primary storage:** `/mnt/storage/` — Kingston A400 SSD in an external USB-SATA enclosure
- **Enclosure bridge:** JMicron JMS583, USB VID:PID `152d:0583` (see `lsusb`)
- **UUID:** `85af7264-6ebb-446c-81e0-94eec769b5d8` (mount by UUID — see warning below)
- **Filesystem:** ext4
- **Capacity:** ~223.6 GB
- **Auto-mount:** Yes (via `/etc/fstab`, with `nofail`)

**Mount configuration:**
```bash
# /etc/fstab entry:
UUID=85af7264-6ebb-446c-81e0-94eec769b5d8  /mnt/storage  ext4  defaults,nofail  0  2
```

> ⚠️ **The device node is NOT stable.** It is usually `/dev/sda1`, but after a USB
> drop the bridge re-enumerates and may come back as `/dev/sdb1`, `/dev/sdc1`, etc.
> Always identify the drive by **UUID** (`lsblk -o NAME,UUID,RO,MOUNTPOINT`), never by
> a hard-coded `/dev/sdX`.

**Verify mount (and that it's actually writable, not just mounted):**
```bash
findmnt /mnt/storage                 # shows source node + rw/ro
lsblk -o NAME,SIZE,RO,UUID,MOUNTPOINT # RO=1 means the kernel set it read-only
sudo -u keelando touch /mnt/storage/whiterock_cam/.probe && \
  sudo -u keelando rm /mnt/storage/whiterock_cam/.probe && echo "WRITABLE"
```
Note: `/mnt/storage` root is `root:root`; only the per-cam subdirs are `keelando`-owned,
so always probe a cam subdir, not the mount root.

### Unplugging / reconnecting the drive

**Golden rule: always unmount before physically unplugging.** A hot-unplug while
mounted is what causes the stranded-mount / aborted-journal mess in *Failure modes*
below — unmounting first flushes writes and closes the journal cleanly, so there's
nothing to recover. You do **not** need to stop cron: the pipeline treats a missing
archive as degraded, keeps the website updating, and just logs `ARCHIVE DEGRADED`.

**Before unplugging:**
```bash
sudo umount /mnt/storage             # if "target is busy": sudo umount -l /mnt/storage
sync
findmnt /mnt/storage || echo "unmounted — safe to unplug"
```

**After reconnecting:**
```bash
# plug in, wait a couple seconds, then:
sudo mount /mnt/storage              # fstab mounts by UUID, so a new /dev/sdX is fine
findmnt /mnt/storage
sudo -u keelando touch /mnt/storage/whiterock_cam/.probe && \
  sudo -u keelando rm /mnt/storage/whiterock_cam/.probe && echo "WRITABLE — archive live again"
```
`mount` is idempotent — if the system auto-mounted it on replug, you'll just get
"already mounted." If you *forgot* to unmount (or the drive dropped on its own),
skip to *Failure modes & recovery* below.

> Note: the UAS quirk is configured on **this** host only. If you take the drive to
> another machine to offload images, the enclosure may run in UAS mode there — fine
> for a quick transfer; just unmount cleanly on that machine too before bringing it back.

### Failure modes & recovery

The USB-SATA bridge has unreliable UAS firmware. Two distinct failure shapes:

1. **Remounted read-only.** ext4 hits I/O errors and flips the mount to `ro`.
   `os.path.ismount()` still returns `True`, so naive checks pass and then every
   write fails. `findmnt` shows `ro`.
2. **Stranded mount after re-enumeration.** The bridge drops off the bus and comes
   back as a *new* device node (e.g. `/dev/sda` → `/dev/sdb`), but `/mnt/storage`
   stays bound to the now-dead old node. `mount` still reports `rw`, yet every write
   returns **`Input/output error`** (`dmesg` shows `device offline error, dev sda`).
   The healthy drive sits *unmounted* under the new node with the correct UUID.

**Diagnose:**
```bash
mount | grep /mnt/storage            # what node is the mount bound to?
ls -l /dev/sda* /dev/sdb*            # does that node still exist?
lsblk -o NAME,UUID,RO,STATE          # where is UUID 85af7264… now, is it RO?
sudo dmesg | grep -iE 'uas|usb|I/O error|EXT4|read-only|reset|152d|sd[a-z]' | tail -50
fuser -vm /mnt/storage               # any userspace process wedged on it?
```

**Recover (case 2 — stranded mount; also works for case 1):**
```bash
sudo umount -l /mnt/storage          # lazy-detach the stale/dead mount
sudo e2fsck -f -y /dev/sdXN          # fsck the LIVE node (from lsblk UUID); recovers journal
sudo mount /mnt/storage              # fstab re-resolves UUID to the live node
# then re-run the writable probe above to confirm
```
A clean `e2fsck` exit 1 ("FILE SYSTEM WAS MODIFIED") is normal — it just means the
aborted journal was replayed and free counts corrected.

### Hardware mitigation: disable UAS (applied 2026-06-15)

The bridge is stable in plain bulk (BOT) transport; only UAS is flaky. We disable UAS
for this VID:PID via the kernel cmdline:
```bash
# /etc/default/grub — appended to GRUB_CMDLINE_LINUX_DEFAULT:
usb-storage.quirks=152d:0583:u       # :u = IGNORE_UAS → forces bulk transport
sudo update-grub                     # then reboot to apply
```
**Verify after reboot:**
```bash
cat /sys/module/usb_storage/parameters/quirks   # → 152d:0583:u
lsusb -t                             # the JMS583 shows Driver=usb-storage, NOT uas
```
To undo: remove the `usb-storage.quirks=…` token (backup at `/etc/default/grub.bak.*`)
and `sudo update-grub`. **Longer-term option:** retire the enclosure entirely by moving
the archive to network storage — if so, mount fail-fast (NFS `soft,nofail,timeo=…` or
CIFS), never an NFS hard mount (D-state hangs survive even the script's write timeout).

### Why the website survives this now

`fetch_webcam.py` (hardened 2026-06-15) captures every frame to a **local temp file**,
updates the website (`latest.jpg` / slideshow / `latest.json`) from it **unconditionally**,
and only then copies to the archive as a **best-effort** step. It detects a non-writable
archive with a real **write+fsync probe** (not `os.path.ismount()`) and bounds it with a
SIGALRM **timeout** so a wedged bridge can't hang or stack cron jobs. A dead archive now
costs you the archive copy only — the live site keeps updating, and degradation is logged
loudly (`ARCHIVE DEGRADED`) and surfaced via the `newest_image_age` MQTT sensor.

## Frontend Integration

**Website:** `site/webcams.html`

**JavaScript:** `site/assets/js/webcams-v4.js`

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
- Repository: `~/envcan_wave` (frontend in `site/`)
- Includes: `latest.jpg`, `slideshow/`, `latest.json`, etc.

## Adding a New Webcam

1. **Add a config entry** to `config/webcams.json` (gitignored; see `config/webcams.example.json` for the schema). Paths in `archive_dir` are absolute; `website_dir` is resolved relative to the repo root.
```json
"newcam": {
  "name": "New Camera Name",
  "youtube_url": "https://www.youtube.com/watch?v=VIDEO_ID",
  "video_id": "VIDEO_ID",
  "archive_dir": "/mnt/storage/newcam_cam",
  "website_dir": "site/data/newcam",
  "prefix": "NC",
  "crop": "in_w:in_h:0:0",
  "source_text": "New Camera - Source Attribution",
  "lat": 49.0000,
  "lon": -123.0000,
  "max_height": 720,
  "check_daylight": true,
  "daylight_margin_minutes": 75,
  "interval_minutes": 15,
  "cron_offset": 10
}
```
For a **direct-image** source instead of YouTube, swap `youtube_url`/`video_id` for `image_url`. Optional keys `image_referer`, `image_user_agent`, and `image_from` are available — use them if the host requires a referer, and set UA/From as good bot-identification practice. Only fetch with permission from the source.

The "Source:" link written into `latest.json` prefers `source_url`, falling back to `youtube_url` then `image_url`. Set `source_url` to the operator's own public page for the cam whenever the feed URL isn't something a visitor should be sent to (e.g. a raw scraped `.jpg`).

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
ls -lh site/data/newcam/
```

5. **Add to frontend** (`site/webcams.html`)

---

**Last updated:** May 2026
**Maintainer:** Keelando
**Live site:** [halibutbank.ca/webcams.html](https://halibutbank.ca/webcams.html)
