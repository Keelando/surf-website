# Feature Plan: Cox Bay Webcam Integration

## Overview
Add Cox Bay (Tofino area, West Coast Vancouver Island) webcam to the website's webcam page. This will provide the third webcam view alongside White Rock East Beach and Boundary Bay, extending coverage to the Pacific Ocean coast.

**YouTube Source:** https://www.youtube.com/watch?v=LqaP8m2OIqM

---

## Webcam Coordinates (Hard-Coded Reference)

### White Rock Pier Cam
- **Coordinates**: 49.021719°N, 122.807111°W (WGS84)
- **Decimal**: `lat: 49.021719, lon: -122.807111`
- **Location**: White Rock Pier, Semiahmoo Bay

### White Rock East Beach (Boundary Bay)
- **Coordinates**: 49.01647°N, 122.79082°W (WGS84)
- **Decimal**: `lat: 49.01647, lon: -122.79082`
- **Location**: East Beach, White Rock

### Cox Bay
- **Coordinates**: 49.106802°N, 125.872949°W (WGS84)
- **Decimal**: `lat: 49.106802, lon: -125.872949`
- **Location**: Pacific Sands Beach Resort, Cox Bay, near Tofino, West Coast Vancouver Island

**Coordinate Reference System**: WGS84 (World Geodetic System 1984)
**Precision**: 6 decimal places (~0.1 meter accuracy)

---

## Location Context

**Cox Bay:**
- Location: Near Tofino, West Coast Vancouver Island
- Coordinates: 49.106802°N, 125.872949°W
- Significance: Popular surf spot, exposed to Pacific Ocean swells
- Complements existing coverage:
  - White Rock Pier: 49.021719°N, 122.807111°W - Semiahmoo Bay (urban, sheltered)
  - White Rock East Beach: 49.01647°N, 122.79082°W - Boundary Bay (shallow, tidal flats)
  - Cox Bay: 49.106802°N, 125.872949°W - **Pacific Ocean coast (exposed, surf conditions)**

**Value for Users:**
- Mariners: Pacific Ocean conditions, wave action
- Surfers: Real-time surf and swell conditions
- Weather enthusiasts: West Coast weather patterns
- Researchers: Pacific coastal conditions vs. inland waters

---

## Implementation Steps

### Phase 1: Backend Configuration (15 minutes)

#### Step 1: Add Cox Bay to `fetch_webcam.py`

**File:** `/home/keelando/envcan_wave/fetch_webcam.py`

**Location:** Lines 28-47 (WEBCAM_CONFIGS dictionary)

**Add new configuration:**
```python
    "coxbay": {
        "name": "Cox Bay",
        "youtube_url": "https://www.youtube.com/watch?v=LqaP8m2OIqM",
        "archive_dir": Path("/mnt/storage/coxbay_cam"),
        "website_dir": Path.home() / "site" / "data" / "coxbay",
        "prefix": "CB",
        "crop": "in_w:in_h:0:0",  # Full frame initially - adjust after testing
        "source_text": "Cox Bay (Tofino) YouTube Livestream"
    }
```

**Initial crop setting:** Full frame (`in_w:in_h:0:0`)
- Test first capture to see if cropping needed
- May want to remove sky/clouds to focus on surf/beach
- May want to remove static elements (buildings, signs)

#### Step 2: Create Required Directories

```bash
# Archive directory (long-term storage on /mnt/storage)
sudo mkdir -p /mnt/storage/coxbay_cam
sudo chown keelando:keelando /mnt/storage/coxbay_cam

# Website directory (served via Caddy)
mkdir -p ~/site/data/coxbay
mkdir -p ~/site/data/coxbay/slideshow
```

#### Step 3: Test Manual Capture

```bash
cd /home/keelando/envcan_wave
./.venv/bin/python3 fetch_webcam.py coxbay
```

**Validation checks:**
1. Image successfully captured to `/mnt/storage/coxbay_cam/`
2. `latest.jpg` created in `~/site/data/coxbay/`
3. `latest.json` metadata created
4. Slideshow directory populated
5. `slideshow_manifest.json` created
6. Check image for quality, framing, and content

**Adjust crop if needed:**
- If too much sky: crop to lower portion
- If static elements: crop them out
- Example crop (keep bottom 75%): `"crop": "in_w:in_h*0.75:0:in_h*0.25"`

#### Step 4: Add Cron Job

**Edit crontab:**
```bash
crontab -e
```

**Add new line (fetch every 10 minutes):**
```cron
# Cox Bay webcam (Tofino) - every 10 minutes
*/10 * * * * /home/keelando/envcan_wave/.venv/bin/python3 /home/keelando/envcan_wave/fetch_webcam.py coxbay >> /home/keelando/envcan_wave/webcam_coxbay.log 2>&1
```

**Frequency rationale:**
- 10 minutes: Balances freshness with storage/bandwidth
- Matches other webcam fetch frequencies
- 144 images per day = ~47 MB/day (at ~330 KB/image)
- Annual storage: ~17 GB (manageable with disk cleanup)

---

### Phase 2: Website Integration (30 minutes)

#### Step 1: Find and Update Webcams Page

Need to locate the webcams page HTML file. Likely:
- `/home/keelando/site/webcams.html` (if exists)
- OR embedded in another page (index.html, conditions.html, etc.)

**If webcams page doesn't exist yet:** Need to create it first.

#### Step 2: Add Cox Bay to Webcam Display

**Pattern to follow (from existing webcams):**

```html
<!-- Cox Bay Webcam -->
<div class="webcam-container">
  <div class="webcam-header">
    <h2>🌊 Cox Bay (Tofino)</h2>
    <span class="webcam-location">West Coast Vancouver Island</span>
  </div>

  <div class="webcam-image-wrapper">
    <img id="coxbay-latest"
         src="/data/coxbay/latest.jpg"
         alt="Cox Bay current conditions"
         loading="lazy">
    <div class="webcam-timestamp" id="coxbay-timestamp">Loading...</div>
  </div>

  <!-- Slideshow (if implemented) -->
  <div class="webcam-slideshow" id="coxbay-slideshow">
    <button class="slideshow-btn" onclick="previousImage('coxbay')">‹</button>
    <div class="slideshow-images" id="coxbay-slideshow-images"></div>
    <button class="slideshow-btn" onclick="nextImage('coxbay')">›</button>
  </div>

  <div class="webcam-footer">
    <a href="https://www.youtube.com/watch?v=LqaP8m2OIqM"
       target="_blank"
       rel="noopener noreferrer">
      📹 View Live Stream
    </a>
  </div>
</div>
```

#### Step 3: Update JavaScript for Auto-Refresh

**Add Cox Bay to refresh logic:**

```javascript
// Refresh webcam images
function refreshWebcams() {
  refreshWebcam('whiterock', '/data/wrcam/latest.json', '/data/wrcam/latest.jpg');
  refreshWebcam('boundarybay', '/data/bbcam/latest.json', '/data/bbcam/latest.jpg');
  refreshWebcam('coxbay', '/data/coxbay/latest.json', '/data/coxbay/latest.jpg');
}

// Refresh every 2 minutes
setInterval(refreshWebcams, 120000);
```

#### Step 4: Add Navigation Link (if needed)

If webcams page is separate, add navigation link in main menu:

```html
<nav>
  <a href="/">Buoys</a>
  <a href="/tides.html">Tides</a>
  <a href="/winds.html">Winds</a>
  <a href="/webcams.html">Webcams</a>  <!-- Add if missing -->
  <!-- ... -->
</nav>
```

---

## Technical Considerations

### Image Quality & Size

**Expected file sizes (based on existing webcams):**
- White Rock: ~260 KB average
- Boundary Bay: ~179 KB average
- **Cox Bay estimate: 200-250 KB** (ocean scenes compress well)

**Compression settings:**
- Already configured: `-q:v 5` (high quality JPEG)
- Good balance for coastal scenery

### Storage Management

**Archive retention (automatic cleanup):**
- Cleanup triggers at 80% disk usage
- Keeps minimum 24 hours of images
- Deletes oldest first until disk usage drops to 75%

**Estimated storage:**
- Daily: ~50 MB (144 images × 350 KB)
- Monthly: ~1.5 GB
- Yearly: ~18 GB
- **Total for all 3 webcams: ~54 GB/year**

**Current disk space on `/mnt/storage/`:**
```bash
df -h /mnt/storage/
```
Verify sufficient space available.

### Stream Availability & Reliability

**Potential issues:**
- YouTube stream may go offline (weather, maintenance)
- Stream URL may change (requires yt-dlp to resolve)
- ffmpeg capture may fail (timeout, network issues)

**Error handling (already implemented):**
- Timeouts: 30s for yt-dlp, 60s for ffmpeg
- Logging: All errors logged to `webcam_coxbay.log`
- Graceful failure: Keeps existing `latest.jpg` if fetch fails

### Performance Impact

**Web page load time:**
- 3 webcams × ~220 KB average = ~660 KB total images
- With lazy loading: Only loads visible images
- Minimal impact on page performance

**Server resources:**
- Cron job runs every 10 minutes
- Each run: ~60s max (yt-dlp + ffmpeg)
- CPU usage: Brief spike during encoding
- Bandwidth: ~220 KB download per capture

---

## Testing Checklist

### Backend Testing

- [ ] Manual capture test successful
- [ ] Image appears in archive directory
- [ ] Image appears in website directory
- [ ] Metadata JSON file created correctly
- [ ] Slideshow manifest created
- [ ] Log file shows no errors
- [ ] Cron job executes successfully
- [ ] Multiple captures over 1 hour work correctly
- [ ] Old slideshow images pruned (only 5 kept)

### Frontend Testing

- [ ] Webcams page loads correctly
- [ ] Cox Bay image displays
- [ ] Timestamp updates correctly
- [ ] Auto-refresh works (2-minute interval)
- [ ] Slideshow navigation works (if implemented)
- [ ] YouTube live stream link works
- [ ] Responsive design (mobile, tablet, desktop)
- [ ] Lazy loading works correctly
- [ ] No console errors

### Integration Testing

- [ ] All 3 webcams display simultaneously
- [ ] No performance degradation
- [ ] Caddy serves images correctly
- [ ] Cache headers set appropriately
- [ ] Images accessible from external network

---

## Potential Enhancements (Future)

### V1.1: Image Cropping Optimization
- Analyze captured images to identify optimal crop
- Remove static elements (buildings, signs)
- Focus on surf zone and wave action
- Adjust for best view of conditions

### V1.2: Surf Condition Annotations
- Overlay wave height estimate (if available)
- Overlay wind direction arrow
- Add tide level indicator
- Color-coded surf rating (flat, small, medium, large)

### V1.3: Time-Lapse Generation
- Daily time-lapse video from slideshow images
- Show wave progression over 24 hours
- Export as MP4 for embedding

### V1.4: Multi-Webcam Comparison View
- Side-by-side view of all 3 webcams
- Synchronized timestamps
- Toggle between locations
- Useful for comparing conditions across region

### V1.5: Archive Browser
- Calendar view to select historical dates
- View past conditions by date/time
- Download historical images
- Compare conditions (e.g., "same time yesterday")

---

## Documentation Updates Needed

### User-Facing Documentation

**Add to website:**
- Webcams page description
- Cox Bay location information
- Interpretation guide (reading surf conditions)
- Update frequency notice (every 10 minutes)

**Example text:**
```markdown
## Cox Bay Webcam

Located near Tofino on the West Coast of Vancouver Island, Cox Bay is a
popular surf spot exposed to Pacific Ocean swells. This webcam provides
real-time views of surf conditions, wave action, and coastal weather.

**Update Frequency:** Every 10 minutes
**Source:** [YouTube Live Stream](https://www.youtube.com/watch?v=LqaP8m2OIqM)

Use this webcam to assess:
- Surf conditions and wave size
- Weather visibility and cloud cover
- Tidal state (beach exposure)
- Overall ocean conditions
```

### Technical Documentation

**Update README/docs:**
- List of webcam sources
- Configuration parameters
- Storage locations
- Maintenance procedures
- Troubleshooting guide

---

## Rollback Plan

If issues arise, rollback steps:

1. **Disable cron job:**
   ```bash
   crontab -e
   # Comment out Cox Bay line
   ```

2. **Remove from website:**
   - Comment out Cox Bay HTML section
   - Remove from JavaScript refresh

3. **Preserve captured data:**
   - Archive directory remains intact
   - Can re-enable later without data loss

4. **Investigate issues:**
   - Check logs: `tail -f ~/envcan_wave/webcam_coxbay.log`
   - Test manual capture
   - Verify YouTube stream availability

---

## Success Metrics

### Functionality Metrics
- [ ] Capture success rate > 95%
- [ ] Image quality acceptable
- [ ] Update latency < 2 minutes (from capture to display)
- [ ] No errors in logs for 24 hours
- [ ] Page load time < 3 seconds with all webcams

### User Engagement (Optional)
- [ ] Webcams page views
- [ ] Average time on page
- [ ] Click-through rate to YouTube stream
- [ ] Mobile vs. desktop usage

---

## Timeline

### Immediate (Today)
1. Add configuration to `fetch_webcam.py` (5 min)
2. Create directories (2 min)
3. Test manual capture (5 min)
4. Adjust crop if needed (5 min)
5. Add cron job (2 min)

**Total: ~20 minutes**

### Short-term (This Week)
1. Locate/create webcams page (10 min)
2. Add Cox Bay HTML section (10 min)
3. Update JavaScript for refresh (5 min)
4. Test on all devices (15 min)
5. Monitor for 24 hours (ongoing)

**Total: ~40 minutes + monitoring**

### Long-term (Optional)
- Image optimization
- Surf condition annotations
- Archive browser

---

## Related Features

This feature complements:
- **FEATURE_ENVIRONMENTAL_ASTRONOMICAL.md** - Weather context
- **Buoys page** - La Perouse Bank is near Tofino
- **Winds page** - West Coast wind stations
- **Forecasts page** - West Coast marine forecasts

Consider linking:
- Cox Bay webcam ↔ La Perouse Bank buoy
- Both show West Coast Vancouver Island conditions
- Webcam shows visual, buoy shows measurements

---

## Questions to Resolve

1. **Cropping:** Full frame or crop to focus on surf zone?
   - Decision: Start with full frame, adjust after testing

2. **Fetch frequency:** Every 10 minutes or different?
   - Decision: 10 minutes (matches other webcams)

3. **Webcams page location:** Standalone page or embedded?
   - Decision: TBD - need to check existing site structure

4. **Storage retention:** How long to keep archives?
   - Decision: Use automatic cleanup (80% threshold)

5. **Slideshow feature:** Implement for Cox Bay?
   - Decision: Yes, already supported by script

---

## Next Steps

Ready to implement when you give the go-ahead! 🌊📹

**Recommended approach:**
1. Start with backend (quick, low-risk)
2. Test captures for a few hours
3. Evaluate image quality and cropping
4. Proceed with frontend integration

**Questions for you:**
1. Do you want full frame or should I analyze the stream for optimal cropping?
2. Is there an existing webcams page or should I create one?
3. Any specific positioning/layout preferences for the webcam display?
4. Should I link Cox Bay webcam to La Perouse Bank buoy data (both West Coast)?
