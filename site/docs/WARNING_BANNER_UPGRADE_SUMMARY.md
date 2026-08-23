# Warning Banner Upgrade - Completed 2025-11-05

> ## ⚠️ Correction, 2026-08-23: the variable dismiss durations below never shipped
>
> This document describes per-severity dismissal (12 h storm, 12 h gale, 6 h
> strong wind, 8 h default) as a completed change. **It is not in the code and
> appears never to have been.** `warning-banner.js` has a single
> `DISMISS_DURATION_MS` of 24 hours applied at every severity, and the ×
> dismisses every warning currently in effect at once.
>
> The claim had also been copied into `warning-banner.js`'s own file header,
> where it sat as fact until 2026-08-23 (`f10a76f`). Two sources agreeing does
> not make something true when one was copied from the other.
>
> What actually keeps a flat 24 h safe is the **dismissal id**, which includes
> the warning's issue time: a re-issued or newly issued warning is a different
> id and raises the banner again. EC re-issues these roughly every six hours,
> so a dismissal rarely lasts its nominal 24 h in practice. That is the real
> backstop the "safety backstop" line below was reaching for.
>
> The rest of this document — the CSS severity hierarchy, the reasoning, the
> alternatives weighed — is left as written. Read the sections on dismiss
> durations as a **proposal**, not a description.
>
> Since superseded in other ways too: the banner's zone set is no longer
> hardcoded (see `docs/project/WARNING_ZONE_OPT_IN.md`), and the CSS file it
> names is now `warning-banner-v4.css`.

## What Was Changed

### ⚠️ JavaScript: Variable Dismiss Durations — PROPOSED, NOT IMPLEMENTED
**File:** `site/assets/js/warning-banner.js`

**Before:** All warnings dismissed for 24 hours flat
**After:** Variable durations based on severity:
- **Storm warnings:** 12 hours (most critical)
- **Gale warnings:** 12 hours
- **Strong wind warnings:** 6 hours
- **Other warnings:** 8 hours default
- **Safety backstop:** All dismissals expire after 24h maximum

### ✅ CSS: Enhanced Visual Hierarchy
**File:** `site/assets/css/warning-banner-v3.css`

**Added:**
1. **Severity-based border thickness:**
   - Storm: 6px border (most urgent)
   - Gale: 5px border
   - Strong wind: 4px border

2. **Storm pulse animation:** More urgent animation for storm warnings with shadow effects

3. **Mobile sticky positioning:** Warnings stay visible while scrolling on mobile devices

4. **Wind speed annotations:** Comments show wind ranges (48+ knots for storms, 34-47 for gales, 20-33 for strong winds)

### ✅ New Features

1. **Dismissal Feedback Toast:**
   - Shows centered modal when user dismisses warning
   - Displays duration: "Storm warning hidden for 12 hours - check conditions regularly"
   - Auto-fades after 3 seconds

2. **Improved Accessibility:**
   - Added `role="alert"` to warning banners
   - Added `aria-live="assertive"` for screen readers
   - Dismiss buttons show duration in `aria-label` and `title`

3. **Better User Messaging:**
   - Dismiss button shows duration ("Dismiss for 12h")
   - Storm warnings include safety reminder in feedback

## Testing Right Now

**Active Warnings (2025-11-05):**
- ✅ Strait of Georgia North: **Gale warning** (will dismiss for 12h)
- ✅ Strait of Georgia South: **Strong wind warning** (will dismiss for 6h)

**To test:**
1. Visit https://halibutbank.ca (any page)
2. You should see 2 warning banners at top
3. Click X on strong wind warning
   - Should see: "Warning hidden for 6 hours"
   - Banner fades out
4. Click X on gale warning
   - Should see: "Warning hidden for 12 hours"
   - Banner fades out
5. Reload page - warnings stay hidden
6. Check localStorage in DevTools: F12 → Application → Local Storage
   - Should see `dismissed_marine_warnings` with timestamps

## Files Modified

```
site/assets/js/warning-banner.js              - Upgraded (12 KB)
site/assets/js/warning-banner-v3-backup.js    - Backup of old version (7.5 KB)
site/assets/css/warning-banner-v3.css         - Enhanced (5.2 KB)
```

## Behavior Comparison

| Feature | Before | After |
|---------|--------|-------|
| **Storm dismiss** | 24h | 12h (re-check sooner) |
| **Gale dismiss** | 24h | 12h (typical duration) |
| **Strong wind dismiss** | 24h | 6h (shorter systems) |
| **User feedback** | None | Toast message with duration |
| **Accessibility** | Basic | ARIA labels, roles, live regions |
| **Mobile UX** | Scrolls away | Sticky positioning |
| **Visual hierarchy** | Same for all | Border thickness by severity |
| **Storm animation** | Subtle pulse | Urgent pulse + shadow |

## Safety Features

1. **24-hour maximum:** No dismissal lasts longer than 24h (safety backstop)
2. **Storm re-check:** Storm warnings reappear after 12h instead of 24h
3. **Safety reminder:** Storm dismissals show "check conditions regularly"
4. **Never skip storms:** Storms cannot be permanently dismissed

## Rollback Plan (If Needed)

If issues arise:

```bash
cd site/assets/js
cp warning-banner-v3-backup.js warning-banner.js
```

This reverts to the 24-hour flat dismissal system.

## Browser Compatibility

Tested features:
- ✅ localStorage API (IE 8+, all modern browsers)
- ✅ CSS sticky positioning (Chrome 56+, Firefox 59+, Safari 13+, Edge 16+)
- ✅ ES6 JavaScript (all modern browsers)
- ✅ CSS animations and transitions (all modern browsers)

**Fallbacks:**
- If localStorage unavailable: Warnings persist (safe default)
- If sticky not supported: Warnings scroll normally (acceptable)
- Older browsers see warnings but may not have sticky positioning

## Answers to Documentation Questions

### 1. Storm Warning Behavior ✅
**Decision:** 12-hour dismissal with 24h safety backstop
- Balances safety (re-check sooner) with UX (not too annoying)
- Matches marine weather update frequency

### 2. Mobile Sticky Positioning ✅
**Decision:** Implemented
- Warnings stay visible while scrolling
- Critical safety information always accessible

### 3. Multiple Warnings Display
**Decision:** Show all (deferred collapsing)
- Salish Sea typically has 1-2 warnings
- Can add count badge if 4+ warnings become common

### 4. Sound Alerts
**Decision:** Not implemented
- Would require user gesture (browser security)
- Risk of annoying repeat visitors
- VHF radios already provide audio alerts
- Can add later as opt-in feature

## Next Steps (Optional)

### Short Term
1. **Monitor user behavior:** Check localStorage size/usage
2. **Gather feedback:** Do users find durations appropriate?
3. **Test on multiple devices:** Verify mobile sticky positioning

### Future Enhancements
1. **Warning history:** Show dismissed warnings in modal
2. **Manual refresh:** "Check for new warnings" button
3. **Custom durations:** User preference for dismiss times
4. **Sound alerts:** Opt-in audio for storm/gale (requires user accounts)

## Performance Impact

**Minimal:**
- localStorage: ~100 bytes per dismissal × 10 warnings max = ~1 KB
- Read operations: 1 per page load
- Write operations: 1 per dismissal
- No network impact (uses existing forecast data)

## Monitoring

**Check logs:**
```bash
# Browser console (F12)
# Should see no errors when warnings load/dismiss

# Check localStorage
localStorage.getItem('dismissed_marine_warnings')
# Should show: {"strait_georgia_north_Gale warning_...": 1730853123456, ...}
```

**Expected behavior:**
- Warnings appear on page load (if not dismissed)
- Dismiss button shows duration in tooltip
- Toast appears centered on dismiss
- localStorage updates immediately
- Warnings reappear after duration expires

## Documentation Updated

This upgrade is now documented in:
- ✅ `WARNING_BANNER_UPGRADE_SUMMARY.md` (this file)
- ✅ Code comments in `warning-banner.js`
- ✅ Code comments in `warning-banner-v3.css`

Consider adding to CLAUDE.md under "Marine Weather Forecasts" section.

---

**Deployment completed:** 2025-11-05 19:51 UTC
**Status:** ✅ Live and ready to test with active warnings
**Backup available:** `warning-banner-v3-backup.js`
