# Dismissible Warning Banners - Implementation Summary

**Date:** 2025-11-04
**Status:** ✅ Complete and Production Ready

> **Still accurate as of 2026-08-23**, with two things it predates:
>
> - **Which zones reach the banner is now the reader's choice**, not a
>   hardcoded pair. The examples below showing Strait of Georgia north/south
>   are the *default*, not the ceiling — see
>   `docs/project/WARNING_ZONE_OPT_IN.md`. Storm warnings raise a banner from
>   any zone we carry regardless of that choice.
> - **The × dismisses every warning currently in effect at once**, not one
>   banner at a time; several zones are combined into a single banner. Still
>   24 hours, still per-warning-id, so a re-issue reappears.
>
> Do not read the sibling `WARNING_BANNER_UPGRADE_SUMMARY.md` for dismiss
> durations — it documents a per-severity scheme that was never implemented.

---

## 🎯 What Was Built

**Dismissible warning banners with cross-page state persistence**

Users can now click an **X button** on any warning banner to dismiss it. The dismissal:
- ✅ Persists across all pages (Buoys, Tides, Forecasts)
- ✅ Remembers dismissals for 24 hours
- ✅ Auto-expires after 24 hours (warnings reappear if still active)
- ✅ Works per-warning (dismiss one, keep others)
- ✅ Smooth fade-out animation
- ✅ No dependencies, pure vanilla JavaScript

---

## 🛠️ Technical Implementation

### localStorage State Management

**Storage Key:** `dismissed_marine_warnings`

**Data Structure:**
```json
{
  "strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00": 1730747282341,
  "strait_georgia_south_Strong wind warning_2025-11-04T18:30:00+00:00": 1730747298123
}
```

**Warning ID Format:**
```javascript
`${zone_key}_${warning_type}_${issued_utc}`
```

**Example:**
- Zone: `strait_georgia_north`
- Type: `Gale warning`
- Issued: `2025-11-04T18:30:00+00:00`
- **ID:** `strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00`

This ensures each unique warning gets its own dismiss state.

---

### Expiry Logic

**24-hour auto-expiry:**
```javascript
const DISMISS_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours

// Check if dismissal has expired
const elapsed = Date.now() - dismissedTime;
if (elapsed > DISMISS_DURATION_MS) {
  // Expired - clean up and show warning again
  delete dismissed[warningId];
}
```

**Why 24 hours?**
- Short enough that users see updates if warnings persist
- Long enough that dismissed warnings don't annoy during same-day browsing
- Automatically shows warnings again next day if still active

---

## 🎨 UI/UX Design

### Dismiss Button Styling

**Desktop:**
- Size: 2rem × 2rem
- Font size: 1.5rem (×)
- Position: Right side of banner (flexbox)
- Hover: Scales to 110%, brighter background
- Click: Scales to 95% (tactile feedback)

**Mobile (≤768px):**
- Size: 1.75rem × 1.75rem
- Font size: 1.25rem

**Small Mobile (≤480px):**
- Position: Absolute top-right corner
- Banner gets extra padding-right for dismiss button

**Visual Hierarchy:**
```
[Icon] [Warning Text..................] [View Forecast →] [×]
```

### Fade-Out Animation

```javascript
banner.style.opacity = '0';
banner.style.transition = 'opacity 0.3s ease';

setTimeout(() => {
  banner.remove();
}, 300);
```

**Smooth user experience:**
1. User clicks X
2. Warning fades out over 300ms
3. Warning removed from DOM
4. If no warnings left, container hidden

---

## 📱 Responsive Behavior

### Desktop (>768px)
```
[💨] [GALE WARNING for Strait of Georgia - north] [View Forecast →] [×]
```

### Tablet (≤768px)
```
[💨] [GALE WARNING]          [View Forecast →] [×]
     [for Strait of Georgia - north]
```

### Mobile (≤480px)
```
                                                [×]  ← top-right
[GALE WARNING]
[for Strait of Georgia - north]
[        View Forecast →        ]  ← full width
```

---

## 🔄 Cross-Page Behavior

**Scenario 1: Dismiss on Buoys page**
1. User visits Buoys page → sees 2 warnings
2. User clicks X on Gale Warning
3. Gale Warning fades out
4. **User navigates to Tides page** → Only Strong Wind Warning shows
5. **User navigates to Forecasts page** → Only Strong Wind Warning shows

**Scenario 2: 24-hour expiry**
1. User dismisses Gale Warning at 10:00 AM
2. Warning hidden for 24 hours
3. Next day at 10:01 AM, if warning still active → Warning reappears

**Scenario 3: New warning issued**
1. User dismisses Gale Warning issued at 18:30 UTC
2. New Gale Warning issued at 23:00 UTC (different issued_utc)
3. **New warning has different ID** → Appears even though old one was dismissed

---

## 📂 Files Modified

### JavaScript (warning-banner.js)
**Added functions:**
- `getWarningId(warning)` - Generate unique warning ID
- `isWarningDismissed(warningId)` - Check localStorage
- `dismissWarning(warningId)` - Save to localStorage + remove from DOM

**Modified functions:**
- `displayWarningBanners()` - Filter dismissed warnings before rendering
- `createWarningBanner()` - Add data-warning-id attribute + dismiss button

**New configuration:**
```javascript
const STORAGE_KEY = 'dismissed_marine_warnings';
const DISMISS_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours
```

### CSS (warning-banner-v3.css)
**Added styles:**
```css
.warning-dismiss-btn {
  background: rgba(255, 255, 255, 0.2);
  border: 1px solid rgba(255, 255, 255, 0.3);
  color: #fff;
  font-size: 1.5rem;
  width: 2rem;
  height: 2rem;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.warning-dismiss-btn:hover {
  background: rgba(255, 255, 255, 0.35);
  transform: scale(1.1);
}
```

**Mobile responsive:**
- Smaller button on tablets
- Absolute positioning on small screens

---

## ✅ Validation

**JavaScript Syntax:**
```bash
node --check warning-banner.js ✓ OK
```

**Browser Compatibility:**
- ✅ localStorage API (all modern browsers)
- ✅ Flexbox layout (IE11+)
- ✅ CSS transitions (all modern browsers)
- ✅ Template literals (ES6+)

**Tested Scenarios:**
- [x] Dismiss single warning
- [x] Dismiss multiple warnings
- [x] Navigate between pages (state persists)
- [x] Fade-out animation works
- [x] 24-hour expiry (simulated with DevTools)
- [x] Mobile responsive layout
- [x] Warning reappears after localStorage clear

---

## 🧪 Testing It

### Quick Test (Browser DevTools)

1. **Open site:** http://localhost:8090/
2. **Open DevTools** → Application tab → Local Storage
3. **Click X** on a warning
4. **Check localStorage:** See `dismissed_marine_warnings` entry
5. **Navigate to Tides page** → Warning stays dismissed
6. **Clear localStorage** → Warning reappears on refresh

### Simulate 24-hour Expiry

```javascript
// In browser console:
let dismissed = JSON.parse(localStorage.getItem('dismissed_marine_warnings'));
// Set timestamp to 25 hours ago
dismissed[Object.keys(dismissed)[0]] = Date.now() - (25 * 60 * 60 * 1000);
localStorage.setItem('dismissed_marine_warnings', JSON.stringify(dismissed));
// Refresh page - warning should reappear
```

---

## 📊 Current Active Warnings

**Live data (as of implementation):**

1. **Gale Warning** - Strait of Georgia (north)
   - ID: `strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00`
   - Perfect for testing dismiss functionality

2. **Strong Wind Warning** - Strait of Georgia (south)
   - ID: `strait_georgia_south_Strong wind warning_2025-11-04T18:30:00+00:00`
   - Can dismiss independently

---

## 🚀 Deployment

**Status:** ✅ Ready - No action needed

Files already in place:
- `/site/assets/js/warning-banner.js` (updated)
- `/site/assets/css/warning-banner-v3.css` (updated)
- All 3 HTML pages already include warning-banner.js

**Just refresh your browser** to see the dismiss buttons!

---

## 🎓 Key Design Decisions

### Why localStorage (not sessionStorage)?
- sessionStorage clears when browser closes
- localStorage persists across sessions
- Users want dismissals to persist beyond single visit

### Why 24-hour expiry (not permanent)?
- Warnings change daily
- 24 hours ensures users see updated/new warnings
- Prevents stale dismissals blocking important updates

### Why per-warning IDs (not per-zone)?
- User might want to dismiss Gale but keep Storm
- Same zone can have multiple warnings
- More granular control

### Why fade-out animation?
- Visual feedback (user knows action succeeded)
- Smooth UX (not jarring instant removal)
- Professional feel

---

## 💡 Future Enhancements (Optional)

- [ ] "Dismiss all warnings" button
- [ ] Configurable expiry duration (user preference)
- [ ] "Undo" button (restore dismissed warning)
- [ ] Browser notification API integration
- [ ] Warning history log ("You dismissed 3 warnings today")

---

## 📝 Framework Discussion Documented

See: `archive/docs/historical_analysis/FRAMEWORK_DISCUSSION.md` (archived 2025-12-06)

We evaluated and documented:
- Alpine.js (lightweight reactivity)
- HTMX (component loading)
- SvelteKit/Astro (full framework)

**Decision:** Stay vanilla + localStorage for now. Revisit if site grows to 10+ pages.

---

**Status:** ✅ Complete
**Lines of code added:** ~150 (JS + CSS)
**Dependencies added:** 0
**Complexity:** Low (vanilla JS)
**Browser support:** All modern browsers
**Mobile-friendly:** Yes

**Ready to use!** 🎉
