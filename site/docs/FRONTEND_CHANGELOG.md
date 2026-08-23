# Frontend Changelog

UI/UX enhancements and feature history for halibutbank.ca. Entries are newest-first.

---

## 2026-08-23: Per-Zone Warning Opt-In, and a Storm Floor Under It

The sitewide banner fired for a hardcoded pair of zones while the feed carried
nine. That pairing was the right *default* and the wrong *ceiling*: a Howe
Sound boater got no banner for the water they were on, and the only way to
change it was to edit a constant.

**The reader now picks the zones**, from a collapsed `<details>` under the zone
dropdown on `forecasts.html`, or from an inline "alert me sitewide about
warnings here" checkbox inside the zone card. The inline one is the discovery
mechanism, and the reason no footer link or settings page was needed: someone
who cares about Howe Sound has to visit this page to read Howe Sound's forecast
at all, which puts the control directly under their eyes.

**Storm warnings ignore that choice.** 48+ knots raises a banner from any zone
we carry, even for a reader who deliberately turned every zone off — a floor,
not an override, living in `collectActiveWarnings()` where the picker cannot
reach it. Precedent: US Wireless Emergency Alerts let you opt out of every
category except national alerts. This is what makes the zero-zone state safe to
allow silently, and it is why the default did **not** widen to all nine zones:
a banner that cries wolf gets dismissed on reflex, which costs the warning that
mattered.

**Storage:** `warning_banner_zones`, a JSON array. The array shape is
load-bearing — `[]` is a real choice and an absent key means "never chosen".
Stored zones missing from the live document are dropped from the effective set
but **left in storage**, so one missing bulletin cannot erase a preference.

**When many zones warn at once** the banner names three and counts the rest,
stating a shared type once ("STORM WARNING in effect for A, B, C") instead of
repeating it. What the remainder is called follows the facts: "+6 more zones"
only when the hidden ones share that type, "+6 more warnings" otherwise — a
banner must never promote a gale by implication. Warnings in the reader's own
zones sort first within a severity tie, so the three named are the three worth
naming.

**A jump strip** above both forecast kinds lists every active warning as a
severity-coloured chip, each switching the page to that zone. Deliberately
**unfiltered** — zone filtering buys a quiet banner at the cost of a blind
spot, and this is the page where that blind spot gets closed.

**`selected_marine_zone` moved to sessionStorage.** Clicking through zones in
one sitting sticks; a new visit opens on home waters. The picker now covers the
"keep me posted about that zone" need, so the dropdown no longer has to carry
it by remembering forever.

### Two bugs this surfaced

- **The storm banner scrolled every page sideways.** `stormPulse` animated
  `transform: scale(1.002)` on a 100%-wide element — about three pixels past
  the viewport, at every width. Pre-existing, but only home waters could raise
  a storm banner before; the severity floor made it reachable from any zone.
  The keyframe is shadow-only now.
- **A `display: block` `<button>` with `width: auto` shrink-wraps in Firefox.**
  The picker's full-width collapse band was 10 px wide there and correct in
  Chromium. State the width; do not leave it to `auto`.

Also: white on the banner's gale orange (`#ea580c`) is 3.55:1, under the 4.5:1
AA floor at chip text sizes — the chips use the dark end of each severity ramp
instead.

Full plan, decisions and follow-up: `docs/project/WARNING_ZONE_OPT_IN.md`.

---

## 2026-08-18: Mobile Nav — Scroll Lock Behind the Drawer

The page behind the open hamburger drawer scrolled freely on mobile. It now
holds still.

**The lock is JavaScript, not CSS, and that is the point.** This nav is
`position: sticky`, and both standard CSS recipes break it outright — measured
at a 1400 px scroll offset on a 390 px viewport, not theorised:

| Recipe | What it does to a sticky nav |
|---|---|
| `position: fixed` + negative `top` on `<body>` | Takes the body out of flow; the sticky bar and its drawer strand 1400 px **above** the viewport — invisible and untappable. |
| `overflow: hidden` on `<html>`/`<body>` | Removes the scrolling box the bar is stuck to, so it snaps back to its static position — same 1400 px above the viewport. Also drops the scroll offset to 0, dumping the reader at the top on close. |

Both were tried and reverted. What ships instead: `syncScrollLock()` in `nav.js`
attaches `wheel`, `touchmove` and `keydown` listeners that `preventDefault()`
while any drawer is open (`passive: false` — the default passive listener cannot
cancel). No layout changes at all, so the bar stays exactly where it is stuck.
Gestures starting inside `.nav-scroll` are exempt so the drawer's own list still
scrolls, and it carries `overscroll-behavior: contain` so a scroll chain cannot
reach the page behind it. `SCROLL_KEYS` (space, PageUp/Down, Home/End, arrows)
are cancelled too, but never when the target is a form control.

The lock reads the DOM (`.main-nav.nav-open`) rather than being toggled by each
call site — three paths clear `nav-open` (the button, outside-click, Escape),
and a lock each one had to remember to release is one that eventually strands
the page unscrollable.

Verified in Chromium and Firefox at 390×800: with the drawer open, wheel and
PageDown both leave `scrollY` at 700 and the bar at `top: 0`; after Escape,
scrolling resumes normally.

---

## 2026-08-18: Forecast Page — Wind Views + Segmented Control

`/forecasts.html` now renders the HRDPS wind that
`scripts/fetch/fetch_wave_forecast.py` had been storing since 2026-08-17 but the
page ignored. Waves stay the default view.

**Segmented control** (`.forecast-mode-toggle`) switches the section between
**Waves** and **Wind** — chart *and* table columns, not just the chart. Two
copies, above the chart and below the table, kept in sync by `setForecastMode()`
in `wave-forecast.js`; same duplicate-and-sync pattern as the 24h/48h toggle on
`winds.html`. Plain buttons with `aria-pressed` inside a labelled `role="group"`,
not tab roles — a duplicated `tablist` would have two tabs claiming
`aria-controls` over one panel. Panels use the `hidden` attribute.

**Wind chart:** speed line (orange), gusts as a sparse **scatter** series (HRDPS
masks the gust at most hours — a line would draw segments across hours with no
gust diagnosed; the tooltip says "none forecast"), direction arrows along the
top. Knots on the axis, converted from the stored km/h at parse time. Axis floor
15 kt, mirroring the wave chart's 1 m floor, so a calm day looks calm.

**Table columns are per-mode:** waves → Height / Period / From / Wind wave;
wind → Wind (kt) / Gust (kt) / From. Defined as data (`TABLE_COLUMNS`) so a
header cannot drift from its cell.

**Mobile:** the split fixed a real layout problem. The combined 8-column table
needed ~800 px against 356 px of usable width at 390 px; the wind view now
measures 356 px and fits with no sideways scroll (cell padding and font-size
trimmed under 600 px). The waves view is 499 px and still scrolls inside its
wrapper, by design.

**Control sizing:** the segmented control and the station picker share one box —
40 vw centred on desktop, **75 vw under 600 px**, matched on both edges. 40 vw is
only ~156 px on a 390 px phone.

Each panel names its own model (`Waves — RDWPS`, `Wind at 10 m — HRDPS`) and the
provenance block prints one `Model: … — run …` line per model, because RDWPS is
forced by HRDPS wind and a run divergence between the two should be visible, not
silent.

Verified in Chromium and Firefox, light and dark, at 1600/1280/900/600/390 px:
zero console errors, axe clean in both themes.

**Known, pre-existing, unrelated:** the page scrolls horizontally between roughly
700–950 px — `.nav-actions`/`.theme-toggle` in the shared nav reach 912 px.
Byte-identical before and after this work.

---

## 2026-07-03: Accessibility — WCAG AA Contrast + Named Map Markers

Full axe-core audit (9 pages × light/dark) — all serious/critical violations
fixed. Light-theme accents darkened for 4.5:1 (primary `#006daf`, accent blue
`#2b6cb0`, green `#2e7d32`, orange `#b45309`, red `#c62828`, teal `#00796b`);
dark theme flips `--color-on-primary`/`--webcam-on-primary` to `#0d1b2a` so
buttons/table headers are navy-on-light-blue; Leaflet markers get
`title: "<name> <type>"` accessible names; legend icon spans get `role="img"`;
inline hardcoded colours converted to `var(--color-*)`.

Audit spec: `tests/playwright/a11y-audit.spec.js`. Findings + remaining scope:
`site/docs/ACCESSIBILITY_AUDIT.md`. Cache-bust `?v=20260703`.

---

## 2026-06-21: Mobile Hamburger Nav

Replaced the mobile (`<=600px`) navigation. It had churned through two weak
patterns — a horizontal scroller (hid off-screen links) and a wrapping grid
(consumed ~half the viewport) — neither of which fit 7 links + the theme toggle
in one slim row.

**New design:** a slim sticky bar carrying the brand ("Halibut Bank", left), the
theme toggle (one-tap, no menu needed), and a hamburger button (right). The 7
links live in an overlay drawer that slides down from below the bar. Desktop nav
is unchanged (brand + hamburger are `display:none` above 600px).

**Details:**
- Drawer is collapsed via `max-height`/`visibility`, revealed by toggling
  `.main-nav.nav-open`. Translucent gradient background (`--nav-drawer-bg`,
  ~0.9 alpha) + `backdrop-filter: blur(10px)` so content faintly shows through.
- Hamburger bars morph to an X when open. Mobile nav `z-index: 1000` so the
  drawer overlays the Leaflet map.
- Closes on link tap / outside tap / Esc / second hamburger tap. Open/close
  listeners re-query the nav so they survive HTMX fragment swaps; per-element
  wiring guarded by `dataset.bound`.

**Files modified:**
- `components/nav.html` (markup: brand + hamburger; `.nav-actions` pulled out of
  `.nav-scroll`)
- `assets/css/nav-tide-styles-v4.css` (slim bar + drawer; `--nav-drawer-bg` vars)
- `assets/js/nav.js` (`initHamburger()`)
- Cache-bust to `?v=20260621d`

**Follow-up parked in TODO:** a floating back-to-top button.

---

## 2026-02-24: Bug Fixes & Housekeeping

### Storm Surge Model Run Timezone Fix

Fixed a bug where the 00Z model run was displayed as "08Z" on both the homepage
storm surge widget and the storm surge page. Root cause: `model_run_time` in the
JSON has no timezone designator, so `new Date()` parsed it as local time (UTC-8),
making `getUTCHours()` return 8 instead of 0.

**Fix:** Append `Z` before parsing if the string has no timezone info.

**Files modified:**
- `assets/js/storm_surge_chart-v4.js`
- `assets/js/storm_surge_page.js` (forecast + hindcast sections)

### Sitemap Updated

- Added `lightstations.html` and `webcams.html` (previously missing)
- Refreshed all `<lastmod>` dates

### SEO Meta Tags

Added Open Graph tags to all pages that were missing them (forecasts, storm surge,
tides, webcams, lightstations). Added `author` and `robots` meta to webcams and
lightstations. Expanded keywords on lightstations and webcams.

### Git Housekeeping

Untracked `data/ambleside/` files — they were committed before the `.gitignore`
rule took effect. Rules were already correct; just needed `git rm --cached`.

---

## 2026-02-08: Webcams Page

### New Page: `/webcams.html`

Added a dedicated webcams page showing live coastal camera feeds.

**Features:**
- Live image feeds with auto-refresh
- Multiple camera locations (White Rock Pier, White Rock East Beach, others)
- Slideshow/gallery layout
- Mobile responsive

**Files created:**
- `webcams.html`
- `assets/js/webcams-v4.js`
- `assets/css/webcams-v4.css`
- `components/header-webcams.html`
- `components/tagline-webcams.html`

---

## 2026-01-22: Tides Page Refactor

### Modular Tides Rewrite

Refactored the tides page JavaScript into a module-based architecture.

**Files created:**
- `assets/js/tides-refactored.js` (replaces `tides.js`)
- `assets/js/tides-modules/` — split into data-loader, chart-renderer, display modules

---

## 2025-11-17: v4 JS/CSS Rollout + New Pages

### Version 4 Asset Refactor

Renamed all CSS and JS files from v3 to v4 conventions. Added `logger.js` for
centralized console logging across all pages (no more raw `console.*` calls).

**Key files:**
- `assets/css/style-v4.css`
- `assets/css/nav-tide-styles-v4.css`
- `assets/js/chart-utils-v4.js`
- `assets/js/logger.js`
- `assets/css/warning-banner-v4.css`

### New Page: `/storm_surge.html`

10-day storm surge forecast and hindcast analysis using the Environment Canada
GDSPS model. Pulls data from `data/storm_surge/*.json` (updated every 6 hours).

**Features:**
- Station selector (Campbell River, Point Atkinson, Tofino, etc.)
- 10-day forecast chart
- Hindcast overlay (historical model runs for comparison)
- Model run time display (00Z / 12Z)

**Files created:**
- `storm_surge.html`
- `assets/js/storm_surge_page.js`
- `assets/js/storm_surge_chart-v4.js`
- `components/header-storm-surge.html`
- `components/tagline-storm-surge.html`
- `docs/HINDCAST_METHODOLOGY.md`

### New Page: `/winds.html`

Real-time wind observations from Environment Canada coastal weather stations,
displayed on an interactive Leaflet map with per-station charts.

**Files created:**
- `winds.html`
- `assets/js/wind-stations.js`
- `assets/js/winds-map.js`
- `assets/js/wind-chart-v4.js`
- `components/header-winds.html`

### New Page: `/lightstations.html`

Real-time observations from BC coastal lightstations (wind, sea state, swell),
updated every 3 hours from Environment Canada. Includes an interactive map.

**Files created:**
- `lightstations.html`
- `components/header-lightstations.html`
- `components/tagline-lightstations.html`

### Navigation Expanded to 7 Tabs

`components/nav.html` updated to include all pages:
```
[Buoys] [Tides] [Winds] [Forecasts] [Storm Surge] [Webcams] [Lightstations]
```

Also added a live Pacific time clock to the nav bar.

---

## 2025-11-05: Warning Banner Improvements

### Variable Dismiss Durations

Enhanced warning dismissal system with severity-based durations.

**Changes:**
- **Storm warnings:** 12h dismissal (was 24h)
- **Gale warnings:** 12h dismissal (was 24h)
- **Strong wind warnings:** 6h dismissal (was 24h)
- **Default warnings:** 8h dismissal (was 24h)

**Rationale:** Matches typical marine weather patterns and encourages regular condition checking

### Dismissal Feedback Toast

Added centered modal notification when user dismisses warning:
- "Storm warning hidden for 12 hours - check conditions regularly"
- "Warning hidden for 12 hours" (gale)
- "Warning hidden for 6 hours" (strong wind)
- Auto-fades after 3 seconds

### Enhanced Visual Hierarchy

**CSS improvements:**
- Storm warnings: 6px border (most urgent)
- Gale warnings: 5px border
- Strong wind warnings: 4px border
- Storm pulse animation with shadow effects

### Mobile Sticky Positioning

Warnings now stick to top of screen while scrolling on mobile devices (position: sticky, z-index: 1000)

### Improved Accessibility

- Added `role="alert"` to warning banners
- Added `aria-live="assertive"` for screen readers
- Dismiss buttons show duration in `aria-label` and `title`

**Files modified:**
- `site/assets/js/warning-banner.js` (12 KB - upgraded)
- `site/assets/css/warning-banner-v3.css` (5.2 KB - enhanced)
- `site/WARNING_BANNER_UPGRADE_SUMMARY.md` (docs)

---

## 2025-11-04: Marine Forecasts & Warning Banners

### New Forecasts Page

Created dedicated page for Environment Canada marine weather forecasts.

**URL:** `/forecasts.html`

**Features:**
- Both forecast zones (north and south of Nanaimo)
- Warning cards with severity-based styling
- Current forecast (Today/Tonight/Tomorrow) with wind and weather details
- Extended forecast (Thursday, Friday, Saturday) in responsive grid
- Wave forecast (when available)
- Auto-refresh every 5 minutes
- Smooth scroll to zone sections via URL hash
- Zone highlight effect when navigating from warnings

**Files created:**
- `site/forecasts.html` (5.2 KB)
- `site/assets/js/forecasts.js` (7.0 KB)

### Warning Banner System

Dismissible warning banners at top of all pages (Buoys, Tides, Forecasts) when warnings are active.

**Features:**
- Severity-based color coding (Storm=red, Gale=orange, Strong Wind=amber)
- Click X to dismiss for 24 hours
- Dismissal persists across all pages (localStorage)
- Auto-expires after 24 hours
- Smooth fade-out animation
- "View Forecast →" link scrolls to relevant zone
- Mobile-optimized compact layout (50% smaller on mobile)
- Automatic sorting by severity

**Files created:**
- `site/assets/js/warning-banner.js` (4.3 KB)
- `site/assets/css/warning-banner-v3.css` (3.4 KB)

### State Management (localStorage)

**Key:** `dismissed_marine_warnings`

**Format:**
```json
{
  "strait_georgia_north_Gale warning_2025-11-04T18:30:00+00:00": 1730747282341,
  "strait_georgia_south_Strong wind warning_2025-11-04T18:30:00+00:00": 1730750123456
}
```

**Features:**
- Per-browser, per-device (not synced)
- Client-side only, never sent to server
- Auto-cleanup of expired dismissals

### Navigation Updates

All pages now have 3-tab navigation:
```
[Buoys] [Tides] [Forecasts]
```

**Files modified:**
- `site/index.html` - Added Forecasts link, warning banner container
- `site/tides.html` - Added Forecasts link, warning banner container
- `site/forecasts.html` - Full navigation with active state

### Mobile UX Improvements

**Warning banner optimizations:**
- Desktop: Full-size with clear spacing
- Tablet (≤768px): 33% less padding, smaller fonts
- Small Mobile (≤480px): 47% less padding, ultra-compact layout

**Space savings:**
- Before: ~80-90px height on mobile
- After: ~40-50px height (50% reduction)

### Scroll-to-Zone Functionality

Smart navigation from warning banners to forecasts:
- Warning links include zone anchor: `/forecasts.html#strait_georgia_north`
- Forecasts page auto-scrolls to zone on load
- Blue highlight effect (2 seconds) shows where you landed
- Smooth scroll behavior

### Framework Decision

**Evaluated:** Alpine.js, HTMX, SvelteKit/Astro

**Decision:** Stay vanilla JavaScript + localStorage
- Current size: 3 pages (may grow to 5-7)
- Simple static site deployment
- No build step needed
- Fast and lightweight

**Revisit when:** Site grows to 10+ pages or needs complex user interactions

**Documentation:**
- `docs/BROWSER_STATE_EXPLAINED.md`
- `docs/STATE_QUICK_REFERENCE.md`
- `docs/DISMISSIBLE_WARNINGS_SUMMARY.md`
- `docs/MOBILE_UX_IMPROVEMENTS.md`
- `archive/docs/historical_analysis/FRAMEWORK_DISCUSSION.md` (archived 2025-12-06)

---

## 2025-11-03: Browser Cache Busting

### CSS Versioning System

Implemented filename versioning for CSS files to force browser cache invalidation.

**Current versions (v3):**
- `style-v3.css` - Main site styles
- `nav-tide-styles-v3.css` - Navigation and tide page styles
- `stations-map-v3.css` - Map component styles

**HTML references:**
```html
<link rel="stylesheet" href="/assets/css/style-v3.css" />
<link rel="stylesheet" href="/assets/css/nav-tide-styles-v3.css" />
<link rel="stylesheet" href="/assets/css/stations-map-v3.css" />
```

**Why this matters:**
- Saves hours debugging "phantom" issues from stale CSS
- Prevents user confusion from mixed old/new assets
- More reliable than query parameters or hard refresh
- Browser sees completely new file path = guaranteed cache bust

**When to increment:** After CSS changes don't appear in browser, or after significant UI/UX updates

---

## 2025-11-02: UI/UX Enhancements

### Directional Arrows on Buoy Cards

Added visual directional arrows for all wind and wave directions.

**Implementation:**
- Helper function `getDirectionalArrow(degrees, arrowType)` in `main.js`
- Wind uses `↓` arrow, waves use `➤` arrow
- CSS transforms rotate arrows to match direction (e.g., 270° = west)
- Styling in `style-v2.css` with `.direction-arrow` class

**Convention:** Arrows show direction wind/waves are coming FROM (meteorological standard)

### Navigation Links (Card → Map/Charts)

Each buoy card now has two navigation buttons:
- **📍 View Location** - Scrolls to map and centers on selected buoy
- **📊 View Charts** - Scrolls to charts section and selects that buoy

**Functions:**
- `scrollToMap(buoyId)` - Smooth scroll + map centering + popup
- `scrollToCharts(buoyId)` - Smooth scroll to buoy selector + auto-select
- `centerMapOnBuoy(buoyId)` - Centers map with animation, opens marker popup

**Features:**
- Pulse animation provides visual feedback
- Chart button disabled if no data available (grayed out)
- Global function accessible via `window.centerMapOnBuoy`

**Map integration:**
- Stores buoy markers in `buoyMarkers{}` object for easy lookup
- Centers map with animation when navigating from card

**Files modified:**
- `site/assets/js/main.js` - Navigation functions
- `site/assets/js/stations-map.js` - Map centering function
- `site/assets/css/style-v2.css` - Nav buttons, pulse animation

### Tide Page Improvements

**Reduced excessive padding by 38-50%:**
- Tide selector: 2rem → 1rem
- Card padding: 2rem → 1.25rem
- Data groups: 2rem → 1.25rem margins/padding
- Tide values: 1rem → 0.5rem padding

**Added station metadata display:**
- Color-coded badge: Green (permanent stations), Orange (prediction-only)
- DFO station code (e.g., "07795")
- Precise coordinates (e.g., "49.3375°N, 123.2536°W")
- Descriptive location (e.g., "West Vancouver")

**Features:**
- Loads metadata from `stations.json` for consistency
- Mobile responsive: items stack vertically on small screens
- Styling in `nav-tide-styles.css` with `.station-metadata` classes

**Files modified:**
- `site/assets/js/tides.js` - Metadata display, stations.json integration
- `site/assets/css/nav-tide-styles.css` - Reduced padding, metadata styles
- `site/tides.html` - Added metadata container div

### Wave Breaking Threshold Annotations

Added explanatory note below wave comparison chart.

**Reference lines:**
- **0.7m (orange):** Small wind-driven waves may begin to break on exposed sandy beaches
- **1.2m (red):** Moderate waves begin breaking on exposed sandy beaches

**Files modified:**
- `site/index.html` - Wave threshold explanation box

---

## 2025-11-01: Station Registry System

### Unified Station Metadata

Created master station registry to replace hardcoded station lists across scripts.

**Master file:** `~/envcan_wave/stations.json`

Contains all monitored stations (6 buoys + 8 tide stations) with:
- Coordinates (latitude, longitude)
- Station names and IDs
- Data types (buoy_type, tide_type)
- Display metadata

**Key files:**
- `stations.json` - Master metadata
- `stations.py` - Python module for accessing station data
- `validate_stations.py` - Validation script

**Web integration:**
- `site/data/stations.json` - Web-accessible copy (chmod 644)
- `site/assets/js/stations-map.js` - Leaflet map displaying all stations
- Map appears on index.html between buoy cards and charts section

**Usage in Python:**
```python
from stations import get_all_buoys, get_tide_station

BUOYS = get_all_buoys()
point_atk = get_tide_station("point_atkinson")
```

---

## Earlier Features

### Tides Page (site/tides.html)

Real-time tide monitoring page for DFO stations.

**Features:**
- Station selector dropdown (alphabetical)
- Auto-loads Point Atkinson by default
- Three data displays:
  1. Current observation (latest water level)
  2. Current prediction (astronomical forecast)
  3. Today's high/low tides table
- 28-hour tide chart (ECharts visualization)
  - Predictions as smooth blue line
  - Observations as green scatter points
  - Interactive tooltips with Pacific time
  - Responsive grid layout
- Auto-refresh every 5 minutes
- Responsive design (mobile, tablet, desktop)

**JavaScript file:** `site/assets/js/tides.js`

**Key functions:**
- `loadTideData()` - Fetches all three JSON files
- `populateStationDropdown()` - Builds selector, sets default
- `displayStation()` - Renders components for selected station
- `displayTideChart()` - Initializes ECharts tide chart

**Data sources:**
- `site/data/tide-latest.json` - Current conditions
- `site/data/tide-timeseries.json` - 28-hour rolling window
- `site/data/tide-hi-low.json` - Today's high/low events

### Chart Max-Width Standards

All chart-containing sections use **1200px max-width** for consistency:

**index.html:**
- `#charts-section` - Buoy charts
- `#wave-height-table-section` - Wave summary table
- `#storm-surge-section` - Storm surge forecasts

**tides.html:**
- `.tide-main-content` - Tide page main container

**CSS implementation:**
```css
#charts-section,
#storm-surge-section,
#wave-height-table-section,
.tide-main-content {
  max-width: 1200px;
  margin: 2rem auto;
  padding: 0 1rem;
}
```

All inline styles moved to CSS files for maintainability.

---

## Design Principles

### Responsive Design

- Mobile-first approach
- Breakpoints: 480px (small mobile), 768px (tablet), 1200px (desktop)
- Touch-friendly targets (44px minimum)
- Readable font sizes (16px base, scales down to 14px on mobile)

### Performance

- Minimal JavaScript dependencies
- CDN-hosted libraries (ECharts, Leaflet)
- Lazy loading for images
- Gzip/Zstd compression via Caddy
- No build step required

### Accessibility

- Semantic HTML
- ARIA labels and roles
- Keyboard navigation support
- Screen reader friendly
- Color contrast compliance

### Browser Support

- Chrome 90+ (tested)
- Firefox 88+ (tested)
- Safari 14+ (tested)
- Edge 90+ (tested)
- Mobile browsers (iOS Safari, Chrome Android)

### Color Palette

**Warnings:**
- Storm: Red (#991b1b to #b91c1c gradient)
- Gale: Orange (#c2410c to #ea580c gradient)
- Strong Wind: Amber (#b45309 to #d97706 gradient)

**Data:**
- Tide predictions: Blue (#2196F3)
- Tide observations: Green (#4CAF50)
- Buoy data: Various blues/teals

---

For backend documentation, see `~/envcan_wave/CLAUDE.md` and related docs.
