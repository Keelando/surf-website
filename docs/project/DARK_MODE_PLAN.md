# Dark Mode Implementation Plan

## Context

Adding dark mode to halibutbank.ca with system preference detection, a manual toggle, and localStorage persistence. The site already uses CSS custom properties extensively, making this a variable-override approach rather than a restructure.

## Approach: `[data-theme="dark"]` Variable Overrides

Set `data-theme="dark"` on `<html>` to activate dark CSS variable overrides. A small blocking JS script (`theme-manager.js`) runs before render to prevent flash. Toggle cycles: system → light → dark.

---

## Implementation Steps

### 1. Create `site/assets/js/theme-manager.js` (new file) — ✅ Completed

Blocking script for `<head>`. Reads localStorage `theme-preference` (wrapped in try/catch so Safari private mode or tracking protection can’t break rendering), applies `data-theme` attribute on `<html>` immediately. Exposes `window.ThemeManager.cycle()` API. Listens for OS `prefers-color-scheme` changes. Dispatches `themechange` CustomEvent for chart re-renders.

### 2. Add dark variable overrides to CSS files — ✅ Completed

**`style-v4.css`** — Add `[data-theme="dark"] { ... }` block after `:root` with ocean-themed dark palette and `color-scheme: light dark` so native form controls/scrollbars adapt:
- Backgrounds: `#0d1b2a`, `#1b2838`, `#162232`
- Text: `#e0e8f0`, `#8899aa`
- Borders: `#2a3f55`
- Brighter primaries/accents for contrast

Also replace hardcoded `background: white`, `#f0f4f8`, etc. with `var(--color-surface)`, `var(--color-surface-alt)` throughout the file.

Add `transition: background-color 0.3s, color 0.3s` on key elements for smooth toggling.

Add `<meta name="theme-color">` overrides per theme to keep Android/iOS UI chrome in sync, and run quick contrast checks to ensure every text/background pair still meets WCAG 2.1 AA.

**`webcams-v4.css`** — Override `--webcam-*` variables under `[data-theme="dark"]`.

**`nav-tide-styles-v4.css`** — Darker nav gradient, toggle button styles.

**`stations-map-v4.css`** — Dark popup/marker backgrounds (per product decision the Leaflet tiles themselves stay light; style controls/popups so they look intentional against light tiles).

**`warning-banner-v4.css`** — No changes needed (already high-contrast with bold gradients).

### 3. Add toggle button to shared nav — ✅ Completed

**`components/nav.html`** — Add `<button class="theme-toggle">` with Unicode icons (☀/☾/⚙), ensure it remains fully keyboard accessible with visible focus, `aria-label` text, and `aria-pressed` state updates.

**`site/assets/js/nav.js`** — Add click handler calling `ThemeManager.cycle()`, update icon on change, and synchronize `aria-pressed` + `aria-label` on every toggle. HTMX can re-render the nav, so guard against duplicate listeners.

### 4. Add `<script src="/assets/js/theme-manager.js"></script>` to all 9 HTML `<head>` sections — ✅ Completed

Must be blocking (no defer/async), before stylesheets ideally, to prevent FOUC.

### 5. Add chart theme support to `chart-utils-v4.js` — ✅ Completed

- New `getChartThemeColors()` function: returns object with tooltip, grid, axis, series colors based on current `data-theme`.
- Update `getMobileOptimizedTooltipConfig()` to use theme-aware colors.
- New `registerChartThemeListener(reRenderFn)` for chart re-render on toggle.

### 6. Update chart files to use `getChartThemeColors()` — ✅ Completed

Replace hardcoded color literals in these 8 files:
- `wave-chart-v4.js`, `wind-chart-v4.js`, `temperature-chart-v4.js`, `comparison-chart-v4.js`
- `storm_surge_chart-v4.js`, `storm_surge_page.js`, `lightstation-charts.js`
- `tides-modules/chart-renderer.js`

Pattern: `const tc = getChartThemeColors();` at top of render functions, then `tc.gridLine`, `tc.primary`, etc.

Each page's chart orchestrator registers a `themechange` listener to re-call `setOption()`.

### 7. Fix inline style colors in JS files — 🔄 In Progress

Most critical inline colors (chart error UI, tide/surge metadata, lightstation tables, etc.) now reference CSS variables. Remaining TODO: sweep the remaining interactive widgets (`tides-modules/display.js`, map marker helpers) to replace any lingering literal hex values.

- `chart-utils-v4.js` `showChartError()` — use `var()` in inline style strings
- `tides-modules/display.js` — use `var()` in inline styles
- Map marker JS (winds-map.js, etc.) — use `getChartThemeColors()` for dynamically set colors

### 8. Fix inline `<style>` blocks in HTML files — 🔄 In Progress

Primary pages now include the blocking script/meta tags and most inline styles were converted to variables, but we still need to audit for stragglers (e.g., button hover snippets in `index.html`). These should be updated or moved into CSS modules before launch.

- `index.html` `.time-range-btn` — replace hardcoded colors with `var(--color-*)` references
- Check other HTML files for similar inline style blocks

---

## Files Modified (summary)

| File | Change |
|------|--------|
| `site/assets/js/theme-manager.js` | **NEW** — theme detection, toggle API, event dispatch |
| `site/assets/css/style-v4.css` | Dark variable overrides, replace hardcoded colors with vars |
| `site/assets/css/webcams-v4.css` | Dark `--webcam-*` overrides |
| `site/assets/css/nav-tide-styles-v4.css` | Dark nav gradient, toggle button styles |
| `site/assets/css/stations-map-v4.css` | Dark popups/markers (tiles unchanged) |
| `site/components/nav.html` | Toggle button markup |
| `site/assets/js/nav.js` | Toggle click handler, icon logic |
| `site/assets/js/chart-utils-v4.js` | `getChartThemeColors()`, theme listener helper |
| `site/assets/js/wave-chart-v4.js` | Theme-aware colors |
| `site/assets/js/wind-chart-v4.js` | Theme-aware colors |
| `site/assets/js/temperature-chart-v4.js` | Theme-aware colors |
| `site/assets/js/comparison-chart-v4.js` | Theme-aware colors |
| `site/assets/js/storm_surge_chart-v4.js` | Theme-aware colors |
| `site/assets/js/storm_surge_page.js` | Theme-aware colors, re-render listener |
| `site/assets/js/lightstation-charts.js` | Theme-aware colors, re-render listener |
| `site/assets/js/tides-modules/chart-renderer.js` | Theme-aware colors |
| 9 HTML files | Add `theme-manager.js` script tag in `<head>` |

---

## Verification

1. Open each page, toggle through system/light/dark — verify no flash, smooth transition
2. Check all charts re-render with correct colors on toggle
3. Verify Leaflet map popups/markers look correct in dark mode (tiles stay light)
4. Check warning banners remain readable in dark mode
5. Test on mobile (responsive toggle button, touch targets)
6. Keyboard/screen reader test the theme toggle (`aria-pressed`, focus ring, label updates) and run quick WCAG contrast spot-checks on newly themed surfaces.
7. Verify localStorage persists preference across page navigation
8. Test system preference: set OS to dark mode with preference=system, confirm auto-detection
