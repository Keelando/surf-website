# CSP Inline JS Removal — Remaining Work

> **Status:** Completed and archived on 2026-03-08. No remaining inline-handler items.

All inline event handlers (`onclick=`, `onmouseover=`, `onmouseout=`, etc.) must be removed from JS template strings to comply with the `script-src-attr` CSP directive. This document tracks what's done and what remains.

## Background

The site uses `setSafeHTML` (DOMPurify wrapper) for buoy card content, which strips inline handlers silently. For other content set via `innerHTML`, the CSP `script-src-attr: 'none'` directive blocks execution at the browser level. Both result in dead buttons/links.

The fix pattern is always the same: remove inline handlers from template strings, add a class or `data-*` attribute, then use `addEventListener` or event delegation.

---

## Completed

- `site/components/nav.html` — inline `<script>` block moved to `nav.js`
- `site/components/footer.html` — inline `<script>` block moved to `footer.js`
- `site/index.html` — all `onclick=` removed from time-range buttons, threshold buttons, show-on-map links
- `site/winds.html` — `onclick=` removed from time-range buttons
- `site/storm_surge.html` — `onclick=` removed from Show on Map links
- `site/tides.html` — `onclick=` removed from Show on Map links
- `site/lightstations.html` — `onclick=` removed from Show on Map link
- `site/webcams.html` — `onclick=` removed from `.intro-section-header` divs
- `assets/js/main.js` — all card button handlers (toggle-details, toggle-history, spread-info, nav links, hide-history) wired via `addEventListener` after `setSafeHTML`
- `assets/js/wave-table-v4.js` — Show More/Less button rebuilt with `createElement` + `addEventListener`
- `assets/js/wind-stations.js` — Map/Chart table links converted to `data-action` + event delegation
- `assets/js/storm_surge_page.js` — Show on Map listeners added
- `assets/js/tides-refactored.js` — Show on Map delegation added
- `assets/js/lightstation-charts.js` — Show on Map listener added
- `assets/js/webcams-v4.js` — intro section collapse delegation added
- `assets/js/wind-stations.js` — time-range button delegation added
- `assets/js/wind-stations.js` — 24hr table toggle rebuilt with DOM API + CSS hover state
- `assets/js/winds-map.js` — Leaflet popup "View Wind Chart" links wired via delegated listener
- `assets/js/lightstation-map.js` — Leaflet popup "View Data" link switched to delegated listener
- `assets/js/webcams-v4.js` — Refresh notice button uses `.reload-page-btn` delegation instead of inline handler

---

## Remaining

None — inline event handlers tracked for this audit have all been removed. Future findings can be added here.

---

## Notes

- **Leaflet popup links** are the trickiest case — popup HTML is a string passed to `.bindPopup()`. DOMPurify is not involved, but CSP still blocks the `onclick=`. Event delegation on `document` is the correct fix since Leaflet inserts popup DOM dynamically.
- **`onmouseover`/`onmouseout`** for hover colour changes should be replaced with CSS `:hover` rules in `style-v4.css` rather than JS. Use `!important` if the element has an inline `style=` background that would otherwise win specificity.
- After each fix, bump the `?v=` query string on the affected script's `<script>` tag in the relevant HTML file.
