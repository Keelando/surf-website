# UI overhaul

Status 2026-10-23: **draft 1 of the home-page mockup is on `dev`**, live at
dev.halibutbank.ca, awaiting the user's verdict on the direction. See "Draft 1"
below for what it does and what it deliberately leaves alone.

## Why

The user, 2026-09-23: the site "appears a bit scrappy… thrown together";
"I have to scroll a lot through some filler to get to the data I want."
The goal is dense data presented cleanly, not overwhelming, especially on
mobile.

The user, 2026-10-04: the data cards are "a little too bulky and 2015
government website-ish"; the page is "a bit dense and old school". **Keep
the Leaflet maps, and keep the map on the home page.**

## Home-page audit (2026-10-04)

Measured with Playwright in Chromium and Firefox at 390×844, 768×1024 and
1280×800, light and dark, against production data. The two engines agree to
within ~5 px everywhere; nothing here is engine-specific.

### The numbers

| | Phone 390 | Tablet 768 | Desktop 1280 |
|---|---|---|---|
| Distance to the first data card | **1,347 px** (1.6 screens) | 1,580 px (1.5) | **1,521 px** (1.9) |
| Cards fully visible on load | 0 | 0 | 0 |
| Card height | 344–388 px | 349–390 px | 349–370 px |
| Of which, the actual readings | **70 px (~20%)** | 70 px | 70 px |
| Page height | 8,908 px | 7,890 px | 7,746 px |

What sits above the first card on a phone: hero 356 px (live conditions for
Halibut Bank only), tagline 93, "How to read this data" 40, map section 603
(legend 143 + map 400). On desktop the intro paragraph is open and the map
is 600 px tall.

Only Strait of Georgia (4 stations) is expanded on load; the other six
stations sit behind three collapsed region bars.

### What makes the cards bulky

Each card's always-visible area holds, top to bottom: name + source badge
(31 px), "Last Update" line (22), the two readings (23 + 23), **two 50 px
toggle buttons** (Show Details, Show History), **two 32 px buttons** (View
Location, View Charts), a "View Source Data" link (33), and dividers, inside
16–20 px padding, a border, a shadow and a 12 px radius. Five controls per
card, repeated identically on every card: 20 buttons for the four Strait of
Georgia stations alone. The readings are a fifth of the card.

### What makes it look "2015"

- **Emoji as icons:** 108 across the ten cards (💨 🌊 📈 📍 📊 🔗 🇨🇦 🇺🇸 🏛️).
  They render differently per OS, clash with the flat UI, and the flag badge
  repeats on every card.
- **Inverted hierarchy in the readings.** Labels are bold and numbers are
  regular weight at the same 15 px, packed into a sentence:
  "**Sig Wave:** W 0.2m @ 2.5s (259°)". Nothing tells the eye where the
  numbers are.
- **Button-heavy cards:** grey bordered buttons plus navy filled buttons on
  every card. This is the strongest "government form" signal.
- **Region headers are full-width saturated gradient bars** (55 px each).
  In dark mode they turn bright cyan, the loudest thing on the page.
- **Chrome around little content:** border + shadow + radius + padding on
  every card; centred section headings with generous margins; a floating
  "Last Updated" box above the regions that repeats the per-card times.
- **Mixed type:** system-ui everywhere except the buttons, which fall back
  to Arial 13.6 px/600.
- **Small defects seen in passing:** at 1280 px the title "Southern Georgia
  Strait" runs into its source badge; the card marked that station "STALE
  (9 hours ago)" on the flat 3 h threshold, which the export's per-station
  `status` would judge on its own cadence (already listed in step 2).

### Below the cards

24-hour trends (2,104 px on desktop), the wave-height summary table
(1,091 px), storm surge (857 px). The **"Time Range" toggle appears three
times**. `index.html` still carries inline styles for the threshold
control, the "Individual buoy" divider and both "Show on Map" buttons.
The charts themselves look clean and current; they are not the problem.

### What already works

- **The hero's conditions strip** (big numbers, small muted labels above
  them) is the most modern element on the page. It's the pattern the
  station summaries should use, not the card layout.
- The Leaflet map, the charts and dark mode are coherent and stay.

### Reference patterns

From general familiarity with Surfline, Windy and Apple Weather, not a
fresh capture: the number is the largest thing on screen; labels are small,
muted and often uppercase; one compact row per location, sometimes with a
tiny sparkline; colour encodes conditions (a wave or wind band) rather than
decoration; actions live behind tapping the row; icons are one consistent
line-SVG set; almost no borders or shadows.

### Brief for the mockup

1. **A compact list of the ten wave stations at the top**, right under the
   nav: the stations that have cards today, not the wind stations,
   lightstations or tide gauges, which stay on their own pages. One row per
   station (~48 px on a phone): name, wave height + period, wind speed +
   gust with a direction arrow, and a freshness dot from the export's
   `status`. No tide (user: it belongs on the tides page). Numbers large,
   labels small and muted. All ten visible, grouped by region under
   lightweight text subheads, not collapsible bars (~600 px with subheads,
   one phone screen; one of today's cards is ~350 px). If ten proves too
   many on screen, the fallback is Strait of Georgia expanded and the other
   regions one tap away, still as compact rows.
2. **Tap a row to expand** details, 12 h history and links inline. No
   always-visible buttons.
3. **The map directly below the list**, legend folded into a small map
   control. Load Leaflet when the map scrolls near, as the charts already
   do, to recover the home-page TBT the plan flagged.
4. **The data summary is the first thing you see** (user, 2026-10-04). The
   hero photo is the site's only image, so it stays, but as the backdrop
   to the summary rather than a block of its own above it: the station list
   sits on the photo, which replaces the single-station strip. Text on a
   photo needs a scrim to keep AA contrast (`site/docs/ACCESSIBILITY_AUDIT.md`).
   A photo gallery comes later, separately (TODO.md).
5. **One shared time-range toggle** for the charts and table below.
6. **SVG icons, no emoji**; one font stack, buttons included.

## Draft 1 (2026-10-23, `dev`)

What it does, against the brief:

1. **Station list on the photo.** `site/assets/js/station-list.js` (pure
   builders, `tests/js/station-list.test.mjs`) renders all ten stations as
   rows on a near-opaque panel over the hero photo: name, age with a status
   dot from the export's `status`, wave height + period, wind speed + gust,
   direction as an arrow (the cardinal is in screen-reader text and the open
   panel). Regions are small text headings; two columns from 900 px. A down
   station shows "No recent data" in the row, not days-old numbers; its last
   readings are in the panel, labelled. Measured: all ten on screen at
   1280×800 and 900 px; nine of ten on a 390×844 phone (three long names
   wrap to two lines).
2. **Tap to expand.** A row is a `<button aria-expanded>`; its panel is built
   on first open: grouped label/value stats, plain-words notes (late, down,
   backup sensor, NOAA "dominant", the registry `caveat`), then text actions
   (12-hour history, Charts, Show on map, source ↗). The history table is the
   existing `buoy-history.js` builder; its scroller is now keyboard-focusable
   (axe `scrollable-region-focusable`). Open rows survive the 5-minute refresh.
3. **Map below the list**, the legend folded into a "Key" toggle in the map's
   corner. **Leaflet loads when the map comes within 150 px of the viewport**
   (`stations-map.js` `ensureMap()`), or at once when a row asks for the map.
   The script URL sits in `data-leaflet-src` on the map element, so
   `update_asset_versions.py` keeps its hash current. `#buoy-<id>` links
   (map popups) now open that row instead of scrolling to the map.
4. **The tagline, the "How to read this data" box and the single-station hero
   strip are gone**; the guide link and the other data pages sit in a link
   row at the foot of the panel.
5. **One time-range toggle** (a 24 h / 48 h segmented control with
   `aria-pressed`) in the trends heading, instead of three.
6. No emoji in the summary; one font stack (controls use `font: inherit`).

Home-only styles live in `site/assets/css/home.css`, so `style-v4.css`, the
nav and the shared modules are untouched until the direction is agreed.

Checked on dev.halibutbank.ca (real CSP), Chromium + Firefox, light + dark:
no console errors; axe clean on the summary, map and toggle at 390 and
1280 with rows and history open. **Axe cannot judge text over a photo** (it
reports "needs review", not a failure), so the panel's contrast was measured
on the rendered pixels instead: shoot the panel with its text made
transparent, then take the worst background pixel inside each text box. At
the user's chosen 72% light / 74% dark panel opacity (blur 24px; 92% read
as too opaque, 60% too see-through and failed in dark mode in Firefox),
every text node passes in both engines, lowest 4.8:1. Firefox headless shows
the photo less blurred than Chromium and is the binding case. Any change to
the panel's opacity, blur, text colours or the photo's tint needs the
measurement redone.

From the first rounds of review (2026-10-23):
- Each region's stations sit in a recessed well (inner shadow + bottom
  highlight); the headings are plain text above them.
- The rows' chevron sits in a tinted disc, and the tagline repeats it ("Tap a
  station ⌄ for…"): opening a row was not obvious.
- "Instrument panel" feel: readings in a fixed-width system monospace,
  status dots glow like LEDs, the panel has a bezel edge. Bigger ideas held
  back: a compass dial for wind direction, a small bar gauge per reading.
- A down/late station's warning opens its panel, ahead of the numbers.
- Line icons (`shared/icons.js`) on the panel's actions, instead of emoji.
- The photo gets a 35% blue overlay layer (`--summary-tint`) over its teal,
  grey-skied water; drop it if the photo is replaced.
- **Warning banners (shared, every page):** each severity class sets a
  `--wb-*` palette that the banner AND the forecasts page's "Warnings in
  effect" rows draw from, so the two match. Light theme: a strong tint of the
  severity, thick coloured bar, deep-shade heading and a filled button. Dark
  theme: a navy strip matching the nav, the severity in a light accent. An
  SVG triangle (`icon("warning")`) replaces the emoji in both places, and
  `getWarningIcon()` is gone. Each zone's own `.warning-card` uses the same
  palette, so one warning looks the same in banner, row and zone card.
- The map key folds away on a click/tap anywhere off it, or Escape. The old white-on-amber banner was ~2:1
  contrast; axe is clean on all five severities in both themes and engines.
No horizontal overflow
from the summary at 390/768/900/1024/1280 (the 768–1024 overflow is the
known nav issue, identical on production). CLS with today's wind-warning
banner: 0.03 / 0.06 / 0.08 / 0.05 at 390 / 768 / 1024 / 1280, against
production's 0.03 / 0.10 / 0.11 / 0.07 under the same conditions; what
remains comes from the banner and nav loading in (step 2).

Left for after the verdict:

- `buoy-card.js` no longer renders anything; only its spread descriptors
  are still imported. Retire it (and its tests) once the rows are agreed.
- The chart dropdown's optgroups in `index.html` repeat `STATION_GROUPS`.
- Below the map is unchanged apart from the toggle: inline styles on the
  threshold control, the "Individual buoy" divider and the "Show on Map"
  buttons remain.
- Map markers for stations with no data are still emoji, so the key keeps
  two emoji entries to match.
- `.claude/skills/verify/SKILL.md` describes the old card flows
  (`.region-header`, `.toggle-details-btn`); update it when this ships.
  The rows' flows: `.sl-row` toggles `aria-expanded` and `#sl-detail-<id>`,
  `[data-action=history|charts|map]` inside the panel, `#buoy-<id>` opens a row.

## Round 3: cohesion and a brand (2026-10-24, `dev`)

From the user's UI refresh spec. Done:

- **Tokens** on `:root` in `style-v4.css`: `--radius` (12px; circles 50%),
  `--space-1…5`, one accent (`--accent`, teal) for what can be clicked,
  `--warn` (amber) for warnings only, `--text-lg/md/sm`, two weights,
  `--font-readout`. Used by the nav and the new home rules; the rest of the
  site moves over page by page, not in one sweep.
- **Brand board** at `/brand.html`: an untracked file in the dev worktree only (so it cannot ship; the page tests require every tracked page to be in the sitemap):
  three candidate marks (swell, buoy, halibut), palette, type. The nav uses
  mark A as a placeholder until one is picked. Open: one name everywhere
  ("Halibut Bank" in the nav, "Salish Sea Wave Conditions" in titles, social
  cards and structured data).
- **Live header wave** (`nav.js`) replaces the blue rule: three layered sine
  bands drawn from Halibut Bank's height (amplitude) and period (drift
  speed), calm default when the buoy is late or down, amber while a warning
  is in effect (`data-marine-warning` on `<html>`, set by
  `warning-banner.js` before the dismissal filter).
- **Per-station scale** (`shared/wave-scale.js`): a height that is "getting
  feisty" for each buoy (Strait of Georgia 1 m and La Perouse 2.5 m are the
  user's calls; the rest from 30-day percentiles). Display only, not in
  `stations.json`, so not on the public API.
- **Nav:** mark + name on the left, links left-aligned, toggle and clock as
  one status cluster, icon-only round theme toggle labelled with what it
  does. The hamburger now runs to 899px: below 900 the one-row bar no longer
  fits, and the 601–1279px wrap bug is gone.
- **Home:** on a phone the title is screen-reader only and the tagline gone,
  so the first station is ~60px higher. Swell lines in each row (static;
  only the open row's moves; none when down; hidden on phones).
  Footer links set apart by rules. The map key also folds on a click inside
  it (on `click`, not `pointerdown`: closing it mid-press crashed Chromium).
- **Station rows on a phone (< 600px), 2026-10-25:** two stacked lines,
  68px: name | dot + age ("36m", no "ago") | plain 20px chevron, then waves
  left, wind right, both spanning the line so wind stays flush right, never
  wrapping. Neah Bay drops "swell" (the 9 s period says it; the panel has
  the detail). Fixed track heights, so a down row's "No recent data" line
  matches the others; it drops " · down" (late rows keep " · late", since
  nothing else says it in words). Ellipsis is the last resort and an open
  row wraps the whole name: at 390px nothing is cut; at 360px only
  "Crescent Channel Ocean", by ~9px (a registry `compact_name` would fix it,
  not built). Waves · Wind ride on the first region heading; on desktop
  they sit inside each column's first row. The open panel indents 24/16px
  and keeps three stat columns at 360px. The photo sits on a fixed
  `::before` layer clipped to the section (`clip-path`), not
  `background-attachment: fixed`; watch scroll smoothness on a cheap phone
  (the panel's backdrop blur repaints over it).
- **Tried and dropped (2026-10-25):** dots, then a dashed line, in place of
  a down row's swell line (blank is cleaner); `?names=short|panel` variants
  for cut-off names (superseded by freeing the width).
- **Wave height decimals:** two only at the Surrey piles (Crescent Beach /
  Channel Ocean), at any height; the user wants the extra precision there.
  EC/NOAA buoys resolve 0.1 m, so "0.40" would be false precision.

Not done, on purpose: the phone warning chip (the banner is already one
compact line and the user likes it), and a stronger panel blur (24px was
tuned with the panel opacity; a no-blur fallback was added instead).

## Where the work happens

On the `dev` branch in `~/envcan_wave-dev`, previewed live at
dev.halibutbank.ca with production's data and CSP. Everything else (backend,
fixes, small frontend changes) keeps going straight to `main`. Workflow,
traps and shipping: `docs/DEV_PREVIEW.md`.

If a new design needs a new data field, add the export on `main` first. It
is harmless there (no page reads it yet), and the preview sees it at once.
Renaming or removing a field the live pages read is not harmless: that
change ships together with the frontend that handles it.

## Order

1. **One home-page mockup, data first**, following the brief in the audit
   above. Agree the direction on this one page before touching any other.
   Open questions for the mockup:
   - ~~What the summary row shows~~: **decided 2026-10-04**, waves (height
     + period), wind (speed, gust, direction) and status. **No tide**: it
     lives on the tides page. Still open: how the row fits a 390 px phone.
   - ~~Whether the hero survives~~: **decided 2026-10-04**, the summary
     comes first and the photo becomes its backdrop (brief, item 4).
   - ~~Whether the map stays on the home page~~: **decided 2026-10-04, it
     stays.** Lazy-load Leaflet instead (TBT ~400 ms is the biggest
     remaining performance lever).
2. **Shared surfaces**, once the direction is agreed: nav, hero, the shared
   CSS tokens in `style-v4.css`, shared modules. Fold in:
   - **Nav overflow at 601–1279 px** (open since 2026-09-04): `.nav-actions`
     and the theme toggle overflow the viewport sitewide at 768/900/1024 px
     and the nav wraps to 89–95 px. A nav rework is the natural place to fix
     it.
   - **Home cards' freshness**: `buoy-card.js` still uses flat 3 h / 12 h
     thresholds; the exports now carry per-station `status`,
     `late_after_minutes` and `down_after_minutes` (2026-09-23). Use them,
     as the maps already do.
3. **Roll out page by page**, one page per commit or small group, not a
   sitewide sweep: forecasts, winds, tides, storm surge, lightstations,
   webcams, guide, api. Land the shared helper together with the one page
   that needs it, and queue the rest.
4. **After the redesign:** re-measure Lighthouse, and only then revisit
   critical-CSS inlining (measured and deferred 2026-09-24; the redesign
   changes the nav and hero it would cover).

## Checks for every step

- **Both engines.** The user browses in Firefox. Probe Chromium and Firefox.
- **A range of widths:** 390, 768, 900, 1024, 1280. Overflow bugs hide
  between the phone breakpoint and a wide desktop.
- **Light and dark themes.**
- **`npm run test:frontend` and the a11y spec**, with `PW_PORT` set when
  testing from the dev worktree (`docs/DEV_PREVIEW.md`, trap 1).
- **CSP on the real preview.** The local test server sends no CSP, so
  Playwright alone cannot catch a CSP break. Load the pages on
  dev.halibutbank.ca.
- **Charts draw on scroll.** Probes and screenshots must scroll first.
- Live Lighthouse runs spaced ~90 s apart.

## Shipping

Once, when the whole overhaul is ready: rebase `dev` onto `main`, run
`scripts/update_asset_versions.py`, `npm test`, then `merge --ff-only` from
the main checkout (`docs/DEV_PREVIEW.md`, "Shipping dev to production"). The
merge is live the moment it lands. Tag it (`deploy-YYYY-MM-DD`) as a
rollback point.

**Next after the overhaul:** CIOPS-SalishSea storm surge (TODO.md).
