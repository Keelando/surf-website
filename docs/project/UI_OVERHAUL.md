# UI overhaul

Status 2026-10-04: **not started.** The groundwork is done (performance pass,
late/down status, folded explanations, 2026-09-23), the preview environment
is live, and the home page has been audited (below). Next is one home-page
mockup.

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

1. **A compact all-station list at the top**, right under the nav. One row
   per station (~48 px on a phone): name, wave height + period, wind speed +
   gust with a direction arrow, and a freshness dot from the export's
   `status`. Numbers large, labels small and muted. All ten stations
   visible, grouped under lightweight text subheads, not collapsible bars
   (10 × ~48 px fits one phone screen).
2. **Tap a row to expand** details, 12 h history and links inline. No
   always-visible buttons.
3. **The map directly below the list**, legend folded into a small map
   control. Load Leaflet when the map scrolls near, as the charts already
   do, to recover the home-page TBT the plan flagged.
4. **The hero shrinks to a slim header, or goes.** Its single-station strip
   becomes redundant once the list shows every station. (User's call.)
5. **One shared time-range toggle** for the charts and table below.
6. **SVG icons, no emoji**; one font stack, buttons included.

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
   - What the summary row shows per station (wind, waves, period, tide,
     status?), and how it collapses on a phone.
   - Whether the hero survives at all, or becomes a slim header.
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
