# UI overhaul

Status 2026-10-04: **not started.** The groundwork is done (performance pass,
late/down status, folded explanations, 2026-09-23), and the preview
environment is live. Next is one home-page mockup.

## Why

The user, 2026-09-23: the site "appears a bit scrappy… thrown together";
"I have to scroll a lot through some filler to get to the data I want."
The goal is dense data presented cleanly, not overwhelming, especially on
mobile.

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

1. **One home-page mockup, data first.** A compact all-station summary at
   the top; map and charts below; a much smaller hero; explanations folded
   away. Agree the direction on this one page before touching any other.
   Open questions for the mockup:
   - What the summary row shows per station (wind, waves, period, tide,
     status?), and how it collapses on a phone.
   - Whether the hero survives at all, or becomes a slim header.
   - Whether the map stays on the home page. Leaflet is a large share of
     home-page JS (TBT ~400 ms is the biggest remaining performance lever).
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
