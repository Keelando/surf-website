# Lighthouse Reports

`scripts/lighthouse/run.mjs` scores the key pages (home, forecasts, winds,
tides, storm surge, lightstations, webcams) on three devices: Lighthouse's
mobile and desktop presets, and a tablet at 800x1280 (the Lenovo Tab Plus held
upright) with mobile throttling.

| Command | Target | Checks |
|---------|--------|--------|
| `npm run lighthouse` | `site/` via `tests/playwright/serve.py` on port 4175 (`LH_PORT`) | fails on CLS > 0.1 or accessibility < 90 |
| `npm run lighthouse:live` | https://halibutbank.ca | none; for the history |

Narrow a run with `-- --pages index,winds --devices tablet`.

Each run appends one line per page x device to `logs/lighthouse/history.jsonl`
(scores, FCP/LCP/TBT/CLS/Speed Index, commit) and prints each value with its
change since the last run of the same target, page and device. HTML reports
go to `logs/lighthouse/<target>-<time>/`. `logs/` is gitignored, so the
history stays on the server.

Chrome is Playwright's Chromium, the one `npm run test:frontend` uses, or
`CHROME_PATH` if set.

## Reading the numbers

- **Local performance scores are not the site's.** `serve.py` is a
  single-threaded Python server with no compression, and Lighthouse simulates
  slow 4G on top, so phone and tablet LCP read several seconds worse than
  production. Use them to compare one change against the last, on the same
  machine. Track the live run for real numbers.
- **CLS and accessibility don't depend on the server**, which is why those two
  are the local checks.
- **Lighthouse does not measure scrolling.** Its numbers cover loading. For
  scroll smoothness use the on-device probe (`/?probe`, below), on the device:
  headless Chromium paints in software and misjudges drawing cost
  (FRONTEND_CHANGELOG.md, 2026-10-29).

## Known failure

- **Webcams page CLS 0.17-0.33** (2026-10-29, local, no webcam data): the
  cards render after the htmx About section has landed and push it down, and
  `.webcam-image-container` reserves `min-height: 300px` (180px on phones)
  where a 16:9 image grows it. Confirm with `npm run lighthouse:live -- --pages
  webcams` before fixing.

## The scroll probe

`/?probe` on the home page loads `site/assets/js/probe.js` (no one else
downloads it). A yellow readout at the top of `<main>` shows the browser,
viewport, pixel ratio and which layout rules apply, then a row of switches:
each reloads the page with one stylesheet rule added (no blur, opaque panel,
phone-style photo layer, WebP photo, small photo, no photo, no blend mode, no
swell lines, nav not sticky, no map). Switches combine. After each scroll it
prints frames, rough fps, the share of late frames and the worst frame.

The frame numbers come from `requestAnimationFrame`, so they catch frames the
page didn't get to draw, not every compositor hiccup: compare switches on the
same device, and trust feel where the two disagree.
