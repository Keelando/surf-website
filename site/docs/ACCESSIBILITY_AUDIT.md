# Accessibility Audit — 2026-07-03

Automated axe-core scan (9 pages × light/dark = 18 scans, WCAG 2.1 A/AA +
best-practice rules) plus a manual review of markup, keyboard support, and
focus handling.

**Re-run the scan anytime:**

```bash
npx playwright test tests/playwright/a11y-audit.spec.js --project=chromium
# JSON results land in a11y-results/ (or set A11Y_OUT)
```

Requires `@axe-core/playwright` (devDependency).

---

## Fixed 2026-07-03 (high priority)

All **serious/critical** axe violations cleared — 169 colour-contrast nodes and
64 unnamed-marker nodes down to 0.

1. **Map markers had no accessible name** (WCAG 4.1.2). Leaflet gives every
   marker `role="button" tabindex="0"`; screen readers heard "button" ×41 on
   the home map. Fix: `title: "<name> <type>"` passed to `L.marker()` in
   `stations-map.js`, `winds-map.js`, `lightstation-map.js`.
2. **Dark mode: white text on light-blue primary fills** (2.2–2.9:1). Fix:
   `--color-on-primary` (and new `--webcam-on-primary`) flip to `#0d1b2a` in
   the dark theme; rules that hardcoded `color: white` on primary fills now use
   the variable (`.data-table th`, `.region-header`, `.button-primary`, webcam
   headers/buttons). Winds `.data-table th` gets a solid dark-mode background
   (the light-to-dark gradient couldn't pass at both ends).
3. **Light mode: too-faint text.** Light-theme palette darkened for ≥4.5:1 on
   the site's light surfaces:
   - `--color-primary` `#0077be` → `#006daf`
   - accents: blue `#4299e1`→`#2b6cb0`, green `#43a047`→`#2e7d32`, orange
     `#ff9800`→`#b45309`, red `#e53935`→`#c62828`, teal `#00897b`→`#00796b`
     (+ matching `--color-warning-text`/`--color-error-text`)
   - webcam vars: `--webcam-accent` `#2b6cb0`, `--webcam-text-muted` `#5a6b7f`,
     `--webcam-text-light` `#5f6d7e`
   - `.status-down-list` opacity 0.7 → 0.85; `.github-link` full opacity with
     underline hover (was 0.8 opacity, blending below AA)
   - inline hardcoded `#0077be`/`#718096`/`#4a5568`/`#666` in HTML replaced
     with `var(--color-*)` equivalents (also makes them dark-mode aware)
   - webcam spread labels return `var(--color-accent-*)` instead of resolved
     values, so they survive live theme toggles
4. **`aria-label` on plain `<span>`s** (prohibited-attr): legend icons in
   `index.html`/`lightstations.html` got `role="img"`.

Dark-theme accents/muted text already passed and were left alone.

**Also fixed 2026-07-03 (follow-up):** every public page's content is now
wrapped in a `<main>` landmark (taglines and About included; header/nav/footer
stay top-level landmarks). Pages that had a *styled* `<main class="...">` kept
the styling on a demoted `<div>` so layout is unchanged. This cleared the
"region" rule (980 flagged nodes → 0 — axe counts every top-level element
outside a landmark, so the winds station table alone contributed ~350×2
themes) and "landmark-one-main" for all pages except analytics.html.

---

## Remaining scope (medium priority)

Axe still reports these as **moderate** (all best-practice tier, not WCAG
failures), plus manual findings axe can't see:

1. **Duplicate identical `<nav>` landmarks** — `components/nav.html` is
   injected at top and bottom of every page with no distinguishing label.
   Cheapest fix in `nav.js`: `aria-label="Footer navigation"` on the second
   instance.
2. **No skip link** — keyboard users tab through brand + 7 links + toggles on
   every page before content. Add a visually-hidden "Skip to main content"
   link (link it to each page's `<main>`).
3. **ECharts charts have no text alternative** — bare canvas. ECharts ships
   `aria: { enabled: true }` which auto-generates a description from series
   data; enable it in the shared chart config.
4. **Current page not announced** — `nav.js` adds `.active` class only; add
   `aria-current="page"` alongside.

## Remaining scope (low priority)

5. **Heading-order skips** — `<h4>` directly under `<h2>`: forecasts
   ("Sunday"), guide ("Data Sources").
6. **`#tide-station-select:focus`** removes the outline, replacement
   box-shadow is 10% alpha (`nav-tide-styles-v4.css` ~line 690) — effectively
   invisible focus. Only `outline: none` in the codebase.
7. **analytics.html** — no `<h1>`/`<main>` (internal, noindex; optional).
8. **No `prefers-reduced-motion` handling** — only the nav drawer slide
   really moves; low stakes.

## Explicitly out of scope / accepted

- Leaflet map tiles stay light in dark mode (existing accepted trade-off).
- Chart series colours on canvas (axe doesn't evaluate; ECharts `aria` option
  in item 3 is the mitigation).
- Decorative SVG fills in legend icons (not text).
