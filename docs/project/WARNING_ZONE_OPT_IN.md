# Per-Zone Warning Opt-In — Plan

**Status:** Planned, not started
**Decided:** 2026-08-23
**Supersedes:** the two conflicting TODO.md entries ("Let users opt in to
warning zones", "Subscribable warning banners"), which disagreed on the
default. This document is the decision.

---

## Problem

The sitewide warning banner interrupts every page. It currently fires for a
hardcoded pair — `DEFAULT_BANNER_ZONES` in
`site/assets/js/shared/warning-zones.js` — while the feed carries nine zones
and the forecasts page renders all of them.

That pairing was the right call on 2026-08-20 and it is still the right
*default*, but it is the wrong *ceiling*. A Howe Sound boater gets no banner
for the water they are actually on, and the only way to change that today is
to edit a constant in a JavaScript file.

The failure mode that governs every choice below: **a banner that cries wolf
is a banner people dismiss on reflex, which costs exactly the warning that
mattered.** Widening the default to all nine zones trades a miss for a habit,
and the habit is worse.

## Decisions (user, 2026-08-23)

| Question | Decision |
|---|---|
| First-visit default | Home waters (the current SoG pair) — **plus Storm warnings from any carried zone, always** |
| Picker location | A section on `forecasts.html`, with sitewide entry points (see below) |
| Zero zones selected | Allowed, silently. No nag, no forced minimum. |

### Why storm-always

Severity is the second axis, and this is the one case where the default
cannot be allowed to lose. A storm warning is 48+ kt; a reader who never
found the picker should still be interrupted by one, wherever it is. Gale and
strong-wind warnings stay zone-scoped, which is what keeps the banner quiet
enough to be believed.

This is a *floor*, not an override: it applies to the resolved zone set
whether that came from the default or from the reader's own choice. A reader
who deliberately turns every zone off still sees storm warnings — the one
place we do not honour "off", and the reason the zero-zone state can be
allowed silently at all.

### Why the forecasts page, and why that is not sufficient

The picker belongs next to the zone `<select>` it mirrors — same vocabulary,
same list, one page owning the concept. But the reader who most needs it is
precisely the one who never visits that page: they are outside the default
zones, so they never see a banner, so a banner-borne link never appears.
Discovery therefore cannot depend on either surface alone. Three entry
points, in order of who they catch:

1. **The banner itself** — a small "zones" affordance. Catches the reader
   annoyed *right now*. Must not disturb the ≤768px layout, where the whole
   banner is already a stretched overlay link (see
   `warning-banner-v4.css`); most likely this control is desktop-only, or
   sits above the stretched link in z-order with its own tap target the way
   `.warning-dismiss-btn` already does.
2. **The footer** — present on all seven pages via `components/footer.html`.
   Catches the reader who has never had a banner fire. This is the entry
   point that actually answers the objection above.
3. **The forecasts page section** — the canonical home, where the control
   lives and where both links point.

---

## Design

### Storage

One key, matching the `selected_marine_zone` pattern already in
`forecasts.js`:

- Key: `warning_banner_zones`
- Value: JSON array of zone keys, e.g. `["howe_sound","haro_strait"]`
- Absent key = never chosen = use the default. **`[]` is a real choice and
  must not be confused with absent** — this is the whole reason the value is
  a JSON array rather than a comma-joined string, where `""` and "unset" are
  indistinguishable.

Zone keys stored here may vanish from a later `marine_forecast.json` (an sr3
`accept` change, an EC rename). Stored keys are therefore filtered against
the live document at read time, never trusted as a list in their own right.
A stored key that no longer appears is dropped from the effective set but
**left in storage**, so a temporarily missing bulletin does not silently
erase a preference the reader made.

### Module layout

The existing split is the right one and should hold: `shared/warning-zones.js`
is pure (data in, data out, unit-tested without a browser),
`warning-banner.js` owns the DOM. Reading `localStorage` inside
`getBannerZones()` would break that, so:

- **New** `site/assets/js/shared/warning-preferences.js` — pure, storage
  injectable:
  - `readBannerZones(storage)` → `string[] | null` (null = never chosen)
  - `writeBannerZones(storage, zoneKeys)` → void
  - `resolveBannerZones(stored, availableZoneKeys)` → the effective set,
    applying the default and the availability filter
  - `listAvailableZones(data)` → the pickable list from
    `marine_forecast.json` (this already exists in spirit as `listZones()`
    in `forecasts.js` — **extract and share it rather than write a second
    copy**; two zone-flattening functions is exactly the two-sources-of-truth
    trap this repo keeps stepping on)
- **Changed** `shared/warning-zones.js`:
  - `getBannerZones()` gains the stored selection as its input, keeping the
    function pure
  - `collectActiveWarnings(data, bannerZones)` gains the storm floor: a
    warning whose severity is storm passes the zone filter regardless. The
    floor lives here, next to `SEVERITY_ORDER`, not in the picker — the
    picker must not be able to switch it off.
- **New** `site/assets/js/warning-zone-picker.js` — the DOM half of the
  picker, imported by `forecasts.js`.

`localStorage` reads/writes are wrapped in try/catch throughout (private
browsing, disabled site data), degrading to the default rather than throwing
— the pattern `forecasts.js` already uses for `ZONE_STORAGE_KEY`.

### Picker UI

A checklist, not a multi-select: nine checkboxes grouped by area, reusing the
same shortened labels and the same Strait-of-Georgia-first ordering the zone
`<select>` now uses (`shortZoneLabel()` and the `DEFAULT_ZONE_KEY`-derived
pin in `forecasts.js` — extract both alongside `listZones()`).

Inside a `<details>` styled like `.verification-help`, collapsed by default:
this is a settings control on a page whose job is forecasts, and it should
not compete with them.

A standing line under the list states the storm floor in plain words, so the
reader understands why they may still be interrupted by a zone they unchecked.

### Copy that must change

`components/footer.html` enumerates what the site stores: *"your wave-height
alert threshold, dismissed warnings, and light/dark theme"*. Adding a key
without adding it to that sentence makes the privacy note false. This is not
a nicety — it is the same tracked-vs-served distinction that governs
`site/data/`.

---

## Testing

Node unit tests in `tests/js/warning-zones.test.mjs` (extend) and a new
`tests/js/warning-preferences.test.mjs`, with a fake storage object:

- absent key → default pair
- `[]` → empty set, and *still* banners a storm warning
- stored zone absent from the document → dropped from the effective set,
  retained in storage
- stored zone list round-trips through write/read
- storm warning outside every selected zone → banners
- gale warning outside every selected zone → does not banner
- gale warning inside a selected zone → banners
- corrupt JSON in the key → treated as absent, does not throw

Browser-level, via the `verify` skill (not the console-error suite, which
only catches errors): check the box for a non-default zone, reload, confirm a
warning in that zone raises the banner and that the banner's zone deep link
still lands correctly (the behaviour shipped in `c559ab8`).

## Sequence

1. Extract `listZones()` / `shortZoneLabel()` / the home-area pin out of
   `forecasts.js` into a shared module. Pure refactor, DOM-diff verifiable —
   land and confirm before anything else moves.
2. `shared/warning-preferences.js` + its tests. No UI yet.
3. Storm floor in `collectActiveWarnings()` + tests. Ships a real behaviour
   change on its own: storm warnings start banners sitewide.
4. The picker UI on `forecasts.html`, reading and writing the key.
5. Footer link + banner affordance; update the footer privacy sentence.

Steps 3 and 4 are each independently shippable, which matters: step 3 is the
safety improvement and does not depend on any UI existing.

## Open questions

- **Hash namespace.** TODO.md flags that zone deep links (`#<zone_key>`,
  shipped in `c559ab8`) share the page's hash space with jump-nav section
  ids, and a future zone slug could collide with one. The picker does not
  make this worse, but if the picker gets its own deep link
  (`#warning-zones`) that is a new reserved id and the overlap should be
  made explicit then, not later.
- **Does the banner affordance survive mobile?** Unresolved above; needs
  measuring against the stretched-overlay layout before it is designed, the
  way the popup widths were measured rather than eyeballed.
