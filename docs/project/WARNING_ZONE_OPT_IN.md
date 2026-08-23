# Per-Zone Warning Opt-In — Plan

**Status:** **Shipped 2026-08-23.** All seven steps landed; outcome notes at
the bottom.
**Decided:** 2026-08-23
**Supersedes:** the two conflicting TODO.md entries ("Let users opt in to
warning zones", "Subscribable warning banners"), which disagreed on the
default. This document is the decision.

---

## Problem

The sitewide warning banner interrupts every page. It fires for a hardcoded
pair — `DEFAULT_BANNER_ZONES` in `site/assets/js/shared/warning-zones.js` —
while the feed carries nine zones and the forecasts page renders all of them.

That pairing is the right *default* and the wrong *ceiling*. A Howe Sound
boater gets no banner for the water they are on, and the only way to change
that today is to edit a constant in a JavaScript file.

The failure mode governing every choice below: **a banner that cries wolf is a
banner people dismiss on reflex, which costs exactly the warning that
mattered.** Widening the default to all nine zones trades one miss for a
habit, and the habit is worse — it costs every future warning, not one.

## Decisions

| Question | Decision |
|---|---|
| First-visit default | Strait of Georgia north + south, **plus storm warnings from any carried zone, always** |
| Picker location | The forecasts page. Only there. |
| Zero zones selected | Allowed, silently. No nag, no forced minimum. |

### The storm floor

Severity is the second axis and the one place the default cannot be allowed
to lose. A storm warning is 48+ kt; a reader who never found the picker
should still be interrupted by one, wherever it is. Gale and strong-wind
warnings stay zone-scoped, which is what keeps the banner quiet enough to be
believed.

Precedent: US Wireless Emergency Alerts let you opt out of most categories
but never national alerts, and NWS's own site never lets you suppress its top
tier in-page. This is a trodden pattern, not an invention.

It is a **floor, not an override**: it applies to the resolved zone set
whether that came from the default or the reader's own choice. A reader who
deliberately turns every zone off still sees storm warnings — the one place
we do not honour "off", and the reason the zero-zone state can be allowed
silently at all.

### Why the forecasts page is enough

The earlier worry was that a reader who never visits the forecasts page never
finds the picker. The resolution is not more surfaces — it is **putting the
control where that reader necessarily ends up.**

- A reader who *sees* a banner and wants to change it already has a path: the
  banner links to the forecasts page (shipped in `c559ab8`), and now lands on
  the right zone.
- A reader who cares about a non-default zone has to visit the forecasts page
  to read that zone's forecast at all. The picker sits next to the zone
  selector, and the zone card itself carries an inline "alert me about this
  zone" toggle — so choosing to *read* Howe Sound puts the control to *be
  alerted about* Howe Sound directly under their eyes.

That inline toggle is the discovery mechanism. It is why no footer link,
no banner gear, and no separate settings page are needed.

---

## Design

### Storage

One key, matching the `selected_marine_zone` pattern in `forecasts.js`:

- Key: `warning_banner_zones`
- Value: JSON array of zone keys, e.g. `["howe_sound","haro_strait"]`
- Absent key = never chosen = use the default. **`[]` is a real choice and
  must not be confused with absent** — the whole reason the value is a JSON
  array rather than a comma-joined string, where `""` and "unset" are
  indistinguishable.

Stored zone keys may vanish from a later `marine_forecast.json` (an sr3
`accept` change, an EC rename). They are filtered against the live document
at read time, never trusted as a list in their own right. A stored key that
no longer appears is dropped from the effective set but **left in storage**,
so a temporarily missing bulletin does not silently erase a preference.

All `localStorage` access wrapped in try/catch (private browsing, disabled
site data), degrading to the default rather than throwing — the pattern
`forecasts.js` already uses for `ZONE_STORAGE_KEY`.

### Module layout

The existing split holds: `shared/warning-zones.js` is pure (data in, data
out, unit-tested without a browser), `warning-banner.js` owns the DOM.
Reading `localStorage` inside `getBannerZones()` would break that.

**New — `site/assets/js/shared/marine-zones.js`** (extracted, see Step 1):
- `listZones(data)` — flatten `{areas:{locations:{}}}` into a zone list
- `shortZoneLabel(zoneName, areaName)` — drop the area prefix
- `orderZonesForDisplay(zones)` — home-area-first ordering, keyed off
  `DEFAULT_ZONE_KEY`

**New — `site/assets/js/shared/warning-preferences.js`** (pure, storage
injected):
- `readBannerZones(storage)` → `string[] | null` (null = never chosen)
- `writeBannerZones(storage, zoneKeys)` → void
- `resolveBannerZones(stored, availableZoneKeys)` → effective set, applying
  the default and the availability filter

**Changed — `shared/warning-zones.js`**:
- `getBannerZones(stored, available)` takes the stored selection as input,
  staying pure
- `collectActiveWarnings(data, bannerZones)` gains the storm floor: a warning
  classified storm passes the zone filter regardless. The floor lives here,
  beside `SEVERITY_ORDER` — **the picker must not be able to switch it off.**

**Changed — `warning-banner.js`**: reads storage, passes the resolved set in.

**New — `site/assets/js/warning-zone-picker.js`**: the DOM half, imported by
`forecasts.js`.

### UI on forecasts.html

**A. The picker**, under the zone `<select>`. A `<details>` styled like
`.verification-help` (border, muted background — shipped this session),
collapsed by default: a settings control on a page whose job is forecasts
should not compete with them.

- Summary line states the current set: *"Alerting on: Strait of Georgia
  north, south — plus storms anywhere"*
- Body: nine checkboxes grouped by area, reusing `shortZoneLabel()` and
  `orderZonesForDisplay()` so the vocabulary and ordering match the
  `<select>` exactly
- A standing line under the list states the storm floor in plain words, so a
  reader understands why an unchecked zone may still interrupt them

**B. The inline toggle**, inside the rendered zone card, one line under the
heading: *"☐ Alert me sitewide about warnings here."* Checked when the zone
is in the effective set. Writes the same key; both controls re-render each
other.

Both are checkboxes, not a multi-select — nine is too many for a
`<select multiple>` on a phone.

---

## Sequence

Each step is independently landable and verifiable.

**Step 1 — Extract the shared zone helpers.** Pure refactor. Move
`listZones`, `shortZoneLabel`, and the home-area pin out of `forecasts.js`
into `shared/marine-zones.js`; `forecasts.js` imports them.
*Done when:* the forecasts page DOM is byte-identical to `dc61db8` (capture
`#forecast-container` innerHTML both sides per the `verify` skill's DOM-diff
recipe), and the dropdown order is unchanged.

**Step 2 — `shared/warning-preferences.js` + tests.** No UI, nothing wired
up. *Done when:* `tests/js/warning-preferences.test.mjs` passes.

**Step 3 — The storm floor.** Add it to `collectActiveWarnings()`, extend
`tests/js/warning-zones.test.mjs`. **Ships a real behaviour change on its
own** — storm warnings begin bannering sitewide — and depends on no UI. This
is the safety improvement; land it before the picker exists.

**Step 4 — Wire storage into the banner.** `warning-banner.js` reads the key
and passes the resolved set to `collectActiveWarnings`. Still no UI, so the
key is always absent and behaviour is unchanged — this step is pure
plumbing, and that is the point: it can be verified to change nothing.

**Step 5 — The picker UI.** `warning-zone-picker.js` + the `<details>` block
on `forecasts.html`.

**Step 6 — The inline per-zone toggle** in the zone card.

**Step 7 — Update the footer privacy copy.** `components/footer.html`
enumerates what the site stores: *"your wave-height alert threshold,
dismissed warnings, and light/dark theme"*. Adding a key without adding it to
that sentence makes the privacy note false. Not optional.

## Testing

Node unit tests with a fake storage object — extend
`tests/js/warning-zones.test.mjs`, add `tests/js/warning-preferences.test.mjs`:

- absent key → default pair
- `[]` → empty set, and **still banners a storm warning**
- corrupt JSON in the key → treated as absent, does not throw
- storage that throws on read/write → treated as absent, does not throw
- stored zone absent from the document → dropped from the effective set,
  **retained in storage**
- stored list round-trips through write/read
- storm warning outside every selected zone → banners
- gale warning outside every selected zone → does not banner
- gale warning inside a selected zone → banners

Browser-level via the `verify` skill (the console-error suite only catches
errors, not behaviour): check a non-default zone, reload, confirm a warning
in that zone raises the banner; confirm the banner's zone deep link still
lands correctly; drive both controls and confirm they re-render each other.
Both engines, both themes, 390px and 1280px.

## Deferred, with reasons

- **All-alerts index page** (the NWS/weather.gc.ca pattern — one page listing
  every active warning across all carried zones). Genuinely useful and the
  standard escape hatch, but it is a new page, not part of this feature.
- **A quiet count of suppressed warnings** on the forecasts page — *"2 active
  warnings in zones you don't follow"*. Cheap (`collectActiveWarnings`
  already walks every area, so the unfiltered set is in hand) and it closes
  the blind spot that zone filtering creates. Worth doing right after Step 6;
  held back only to keep this sequence shippable.
- **Settings-link URL encoding** — the answer to `localStorage` being
  per-device, and what account-less sites use instead of accounts. Collides
  with the hash-namespace question below; revisit together.
- **Footer link, banner gear, dedicated settings page.** Made unnecessary by
  the inline toggle.

## Open questions

- **Hash namespace.** TODO.md flags that zone deep links (`#<zone_key>`,
  shipped in `c559ab8`) share the page's hash space with jump-nav section ids,
  and a future zone slug could collide with one. This feature does not make it
  worse, but if the picker gets a deep link (`#warning-zones`) that is a new
  reserved id, and the overlap should be made explicit then, not later.

---

## Outcome — 2026-08-23

Shipped as designed. Seven commits, one per step. Two deviations, both
deliberate:

**Resolution lives in `warning-zones.js`, not `warning-preferences.js`.** The
design listed both a `resolveBannerZones(stored, available)` in preferences and
a `getBannerZones(stored, available)` in warning-zones — the same function
under two names, in two files. Resolution stayed in `warning-zones.js` beside
`DEFAULT_BANNER_ZONES` and the storm floor (one place to read to know what
banners); `warning-preferences.js` was left owning storage alone, with the
storage object injected rather than reached for. Nothing else changed.

**Single-zone areas get no group heading in the picker.** A `<legend>` reading
"HOWE SOUND" over a single row reading "Howe Sound" is the same redundancy the
zone `<select>` already avoids by emitting single-zone areas as plain options
rather than an `<optgroup>`. The picker now makes the same call, which is what
"the vocabulary and ordering match the `<select>` exactly" actually required.

**One addition:** toggling a zone dispatches a `warning-zones:changed` window
event that `warning-banner.js` listens for. The picker sits on a page that
carries the banner, so without it a reader would tick a zone with a live
warning in it and see nothing happen until the next page load — the control
would look broken. Ticking now raises or clears the banner immediately.

### Verified in the browser

Driven with Playwright against live `site/data/`, both engines, both themes,
390 px and 1280 px, zero console errors, zero axe violations on the picker:

- First visit (no stored key) → default pair, and the two live Juan de Fuca
  strong-wind warnings in the feed correctly raise no banner.
- Inline toggle on the Juan de Fuca card → banner appears immediately without a
  reload, the picker checkbox and the summary line both update, and the
  preference survives a reload.
- Unticking in the picker → the inline toggle follows and the banner clears.
- Zero zones stored → summary reads "Alerting on: storm warnings only,
  anywhere", the strong-wind warnings stay suppressed, and a storm warning in
  an unticked zone still collects.
- Corrupt JSON in the key → falls back to the default without throwing.

### Next

The two deferred items are unchanged and still worth doing in this order: the
quiet count of suppressed warnings on the forecasts page ("2 active warnings in
zones you don't follow"), then the all-alerts index page. The hash-namespace
open question is also unchanged — the picker did not get a deep link, so no
new reserved id was introduced.
