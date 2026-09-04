# Lightstation parse fixes: duplicate observations and dropped sea heights

**Status:** Bug 2 FIXED 2026-09-04; Bug 1 (duplicate observations) still QUEUED
(written 2026-09-03)
**Target:** `scripts/parse/parse_lightstation.py`, `lib/lightstation_schedule.py`,
`site/lightstations.html`
**Origin:** the lightstation page shows Merry Island reporting twice, half an
hour apart, then going quiet for two and a half hours. That is not what the
station does.

Two independent bugs in the same parser. Neither is visible as an error: both
produce plausible-looking data, which is why they have survived.

---

## Bug 1: one observation stored twice

### What the data shows

EC publishes BC lightstation observations in two products, and we subscribe to
both (`config/sr3/bc_lightstation_obs.conf`):

- **FPCN61** — verbose prose, every station, ~3 hourly at HH:10 UTC
- **SXCN23/25/26** — compact coded, regional subsets, at HH:40 UTC

For a station that appears in both, the same observation arrives twice. Merry
Island, 2026-09-03:

```
SXCN26 CWVR 032040                    FPCN61 CWVR 032110
VAI                                   ...
MERRY  CLDY 15 SE11E 2FT CHP          STRAIT OF GEORGIA.
                                      2 PM Thursday
SUPPLEMENTARY WEATHER INFORMATION     MERRY ISLAND. ESTIMATED WIND SOUTHEAST
MERRY  2040 CLD EST BKN ABV 25        11 KNOTS. SEAS 2 FOOT CHOP.
```

Southeast 11 knots, 2 foot chop, both times. The SXCN26 supplementary line
stamps the observation `2040`; FPCN61 re-publishes it half an hour later under
the rounded label "2 PM Thursday". One reading, two bulletins.

### Why we store it twice

`parse_report_time()` and `parse_sxcn_time()` both take the timestamp from the
**WMO header** — the bulletin's issue time — not from the observation. The
FPCN61 hour ("2 PM Thursday") is parsed only for its *day*
(`extract_observation_day`); the hour it states is discarded. So the two copies
land 30 minutes apart and the unique index
(`station_name`, `observation_time`) sees them as distinct.

### Scale

Corrected 2026-09-04: the first pass measured only Merry and Trial and
concluded they were the only affected stations. They are not. Nine of the 23
stations are published in both families, on two offsets:

| SXCN family | Stations paired with FPCN61 | FPCN61 minus SXCN |
|---|---|---|
| SXCN23 (north/central coast) | Addenbroke, Boat Bluff, Bonilla, Dryad, Ivory, Langara, McInnes | +40 min |
| SXCN26 (Georgia Strait) | Merry Island, Trial Island | +30 min |

Counts over the last 7 days, per station: 47 of 48 FPCN61 rows have an SXCN
partner inside an hour for each of the SXCN23 stations, 47 of 48 for Merry,
10 of 10 for Trial. Cape Beale, Lennard and Nootka (SXCN25) and Chrome and
Entrance (SXCN26) are SXCN-only; Cape Mudge, Cape Scott, Chatham, Pine,
Pulteney, Quatsino and Scarlett are FPCN61-only, since `SXCN24` is
deliberately not subscribed.

So the tolerance window must cover 40 min, not just 30, and the fix touches
nine stations rather than two.

### Consequences

1. Merry and Trial chart points are drawn twice, 30 min apart — fake
   resolution on exactly the two stations closest to most readers.
2. `lib/lightstation_schedule.py` infers cadence from observation timestamps,
   so it learns both HH:10 and HH:40 slots and reports a station that
   "publishes every 3 hours with two reports 30 minutes apart".
3. The page's *How the reporting schedule works* disclosure describes that
   invented pattern as fact, and gets the mechanism wrong twice over: it says
   "Two FPCN61 bulletin cycles run" (they are two different products), and it
   presents the pair as two reports rather than one published twice.

### Fix

Do not drop a bulletin family — each carries fields the other lacks (see Bug 2;
SXCN has sea state where FPCN61 currently does not, and FPCN61 has swell where
the compact format does not). Deduplicate at insert instead:

- Treat SXCN as authoritative for **time**. Its supplementary line states the
  observation minute outright; FPCN61 states a rounded hour.
- When an FPCN61 report matches an existing SXCN report for the same station
  within a tolerance window (60 min covers both observed offsets, 30 and 40 min), merge
  it into that row rather than inserting a new one: fill fields the SXCN row
  left null, keep the SXCN timestamp.
- Order matters. SXCN (HH:40) arrives *before* FPCN61 (HH:10 of the next hour),
  so the SXCN row will normally exist first. Handle the reverse too — a
  re-parse, or a delayed SXCN — by collapsing whichever pair is found.
- Backfill the existing duplicate pairs, or accept that they age out of the
  30-day window.

Then re-word the disclosure to describe what actually happens: one observation
roughly every 3 hours, on the :40 cycle, published in two bulletin formats.

---

## Bug 2: FPCN61 sea heights under 4 ft are silently dropped

`parse_station_entry()`:

```python
# Pattern: "SEAS 4 FEET MODERATE" or "SEAS 2 FOOT CHOP"
seas_match = re.search(r"SEAS?\s+(\d+)\s+FEET?\s+([A-Z]+)", line)
```

`FEET?` matches `FEE` or `FEET`. It does not match `FOOT`. The comment claims
the singular case is handled; it never has been. EC writes `FOOT` for 1–3 ft
and `FEET` for 4 ft and up, so **every calm-to-moderate sea reading in FPCN61
is discarded**.

In the raw bulletins currently on disk:

| Text | Occurrences | Parsed? |
|---|---|---|
| `SEAS 1 FOOT CHOP` | 26 | no |
| `SEAS 2 FOOT CHOP` | 24 | no |
| `SEAS 3 FOOT MODERATE` | 15 | no |
| `SEAS 6 FEET MODERATE` | 4 | yes |
| `SEAS 5 FEET MODERATE` | 2 | yes |

65 dropped against 6 captured. Database-wide the shape is the same: **390 of
603 FPCN61 rows (65%) have no sea height**, against 30 of 530 (6%) for SXCN.

This is worse than the duplication. Wave height is the headline number on this
page, and for the fifteen-odd FPCN61-only stations it is present only when the
sea is running 4 ft or more — so the chart is blank in calm weather and the
site looks like it has no data rather than like the sea is flat.

### Fix — applied 2026-09-04

```python
seas_match = re.search(r"SEAS?\s+(\d+)\s+(?:FOOT|FEET|FT)\s+([A-Z]+)", line)
```

Covered by `test_seas_in_feet_singular` in `tests/test_lightstation_parse.py`.
Existing rows could only be repaired where the raw bulletin survives: raw
retention is 1 day, so a one-off re-parse of the FPCN61 files on disk filled
39 rows. The remaining 351 null-sea-height FPCN61 rows predate that and age
out of the 30-day window on their own.

Add table-driven cases for both spellings, plus `SEAS RIPPLED`, so the pairing
is asserted rather than described in a comment. Then re-parse the retained raw
bulletins to backfill what is recoverable
(`LIGHTSTATION_RAW_RETENTION_DAYS` bounds how far back that reaches).

---

## Suggested order

1. **Bug 2 first** — one regex, largest data recovery, no schema or
   dedupe reasoning involved. Ship it with tests over both spellings.
2. **Bug 1 dedupe** at insert, with tests for both arrival orders.
3. Re-run schedule inference and confirm Merry reads as one report per
   3 hours.
4. Rewrite the *How the reporting schedule works* copy to match.

## Verification

- Parser tests over real bulletin fixtures for every observed sea-state
  spelling, both bulletin families.
- A test asserting no station has two observations within 60 minutes.
- After backfill, compare Merry Island's chart against EC's own
  Lightstation Reports page for the same window.
