# Lightstation parse fixes: duplicate observations and dropped sea heights

**Status:** both bugs FIXED — Bug 2 on 2026-09-04, Bug 1 on 2026-09-06
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

### Fix — applied 2026-09-06

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

### What shipped

`insert_observations()` now routes every observation through
`find_existing_row()` / `merge_observation()` rather than straight to an
`INSERT`. Three details are worth keeping in mind:

- **The window is an offset band, not a tolerance.** `PAIR_OFFSET_MIN_SEC` to
  `PAIR_OFFSET_MAX_SEC` is 20–50 min, bracketing the two offsets EC uses. The
  60-minute tolerance this document originally proposed is too wide: Coast
  Guard SPECIAL reports arrive off-cycle, and Addenbroke published one exactly
  60 min after its regular SXCN23 slot on 2026-09-06. That is a real second
  observation — same wind and sea, different visibility — so a plain tolerance
  would have eaten it. `test_special_report_stays_separate` pins the case.
- **Partner lookup matches on family membership** (`LIKE '%SXCN%'`), not on
  the filename prefix, because a merged row names both products and re-parsing
  either file has to find it again. `source_file` becomes `<first>+<second>`.
- **`region` is deliberately not merged.** The two products disagree — SXCN
  can only name the area its whole bulletin covers, so it files the
  central-coast lights under Hecate Strait and Trial Island under the Strait
  of Georgia — but region belongs to the station, not the reading. Taking
  FPCN61's answer moved a station between groups on the page every time a
  merge landed. `config/stations.json` already carries a per-station region;
  wiring the export to read it is the real fix and is its own change (note
  that its `INSIDE PASSAGE` value has no group in the page's `regionOrder`).

Backfill: `scripts/utils/dedupe_lightstation_observations.py` applies the same
merge to rows written before the fix. It collapsed all 464 pairs, 1552 rows →
1088, with no ambiguous matches. No value was lost — 189 FPCN61 sea-height
gaps and 21 wind-speed gaps were filled from the SXCN copy, and all 69
gusting flags survived.

Verified afterwards: every dual-feed station now infers as 7 reports/day on a
single cycle with a 6.0 h overnight gap, the same shape as the single-feed
stations, and a live parser run over the retained bulletins merged 7 second
copies without re-splitting anything.

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

## Bugs 3–8: found by diffing every raw bulletin against what was stored — 2026-09-23

The method, which found all six in one pass: run each raw bulletin on disk
through the parser and compare every station line with the fields it produced.
It is cheap (a few hundred lines) and it catches silent drops that no amount
of reading the regexes did. Worth repeating whenever a new bulletin or phrasing
appears.

| # | Bug | Scale | Fix |
|---|---|---|---|
| 3 | SXCN24 names guessed (`PINE`, `EGG`) — the bulletin writes `PINE ISLAND`, `EGG ISLAND`, and `CAPE MUDGE` was missing | 5,198 "Unmapped" warnings over two weeks; mostly duplicates of FPCN61 readings | `5588462` |
| 4 | SXCN wind under 10 kt without a leading zero (`NW8E`) and `CLM,` fell through | 7 of 119 lines on disk | `\d{1,3}`, `\bCLM\b` |
| 5 | SXCN swell `MDT` and ranges (`LO-MDT SW`) missed; FPCN61 `LOW TO MODERATE` stored as MODERATE | 8 of 119 SXCN lines; most Cape Scott / McInnes / Quatsino FPCN61 lines | ranges kept whole; swell searched after the seas group |
| 6 | DDHHMM from the 31st read on the 1st of a 30-day month → `datetime(…, 4, 31)` raised, bulletin dropped every run | a few bulletins at four month-ends a year | `resolve_ddhhmm()` shared by all three call sites |
| 7 | WMO corrections (`_CCA_`) could only fill gaps, never replace a wrong value | 2 corrections in one day | corrections overwrite values; flags still only accumulate |
| 8 | `NA` / `UNAVAILABLE` stored as all-NULL rows | **168 rows**; Chrome, Entrance, Merry Island looked like they reported | `SXCN_UNAVAILABLE`; rows deleted (backup kept); consumers filter on `lib/lightstation_readings.HAS_READING_SQL` |

Knock-on fixes the same day: calm wind is exported as a 0 kt point
(`calm: true`) rather than dropped (47 readings), the wind chart's line runs
through gusting readings, the pipeline moved from hourly :05 to :16/:47/:58
(reports had sat 25–55 min unpublished), and raw bulletins are kept 7 days
instead of 1 so a fix like these can be re-applied to a week of data.

## Order taken

1. **Bug 2 first** — one regex, largest data recovery, no schema or
   dedupe reasoning involved. Shipped with tests over both spellings.
2. **Bug 1 dedupe** at insert, with tests for both arrival orders. *Done.*
3. Re-ran schedule inference: Merry reads as one report per 3 hours. *Done.*
4. Rewrote the *How the reporting schedule works* copy to match. *Done.*

## Verification

- Parser tests over real bulletin fixtures for every observed sea-state
  spelling, both bulletin families.
- A test asserting no station has two observations within 60 minutes.
- After backfill, compare Merry Island's chart against EC's own
  Lightstation Reports page for the same window.
