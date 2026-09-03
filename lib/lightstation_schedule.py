"""Infer each lightstation's publishing schedule from what it has published.

`config/stations.json` carries `update_frequency_hours: 3` for every
lightstation. That is the nominal FPCN61 cycle, and for most stations it is
roughly right, but it is not what a reader wanting to know when to check back
needs, and for several stations it is simply wrong:

* Two bulletin cycles run, not one. Some stations report at HH:10 on the
  00/03/06/09/12/15/18/21 cycle, others at HH:40 on 02/05/08/11/14/17/20/23,
  and a third group appears in *both* — so their reports land ~30 minutes and
  then ~2.5 hours apart, alternating, not every 3 hours.
* Cape Mudge and Pulteney Point publish four times a day, clustered in Pacific
  daylight hours, not eight times evenly.
* No station has ever published the 08:40 or 09:10 UTC slot in the history we
  hold — every cycle has an overnight gap.

So the schedule is derived from observations rather than declared. It is
self-maintaining, it cannot drift from what the feed actually does, and it
degrades honestly: with too little history it reports low confidence and the
caller can fall back to the registry's nominal figure.

Slot times are UTC; the frontend converts to Pacific for display.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

# Report times cluster on ten-minute boundaries (:10, :30, :40). Rounding to
# the nearest ten collapses the occasional one-off (a 14:22 from Lennard
# Island, a 19:00 from Trial Island) into its slot instead of inventing a new
# one that fires once.
SLOT_MINUTE_ROUNDING = 10

# A slot counts as scheduled once it has fired on this fraction of the days
# covered. Below it, the slot is a straggler rather than part of the cycle.
SLOT_REGULARITY = 0.6

# Never call a slot regular on a single sighting, however short the history.
MIN_SLOT_DAYS = 2

# Under this much history, the shape of a daily cycle is not yet established:
# report the schedule but flag it, so the UI can hedge or fall back.
MIN_CONFIDENT_DAYS = 3.0


def _slot_key(timestamp: int) -> str:
    """Epoch seconds → "HH:MM" UTC, minutes rounded to the nearest ten."""
    moment = datetime.fromtimestamp(timestamp, timezone.utc)
    minute = int(round(moment.minute / SLOT_MINUTE_ROUNDING) * SLOT_MINUTE_ROUNDING)
    hour = moment.hour
    if minute == 60:  # 23:57 → 00:00 the next day
        minute = 0
        hour = (hour + 1) % 24
    return f"{hour:02d}:{minute:02d}"


def _slot_minutes(slot: str) -> int:
    hour, minute = slot.split(":")
    return int(hour) * 60 + int(minute)


def _gaps_between(slots: List[str]) -> List[float]:
    """Hours between consecutive slots, wrapping through midnight."""
    if len(slots) < 2:
        return []
    minutes = sorted(_slot_minutes(s) for s in slots)
    gaps = [(b - a) / 60 for a, b in zip(minutes, minutes[1:])]
    gaps.append((minutes[0] + 24 * 60 - minutes[-1]) / 60)  # last → first
    return gaps


def infer_schedule(timestamps: Iterable[int]) -> Optional[Dict]:
    """Summarize when a station publishes.

    :param timestamps: Observation times, epoch seconds UTC, any order.
    :returns: A dict describing the schedule, or None when there is nothing to
        infer from. Keys:

        ``slots_utc``
            "HH:MM" UTC times the station reliably publishes at, in order.
        ``reports_per_day``
            How many of those there are.
        ``interval_hours``
            The spacing, when the slots are evenly spread; None when they are
            not (a daytime-only station, or any cycle with an overnight gap).
        ``longest_gap_hours``
            The biggest wait between consecutive slots — the number a reader
            actually cares about, since it is when nothing will arrive.
        ``sample_days`` / ``observations``
            What the inference is based on.
        ``confident``
            False when the history is too short to trust the shape.
    """
    times = sorted(t for t in timestamps if t)
    if not times:
        return None

    sample_days = (times[-1] - times[0]) / 86400
    days_by_slot: Dict[str, set] = defaultdict(set)
    for t in times:
        moment = datetime.fromtimestamp(t, timezone.utc)
        days_by_slot[_slot_key(t)].add(moment.date())

    threshold = max(MIN_SLOT_DAYS, SLOT_REGULARITY * sample_days)
    regular = sorted(
        (slot for slot, days in days_by_slot.items() if len(days) >= threshold),
        key=_slot_minutes,
    )

    gaps = _gaps_between(regular)
    # Floats from a /60 division: compare rounded, so 3.0 and 3.0000001 agree.
    even = bool(gaps) and len({round(g, 3) for g in gaps}) == 1

    return {
        "slots_utc": regular,
        "reports_per_day": len(regular),
        "interval_hours": round(gaps[0], 2) if even else None,
        "longest_gap_hours": round(max(gaps), 2) if gaps else None,
        "sample_days": round(sample_days, 1),
        "observations": len(times),
        "confident": sample_days >= MIN_CONFIDENT_DAYS and len(regular) > 0,
    }
