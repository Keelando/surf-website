"""Surrey tide residual (storm surge), computed here rather than taken from FlowWorks.

Surrey's tidal-residual channels (2414, 3660) are observed − predicted worked
out inside FlowWorks. Since about 2026-11-03 06-12 UTC their *observed*
water-level channels stamp times in UTC-7 (BC's permanent time) while the
*predicted* channels still stamp UTC-8. FlowWorks' own residual therefore
subtracts a tide curve from itself an hour apart and swings ±0.5-1 m with the
tide. Before that both channels stamped UTC-8, consistently, so the residual
looked right (and our stamps for both were an hour early).

So the fetcher measures the shift between the two channels once a day
(`detect_prediction_shift`) and stores predictions at corrected times, and
every consumer takes observed − predicted from our own tables
(`residual_series`). When Surrey fixes the predicted channel's clock, the
measured shift drops to 0 and nothing here needs changing.

Observed and predicted are both in Surrey's CGVD28 datum, so no datum offset
enters — unlike comparing Surrey observations against DFO's chart-datum
predictions, which is where the 2025-12 "mysterious ~0.3-0.7 m offset" came
from.

Surrey's own residual is still fetched and stored (`surrey_geodetic_data`);
`compare_with_surrey` reports how far it sits from ours, so the log shows when
it becomes trustworthy again.
"""

import statistics
from bisect import bisect_left

# Clock mistakes are whole hours; anything else is not a timezone problem and
# should not be "corrected" by this.
CANDIDATE_SHIFTS = tuple(h * 3600 for h in (-2, -1, 0, 1, 2))
# 12 h of 20-minute predictions — a full tidal cycle, so the tide's slope
# (which is what a time shift turns into residual) is sampled both ways.
MIN_OVERLAP = 36
# A non-zero shift must cut the residual's spread to below this fraction of
# the unshifted spread. A genuine 1 h error cuts it ~4x (0.35 -> 0.09 m on
# 2026-11-07); real surge does not mimic that.
IMPROVEMENT = 0.6
# Interpolate a prediction only across a normal 20-minute step (with slack),
# never across a gap.
MAX_INTERP_GAP = 1800


def detect_prediction_shift(observed, predicted):
    """Measure how far the predicted channel's clock is off the observed channel's.

    Args:
        observed: {epoch_s: level} observations (1-minute).
        predicted: {epoch_s: level} predictions as stamped by FlowWorks.

    Returns:
        (shift, spreads): ``shift`` is the whole-hour number of seconds to ADD
        to each raw prediction stamp, or None when too few points overlap to
        tell; ``spreads`` maps each tested shift to std(observed − predicted),
        for the log.
    """
    spreads = {}
    for shift in CANDIDATE_SHIFTS:
        diffs = [observed[t + shift] - level for t, level in predicted.items() if t + shift in observed]
        if len(diffs) >= MIN_OVERLAP:
            spreads[shift] = statistics.pstdev(diffs)
    if 0 not in spreads:
        return None, spreads
    best = min(spreads, key=spreads.get)
    if best != 0 and spreads[best] < IMPROVEMENT * spreads[0]:
        return best, spreads
    return 0, spreads


def residual_series(conn, station_id, start_ts, end_ts=None):
    """Observed − predicted for each observation in [start_ts, end_ts].

    Predictions come every 20 minutes and observations every minute, so the
    prediction is linearly interpolated between its neighbours (error under
    1 cm for a 4 m tide). Observations outside the predicted span, or in a gap
    in it, are dropped.

    Returns:
        [(epoch_s, residual_m)] in time order.
    """
    end_ts = end_ts if end_ts is not None else 2**62
    obs = conn.execute(
        """
        SELECT observation_time, water_level FROM tide_observation
        WHERE station_id = ? AND observation_time BETWEEN ? AND ?
          AND water_level IS NOT NULL
        ORDER BY observation_time
        """,
        (station_id, start_ts, end_ts),
    ).fetchall()
    if not obs:
        return []
    pred = conn.execute(
        """
        SELECT prediction_time, water_level FROM tide_prediction
        WHERE station_id = ? AND prediction_time BETWEEN ? AND ?
          AND water_level IS NOT NULL
        ORDER BY prediction_time
        """,
        (station_id, obs[0][0] - MAX_INTERP_GAP, obs[-1][0] + MAX_INTERP_GAP),
    ).fetchall()
    times = [int(t) for t, _ in pred]
    levels = [float(v) for _, v in pred]

    out = []
    for t, level in obs:
        t = int(t)
        i = bisect_left(times, t)
        if i < len(times) and times[i] == t:
            predicted = levels[i]
        elif 0 < i < len(times) and times[i] - times[i - 1] <= MAX_INTERP_GAP:
            t0, t1 = times[i - 1], times[i]
            predicted = levels[i - 1] + (levels[i] - levels[i - 1]) * (t - t0) / (t1 - t0)
        else:
            continue
        out.append((t, float(level) - predicted))
    return out


def compare_with_surrey(conn, station_id, start_ts):
    """How far Surrey's own tidal_residual sits from ours since start_ts.

    Returns:
        (n, mean_diff_m, max_abs_diff_m) over matching minutes, or None if
        none match. Agreement within a centimetre or so means FlowWorks'
        residual is usable again.
    """
    ours = dict(residual_series(conn, station_id, start_ts))
    theirs = conn.execute(
        """
        SELECT observation_time, tidal_residual FROM surrey_geodetic_data
        WHERE station_id = ? AND observation_time >= ? AND tidal_residual IS NOT NULL
        """,
        (station_id, start_ts),
    ).fetchall()
    diffs = [float(v) - ours[int(t)] for t, v in theirs if int(t) in ours]
    if not diffs:
        return None
    return len(diffs), statistics.fmean(diffs), max(abs(d) for d in diffs)
