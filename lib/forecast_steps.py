"""Shared helpers for thinning a model's published forecast time steps.

Both WMS fetchers (waves, storm surge) buy one value per request, so the time
axis is what drives the request count. Neither model's skill justifies hourly
resolution all the way to its horizon, so both taper: hourly over a near-term
window, coarser after it.
"""


def taper_time_steps(steps, fine_horizon_hours, coarse_step_hours):
    """Thin hourly steps: hourly to `fine_horizon_hours`, then every
    `coarse_step_hours`. Keeps the last step so the horizon is unchanged.

    Driven off the first step (the run hour) rather than wall-clock, so a late
    fetch still tapers at the same lead times.
    """
    if not steps:
        return steps

    run_start = steps[0]
    kept = []
    for step in steps:
        lead = (step - run_start).total_seconds() / 3600
        if lead <= fine_horizon_hours or lead % coarse_step_hours == 0:
            kept.append(step)
    if kept[-1] != steps[-1]:
        kept.append(steps[-1])
    return kept
