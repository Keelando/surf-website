/**
 * Present a lightstation's publishing schedule (ES module).
 *
 * The backend infers the schedule from each station's own history and ships
 * it on every entry in `latest_lightstation.json` — see
 * `lib/lightstation_schedule.py` for the shape and for why it is not the
 * registry's flat `update_frequency_hours: 3`.
 *
 * These helpers only turn that into words, and into the one thing a reader
 * checking a lightstation actually wants: when the next report is due.
 */

import { PACIFIC_TZ } from "./format-time.js";

/** Slot strings are UTC "HH:MM"; readers are on the coast. */
function slotToPacific(slotUtc, reference = new Date()) {
  const [hour, minute] = slotUtc.split(":").map(Number);
  const slot = new Date(
    Date.UTC(
      reference.getUTCFullYear(),
      reference.getUTCMonth(),
      reference.getUTCDate(),
      hour,
      minute,
    ),
  );
  return slot;
}

/**
 * The next scheduled report at or after `from`.
 *
 * Slots repeat daily, so the search is today's slots then tomorrow's; a
 * station with no established slots has no answer.
 *
 * @param {Object|null} schedule - The exported schedule object
 * @param {Date} [from] - Defaults to now
 * @returns {Date|null}
 */
export function nextReportTime(schedule, from = new Date()) {
  if (!schedule?.slots_utc?.length) return null;

  for (const dayOffset of [0, 1]) {
    const reference = new Date(from.getTime() + dayOffset * 86400000);
    for (const slot of schedule.slots_utc) {
      const candidate = slotToPacific(slot, reference);
      if (candidate > from) return candidate;
    }
  }
  return null;
}

/** "2026-09-06" in Pacific time — the key for comparing calendar days. */
function pacificDayKey(date) {
  return date.toLocaleDateString("en-CA", { timeZone: PACIFIC_TZ });
}

/** "Today" / "Tomorrow" / "Tuesday", relative to `from`, in Pacific time. */
function dayLabel(date, from) {
  const day = pacificDayKey(date);
  if (day === pacificDayKey(from)) return "Today";
  if (day === pacificDayKey(new Date(from.getTime() + 86400000))) return "Tomorrow";
  return date.toLocaleDateString("en-US", { timeZone: PACIFIC_TZ, weekday: "long" });
}

/** "14:40" in Pacific time, for a Date. */
function clock(date) {
  return date.toLocaleString("en-US", {
    timeZone: PACIFIC_TZ,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

/**
 * One-line summary of how often a station reports.
 *
 * Deliberately leads with the count rather than an interval: only some
 * stations are evenly spaced, and calling a 4-reports-in-9-hours-then-nothing
 * station "every 3 hours" is the misdescription this whole feature exists to
 * remove.
 *
 * @param {Object|null} schedule
 * @param {number|null} [nominalHours] - The registry's declared frequency,
 *   used only when there is not enough history to say anything better.
 * @returns {string|null} null when nothing can be said at all
 */
export function describeSchedule(schedule, nominalHours = null) {
  if (!schedule?.confident || !schedule.reports_per_day) {
    return nominalHours ? `Nominally every ${nominalHours} h (irregular in practice)` : null;
  }

  const perDay = schedule.reports_per_day;
  const count = `${perDay}× daily`;

  if (schedule.interval_hours) {
    return `${count}, every ${schedule.interval_hours} h`;
  }

  // Not evenly spaced. The longest gap is the useful half of that fact — it
  // is the stretch during which no report will arrive.
  const gap = schedule.longest_gap_hours;
  return gap ? `${count}, up to ${gap} h apart` : count;
}

/**
 * The slot list rendered in Pacific time, e.g. "08:10, 11:10, 14:10, 17:10".
 *
 * @param {Object|null} schedule
 * @returns {string|null}
 */
export function describeSlots(schedule) {
  if (!schedule?.slots_utc?.length) return null;

  const reference = new Date();
  const times = schedule.slots_utc
    .map((slot) => slotToPacific(slot, reference))
    .map(clock)
    .sort();
  return times.join(", ");
}

/**
 * "Today ~14:40" / "Tomorrow ~04:40" — the actionable half, for a card or
 * popup line.
 *
 * Only meaningful for a station that is still reporting. A station five days
 * silent has a next *scheduled* slot like any other, and saying "next ~16:40"
 * beside "5 days ago" is the card contradicting itself: two of the three lines
 * describe a healthy station and one does not. Callers must pass `isStale` so
 * this can decline to make a promise the station is not keeping.
 *
 * @param {Object|null} schedule
 * @param {boolean} [isStale] - The exported staleness flag for this station
 * @returns {string|null} null when there is nothing honest to say
 */
export function describeNextReport(schedule, isStale = false, from = new Date()) {
  if (isStale) return null;
  const next = nextReportTime(schedule, from);
  // The day matters: a bare "~16:40" late in the evening is read as tonight
  // when the next slot is actually tomorrow morning, and the whole point of
  // this line is telling a reader when to come back.
  return next ? `${dayLabel(next, from)} ~${clock(next)}` : null;
}

/**
 * "35 scheduled reports missed" — what to say instead of a next-report time
 * once a station has stopped.
 *
 * The count is what separates a station that is merely late from one that is
 * off air, which the age alone does not: "5 days ago" reads the same whether
 * the station reports hourly or twice a week.
 *
 * @param {Object|null} schedule
 * @param {string|number|Date|null} observationTime - Last report
 * @param {Date} [now]
 * @returns {string|null} null when the cadence is unknown or nothing is missed
 */
export function describeMissedReports(schedule, observationTime, now = new Date()) {
  const perDay = schedule?.reports_per_day;
  if (!schedule?.confident || !perDay || !observationTime) return null;

  const last = new Date(observationTime);
  if (Number.isNaN(last.getTime())) return null;

  const days = (now.getTime() - last.getTime()) / 86400000;
  const missed = Math.floor(days * perDay);
  if (missed < 1) return null;
  return `${missed} scheduled report${missed === 1 ? "" : "s"} missed`;
}
