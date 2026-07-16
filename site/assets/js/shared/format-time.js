/**
 * Shared time-formatting helpers (ES module).
 *
 * Site convention: user-facing times are Pacific (America/Vancouver),
 * "en-US" locale, 24-hour clock unless noted. Every formatter accepts a
 * Date or an ISO 8601 string.
 *
 * These are the canonical replacements for the per-file copies that
 * drifted apart (chart-utils-v4.js, forecasts.js, webcams-v4.js,
 * lightstation-charts.js, tides-modules/utils.js). Each legacy copy is
 * removed when its page migrates to ES modules — see shared/README.md.
 */

export const PACIFIC_TZ = "America/Vancouver";

function toDate(input) {
  return input instanceof Date ? input : new Date(input);
}

function fmt(input, options) {
  return toDate(input).toLocaleString("en-US", { timeZone: PACIFIC_TZ, ...options });
}

/** "14:30" — replaces chart-utils-v4 formatTimeOnly(). */
export function formatTimeHM(input) {
  return fmt(input, { hour: "2-digit", minute: "2-digit", hour12: false });
}

/** "14:30 7/14" — replaces chart-utils-v4 formatTimestamp(). */
export function formatTimeWithDate(input) {
  const day = fmt(input, { month: "numeric", day: "numeric" });
  return `${formatTimeHM(input)} ${day}`;
}

/**
 * "Jul 14, 14:30" — replaces chart-utils-v4 formatTimeAxis(),
 * tides-modules/utils formatTime(), and webcams-v4 formatShortTimestamp().
 */
export function formatMonthDayTime(input) {
  return fmt(input, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

/** "7/14 14:30" — replaces lightstation-charts formatTimestamp(). */
export function formatNumericDayTime(input) {
  return fmt(input, {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).replace(",", "");
}

/** "Jul 14, 2026, 14:30 PDT" — replaces webcams-v4 formatTimestamp(). */
export function formatFullTimestamp(input) {
  return fmt(input, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZoneName: "short",
  });
}

/**
 * "Mon, Jul 14, 2026, 02:30 PM PDT" — replaces forecasts.js
 * formatTimestamp(). 12-hour clock, matching the forecasts page. Pinned to
 * Pacific time; the legacy copy used the browser's local zone — the only
 * formatter on the site that did.
 */
export function formatForecastTimestamp(input) {
  return fmt(input, {
    weekday: "short",
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

/**
 * "Wednesday Jul 16, 11:05" — replaces the report-time formatting
 * duplicated between lightstation-map.js and lightstation-page.js.
 * (Long weekday, 24-hour clock; first comma stripped, matching the
 * legacy output.)
 */
export function formatWeekdayDayTime(input) {
  return fmt(input, {
    weekday: "long",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).replace(",", "");
}

/**
 * Compact age: "just now" / "5m ago" / "3h ago" / "1 day ago" /
 * "4 days ago" — replaces the age arithmetic duplicated between
 * lightstation-map.js and lightstation-page.js. Longer-form sibling of
 * getAgeString(). `now` is injectable for tests.
 */
export function getShortAgeString(date, now = new Date()) {
  const ageMs = now - toDate(date);
  const ageDays = Math.floor(ageMs / (1000 * 60 * 60 * 24));
  const ageHours = Math.floor(ageMs / (1000 * 60 * 60));
  const ageMinutes = Math.floor((ageMs % (1000 * 60 * 60)) / (1000 * 60));

  if (ageDays >= 1) return ageDays === 1 ? "1 day ago" : `${ageDays} days ago`;
  if (ageHours > 0) return `${ageHours}h ago`;
  if (ageMinutes > 0) return `${ageMinutes}m ago`;
  return "just now";
}

/**
 * "5 minutes ago" / "2 hours ago" / "just now" — moved from
 * tides-modules/utils getAgeString(). `now` is injectable for tests.
 */
export function getAgeString(date, now = new Date()) {
  const diffMs = now - toDate(date);
  const diffMins = Math.floor(diffMs / 60000);

  if (diffMins < 1) return "just now";
  if (diffMins === 1) return "1 minute ago";
  if (diffMins < 60) return `${diffMins} minutes ago`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours === 1) return "1 hour ago";
  if (diffHours < 24) return `${diffHours} hours ago`;

  const diffDays = Math.floor(diffHours / 24);
  if (diffDays === 1) return "1 day ago";
  return `${diffDays} days ago`;
}
