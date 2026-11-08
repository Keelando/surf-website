/**
 * Shared time-formatting helpers (ES module).
 *
 * Site convention: user-facing times are Pacific (fixed UTC-7, see PACIFIC_TZ),
 * "en-US" locale, 24-hour clock unless noted. Every formatter accepts a
 * Date or an ISO 8601 string.
 *
 * These are the canonical replacements for the per-file copies that
 * drifted apart (chart-utils-v4.js, forecasts.js, webcams-v4.js,
 * lightstation-charts.js, tides-modules/utils.js). Each legacy copy is
 * removed when its page migrates to ES modules — see shared/README.md.
 */

/**
 * BC stopped changing its clocks: from the spring-forward of 2026-03-08 it
 * keeps UTC-7 all year. A browser whose time-zone data predates the change
 * still puts America/Vancouver back on UTC-8 after 2026-11-01, an hour off,
 * so the site formats Pacific times in a fixed UTC-7 zone and prints the
 * label up-to-date browsers do ("PDT"; user's pick, 2026-11-06).
 * "Etc/GMT+7" is UTC-7: POSIX zone names invert the sign.
 *
 * Scripts that only ever show recent data (the exports keep 30 days at most)
 * use PACIFIC_TZ directly. The formatters below go through pacificTimeZone(),
 * which keeps America/Vancouver for instants before the switch, so a time
 * from winter 2025–26 still reads as the PST it was.
 */
export const PACIFIC_TZ = "Etc/GMT+7";
export const PACIFIC_LABEL = "PDT";
const BC_FIXED_UTC7_FROM = Date.UTC(2026, 2, 8, 10); // 02:00 PST, 2026-03-08

function toDate(input) {
  return input instanceof Date ? input : new Date(input);
}

/** The zone to format an instant in: fixed UTC-7 from 2026-03-08 on. */
export function pacificTimeZone(input) {
  return toDate(input).getTime() >= BC_FIXED_UTC7_FROM ? PACIFIC_TZ : "America/Vancouver";
}

/** Intl names the fixed zone "GMT-7"; say what local clocks say instead. */
export function pacificLabel(text) {
  return text.replace("GMT-7", PACIFIC_LABEL);
}

function fmt(input, options) {
  const date = toDate(input);
  return pacificLabel(
    date.toLocaleString("en-US", { timeZone: pacificTimeZone(date), ...options }),
  );
}

/** "14:30" — replaces chart-utils-v4 formatTimeOnly(). */
export function formatTimeHM(input) {
  return fmt(input, { hour: "2-digit", minute: "2-digit", hour12: false });
}

/** "Sun" — the Pacific weekday, short. */
export function formatWeekdayShort(input) {
  return fmt(input, { weekday: "short" });
}

/**
 * "2026-11-08" — the Pacific calendar day, as a key to compare two times by.
 * Tables over 24 h mark the row where it changes (a thicker line at midnight).
 */
export function pacificDayKey(input) {
  const date = toDate(input);
  return date.toLocaleDateString("en-CA", { timeZone: pacificTimeZone(date) });
}

/** "7/14" — the date alone, for chart axes that show the day boundary only. */
export function formatDayMonthNumeric(input) {
  return fmt(input, { month: "numeric", day: "numeric" });
}

/** "14:30 7/14" — replaces chart-utils-v4 formatTimestamp(). */
export function formatTimeWithDate(input) {
  return `${formatTimeHM(input)} ${formatDayMonthNumeric(input)}`;
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

/**
 * "Jul 14, 14:30 PDT" — replaces the metadata formatDate closures and the
 * forecast tooltip time in storm_surge_page.js (and, when the index page
 * migrates, the identical formatDate in storm_surge_chart-v4.js).
 */
export function formatMonthDayTimeTZ(input) {
  return fmt(input, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZoneName: "short",
  });
}

/**
 * "Jul 14 12Z" from a model-run timestamp string, or "" when absent —
 * replaces the model-run blocks duplicated across storm_surge_page.js and
 * storm_surge_chart-v4.js. Deliberately UTC (model runs are labelled in Z
 * hours) — the one exception to the Pacific rule above. A bare timestamp
 * without zone info is treated as UTC, matching the backend export.
 */
export function formatModelRunTime(runStr) {
  if (!runStr) return "";
  const runTime = new Date(runStr.endsWith("Z") || runStr.includes("+") ? runStr : `${runStr}Z`);
  const dateStr = runTime.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  return `${dateStr} ${runTime.getUTCHours().toString().padStart(2, "0")}Z`;
}

/**
 * "Jul 14, 05:00 PDT (12Z)" from a model-run timestamp string, or "" when
 * absent.
 *
 * The Pacific-first sibling of formatModelRunTime(). A run labelled "12Z" is
 * unambiguous to anyone who works with models and opaque to everyone else —
 * the reader wants to know when the forecast was made in the time they live
 * in, not which synoptic hour it belongs to. So the local time leads and the
 * Z hour follows in brackets, keeping the label that the model documentation,
 * the archive and the logs all use.
 *
 * Both halves describe the same instant; the bracket is a second name for it,
 * not a second time. The zone abbreviation is kept because dropping it would
 * leave two bare numbers on the line with nothing saying which is which.
 *
 * @param {string} runStr - ISO 8601, tolerating the bare-UTC backend spelling
 * @returns {string}
 */
export function formatModelRunTimeLocal(runStr) {
  if (!runStr) return "";
  const runTime = new Date(runStr.endsWith("Z") || runStr.includes("+") ? runStr : `${runStr}Z`);
  const local = fmt(runTime, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZoneName: "short",
  });
  return `${local} (${runTime.getUTCHours().toString().padStart(2, "0")}Z)`;
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
