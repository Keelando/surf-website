/**
 * Shared staleness-presentation helpers (ES module).
 *
 * Staleness itself is computed by the backend (each JSON export carries a
 * `stale` flag, and buoy/wind/lightstation exports a per-station `status`
 * with its thresholds); these helpers re-apply it to the live clock and
 * standardize how late and down stations LOOK.
 * Canonical replacements for blocks inlined in stations-map.js (buoy +
 * lightstation popups), lightstation-map.js, and winds-map.js — see
 * shared/README.md for migration status.
 */

import { formatWeekdayDayTime } from "./format-time.js";

/**
 * Map-marker opacity for a station that is down (or, from a caller that only
 * has the legacy `stale` flag, stale).
 */
export const STALE_MARKER_OPACITY = 0.35;

/** Map-marker opacity for a station that is late: dimmed, but less than down. */
export const LATE_MARKER_OPACITY = 0.6;

const STATUSES = new Set(["ok", "late", "down"]);

/** Severity order, so a judgement can only ever move toward "down". */
const SEVERITY = { ok: 0, late: 1, down: 2 };

/**
 * Minutes since the station's newest reading, measured against `now`.
 *
 * The export's `age_minutes` is a snapshot from when the file was written:
 * a page left open, or a pipeline that stalls, would keep showing "22m ago"
 * forever. `observation_time` does not go stale, so it wins when present.
 *
 * @param {Object} data - One station's latest-observation entry
 * @param {number} [now] - Epoch ms; injectable for tests
 * @returns {number|null}
 */
export function liveAgeMinutes(data, now = Date.now()) {
  const observed = data?.observation_time ? Date.parse(data.observation_time) : Number.NaN;
  if (Number.isFinite(observed)) return Math.max(0, (now - observed) / 60_000);
  return Number.isFinite(data?.age_minutes) ? data.age_minutes : null;
}

/**
 * A station's reporting status: "ok", "late" or "down".
 *
 * The export judges it against the station's own rhythm (lib/report_status.py)
 * and ships it as `status`, with the thresholds it used (`late_after_minutes`,
 * `down_after_minutes`). Those are re-applied here to the live age, with the
 * same strict comparison, so the dot keeps up with the clock between exports.
 * Never kinder than the export: time only moves forward, and an export may
 * mark a station down for reasons of its own.
 *
 * A payload without a status (an older cached file) falls back to the legacy
 * flat flag: stale reads as late, never down.
 *
 * @param {Object} data - One station's latest-observation entry
 * @param {number} [now] - Epoch ms; injectable for tests
 * @returns {"ok"|"late"|"down"}
 */
export function reportStatus(data, now = Date.now()) {
  const exported = STATUSES.has(data?.status) ? data.status : data?.stale ? "late" : "ok";
  const age = liveAgeMinutes(data, now);
  const late = data?.late_after_minutes;
  const down = data?.down_after_minutes;
  if (age === null || !Number.isFinite(late) || !Number.isFinite(down)) return exported;
  const live = age > down ? "down" : age > late ? "late" : "ok";
  return SEVERITY[live] > SEVERITY[exported] ? live : exported;
}

/** Marker opacity for a reporting status. */
export function statusMarkerOpacity(status) {
  if (status === "down") return STALE_MARKER_OPACITY;
  if (status === "late") return LATE_MARKER_OPACITY;
  return 1.0;
}

/**
 * How old a stale reading is: "13h" under two days, "2d 4h" up to a week,
 * "12d" past that. Whole units, rounded down — "3h" means at least three.
 *
 * @param {Date|string} observedAt
 * @param {Date} [now] - Injectable for tests
 * @returns {string|null} null when the time is missing or unparseable
 */
export function staleAgeLabel(observedAt, now = new Date()) {
  if (!observedAt) return null;
  const ageMs = now - (observedAt instanceof Date ? observedAt : new Date(observedAt));
  if (!Number.isFinite(ageMs)) return null;
  const hours = Math.max(0, Math.floor(ageMs / 3_600_000));
  if (hours < 48) return `${hours}h`;
  if (hours < 7 * 24) return `${Math.floor(hours / 24)}d ${hours % 24}h`;
  return `${Math.floor(hours / 24)}d`;
}

/**
 * "Currently down for service. <reason> Station page ↗" for a station its
 * owner has taken out of service (stations.json `out_of_service`, carried
 * into the export). HTML; render through setSafeHTML.
 *
 * @param {{since?: string, reason?: string}} outOfService
 * @param {string|null} [sourceUrl] - The owner's page for the station
 * @returns {string}
 */
export function outOfServiceHTML(outOfService, sourceUrl = null) {
  const reason = outOfService?.reason ? ` ${outOfService.reason}` : "";
  const link = sourceUrl
    ? ` <a href="${sourceUrl}" target="_blank" rel="noopener">Station page ↗</a>`
    : "";
  return `Currently down for service.${reason}${link}`;
}

/**
 * Colour theme + header line for a map-popup "Latest Conditions" block.
 *
 * Plain words for a lay reader, age first:
 *   ok    "Latest Conditions:"
 *   late  "Latest Conditions (late: 4h ago):"                   amber
 *   down  "Down: no report since Tuesday Sep 22, 14:20. Last report:"   red
 *
 * @param {"ok"|"late"|"down"} status - From reportStatus()
 * @param {Object} [opts]
 * @param {string} [opts.label] - Header label, e.g. "Latest Conditions"
 * @param {string} [opts.staleLabel] - Label when late, if different (winds:
 *   "Current Wind" / "Last Wind")
 * @param {string} [opts.threshold] - Human-readable threshold, e.g. ">3h";
 *   only used when `observedAt` is absent
 * @param {Date|string} [opts.observedAt] - Observation time
 * @param {Date} [opts.now] - Injectable for tests
 * @param {Object} [opts.outOfService] - The export's `out_of_service`; when
 *   set it wins over `status`: the owner's word, not our clock
 * @param {string} [opts.sourceUrl] - Linked from the out-of-service line
 * @returns {{bg: string, border: string, headingColor: string, headerText: string}}
 */
export function statusPopupTheme(
  status,
  {
    label = "Latest Conditions",
    staleLabel = label,
    threshold = ">3h",
    observedAt = null,
    now = new Date(),
    outOfService = null,
    sourceUrl = null,
  } = {},
) {
  if (outOfService) {
    return {
      bg: "var(--color-callout-danger-bg, #fff5f5)",
      border: "var(--color-accent-red)",
      headingColor: "var(--color-accent-red)",
      headerText: outOfServiceHTML(outOfService, sourceUrl),
    };
  }
  if (status === "down") {
    const since = observedAt ? formatWeekdayDayTime(observedAt) : null;
    return {
      bg: "var(--color-callout-danger-bg, #fff5f5)",
      border: "var(--color-accent-red)",
      headingColor: "var(--color-accent-red)",
      headerText: since
        ? `Down: no report since ${since}. Last report:`
        : "Down: no recent report. Last report:",
    };
  }
  if (status === "late") {
    const age = staleAgeLabel(observedAt, now);
    return {
      bg: "var(--color-callout-warning-bg, #fff3e0)",
      border: "var(--color-accent-orange)",
      headingColor: "var(--color-accent-orange)",
      headerText: `${staleLabel} (late: ${age ? `${age} ago` : `${threshold} old`}):`,
    };
  }
  return {
    bg: "var(--color-callout-info-bg, #f0f8ff)",
    border: "var(--color-primary)",
    headingColor: "var(--map-popup-heading, var(--color-primary-dark))",
    headerText: `${label}:`,
  };
}

/**
 * Human-readable observation age: "5 minutes ago" / "3 hours ago" / "2 days ago".
 *
 * @param {number|null} ageMinutes
 * @returns {string|null} null when the age is unknown
 */
export function formatDataAge(ageMinutes) {
  if (ageMinutes == null) return null;

  if (ageMinutes < 60) {
    const mins = Math.round(ageMinutes);
    return `${mins} minute${mins !== 1 ? "s" : ""} ago`;
  }
  if (ageMinutes < 1440) {
    const hours = Math.round(ageMinutes / 60);
    return `${hours} hour${hours !== 1 ? "s" : ""} ago`;
  }
  const days = Math.round(ageMinutes / 1440);
  return `${days} day${days !== 1 ? "s" : ""} ago`;
}

/**
 * Human-readable staleness threshold for one lightstation, e.g. ">9h".
 *
 * The number is per station and comes from the export, which measures it
 * against that station's own inferred publishing cadence — most report every
 * three hours and are flagged at 9 h, while Cape Mudge, Chatham Point and
 * Pulteney Point report four times a day in daylight only and are normally
 * silent for 15 h overnight, so they are flagged at 18 h. Hardcoding ">12h"
 * here, as all three renderers used to, was wrong for both groups at once.
 *
 * @param {Object} station - Entry from latest_lightstation.json
 * @returns {string} Threshold label, falling back to the pre-2026-09-06 flat
 *   value when an older payload has no `stale_after_hours`.
 */
export function staleThresholdLabel(station) {
  const hours = station?.stale_after_hours;
  return `>${Number.isFinite(hours) ? Math.round(hours) : 12}h`;
}
