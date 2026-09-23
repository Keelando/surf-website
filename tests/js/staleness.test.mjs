import assert from "node:assert/strict";
import { test } from "node:test";
import {
  formatDataAge,
  LATE_MARKER_OPACITY,
  reportStatus,
  STALE_MARKER_OPACITY,
  staleAgeLabel,
  statusMarkerOpacity,
  statusPopupTheme,
} from "../../site/assets/js/shared/staleness.js";

test("ok theme uses info colours and plain header", () => {
  const t = statusPopupTheme("ok");
  assert.equal(t.bg, "var(--color-callout-info-bg, #f0f8ff)");
  assert.equal(t.border, "var(--color-primary)");
  assert.equal(t.headingColor, "var(--map-popup-heading, var(--color-primary-dark))");
  assert.equal(t.headerText, "Latest Conditions:");
});

test("late theme is amber and leads with the age", () => {
  const now = new Date("2026-09-23T18:00:00Z");
  const t = statusPopupTheme("late", { observedAt: "2026-09-23T14:40:00Z", now });
  assert.equal(t.bg, "var(--color-callout-warning-bg, #fff3e0)");
  assert.equal(t.border, "var(--color-accent-orange)");
  assert.equal(t.headingColor, "var(--color-accent-orange)");
  assert.equal(t.headerText, "Latest Conditions (late: 3h ago):");
});

test("late falls back to the threshold when the time is unknown", () => {
  assert.equal(
    statusPopupTheme("late", { threshold: ">9h" }).headerText,
    "Latest Conditions (late: >9h old):",
  );
});

test("down theme is red and names when the last report was, in Pacific time", () => {
  const t = statusPopupTheme("down", { observedAt: "2026-09-22T21:20:00Z" });
  assert.equal(t.bg, "var(--color-callout-danger-bg, #fff5f5)");
  assert.equal(t.border, "var(--color-accent-red)");
  assert.equal(t.headingColor, "var(--color-accent-red)");
  assert.equal(t.headerText, "Down: no report since Tuesday Sep 22, 14:20. Last report:");
  assert.equal(
    statusPopupTheme("down").headerText,
    "Down: no recent report. Last report:",
  );
});

test("labels are configurable (winds)", () => {
  const now = new Date("2026-09-23T18:00:00Z");
  const winds = { label: "Current Wind", staleLabel: "Last Wind", now };
  assert.equal(
    statusPopupTheme("late", { ...winds, observedAt: "2026-09-23T13:00:00Z" }).headerText,
    "Last Wind (late: 5h ago):",
  );
  assert.equal(statusPopupTheme("ok", winds).headerText, "Current Wind:");
});

test("reportStatus reads the export's status, else the legacy stale flag", () => {
  assert.equal(reportStatus({ status: "down", stale: true }), "down");
  assert.equal(reportStatus({ status: "late", stale: false }), "late");
  assert.equal(reportStatus({ status: "ok", stale: false }), "ok");
  // Older payload, no status: stale reads as late, never down.
  assert.equal(reportStatus({ stale: true }), "late");
  assert.equal(reportStatus({ stale: false }), "ok");
  assert.equal(reportStatus({ status: "bogus", stale: true }), "late");
  assert.equal(reportStatus(null), "ok");
});

test("marker opacity: down dims fully, late part-way", () => {
  assert.equal(STALE_MARKER_OPACITY, 0.35);
  assert.equal(LATE_MARKER_OPACITY, 0.6);
  assert.equal(statusMarkerOpacity("down"), 0.35);
  assert.equal(statusMarkerOpacity("late"), 0.6);
  assert.equal(statusMarkerOpacity("ok"), 1.0);
});

test("formatDataAge scales minutes to hours to days", () => {
  assert.equal(formatDataAge(0), "0 minutes ago");
  assert.equal(formatDataAge(1), "1 minute ago");
  assert.equal(formatDataAge(59), "59 minutes ago");
  assert.equal(formatDataAge(60), "1 hour ago");
  assert.equal(formatDataAge(180), "3 hours ago");
  assert.equal(formatDataAge(1440), "1 day ago");
  assert.equal(formatDataAge(4320), "3 days ago");
});

test("formatDataAge returns null when the age is unknown", () => {
  assert.equal(formatDataAge(null), null);
  assert.equal(formatDataAge(undefined), null);
});

test("stale age switches to days past 48 hours", () => {
  const now = new Date("2026-09-23T18:00:00Z");
  assert.equal(staleAgeLabel("2026-09-22T00:00:00Z", now), "42h");
  assert.equal(staleAgeLabel("2026-09-21T14:00:00Z", now), "2d 4h");
  assert.equal(staleAgeLabel("not a date", now), null);
  assert.equal(staleAgeLabel(null, now), null);
});
