import assert from "node:assert/strict";
import { test } from "node:test";
import {
  formatDataAge,
  STALE_MARKER_OPACITY,
  staleDataWarningHTML,
  staleAgeLabel,
  stalePopupTheme,
} from "../../site/assets/js/shared/staleness.js";

test("fresh theme uses info colours and plain header", () => {
  const t = stalePopupTheme(false);
  assert.equal(t.bg, "var(--color-callout-info-bg, #f0f8ff)");
  assert.equal(t.border, "var(--color-primary)");
  assert.equal(t.headingColor, "var(--map-popup-heading, var(--color-primary-dark))");
  assert.equal(t.headerText, "Latest Conditions:");
});

test("stale theme uses danger colours and threshold header", () => {
  const t = stalePopupTheme(true);
  assert.equal(t.bg, "var(--color-callout-danger-bg, #fff5f5)");
  assert.equal(t.border, "var(--color-accent-red)");
  assert.equal(t.headingColor, "var(--color-accent-red)");
  assert.equal(t.headerText, "Latest Conditions (STALE - >3h old):");
});

test("label and threshold are configurable (lightstations, winds)", () => {
  assert.equal(
    stalePopupTheme(true, { threshold: ">12h" }).headerText,
    "Latest Conditions (STALE - >12h old):",
  );
  const winds = { label: "Current Wind", staleLabel: "Last Wind" };
  assert.equal(stalePopupTheme(true, winds).headerText, "Last Wind (STALE - >3h old):");
  assert.equal(stalePopupTheme(false, winds).headerText, "Current Wind:");
});

test("stale marker opacity matches the site-wide value", () => {
  assert.equal(STALE_MARKER_OPACITY, 0.35);
});

test("stale warning line is red and bold", () => {
  const html = staleDataWarningHTML();
  assert.match(html, /STALE DATA/);
  assert.match(html, /var\(--color-accent-red\)/);
  assert.match(html, /font-weight: 600/);
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

test("stale header states the actual age when the observation time is known", () => {
  const now = new Date("2026-09-23T18:00:00Z");
  const t = stalePopupTheme(true, { observedAt: "2026-09-23T04:40:00Z", now });
  assert.equal(t.headerText, "Latest Conditions (STALE - 13h old):");
});

test("observedAt wins over the threshold; threshold is the fallback", () => {
  const now = new Date("2026-09-23T18:00:00Z");
  assert.equal(
    stalePopupTheme(true, { threshold: ">9h", observedAt: "2026-09-23T06:00:00Z", now }).headerText,
    "Latest Conditions (STALE - 12h old):",
  );
  assert.equal(
    stalePopupTheme(true, { threshold: ">9h", observedAt: null, now }).headerText,
    "Latest Conditions (STALE - >9h old):",
  );
});

test("stale age switches to days past 48 hours", () => {
  const now = new Date("2026-09-23T18:00:00Z");
  assert.equal(staleAgeLabel("2026-09-22T00:00:00Z", now), "42h");
  assert.equal(staleAgeLabel("2026-09-21T14:00:00Z", now), "2d 4h");
  assert.equal(staleAgeLabel("not a date", now), null);
  assert.equal(staleAgeLabel(null, now), null);
});
