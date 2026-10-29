#!/usr/bin/env node
/*
 * Lighthouse reports for the key pages, on phone, tablet and desktop.
 *
 *   npm run lighthouse              local site/ (tests/playwright/serve.py), checks enforced
 *   npm run lighthouse:live         https://halibutbank.ca, for the history; no checks
 *   ... -- --pages index,winds --devices tablet   a subset
 *
 * Every run appends one line per page x device to logs/lighthouse/history.jsonl
 * and prints each score next to its change since the last run of the same
 * target, page and device. HTML reports land in logs/lighthouse/<run>/.
 *
 * Local scores are for catching regressions before a push, not for quoting:
 * serve.py is a single-threaded Python server with no compression, and
 * Lighthouse simulates slow 4G on top. The live run is the one to track.
 * Local checks fail the run (exit 1) on layout shift and accessibility, which
 * don't depend on the server; performance is reported, not enforced.
 *
 * Chrome: Playwright's Chromium (already installed for npm run test:frontend),
 * or CHROME_PATH if set.
 */
import { execFileSync, spawn } from "node:child_process";
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "@playwright/test";
import * as chromeLauncher from "chrome-launcher";
import lighthouse from "lighthouse";
import desktopConfig from "lighthouse/core/config/desktop-config.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const OUT_DIR = path.join(ROOT, "logs", "lighthouse");
const HISTORY = path.join(OUT_DIR, "history.jsonl");
const LIVE_URL = "https://halibutbank.ca";
const PORT = Number(process.env.LH_PORT || 4175);

const PAGES = ["index", "forecasts", "winds", "tides", "storm_surge", "lightstations", "webcams"];

// Lighthouse's own mobile and desktop presets, plus a tablet: 800x1280, the
// size the 2026-10-29 tablet work emulated (a Lenovo Tab Plus held upright),
// with mobile throttling.
const DEVICES = {
  mobile: undefined,
  tablet: {
    extends: "lighthouse:default",
    settings: {
      formFactor: "mobile",
      screenEmulation: { mobile: true, width: 800, height: 1280, deviceScaleFactor: 1.5, disabled: false },
    },
  },
  desktop: desktopConfig,
};

// Local runs only. Thresholds on scores are 0-1, as Lighthouse reports them.
const CHECKS = [
  { name: "layout shift", metric: "cls", max: 0.1 },
  { name: "accessibility", score: "accessibility", min: 0.9 },
];

const CATEGORIES = { performance: "perf", accessibility: "a11y", "best-practices": "best", seo: "seo" };
const METRICS = {
  "first-contentful-paint": "fcp",
  "largest-contentful-paint": "lcp",
  "total-blocking-time": "tbt",
  "cumulative-layout-shift": "cls",
  "speed-index": "si",
};

function parseArgs(argv) {
  const opts = { live: false, pages: PAGES, devices: Object.keys(DEVICES) };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--live") opts.live = true;
    else if (arg === "--pages") opts.pages = argv[++i].split(",");
    else if (arg === "--devices") opts.devices = argv[++i].split(",");
    else throw new Error(`Unknown option ${arg} (use --live, --pages a,b, --devices a,b)`);
  }
  for (const d of opts.devices) if (!(d in DEVICES)) throw new Error(`Unknown device ${d}`);
  return opts;
}

function pageUrl(base, page) {
  return page === "index" ? `${base}/` : `${base}/${page}.html`;
}

async function startLocalServer() {
  const server = spawn("python3", ["tests/playwright/serve.py", String(PORT)], { cwd: ROOT, stdio: "ignore" });
  const base = `http://127.0.0.1:${PORT}`;
  for (let i = 0; i < 100; i++) {
    try {
      if ((await fetch(base)).ok) return { base, server };
    } catch {
      // not listening yet
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  server.kill();
  throw new Error(`serve.py did not answer on port ${PORT}`);
}

function lastRuns() {
  const last = new Map();
  try {
    for (const line of readFileSync(HISTORY, "utf8").split("\n")) {
      if (!line.trim()) continue;
      const row = JSON.parse(line);
      last.set(`${row.target}|${row.page}|${row.device}`, row);
    }
  } catch {
    // no history yet
  }
  return last;
}

function summarise(lhr) {
  const scores = {};
  for (const [id, key] of Object.entries(CATEGORIES)) scores[key] = lhr.categories[id]?.score ?? null;
  const metrics = {};
  for (const [id, key] of Object.entries(METRICS)) metrics[key] = lhr.audits[id]?.numericValue ?? null;
  return { scores, metrics };
}

function withDelta(now, before, digits = 0, scale = 100) {
  if (now == null) return "-";
  const shown = (now * scale).toFixed(digits);
  if (before == null) return shown;
  const delta = Math.round((now - before) * scale * 10 ** digits) / 10 ** digits;
  return delta ? `${shown} (${delta > 0 ? "+" : ""}${delta})` : shown;
}

function failedChecks(result) {
  const failed = [];
  for (const check of CHECKS) {
    const value = check.metric ? result.metrics[check.metric] : result.scores[check.score];
    if (value == null) continue;
    if (check.max != null && value > check.max) failed.push(`${check.name} ${value.toFixed(3)} > ${check.max}`);
    if (check.min != null && value < check.min) failed.push(`${check.name} ${value} < ${check.min}`);
  }
  return failed;
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const target = opts.live ? "live" : "local";
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(OUT_DIR, `${target}-${stamp}`);
  mkdirSync(runDir, { recursive: true });

  let commit = null;
  try {
    commit = execFileSync("git", ["rev-parse", "--short", "HEAD"], { cwd: ROOT }).toString().trim();
  } catch {
    // not a checkout
  }

  const local = opts.live ? null : await startLocalServer();
  const base = opts.live ? LIVE_URL : local.base;
  const chrome = await chromeLauncher.launch({
    chromePath: process.env.CHROME_PATH || chromium.executablePath(),
    chromeFlags: ["--headless=new", "--no-sandbox"],
  });

  const previous = lastRuns();
  const rows = [];
  const failures = [];
  try {
    for (const page of opts.pages) {
      for (const device of opts.devices) {
        process.stdout.write(`${page} on ${device}... `);
        const runner = await lighthouse(
          pageUrl(base, page),
          { port: chrome.port, output: "html", logLevel: "error" },
          DEVICES[device],
        );
        if (runner.lhr.runtimeError) {
          console.log(`error: ${runner.lhr.runtimeError.message}`);
          failures.push(`${page} on ${device}: ${runner.lhr.runtimeError.code}`);
          continue;
        }
        writeFileSync(path.join(runDir, `${page}-${device}.html`), runner.report);
        const result = { time: new Date().toISOString(), target, commit, page, device, ...summarise(runner.lhr) };
        appendFileSync(HISTORY, `${JSON.stringify(result)}\n`);
        const before = previous.get(`${target}|${page}|${device}`);
        const failed = opts.live ? [] : failedChecks(result);
        failures.push(...failed.map((f) => `${page} on ${device}: ${f}`));
        console.log(failed.length ? `FAILED: ${failed.join(", ")}` : "done");
        rows.push({
          page,
          device,
          perf: withDelta(result.scores.perf, before?.scores.perf),
          a11y: withDelta(result.scores.a11y, before?.scores.a11y),
          best: withDelta(result.scores.best, before?.scores.best),
          seo: withDelta(result.scores.seo, before?.scores.seo),
          "LCP s": withDelta(result.metrics.lcp, before?.metrics.lcp, 1, 1 / 1000),
          "TBT ms": withDelta(result.metrics.tbt, before?.metrics.tbt, 0, 1),
          CLS: withDelta(result.metrics.cls, before?.metrics.cls, 3, 1),
        });
      }
    }
  } finally {
    await chrome.kill();
    local?.server.kill();
  }

  console.log(`\nLighthouse, ${target} (${base})${commit ? ` at ${commit}` : ""}; change since the last ${target} run in brackets`);
  console.table(rows);
  console.log(`Reports: ${path.relative(ROOT, runDir)}/   History: ${path.relative(ROOT, HISTORY)}`);
  if (failures.length) {
    console.error(`\n${failures.length} check(s) failed:\n  ${failures.join("\n  ")}`);
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
