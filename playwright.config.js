const { defineConfig } = require("@playwright/test");
const path = require("path");

// PW_PORT gives a run its own server, so a run in the dev worktree can never
// reuse a stray server on 4173 started from the main checkout, which would
// test the wrong files and pass (docs/DEV_PREVIEW.md).
const PORT = Number(process.env.PW_PORT || 4173);

// Browsers refuse the Fetch standard's "bad ports" (https://fetch.spec.whatwg.org/#port-blocking).
// Firefox enforces them and Playwright's Chromium doesn't, so a run on one
// (PW_PORT=4190, ManageSieve, 2026-11-13) failed every Firefox test with
// "navigation is interrupted by another navigation" and passed in Chromium.
const BAD_PORTS = new Set([
  1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23, 25, 37, 42, 43, 53, 69, 77, 79, 87, 95, 101, 102,
  103, 104, 109, 110, 111, 113, 115, 117, 119, 123, 135, 137, 139, 143, 161, 179, 389, 427, 465,
  512, 513, 514, 515, 526, 530, 531, 532, 540, 548, 554, 556, 563, 587, 601, 636, 989, 990, 993,
  995, 1719, 1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061, 6000, 6566, 6665, 6666, 6667, 6668,
  6669, 6679, 6697, 10080,
]);
if (BAD_PORTS.has(PORT)) {
  throw new Error(`PW_PORT=${PORT} is a port browsers refuse to load (Fetch "bad port"); pick another, e.g. 4180.`);
}

module.exports = defineConfig({
  testDir: "./tests/playwright",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: [
    ["list"],
    ["html", { open: "never", outputFolder: "playwright-report" }],
  ],
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { browserName: "chromium" },
    },
    {
      name: "firefox",
      use: { browserName: "firefox" },
    },
  ],
  webServer: {
    command: `python3 tests/playwright/serve.py ${PORT}`,
    port: PORT,
    reuseExistingServer: !process.env.CI && !process.env.PW_PORT,
    timeout: 120 * 1000,
  },
});
