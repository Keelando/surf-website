const { defineConfig } = require("@playwright/test");
const path = require("path");

// PW_PORT gives a run its own server. scripts/deploy.py sets it so the suite
// can never reuse a stray server on 4173 started from the other checkout,
// which would test the wrong files and pass (docs/DEPLOY.md, traps).
const PORT = Number(process.env.PW_PORT || 4173);

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
