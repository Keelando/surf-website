const { test, expect } = require("@playwright/test");

const monitoredRoutes = [
  "/",
  "/tides.html",
  "/winds.html",
  "/storm_surge.html",
  "/lightstations.html",
  "/webcams.html",
  "/forecasts.html",
  "/guide.html",
];

test.describe("Frontend console health", () => {
  for (const route of monitoredRoutes) {
    test(`no console errors on ${route}`, async ({ page }, testInfo) => {
      const consoleLogs = [];
      const consoleErrors = [];
      const pageErrors = [];
      const failedRequests = [];

      page.on("console", (message) => {
        const entry = `[${message.type()}] ${message.text()}`;
        consoleLogs.push(entry);
        if (message.type() === "error") {
          consoleErrors.push(entry);
        }
      });

      page.on("pageerror", (error) => {
        pageErrors.push(error?.message || String(error));
      });

      page.on("requestfailed", (request) => {
        failedRequests.push(`${request.method()} ${request.url()} — ${request.failure()?.errorText}`);
      });

      const response = await page.goto(route, { waitUntil: "networkidle" });
      expect(response, `No HTTP response for ${route}`).not.toBeNull();
      if (response) {
        expect(response.ok(), `HTTP ${response.status()} on ${route}`).toBeTruthy();
      }

      await testInfo.attach(`console-log-${route}`, {
        body: consoleLogs.join("\n") || "No console output",
        contentType: "text/plain",
      });

      if (failedRequests.length > 0) {
        await testInfo.attach(`failed-requests-${route}`, {
          body: failedRequests.join("\n"),
          contentType: "text/plain",
        });
      }

      expect(pageErrors, `pageerror events on ${route}:\n${pageErrors.join("\n")}`).toEqual([]);
      expect(
        consoleErrors,
        `console.error output on ${route}:\n${consoleErrors.join("\n")}`,
      ).toEqual([]);
      expect(
        failedRequests,
        `Failed network requests on ${route}:\n${failedRequests.join("\n")}`,
      ).toEqual([]);
    });
  }
});
