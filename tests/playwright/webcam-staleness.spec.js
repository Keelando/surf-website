// Webcam staleness: STALE past 1 h, DOWN past 3 h, and daylight-only cams
// exempt while they are off duty.
//
// Needs a browser rather than a unit test for two reasons: the thresholds live
// in a module that exports none of this, and the daylight-only exemption reads
// /data/sunlight_times.json at runtime. Both the clock and the frame age are
// pinned here — the real files are rewritten by cron every few minutes, so an
// unpinned run would be testing today's weather.
//
// The overnight case is the one that matters. Four of the six cams stop at
// night by design (fetch_webcam.py skips them outside the capture window), so
// judging them on wall-clock age paints them DOWN every night; their age is
// measured from whichever is later, the last frame or the window opening.
const { test, expect } = require("@playwright/test");

const DAYLIGHT_ONLY = ["Ambleside", "Mud Bay HD (SE)", "Mud Bay HD (SW)", "Cox Bay"];

/** Freeze Date, and serve every cam a frame of exactly `ageMinutes`. */
async function pinPage(page, { nowISO, ageMinutes }) {
  const fixedNow = new Date(nowISO).getTime();
  await page.addInitScript(`{
    const F = ${fixedNow};
    const R = Date;
    class D extends R {
      constructor(...a) { return a.length ? new R(...a) : new R(F); }
      static now() { return F; }
    }
    Date = D;
  }`);
  const stamp = new Date(fixedNow - ageMinutes * 60000).toISOString();
  await page.route("**/data/*/latest.json", async (route) => {
    const response = await route.fetch();
    const body = JSON.parse(await response.text());
    body.timestamp = stamp;
    route.fulfill({ contentType: "application/json", body: JSON.stringify(body) });
  });
}

async function readCards(page) {
  await page.goto("/webcams.html", { waitUntil: "networkidle" });
  await expect(page.locator(".webcam-card").first()).toBeAttached({ timeout: 10000 });
  await page.waitForTimeout(1200); // metadata refresh settles
  return page.evaluate(() =>
    [...document.querySelectorAll(".webcam-card")].map((card) => ({
      name: (card.querySelector(".webcam-title, h3, h2")?.textContent || "").trim().split("\n")[0],
      state: card.classList.contains("webcam-stale-error")
        ? "DOWN"
        : card.classList.contains("webcam-stale")
          ? "STALE"
          : "ok",
      daylightOnly: /stop at night/.test(
        card.querySelector(".webcam-update-notice")?.textContent || "",
      ),
    })),
  );
}

// 20:00 UTC is 13:00 PDT: every cam's capture window is open.
const MIDDAY = "2026-09-06T20:00:00Z";
// 10:00 UTC is 03:00 PDT: every daylight-only cam is off duty.
const NIGHT = "2026-09-07T10:00:00Z";

test.describe("Webcam staleness thresholds", () => {
  test("a fresh frame is not flagged", async ({ page }) => {
    await pinPage(page, { nowISO: MIDDAY, ageMinutes: 20 });
    for (const card of await readCards(page)) expect(card.state, card.name).toBe("ok");
  });

  test("past 1 h is STALE, not yet DOWN", async ({ page }) => {
    await pinPage(page, { nowISO: MIDDAY, ageMinutes: 90 });
    for (const card of await readCards(page)) expect(card.state, card.name).toBe("STALE");
  });

  test("past 3 h is DOWN", async ({ page }) => {
    await pinPage(page, { nowISO: MIDDAY, ageMinutes: 300 });
    for (const card of await readCards(page)) expect(card.state, card.name).toBe("DOWN");
  });

  test("overnight, daylight-only cams are exempt and 24/7 cams are not", async ({ page }) => {
    await pinPage(page, { nowISO: NIGHT, ageMinutes: 600 });
    const cards = await readCards(page);

    const daylight = cards.filter((c) => c.daylightOnly);
    expect(daylight.length, "expected the daylight-only cams to be identifiable").toBe(
      DAYLIGHT_ONLY.length,
    );
    for (const card of daylight) expect(card.state, `${card.name} at night`).toBe("ok");

    const alwaysOn = cards.filter((c) => !c.daylightOnly);
    expect(alwaysOn.length).toBeGreaterThan(0);
    for (const card of alwaysOn) expect(card.state, `${card.name} at night`).toBe("DOWN");
  });

  test("after sunrise the clock starts at the window, not at last night's frame", async ({
    page,
  }) => {
    // 14:00 UTC is ~20 min after sunrise; the frame is from last night's close.
    await pinPage(page, { nowISO: "2026-09-06T14:00:00Z", ageMinutes: 690 });
    const cards = await readCards(page);

    // 11.5 h of frame age, but only ~1.5 h of it inside the capture window: the
    // cams are late, not dead, and must not jump straight to DOWN.
    for (const card of cards.filter((c) => c.daylightOnly)) {
      expect(card.state, `${card.name} just after sunrise`).toBe("STALE");
    }
    // A 24/7 cam has no such excuse.
    for (const card of cards.filter((c) => !c.daylightOnly)) {
      expect(card.state, `${card.name} just after sunrise`).toBe("DOWN");
    }
  });
});
