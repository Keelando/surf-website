// Footer initialization - runs when footer.html is loaded by HTMX
// This file replaces the inline script that was previously in footer.html
(function () {
  function setSafeHTML(element, html) {
    if (!element) return;
    if (typeof window.setSanitizedHTML === "function") {
      window.setSanitizedHTML(element, html);
    } else {
      element.innerHTML = html;
    }
  }

  // Badge colour comes from the reporting *fraction*, not from a raw count, so
  // it survives the station list growing. A check-level failure in
  // system_health.json can still force the badge past these bands (see below).
  const REPORTING_OK_PCT = 93;
  const REPORTING_WARNING_PCT = 75;

  const SEVERITY_RANK = { ok: 0, warning: 1, error: 2 };

  function statusFromReporting(pct) {
    if (pct >= REPORTING_OK_PCT) return "ok";
    if (pct >= REPORTING_WARNING_PCT) return "warning";
    return "error";
  }

  /** Worst of the two signals wins, so a broken check is never painted green. */
  function worstStatus(a, b) {
    const rankA = SEVERITY_RANK[a] ?? 0;
    const rankB = SEVERITY_RANK[b] ?? 0;
    return rankA >= rankB ? a : b;
  }

  fetch("/data/system_health.json")
    .then((res) => res.json())
    .then((data) => {
      const badge = document.getElementById("system-status-badge");
      if (!badge) return;
      const text = badge.querySelector(".status-text");

      const freshness = data.checks.data_freshness;
      const total = freshness.total_stations;

      // Shorthand names for stale stations (keep footer compact)
      const shortNames = {
        "Halibut Bank": "Halibut",
        "English Bay": "EngBay",
        "Southern Georgia Strait": "SGS",
        "Sentry Shoal": "Sentry",
        "Crescent Beach Ocean": "CRPILE",
        "Angeles Point": "Angeles",
        "Neah Bay": "Neah",
        "New Dungeness": "Dungeness",
        "White Rock Pier": "WR Cam",
        "Cox Bay": "Cox Cam",
        "Mud Bay HD": "Mud Cam",
        Ambleside: "Ambl Cam",
        "White Rock East Beach": "BB Cam",
        "Point Atkinson": "PtAtk",
        Kitsilano: "Kits",
        "New Westminster": "NewWest",
        "Campbell River": "CampR",
        "Sisters Islets": "Sisters",
        Ballenas: "Ballenas",
        "Entrance Island": "Entrance",
        "Sand Heads": "SandH",
        Saturna: "Saturna",
        "Race Rocks": "RaceR",
        "Trial Island": "Trial",
      };

      const staleStations = freshness.stale_stations.filter(
        (s) => s.severity === "error" || s.severity === "warning",
      );

      // Stations the backend left out of the count on purpose — a daylight-only
      // cam after dark, a lightstation with no feed. They are already out of
      // `total`, so the tooltip only has to explain why it moved.
      const excluded = freshness.excluded_stations || [];
      const excludedNote = excluded.length
        ? "\nNot counted right now:\n" + excluded.map((s) => `${s.name} (${s.reason})`).join("\n")
        : "";
      const reporting = total - staleStations.length;
      const pct = Math.round((reporting / total) * 100);

      // data_freshness is excluded: the reporting fraction already covers it,
      // and letting it through would repaint the badge red for one stale
      // station — the very thing these bands exist to avoid. The other checks
      // (storage, database integrity, export freshness) are failures the
      // station count can't show, so they still escalate the colour.
      const otherChecks = Object.entries(data.checks)
        .filter(([name]) => name !== "data_freshness")
        .map(([, check]) => check.status);

      const status = otherChecks.reduce(worstStatus, statusFromReporting(pct));

      badge.classList.remove("status-ok", "status-warning", "status-error");
      badge.classList.add("status-" + status);

      if (staleStations.length === 0) {
        text.textContent = `${reporting}/${total} stations (${pct}%)`;
        badge.title =
          `System Status: ${status.toUpperCase()}\n` +
          (status === "ok"
            ? "All stations reporting normally"
            : "All stations reporting; a system check is failing") +
          excludedNote;
      } else {
        // Build compact down list with shorthand names
        const downNames = staleStations.map((s) => shortNames[s.name] || s.name.split(" ")[0]);
        const downList = downNames.join(", ");

        setSafeHTML(
          text,
          `${reporting}/${total} (${pct}%) ` +
            `<span class="status-down-list">Down: ${downList}</span>`,
        );

        badge.title =
          `System Status: ${status.toUpperCase()}\n` +
          staleStations
            .map((s) => {
              const age = s.age_hours
                ? s.age_hours < 1
                  ? `${Math.round(s.age_hours * 60)}m`
                  : `${Math.round(s.age_hours)}h`
                : "no data";
              return `${s.name} (${s.type}, ${age})`;
            })
            .join("\n") +
          excludedNote;
      }
    })
    .catch((err) => {
      const badge = document.getElementById("system-status-badge");
      if (badge) badge.querySelector(".status-text").textContent = "Status unavailable";
      console.error("Failed to load system health:", err);
    });
})();
