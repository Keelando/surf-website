// Footer initialization - runs when footer.html is loaded by HTMX
// This file replaces the inline script that was previously in footer.html
(function () {
  fetch("/data/system_health.json")
    .then((res) => res.json())
    .then((data) => {
      const badge = document.getElementById("system-status-badge");
      if (!badge) return;
      const dot = badge.querySelector(".status-dot");
      const text = badge.querySelector(".status-text");

      const freshness = data.checks.data_freshness;
      const total = freshness.total_stations;
      const reporting = total - freshness.stale_count;
      const pct = Math.round((reporting / total) * 100);

      badge.classList.remove("status-ok", "status-warning", "status-error");
      badge.classList.add("status-" + data.overall_status);

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

      if (staleStations.length === 0) {
        text.textContent = `${reporting}/${total} stations (${pct}%)`;
        badge.title = "System Status: OK\nAll stations reporting normally";
      } else {
        // Build compact down list with shorthand names
        const downNames = staleStations.map((s) => shortNames[s.name] || s.name.split(" ")[0]);
        const downList = downNames.join(", ");

        text.innerHTML =
          `${reporting}/${total} (${pct}%) ` +
          `<span class="status-down-list">Down: ${downList}</span>`;

        badge.title =
          `System Status: ${data.overall_status.toUpperCase()}\n` +
          staleStations
            .map((s) => {
              const age = s.age_hours
                ? s.age_hours < 1
                  ? `${Math.round(s.age_hours * 60)}m`
                  : `${Math.round(s.age_hours)}h`
                : "no data";
              return `${s.name} (${s.type}, ${age})`;
            })
            .join("\n");
      }
    })
    .catch((err) => {
      const badge = document.getElementById("system-status-badge");
      if (badge) badge.querySelector(".status-text").textContent = "Status unavailable";
      console.error("Failed to load system health:", err);
    });
})();
