// degreesToCardinal, getDirectionalArrow, formatTimestamp, formatTimeOnly
// provided by chart-utils-v4.js (loaded earlier)

// Helper function to format data age in human-readable format
function formatDataAge(ageMinutes) {
  if (ageMinutes == null) return null;

  if (ageMinutes < 60) {
    const mins = Math.round(ageMinutes);
    return `${mins} minute${mins !== 1 ? "s" : ""} ago`;
  } else if (ageMinutes < 1440) {
    // Less than 24 hours
    const hours = Math.round(ageMinutes / 60);
    return `${hours} hour${hours !== 1 ? "s" : ""} ago`;
  } else {
    // Days
    const days = Math.round(ageMinutes / 1440);
    return `${days} day${days !== 1 ? "s" : ""} ago`;
  }
}

// Helper function to create angular spread vector visualization
// Shows main direction arrow with smaller arrows indicating directional spread
function createAngularSpreadVector(avgDirection, spread, size = 60) {
  if (avgDirection == null || spread == null) return "";

  const halfSpread = spread / 2;
  const minDir = avgDirection - halfSpread;
  const maxDir = avgDirection + halfSpread;

  // Center point
  const cx = size / 2;
  const cy = size / 2;
  const radius = size * 0.42; // Arrow radius from center

  // Match the existing getDirectionalArrow rotation convention:
  // Base arrow points DOWN (towards south), then rotate by degrees
  const mainRot = avgDirection;
  const minRot = minDir;
  const maxRot = maxDir;

  // Calculate arc path for the spread sector
  // The sector should show TRAVEL directions (where waves go TO), not source directions
  // So add 180° to convert from source to travel direction, matching the arrow
  // SVG arc angles: 0° = right (3 o'clock), 90° = down (6 o'clock), measured clockwise
  // Subtract 90° to convert from compass to SVG angles
  const startAngleSVG = minRot + 180 - 90;
  const endAngleSVG = maxRot + 180 - 90;
  const startAngleRad = (startAngleSVG * Math.PI) / 180;
  const endAngleRad = (endAngleSVG * Math.PI) / 180;
  const arcRadius = radius + 2; // Extend to circle edge

  const x1 = cx + arcRadius * Math.cos(startAngleRad);
  const y1 = cy + arcRadius * Math.sin(startAngleRad);
  const x2 = cx + arcRadius * Math.cos(endAngleRad);
  const y2 = cy + arcRadius * Math.sin(endAngleRad);

  const largeArc = spread > 180 ? 1 : 0;

  return `
    <svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" style="display: inline-block; vertical-align: middle; margin-left: 0.5rem;">
      <!-- Background circle -->
      <circle cx="${cx}" cy="${cy}" r="${radius + 2}" fill="none" stroke="#e0e7ee" stroke-width="1"/>

      <!-- Spread sector -->
      <path d="M ${cx},${cy} L ${x1},${y1} A ${arcRadius},${arcRadius} 0 ${largeArc},1 ${x2},${y2} Z"
            fill="rgba(30, 136, 229, 0.15)"
            stroke="rgba(30, 136, 229, 0.3)"
            stroke-width="1"/>

      <!-- Main direction arrow (single arrow within sector) -->
      <g transform="rotate(${mainRot} ${cx} ${cy})">
        <!-- Arrow shaft -->
        <line x1="${cx}" y1="${cy - radius + 8}" x2="${cx}" y2="${cy + radius - 3}"
              stroke="#1e88e5" stroke-width="2.5"/>
        <!-- Triangular arrowhead -->
        <path d="M${cx},${cy + radius + 2} L${cx - 5},${cy + radius - 8} L${cx + 5},${cy + radius - 8} Z"
              fill="#1e88e5"/>
      </g>

      <!-- Cardinal directions -->
      <text x="${cx}" y="8" text-anchor="middle" font-size="8" fill="#999">N</text>
      <text x="${size - 6}" y="${cy + 3}" text-anchor="middle" font-size="8" fill="#999">E</text>
      <text x="${cx}" y="${size - 2}" text-anchor="middle" font-size="8" fill="#999">S</text>
      <text x="6" y="${cy + 3}" text-anchor="middle" font-size="8" fill="#999">W</text>
    </svg>
  `;
}

function setSafeHTML(element, html) {
  if (!element) return;

  if (typeof window.setSanitizedHTML === "function") {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

async function loadBuoyData() {
  const container = document.getElementById("buoy-container");
  const timestamp = document.getElementById("timestamp");

  // Grouped by geographic region
  const buoyGroups = [
    {
      region: "Strait of Georgia",
      stations: [
        "4600146", // Halibut Bank
        "4600304", // English Bay
        "4600303", // Southern Georgia Strait
        "4600131", // Sentry Shoal
      ],
    },
    {
      region: "Boundary Bay",
      stations: [
        "CRPILE", // Crescent Beach Ocean
        "CRCHAN", // Crescent Channel
      ],
    },
    {
      region: "Juan de Fuca Strait",
      stations: [
        "46087", // Neah Bay
        "46088", // New Dungeness
        "46267", // Angeles Point
      ],
    },
    {
      region: "West Coast Vancouver Island",
      stations: [
        "4600206", // La Perouse Bank
      ],
    },
  ];
  // COLEB excluded - wind-only station, available in charts only

  // Source links for each buoy
  const sourceLinks = {
    4600146:
      "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46146",
    4600304:
      "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46304",
    4600303:
      "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46303",
    4600131:
      "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46131",
    4600206:
      "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46206",
    46087: "https://www.ndbc.noaa.gov/station_page.php?station=46087",
    46088: "https://www.ndbc.noaa.gov/station_page.php?station=46088",
    46267: "https://www.ndbc.noaa.gov/station_page.php?station=46267",
    CRPILE: "https://developers.flowworks.com/",
    CRCHAN: "https://developers.flowworks.com/",
    COLEB: "https://developers.flowworks.com/",
  };

  try {
    const data = await fetchWithTimeout(`/data/latest_buoy_v2.json?t=${Date.now()}`);

    container.textContent = "";

    // Find most recent observation time
    let mostRecentTime = null;
    Object.values(data).forEach((buoy) => {
      if (buoy.observation_time) {
        const time = new Date(buoy.observation_time);
        if (!mostRecentTime || time > mostRecentTime) {
          mostRecentTime = time;
        }
      }
    });

    // Add "Last Updated" header (24-hour, shorter format: "11/11 19:58")
    if (mostRecentTime) {
      const updateHeader = document.createElement("div");
      updateHeader.className = "last-updated-header";
      updateHeader.textContent = `Last Updated: ${mostRecentTime
        .toLocaleString("en-US", {
          month: "numeric",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
          timeZone: "America/Vancouver",
        })
        .replace(",", "")}`;
      container.appendChild(updateHeader);
    }

    // Render buoys grouped by region
    buoyGroups.forEach((group) => {
      // Create region group container
      const regionGroup = document.createElement("div");
      regionGroup.className = "region-group";

      // Add region header (clickable to collapse/expand)
      const regionHeader = document.createElement("div");
      regionHeader.className = "region-header";
      regionHeader.style.cursor = "pointer";
      regionHeader.style.userSelect = "none";
      setSafeHTML(
        regionHeader,
        `<span class="region-toggle-btn">▼</span> ${group.region} <span style="font-size: 0.8em; font-weight: normal; opacity: 0.8;">(${group.stations.length} stations)</span>`,
      );
      regionHeader.addEventListener("click", () => toggleRegion(group.region));
      regionGroup.appendChild(regionHeader);
      regionGroup.id = `region-${group.region.replace(/\s+/g, "-")}`;

      // Create grid container for this region's cards
      const cardsGrid = document.createElement("div");
      cardsGrid.className = "buoy-cards-grid";

      // Render stations in this region
      group.stations.forEach((id) => {
        const b = data[id];
        if (!b) return;

        const card = document.createElement("div");
        card.className = "buoy-card";
        card.id = `buoy-${id}`; // Add ID for anchor linking from map

        // Special styling for NOAA buoys
        if (id === "46087" || id === "46088" || id === "46267") {
          card.style.borderLeft = "4px solid var(--color-source-noaa-border)";
        }

        // Special styling for Surrey FlowWorks stations
        if (id === "CRPILE" || id === "CRCHAN" || id === "COLEB") {
          card.style.borderLeft = "4px solid var(--color-accent-green)";
        }

        // No data in DB at all (e.g. buoy offline and records purged) — render minimal card
        if (b.no_data) {
          let noDataContent = `<div class="buoy-card-inner"><h2>${b.name || id}`;
          if (id === "46087" || id === "46088" || id === "46267") {
            noDataContent += ` <span style="font-size: 0.7em; color: var(--color-source-noaa-text); font-weight: normal;">🇺🇸 NOAA</span>`;
          } else if (id === "CRPILE" || id === "CRCHAN" || id === "COLEB") {
            noDataContent += ` <span style="font-size: 0.7em; color: var(--color-accent-green); font-weight: normal;">🏛️ Surrey (FlowWorks)</span>`;
          } else {
            noDataContent += ` <span style="font-size: 0.7em; color: var(--color-source-envcan-text); font-weight: normal;">🇨🇦 Env Canada</span>`;
          }
          noDataContent += `</h2>
            <p class="buoy-metric" style="margin: 1rem 0; padding: 1rem; background: var(--color-callout-danger-bg); border-left: 4px solid var(--color-accent-red); border-radius: 4px; color: var(--color-error-text); font-weight: 600;">
              🔴 Station offline — no data available
            </p>`;
          if (sourceLinks[id]) {
            noDataContent += `<p style="margin-top: 0.75rem; margin-bottom: 0; padding-top: 0.5rem; border-top: 1px solid var(--color-border); text-align: center;">
              <a href="${sourceLinks[id]}" target="_blank" rel="noopener noreferrer" style="font-size: 0.85em; color: var(--color-primary-dark); text-decoration: none; font-weight: 500;">🔗 View Source Data</a>
            </p>`;
          }
          noDataContent += `</div>`;
          setSafeHTML(card, noDataContent);
          cardsGrid.appendChild(card);
          return;
        }

        // Format timestamp in Pacific Time (24-hour, shorter format: "11/11 19:58")
        const updated = b.observation_time
          ? new Date(b.observation_time)
              .toLocaleString("en-US", {
                month: "numeric",
                day: "numeric",
                hour: "2-digit",
                minute: "2-digit",
                hour12: false,
                timeZone: "America/Vancouver",
              })
              .replace(",", "")
          : "—";

        // Data freshness handling:
        // - Up to 3 hours: No warning
        // - 3-12 hours: Show age warning, display data
        // - >12 hours: Show "STATION DOWN", hide data
        const ageMinutes = b.age_minutes || 0;
        const ageHours = ageMinutes / 60;
        const isDown = ageHours > 12;
        const isStale = ageHours > 3 && !isDown;

        let ageWarning = "";
        if (isDown) {
          ageWarning = ` <span style="color: var(--color-accent-red); font-weight: bold; background: var(--color-callout-danger-bg); padding: 0.2rem 0.5rem; border-radius: 3px;">🔴 STATION DOWN (${formatDataAge(ageMinutes)})</span>`;
        } else if (isStale) {
          ageWarning = ` <span style="color: var(--color-error-text); font-weight: bold;">⚠️ STALE (${formatDataAge(ageMinutes)})</span>`;
        }

        // Round wind speeds to integers
        const windSpeed = b.wind_speed != null ? Math.round(b.wind_speed) : "—";
        const windGust = b.wind_gust != null ? Math.round(b.wind_gust) : "—";

        // Build the card content based on buoy type
        let cardContent = `<h2>${b.name || id}`;

        // Add source badge
        if (id === "46087" || id === "46088" || id === "46267") {
          cardContent += ` <span style="font-size: 0.7em; color: var(--color-source-noaa-text); font-weight: normal;">🇺🇸 NOAA</span>`;
        } else if (id === "CRPILE" || id === "CRCHAN" || id === "COLEB") {
          cardContent += ` <span style="font-size: 0.7em; color: var(--color-accent-green); font-weight: normal;">🏛️ Surrey (FlowWorks)</span>`;
        } else {
          cardContent += ` <span style="font-size: 0.7em; color: var(--color-source-envcan-text); font-weight: normal;">🇨🇦 Env Canada</span>`;
        }

        cardContent += `</h2>`;
        cardContent += `<p style="font-size: 0.9em; color: var(--color-text-muted); margin-top: -0.5rem;">Last Update: ${updated}${ageWarning}</p>`;

        // === CONDENSED VIEW (Always visible) ===
        cardContent += `<div class="card-compact-view">`;

        // Determine decimal precision for wave height (Boundary Bay stations use 2 decimals)
        // Declared here so it's available in both compact view and details section
        const isBoundaryBay = id === "CRPILE" || id === "CRCHAN";
        const heightPrecision = isBoundaryBay ? 2 : 1;

        // If station is down (>12 hours), show station down message instead of data
        if (isDown) {
          cardContent += `
          <p class="buoy-metric" style="margin: 1rem 0; padding: 1rem; background: var(--color-callout-danger-bg); border-left: 4px solid var(--color-accent-red); border-radius: 4px; color: var(--color-error-text); font-weight: 600;">
            🔴 Station Down - No recent data available
          </p>
          <p style="font-size: 0.85em; color: var(--color-text-muted); text-align: center; margin-top: 0.5rem;">
            Last data received ${formatDataAge(ageMinutes)}
          </p>
        `;
        } else {
          // Compact Wind Line - Format: "WNW 15 G 20 kn (350°)"
          let windDisplay = "No data";
          if (windSpeed !== "—") {
            const windCardinal = b.wind_direction_cardinal ?? "—";
            const windDir = b.wind_direction_deg || b.wind_direction;
            const windDegrees = windDir != null ? ` (${Math.round(windDir)}°)` : "";
            const gustPart = windGust !== "—" ? ` G ${windGust}` : "";
            windDisplay = `${windCardinal} ${windSpeed}${gustPart} kn${windDegrees} ${getDirectionalArrow(windDir, "wind")}`;
          }
          cardContent += `<p class="buoy-metric" style="margin: 0.5rem 0;"><b>💨 Wind:</b> ${windDisplay}</p>`;

          // Compact Wave Line - Format: "Sig Wave: WSW 0.9m @ 4.1s (251°)"
          // Height is always significant; the period type is tagged when it isn't
          // the significant value (e.g. NOAA dominant, or an avg/peak fallback).
          let waveDisplay = "No data";
          let waveLabel = "🌊 Sig Wave:";

          // Small muted tag noting which period type is shown
          const periodTag = (label) =>
            ` <span style="color: var(--color-text-muted); font-size: 0.85em;">${label}</span>`;

          if (id === "46087") {
            // Neah Bay (NOAA) - show swell info (continuous open-ocean swell)
            waveLabel = "🌊 Swell:";
            const swellHeight =
              b.swell_height != null ? b.swell_height.toFixed(heightPrecision) : "—";
            const swellPeriod = b.swell_period != null ? b.swell_period.toFixed(1) : null;
            const swellDir = b.swell_direction_cardinal ?? null;
            const swellDegrees =
              b.swell_direction != null ? ` (${Math.round(b.swell_direction)}°)` : "";
            if (swellHeight !== "—") {
              const dirDisplay = swellDir ? `${swellDir} ` : "";
              const arrowDisplay =
                b.swell_direction != null
                  ? ` ${getDirectionalArrow(b.swell_direction, "wave")}`
                  : "";
              const periodDisplay = swellPeriod != null ? ` @ ${swellPeriod}s` : "";
              waveDisplay = `${dirDisplay}${swellHeight}m${periodDisplay}${swellDegrees}${arrowDisplay}`;
            }
          } else if (id === "46088" || id === "46267") {
            // NOAA (New Dungeness, Angeles Point) - sig height + dominant period (DPD).
            // NOAA stores DPD in wave_period_sig; tag it "dominant" so it's explicit.
            const waveHeight =
              b.wave_height_sig != null ? b.wave_height_sig.toFixed(heightPrecision) : "—";
            const wavePeriod = b.wave_period_sig != null ? b.wave_period_sig.toFixed(1) : null;
            const waveDir = b.wave_direction_peak_cardinal ?? b.wave_direction_avg_cardinal ?? null;
            const waveDirectionValue = b.wave_direction_peak ?? b.wave_direction_avg;
            const waveDegrees =
              waveDirectionValue != null ? ` (${Math.round(waveDirectionValue)}°)` : "";
            if (waveHeight !== "—") {
              const dirDisplay = waveDir ? `${waveDir} ` : "";
              const arrowDisplay =
                waveDirectionValue != null
                  ? ` ${getDirectionalArrow(waveDirectionValue, "wave")}`
                  : "";
              const periodDisplay =
                wavePeriod != null ? ` @ ${wavePeriod}s${periodTag("dominant")}` : "";
              waveDisplay = `${dirDisplay}${waveHeight}m${periodDisplay}${waveDegrees}${arrowDisplay}`;
            }
          } else {
            // EC buoys - significant height + significant period. The two buoy families
            // publish sig period under different SWOB names (wave_period_sig /
            // wave_period_sig_basic), so coalesce. Fall back to avg/peak only for
            // non-EC stations, and tag any fallback explicitly.
            const waveHeight =
              b.wave_height_sig != null ? b.wave_height_sig.toFixed(heightPrecision) : "—";
            let wavePeriodValue = b.wave_period_sig ?? b.wave_period_sig_basic;
            let periodType = "sig";
            if (wavePeriodValue == null) {
              if (b.wave_period_avg != null) {
                wavePeriodValue = b.wave_period_avg;
                periodType = "avg";
              } else if (b.wave_period_peak != null) {
                wavePeriodValue = b.wave_period_peak;
                periodType = "peak";
              }
            }
            const wavePeriod = wavePeriodValue != null ? wavePeriodValue.toFixed(1) : null;
            const waveDir = b.wave_direction_peak_cardinal ?? b.swell_direction_cardinal ?? null;
            const waveDirectionValue = b.wave_direction_peak ?? b.swell_direction;
            const waveDegrees =
              waveDirectionValue != null ? ` (${Math.round(waveDirectionValue)}°)` : "";

            if (waveHeight !== "—") {
              const dirDisplay = waveDir ? `${waveDir} ` : "";
              const arrowDisplay =
                waveDirectionValue != null
                  ? ` ${getDirectionalArrow(waveDirectionValue, "wave")}`
                  : "";
              // Only tag the period when it isn't the significant value
              const periodSuffix = periodType === "sig" ? "" : periodTag(periodType);
              const periodDisplay = wavePeriod != null ? ` @ ${wavePeriod}s${periodSuffix}` : "";
              waveDisplay = `${dirDisplay}${waveHeight}m${periodDisplay}${waveDegrees}${arrowDisplay}`;
            }
          }
          cardContent += `<p class="buoy-metric" style="margin: 0.5rem 0;"><b>${waveLabel}</b> ${waveDisplay}</p>`;

          // NOAA reports a "dominant" period rather than a significant one; readers
          // won't know the term, so add a small footnote on these cards.
          if (id === "46088" || id === "46267") {
            cardContent += `<p style="margin: -0.25rem 0 0.5rem 0; font-size: 0.7em; color: var(--color-text-muted); line-height: 1.3;">Dominant = the wave period with the most energy (NOAA's term for peak period).</p>`;
          }
        } // End of if (isDown) else block

        cardContent += `</div>`; // End compact view

        // === EXPANDABLE BUTTONS ===
        cardContent += `
        <div style="display: flex; gap: 0.5rem; margin-top: 0.75rem;">
          <button class="toggle-details-btn" style="
            flex: 1;
            padding: 0.5rem;
            background: var(--color-card-muted-bg);
            border: 1px solid var(--color-card-muted-border);
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            color: var(--color-primary-dark);
            font-weight: 600;
            transition: background 0.2s;
          ">
            ▼ Show Details
          </button>
          <button class="toggle-history-btn" style="
            flex: 1;
            padding: 0.5rem;
            background: var(--color-card-muted-bg);
            border: 1px solid var(--color-card-muted-border);
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            color: var(--color-primary-dark);
            font-weight: 600;
            transition: background 0.2s;
          ">
            📈 Show History (12h)
          </button>
        </div>
      `;

        // === EXPANDABLE DETAILS SECTION (Hidden by default) ===
        cardContent += `<div id="card-details-${id}" style="display: none; margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid var(--color-border);">`;

        // Show warning banner if station is down
        if (isDown) {
          cardContent += `
          <div style="margin-bottom: 1rem; padding: 0.75rem; background: var(--color-callout-warning-bg); border-left: 4px solid var(--color-accent-orange); border-radius: 4px;">
            <p style="margin: 0; color: var(--color-warning-text); font-weight: 600;">⚠️ Station Down - Showing Last Known Data</p>
            <p style="margin: 0.25rem 0 0 0; font-size: 0.85em; color: var(--color-warning-text);">
              This data is from ${formatDataAge(ageMinutes)} and does not reflect current conditions.
            </p>
          </div>
        `;
        } else if (isStale) {
          cardContent += `
          <div style="margin-bottom: 1rem; padding: 0.75rem; background: var(--color-callout-warning-bg); border-left: 4px solid var(--color-accent-orange); border-radius: 4px;">
            <p style="margin: 0; color: var(--color-warning-text); font-weight: 600;">⚠️ Stale Data Warning</p>
            <p style="margin: 0.25rem 0 0 0; font-size: 0.85em; color: var(--color-warning-text);">
              This data is ${formatDataAge(ageMinutes)} old. Newer data may not be available.
            </p>
          </div>
        `;
        }

        // Check if NOAA buoy with spectral data
        const isNOAA = id === "46087" || id === "46088" || id === "46267";

        if (isNOAA) {
          // NOAA Spectral Wave Breakdown
          cardContent += `<p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">Detailed Wave Metrics</p>`;

          // Significant/Combined Wave Metrics
          cardContent += `
          <p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">📊 Significant Wave (Combined)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Sig Height:</b> ${b.wave_height_sig ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Dominant Period:</b> ${b.wave_period_sig ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Period:</b> ${b.wave_period_avg ?? "—"} s</p>
        `;

          // Spectral wave breakdown
          cardContent += `
          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">💨 Wind Waves (Local Chop)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Height:</b> ${b.wind_wave_height ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Period:</b> ${b.wind_wave_period ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Direction:</b> ${b.wind_wave_direction_cardinal ?? "—"} (${b.wind_wave_direction ?? "—"}°) ${getDirectionalArrow(b.wind_wave_direction, "wave")}</p>

          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">🌊 Ocean Swell (Long Period)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Height:</b> ${b.swell_height ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Period:</b> ${b.swell_period ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Direction:</b> ${b.swell_direction_cardinal ?? "—"} (${b.swell_direction ?? "—"}°) ${getDirectionalArrow(b.swell_direction, "wave")}</p>

          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">📈 Peak Metrics</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Direction:</b> ${b.wave_direction_peak_cardinal ?? "—"} (${b.wave_direction_peak ?? "—"}°) ${getDirectionalArrow(b.wave_direction_peak, "wave")}</p>
        `;
        } else {
          // EC Buoys and other stations - show only additional peak values not already displayed
          // Keep it simple: sig is the headline (compact card above); here we add
          // average + one "high-end" reading. EC buoy families report the high end
          // differently (average-max / peak / plain max), so pick whichever exists.
          let highHeight = null;
          let highHeightLabel = "";
          if (b.wave_height_max_avg != null) {
            highHeight = b.wave_height_max_avg;
            highHeightLabel = "Avg Max Height";
          } else if (b.wave_height_peak != null) {
            highHeight = b.wave_height_peak;
            highHeightLabel = "Peak Height";
          } else if (b.wave_height_max != null) {
            highHeight = b.wave_height_max;
            highHeightLabel = "Max Height";
          }

          let highPeriod = null;
          let highPeriodLabel = "";
          if (b.wave_period_peak != null) {
            highPeriod = b.wave_period_peak;
            highPeriodLabel = "Peak Period";
          } else if (b.wave_period_max_wave != null) {
            highPeriod = b.wave_period_max_wave;
            highPeriodLabel = "Period of Largest Wave";
          }

          const hasDetailData =
            b.wave_height_avg != null ||
            highHeight != null ||
            b.wave_period_avg != null ||
            highPeriod != null;

          if (hasDetailData) {
            cardContent += `<p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">📊 Additional Metrics</p>`;

            // --- Heights (sig is shown on the compact card above) ---
            if (b.wave_height_avg != null) {
              cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Height:</b> ${b.wave_height_avg.toFixed(heightPrecision)} m</p>`;
            }

            if (highHeight != null) {
              const sigHeight = b.wave_height_sig || 0;
              const ratio = sigHeight > 0 ? (highHeight / sigHeight).toFixed(1) : "";
              const ratioText = ratio
                ? ` <span style="color: var(--color-text-muted); font-size: 0.9em;">(${ratio}× sig)</span>`
                : "";
              cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;${highHeightLabel}:</b> ${highHeight.toFixed(heightPrecision)} m${ratioText}</p>`;
            }

            // --- Periods (sig is shown on the compact card above) ---
            if (b.wave_period_avg != null) {
              cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Period:</b> ${b.wave_period_avg.toFixed(1)} s</p>`;
            }

            if (highPeriod != null) {
              cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;${highPeriodLabel}:</b> ${highPeriod.toFixed(1)} s</p>`;
            }

            // Show wave direction angular spread
            if (b.wave_direction_spread_peak != null || b.wave_direction_spread_avg != null) {
              const peakSpread = b.wave_direction_spread_peak;
              const avgSpread = b.wave_direction_spread_avg;

              cardContent += `
              <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: var(--color-primary-dark);">
                🧭 Wave Direction Angular Spread
                <span class="spread-info-btn" style="cursor: pointer; font-size: 0.9em; margin-left: 0.3rem; color: var(--color-primary); user-select: none;" title="Click for explanation">ℹ️</span>
              </p>
            `;

              // Show Peak Spread (dominant frequency) with labels
              if (peakSpread != null) {
                let peakDesc = "";
                let peakColor = "var(--color-text-muted)";

                if (peakSpread < 25) {
                  peakDesc = "very organized";
                  peakColor = "var(--color-accent-green)";
                } else if (peakSpread < 35) {
                  peakDesc = "organized";
                  peakColor = "var(--color-accent-green)";
                } else if (peakSpread < 45) {
                  peakDesc = "moderate";
                  peakColor = "var(--color-accent-orange)";
                } else {
                  peakDesc = "confused";
                  peakColor = "var(--color-accent-red)";
                }

                cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Spread:</b> ${peakSpread}° <span style="color: ${peakColor}; font-weight: 600;">(${peakDesc})</span> <span style="font-size: 0.85em; color: var(--color-text-muted);">— dominant swell</span></p>`;
              }

              // Show Average Spread (all frequencies) with labels
              if (avgSpread != null) {
                let avgDesc = "";
                let avgColor = "var(--color-text-muted)";

                if (avgSpread < 30) {
                  avgDesc = "very clean";
                  avgColor = "var(--color-accent-green)";
                } else if (avgSpread < 45) {
                  avgDesc = "clean";
                  avgColor = "var(--color-accent-green)";
                } else if (avgSpread < 60) {
                  avgDesc = "mixed";
                  avgColor = "var(--color-accent-orange)";
                } else {
                  avgDesc = "messy";
                  avgColor = "var(--color-accent-red)";
                }

                cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Spread:</b> ${avgSpread}° <span style="color: ${avgColor}; font-weight: 600;">(${avgDesc})</span> <span style="font-size: 0.85em; color: var(--color-text-muted);">— all frequencies</span></p>`;
              }

              // Add visual angular spread vectors
              const peakDir = b.wave_direction_peak;
              const avgDir = b.wave_direction_avg;

              if (
                (peakDir != null && peakSpread != null) ||
                (avgDir != null && avgSpread != null)
              ) {
                cardContent += `<div style="margin-top: 0.75rem; padding: 0.75rem; background: var(--color-surface-alt); border-radius: 4px; border: 1px solid var(--color-border-light);">`;
                cardContent += `<p class="buoy-metric" style="font-weight: 600; color: var(--color-primary-dark); margin-bottom: 0.5rem;">Visual Direction & Spread</p>`;

                // Peak direction + spread vector
                if (peakDir != null && peakSpread != null) {
                  cardContent += `
                  <div style="margin: 0.5rem 0;">
                    <span style="font-size: 0.85em; color: var(--color-text-muted); font-weight: 600;">Peak:</span>
                    ${createAngularSpreadVector(peakDir, peakSpread, 70)}
                    <span style="font-size: 0.75em; color: var(--color-text-muted); margin-left: 0.5rem;">${b.wave_direction_peak_cardinal ?? degreesToCardinal(peakDir)} ${Math.round(peakDir)}° ± ${Math.round(peakSpread / 2)}°</span>
                  </div>
                `;
                }

                // Average direction + spread vector
                if (avgDir != null && avgSpread != null) {
                  cardContent += `
                  <div style="margin: 0.5rem 0;">
                    <span style="font-size: 0.85em; color: var(--color-text-muted); font-weight: 600;">Average:</span>
                    ${createAngularSpreadVector(avgDir, avgSpread, 70)}
                    <span style="font-size: 0.75em; color: var(--color-text-muted); margin-left: 0.5rem;">${b.wave_direction_avg_cardinal ?? degreesToCardinal(avgDir)} ${Math.round(avgDir)}° ± ${Math.round(avgSpread / 2)}°</span>
                  </div>
                `;
                }

                cardContent += `<p style="font-size: 0.75em; color: var(--color-text-light); margin-top: 0.5rem; margin-bottom: 0;">Arrows show wave travel direction. Sector shows angular spread.</p>`;
                cardContent += `</div>`;
              }

              // Add collapsible explanatory footnote (hidden by default)
              if (peakSpread != null && avgSpread != null) {
                cardContent += `
                <div id="spread-info-${id}" style="display: none; font-size: 0.85em; color: var(--color-text-light); margin-top: 0.5rem; padding: 0.75rem; background: var(--color-callout-info-bg); border-left: 3px solid var(--color-primary); border-radius: 4px; line-height: 1.5;">
                  <strong style="color: var(--color-tagline-text);">Angular Spread</strong> measures how organized the waves are:<br>
                  <br>
                  <strong>Lower numbers</strong> = waves coming from one direction (clean swell)<br>
                  <strong>Higher numbers</strong> = waves from multiple directions (choppy/messy)<br>
                  <br>
                  • <strong>Peak:</strong> The main swell direction<br>
                  • <strong>Average:</strong> Overall surface (includes wind chop)<br>
                  <br>
                  <span style="font-size: 0.9em; color: var(--color-text-muted);">Beach conditions may differ from open-ocean buoy readings.</span>
                </div>
              `;
              }
            }
          }
        }

        // Temperatures and pressure (all stations)
        const isSurrey = id === "CRPILE" || id === "CRCHAN";
        const seaTemp = b.sea_temp != null ? (isSurrey ? b.sea_temp.toFixed(1) : b.sea_temp) : "—";
        const airTemp = b.air_temp != null ? (isSurrey ? b.air_temp.toFixed(1) : b.air_temp) : "—";

        cardContent += `
        <p class="buoy-metric" style="margin-top: 0.75rem;"><b>🌡️ Sea:</b> ${seaTemp} °C | <b>Air:</b> ${airTemp} °C</p>
        <p class="buoy-metric"><b>⏱️ Pressure:</b> ${b.pressure ?? "—"} hPa</p>
      `;

        cardContent += `</div>`; // Close expandable details section

        // === EXPANDABLE HISTORY SECTION (Hidden by default) ===
        cardContent += `<div id="card-history-${id}" style="display: none; margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid var(--color-border);"></div>`;

        // === NAVIGATION LINKS ===
        const hasChartData = b.wave_height_sig != null || b.wind_speed != null;
        const chartButtonDisabled = !hasChartData
          ? 'disabled style="opacity: 0.5; cursor: not-allowed;"'
          : "";

        cardContent += `
        <div class="buoy-nav-links" style="display: flex; gap: 0.5rem; margin-top: 0.75rem;">
          <button class="buoy-nav-link" data-action="map" style="
            flex: 1;
            padding: 0.5rem;
            background: #004b7c;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            font-weight: 600;
            transition: background 0.2s;
          ">
            📍 View Location
          </button>
          <button class="buoy-nav-link" data-action="charts" ${chartButtonDisabled} style="
            flex: 1;
            padding: 0.5rem;
            background: #004b7c;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            font-weight: 600;
            transition: background 0.2s;
          ">
            📊 View Charts
          </button>
        </div>
      `;

        // Add source link at the bottom of the card
        if (sourceLinks[id]) {
          cardContent += `
          <p style="margin-top: 0.75rem; margin-bottom: 0; padding-top: 0.5rem; border-top: 1px solid var(--color-border); text-align: center;">
            <a href="${sourceLinks[id]}" target="_blank" rel="noopener noreferrer" style="
              font-size: 0.85em;
              color: var(--color-primary-dark);
              text-decoration: none;
              font-weight: 500;
            ">
              🔗 View Source Data
            </a>
          </p>
        `;
        }

        cardContent += `</div>`;
        setSafeHTML(card, cardContent);

        // Wire up event listeners (inline handlers stripped by DOMPurify)
        const detailsBtn = card.querySelector(".toggle-details-btn");
        if (detailsBtn) detailsBtn.addEventListener("click", () => toggleCardDetails(id));

        const historyBtn = card.querySelector(".toggle-history-btn");
        if (historyBtn) historyBtn.addEventListener("click", () => toggleCardHistory(id));

        const spreadBtn = card.querySelector(".spread-info-btn");
        if (spreadBtn) spreadBtn.addEventListener("click", () => toggleSpreadInfo(id));

        const mapBtn = card.querySelector('.buoy-nav-link[data-action="map"]');
        if (mapBtn) mapBtn.addEventListener("click", () => scrollToMap(id));

        const chartsBtn = card.querySelector('.buoy-nav-link[data-action="charts"]');
        if (chartsBtn) chartsBtn.addEventListener("click", () => scrollToCharts(id));

        cardsGrid.appendChild(card);
      }); // end stations forEach

      // Add grid to region group, then add region group to container
      regionGroup.appendChild(cardsGrid);
      container.appendChild(regionGroup);

      // Collapse Boundary Bay and Juan de Fuca by default (keep Strait of Georgia expanded)
      if (group.region !== "Strait of Georgia") {
        const toggleBtn = regionHeader.querySelector(".region-toggle-btn");
        if (toggleBtn && cardsGrid) {
          cardsGrid.style.display = "none";
          toggleBtn.textContent = "▶";
        }
      }
    }); // end buoyGroups forEach

    const now = new Date();
    if (timestamp) {
      timestamp.textContent = `Page refreshed at ${now.toLocaleString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false,
        timeZone: "America/Vancouver",
        timeZoneName: "short",
      })}`;
    }

    // Handle hash navigation after cards are loaded
    handleHashNavigation();
  } catch (err) {
    logger.error("BuoyData", "Error loading buoy data", err);
    setSafeHTML(
      container,
      `<p class="error">⚠️ Error loading buoy data. Please try again later.</p>`,
    );
  }
}

// Scroll to charts section and select buoy
function scrollToCharts(buoyId) {
  const buoySelector = document.getElementById("buoy-selector");
  const chartsSection = document.getElementById("charts-section");

  // Prefer scrolling to the selector dropdown if it exists
  const scrollTarget = buoySelector || chartsSection;
  if (!scrollTarget) return;

  // Smooth scroll to buoy selector
  scrollTarget.scrollIntoView({ behavior: "smooth", block: "start" });

  // Wait for scroll to complete, then trigger chart selection
  setTimeout(() => {
    const chartSelect = document.getElementById("chart-buoy-select");
    if (chartSelect) {
      chartSelect.value = buoyId;
      chartSelect.dispatchEvent(new Event("change"));

      // Add highlight pulse effect to the selector
      if (buoySelector) {
        buoySelector.classList.add("highlight-pulse");
        setTimeout(() => buoySelector.classList.remove("highlight-pulse"), 2000);
      }
    }
  }, 800);
}

// Scroll to map section and center on buoy
function scrollToMap(buoyId) {
  const mapSection = document.getElementById("map-section");
  if (!mapSection) return;

  // Smooth scroll to map section
  mapSection.scrollIntoView({ behavior: "smooth", block: "start" });

  // Wait for scroll to complete, then center map on buoy
  setTimeout(() => {
    if (typeof window.centerMapOnBuoy === "function") {
      window.centerMapOnBuoy(buoyId);

      // Add highlight pulse effect
      mapSection.classList.add("highlight-pulse");
      setTimeout(() => mapSection.classList.remove("highlight-pulse"), 2000);
    }
  }, 800);
}

// Toggle region collapse/expand
function toggleRegion(regionName) {
  const regionGroup = document.getElementById(`region-${regionName.replace(/\s+/g, "-")}`);
  if (!regionGroup) return;

  const cardsGrid = regionGroup.querySelector(".buoy-cards-grid");
  const toggleBtn = regionGroup.querySelector(".region-toggle-btn");

  if (cardsGrid && toggleBtn) {
    const isHidden = cardsGrid.style.display === "none";
    cardsGrid.style.display = isHidden ? "grid" : "none";
    toggleBtn.textContent = isHidden ? "▼" : "▶";

    // Save state to localStorage
    const regionKey = `region-${regionName}-collapsed`;
    localStorage.setItem(regionKey, isHidden ? "false" : "true");
  }
}

// Toggle card details (full metrics)
function toggleCardDetails(buoyId) {
  const detailsDiv = document.getElementById(`card-details-${buoyId}`);
  const button = document.querySelector(`#buoy-${buoyId} .toggle-details-btn`);

  if (detailsDiv && button) {
    const isHidden = detailsDiv.style.display === "none";
    detailsDiv.style.display = isHidden ? "block" : "none";
    button.textContent = isHidden ? "▲ Hide Details" : "▼ Show Details";
  }
}

// Toggle angular spread explanation
function toggleSpreadInfo(buoyId) {
  const infoDiv = document.getElementById(`spread-info-${buoyId}`);
  if (infoDiv) {
    const isHidden = infoDiv.style.display === "none";
    infoDiv.style.display = isHidden ? "block" : "none";
  }
}

// Toggle card history table
async function toggleCardHistory(buoyId) {
  const historyDiv = document.getElementById(`card-history-${buoyId}`);
  const button = document.querySelector(`#buoy-${buoyId} .toggle-history-btn`);

  if (!historyDiv || !button) return;

  const isHidden = historyDiv.style.display === "none";

  if (isHidden) {
    // Auto-collapse Details section when opening History
    const detailsDiv = document.getElementById(`card-details-${buoyId}`);
    const detailsButton = document.querySelector(`#buoy-${buoyId} .toggle-details-btn`);
    if (detailsDiv && detailsDiv.style.display !== "none") {
      detailsDiv.style.display = "none";
      if (detailsButton) detailsButton.textContent = "▼ Show Details";
    }

    // Load and display history
    button.textContent = "Loading...";
    button.disabled = true;

    try {
      const timeseriesData = await fetchWithTimeout(
        `/data/buoy_timeseries_48h.json?t=${Date.now()}`,
      );
      const buoyData = timeseriesData[buoyId];

      if (buoyData && buoyData.timeseries) {
        setSafeHTML(historyDiv, renderHistoryTable(buoyId, buoyData.timeseries));
        const hideBtn = historyDiv.querySelector(".hide-history-btn");
        if (hideBtn) hideBtn.addEventListener("click", () => toggleCardHistory(buoyId));
        historyDiv.style.display = "block";
        button.textContent = "▲ Hide History";
        button.disabled = false;
      } else {
        setSafeHTML(
          historyDiv,
          '<p style="color: var(--color-text-light); text-align: center; padding: 1rem;">No historical data available</p>',
        );
        historyDiv.style.display = "block";
        button.textContent = "▲ Hide History";
        button.disabled = false;
      }
    } catch (error) {
      logger.error("BuoyData", "Error loading history", error);
      setSafeHTML(
        historyDiv,
        '<p style="color: var(--color-error-text); text-align: center; padding: 1rem;">Error loading historical data</p>',
      );
      historyDiv.style.display = "block";
      button.textContent = "▲ Hide History";
      button.disabled = false;
    }
  } else {
    historyDiv.style.display = "none";
    button.textContent = "▼ Show History (12h)";
  }
}

// Render history table
function renderHistoryTable(buoyId, timeseries) {
  // Get the most recent 12 hourly observations
  const windSpeed = timeseries.wind_speed?.data || [];
  const windDir = timeseries.wind_direction?.data || [];
  const windGust = timeseries.wind_gust?.data || [];

  // Height is always significant (Neah Bay shows swell).
  const isNeahBay = buoyId === "46087";
  const isNOAA = buoyId === "46088" || buoyId === "46267";
  const waveHeight = isNeahBay
    ? timeseries.swell_height?.data || []
    : timeseries.wave_height_sig?.data || [];

  // Period source, in priority order per buoy type:
  //  - Neah Bay (NOAA): swell period
  //  - NOAA (Dungeness, Angeles Pt): dominant period (DPD, stored in wave_period_sig)
  //  - EC: significant period (two SWOB names), falling back to avg then peak
  //    for non-EC stations (e.g. Crescent on the radar sensor)
  let wavePeriodSources;
  if (isNeahBay) {
    wavePeriodSources = [timeseries.swell_period?.data || []];
  } else if (isNOAA) {
    wavePeriodSources = [timeseries.wave_period_sig?.data || []];
  } else {
    wavePeriodSources = [
      timeseries.wave_period_sig?.data || [],
      timeseries.wave_period_sig_basic?.data || [],
      timeseries.wave_period_avg?.data || [],
      timeseries.wave_period_peak?.data || [],
    ];
  }

  const airTemp = timeseries.air_temp?.data || [];
  const seaTemp = timeseries.sea_temp?.data || [];

  logger.debug(
    "History",
    `${buoyId}: windSpeed=${windSpeed.length}, waveHeight=${waveHeight.length} points`,
  );

  // Build the time axis from wave data when present, otherwise fall back to
  // wind (and then temperature) so the table still renders when a buoy's wave
  // sensor is offline.
  const timeSource = waveHeight.length
    ? waveHeight
    : windSpeed.length
      ? windSpeed
      : seaTemp.length
        ? seaTemp
        : airTemp;
  let allTimes = timeSource.map((d) => d.time);

  // For Crescent stations, filter to hourly intervals only (on the hour)
  const isCrescentStation = buoyId === "CRPILE" || buoyId === "CRCHAN";
  if (isCrescentStation) {
    allTimes = allTimes.filter((time) => {
      const date = new Date(time);
      return date.getMinutes() === 0; // Only include times on the hour
    });
  }

  // Get last 12 hours of wave data
  const now = new Date();
  const twelveHoursAgo = new Date(now - 12 * 60 * 60 * 1000);
  const times = allTimes
    .filter((time) => new Date(time) >= twelveHoursAgo)
    .sort()
    .reverse();

  logger.debug(
    "History",
    `${buoyId}: Showing ${times.length} rows (all wave data, wind when available)`,
  );

  logger.debug("History", `${buoyId}: Generated ${times.length} time entries for table`);

  // Responsive scroll indicator - only show on mobile, positioned OUTSIDE table
  const scrollIndicator =
    window.innerWidth < 768
      ? `<div style="text-align: center; margin-bottom: 0.25rem; padding: 0.25rem 0.5rem; background: var(--color-active-indicator-bg); font-size: 0.65rem; color: var(--color-primary-dark); border-radius: 4px;">← Scroll table horizontally →</div>`
      : "";

  let tableHTML = `
    ${scrollIndicator}
    <div style="overflow-x: auto; margin-top: 0.5rem; width: 100%; max-width: 100%; -webkit-overflow-scrolling: touch; border: 1px solid var(--color-border); border-radius: 4px; box-sizing: border-box;">
      <table style="border-collapse: collapse; font-size: 0.8rem; width: max-content; min-width: 100%; table-layout: auto;">
        <thead>
          <tr style="background: var(--color-surface-alt);">
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border); border-right: 1px solid var(--color-border); text-align: left; white-space: nowrap; min-width: 55px;">Time</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border); border-right: 1px solid var(--color-border); text-align: center; white-space: nowrap; min-width: 95px;">Wind [kn]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border); border-right: 1px solid var(--color-border); text-align: center; white-space: nowrap; min-width: 50px;">Wave Ht [m]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border); border-right: 1px solid var(--color-border); text-align: center; white-space: nowrap; min-width: 60px;">Period [s]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border); border-right: 1px solid var(--color-border); text-align: center; white-space: nowrap; min-width: 55px;">Sea [°C]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border); text-align: center; white-space: nowrap; min-width: 55px;">Air [°C]</th>
          </tr>
        </thead>
        <tbody>
  `;

  // Determine wave height precision based on buoy type
  const isBoundaryBay = buoyId === "CRPILE" || buoyId === "CRCHAN";
  const waveHeightDecimals = isBoundaryBay ? 2 : 1;

  // Track previous date for conditional date display
  let previousDate = null;

  times.forEach((time, index) => {
    const windSpeedVal = windSpeed.find((d) => d.time === time)?.value;
    const windDirVal = windDir.find((d) => d.time === time)?.value;
    const windGustVal = windGust.find((d) => d.time === time)?.value;
    const waveHeightVal = waveHeight.find((d) => d.time === time)?.value;
    let wavePeriodVal;
    for (const src of wavePeriodSources) {
      const v = src.find((d) => d.time === time)?.value;
      if (v != null) {
        wavePeriodVal = v;
        break;
      }
    }
    const airTempVal = airTemp.find((d) => d.time === time)?.value;
    const seaTempVal = seaTemp.find((d) => d.time === time)?.value;

    const dateObj = new Date(time);

    // Format: "Mo-11 08h10" (2-letter weekday, day, hour, minutes if not :00)
    const dayOfWeek = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"][dateObj.getDay()];
    const dayOfMonth = dateObj.toLocaleString("en-US", {
      day: "numeric",
      timeZone: "America/Vancouver",
    });
    const hour = dateObj.toLocaleString("en-US", {
      hour: "2-digit",
      hour12: false,
      timeZone: "America/Vancouver",
    });
    const minute = dateObj.toLocaleString("en-US", {
      minute: "2-digit",
      timeZone: "America/Vancouver",
    });

    // Only show date prefix if it changed from previous row
    const currentDate = `${dayOfWeek}-${dayOfMonth}`;
    const minuteSuffix = minute !== "00" ? minute : "";
    let timeStr;
    if (currentDate !== previousDate) {
      // New date: show date on first line, hour+minutes on second line
      timeStr = `${currentDate}<br/>${hour}h${minuteSuffix}`;
      previousDate = currentDate;
    } else {
      // Same date: just show hour+minutes
      timeStr = `${hour}h${minuteSuffix}`;
    }

    // Format wind with cardinal direction and gust: "WNW 10 gust 15"
    let windDisplay = "—";
    if (windSpeedVal != null) {
      const cardinal = degreesToCardinal(windDirVal);
      const cardinalStr = cardinal ? `${cardinal} ` : "";
      const gustStr = windGustVal != null ? ` gust ${Math.round(windGustVal)}` : "";
      windDisplay = `${cardinalStr}${Math.round(windSpeedVal)}${gustStr}`;
    }

    // Alternating row background color
    const rowBg = index % 2 === 0 ? "background: rgba(0, 75, 124, 0.03);" : "";

    tableHTML += `
      <tr style="${rowBg}">
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border-light); border-right: 1px solid var(--color-border-light); white-space: nowrap;">${timeStr}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border-light); border-right: 1px solid var(--color-border-light); text-align: center; white-space: nowrap;">${windDisplay}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border-light); border-right: 1px solid var(--color-border-light); text-align: center;">${waveHeightVal != null ? waveHeightVal.toFixed(waveHeightDecimals) : "—"}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border-light); border-right: 1px solid var(--color-border-light); text-align: center;">${wavePeriodVal != null ? wavePeriodVal.toFixed(1) : "—"}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border-light); border-right: 1px solid var(--color-border-light); text-align: center;">${seaTempVal != null ? seaTempVal.toFixed(1) : "—"}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid var(--color-border-light); text-align: center;">${airTempVal != null ? airTempVal.toFixed(1) : "—"}</td>
      </tr>
    `;
  });

  tableHTML += `
        </tbody>
      </table>
    </div>
  `;

  // Add duplicate Hide button at bottom of history table
  tableHTML += `
    <div style="text-align: center; margin-top: 0.75rem;">
      <button class="hide-history-btn" style="padding: 0.5rem 1rem; background: var(--color-primary-dark); color: var(--color-on-primary); border: none; border-radius: 4px; cursor: pointer; font-size: 0.85rem; transition: background 0.2s;">
        ▲ Hide History
      </button>
    </div>
  `;

  // Add note for Neah Bay explaining swell data
  if (isNeahBay) {
    tableHTML += `
      <div style="margin-top: 0.5rem; padding: 0.5rem; background: var(--color-callout-info-bg); border-left: 3px solid var(--color-source-noaa-border); font-size: 0.75rem; color: var(--color-text-light); line-height: 1.4;">
        <strong>Note:</strong> Neah Bay displays <strong>swell data</strong> (long-period ocean waves from distant storms) rather than combined wave metrics. Wind waves are typically much smaller at this location.
      </div>
    `;
  } else if (isNOAA) {
    tableHTML += `
      <div style="margin-top: 0.5rem; padding: 0.5rem; background: var(--color-callout-info-bg); border-left: 3px solid var(--color-source-noaa-border); font-size: 0.75rem; color: var(--color-text-light); line-height: 1.4;">
        <strong>Note:</strong> Height is significant wave height; period is the <strong>dominant period</strong> — NOAA's term for the wave period carrying the most energy (equivalent to peak period).
      </div>
    `;
  }

  logger.debug("History", `${buoyId}: Rendered table with ${times.length} rows`);
  return tableHTML;
}

// Handle hash navigation from map (e.g., /#buoy-46087)
function handleHashNavigation() {
  const hash = window.location.hash;
  if (hash && hash.startsWith("#buoy-")) {
    const buoyId = hash.replace("#buoy-", "");
    const buoyCard = document.getElementById(`buoy-${buoyId}`);

    if (buoyCard) {
      // Find the parent region
      const regionGroup = buoyCard.closest(".region-group");
      if (regionGroup) {
        const cardsGrid = regionGroup.querySelector(".buoy-cards-grid");
        const toggleBtn = regionGroup.querySelector(".region-toggle-btn");

        // Expand region if collapsed
        if (cardsGrid && cardsGrid.style.display === "none") {
          cardsGrid.style.display = "grid";
          if (toggleBtn) toggleBtn.textContent = "▼";
        }
      }

      // Scroll to the card after a short delay to ensure rendering
      setTimeout(() => {
        buoyCard.scrollIntoView({ behavior: "smooth", block: "center" });
        buoyCard.classList.add("highlight-pulse");
        setTimeout(() => buoyCard.classList.remove("highlight-pulse"), 2000);
      }, 300);
    }
  }
}

// Wait for HTMX to load footer (which contains timestamp element) before initializing
document.addEventListener(
  "htmx:load",
  function () {
    loadBuoyData();
  },
  { once: true },
);

setInterval(loadBuoyData, 5 * 60 * 1000);

// Handle hash navigation when hash changes (clicking map links)
window.addEventListener("hashchange", handleHashNavigation);

// Auto-open site intro on desktop (replaces inline script removed for CSP compliance)
(function () {
  var d = document.querySelector(".site-intro-details");
  if (d && window.innerWidth > 600) d.open = true;
})();

// Event listeners replacing onclick= attributes (CSP compliance)
document.addEventListener("click", function (e) {
  var btn = e.target.closest(".time-range-btn");
  if (btn) {
    var hours = parseInt(btn.dataset.hours, 10);
    if (hours && typeof setTimeRange === "function") setTimeRange(hours);
  }
});

var applyBtn = document.getElementById("apply-wave-threshold-btn");
if (applyBtn)
  applyBtn.addEventListener("click", function () {
    applyWaveThreshold();
  });

var clearBtn = document.getElementById("clear-wave-threshold-btn");
if (clearBtn)
  clearBtn.addEventListener("click", function () {
    clearWaveThreshold();
  });

var buoyMapBtn = document.getElementById("show-buoy-on-map-btn");
if (buoyMapBtn)
  buoyMapBtn.addEventListener("click", function (e) {
    showSelectedBuoyOnMap(e);
  });

var surgeMapBtn = document.getElementById("show-surge-on-map-btn");
if (surgeMapBtn)
  surgeMapBtn.addEventListener("click", function (e) {
    showSelectedSurgeOnMap(e);
  });
