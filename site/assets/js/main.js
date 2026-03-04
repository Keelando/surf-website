// Helper function to convert degrees to cardinal direction
function degreesToCardinal(degrees) {
  if (degrees == null) return null;
  const directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}

// Helper function to format data age in human-readable format
function formatDataAge(ageMinutes) {
  if (ageMinutes == null) return null;

  if (ageMinutes < 60) {
    const mins = Math.round(ageMinutes);
    return `${mins} minute${mins !== 1 ? 's' : ''} ago`;
  } else if (ageMinutes < 1440) {
    // Less than 24 hours
    const hours = Math.round(ageMinutes / 60);
    return `${hours} hour${hours !== 1 ? 's' : ''} ago`;
  } else {
    // Days
    const days = Math.round(ageMinutes / 1440);
    return `${days} day${days !== 1 ? 's' : ''} ago`;
  }
}

// Helper function to create rotated directional arrow
// SVG approach - bulletproof across ALL browsers/devices (fixes Firefox Android tablet bug)
function getDirectionalArrow(degrees, arrowType = 'wind') {
  if (degrees == null || degrees === '—') return '';

  // Meteorological convention: direction indicates WHERE wind/waves are COMING FROM
  const rotation = arrowType === 'wind' ? degrees : degrees + 90;

  // SVG arrows: wind points down, wave points right
  const svg = arrowType === 'wind'
    ? `<svg width="16" height="16" viewBox="0 0 16 16"><path d="M8 2v12m0 0l-3-3m3 3l3-3" stroke="#004b7c" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`
    : `<svg width="16" height="16" viewBox="0 0 16 16"><path d="M2 8h12m0 0l-3-3m3 3l-3 3" stroke="#004b7c" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`;

  return `<span style="display:inline-block;transform:rotate(${rotation}deg);margin-left:0.3rem;vertical-align:middle;">${svg}</span>`;
}

// Helper function to create angular spread vector visualization
// Shows main direction arrow with smaller arrows indicating directional spread
function createAngularSpreadVector(avgDirection, spread, size = 60) {
  if (avgDirection == null || spread == null) return '';

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
  const startAngleSVG = (minRot + 180) - 90;
  const endAngleSVG = (maxRot + 180) - 90;
  const startAngleRad = startAngleSVG * Math.PI / 180;
  const endAngleRad = endAngleSVG * Math.PI / 180;
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
      ]
    },
    {
      region: "Boundary Bay",
      stations: [
        "CRPILE",  // Crescent Beach Ocean
        "CRCHAN",  // Crescent Channel
      ]
    },
    {
      region: "Juan de Fuca Strait",
      stations: [
        "46087",   // Neah Bay
        "46088",   // New Dungeness
        "46267",   // Angeles Point
      ]
    },
    {
      region: "West Coast Vancouver Island",
      stations: [
        "4600206", // La Perouse Bank
      ]
    }
  ];
  // COLEB excluded - wind-only station, available in charts only

  // Source links for each buoy
  const sourceLinks = {
    "4600146": "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46146",
    "4600304": "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46304",
    "4600303": "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=02&siteID=14305&stationID=46303",
    "4600131": "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46131",
    "4600206": "https://weather.gc.ca/marine/weatherConditions-currentConditions_e.html?mapID=03&siteID=06400&stationID=46206",
    "46087": "https://www.ndbc.noaa.gov/station_page.php?station=46087",
    "46088": "https://www.ndbc.noaa.gov/station_page.php?station=46088",
    "46267": "https://www.ndbc.noaa.gov/station_page.php?station=46267",
    "CRPILE": "https://developers.flowworks.com/",
    "CRCHAN": "https://developers.flowworks.com/",
    "COLEB": "https://developers.flowworks.com/"
  };

  try {
    const data = await fetchWithTimeout(`/data/latest_buoy_v2.json?t=${Date.now()}`);

    container.innerHTML = "";

    // Find most recent observation time
    let mostRecentTime = null;
    Object.values(data).forEach(buoy => {
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
      updateHeader.textContent = `Last Updated: ${mostRecentTime.toLocaleString("en-US", {
        month: "numeric",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
        timeZone: "America/Vancouver"
      }).replace(',', '')}`;
      container.appendChild(updateHeader);
    }

    // Render buoys grouped by region
    buoyGroups.forEach(group => {
      // Create region group container
      const regionGroup = document.createElement("div");
      regionGroup.className = "region-group";

      // Add region header (clickable to collapse/expand)
      const regionHeader = document.createElement("div");
      regionHeader.className = "region-header";
      regionHeader.style.cursor = "pointer";
      regionHeader.style.userSelect = "none";
      regionHeader.innerHTML = `<span class="region-toggle-btn">▼</span> ${group.region} <span style="font-size: 0.8em; font-weight: normal; opacity: 0.8;">(${group.stations.length} stations)</span>`;
      regionHeader.onclick = () => toggleRegion(group.region);
      regionGroup.appendChild(regionHeader);
      regionGroup.id = `region-${group.region.replace(/\s+/g, '-')}`;

      // Create grid container for this region's cards
      const cardsGrid = document.createElement("div");
      cardsGrid.className = "buoy-cards-grid";

      // Render stations in this region
      group.stations.forEach(id => {
        const b = data[id];
        if (!b) return;

        const card = document.createElement("div");
        card.className = "buoy-card";
        card.id = `buoy-${id}`; // Add ID for anchor linking from map

        // Special styling for NOAA buoys
        if (id === "46087" || id === "46088" || id === "46267") {
          card.style.borderLeft = "4px solid #003087";
        }

        // Special styling for Surrey FlowWorks stations
        if (id === "CRPILE" || id === "CRCHAN" || id === "COLEB") {
          card.style.borderLeft = "4px solid #006837";
        }

      // Format timestamp in Pacific Time (24-hour, shorter format: "11/11 19:58")
      const updated = b.observation_time
        ? new Date(b.observation_time).toLocaleString("en-US", {
            month: "numeric",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
            timeZone: "America/Vancouver",
          }).replace(',', '')
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
        ageWarning = ` <span style="color: #e53935; font-weight: bold; background: #ffebee; padding: 0.2rem 0.5rem; border-radius: 3px;">🔴 STATION DOWN (${formatDataAge(ageMinutes)})</span>`;
      } else if (isStale) {
        ageWarning = ` <span style="color: #c62828; font-weight: bold;">⚠️ STALE (${formatDataAge(ageMinutes)})</span>`;
      }

      // Round wind speeds to integers
      const windSpeed = b.wind_speed != null ? Math.round(b.wind_speed) : "—";
      const windGust = b.wind_gust != null ? Math.round(b.wind_gust) : "—";

      // Build the card content based on buoy type
      let cardContent = `<h2>${b.name || id}`;

      // Add source badge
      if (id === "46087" || id === "46088" || id === "46267") {
        cardContent += ` <span style="font-size: 0.7em; color: #003087; font-weight: normal;">🇺🇸 NOAA</span>`;
      } else if (id === "CRPILE" || id === "CRCHAN" || id === "COLEB") {
        cardContent += ` <span style="font-size: 0.7em; color: #006837; font-weight: normal;">🏛️ Surrey (FlowWorks)</span>`;
      } else {
        cardContent += ` <span style="font-size: 0.7em; color: #006400; font-weight: normal;">🇨🇦 Env Canada</span>`;
      }
      
      cardContent += `</h2>`;
      cardContent += `<p style="font-size: 0.9em; color: #666; margin-top: -0.5rem;">Last Update: ${updated}${ageWarning}</p>`;

      // === CONDENSED VIEW (Always visible) ===
      cardContent += `<div class="card-compact-view">`;

      // Determine decimal precision for wave height (Boundary Bay stations use 2 decimals)
      // Declared here so it's available in both compact view and details section
      const isBoundaryBay = (id === "CRPILE" || id === "CRCHAN");
      const heightPrecision = isBoundaryBay ? 2 : 1;

      // If station is down (>12 hours), show station down message instead of data
      if (isDown) {
        cardContent += `
          <p class="buoy-metric" style="margin: 1rem 0; padding: 1rem; background: #ffebee; border-left: 4px solid #e53935; border-radius: 4px; color: #c62828; font-weight: 600;">
            🔴 Station Down - No recent data available
          </p>
          <p style="font-size: 0.85em; color: #666; text-align: center; margin-top: 0.5rem;">
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
          windDisplay = `${windCardinal} ${windSpeed}${gustPart} kn${windDegrees} ${getDirectionalArrow(windDir, 'wind')}`;
        }
        cardContent += `<p class="buoy-metric" style="margin: 0.5rem 0;"><b>💨 Wind:</b> ${windDisplay}</p>`;

        // Compact Wave Line - Format: "W 0.4m @ 3.3s (270°)"
        // For Neah Bay (46087), prioritize swell data as it measures continuous open swells
        let waveDisplay = "No data";
        let waveLabel = "🌊 Wave:";

        if (id === "46087") {
          // Neah Bay - show swell info
          waveLabel = "🌊 Swell:";
          const swellHeight = b.swell_height != null ? b.swell_height.toFixed(heightPrecision) : "—";
          const swellPeriod = b.swell_period != null ? b.swell_period.toFixed(1) : null;
          const swellDir = b.swell_direction_cardinal ?? null;
          const swellDegrees = b.swell_direction != null ? ` (${Math.round(b.swell_direction)}°)` : "";
          if (swellHeight !== "—") {
            const dirDisplay = swellDir ? `${swellDir} ` : "";
            const arrowDisplay = b.swell_direction != null ? ` ${getDirectionalArrow(b.swell_direction, 'wave')}` : "";
            const periodDisplay = swellPeriod != null ? ` @ ${swellPeriod}s` : "";
            waveDisplay = `${dirDisplay}${swellHeight}m${periodDisplay}${swellDegrees}${arrowDisplay}`;
          }
        } else if (id === "46088") {
          // New Dungeness - show significant wave height and average period
          const waveHeight = b.wave_height_sig != null ? b.wave_height_sig.toFixed(heightPrecision) : "—";
          const wavePeriod = b.wave_period_avg != null ? b.wave_period_avg.toFixed(1) : null;
          const waveDir = b.wave_direction_peak_cardinal ?? null;
          const waveDegrees = b.wave_direction_peak != null ? ` (${Math.round(b.wave_direction_peak)}°)` : "";
          if (waveHeight !== "—") {
            const dirDisplay = waveDir ? `${waveDir} ` : "";
            const arrowDisplay = b.wave_direction_peak != null ? ` ${getDirectionalArrow(b.wave_direction_peak, 'wave')}` : "";
            const periodDisplay = wavePeriod != null ? ` @ ${wavePeriod}s` : "";
            waveDisplay = `${dirDisplay}${waveHeight}m${periodDisplay}${waveDegrees}${arrowDisplay}`;
          }
        } else {
          // Other buoys - show combined wave data
          const waveHeight = b.wave_height_sig != null ? b.wave_height_sig.toFixed(heightPrecision) : "—";
          const wavePeriod = b.wave_period_avg != null ? b.wave_period_avg.toFixed(1) : b.wave_period_peak != null ? b.wave_period_peak.toFixed(1) : null;
          const waveDir = b.wave_direction_peak_cardinal ?? b.swell_direction_cardinal ?? null;
          const waveDirectionValue = b.wave_direction_peak ?? b.swell_direction;
          const waveDegrees = waveDirectionValue != null ? ` (${Math.round(waveDirectionValue)}°)` : "";

          if (waveHeight !== "—") {
            const dirDisplay = waveDir ? `${waveDir} ` : "";
            const arrowDisplay = waveDirectionValue != null ? ` ${getDirectionalArrow(waveDirectionValue, 'wave')}` : "";
            const periodDisplay = wavePeriod != null ? ` @ ${wavePeriod}s` : "";
            waveDisplay = `${dirDisplay}${waveHeight}m${periodDisplay}${waveDegrees}${arrowDisplay}`;
          }
        }
        cardContent += `<p class="buoy-metric" style="margin: 0.5rem 0;"><b>${waveLabel}</b> ${waveDisplay}</p>`;
      } // End of if (isDown) else block

      cardContent += `</div>`; // End compact view

      // === EXPANDABLE BUTTONS ===
      cardContent += `
        <div style="display: flex; gap: 0.5rem; margin-top: 0.75rem;">
          <button class="toggle-details-btn" onclick="toggleCardDetails('${id}')" style="
            flex: 1;
            padding: 0.5rem;
            background: #f0f4f8;
            border: 1px solid #d0d7de;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            color: #004b7c;
            font-weight: 600;
            transition: background 0.2s;
          " onmouseover="this.style.background='#e1e8ed'" onmouseout="this.style.background='#f0f4f8'">
            ▼ Show Details
          </button>
          <button class="toggle-history-btn" onclick="toggleCardHistory('${id}')" style="
            flex: 1;
            padding: 0.5rem;
            background: #f0f4f8;
            border: 1px solid #d0d7de;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.85em;
            color: #004b7c;
            font-weight: 600;
            transition: background 0.2s;
          " onmouseover="this.style.background='#e1e8ed'" onmouseout="this.style.background='#f0f4f8'">
            📈 Show History (12h)
          </button>
        </div>
      `;

      // === EXPANDABLE DETAILS SECTION (Hidden by default) ===
      cardContent += `<div id="card-details-${id}" style="display: none; margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid #e0e0e0;">`;

      // Show warning banner if station is down
      if (isDown) {
        cardContent += `
          <div style="margin-bottom: 1rem; padding: 0.75rem; background: #fff3cd; border-left: 4px solid #ff9800; border-radius: 4px;">
            <p style="margin: 0; color: #856404; font-weight: 600;">⚠️ Station Down - Showing Last Known Data</p>
            <p style="margin: 0.25rem 0 0 0; font-size: 0.85em; color: #856404;">
              This data is from ${formatDataAge(ageMinutes)} and does not reflect current conditions.
            </p>
          </div>
        `;
      } else if (isStale) {
        cardContent += `
          <div style="margin-bottom: 1rem; padding: 0.75rem; background: #fff3cd; border-left: 4px solid #ffa726; border-radius: 4px;">
            <p style="margin: 0; color: #856404; font-weight: 600;">⚠️ Stale Data Warning</p>
            <p style="margin: 0.25rem 0 0 0; font-size: 0.85em; color: #856404;">
              This data is ${formatDataAge(ageMinutes)} old. Newer data may not be available.
            </p>
          </div>
        `;
      }

      // Check if NOAA buoy with spectral data
      const isNOAA = (id === "46087" || id === "46088" || id === "46267");

      if (isNOAA) {
        // NOAA Spectral Wave Breakdown
        cardContent += `<p class="buoy-metric" style="font-weight: 600; color: #004b7c; margin-bottom: 0.5rem;">Detailed Wave Metrics</p>`;

        // Significant/Combined Wave Metrics
        cardContent += `
          <p class="buoy-metric" style="font-weight: 600; color: #004b7c; margin-bottom: 0.5rem;">📊 Significant Wave (Combined)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Sig Height:</b> ${b.wave_height_sig ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Avg Period:</b> ${b.wave_period_avg ?? "—"} s</p>
        `;

        // Spectral wave breakdown
        cardContent += `
          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: #004b7c; margin-bottom: 0.5rem;">💨 Wind Waves (Local Chop)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Height:</b> ${b.wind_wave_height ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Period:</b> ${b.wind_wave_period ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Direction:</b> ${b.wind_wave_direction_cardinal ?? "—"} (${b.wind_wave_direction ?? "—"}°) ${getDirectionalArrow(b.wind_wave_direction, 'wave')}</p>

          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: #004b7c; margin-bottom: 0.5rem;">🌊 Ocean Swell (Long Period)</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Height:</b> ${b.swell_height ?? "—"} m</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Period:</b> ${b.swell_period ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Direction:</b> ${b.swell_direction_cardinal ?? "—"} (${b.swell_direction ?? "—"}°) ${getDirectionalArrow(b.swell_direction, 'wave')}</p>

          <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: #004b7c; margin-bottom: 0.5rem;">📈 Peak Metrics</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Period:</b> ${b.wave_period_peak ?? "—"} s</p>
          <p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Direction:</b> ${b.wave_direction_peak_cardinal ?? "—"} (${b.wave_direction_peak ?? "—"}°) ${getDirectionalArrow(b.wave_direction_peak, 'wave')}</p>
        `;
      } else {
        // EC Buoys and other stations - show only additional peak values not already displayed
        const hasPeakData = b.wave_period_peak != null || b.wave_height_peak != null || b.wave_height_max != null;

        if (hasPeakData) {
          cardContent += `<p class="buoy-metric" style="font-weight: 600; color: #004b7c; margin-bottom: 0.5rem;">📊 Additional Metrics</p>`;

          // Show peak wave height (English Bay, Southern Strait)
          if (b.wave_height_peak != null) {
            cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Wave Height:</b> ${b.wave_height_peak.toFixed(heightPrecision)} m</p>`;
          }

          // Show maximum wave height (Halibut Bank, Sentry Shoal)
          if (b.wave_height_max != null) {
            const sigHeight = b.wave_height_sig || 0;
            const ratio = sigHeight > 0 ? (b.wave_height_max / sigHeight).toFixed(1) : '';
            const ratioText = ratio ? ` <span style="color: #666; font-size: 0.9em;">(${ratio}× sig)</span>` : '';
            cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Max Wave Height:</b> ${b.wave_height_max.toFixed(heightPrecision)} m${ratioText}</p>`;
          }

          // Show peak period (right after peak/max wave height)
          if (b.wave_period_peak != null) {
            cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Period:</b> ${b.wave_period_peak.toFixed(1)} s</p>`;
          }

          // Show wave direction angular spread
          if (b.wave_direction_spread_peak != null || b.wave_direction_spread_avg != null) {
            const peakSpread = b.wave_direction_spread_peak;
            const avgSpread = b.wave_direction_spread_avg;

            cardContent += `
              <p class="buoy-metric" style="margin-top: 0.75rem; font-weight: 600; color: #004b7c;">
                🧭 Wave Direction Angular Spread
                <span onclick="toggleSpreadInfo('${id}')" style="cursor: pointer; font-size: 0.9em; margin-left: 0.3rem; color: #0077be; user-select: none;" title="Click for explanation">ℹ️</span>
              </p>
            `;

            // Show Peak Spread (dominant frequency) with labels
            if (peakSpread != null) {
              let peakDesc = '';
              let peakColor = '#666';

              if (peakSpread < 25) {
                peakDesc = 'very organized';
                peakColor = '#38a169';
              } else if (peakSpread < 35) {
                peakDesc = 'organized';
                peakColor = '#48bb78';
              } else if (peakSpread < 45) {
                peakDesc = 'moderate';
                peakColor = '#d69e2e';
              } else {
                peakDesc = 'confused';
                peakColor = '#e53e3e';
              }

              cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Peak Spread:</b> ${peakSpread}° <span style="color: ${peakColor}; font-weight: 600;">(${peakDesc})</span> <span style="font-size: 0.85em; color: #666;">— dominant swell</span></p>`;
            }

            // Show Average Spread (all frequencies) with labels
            if (avgSpread != null) {
              let avgDesc = '';
              let avgColor = '#666';

              if (avgSpread < 30) {
                avgDesc = 'very clean';
                avgColor = '#38a169';
              } else if (avgSpread < 45) {
                avgDesc = 'clean';
                avgColor = '#48bb78';
              } else if (avgSpread < 60) {
                avgDesc = 'mixed';
                avgColor = '#d69e2e';
              } else {
                avgDesc = 'messy';
                avgColor = '#e53e3e';
              }

              cardContent += `<p class="buoy-metric"><b>&nbsp;&nbsp;&nbsp;&nbsp;Average Spread:</b> ${avgSpread}° <span style="color: ${avgColor}; font-weight: 600;">(${avgDesc})</span> <span style="font-size: 0.85em; color: #666;">— all frequencies</span></p>`;
            }

            // Add visual angular spread vectors
            const peakDir = b.wave_direction_peak;
            const avgDir = b.wave_direction_avg;

            if ((peakDir != null && peakSpread != null) || (avgDir != null && avgSpread != null)) {
              cardContent += `<div style="margin-top: 0.75rem; padding: 0.75rem; background: #f8fafc; border-radius: 4px; border: 1px solid #e2e8f0;">`;
              cardContent += `<p class="buoy-metric" style="font-weight: 600; color: #004b7c; margin-bottom: 0.5rem;">Visual Direction & Spread</p>`;

              // Peak direction + spread vector
              if (peakDir != null && peakSpread != null) {
                cardContent += `
                  <div style="margin: 0.5rem 0;">
                    <span style="font-size: 0.85em; color: #666; font-weight: 600;">Peak:</span>
                    ${createAngularSpreadVector(peakDir, peakSpread, 70)}
                    <span style="font-size: 0.75em; color: #666; margin-left: 0.5rem;">${b.wave_direction_peak_cardinal ?? degreesToCardinal(peakDir)} ${Math.round(peakDir)}° ± ${Math.round(peakSpread/2)}°</span>
                  </div>
                `;
              }

              // Average direction + spread vector
              if (avgDir != null && avgSpread != null) {
                cardContent += `
                  <div style="margin: 0.5rem 0;">
                    <span style="font-size: 0.85em; color: #666; font-weight: 600;">Average:</span>
                    ${createAngularSpreadVector(avgDir, avgSpread, 70)}
                    <span style="font-size: 0.75em; color: #666; margin-left: 0.5rem;">${b.wave_direction_avg_cardinal ?? degreesToCardinal(avgDir)} ${Math.round(avgDir)}° ± ${Math.round(avgSpread/2)}°</span>
                  </div>
                `;
              }

              cardContent += `<p style="font-size: 0.75em; color: #999; margin-top: 0.5rem; margin-bottom: 0;">Arrows show wave travel direction. Sector shows angular spread.</p>`;
              cardContent += `</div>`;
            }

            // Add collapsible explanatory footnote (hidden by default)
            if (peakSpread != null && avgSpread != null) {
              cardContent += `
                <div id="spread-info-${id}" style="display: none; font-size: 0.85em; color: #555; margin-top: 0.5rem; padding: 0.75rem; background: #f0f8ff; border-left: 3px solid #0077be; border-radius: 4px; line-height: 1.5;">
                  <strong style="color: #2c5282;">Angular Spread</strong> measures how organized the waves are:<br>
                  <br>
                  <strong>Lower numbers</strong> = waves coming from one direction (clean swell)<br>
                  <strong>Higher numbers</strong> = waves from multiple directions (choppy/messy)<br>
                  <br>
                  • <strong>Peak:</strong> The main swell direction<br>
                  • <strong>Average:</strong> Overall surface (includes wind chop)<br>
                  <br>
                  <span style="font-size: 0.9em; color: #718096;">Beach conditions may differ from open-ocean buoy readings.</span>
                </div>
              `;
            }
          }
        }
      }

      // Temperatures and pressure (all stations)
      const isSurrey = (id === "CRPILE" || id === "CRCHAN");
      const seaTemp = b.sea_temp != null ? (isSurrey ? b.sea_temp.toFixed(1) : b.sea_temp) : "—";
      const airTemp = b.air_temp != null ? (isSurrey ? b.air_temp.toFixed(1) : b.air_temp) : "—";

      cardContent += `
        <p class="buoy-metric" style="margin-top: 0.75rem;"><b>🌡️ Sea:</b> ${seaTemp} °C | <b>Air:</b> ${airTemp} °C</p>
        <p class="buoy-metric"><b>⏱️ Pressure:</b> ${b.pressure ?? "—"} hPa</p>
      `;

      cardContent += `</div>`; // Close expandable details section

      // === EXPANDABLE HISTORY SECTION (Hidden by default) ===
      cardContent += `<div id="card-history-${id}" style="display: none; margin-top: 0.75rem; padding-top: 0.75rem; border-top: 1px solid #e0e0e0;"></div>`;

      // === NAVIGATION LINKS ===
      const hasChartData = b.wave_height_sig != null || b.wind_speed != null;
      const chartButtonDisabled = !hasChartData ? 'disabled style="opacity: 0.5; cursor: not-allowed;"' : '';

      cardContent += `
        <div class="buoy-nav-links" style="display: flex; gap: 0.5rem; margin-top: 0.75rem;">
          <button class="buoy-nav-link" onclick="scrollToMap('${id}')" style="
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
          " onmouseover="this.style.background='#003a5d'" onmouseout="this.style.background='#004b7c'">
            📍 View Location
          </button>
          <button class="buoy-nav-link" onclick="scrollToCharts('${id}')" ${chartButtonDisabled} style="
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
          " onmouseover="this.style.background='#003a5d'" onmouseout="this.style.background='#004b7c'">
            📊 View Charts
          </button>
        </div>
      `;

      // Add source link at the bottom of the card
      if (sourceLinks[id]) {
        cardContent += `
          <p style="margin-top: 0.75rem; margin-bottom: 0; padding-top: 0.5rem; border-top: 1px solid #e0e0e0; text-align: center;">
            <a href="${sourceLinks[id]}" target="_blank" rel="noopener noreferrer" style="
              font-size: 0.85em;
              color: #004b7c;
              text-decoration: none;
              font-weight: 500;
            " onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">
              🔗 View Source Data
            </a>
          </p>
        `;
      }

        cardContent += `</div>`;
        card.innerHTML = cardContent;
        cardsGrid.appendChild(card);
      }); // end stations forEach

      // Add grid to region group, then add region group to container
      regionGroup.appendChild(cardsGrid);
      container.appendChild(regionGroup);

      // Collapse Boundary Bay and Juan de Fuca by default (keep Strait of Georgia expanded)
      if (group.region !== "Strait of Georgia") {
        const toggleBtn = regionHeader.querySelector('.region-toggle-btn');
        if (toggleBtn && cardsGrid) {
          cardsGrid.style.display = 'none';
          toggleBtn.textContent = '▶';
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
        timeZoneName: "short"
      })}`;
    }

    // Handle hash navigation after cards are loaded
    handleHashNavigation();
  } catch (err) {
    logger.error("BuoyData", "Error loading buoy data", err);
    container.innerHTML =
      `<p class="error">⚠️ Error loading buoy data. Please try again later.</p>`;
  }
}

// Scroll to charts section and select buoy
function scrollToCharts(buoyId) {
  const buoySelector = document.getElementById('buoy-selector');
  const chartsSection = document.getElementById('charts-section');

  // Prefer scrolling to the selector dropdown if it exists
  const scrollTarget = buoySelector || chartsSection;
  if (!scrollTarget) return;

  // Smooth scroll to buoy selector
  scrollTarget.scrollIntoView({ behavior: 'smooth', block: 'start' });

  // Wait for scroll to complete, then trigger chart selection
  setTimeout(() => {
    const chartSelect = document.getElementById('chart-buoy-select');
    if (chartSelect) {
      chartSelect.value = buoyId;
      chartSelect.dispatchEvent(new Event('change'));

      // Add highlight pulse effect to the selector
      if (buoySelector) {
        buoySelector.classList.add('highlight-pulse');
        setTimeout(() => buoySelector.classList.remove('highlight-pulse'), 2000);
      }
    }
  }, 800);
}

// Scroll to map section and center on buoy
function scrollToMap(buoyId) {
  const mapSection = document.getElementById('map-section');
  if (!mapSection) return;

  // Smooth scroll to map section
  mapSection.scrollIntoView({ behavior: 'smooth', block: 'start' });

  // Wait for scroll to complete, then center map on buoy
  setTimeout(() => {
    if (typeof window.centerMapOnBuoy === 'function') {
      window.centerMapOnBuoy(buoyId);

      // Add highlight pulse effect
      mapSection.classList.add('highlight-pulse');
      setTimeout(() => mapSection.classList.remove('highlight-pulse'), 2000);
    }
  }, 800);
}

// Toggle region collapse/expand
function toggleRegion(regionName) {
  const regionGroup = document.getElementById(`region-${regionName.replace(/\s+/g, '-')}`);
  if (!regionGroup) return;

  const cardsGrid = regionGroup.querySelector('.buoy-cards-grid');
  const toggleBtn = regionGroup.querySelector('.region-toggle-btn');

  if (cardsGrid && toggleBtn) {
    const isHidden = cardsGrid.style.display === 'none';
    cardsGrid.style.display = isHidden ? 'grid' : 'none';
    toggleBtn.textContent = isHidden ? '▼' : '▶';

    // Save state to localStorage
    const regionKey = `region-${regionName}-collapsed`;
    localStorage.setItem(regionKey, isHidden ? 'false' : 'true');
  }
}

// Toggle card details (full metrics)
function toggleCardDetails(buoyId) {
  const detailsDiv = document.getElementById(`card-details-${buoyId}`);
  const button = document.querySelector(`#buoy-${buoyId} .toggle-details-btn`);

  if (detailsDiv && button) {
    const isHidden = detailsDiv.style.display === 'none';
    detailsDiv.style.display = isHidden ? 'block' : 'none';
    button.textContent = isHidden ? '▲ Hide Details' : '▼ Show Details';
  }
}

// Toggle angular spread explanation
function toggleSpreadInfo(buoyId) {
  const infoDiv = document.getElementById(`spread-info-${buoyId}`);
  if (infoDiv) {
    const isHidden = infoDiv.style.display === 'none';
    infoDiv.style.display = isHidden ? 'block' : 'none';
  }
}

// Toggle card history table
async function toggleCardHistory(buoyId) {
  const historyDiv = document.getElementById(`card-history-${buoyId}`);
  const button = document.querySelector(`#buoy-${buoyId} .toggle-history-btn`);

  if (!historyDiv || !button) return;

  const isHidden = historyDiv.style.display === 'none';

  if (isHidden) {
    // Auto-collapse Details section when opening History
    const detailsDiv = document.getElementById(`card-details-${buoyId}`);
    const detailsButton = document.querySelector(`#buoy-${buoyId} .toggle-details-btn`);
    if (detailsDiv && detailsDiv.style.display !== 'none') {
      detailsDiv.style.display = 'none';
      if (detailsButton) detailsButton.textContent = '▼ Show Details';
    }

    // Load and display history
    button.textContent = 'Loading...';
    button.disabled = true;

    try {
      const timeseriesData = await fetchWithTimeout(`/data/buoy_timeseries_48h.json?t=${Date.now()}`);
      const buoyData = timeseriesData[buoyId];

      if (buoyData && buoyData.timeseries) {
        historyDiv.innerHTML = renderHistoryTable(buoyId, buoyData.timeseries);
        historyDiv.style.display = 'block';
        button.textContent = '▲ Hide History';
        button.disabled = false;
      } else {
        historyDiv.innerHTML = '<p style="color: #999; text-align: center; padding: 1rem;">No historical data available</p>';
        historyDiv.style.display = 'block';
        button.textContent = '▲ Hide History';
        button.disabled = false;
      }
    } catch (error) {
      logger.error("BuoyData", "Error loading history", error);
      historyDiv.innerHTML = '<p style="color: #e53935; text-align: center; padding: 1rem;">Error loading historical data</p>';
      historyDiv.style.display = 'block';
      button.textContent = '▲ Hide History';
      button.disabled = false;
    }
  } else {
    historyDiv.style.display = 'none';
    button.textContent = '▼ Show History (12h)';
  }
}

// Render history table
function renderHistoryTable(buoyId, timeseries) {
  // Get the most recent 12 hourly observations
  const windSpeed = timeseries.wind_speed?.data || [];
  const windDir = timeseries.wind_direction?.data || [];
  const windGust = timeseries.wind_gust?.data || [];

  // For Neah Bay, use swell data (long-period ocean waves)
  // For other buoys, use combined wave metrics
  const isNeahBay = (buoyId === "46087");
  const waveHeight = isNeahBay
    ? (timeseries.swell_height?.data || [])
    : (timeseries.wave_height_sig?.data || []);
  const wavePeriod = isNeahBay
    ? (timeseries.swell_period?.data || [])
    : (timeseries.wave_period_avg?.data || []);

  const airTemp = timeseries.air_temp?.data || [];
  const seaTemp = timeseries.sea_temp?.data || [];

  logger.debug("History", `${buoyId}: windSpeed=${windSpeed.length}, waveHeight=${waveHeight.length} points`);

  // Show all rows where wave data exists (wind may have gaps)
  let allTimes = waveHeight.map(d => d.time);

  // For Crescent stations, filter to hourly intervals only (on the hour)
  const isCrescentStation = (buoyId === "CRPILE" || buoyId === "CRCHAN");
  if (isCrescentStation) {
    allTimes = allTimes.filter(time => {
      const date = new Date(time);
      return date.getMinutes() === 0; // Only include times on the hour
    });
  }

  // Get last 12 hours of wave data
  const now = new Date();
  const twelveHoursAgo = new Date(now - 12 * 60 * 60 * 1000);
  const times = allTimes.filter(time => new Date(time) >= twelveHoursAgo).sort().reverse();

  logger.debug("History", `${buoyId}: Showing ${times.length} rows (all wave data, wind when available)`);

  logger.debug("History", `${buoyId}: Generated ${times.length} time entries for table`);

  // Responsive scroll indicator - only show on mobile, positioned OUTSIDE table
  const scrollIndicator = window.innerWidth < 768
    ? `<div style="text-align: center; margin-bottom: 0.25rem; padding: 0.25rem 0.5rem; background: rgba(0, 75, 124, 0.1); font-size: 0.65rem; color: #004b7c; border-radius: 4px;">← Scroll table horizontally →</div>`
    : '';

  let tableHTML = `
    ${scrollIndicator}
    <div style="overflow-x: auto; margin-top: 0.5rem; width: 100%; max-width: 100%; -webkit-overflow-scrolling: touch; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box;">
      <table style="border-collapse: collapse; font-size: 0.8rem; width: max-content; min-width: 100%; table-layout: auto;">
        <thead>
          <tr style="background: #f5f5f5;">
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #ddd; border-right: 1px solid #ddd; text-align: left; white-space: nowrap; min-width: 55px;">Time</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #ddd; border-right: 1px solid #ddd; text-align: center; white-space: nowrap; min-width: 95px;">Wind [kn]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #ddd; border-right: 1px solid #ddd; text-align: center; white-space: nowrap; min-width: 50px;">Wave Ht [m]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #ddd; border-right: 1px solid #ddd; text-align: center; white-space: nowrap; min-width: 60px;">Period [s]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #ddd; border-right: 1px solid #ddd; text-align: center; white-space: nowrap; min-width: 55px;">Sea [°C]</th>
            <th style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #ddd; text-align: center; white-space: nowrap; min-width: 55px;">Air [°C]</th>
          </tr>
        </thead>
        <tbody>
  `;

  // Determine wave height precision based on buoy type
  const isBoundaryBay = (buoyId === "CRPILE" || buoyId === "CRCHAN");
  const waveHeightDecimals = isBoundaryBay ? 2 : 1;

  // Track previous date for conditional date display
  let previousDate = null;

  times.forEach((time, index) => {
    const windSpeedVal = windSpeed.find(d => d.time === time)?.value;
    const windDirVal = windDir.find(d => d.time === time)?.value;
    const windGustVal = windGust.find(d => d.time === time)?.value;
    const waveHeightVal = waveHeight.find(d => d.time === time)?.value;
    const wavePeriodVal = wavePeriod.find(d => d.time === time)?.value;
    const airTempVal = airTemp.find(d => d.time === time)?.value;
    const seaTempVal = seaTemp.find(d => d.time === time)?.value;

    const dateObj = new Date(time);

    // Format: "Mo-11 08h10" (2-letter weekday, day, hour, minutes if not :00)
    const dayOfWeek = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'][dateObj.getDay()];
    const dayOfMonth = dateObj.toLocaleString('en-US', { day: 'numeric', timeZone: 'America/Vancouver' });
    const hour = dateObj.toLocaleString('en-US', { hour: '2-digit', hour12: false, timeZone: 'America/Vancouver' });
    const minute = dateObj.toLocaleString('en-US', { minute: '2-digit', timeZone: 'America/Vancouver' });

    // Only show date prefix if it changed from previous row
    const currentDate = `${dayOfWeek}-${dayOfMonth}`;
    const minuteSuffix = minute !== '00' ? minute : '';
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
    let windDisplay = '—';
    if (windSpeedVal != null) {
      const cardinal = degreesToCardinal(windDirVal);
      const cardinalStr = cardinal ? `${cardinal} ` : '';
      const gustStr = windGustVal != null ? ` gust ${Math.round(windGustVal)}` : '';
      windDisplay = `${cardinalStr}${Math.round(windSpeedVal)}${gustStr}`;
    }

    // Alternating row background color
    const rowBg = index % 2 === 0 ? 'background: rgba(0, 75, 124, 0.03);' : '';

    tableHTML += `
      <tr style="${rowBg}">
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #eee; border-right: 1px solid #eee; white-space: nowrap;">${timeStr}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #eee; border-right: 1px solid #eee; text-align: center; white-space: nowrap;">${windDisplay}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #eee; border-right: 1px solid #eee; text-align: center;">${waveHeightVal != null ? waveHeightVal.toFixed(waveHeightDecimals) : '—'}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #eee; border-right: 1px solid #eee; text-align: center;">${wavePeriodVal != null ? wavePeriodVal.toFixed(1) : '—'}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #eee; border-right: 1px solid #eee; text-align: center;">${seaTempVal != null ? seaTempVal.toFixed(1) : '—'}</td>
        <td style="padding: 0.4rem 0.3rem; border-bottom: 1px solid #eee; text-align: center;">${airTempVal != null ? airTempVal.toFixed(1) : '—'}</td>
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
      <button onclick="toggleCardHistory('${buoyId}')" style="padding: 0.5rem 1rem; background: #004b7c; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 0.85rem; transition: background 0.2s;">
        ▲ Hide History
      </button>
    </div>
  `;

  // Add note for Neah Bay explaining swell data
  if (isNeahBay) {
    tableHTML += `
      <div style="margin-top: 0.5rem; padding: 0.5rem; background: #f0f8ff; border-left: 3px solid #003087; font-size: 0.75rem; color: #555; line-height: 1.4;">
        <strong>Note:</strong> Neah Bay displays <strong>swell data</strong> (long-period ocean waves from distant storms) rather than combined wave metrics. Wind waves are typically much smaller at this location.
      </div>
    `;
  }

  logger.debug("History", `${buoyId}: Rendered table with ${times.length} rows`);
  return tableHTML;
}

// Handle hash navigation from map (e.g., /#buoy-46087)
function handleHashNavigation() {
  const hash = window.location.hash;
  if (hash && hash.startsWith('#buoy-')) {
    const buoyId = hash.replace('#buoy-', '');
    const buoyCard = document.getElementById(`buoy-${buoyId}`);

    if (buoyCard) {
      // Find the parent region
      const regionGroup = buoyCard.closest('.region-group');
      if (regionGroup) {
        const cardsGrid = regionGroup.querySelector('.buoy-cards-grid');
        const toggleBtn = regionGroup.querySelector('.region-toggle-btn');

        // Expand region if collapsed
        if (cardsGrid && cardsGrid.style.display === 'none') {
          cardsGrid.style.display = 'grid';
          if (toggleBtn) toggleBtn.textContent = '▼';
        }
      }

      // Scroll to the card after a short delay to ensure rendering
      setTimeout(() => {
        buoyCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
        buoyCard.classList.add('highlight-pulse');
        setTimeout(() => buoyCard.classList.remove('highlight-pulse'), 2000);
      }, 300);
    }
  }
}

// Wait for HTMX to load footer (which contains timestamp element) before initializing
document.addEventListener('htmx:load', function() {
  loadBuoyData();
}, { once: true });

setInterval(loadBuoyData, 5 * 60 * 1000);

// Handle hash navigation when hash changes (clicking map links)
window.addEventListener('hashchange', handleHashNavigation);
