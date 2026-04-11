/**
 * Winds Page Map
 * Displays wind stations and buoys with wind data on an interactive Leaflet map
 */

// --- Constants ---
const MAP_FOCUS_DELAY_MS = 300; // Delay after map pan before opening popup

let windsMap = null;
let windMarkersLayer = null;
let windMarkers = {}; // Store markers by ID for easy access

// degreesToCardinal, getDirectionalArrow, fetchWithTimeout provided by chart-utils-v4.js (loaded earlier)

/**
 * Create directional marker with triangular arrow for map markers
 * @param {number} direction - Direction in degrees (meteorological: coming FROM)
 * @param {number} speed - Wind speed in knots
 * @param {boolean} stale - Whether the data is stale (>3 hours old)
 * @returns {string} HTML for marker
 */
function createDirectionalMarker(direction, speed, stale = false) {
  const arrowColor = "var(--map-arrow-wind, #dc2626)";
  const opacity = stale ? 0.35 : 1.0; // Transparent if stale

  // Meteorological convention: direction value = where wind is COMING FROM
  // Arrow shows direction wind is TRAVELING TO
  // Arrow SVG points DOWN at rotation=0 (South/180° compass)
  const rotation = direction;

  // Build speed label if available
  const speedLabel =
    speed !== null && speed !== undefined
      ? `<div style="
        background: transparent;
        color: var(--map-marker-text, #004b7c);
        padding: 2px 5px;
        border-radius: 3px;
        font-size: 13px;
        font-weight: bold;
        white-space: nowrap;
        text-shadow: 1px 1px 2px rgba(255,255,255,0.9), -1px -1px 2px rgba(255,255,255,0.9), 1px -1px 2px rgba(255,255,255,0.9), -1px 1px 2px rgba(255,255,255,0.9);
        margin-bottom: -3px;
      ">${Math.round(speed)}kt</div>`
      : "";

  return `
    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; opacity: ${opacity};">
      ${speedLabel}
      <div style="transform: rotate(${rotation}deg); transform-origin: center center;">
        <svg width="26" height="30" viewBox="-6 -10 12 24" style="filter: drop-shadow(0 2px 3px rgba(0,0,0,0.5)); color: ${arrowColor};">
          <path d="M0,12 L-5,-8 L0,-5 L5,-8 Z" fill="currentColor" fill-opacity="0.98" stroke="currentColor" stroke-width="1.5"/>
        </svg>
      </div>
    </div>
  `;
}

// Initialize the map
function initWindsMap() {
  // Create map centered on Salish Sea
  windsMap = L.map("winds-map", {
    center: [49.2, -123.3],
    zoom: 8,
    scrollWheelZoom: true,
    zoomControl: true,
  });

  // Add OpenStreetMap tiles
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  }).addTo(windsMap);

  // Create layer group for markers
  windMarkersLayer = L.layerGroup().addTo(windsMap);

  // Load stations and add markers
  loadWindStationsAndMarkers();
}

// Load wind stations and buoys with wind data (pre-fetched by wind-data.js)
async function loadWindStationsAndMarkers() {
  try {
    await window.windData.ready;
    const stations = window.windData.stations;
    const latestAll = window.windData.latestAll;

    // Add wind station markers
    if (stations.wind) {
      Object.values(stations.wind).forEach((windStation) => {
        const currentData = latestAll[windStation.id];
        addWindMarker(windStation, currentData, false);
      });
    }

    // Add buoy markers (only those with wind data — already normalized)
    if (stations.buoys) {
      Object.values(stations.buoys).forEach((buoy) => {
        const currentData = latestAll[buoy.id];
        if (currentData) {
          addWindMarker(buoy, currentData, true);
        }
      });
    }
  } catch (error) {
    console.error("Error loading wind stations for map:", error);
  }
}

/**
 * Resolve a human-readable type label for a station
 */
function getStationTypeLabel(station, isBuoy) {
  if (!isBuoy) return "Weather Station";
  if (station.type === "pile_mounted_wave_station") return "Pile-Mounted Wave Station";
  if (station.type === "wind_monitoring_station") return "Wind Monitoring Station";
  return "Wave Buoy";
}

/**
 * Add a wind marker (station or buoy) to the map
 * @param {Object} station - Station/buoy metadata (id, name, lat, lon, location, source, type)
 * @param {Object|null} currentData - Normalized latest wind data
 * @param {boolean} isBuoy - Whether this is a buoy (affects CSS class, type label, border color)
 */
function addWindMarker(station, currentData, isBuoy = false) {
  // Create directional marker if we have wind direction data
  let iconHtml = `<div class="marker-icon">${isBuoy ? "🌊" : "💨"}</div>`;
  let iconSize = [30, 30];
  let iconAnchor = [15, 15];

  const windDir = currentData ? currentData.wind_direction_deg || currentData.wind_direction : null;
  if (currentData && windDir !== null && windDir !== undefined) {
    const windSpeed = currentData.wind_speed_kt;
    const isStale = currentData.stale || false;
    iconHtml = createDirectionalMarker(windDir, windSpeed, isStale);
    iconSize = [26, windSpeed ? 48 : 30];
    iconAnchor = [13, windSpeed ? 38 : 15];
  }

  const markerClass = isBuoy ? "buoy-wind-marker" : "wind-station-marker";
  const icon = L.divIcon({
    className: `station-marker ${markerClass}`,
    html: iconHtml,
    iconSize: iconSize,
    iconAnchor: iconAnchor,
    popupAnchor: [0, -15],
  });

  const marker = L.marker([station.lat, station.lon], { icon: icon });

  // Build popup with current wind data
  let popupContent = `<div class="station-popup"><h3>${station.name}</h3>`;

  if (currentData) {
    const bgColor = currentData.stale
      ? "var(--color-callout-danger-bg, #fff5f5)"
      : "var(--color-callout-info-bg, #f0f8ff)";
    const borderColor = currentData.stale
      ? "var(--color-accent-red)"
      : isBuoy
        ? "var(--color-primary)"
        : "var(--color-accent-orange)";
    const headerText = currentData.stale ? "Last Wind (STALE - >3h old):" : "Current Wind:";
    popupContent += `<div style="background: ${bgColor}; padding: 8px; margin: 8px 0; border-radius: 4px; border-left: 3px solid ${borderColor};">`;
    popupContent += `<div style="font-weight: 600; margin-bottom: 4px; color: ${
      currentData.stale ? "var(--color-accent-red)" : "var(--color-primary-dark)"
    };">${headerText}</div>`;

    // Wind speed and gust
    if (currentData.wind_speed_kt != null) {
      const windSpeed = Math.round(currentData.wind_speed_kt);
      const windGust =
        currentData.wind_gust_kt != null ? Math.round(currentData.wind_gust_kt) : null;
      popupContent += `<div><strong>Speed:</strong> ${windSpeed} kt`;
      if (windGust != null) {
        popupContent += ` (gust ${windGust} kt)`;
      }
      popupContent += `</div>`;
    }

    // Wind direction
    const windDir = currentData.wind_direction_deg || currentData.wind_direction;
    if (windDir != null) {
      const arrow = getDirectionalArrow(windDir);
      const cardinal = currentData.wind_direction_cardinal || degreesToCardinal(windDir);
      popupContent += `<div><strong>Direction:</strong> ${cardinal} (${windDir}°) ${arrow}</div>`;
    }

    // Temperature
    if (currentData.air_temp_c != null) {
      popupContent += `<div><strong>Temp:</strong> ${currentData.air_temp_c.toFixed(1)}°C</div>`;
    }

    // Timestamp
    if (currentData.observation_time) {
      const obsTime = new Date(currentData.observation_time);
      const timeStr = obsTime.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
        timeZone: "America/Vancouver",
        timeZoneName: "short",
      });
      popupContent += `<div style="font-size: 0.85em; color: var(--color-text-muted); margin-top: 4px;">Updated: ${timeStr}</div>`;
    }

    popupContent += `</div>`;
  }

  // Station details
  const typeLabel = getStationTypeLabel(station, isBuoy);
  popupContent += `
    <div style="font-size: 0.9em; line-height: 1.4; margin-top: 8px;">
      <div><strong>ID:</strong> ${station.id}</div>
      <div><strong>Location:</strong> ${station.location}</div>
      <div><strong>Source:</strong> ${station.source}</div>
      <div><strong>Type:</strong> ${typeLabel}</div>
    </div>
    <a href="#" class="view-data-btn" data-wind-station-id="${station.id}" style="display: inline-block; margin-top: 8px; padding: 6px 12px; background: var(--color-primary); color: var(--color-on-primary); text-decoration: none; border-radius: 4px; font-size: 0.9em;">View Wind Chart →</a>
  </div>`;

  marker.bindPopup(popupContent);
  marker.addTo(windMarkersLayer);

  // Store marker reference
  windMarkers[station.id] = marker;
}

/**
 * Focus on a specific station by ID
 * Centers the map and opens the station's popup
 */
function focusStation(stationId) {
  const marker = windMarkers[stationId];
  if (!marker) {
    console.warn(`Station ${stationId} not found on map`);
    return;
  }

  // Center map on station
  windsMap.setView(marker.getLatLng(), 10, {
    animate: true,
    duration: 0.5,
  });

  // Open popup after a short delay to allow map animation
  setTimeout(() => {
    marker.openPopup();
  }, MAP_FOCUS_DELAY_MS);
}

// Listen for focus requests from wind-stations.js
document.addEventListener("winds:focus-station", (e) => {
  const stationId = e.detail?.stationId;
  if (stationId) focusStation(stationId);
});

// Event delegation for popup "View Wind Chart" links (CSP-safe)
document.addEventListener("click", (event) => {
  const target = event.target.closest(".view-data-btn[data-wind-station-id]");
  if (!target) {
    return;
  }

  event.preventDefault();
  const stationId = target.getAttribute("data-wind-station-id");
  if (stationId) {
    document.dispatchEvent(new CustomEvent("winds:select-station", { detail: { stationId } }));
  }
});

// Initialize map when DOM is ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    initWindsMap();
  });
} else {
  initWindsMap();
}
