/**
 * Winds Page Map (ES module)
 * Displays wind stations and buoys with wind data on an interactive Leaflet map
 *
 * degreesToCardinal and getDirectionalArrow come from chart-utils-v4.js
 * (classic script, loaded earlier).
 */

import { addFullscreenControl } from "./shared/map-fullscreen.js";
import { getPopupOptions } from "./shared/map-popup.js";
import { createDirectionalMarker } from "./shared/markers.js";
import { stationTypeLabel } from "./shared/station-meta.js";
import { windData } from "./wind-data.js";

// --- Constants ---
const MAP_FOCUS_DELAY_MS = 300; // Delay after map pan before opening popup

let windsMap = null;
let windMarkersLayer = null;
let windMarkers = {}; // Store markers by ID for easy access

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

  addFullscreenControl(windsMap, { title: "View map fullscreen" });

  // Create layer group for markers
  windMarkersLayer = L.layerGroup().addTo(windsMap);

  // Load stations and add markers
  loadWindStationsAndMarkers();
}

// Load wind stations and buoys with wind data (pre-fetched by wind-data.js)
async function loadWindStationsAndMarkers() {
  try {
    await windData.ready;
    const stations = windData.stations;
    const latestAll = windData.latestAll;

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
    const mapEl = document.getElementById("winds-map");
    if (mapEl) {
      mapEl.innerHTML =
        '<div class="winds-map-error">Unable to load station markers. Try refreshing the page.</div>';
    }
  }
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
    iconHtml = createDirectionalMarker(windDir, windSpeed, { type: "wind", stale: isStale });
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

  const marker = L.marker([station.lat, station.lon], {
    icon: icon,
    title: `${station.name} wind station`,
  });

  // Build popup with current wind data
  let popupContent = `<div class="station-popup"><h3>${station.name}</h3>`;

  if (currentData) {
    const staleClass = currentData.stale ? "popup-wind-card--stale" : "popup-wind-card--fresh";
    const typeClass = isBuoy ? "popup-wind-card--buoy" : "popup-wind-card--station";
    const headerClass = currentData.stale ? "popup-wind-header--stale" : "popup-wind-header--fresh";
    const headerText = currentData.stale ? "Last Wind (STALE - >3h old):" : "Current Wind:";
    popupContent += `<div class="popup-wind-card ${staleClass} ${typeClass}">`;
    popupContent += `<div class="popup-wind-header ${headerClass}">${headerText}</div>`;

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
      popupContent += `<div class="popup-timestamp">Updated: ${timeStr}</div>`;
    }

    popupContent += `</div>`;
  }

  // Station details
  const typeLabel = stationTypeLabel(station);
  popupContent += `
    <div class="popup-station-details">
      <div><strong>ID:</strong> ${station.id}</div>
      <div><strong>Location:</strong> ${station.location}</div>
      <div><strong>Source:</strong> ${station.source}</div>
      <div><strong>Type:</strong> ${typeLabel}</div>
    </div>
    <a href="#" class="view-data-btn" data-wind-station-id="${station.id}">View Wind Chart →</a>
  </div>`;

  marker.bindPopup(popupContent, getPopupOptions());
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

// Module scripts are deferred, so the DOM is already parsed.
initWindsMap();
