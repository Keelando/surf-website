/**
 * Salish Sea Stations Map (ES module)
 * Displays all buoy and tide stations on an interactive Leaflet map
 *
 * fetchWithTimeout, logger, and L (Leaflet) still come from classic
 * scripts loaded before the entry point.
 */

import { formatWeekdayDayTime, getShortAgeString } from "./shared/format-time.js";
import { addFullscreenControl } from "./shared/map-fullscreen.js";
import { getPopupOptions } from "./shared/map-popup.js";
import { createDirectionalMarker } from "./shared/markers.js";
import { isNoaaStation, isWaveStation, stationTypeLabel } from "./shared/station-meta.js";
import { staleDataWarningHTML, stalePopupTheme } from "./shared/staleness.js";

let stationsMap = null;
let markersLayer = null;
let buoyMarkers = {}; // Store buoy markers by ID for easy access (includes wind stations)
let latestBuoyData = null; // Cache for latest buoy data
let latestWindData = null; // Cache for latest wind station data
let stormSurgeData = null; // Cache for storm surge forecast data
let lightstationMarkers = {}; // Store lightstation markers by ID for easy access
let latestLightstationData = null; // Cache for latest lightstation observations
// Station ids with an RDWPS wave forecast, from the fetcher's own index. Kept
// as a set because the only question asked of it is membership: does this
// marker's station get a "Wave Forecast" link? Reading the index rather than
// listing the stations here means adding a forecast point to the fetcher lights
// up its popup with no change on this side.
let waveForecastStations = new Set();
let webcamMarkers = {}; // Store webcam markers by ID for easy access
let tideMarkers = {}; // Store tide station markers by ID for easy access

// Helper function for directional arrows
function getDirectionalArrow(degrees, arrowType = "wind") {
  if (degrees == null || degrees === "—") return "";

  // Meteorological convention: direction indicates WHERE wind/waves are COMING FROM
  const rotation = arrowType === "wind" ? degrees : degrees + 90;

  const arrowColor =
    arrowType === "wind" ? "var(--map-arrow-wind, #dc2626)" : "var(--map-arrow-wave, #0077be)";

  // SVG arrows: wind points down, wave points right
  const svg =
    arrowType === "wind"
      ? `<svg width="16" height="16" viewBox="0 0 16 16" style="color: ${arrowColor};"><path d="M8 2v12m0 0l-3-3m3 3l3-3" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`
      : `<svg width="16" height="16" viewBox="0 0 16 16" style="color: ${arrowColor};"><path d="M2 8h12m0 0l-3-3m3 3l-3 3" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round"/></svg>`;

  return `<span style="display:inline-block;transform:rotate(${rotation}deg);margin-left:0.3rem;vertical-align:middle;">${svg}</span>`;
}

// Desktop gets one extra zoom level: at 600px+ the map is wide enough that
// zoom 8 leaves the Salish Sea stations bunched in the middle. Narrow screens
// stay at 8, where the extra level would push most stations off-screen.
// 600px matches the nav's mobile breakpoint (nav-tide-styles-v4.css).
const DESKTOP_BREAKPOINT_PX = 600;
const STATIONS_MAP_ZOOM_DESKTOP = 9;
const STATIONS_MAP_ZOOM_MOBILE = 8;

// Marker label geometry, in px above the 30px arrow. Shared by the icon
// builder and the collision test so the two cannot disagree about how tall a
// marker is.
const WAVE_LABEL_HEIGHT_PX = 18;
const WIND_LABEL_HEIGHT_PX = 14;

// Clear space required around a marker before it may grow a second label
// line. Small: the point is to stop labels touching, not to space the map out.
const LABEL_COLLISION_PADDING_PX = 4;

// Initialize the map
function initStationsMap() {
  // Create map centered on Salish Sea
  stationsMap = L.map("stations-map", {
    center: [49.2, -123.3],
    zoom:
      window.innerWidth >= DESKTOP_BREAKPOINT_PX
        ? STATIONS_MAP_ZOOM_DESKTOP
        : STATIONS_MAP_ZOOM_MOBILE,
    scrollWheelZoom: true,
    zoomControl: true,
  });

  // Add OpenStreetMap tiles
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution:
      '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19,
  }).addTo(stationsMap);

  addFullscreenControl(stationsMap, { title: "View map fullscreen" });

  // Rebuild buoy icons when a zoom crosses the wind-label threshold.
  stationsMap.on("zoomend", refreshWindLabels);

  // Create layer group for markers
  markersLayer = L.layerGroup().addTo(stationsMap);

  // Load stations and add markers
  loadStationsAndMarkers();
}

// Load stations.json from the envcan_wave directory
async function loadStationsAndMarkers() {
  try {
    // Fetch latest buoy data
    try {
      latestBuoyData = await fetchWithTimeout("/data/latest_buoy_v2.json");
    } catch (err) {
      logger.warn("StationsMap", "Could not fetch latest buoy data", err);
    }

    // Fetch latest wind station data
    try {
      latestWindData = await fetchWithTimeout("/data/latest_wind.json");
      logger.debug("StationsMap", "Loaded latest wind station data");
    } catch (err) {
      logger.warn("StationsMap", "Could not fetch latest wind data", err);
    }

    // Fetch storm surge forecast data
    try {
      stormSurgeData = await fetchWithTimeout("/data/storm_surge/combined_forecast.json");
    } catch (err) {
      logger.warn("StationsMap", "Could not fetch storm surge data", err);
    }

    // Fetch latest lightstation observations
    try {
      latestLightstationData = await fetchWithTimeout("/data/latest_lightstation.json");
      logger.debug("StationsMap", "Loaded latest lightstation observations");
    } catch (err) {
      logger.warn("StationsMap", "Could not fetch latest lightstation data", err);
    }

    // Fetch the wave-forecast station index. Optional: without it the popups
    // simply carry no forecast link, which is the same as before it existed.
    try {
      const waveIndex = await fetchWithTimeout("/data/wave_forecast/index.json");
      waveForecastStations = new Set(
        (waveIndex.stations || []).map((station) => station.station_id),
      );
    } catch (err) {
      logger.warn("StationsMap", "Could not fetch wave forecast index", err);
    }

    // Fetch stations metadata
    const stations = await fetchWithTimeout("/data/stations.json");

    // Add buoy markers
    if (stations.buoys) {
      Object.values(stations.buoys).forEach((buoy) => {
        addBuoyMarker(buoy);
      });
    }

    // Add tide station markers
    if (stations.tides) {
      // Filter out geodetic stations that are already shown as wave/buoy markers
      const geodeticStations = ["crescent_beach_ocean", "crescent_channel_ocean"];
      const tidesToShow = Object.entries(stations.tides).filter(
        ([key]) => !geodeticStations.includes(key),
      );

      logger.debug(
        "StationsMap",
        `Loading ${tidesToShow.length} tide stations to map (excluding ${geodeticStations.length} geodetic stations)...`,
      );
      tidesToShow.forEach(([stationKey, tide]) => {
        addTideMarker(tide, stationKey);
        logger.debug("StationsMap", `Added tide marker: ${stationKey} (${tide.name})`);
      });
    }

    // Add wind station markers (as buoys since they use same marker type)
    if (stations.wind) {
      const windCount = Object.keys(stations.wind).length;
      logger.debug("StationsMap", `Loading ${windCount} wind stations to map...`);
      Object.values(stations.wind).forEach((windStation) => {
        // Wind stations use the same marker function as buoys
        addBuoyMarker(windStation);
      });
    }

    // Add lighthouse station markers
    if (stations.lightstations) {
      const lightstationCount = Object.keys(stations.lightstations).length;
      logger.debug("StationsMap", `Loading ${lightstationCount} lighthouse stations to map...`);
      Object.values(stations.lightstations).forEach((lightstation) => {
        addLightstationMarker(lightstation);
      });
    }

    // Add webcam markers
    if (stations.webcams) {
      const webcamCount = Object.keys(stations.webcams).length;
      logger.debug("StationsMap", `Loading ${webcamCount} webcam(s) to map...`);
      Object.values(stations.webcams).forEach((webcam) => {
        addWebcamMarker(webcam);
      });
    }

    // Markers start without the wind line; now that every one of them is on
    // the map, work out which have room for it.
    refreshWindLabels();

    // Check for station parameter in URL and zoom to it
    checkAndZoomToStation();
  } catch (error) {
    logger.error("StationsMap", "Error loading stations", error);
    // Fallback to inline station data if fetch fails
    loadFallbackStations();
  }
}

/**
 * Check URL parameters for station ID and zoom to that station if found
 * URL format: /?station=<stationId>#map-section
 */
function checkAndZoomToStation() {
  try {
    const urlParams = new URLSearchParams(window.location.search);
    const stationId = urlParams.get("station");

    if (!stationId) return;

    // Check all marker types for the station
    const allMarkers = {
      ...buoyMarkers, // Includes wind stations
      ...tideMarkers,
      ...lightstationMarkers,
      ...webcamMarkers,
    };

    const marker = allMarkers[stationId];

    if (marker) {
      // Get marker position
      const latLng = marker.getLatLng();

      // Zoom to marker with smooth animation
      stationsMap.setView(latLng, 12, {
        animate: true,
        duration: 1.0,
      });

      // Open popup after zoom animation completes
      setTimeout(() => {
        marker.openPopup();
      }, 1000);

      logger.info("StationsMap", `Zoomed to station: ${stationId}`);
    } else {
      logger.warn("StationsMap", `Station not found: ${stationId}`);
    }
  } catch (error) {
    logger.error("StationsMap", "Error zooming to station", error);
  }
}

/**
 * Create custom lighthouse SVG icon
 * Classic lighthouse tower with red/white stripes and beacon light
 * @returns {string} SVG string
 */
function createLighthouseSVG() {
  return `
    <svg width="28" height="32" viewBox="0 0 28 32" xmlns="http://www.w3.org/2000/svg">
      <!-- Light beacon rays -->
      <g opacity="0.4">
        <path d="M14 3 L8 0 L10 4 Z" fill="#FFD700"/>
        <path d="M14 3 L20 0 L18 4 Z" fill="#FFD700"/>
        <path d="M14 3 L5 1 L8 5 Z" fill="#FFA500"/>
        <path d="M14 3 L23 1 L20 5 Z" fill="#FFA500"/>
      </g>

      <!-- Lighthouse beacon (top) -->
      <circle cx="14" cy="4" r="2.5" fill="#FFD700" stroke="#FFA500" stroke-width="1"/>

      <!-- Lighthouse tower (tapered) -->
      <!-- White stripe (top) -->
      <path d="M11 7 L10.5 13 L17.5 13 L17 7 Z" fill="#FFFFFF" stroke="#2c3e50" stroke-width="0.5"/>

      <!-- Red stripe -->
      <path d="M10.5 13 L10 19 L18 19 L17.5 13 Z" fill="#E53935" stroke="#2c3e50" stroke-width="0.5"/>

      <!-- White stripe -->
      <path d="M10 19 L9.5 25 L18.5 25 L18 19 Z" fill="#FFFFFF" stroke="#2c3e50" stroke-width="0.5"/>

      <!-- Base (red) -->
      <rect x="8" y="25" width="12" height="4" rx="1" fill="#C62828" stroke="#2c3e50" stroke-width="0.5"/>

      <!-- Foundation -->
      <rect x="6" y="29" width="16" height="2" rx="1" fill="#5D4037" stroke="#3e2723" stroke-width="0.5"/>

      <!-- Light beacon glow -->
      <circle cx="14" cy="4" r="2" fill="#FFEB3B" opacity="0.6"/>
      <circle cx="14" cy="4" r="1.5" fill="#FFF9C4"/>
    </svg>
  `;
}

/**
 * Create custom webcam SVG icon
 * Rounded camera icon for webcam markers
 * @returns {string} SVG string
 */
function createWebcamSVG() {
  return `
    <svg width="28" height="28" viewBox="0 0 28 28" xmlns="http://www.w3.org/2000/svg">
      <!-- Background circle -->
      <circle cx="14" cy="14" r="13" fill="#2c5282" stroke="#1a365d" stroke-width="1.5"/>

      <!-- Camera body -->
      <rect x="6" y="10" width="16" height="11" rx="1.5" fill="#ffffff" stroke="#1a365d" stroke-width="0.8"/>

      <!-- Lens -->
      <circle cx="14" cy="15.5" r="4" fill="#4a5568" stroke="#2d3748" stroke-width="0.8"/>
      <circle cx="14" cy="15.5" r="2.5" fill="#718096"/>
      <circle cx="14" cy="15.5" r="1.3" fill="#2d3748"/>
      <circle cx="14.8" cy="14.7" r="0.6" fill="#e2e8f0" opacity="0.7"/>

      <!-- Viewfinder -->
      <circle cx="18.5" cy="12" r="1" fill="#e53e3e"/>
    </svg>
  `;
}

/**
 * Create simple tide station SVG icon
 * Purple circle - distinguishes from blue wave markers
 * @returns {string} SVG string
 */
function createTideGaugeSVG() {
  return `
    <svg width="24" height="24" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
      <circle cx="12" cy="12" r="10" fill="#9333ea" stroke="#6b21a8" stroke-width="2"/>
    </svg>
  `;
}

/**
 * Latest observation for a station, from whichever export feeds it:
 * wave stations → latest_buoy_v2.json, wind stations → latest_wind.json.
 * Single lookup shared by the marker and popup paths.
 */
function latestStationData(station) {
  const source = isWaveStation(station) ? latestBuoyData : latestWindData;
  return source?.[station.id] ?? null;
}

/**
 * divIcon for a buoy or wind station: directional arrow plus value labels,
 * falling back to the type emoji when there is no live direction.
 *
 * Split out of addBuoyMarker because the zoom handler rebuilds it: the
 * wind-speed line appears and disappears as the user zooms, and re-running
 * the same builder is what keeps the two paths from drifting.
 */
function buildBuoyIcon(buoy, { showWind = false } = {}) {
  const isWave = isWaveStation(buoy);
  const markerEmoji = isWave ? "🌊" : "💨";

  // Check if we have live data with direction for this buoy
  let iconHtml = `<div class="marker-icon">${markerEmoji}</div>`;
  let iconSize = [30, 30];
  let iconAnchor = [15, 15];
  // How much taller this icon WOULD get if the wind line were switched on.
  // Zero once it is on, or when the station has no wind reading to show. The
  // collision test reads it to size every marker at its maximum, so the
  // answer does not depend on which markers happen to be expanded already.
  let windLabelDelta = 0;

  try {
    const data = latestStationData(buoy);

    if (data) {
      // Try multiple possible field names for wave direction
      const waveDirection =
        data.wave_direction_avg || data.wave_direction_peak || data.wave_direction;
      const waveHeight = data.wave_height_sig;
      // Wind direction: unified field name (wind_direction_deg), fallback to old name for buoys
      const windDirection = data.wind_direction_deg || data.wind_direction;
      const isStale = data.stale || false;
      // Wind stations use wind_speed_kt, buoys use wind_speed. Both are
      // already knots. Absent means outside the freshness window (the export
      // drops stale fields), so there is nothing to second-guess here.
      const windSpeed = data.wind_speed_kt !== undefined ? data.wind_speed_kt : data.wind_speed;
      // Wave markers only: the wind marker's own label is the wind speed.
      const waveWindLabel =
        showWind && windSpeed !== null && windSpeed !== undefined ? windSpeed : null;
      // Each label line stacks above the 30px arrow; the anchor keeps the
      // arrow tip on the station.
      const labelHeight =
        (waveHeight ? WAVE_LABEL_HEIGHT_PX : 0) +
        (waveWindLabel !== null ? WIND_LABEL_HEIGHT_PX : 0);
      const hasWind = windSpeed !== null && windSpeed !== undefined;

      // For wave stations with wave direction data
      if (isWave && waveDirection !== null && waveDirection !== undefined) {
        // Create directional arrow marker with wave height (BLUE)
        iconHtml = createDirectionalMarker(waveDirection, waveHeight, {
          type: "wave",
          stale: isStale,
          windSpeed: waveWindLabel,
        });
        // Arrow size: 26x30px (fattened), plus whatever the labels take
        iconSize = [26, 30 + labelHeight];
        // Anchor at center of rotation (arrow centre when there is no label)
        iconAnchor = [13, labelHeight ? 20 + labelHeight : 15];
        windLabelDelta = hasWind && waveWindLabel === null ? WIND_LABEL_HEIGHT_PX : 0;
      }
      // For wave stations without wave direction but with wind direction and wave height
      else if (
        isWave &&
        waveHeight !== null &&
        waveHeight !== undefined &&
        windDirection !== null &&
        windDirection !== undefined
      ) {
        // Still a wave station, so still wave blue - drawn hollow because the
        // direction is the wind's, not a wave measurement (see the popup note)
        iconHtml = createDirectionalMarker(windDirection, waveHeight, {
          type: "wave-inferred",
          stale: isStale,
          windSpeed: waveWindLabel,
        });
        iconSize = [26, 30 + labelHeight];
        iconAnchor = [13, 20 + labelHeight];
        windLabelDelta = hasWind && waveWindLabel === null ? WIND_LABEL_HEIGHT_PX : 0;
      }
      // For non-wave stations with wind direction
      else if (!isWave && windDirection !== null && windDirection !== undefined) {
        // Show red wind direction marker
        iconHtml = createDirectionalMarker(windDirection, windSpeed, {
          type: "wind",
          stale: isStale,
        });
        iconSize = [26, windSpeed ? 48 : 30];
        iconAnchor = [13, windSpeed ? 38 : 15];
      }
    }
  } catch (error) {
    logger.error("StationsMap", `Error creating directional marker for ${buoy.id}`, error);
    // Fall back to emoji on error
    iconHtml = `<div class="marker-icon">${markerEmoji}</div>`;
    iconSize = [30, 30];
    iconAnchor = [15, 15];
  }

  return L.divIcon({
    className: `station-marker buoy-marker ${buoy.type || "wave_buoy"}`,
    html: iconHtml,
    iconSize: iconSize,
    iconAnchor: iconAnchor,
    popupAnchor: [0, -15],
    windLabelDelta: windLabelDelta,
  });
}

/**
 * A marker's pixel footprint at the current zoom, sized at its MAXIMUM: any
 * marker that could grow a wind line is measured as though it already had
 * one. Layer points rather than container points, so the answer depends only
 * on zoom and not on where the map happens to be panned.
 *
 * @returns {{left: number, top: number, right: number, bottom: number}|null}
 */
function markerBox(marker) {
  const icon = marker.options?.icon?.options;
  if (!icon?.iconSize || !icon?.iconAnchor) return null;

  const [width, height] = icon.iconSize;
  const [anchorX, anchorY] = icon.iconAnchor;
  const grow = icon.windLabelDelta || 0; // labels stack upward
  const point = stationsMap.latLngToLayerPoint(marker.getLatLng());
  const left = point.x - anchorX;
  const top = point.y - anchorY - grow;

  return { left, top, right: left + width, bottom: top + height + grow };
}

/** Do two boxes come within LABEL_COLLISION_PADDING_PX of each other? */
function boxesCollide(a, b) {
  const pad = LABEL_COLLISION_PADDING_PX;
  return (
    a.left - pad < b.right &&
    b.left - pad < a.right &&
    a.top - pad < b.bottom &&
    b.top - pad < a.bottom
  );
}

/**
 * Show the wind-speed line on every buoy marker that has room for it at the
 * current zoom, and hide it on the ones that would collide with a neighbour.
 *
 * Replaces a single zoom threshold: a station alone in the middle of the
 * strait (Halibut Bank) can carry two label lines at zoom 9, while the
 * English Bay cluster cannot until it is pulled much further apart. Stations
 * are fixed points, so their pixel separation is a function of zoom alone -
 * projecting them and measuring is exact, not a heuristic.
 *
 * Every box is measured at its maximum (see markerBox), which makes the
 * result symmetric: it does not matter which markers are already expanded, so
 * zooming out and back in lands on the same layout.
 *
 * Icons only - popups are untouched, so an open popup survives the zoom.
 */
function refreshWindLabels() {
  if (!stationsMap || !markersLayer) return;

  // Every marker on the map is an obstacle, not just the buoys: a wind line
  // is just as unreadable under a lightstation or webcam pin.
  const boxes = markersLayer
    .getLayers()
    .map((marker) => ({ marker, box: markerBox(marker) }))
    .filter((entry) => entry.box !== null);

  boxes.forEach(({ marker, box }) => {
    // Buoy and wind-station markers only; the rest carry no labels.
    if (!marker.stationMeta) return;

    const show = !boxes.some((other) => other.marker !== marker && boxesCollide(box, other.box));
    if (show === marker.windLabelShown) return;

    marker.windLabelShown = show;
    marker.setIcon(buildBuoyIcon(marker.stationMeta, { showWind: show }));
  });
}

// Add buoy marker to map
function addBuoyMarker(buoy) {
  const isWave = isWaveStation(buoy);
  const typeLabel = stationTypeLabel(buoy);

  const marker = L.marker([buoy.lat, buoy.lon], {
    icon: buildBuoyIcon(buoy, { showWind: false }),
    title: `${buoy.name} buoy`,
  });
  // Kept on the marker so the zoom handler can rebuild the icon without
  // re-reading stations.json.
  marker.stationMeta = buoy;
  marker.windLabelShown = false;

  // Build popup with latest data at top
  let popupContent = `<div class="station-popup"><h3>${buoy.name}</h3>`;

  // Add latest data if available (priority data at top)
  const popupData = latestStationData(buoy);

  if (popupData) {
    const data = popupData;
    const obsTime = data.observation_time ? new Date(data.observation_time) : null;
    const isStale = data.stale || false;
    const popupTheme = stalePopupTheme(isStale);

    popupContent += `<div style="background: ${popupTheme.bg}; padding: 8px; margin: 8px 0; border-radius: 4px; border-left: 3px solid ${popupTheme.border};">`;
    popupContent += `<div style="font-weight: 600; margin-bottom: 4px; color: ${popupTheme.headingColor};">${popupTheme.headerText}</div>`;

    // Show wind data (handle both buoy and wind station formats)
    const windSpeed = data.wind_speed_kt !== undefined ? data.wind_speed_kt : data.wind_speed;
    if (windSpeed !== null && windSpeed !== undefined) {
      const windSpeedRounded = Math.round(windSpeed);
      const windGust = data.wind_gust_kt !== undefined ? data.wind_gust_kt : data.wind_gust;
      const windGustRounded =
        windGust !== null && windGust !== undefined ? Math.round(windGust) : null;
      const windCardinal = data.wind_direction_cardinal || "—";
      const windDir = data.wind_direction_deg || data.wind_direction;
      const windDegrees =
        windDir !== null && windDir !== undefined ? ` (${Math.round(windDir)}°)` : "";
      const windArrow = getDirectionalArrow(windDir, "wind");
      const gustPart = windGustRounded !== null ? ` G ${windGustRounded}` : "";

      popupContent += `<div><strong>💨 Wind:</strong> ${windCardinal} ${windSpeedRounded}${gustPart} kt${windDegrees} ${windArrow}</div>`;
    }

    // Show wave data with direction
    if (data.wave_height_sig !== null && data.wave_height_sig !== undefined) {
      const waveHeight = data.wave_height_sig.toFixed(1);
      const period = data.wave_period_avg || data.wave_period_peak || null;
      const periodStr =
        period !== null
          ? ` @ ${typeof period === "number" ? period.toFixed(1) + "s" : period}`
          : "";

      // Check if this is a NOAA buoy with spectral wave data
      const hasSpectralData =
        isNoaaStation(buoy) && (data.swell_height !== null || data.wind_wave_height !== null);

      if (hasSpectralData) {
        // Show detailed wave breakdown for NOAA buoys
        popupContent += `<div style="margin: 4px 0;"><strong>🌊 Waves (Spectral):</strong></div>`;
        popupContent += `<div style="margin-left: 8px; font-size: 0.9em;">`;

        // Significant wave (combined)
        popupContent += `<div style="margin: 2px 0;"><em>Combined:</em> ${waveHeight}m${periodStr}</div>`;

        // Wind waves (local chop)
        if (data.wind_wave_height !== null && data.wind_wave_height !== undefined) {
          const windWaveHeight = data.wind_wave_height.toFixed(1);
          const windWavePeriod =
            data.wind_wave_period !== null ? ` @ ${data.wind_wave_period.toFixed(1)}s` : "";
          const windWaveCardinal = data.wind_wave_direction_cardinal || "";
          const windWaveDeg =
            data.wind_wave_direction !== null ? ` (${Math.round(data.wind_wave_direction)}°)` : "";
          const windWaveArrow =
            data.wind_wave_direction !== null
              ? getDirectionalArrow(data.wind_wave_direction, "wave")
              : "";
          const windWaveDir = windWaveCardinal ? `${windWaveCardinal} ` : "";

          popupContent += `<div style="margin: 2px 0;"><em>Wind Wave:</em> ${windWaveDir}${windWaveHeight}m${windWavePeriod}${windWaveDeg} ${windWaveArrow}</div>`;
        }

        // Ocean swell
        if (data.swell_height !== null && data.swell_height !== undefined) {
          const swellHeight = data.swell_height.toFixed(1);
          const swellPeriod =
            data.swell_period !== null ? ` @ ${data.swell_period.toFixed(1)}s` : "";
          const swellCardinal = data.swell_direction_cardinal || "";
          const swellDeg =
            data.swell_direction !== null ? ` (${Math.round(data.swell_direction)}°)` : "";
          const swellArrow =
            data.swell_direction !== null ? getDirectionalArrow(data.swell_direction, "wave") : "";
          const swellDir = swellCardinal ? `${swellCardinal} ` : "";

          popupContent += `<div style="margin: 2px 0;"><em>Swell:</em> ${swellDir}${swellHeight}m${swellPeriod}${swellDeg} ${swellArrow}</div>`;
        }

        popupContent += `</div>`;
      } else {
        // Standard wave display for non-spectral buoys
        // Match map marker logic: use wave_direction_avg first, then peak, then swell (same as map markers)
        const waveDir =
          data.wave_direction_avg || data.wave_direction_peak || data.swell_direction || null;

        if (waveDir !== null) {
          // Try to get cardinal direction matching the numeric direction we're using
          let waveCardinal = "";
          if (data.wave_direction_avg && data.wave_direction_avg === waveDir) {
            waveCardinal = data.wave_direction_avg_cardinal || "";
          } else if (data.wave_direction_peak && data.wave_direction_peak === waveDir) {
            waveCardinal = data.wave_direction_peak_cardinal || "";
          } else if (data.swell_direction && data.swell_direction === waveDir) {
            waveCardinal = data.swell_direction_cardinal || "";
          }

          const waveDegrees = ` (${Math.round(waveDir)}°)`;
          const waveArrow = getDirectionalArrow(waveDir, "wave");
          const dirDisplay = waveCardinal ? `${waveCardinal} ` : "";

          popupContent += `<div><strong>🌊 Wave:</strong> ${dirDisplay}${waveHeight}m${periodStr}${waveDegrees} ${waveArrow}</div>`;
        } else {
          // No wave direction: say so, because the marker is still an arrow
          // and it is pointing with the wind (see the 'wave-inferred' type).
          popupContent += `<div><strong>🌊 Wave:</strong> ${waveHeight}m${periodStr}</div>`;
          popupContent += `<div style="font-size: 0.85em; color: var(--color-text-muted); margin-top: 2px;">Height and period only — the marker arrow shows wind direction.</div>`;
        }
      }
    }

    // Show temperatures
    if (
      (data.sea_temp !== null && data.sea_temp !== undefined) ||
      (data.air_temp !== null && data.air_temp !== undefined)
    ) {
      const seaTemp =
        data.sea_temp !== null && data.sea_temp !== undefined ? data.sea_temp.toFixed(1) : "—";
      const airTemp =
        data.air_temp !== null && data.air_temp !== undefined ? data.air_temp.toFixed(1) : "—";
      popupContent += `<div><strong>🌡️ Temp:</strong> Sea ${seaTemp}°C | Air ${airTemp}°C</div>`;
    }

    // Show timestamp
    if (obsTime) {
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

  // Station details (condensed)
  popupContent += `
    <div style="font-size: 0.9em; line-height: 1.4;">
      <div><strong>ID:</strong> ${buoy.id}</div>
      <div><strong>Location:</strong> ${buoy.location}</div>
      <div><strong>Source:</strong> ${buoy.source}</div>
      <div><strong>Type:</strong> ${typeLabel}</div>
      <div><strong>Coordinates:</strong> ${buoy.lat.toFixed(4)}, ${buoy.lon.toFixed(4)}</div>
    </div>`;

  // Add tide data note for stations that provide it
  if (buoy.provides_tide) {
    popupContent += `
      <div style="background: var(--color-callout-warning-bg); padding: 6px; margin: 8px 0; border-radius: 4px; border-left: 3px solid var(--color-accent-orange); font-size: 0.85em;">
        <strong>📊 Also provides:</strong> Tide data (Geodetic CGVD28)
      </div>`;
  }

  // Determine link based on station type
  const linkHref = isWave ? `/#buoy-${buoy.id}` : `/winds.html#wind-${buoy.id}`;
  const linkText = isWave ? "View Data →" : "View on Winds Page →";

  popupContent += `
    <div class="popup-actions">
      <a href="${linkHref}" class="view-data-btn">${linkText}</a>`;

  // Only where RDWPS is actually extracted for this station. Secondary styling
  // deliberately: the popup is showing measurements, and the forecast is a
  // model's opinion — it should not compete with "View Data" for the eye.
  if (waveForecastStations.has(buoy.id)) {
    popupContent += `
      <a href="/forecasts.html#wave-${buoy.id}" class="view-data-btn wave-forecast-link">🌊 Wave Forecast →</a>`;
  }

  popupContent += `
    </div>
  </div>`;

  marker.bindPopup(popupContent, getPopupOptions());
  marker.addTo(markersLayer);

  // Store marker reference for later access
  buoyMarkers[buoy.id] = marker;
}

// Mapping from tide station keys to storm surge station names
const TIDE_TO_SURGE_MAP = {
  point_atkinson: "Point_Atkinson",
  campbell_river: "Campbell_River",
  crescent_pile: "Crescent_Beach_Channel",
  crescent_beach_ocean: "Crescent_Beach_Ocean",
  crescent_channel_ocean: "Crescent_Channel_Ocean",
  tofino: "Tofino",
};

// Mapping from surge station names to map markers (reverse lookup)
const SURGE_TO_MARKER_MAP = {
  Point_Atkinson: { type: "tide", id: "point_atkinson" },
  Campbell_River: { type: "tide", id: "campbell_river" },
  Crescent_Beach_Channel: { type: "tide", id: "crescent_pile" },
  Crescent_Beach_Ocean: { type: "tide", id: "crescent_beach_ocean" },
  Crescent_Channel_Ocean: { type: "tide", id: "crescent_channel_ocean" },
  Neah_Bay: { type: "buoy", id: "46087" },
  New_Dungeness: { type: "buoy", id: "46088" },
  Tofino: { type: "tide", id: "tofino" },
};

// Get current storm surge forecast for a tide station
function getCurrentSurgeForecast(stationKey) {
  if (!stormSurgeData) return null;

  const surgeStationName = TIDE_TO_SURGE_MAP[stationKey];
  if (!surgeStationName) return null;

  const station = stormSurgeData.stations?.[surgeStationName];
  if (!station || !station.forecast) return null;

  // Find the nearest forecast time (current or next)
  const now = new Date();
  const forecastTimes = Object.keys(station.forecast)
    .map((t) => new Date(t))
    .filter((t) => t >= now)
    .sort((a, b) => a - b);

  if (forecastTimes.length === 0) return null;

  const nextTime = forecastTimes[0];
  const nextTimeStr = nextTime.toISOString();
  const value = station.forecast[nextTimeStr];

  // Return null if value is invalid
  if (value === null || value === undefined) return null;

  return {
    value: value,
    time: nextTime,
    stationName: station.station_name,
  };
}

// Add tide station marker to map
function addTideMarker(tide, stationKey) {
  const icon = L.divIcon({
    className: "station-marker tide-marker",
    html: createTideGaugeSVG(),
    iconSize: [24, 24],
    iconAnchor: [12, 12],
    popupAnchor: [0, -12],
  });

  const marker = L.marker([tide.lat, tide.lon], {
    icon: icon,
    title: `${tide.name} tide station`,
  });

  const hasObservations = tide.series && tide.series.includes("wlo");
  const stationType = hasObservations
    ? "Permanent (with observations)"
    : "Temporary (predictions only)";

  // Build popup with storm surge at top if available
  let popupContent = `<div class="station-popup"><h3>${tide.name}</h3>`;

  // Add storm surge forecast if available (priority data at top)
  const surgeForecast = getCurrentSurgeForecast(stationKey);
  if (surgeForecast && surgeForecast.value !== null && surgeForecast.value !== undefined) {
    const surgeSign = surgeForecast.value >= 0 ? "+" : "";
    const timeStr = surgeForecast.time.toLocaleTimeString("en-US", {
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
      timeZone: "America/Vancouver",
      timeZoneName: "short",
    });

    popupContent += `<div style="background: var(--color-callout-warning-bg); padding: 8px; margin: 8px 0; border-radius: 4px; border-left: 3px solid var(--color-accent-orange);">`;
    popupContent += `<div style="font-weight: 600; margin-bottom: 4px;">Storm Surge Forecast:</div>`;
    popupContent += `<div><strong>${surgeSign}${surgeForecast.value.toFixed(2)}m</strong></div>`;
    popupContent += `<div style="font-size: 0.85em; color: var(--color-text-muted); margin-top: 4px;">Next: ${timeStr}</div>`;
    popupContent += `</div>`;
  }

  // Station details (condensed)
  popupContent += `
    <div style="font-size: 0.9em; line-height: 1.4;">
      <div><strong>Code:</strong> ${tide.code}</div>
      <div><strong>Location:</strong> ${tide.location}</div>
      <div><strong>Source:</strong> ${tide.source}</div>
      <div><strong>Type:</strong> ${stationType}</div>
      <div><strong>Coordinates:</strong> ${tide.lat.toFixed(4)}, ${tide.lon.toFixed(4)}</div>
      ${tide.note ? `<div style="font-style: italic; margin-top: 4px; color: var(--color-text-muted);">${tide.note}</div>` : ""}
    </div>
    <div class="popup-actions">
      <a href="/tides.html?station=${stationKey}" class="view-data-btn">View Data →</a>
    </div>
  </div>`;

  marker.bindPopup(popupContent, getPopupOptions());
  marker.addTo(markersLayer);

  // Store marker reference for later access
  tideMarkers[stationKey] = marker;
}

// Add lightstation marker to map
function addLightstationMarker(lightstation) {
  // Check if this station has any current data
  const lookupName = lightstation.id.replace(/_/g, " ");
  const hasData = latestLightstationData && latestLightstationData[lookupName];

  // Inactive stations (no data) get reduced opacity
  const markerClass = hasData
    ? "station-marker lightstation-marker"
    : "station-marker lightstation-marker lightstation-inactive";

  const icon = L.divIcon({
    className: markerClass,
    html: createLighthouseSVG(),
    iconSize: [28, 32],
    iconAnchor: [14, 32],
    popupAnchor: [0, -32],
  });

  const marker = L.marker([lightstation.lat, lightstation.lon], {
    icon: icon,
    title: `${lightstation.name} lightstation`,
  });

  // Build popup
  let popupContent = `<div class="station-popup"><h3>${lightstation.name}</h3>`;

  if (hasData) {
    const obs = latestLightstationData[lookupName];
    const isStale = obs.stale || false;
    const popupTheme = stalePopupTheme(isStale, { threshold: ">12h" });

    popupContent += `<div style="background: ${popupTheme.bg}; padding: 8px; margin: 8px 0; border-radius: 4px; border-left: 3px solid ${popupTheme.border};">`;
    popupContent += `<div style="font-weight: 600; margin-bottom: 6px; color: ${popupTheme.headingColor}; font-size: 0.95em;">${popupTheme.headerText}</div>`;

    // Wave Height (prominent display)
    if (obs.sea_height_ft !== null) {
      popupContent += `<div style="background: linear-gradient(135deg, var(--color-callout-gradient-start), var(--color-callout-gradient-end)); color: var(--color-on-primary); padding: 6px 8px; border-radius: 4px; margin-bottom: 6px; text-align: center; font-weight: 600;">`;
      popupContent += `🌊 Wave Height: ${obs.sea_height_ft} ft`;
      popupContent += `</div>`;
    }

    // Wind
    if (!obs.wind_calm) {
      const windText = `${obs.wind_direction || "N/A"} ${obs.wind_speed_kt || "N/A"} kt${obs.wind_gusting ? " (gusting)" : ""}${obs.wind_estimated ? " (est)" : ""}`;
      popupContent += `<div style="margin: 4px 0;"><strong>💨 Wind:</strong> ${windText}</div>`;
    } else {
      popupContent += `<div style="margin: 4px 0;"><strong>💨 Wind:</strong> CALM</div>`;
    }

    // Sea condition (if available, separate from height)
    if (obs.sea_condition) {
      popupContent += `<div style="margin: 4px 0;"><strong>🌊 Sea Condition:</strong> ${obs.sea_condition}</div>`;
    }

    // Swell
    if (obs.swell_intensity || obs.swell_direction) {
      const swellText = `${obs.swell_intensity || ""} ${obs.swell_direction || ""} swell`.trim();
      popupContent += `<div style="margin: 4px 0;"><strong>〰️ Swell:</strong> ${swellText || "N/A"}</div>`;
    }

    // Report time (with full date, day of week, and age in 24h format)
    if (obs.observation_time) {
      const formattedDate = formatWeekdayDayTime(obs.observation_time);
      const ageText = ` (${getShortAgeString(new Date(obs.observation_time))})`;

      popupContent += `<div style="font-size: 0.85em; color: var(--color-text-light); margin-top: 6px; padding-top: 4px; border-top: 1px solid var(--color-callout-info-divider, rgba(0,75,124,0.2));">📅 Report: ${formattedDate}${ageText}</div>`;
    } else if (obs.report_time_str) {
      popupContent += `<div style="font-size: 0.85em; color: var(--color-text-light); margin-top: 6px; padding-top: 4px; border-top: 1px solid var(--color-callout-info-divider, rgba(0,75,124,0.2));">📅 Report: ${obs.report_time_str}</div>`;
    }

    // Staleness warning (already shown in header, but keep for emphasis)
    if (obs.stale) {
      popupContent += staleDataWarningHTML();
    }

    popupContent += `</div>`;
  } else {
    popupContent += `<div style="background: var(--color-surface-alt, #f5f5f5); padding: 8px; margin: 8px 0; border-radius: 4px; border-left: 3px solid var(--color-text-muted, #999); color: var(--color-text-muted); font-size: 0.9em;">No current data — this station is not reporting in the FPCN61 bulletin.</div>`;
  }

  // Station details
  popupContent += `
    <div style="font-size: 0.9em; line-height: 1.4;">
      <div><strong>ID:</strong> ${lightstation.id}</div>
      <div><strong>Location:</strong> ${lightstation.location}</div>
      <div><strong>Region:</strong> ${lightstation.region}</div>
      <div><strong>Source:</strong> ${lightstation.source}</div>
      <div><strong>Type:</strong> Lightstation</div>
      <div><strong>Coordinates:</strong> ${lightstation.lat.toFixed(4)}, ${lightstation.lon.toFixed(4)}</div>
      ${lightstation.established ? `<div><strong>Established:</strong> ${lightstation.established}</div>` : ""}
      ${lightstation.notes ? `<div style="font-style: italic; margin-top: 4px; color: var(--color-text-muted);">${lightstation.notes}</div>` : ""}
    </div>
    <div class="popup-actions">
      <a href="/lightstations.html#lightstation-${lightstation.id}" class="view-data-btn">View Data →</a>
    </div>
  </div>`;

  marker.bindPopup(popupContent, getPopupOptions());
  marker.addTo(markersLayer);

  // Store marker reference for later access
  lightstationMarkers[lightstation.id] = marker;
}

// Add webcam marker to map
function addWebcamMarker(webcam) {
  const icon = L.divIcon({
    className: "station-marker webcam-marker",
    html: createWebcamSVG(),
    iconSize: [28, 28],
    iconAnchor: [14, 28],
    popupAnchor: [0, -28],
  });

  const marker = L.marker([webcam.lat, webcam.lon], {
    icon: icon,
    title: `${webcam.name} webcam`,
  });

  // Build popup
  let popupContent = `<div class="station-popup">`;
  popupContent += `<h3>📹 ${webcam.name}</h3>`;

  // Webcam info
  popupContent += `<div style="background: var(--color-callout-info-bg, #f0f8ff); padding: 8px; margin: 8px 0; border-radius: 4px; border-left: 3px solid var(--color-primary-dark);">`;
  popupContent += `<div style="font-weight: 600; margin-bottom: 6px;">Webcam Details:</div>`;
  popupContent += `<div><strong>📍 Location:</strong> ${webcam.location}</div>`;
  popupContent += `<div><strong>🔄 Updates:</strong> Every ${webcam.update_frequency_minutes} minutes</div>`;
  popupContent += `<div><strong>⏱️ Stream Delay:</strong> ~${webcam.stream_delay_minutes} min</div>`;
  popupContent += `<div><strong>📡 Source:</strong> ${webcam.source}</div>`;
  popupContent += `</div>`;

  // Station details
  popupContent += `
    <div style="font-size: 0.9em; line-height: 1.4; margin-top: 8px;">
      <div><strong>ID:</strong> ${webcam.id}</div>
      <div><strong>Type:</strong> Webcam</div>
      <div><strong>Coordinates:</strong> ${webcam.lat.toFixed(4)}, ${webcam.lon.toFixed(4)}</div>
    </div>
    <div class="popup-actions">
      <a href="${webcam.page_url}" class="view-data-btn view-data-btn--alt">View Webcam →</a>
    </div>
  </div>`;

  marker.bindPopup(popupContent, getPopupOptions());
  marker.addTo(markersLayer);

  // Store marker reference for later access
  webcamMarkers[webcam.id] = marker;
}

// Fallback station data if fetch fails
function loadFallbackStations() {
  // Hardcoded fallback stations (will be replaced by fetch in production)
  const fallbackBuoys = [
    {
      id: "4600146",
      name: "Halibut Bank",
      location: "Off Vancouver",
      lat: 49.337,
      lon: -123.731,
      source: "Environment Canada",
      type: "wave_buoy",
      data_types: ["wave_height", "wind_speed", "air_temp"],
    },
    {
      id: "4600303",
      name: "Southern Georgia Strait",
      location: "Southern Strait",
      lat: 48.833,
      lon: -123.417,
      source: "Environment Canada",
      type: "wave_buoy",
      data_types: ["wave_height", "wind_speed", "air_temp"],
    },
    {
      id: "4600304",
      name: "English Bay",
      location: "Vancouver Harbor",
      lat: 49.291,
      lon: -123.181,
      source: "Environment Canada",
      type: "wave_buoy",
      data_types: ["wave_height", "wind_speed", "air_temp"],
    },
    {
      id: "4600131",
      name: "Sentry Shoal",
      location: "Northern Strait of Georgia",
      lat: 49.917,
      lon: -124.917,
      source: "Environment Canada",
      type: "wave_buoy",
      data_types: ["wave_height", "wind_speed", "air_temp"],
    },
    {
      id: "46087",
      name: "Neah Bay",
      location: "Cape Flattery, WA",
      lat: 48.495,
      lon: -124.728,
      source: "NOAA NDBC",
      type: "wave_buoy",
      data_types: ["wave_height", "wind_speed", "swell_height"],
    },
    {
      id: "46088",
      name: "New Dungeness",
      location: "Hein Bank",
      lat: 48.333,
      lon: -123.167,
      source: "NOAA NDBC",
      type: "wave_buoy",
      data_types: ["wave_height", "wind_speed", "swell_height"],
    },
    {
      id: "CRPILE",
      name: "Crescent Beach Ocean",
      location: "Crescent Beach, Surrey",
      lat: 49.0122,
      lon: -122.9411,
      source: "Surrey FlowWorks",
      type: "pile_mounted_wave_station",
      data_types: ["wave_height", "wind_speed", "air_temp", "sea_temp", "water_level_geodetic"],
      provides_tide: true,
    },
    {
      id: "CRCHAN",
      name: "Crescent Channel",
      location: "Boundary Bay Channel Marker",
      lat: 49.0536,
      lon: -122.8969,
      source: "Surrey FlowWorks",
      type: "pile_mounted_wave_station",
      data_types: ["wave_height", "wind_speed", "air_temp", "water_level_geodetic"],
      provides_tide: true,
    },
    {
      id: "COLEB",
      name: "Colebrook",
      location: "Colebrook Pump House",
      lat: 49.0858,
      lon: -122.845,
      source: "Surrey FlowWorks",
      type: "wind_monitoring_station",
      data_types: ["wind_speed", "air_temp"],
    },
  ];

  const fallbackTides = {
    point_atkinson: {
      code: "07795",
      name: "Point Atkinson",
      location: "West Vancouver",
      lat: 49.3375,
      lon: -123.253583,
      source: "DFO IWLS",
      series: ["wlo", "wlp"],
      data_types: ["water_level_observed", "water_level_predicted"],
    },
    kitsilano: {
      code: "07707",
      name: "Kitsilano",
      location: "Vancouver",
      lat: 49.276583,
      lon: -123.13936,
      source: "DFO IWLS",
      series: ["wlo", "wlp"],
      data_types: ["water_level_observed", "water_level_predicted"],
    },
  };

  fallbackBuoys.forEach((buoy) => addBuoyMarker(buoy));
  Object.entries(fallbackTides).forEach(([key, tide]) => addTideMarker(tide, key));
  refreshWindLabels();
}

// Center map on specific buoy and open popup
export function centerMapOnBuoy(buoyId, retryCount = 0) {
  if (!stationsMap || !buoyMarkers[buoyId]) {
    // Retry up to 5 times with 500ms delay
    if (retryCount < 5) {
      logger.debug(
        "StationsMap",
        `Waiting for buoy marker ${buoyId}... (attempt ${retryCount + 1}/5)`,
      );
      setTimeout(() => centerMapOnBuoy(buoyId, retryCount + 1), 500);
      return;
    }
    logger.warn("StationsMap", `Map or marker not ready for buoy ${buoyId}`);
    return;
  }

  const marker = buoyMarkers[buoyId];
  const latlng = marker.getLatLng();

  // Center map on buoy with animation
  stationsMap.setView(latlng, 10, {
    animate: true,
    duration: 1.0,
  });

  // Open popup after centering
  setTimeout(() => {
    marker.openPopup();
  }, 1100);
}

// Center map on specific tide station and open popup
function centerMapOnTide(stationKey, retryCount = 0) {
  if (!stationsMap || !tideMarkers[stationKey]) {
    // Retry up to 5 times with 500ms delay
    if (retryCount < 5) {
      logger.debug(
        "StationsMap",
        `Waiting for tide marker ${stationKey}... (attempt ${retryCount + 1}/5)`,
      );
      setTimeout(() => centerMapOnTide(stationKey, retryCount + 1), 500);
      return;
    }
    logger.warn("StationsMap", `Map or marker not ready for tide station ${stationKey}`);
    return;
  }

  const marker = tideMarkers[stationKey];
  const latlng = marker.getLatLng();

  // Center map on tide station with animation
  stationsMap.setView(latlng, 10, {
    animate: true,
    duration: 1.0,
  });

  // Open popup after centering
  setTimeout(() => {
    marker.openPopup();
  }, 1100);
}

// Show selected buoy on map from dropdown
export function showSelectedBuoyOnMap(event) {
  event.preventDefault();

  const select = document.getElementById("chart-buoy-select");
  if (!select) return;

  const buoyId = select.value;
  if (!buoyId) return;

  // Center map on selected buoy
  centerMapOnBuoy(buoyId);

  // Scroll to map section smoothly
  const mapSection = document.getElementById("stations-map");
  if (mapSection) {
    mapSection.scrollIntoView({ behavior: "smooth", block: "center" });
  }
}

// Show selected surge station on map from any dropdown
function showSurgeStationOnMap(surgeStationName, scrollToMap = true) {
  const marker = SURGE_TO_MARKER_MAP[surgeStationName];
  if (!marker) {
    logger.warn("StationsMap", `No map marker found for surge station: ${surgeStationName}`);
    return;
  }

  // Center map on the appropriate marker
  if (marker.type === "buoy") {
    centerMapOnBuoy(marker.id);
  } else if (marker.type === "tide") {
    centerMapOnTide(marker.id);
  }

  // Scroll to map section if requested
  if (scrollToMap) {
    const mapSection = document.getElementById("stations-map");
    if (mapSection) {
      mapSection.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }
}

// Show selected surge station on map from index.html surge selector
export function showSelectedSurgeOnMap(event) {
  if (event) event.preventDefault();

  const select = document.getElementById("surge-station-select");
  if (!select || !select.value) return;

  showSurgeStationOnMap(select.value, true);
}

// Check URL hash for station to show on map
function checkHashForStation() {
  const hash = window.location.hash;

  if (hash.startsWith("#tide-")) {
    const stationKey = hash.substring(6); // Remove '#tide-'
    // Short delay to ensure map starts initializing, then retry logic kicks in
    setTimeout(() => {
      centerMapOnTide(stationKey);

      // Scroll to map section
      const mapSection = document.getElementById("stations-map");
      if (mapSection) {
        mapSection.scrollIntoView({ behavior: "smooth", block: "center" });
      }
    }, 500);
  } else if (hash.startsWith("#buoy-")) {
    const buoyId = hash.substring(6); // Remove '#buoy-'
    setTimeout(() => {
      centerMapOnBuoy(buoyId);

      // Scroll to map section
      const mapSection = document.getElementById("stations-map");
      if (mapSection) {
        mapSection.scrollIntoView({ behavior: "smooth", block: "center" });
      }
    }, 500);
  } else if (hash.startsWith("#surge-")) {
    const surgeStation = hash.substring(7); // Remove '#surge-'
    setTimeout(() => {
      showSurgeStationOnMap(surgeStation, true);
    }, 500);
  }
}

// Initialize map when DOM is ready
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => {
    initStationsMap();
    checkHashForStation();
  });
} else {
  initStationsMap();
  checkHashForStation();
}
