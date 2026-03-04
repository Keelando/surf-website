/**
 * BC Lightstation Map
 * Displays all BC lighthouse stations on an interactive Leaflet map
 */

let lightstationMap = null;
let lightstationMarkersLayer = null;
let lightstationMapMarkers = {}; // Store lightstation markers by ID for easy access
let latestLightstationData = null; // Cache for latest lightstation observations

// Helper function to convert cardinal direction to degrees
function cardinalToDegrees(cardinal) {
  if (!cardinal) return null;
  const directions = {
    'N': 0, 'NORTH': 0,
    'NNE': 22.5, 'NORTH-NORTHEAST': 22.5,
    'NE': 45, 'NORTHEAST': 45,
    'ENE': 67.5, 'EAST-NORTHEAST': 67.5,
    'E': 90, 'EAST': 90,
    'ESE': 112.5, 'EAST-SOUTHEAST': 112.5,
    'SE': 135, 'SOUTHEAST': 135,
    'SSE': 157.5, 'SOUTH-SOUTHEAST': 157.5,
    'S': 180, 'SOUTH': 180,
    'SSW': 202.5, 'SOUTH-SOUTHWEST': 202.5,
    'SW': 225, 'SOUTHWEST': 225,
    'WSW': 247.5, 'WEST-SOUTHWEST': 247.5,
    'W': 270, 'WEST': 270,
    'WNW': 292.5, 'WEST-NORTHWEST': 292.5,
    'NW': 315, 'NORTHWEST': 315,
    'NNW': 337.5, 'NORTH-NORTHWEST': 337.5
  };
  return directions[cardinal.toUpperCase()] ?? null;
}

/**
 * Create directional marker with triangular arrow for map markers
 * @param {number} direction - Direction in degrees (meteorological: coming FROM)
 * @param {number} speed - Wind speed in knots
 * @returns {string} HTML for marker
 */
function createDirectionalMarker(direction, speed) {
  const arrowColor = '#dc2626'; // Red for wind

  // Meteorological convention: direction value = where wind is COMING FROM
  // Arrow shows direction wind is TRAVELING TO
  const rotation = direction;

  // Build speed label if available
  const speedLabel = (speed !== null && speed !== undefined)
    ? `<div style="
        background: ${arrowColor};
        color: white;
        padding: 2px 5px;
        border-radius: 3px;
        font-size: 13px;
        font-weight: bold;
        white-space: nowrap;
        box-shadow: 0 1px 2px rgba(0,0,0,0.3);
        margin-bottom: -3px;
      ">${Math.round(speed)}kt</div>`
    : '';

  return `
    <div style="display: flex; flex-direction: column; align-items: center; justify-content: center;">
      ${speedLabel}
      <div style="transform: rotate(${rotation}deg); transform-origin: center center;">
        <svg width="26" height="30" viewBox="-6 -10 12 24" style="filter: drop-shadow(0 2px 3px rgba(0,0,0,0.5));">
          <path d="M0,12 L-5,-8 L0,-5 L5,-8 Z" fill="${arrowColor}" fill-opacity="0.98" stroke="${arrowColor}" stroke-width="1.5"/>
        </svg>
      </div>
    </div>
  `;
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

// Initialize the lightstation map
function initLightstationMap() {
  // Create map centered on Salish Sea and Vancouver Island
  lightstationMap = L.map('lightstation-map', {
    center: [49.5, -125.0], // Center on Salish Sea/Vancouver Island region
    zoom: 7,
    scrollWheelZoom: true,
    zoomControl: true
  });

  // Add OpenStreetMap tiles
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    maxZoom: 19
  }).addTo(lightstationMap);

  // Create layer group for markers
  lightstationMarkersLayer = L.layerGroup().addTo(lightstationMap);

  // Listen to zoom events to show/hide labels based on zoom level
  lightstationMap.on('zoomend', toggleLightstationLabels);

  // Load lightstation stations and add markers
  loadLightstationsAndMarkers();
}

// Toggle lightstation labels based on zoom level
function toggleLightstationLabels() {
  const zoomLevel = lightstationMap.getZoom();
  const showLabels = zoomLevel >= 8; // Show labels when zoomed in to level 8 or higher

  // Toggle CSS class on all lightstation tooltips
  const tooltips = document.querySelectorAll('.lightstation-label');
  tooltips.forEach(tooltip => {
    if (showLabels) {
      tooltip.style.opacity = '1';
      tooltip.style.visibility = 'visible';
    } else {
      tooltip.style.opacity = '0';
      tooltip.style.visibility = 'hidden';
    }
  });
}

// Load stations.json and add lightstation markers
async function loadLightstationsAndMarkers() {
  try {
    // Fetch latest lightstation observations
    try {
      const obsResponse = await fetch('/data/latest_lightstation.json');
      latestLightstationData = await obsResponse.json();
    } catch (err) {
      console.warn('Could not fetch latest lightstation data:', err);
    }

    // Fetch stations metadata
    const response = await fetch('/data/stations.json');
    const stations = await response.json();

    // Add lighthouse station markers
    if (stations.lightstations) {
      Object.values(stations.lightstations).forEach(lightstation => {
        addLightstationMapMarker(lightstation);
      });

      // Set initial label visibility based on current zoom level
      setTimeout(() => {
        toggleLightstationLabels();
      }, 100);
    }
  } catch (error) {
    console.error('Error loading lightstation data:', error);
  }
}

// Add lightstation marker to map
function addLightstationMapMarker(lightstation) {
  // Always use lighthouse icon for lightstations
  const icon = L.divIcon({
    className: 'station-marker lightstation-marker',
    html: createLighthouseSVG(),
    iconSize: [28, 32],
    iconAnchor: [14, 32],
    popupAnchor: [0, -32]
  });

  const marker = L.marker([lightstation.lat, lightstation.lon], { icon: icon });

  // Build popup
  let popupContent = `<div class="station-popup"><h3>${lightstation.name}</h3>`;

  // Add latest observations if available
  // Convert station ID (e.g., "ADDENBROKE_ISLAND") to match JSON format (e.g., "ADDENBROKE ISLAND")
  const lookupName = lightstation.id.replace(/_/g, ' ');
  if (latestLightstationData && latestLightstationData[lookupName]) {
    const obs = latestLightstationData[lookupName];
    const isStale = obs.stale || false;
    const bgColor = isStale ? '#fff5f5' : '#f0f8ff';
    const borderColor = isStale ? '#e53935' : '#0077be';
    const headerText = isStale ? 'Latest Conditions (STALE - >12h old):' : 'Latest Conditions:';
    const headerColor = isStale ? '#c62828' : '#004b7c';

    popupContent += `<div style="background: ${bgColor}; padding: 8px; margin: 8px 0; border-radius: 4px; border-left: 3px solid ${borderColor};">`;
    popupContent += `<div style="font-weight: 600; margin-bottom: 6px; color: ${headerColor}; font-size: 0.95em;">${headerText}</div>`;

    // Wave Height (prominent display)
    if (obs.sea_height_ft !== null) {
      popupContent += `<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 6px 8px; border-radius: 4px; margin-bottom: 6px; text-align: center; font-weight: 600;">`;
      popupContent += `🌊 Wave Height: ${obs.sea_height_ft} ft`;
      popupContent += `</div>`;
    }

    // Wind
    if (!obs.wind_calm) {
      const windText = `${obs.wind_direction || 'N/A'} ${obs.wind_speed_kt || 'N/A'} kt${obs.wind_gusting ? ' (gusting)' : ''}${obs.wind_estimated ? ' (est)' : ''}`;
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
      const swellText = `${obs.swell_intensity || ''} ${obs.swell_direction || ''} swell`.trim();
      popupContent += `<div style="margin: 4px 0;"><strong>〰️ Swell:</strong> ${swellText || 'N/A'}</div>`;
    }

    // Report time (with full date, day of week, and age in 24h format)
    if (obs.observation_time) {
      const obsDate = new Date(obs.observation_time);
      const dateOptions = {
        timeZone: 'America/Vancouver',
        weekday: 'long',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false
      };
      const formattedDate = obsDate.toLocaleString('en-US', dateOptions).replace(',', '');

      // Calculate age
      const now = new Date();
      const ageMs = now - obsDate;
      const ageDays = Math.floor(ageMs / (1000 * 60 * 60 * 24));
      const ageHours = Math.floor(ageMs / (1000 * 60 * 60));
      const ageMinutes = Math.floor((ageMs % (1000 * 60 * 60)) / (1000 * 60));

      let ageText = '';
      if (ageDays >= 1) {
        ageText = ageDays === 1 ? ' (1 day ago)' : ` (${ageDays} days ago)`;
      } else if (ageHours > 0) {
        ageText = ` (${ageHours}h ago)`;
      } else if (ageMinutes > 0) {
        ageText = ` (${ageMinutes}m ago)`;
      } else {
        ageText = ' (just now)';
      }

      popupContent += `<div style="font-size: 0.85em; color: #555; margin-top: 6px; padding-top: 4px; border-top: 1px solid rgba(0,75,124,0.2);">📅 Report: ${formattedDate}${ageText}</div>`;
    } else if (obs.report_time_str) {
      popupContent += `<div style="font-size: 0.85em; color: #555; margin-top: 6px; padding-top: 4px; border-top: 1px solid rgba(0,75,124,0.2);">📅 Report: ${obs.report_time_str}</div>`;
    }

    // Staleness warning (already shown in header, but keep for emphasis)
    if (obs.stale) {
      popupContent += `<div style="color: #c53030; font-size: 0.85em; margin-top: 4px; font-weight: 600;">⚠️ STALE DATA</div>`;
    }

    popupContent += `</div>`;
  }

  // Station details
  popupContent += `
    <div style="font-size: 0.9em; line-height: 1.4;">
      <div><strong>ID:</strong> ${lightstation.id}</div>
      <div><strong>Location:</strong> ${lightstation.location}</div>
      <div><strong>Region:</strong> ${lightstation.region}</div>
      <div><strong>Source:</strong> ${lightstation.source}</div>
      <div><strong>Type:</strong> Lightstation</div>
      ${lightstation.established ? `<div><strong>Established:</strong> ${lightstation.established}</div>` : ''}
      ${lightstation.notes ? `<div style="font-style: italic; margin-top: 4px; color: #666;">${lightstation.notes}</div>` : ''}
    </div>
    <a href="#lightstation-data-table-section" class="view-data-btn" onclick="viewLightstationData('${lightstation.id}')" style="display: inline-block; margin-top: 8px; padding: 6px 12px; background: #0077be; color: white; text-decoration: none; border-radius: 4px; font-size: 0.9em;">View Data →</a>
  </div>`;

  marker.bindPopup(popupContent);

  // Add permanent label (station name) that shows/hides based on zoom
  marker.bindTooltip(lightstation.name, {
    permanent: true,
    direction: 'top',
    className: 'lightstation-label',
    offset: [0, -30]
  });

  marker.addTo(lightstationMarkersLayer);

  // Store marker reference for later access
  lightstationMapMarkers[lightstation.id] = marker;
}

// Center map on specific lightstation and open popup
function centerMapOnLightstation(lightstationId, retryCount = 0) {
  if (!lightstationMap || !lightstationMapMarkers[lightstationId]) {
    // Retry up to 5 times with 500ms delay
    if (retryCount < 5) {
      setTimeout(() => centerMapOnLightstation(lightstationId, retryCount + 1), 500);
      return;
    }
    console.warn(`Map or marker not ready for lightstation ${lightstationId}`);
    return;
  }

  const marker = lightstationMapMarkers[lightstationId];
  const latlng = marker.getLatLng();

  // Center map on lightstation with animation
  lightstationMap.setView(latlng, 10, {
    animate: true,
    duration: 1.0
  });

  // Open popup after centering
  setTimeout(() => {
    marker.openPopup();
  }, 1100);
}

// Make functions globally accessible
window.centerMapOnLightstation = centerMapOnLightstation;

// Function to view lightstation data (called from map popups)
function viewLightstationData(lightstationId) {
  // Find the lightstation name from the ID
  const select = document.getElementById('lightstation-station-select');
  if (!select) return;

  // Convert ID to match dropdown value format (uppercase with spaces)
  // e.g., "CHROME_ISLAND" → "CHROME ISLAND"
  const stationName = lightstationId.replace(/_/g, ' ');

  // Check if station exists in timeseries data
  if (!window.lightstationTimeseriesData || !window.lightstationTimeseriesData[stationName]) {
    // Station doesn't have 24hr data - show alert instead of scrolling
    alert(`${stationName} does not have data from the past 24 hours.\n\nMost recent observation may be older than 24 hours.`);
    return;
  }

  // Select the station in dropdown (value matches the uppercase format)
  select.value = stationName;

  // Trigger chart render if the function exists
  if (typeof window.renderLightstationCharts === 'function') {
    window.renderLightstationCharts(stationName);
  }

  // Scroll to chart section
  const chartSection = document.getElementById('lightstation-data-table-section');
  if (chartSection) {
    chartSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

// Make function globally accessible
window.viewLightstationData = viewLightstationData;

// Initialize map when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    initLightstationMap();
  });
} else {
  initLightstationMap();
}
