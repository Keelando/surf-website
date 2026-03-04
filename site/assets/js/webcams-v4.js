/* ==========================================================================
   Webcams Page - Main JavaScript
   ========================================================================== */

// ==========================================================================
// Configuration
// ==========================================================================

const webcamRegions = {
  'english_bay': { name: 'English Bay - Vancouver' },
  'salish_sea_south': { name: 'Salish Sea - Boundary Bay - White Rock' },
  'west_coast_vi': { name: 'West Coast Vancouver Island (Tofino)' }
};

const webcams = [
  {
    id: 'ambleside',
    region: 'english_bay',
    name: 'Ambleside Beach',
    location: 'West Vancouver, BC',
    dataUrl: '/data/ambleside/latest.json',
    imageUrl: '/data/ambleside/latest.jpg',
    slideshowUrl: '/data/ambleside/slideshow_manifest.json',
    slideshowPath: '/data/ambleside/',
    updateInterval: 20,
    streamDelay: 1,
    daylightOnly: true,
    attribution: {
      text: 'Webcam screenshots provided by Hollyburn Sailing Club',
      url: 'https://www.hollyburnsailingclub.ca/'
    },
    conditions: [
      { label: 'English Bay Buoy', buoyStation: '4600304', fields: ['wind', 'waves'] },
      { label: 'Jericho Sailing Centre', windStation: 'JERICHO', fields: ['wind'] }
    ]
  },
  {
    id: 'whiterock',
    region: 'salish_sea_south',
    name: 'White Rock Pier Cam',
    location: 'White Rock, BC',
    dataUrl: '/data/wrcam/latest.json',
    imageUrl: '/data/wrcam/latest.jpg',
    slideshowUrl: '/data/wrcam/slideshow_manifest.json',
    slideshowPath: '/data/wrcam/',
    updateInterval: 10,
    streamDelay: 6,
    conditions: [
      { label: 'White Rock East Beach', customStation: 'whiterock_east', fields: ['wind_speed_only'] },
      { label: 'Crescent Pile', buoyStation: 'CRPILE', fields: ['wind', 'waves'] }
    ]
  },
  {
    id: 'boundarybay',
    region: 'salish_sea_south',
    name: 'White Rock East Beach',
    location: 'White Rock, BC',
    dataUrl: '/data/bbcam/latest.json',
    imageUrl: '/data/bbcam/latest.jpg',
    slideshowUrl: '/data/bbcam/slideshow_manifest.json',
    slideshowPath: '/data/bbcam/',
    updateInterval: 10,
    streamDelay: 20,
    downNotice: 'YouTube livestream temporarily unavailable. Images shown are from last successful capture.',
    conditions: [
      { label: 'White Rock East Beach', customStation: 'whiterock_east', fields: ['wind_speed_only'] },
      { label: 'Crescent Pile', buoyStation: 'CRPILE', fields: ['wind', 'waves'] }
    ]
  },
  {
    id: 'mudbay',
    region: 'salish_sea_south',
    name: 'Mud Bay HD',
    location: 'South Surrey, BC',
    dataUrl: '/data/mudbay/latest.json',
    imageUrl: '/data/mudbay/latest.jpg',
    slideshowUrl: '/data/mudbay/slideshow_manifest.json',
    slideshowPath: '/data/mudbay/',
    updateInterval: 30,
    streamDelay: null,
    daylightOnly: true,
    conditions: [
      { label: 'White Rock East Beach', customStation: 'whiterock_east', fields: ['wind_speed_only'] },
      { label: 'Crescent Pile', buoyStation: 'CRPILE', fields: ['wind', 'waves'] }
    ]
  },
  {
    id: 'coxbay',
    region: 'west_coast_vi',
    name: 'Cox Bay',
    location: 'Tofino, BC',
    dataUrl: '/data/coxbay/latest.json',
    imageUrl: '/data/coxbay/latest.jpg',
    slideshowUrl: '/data/coxbay/slideshow_manifest.json',
    slideshowPath: '/data/coxbay/',
    updateInterval: 15,
    streamDelay: 20,
    daylightOnly: true,
    conditions: [
      { label: 'La Perouse Bank', buoyStation: '4600206', fields: ['wind', 'waves_detailed'] }
    ]
  }
];

// ==========================================================================
// State
// ==========================================================================

let cachedMarineData = null;
const slideshowState = {};

// ==========================================================================
// Utility Functions
// ==========================================================================

function formatTimestamp(isoString) {
  return new Date(isoString).toLocaleString('en-US', {
    timeZone: 'America/Vancouver',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZoneName: 'short'
  });
}

function formatShortTimestamp(isoString) {
  return new Date(isoString).toLocaleString('en-US', {
    timeZone: 'America/Vancouver',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
}

function createElement(tag, className, content) {
  const el = document.createElement(tag);
  if (className) el.className = className;
  if (content) el.textContent = content;
  return el;
}

// ==========================================================================
// Data Fetching
// ==========================================================================

async function fetchMarineData() {
  if (cachedMarineData) return cachedMarineData;

  try {
    const [buoyResponse, windResponse, whiterockResponse] = await Promise.all([
      fetch('/data/latest_buoy_v2.json'),
      fetch('/data/latest_wind.json'),
      fetch('/data/whiterock_weather.json')
    ]);

    cachedMarineData = {
      buoy: buoyResponse.ok ? await buoyResponse.json() : {},
      wind: windResponse.ok ? await windResponse.json() : {},
      custom: {
        whiterock_east: whiterockResponse.ok ? await whiterockResponse.json() : null
      }
    };
    return cachedMarineData;
  } catch (error) {
    console.error('Failed to fetch marine data:', error);
    return { buoy: {}, wind: {}, custom: {} };
  }
}

function isWebcamStale(metadata, updateInterval) {
  if (!metadata?.timestamp) return false;

  const now = Date.now();
  const lastUpdate = new Date(metadata.timestamp).getTime();
  const ageMinutes = (now - lastUpdate) / (1000 * 60);

  // Consider stale if older than 3x the update interval (e.g., 30 min for 10-min interval)
  const staleThreshold = updateInterval * 3;
  return ageMinutes > staleThreshold;
}

function formatAge(ageMinutes) {
  if (ageMinutes >= 2880) {
    // >= 48 hours: show days
    const days = Math.round(ageMinutes / 1440);
    return `${days} day${days !== 1 ? 's' : ''} ago`;
  } else if (ageMinutes >= 120) {
    // >= 2 hours but < 48 hours: show hours
    const hours = Math.round(ageMinutes / 60);
    return `${hours} hour${hours !== 1 ? 's' : ''} ago`;
  }
  // < 2 hours: show minutes
  return `${Math.round(ageMinutes)} min ago`;
}

async function loadWebcamMetadata(webcam, card) {
  try {
    const response = await fetch(webcam.dataUrl);
    const metadata = await response.json();

    if (card) {
      const timestampEl = card.querySelector('.webcam-timestamp');
      if (timestampEl) {
        const stale = isWebcamStale(metadata, webcam.updateInterval || 10);
        const now = Date.now();
        const lastUpdate = new Date(metadata.timestamp).getTime();
        const ageMinutes = Math.round((now - lastUpdate) / (1000 * 60));

        let timestampText = 'Last updated: ' + formatTimestamp(metadata.timestamp);

        if (stale) {
          const ageText = formatAge(ageMinutes);
          timestampText += ` <span class="stale-indicator" title="Image is ${ageMinutes} minutes old">⚠️ STALE (${ageText})</span>`;
          card.classList.add('webcam-stale');
        } else {
          card.classList.remove('webcam-stale');
        }

        timestampEl.innerHTML = timestampText;
      }
    }
    return metadata;
  } catch (error) {
    console.error(`Failed to load metadata for ${webcam.name}:`, error);
    return null;
  }
}

// ==========================================================================
// SVG Arrow Components (Unified)
// ==========================================================================

const ARROW_COLORS = {
  wind: '#dc2626',
  wave: '#2563eb'
};

function createDirectionalArrow(degrees, type = 'wind') {
  const color = ARROW_COLORS[type] || ARROW_COLORS.wind;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('width', '20');
  svg.setAttribute('height', '20');
  svg.setAttribute('viewBox', '-6 -10 12 24');
  svg.style.transform = `rotate(${degrees}deg)`;

  const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  path.setAttribute('d', 'M0,12 L-5,-8 L0,-5 L5,-8 Z');
  path.setAttribute('fill', color);
  path.setAttribute('stroke', color);
  path.setAttribute('stroke-width', '1.5');

  svg.appendChild(path);
  return svg;
}

function createAngularSpreadVector(avgDirection, spread, size = 70) {
  if (avgDirection == null || spread == null) return null;

  const halfSpread = spread / 2;
  const cx = size / 2;
  const cy = size / 2;
  const radius = size * 0.42;

  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('width', size);
  svg.setAttribute('height', size);
  svg.setAttribute('viewBox', `0 0 ${size} ${size}`);
  svg.style.display = 'inline-block';
  svg.style.verticalAlign = 'middle';
  svg.style.marginLeft = '0.5rem';

  // Background circle
  const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
  circle.setAttribute('cx', cx);
  circle.setAttribute('cy', cy);
  circle.setAttribute('r', radius + 2);
  circle.setAttribute('fill', 'none');
  circle.setAttribute('stroke', '#e0e7ee');
  circle.setAttribute('stroke-width', '1');
  svg.appendChild(circle);

  // Spread sector arc
  const startAngleSVG = (avgDirection - halfSpread + 180) - 90;
  const endAngleSVG = (avgDirection + halfSpread + 180) - 90;
  const startRad = startAngleSVG * Math.PI / 180;
  const endRad = endAngleSVG * Math.PI / 180;
  const arcRadius = radius + 2;

  const x1 = cx + arcRadius * Math.cos(startRad);
  const y1 = cy + arcRadius * Math.sin(startRad);
  const x2 = cx + arcRadius * Math.cos(endRad);
  const y2 = cy + arcRadius * Math.sin(endRad);

  const arcPath = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  arcPath.setAttribute('d', `M ${cx},${cy} L ${x1},${y1} A ${arcRadius},${arcRadius} 0 ${spread > 180 ? 1 : 0},1 ${x2},${y2} Z`);
  arcPath.setAttribute('fill', 'rgba(30, 136, 229, 0.15)');
  arcPath.setAttribute('stroke', 'rgba(30, 136, 229, 0.3)');
  arcPath.setAttribute('stroke-width', '1');
  svg.appendChild(arcPath);

  // Main direction arrow
  const arrowGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
  arrowGroup.setAttribute('transform', `rotate(${avgDirection} ${cx} ${cy})`);

  const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
  line.setAttribute('x1', cx);
  line.setAttribute('y1', cy - radius + 8);
  line.setAttribute('x2', cx);
  line.setAttribute('y2', cy + radius - 3);
  line.setAttribute('stroke', '#1e88e5');
  line.setAttribute('stroke-width', '2.5');
  arrowGroup.appendChild(line);

  const arrowHead = document.createElementNS('http://www.w3.org/2000/svg', 'path');
  arrowHead.setAttribute('d', `M${cx},${cy + radius + 2} L${cx - 5},${cy + radius - 8} L${cx + 5},${cy + radius - 8} Z`);
  arrowHead.setAttribute('fill', '#1e88e5');
  arrowGroup.appendChild(arrowHead);

  svg.appendChild(arrowGroup);

  // Cardinal directions
  const addText = (x, y, text) => {
    const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    t.setAttribute('x', x);
    t.setAttribute('y', y);
    t.setAttribute('text-anchor', 'middle');
    t.setAttribute('font-size', '8');
    t.setAttribute('fill', '#999');
    t.textContent = text;
    svg.appendChild(t);
  };

  addText(cx, 8, 'N');
  addText(size - 6, cy + 3, 'E');
  addText(cx, size - 2, 'S');
  addText(6, cy + 3, 'W');

  return svg;
}

// ==========================================================================
// Condition Display Components
// ==========================================================================

function createWindDisplay(data, showLabel = false) {
  const windSpeed = data.wind_speed ?? data.wind_speed_kt;
  const windGust = data.wind_gust ?? data.wind_gust_kt;

  if (windSpeed == null || data.wind_direction == null) return null;

  const container = createElement('div', 'condition-section');

  if (showLabel) {
    container.appendChild(createElement('div', 'condition-section-label', 'Wind:'));
  }

  const windDiv = createElement('div', 'condition-wind');

  const arrow = createElement('span', 'condition-wind-arrow');
  arrow.appendChild(createDirectionalArrow(data.wind_direction, 'wind'));
  windDiv.appendChild(arrow);

  const details = createElement('div', 'condition-wind-details');
  details.innerHTML = `
    <span class="wind-cardinal">${data.wind_direction_cardinal || ''}</span>
    <span class="wind-degrees">(${Math.round(data.wind_direction)}°)</span>
    <span class="wind-speed">${windSpeed.toFixed(0)}</span>
    ${windGust ? `<span class="wind-gust">G ${windGust.toFixed(0)}</span>` : ''}
    <span class="wind-gust">kt</span>
  `;
  windDiv.appendChild(details);

  container.appendChild(windDiv);
  return container;
}

function createSimpleWaveDisplay(data) {
  if (data.wave_height_sig == null) return null;

  const container = createElement('div', 'condition-section');
  container.appendChild(createElement('div', 'condition-section-label', 'Waves:'));

  const waveDiv = createElement('div', 'condition-waves');
  waveDiv.innerHTML = `
    <span class="wave-icon">🌊</span>
    <div class="wave-details">
      <span class="wave-height">${data.wave_height_sig.toFixed(2)}m</span>
      ${data.wave_period_avg ? `<span class="wave-period">@ ${data.wave_period_avg.toFixed(1)}s</span>` : ''}
    </div>
  `;

  container.appendChild(waveDiv);
  return container;
}

function getSpreadDescription(spread, type = 'peak') {
  const thresholds = type === 'peak'
    ? { veryGood: 25, good: 35, moderate: 45 }
    : { veryGood: 30, good: 45, moderate: 60 };

  const labels = type === 'peak'
    ? { veryGood: 'very organized', good: 'organized', moderate: 'moderate', bad: 'confused' }
    : { veryGood: 'very clean', good: 'clean', moderate: 'mixed', bad: 'messy' };

  const colors = { veryGood: '#38a169', good: '#48bb78', moderate: '#d69e2e', bad: '#e53e3e' };

  let level = 'bad';
  if (spread < thresholds.veryGood) level = 'veryGood';
  else if (spread < thresholds.good) level = 'good';
  else if (spread < thresholds.moderate) level = 'moderate';

  return { label: labels[level], color: colors[level] };
}

function createDetailedWaveDisplay(data) {
  if (data.wave_height_sig == null) return null;

  const container = createElement('div', 'condition-section');
  container.appendChild(createElement('div', 'condition-section-label', 'Waves:'));

  const waveDiv = createElement('div', 'condition-waves');
  const waveDetails = createElement('div', 'wave-details-extended');

  // Icon section with direction arrow
  const iconSection = createElement('div', 'wave-icon-section');
  iconSection.innerHTML = '<span class="wave-icon">🌊</span>';

  if (data.wave_direction_peak != null) {
    const arrowContainer = createElement('span', 'wave-arrow-container');
    arrowContainer.appendChild(createDirectionalArrow(data.wave_direction_peak, 'wave'));
    iconSection.appendChild(arrowContainer);
  }
  waveDetails.appendChild(iconSection);

  // Data grid
  const dataGrid = createElement('div', 'wave-data-grid');

  // Significant height
  const sigMetric = createElement('div', 'wave-metric');
  sigMetric.innerHTML = `<span class="wave-label">Sig:</span> <span class="wave-value">${data.wave_height_sig.toFixed(1)}m @ ${data.wave_period_sig ? data.wave_period_sig.toFixed(1) + 's' : 'N/A'}</span>`;
  dataGrid.appendChild(sigMetric);

  // Peak height
  if (data.wave_height_max != null) {
    const peakMetric = createElement('div', 'wave-metric');
    peakMetric.innerHTML = `<span class="wave-label">Peak:</span> <span class="wave-value">${data.wave_height_max.toFixed(1)}m @ ${data.wave_period_peak ? data.wave_period_peak.toFixed(1) + 's' : 'N/A'}</span>`;
    dataGrid.appendChild(peakMetric);
  }

  // Direction
  if (data.wave_direction_peak != null) {
    const dirMetric = createElement('div', 'wave-metric');
    dirMetric.innerHTML = `<span class="wave-label">Dir:</span> <span class="wave-value">${data.wave_direction_peak_cardinal || ''} (${Math.round(data.wave_direction_peak)}°)</span>`;
    dataGrid.appendChild(dirMetric);

    // Peak spread
    if (data.wave_direction_spread_peak != null) {
      const peakDesc = getSpreadDescription(data.wave_direction_spread_peak, 'peak');
      const peakSpreadMetric = createElement('div', 'wave-metric');
      peakSpreadMetric.innerHTML = `<span class="wave-label">Peak Spread:</span> <span class="wave-value">${Math.round(data.wave_direction_spread_peak)}° <span style="color: ${peakDesc.color}; font-weight: 600;">(${peakDesc.label})</span> <span style="font-size: 0.85em; color: #666;">— dominant swell</span></span>`;
      dataGrid.appendChild(peakSpreadMetric);

      // Average spread
      if (data.wave_direction_spread_avg != null) {
        const avgDesc = getSpreadDescription(data.wave_direction_spread_avg, 'avg');
        const avgSpreadMetric = createElement('div', 'wave-metric');
        avgSpreadMetric.innerHTML = `<span class="wave-label">Avg Spread:</span> <span class="wave-value">${Math.round(data.wave_direction_spread_avg)}° <span style="color: ${avgDesc.color}; font-weight: 600;">(${avgDesc.label})</span> <span style="font-size: 0.85em; color: #666;">— all frequencies</span></span>`;
        dataGrid.appendChild(avgSpreadMetric);
      }

      // Visual spread vectors
      const vectorsContainer = createElement('div', 'wave-spread-vectors');

      const peakVectorDiv = createElement('div', 'wave-spread-vector');
      peakVectorDiv.appendChild(createElement('div', 'wave-spread-vector-label', 'Peak Spread'));
      const peakSvg = createAngularSpreadVector(data.wave_direction_peak, data.wave_direction_spread_peak, 70);
      if (peakSvg) peakVectorDiv.appendChild(peakSvg);
      peakVectorDiv.appendChild(createElement('div', 'wave-spread-vector-caption', 'Dominant swell'));
      vectorsContainer.appendChild(peakVectorDiv);

      if (data.wave_direction_spread_avg != null) {
        const avgVectorDiv = createElement('div', 'wave-spread-vector');
        avgVectorDiv.appendChild(createElement('div', 'wave-spread-vector-label', 'Average Spread'));
        const avgSvg = createAngularSpreadVector(data.wave_direction_peak, data.wave_direction_spread_avg, 70);
        if (avgSvg) avgVectorDiv.appendChild(avgSvg);
        avgVectorDiv.appendChild(createElement('div', 'wave-spread-vector-caption', 'All frequencies'));
        vectorsContainer.appendChild(avgVectorDiv);
      }

      dataGrid.appendChild(vectorsContainer);
    }
  }

  waveDetails.appendChild(dataGrid);
  waveDiv.appendChild(waveDetails);
  container.appendChild(waveDiv);
  return container;
}

function createConditionRow(label, data, fields) {
  if (!data || data.stale) return null;

  const row = createElement('div', 'condition-row');
  row.appendChild(createElement('div', 'condition-station-name', label));

  const dataDiv = createElement('div', 'condition-data');

  // Wind display
  if (fields.includes('wind') || fields.includes('wind_speed_only')) {
    const windDisplay = createWindDisplay(data, true);
    if (windDisplay) dataDiv.appendChild(windDisplay);
  }

  // Wave display
  if (fields.includes('waves')) {
    const waveDisplay = createSimpleWaveDisplay(data);
    if (waveDisplay) dataDiv.appendChild(waveDisplay);
  } else if (fields.includes('waves_detailed')) {
    const waveDisplay = createDetailedWaveDisplay(data);
    if (waveDisplay) dataDiv.appendChild(waveDisplay);
  }

  // Timestamp
  if (data.observation_time) {
    dataDiv.appendChild(createElement('div', 'condition-timestamp', formatShortTimestamp(data.observation_time)));
  }

  row.appendChild(dataDiv);
  return row;
}

function getStationData(condition, marineData) {
  if (condition.buoyStation) return marineData.buoy?.[condition.buoyStation];
  if (condition.customStation) return marineData.custom?.[condition.customStation];
  if (condition.windStation) return marineData.wind?.[condition.windStation];
  return null;
}

function createConditionsSection(conditions, marineData, id = null) {
  if (!marineData || !conditions?.length) return null;

  // Deduplicate conditions by station key
  const seen = new Set();
  const uniqueConditions = conditions.filter(c => {
    const key = c.buoyStation || c.customStation || c.windStation;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  const section = createElement('div', 'marine-conditions-banner region-conditions');
  if (id) section.id = id;

  section.appendChild(createElement('h3', null, 'Current Marine Conditions'));

  const stack = createElement('div', 'conditions-stack');

  uniqueConditions.forEach(condition => {
    const data = getStationData(condition, marineData);
    const row = createConditionRow(condition.label, data, condition.fields || []);
    if (row) stack.appendChild(row);
  });

  if (stack.children.length === 0) return null;

  section.appendChild(stack);
  return section;
}

// ==========================================================================
// Webcam Card
// ==========================================================================

async function createWebcamCard(webcam, metadata) {
  const card = createElement('div', 'webcam-card');
  card.id = webcam.id;
  card.dataset.webcamId = webcam.id;

  // Header
  const header = createElement('div', 'webcam-header');

  // Title with daylight indicator
  const title = createElement('h3');
  const titleText = document.createTextNode(webcam.name + ' ');
  title.appendChild(titleText);

  // Add sun/moon indicator
  const indicator = createElement('span', 'daylight-indicator');
  if (webcam.daylightOnly) {
    indicator.textContent = '🌞';
    indicator.title = 'Daylight only - screen grabs during daytime hours';
    indicator.style.fontSize = '0.8em';
  } else {
    indicator.textContent = '🌞🌙';
    indicator.title = '24/7 - screen grabs day and night';
    indicator.style.fontSize = '0.8em';
  }
  title.appendChild(indicator);

  header.appendChild(title);
  header.appendChild(createElement('p', 'webcam-location', webcam.location));
  card.appendChild(header);

  // Image container
  const imageContainer = createElement('div', 'webcam-image-container');
  const image = createElement('img', 'webcam-image');
  image.src = webcam.imageUrl + '?t=' + Date.now();
  image.alt = webcam.name + ' webcam view';
  image.loading = 'lazy';
  imageContainer.appendChild(image);
  card.appendChild(imageContainer);

  // Slideshow controls
  const controls = createElement('div', 'slideshow-controls');

  const prevBtn = createElement('button', 'slideshow-nav prev', '‹');
  prevBtn.onclick = () => navigateSlideshow(webcam.id, 1);

  const dotsContainer = createElement('div', 'slideshow-dots');

  const nextBtn = createElement('button', 'slideshow-nav next', '›');
  nextBtn.onclick = () => navigateSlideshow(webcam.id, -1);

  controls.appendChild(prevBtn);
  controls.appendChild(dotsContainer);
  controls.appendChild(nextBtn);
  card.appendChild(controls);

  // Info section
  const info = createElement('div', 'webcam-info');

  // Attribution
  if (webcam.attribution) {
    const attr = createElement('div', 'webcam-attribution');
    if (webcam.attribution.url) {
      attr.innerHTML = `${webcam.attribution.text} &mdash; <a href="${webcam.attribution.url}" target="_blank" rel="noopener">⛵ Visit their website</a>`;
    } else {
      attr.textContent = webcam.attribution.text;
    }
    info.appendChild(attr);
  }

  // Down notice
  if (webcam.downNotice) {
    const notice = createElement('div', 'webcam-update-notice webcam-down-notice');
    notice.textContent = '⚠️ ' + webcam.downNotice;
    info.appendChild(notice);
  }

  // Update interval notice
  const updateNotice = createElement('div', 'webcam-update-notice');
  const delayText = webcam.streamDelay === null ? 'unknown delay' : `~${webcam.streamDelay} min stream delay`;
  let noticeText = `Updated every ${webcam.updateInterval || 10} minutes • ${delayText}`;

  // Add daylight-only note
  if (webcam.daylightOnly) {
    noticeText += ' • Screen grabs stop at night';
  }

  updateNotice.textContent = noticeText;
  info.appendChild(updateNotice);

  if (metadata) {
    // Timestamp with staleness check
    const timestampEl = createElement('div', 'webcam-timestamp');
    const stale = isWebcamStale(metadata, webcam.updateInterval || 10);
    const now = Date.now();
    const lastUpdate = new Date(metadata.timestamp).getTime();
    const ageMinutes = Math.round((now - lastUpdate) / (1000 * 60));

    let timestampText = 'Last updated: ' + formatTimestamp(metadata.timestamp);

    if (stale) {
      const ageText = formatAge(ageMinutes);
      timestampText += ` <span class="stale-indicator" title="Image is ${ageMinutes} minutes old">⚠️ STALE (${ageText})</span>`;
      card.classList.add('webcam-stale');
    }

    timestampEl.innerHTML = timestampText;
    info.appendChild(timestampEl);

    // Source link
    if (metadata.source || metadata.url) {
      const source = createElement('div', 'webcam-source');
      if (metadata.url) {
        const link = createElement('a');
        link.href = metadata.url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = metadata.source || 'View Source';
        source.appendChild(document.createTextNode('Source: '));
        source.appendChild(link);
      } else {
        source.textContent = 'Source: ' + metadata.source;
      }
      info.appendChild(source);
    }

    // Map link
    const mapLink = createElement('div', 'webcam-map-link');
    const mapAnchor = createElement('a');
    mapAnchor.href = `/?station=${webcam.id}#map-section`;
    mapAnchor.textContent = '📍 Show on map';
    mapLink.appendChild(mapAnchor);
    info.appendChild(mapLink);

    // Refresh button
    const refreshBtn = createElement('button', 'refresh-button', 'Refresh Image');
    refreshBtn.onclick = () => refreshWebcam(webcam, card, image);
    info.appendChild(refreshBtn);
  }

  card.appendChild(info);
  return card;
}

async function refreshWebcam(webcam, card, image) {
  image.src = webcam.imageUrl + '?t=' + Date.now();
  loadWebcamMetadata(webcam, card);

  const state = slideshowState[webcam.id];
  if (state) {
    state.currentIndex = 0;
    updateSlideshowDisplay(webcam.id);
  }

  cachedMarineData = null;
  const freshData = await fetchMarineData();
  updateConditionsBanner(freshData);
}

function updateConditionsBanner(marineData) {
  if (!marineData) return;

  // Collect all conditions from all webcams
  const allConditions = webcams.flatMap(w => w.conditions || []);
  const newBanner = createConditionsSection(allConditions, marineData, 'marine-conditions-banner');
  const existingBanner = document.getElementById('marine-conditions-banner');

  if (existingBanner && newBanner) {
    existingBanner.replaceWith(newBanner);
  } else if (!existingBanner && newBanner) {
    const container = document.getElementById('webcams-container');
    container?.parentNode?.insertBefore(newBanner, container);
  }
}

// ==========================================================================
// Slideshow Management
// ==========================================================================

async function loadSlideshow(webcam) {
  try {
    const response = await fetch(webcam.slideshowUrl + '?t=' + Date.now());
    if (!response.ok) return;

    const manifest = await response.json();
    if (!manifest?.length) return;

    slideshowState[webcam.id] = {
      images: manifest,
      currentIndex: 0,
      basePath: webcam.slideshowPath
    };

    const card = document.querySelector(`[data-webcam-id="${webcam.id}"]`);
    if (card && manifest.length > 1) {
      const controls = card.querySelector('.slideshow-controls');
      const dotsContainer = card.querySelector('.slideshow-dots');

      controls.classList.add('visible');
      dotsContainer.innerHTML = '';

      // Create dots (reversed: rightmost = newest)
      manifest.slice().reverse().forEach((_, reverseIndex) => {
        const actualIndex = manifest.length - 1 - reverseIndex;
        const dot = createElement('div', 'slideshow-dot' + (actualIndex === 0 ? ' active' : ''));
        dot.dataset.index = actualIndex;
        dot.onclick = () => goToSlide(webcam.id, actualIndex);
        dotsContainer.appendChild(dot);
      });
    }
  } catch (error) {
    console.error(`Failed to load slideshow for ${webcam.name}:`, error);
  }
}

function navigateSlideshow(webcamId, direction) {
  const state = slideshowState[webcamId];
  if (!state?.images) return;

  const newIndex = state.currentIndex + direction;
  if (newIndex < 0 || newIndex >= state.images.length) return;

  state.currentIndex = newIndex;
  updateSlideshowDisplay(webcamId);
}

function goToSlide(webcamId, index) {
  const state = slideshowState[webcamId];
  if (!state?.images) return;

  state.currentIndex = index;
  updateSlideshowDisplay(webcamId);
}

function updateSlideshowDisplay(webcamId) {
  const state = slideshowState[webcamId];
  if (!state) return;

  const card = document.querySelector(`[data-webcam-id="${webcamId}"]`);
  if (!card) return;

  const image = card.querySelector('.webcam-image');
  const currentImage = state.images[state.currentIndex];

  image.src = state.basePath + currentImage.path + '?t=' + Date.now();

  // Update timestamp
  const timestamp = card.querySelector('.webcam-timestamp');
  if (timestamp) {
    timestamp.textContent = 'Captured: ' + formatTimestamp(currentImage.timestamp);

    // Age indicator
    let ageIndicator = card.querySelector('.slideshow-age-indicator');
    if (state.currentIndex > 0) {
      const webcam = webcams.find(w => w.id === webcamId);
      const minutesAgo = state.currentIndex * (webcam?.updateInterval || 10);

      if (!ageIndicator) {
        ageIndicator = createElement('div', 'slideshow-age-indicator');
        timestamp.parentNode.insertBefore(ageIndicator, timestamp.nextSibling);
      }
      ageIndicator.textContent = `(${minutesAgo} minutes ago)`;
    } else if (ageIndicator) {
      ageIndicator.remove();
    }
  }

  // Update nav buttons
  const prevBtn = card.querySelector('.slideshow-nav.prev');
  const nextBtn = card.querySelector('.slideshow-nav.next');
  if (prevBtn) prevBtn.disabled = state.currentIndex === state.images.length - 1;
  if (nextBtn) nextBtn.disabled = state.currentIndex === 0;

  // Update dots
  card.querySelectorAll('.slideshow-dot').forEach(dot => {
    dot.classList.toggle('active', parseInt(dot.dataset.index) === state.currentIndex);
  });
}

// ==========================================================================
// Page Initialization
// ==========================================================================

async function loadWebcams() {
  const container = document.getElementById('webcams-container');
  if (!container) return;

  if (webcams.length === 0) {
    container.innerHTML = '<p style="text-align: center; color: #718096; padding: 2rem;">No webcams currently available.</p>';
    return;
  }

  container.innerHTML = '';

  const marineData = await fetchMarineData();

  // Group webcams by region
  const grouped = {};
  webcams.forEach(webcam => {
    const region = webcam.region || 'other';
    if (!grouped[region]) grouped[region] = [];
    grouped[region].push(webcam);
  });

  // Render each region
  for (const [regionKey, regionWebcams] of Object.entries(grouped)) {
    const regionInfo = webcamRegions[regionKey];
    const regionContainer = createElement('div', 'webcam-region');

    // Region header
    if (regionInfo) {
      const header = createElement('div', 'webcam-region-header');
      const count = regionWebcams.length;
      header.innerHTML = `<h2><span class="webcam-region-toggle-btn">▼</span>${regionInfo.name} <span class="webcam-region-count">(${count} webcam${count !== 1 ? 's' : ''})</span></h2>`;
      header.onclick = () => regionContainer.classList.toggle('collapsed');
      regionContainer.appendChild(header);
    }

    // Content wrapper
    const content = createElement('div', 'webcam-region-content');

    // Regional conditions
    const regionConditions = regionWebcams.flatMap(w => w.conditions || []);
    const conditionsSection = createConditionsSection(regionConditions, marineData);
    if (conditionsSection) content.appendChild(conditionsSection);

    // Webcam grid
    const grid = createElement('div', 'webcam-grid');
    const webcamsToLoadSlideshow = [];

    for (const webcam of regionWebcams) {
      try {
        const metadata = await loadWebcamMetadata(webcam);
        const card = await createWebcamCard(webcam, metadata);
        grid.appendChild(card);
        webcamsToLoadSlideshow.push(webcam);
      } catch (error) {
        console.error(`Failed to load webcam ${webcam.name}:`, error);
        const errorCard = createElement('div', 'webcam-card');
        errorCard.innerHTML = `
          <div class="webcam-header">
            <h3>${webcam.name}</h3>
            <p class="webcam-location">${webcam.location}</p>
          </div>
          <div class="webcam-error">Failed to load webcam data. Please try again later.</div>
        `;
        grid.appendChild(errorCard);
      }
    }

    content.appendChild(grid);
    regionContainer.appendChild(content);
    container.appendChild(regionContainer);

    // Load slideshows after DOM insertion
    webcamsToLoadSlideshow.forEach(loadSlideshow);
  }

  // Handle hash anchor
  if (window.location.hash) {
    setTimeout(() => {
      const target = document.getElementById(window.location.hash.substring(1));
      target?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 300);
  }
}

// ==========================================================================
// Auto-Refresh
// ==========================================================================

function startAutoRefresh() {
  const REFRESH_INTERVAL = 5 * 60 * 1000;
  const MAX_REFRESH_TIME = 30 * 60 * 1000;
  const startTime = Date.now();
  let refreshCount = 0;

  const intervalId = setInterval(() => {
    if (Date.now() - startTime >= MAX_REFRESH_TIME) {
      clearInterval(intervalId);
      console.log('Auto-refresh stopped after 30 minutes.');
      showRefreshNotice();
      return;
    }

    refreshCount++;
    console.log(`Auto-refreshing webcam images... (${refreshCount}/6)`);

    cachedMarineData = null;

    fetchMarineData().then(marineData => {
      webcams.forEach(webcam => {
        const card = document.querySelector(`[data-webcam-id="${webcam.id}"]`);
        if (!card) return;

        const state = slideshowState[webcam.id];
        if (!state || state.currentIndex === 0) {
          const image = card.querySelector('.webcam-image');
          image.src = webcam.imageUrl + '?t=' + Date.now();
        }

        if (marineData && webcam === webcams[0]) {
          updateConditionsBanner(marineData);
        }

        loadSlideshow(webcam);
        loadWebcamMetadata(webcam, card);
      });
    });
  }, REFRESH_INTERVAL);
}

function showRefreshNotice() {
  const notice = createElement('div', 'refresh-notice');
  notice.innerHTML = `
    Auto-refresh stopped after 30 minutes
    <button onclick="location.reload()">Refresh Page</button>
  `;
  document.body.appendChild(notice);

  setTimeout(() => {
    notice.style.opacity = '0';
    setTimeout(() => notice.remove(), 500);
  }, 10000);
}

// ==========================================================================
// Initialize
// ==========================================================================

function initWebcamsPage() {
  loadWebcams();
  startAutoRefresh();
}

// Start when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initWebcamsPage);
} else {
  initWebcamsPage();
}
