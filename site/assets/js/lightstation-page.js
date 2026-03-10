function setSafeHTML(element, html) {
  if (!element) return;

  if (typeof window.setSanitizedHTML === 'function') {
    window.setSanitizedHTML(element, html);
  } else {
    element.innerHTML = html;
  }
}

// Load and display lightstation data
async function loadLightstationData() {
  try {
    // Load station metadata for details expansion
    try {
      const metaResponse = await fetch('/data/stations.json');
      const metaData = await metaResponse.json();
      // Store lightstation metadata globally for card creation
      window.stationMetadata = {};
      if (metaData.lightstations) {
        Object.values(metaData.lightstations).forEach(station => {
          // Store with multiple key formats for easier lookup
          // Format 1: Title case name (e.g., "Addenbroke Island")
          window.stationMetadata[station.name] = station;
          // Format 2: Uppercase with spaces (e.g., "ADDENBROKE ISLAND") - matches latest_lightstation.json
          window.stationMetadata[station.name.toUpperCase()] = station;
          // Format 3: Uppercase with underscores (e.g., "ADDENBROKE_ISLAND") - matches ID
          window.stationMetadata[station.id] = station;
        });
      }
    } catch (err) {
      console.warn('Could not load station metadata:', err);
    }

    const response = await fetch('/data/latest_lightstation.json');
    const data = await response.json();

    // Group stations by region
    const regions = {};
    for (const [stationName, stationData] of Object.entries(data)) {
      const region = stationData.region || 'OTHER';
      if (!regions[region]) {
        regions[region] = [];
      }
      regions[region].push({ name: stationName, ...stationData });
    }

    // Render grouped by region
    const container = document.getElementById('lightstations-container');
    container.textContent = '';

    const regionOrder = [
      'STRAIT OF GEORGIA',
      'JUAN DE FUCA STRAIT',
      'WEST COAST VANCOUVER ISLAND',
      'CENTRAL COAST',
      'HECATE STRAIT'
    ];

    for (const region of regionOrder) {
      if (!regions[region]) continue;

      const section = document.createElement('div');
      section.className = 'region-section';
      section.setAttribute('data-region', region);

      // Collapse all regions except Strait of Georgia by default
      if (region !== 'STRAIT OF GEORGIA') {
        section.classList.add('collapsed');
      }

      const header = document.createElement('div');
      header.className = 'region-header';

      // Add toggle arrow, region name, and station count
      const toggleArrow = document.createElement('span');
      toggleArrow.className = 'region-toggle-btn';
      toggleArrow.textContent = '▼';

      const stationCount = document.createElement('span');
      stationCount.style.fontSize = '0.8em';
      stationCount.style.fontWeight = 'normal';
      stationCount.style.opacity = '0.8';
      stationCount.textContent = ` (${regions[region].length} stations)`;

      header.textContent = '';
      header.appendChild(toggleArrow);
      header.appendChild(document.createTextNode(' ' + region + ' '));
      header.appendChild(stationCount);

      // Add click handler to toggle collapse
      header.addEventListener('click', () => {
        section.classList.toggle('collapsed');
      });

      section.appendChild(header);

      const grid = document.createElement('div');
      grid.className = 'lightstation-grid';

      // Sort stations alphabetically
      regions[region].sort((a, b) => a.name.localeCompare(b.name));

      for (const station of regions[region]) {
        const card = createStationCard(station);
        grid.appendChild(card);
      }

      section.appendChild(grid);
      container.appendChild(section);
    }

    // Handle hash navigation (e.g., #lightstation-CHROME_ISLAND)
    handleLightstationHash();

  } catch (error) {
    console.error('Failed to load lightstation data:', error);
    const fallbackContainer = document.getElementById('lightstations-container');
    if (fallbackContainer) {
      setSafeHTML(
        fallbackContainer,
        '<p style="text-align: center; color: #e53e3e; padding: 2rem;">Failed to load lightstation data.</p>'
      );
    }
  }
}

// Handle hash-based navigation to specific lightstation
function handleLightstationHash() {
  const hash = window.location.hash;
  if (!hash || !hash.startsWith('#lightstation-')) return;

  // Extract station ID from hash (e.g., "#lightstation-CHROME_ISLAND" -> "CHROME_ISLAND")
  const stationId = hash.replace('#lightstation-', '');

  // Find the card with this station ID
  const targetCard = document.querySelector(`[data-station-id="${stationId}"]`);
  if (!targetCard) {
    console.warn('Station not found:', stationId);
    return;
  }

  // Find the region section containing this card
  const regionSection = targetCard.closest('.region-section');
  if (!regionSection) return;

  // Collapse all region sections first
  document.querySelectorAll('.region-section').forEach(section => {
    section.classList.add('collapsed');
  });

  // Expand the target region
  regionSection.classList.remove('collapsed');

  // Scroll to the region header with the card
  setTimeout(() => {
    regionSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, 300);
}

function createStationCard(station) {
  const card = document.createElement('div');
  card.className = 'lightstation-card';
  // Add data attribute for hash navigation
  if (window.stationMetadata && window.stationMetadata[station.name]) {
    card.setAttribute('data-station-id', window.stationMetadata[station.name].id);
  }

  const title = document.createElement('h3');
  title.textContent = station.name;
  card.appendChild(title);

  // Wind
  if (!station.wind_calm) {
    const windRow = createConditionRow('Wind',
      `${station.wind_direction || 'N/A'} ${station.wind_speed_kt || 'N/A'} kt${station.wind_gusting ? ' (gusting)' : ''}${station.wind_estimated ? ' (est)' : ''}`
    );
    card.appendChild(windRow);
  } else {
    card.appendChild(createConditionRow('Wind', 'CALM'));
  }

  // Sea state
  if (station.sea_height_ft !== null || station.sea_condition) {
    const seaText = station.sea_height_ft !== null
      ? `${station.sea_height_ft} ft ${station.sea_condition || ''}`
      : station.sea_condition || 'N/A';
    card.appendChild(createConditionRow('Sea State', seaText));
  }

  // Swell
  if (station.swell_intensity || station.swell_direction) {
    const swellText = `${station.swell_intensity || ''} ${station.swell_direction || ''} swell`.trim();
    card.appendChild(createConditionRow('Swell', swellText || 'N/A'));
  }

  // Report time with age
  if (station.observation_time) {
    const reportTime = document.createElement('div');
    reportTime.className = 'report-time';

    const obsDate = new Date(station.observation_time);
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

    reportTime.textContent = `Report: ${formattedDate}${ageText}`;
    card.appendChild(reportTime);
  } else if (station.report_time_str) {
    const reportTime = document.createElement('div');
    reportTime.className = 'report-time';
    reportTime.textContent = `Report: ${station.report_time_str}`;
    card.appendChild(reportTime);
  }

  // Staleness warning
  if (station.stale) {
    const warning = document.createElement('div');
    warning.className = 'stale-warning';
    warning.style.color = '#c62828';
    warning.style.fontWeight = '600';
    warning.style.marginTop = '0.5rem';
    warning.textContent = '⚠️ STALE DATA (>12h old)';
    card.appendChild(warning);
  }

  // Navigation links container
  const navLinks = document.createElement('div');
  navLinks.style.display = 'flex';
  navLinks.style.gap = '0.5rem';
  navLinks.style.marginTop = '0.75rem';

  // View historical data link
  const chartLink = document.createElement('a');
  chartLink.className = 'view-chart-link';
  chartLink.href = '#lightstation-data-table-section';
  chartLink.textContent = 'View historical';
  chartLink.style.flex = '1';
  chartLink.style.textAlign = 'center';
  chartLink.style.padding = '0.4rem';
  chartLink.style.background = '#f7fafc';
  chartLink.style.border = '1px solid #e2e8f0';
  chartLink.style.borderRadius = '4px';
  chartLink.style.textDecoration = 'none';
  chartLink.style.fontSize = '0.85rem';
  chartLink.onclick = (e) => {
    e.preventDefault();
    viewLightstationChart(station.name);
  };
  navLinks.appendChild(chartLink);

  // Show on Map button
  const mapLink = document.createElement('a');
  mapLink.className = 'view-chart-link';
  mapLink.href = '#lightstation-map-section';
  mapLink.textContent = '📍 Show on Map';
  mapLink.style.flex = '1';
  mapLink.style.textAlign = 'center';
  mapLink.style.padding = '0.4rem';
  mapLink.style.background = '#f7fafc';
  mapLink.style.border = '1px solid #e2e8f0';
  mapLink.style.borderRadius = '4px';
  mapLink.style.textDecoration = 'none';
  mapLink.style.fontSize = '0.85rem';
  mapLink.onclick = (e) => {
    e.preventDefault();
    const stationId = window.stationMetadata && window.stationMetadata[station.name]
      ? window.stationMetadata[station.name].id
      : null;

    if (stationId) {
      // Scroll to map section
      const mapSection = document.getElementById('lightstation-map-section');
      if (mapSection) {
        mapSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }

      // Center map on lightstation after scroll
      setTimeout(() => {
        if (typeof window.centerMapOnLightstation === 'function') {
          window.centerMapOnLightstation(stationId);
        }
      }, 800);
    }
  };
  navLinks.appendChild(mapLink);

  card.appendChild(navLinks);

  // Station details toggle button
  const detailsToggle = document.createElement('div');
  detailsToggle.className = 'station-details-toggle';
  setSafeHTML(detailsToggle, 'Station Details <span class="toggle-icon">▼</span>');

  // Station details content (initially hidden)
  const detailsContent = document.createElement('div');
  detailsContent.className = 'station-details-content';

  // Add metadata rows (fetch from window.stationMetadata if available)
  if (window.stationMetadata && window.stationMetadata[station.name]) {
    const meta = window.stationMetadata[station.name];

    if (meta.id) {
      const idRow = document.createElement('div');
      idRow.className = 'detail-row';
      setSafeHTML(
        idRow,
        `<span class="detail-label">Station ID:</span><span class="detail-value">${meta.id}</span>`
      );
      detailsContent.appendChild(idRow);
    }

    if (meta.lat && meta.lon) {
      const coordRow = document.createElement('div');
      coordRow.className = 'detail-row';
      setSafeHTML(
        coordRow,
        `<span class="detail-label">Coordinates:</span><span class="detail-value">${meta.lat.toFixed(4)}°N, ${Math.abs(meta.lon).toFixed(4)}°W</span>`
      );
      detailsContent.appendChild(coordRow);
    }

    if (meta.region) {
      const regionRow = document.createElement('div');
      regionRow.className = 'detail-row';
      setSafeHTML(
        regionRow,
        `<span class="detail-label">Region:</span><span class="detail-value">${meta.region}</span>`
      );
      detailsContent.appendChild(regionRow);
    }

    if (meta.established) {
      const estRow = document.createElement('div');
      estRow.className = 'detail-row';
      setSafeHTML(
        estRow,
        `<span class="detail-label">Established:</span><span class="detail-value">${meta.established}</span>`
      );
      detailsContent.appendChild(estRow);
    }

    if (meta.update_frequency_hours) {
      const freqRow = document.createElement('div');
      freqRow.className = 'detail-row';
      setSafeHTML(
        freqRow,
        `<span class="detail-label">Update Frequency:</span><span class="detail-value">Every ${meta.update_frequency_hours} hours</span>`
      );
      detailsContent.appendChild(freqRow);
    }

    if (meta.notes) {
      const notesRow = document.createElement('div');
      notesRow.className = 'detail-row';
      notesRow.style.flexDirection = 'column';
      notesRow.style.alignItems = 'flex-start';
      setSafeHTML(
        notesRow,
        `<span class="detail-label">Notes:</span><span class="detail-value" style="text-align: left; margin-top: 0.3rem; font-size: 0.8rem; color: #666;">${meta.notes}</span>`
      );
      detailsContent.appendChild(notesRow);
    }
  }

  // Toggle functionality
  detailsToggle.addEventListener('click', () => {
    detailsToggle.classList.toggle('expanded');
    detailsContent.classList.toggle('expanded');
  });

  card.appendChild(detailsToggle);
  card.appendChild(detailsContent);

  return card;
}

// Function to view station chart (called from cards)
function viewLightstationChart(stationName) {
  const select = document.getElementById('lightstation-station-select');
  if (!select) return;

  // Check if station exists in timeseries data
  if (!window.lightstationTimeseriesData || !window.lightstationTimeseriesData[stationName]) {
    // Station doesn't have 24hr data - show alert instead of scrolling
    alert(`${stationName} does not have data from the past 24 hours.\n\nMost recent observation may be older than 24 hours.`);
    return;
  }

  // Select the station in dropdown
  select.value = stationName;

  // Trigger chart render if the function exists
  if (typeof window.renderLightstationCharts === 'function') {
    window.renderLightstationCharts(stationName);
  }

  // Scroll to data table section (top of the tables/charts area)
  const tableSection = document.getElementById('lightstation-data-table-section');
  if (tableSection) {
    tableSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }
}

function createConditionRow(label, value) {
  const row = document.createElement('div');
  row.className = 'condition-row';

  const labelEl = document.createElement('span');
  labelEl.className = 'condition-label';
  labelEl.textContent = label + ':';

  const valueEl = document.createElement('span');
  valueEl.className = 'condition-value';
  valueEl.textContent = value;

  row.appendChild(labelEl);
  row.appendChild(valueEl);

  return row;
}

// Function to show selected lightstation on map (from dropdown)
function showSelectedLightstationOnMap() {
  const select = document.getElementById('lightstation-station-select');
  if (!select || !select.value) {
    console.warn('No lightstation selected');
    return;
  }

  const stationName = select.value;

  // Get station ID from metadata
  if (window.stationMetadata && window.stationMetadata[stationName]) {
    const stationId = window.stationMetadata[stationName].id;

    // Scroll to map section
    const mapSection = document.getElementById('lightstation-map-section');
    if (mapSection) {
      mapSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    // Center map on lightstation after scroll
    setTimeout(() => {
      if (typeof window.centerMapOnLightstation === 'function') {
        window.centerMapOnLightstation(stationId);
      }
    }, 800);
  }
}

// Make function globally accessible
window.showSelectedLightstationOnMap = showSelectedLightstationOnMap;

// Load data on page load
loadLightstationData();
