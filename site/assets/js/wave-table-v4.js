/* -----------------------------
   Wave Table Module
   Generates 24-hour wave height summary table
   ----------------------------- */

/**
 * Generate 24-hour wave height summary table
 * @param {Object} chartData - Full chart data object with all buoys
 */
function generateWaveHeightTable(chartData) {
  try {
    if (!chartData) return;

    const table = document.getElementById("wave-height-table");
    if (!table) return;

    const buoyOrder = ["4600146", "4600304", "CRPILE", "4600303", "4600131", "4600206", "46087", "46088", "46267"];
  const hourMap = new Map();

  buoyOrder.forEach((buoyId) => {
    const buoy = chartData[buoyId];
    if (!buoy || !buoy.timeseries) return;

    // Use swell_height ONLY for Neah Bay (46087), sig wave height for all others
    const waveData = (buoyId === "46087")
      ? buoy.timeseries.swell_height?.data
      : buoy.timeseries.wave_height_sig?.data;

    if (!waveData) return;

    waveData.forEach((point) => {
      const date = new Date(point.time);
      const hourKey = new Date(date);
      hourKey.setMinutes(0, 0, 0);
      const hourStr = hourKey.toISOString();

      if (!hourMap.has(hourStr)) {
        hourMap.set(hourStr, {});
      }
      hourMap.get(hourStr)[buoyId] = point.value;
    });
  });

  const sortedHours = Array.from(hourMap.keys()).sort().reverse();
  const totalRows = sortedHours.length;

  // Show first half of data by default:
  // - 24hr mode: ~24 rows total → show 12, hide 12
  // - 48hr mode: ~48 rows total → show 24, hide 24
  const halfwayPoint = Math.ceil(totalRows / 2);

  let tableHTML = `
    <thead>
      <tr>
        <th>Time</th>
        <th>Halibut Bank</th>
        <th>English Bay</th>
        <th>Crescent Beach Ocean</th>
        <th>Southern Georgia Strait</th>
        <th>Sentry Shoal</th>
        <th>La Perouse Bank</th>
        <th>Neah Bay<br><span style="font-size: 0.8em; font-weight: normal; color: #666;">(Swell)</span></th>
        <th>New Dungeness</th>
        <th>Angeles Point</th>
      </tr>
    </thead>
    <tbody>
  `;

  let previousDate = null;

  sortedHours.forEach((hourStr, index) => {
    const date = new Date(hourStr);

    // Format: "Sa-22 05h" (2-letter weekday, day, hour)
    const dayOfWeek = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'][date.getDay()];
    const dayOfMonth = date.toLocaleString('en-US', { day: 'numeric', timeZone: 'America/Vancouver' });
    const hour = date.toLocaleString('en-US', { hour: '2-digit', hour12: false, timeZone: 'America/Vancouver' });

    // Only show date prefix if it changed from previous row
    const currentDate = `${dayOfWeek}-${dayOfMonth}`;
    let timeLabel;
    if (currentDate !== previousDate) {
      timeLabel = `${currentDate} ${hour}h`;
      previousDate = currentDate;
    } else {
      timeLabel = `${hour}h`;
    }

    const values = hourMap.get(hourStr);

    // Add 'collapsed-row' class to second half of rows
    const rowClass = index >= halfwayPoint ? ' class="collapsed-row"' : '';

    tableHTML += `
      <tr${rowClass}>
        <td><strong>${timeLabel}</strong></td>
        <td>${values["4600146"] != null ? values["4600146"] + " m" : "—"}</td>
        <td>${values["4600304"] != null ? values["4600304"] + " m" : "—"}</td>
        <td>${values["CRPILE"] != null ? values["CRPILE"].toFixed(2) + " m" : "—"}</td>
        <td>${values["4600303"] != null ? values["4600303"] + " m" : "—"}</td>
        <td>${values["4600131"] != null ? values["4600131"] + " m" : "—"}</td>
        <td>${values["4600206"] != null ? values["4600206"] + " m" : "—"}</td>
        <td>${values["46087"] != null ? values["46087"] + " m" : "—"}</td>
        <td>${values["46088"] != null ? values["46088"] + " m" : "—"}</td>
        <td>${values["46267"] != null ? values["46267"] + " m" : "—"}</td>
      </tr>
    `;
  });

  tableHTML += "</tbody>";
  table.innerHTML = tableHTML;

  // Reset any inline styles from previous toggle (ensure rows start hidden)
  setTimeout(() => {
    const collapsedRows = document.querySelectorAll('#wave-height-table .collapsed-row');
    collapsedRows.forEach(row => {
      row.style.display = ''; // Clear inline style, let CSS class control it
    });
  }, 0);

  // Add or update the toggle button
  updateTableToggleButton(totalRows > halfwayPoint);
  } catch (error) {
    logger.error('WaveTable', 'Error generating wave height table', error);
    const table = document.getElementById("wave-height-table");
    if (table) {
      table.innerHTML = '<tbody><tr><td colspan="10" style="text-align: center; color: #e53935; padding: 2rem;">Error generating table</td></tr></tbody>';
    }
  }
}

/**
 * Update or create the table toggle button
 */
function updateTableToggleButton(shouldShow) {
  const container = document.getElementById('wave-height-table-section');
  if (!container) return;

  // Remove existing button if present
  const existingBtn = document.getElementById('wave-table-toggle-btn');
  if (existingBtn) {
    existingBtn.remove();
  }

  // Only add button if there are enough rows to collapse
  if (shouldShow) {
    const toggleBtn = document.createElement('div');
    toggleBtn.id = 'wave-table-toggle-btn';
    toggleBtn.style.cssText = 'text-align: center; margin-top: 1rem;';
    toggleBtn.innerHTML = `
      <button onclick="toggleWaveTableRows()" style="
        padding: 0.5rem 1.5rem;
        background: #0077be;
        color: white;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        font-size: 0.9rem;
        font-weight: 600;
        transition: background 0.2s;
      " onmouseover="this.style.background='#005a94'" onmouseout="this.style.background='#0077be'">
        ▼ Show More Rows
      </button>
    `;
    container.appendChild(toggleBtn);
  }
}

/**
 * Toggle visibility of collapsed table rows
 */
function toggleWaveTableRows() {
  const collapsedRows = document.querySelectorAll('#wave-height-table .collapsed-row');
  const button = document.querySelector('#wave-table-toggle-btn button');

  if (!collapsedRows.length || !button) return;

  // Check actual computed style, not just inline style
  const firstRow = collapsedRows[0];
  const computedStyle = window.getComputedStyle(firstRow);
  const isHidden = computedStyle.display === 'none';

  collapsedRows.forEach(row => {
    // Set inline style to override CSS class
    row.style.display = isHidden ? 'table-row' : 'none';
  });

  button.innerHTML = isHidden ? '▲ Show Less Rows' : '▼ Show More Rows';
}
