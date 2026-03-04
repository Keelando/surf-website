# REFACTORING CODE EXAMPLES
## Salish Sea Wave Conditions - Frontend Improvements

This document provides concrete code examples for addressing the issues found in the analysis.

---

## 1. REPLACE INLINE EVENT HANDLERS WITH CSS

### BEFORE (Current - Bad Practice)
```html
<!-- index.html line 211 -->
<a href="https://github.com/keelando/envcan_wave" 
   target="_blank" rel="noopener noreferrer"
   style="color: white; text-decoration: none; opacity: 0.8; transition: opacity 0.2s;"
   onmouseover="this.style.opacity='1'"
   onmouseout="this.style.opacity='0.8'">
  <svg>...</svg>
  View on GitHub
</a>

<!-- index.html line 136 -->
<a href="/storm_surge.html"
   style="padding: 0.5rem 1rem; background: #0077be; color: white; 
           text-decoration: none; border-radius: 4px; font-size: 0.9rem; 
           font-weight: 600; transition: background 0.2s;"
   onmouseover="this.style.background='#005a8f'"
   onmouseout="this.style.background='#0077be'">
  View Full Forecast →
</a>
```

### AFTER (Better - CSS Solution)
```html
<!-- In HTML -->
<a href="https://github.com/keelando/envcan_wave" 
   target="_blank" rel="noopener noreferrer"
   class="github-link">
  <svg>...</svg>
  View on GitHub
</a>

<a href="/storm_surge.html" class="button button-primary">
  View Full Forecast →
</a>
```

```css
/* In style-v3.css or footer.css */
.github-link {
  color: white;
  text-decoration: none;
  opacity: 0.8;
  transition: opacity 0.2s ease;
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
}

.github-link:hover {
  opacity: 1;
}

.button {
  display: inline-block;
  padding: 0.5rem 1rem;
  text-decoration: none;
  border-radius: 4px;
  font-size: 0.9rem;
  font-weight: 600;
  transition: background 0.2s ease;
  border: none;
  cursor: pointer;
}

.button-primary {
  background: #0077be;
  color: white;
}

.button-primary:hover {
  background: #005a8f;
}

.button-primary:focus {
  outline: 2px solid #004b7c;
  outline-offset: 2px;
}
```

**Benefits:**
- Keyboard accessible (works with Tab + Enter)
- Works without JavaScript
- Easier to maintain (change once in CSS)
- Better performance (no DOM manipulation on hover)

---

## 2. CREATE SHARED FETCH WRAPPER WITH TIMEOUT

### BEFORE (Current - No Timeout)
```javascript
// main.js line 71
async function loadBuoyData() {
  try {
    const response = await fetch(`/data/latest_buoy_v2.json?t=${Date.now()}`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    // Process data...
  } catch (error) {
    console.error('Error:', error);
    // Show error to user
  }
}
```

**Problems:**
- No timeout - user could wait forever on slow network
- No retry mechanism - transient failures fail permanently
- Code repeated in multiple files

### AFTER (Better - Reusable with Retry)
```javascript
// Create new file: assets/js/utils/fetch-helper.js

/**
 * Fetch with timeout, retry, and better error handling
 * @param {string} url - URL to fetch
 * @param {Object} options - Options object
 * @param {number} options.timeout - Request timeout in ms (default: 10000)
 * @param {number} options.maxRetries - Max retry attempts (default: 3)
 * @param {number} options.retryDelay - Delay between retries in ms (default: 1000)
 * @returns {Promise<Object>} Parsed JSON response
 */
async function fetchWithRetry(url, options = {}) {
  const {
    timeout = 10000,
    maxRetries = 3,
    retryDelay = 1000,
    onRetry = null
  } = options;

  let lastError;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      // Create abort controller for timeout
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);

      // Make the fetch request
      const response = await fetch(url, { 
        signal: controller.signal 
      });
      
      clearTimeout(timeoutId);

      // Check HTTP status
      if (!response.ok) {
        throw new Error(
          `HTTP ${response.status}: ${response.statusText}`
        );
      }

      // Parse and return JSON
      return await response.json();

    } catch (error) {
      lastError = error;
      
      // Log retry attempt
      if (attempt < maxRetries) {
        console.warn(
          `Fetch attempt ${attempt}/${maxRetries} failed:`,
          error.message,
          `Retrying in ${retryDelay}ms...`
        );
        
        // Call retry callback if provided (for UI updates)
        if (onRetry) {
          onRetry({ attempt, maxRetries, error });
        }
        
        // Wait before retrying (exponential backoff)
        await new Promise(resolve => 
          setTimeout(resolve, retryDelay * attempt)
        );
      }
    }
  }

  // All retries exhausted
  throw new Error(
    `Failed to fetch ${url} after ${maxRetries} attempts: ${lastError.message}`
  );
}

// Export for use
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { fetchWithRetry };
}
```

**Usage:**
```javascript
// In main.js, tides.js, etc.

async function loadBuoyData() {
  const container = document.getElementById('buoy-container');
  
  try {
    const data = await fetchWithRetry(
      '/data/latest_buoy_v2.json',
      {
        timeout: 8000,        // 8 second timeout
        maxRetries: 3,        // Try 3 times
        retryDelay: 2000,     // Wait 2s between retries
        onRetry: ({ attempt, maxRetries }) => {
          console.log(`Retrying data fetch (${attempt}/${maxRetries})...`);
        }
      }
    );
    
    // Process data
    renderBuoyCards(data);
    
  } catch (error) {
    console.error('Error loading buoy data:', error);
    container.innerHTML = `
      <div class="error-state">
        <h2>Unable to Load Buoy Data</h2>
        <p>${error.message}</p>
        <p>Please try refreshing the page in a moment.</p>
      </div>
    `;
  }
}
```

**In HTML:**
```html
<!-- In index.html, before main.js -->
<script src="/assets/js/utils/fetch-helper.js"></script>
<script src="/assets/js/main.js"></script>
```

**Benefits:**
- Timeout prevents hanging requests
- Automatic retry on failure
- Consistent error handling across all pages
- Reusable in all modules
- Easy to test

---

## 3. ADD ERROR HANDLING TO CHART MODULES

### BEFORE (Current - Silent Failure)
```javascript
// wave-chart-v2.js - No error handling
function renderWaveChart(waveChart, buoy, buoyId) {
  const ts = buoy.timeseries;
  const sigWaveHeight = ts.wave_height_sig?.data || [];
  
  // If data is bad, chart.setOption() silently fails
  waveChart.setOption({
    title: { text: `${buoy.name} - Wave Height` },
    // ... more config
  });
}
```

### AFTER (Better - With Error Handling)
```javascript
// wave-chart-v2.js - With error handling

/**
 * Safely render wave chart with error handling
 * @param {Object} waveChart - ECharts instance
 * @param {Object} buoy - Buoy data object
 * @param {string} buoyId - Buoy identifier
 */
function renderWaveChart(waveChart, buoy, buoyId) {
  try {
    // Validate input
    if (!waveChart || !buoy || !buoy.timeseries) {
      throw new Error('Invalid chart data provided');
    }

    const ts = buoy.timeseries;
    const sigWaveHeight = ts.wave_height_sig?.data || [];
    
    // Validate data array
    if (!Array.isArray(sigWaveHeight)) {
      throw new Error('Wave height data is not an array');
    }

    // Sanitize data before rendering
    const sanitizedData = sanitizeSeriesData(sigWaveHeight);
    
    if (sanitizedData.length === 0) {
      showChartError(waveChart, 'No wave height data available');
      return;
    }

    // Build chart option
    const option = {
      title: {
        text: `${buoy.name} - Wave Height`,
        left: 'center'
      },
      // ... rest of config
    };

    // Render chart
    waveChart.setOption(option);

  } catch (error) {
    console.error('Error rendering wave chart:', error);
    showChartError(waveChart, error.message);
  }
}

/**
 * Display error message in chart container
 * @param {Object} chart - ECharts instance
 * @param {string} message - Error message
 */
function showChartError(chart, message) {
  try {
    chart.setOption({
      title: {
        text: 'Chart Failed to Load',
        subtext: message,
        left: 'center',
        top: 'center'
      },
      textStyle: { color: '#999' }
    });
  } catch (e) {
    console.error('Failed to show chart error:', e);
  }
}
```

**Benefits:**
- Users see error message instead of blank chart
- Easier to debug issues
- Prevents cascading failures
- Graceful degradation

---

## 4. EXTRACT SHARED UTILITIES

### BEFORE (Current - Scattered Functions)
```javascript
// forecasts.js - Has formatTimestamp()
function formatTimestamp(date) {
  const options = {
    weekday: 'short',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short'
  };
  return date.toLocaleString('en-US', options);
}

// chart-utils-v2.js - Has formatTimeAxis() (similar)
function formatTimeAxis(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'America/Vancouver',
  });
}

// main.js - Has degreesToCardinal()
function degreesToCardinal(degrees) {
  if (degrees == null) return null;
  const directions = ['N', 'NNE', 'NE', ...];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}
```

### AFTER (Better - Centralized Utils)

```javascript
// Create new file: assets/js/utils/common.js

/**
 * Common utility functions shared across all pages
 */

// ===== TIME & DATE FORMATTING =====

/**
 * Format timestamp for display (full format)
 * Example: "Mon, Nov 14, 2025 14:30 PST"
 */
function formatTimestamp(date) {
  const options = {
    weekday: 'short',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short'
  };
  return date.toLocaleString('en-US', options);
}

/**
 * Format time for chart axes (compact)
 * Example: "Oct 25 14:30"
 */
function formatTimeAxis(isoString) {
  const date = new Date(isoString);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
    timeZone: 'America/Vancouver',
  });
}

/**
 * Format compact time label for chart axes
 * Example: "Mon 14h"
 */
function formatCompactTimeLabel(isoString) {
  const d = new Date(isoString);
  const dayOfWeek = d.toLocaleString('en-US', {
    weekday: 'short',
    timeZone: 'America/Vancouver',
  });
  const hour = d.toLocaleString('en-US', {
    hour: '2-digit',
    hour12: false,
    timeZone: 'America/Vancouver',
  });
  return `${dayOfWeek} ${hour}h`;
}

// ===== WEATHER & DIRECTION =====

/**
 * Convert compass degrees to cardinal direction
 * Example: 0° = N, 90° = E, 180° = S, 270° = W
 */
function degreesToCardinal(degrees) {
  if (degrees == null) return null;
  const directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 
                     'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];
  const index = Math.round(degrees / 22.5) % 16;
  return directions[index];
}

/**
 * Get directional arrow SVG (rotated for weather data)
 * Fixes Firefox Android tablet SVG rotation bugs
 */
function getDirectionalArrow(degrees, arrowType = 'wind') {
  if (degrees == null || degrees === '—') return '';
  
  const rotation = arrowType === 'wind' ? degrees : degrees + 90;
  
  const svg = arrowType === 'wind'
    ? `<svg width="16" height="16" viewBox="0 0 16 16">
        <path d="M8 2v12m0 0l-3-3m3 3l3-3" 
              stroke="#004b7c" stroke-width="2" fill="none" stroke-linecap="round"/>
       </svg>`
    : `<svg width="16" height="16" viewBox="0 0 16 16">
        <path d="M2 8h12m0 0l-3-3m3 3l-3 3" 
              stroke="#004b7c" stroke-width="2" fill="none" stroke-linecap="round"/>
       </svg>`;
  
  return `<span style="display:inline-block;transform:rotate(${rotation}deg);
                       margin-left:0.3rem;vertical-align:middle;">${svg}</span>`;
}

// ===== DATA VALIDATION =====

/**
 * Sanitize series data for ECharts (convert bad values to null)
 */
function sanitizeSeriesData(dataArray) {
  return dataArray.map(d => {
    const y = parseFloat(d.value);
    if (isNaN(y) || d.value == null || d.value === 'MM') {
      return [new Date(d.time).getTime(), null];
    }
    return [new Date(d.time).getTime(), y];
  });
}
```

**Usage in HTML:**
```html
<!-- Load utilities BEFORE page-specific scripts -->
<script src="/assets/js/utils/fetch-helper.js"></script>
<script src="/assets/js/utils/common.js"></script>
<script src="/assets/js/main.js"></script>
<script src="/assets/js/tides.js"></script>
```

**Usage in files:**
```javascript
// In tides.js or any other file:
const timestamp = formatTimestamp(new Date());
const direction = degreesToCardinal(45);
const arrow = getDirectionalArrow(180, 'wave');
```

**Benefits:**
- Single source of truth for shared logic
- Easier to maintain (change once, updates everywhere)
- Consistent formatting across all pages
- Testable functions

---

## 5. ADD ACCESSIBILITY ATTRIBUTES

### BEFORE (Current - Not Accessible)
```html
<!-- index.html -->
<button onclick="toggleCardDetails('${id}')">▼ Show Details</button>

<a href="https://github.com/keelando/envcan_wave" target="_blank">
  <svg>...</svg>
  View on GitHub
</a>

<div id="buoy-container">
  <!-- Dynamic content updated by JS -->
</div>

<select id="chart-buoy-select">
  <option value="4600146">Halibut Bank</option>
</select>
```

### AFTER (Better - Accessible)
```html
<!-- index.html -->
<button 
  aria-label="Toggle buoy card details for ${stationName}"
  data-buoy-id="${id}"
  class="toggle-details-btn">
  ▼ Show Details
</button>

<!-- JavaScript event delegation instead of inline onclick -->
<script>
document.addEventListener('click', (e) => {
  const btn = e.target.closest('.toggle-details-btn');
  if (btn) {
    toggleCardDetails(btn.dataset.buoyId);
  }
});
</script>

<a href="https://github.com/keelando/envcan_wave" 
   target="_blank" 
   rel="noopener noreferrer"
   aria-label="View source code on GitHub (opens in new window)">
  <svg aria-hidden="true">...</svg>
  View on GitHub
</a>

<div id="buoy-container"
     role="region"
     aria-label="Real-time buoy observations"
     aria-live="polite"
     aria-busy="false">
  <!-- Dynamic content updated by JS -->
</div>

<label for="chart-buoy-select">Select Buoy for Chart:</label>
<select id="chart-buoy-select" aria-describedby="chart-help">
  <option value="4600146">Halibut Bank</option>
  <option value="4600304">English Bay</option>
</select>
<span id="chart-help">Choose a buoy to view 24-hour wave height trends</span>
```

**CSS for Focus Visibility:**
```css
/* Ensure focus is visible on all interactive elements */
button:focus,
a:focus,
select:focus,
input:focus {
  outline: 2px solid #0077be;
  outline-offset: 2px;
}

/* Better button focus state */
.toggle-details-btn:focus {
  outline: 3px solid #004b7c;
  outline-offset: 3px;
}
```

**Benefits:**
- Screen reader users can understand content
- Keyboard navigation works properly
- Dynamic content updates announced
- Meets WCAG 2.1 AA standards

---

## 6. MODULE PATTERN FOR STATE MANAGEMENT

### BEFORE (Current - Global Variables)
```javascript
// tides.js
let tideCurrentData = null;
let tideTimeseriesData = null;
let tideHighLowData = null;
let tideChart = null;
let currentDayOffset = 0;
let currentStationKey = null;

async function loadTideData() { ... }
function populateStationDropdown() { ... }
function updateChart() { ... }
```

**Problems:**
- Variables accessible globally (hard to reason about)
- Dependencies between functions implicit
- Hard to test
- Risk of name collisions

### AFTER (Better - Module Pattern)
```javascript
// assets/js/modules/tide-module.js

const TideModule = (() => {
  // Private state
  const state = {
    currentData: null,
    timeseriesData: null,
    highLowData: null,
    combinedWaterLevelData: null,
    stationsMetadata: null,
    chart: null,
    currentDayOffset: 0,
    currentStationKey: null
  };

  // Private functions
  const logState = (message) => {
    console.log(`[TideModule] ${message}`, state);
  };

  // Public API
  return {
    // Initialize module
    async init() {
      try {
        await this.loadData();
        this.setupEventListeners();
        this.render();
        logState('Initialized');
      } catch (error) {
        console.error('Failed to initialize TideModule:', error);
        this.showError(error);
      }
    },

    // Load all tide data
    async loadData() {
      try {
        const [currentRes, timeseriesRes, highlowRes] = await Promise.all([
          fetch('/data/tide-latest.json'),
          fetch('/data/tide-timeseries.json'),
          fetch('/data/tide-hi-low.json')
        ]);

        if (!currentRes.ok || !timeseriesRes.ok || !highlowRes.ok) {
          throw new Error('One or more tide API calls failed');
        }

        state.currentData = await currentRes.json();
        state.timeseriesData = await timeseriesRes.json();
        state.highLowData = await highlowRes.json();

        logState('Data loaded');
      } catch (error) {
        throw new Error(`Failed to load tide data: ${error.message}`);
      }
    },

    // Setup event listeners
    setupEventListeners() {
      const select = document.getElementById('tide-station-select');
      if (select) {
        select.addEventListener('change', (e) => {
          this.selectStation(e.target.value);
        });
      }

      const prevBtn = document.getElementById('prev-day-btn');
      const nextBtn = document.getElementById('next-day-btn');
      
      if (prevBtn) prevBtn.addEventListener('click', () => this.previousDay());
      if (nextBtn) nextBtn.addEventListener('click', () => this.nextDay());
    },

    // Select a station
    selectStation(stationKey) {
      state.currentStationKey = stationKey;
      state.currentDayOffset = 0; // Reset to today
      this.render();
      logState(`Selected station: ${stationKey}`);
    },

    // Navigate to previous day
    previousDay() {
      if (state.currentDayOffset > 0) {
        state.currentDayOffset--;
        this.render();
      }
    },

    // Navigate to next day
    nextDay() {
      if (state.currentDayOffset < 2) {
        state.currentDayOffset++;
        this.render();
      }
    },

    // Render current state
    render() {
      if (!state.currentStationKey) return;
      
      this.renderStationCard();
      this.renderChart();
      this.updateTimestamp();
    },

    // Render station info card
    renderStationCard() {
      // Implementation...
    },

    // Render tide chart
    renderChart() {
      try {
        if (!state.chart) {
          state.chart = echarts.init(document.getElementById('tide-chart'));
        }
        
        // Build chart option
        const option = this.buildChartOption();
        state.chart.setOption(option);
        
      } catch (error) {
        console.error('Chart render error:', error);
        this.showChartError(error);
      }
    },

    // Build chart configuration
    buildChartOption() {
      // Return ECharts option object
      return { /* ... */ };
    },

    // Update page timestamp
    updateTimestamp() {
      const el = document.getElementById('timestamp');
      if (el && state.currentData?.generated_utc) {
        const date = new Date(state.currentData.generated_utc);
        el.textContent = `Last updated: ${formatTimestamp(date)}`;
      }
    },

    // Show error message
    showError(error) {
      const container = document.getElementById('tide-error');
      if (container) {
        container.style.display = 'block';
        container.innerHTML = `<p>Error: ${error.message}</p>`;
      }
    },

    // Show chart error
    showChartError(error) {
      const container = document.getElementById('tide-chart');
      if (container) {
        container.innerHTML = `
          <div class="chart-error">
            <p>Chart failed to load: ${error.message}</p>
          </div>
        `;
      }
    },

    // Get current state (read-only for testing)
    getState: () => ({ ...state })
  };
})();

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
  TideModule.init();
});
```

**Benefits:**
- State encapsulated and controlled
- Clear public API
- Easier to test
- No global variable pollution
- Predictable behavior

---

## 7. CSS VARIABLES FOR THEMING

### BEFORE (Current - Hard-coded Colors)
```css
/* style-v3.css */
header {
  background: #004b7c;
}

.nav-link {
  color: #fff;
  border-bottom-color: #0077be;
}

.buoy-card h2 {
  color: #0066a1;
}

.data-table th {
  background: #004b7c;
  color: white;
}

/* ... many more color occurrences ... */
```

### AFTER (Better - CSS Variables)
```css
/* At the top of style-v3.css */
:root {
  /* ===== PRIMARY COLORS ===== */
  --color-primary-dark: #004b7c;
  --color-primary: #0077be;
  --color-primary-light: #0066a1;
  
  /* ===== SEMANTIC COLORS ===== */
  --color-success: #48bb78;
  --color-warning: #d97706;
  --color-error: #ef4444;
  --color-info: #3b82f6;
  
  /* ===== NEUTRALS ===== */
  --color-bg-primary: #ffffff;
  --color-bg-secondary: #f7fafc;
  --color-bg-tertiary: #eef4f8;
  --color-text-primary: #222;
  --color-text-secondary: #666;
  --color-text-muted: #999;
  --color-border: #ddd;
  --color-border-light: #e0e8f0;
  
  /* ===== SPACING SYSTEM ===== */
  --spacing-2xs: 0.25rem;
  --spacing-xs: 0.5rem;
  --spacing-sm: 0.75rem;
  --spacing-md: 1rem;
  --spacing-lg: 1.5rem;
  --spacing-xl: 2rem;
  --spacing-2xl: 3rem;
  
  /* ===== FONT SIZING ===== */
  --font-size-xs: 0.75rem;
  --font-size-sm: 0.875rem;
  --font-size-base: 1rem;
  --font-size-lg: 1.125rem;
  --font-size-xl: 1.25rem;
  --font-size-2xl: 1.5rem;
  
  /* ===== BORDER RADIUS ===== */
  --radius-sm: 4px;
  --radius-md: 6px;
  --radius-lg: 8px;
  --radius-xl: 12px;
  
  /* ===== SHADOWS ===== */
  --shadow-sm: 0 1px 2px rgba(0, 0, 0, 0.05);
  --shadow-md: 0 4px 6px rgba(0, 0, 0, 0.1);
  --shadow-lg: 0 10px 15px rgba(0, 0, 0, 0.1);
  
  /* ===== TRANSITIONS ===== */
  --transition-fast: 0.15s ease;
  --transition-base: 0.2s ease;
  --transition-slow: 0.3s ease;
  
  /* ===== BREAKPOINTS ===== */
  --breakpoint-xs: 480px;
  --breakpoint-sm: 600px;
  --breakpoint-md: 768px;
  --breakpoint-lg: 1000px;
  --breakpoint-xl: 1200px;
}

/* Now use variables throughout */
header {
  background: linear-gradient(
    to right, 
    var(--color-primary-dark), 
    var(--color-primary)
  );
  padding: var(--spacing-xl) var(--spacing-md);
}

.main-nav {
  background: var(--color-primary-dark);
  padding: var(--spacing-md) 0;
}

.nav-link {
  color: white;
  padding: var(--spacing-md) var(--spacing-lg);
  transition: background var(--transition-base);
  border-bottom-color: var(--color-primary);
}

.nav-link:hover {
  background: rgba(255, 255, 255, 0.1);
}

.buoy-card {
  background: var(--color-bg-primary);
  border-radius: var(--radius-lg);
  box-shadow: var(--shadow-md);
  padding: var(--spacing-lg);
  transition: transform var(--transition-base), 
              box-shadow var(--transition-base);
}

.buoy-card h2 {
  color: var(--color-primary-light);
  margin-bottom: var(--spacing-md);
}

.data-table th {
  background: var(--color-primary-dark);
  color: white;
  padding: var(--spacing-md);
}

/* Responsive without repeating breakpoints */
@media (max-width: var(--breakpoint-sm)) {
  body {
    font-size: var(--font-size-sm);
  }
  
  .buoy-card {
    padding: var(--spacing-md);
  }
}
```

**Dark Mode Support (Bonus):**
```css
/* Optional: Dark mode theme */
@media (prefers-color-scheme: dark) {
  :root {
    --color-primary-dark: #1e3a8a;
    --color-primary: #3b82f6;
    --color-text-primary: #e5e7eb;
    --color-text-secondary: #9ca3af;
    --color-bg-primary: #1f2937;
    --color-bg-secondary: #111827;
  }
}
```

**Benefits:**
- Change theme colors in one place
- Consistent spacing system
- Easy to implement dark mode
- Better maintainability
- Easier A/B testing

---

## BACKEND: DATA SOURCE UNIFICATION FOR WIND STATIONS

### ISSUE
Wind station data is currently split across two JSON files based on data source rather than station type, creating confusion in the frontend code.

**Current State:**
- Environment Canada wind stations → `latest_wind.json` (CWVF, CWGT, CWEL, etc.)
- NOAA land/C-MAN stations → `latest_buoy_v2.json` (CPMW1, SISW1, COLEB)

**Problems:**
1. Station type (`wind_monitoring_station`, `land_station`, `c_man_station`) doesn't indicate data source
2. Frontend must check both JSON files for wind station data
3. Field names differ between sources:
   - `latest_wind.json`: `wind_direction_deg`, `wind_speed_kt`
   - `latest_buoy_v2.json`: `wind_direction`, `wind_speed`
4. Popup code duplicates data source checking logic
5. New developers won't know where to find wind station data

**Current Workaround (stations-map.js:316-332):**
```javascript
// For wind stations, check both sources (prefer wind data, fall back to buoy data)
data = (latestWindData && latestWindData[buoy.id]) || (latestBuoyData && latestBuoyData[buoy.id]);
```

### SOLUTION OPTIONS

#### Option 1: Unify All Wind Stations (Recommended)
Move all wind-measuring stations (regardless of source) into a single `latest_wind.json` file.

**Backend Changes:**
```python
# In export script (e.g., export_wind_json.py)
def export_unified_wind_data():
    wind_data = {}

    # Add EC wind stations
    ec_stations = fetch_ec_wind_stations()
    for station_id, data in ec_stations.items():
        wind_data[station_id] = {
            'wind_speed_kt': data['wind_speed_kt'],
            'wind_direction_deg': data['wind_direction_deg'],
            'wind_direction_cardinal': data['wind_direction_cardinal'],
            'wind_gust_kt': data.get('wind_gust_kt'),
            'air_temp_c': data.get('air_temp_c'),
            'observation_time': data['observation_time'],
            'stale': data['stale']
        }

    # Add NOAA land/C-MAN stations (normalize field names)
    noaa_wind_stations = ['CPMW1', 'SISW1', 'COLEB']
    buoy_data = load_latest_buoy_data()
    for station_id in noaa_wind_stations:
        if station_id in buoy_data:
            data = buoy_data[station_id]
            wind_data[station_id] = {
                'wind_speed_kt': data['wind_speed'],  # Normalize to kt
                'wind_direction_deg': data['wind_direction'],  # Normalize to deg
                'wind_direction_cardinal': data.get('wind_direction_cardinal'),
                'wind_gust_kt': data.get('wind_gust'),
                'air_temp_c': data.get('air_temp'),
                'observation_time': data['observation_time'],
                'stale': data.get('stale', False)
            }

    # Write unified file
    with open('latest_wind.json', 'w') as f:
        json.dump(wind_data, f)
```

**Frontend Simplification (stations-map.js):**
```javascript
// BEFORE (complex):
let data = null;
if (isWaveStation) {
  data = latestBuoyData ? latestBuoyData[buoy.id] : null;
} else {
  data = (latestWindData && latestWindData[buoy.id]) || (latestBuoyData && latestBuoyData[buoy.id]);
}

// AFTER (simple):
const data = isWaveStation
  ? latestBuoyData?.[buoy.id]
  : latestWindData?.[buoy.id];
```

**Benefits:**
- Single source of truth for wind data
- Consistent field names across all wind stations
- Simpler frontend logic
- Clearer separation: buoys = waves, wind = wind

#### Option 2: Separate Station Collections in stations.json
Keep data sources separate but organize `stations.json` by data source.

**stations.json Structure:**
```json
{
  "wave_buoys": {
    "4600146": { "name": "Halibut Bank", "type": "wave_buoy", ... },
    "46087": { "name": "Neah Bay", "type": "wave_buoy", ... }
  },
  "ec_wind_stations": {
    "CWVF": { "name": "Vancouver", "source": "latest_wind.json", ... }
  },
  "noaa_wind_stations": {
    "CPMW1": { "name": "Cherry Point", "source": "latest_buoy_v2.json", ... }
  }
}
```

**Pros:** Explicit about data sources
**Cons:** Still requires frontend to check multiple files; doesn't solve field name inconsistency

### RECOMMENDATION
**Option 1** is strongly recommended. It provides:
- Clear data ownership (wind stations → wind file)
- Normalized field names
- Simplified frontend code
- Better developer experience

**Implementation Priority:** Medium (works fine now, but technical debt will grow as more stations are added)

**Affected Files:**
- Backend: `buoy_to_influx_sqlite.py`, wind export scripts
- Frontend: `stations-map.js`, `winds-map.js`
- Data: `latest_wind.json`, `latest_buoy_v2.json`

---

## Summary

These examples provide concrete implementations for the recommendations in the analysis report. Start with the quick wins and work your way through the recommendations in priority order.

For questions about any of these patterns, refer back to the detailed analysis report.
