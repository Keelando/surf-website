# COMPREHENSIVE FRONTEND CODEBASE ANALYSIS REPORT
# Salish Sea Wave Conditions Website
# Date: November 14, 2025

## EXECUTIVE SUMMARY

The frontend codebase demonstrates solid foundational structure with modern patterns
(async/await, ES6+), but shows clear opportunities for refactoring and consolidation.
The site is reasonably functional but has code duplication, accessibility gaps, and
inconsistent patterns that could be improved.

Total Size: 4,708 lines of JavaScript, 25.9K CSS, 4 HTML pages

---

## 1. CODE DUPLICATION ANALYSIS

### 1.1 CRITICAL DUPLICATION - Navigation (Across All 4 HTML Files)

ISSUE: Every HTML file duplicates the navigation bar TWICE (top and bottom):

FILES AFFECTED:
  - /home/keelando/site/index.html (lines 63-68, 200-205)
  - /home/keelando/site/tides.html (lines 26-31, 157-162)
  - /home/keelando/site/forecasts.html (lines 189-194, 209-215)
  - /home/keelando/site/storm_surge.html (lines 129-134, 213-219)

DUPLICATION IMPACT: 8 identical nav blocks across 4 pages = 32 DOM elements per page

RECOMMENDATION: Extract to shared nav component (HTML include, template, or JS injection)

---

### 1.2 CRITICAL DUPLICATION - Footer & Meta Tags

ISSUE: Footer structure is nearly identical across all pages with minimal variation:

FILES AFFECTED:
  - All 4 HTML files contain footer with GitHub link, disclaimer, timestamp
  - Identical SVG GitHub icon (4x) - 9 lines per file
  - Identical styling: onmouseover/onmouseout handlers (appears in 4 files)

CONTENT DIFFERENCES:
  - index.html: "Data sources: Environment Canada, NOAA NDBC, DFO IWLS, City of Surrey"
  - tides.html: "Auto-updated from DFO IWLS API"
  - forecasts.html: "Auto-updated from Environment Canada's marine weather API"
  - storm_surge.html: "Auto-updated from Environment Canada GeoMet"

RECOMMENDATION: Use PHP includes, server-side templates, or JavaScript to inject footer

---

### 1.3 HEAD SECTION DUPLICATION

ISSUE: Meta tags and stylesheets loaded inconsistently:

index.html (no leading slash):
  <link rel="stylesheet" href="/assets/css/style-v3.css?v=20251112">
  <script src="assets/js/main.js?v=20251112b">

tides.html (inconsistent paths):
  <link rel="stylesheet" href="/assets/css/style-v3.css">
  <script src="assets/js/tides.js">

forecasts.html (with leading slashes):
  <link rel="stylesheet" href="/assets/css/style-v3.css">
  <script src="/assets/js/forecasts.js">

ISSUE: Cache busting applied inconsistently - only main.js has version params

RECOMMENDATION:
  - Use consistent path style (always leading slash)
  - Implement centralized cache busting strategy
  - Consider build step for automatic versioning

---

### 1.4 JavaScript Event Handlers - Inline Duplication

ISSUE: Inline event handler duplication (onmouseover/onmouseout):

LOCATION: 4 identical blocks in footer GitHub links

Example (appears in index.html:211, tides.html:168, forecasts.html:221, storm_surge.html:225):
  <a href="https://github.com/keelando/envcan_wave"
     onmouseover="this.style.opacity='1'"
     onmouseout="this.style.opacity='0.8'">

ALSO appears in index.html:136 (View Full Forecast button) with identical pattern:
  onmouseover="this.style.background='#005a8f'"
  onmouseout="this.style.background='#0077be'"

IMPACT: Maintenance burden - changes require editing multiple files

RECOMMENDATION: Move to CSS hover states or unified JavaScript event delegation

---

### 1.5 CSS Redundancy - Related Stylesheets

FILES: 4 CSS files totaling 25.9K

  - style-v3.css (13K) - Main styles
  - nav-tide-styles-v3.css (5.0K) - Navigation + Tide page styles
  - warning-banner-v3.css (5.2K) - Warning banner styles
  - stations-map-v3.css (2.7K) - Map-specific styles

ISSUE: Potential overlap in selectors:
  - `.main-nav` defined in nav-tide-styles-v3.css (lines 5-44)
  - `.tide-card`, `.tide-data-group` in nav-tide-styles-v3.css
  - Similar card/box patterns used across multiple files

RECOMMENDATION: Consolidate related CSS files, consider CSS variables for colors

---

## 2. INLINE STYLES vs EXTERNAL CSS

### SEVERITY: Moderate

### Inline Styles Found: 56 occurrences across HTML files

index.html has 29 inline styles:
  - Line 96: width/height divs for wave threshold explanation
  - Line 134-139: Storm surge forecast section header with flexbox
  - Line 136: Button with onmouseover/onmouseout handlers (BAD PRACTICE)
  - Line 143-146: Select dropdown and indicators
  - Line 149-150: Chart containers with margin
  - Line 172-178: GitHub link with opacity and icon alignment

tides.html has 13 inline styles:
  - Line 41-44: Label styling for station selector
  - Line 106-114: Chart date controls (flexbox)
  - Line 168: GitHub link in footer (repeated pattern)

forecasts.html has 4 inline styles:
  - Line 19-181: Entire style block (reasonable for page-specific styles)
  - Line 221: GitHub link (repeated pattern)

storm_surge.html has 10 inline styles:
  - Line 19-121: Entire style block (page-specific, acceptable)
  - Line 225: GitHub link (repeated pattern)

EXAMPLES OF PROBLEMATIC INLINE STYLES:

index.html line 136 (View Full Forecast button):
  style="padding: 0.5rem 1rem; background: #0077be; color: white; text-decoration: none; 
          border-radius: 4px; font-size: 0.9rem; font-weight: 600; transition: background 0.2s;"
  + onmouseover/onmouseout handlers

index.html line 244:
  <p class="buoy-metric" style="margin: 0.5rem 0;"><b>${waveLabel}</b> ${waveDisplay}</p>
  (CSS could define .buoy-metric margins)

index.html lines 250-280:
  Dynamic button creation with 15 inline style properties

RECOMMENDATIONS:
1. Extract repeated button styles to CSS classes
2. Create `.button-primary`, `.button-secondary` classes
3. Use CSS :hover states instead of onmouseover/onmouseout
4. Keep page-specific <style> blocks in <head> (currently done for forecasts/storm_surge)

---

## 3. JAVASCRIPT ORGANIZATION & PATTERNS

### 3.1 File Organization - Decent but Could Be Better

Main files (by lines of code):
  - tides.js (904 lines) - Main tide page logic
  - main.js (725 lines) - Main buoy page logic
  - storm_surge_page.js (677 lines) - Storm surge page logic
  - forecasts.js (367 lines) - Forecasts page logic
  - warning-banner.js (348 lines) - Warning system
  - wave-chart-v2.js (338 lines) - Wave visualization
  - stations-map.js (200 lines) - Map functionality
  - storm_surge_chart-v2.js (249 lines) - Storm surge chart
  - wind-chart-v2.js (163 lines) - Wind visualization
  - comparison-chart-v2.js (118 lines) - Comparison chart
  - charts-v2.js (100 lines) - General chart management
  - wave-table-v2.js (95 lines) - Wave data table
  - chart-utils-v2.js (86 lines) - Shared utilities
  - temperature-chart-v2.js (77 lines) - Temperature visualization
  - warning-banner-v3-backup.js (261 lines) - Unused backup file

ISSUE: Backup file "warning-banner-v3-backup.js" should be removed

STRUCTURE: Each page has its own main JS file + shared utilities
  - Index page loads: main.js + 7 chart modules + shared utilities
  - Tides page loads: tides.js (monolithic)
  - Forecasts page loads: forecasts.js (self-contained)
  - Storm surge page loads: storm_surge_page.js + storm_surge_chart-v2.js

---

### 3.2 Data Fetching Patterns

PATTERN CONSISTENCY: Good async/await usage across the board

Examples:

forecasts.js (lines 14-33):
  async function loadForecasts() {
    try {
      const response = await fetch('/data/marine_forecast.json');
      if (!response.ok) throw new Error('Failed to load forecast data');
      forecastData = await response.json();
      displayForecasts();
      updateTimestamp();
    } catch (error) {
      console.error('Error loading forecasts:', error);
      // User-friendly error display
    }
  }

tides.js (lines 34-72):
  Similar pattern with Promise.all() for parallel requests - GOOD

main.js (line 71):
  const response = await fetch(`/data/latest_buoy_v2.json?t=${Date.now()}`);
  Uses cache-busting with timestamp

RECOMMENDATIONS:
1. Create shared fetch wrapper with retry logic
2. Standardize error handling across all pages
3. Consider caching stale data as fallback
4. Add request timeout handling

---

### 3.3 State Management - Scattered Global Variables

ANTIPATTERN: Global variables for page state

tides.js (lines 5-12):
  let tideCurrentData = null;
  let tideTimeseriesData = null;
  let tideHighLowData = null;
  let combinedWaterLevelData = null;
  let stationsMetadata = null;
  let tideChart = null;
  let currentDayOffset = 0;
  let currentStationKey = null;

main.js (implicit - chart instances scattered)

storm_surge_page.js (lines 5-9):
  let forecastChart = null;
  let hindcastChart = null;
  let forecastData = null;
  let hindcastData = null;
  let observedSurgeData = null;

ISSUES:
- No namespace/module pattern
- Implicit dependencies between functions
- Hard to test individual functions
- Memory leaks possible if charts not destroyed

RECOMMENDATION: Consider module pattern or simple object namespacing

Example refactor:
  const tideModule = {
    data: { current: null, timeseries: null, ... },
    chart: null,
    currentDayOffset: 0,
    load: async () => { ... },
    render: () => { ... }
  };

---

### 3.4 Error Handling - Adequate but Could Be Better

GOOD PRACTICES FOUND:
- Try/catch blocks in async functions
- User-friendly error messages (forecasts.js, tides.js)
- HTTP status checking before parsing JSON
- Console errors logged

ISSUES:
1. Some files have minimal error handling:
   - wave-chart-v2.js: No error handling visible
   - wind-chart-v2.js: No error handling visible
   - temperature-chart-v2.js: No error handling visible

2. Error states not consistently styled/displayed:
   - forecasts.js uses `.error-state` CSS class
   - tides.js uses `showError()` function
   - main.js doesn't show error UI

3. Network failures could silently fail:
   - No retry mechanism
   - No timeout handling
   - No user notification in some cases

EXAMPLES OF GAPS:

main.js (line 71-86):
  Fetch without timeout:
    const response = await fetch(`/data/latest_buoy_v2.json?t=${Date.now()}`);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
  // Missing: What if JSON parse fails? What if network is slow?

wave-chart-v2.js (renders charts):
  No try/catch - chart rendering could fail silently

RECOMMENDATIONS:
1. Add fetch timeout wrapper:
     const fetchWithTimeout = (url, timeout = 5000) => {
       return Promise.race([
         fetch(url),
         new Promise((_, reject) => 
           setTimeout(() => reject(new Error('Timeout')), timeout)
         )
       ];
     }

2. Wrap chart rendering in try/catch

3. Consistent error UI across all pages

---

## 4. CSS ORGANIZATION

### 4.1 File Structure

Four CSS files with "-v3" version suffix (outdated versioning strategy):
  - style-v3.css (13K) - Main/core styles
  - nav-tide-styles-v3.css (5.0K) - Navigation and tide-specific
  - warning-banner-v3.css (5.2K) - Warning banner states
  - stations-map-v3.css (2.7K) - Map component styles

ISSUE: "v3" suffix suggests multiple iterations exist. No v1/v2 found - confusing naming.

TOTAL SIZE: 25.9K (reasonable for this project)

---

### 4.2 Color Inconsistencies

PRIMARY COLOR USAGE:
  #004b7c appears 17 times in style-v3.css alone
  #0077be appears 2+ times
  #0066a1 appears 1 time
  Similar shades used for headers, links, backgrounds

RECOMMENDATION: Define CSS variables for theming:
  :root {
    --primary-dark: #004b7c;
    --primary-light: #0077be;
    --primary-lighter: #0066a1;
    --background: #eef4f8;
    --text-default: #222;
    --text-muted: #666;
    --border: #ddd;
  }

---

### 4.3 Media Queries - Scattered and Adequate

Good practice: Multiple breakpoints at 600px, 768px, 1000px, 1200px

Issues:
- Same breakpoints defined in multiple files (breakpoint duplication)
- CSS Grid/Flexbox used well
- Some hardcoded sizes could be more responsive

---

### 4.4 Animation & Transitions

ANIMATIONS FOUND:
  - @keyframes fadeIn (style-v3.css line 509)
  - @keyframes pulse (style-v3.css line 520)
  - @keyframes warningPulse (warning-banner-v3.css line 154)
  - @keyframes stormPulse (warning-banner-v3.css line 164)

GOOD: Subtle, accessible animations (2-4s duration)

ISSUE: Animation durations not centralized (could use CSS variables)

---

## 5. ACCESSIBILITY ANALYSIS

### SEVERITY: Moderate - Missing WCAG Compliance Features

### 5.1 Missing Alt Text & ARIA Labels

FINDING: Zero alt attributes or ARIA labels found in HTML files

  - SVG icons used throughout (GitHub logo in footer, weather icons)
  - Emoji used as icons (🌊, 💨, 📍) - not accessible to screen readers
  - No role attributes for custom interactive elements
  - No aria-live regions for dynamic content updates

RECOMMENDATIONS:
1. Add alt="GitHub repository" to GitHub SVG links
2. Replace emoji with proper icon fonts or labeled icons
3. Add aria-label to interactive buttons:
     <button aria-label="Toggle buoy card details">▼ Show Details</button>
4. Add aria-live regions for dynamic updates:
     <div id="buoy-container" aria-live="polite" aria-label="Buoy data cards">

---

### 5.2 Keyboard Navigation

ISSUES:
  - Inline onclick handlers (not keyboard accessible):
    index.html line 251-262 (toggleCardDetails function)
  - Some interactive elements may lack :focus styles
  - Map interactions (Leaflet) should inherit accessibility

RECOMMENDATIONS:
1. Replace inline onclick with proper form submission or data attributes
2. Ensure all interactive elements have visible :focus styles
3. Test with keyboard navigation (Tab key)

---

### 5.3 Color Contrast

FINDING: Most text passes contrast ratio, but some areas may have issues:
  - Warning banners use gradients - contrast should be verified
  - Disabled states (if any) need WCAG AA compliance check

RECOMMENDATION: Run through WebAIM Contrast Checker on live site

---

### 5.4 Form Labels

FINDING: Selectors properly labeled:
  - "Select Tide Station:" in tides.html
  - "Select Buoy:" in index.html
  - "Select Station:" in forecasts/storm_surge

GOOD: Associated with <label for="..."> elements

---

## 6. OBVIOUS BUGS & ISSUES

### 6.1 Unused/Dead Code

FILE: /home/keelando/site/assets/js/warning-banner-v3-backup.js (261 lines)
  - Appears to be old version of warning-banner.js
  - Not referenced in any HTML file
  - Should be deleted

ACTION: Remove warning-banner-v3-backup.js

---

### 6.2 Version/Cache Busting Inconsistency

index.html:
  <link rel="stylesheet" href="/assets/css/style-v3.css?v=20251112">
  <script src="assets/js/main.js?v=20251112b">

tides.html, forecasts.html, storm_surge.html:
  No version parameters on CSS/JS

PROBLEM: 
- CSS file has cache buster, JS doesn't (or inconsistently)
- Manual versioning is error-prone
- Easy to miss updates when deploying

RECOMMENDATION: Use automatic versioning or consistent strategy

---

### 6.3 Inconsistent Data Age/Staleness Handling

forecasts.js (lines 40-66):
  Checks if data is > 8 hours old and displays warning
  Clear staleness thresholds defined

main.js (lines 155-158):
  Checks if observation is > 180 minutes and marks as stale

tides.js:
  No visible staleness check in provided excerpt

ISSUE: Different thresholds and handling across pages

RECOMMENDATION: Centralize staleness checking logic

---

### 6.4 Missing Error Handling in Chart Modules

Files with no visible error handling:
  - wave-chart-v2.js
  - wind-chart-v2.js
  - temperature-chart-v2.js
  - comparison-chart-v2.js
  - wave-table-v2.js
  - stations-map.js

If data is malformed or API fails, charts silently break.

---

### 6.5 Chart Initialization Race Conditions

index.html loads multiple chart modules:
  - chart-utils-v2.js
  - wave-chart-v2.js
  - wind-chart-v2.js
  - temperature-chart-v2.js
  - comparison-chart-v2.js
  - wave-table-v2.js
  - charts-v2.js
  - storm_surge_chart-v2.js

POTENTIAL ISSUE: If main.js runs before utility modules load, ReferenceError possible

CURRENT SCRIPT ORDER (index.html lines 220-236):
  1. Leaflet JS (external)
  2. ECharts CDN
  3. warning-banner.js
  4. main.js
  5. stations-map.js
  6. chart-utils-v2.js
  7. wave-chart-v2.js
  ... more modules

RISK: main.js (line 4) might call functions from chart-utils before they're loaded

RECOMMENDATION: Ensure chart-utils-v2.js loads before main.js

---

## 7. OPPORTUNITIES FOR REFACTORING

### 7.1 Component Extraction (High Priority)

EXTRACT SHARED COMPONENTS:

1. Navigation Bar
   - Current: Hardcoded in 4 places (8 instances total)
   - Extract to: nav.html or nav.js snippet
   - Loads once, includes via JS or template

2. Footer
   - Current: Duplicated across 4 pages with slight variations
   - Extract to: shared footer.html
   - Pass data source string as parameter

3. Warning Banner Container
   - Current: <div id="warning-banner-container"></div> on every page
   - Same across all pages
   - Good: Already uses JS injection (warning-banner.js)
   - Current state is good - no change needed

---

### 7.2 Utility Function Consolidation (Medium Priority)

SCATTERED UTILITIES:
  - formatTimestamp() in forecasts.js (lines 301-313)
  - Similar formatting in tides.js and main.js
  - degreesToCardinal() and getDirectionalArrow() in main.js
  - formatTimeAxis(), formatCompactTimeLabel() in chart-utils-v2.js

RECOMMENDATION: Create utils.js with all shared functions:
  utils/datetime.js:
    - formatTimestamp()
    - formatTimeAxis()
    - formatCompactTimeLabel()

  utils/weather.js:
    - degreesToCardinal()
    - getDirectionalArrow()
    - getWarningSeverityClass() (currently in forecasts.js)

  utils/fetch.js:
    - fetchWithTimeout()
    - retryFetch()
    - handleErrors()

---

### 7.3 CSS Variables for Theming (Medium Priority)

Add to top of style-v3.css:
  :root {
    /* Colors */
    --color-primary-dark: #004b7c;
    --color-primary: #0077be;
    --color-primary-light: #0066a1;
    --color-background: #eef4f8;
    --color-surface: #ffffff;
    --color-surface-alt: #f7fafc;
    --color-border: #ddd;
    --color-text: #222;
    --color-text-muted: #666;
    
    /* Spacing */
    --spacing-xs: 0.25rem;
    --spacing-sm: 0.5rem;
    --spacing-md: 1rem;
    --spacing-lg: 1.5rem;
    --spacing-xl: 2rem;
    
    /* Breakpoints (for media queries) */
    --breakpoint-mobile: 600px;
    --breakpoint-tablet: 768px;
    --breakpoint-desktop: 1200px;
  }

Then use: background: var(--color-primary-dark) instead of #004b7c

---

### 7.4 State Management Improvement (Medium Priority)

Current: Loose global variables
Better: Module pattern with data encapsulation

Example for tides page:
  const TideModule = {
    state: {
      currentData: null,
      timeseriesData: null,
      currentDayOffset: 0,
      currentStationKey: null
    },
    
    async init() {
      await this.loadData();
      this.setupEventListeners();
      this.render();
    },
    
    async loadData() { ... },
    setupEventListeners() { ... },
    render() { ... }
  };
  
  // Initialize on page load
  document.addEventListener('DOMContentLoaded', () => TideModule.init());

Benefits:
  - Data scoped to module
  - Easier to test
  - Clearer dependencies
  - Prevents global namespace pollution

---

### 7.5 Chart Initialization Wrapper (Low Priority)

Currently: Each page has different chart initialization

Suggest: Create chart-manager.js wrapper:
  class ChartManager {
    constructor(containerId) {
      this.container = document.getElementById(containerId);
      this.chart = null;
    }
    
    async init(dataFetcher) {
      try {
        const data = await dataFetcher();
        this.render(data);
      } catch (error) {
        this.showError(error);
      }
    }
    
    render(data) {
      this.chart = echarts.init(this.container);
      this.chart.setOption(this.getOption(data));
    }
    
    showError(error) {
      this.container.innerHTML = `<div class="error">Chart failed to load</div>`;
    }
  }

---

## 8. DATA FETCHING PATTERNS & IMPROVEMENTS

### Current Pattern:

1. Async/await used consistently (good)
2. Cache busting with ?t=${Date.now()} (good)
3. Basic error handling (adequate)

### Issues:

1. No timeout handling
2. No retry mechanism
3. No offline fallback
4. No progress indication for large fetches
5. Error messages not user-friendly everywhere

### Recommended Fetch Wrapper:

```javascript
async function fetchWithRetry(url, options = {}) {
  const maxRetries = options.maxRetries || 3;
  const timeout = options.timeout || 10000;
  const retryDelay = options.retryDelay || 1000;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), timeout);
      
      const response = await fetch(url, { ...options, signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      return await response.json();
    } catch (error) {
      if (attempt === maxRetries) throw error;
      
      console.warn(`Fetch attempt ${attempt} failed, retrying...`, error);
      await new Promise(resolve => setTimeout(resolve, retryDelay * attempt));
    }
  }
}
```

---

## 9. RESPONSIVE DESIGN QUALITY

### Good Practices:
- Multiple media queries (600px, 768px, 1200px)
- Flexbox and CSS Grid used effectively
- Mobile-first approach in most components
- Map resizes properly
- Tables scrollable on mobile

### Issues:
- Some hardcoded pixel widths could be more flexible
- Chart heights fixed (500px, 400px) - could be relative
- Navigation might overflow on very small screens

---

## 10. SUMMARY OF FINDINGS

### By Severity:

CRITICAL (Fix Now):
  1. Remove unused warning-banner-v3-backup.js
  2. Consolidate duplicated navigation (8 instances)
  3. Standardize error handling across all data fetches
  4. Replace inline onmouseover/onmouseout with CSS :hover

HIGH (Fix Soon):
  1. Add accessibility labels (alt text, ARIA)
  2. Extract and consolidate footer
  3. Implement fetch timeout and retry logic
  4. Add error handling to chart modules
  5. Consistent path styling (leading slashes)

MEDIUM (Nice to Have):
  1. CSS variables for colors and spacing
  2. State management refactoring (module pattern)
  3. Shared utility consolidation
  4. Consistent cache busting strategy
  5. Centralized timestamp formatting

LOW (Polish):
  1. Remove "-v3" suffix, use SemVer
  2. Chart initialization wrapper
  3. Remove console.logs from production

---

## 11. ACCESSIBILITY SCORE

Current: 4/10

Issues:
  - No alt text: -3 points
  - No ARIA labels: -2 points
  - Emoji icons not labeled: -1 point

Quick wins to improve:
  1. Add alt="GitHub repository" to GitHub links
  2. Add aria-label to buttons
  3. Add aria-live to dynamic sections
  4. Ensure :focus styles visible

With improvements: Could reach 8/10

---

## 12. PERFORMANCE OBSERVATIONS

Positive:
  - Reasonable CSS size (25.9K)
  - No render-blocking resources detected
  - Async scripts used
  - LocalStorage used for dismissals

Potential Issues:
  - Large JSON files from /data/ not compressed visibility
  - ECharts is large library (5.4.3) - 200KB+ minified
  - Multiple chart instances per page

Recommendations:
  - Compress JSON responses with gzip
  - Consider lazy-loading charts
  - Use smaller charting library if possible

---

## CONCLUSION

The codebase is FUNCTIONAL and REASONABLY WELL-ORGANIZED, but shows clear
opportunities for refactoring and consolidation. With focused effort on the
CRITICAL and HIGH priority items, code quality and maintainability could
improve significantly.

ESTIMATED EFFORT TO ADDRESS:
  - Critical items: 4-6 hours
  - High priority: 8-12 hours  
  - Medium priority: 12-16 hours
  - Low priority: 4-8 hours
  
Total: ~30-40 hours to address all recommendations

QUICK WINS (1-2 hours):
  1. Remove unused backup file
  2. Replace 4 instances of onmouseover/onmouseout with CSS
  3. Add basic alt text to images/icons
  4. Add error handling to chart modules

---

