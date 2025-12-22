# TODO - Future Improvements

## Tide Charts
- [ ] Add current time indicator on tide plots when viewing "today"
  - Option 1: Large dot at current position on the prediction/observation line
  - Option 2: Vertical dashed line spanning the chart at current time
  - Should only show when `currentDayOffset === 0` (viewing today)
  - File: `/home/keelando/site/assets/js/tides.js` - `displayTideChart()` function

## Mobile ECharts Issues
- [ ] Fix cursor/touch behavior on mobile for all ECharts plots
  - Currently "funky" - investigate specific issues
  - Affects: tide charts, buoy charts, wind charts, lightstation charts
  - May need to adjust ECharts touch/tooltip configuration
  - Test on actual mobile devices after fixing

## Completed (2025-12-22)
- [x] Daylight detection for webcam captures (Cox Bay only during daylight hours)
- [x] Sunlight times export for all tide stations and webcams
- [x] Sunlight times widget on tides page (updates per station)
- [x] Fixed Crescent Beach Ocean buoy decimal precision (2 decimals for heights, 1 for period)
