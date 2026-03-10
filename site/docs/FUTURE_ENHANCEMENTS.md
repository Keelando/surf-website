# Future Enhancements

## Dependency Upgrades

The following CDN-loaded libraries are behind a major version and should be reviewed and tested before upgrading:

| Library | Current | Latest | Notes |
|---------|---------|--------|-------|
| htmx | 1.9.10 | 2.0.8 | Major version — review breaking changes |
| ECharts | 5.4.3 | 6.0.0 | Major version — chart configs may need updates |
| Leaflet | 1.9.4 | 1.9.4 | Already up to date |

### Priority
Low — no known bugs, but staying current improves security and compatibility.

---

## ~~Wave Direction Arrows~~ (Completed)

Implemented across all chart types:
- **Individual buoy charts**: Arrows for Halibut Bank, Sentry Shoal, Angeles Point (wave-chart-v4.js)
- **NOAA spectral charts**: Separate wind wave + swell direction arrows on height and period charts
- **Comparison chart**: Halibut Bank direction arrows above the global max height (comparison-chart-v4.js)
- Arrow rotation uses centralized `calculateArrowRotation()` in chart-utils-v4.js
- Responsive sampling: 3h desktop, 6h mobile
