# Frontend Version Unification Plan

**Date:** 2025-11-14
**Goal:** Standardize all frontend assets to a single version number (v4)

## Current State Analysis

### CSS Files (Currently v3)
- `assets/css/style-v3.css`
- `assets/css/nav-tide-styles-v3.css`
- `assets/css/stations-map-v3.css`
- `assets/css/warning-banner-v3.css`

### JS Files (Currently v2)
- `assets/js/charts-v2.js`
- `assets/js/chart-utils-v2.js`
- `assets/js/comparison-chart-v2.js`
- `assets/js/storm_surge_chart-v2.js`
- `assets/js/temperature-chart-v2.js`
- `assets/js/wave-chart-v2.js`
- `assets/js/wave-table-v2.js`
- `assets/js/wind-chart-v2.js`

### Unversioned JS Files
- `assets/js/main.js`
- `assets/js/storm_surge_page.js`
- `assets/js/forecasts.js`
- `assets/js/stations-map.js`
- `assets/js/tides.js`
- `assets/js/warning-banner.js`

### Backup/Unused Files (DELETE)
- `assets/js/main.js.backup` ❌
- `assets/js/warning-banner-v3-backup.js` ❌
- `git_backup.log` ❌

## Version Unification Strategy

**Target Version: v4** (major cleanup justifies major version bump)

### Phase 1: Cleanup (DELETE backups)
```bash
rm assets/js/main.js.backup
rm assets/js/warning-banner-v3-backup.js
rm git_backup.log
```

### Phase 2: Rename CSS Files (v3 → v4)
```bash
git mv assets/css/style-v3.css assets/css/style-v4.css
git mv assets/css/nav-tide-styles-v3.css assets/css/nav-tide-styles-v4.css
git mv assets/css/stations-map-v3.css assets/css/stations-map-v4.css
git mv assets/css/warning-banner-v3.css assets/css/warning-banner-v4.css
```

### Phase 3: Rename JS Files (v2 → v4)
```bash
git mv assets/js/charts-v2.js assets/js/charts-v4.js
git mv assets/js/chart-utils-v2.js assets/js/chart-utils-v4.js
git mv assets/js/comparison-chart-v2.js assets/js/comparison-chart-v4.js
git mv assets/js/storm_surge_chart-v2.js assets/js/storm_surge_chart-v4.js
git mv assets/js/temperature-chart-v2.js assets/js/temperature-chart-v4.js
git mv assets/js/wave-chart-v2.js assets/js/wave-chart-v4.js
git mv assets/js/wave-table-v2.js assets/js/wave-table-v4.js
git mv assets/js/wind-chart-v2.js assets/js/wind-chart-v4.js
```

### Phase 4: Version Unversioned Files
Consider adding v4 to currently unversioned files for consistency:
```bash
git mv assets/js/main.js assets/js/main-v4.js
git mv assets/js/storm_surge_page.js assets/js/storm_surge_page-v4.js
git mv assets/js/forecasts.js assets/js/forecasts-v4.js
git mv assets/js/stations-map.js assets/js/stations-map-v4.js
git mv assets/js/tides.js assets/js/tides-v4.js
git mv assets/js/warning-banner.js assets/js/warning-banner-v4.js
```

### Phase 5: Update HTML References

**Files to update:**
- `index.html` (11 references)
- `forecasts.html` (3 CSS references)
- `storm_surge.html` (3 CSS references)
- `tides.html` (3 CSS references)

**Search & Replace:**
- `-v3.css` → `-v4.css` (CSS files)
- `-v2.js` → `-v4.js` (JS files)
- `.js"` → `-v4.js"` (for unversioned files, if versioning them)

### Phase 6: Add Consistent Cache Busting

**Current state:** Only `index.html` has cache busting on one CSS file:
```html
<link rel="stylesheet" href="/assets/css/style-v3.css?v=20251112" />
```

**Standardize to:**
```html
<!-- Use YYYYMMDD format based on last file modification -->
<link rel="stylesheet" href="/assets/css/style-v4.css?v=20251114" />
<script src="assets/js/charts-v4.js?v=20251114"></script>
```

**Apply to all 4 HTML files consistently.**

## Implementation Checklist

- [ ] Phase 1: Delete 3 backup files
- [ ] Phase 2: Rename 4 CSS files (v3 → v4)
- [ ] Phase 3: Rename 8 JS files (v2 → v4)
- [ ] Phase 4: Decide on versioning unversioned JS files
- [ ] Phase 5: Update references in 4 HTML files
- [ ] Phase 6: Add cache busting to all asset references
- [ ] Test all pages load correctly
- [ ] Git commit with message: "Standardize all assets to v4"

## Benefits

✅ **Clarity:** Single version number across all files
✅ **Maintainability:** Easy to track what's current vs old
✅ **Cache Control:** Consistent cache busting strategy
✅ **Clean Repo:** No backup files cluttering the codebase
✅ **Git History:** Using `git mv` preserves file history

## Estimated Time

- Phases 1-3: 10 minutes (file operations)
- Phase 4: 5 minutes (decision + renames if needed)
- Phase 5: 20 minutes (HTML updates)
- Phase 6: 15 minutes (cache busting)
- Testing: 10 minutes

**Total: ~60 minutes**

## After Completion

Update this document with:
- [x] Completion date
- [x] Final version number used
- [x] Any issues encountered
- [x] Git commit hash
