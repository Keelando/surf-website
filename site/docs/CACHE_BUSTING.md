# Cache Busting Strategy

## Simple Manual Versioning

**Rule:** Bump the version parameter whenever you edit a file.

### For JavaScript Files

```html
<!-- When you edit main.js, increment the version -->
<script src="/assets/js/main.js?v=20251112b"></script>
<script src="/assets/js/main.js?v=20251114a"></script>  <!-- after edit -->
```

**Version format:** `YYYYMMDD` + optional letter suffix (`a`, `b`, `c`) for same-day updates

### For CSS Files

CSS files use filename versioning (`-v3`, `-v4`):
```html
<link rel="stylesheet" href="/assets/css/style-v3.css">
```

When making CSS changes, bump the filename version number:
```bash
git mv assets/css/style-v3.css assets/css/style-v4.css
# Update HTML references
```

## When to Bump Versions

✅ **Always bump when:**
- Modifying JavaScript logic
- Changing CSS styles
- Fixing bugs in code
- Adding new features

❌ **Don't bump when:**
- Only changing HTML content
- Updating data files (JSON)
- Changing comments only

## Quick Reference

| File Type | Strategy | Example |
|-----------|----------|---------|
| CSS | Filename version | `style-v3.css` → `style-v4.css` |
| JS | Query param | `main.js?v=20251114a` |
| HTML | No versioning | `index.html` |

## During Rapid Development

For rapid development phase, increment the letter suffix for same-day changes:
- `?v=20251114a` (first change)
- `?v=20251114b` (second change)
- `?v=20251114c` (third change)

Once development stabilizes, use date-only versions.
