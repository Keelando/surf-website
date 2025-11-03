# Firefox Android Directional Arrow Bug

## Issue Description
Rotated directional arrows (wind/wave direction indicators) do not render on Firefox for Android tablets, despite working correctly on all other browsers including Chrome on the same device.

## Environment
- **Device:** Lenovo Tab Plus
- **Browser:** Mozilla Firefox (Android)
- **Affected Elements:** `.direction-arrow` / `.dir-arrow` spans with CSS transforms
- **Works On:** Chrome Android, Safari iOS, all desktop browsers

## Symptoms
- Arrows completely invisible (not present in rendered DOM output)
- No console errors
- Degree numbers and cardinal directions display correctly
- Empty space where arrows should appear

## Attempted Fixes (All Failed)
1. GPU acceleration (`translateZ(0)`, `will-change: transform`)
2. Removed inline style conflicts (duplicate `display` properties)
3. Simplified CSS from 20+ properties to 6 essential properties
4. Removed `backface-visibility`, font smoothing, `text-rendering` hints
5. Added explicit font-family stack (Noto Sans Symbols, Segoe UI, etc.)
6. Changed from Unicode literals (`'↓'`, `'➤'`) to HTML entities (`&#8595;`, `&#10142;`)
7. Changed class names and simplified to bare minimum CSS

## Current Implementation
Reverted to absolute simplest implementation:
- Single arrow character (`↓`) rotated via inline `transform:rotate()`
- Minimal CSS: just `display: inline-block`, `margin-left`, `color`
- No font-family overrides, no vendor prefixes, no optimization hints

## Possible Root Causes
1. **Firefox Content Sanitization:** Firefox Android may strip or block certain Unicode characters when inserted via `innerHTML`
2. **Font Rendering Pipeline:** Different text rendering engine than Chrome on Android
3. **Transform Bug:** Known Firefox Android issue with rotated inline elements
4. **CSP or Security Policy:** Firefox may have stricter default policies

## Research Directions
- Check Firefox Android dev console for hidden warnings
- Test with CSS-generated arrows (`:before` pseudo-elements with `content: "↓"`)
- Try SVG arrows instead of Unicode
- Test with simple text characters like `^` or `v` to see if rotation works at all
- Check if Firefox Android has known bugs with `transform` on `inline-block` spans
- Test rendering in Firefox Nightly to see if it's a known/fixed issue

## Workaround Options
1. **CSS Pseudo-elements:** Generate arrows via `:before` or `:after` with `content` property
2. **SVG Icons:** Replace Unicode with inline SVG arrows
3. **Text Fallback:** Use ASCII characters (`^`, `v`, `<`, `>`) instead of Unicode
4. **Emoji:** Try emoji arrows (🔽, ➡️) which may have better font support
5. **Background Images:** Use data URIs or small PNG arrows
6. **User-Agent Detection:** Serve different HTML to Firefox Android

## Files Affected
- `/home/keelando/site/assets/js/main.js` - `getDirectionalArrow()` function
- `/home/keelando/site/assets/css/style-v2.css` - `.dir-arrow` class

## Date
2025-11-03
