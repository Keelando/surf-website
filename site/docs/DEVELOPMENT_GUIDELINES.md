# Development Guidelines

## File Permissions

### Critical: Set Permissions on File Creation

**Problem**: Files created via Write tool default to `600` permissions (owner read/write only), which prevents the web server from serving them.

**Solution**: Always set permissions to `644` immediately after creating any web-accessible file.

### Standard Practice

When creating new files in the web directory, follow this pattern:

```bash
# After creating a file with Write tool:
chmod 644 /path/to/new/file.js
chmod 644 /path/to/new/file.css
chmod 644 /path/to/new/file.html
```

### Required Permissions by File Type

- **JavaScript files** (`.js`): `644` (`-rw-r--r--`)
- **CSS files** (`.css`): `644` (`-rw-r--r--`)
- **HTML files** (`.html`): `644` (`-rw-r--r--`)
- **JSON data files** (`.json`): `644` (`-rw-r--r--`)
- **Image files** (`.png`, `.jpg`, `.ico`): `644` (`-rw-r--r--`)
- **Markdown docs** (`.md`): `644` (`-rw-r--r--`)
- **Directories**: `755` (`drwxr-xr-x`)

### Verification Command

After creating files, verify permissions:

```bash
ls -lh /path/to/file
```

Expected output: `-rw-r--r--` (644)

### Batch Fix Command

If multiple files have wrong permissions:

```bash
# Fix all JS files
find /home/keelando/site/assets/js -type f -name "*.js" -exec chmod 644 {} \;

# Fix all CSS files
find /home/keelando/site/assets/css -type f -name "*.css" -exec chmod 644 {} \;

# Fix all HTML files
find /home/keelando/site -maxdepth 1 -type f -name "*.html" -exec chmod 644 {} \;
```

## Historical Issues

### 2025-11-17: Logger.js Permission Issue
- **File**: `/home/keelando/site/assets/js/logger.js`
- **Problem**: Created with `600` permissions, web server couldn't serve it
- **Symptom**: `ReferenceError: logger is not defined` in browser console
- **Solution**: `chmod 644 /home/keelando/site/assets/js/logger.js`
- **Lesson**: Always set permissions immediately after file creation

## Code Quality Standards

### Logging
- Use centralized logger (`logger.debug()`, `logger.info()`, `logger.warn()`, `logger.error()`)
- Never use raw `console.*` statements (except in logger.js itself)
- Always provide context as first parameter: `logger.error('ComponentName', 'Error message', error)`

### JavaScript Best Practices
- Use strict mode when appropriate
- Prefer `const` and `let` over `var`
- Use template literals for string interpolation
- Handle errors gracefully with try/catch
- Validate function parameters
- Use JSDoc comments for public functions

### Mobile Responsiveness
- Test responsive sampling for visual elements (e.g., wind arrows: 6h mobile, 3h desktop)
- Use `window.innerWidth < 600` as mobile breakpoint
- Always test on mobile before deploying

### Wind/Wave Direction Arrow Rotation (CRITICAL - Very Easy to Mess Up!)

**⚠️ WARNING**: Direction arrow rotation is **extremely confusing** and has caused multiple bugs. Read this entire section before touching arrow code!

#### The Struggle (Historical Note)
This took multiple debugging sessions to get right. The confusion stems from:
1. Different coordinate systems between inline SVG and ECharts
2. ECharts rotating **counter-clockwise** despite documentation suggesting clockwise
3. Meteorological convention (wind FROM direction vs arrow showing TO direction)

**The Short Answer (if you're in a hurry):**
```javascript
symbolRotate: -direction  // NEGATIVE! ECharts rotates counter-clockwise!
```

#### Arrow Symbol
Custom centered arrow: `'path://M0,12 L-4,-8 L0,-6 L4,-8 Z'` (also `'path://M0,10 L-4,-10 L0,-8 L4,-10 Z'`)
- Default orientation: Arrow points **DOWN** (South) at 0° rotation
- Centered at origin (0,0) to prevent offset during rotation
- Arrow point at (0,12) or (0,10), wings at top

#### Meteorological Convention
Wind/wave direction indicates where wind/waves are **COMING FROM**:
- 0° = North (coming from North, blowing/moving toward South)
- 90° = East (coming from East, blowing/moving toward West)
- 180° = South (coming from South, blowing/moving toward North)
- 270° = West (coming from West, blowing/moving toward East)

**Arrows point in the direction wind/waves are GOING TO** (opposite of where they come from).

#### The Critical Discovery: ECharts Rotates COUNTER-CLOCKWISE

After empirical testing with colored test arrows, we discovered:
- **ECharts rotation is COUNTER-CLOCKWISE**, not clockwise as expected
- When `symbolRotate: 90`, the arrow rotates 90° counter-clockwise from DOWN → points RIGHT
- But we NEED it to rotate clockwise: 90° clockwise from DOWN → points LEFT

**Solution**: Negate the angle to reverse rotation direction!

```javascript
symbolRotate: -direction  // Negate to convert counter-clockwise to clockwise
```

#### Expected Behavior (VERIFIED with Test Arrows)

| Direction | Coming From | Arrow Points | symbolRotate Value | ECharts Result |
|-----------|-------------|--------------|-------------------|----------------|
| 0° (North) | N → S | **DOWN** (↓) | `-0° = 0°` | DOWN ✓ |
| 90° (East) | E → W | **LEFT** (←) | `-90° = -90°` | LEFT ✓ |
| 180° (South) | S → N | **UP** (↑) | `-180° = -180°` | UP ✓ |
| 270° (West) | W → E | **RIGHT** (→) | `-270° = -270°` | RIGHT ✓ |

**CRITICAL RULE**: **0 degrees North = arrow points DOWN** ↓, **90 degrees East = arrow points LEFT** ←

#### Why This Works
1. Our arrow SVG points DOWN by default
2. For a North wind (0°), we want arrow to point DOWN → `-0° = 0°` → no rotation ✓
3. For an East wind (90°), we want arrow to point LEFT (90° clockwise from DOWN)
4. But ECharts rotates counter-clockwise, so 90° would point RIGHT ✗
5. By negating: `-90°` rotates 90° counter-clockwise backwards = 270° counter-clockwise = 90° clockwise ✓

#### Common Mistakes (These ALL Failed!)

❌ **Wrong #1**: Using direction directly
```javascript
symbolRotate: direction  // WRONG - produces backwards horizontal arrows
// Result: 90° points RIGHT (should be LEFT), 270° points LEFT (should be RIGHT)
```

❌ **Wrong #2**: Adding 180° offset
```javascript
symbolRotate: (direction + 180) % 360  // WRONG - completely reverses everything
// Result: 0° points UP, 180° points DOWN (totally backwards)
```

❌ **Wrong #3**: Assuming clockwise rotation
```javascript
// The documentation LIES! ECharts rotates counter-clockwise!
```

✅ **CORRECT**: Negate the angle
```javascript
const direction = dirPoint.value; // Meteorological direction (coming FROM)
// ECharts rotates counter-clockwise, so negate to get clockwise rotation
symbolRotate: -direction  // This is the ONLY way that works!
```

#### Inline SVG vs ECharts Difference

**Inline SVG (in HTML tables):**
```javascript
// From wind-stations.js getDirectionalArrow()
const rotation = degrees; // Works without negation in CSS transform
return `<span style="transform:rotate(${rotation}deg)">${svg}</span>`;
```
CSS `transform: rotate()` is **clockwise**, so no negation needed.

**ECharts (in charts):**
```javascript
// From wind-stations.js, wind-chart-v4.js, wave-chart-v4.js
symbolRotate: -direction  // Must negate because ECharts is counter-clockwise
```

**DO NOT** try to make these consistent - they use different coordinate systems!

#### Testing Procedure

**ALWAYS test arrow rotation with this diagnostic code before trusting it:**

```javascript
// Add 4 test arrows at cardinal directions
const tests = [
  { deg: 0, color: '#ff0000', label: 'N(↓)' },   // Red - should point DOWN
  { deg: 90, color: '#00ff00', label: 'E(←)' },  // Green - should point LEFT
  { deg: 180, color: '#0000ff', label: 'S(↑)' }, // Blue - should point UP
  { deg: 270, color: '#ff00ff', label: 'W(→)' }  // Magenta - should point RIGHT
];

tests.forEach(test => {
  testArrowData.push({
    value: [timestamp, yValue],
    symbolRotate: -test.deg,  // Apply same transform as real arrows
    itemStyle: { color: test.color }
  });
});
```

View the chart and verify:
1. **Red** (North, 0°) → Points DOWN ↓
2. **Green** (East, 90°) → Points LEFT ←
3. **Blue** (South, 180°) → Points UP ↑
4. **Magenta** (West, 270°) → Points RIGHT →

If ANY arrow points wrong, DO NOT DEPLOY!

#### Affected Files
- `/assets/js/wind-stations.js` (line ~504)
- `/assets/js/wind-chart-v4.js` (line ~47)
- `/assets/js/wave-chart-v4.js` (line ~47)

**All three files MUST use the same formula: `symbolRotate: -direction`**

#### Final Notes
- This was debugged empirically with test arrows in November 2025
- Previous versions incorrectly used `direction + 180` or `direction` directly
- The negation is NOT a hack - it's the correct solution for ECharts' counter-clockwise rotation
- DO NOT "fix" this to match inline SVG - they use different coordinate systems
- When in doubt, add the diagnostic test arrows and verify visually

## Git Workflow

### Committing Changes
- Only commit when explicitly requested by user
- Write clear, descriptive commit messages
- Follow conventional commit format when appropriate
- Include co-author attribution as specified in git guidelines

## File Organization

### Directory Structure
```
/home/keelando/site/
├── assets/
│   ├── js/          # JavaScript modules
│   └── css/         # Stylesheets
├── data/            # JSON data files
├── docs/            # Documentation and planning
└── *.html           # HTML pages (root level)
```

### Documentation Location
- Implementation docs: `/docs/`
- Future enhancements: `/docs/FUTURE_ENHANCEMENTS.md`
- Development guidelines: `/docs/DEVELOPMENT_GUIDELINES.md` (this file)
- Planning/analysis: Keep in `/docs/` to avoid root clutter

## Testing Checklist

Before deploying changes:
- [ ] Check file permissions (`ls -lh`)
- [ ] Verify no console.* statements (except in logger.js)
- [ ] Test on desktop browser
- [ ] Test on mobile browser or responsive mode
- [ ] Hard refresh browser (Ctrl+Shift+R / Cmd+Shift+R)
- [ ] Check browser console for errors
- [ ] Verify all charts/features load correctly
