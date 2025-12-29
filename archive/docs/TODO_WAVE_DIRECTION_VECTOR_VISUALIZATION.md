# TODO: Wave Direction Vector Visualization

## Feature Description
Create a visual vector representation of wave/swell direction showing the directional spread.

## Visual Design
```
        ↑ (small arrow at max angle)
       /
      /
     ↑ (main arrow at average direction - larger)
      \
       \
        ↑ (small arrow at min angle)
```

## Implementation Details

### What to Show
- **Main vector arrow**: Points in the average wave direction
  - Larger, more prominent arrow
  - Uses `wave_direction_avg` or `wave_direction_peak`

- **Spread indicators**: Two smaller arrows showing the angular range
  - Left arrow: `wave_direction_avg - (wave_direction_spread_avg / 2)`
  - Right arrow: `wave_direction_avg + (wave_direction_spread_avg / 2)`
  - Smaller, lighter colored to distinguish from main arrow

### Visual Properties
- **Main arrow**:
  - Color: Blue (#1e88e5)
  - Size: Larger (e.g., 24px)
  - Opacity: 1.0

- **Spread arrows**:
  - Color: Light blue or gray (rgba(30, 136, 229, 0.5))
  - Size: Smaller (e.g., 16px)
  - Opacity: 0.6

- **Optional: Arc/Fan**
  - Draw a subtle arc between the two spread arrows
  - Shows the angular sector visually
  - Very light gray or blue, low opacity

### Where to Display
Options:
1. **In buoy card details section** - Small inline diagram next to wave direction
2. **Dedicated visualization panel** - Larger compass-style display
3. **On map markers** - Mini version on buoy map markers
4. **In charts** - Add to wave direction charts

### Technical Approach

#### SVG Implementation
```javascript
function createWaveDirectionVector(avgDirection, spread) {
  const halfSpread = spread / 2;
  const minDir = avgDirection - halfSpread;
  const maxDir = avgDirection + halfSpread;

  return `
    <svg width="60" height="60" viewBox="0 0 60 60">
      <!-- Background circle (optional) -->
      <circle cx="30" cy="30" r="25" fill="none" stroke="#e0e7ee" stroke-width="1"/>

      <!-- Spread arc -->
      <path d="..." fill="rgba(30, 136, 229, 0.1)" />

      <!-- Min direction arrow (smaller, lighter) -->
      <g transform="rotate(${minDir} 30 30)">
        <path d="M30,45 L26,35 L30,37 L34,35 Z"
              fill="rgba(30, 136, 229, 0.5)" />
      </g>

      <!-- Max direction arrow (smaller, lighter) -->
      <g transform="rotate(${maxDir} 30 30)">
        <path d="M30,45 L26,35 L30,37 L34,35 Z"
              fill="rgba(30, 136, 229, 0.5)" />
      </g>

      <!-- Main average direction arrow (larger, prominent) -->
      <g transform="rotate(${avgDirection} 30 30)">
        <path d="M30,48 L24,32 L30,35 L36,32 Z"
              fill="#1e88e5" />
      </g>

      <!-- Cardinal directions (N, E, S, W) -->
      <text x="30" y="10" text-anchor="middle" font-size="8" fill="#666">N</text>
      <text x="50" y="33" text-anchor="middle" font-size="8" fill="#666">E</text>
      <text x="30" y="55" text-anchor="middle" font-size="8" fill="#666">S</text>
      <text x="10" y="33" text-anchor="middle" font-size="8" fill="#666">W</text>
    </svg>
  `;
}
```

#### Canvas Implementation (Alternative)
Could use HTML5 Canvas for more dynamic/animated visualizations.

### Data Requirements
Available from EC buoys (Halibut Bank, Sentry Shoal):
- ✅ `wave_direction_avg` - Average wave direction
- ✅ `wave_direction_peak` - Peak wave direction
- ✅ `wave_direction_spread_avg` - Average directional spread
- ✅ `wave_direction_spread_peak` - Peak directional spread

### Benefits
1. **Intuitive visualization** - Users immediately understand directional spread
2. **Quick assessment** - See at a glance if seas are organized or confused
3. **Better than text** - Visual > "E to ESE (84° to 120°)"
4. **Professional appearance** - Looks like proper marine weather displays
5. **Educational** - Helps users understand wave mechanics

### Implementation Priority
**Medium** - Nice to have, enhances UX significantly

### Estimated Effort
- **Design**: 1-2 hours (finalize visual style, colors, sizes)
- **SVG Implementation**: 2-3 hours (create vector graphics, test rotations)
- **Integration**: 1-2 hours (add to buoy cards, responsive design)
- **Testing**: 1 hour (verify all angles, edge cases like 360° wrap)
- **Total**: ~6-8 hours

### Similar Examples
- NOAA buoy plots show wave direction with arrows
- Marine weather apps (Windy, PredictWind) use vector displays
- Sailing/surfing apps show wind/wave direction visually

### Edge Cases to Handle
1. **360° wraparound**: When spread crosses 0°/360° boundary
   - e.g., avgDir = 350°, spread = 40° → range is 330° to 10°
2. **Very large spread** (>180°): How to display?
3. **No spread data**: Fall back to single arrow
4. **Mobile display**: Scale appropriately for small screens

### Future Enhancements
- **Animated arrows**: Subtle pulse/wave animation
- **Interactive**: Click to see detailed breakdown
- **Historical overlay**: Show how direction changed over 24h
- **Wind direction comparison**: Overlay wind vectors to compare

## Notes
- Use meteorological convention: direction = where waves are COMING FROM
- Arrows point where waves are TRAVELING TO (wave direction + 180°... no wait, we use different convention)
- Actually: re-verify the exact convention used in our codebase before implementing!
