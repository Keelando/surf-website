# Chart layout standardization

**Status:** helper landed 2026-08-29, forecasts page migrated. Everything else
is queued, to be done a page at a time.

## The two defects

Both come from ECharts defaults that no chart on this site overrode, and both
were found on `forecasts.html`:

1. **Axis names are clipped at the canvas edge.** An `end`-located axis name is
   centred on the axis line. The grid's side gutters are 4–8 px
   (`getChartSideGutters`), so roughly half of "Wave height (m)" hangs outside
   the canvas and is simply not drawn. The fix is one property:
   `nameTextStyle: { align: "left" }` on a left axis, `align: "right"` on a
   right-positioned one.

2. **A wrapped legend writes over the x-axis labels.** ECharts lays a bottom
   legend out left to right, wraps when it runs out of chart width, and anchors
   the block by its *bottom* edge — so a second row grows upward into the
   labels. The percentage bands in `getResponsiveGridConfig` budget for one row.
   Three legend entries is two rows on a phone.

## The helper

`getResponsiveGridConfig(isComparisonChart, legendData, size)` in
`site/assets/js/chart-utils-v4.js` now takes two optional arguments. Given the
legend labels and the rendered chart size it measures the legend the way
ECharts lays it out (`getLegendRowCount`), then widens the bottom band to
`legendBottom + rows × 24 px + 18 px` so the legend sits below the labels.
It only ever widens the existing percentage band, and callers that pass
nothing keep the old behaviour — that is what makes a piecemeal migration safe.

## Migrating one chart

Two mechanical edits per chart, then look at it:

```js
// before
grid: getResponsiveGridConfig(false),

// after — chartInstance is whatever ECharts instance the function was handed
grid: getResponsiveGridConfig(false, legendData, {
  width: chartInstance.getWidth(),
  height: chartInstance.getHeight(),
}),
```

```js
// and on each y-axis
nameTextStyle: { color: textColor, align: "left" },   // left axis
nameTextStyle: { color: textColor, align: "right" },  // position: "right"
```

Verify at 360/390 px and 1280 px in **both** Chromium and Firefox — the legend
wrap point depends on font metrics, so one engine is not evidence about the
other.

## Queue

| Chart | File | Notes |
|---|---|---|
| ✅ Wave + wind forecast, both verification charts | `wave-forecast.js` | Done 2026-08-29 |
| Buoy wave chart | `wave-chart-v4.js` | Three call sites (standard, spectral, dual-axis); the dual-axis one needs `align: "right"` on "Period (s)" |
| Buoy wind chart | `wind-chart-v4.js` | Instance is `windChart` |
| Buoy temperature chart | `temperature-chart-v4.js` | Legend is a literal, pass the same array to both |
| Wind stations trend | `wind-stations.js` | Instance is `windChart` |
| Storm surge | `storm_surge_chart-v4.js` | Axis-name alignment only; legend layout is its own |
| Comparison chart | `comparison-chart-v4.js` | **Do not pass legend data.** It pins its legend at a fixed `bottom: "3%"` with a 16/28 % band, which the helper's arithmetic assumes nothing about. Axis-name alignment is still worth doing |
| Lightstation charts | `lightstation-charts.js` | Legend is at the top and the axis name is rotated (`nameLocation: "middle"`), so neither defect applies. Convert only if the layout is unified |
| Tide chart | `tides-modules/chart-renderer.js` | Same: rotated axis name with its own `nameGap` |

A prototype of the `index.html` and `winds.html` rows was built and checked in
both engines before being shelved on 2026-08-29 — the edits above are exactly
what it did.
