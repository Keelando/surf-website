// Trimmed ECharts bundle for site/assets/vendor/echarts.min.js.
// Rebuild with scripts/build/build_echarts.sh after changing this list.
//
// Only what site/assets/js actually uses: line + scatter series; grid axes
// (time/value/category come with the grid); title, legend (plain + scroll),
// tooltip, axisPointer, dataZoom (inside + slider), markLine, markPoint,
// markArea, and aria (chart-utils-v4.js turns it on for every chart); canvas
// renderer. A series or component missing from here does not throw in the
// minified build: the chart silently draws without it. Add the import here
// before using a new one.
import * as echarts from "echarts/core";
import { LineChart, ScatterChart } from "echarts/charts";
import {
  AriaComponent,
  AxisPointerComponent,
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  MarkAreaComponent,
  MarkLineComponent,
  MarkPointComponent,
  TitleComponent,
  TooltipComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  LineChart,
  ScatterChart,
  AriaComponent,
  AxisPointerComponent,
  DataZoomComponent,
  GridComponent,
  LegendComponent,
  MarkAreaComponent,
  MarkLineComponent,
  MarkPointComponent,
  TitleComponent,
  TooltipComponent,
  CanvasRenderer,
]);

// A plain object, not the module namespace: esbuild's namespace has getter-only
// properties, so chart-utils-v4.js's `echarts.init = ...` wrapper (aria text,
// lazy drawing) would be silently ignored. The full build's global was a
// plain, writable object too.
globalThis.echarts = { ...echarts };
