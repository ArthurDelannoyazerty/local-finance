import { compactAxisLabel, type Selection } from "./chart-utils";

type Style = Record<string, unknown> & {
  color?: string;
  type?: string;
  opacity?: number;
};
export type SeriesConfig = Record<string, unknown> & {
  name?: string;
  type?: string;
  color?: string;
  symbol?: string;
  itemStyle?: Style;
  lineStyle?: Style;
  areaStyle?: Style;
  data?: unknown[];
};
export type AxisConfig = Record<string, unknown> & {
  type?: string;
  data?: unknown[];
  axisLabel?: Record<string, unknown>;
};
type LegendConfig = Record<string, unknown> & {
  show?: boolean;
  data?: (string | { name: string })[];
  selected?: Selection;
};
export type ChartConfig = Record<string, unknown> & {
  series?: SeriesConfig | SeriesConfig[];
  color?: string[];
  legend?: LegendConfig | LegendConfig[];
  xAxis?: AxisConfig | AxisConfig[];
  yAxis?: AxisConfig | AxisConfig[];
  tooltip?: Record<string, unknown>;
};
export type LegendEntry = {
  name: string;
  color: string;
  line: boolean;
  dash?: string;
};
export type AxisZoom = {
  id: string;
  startValue: number;
  endValue: number;
};
export const palette = [
  "#67e8b6",
  "#72a5ff",
  "#f5c66e",
  "#b99cff",
  "#5bd7e8",
  "#ff8585",
];
export const asArray = <T,>(value: T | T[] | undefined): T[] =>
  value === undefined ? [] : Array.isArray(value) ? value : [value];

export function legendEntries(config: ChartConfig): LegendEntry[] {
  const legends = asArray(config.legend);
  if (!legends.length || legends.every((legend) => legend.show === false)) {
    return [];
  }
  const colors = config.color?.length ? config.color : palette;
  const result: LegendEntry[] = [];
  asArray(config.series).forEach((series, index) => {
    const color =
      series.lineStyle?.color ??
      series.itemStyle?.color ??
      series.color ??
      colors[index % colors.length];
    if (series.type === "pie") {
      (series.data ?? []).forEach((item, dataIndex) => {
        if (!item || typeof item !== "object" || !("name" in item)) return;
        const slice = item as { name: string; itemStyle?: Style };
        result.push({
          name: slice.name,
          color: slice.itemStyle?.color ?? colors[dataIndex % colors.length],
          line: false,
        });
      });
    } else if (series.name) {
      // An invisible stacking baseline must not get a visible legend entry.
      const hidden =
        series.lineStyle?.opacity === 0 && !series.areaStyle?.opacity;
      if (hidden) return;
      const areaOnly =
        series.lineStyle?.opacity === 0 && Boolean(series.areaStyle?.opacity);
      result.push({
        name: series.name,
        color: areaOnly ? (series.areaStyle?.color ?? color) : color,
        line: series.type === "line" && !areaOnly,
        dash:
          series.lineStyle?.type === "dashed"
            ? "6 4"
            : series.lineStyle?.type === "dotted"
              ? "2 3"
              : undefined,
      });
    }
  });
  const explicit = legends
    .flatMap((legend) => legend.data ?? [])
    .map((item) => (typeof item === "string" ? item : item.name));
  const names = explicit.length ? explicit : result.map((entry) => entry.name);
  return [...new Set(names)].flatMap((name) => {
    const entry = result.find((item) => item.name === name);
    return entry ? [entry] : [];
  });
}

export function chartOption(
  config: ChartConfig,
  selected: Selection,
  zoom: AxisZoom[],
): ChartConfig {
  const entries = legendEntries(config);
  const colors = config.color?.length ? config.color : palette;
  const cartesian = Boolean(config.xAxis && config.yAxis);
  const axes = (dimension: "x" | "y") => asArray(config[`${dimension}Axis`]);
  const dataZoom = (["x", "y"] as const).flatMap((dimension) =>
    axes(dimension).map((_, index) => {
      const id = `local-${dimension}-${index}`;
      const range = zoom.find((item) => item.id === id);
      return {
        id,
        type: "inside",
        [`${dimension}AxisIndex`]: index,
        filterMode: "none",
        zoomOnMouseWheel: false,
        moveOnMouseMove: false,
        moveOnMouseWheel: false,
        ...(range
          ? { startValue: range.startValue, endValue: range.endValue }
          : { start: 0, end: 100 }),
      };
    }),
  );
  return {
    ...config,
    color: colors,
    animation: false,
    animationDuration: 0,
    animationDurationUpdate: 0,
    tooltip: { ...config.tooltip, confine: true },
    // Native filtering still works; accessible HTML buttons draw the swatches.
    legend: { show: false, data: entries.map((item) => item.name), selected },
    ...(cartesian ? { dataZoom } : {}),
    ...(config.yAxis
      ? {
          yAxis: axes("y").map((axis) =>
            axis.type === "value"
              ? {
                  ...axis,
                  axisLabel: {
                    ...axis.axisLabel,
                    formatter: compactAxisLabel,
                  },
                }
              : axis,
          ),
        }
      : {}),
    series: asArray(config.series).map((series, index) => {
      const color =
        series.lineStyle?.color ??
        series.itemStyle?.color ??
        series.color ??
        colors[index % colors.length];
      return {
        ...series,
        ...(series.type === "line"
          ? { itemStyle: { ...series.itemStyle, color } }
          : {}),
        ...(series.type === "sankey" ? { draggable: false } : {}),
        ...(series.type === "treemap" ? { roam: false } : {}),
      };
    }),
  };
}
