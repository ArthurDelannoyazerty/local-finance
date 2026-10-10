import { useEffect, useMemo, useRef, useState } from "react";
import type { PointerEvent } from "react";
import {
  BarChart,
  LineChart,
  PieChart,
  SankeyChart,
  TreemapChart,
} from "echarts/charts";
import {
  DataZoomComponent,
  GraphicComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  TooltipComponent,
} from "echarts/components";
import * as echarts from "echarts/core";
import { LabelLayout } from "echarts/features";
import { CanvasRenderer } from "echarts/renderers";
import ReactEChartsCore from "echarts-for-react/esm/core";
import {
  asArray,
  chartOption,
  legendEntries,
  type AxisZoom,
  type ChartConfig,
} from "../chart-options";
import {
  initialViewport,
  isolateLegend,
  selectionRect,
  zoomViewport,
  type Point,
  type Selection,
} from "../chart-utils";
import "./Chart.css";

echarts.use([
  BarChart,
  LineChart,
  PieChart,
  SankeyChart,
  TreemapChart,
  DataZoomComponent,
  GraphicComponent,
  GridComponent,
  LegendComponent,
  MarkLineComponent,
  TooltipComponent,
  LabelLayout,
  CanvasRenderer,
]);

type Instance = ReturnType<typeof echarts.init>;
type Drag = { id: number; start: Point; end: Point };

export default function Chart({
  option,
  height = 380,
}: {
  option: object;
  height?: number;
}) {
  const config = option as ChartConfig;
  const instance = useRef<Instance | null>(null);
  const [selected, setSelected] = useState<Selection>({});
  const beforeClick = useRef<Selection>({});
  const [zoom, setZoom] = useState<AxisZoom[]>([]);
  const [view, setView] = useState(initialViewport);
  const [selecting, setSelecting] = useState(true);
  const [drag, setDrag] = useState<Drag | null>(null);
  const dragRef = useRef<Drag | null>(null);
  const moved = useRef(false);
  const entries = legendEntries(config);
  const names = entries.map((entry) => entry.name);
  const cartesian = Boolean(config.xAxis && config.yAxis);
  // Keep selection across data updates, but discard a zoom into another domain.
  const domain = JSON.stringify([
    asArray(config.xAxis).map((axis) => [axis.data, axis.min, axis.max]),
    asArray(config.yAxis).map((axis) => [axis.data, axis.min, axis.max]),
    asArray(config.series).map((series) => [series.name, series.type]),
  ]);
  useEffect(() => {
    setZoom([]);
    setView(initialViewport);
    setDrag(null);
    dragRef.current = null;
  }, [domain]);
  const prepared = useMemo(
    () => chartOption(config, selected, zoom),
    [config, selected, zoom],
  );
  const reset = () => {
    setZoom([]);
    setView(initialViewport);
    setDrag(null);
    dragRef.current = null;
  };
  const point = (event: PointerEvent<HTMLDivElement>): Point => {
    const rect = event.currentTarget.getBoundingClientRect();
    return {
      x: Math.max(0, Math.min(rect.width, event.clientX - rect.left)),
      y: Math.max(0, Math.min(rect.height, event.clientY - rect.top)),
    };
  };
  const startDrag = (event: PointerEvent<HTMLDivElement>) => {
    moved.current = false;
    if (!selecting || event.button !== 0 || !event.isPrimary) return;
    const start = point(event);
    if (
      cartesian &&
      !instance.current?.containPixel({ gridIndex: 0 }, [start.x, start.y])
    ) {
      return;
    }
    event.currentTarget.focus({ preventScroll: true });
    dragRef.current = { id: event.pointerId, start, end: start };
    setDrag(dragRef.current);
  };
  const finishDrag = (event: PointerEvent<HTMLDivElement>) => {
    const current = dragRef.current;
    if (!current || current.id !== event.pointerId) return;
    const end = point(event);
    dragRef.current = null;
    setDrag(null);
    if (event.currentTarget.hasPointerCapture(event.pointerId)) {
      event.currentTarget.releasePointerCapture(event.pointerId);
    }
    const rect = selectionRect(current.start, end);
    if (rect.width < 8 || rect.height < 8) return;
    moved.current = true;
    if (cartesian && instance.current) {
      const chart = instance.current;
      const next = (["x", "y"] as const).flatMap((dimension) =>
        asArray(config[`${dimension}Axis`]).flatMap((_, index) => {
          const finder = { [`${dimension}AxisIndex`]: index };
          const first = chart.convertFromPixel(finder, current.start[dimension]);
          const last = chart.convertFromPixel(finder, end[dimension]);
          if (typeof first !== "number" || typeof last !== "number" || !Number.isFinite(first) || !Number.isFinite(last)) return [];
          return [{
            id: `local-${dimension}-${index}`,
            startValue: Math.min(first, last),
            endValue: Math.max(first, last),
          }];
        }),
      );
      setZoom(next);
    } else {
      const bounds = event.currentTarget.getBoundingClientRect();
      setView((previous) =>
        zoomViewport(previous, current.start, end, bounds.width, bounds.height),
      );
    }
  };
  const hasZoom = zoom.length > 0 || view.scale !== 1;
  return (
    <div className="interactive-chart">
      {entries.length > 0 && (
        <div className="chart-legend" aria-label="Légende du graphique">
          {entries.map((entry) => (
            <button
              type="button"
              key={entry.name}
              className="chart-legend-item"
              aria-pressed={selected[entry.name] !== false}
              title="Clic : masquer / afficher. Double-clic : isoler / tout afficher."
              onClick={(event) => {
                if (event.detail > 1) return;
                beforeClick.current = selected;
                setSelected((value) => ({
                  ...value,
                  [entry.name]: value[entry.name] === false,
                }));
              }}
              onDoubleClick={() =>
                setSelected(isolateLegend(names, beforeClick.current, entry.name))
              }
              onKeyDown={(event) => {
                if (event.shiftKey && event.key === "Enter") {
                  event.preventDefault();
                  setSelected(isolateLegend(names, selected, entry.name));
                }
              }}
            >
              <svg width="26" height="12" viewBox="0 0 26 12" aria-hidden="true">
                {entry.line ? (
                  <line
                    x1="1"
                    x2="25"
                    y1="6"
                    y2="6"
                    stroke={entry.color}
                    strokeWidth="2"
                    strokeDasharray={entry.dash}
                  />
                ) : (
                  <rect x="1" y="1" width="24" height="10" rx="2" fill={entry.color} />
                )}
              </svg>
              {entry.name}
            </button>
          ))}
        </div>
      )}
      <div className="chart-tools">
        <button
          type="button"
          className="button button-ghost"
          aria-pressed={selecting}
          onClick={() => setSelecting((value) => !value)}
        >
          Zoom par sélection
        </button>
        <button
          type="button"
          className="button button-ghost"
          disabled={!hasZoom}
          onClick={reset}
        >
          Réinitialiser le zoom
        </button>
        <span>Glissez pour cadrer une zone · Échap pour réinitialiser</span>
      </div>
      <div
        className={`chart-viewport${selecting ? " chart-selecting" : ""}`}
        style={{ height }}
        tabIndex={0}
        aria-label="Graphique interactif. Glisser pour zoomer, Échap pour réinitialiser."
        onPointerDown={startDrag}
        onPointerMove={(event) => {
          const current = dragRef.current;
          if (current && current.id === event.pointerId) {
            const end = point(event);
            // Capture only an actual drag; ordinary clicks must still reach
            // the canvas (notably treemap drill-down and breadcrumbs).
            if (Math.max(Math.abs(end.x - current.start.x), Math.abs(end.y - current.start.y)) >= 8) {
              event.currentTarget.setPointerCapture(event.pointerId);
              moved.current = true;
            }
            dragRef.current = { ...current, end };
            setDrag(dragRef.current);
          }
        }}
        onPointerUp={finishDrag}
        onPointerCancel={() => {
          dragRef.current = null;
          setDrag(null);
        }}
        onLostPointerCapture={() => {
          dragRef.current = null;
          setDrag(null);
        }}
        onClickCapture={(event) => {
          if (moved.current) {
            event.preventDefault();
            event.stopPropagation();
            moved.current = false;
          }
        }}
        onKeyDown={(event) => {
          if (event.key === "Escape") reset();
        }}
      >
        <div
          className="chart-surface"
          style={{
            transform: `translate(${view.x}px, ${view.y}px) scale(${view.scale})`,
          }}
        >
          <ReactEChartsCore
            echarts={echarts}
            option={prepared}
            notMerge
            lazyUpdate={false}
            onChartReady={(chart: Instance) => { instance.current = chart; }}
            style={{ height, width: "100%" }}
            opts={{ renderer: "canvas" }}
          />
        </div>
        {drag && (
          <div className="chart-selection" style={selectionRect(drag.start, drag.end)} />
        )}
      </div>
    </div>
  );
}
