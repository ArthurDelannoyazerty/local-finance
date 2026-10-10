import { describe, expect, it } from "vitest";
import {
  compactAxisLabel,
  initialViewport,
  isolateLegend,
  selectionRect,
  zoomViewport,
} from "./chart-utils";
import { asArray, chartOption, legendEntries } from "./chart-options";
import { availableMonths, monthLabel, toggleMonths } from "./months";
import {
  initialProjection,
  normalizeProjection,
  projectionError,
  updateProjection,
  waitForQuiet,
} from "./fire-state";
import {
  monteCarloChart,
  projectionChart,
  type Projection,
} from "./fire-charts";

describe("chart interactions", () => {
  it("keeps adjacent monetary ticks distinct", () => {
    const labels = [0, 500, 1000, 1500, 2000, 2500, 3000].map(compactAxisLabel);
    expect(new Set(labels).size).toBe(7);
    expect(compactAxisLabel(2500)).toBe("2,5 k");
    expect(compactAxisLabel(0.0025)).toBe("0,0025");
    expect(compactAxisLabel(-2500)).toBe("-2,5 k");
    expect(compactAxisLabel(NaN)).toBe("");
  });
  it("isolates and restores series from the pre-click state", () => {
    const names = ["A", "B", "C"];
    const solo = isolateLegend(names, {}, "B");
    expect(solo).toEqual({ A: false, B: true, C: false });
    expect(isolateLegend(names, solo, "B")).toEqual({
      A: true,
      B: true,
      C: true,
    });
    expect(isolateLegend(names, solo, "A")).toEqual({
      A: true,
      B: false,
      C: false,
    });
  });
  it("normalizes rectangle direction and caps diagram zoom", () => {
    expect(selectionRect({ x: 100, y: 200 }, { x: 20, y: 30 })).toEqual({
      left: 20,
      top: 30,
      width: 80,
      height: 170,
    });
    expect(
      zoomViewport(initialViewport, { x: 0, y: 0 }, { x: 2, y: 2 }, 600, 400),
    ).toEqual(initialViewport);
    const view = zoomViewport(
      initialViewport,
      { x: 150, y: 100 },
      { x: 450, y: 300 },
      600,
      400,
    );
    expect(view).toEqual({ scale: 2, x: -300, y: -200 });
    const again = zoomViewport(
      view,
      { x: 10, y: 10 },
      { x: 20, y: 20 },
      600,
      400,
    );
    expect(again.scale).toBe(16);
  });
  it("uses only a matching dashed line for a line legend", () => {
    const config = {
      legend: {},
      xAxis: { type: "category" },
      yAxis: { type: "value" },
      series: [
        {
          name: "Average",
          type: "line",
          symbol: "none",
          lineStyle: { color: "#f5c66e", type: "dashed" },
        },
      ],
    };
    expect(legendEntries(config)).toEqual([
      { name: "Average", color: "#f5c66e", line: true, dash: "6 4" },
    ]);
    const option = chartOption(config, { Average: false }, [
      { id: "local-x-0", startValue: 2, endValue: 4 },
    ]);
    expect(option.animation).toBe(false);
    expect(asArray(option.series)[0].itemStyle?.color).toBe("#f5c66e");
    expect(asArray(option.legend)[0].selected).toEqual({ Average: false });
    expect(
      (option.dataZoom as { id: string; startValue: number }[])[0].startValue,
    ).toBe(2);
    expect(
      (asArray(option.yAxis)[0].axisLabel?.formatter as (v: number) => string)(
        2500,
      ),
    ).toBe("2,5 k");
  });
  it("builds pie legends from slices without inventing Cartesian axes", () => {
    const config = {
      legend: {},
      color: ["red", "blue"],
      series: [
        {
          type: "pie",
          data: [
            { name: "Food", value: 1 },
            { name: "Other", value: 2 },
          ],
        },
      ],
    };
    expect(legendEntries(config).map((entry) => entry.name)).toEqual([
      "Food",
      "Other",
    ]);
    expect(chartOption(config, {}, []).dataZoom).toBeUndefined();
  });
});

describe("month selection", () => {
  it("includes every calendar month across years", () => {
    expect(availableMonths("2024-11-28", "2025-02-01")).toEqual([
      "2024-11",
      "2024-12",
      "2025-01",
      "2025-02",
    ]);
    expect(availableMonths("bad", "2025-01-01")).toEqual([]);
    expect(availableMonths("2025-02-01", "2025-01-01")).toEqual([]);
    expect(monthLabel("2025-02")).toContain("2025");
  });
  it("adds and removes without duplicates, including an empty selection", () => {
    expect(toggleMonths(["2025-02"], ["2025-01", "2025-02"], true)).toEqual([
      "2025-01",
      "2025-02",
    ]);
    expect(toggleMonths(["2025-02"], ["2025-02"], false)).toEqual([]);
  });
});

describe("automatic FIRE projections", () => {
  it("rounds defaults, scenarios and edits to cents", () => {
    const normalized = normalizeProjection({
      ...initialProjection,
      monthly_savings: 1816.76285714286,
      monthly_expenses: 1234.567,
    });
    expect(normalized.monthly_savings).toBe(1816.76);
    expect(normalized.monthly_expenses).toBe(1234.57);
    expect(
      updateProjection(normalized, "monthly_savings", 8.888).monthly_savings,
    ).toBe(8.89);
    expect(projectionError(normalized)).toBeNull();
    expect(
      projectionError({ ...normalized, monthly_expenses: -1 }),
    ).not.toBeNull();
    expect(
      projectionError({ ...normalized, start_capital: Infinity }),
    ).not.toBeNull();
  });
  it("cancels superseded Monte Carlo waits before sending requests", async () => {
    const controller = new AbortController();
    const waiting = waitForQuiet(controller.signal, 1000);
    controller.abort();
    await expect(waiting).rejects.toMatchObject({ name: "AbortError" });
    await expect(waitForQuiet(controller.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
    await expect(
      waitForQuiet(new AbortController().signal, 0),
    ).resolves.toBeUndefined();
  });
  it("uses a numeric age axis and couples results to their parameters", () => {
    const data: Projection = {
      items: [
        {
          month: 1,
          year: 1 / 12,
          age: 30 + 1 / 12,
          net_real: 10,
          net_nominal: 11,
        },
      ],
      metrics: {
        final_wealth: 10,
        monthly_rent_4_percent: 1,
        fire_age: null,
        tipping_age: 30.5,
        fire_target: 100,
        lean_fire_target: 80,
        fat_fire_target: 150,
      },
    };
    const option = projectionChart(
      {
        result: data,
        parameters: { ...initialProjection, stop_working_age: 35 },
      },
      true,
      true,
      true,
      [100000],
    );
    expect(asArray(option.xAxis)[0].type).toBe("value");
    expect(asArray(option.series)[0].data).toEqual([[30 + 1 / 12, 10]]);
    const markers = asArray(option.series)[0].markLine as {
      data: { xAxis: number }[];
    };
    expect(markers.data.map((item) => item.xAxis)).toEqual([35, 30.5]);
  });
  it("excludes the invisible Monte Carlo baseline from its legend", () => {
    const option = monteCarloChart({
      parameters: initialProjection,
      result: {
        items: [{ year: 1, age: 31, p10: -10, p50: 10, p90: 30 }],
        metrics: {
          success_probability: 50,
          median_final: 10,
          pessimistic_final: -10,
          optimistic_final: 30,
        },
      },
    });
    const entries = legendEntries(option);
    expect(entries.map((entry) => entry.name)).toEqual(["Zone 80%", "Médiane"]);
    expect(entries[0].line).toBe(false);
    expect(entries[0].color).toBe("#72a5ff");
    expect(asArray(option.series)[1].stackStrategy).toBe("all");
  });
});
