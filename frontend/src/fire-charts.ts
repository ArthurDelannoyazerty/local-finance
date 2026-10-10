import type { ChartConfig, SeriesConfig } from "./chart-options";
import type { ProjectionInput } from "./types";

export type Projection = {
  items: {
    month: number;
    year: number;
    age: number;
    net_nominal: number;
    net_real: number;
  }[];
  metrics: {
    final_wealth: number;
    monthly_rent_4_percent: number;
    fire_age: number | null;
    tipping_age: number | null;
    fire_target: number;
    lean_fire_target: number;
    fat_fire_target: number;
  };
};
export type MonteCarlo = {
  items: { year: number; age: number; p10: number; p50: number; p90: number }[];
  metrics: {
    success_probability: number;
    median_final: number;
    pessimistic_final: number;
    optimistic_final: number;
  };
};
export type ProjectionResult<T> = { result: T; parameters: ProjectionInput };
const money = (value: number) =>
  new Intl.NumberFormat("fr-FR", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(value);

function axes(input: ProjectionInput): ChartConfig {
  return {
    animation: false,
    legend: { top: 0, textStyle: { color: "#8c9aae" } },
    grid: { left: 62, right: 18, top: 20, bottom: 36 },
    xAxis: {
      type: "value",
      name: "Âge",
      min: input.current_age,
      max: input.current_age + input.years,
      axisLabel: { color: "#748195" },
      axisLine: { lineStyle: { color: "#273142" } },
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: { color: "#748195" },
      splitLine: { lineStyle: { color: "rgba(255,255,255,.055)" } },
    },
  };
}

export function projectionChart(
  response: ProjectionResult<Projection>,
  showLean: boolean,
  showFat: boolean,
  showCoast: boolean,
  milestones: number[],
): ChartConfig {
  const { result: data, parameters: active } = response;
  const items = data.items;
  const targets = (value: number) =>
    items.map((item) => [
      item.age,
      active.show_real
        ? value
        : value * (1 + active.inflation_rate) ** item.year,
    ]);
  const horizontal = (
    name: string,
    value: number,
    color: string,
  ): SeriesConfig => ({
    name,
    type: "line",
    symbol: "none",
    data: targets(value),
    lineStyle: { color, width: 1.5, type: "dashed" },
    emphasis: { disabled: true },
  });
  const markers = [
    active.stop_working_age !== null
      ? {
          name: "Arrêt",
          xAxis: active.stop_working_age,
          lineStyle: { color: "#ff8585" },
        }
      : null,
    data.metrics.tipping_age !== null
      ? {
          name: "Bascule",
          xAxis: data.metrics.tipping_age,
          lineStyle: { color: "#72a5ff" },
        }
      : null,
  ].filter(Boolean);
  const series: SeriesConfig[] = [
    {
      name: active.show_real ? "Patrimoine net réel" : "Patrimoine net nominal",
      type: "line",
      symbol: "none",
      smooth: 0.15,
      data: items.map((item) => [
        item.age,
        active.show_real ? item.net_real : item.net_nominal,
      ]),
      lineStyle: { color: "#67e8b6", width: 3 },
      areaStyle: { color: "#67e8b6", opacity: 0.08 },
      markLine: {
        silent: true,
        symbol: "none",
        label: { color: "#8c9aae", formatter: "{b}" },
        lineStyle: { color: "#415066", type: "dashed" },
        data: markers,
      },
    },
    horizontal("FIRE 4%", data.metrics.fire_target, "#72a5ff"),
  ];
  if (showLean)
    series.push(
      horizontal("Lean FIRE", data.metrics.lean_fire_target, "#f5c66e"),
    );
  if (showFat)
    series.push(
      horizontal("Fat FIRE", data.metrics.fat_fire_target, "#b99cff"),
    );
  milestones.forEach((value) =>
    series.push(horizontal(`${value / 1000}k`, value, "rgba(255,255,255,.4)")),
  );
  if (showCoast) {
    const realReturn =
      (1 + active.annual_return_rate) / (1 + active.inflation_rate) - 1;
    const rate = active.show_real ? realReturn : active.annual_return_rate;
    const target = active.show_real
      ? data.metrics.fire_target
      : data.metrics.fire_target *
        (1 + active.inflation_rate) **
          (active.retirement_age - active.current_age);
    series.push({
      name: "Coast FIRE",
      type: "line",
      symbol: "none",
      data: items.map((item) => [
        item.age,
        item.age <= active.retirement_age
          ? target / (1 + rate) ** (active.retirement_age - item.age)
          : null,
      ]),
      lineStyle: { color: "#f5c66e", width: 2, type: "dotted" },
    });
  }
  return {
    ...axes(active),
    tooltip: {
      trigger: "axis",
      valueFormatter: (value: number | number[]) =>
        money(Array.isArray(value) ? value[1] : value),
    },
    series,
  };
}

export function monteCarloChart(
  response: ProjectionResult<MonteCarlo>,
): ChartConfig {
  const { result, parameters } = response;
  return {
    ...axes(parameters),
    legend: { data: ["Zone 80%", "Médiane"] },
    tooltip: {
      trigger: "axis",
      formatter: (params: { dataIndex: number }[]) => {
        const row = result.items[params[0]?.dataIndex];
        if (!row) return "";
        return `Âge ${row.age.toFixed(1)}<br/>P10 : ${money(row.p10)}<br/>Médiane : ${money(row.p50)}<br/>P90 : ${money(row.p90)}`;
      },
    },
    series: [
      {
        name: "Zone 80%",
        type: "line",
        stack: "band",
        stackStrategy: "all",
        symbol: "none",
        data: result.items.map((item) => [item.age, item.p10]),
        lineStyle: { opacity: 0 },
        areaStyle: { opacity: 0 },
        emphasis: { disabled: true },
        tooltip: { show: false },
      },
      {
        name: "Zone 80%",
        type: "line",
        stack: "band",
        stackStrategy: "all",
        symbol: "none",
        data: result.items.map((item) => [item.age, item.p90 - item.p10]),
        lineStyle: { opacity: 0 },
        areaStyle: { color: "#72a5ff", opacity: 0.18 },
        emphasis: { disabled: true },
      },
      {
        name: "Médiane",
        type: "line",
        symbol: "none",
        data: result.items.map((item) => [item.age, item.p50]),
        lineStyle: { color: "#67e8b6", width: 3 },
      },
    ],
  };
}
