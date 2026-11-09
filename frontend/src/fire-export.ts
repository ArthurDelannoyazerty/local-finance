import { asArray, type ChartConfig } from "./chart-options";
import type { Selection } from "./chart-utils";
import { delimitedText } from "./clipboard";
import type { MonteCarlo } from "./fire-charts";

export function projectionCsv(
  option: ChartConfig,
  selected: Selection = {},
): string {
  const allSeries = asArray(option.series);
  const visible = allSeries.filter(
    (series) => series.name && selected[series.name] !== false,
  );
  const value = (point: unknown, coordinate: number): number | null =>
    Array.isArray(point) && typeof point[coordinate] === "number"
      ? point[coordinate]
      : null;
  return delimitedText(
    ["Âge", ...visible.map((series) => series.name!)],
    (allSeries[0]?.data ?? []).map((point, index) => [
      value(point, 0),
      ...visible.map((series) => value(series.data?.[index], 1)),
    ]),
  );
}

export function monteCarloCsv(
  items: MonteCarlo["items"],
  selected: Selection = {},
): string {
  const band = selected["Zone 80%"] !== false;
  const median = selected["Médiane"] !== false;
  return delimitedText(
    ["Années", "Âge", ...(band ? ["P10", "P90"] : []), ...(median ? ["Médiane"] : [])],
    items.map((item) => [
      item.year,
      item.age,
      ...(band ? [item.p10, item.p90] : []),
      ...(median ? [item.p50] : []),
    ]),
  );
}
