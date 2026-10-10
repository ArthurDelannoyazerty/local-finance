/** Small, testable chart interactions shared by every page. */
export type Selection = Record<string, boolean>;
export type Point = { x: number; y: number };
export type Viewport = { scale: number; x: number; y: number };
export const initialViewport: Viewport = { scale: 1, x: 0, y: 0 };

export function compactAxisLabel(value: number): string {
  if (!Number.isFinite(value)) return "";
  const magnitude = Math.abs(value);
  const divisor = magnitude >= 1e6 ? 1e6 : magnitude >= 1e3 ? 1e3 : 1;
  const suffix = divisor === 1e6 ? " M" : divisor === 1e3 ? " k" : "";
  return (
    new Intl.NumberFormat("fr-FR", { maximumSignificantDigits: 8 }).format(
      value / divisor,
    ) + suffix
  );
}

/** A second double-click on an isolated entry restores all entries. */
export function isolateLegend(
  names: string[],
  selected: Selection,
  name: string,
): Selection {
  const alreadySolo = names.every(
    (item) => (selected[item] !== false) === (item === name),
  );
  return Object.fromEntries(
    names.map((item) => [item, alreadySolo || item === name]),
  );
}

export function selectionRect(a: Point, b: Point) {
  return {
    left: Math.min(a.x, b.x),
    top: Math.min(a.y, b.y),
    width: Math.abs(a.x - b.x),
    height: Math.abs(a.y - b.y),
  };
}

/** Diagram zoom is a viewport crop, not an invented numeric axis. */
export function zoomViewport(
  view: Viewport,
  a: Point,
  b: Point,
  width: number,
  height: number,
): Viewport {
  const rect = selectionRect(a, b);
  if (rect.width < 8 || rect.height < 8 || width <= 0 || height <= 0) {
    return view;
  }
  const scale = Math.min(
    16,
    view.scale * Math.min(width / rect.width, height / rect.height),
  );
  const factor = scale / view.scale;
  return {
    scale,
    x: width / 2 - (rect.left + rect.width / 2 - view.x) * factor,
    y: height / 2 - (rect.top + rect.height / 2 - view.y) * factor,
  };
}
