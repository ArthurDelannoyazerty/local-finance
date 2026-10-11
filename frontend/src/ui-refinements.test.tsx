import { describe, expect, it, vi } from "vitest";
import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router-dom";
import Chart from "./components/Chart";
import Layout from "./components/Layout";
import { asArray, chartOption } from "./chart-options";

vi.mock("echarts-for-react/esm/core", () => ({ default: () => null }));

const option = {
  legend: {},
  xAxis: { type: "category", data: ["Jan", "Feb"] },
  yAxis: { type: "value" },
  dataZoom: [{ type: "inside" }],
  series: [{ name: "Wealth", type: "line", data: [10, 20], symbol: "none" }],
};

describe("zoom is opt-in for Portfolio", () => {
  it("hides tools, crosshair and instructions by default", () => {
    const html = renderToStaticMarkup(<Chart option={option} />);
    expect(html).not.toContain('class="chart-tools"');
    expect(html).not.toContain("chart-selecting");
    expect(html).not.toContain("Glisser pour zoomer");
    expect(html).toContain("Wealth");
    expect(html).toContain("Double-clic");
  });

  it("keeps Portfolio rectangle selection and reset available", () => {
    const html = renderToStaticMarkup(<Chart option={option} zoomable />);
    expect(html).toContain('class="chart-tools"');
    expect(html).toContain("chart-selecting");
    expect(html).toContain("Zoom par sélection");
    const prepared = chartOption(option, {}, [], true);
    expect(prepared.dataZoom).toHaveLength(2);
  });

  it("removes page-provided wheel/pinch zoom but preserves legends", () => {
    const prepared = chartOption(option, { Wealth: false }, [], false);
    expect(prepared.dataZoom).toEqual([]);
    expect(asArray(prepared.legend)[0].selected).toEqual({ Wealth: false });
    expect(prepared.animation).toBe(false);
  });

  it("disables treemap node zoom, roaming and breadcrumb navigation", () => {
    const prepared = chartOption(
      {
        series: [
          {
            type: "treemap",
            roam: true,
            nodeClick: "zoomToNode",
            breadcrumb: { show: true },
          },
        ],
      },
      {},
      [],
      false,
    );
    expect(asArray(prepared.series)[0]).toMatchObject({
      roam: false,
      nodeClick: false,
      breadcrumb: { show: false },
    });
  });
});

describe("global navigation", () => {
  it.each([
    "/",
    "/portfolio",
    "/fire",
    "/allocation",
    "/flux",
    "/transactions",
    "/donnees",
  ])("does not mount an importer on %s", (route) => {
    const html = renderToStaticMarkup(
      <MemoryRouter initialEntries={[route]}>
        <Layout
          range={{ start: "2026-01-01", end: "2026-10-11" }}
          onRange={() => undefined}
        >
          <div>Page content</div>
        </Layout>
      </MemoryRouter>,
    );
    expect(html).not.toContain("Importer");
    expect(html).not.toContain("broker-import-button");
    expect(html).toContain("Page content");
  });
});
