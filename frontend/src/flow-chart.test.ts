import { describe, expect, it } from "vitest";
import * as echarts from "echarts";
import { flowChart, flowGraph, type Sankey } from "./flow-chart";
import {
  accountColor,
  allocationNodes,
  type AllocationItem,
} from "./allocation-data";
import { contiguousDonut } from "./chart-appearance";

const colors = ["#72a5ff", "#5bd7e8", "#b99cff"];
const reciprocal: Sankey = {
  nodes: [{ name: "A" }, { name: "B" }],
  links: [
    { source: "A", target: "B", value: 100 },
    { source: "B", target: "A", value: 40 },
  ],
};

function svg(option: object): string {
  const chart = echarts.init(null, undefined, {
    renderer: "svg",
    ssr: true,
    width: 600,
    height: 400,
  });
  try {
    chart.setOption({ ...option, animation: false });
    return chart.renderToSVGString();
  } finally {
    chart.dispose();
  }
}

describe("Sankey transfer cycles", () => {
  it("reproduces the original renderer failure, then renders both directions", () => {
    expect(() => svg(flowChart(reciprocal, colors))).toThrow(/cycle|DAG/i);
    const rendered = svg(flowChart(reciprocal, colors, true));
    expect(rendered).toContain("<path");
    expect(rendered).toContain("A (sorties)");
    expect(rendered).toContain("B (sorties)");
    expect(rendered).not.toContain("NaN");
  });

  it("preserves gross amounts instead of subtracting opposite transfers", () => {
    const graph = flowGraph(reciprocal, colors, true);
    expect(graph.nodes).toHaveLength(4);
    expect(graph.links.map((link) => link.value)).toEqual([100, 40]);
    expect(graph.links.reduce((sum, link) => sum + link.value, 0)).toBe(140);
    const depths = new Map(graph.nodes.map((node) => [node.name, node.depth]));
    for (const link of graph.links) {
      expect(depths.get(link.source)).toBe(0);
      expect(depths.get(link.target)).toBe(1);
    }
    expect(reciprocal.links[0].source).toBe("A");
  });

  it("renders longer cycles and buy/sell round trips", () => {
    const data: Sankey = {
      nodes: [{ name: "A" }, { name: "B" }, { name: "ETF.PA" }],
      links: [
        { source: "A", target: "B", value: 100 },
        { source: "B", target: "ETF.PA", value: 80 },
        { source: "ETF.PA", target: "A", value: 90 },
      ],
    };
    expect(svg(flowChart(data, colors, true))).not.toContain("NaN");
    expect(flowGraph(data, colors, true).links).toHaveLength(3);
  });

  it("uses collision-free internal IDs and escapes tooltip labels", () => {
    const data: Sankey = {
      nodes: [],
      links: [
        { source: '["from","A"]', target: "A", value: 5 },
        { source: "A", target: "<img src=x>", value: 6 },
      ],
    };
    const option = flowChart(data, colors, true);
    const nodes = option.series[0].data;
    expect(new Set(nodes.map((node) => node.name)).size).toBe(4);
    const tooltip = option.tooltip.formatter({
      dataType: "edge",
      data: option.series[0].links[1],
      value: 6,
    });
    expect(tooltip).toContain("&lt;img src=x&gt;");
    expect(tooltip).not.toContain("<img");
    expect(tooltip).not.toContain('["from"');
  });

  it("retains the acyclic budget graph and supports empty selections", () => {
    const budget: Sankey = {
      nodes: [{ name: "Income" }, { name: "Expenses" }],
      links: [{ source: "Income", target: "Expenses", value: 15 }],
    };
    const graph = flowGraph(budget, colors);
    expect(graph.nodes.map((node) => node.name)).toEqual([
      "Income",
      "Expenses",
    ]);
    expect(graph.links[0].value).toBe(15);
    expect(flowGraph({ nodes: [], links: [] }, [], true)).toEqual({
      nodes: [],
      links: [],
    });
  });
});

const position = (account: string): AllocationItem => ({
  account,
  type: "INVESTMENT",
  ticker: "ETF.PA",
  name: "Example fund",
  quantity: 10,
  unit_price: 10,
  value: 100,
  currency: "EUR",
  start_value: 90,
  net_contribution: 0,
  performance_percent: 10,
});

describe("chart appearance", () => {
  it("renders small donut slices without rounding or padding gaps", () => {
    expect(contiguousDonut.padAngle).toBe(0);
    expect(contiguousDonut.itemStyle).toEqual({
      borderRadius: 0,
      borderWidth: 0,
    });
    const rendered = svg({
      series: [
        {
          type: "pie",
          radius: ["52%", "76%"],
          label: { show: false },
          ...contiguousDonut,
          data: [
            { name: "Large", value: 1000 },
            { name: "Small", value: 0.01 },
          ],
        },
      ],
    });
    expect((rendered.match(/<path\b/g) ?? []).length).toBeGreaterThanOrEqual(2);
    expect(rendered).not.toContain("NaN");
  });

  it("gives account-only tiles explicit, stable, rendered colors", () => {
    const data = allocationNodes([position("A"), position("B")], "accounts");
    expect(data[0].itemStyle.color).toBe(accountColor("A"));
    expect(data[0].itemStyle.color).not.toBe(data[1].itemStyle.color);
    expect(data[0]).not.toHaveProperty("children");
    const reordered = allocationNodes([position("B"), position("A")], "assets");
    expect(reordered[1].itemStyle.color).toBe(data[0].itemStyle.color);
    const rendered = svg({ series: [{ type: "treemap", data }] });
    expect(rendered).toContain(accountColor("A"));
    expect(rendered).toContain(accountColor("B"));
  });

  it("keeps performance colors on assets instead of account colors", () => {
    const nodes = allocationNodes(
      [position("A"), { ...position("B"), performance_percent: -10 }],
      "performance",
    );
    expect(nodes[0].children?.[0].itemStyle?.color).toContain("rgba(70,");
    expect(nodes[1].children?.[0].itemStyle?.color).not.toEqual(
      nodes[0].children?.[0].itemStyle?.color,
    );
  });
});
