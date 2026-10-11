export type Sankey = {
  nodes: { name: string }[];
  links: { source: string; target: string; value: number }[];
};

type FlowNode = {
  name: string;
  displayName: string;
  depth?: number;
  itemStyle?: { color: string };
};
type FlowLink = Sankey["links"][number] & {
  sourceLabel: string;
  targetLabel: string;
};

export function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

/** ECharts Sankey requires a DAG. Separate sending and receiving roles so
 * A -> B and B -> A remain visible, without netting or dropping either flow.
 */
export function flowGraph(
  data: Sankey,
  colors: string[],
  splitEndpoints = false,
): { nodes: FlowNode[]; links: FlowLink[] } {
  if (!splitEndpoints) {
    return {
      nodes: data.nodes.map((node) => ({
        ...node,
        displayName: node.name,
      })),
      links: data.links.map((link) => ({
        ...link,
        sourceLabel: link.source,
        targetLabel: link.target,
      })),
    };
  }
  const palette = colors.length ? colors : ["#72a5ff"];
  const names = [...new Set(data.links.flatMap((l) => [l.source, l.target]))];
  const colorByName = new Map(
    names.sort().map((name, index) => [name, palette[index % palette.length]]),
  );
  const nodes = new Map<string, FlowNode>();
  const endpoint = (name: string, role: "from" | "to") => {
    // Tuple encoding prevents collisions with literal account names.
    const id = JSON.stringify([role, name]);
    nodes.set(id, {
      name: id,
      displayName: `${name} (${role === "from" ? "sorties" : "entrées"})`,
      depth: role === "from" ? 0 : 1,
      itemStyle: { color: colorByName.get(name)! },
    });
    return id;
  };
  const links = data.links.map((link) => ({
    ...link,
    source: endpoint(link.source, "from"),
    target: endpoint(link.target, "to"),
    sourceLabel: link.source,
    targetLabel: link.target,
  }));
  return { nodes: [...nodes.values()], links };
}

const currency = new Intl.NumberFormat("fr-FR", {
  style: "currency",
  currency: "EUR",
  maximumFractionDigits: 2,
});

export function flowChart(
  data: Sankey,
  colors: string[],
  splitEndpoints = false,
) {
  const graph = flowGraph(data, colors, splitEndpoints);
  return {
    tooltip: {
      trigger: "item",
      triggerOn: "mousemove",
      formatter: (params: {
        dataType: string;
        data: FlowNode | FlowLink;
        value: number;
      }) => {
        const item = params.data;
        const label =
          "sourceLabel" in item
            ? `${escapeHtml(item.sourceLabel)} → ${escapeHtml(item.targetLabel)}`
            : escapeHtml(item.displayName);
        return `${label}<br/>${currency.format(params.value)}`;
      },
    },
    color: colors,
    series: [
      {
        type: "sankey",
        data: graph.nodes,
        links: graph.links,
        emphasis: { focus: "adjacency" },
        nodeAlign: "justify",
        nodeGap: 14,
        nodeWidth: 15,
        layoutIterations: 40,
        lineStyle: { color: "gradient", curveness: 0.52, opacity: 0.35 },
        itemStyle: {
          borderColor: "rgba(255,255,255,.22)",
          borderWidth: 1,
          borderRadius: 2,
        },
        label: {
          color: "#aeb9c8",
          fontSize: 11,
          formatter: (params: { data: FlowNode }) => params.data.displayName,
        },
      },
    ],
  };
}
