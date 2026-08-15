export type SankeyData = {
  nodes: { name: string }[];
  links: { source: string; target: string; value: number }[];
};

export type SankeyVariant = "cash-flow" | "generic";

export type AllocationMode = "accounts" | "assets" | "performance";

export type AllocationItem = {
  account: string;
  type: "CASH" | "INVESTMENT";
  ticker: string;
  name: string;
  value: number;
  performance_absolute: number | null;
  performance_percent: number | null;
};

export type AllocationTreeNode = {
  id: string;
  name: string;
  value: number;
  displayValue: number;
  kind: "account" | "asset";
  ticker?: string;
  performanceAbsolute?: number | null;
  performancePercent?: number | null;
  children?: AllocationTreeNode[];
  itemStyle: {
    color: string;
    borderColor?: string;
  };
  upperLabel?: {
    backgroundColor: string;
    color: string;
  };
};

export const CATEGORY_PALETTE = [
  "#62d9ae",
  "#72a5f5",
  "#f0bd62",
  "#ee8582",
  "#ac91ee",
  "#58c4d8",
  "#9bc96e",
  "#d783a5",
  "#5eb4a9",
  "#8297dd",
  "#e39a5e",
  "#6aafe1",
  "#dc7b91",
  "#adb966",
  "#9985c8",
  "#6bc9bd",
];

const ACCOUNT_PALETTE = [
  "#347e78",
  "#426fa5",
  "#8d7043",
  "#735b91",
  "#397d91",
  "#8a536b",
];

const hashIndex = (key: string, size: number) => {
  let hash = 0;
  for (const character of key) hash = (hash * 31 + character.charCodeAt(0)) | 0;
  return Math.abs(hash) % size;
};

const fallbackColor = (key: string) =>
  CATEGORY_PALETTE[hashIndex(key, CATEGORY_PALETTE.length)];

export const cashFlowDepth = (name: string) => {
  if (name.startsWith("Revenu · ")) return 0;
  if (name === "Total revenus" || name === "Déficit") return 1;
  if (name === "Total dépenses" || name === "Épargne") return 2;
  if (name.startsWith("Dépense · ")) return 3;
  return undefined;
};

const cashFlowKey = (link: SankeyData["links"][number]) => {
  if (link.source.startsWith("Revenu · ")) return link.source;
  if (link.target.startsWith("Dépense · ")) return link.target;
  if (link.target === "Épargne") return link.target;
  if (link.source === "Déficit") return link.source;
  return "Flux consolidé";
};

export function styledSankey(data: SankeyData, variant: SankeyVariant) {
  const linkKey = (link: SankeyData["links"][number]) =>
    variant === "cash-flow" ? cashFlowKey(link) : link.target;
  const categoryKeys = Array.from(new Set(data.links.map(linkKey))).filter(
    (key) => key !== "Flux consolidé",
  );
  categoryKeys.sort((left, right) => left.localeCompare(right, "fr"));
  const colors = new Map(
    categoryKeys.map((key, index) => [
      key,
      CATEGORY_PALETTE[index % CATEGORY_PALETTE.length],
    ]),
  );
  const colorFor = (key: string) =>
    key === "Flux consolidé"
      ? "#8a96a8"
      : (colors.get(key) ?? fallbackColor(key));

  const nodes = data.nodes.map((node) => {
    const depth =
      variant === "cash-flow" ? cashFlowDepth(node.name) : undefined;
    const aggregateColor: Record<string, string> = {
      "Total revenus": "#d6a958",
      "Total dépenses": "#5da9d0",
    };
    return {
      ...node,
      ...(depth === undefined ? {} : { depth }),
      itemStyle: {
        color:
          aggregateColor[node.name] ??
          colorFor(
            node.name.startsWith("Revenu · ") ||
              node.name.startsWith("Dépense · ") ||
              node.name === "Épargne" ||
              node.name === "Déficit"
              ? node.name
              : "Flux consolidé",
          ),
      },
    };
  });
  const links = data.links.map((link) => ({
    ...link,
    lineStyle: {
      color: colorFor(linkKey(link)),
      opacity: linkKey(link) === "Flux consolidé" ? 0.38 : 0.52,
      curveness: 0.5,
    },
  }));
  return {
    nodes,
    links,
    nodeAlign: variant === "cash-flow" ? "left" : "justify",
  };
}

const mix = (
  from: [number, number, number],
  to: [number, number, number],
  ratio: number,
) =>
  `rgb(${from.map((value, index) => Math.round(value + (to[index] - value) * ratio)).join(", ")})`;

export const performanceColor = (value: number | null) => {
  if (value === null || !Number.isFinite(value)) return "#59677a";
  const ratio = Math.min(1, Math.abs(value) / 25);
  const neutral: [number, number, number] = [86, 101, 120];
  return value >= 0
    ? mix(neutral, [54, 190, 135], ratio)
    : mix(neutral, [222, 88, 100], ratio);
};

const sizeValue = (value: number) => Math.max(Math.abs(value), 0.01);

export function allocationTreeData(
  items: AllocationItem[],
  mode: AllocationMode,
): AllocationTreeNode[] {
  const visible =
    mode === "performance"
      ? items.filter((item) => item.type === "INVESTMENT")
      : items;
  const accounts = Array.from(new Set(visible.map((item) => item.account)));

  return accounts.map((account, accountIndex) => {
    const accountItems = visible.filter((item) => item.account === account);
    const displayValue = accountItems.reduce(
      (sum, item) => sum + item.value,
      0,
    );
    const accountColor = ACCOUNT_PALETTE[accountIndex % ACCOUNT_PALETTE.length];
    const accountStyle = {
      itemStyle: {
        color: accountColor,
        borderColor: accountColor,
      },
      upperLabel: {
        backgroundColor: accountColor,
        color: "#ffffff",
      },
    };
    if (mode === "accounts") {
      return {
        id: `account:${account}`,
        name: account,
        value: sizeValue(displayValue),
        displayValue,
        kind: "account",
        itemStyle: accountStyle,
      };
    }

    const children = accountItems.map((item) => ({
      id: `asset:${account}:${item.ticker}`,
      name: item.name,
      value: sizeValue(item.value),
      displayValue: item.value,
      kind: "asset" as const,
      ticker: item.ticker,
      performanceAbsolute: item.performance_absolute,
      performancePercent: item.performance_percent,
      itemStyle: {
        color:
          mode === "performance"
            ? performanceColor(item.performance_percent)
            : fallbackColor(
                item.type === "CASH" ? `${account}:Liquidités` : item.ticker,
              ),
      },
    }));
    return {
      id: `account:${account}`,
      name: account,
      value: children.reduce((sum, item) => sum + item.value, 0),
      displayValue,
      kind: "account",
      itemStyle: accountStyle,
      children,
    };
  });
}
