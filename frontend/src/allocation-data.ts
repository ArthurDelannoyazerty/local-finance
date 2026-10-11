export type AllocationItem = {
  account: string;
  type: "CASH" | "INVESTMENT";
  ticker: string;
  name: string;
  quantity: number;
  unit_price: number;
  value: number;
  currency: string;
  start_value: number;
  net_contribution: number;
  performance_percent: number | null;
};
export type AllocationMode = "accounts" | "assets" | "performance";

const accountColors = [
  "#356b69",
  "#385b83",
  "#725d3d",
  "#65507c",
  "#346779",
  "#7b465a",
];

/** Explicit fills also work for accounts with no child tiles. */
export function accountColor(name: string): string {
  let hash = 0;
  for (const character of name) {
    hash = (Math.imul(hash, 31) + character.charCodeAt(0)) >>> 0;
  }
  return accountColors[hash % accountColors.length];
}

const performanceColor = (value: number | null) => {
  if (value === null) return "#354153";
  const magnitude = Math.min(1, Math.abs(value) / 25);
  return value >= 0
    ? `rgba(70, ${Math.round(150 + 65 * magnitude)}, ${Math.round(125 + 45 * magnitude)}, .9)`
    : `rgba(${Math.round(185 + 55 * magnitude)}, ${Math.round(100 - 30 * magnitude)}, ${Math.round(105 - 20 * magnitude)}, .9)`;
};

export function allocationNodes(
  items: AllocationItem[],
  mode: AllocationMode,
) {
  const visible =
    mode === "performance"
      ? items.filter((item) => item.type === "INVESTMENT")
      : items;
  const accounts = [...new Set(visible.map((item) => item.account))];
  return accounts.map((account) => {
    const children = visible
      .filter((item) => item.account === account)
      .map((item) => ({
        id: JSON.stringify(["asset", account, item.ticker]),
        name: item.name,
        value: item.value,
        ticker: item.ticker,
        performance: item.performance_percent,
        itemStyle:
          mode === "performance"
            ? { color: performanceColor(item.performance_percent) }
            : undefined,
      }));
    return {
      id: JSON.stringify(["account", account]),
      name: account,
      value: children.reduce((sum, item) => sum + item.value, 0),
      itemStyle: { color: accountColor(account) },
      ...(mode === "accounts" ? {} : { children }),
    };
  });
}
