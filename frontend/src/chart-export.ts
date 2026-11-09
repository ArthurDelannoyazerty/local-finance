import type { AllocationItem, AllocationMode } from "./allocation-data";
import { delimitedText } from "./clipboard";
import type { Selection } from "./chart-utils";
import type { Sankey } from "./flow-chart";

export function expenseText(
  rows: { category: string; amount: number }[],
  selected: Selection = {},
): string {
  return delimitedText(
    ["Type de dépense", "Montant"],
    rows
      .filter((row) => selected[row.category] !== false)
      .map((row) => [row.category, row.amount]),
    "\t",
  );
}

type MonthlyRow = {
  month: string;
  income: number;
  expense: number;
  expense_average_3m: number;
};

export function monthlyCsv(rows: MonthlyRow[], selected: Selection = {}): string {
  const columns = [
    { name: "Revenus", header: "Revenus", key: "income" },
    { name: "Dépenses", header: "Dépenses", key: "expense" },
    {
      name: "Moy. dépenses 3M",
      header: "Moyenne dépenses 3 mois",
      key: "expense_average_3m",
    },
  ] as const;
  const visible = columns.filter((column) => selected[column.name] !== false);
  return delimitedText(
    ["Date", ...visible.map((column) => column.header)],
    rows.map((row) => [row.month, ...visible.map((column) => row[column.key])]),
  );
}

export function flowCsv(data: Sankey): string {
  // Export the original directed links, never ECharts' synthetic node IDs.
  return delimitedText(
    ["Source", "Destination", "Montant"],
    data.links.map((link) => [link.source, link.target, link.value]),
  );
}

type Evolution = {
  accounts: string[];
  items: {
    date: string;
    total_wealth: number;
    total_investment: number;
    accounts: Record<string, number>;
  }[];
};

export function portfolioCsv(
  data: Evolution,
  mode: "total" | "accounts",
  selected: Selection = {},
): string {
  if (mode === "accounts") {
    const accounts = data.accounts.filter((name) => selected[name] !== false);
    return delimitedText(
      ["Date", ...accounts],
      data.items.map((row) => [
        row.date,
        ...accounts.map((name) => row.accounts[name] ?? 0),
      ]),
    );
  }
  const columns = [
    { name: "Patrimoine net", header: "Patrimoine net", key: "total_wealth" },
    {
      name: "Dont investissements",
      header: "Investissements",
      key: "total_investment",
    },
  ] as const;
  const visible = columns.filter((column) => selected[column.name] !== false);
  return delimitedText(
    ["Date", ...visible.map((column) => column.header)],
    data.items.map((row) => [row.date, ...visible.map((column) => row[column.key])]),
  );
}

export function allocationCsv(
  items: AllocationItem[],
  mode: AllocationMode,
  at: string,
  start: string,
): string {
  if (mode === "accounts") {
    const totals = new Map<string, number>();
    for (const item of items) {
      totals.set(item.account, (totals.get(item.account) ?? 0) + item.value);
    }
    return delimitedText(
      ["Date", "Compte", "Valeur"],
      [...totals].map(([account, value]) => [at, account, value]),
    );
  }
  const visible =
    mode === "performance"
      ? items.filter((item) => item.type === "INVESTMENT")
      : items;
  return delimitedText(
    [
      "Date",
      "Compte",
      "Type",
      "Actif",
      "Ticker",
      "Quantité",
      "Prix unitaire",
      "Devise",
      "Valeur",
      ...(mode === "performance" ? ["Début", "Performance (%)"] : []),
    ],
    visible.map((item) => [
      at,
      item.account,
      item.type === "CASH" ? "Liquidités" : "Investissement",
      item.name,
      item.ticker,
      item.quantity,
      item.unit_price,
      item.currency,
      item.value,
      ...(mode === "performance" ? [start, item.performance_percent] : []),
    ]),
  );
}
