import { describe, expect, it } from "vitest";
import type { AllocationItem } from "./allocation-data";
import {
  allocationCsv,
  expenseText,
  flowCsv,
  monthlyCsv,
  portfolioCsv,
} from "./chart-export";
import { asArray, chartOption } from "./chart-options";
import { monteCarloCsv, projectionCsv } from "./fire-export";
import { flowChart } from "./flow-chart";

const items: AllocationItem[] = [
  {
    account: "PEA",
    type: "CASH",
    ticker: "CASH",
    name: "Liquidités",
    quantity: 1,
    unit_price: 25,
    value: 25,
    currency: "EUR",
    start_value: 25,
    net_contribution: 0,
    performance_percent: null,
  },
  {
    account: "PEA",
    type: "INVESTMENT",
    ticker: "ETF",
    name: "Mon ETF",
    quantity: 2,
    unit_price: 50,
    value: 100,
    currency: "EUR",
    start_value: 90,
    net_contribution: 0,
    performance_percent: 11.111,
  },
];

describe("chart exports match the active view", () => {
  it("copies expense categories as a two-column text table", () => {
    expect(expenseText([{ category: "Courses", amount: 42.56 }])).toBe(
      "Type de dépense\tMontant\r\nCourses\t42.56",
    );
    expect(
      expenseText([{ category: "Courses", amount: 42.56 }], { Courses: false }),
    ).toBe("Type de dépense\tMontant");
  });
  it("copies monthly dates and all three displayed measures", () => {
    const rows = [
      { month: "2026-01", income: 2000, expense: 500, expense_average_3m: 450 },
      { month: "2026-02", income: 2100, expense: 600, expense_average_3m: 500 },
    ];
    expect(monthlyCsv(rows)).toBe(
      "Date,Revenus,Dépenses,Moyenne dépenses 3 mois\r\n2026-01,2000,500,450\r\n2026-02,2100,600,500",
    );
    expect(monthlyCsv(rows, { Dépenses: false })).not.toContain(
      "Date,Revenus,Dépenses,",
    );
  });
  it("exports both directions of flows without synthetic node IDs or netting", () => {
    const data = {
      nodes: [{ name: "Courant" }, { name: "Épargne" }],
      links: [
        { source: "Courant", target: "Épargne", value: 100 },
        { source: "Épargne", target: "Courant", value: 40 },
      ],
    };
    expect(flowCsv(data)).toBe(
      "Source,Destination,Montant\r\nCourant,Épargne,100\r\nÉpargne,Courant,40",
    );
  });
  it("switches portfolio columns with Total / Par compte", () => {
    const data = {
      accounts: ["Courant", "PEA"],
      items: [
        {
          date: "2026-01-01",
          total_wealth: 150.5,
          total_investment: 100,
          accounts: { Courant: 50.5, PEA: 100 },
        },
      ],
    };
    expect(portfolioCsv(data, "total")).toBe(
      "Date,Patrimoine net,Investissements\r\n2026-01-01,150.5,100",
    );
    expect(portfolioCsv(data, "accounts")).toBe(
      "Date,Courant,PEA\r\n2026-01-01,50.5,100",
    );
    expect(portfolioCsv(data, "accounts", { PEA: false })).toBe(
      "Date,Courant\r\n2026-01-01,50.5",
    );
  });
  it("copies account totals rather than individual assets in accounts mode", () => {
    expect(allocationCsv(items, "accounts", "2026-03-01", "2026-01-01")).toBe(
      "Date,Compte,Valeur\r\n2026-03-01,PEA,125",
    );
  });
  it("includes assets and cash in assets mode, without performance columns", () => {
    const csv = allocationCsv(items, "assets", "2026-03-01", "2026-01-01");
    expect(csv).toContain("Liquidités");
    expect(csv).toContain("Mon ETF");
    expect(csv).not.toContain("Performance");
    expect(csv.split("\r\n")).toHaveLength(3);
  });
  it("copies only plotted investments and the active dates in performance mode", () => {
    const csv = allocationCsv(items, "performance", "2026-03-01", "2026-01-01");
    expect(csv).not.toContain("Liquidités");
    expect(csv).toContain("Début,Performance (%)");
    expect(csv).toContain("2026-03-01,PEA,Investissement,Mon ETF");
    expect(csv).toContain("2026-01-01,11.111");
    const unknown = [{ ...items[1], performance_percent: null }];
    expect(allocationCsv(unknown, "performance", "2026-03-01", "2026-01-01")).toMatch(
      /2026-01-01,$/,
    );
  });
  it("honors FIRE series selection and preserves missing Coast values", () => {
    const option = {
      series: [
        { name: "Capital", data: [[30, 100], [31, 200]] },
        { name: "Coast", data: [[30, 80], [31, null]] },
      ],
    };
    expect(projectionCsv(option)).toBe("Âge,Capital,Coast\r\n30,100,80\r\n31,200,");
    expect(projectionCsv(option, { Coast: false })).toBe(
      "Âge,Capital\r\n30,100\r\n31,200",
    );
  });
  it("copies real Monte Carlo bounds, not the internal stacked-band delta", () => {
    const rows = [{ year: 1, age: 31, p10: -10, p50: 20, p90: 90 }];
    expect(monteCarloCsv(rows)).toBe("Années,Âge,P10,P90,Médiane\r\n1,31,-10,90,20");
    expect(monteCarloCsv(rows, { "Zone 80%": false })).toBe(
      "Années,Âge,Médiane\r\n1,31,20",
    );
  });
  it("allows dragging the first Sankey through the shared option builder", () => {
    const data = {
      nodes: [{ name: "A" }, { name: "B" }],
      links: [{ source: "A", target: "B", value: 10 }],
    };
    const first = chartOption(flowChart(data, []), {}, []);
    const accounts = chartOption(flowChart(data, [], true), {}, []);
    expect(asArray(first.series)[0].draggable).toBe(true);
    expect(asArray(accounts.series)[0].draggable).toBe(false);
    expect(
      asArray(chartOption({ series: { type: "sankey" } }, {}, []).series)[0].draggable,
    ).toBe(false);
  });
});
