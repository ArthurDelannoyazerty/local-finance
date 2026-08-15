import { describe, expect, it } from "vitest";
import {
  allocationTreeData,
  cashFlowDepth,
  styledSankey,
  type AllocationItem,
} from "./chartOptions";

describe("cash-flow Sankey", () => {
  it("keeps savings beside total expenses and gives categories solid colors", () => {
    const styled = styledSankey(
      {
        nodes: [
          { name: "Revenu · Salaire" },
          { name: "Total revenus" },
          { name: "Total dépenses" },
          { name: "Dépense · Maison" },
          { name: "Dépense · Transport" },
          { name: "Épargne" },
        ],
        links: [
          { source: "Revenu · Salaire", target: "Total revenus", value: 3000 },
          { source: "Total revenus", target: "Total dépenses", value: 2000 },
          { source: "Total dépenses", target: "Dépense · Maison", value: 1500 },
          {
            source: "Total dépenses",
            target: "Dépense · Transport",
            value: 500,
          },
          { source: "Total revenus", target: "Épargne", value: 1000 },
        ],
      },
      "cash-flow",
    );

    expect(cashFlowDepth("Total dépenses")).toBe(2);
    expect(cashFlowDepth("Épargne")).toBe(2);
    expect(styled.nodeAlign).toBe("left");
    const categoryLinks = styled.links.filter((link) =>
      link.target.startsWith("Dépense · "),
    );
    expect(categoryLinks[0].lineStyle.color).not.toBe(
      categoryLinks[1].lineStyle.color,
    );
    expect(
      categoryLinks.every((link) => link.lineStyle.color !== "gradient"),
    ).toBe(true);
    expect(categoryLinks.every((link) => link.lineStyle.opacity < 1)).toBe(
      true,
    );
  });
});

describe("allocation treemap", () => {
  const items: AllocationItem[] = [
    {
      account: "PEA",
      type: "CASH",
      ticker: "CASH",
      name: "Liquidités",
      value: 5000,
      performance_absolute: null,
      performance_percent: null,
    },
    {
      account: "PEA",
      type: "INVESTMENT",
      ticker: "CW8.PA",
      name: "CW8",
      value: 12000,
      performance_absolute: 1000,
      performance_percent: 9.1,
    },
  ];

  it("colors account-only leaves explicitly", () => {
    const [account] = allocationTreeData(items, "accounts");
    expect(account.id).toBe("account:PEA");
    expect(account.itemStyle.color).toMatch(/^#/);
    expect(account.children).toBeUndefined();
  });

  it("nests assets under a unique account node", () => {
    const [account] = allocationTreeData(items, "assets");
    expect(account.children).toHaveLength(2);
    expect(account.children?.map((child) => child.id)).toEqual([
      "asset:PEA:CASH",
      "asset:PEA:CW8.PA",
    ]);
  });

  it("keeps absolute and percentage performance on investment leaves", () => {
    const [account] = allocationTreeData(items, "performance");
    expect(account.children).toHaveLength(1);
    expect(account.children?.[0]).toMatchObject({
      displayValue: 12000,
      performanceAbsolute: 1000,
      performancePercent: 9.1,
    });
  });
});
