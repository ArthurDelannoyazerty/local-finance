import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, subMonths } from "date-fns";
import { api, searchParams } from "../api";
import {
  allocationTreeData,
  type AllocationMode,
  type AllocationTreeNode,
} from "../chartOptions";
import Chart from "../components/Chart";
import {
  Empty,
  ErrorBlock,
  Loading,
  money,
  PageHeader,
  Panel,
  percent,
} from "../components/ui";

type Item = {
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
  performance_absolute: number | null;
  performance_percent: number | null;
};
type Response = { date: string; start: string | null; items: Item[] };

const signedMoney = (value: number | null | undefined) =>
  value === null || value === undefined
    ? "—"
    : `${value > 0 ? "+" : ""}${money(value)}`;

const signedPercent = (value: number | null | undefined) => {
  const formatted = percent(value);
  return value !== null && value !== undefined && value > 0
    ? `+${formatted}`
    : formatted;
};

export default function Allocation() {
  const [mode, setMode] = useState<AllocationMode>("assets");
  const [at, setAt] = useState(format(new Date(), "yyyy-MM-dd"));
  const [start, setStart] = useState(
    format(subMonths(new Date(), 3), "yyyy-MM-dd"),
  );
  const query = useQuery({
    queryKey: ["allocation", at, start, mode],
    queryFn: () =>
      api<Response>(
        `/api/portfolio/allocation${searchParams({ date: at, start: mode === "performance" ? start : undefined })}`,
      ),
  });
  const items = query.data?.items ?? [];
  const option = useMemo(() => {
    const data = allocationTreeData(items, mode);
    return {
      tooltip: {
        formatter: (params: { data?: AllocationTreeNode }) => {
          if (!params.data) return "";
          const value = `<strong>${params.data.name}</strong><br/>Valeur : ${money(params.data.displayValue)}`;
          return params.data.performancePercent !== undefined
            ? `${value}<br/>Variation : ${signedMoney(params.data.performanceAbsolute)} · ${signedPercent(params.data.performancePercent)}`
            : value;
        },
      },
      series: [
        {
          type: "treemap",
          data,
          roam: true,
          nodeClick: "zoomToNode",
          breadcrumb: {
            show: mode !== "accounts",
            itemStyle: { color: "#253247", borderColor: "#536176" },
            textStyle: { color: "#e5ebf3" },
          },
          upperLabel: {
            show: true,
            height: 26,
            color: "#e6edf5",
            fontWeight: 650,
          },
          label: {
            show: true,
            formatter: (params: { data?: AllocationTreeNode }) => {
              if (!params.data?.id) return "";
              if (mode === "performance" && params.data.kind === "asset") {
                return `${params.data.name}\n${signedMoney(params.data.performanceAbsolute)}\n${signedPercent(params.data.performancePercent)}`;
              }
              return `${params.data.name}\n${money(params.data.displayValue)}`;
            },
            color: "#f1f5fa",
            fontSize: 11,
            lineHeight: 17,
            overflow: "truncate",
          },
          itemStyle: { borderColor: "#111a26", borderWidth: 1, gapWidth: 0 },
          levels: [
            {
              itemStyle: {
                borderColor: "#111a26",
                borderWidth: 0,
                gapWidth: 0,
              },
              label: { show: false },
              upperLabel: { show: false },
            },
            {
              itemStyle: {
                borderColor: "#68778d",
                borderWidth: mode === "accounts" ? 1 : 2,
                gapWidth: 0,
              },
              upperLabel: { show: mode !== "accounts", height: 27 },
            },
            {
              itemStyle: {
                borderColor: "#172130",
                borderWidth: 1,
                gapWidth: 0,
              },
            },
          ],
        },
      ],
    };
  }, [items, mode]);
  return (
    <>
      <PageHeader
        eyebrow="Patrimoine"
        title="Allocation"
        description="Explorez le poids de chaque compte et actif, ou leur performance corrigée des achats et ventes."
        actions={
          <div className="segmented">
            <button
              className={mode === "accounts" ? "active" : ""}
              onClick={() => setMode("accounts")}
            >
              Comptes
            </button>
            <button
              className={mode === "assets" ? "active" : ""}
              onClick={() => setMode("assets")}
            >
              Comptes & actifs
            </button>
            <button
              className={mode === "performance" ? "active" : ""}
              onClick={() => setMode("performance")}
            >
              Performance
            </button>
          </div>
        }
      />
      <Panel
        title="Carte du patrimoine"
        description={
          mode === "performance"
            ? "Variation absolue et relative corrigée des achats et ventes · rouge en baisse, gris stable, vert en hausse."
            : "Cliquez dans un bloc pour zoomer, utilisez le fil d’Ariane pour revenir."
        }
        action={
          <div className="filters" style={{ margin: 0 }}>
            {mode === "performance" && (
              <div className="field">
                <span>Début</span>
                <input
                  className="input"
                  type="date"
                  value={start}
                  max={at}
                  onChange={(e) => setStart(e.target.value)}
                />
              </div>
            )}
            <div className="field">
              <span>Situation au</span>
              <input
                className="input"
                type="date"
                value={at}
                min={mode === "performance" ? start : undefined}
                onChange={(e) => setAt(e.target.value)}
              />
            </div>
          </div>
        }
      >
        {query.isLoading ? (
          <Loading />
        ) : query.error ? (
          <ErrorBlock error={query.error} />
        ) : items.length ? (
          <Chart option={option} height={610} />
        ) : (
          <Empty
            title="Aucun actif à cette date"
            description="Choisissez une date plus récente ou ajoutez des comptes et investissements."
          />
        )}
      </Panel>
      {items.length > 0 && (
        <Panel
          className="panel-flush"
          title="Détail de la situation"
          description={`${items.length} positions et soldes`}
        >
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Compte</th>
                  <th>Type</th>
                  <th>Actif</th>
                  <th className="cell-number">Quantité</th>
                  <th className="cell-number">Prix</th>
                  <th className="cell-number">Valeur</th>
                  {mode === "performance" && (
                    <th className="cell-number">Performance</th>
                  )}
                </tr>
              </thead>
              <tbody>
                {items.map((item) => (
                  <tr key={`${item.account}-${item.ticker}`}>
                    <td>{item.account}</td>
                    <td>
                      <span className="badge">
                        {item.type === "CASH" ? "Liquidités" : "Investissement"}
                      </span>
                    </td>
                    <td>{item.name}</td>
                    <td className="cell-number">
                      {item.quantity.toLocaleString("fr-FR")}
                    </td>
                    <td className="cell-number">
                      {money(item.unit_price, item.currency, 2)}
                    </td>
                    <td className="cell-number">
                      <strong>{money(item.value)}</strong>
                    </td>
                    {mode === "performance" && (
                      <td
                        className={`cell-number ${
                          item.performance_percent === null
                            ? ""
                            : item.performance_percent >= 0
                              ? "positive"
                              : "negative"
                        }`}
                      >
                        {item.performance_absolute === null
                          ? "—"
                          : `${signedMoney(item.performance_absolute)} · ${signedPercent(item.performance_percent)}`}
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}
    </>
  );
}
