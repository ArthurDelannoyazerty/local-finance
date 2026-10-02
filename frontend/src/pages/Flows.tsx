import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, subMonths } from "date-fns";
import { Plus } from "lucide-react";
import { api, searchParams } from "../api";
import {
  styledSankey,
  type SankeyData,
  type SankeyVariant,
} from "../chartOptions";
import Chart from "../components/Chart";
import {
  Empty,
  ErrorBlock,
  Loading,
  PageHeader,
  Panel,
} from "../components/ui";

type Response = {
  cash_flow: SankeyData;
  transfers: SankeyData;
  investments: SankeyData;
};

function option(data: SankeyData, variant: SankeyVariant = "generic") {
  const styled = styledSankey(data, variant);
  return {
    tooltip: {
      trigger: "item",
      triggerOn: "mousemove",
      valueFormatter: (value: number) =>
        `${Math.round(value).toLocaleString("fr-FR")} €`,
    },
    series: [
      {
        type: "sankey",
        data: styled.nodes,
        links: styled.links,
        emphasis: { focus: "adjacency" },
        nodeAlign: styled.nodeAlign,
        nodeGap: 14,
        nodeWidth: 15,
        layoutIterations: 40,
        lineStyle: { curveness: 0.5, opacity: 0.52 },
        itemStyle: {
          borderColor: "rgba(255,255,255,.22)",
          borderWidth: 1,
          borderRadius: 2,
        },
        label: { color: "#aeb9c8", fontSize: 11 },
      },
    ],
  };
}

const monthLabel = (month: string) =>
  new Intl.DateTimeFormat("fr-FR", {
    month: "short",
    year: "2-digit",
  }).format(new Date(`${month}-01T12:00:00`));

export default function Flows() {
  const defaults = useMemo(
    () =>
      [0, 1, 2].map((offset) =>
        format(subMonths(new Date(), offset), "yyyy-MM"),
      ),
    [],
  );
  const available = useMemo(
    () =>
      Array.from({ length: 12 }, (_, index) =>
        format(subMonths(new Date(), index), "yyyy-MM"),
      ),
    [],
  );
  const [months, setMonths] = useState(defaults);
  const [newMonth, setNewMonth] = useState("");
  const query = useQuery({
    queryKey: ["flows", months],
    queryFn: () =>
      api<Response>(`/api/flows${searchParams({ month: months })}`),
  });
  const toggleMonth = (month: string) =>
    setMonths((current) =>
      current.includes(month)
        ? current.filter((value) => value !== month)
        : [...current, month].sort().reverse(),
    );
  const addMonth = () => {
    if (!newMonth) return;
    toggleMonth(newMonth);
    setNewMonth("");
  };
  return (
    <>
      <PageHeader
        eyebrow="Quotidien"
        title="Flux"
        description="Suivez le chemin de l’argent entre revenus, dépenses, comptes et investissements."
        actions={
          <div className="month-picker">
            <span className="month-picker-label">Mois affichés :</span>
            <div className="month-options">
              {available.map((month) => (
                <button
                  key={month}
                  className={`month-toggle ${months.includes(month) ? "on" : ""}`}
                  onClick={() => toggleMonth(month)}
                  aria-pressed={months.includes(month)}
                >
                  {monthLabel(month)}
                </button>
              ))}
            </div>
            <div className="month-add">
              <input
                className="input"
                type="month"
                style={{ width: 150 }}
                value={newMonth}
                onChange={(event) => setNewMonth(event.target.value)}
              />
              <button
                className="button button-secondary"
                onClick={addMonth}
                disabled={!newMonth || months.includes(newMonth)}
              >
                <Plus size={15} />
                Ajouter
              </button>
            </div>
          </div>
        }
      />
      {query.isLoading ? (
        <Loading />
      ) : query.error ? (
        <ErrorBlock error={query.error} />
      ) : months.length === 0 ? (
        <Panel>
          <Empty
            title="Aucun mois sélectionné"
            description="Cliquez sur un mois ci-dessus pour reconstruire les flux."
          />
        </Panel>
      ) : (
        <div className="stack">
          <Panel
            title="Revenus → dépenses"
            description="Une lecture consolidée des catégories sur les mois sélectionnés."
          >
            {query.data!.cash_flow.links.length ? (
              <Chart
                option={option(query.data!.cash_flow, "cash-flow")}
                height={510}
              />
            ) : (
              <Empty
                title="Aucun flux quotidien"
                description="Cette sélection ne contient ni revenu ni dépense."
              />
            )}
          </Panel>
          <div className="grid-2">
            <Panel
              title="Entre vos comptes"
              description="Transferts internes, sans les confondre avec des dépenses."
            >
              {query.data!.transfers.links.length ? (
                <Chart option={option(query.data!.transfers)} height={350} />
              ) : (
                <Empty
                  title="Aucun transfert"
                  description="Aucun transfert sur cette période."
                />
              )}
            </Panel>
            <Panel
              title="Vers vos actifs"
              description="Achats et ventes reliant comptes et tickers."
            >
              {query.data!.investments.links.length ? (
                <Chart option={option(query.data!.investments)} height={350} />
              ) : (
                <Empty
                  title="Aucune opération boursière"
                  description="Aucun achat ou vente sur cette période."
                />
              )}
            </Panel>
          </div>
        </div>
      )}
    </>
  );
}
