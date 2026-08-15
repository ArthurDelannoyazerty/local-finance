import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, subMonths } from "date-fns";
import { Plus, X } from "lucide-react";
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

export default function Flows() {
  const defaults = useMemo(
    () =>
      [0, 1, 2].map((offset) =>
        format(subMonths(new Date(), offset), "yyyy-MM"),
      ),
    [],
  );
  const [months, setMonths] = useState(defaults);
  const [newMonth, setNewMonth] = useState(format(new Date(), "yyyy-MM"));
  const query = useQuery({
    queryKey: ["flows", months],
    queryFn: () =>
      api<Response>(`/api/flows${searchParams({ month: months })}`),
  });
  const addMonth = () =>
    setMonths((current) =>
      Array.from(new Set([...current, newMonth]))
        .sort()
        .reverse(),
    );
  return (
    <>
      <PageHeader
        eyebrow="Quotidien"
        title="Flux"
        description="Suivez le chemin de l’argent entre revenus, dépenses, comptes et investissements."
        actions={
          <div className="month-picker">
            {months.map((month) => (
              <span className="month-chip" key={month}>
                {month}
                <button
                  onClick={() =>
                    setMonths((current) =>
                      current.filter((value) => value !== month),
                    )
                  }
                  aria-label={`Retirer ${month}`}
                >
                  <X size={12} />
                </button>
              </span>
            ))}
            <input
              className="input"
              type="month"
              value={newMonth}
              onChange={(event) => setNewMonth(event.target.value)}
              style={{ width: 140 }}
            />
            <button className="button button-secondary" onClick={addMonth}>
              <Plus size={15} />
              Ajouter
            </button>
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
            description="Ajoutez au moins un mois pour reconstruire les flux."
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
