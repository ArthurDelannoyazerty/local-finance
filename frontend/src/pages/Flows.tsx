import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api, searchParams } from "../api";
import Chart from "../components/Chart";
import MonthSelector from "../components/MonthSelector";
import { flowChart, type Sankey } from "../flow-chart";
import { availableMonths } from "../months";
import {
  Empty,
  ErrorBlock,
  Loading,
  PageHeader,
  Panel,
} from "../components/ui";

type Response = { cash_flow: Sankey; transfers: Sankey; investments: Sankey };
type Bounds = { min: string; max: string; today: string };

export default function Flows() {
  const bounds = useQuery({
    queryKey: ["date-bounds"],
    queryFn: ({ signal }) => api<Bounds>("/api/meta/date-bounds", { signal }),
  });
  const available = useMemo(() => {
    if (!bounds.data) return [];
    const { min, max, today } = bounds.data;
    return availableMonths(min, max > today ? max : today);
  }, [bounds.data]);
  // null means the initial preset; [] means deliberately selecting no months.
  const [selection, setSelection] = useState<string[] | null>(null);
  const months = useMemo(() => {
    if (selection !== null) {
      return available.filter((month) => selection.includes(month));
    }
    return available
      .filter((month) => month <= (bounds.data?.today.slice(0, 7) ?? ""))
      .slice(-3);
  }, [available, selection, bounds.data]);
  const query = useQuery({
    queryKey: ["flows", months],
    queryFn: ({ signal }) =>
      api<Response>(`/api/flows${searchParams({ month: months })}`, { signal }),
    enabled: Boolean(bounds.data) && months.length > 0,
  });
  const data = query.data;
  return (
    <>
      <PageHeader
        eyebrow="Quotidien"
        title="Flux"
        description="Suivez le chemin de l’argent entre revenus, dépenses, comptes et investissements."
        actions={
          bounds.data && (
            <MonthSelector
              months={available}
              selected={months}
              today={bounds.data.today}
              onChange={setSelection}
            />
          )
        }
      />
      {bounds.isLoading ? (
        <Loading />
      ) : bounds.error ? (
        <ErrorBlock error={bounds.error} />
      ) : months.length === 0 ? (
        <Panel>
          <Empty
            title="Aucun mois sélectionné"
            description="Cochez les mois à afficher dans le sélecteur."
          />
        </Panel>
      ) : query.isLoading ? (
        <Loading />
      ) : query.error ? (
        <ErrorBlock error={query.error} />
      ) : data ? (
        <div className="stack">
          <Panel
            title="Revenus → dépenses"
            description="Une lecture consolidée des catégories sur les mois sélectionnés."
          >
            {data.cash_flow.links.length ? (
              <Chart
                option={flowChart(data.cash_flow, [
                  "#67e8b6",
                  "#72a5ff",
                  "#ff8585",
                  "#f5c66e",
                ])}
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
              description="Sorties à gauche, entrées à droite. Les deux sens sont conservés, sans compensation."
            >
              {data.transfers.links.length ? (
                <Chart
                  option={flowChart(
                    data.transfers,
                    ["#72a5ff", "#5bd7e8", "#b99cff"],
                    true,
                  )}
                  height={350}
                />
              ) : (
                <Empty
                  title="Aucun transfert"
                  description="Aucun transfert sur cette période."
                />
              )}
            </Panel>
            <Panel
              title="Vers vos actifs"
              description="Achats et ventes, des sources à gauche vers les destinations à droite."
            >
              {data.investments.links.length ? (
                <Chart
                  option={flowChart(
                    data.investments,
                    ["#f5c66e", "#67e8b6", "#ff8f8f"],
                    true,
                  )}
                  height={350}
                />
              ) : (
                <Empty
                  title="Aucune opération boursière"
                  description="Aucun achat ou vente sur cette période."
                />
              )}
            </Panel>
          </div>
        </div>
      ) : null}
    </>
  );
}
