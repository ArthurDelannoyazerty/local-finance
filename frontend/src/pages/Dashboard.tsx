import { useQuery } from "@tanstack/react-query";
import { api, searchParams } from "../api";
import Chart from "../components/Chart";
import {
  Empty,
  ErrorBlock,
  Loading,
  MetricCard,
  money,
  PageHeader,
  Panel,
  percent,
} from "../components/ui";
import type { DateRangeValue } from "../types";

type DashboardData = {
  totals: {
    income: number;
    expense: number;
    savings: number;
    savings_rate: number;
  };
  monthly_average: { income: number; expense: number; savings: number };
  expenses_by_category: { category: string; amount: number }[];
  monthly: {
    month: string;
    income: number;
    expense: number;
    expense_average_3m: number;
  }[];
};

const chartText = { color: "#8c9aae", fontFamily: "Inter, sans-serif" };

export default function Dashboard({ range }: { range: DateRangeValue }) {
  const query = useQuery({
    queryKey: ["dashboard", range],
    queryFn: () => api<DashboardData>(`/api/dashboard${searchParams(range)}`),
  });
  if (query.isLoading)
    return <Loading label="Calcul de votre vue d’ensemble…" />;
  if (query.error) return <ErrorBlock error={query.error} />;
  const data = query.data!;
  const hasData = data.totals.income !== 0 || data.totals.expense !== 0;

  const donut = {
    tooltip: {
      trigger: "item",
      valueFormatter: (value: number) => money(value),
    },
    legend: { bottom: 0, textStyle: chartText, itemWidth: 9, itemHeight: 9 },
    color: [
      "#67e8b6",
      "#72a5ff",
      "#f5c66e",
      "#ff8f8f",
      "#b99cff",
      "#5bd7e8",
      "#a7d178",
    ],
    series: [
      {
        type: "pie",
        radius: ["52%", "76%"],
        center: ["50%", "43%"],
        padAngle: 2,
        itemStyle: { borderRadius: 5, borderColor: "#101722", borderWidth: 3 },
        label: { show: false },
        data: data.expenses_by_category.map((item) => ({
          name: item.category,
          value: item.amount,
        })),
      },
    ],
    graphic: [
      {
        type: "text",
        left: "center",
        top: "34%",
        style: { text: "DÉPENSES", fill: "#758297", font: "600 10px Inter" },
      },
      {
        type: "text",
        left: "center",
        top: "42%",
        style: {
          text: money(data.totals.expense),
          fill: "#eef2f8",
          font: "650 22px Inter",
        },
      },
    ],
  };
  const monthly = {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "shadow" },
      valueFormatter: (value: number) => money(value),
    },
    legend: { top: 0, right: 0, textStyle: chartText },
    grid: { left: 42, right: 18, top: 44, bottom: 30 },
    xAxis: {
      type: "category",
      data: data.monthly.map((item) => item.month),
      axisLabel: chartText,
      axisLine: { lineStyle: { color: "#273142" } },
    },
    yAxis: {
      type: "value",
      axisLabel: {
        ...chartText,
        formatter: (value: number) => `${Math.round(value / 1000)}k`,
      },
      splitLine: { lineStyle: { color: "rgba(255,255,255,.055)" } },
    },
    series: [
      {
        name: "Revenus",
        type: "bar",
        data: data.monthly.map((item) => item.income),
        itemStyle: { color: "#67e8b6", borderRadius: [4, 4, 0, 0] },
        barMaxWidth: 24,
      },
      {
        name: "Dépenses",
        type: "bar",
        data: data.monthly.map((item) => item.expense),
        itemStyle: { color: "#ff8585", borderRadius: [4, 4, 0, 0] },
        barMaxWidth: 24,
      },
      {
        name: "Moy. dépenses 3M",
        type: "line",
        data: data.monthly.map((item) => item.expense_average_3m),
        symbol: "none",
        lineStyle: { color: "#f5c66e", width: 2, type: "dashed" },
      },
    ],
  };

  return (
    <>
      <PageHeader
        eyebrow="Quotidien"
        title="Vue d’ensemble"
        description="Votre argent du quotidien, résumé sans perdre de vue la construction du patrimoine."
      />
      <div className="metrics">
        <MetricCard
          label="Revenus"
          value={money(data.totals.income)}
          tone="positive"
          note={<>Moyenne · {money(data.monthly_average.income)}/mois</>}
        />
        <MetricCard
          label="Dépenses"
          value={money(data.totals.expense)}
          tone="negative"
          note={<>Moyenne · {money(data.monthly_average.expense)}/mois</>}
        />
        <MetricCard
          label="Épargne nette"
          value={money(data.totals.savings)}
          tone={data.totals.savings >= 0 ? "accent" : "negative"}
          note={<>Moyenne · {money(data.monthly_average.savings)}/mois</>}
        />
        <MetricCard
          label="Taux d’épargne"
          value={percent(data.totals.savings_rate)}
          note="Revenus moins dépenses"
        />
      </div>
      {!hasData ? (
        <Panel>
          <Empty
            title="Aucune transaction sur cette période"
            description="Importez un export Excel ou choisissez une période plus large."
          />
        </Panel>
      ) : (
        <div className="grid-2">
          <Panel
            title="Où part votre argent"
            description="Dépenses regroupées selon les catégories de l’application mobile."
          >
            <Chart option={donut} height={410} />
          </Panel>
          <Panel
            title="Rythme mensuel"
            description="Revenus, dépenses et tendance glissante des trois derniers mois."
          >
            <Chart option={monthly} height={410} />
          </Panel>
        </div>
      )}
    </>
  );
}
