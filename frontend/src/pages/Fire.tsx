import { useEffect, useMemo, useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Dice5, Plus, Save, Trash2 } from "lucide-react";
import { api } from "../api";
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
import type { LifeEvent, ProjectionInput } from "../types";

type Projection = {
  items: {
    month: number;
    year: number;
    age: number;
    net_nominal: number;
    net_real: number;
  }[];
  metrics: {
    final_wealth: number;
    monthly_rent_4_percent: number;
    fire_age: number | null;
    tipping_age: number | null;
    fire_target: number;
    lean_fire_target: number;
    fat_fire_target: number;
  };
};
type MonteCarlo = {
  items: { year: number; age: number; p10: number; p50: number; p90: number }[];
  metrics: {
    success_probability: number;
    median_final: number;
    pessimistic_final: number;
    optimistic_final: number;
  };
};
type Scenario = {
  id: string;
  name: string;
  created_at: string;
  parameters: ProjectionInput;
};

const initial: ProjectionInput = {
  current_age: 30,
  retirement_age: 65,
  start_capital: 0,
  monthly_savings: 500,
  monthly_expenses: 1500,
  years: 40,
  annual_return_rate: 0.07,
  inflation_rate: 0.02,
  salary_growth_rate: 0.015,
  tax_rate: 0.3,
  volatility: 0.15,
  stop_working_age: null,
  life_events: [],
  show_real: true,
  simulations: 300,
  seed: 42,
};

function RangeField({
  label,
  value,
  min,
  max,
  step,
  suffix,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  suffix: string;
  onChange: (value: number) => void;
}) {
  return (
    <div className="range-field">
      <label>{label}</label>
      <output>
        {value.toLocaleString("fr-FR")}
        {suffix}
      </output>
      <input
        type="range"
        value={value}
        min={min}
        max={max}
        step={step}
        onChange={(e) => onChange(Number(e.target.value))}
      />
    </div>
  );
}

export default function Fire() {
  const client = useQueryClient();
  const defaults = useQuery({
    queryKey: ["projection-defaults"],
    queryFn: () =>
      api<{
        wealth: number;
        monthly_savings: number;
        monthly_expenses: number;
      }>("/api/projections/defaults"),
  });
  const scenarios = useQuery({
    queryKey: ["scenarios"],
    queryFn: () => api<Scenario[]>("/api/scenarios"),
  });
  const [draft, setDraft] = useState<ProjectionInput>(initial);
  const [active, setActive] = useState<ProjectionInput>(initial);
  const [defaultsApplied, setDefaultsApplied] = useState(false);
  const [scenarioName, setScenarioName] = useState("Scénario principal");
  const [event, setEvent] = useState<LifeEvent>({
    name: "Achat voiture",
    year: 5,
    amount: -15000,
  });
  const [showLean, setShowLean] = useState(true);
  const [showFat, setShowFat] = useState(false);
  const [showCoast, setShowCoast] = useState(false);
  const [milestones, setMilestones] = useState([100000]);
  useEffect(() => {
    if (defaults.data && !defaultsApplied) {
      const next = {
        ...initial,
        start_capital: defaults.data.wealth,
        monthly_savings: defaults.data.monthly_savings,
        monthly_expenses: defaults.data.monthly_expenses,
      };
      setDraft(next);
      setActive(next);
      setDefaultsApplied(true);
    }
  }, [defaults.data, defaultsApplied]);
  const projection = useQuery({
    queryKey: ["projection", active],
    queryFn: () =>
      api<Projection>("/api/projections/calculate", {
        method: "POST",
        body: JSON.stringify(active),
      }),
  });
  const monteCarlo = useMutation({
    mutationFn: () =>
      api<MonteCarlo>("/api/projections/monte-carlo", {
        method: "POST",
        body: JSON.stringify(active),
      }),
  });
  const save = useMutation({
    mutationFn: () =>
      api("/api/scenarios", {
        method: "POST",
        body: JSON.stringify({ name: scenarioName, parameters: draft }),
      }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["scenarios"] }),
  });
  const removeScenario = useMutation({
    mutationFn: (id: string) =>
      api(`/api/scenarios/${id}`, { method: "DELETE" }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["scenarios"] }),
  });
  const update = <K extends keyof ProjectionInput>(
    key: K,
    value: ProjectionInput[K],
  ) =>
    setDraft((current) => {
      const next = { ...current, [key]: value } as ProjectionInput;
      next.retirement_age = Math.max(next.current_age, next.retirement_age);
      if (
        next.stop_working_age !== null &&
        (next.stop_working_age < next.current_age ||
          next.stop_working_age > next.current_age + next.years)
      ) {
        next.stop_working_age = null;
      }
      next.life_events = next.life_events.filter(
        (item) => item.year <= next.years,
      );
      return next;
    });
  const addEvent = (e: FormEvent) => {
    e.preventDefault();
    update("life_events", [...draft.life_events, event]);
  };
  const data = projection.data;
  const chart = useMemo(() => {
    if (!data) return {};
    const items = data.items;
    const series: Record<string, unknown>[] = [
      {
        name: active.show_real
          ? "Patrimoine net réel"
          : "Patrimoine net nominal",
        type: "line",
        symbol: "none",
        smooth: 0.15,
        data: items.map((item) =>
          active.show_real ? item.net_real : item.net_nominal,
        ),
        lineStyle: { color: "#67e8b6", width: 3 },
        areaStyle: { color: "#67e8b6", opacity: 0.08 },
        markLine: {
          silent: true,
          symbol: "none",
          label: { color: "#8c9aae", formatter: "{b}" },
          lineStyle: { color: "#415066", type: "dashed" },
          data: [
            active.stop_working_age
              ? {
                  name: "Arrêt",
                  xAxis: active.stop_working_age,
                  lineStyle: { color: "#ff8585" },
                }
              : null,
            data.metrics.tipping_age
              ? {
                  name: "Bascule",
                  xAxis: data.metrics.tipping_age,
                  lineStyle: { color: "#72a5ff" },
                }
              : null,
          ].filter(Boolean),
        },
      },
    ];
    const targetValues = (value: number) =>
      items.map((item) =>
        active.show_real
          ? value
          : value * (1 + active.inflation_rate) ** item.year,
      );
    const horizontal = (name: string, value: number, color: string) => ({
      name,
      type: "line",
      symbol: "none",
      data: targetValues(value),
      lineStyle: { color, width: 1.5, type: "dashed" },
      emphasis: { disabled: true },
    });
    series.push(horizontal("FIRE 4%", data.metrics.fire_target, "#72a5ff"));
    if (showLean)
      series.push(
        horizontal("Lean FIRE", data.metrics.lean_fire_target, "#f5c66e"),
      );
    if (showFat)
      series.push(
        horizontal("Fat FIRE", data.metrics.fat_fire_target, "#b99cff"),
      );
    milestones.forEach((value) =>
      series.push(
        horizontal(`${value / 1000}k`, value, "rgba(255,255,255,.22)"),
      ),
    );
    if (showCoast) {
      const realReturn =
        (1 + active.annual_return_rate) / (1 + active.inflation_rate) - 1;
      const rate = active.show_real ? realReturn : active.annual_return_rate;
      const retirementTarget = active.show_real
        ? data.metrics.fire_target
        : data.metrics.fire_target *
          (1 + active.inflation_rate) **
            (active.retirement_age - active.current_age);
      series.push({
        name: "Coast FIRE",
        type: "line",
        symbol: "none",
        data: items.map((item) =>
          item.age <= active.retirement_age
            ? retirementTarget /
              (1 + rate) ** (active.retirement_age - item.age)
            : null,
        ),
        lineStyle: { color: "#f5c66e", width: 2, type: "dotted" },
      });
    }
    return {
      tooltip: {
        trigger: "axis",
        valueFormatter: (value: number) => money(value),
      },
      legend: { top: 0, right: 0, textStyle: { color: "#8c9aae" } },
      grid: { left: 58, right: 18, top: 46, bottom: 36 },
      xAxis: {
        type: "category",
        data: items.map((item) => Number(item.age.toFixed(1))),
        axisLabel: { color: "#748195" },
        axisLine: { lineStyle: { color: "#273142" } },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: {
          color: "#748195",
          formatter: (value: number) => `${Math.round(value / 1000)}k`,
        },
        splitLine: { lineStyle: { color: "rgba(255,255,255,.055)" } },
      },
      dataZoom: [{ type: "inside" }],
      series,
    };
  }, [data, active, showLean, showFat, showCoast, milestones]);
  const mcChart = useMemo(() => {
    const result = monteCarlo.data;
    if (!result) return {};
    return {
      tooltip: {
        trigger: "axis",
        valueFormatter: (value: number) => money(value),
      },
      legend: { top: 0, right: 0, textStyle: { color: "#8c9aae" } },
      grid: { left: 58, right: 18, top: 45, bottom: 34 },
      xAxis: {
        type: "category",
        data: result.items.map((item) => Number(item.age.toFixed(1))),
        axisLabel: { color: "#748195" },
      },
      yAxis: {
        type: "value",
        axisLabel: {
          color: "#748195",
          formatter: (value: number) => `${Math.round(value / 1000)}k`,
        },
        splitLine: { lineStyle: { color: "rgba(255,255,255,.055)" } },
      },
      series: [
        {
          name: "P10",
          type: "line",
          stack: "band",
          symbol: "none",
          data: result.items.map((item) => item.p10),
          lineStyle: { opacity: 0 },
          areaStyle: { opacity: 0 },
        },
        {
          name: "Zone 80%",
          type: "line",
          stack: "band",
          symbol: "none",
          data: result.items.map((item) => item.p90 - item.p10),
          lineStyle: { opacity: 0 },
          areaStyle: { color: "#72a5ff", opacity: 0.18 },
        },
        {
          name: "Médiane",
          type: "line",
          symbol: "none",
          data: result.items.map((item) => item.p50),
          lineStyle: { color: "#67e8b6", width: 3 },
        },
      ],
    };
  }, [monteCarlo.data]);
  return (
    <>
      <PageHeader
        eyebrow="Planification"
        title="Projections FIRE"
        description="Un espace dédié aux hypothèses, événements de vie, scénarios enregistrés et simulations de risque—sans détourner la navigation globale."
      />
      <div className="fire-layout">
        <Panel
          className="fire-controls"
          title="Hypothèses"
          description="Modifiez librement, puis appliquez les changements au graphique."
        >
          <div className="control-section">
            <h3>Profil</h3>
            <RangeField
              label="Âge actuel"
              value={draft.current_age}
              min={18}
              max={75}
              step={1}
              suffix=" ans"
              onChange={(v) => update("current_age", v)}
            />
            <RangeField
              label="Retraite de référence"
              value={draft.retirement_age}
              min={Math.max(18, draft.current_age)}
              max={90}
              step={1}
              suffix=" ans"
              onChange={(v) => update("retirement_age", v)}
            />
            <RangeField
              label="Horizon"
              value={draft.years}
              min={5}
              max={65}
              step={1}
              suffix=" ans"
              onChange={(v) => update("years", v)}
            />
          </div>
          <div className="control-section">
            <h3>Situation</h3>
            <div className="field">
              <span>Patrimoine actuel</span>
              <input
                className="input"
                type="number"
                value={draft.start_capital}
                onChange={(e) =>
                  update("start_capital", Number(e.target.value))
                }
              />
            </div>
            <div className="field">
              <span>Épargne mensuelle</span>
              <input
                className="input"
                type="number"
                value={draft.monthly_savings}
                onChange={(e) =>
                  update("monthly_savings", Number(e.target.value))
                }
              />
            </div>
            <div className="field">
              <span>Dépenses mensuelles</span>
              <input
                className="input"
                type="number"
                value={draft.monthly_expenses}
                onChange={(e) =>
                  update("monthly_expenses", Number(e.target.value))
                }
              />
            </div>
          </div>
          <div className="control-section">
            <h3>Marché & fiscalité</h3>
            <RangeField
              label="Rendement"
              value={draft.annual_return_rate * 100}
              min={0}
              max={15}
              step={0.1}
              suffix="%"
              onChange={(v) => update("annual_return_rate", v / 100)}
            />
            <RangeField
              label="Inflation"
              value={draft.inflation_rate * 100}
              min={0}
              max={8}
              step={0.1}
              suffix="%"
              onChange={(v) => update("inflation_rate", v / 100)}
            />
            <RangeField
              label="Hausse de l’épargne"
              value={draft.salary_growth_rate * 100}
              min={0}
              max={8}
              step={0.1}
              suffix="%"
              onChange={(v) => update("salary_growth_rate", v / 100)}
            />
            <RangeField
              label="Fiscalité des gains"
              value={draft.tax_rate * 100}
              min={0}
              max={50}
              step={0.5}
              suffix="%"
              onChange={(v) => update("tax_rate", v / 100)}
            />
            <RangeField
              label="Volatilité"
              value={draft.volatility * 100}
              min={1}
              max={40}
              step={1}
              suffix="%"
              onChange={(v) => update("volatility", v / 100)}
            />
          </div>
          <div className="control-section">
            <h3>Arrêt d’activité</h3>
            <label className="field">
              <span>Âge d’arrêt (vide = aucun)</span>
              <input
                className="input"
                type="number"
                min={draft.current_age}
                max={draft.current_age + draft.years}
                value={draft.stop_working_age ?? ""}
                onChange={(e) =>
                  update(
                    "stop_working_age",
                    e.target.value ? Number(e.target.value) : null,
                  )
                }
              />
            </label>
            <label
              style={{
                display: "flex",
                gap: 8,
                alignItems: "center",
                color: "#9ba8ba",
                fontSize: 12,
              }}
            >
              <input
                type="checkbox"
                checked={draft.show_real}
                onChange={(e) => update("show_real", e.target.checked)}
              />
              Afficher en euros constants
            </label>
          </div>
          <button
            className="button button-primary"
            style={{ width: "100%" }}
            onClick={() => {
              setActive(draft);
              monteCarlo.reset();
            }}
          >
            Recalculer
          </button>
          <div className="control-section">
            <h3>Scénario</h3>
            <input
              className="input"
              value={scenarioName}
              onChange={(e) => setScenarioName(e.target.value)}
            />
            <button
              className="button button-secondary"
              disabled={save.isPending || !scenarioName.trim()}
              onClick={() => save.mutate()}
            >
              <Save size={14} />
              Enregistrer
            </button>
            {save.error && (
              <small className="negative">{save.error.message}</small>
            )}
            {scenarios.error && (
              <small className="negative">{scenarios.error.message}</small>
            )}
            <select
              className="select"
              defaultValue=""
              onChange={(e) => {
                const selected = scenarios.data?.find(
                  (item) => item.id === e.target.value,
                );
                if (selected) {
                  setDraft(selected.parameters);
                  setActive(selected.parameters);
                }
              }}
            >
              <option value="">Charger un scénario…</option>
              {scenarios.data?.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.name}
                </option>
              ))}
            </select>
            {removeScenario.error && (
              <small className="negative">{removeScenario.error.message}</small>
            )}
            {scenarios.data?.map((item) => (
              <div
                key={item.id}
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  fontSize: 11,
                  color: "#8c9aae",
                }}
              >
                <span>{item.name}</span>
                <button
                  className="icon-button"
                  onClick={() => removeScenario.mutate(item.id)}
                  aria-label={`Supprimer ${item.name}`}
                >
                  <Trash2 size={13} />
                </button>
              </div>
            ))}
          </div>
        </Panel>
        <div className="stack">
          {projection.isLoading ? (
            <Loading />
          ) : projection.error ? (
            <ErrorBlock error={projection.error} />
          ) : (
            data && (
              <>
                <div
                  className="metrics"
                  style={{ gridTemplateColumns: "repeat(3,minmax(0,1fr))" }}
                >
                  <MetricCard
                    label="Patrimoine final"
                    value={money(data.metrics.final_wealth)}
                    tone="accent"
                  />
                  <MetricCard
                    label="Rente mensuelle à 4%"
                    value={money(data.metrics.monthly_rent_4_percent)}
                  />
                  <MetricCard
                    label="FIRE standard"
                    value={
                      data.metrics.fire_age
                        ? `${data.metrics.fire_age.toFixed(1)} ans`
                        : "Non atteint"
                    }
                    tone={data.metrics.fire_age ? "positive" : "default"}
                  />
                </div>
                <Panel
                  title="Trajectoire de vie"
                  description="Capital net de fiscalité, avec retraits après l’arrêt de travail et événements inclus."
                  action={
                    <div className="filters" style={{ margin: 0 }}>
                      <label style={{ fontSize: 11, color: "#8c9aae" }}>
                        <input
                          type="checkbox"
                          checked={showLean}
                          onChange={(e) => setShowLean(e.target.checked)}
                        />{" "}
                        Lean
                      </label>
                      <label style={{ fontSize: 11, color: "#8c9aae" }}>
                        <input
                          type="checkbox"
                          checked={showFat}
                          onChange={(e) => setShowFat(e.target.checked)}
                        />{" "}
                        Fat
                      </label>
                      <label style={{ fontSize: 11, color: "#8c9aae" }}>
                        <input
                          type="checkbox"
                          checked={showCoast}
                          onChange={(e) => setShowCoast(e.target.checked)}
                        />{" "}
                        Coast
                      </label>
                      <select
                        className="select"
                        style={{ width: 130 }}
                        value={milestones.join(",")}
                        onChange={(e) =>
                          setMilestones(
                            e.target.value
                              ? e.target.value.split(",").map(Number)
                              : [],
                          )
                        }
                      >
                        <option value="">Sans jalon</option>
                        <option value="100000">100k</option>
                        <option value="100000,500000">100k · 500k</option>
                        <option value="100000,500000,1000000">
                          100k · 500k · 1M
                        </option>
                      </select>
                    </div>
                  }
                >
                  <Chart option={chart} height={520} />
                </Panel>
              </>
            )
          )}
          <Panel
            title="Événements de vie"
            description="Apport immobilier, voiture, héritage ou autre impact ponctuel."
          >
            <form className="filters" onSubmit={addEvent}>
              <div className="field field-grow">
                <span>Nom</span>
                <input
                  className="input"
                  required
                  value={event.name}
                  onChange={(e) => setEvent({ ...event, name: e.target.value })}
                />
              </div>
              <div className="field">
                <span>Dans</span>
                <input
                  className="input"
                  type="number"
                  min="0.1"
                  max={draft.years}
                  step="0.1"
                  required
                  value={event.year}
                  onChange={(e) =>
                    setEvent({ ...event, year: Number(e.target.value) })
                  }
                />
              </div>
              <div className="field">
                <span>Montant</span>
                <input
                  className="input"
                  type="number"
                  step="100"
                  required
                  value={event.amount}
                  onChange={(e) =>
                    setEvent({ ...event, amount: Number(e.target.value) })
                  }
                />
              </div>
              <button className="button button-secondary">
                <Plus size={14} />
                Ajouter
              </button>
            </form>
            {draft.life_events.length ? (
              draft.life_events.map((item, index) => (
                <div className="event-row" key={`${item.name}-${index}`}>
                  <strong>{item.name}</strong>
                  <span className="muted">+{item.year} ans</span>
                  <span className={item.amount >= 0 ? "positive" : "negative"}>
                    {money(item.amount)}
                  </span>
                  <button
                    className="icon-button"
                    onClick={() =>
                      update(
                        "life_events",
                        draft.life_events.filter(
                          (_, current) => current !== index,
                        ),
                      )
                    }
                  >
                    <Trash2 size={13} />
                  </button>
                </div>
              ))
            ) : (
              <Empty
                title="Aucun événement"
                description="Le scénario utilise uniquement les flux mensuels."
              />
            )}
          </Panel>
          <Panel
            title="Monte Carlo"
            description="La même trajectoire, avec volatilité, fiscalité, inflation, retraits et événements."
            action={
              <button
                className="button button-secondary"
                onClick={() => monteCarlo.mutate()}
                disabled={monteCarlo.isPending}
              >
                <Dice5 size={15} />
                {monteCarlo.isPending
                  ? "Simulation…"
                  : `${active.simulations} simulations`}
              </button>
            }
          >
            {monteCarlo.error ? (
              <ErrorBlock error={monteCarlo.error} />
            ) : monteCarlo.data ? (
              <>
                <div
                  className="metrics"
                  style={{ gridTemplateColumns: "repeat(3,minmax(0,1fr))" }}
                >
                  <MetricCard
                    label="Probabilité cible FIRE"
                    value={percent(monteCarlo.data.metrics.success_probability)}
                    tone="positive"
                  />
                  <MetricCard
                    label="Final médian"
                    value={money(monteCarlo.data.metrics.median_final)}
                  />
                  <MetricCard
                    label="Scénario prudent P10"
                    value={money(monteCarlo.data.metrics.pessimistic_final)}
                  />
                </div>
                <Chart option={mcChart} height={430} />
              </>
            ) : (
              <Empty
                title="Simulation prête"
                description="Lancez le calcul pour afficher la médiane et la zone P10–P90."
              />
            )}
          </Panel>
        </div>
      </div>
    </>
  );
}
