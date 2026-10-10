import { useEffect, useMemo, useState, type FormEvent } from "react";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Plus, Save, Trash2 } from "lucide-react";
import { api } from "../api";
import Chart from "../components/Chart";
import {
  Empty, ErrorBlock, Loading, MetricCard, money, PageHeader, Panel, percent,
} from "../components/ui";
import {
  initialProjection, normalizeProjection, projectionError, updateProjection, waitForQuiet,
} from "../fire-state";
import {
  monteCarloChart, projectionChart,
  type MonteCarlo, type Projection, type ProjectionResult,
} from "../fire-charts";
import type { LifeEvent, ProjectionInput } from "../types";

type Scenario = { id: string; name: string; parameters: ProjectionInput };

function RangeField({ label, value, min, max, step, suffix, onChange }: {
  label: string; value: number; min: number; max: number; step: number;
  suffix: string; onChange: (value: number) => void;
}) {
  return (
    <div className="range-field">
      <label>{label}</label>
      <output>{value.toLocaleString("fr-FR")}{suffix}</output>
      <input type="range" aria-label={label} value={value} min={min} max={max} step={step} onChange={(event) => onChange(Number(event.target.value))} />
    </div>
  );
}

export default function Fire() {
  const client = useQueryClient();
  const defaults = useQuery({
    queryKey: ["projection-defaults"],
    queryFn: ({ signal }) => api<{ wealth: number; monthly_savings: number; monthly_expenses: number }>("/api/projections/defaults", { signal }),
  });
  const scenarios = useQuery({
    queryKey: ["scenarios"],
    queryFn: ({ signal }) => api<Scenario[]>("/api/scenarios", { signal }),
  });
  const [draft, setDraft] = useState<ProjectionInput>(initialProjection);
  const [defaultsApplied, setDefaultsApplied] = useState(false);
  const [scenarioName, setScenarioName] = useState("Scénario principal");
  const [event, setEvent] = useState<LifeEvent>({ name: "Achat voiture", year: 5, amount: -15000 });
  const [showLean, setShowLean] = useState(true);
  const [showFat, setShowFat] = useState(false);
  const [showCoast, setShowCoast] = useState(false);
  const [milestones, setMilestones] = useState([100000]);
  useEffect(() => {
    // Do not overwrite an edit or loaded scenario if defaults arrive later.
    if (defaultsApplied) return;
    if (defaults.data) {
      setDraft(normalizeProjection({
        ...initialProjection,
        start_capital: Math.max(0, defaults.data.wealth),
        monthly_savings: defaults.data.monthly_savings,
        monthly_expenses: defaults.data.monthly_expenses,
      }));
      setDefaultsApplied(true);
    } else if (defaults.isError) {
      setDefaultsApplied(true);
    }
  }, [defaults.data, defaults.isError, defaultsApplied]);
  const error = projectionError(draft);
  const enabled = defaultsApplied && !error;
  const projection = useQuery({
    queryKey: ["projection", draft],
    queryFn: async ({ signal }): Promise<ProjectionResult<Projection>> => ({
      result: await api<Projection>("/api/projections/calculate", {
        method: "POST", body: JSON.stringify(draft), signal,
      }),
      parameters: draft,
    }),
    enabled,
    placeholderData: keepPreviousData,
    retry: false,
  });
  const monteCarlo = useQuery({
    queryKey: ["monte-carlo", draft],
    queryFn: async ({ signal }): Promise<ProjectionResult<MonteCarlo>> => {
      // Slider drags cancel this wait before an expensive request is sent.
      await waitForQuiet(signal);
      return {
        result: await api<MonteCarlo>("/api/projections/monte-carlo", {
          method: "POST", body: JSON.stringify(draft), signal,
        }),
        parameters: draft,
      };
    },
    enabled,
    placeholderData: keepPreviousData,
    retry: false,
  });
  const save = useMutation({
    mutationFn: () => api("/api/scenarios", {
      method: "POST", body: JSON.stringify({ name: scenarioName, parameters: draft }),
    }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["scenarios"] }),
  });
  const removeScenario = useMutation({
    mutationFn: (id: string) => api(`/api/scenarios/${id}`, { method: "DELETE" }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["scenarios"] }),
  });
  const update = <K extends keyof ProjectionInput>(key: K, value: ProjectionInput[K]) => {
    setDefaultsApplied(true);
    setDraft((current) => updateProjection(current, key, value));
  };
  const addEvent = (e: FormEvent) => {
    e.preventDefault();
    update("life_events", [...draft.life_events, { ...event, name: event.name.trim() }]);
  };
  const data = projection.data?.result;
  const mc = monteCarlo.data?.result;
  const chart = useMemo(() => projection.data ? projectionChart(projection.data, showLean, showFat, showCoast, milestones) : {}, [projection.data, showLean, showFat, showCoast, milestones]);
  const mcChart = useMemo(() => monteCarlo.data ? monteCarloChart(monteCarlo.data) : {}, [monteCarlo.data]);
  return (
    <>
      <PageHeader eyebrow="Planification" title="Projections FIRE" description="Hypothèses, événements de vie et simulations actualisés automatiquement." />
      <div className="fire-layout">
        <Panel className="fire-controls" title="Hypothèses" description="Chaque modification met à jour les projections.">
          <div className="control-section">
            <h3>Profil</h3>
            <RangeField label="Âge actuel" value={draft.current_age} min={18} max={75} step={1} suffix=" ans" onChange={(value) => update("current_age", value)} />
            <RangeField label="Retraite de référence" value={draft.retirement_age} min={draft.current_age} max={90} step={1} suffix=" ans" onChange={(value) => update("retirement_age", value)} />
            <RangeField label="Horizon" value={draft.years} min={5} max={65} step={1} suffix=" ans" onChange={(value) => update("years", value)} />
          </div>
          <div className="control-section">
            <h3>Situation</h3>
            {([
              ["start_capital", "Patrimoine actuel"],
              ["monthly_savings", "Épargne mensuelle"],
              ["monthly_expenses", "Dépenses mensuelles"],
            ] as const).map(([key, label]) => (
              <label className="field" key={key}>
                <span>{label}</span>
                <input className="input" type="number" min="0" step="0.01" value={draft[key]} onChange={(e) => update(key, Number(e.target.value))} />
              </label>
            ))}
          </div>
          <div className="control-section">
            <h3>Marché & fiscalité</h3>
            {([
              ["annual_return_rate", "Rendement", 0, 15, 0.1],
              ["inflation_rate", "Inflation", 0, 8, 0.1],
              ["salary_growth_rate", "Hausse de l’épargne", 0, 8, 0.1],
              ["tax_rate", "Fiscalité des gains", 0, 50, 0.5],
              ["volatility", "Volatilité", 1, 40, 1],
            ] as const).map(([key, label, min, max, step]) => (
              <RangeField key={key} label={label} value={draft[key] * 100} min={min} max={max} step={step} suffix="%" onChange={(value) => update(key, value / 100)} />
            ))}
          </div>
          <div className="control-section">
            <h3>Arrêt d’activité</h3>
            <label className="field">
              <span>Âge d’arrêt (vide = aucun)</span>
              <input className="input" type="number" min={draft.current_age} max={draft.current_age + draft.years} step="1" value={draft.stop_working_age ?? ""} onChange={(e) => update("stop_working_age", e.target.value ? Number(e.target.value) : null)} />
            </label>
            <label>
              <input type="checkbox" checked={draft.show_real} onChange={(e) => update("show_real", e.target.checked)} />{" "}
              Afficher en euros constants
            </label>
          </div>
          {error && <p role="alert" className="negative">{error}</p>}
          {defaults.error && <p className="muted">Les valeurs initiales n’ont pas pu être chargées. Saisissez-les ci-dessus.</p>}
          <div className="control-section">
            <h3>Scénario</h3>
            <input className="input" aria-label="Nom du scénario" value={scenarioName} onChange={(e) => setScenarioName(e.target.value)} />
            <button className="button button-secondary" disabled={save.isPending || !scenarioName.trim() || Boolean(error)} onClick={() => save.mutate()}>
              <Save size={14} /> Enregistrer
            </button>
            {save.error && <small className="negative">{save.error.message}</small>}
            {scenarios.error && <small className="negative">{scenarios.error.message}</small>}
            <select className="select" aria-label="Charger un scénario" value="" onChange={(e) => {
              const selected = scenarios.data?.find((item) => item.id === e.target.value);
              if (selected) {
                setDefaultsApplied(true);
                setDraft(normalizeProjection(selected.parameters));
                setScenarioName(selected.name);
              }
            }}>
              <option value="">Charger un scénario…</option>
              {scenarios.data?.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
            </select>
            {removeScenario.error && <small className="negative">{removeScenario.error.message}</small>}
            {scenarios.data?.map((item) => (
              <div className="event-row" key={item.id}>
                <span>{item.name}</span>
                <button className="icon-button" onClick={() => removeScenario.mutate(item.id)} aria-label={`Supprimer ${item.name}`}><Trash2 size={13} /></button>
              </div>
            ))}
          </div>
        </Panel>
        <div className="stack">
          <div role="status" aria-live="polite" className="muted">
            {error ? "Corrigez les hypothèses pour actualiser les projections." : projection.error ? "Calcul indisponible." : projection.isFetching ? "Mise à jour de la trajectoire…" : "Trajectoire à jour"}
          </div>
          {projection.error && <ErrorBlock error={projection.error} />}
          {!data && !error && !projection.error && <Loading label="Calcul de la trajectoire…" />}
          {data && (
            <>
              <div className="metrics" style={{ gridTemplateColumns: "repeat(3,minmax(0,1fr))" }}>
                <MetricCard label="Patrimoine final" value={money(data.metrics.final_wealth)} tone="accent" />
                <MetricCard label="Rente mensuelle à 4%" value={money(data.metrics.monthly_rent_4_percent)} />
                <MetricCard label="FIRE standard" value={data.metrics.fire_age !== null ? `${data.metrics.fire_age.toFixed(1)} ans` : "Non atteint"} tone={data.metrics.fire_age !== null ? "positive" : "default"} />
              </div>
              <Panel title="Trajectoire de vie" description="Capital net de fiscalité, retraits et événements inclus." action={
                <div className="filters" style={{ margin: 0 }}>
                  <label><input type="checkbox" checked={showLean} onChange={(e) => setShowLean(e.target.checked)} /> Lean</label>
                  <label><input type="checkbox" checked={showFat} onChange={(e) => setShowFat(e.target.checked)} /> Fat</label>
                  <label><input type="checkbox" checked={showCoast} onChange={(e) => setShowCoast(e.target.checked)} /> Coast</label>
                  <select className="select" aria-label="Jalons" style={{ width: 150 }} value={milestones.join(",")} onChange={(e) => setMilestones(e.target.value ? e.target.value.split(",").map(Number) : [])}>
                    <option value="">Sans jalon</option>
                    <option value="100000">100k</option>
                    <option value="100000,500000">100k · 500k</option>
                    <option value="100000,500000,1000000">100k · 500k · 1M</option>
                  </select>
                </div>
              }>
                <Chart option={chart} height={520} />
              </Panel>
            </>
          )}
          <Panel title="Événements de vie" description="Apport immobilier, voiture, héritage ou autre impact ponctuel.">
            <form className="filters" onSubmit={addEvent}>
              <label className="field field-grow"><span>Nom</span><input className="input" required maxLength={120} value={event.name} onChange={(e) => setEvent({ ...event, name: e.target.value })} /></label>
              <label className="field"><span>Dans (années)</span><input className="input" type="number" min="0.1" max={draft.years} step="0.1" required value={event.year} onChange={(e) => setEvent({ ...event, year: Number(e.target.value) })} /></label>
              <label className="field"><span>Montant</span><input className="input" type="number" step="0.01" required value={event.amount} onChange={(e) => setEvent({ ...event, amount: Number(e.target.value) })} /></label>
              <button className="button button-secondary" disabled={!event.name.trim()}><Plus size={14} /> Ajouter</button>
            </form>
            {draft.life_events.length ? draft.life_events.map((item, index) => (
              <div className="event-row" key={`${item.name}-${index}`}>
                <strong>{item.name}</strong><span className="muted">+{item.year} ans</span>
                <span className={item.amount >= 0 ? "positive" : "negative"}>{money(item.amount)}</span>
                <button className="icon-button" aria-label={`Supprimer ${item.name}`} onClick={() => update("life_events", draft.life_events.filter((_, current) => current !== index))}><Trash2 size={13} /></button>
              </div>
            )) : <Empty title="Aucun événement" description="Le scénario utilise uniquement les flux mensuels." />}
          </Panel>
          <Panel title="Monte Carlo" description={`${draft.simulations} simulations automatiques avec volatilité, fiscalité, inflation, retraits et événements.`}>
            <div role="status" aria-live="polite" className="muted">
              {error ? "En attente d’hypothèses valides." : monteCarlo.error ? "Simulation indisponible." : monteCarlo.isFetching ? "Mise à jour des simulations…" : "Simulations à jour"}
            </div>
            {monteCarlo.error && <ErrorBlock error={monteCarlo.error} />}
            {!mc && !error && !monteCarlo.error && <Loading label="Simulation…" />}
            {mc && <>
              <div className="metrics" style={{ gridTemplateColumns: "repeat(3,minmax(0,1fr))" }}>
                <MetricCard label="Probabilité cible FIRE" value={percent(mc.metrics.success_probability)} tone="positive" />
                <MetricCard label="Final médian" value={money(mc.metrics.median_final)} />
                <MetricCard label="Scénario prudent P10" value={money(mc.metrics.pessimistic_final)} />
              </div>
              <Chart option={mcChart} height={430} />
            </>}
          </Panel>
        </div>
      </div>
    </>
  );
}
