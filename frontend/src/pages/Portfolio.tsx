import { useDeferredValue, useMemo, useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Download,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  Trash2,
} from "lucide-react";
import { api, download, searchParams } from "../api";
import Chart from "../components/Chart";
import {
  Empty,
  ErrorBlock,
  Loading,
  MetricCard,
  Modal,
  money,
  PageHeader,
  Pagination,
  Panel,
  percent,
  shortDate,
} from "../components/ui";
import type { Account, DateRangeValue, Trade } from "../types";

type Summary = {
  net_invested: number;
  current_value: number;
  pnl: number;
  pnl_percent: number | null;
  total_wealth: number;
  market_data: { fetched_at: string | null; latest_price_date: string | null };
};
type Evolution = {
  accounts: string[];
  items: {
    date: string;
    total_wealth: number;
    total_investment: number;
    accounts: Record<string, number>;
  }[];
};
type Trades = {
  items: Trade[];
  total: number;
  page_size: number;
  filters: { accounts: string[]; tickers: string[] };
};

const blankTrade = (account = "") => ({
  date: new Date().toISOString().slice(0, 10),
  ticker: "",
  name: "",
  action: "BUY" as const,
  quantity: 0,
  unit_price: 0,
  fees: 0,
  currency: "EUR",
  account,
  comment: "",
});

function TradeForm({
  trade,
  accounts,
  onClose,
  onSaved,
}: {
  trade: Trade | null;
  accounts: Account[];
  onClose: () => void;
  onSaved: () => void;
}) {
  const client = useQueryClient();
  const [form, setForm] = useState(() =>
    trade ? { ...trade } : blankTrade(accounts[0]?.name),
  );
  const mutation = useMutation({
    mutationFn: () =>
      trade
        ? api(`/api/investments/${trade.id}`, {
            method: "PUT",
            body: JSON.stringify({ ...form, revision: trade.revision }),
          })
        : api("/api/investments", {
            method: "POST",
            body: JSON.stringify(form),
          }),
    onSuccess: async () => {
      await Promise.all([
        client.invalidateQueries({ queryKey: ["trades"] }),
        client.invalidateQueries({ queryKey: ["portfolio-summary"] }),
        client.invalidateQueries({ queryKey: ["portfolio-evolution"] }),
        client.invalidateQueries({ queryKey: ["allocation"] }),
      ]);
      onClose();
      onSaved();
    },
  });
  const set = (key: string, value: string | number) =>
    setForm((current) => ({ ...current, [key]: value }));
  const submit = (event: FormEvent) => {
    event.preventDefault();
    mutation.mutate();
  };
  return (
    <form onSubmit={submit}>
      <div className="form-grid">
        <div className="field">
          <span>Date d’exécution</span>
          <input
            className="input"
            type="date"
            required
            value={form.date}
            onChange={(e) => set("date", e.target.value)}
          />
        </div>
        <div className="field">
          <span>Opération</span>
          <select
            className="select"
            value={form.action}
            onChange={(e) => set("action", e.target.value)}
          >
            <option value="BUY">Achat</option>
            <option value="SELL">Vente</option>
          </select>
        </div>
        <div className="field">
          <span>Ticker</span>
          <input
            className="input"
            required
            value={form.ticker}
            onChange={(e) => set("ticker", e.target.value.toUpperCase())}
            placeholder="CW8.PA"
          />
          <small className="field-help">
            Symbole Yahoo Finance complet, suffixe de place inclus (ex. CW8.PA).
          </small>
        </div>
        <div className="field">
          <span>Nom du produit</span>
          <input
            className="input"
            required
            value={form.name}
            onChange={(e) => set("name", e.target.value)}
            placeholder="Amundi MSCI World"
          />
        </div>
        <div className="field">
          <span>Quantité</span>
          <input
            className="input"
            type="number"
            min="0.000001"
            step="any"
            required
            value={form.quantity}
            onChange={(e) => set("quantity", Number(e.target.value))}
          />
        </div>
        <div className="field">
          <span>Prix unitaire</span>
          <input
            className="input"
            type="number"
            min="0"
            step="any"
            required
            value={form.unit_price}
            onChange={(e) => set("unit_price", Number(e.target.value))}
          />
        </div>
        <div className="field">
          <span>Frais</span>
          <input
            className="input"
            type="number"
            min="0"
            step="any"
            value={form.fees}
            onChange={(e) => set("fees", Number(e.target.value))}
          />
        </div>
        <div className="field">
          <span>Devise de valorisation</span>
          <input className="input" readOnly value={form.currency} />
        </div>
        <div className="field field-full">
          <span>Compte impacté</span>
          <select
            className="select"
            required
            value={form.account}
            onChange={(e) => set("account", e.target.value)}
          >
            <option value="" disabled>
              Choisir un compte
            </option>
            {accounts
              .filter((item) => item.is_visible)
              .map((item) => (
                <option key={item.name}>{item.name}</option>
              ))}
          </select>
        </div>
        <div className="field field-full">
          <span>Commentaire</span>
          <textarea
            className="textarea"
            value={form.comment}
            onChange={(e) => set("comment", e.target.value)}
            placeholder="Optionnel"
          />
        </div>
      </div>
      {mutation.error && (
        <div className="callout callout-warning" style={{ marginTop: 14 }}>
          {mutation.error.message}
        </div>
      )}
      <div className="form-actions">
        <button className="button button-ghost" type="button" onClick={onClose}>
          Annuler
        </button>
        <button className="button button-primary" disabled={mutation.isPending}>
          {mutation.isPending
            ? "Enregistrement…"
            : trade
              ? "Mettre à jour"
              : "Enregistrer"}
        </button>
      </div>
    </form>
  );
}

export default function Portfolio({ range }: { range: DateRangeValue }) {
  const client = useQueryClient();
  const [mode, setMode] = useState<"total" | "accounts">("total");
  const [q, setQ] = useState("");
  const [action, setAction] = useState("");
  const [account, setAccount] = useState("");
  const [ticker, setTicker] = useState("");
  const [page, setPage] = useState(1);
  const [editing, setEditing] = useState<Trade | "new" | null>(null);
  const [exportError, setExportError] = useState<string | null>(null);
  const deferredQuery = useDeferredValue(q);
  const summary = useQuery({
    queryKey: ["portfolio-summary"],
    queryFn: () => api<Summary>("/api/portfolio/summary"),
  });
  const evolution = useQuery({
    queryKey: ["portfolio-evolution", range],
    queryFn: () =>
      api<Evolution>(`/api/portfolio/evolution${searchParams(range)}`),
  });
  const accounts = useQuery({
    queryKey: ["accounts"],
    queryFn: () => api<Account[]>("/api/accounts"),
  });
  const tradeParams = {
    ...range,
    q: deferredQuery,
    action,
    account,
    ticker,
    page,
    page_size: 30,
  };
  const trades = useQuery({
    queryKey: ["trades", tradeParams],
    queryFn: () => api<Trades>(`/api/investments${searchParams(tradeParams)}`),
  });
  const refresh = useMutation({
    mutationFn: () =>
      api<{ updated: Record<string, number>; errors: Record<string, string> }>(
        "/api/market-data/refresh",
        { method: "POST" },
      ),
    onSuccess: async () => {
      await Promise.all([
        client.invalidateQueries({ queryKey: ["portfolio-summary"] }),
        client.invalidateQueries({ queryKey: ["portfolio-evolution"] }),
        client.invalidateQueries({ queryKey: ["allocation"] }),
      ]);
    },
  });
  const remove = useMutation({
    mutationFn: (trade: Trade) =>
      api(`/api/investments/${trade.id}?revision=${trade.revision}`, {
        method: "DELETE",
      }),
    onSuccess: async () => {
      await Promise.all([
        client.invalidateQueries({ queryKey: ["trades"] }),
        client.invalidateQueries({ queryKey: ["portfolio-summary"] }),
        client.invalidateQueries({ queryKey: ["portfolio-evolution"] }),
        client.invalidateQueries({ queryKey: ["allocation"] }),
      ]);
    },
  });

  const chart = useMemo(() => {
    const items = evolution.data?.items ?? [];
    const common = {
      type: "line",
      symbol: "none",
      smooth: 0.18,
      emphasis: { focus: "series" },
      areaStyle: { opacity: 0.04 },
    };
    const series =
      mode === "total"
        ? [
            {
              ...common,
              name: "Patrimoine net",
              data: items.map((item) => item.total_wealth),
              lineStyle: { color: "#67e8b6", width: 3 },
              areaStyle: { color: "#67e8b6", opacity: 0.08 },
            },
            {
              ...common,
              name: "Dont investissements",
              data: items.map((item) => item.total_investment),
              lineStyle: { color: "#f5c66e", width: 2, type: "dashed" },
              areaStyle: { opacity: 0 },
            },
          ]
        : (evolution.data?.accounts ?? []).map((name, index) => ({
            ...common,
            name,
            stack: "accounts",
            data: items.map((item) => item.accounts[name] ?? 0),
            lineStyle: { width: 1.5 },
            areaStyle: { opacity: 0.22 },
            color: ["#67e8b6", "#72a5ff", "#f5c66e", "#b99cff", "#5bd7e8"][
              index % 5
            ],
          }));
    return {
      tooltip: {
        trigger: "axis",
        valueFormatter: (value: number) => money(value, "EUR", 2),
      },
      legend: { top: 0, right: 0, textStyle: { color: "#8c9aae" } },
      grid: { left: 54, right: 18, top: 46, bottom: 32 },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: items.map((item) => item.date),
        axisLabel: {
          color: "#748195",
          formatter: (value: string) => value.slice(5),
        },
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
  }, [evolution.data, mode]);

  const doExport = async (format: "csv" | "xlsx") => {
    setExportError(null);
    try {
      await download(
        `/api/investments/export${searchParams({ ...tradeParams, page: undefined, page_size: undefined, format })}`,
        `investments.${format}`,
      );
    } catch (error) {
      setExportError(
        error instanceof Error ? error.message : "L’export a échoué",
      );
    }
  };
  return (
    <>
      <PageHeader
        eyebrow="Patrimoine"
        title="Portfolio"
        description="Valeur nette, historique des comptes et registre complet de vos achats et ventes."
        actions={
          <>
            <button
              className="button button-secondary"
              onClick={() => refresh.mutate()}
              disabled={refresh.isPending}
            >
              <RefreshCw
                size={15}
                className={refresh.isPending ? "spin" : ""}
              />
              Actualiser les cours
            </button>
            <button
              className="button button-primary"
              onClick={() => setEditing("new")}
            >
              <Plus size={16} />
              Nouvelle opération
            </button>
          </>
        }
      />
      {summary.isLoading ? (
        <Loading />
      ) : summary.error ? (
        <ErrorBlock error={summary.error} />
      ) : (
        <div className="metrics">
          <MetricCard
            label="Capital net investi"
            value={money(summary.data!.net_invested)}
          />
          <MetricCard
            label="Valeur des actifs"
            value={money(summary.data!.current_value)}
            tone="accent"
          />
          <MetricCard
            label="Plus / moins-value"
            value={money(summary.data!.pnl)}
            tone={summary.data!.pnl >= 0 ? "positive" : "negative"}
            note={percent(summary.data!.pnl_percent)}
          />
          <MetricCard
            label="Patrimoine total"
            value={money(summary.data!.total_wealth)}
            note={
              summary.data!.market_data.latest_price_date
                ? `Cours au ${shortDate(summary.data!.market_data.latest_price_date)}`
                : "Cours d’achat utilisés par défaut"
            }
          />
        </div>
      )}
      {refresh.error && (
        <div className="callout callout-warning" style={{ marginBottom: 15 }}>
          {refresh.error.message}
        </div>
      )}
      {refresh.data && Object.keys(refresh.data.errors).length > 0 && (
        <div className="callout callout-warning" style={{ marginBottom: 15 }}>
          Cours non actualisés :{" "}
          {Object.entries(refresh.data.errors)
            .map(([name, message]) => `${name} (${message})`)
            .join(", ")}
        </div>
      )}
      {remove.error && (
        <div className="callout callout-warning" style={{ marginBottom: 15 }}>
          {remove.error.message}
        </div>
      )}
      {exportError && (
        <div className="callout callout-warning" style={{ marginBottom: 15 }}>
          {exportError}
        </div>
      )}
      <Panel
        title="Évolution du patrimoine"
        description="Les achats et ventes déplacent le cash vers les actifs sans créer de faux revenu ou dépense."
        action={
          <div className="segmented">
            <button
              className={mode === "total" ? "active" : ""}
              onClick={() => setMode("total")}
            >
              Total
            </button>
            <button
              className={mode === "accounts" ? "active" : ""}
              onClick={() => setMode("accounts")}
            >
              Par compte
            </button>
          </div>
        }
      >
        {evolution.isLoading ? (
          <Loading />
        ) : evolution.error ? (
          <ErrorBlock error={evolution.error} />
        ) : evolution.data!.items.length ? (
          <Chart option={chart} height={470} />
        ) : (
          <Empty
            title="Pas encore d’historique"
            description="Ajoutez un compte ou importez des transactions pour commencer."
          />
        )}
      </Panel>
      <Panel
        className="panel-flush"
        title="Historique boursier"
        description="Filtrez, corrigez et exportez les opérations enregistrées dans Local Finance."
        action={
          <>
            <button
              className="button button-ghost"
              onClick={() => doExport("csv")}
            >
              <Download size={14} />
              CSV
            </button>
            <button
              className="button button-ghost"
              onClick={() => doExport("xlsx")}
            >
              <Download size={14} />
              Excel
            </button>
          </>
        }
      >
        <div className="filters" style={{ padding: "8px 21px 6px" }}>
          <div className="field field-grow">
            <span>Recherche</span>
            <div style={{ position: "relative" }}>
              <Search
                size={15}
                style={{
                  position: "absolute",
                  left: 11,
                  top: 12,
                  color: "#687588",
                }}
              />
              <input
                className="input"
                style={{ paddingLeft: 34 }}
                placeholder="Date, ticker, nom, montant, compte ou note…"
                value={q}
                onChange={(e) => {
                  setQ(e.target.value);
                  setPage(1);
                }}
              />
            </div>
          </div>
          <div className="field">
            <span>Opération</span>
            <select
              className="select"
              value={action}
              onChange={(e) => {
                setAction(e.target.value);
                setPage(1);
              }}
            >
              <option value="">Toutes</option>
              <option value="BUY">Achats</option>
              <option value="SELL">Ventes</option>
            </select>
          </div>
          <div className="field">
            <span>Compte</span>
            <select
              className="select"
              value={account}
              onChange={(e) => {
                setAccount(e.target.value);
                setPage(1);
              }}
            >
              <option value="">Tous</option>
              {trades.data?.filters.accounts.map((value) => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </div>
          <div className="field">
            <span>Ticker</span>
            <select
              className="select"
              value={ticker}
              onChange={(e) => {
                setTicker(e.target.value);
                setPage(1);
              }}
            >
              <option value="">Tous</option>
              {trades.data?.filters.tickers.map((value) => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </div>
        </div>
        {trades.isLoading ? (
          <Loading />
        ) : trades.error ? (
          <ErrorBlock error={trades.error} />
        ) : trades.data!.items.length === 0 ? (
          <Empty
            title="Aucune opération"
            description="Ajoutez votre premier achat ou élargissez les filtres."
          />
        ) : (
          <>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Type</th>
                    <th>Ticker</th>
                    <th>Produit</th>
                    <th className="cell-number">Quantité</th>
                    <th className="cell-number">Prix</th>
                    <th className="cell-number">Frais</th>
                    <th>Compte</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {trades.data!.items.map((trade) => (
                    <tr key={trade.id}>
                      <td>{shortDate(trade.date)}</td>
                      <td>
                        <span
                          className={`badge badge-${trade.action.toLowerCase()}`}
                        >
                          {trade.action === "BUY" ? "Achat" : "Vente"}
                        </span>
                      </td>
                      <td>
                        <strong>{trade.ticker}</strong>
                      </td>
                      <td>{trade.name}</td>
                      <td className="cell-number">
                        {trade.quantity.toLocaleString("fr-FR")}
                      </td>
                      <td className="cell-number">
                        {money(trade.unit_price, trade.currency, 2)}
                      </td>
                      <td className="cell-number">
                        {money(trade.fees, trade.currency, 2)}
                      </td>
                      <td>{trade.account}</td>
                      <td>
                        <div className="row-actions">
                          <button
                            className="icon-button"
                            aria-label="Modifier"
                            onClick={() => setEditing(trade)}
                          >
                            <Pencil size={14} />
                          </button>
                          <button
                            className="icon-button"
                            aria-label="Supprimer"
                            onClick={() => {
                              if (
                                confirm(
                                  `Supprimer l’opération ${trade.ticker} du ${trade.date} ?`,
                                )
                              )
                                remove.mutate(trade);
                            }}
                          >
                            <Trash2 size={14} />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pagination
              page={page}
              pageSize={trades.data!.page_size}
              total={trades.data!.total}
              onPage={setPage}
            />
          </>
        )}
      </Panel>
      {editing && (
        <Modal
          title={
            editing === "new"
              ? "Nouvelle opération boursière"
              : `Modifier ${editing.ticker}`
          }
          onClose={() => setEditing(null)}
        >
          <TradeForm
            trade={editing === "new" ? null : editing}
            accounts={accounts.data ?? []}
            onClose={() => setEditing(null)}
            onSaved={() => refresh.mutate()}
          />
        </Modal>
      )}
    </>
  );
}
