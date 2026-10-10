import { useEffect, useRef, useState, type DragEvent } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { Upload, X } from "lucide-react";
import { api } from "../api";
import { MAX_IMPORT_BYTES, readCsvFile, validateCsvText } from "../imports";
import { ErrorBlock, Loading, money, number } from "./ui";
import type { Account } from "../types";
import "./ImportDialog.css";

type Source = {
  id: string;
  label: string;
  mode: "csv" | "existing";
  path?: string;
};
type ImportRow = {
  line: number;
  raw_date: string;
  date: string;
  description: string;
  ticker: string;
  kind: string;
  quantity: string;
  unit_price: string;
  fees: string;
  net: string;
  status: "add" | "duplicate" | "link" | "ignored" | "error";
};
type Summary = {
  add: number;
  duplicate: number;
  link: number;
  ignored: number;
  error: number;
  fees: string;
  cash_change: string;
};
type Suggestion = {
  ticker: string;
  isin: string;
  name: string;
  note: string;
  evidence_url: string;
  currency: string;
  exchange: string;
};
type Preview = {
  id: string;
  rows: ImportRow[];
  errors: string[];
  warnings: string[];
  instruments: string[];
  suggestions: Record<string, Suggestion[]>;
  mappings: Record<string, string>;
  summary: Summary;
};
const statusNames = {
  add: "À ajouter",
  duplicate: "Déjà importée",
  link: "Opération existante",
  ignored: "Ignorée (Android)",
  error: "À corriger",
};

export default function ImportDialog({ onClose }: { onClose: () => void }) {
  const client = useQueryClient();
  const dialog = useRef<HTMLDialogElement>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const latestPreview = useRef<string | null>(null);
  const mounted = useRef(true);
  const [source, setSource] = useState("bourse-direct");
  const [account, setAccount] = useState("");
  const [dateFormat, setDateFormat] = useState("");
  const [deposits, setDeposits] = useState("android");
  const [text, setText] = useState("");
  const [filename, setFilename] = useState("texte.csv");
  const [mappings, setMappings] = useState<Record<string, string>>({});
  const [suggestions, setSuggestions] = useState<Record<string, Suggestion[]>>(
    {},
  );
  const [preview, setPreview] = useState<Preview | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [result, setResult] = useState<Summary | null>(null);
  const [page, setPage] = useState(1);
  const sources = useQuery({
    queryKey: ["import-sources"],
    queryFn: () => api<Source[]>("/api/broker-imports/sources"),
  });
  const accounts = useQuery({
    queryKey: ["accounts"],
    queryFn: () => api<Account[]>("/api/accounts"),
  });
  const selected = sources.data?.find((item) => item.id === source);

  const discardPreview = () => {
    if (latestPreview.current) {
      void api(`/api/broker-imports/${latestPreview.current}`, {
        method: "DELETE",
      }).catch(() => undefined);
      latestPreview.current = null;
    }
  };
  useEffect(() => {
    mounted.current = true;
    const previousFocus = document.activeElement;
    const previousOverflow = document.body.style.overflow;
    const element = dialog.current;
    document.body.style.overflow = "hidden";
    element?.showModal();
    return () => {
      mounted.current = false;
      document.body.style.overflow = previousOverflow;
      element?.close();
      if (previousFocus instanceof HTMLElement) previousFocus.focus();
    };
  }, []);

  const changed = () => {
    discardPreview();
    setPreview(null);
    setError(null);
    setResult(null);
    setPage(1);
  };
  const close = () => {
    if (busy) return;
    discardPreview();
    onClose();
  };
  const loadFile = async (file: File) => {
    changed();
    setBusy(true);
    setText("");
    try {
      const csv = await readCsvFile(file);
      if (mounted.current) {
        setText(csv);
        setFilename(file.name);
      }
    } catch (cause) {
      setError(cause);
    } finally {
      setBusy(false);
    }
  };
  const drop = (event: DragEvent) => {
    event.preventDefault();
    if (busy || selected?.mode !== "csv") return;
    if (event.dataTransfer.files.length > 1) {
      changed();
      setText("");
      setError(new Error("Déposer un seul fichier à la fois."));
      return;
    }
    const file = event.dataTransfer.files[0];
    if (file) {
      void loadFile(file);
    } else {
      changed();
      try {
        setText(validateCsvText(event.dataTransfer.getData("text/plain")));
        setFilename("texte.csv");
      } catch (cause) {
        setText("");
        setError(cause);
      }
    }
  };
  const compare = async () => {
    changed();
    setBusy(true);
    try {
      const value = await api<Preview>("/api/broker-imports/preview", {
        method: "POST",
        body: JSON.stringify({
          source,
          account,
          date_format: dateFormat,
          deposit_policy: deposits,
          csv_text: validateCsvText(text),
          filename,
          mappings,
        }),
      });
      latestPreview.current = value.id;
      setPreview(value);
      setMappings(value.mappings);
      setSuggestions(value.suggestions);
    } catch (cause) {
      setError(cause);
    } finally {
      setBusy(false);
    }
  };
  const apply = async () => {
    if (!preview) return;
    setBusy(true);
    setError(null);
    try {
      const value = await api<{ summary: Summary }>(
        `/api/broker-imports/${preview.id}/apply`,
        { method: "POST" },
      );
      latestPreview.current = null;
      setResult(value.summary);
      setPreview(null);
      await client.invalidateQueries();
    } catch (cause) {
      setError(cause);
    } finally {
      setBusy(false);
    }
  };

  return (
    <dialog
      ref={dialog}
      className="broker-dialog"
      aria-labelledby="broker-import-title"
      onCancel={(event) => {
        event.preventDefault();
        close();
      }}
    >
      <div className="modal-heading">
        <h2 id="broker-import-title">Importer des opérations</h2>
        <button
          type="button"
          className="icon-button"
          disabled={busy}
          onClick={close}
          aria-label="Fermer"
        >
          <X size={20} />
        </button>
      </div>
      <p className="broker-help">
        Comparer avant d'ajouter. Aucun import ne supprime vos opérations
        existantes.
      </p>
      {sources.isPending || accounts.isPending ? <Loading /> : null}
      {sources.error && <ErrorBlock error={sources.error} />}
      {accounts.error && <ErrorBlock error={accounts.error} />}
      <fieldset disabled={busy} className="broker-fields">
        <label className="field">
          <span>Source de l'import</span>
          <select
            className="select"
            value={source}
            onChange={(event) => {
              changed();
              setSource(event.target.value);
              setMappings({});
              setSuggestions({});
              setText("");
              setFilename("texte.csv");
              setDateFormat("");
            }}
          >
            {sources.data?.map((item) => (
              <option key={item.id} value={item.id}>
                {item.label}
              </option>
            ))}
          </select>
        </label>
        {selected?.mode === "existing" ? (
          <div className="callout">
            <p>
              L'import Excel Android conserve son aperçu et ses règles de
              synchronisation.
            </p>
            <Link
              className="button button-primary"
              to={selected.path ?? "/donnees"}
              onClick={close}
            >
              Ouvrir l'import Excel
            </Link>
          </div>
        ) : selected?.mode === "csv" ? (
          <>
            <div className="broker-grid">
              <label className="field">
                <span>Compte destinataire</span>
                <select
                  className="select"
                  value={account}
                  onChange={(event) => {
                    changed();
                    setAccount(event.target.value);
                    setMappings({});
                    setSuggestions({});
                  }}
                >
                  <option value="">Choisir un compte</option>
                  {accounts.data?.map((item) => (
                    <option key={item.name} value={item.name}>
                      {item.name}
                      {item.is_visible ? "" : " (masqué)"}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span>Format des dates du CSV</span>
                <select
                  className="select"
                  value={dateFormat}
                  onChange={(event) => {
                    changed();
                    setDateFormat(event.target.value);
                  }}
                >
                  <option value="">Choisir explicitement</option>
                  <option value="dmy">JJ/MM/AAAA (15/02/2025)</option>
                  <option value="mdy">MM/JJ/AAAA (2/15/2025)</option>
                </select>
              </label>
            </div>
            <label className="field">
              <span>Versements en espèces</span>
              <select
                className="select"
                value={deposits}
                onChange={(event) => {
                  changed();
                  setDeposits(event.target.value);
                }}
              >
                <option value="android">
                  Déjà gérés par Android / le solde initial : ne pas ajouter
                </option>
                <option value="external">
                  Apports depuis un compte non suivi : créditer les liquidités
                </option>
              </select>
            </label>
            <p className="broker-help">
              Ne pas compter deux fois les versements. Les remboursements de
              frais sont ajoutés aux liquidités, jamais aux revenus du budget.
              Les titres doivent être cotés en EUR.
            </p>
            <div
              className="broker-drop"
              onDragOver={(event) => event.preventDefault()}
              onDrop={drop}
            >
              <Upload size={22} aria-hidden="true" />
              <strong>Déposer un fichier CSV ou une sélection de texte</strong>
              <button
                type="button"
                className="button button-ghost"
                onClick={() => fileInput.current?.click()}
              >
                Choisir un fichier
              </button>
              <input
                hidden
                ref={fileInput}
                type="file"
                accept=".csv,.tsv,.txt"
                onChange={(event) => {
                  const file = event.target.files?.[0];
                  event.target.value = "";
                  if (file) void loadFile(file);
                }}
              />
              <small>CSV, TSV, TXT · 5 Mo maximum · {filename}</small>
            </div>
            <label className="field">
              <span>Ou coller le CSV, avec son en-tête</span>
              <textarea
                className="textarea broker-text"
                value={text}
                spellCheck={false}
                onChange={(event) => {
                  changed();
                  if (event.target.value.length > MAX_IMPORT_BYTES) {
                    setText("");
                    setError(new Error("Le texte dépasse 5 Mo."));
                    return;
                  }
                  setText(event.target.value);
                  setFilename("texte.csv");
                }}
              />
            </label>
            {Object.keys(mappings).length > 0 && (
              <section
                className="broker-mappings"
                aria-label="Associations de titres"
              >
                <h3>
                  Associer les désignations aux tickers de votre portefeuille
                </h3>
                <p className="broker-help">
                  Suggestions locales : vérifiez l’ISIN avant de confirmer.
                  Aucune recherche externe ne reçoit votre CSV. Vos associations
                  confirmées sont conservées pour les imports suivants.
                </p>
                {Object.entries(mappings).map(([name, ticker]) => (
                  <div className="broker-candidate" key={name}>
                    <label className="field">
                      <span>{name}</span>
                      <input
                        className="input"
                        value={ticker}
                        maxLength={32}
                        placeholder="Ticker coté en EUR"
                        onChange={(event) => {
                          changed();
                          setMappings((current) => ({
                            ...current,
                            [name]: event.target.value.toUpperCase(),
                          }));
                        }}
                      />
                    </label>
                    {(suggestions[name] ?? []).map((candidate) => (
                      <div
                        className="broker-candidate-info"
                        key={candidate.isin}
                      >
                        <strong>{candidate.name}</strong>
                        <span>
                          {candidate.ticker} · {candidate.isin} ·{" "}
                          {candidate.exchange} · {candidate.currency}
                        </span>
                        <small>{candidate.note}</small>
                        <a
                          href={candidate.evidence_url}
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          Fiche du titre
                        </a>
                        <button
                          type="button"
                          className="button button-secondary"
                          disabled={ticker === candidate.ticker}
                          onClick={() => {
                            changed();
                            setMappings((current) => ({
                              ...current,
                              [name]: candidate.ticker,
                            }));
                          }}
                        >
                          {ticker === candidate.ticker
                            ? "Association sélectionnée"
                            : `Utiliser ${candidate.ticker} après vérification`}
                        </button>
                      </div>
                    ))}
                  </div>
                ))}
              </section>
            )}
            <button
              type="button"
              className="button button-primary"
              disabled={!account || !dateFormat || !text.trim()}
              onClick={() => void compare()}
            >
              Comparer avant import
            </button>
          </>
        ) : null}
      </fieldset>
      {busy && <Loading label="Traitement de l'import…" />}
      {!!error && <ErrorBlock error={error} />}
      {result && (
        <div className="callout" role="status">
          Import appliqué : {result.add} ajout(s), {result.duplicate} ligne(s)
          déjà importée(s), {result.link} opération(s) existante(s) associée(s),{" "}
          {result.ignored} versement(s) ignoré(s).
        </div>
      )}
      {preview && (
        <section aria-label="Aperçu de l'import" className="broker-preview">
          <h3>
            {preview.summary.add} ajout(s) · {preview.summary.duplicate}{" "}
            doublon(s) · {preview.summary.link} correspondance(s) existante(s)
          </h3>
          <p>
            Variation des liquidités :{" "}
            {money(Number(preview.summary.cash_change), "EUR", 2)} · Frais
            effectifs : {money(Number(preview.summary.fees), "EUR", 4)}
          </p>
          {preview.warnings.map((warning) => (
            <p className="callout" key={warning}>
              {warning}
            </p>
          ))}
          {preview.errors.length > 0 && (
            <div className="callout callout-warning" role="alert">
              <strong>
                {preview.errors.length} erreur(s) : aucun ajout ne sera
                appliqué.
              </strong>
              {preview.errors.slice(0, 30).map((message, index) => (
                <p key={index}>{message}</p>
              ))}
            </div>
          )}
          <div className="broker-table-scroll">
            <table className="broker-table">
              <thead>
                <tr>
                  <th>Date CSV → ISO</th>
                  <th>Opération</th>
                  <th>Quantité</th>
                  <th>Cours</th>
                  <th>Frais</th>
                  <th>Net espèces</th>
                  <th>État</th>
                </tr>
              </thead>
              <tbody>
                {preview.rows.slice((page - 1) * 50, page * 50).map((row) => (
                  <tr key={row.line}>
                    <td>
                      {row.raw_date}
                      <br />
                      <strong>{row.date}</strong>
                    </td>
                    <td>
                      {row.description}
                      <br />
                      <small>{row.ticker}</small>
                    </td>
                    <td>{number(Number(row.quantity), 8)}</td>
                    <td>{number(Number(row.unit_price), 8)}</td>
                    <td title={row.fees}>{number(Number(row.fees), 8)}</td>
                    <td>{money(Number(row.net), "EUR", 2)}</td>
                    <td>{statusNames[row.status]}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="broker-actions">
            <button
              type="button"
              className="button button-ghost"
              disabled={busy || page <= 1}
              onClick={() => setPage(page - 1)}
            >
              Précédent
            </button>
            <span>
              {page} / {Math.max(1, Math.ceil(preview.rows.length / 50))}
            </span>
            <button
              type="button"
              className="button button-ghost"
              disabled={busy || page * 50 >= preview.rows.length}
              onClick={() => setPage(page + 1)}
            >
              Suivant
            </button>
            <button
              type="button"
              className="button button-primary"
              disabled={
                busy ||
                preview.errors.length > 0 ||
                preview.summary.add + preview.summary.link === 0
              }
              onClick={() => void apply()}
            >
              Confirmer les ajouts
            </button>
          </div>
        </section>
      )}
    </dialog>
  );
}
