import { useEffect, useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  CheckCircle2,
  DatabaseBackup,
  FileSpreadsheet,
  Info,
  Plus,
  ShieldCheck,
  UploadCloud,
} from "lucide-react";
import { api, download } from "../api";
import {
  Empty,
  ErrorBlock,
  Loading,
  money,
  PageHeader,
  Panel,
} from "../components/ui";
import type { Account } from "../types";

type DiffSection = {
  added: number;
  removed: number;
  unchanged: number;
  added_preview: Record<string, unknown>[];
  removed_preview: Record<string, unknown>[];
};
type Preview = {
  id: string;
  filename: string;
  status: string;
  total: { added: number; removed: number; unchanged: number };
  sheets: Record<string, DiffSection>;
};
type Batch = {
  id: string;
  filename: string;
  created_at: string;
  applied_at: string | null;
  status: string;
  summary: {
    total: { added: number; removed: number; unchanged: number };
    applied?: { added: number; removed: number; deletions_skipped: number };
  };
};

const labels: Record<string, string> = {
  INCOME: "Revenus",
  EXPENSE: "Dépenses",
  TRANSFER: "Transferts",
};
const statusLabels: Record<string, string> = {
  PREVIEW: "En attente",
  APPLIED: "Appliqué",
  CANCELLED: "Annulé",
};

function DiffRows({
  kind,
  rows,
}: {
  kind: string;
  rows: Record<string, unknown>[];
}) {
  if (rows.length === 0) return <p className="muted">Aucune ligne.</p>;
  const text = (row: Record<string, unknown>, key: string) =>
    String(row[key] ?? "");
  const amount = (row: Record<string, unknown>) =>
    money(Number(row.amount ?? 0), text(row, "currency") || "EUR", 2);
  return (
    <div
      className="table-wrap"
      style={{ border: "1px solid var(--border)", borderRadius: 10 }}
    >
      <table>
        <thead>
          <tr>
            <th>Date</th>
            {kind === "TRANSFER" ? (
              <>
                <th>De</th>
                <th>Vers</th>
              </>
            ) : (
              <>
                <th>Catégorie</th>
                <th>Compte</th>
              </>
            )}
            <th className="cell-number">Montant</th>
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 8).map((row, index) => (
            <tr key={`${text(row, "date")}-${index}`}>
              <td>{text(row, "date")}</td>
              {kind === "TRANSFER" ? (
                <>
                  <td>{text(row, "source_account")}</td>
                  <td>{text(row, "target_account")}</td>
                </>
              ) : (
                <>
                  <td>{text(row, "category")}</td>
                  <td>{text(row, "account")}</td>
                </>
              )}
              <td className="cell-number">{amount(row)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AccountRow({ account }: { account: Account }) {
  const client = useQueryClient();
  const [value, setValue] = useState(account);
  useEffect(() => setValue(account), [account]);
  const save = useMutation({
    mutationFn: () =>
      api(`/api/accounts/${encodeURIComponent(account.name)}`, {
        method: "PUT",
        body: JSON.stringify({
          initial_balance: value.initial_balance,
          opening_balance_date: value.opening_balance_date || null,
          is_visible: value.is_visible,
          revision: account.revision,
        }),
      }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["accounts"] }),
  });
  return (
    <tr>
      <td>
        <strong>{account.name}</strong>
      </td>
      <td>
        <input
          className="input"
          type="number"
          step="0.01"
          value={value.initial_balance}
          onChange={(e) =>
            setValue({ ...value, initial_balance: Number(e.target.value) })
          }
        />
      </td>
      <td>
        <input
          className="input"
          type="date"
          value={value.opening_balance_date ?? ""}
          onChange={(e) =>
            setValue({ ...value, opening_balance_date: e.target.value || null })
          }
        />
      </td>
      <td>
        <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <input
            type="checkbox"
            checked={value.is_visible}
            onChange={(e) =>
              setValue({ ...value, is_visible: e.target.checked })
            }
          />
          Analyses
        </label>
      </td>
      <td>
        <div style={{ display: "grid", gap: 5 }}>
          <button
            className="button button-ghost"
            onClick={() => save.mutate()}
            disabled={save.isPending}
          >
            Enregistrer
          </button>
          {save.error && (
            <small className="negative">{save.error.message}</small>
          )}
        </div>
      </td>
    </tr>
  );
}

export default function DataPage() {
  const client = useQueryClient();
  const [file, setFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [confirmRemoval, setConfirmRemoval] = useState(false);
  const [newAccount, setNewAccount] = useState("");
  const accounts = useQuery({
    queryKey: ["accounts"],
    queryFn: () => api<Account[]>("/api/accounts"),
  });
  const history = useQuery({
    queryKey: ["imports"],
    queryFn: () => api<Batch[]>("/api/imports"),
  });
  const previewMutation = useMutation({
    mutationFn: async () => {
      if (!file) throw new Error("Choisissez un fichier Excel");
      const form = new FormData();
      form.append("file", file);
      return api<Preview>("/api/imports/preview", {
        method: "POST",
        body: form,
      });
    },
    onSuccess: (data) => {
      setPreview(data);
      setConfirmRemoval(false);
    },
  });
  const applyMutation = useMutation({
    mutationFn: (allowDeletions: boolean) =>
      api(`/api/imports/${preview!.id}/apply`, {
        method: "POST",
        body: JSON.stringify({ allow_deletions: allowDeletions }),
      }),
    onSuccess: async () => {
      setPreview(null);
      setFile(null);
      await Promise.all([client.invalidateQueries(), history.refetch()]);
    },
  });
  const cancelMutation = useMutation({
    mutationFn: () => api(`/api/imports/${preview!.id}`, { method: "DELETE" }),
    onSuccess: async () => {
      setPreview(null);
      setFile(null);
      await client.invalidateQueries({ queryKey: ["imports"] });
    },
  });
  const cancelHistory = useMutation({
    mutationFn: (id: string) => api(`/api/imports/${id}`, { method: "DELETE" }),
    onSuccess: () => client.invalidateQueries({ queryKey: ["imports"] }),
  });
  const createAccount = useMutation({
    mutationFn: () =>
      api("/api/accounts", {
        method: "POST",
        body: JSON.stringify({
          name: newAccount,
          initial_balance: 0,
          is_visible: true,
        }),
      }),
    onSuccess: async () => {
      setNewAccount("");
      await client.invalidateQueries({ queryKey: ["accounts"] });
    },
  });
  return (
    <>
      <PageHeader
        eyebrow="Système"
        title="Données & comptes"
        description="Import contrôlé, configuration des soldes d’ouverture et sauvegardes récupérables de votre base locale."
        actions={
          <button
            className="button button-secondary"
            onClick={() =>
              download("/api/backups/database", "finance-backup.db")
            }
          >
            <DatabaseBackup size={15} />
            Télécharger une sauvegarde
          </button>
        }
      />
      <div className="grid-wide">
        <div className="stack">
          <Panel
            title="Synchroniser l’export Excel"
            description="Revenus, Dépenses et Transferts restent intégralement pilotés par l’application Android."
          >
            {!preview ? (
              <>
                <div className="import-drop">
                  <label>
                    <UploadCloud size={31} />
                    <strong>
                      {file ? file.name : "Choisir l’export .xlsx"}
                    </strong>
                    <span>
                      Le fichier est entièrement validé avant toute écriture.
                    </span>
                    <input
                      type="file"
                      accept=".xlsx"
                      onChange={(e) => setFile(e.target.files?.[0] ?? null)}
                    />
                  </label>
                </div>
                {previewMutation.error && (
                  <ErrorBlock error={previewMutation.error} />
                )}
                <div className="form-actions">
                  <button
                    className="button button-primary"
                    onClick={() => previewMutation.mutate()}
                    disabled={!file || previewMutation.isPending}
                  >
                    {previewMutation.isPending
                      ? "Analyse du fichier…"
                      : "Comparer avec la base"}
                  </button>
                </div>
              </>
            ) : (
              <div className="stack">
                <div className="callout">
                  <ShieldCheck size={19} />
                  <div>
                    <strong>Aucun changement n’a encore été appliqué.</strong>{" "}
                    Vérifiez le résumé ci-dessous avant de choisir le mode de
                    synchronisation.
                  </div>
                </div>
                <div className="diff-grid">
                  <div className="diff-card">
                    <span>À ajouter</span>
                    <strong className="positive">+{preview.total.added}</strong>
                  </div>
                  <div className="diff-card">
                    <span>Absentes du fichier</span>
                    <strong className={preview.total.removed ? "negative" : ""}>
                      −{preview.total.removed}
                    </strong>
                  </div>
                  <div className="diff-card">
                    <span>Identiques</span>
                    <strong>{preview.total.unchanged}</strong>
                  </div>
                </div>
                {Object.entries(preview.sheets).map(([kind, section]) => (
                  <details key={kind}>
                    <summary
                      style={{
                        cursor: "pointer",
                        color: "#c7d0dc",
                        padding: "10px 0",
                      }}
                    >
                      <strong>{labels[kind] ?? kind}</strong> · +{section.added}{" "}
                      / −{section.removed} / {section.unchanged} identiques
                    </summary>
                    {(section.added_preview.length > 0 ||
                      section.removed_preview.length > 0) && (
                      <div className="grid-2">
                        <div>
                          <span className="eyebrow">Ajouts — aperçu</span>
                          <DiffRows kind={kind} rows={section.added_preview} />
                        </div>
                        <div>
                          <span className="eyebrow">Suppressions — aperçu</span>
                          <DiffRows
                            kind={kind}
                            rows={section.removed_preview}
                          />
                        </div>
                      </div>
                    )}
                  </details>
                ))}
                {preview.total.removed > 0 && (
                  <label className="callout callout-warning">
                    <Info size={18} />
                    <input
                      type="checkbox"
                      checked={confirmRemoval}
                      onChange={(e) => setConfirmRemoval(e.target.checked)}
                    />
                    <span>
                      Je confirme que les {preview.total.removed} lignes
                      absentes de cet export doivent être supprimées de la copie
                      locale.
                    </span>
                  </label>
                )}
                {applyMutation.error && (
                  <ErrorBlock error={applyMutation.error} />
                )}
                {cancelMutation.error && (
                  <ErrorBlock error={cancelMutation.error} />
                )}
                <div className="form-actions">
                  <button
                    className="button button-ghost"
                    onClick={() => cancelMutation.mutate()}
                    disabled={
                      cancelMutation.isPending || applyMutation.isPending
                    }
                  >
                    Annuler
                  </button>
                  {preview.total.removed > 0 && (
                    <button
                      className="button button-secondary"
                      onClick={() => applyMutation.mutate(false)}
                      disabled={applyMutation.isPending}
                    >
                      Ajouter sans supprimer
                    </button>
                  )}
                  <button
                    className={
                      preview.total.removed
                        ? "button button-danger"
                        : "button button-primary"
                    }
                    onClick={() =>
                      applyMutation.mutate(preview.total.removed > 0)
                    }
                    disabled={
                      applyMutation.isPending ||
                      (preview.total.removed > 0 && !confirmRemoval)
                    }
                  >
                    {preview.total.removed
                      ? "Synchroniser exactement"
                      : "Appliquer l’import"}
                  </button>
                </div>
              </div>
            )}
          </Panel>
          <Panel
            className="panel-flush"
            title="Comptes"
            description="Le solde d’ouverture prend effet à la date choisie; la visibilité ne modifie jamais l’historique importé."
            action={
              <div style={{ display: "flex", gap: 8 }}>
                <input
                  className="input"
                  placeholder="Nouveau compte"
                  value={newAccount}
                  onChange={(e) => setNewAccount(e.target.value)}
                />
                <button
                  className="button button-secondary"
                  disabled={!newAccount.trim() || createAccount.isPending}
                  onClick={() => createAccount.mutate()}
                >
                  <Plus size={14} />
                  Créer
                </button>
              </div>
            }
          >
            {createAccount.error && (
              <div
                className="callout callout-warning"
                style={{ margin: "10px 21px" }}
              >
                {createAccount.error.message}
              </div>
            )}
            {accounts.isLoading ? (
              <Loading />
            ) : accounts.error ? (
              <ErrorBlock error={accounts.error} />
            ) : accounts.data!.length ? (
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>Compte</th>
                      <th>Solde d’ouverture</th>
                      <th>Date d’ouverture</th>
                      <th>Visible</th>
                      <th />
                    </tr>
                  </thead>
                  <tbody>
                    {accounts.data!.map((account) => (
                      <AccountRow account={account} key={account.name} />
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <Empty
                title="Aucun compte"
                description="Importez un fichier ou créez votre premier compte."
              />
            )}
          </Panel>
        </div>
        <Panel
          title="Historique des imports"
          description="Chaque comparaison est horodatée et conserve son résultat."
        >
          {cancelHistory.error && (
            <div
              className="callout callout-warning"
              style={{ marginBottom: 12 }}
            >
              {cancelHistory.error.message}
            </div>
          )}
          {history.isLoading ? (
            <Loading />
          ) : history.error ? (
            <ErrorBlock error={history.error} />
          ) : history.data!.length ? (
            <div className="stack">
              {history.data!.map((batch) => (
                <article
                  key={batch.id}
                  style={{
                    paddingBottom: 13,
                    borderBottom: "1px solid var(--border)",
                  }}
                >
                  <div
                    style={{ display: "flex", alignItems: "center", gap: 9 }}
                  >
                    <FileSpreadsheet size={17} color="#67e8b6" />
                    <strong style={{ fontSize: 12 }}>{batch.filename}</strong>
                    <span className="badge">
                      {statusLabels[batch.status] ?? batch.status}
                    </span>
                    {batch.status === "PREVIEW" && (
                      <button
                        className="button button-ghost"
                        style={{ minHeight: 28, marginLeft: "auto" }}
                        onClick={() => cancelHistory.mutate(batch.id)}
                        disabled={cancelHistory.isPending}
                      >
                        Annuler
                      </button>
                    )}
                  </div>
                  <div
                    className="muted"
                    style={{ margin: "7px 0 0 26px", fontSize: 11 }}
                  >
                    {new Date(batch.created_at).toLocaleString("fr-FR")} · +
                    {batch.summary.total.added} / −{batch.summary.total.removed}
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <Empty
              title="Aucun import"
              description="La première comparaison apparaîtra ici."
            />
          )}
          <div className="callout" style={{ marginTop: 18 }}>
            <CheckCircle2 size={18} />
            <span>
              Une sauvegarde automatique est créée avant toute synchronisation
              comportant des suppressions.
            </span>
          </div>
        </Panel>
      </div>
    </>
  );
}
