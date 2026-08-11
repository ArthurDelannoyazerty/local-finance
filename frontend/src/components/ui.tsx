import type { ReactNode } from "react";
import { AlertCircle, Inbox, LoaderCircle, X } from "lucide-react";

export const money = (value: number, currency = "EUR", digits = 0) =>
  new Intl.NumberFormat("fr-FR", {
    style: "currency",
    currency,
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(value);

export const number = (value: number, digits = 2) =>
  new Intl.NumberFormat("fr-FR", { maximumFractionDigits: digits }).format(
    value,
  );

export const percent = (value: number | null | undefined) =>
  value === null || value === undefined
    ? "—"
    : new Intl.NumberFormat("fr-FR", {
        style: "percent",
        maximumFractionDigits: 1,
      }).format(value / 100);

export const shortDate = (value: string) =>
  new Intl.DateTimeFormat("fr-FR", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  }).format(new Date(`${value}T12:00:00`));

export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
}: {
  eyebrow: string;
  title: string;
  description: string;
  actions?: ReactNode;
}) {
  return (
    <header className="page-header">
      <div>
        <span className="eyebrow">{eyebrow}</span>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </header>
  );
}

export function MetricCard({
  label,
  value,
  note,
  tone = "default",
}: {
  label: string;
  value: string;
  note?: ReactNode;
  tone?: "default" | "positive" | "negative" | "accent";
}) {
  return (
    <article className={`metric-card metric-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      {note && <small>{note}</small>}
    </article>
  );
}

export function Panel({
  title,
  description,
  action,
  children,
  className = "",
}: {
  title?: string;
  description?: string;
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel ${className}`}>
      {(title || description || action) && (
        <div className="panel-heading">
          <div>
            {title && <h2>{title}</h2>}
            {description && <p>{description}</p>}
          </div>
          {action}
        </div>
      )}
      {children}
    </section>
  );
}

export function Loading({ label = "Chargement…" }: { label?: string }) {
  return (
    <div className="state-block" role="status">
      <LoaderCircle className="spin" size={22} />
      <span>{label}</span>
    </div>
  );
}

export function ErrorBlock({ error }: { error: unknown }) {
  return (
    <div className="state-block state-error" role="alert">
      <AlertCircle size={22} />
      <span>
        {error instanceof Error ? error.message : "Une erreur est survenue"}
      </span>
    </div>
  );
}

export function Empty({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <div className="empty-state">
      <Inbox size={26} />
      <strong>{title}</strong>
      <span>{description}</span>
    </div>
  );
}

export function Modal({
  title,
  children,
  onClose,
}: {
  title: string;
  children: ReactNode;
  onClose: () => void;
}) {
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section
        className="modal"
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <div className="modal-heading">
          <h2>{title}</h2>
          <button className="icon-button" onClick={onClose} aria-label="Fermer">
            <X size={18} />
          </button>
        </div>
        {children}
      </section>
    </div>
  );
}

export function Pagination({
  page,
  pageSize,
  total,
  onPage,
}: {
  page: number;
  pageSize: number;
  total: number;
  onPage: (page: number) => void;
}) {
  const pages = Math.max(1, Math.ceil(total / pageSize));
  return (
    <div className="pagination">
      <span>
        {total.toLocaleString("fr-FR")} ligne{total > 1 ? "s" : ""}
      </span>
      <div>
        <button
          className="button button-ghost"
          disabled={page <= 1}
          onClick={() => onPage(page - 1)}
        >
          Précédent
        </button>
        <span>
          {page} / {pages}
        </span>
        <button
          className="button button-ghost"
          disabled={page >= pages}
          onClick={() => onPage(page + 1)}
        >
          Suivant
        </button>
      </div>
    </div>
  );
}
