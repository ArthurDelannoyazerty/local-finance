import { useDeferredValue, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { LockKeyhole, Search } from "lucide-react";
import { api, searchParams } from "../api";
import {
  Empty,
  ErrorBlock,
  Loading,
  MetricCard,
  money,
  PageHeader,
  Pagination,
  Panel,
  shortDate,
} from "../components/ui";
import type { DateRangeValue, Transaction } from "../types";

type Response = {
  items: Transaction[];
  total: number;
  page: number;
  page_size: number;
  summary: { income: number; expense: number; net: number };
  filters: { categories: string[]; accounts: string[] };
};

const column = createColumnHelper<Transaction>();

export default function Transactions({ range }: { range: DateRangeValue }) {
  const [q, setQ] = useState("");
  const [type, setType] = useState("");
  const [category, setCategory] = useState("");
  const [account, setAccount] = useState("");
  const [page, setPage] = useState(1);
  const deferredQuery = useDeferredValue(q);
  const params = {
    ...range,
    q: deferredQuery,
    type,
    category,
    account,
    page,
    page_size: 50,
  };
  const query = useQuery({
    queryKey: ["transactions", params],
    queryFn: () => api<Response>(`/api/transactions${searchParams(params)}`),
  });
  const columns = useMemo(
    () => [
      column.accessor("date", {
        header: "Date",
        cell: (info) => shortDate(info.getValue()),
      }),
      column.accessor("type", {
        header: "Type",
        cell: (info) => (
          <span className={`badge badge-${info.getValue().toLowerCase()}`}>
            {info.getValue() === "INCOME" ? "Revenu" : "Dépense"}
          </span>
        ),
      }),
      column.accessor("category", { header: "Catégorie" }),
      column.accessor("account", { header: "Compte" }),
      column.accessor("comment", {
        header: "Commentaire",
        cell: (info) => <span className="muted">{info.getValue() || "—"}</span>,
      }),
      column.accessor("amount", {
        header: "Montant",
        cell: (info) => {
          const row = info.row.original;
          return (
            <span
              className={
                row.type === "INCOME" ? "amount-income" : "amount-expense"
              }
            >
              {row.type === "INCOME" ? "+" : "−"}
              {money(info.getValue(), row.currency, 2)}
            </span>
          );
        },
        meta: { numeric: true },
      }),
    ],
    [],
  );
  const table = useReactTable({
    data: query.data?.items ?? [],
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <>
      <PageHeader
        eyebrow="Quotidien"
        title="Transactions"
        description="Recherchez vos revenus et dépenses sans créer une seconde version des données de votre application mobile."
      />
      <div className="callout" style={{ marginBottom: 15 }}>
        <LockKeyhole size={18} />
        <div>
          <strong>Registre en lecture seule.</strong> Les corrections se font
          dans l’application Android, puis reviennent ici au prochain import
          Excel confirmé.
        </div>
      </div>
      {query.data && (
        <div
          className="metrics"
          style={{ gridTemplateColumns: "repeat(3,minmax(0,1fr))" }}
        >
          <MetricCard
            label="Revenus filtrés"
            value={money(query.data.summary.income)}
            tone="positive"
          />
          <MetricCard
            label="Dépenses filtrées"
            value={money(query.data.summary.expense)}
            tone="negative"
          />
          <MetricCard
            label="Solde filtré"
            value={money(query.data.summary.net)}
            tone={query.data.summary.net >= 0 ? "accent" : "negative"}
          />
        </div>
      )}
      <Panel
        className="panel-flush"
        title="Registre importé"
        description={
          query.data
            ? `${query.data.total.toLocaleString("fr-FR")} opérations · Solde filtré ${money(query.data.summary.net)}`
            : "Chargement du registre"
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
                value={q}
                onChange={(event) => {
                  setQ(event.target.value);
                  setPage(1);
                }}
                placeholder="Date, catégorie, compte, montant ou commentaire…"
              />
            </div>
          </div>
          <div className="field">
            <span>Type</span>
            <select
              className="select"
              value={type}
              onChange={(event) => {
                setType(event.target.value);
                setPage(1);
              }}
            >
              <option value="">Tous</option>
              <option value="INCOME">Revenus</option>
              <option value="EXPENSE">Dépenses</option>
            </select>
          </div>
          <div className="field">
            <span>Catégorie</span>
            <select
              className="select"
              value={category}
              onChange={(event) => {
                setCategory(event.target.value);
                setPage(1);
              }}
            >
              <option value="">Toutes</option>
              {query.data?.filters.categories.map((value) => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </div>
          <div className="field">
            <span>Compte</span>
            <select
              className="select"
              value={account}
              onChange={(event) => {
                setAccount(event.target.value);
                setPage(1);
              }}
            >
              <option value="">Tous</option>
              {query.data?.filters.accounts.map((value) => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </div>
        </div>
        {query.isLoading ? (
          <Loading />
        ) : query.error ? (
          <ErrorBlock error={query.error} />
        ) : query.data?.items.length === 0 ? (
          <Empty
            title="Aucun résultat"
            description="Modifiez les filtres ou la période sélectionnée."
          />
        ) : (
          <>
            <div className="table-wrap">
              <table>
                <thead>
                  {table.getHeaderGroups().map((group) => (
                    <tr key={group.id}>
                      {group.headers.map((header) => (
                        <th
                          key={header.id}
                          className={
                            (
                              header.column.columnDef.meta as
                                { numeric?: boolean } | undefined
                            )?.numeric
                              ? "cell-number"
                              : ""
                          }
                        >
                          {flexRender(
                            header.column.columnDef.header,
                            header.getContext(),
                          )}
                        </th>
                      ))}
                    </tr>
                  ))}
                </thead>
                <tbody>
                  {table.getRowModel().rows.map((row) => (
                    <tr key={row.id}>
                      {row.getVisibleCells().map((cell) => (
                        <td
                          key={cell.id}
                          className={
                            (
                              cell.column.columnDef.meta as
                                { numeric?: boolean } | undefined
                            )?.numeric
                              ? "cell-number"
                              : ""
                          }
                        >
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext(),
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <Pagination
              page={page}
              pageSize={query.data!.page_size}
              total={query.data!.total}
              onPage={setPage}
            />
          </>
        )}
      </Panel>
    </>
  );
}
