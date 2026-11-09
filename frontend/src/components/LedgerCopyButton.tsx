import { useQuery } from "@tanstack/react-query";
import { api, ApiError } from "../api";
import CopyButton from "./CopyButton";

type LedgerExport = { text: string; row_count: number; byte_count: number };

export default function LedgerCopyButton() {
  const available = Boolean(globalThis.navigator?.clipboard?.writeText);
  // Prepare a bounded snapshot before the click, so the actual clipboard
  // write retains browser user activation. Never cache this after navigation.
  const query = useQuery({
    queryKey: ["ledger-clipboard"],
    queryFn: ({ signal }) =>
      api<LedgerExport>("/api/ledger/clipboard", { signal }),
    enabled: available,
    staleTime: 0,
    gcTime: 0,
    refetchOnMount: "always",
    retry: false,
  });
  if (
    !available ||
    (query.error instanceof ApiError && query.error.status === 413)
  ) {
    return null;
  }
  if (query.error) {
    return (
      <span className="muted" role="status" title={query.error.message}>
        Copie complète indisponible.
      </span>
    );
  }
  return (
    <CopyButton
      text={query.data?.text ?? ""}
      disabled={query.isFetching || !query.data?.row_count}
      caption={
        query.isPending ? "Préparation de la copie…" : "Tout copier (CSV)"
      }
      label="Copier toutes les opérations : revenus, dépenses, transferts, achats et ventes, toutes dates et tous comptes, sans les filtres de cette page"
    />
  );
}
