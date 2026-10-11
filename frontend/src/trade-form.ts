import { format } from "date-fns";
import type { Trade } from "./types";

export type TradeDraft = Pick<
  Trade,
  | "date"
  | "ticker"
  | "name"
  | "action"
  | "quantity"
  | "unit_price"
  | "fees"
  | "currency"
  | "account"
  | "comment"
>;
export type TradeEditor =
  { mode: "new" } | { mode: "edit" | "duplicate"; trade: Trade };

/** Whitelist editable fields: never copy identity, revisions or import receipts. */
export function tradeFields(trade: TradeDraft): TradeDraft {
  return {
    date: trade.date,
    ticker: trade.ticker,
    name: trade.name,
    action: trade.action,
    quantity: trade.quantity,
    unit_price: trade.unit_price,
    fees: trade.fees,
    currency: trade.currency,
    account: trade.account,
    comment: trade.comment,
  };
}

export function initialTradeDraft(
  editor: TradeEditor,
  account = "",
  now = new Date(),
): TradeDraft {
  if (editor.mode !== "new") {
    const fields = tradeFields(editor.trade);
    return editor.mode === "duplicate"
      ? { ...fields, date: format(now, "yyyy-MM-dd") }
      : fields;
  }
  return {
    date: format(now, "yyyy-MM-dd"),
    ticker: "",
    name: "",
    action: "BUY",
    quantity: 0,
    unit_price: 0,
    fees: 0,
    currency: "EUR",
    account,
    comment: "",
  };
}

export function tradeRequest(
  editor: TradeEditor,
  draft: TradeDraft,
): { path: string; init: RequestInit } {
  const fields = tradeFields(draft);
  if (editor.mode === "edit") {
    return {
      path: `/api/investments/${editor.trade.id}`,
      init: {
        method: "PUT",
        body: JSON.stringify({ ...fields, revision: editor.trade.revision }),
      },
    };
  }
  return {
    path: "/api/investments",
    init: { method: "POST", body: JSON.stringify(fields) },
  };
}
