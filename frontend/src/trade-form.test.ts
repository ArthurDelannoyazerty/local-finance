import { describe, expect, it } from "vitest";
import { initialTradeDraft, tradeRequest } from "./trade-form";
import type { Trade } from "./types";

const original: Trade = {
  id: "original-id",
  date: "2025-02-15",
  ticker: "ETF.PA",
  name: "Example fund",
  action: "BUY",
  quantity: 10,
  unit_price: 12.34,
  fees: 0.99,
  currency: "EUR",
  account: "PEA",
  comment: "Example",
  revision: 5,
  created_at: "2025-02-15T12:00:00Z",
  updated_at: "2025-02-16T12:00:00Z",
};

describe("duplicate trade drafts", () => {
  it("prefills a new draft using the local date without touching the original", () => {
    const snapshot = { ...original };
    const draft = initialTradeDraft(
      { mode: "duplicate", trade: original },
      "Ignored default",
      new Date(2026, 9, 11, 0, 15),
    );
    expect(draft).toMatchObject({
      date: "2026-10-11",
      ticker: original.ticker,
      name: original.name,
      account: original.account,
      quantity: original.quantity,
      unit_price: original.unit_price,
      fees: original.fees,
    });
    for (const key of ["id", "revision", "created_at", "updated_at"]) {
      expect(draft).not.toHaveProperty(key);
    }
    draft.quantity = 20;
    draft.date = "2026-10-12";
    expect(original).toEqual(snapshot);
  });

  it("saves duplication as POST with edited fields and no source identity", () => {
    const editor = { mode: "duplicate" as const, trade: original };
    const draft = initialTradeDraft(editor);
    const request = tradeRequest(editor, {
      ...draft,
      date: "2026-10-12",
      quantity: 3,
    });
    expect(request.path).toBe("/api/investments");
    expect(request.init.method).toBe("POST");
    const body = JSON.parse(request.init.body as string);
    expect(body).toMatchObject({
      ticker: "ETF.PA",
      name: "Example fund",
      date: "2026-10-12",
      quantity: 3,
    });
    expect(body).not.toHaveProperty("id");
    expect(body).not.toHaveProperty("revision");
  });

  it("does not propagate unexpected imported metadata into a new order", () => {
    const imported = { ...original, source_key: "receipt", import_batch_id: "batch" };
    const editor = { mode: "duplicate" as const, trade: imported };
    const request = tradeRequest(editor, imported);
    const body = JSON.parse(request.init.body as string);
    expect(body).not.toHaveProperty("source_key");
    expect(body).not.toHaveProperty("import_batch_id");
    expect(body).not.toHaveProperty("id");
  });

  it("keeps ordinary editing as PUT with the original optimistic revision", () => {
    const editor = { mode: "edit" as const, trade: original };
    const draft = initialTradeDraft(editor);
    expect(draft.date).toBe(original.date);
    const request = tradeRequest(editor, draft);
    expect(request.path).toBe("/api/investments/original-id");
    expect(request.init.method).toBe("PUT");
    expect(JSON.parse(request.init.body as string).revision).toBe(5);
  });

  it("preserves sell actions and keeps blank creation available", () => {
    const sell = initialTradeDraft({
      mode: "duplicate",
      trade: { ...original, action: "SELL" },
    });
    expect(sell.action).toBe("SELL");
    const blank = initialTradeDraft({ mode: "new" }, "Current account");
    expect(blank).toMatchObject({ ticker: "", name: "", account: "Current account" });
    expect(tradeRequest({ mode: "new" }, blank).init.method).toBe("POST");
  });
});
