import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, api, searchParams } from "./api";

afterEach(() => vi.unstubAllGlobals());

describe("searchParams", () => {
  it("omits empty filters and repeats list values", () => {
    expect(
      searchParams({
        q: "world",
        page: 2,
        empty: "",
        missing: undefined,
        month: ["2026-07", "2026-08"],
      }),
    ).toBe("?q=world&page=2&month=2026-07&month=2026-08");
  });
});

describe("api", () => {
  it("surfaces the API detail and status", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "Aperçu périmé" }), {
          status: 409,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );

    try {
      await api("/api/imports/example/apply");
      throw new Error("Expected the request to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(ApiError);
      expect(error).toMatchObject({ status: 409, message: "Aperçu périmé" });
    }
  });
});
