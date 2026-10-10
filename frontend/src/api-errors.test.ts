import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "./api";

afterEach(() => vi.unstubAllGlobals());
function failure(detail: unknown) {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
    new Response(JSON.stringify({ detail }), { status: 422 }),
  ));
}
describe("API error messages", () => {
  it("renders field validation errors", async () => {
    failure([{ loc: ["body", "mappings"], msg: "Invalid ticker" }]);
    await expect(api("/test")).rejects.toThrow("mappings: Invalid ticker");
  });
  it("preserves normal error strings", async () => {
    failure("Invalid import");
    await expect(api("/test")).rejects.toThrow("Invalid import");
  });
  it("does not render arbitrary objects", async () => {
    failure({ arbitrary: "payload" });
    await expect(api("/test")).rejects.toThrow("422");
  });
});
