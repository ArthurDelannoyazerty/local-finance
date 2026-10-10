import { describe, expect, it } from "vitest";
import { MAX_IMPORT_BYTES, readCsvFile, validateCsvText } from "./imports";

const text = "Date;D\u00e9signation;Cr\u00e9dit;Qt\u00e9;Cours;D\u00e9bit\r\n";
const file = (bytes: Uint8Array, name = "export.csv") =>
  ({ name, size: bytes.length, arrayBuffer: async () => bytes.buffer }) as File;

describe("CSV input", () => {
  it.each([false, true])("decodes UTF-8, BOM=%s", async (bom) => {
    expect(await readCsvFile(file(new TextEncoder().encode((bom ? "\ufeff" : "") + text)))).toBe(text);
  });
  it("decodes Windows-1252", async () => {
    expect(await readCsvFile(file(Uint8Array.from(text, (c) => c.charCodeAt(0))))).toBe(text);
  });
  it.each([false, true])("decodes UTF-16, big-endian=%s", async (big) => {
    const bytes = big ? [254, 255] : [255, 254];
    for (const character of text) {
      const n = character.charCodeAt(0);
      bytes.push(...(big ? [n >> 8, n & 255] : [n & 255, n >> 8]));
    }
    expect(await readCsvFile(file(new Uint8Array(bytes)))).toBe(text);
  });
  it("retains empty final TSV columns", () => {
    const tsv = "Date\tDesignation\tCredit\tQte\tCours\tDebit\n1/2/2020\tTEST\t1\t\t\t\n";
    expect(validateCsvText(tsv)).toBe(tsv);
  });
  it("rejects an unexpected file type", async () => {
    await expect(readCsvFile(file(new Uint8Array(), "export.xlsx"))).rejects.toThrow();
  });
  it("rejects binary and empty input", async () => {
    await expect(readCsvFile(file(new Uint8Array([65, 0, 66])))).rejects.toThrow();
    await expect(readCsvFile(file(new Uint8Array()))).rejects.toThrow();
  });
  it("bounds size before reading a file", async () => {
    const oversized = {
      name: "large.csv", size: MAX_IMPORT_BYTES + 1,
      arrayBuffer: () => { throw new Error("must not read"); },
    } as unknown as File;
    await expect(readCsvFile(oversized)).rejects.not.toThrow("must not read");
  });
  it("bounds decoded UTF-8 bytes, not JavaScript string length", () => {
    expect(() => validateCsvText("\u00e9".repeat(MAX_IMPORT_BYTES / 2 + 1))).toThrow();
  });
});
