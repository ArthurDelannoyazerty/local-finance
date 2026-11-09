import { describe, expect, it, vi } from "vitest";
import {
  clipboardBytes,
  delimitedText,
  MAX_CLIPBOARD_BYTES,
  writeClipboard,
} from "./clipboard";

describe("clipboard text", () => {
  it("escapes delimiters, quotes and multiline comments without rounding", () => {
    expect(
      delimitedText(
        ["Compte", "Note", "Montant"],
        [["Compte, joint", 'Ligne 1\n"Ligne 2"', -1234.5678]],
      ),
    ).toBe(
      'Compte,Note,Montant\r\n"Compte, joint","Ligne 1\n""Ligne 2""",-1234.5678',
    );
  });
  it("represents missing values as empty cells, not zero or NaN", () => {
    expect(delimitedText(["A", "B", "C"], [[null, undefined, NaN]])).toBe(
      "A,B,C\r\n,,",
    );
  });
  it("protects spreadsheet formulas in textual cells, not numeric negatives", () => {
    expect(delimitedText(["Nom", "Montant"], [[" =1+1", -20]])).toBe(
      "Nom,Montant\r\n' =1+1,-20",
    );
  });
  it("keeps Unicode and tabs in a quoted text table", () => {
    expect(delimitedText(["Catégorie", "Montant"], [["Été\tA", 2]], "\t")).toBe(
      'Catégorie\tMontant\r\n"Été\tA"\t2',
    );
  });
  it("writes the whole string without truncation", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    const text = "Date,Montant\r\n2026-01-01,42";
    await writeClipboard(text, { writeText });
    expect(writeText).toHaveBeenCalledExactlyOnceWith(text);
  });
  it("does not report success while the platform write is pending", async () => {
    let complete!: () => void;
    const pending = new Promise<void>((resolve) => {
      complete = resolve;
    });
    const done = vi.fn();
    const task = writeClipboard("data", { writeText: () => pending }).then(
      done,
    );
    await Promise.resolve();
    expect(done).not.toHaveBeenCalled();
    complete();
    await task;
    expect(done).toHaveBeenCalledOnce();
  });
  it("propagates denied clipboard permission instead of claiming success", async () => {
    const denied = new DOMException("Permission denied", "NotAllowedError");
    await expect(
      writeClipboard("data", { writeText: () => Promise.reject(denied) }),
    ).rejects.toBe(denied);
  });
  it("rejects an unavailable clipboard API", async () => {
    await expect(
      writeClipboard("data", {} as Pick<Clipboard, "writeText">),
    ).rejects.toThrow("HTTPS");
  });
  it("bounds UTF-8 bytes, not character count, before touching the clipboard", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    expect(clipboardBytes("é")).toBe(2);
    await expect(
      writeClipboard("é".repeat(MAX_CLIPBOARD_BYTES / 2 + 1), { writeText }),
    ).rejects.toThrow("volumineuse");
    expect(writeText).not.toHaveBeenCalled();
    await writeClipboard("x".repeat(MAX_CLIPBOARD_BYTES), { writeText });
    expect(writeText).toHaveBeenCalledOnce();
  });
});
