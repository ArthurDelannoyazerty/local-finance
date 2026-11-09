export const MAX_CLIPBOARD_BYTES = 5 * 1024 * 1024;

export type TextCell = string | number | boolean | null | undefined;

function cellText(value: TextCell, separator: string): string {
  let text = value == null ? "" : String(value);
  if (typeof value === "number" && !Number.isFinite(value)) text = "";
  // A copied CSV may also be pasted into a spreadsheet. Keep text as text.
  if (typeof value === "string" && /^[\s]*[=+\-@]/u.test(value)) {
    text = `'${text}`;
  }
  return text.includes(separator) || /["\r\n]/u.test(text)
    ? `"${text.replaceAll('"', '""')}"`
    : text;
}

export function delimitedText(
  headers: string[],
  rows: TextCell[][],
  separator = ",",
): string {
  return [headers, ...rows]
    .map((row) => row.map((value) => cellText(value, separator)).join(separator))
    .join("\r\n");
}

export function clipboardBytes(text: string): number {
  return new TextEncoder().encode(text).byteLength;
}

export async function writeClipboard(
  text: string,
  clipboard: Pick<Clipboard, "writeText"> | undefined = globalThis.navigator
    ?.clipboard,
): Promise<void> {
  if (clipboardBytes(text) > MAX_CLIPBOARD_BYTES) {
    throw new Error("Copie trop volumineuse (limite de sécurité : 5 Mio).");
  }
  if (!clipboard?.writeText) {
    throw new Error("Le presse-papiers nécessite un navigateur compatible et HTTPS.");
  }
  await clipboard.writeText(text);
}
