export const MAX_IMPORT_BYTES = 5 * 1024 * 1024;

export function validateCsvText(text: string): string {
  text = text.replace(/^\uFEFF/, "");
  if (!text.trim()) throw new Error("Le fichier est vide.");
  if (text.includes("\0"))
    throw new Error("Le fichier n'est pas un texte CSV lisible.");
  if (new TextEncoder().encode(text).length > MAX_IMPORT_BYTES) {
    throw new Error("Le CSV dépasse la limite de 5 Mo.");
  }
  // Do not trim trailing tabs: they represent empty final columns.
  return text;
}

export async function readCsvFile(file: File): Promise<string> {
  if (!/\.(csv|tsv|txt)$/i.test(file.name)) {
    throw new Error("Choisir un fichier CSV, TSV ou TXT.");
  }
  if (file.size > MAX_IMPORT_BYTES) throw new Error("Le fichier dépasse 5 Mo.");
  const bytes = new Uint8Array(await file.arrayBuffer());
  let text: string;
  if (bytes[0] === 255 && bytes[1] === 254) {
    text = new TextDecoder("utf-16le", { fatal: true }).decode(bytes);
  } else if (bytes[0] === 254 && bytes[1] === 255) {
    text = new TextDecoder("utf-16be", { fatal: true }).decode(bytes);
  } else {
    try {
      text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch {
      text = new TextDecoder("windows-1252", { fatal: true }).decode(bytes);
    }
  }
  return validateCsvText(text);
}
