/** Calendar-month helpers; no UTC parsing or daylight-saving arithmetic. */
export function monthIndex(value: string): number | null {
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(value)) return null;
  const [year, month] = value.split("-").map(Number);
  return year * 12 + month - 1;
}

export function availableMonths(first: string, last: string): string[] {
  const start = monthIndex(first.slice(0, 7));
  const end = monthIndex(last.slice(0, 7));
  if (start === null || end === null || end < start) return [];
  return Array.from({ length: end - start + 1 }, (_, offset) => {
    const index = start + offset;
    const year = String(Math.floor(index / 12)).padStart(4, "0");
    return `${year}-${String((index % 12) + 1).padStart(2, "0")}`;
  });
}

export function monthLabel(month: string): string {
  const index = monthIndex(month);
  if (index === null) return month;
  const date = new Date(0);
  date.setUTCFullYear(Math.floor(index / 12), index % 12, 1);
  return new Intl.DateTimeFormat("fr-FR", {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  }).format(date);
}

export function toggleMonths(
  selected: string[],
  months: string[],
  checked: boolean,
): string[] {
  const next = new Set(selected);
  months.forEach((month) => (checked ? next.add(month) : next.delete(month)));
  return [...next].sort();
}
