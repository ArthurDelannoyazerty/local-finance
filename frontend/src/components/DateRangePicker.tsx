import { format, startOfYear, subMonths } from "date-fns";
import { CalendarDays } from "lucide-react";
import type { DateRangeValue } from "../types";

const iso = (value: Date) => format(value, "yyyy-MM-dd");

export default function DateRangePicker({
  value,
  min,
  onChange,
}: {
  value: DateRangeValue;
  min?: string;
  onChange: (value: DateRangeValue) => void;
}) {
  const today = new Date();
  const presets = [
    {
      label: "1M",
      range: { start: iso(subMonths(today, 1)), end: iso(today) },
    },
    {
      label: "3M",
      range: { start: iso(subMonths(today, 3)), end: iso(today) },
    },
    {
      label: "6M",
      range: { start: iso(subMonths(today, 6)), end: iso(today) },
    },
    {
      label: "YTD",
      range: { start: iso(startOfYear(today)), end: iso(today) },
    },
    {
      label: "Tout",
      range: { start: min ?? iso(startOfYear(today)), end: iso(today) },
    },
  ];
  return (
    <div className="date-range" aria-label="Période d’analyse">
      <CalendarDays size={17} />
      <div className="date-presets">
        {presets.map((preset) => (
          <button
            key={preset.label}
            className="date-preset"
            onClick={() => onChange(preset.range)}
            type="button"
          >
            {preset.label}
          </button>
        ))}
      </div>
      <label>
        <span>Du</span>
        <input
          type="date"
          value={value.start}
          min={min}
          max={value.end}
          onChange={(event) =>
            onChange({ ...value, start: event.target.value })
          }
        />
      </label>
      <label>
        <span>au</span>
        <input
          type="date"
          value={value.end}
          min={value.start}
          onChange={(event) => onChange({ ...value, end: event.target.value })}
        />
      </label>
    </div>
  );
}
