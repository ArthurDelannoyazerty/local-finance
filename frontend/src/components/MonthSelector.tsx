import { monthLabel, toggleMonths } from "../months";
import "./MonthSelector.css";

export default function MonthSelector({
  months,
  selected,
  today,
  onChange,
}: {
  months: string[];
  selected: string[];
  today: string;
  onChange: (months: string[]) => void;
}) {
  const chosen = new Set(selected);
  const years = [
    ...new Set(months.map((month) => month.slice(0, 4))),
  ].reverse();
  return (
    <details className="month-selector">
      <summary>Choisir les mois · {selected.length} sélectionnés</summary>
      <div className="month-selector-body">
        <div className="month-presets">
          <button
            type="button"
            className="button button-ghost"
            onClick={() =>
              onChange(
                months.filter((month) => month <= today.slice(0, 7)).slice(-3),
              )
            }
          >
            3 derniers mois
          </button>
          <button
            type="button"
            className="button button-ghost"
            onClick={() =>
              onChange(
                months.filter((month) => month.startsWith(today.slice(0, 4))),
              )
            }
          >
            Cette année
          </button>
          <button
            type="button"
            className="button button-ghost"
            onClick={() => onChange(months)}
          >
            Tout
          </button>
          <button
            type="button"
            className="button button-ghost"
            onClick={() => onChange([])}
          >
            Aucun
          </button>
        </div>
        <div className="month-years">
          {years.map((year) => {
            const group = months.filter((month) => month.startsWith(year));
            const all = group.every((month) => chosen.has(month));
            return (
              <fieldset key={year}>
                <legend>{year}</legend>
                <button
                  type="button"
                  className="button button-ghost"
                  onClick={() => onChange(toggleMonths(selected, group, !all))}
                >
                  {all ? "Décocher" : "Cocher"} {year}
                </button>
                <div className="month-checkboxes">
                  {group.map((month) => (
                    <label key={month}>
                      <input
                        type="checkbox"
                        checked={chosen.has(month)}
                        onChange={(event) =>
                          onChange(
                            toggleMonths(
                              selected,
                              [month],
                              event.target.checked,
                            ),
                          )
                        }
                      />
                      {monthLabel(month)}
                    </label>
                  ))}
                </div>
              </fieldset>
            );
          })}
        </div>
      </div>
    </details>
  );
}
