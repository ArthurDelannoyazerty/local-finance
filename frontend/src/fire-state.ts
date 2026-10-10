import type { ProjectionInput } from "./types";

export const initialProjection: ProjectionInput = {
  current_age: 30,
  retirement_age: 65,
  start_capital: 0,
  monthly_savings: 500,
  monthly_expenses: 1500,
  years: 40,
  annual_return_rate: 0.07,
  inflation_rate: 0.02,
  salary_growth_rate: 0.015,
  tax_rate: 0.3,
  volatility: 0.15,
  stop_working_age: null,
  life_events: [],
  show_real: true,
  simulations: 300,
  seed: 42,
};

export function roundMoney(value: number): number {
  if (!Number.isFinite(value)) return value;
  return (
    (Math.sign(value) * Math.round((Math.abs(value) + Number.EPSILON) * 100)) /
    100
  );
}

/** All entry points (defaults, scenarios and edits) use the same precision. */
export function normalizeProjection(value: ProjectionInput): ProjectionInput {
  return {
    ...value,
    monthly_savings: roundMoney(value.monthly_savings),
    monthly_expenses: roundMoney(value.monthly_expenses),
    start_capital: roundMoney(value.start_capital),
  };
}

export function updateProjection<K extends keyof ProjectionInput>(
  current: ProjectionInput,
  key: K,
  value: ProjectionInput[K],
): ProjectionInput {
  const next = normalizeProjection({ ...current, [key]: value });
  next.retirement_age = Math.max(next.current_age, next.retirement_age);
  // Preserve intermediate input such as "3" while the user is typing "35".
  // Validation pauses requests; only profile changes clear an invalid age.
  if (
    (key === "current_age" || key === "years") &&
    next.stop_working_age !== null &&
    (next.stop_working_age < next.current_age ||
      next.stop_working_age > next.current_age + next.years)
  ) {
    next.stop_working_age = null;
  }
  next.life_events = next.life_events.filter((item) => item.year <= next.years);
  return next;
}

export function projectionError(value: ProjectionInput): string | null {
  const limits: [number, number, number][] = [
    [value.current_age, 18, 90],
    [value.retirement_age, value.current_age, 100],
    [value.years, 1, 80],
    [value.start_capital, 0, Number.MAX_SAFE_INTEGER],
    [value.monthly_savings, 0, Number.MAX_SAFE_INTEGER],
    [value.monthly_expenses, 0, Number.MAX_SAFE_INTEGER],
    [value.annual_return_rate, -0.5, 0.5],
    [value.inflation_rate, -0.1, 0.2],
    [value.salary_growth_rate, -0.2, 0.3],
    [value.tax_rate, 0, 1],
    [value.volatility, 0, 1],
    [value.simulations, 10, 5000],
  ];
  if (
    limits.some(
      ([number, min, max]) =>
        !Number.isFinite(number) || number < min || number > max,
    )
  ) {
    return "Vérifiez les montants et les limites des hypothèses.";
  }
  if (
    ![
      value.current_age,
      value.retirement_age,
      value.years,
      value.simulations,
    ].every(Number.isInteger)
  ) {
    return "Les âges, l’horizon et le nombre de simulations doivent être entiers.";
  }
  if (
    value.stop_working_age !== null &&
    (!Number.isInteger(value.stop_working_age) ||
      value.stop_working_age < value.current_age ||
      value.stop_working_age > value.current_age + value.years)
  ) {
    return "L’âge d’arrêt doit se situer dans l’horizon choisi.";
  }
  if (
    value.life_events.some(
      (event) =>
        !event.name.trim() ||
        event.name.length > 120 ||
        !Number.isFinite(event.amount) ||
        !Number.isFinite(event.year) ||
        event.year <= 0 ||
        event.year > value.years,
    )
  ) {
    return "Vérifiez le nom, l’année et le montant des événements.";
  }
  return null;
}

/** Only Monte Carlo waits briefly; deterministic projections request immediately. */
export function waitForQuiet(
  signal: AbortSignal,
  milliseconds = 150,
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const abort = () => {
      clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", abort, { once: true });
  });
}
