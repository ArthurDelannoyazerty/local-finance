export type DateRangeValue = { start: string; end: string };

export type Account = {
  name: string;
  initial_balance: number;
  opening_balance_date: string | null;
  is_visible: boolean;
  revision: number;
  updated_at: string | null;
};

export type Trade = {
  id: string;
  date: string;
  ticker: string;
  name: string;
  action: "BUY" | "SELL";
  quantity: number;
  unit_price: number;
  fees: number;
  currency: string;
  account: string;
  comment: string;
  revision: number;
  created_at: string;
  updated_at: string;
};

export type Transaction = {
  id: string;
  date: string;
  category: string;
  account: string;
  amount: number;
  currency: string;
  comment: string;
  type: "INCOME" | "EXPENSE";
  imported_at: string | null;
};

export type LifeEvent = { name: string; year: number; amount: number };

export type ProjectionInput = {
  current_age: number;
  retirement_age: number;
  start_capital: number;
  monthly_savings: number;
  monthly_expenses: number;
  years: number;
  annual_return_rate: number;
  inflation_rate: number;
  salary_growth_rate: number;
  tax_rate: number;
  volatility: number;
  stop_working_age: number | null;
  life_events: LifeEvent[];
  show_real: boolean;
  simulations: number;
  seed: number | null;
};
