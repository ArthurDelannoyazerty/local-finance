from __future__ import annotations

import json
import math
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np

from .db import Database, database, json_dumps, utc_now
from .portfolio import portfolio_snapshot
from .schemas import ProjectionRequest, ScenarioInput

LEGACY_SCENARIO_DEFAULTS: dict[str, Any] = {
    "current_age": 30,
    "retirement_age": 65,
    "start_capital": 0,
    "monthly_savings": 500,
    "monthly_expenses": 1500,
    "years": 40,
    "annual_return_rate": 0.07,
    "inflation_rate": 0.02,
    "salary_growth_rate": 0.015,
    "tax_rate": 0.3,
    "volatility": 0.15,
    "stop_working_age": None,
    "life_events": [],
    "show_real": True,
    "simulations": 300,
    "seed": 42,
}


def projection_defaults(*, db: Database = database) -> dict[str, Any]:
    today = datetime.now(UTC).date()
    snapshot = portfolio_snapshot(today, db=db)
    wealth = sum(float(row["value"]) for row in snapshot)
    six_months_ago = today - timedelta(days=183)
    with db.read() as connection:
        rows = connection.execute(
            """
            SELECT substr(date, 1, 7) AS month,
                   SUM(CASE WHEN type = 'INCOME' THEN amount ELSE 0 END) AS income,
                   SUM(CASE WHEN type = 'EXPENSE' THEN amount ELSE 0 END) AS expense
            FROM transactions
            WHERE is_excluded = 0 AND date >= ?
              AND account IN (SELECT name FROM accounts WHERE is_visible = 1)
            GROUP BY month ORDER BY month
            """,
            (six_months_ago.isoformat(),),
        ).fetchall()
    month_count = max(1, len(rows))
    income = sum(float(row["income"] or 0) for row in rows)
    expense = sum(float(row["expense"] or 0) for row in rows)
    return {
        "wealth": wealth,
        "monthly_savings": max(0.0, (income - expense) / month_count),
        "monthly_expenses": expense / month_count if expense else 1500.0,
        "months_observed": len(rows),
    }


def _event_map(payload: ProjectionRequest) -> dict[int, float]:
    events: dict[int, float] = {}
    for event in payload.life_events:
        month = max(1, round(event.year * 12))
        events[month] = events.get(month, 0.0) + event.amount
    return events


def calculate_projection(payload: ProjectionRequest) -> dict[str, Any]:
    months = payload.years * 12
    monthly_return = (1 + payload.annual_return_rate) ** (1 / 12) - 1
    monthly_inflation = (1 + payload.inflation_rate) ** (1 / 12) - 1
    stop_month = (
        (payload.stop_working_age - payload.current_age) * 12
        if payload.stop_working_age is not None
        else None
    )
    events = _event_map(payload)
    capital = payload.start_capital
    savings = payload.monthly_savings
    contributed = payload.start_capital
    items: list[dict[str, float]] = []
    tipping_age: float | None = None

    for month in range(1, months + 1):
        growth = capital * monthly_return
        capital += growth
        if stop_month is not None and month >= stop_month:
            withdrawal = payload.monthly_expenses * ((1 + monthly_inflation) ** month)
            capital -= withdrawal
        else:
            capital += savings
            contributed += max(0, savings)
        event_amount = events.get(month, 0.0)
        capital += event_amount
        if event_amount > 0:
            contributed += event_amount
        if month % 12 == 0 and (stop_month is None or month < stop_month):
            savings *= 1 + payload.salary_growth_rate
        if tipping_age is None and growth >= max(0, savings):
            tipping_age = payload.current_age + month / 12

        gains = max(0.0, capital - contributed)
        tax = gains * payload.tax_rate
        nominal_net = capital - tax
        real_net = nominal_net / ((1 + monthly_inflation) ** month)
        items.append(
            {
                "month": month,
                "year": month / 12,
                "age": payload.current_age + month / 12,
                "nominal_capital": capital,
                "real_capital": capital / ((1 + monthly_inflation) ** month),
                "total_contributed": contributed,
                "tax": tax,
                "net_nominal": nominal_net,
                "net_real": real_net,
            }
        )

    target = payload.monthly_expenses * 12 / 0.04
    fire_row = next((row for row in items if row["net_real"] >= target), None)
    final = items[-1]
    return {
        "items": items,
        "metrics": {
            "final_wealth": final["net_real"] if payload.show_real else final["net_nominal"],
            "monthly_rent_4_percent": (
                (final["net_real"] if payload.show_real else final["net_nominal"]) * 0.04 / 12
            ),
            "fire_age": fire_row["age"] if fire_row else None,
            "tipping_age": tipping_age,
            "fire_target": target,
            "lean_fire_target": target * 0.8,
            "fat_fire_target": target * 1.5,
        },
    }


def calculate_monte_carlo(payload: ProjectionRequest) -> dict[str, Any]:
    months = payload.years * 12
    simulations = payload.simulations
    generator = np.random.default_rng(payload.seed)
    dt = 1 / 12
    monthly_inflation = (1 + payload.inflation_rate) ** (1 / 12) - 1
    stop_month = (
        (payload.stop_working_age - payload.current_age) * 12
        if payload.stop_working_age is not None
        else None
    )
    events = _event_map(payload)
    capital = np.full(simulations, payload.start_capital, dtype=float)
    contributed = np.full(simulations, payload.start_capital, dtype=float)
    savings = payload.monthly_savings
    results = np.zeros((months, simulations), dtype=float)

    for index in range(months):
        month = index + 1
        shock = generator.normal(0, 1, simulations)
        growth_factor = np.exp(
            (payload.annual_return_rate - 0.5 * payload.volatility**2) * dt
            + payload.volatility * math.sqrt(dt) * shock
        )
        capital *= growth_factor
        if stop_month is not None and month >= stop_month:
            capital -= payload.monthly_expenses * ((1 + monthly_inflation) ** month)
        else:
            capital += savings
            contributed += max(0, savings)
        event_amount = events.get(month, 0.0)
        capital += event_amount
        if event_amount > 0:
            contributed += event_amount
        if month % 12 == 0 and (stop_month is None or month < stop_month):
            savings *= 1 + payload.salary_growth_rate

        gains = np.maximum(0, capital - contributed)
        net = capital - gains * payload.tax_rate
        if payload.show_real:
            net /= (1 + monthly_inflation) ** month
        results[index] = net

    p10 = np.percentile(results, 10, axis=1)
    p50 = np.percentile(results, 50, axis=1)
    p90 = np.percentile(results, 90, axis=1)
    items = [
        {
            "year": (index + 1) / 12,
            "age": payload.current_age + (index + 1) / 12,
            "p10": float(p10[index]),
            "p50": float(p50[index]),
            "p90": float(p90[index]),
        }
        for index in range(months)
    ]
    fire_target = payload.monthly_expenses * 12 / 0.04
    final_gains = np.maximum(0, capital - contributed)
    final_real = (capital - final_gains * payload.tax_rate) / ((1 + monthly_inflation) ** months)
    success_probability = float(np.mean(final_real >= fire_target) * 100)
    return {
        "items": items,
        "metrics": {
            "success_probability": success_probability,
            "median_final": float(p50[-1]),
            "pessimistic_final": float(p10[-1]),
            "optimistic_final": float(p90[-1]),
        },
    }


def save_scenario(payload: ScenarioInput, *, db: Database = database) -> dict[str, Any]:
    scenario_id, now = str(uuid.uuid4()), utc_now()
    parameters = payload.parameters.model_dump(mode="json")
    with db.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO projections(id, name, created_at, updated_at, parameters_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (scenario_id, payload.name.strip(), now, now, json_dumps(parameters)),
        )
    return {
        "id": scenario_id,
        "name": payload.name.strip(),
        "created_at": now,
        "parameters": parameters,
    }


def list_scenarios(*, db: Database = database) -> list[dict[str, Any]]:
    with db.read() as connection:
        rows = connection.execute(
            """
            SELECT id, name, created_at, updated_at, parameters_json
            FROM projections ORDER BY COALESCE(updated_at, created_at) DESC
            """
        ).fetchall()
    result = []
    for row in rows:
        stored = json.loads(row["parameters_json"])
        parameters = ProjectionRequest.model_validate(
            {**LEGACY_SCENARIO_DEFAULTS, **stored}
        ).model_dump(mode="json")
        result.append(
            {
                "id": row["id"],
                "name": row["name"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "parameters": parameters,
            }
        )
    return result


def delete_scenario(scenario_id: str, *, db: Database = database) -> None:
    with db.transaction(immediate=True) as connection:
        cursor = connection.execute("DELETE FROM projections WHERE id = ?", (scenario_id,))
        if cursor.rowcount != 1:
            raise KeyError("Scenario not found")
