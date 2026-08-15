from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from local_finance.ledger import (
    InventoryError,
    RevisionConflict,
    create_account,
    create_trade,
    export_trades,
    update_account,
    update_trade,
)
from local_finance.portfolio import (
    allocation_snapshot,
    portfolio_snapshot,
    refresh_market_data,
    wealth_evolution,
)
from local_finance.projections import (
    calculate_monte_carlo,
    calculate_projection,
    list_scenarios,
)
from local_finance.schemas import (
    AccountCreate,
    AccountUpdate,
    ProjectionRequest,
    TradeInput,
    TradeUpdate,
)


def test_trade_inventory_revision_and_exports(db) -> None:
    create_account(AccountCreate(name="PEA"), db=db)
    buy = create_trade(
        TradeInput(
            date=date(2026, 1, 1),
            ticker="cw8.pa",
            name="World",
            action="BUY",
            quantity=2,
            unit_price=500,
            fees=1,
            account="PEA",
        ),
        db=db,
    )
    with pytest.raises(InventoryError):
        create_trade(
            TradeInput(
                date=date(2026, 1, 2),
                ticker="CW8.PA",
                name="World",
                action="SELL",
                quantity=3,
                unit_price=510,
                fees=1,
                account="PEA",
            ),
            db=db,
        )
    updated = update_trade(
        buy["id"],
        TradeUpdate(
            date=date(2026, 1, 1),
            ticker="CW8.PA",
            name="World",
            action="BUY",
            quantity=3,
            unit_price=500,
            fees=1,
            account="PEA",
            revision=1,
        ),
        db=db,
    )
    assert updated["revision"] == 2
    with pytest.raises(RevisionConflict):
        update_trade(
            buy["id"],
            TradeUpdate(
                date=date(2026, 1, 1),
                ticker="CW8.PA",
                name="World",
                action="BUY",
                quantity=3,
                unit_price=500,
                fees=1,
                account="PEA",
                revision=1,
            ),
            db=db,
        )
    assert export_trades(file_format="csv", db=db).startswith(b"\xef\xbb\xbfDate")
    assert export_trades(file_format="xlsx", db=db).startswith(b"PK")


def test_market_refresh_rejects_a_quote_in_another_currency(db, monkeypatch) -> None:
    create_account(AccountCreate(name="PEA"), db=db)
    create_trade(
        TradeInput(
            date=date(2026, 1, 1),
            ticker="AAPL",
            name="Apple",
            action="BUY",
            quantity=1,
            unit_price=200,
            account="PEA",
        ),
        db=db,
    )

    class ForeignTicker:
        fast_info = SimpleNamespace(currency="USD")

        def history(self, **_kwargs):
            raise AssertionError("History must not be stored with the wrong currency")

    monkeypatch.setattr("local_finance.portfolio.yf.Ticker", lambda _ticker: ForeignTicker())
    result = refresh_market_data(db=db)
    assert "AAPL" in result["errors"]
    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM market_prices").fetchone()[0] == 0


def test_market_refresh_backfills_older_trade_and_moves_the_investment_curve(
    db,
    monkeypatch,
) -> None:
    create_account(
        AccountCreate(
            name="PEA",
            initial_balance=1000,
            opening_balance_date=date(2026, 1, 1),
        ),
        db=db,
    )
    create_trade(
        TradeInput(
            date=date(2026, 1, 1),
            ticker="CW8.PA",
            name="World",
            action="BUY",
            quantity=1,
            unit_price=100,
            account="PEA",
        ),
        db=db,
    )
    with db.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO market_prices(date, ticker, price, currency, fetched_at)
            VALUES ('2026-01-20', 'CW8.PA', 110, 'EUR', '2026-01-20T00:00:00Z')
            """
        )

    history_calls = []

    class EuroTicker:
        fast_info = SimpleNamespace(currency="EUR")

        def history(self, **kwargs):
            history_calls.append(kwargs)
            return pd.DataFrame(
                {"Close": [101.0, 103.0]},
                index=pd.to_datetime(["2026-01-01", "2026-01-02"]),
            )

    monkeypatch.setattr("local_finance.portfolio.yf.Ticker", lambda _ticker: EuroTicker())
    result = refresh_market_data(db=db)

    assert result["errors"] == {}
    assert result["updated"]["CW8.PA"] == 2
    assert history_calls[0]["start"] == date(2026, 1, 1)
    assert history_calls[0]["repair"] is True
    evolution = wealth_evolution(date(2026, 1, 1), date(2026, 1, 2), db=db)
    assert [item["total_investment"] for item in evolution["items"]] == [101, 103]


def test_market_refresh_reports_an_empty_yahoo_result_as_an_error(db, monkeypatch) -> None:
    create_account(AccountCreate(name="PEA"), db=db)
    create_trade(
        TradeInput(
            date=date(2026, 1, 1),
            ticker="NOT-A-TICKER",
            name="Invalid",
            action="BUY",
            quantity=1,
            unit_price=1,
            account="PEA",
        ),
        db=db,
    )

    class EmptyTicker:
        fast_info = SimpleNamespace(currency="EUR")

        def history(self, **_kwargs):
            return pd.DataFrame({"Close": []})

    monkeypatch.setattr("local_finance.portfolio.yf.Ticker", lambda _ticker: EmptyTicker())
    result = refresh_market_data(db=db)

    assert "NOT-A-TICKER" in result["errors"]
    assert "full symbol" in result["errors"]["NOT-A-TICKER"]


def test_allocation_reports_cash_flow_adjusted_absolute_and_percent_performance(db) -> None:
    create_account(
        AccountCreate(
            name="PEA",
            initial_balance=1000,
            opening_balance_date=date(2026, 1, 1),
        ),
        db=db,
    )
    create_trade(
        TradeInput(
            date=date(2026, 1, 1),
            ticker="CW8.PA",
            name="World",
            action="BUY",
            quantity=2,
            unit_price=100,
            account="PEA",
        ),
        db=db,
    )
    create_trade(
        TradeInput(
            date=date(2026, 1, 20),
            ticker="CW8.PA",
            name="World",
            action="BUY",
            quantity=1,
            unit_price=120,
            account="PEA",
        ),
        db=db,
    )
    with db.transaction(immediate=True) as connection:
        connection.executemany(
            """
            INSERT INTO market_prices(date, ticker, price, currency)
            VALUES (?, 'CW8.PA', ?, 'EUR')
            """,
            [("2026-01-10", 110), ("2026-02-10", 125)],
        )

    result = allocation_snapshot(date(2026, 2, 10), date(2026, 1, 10), db=db)
    world = next(item for item in result["items"] if item["ticker"] == "CW8.PA")
    assert world["value"] == pytest.approx(375)
    assert world["start_value"] == pytest.approx(220)
    assert world["net_contribution"] == pytest.approx(120)
    assert world["performance_absolute"] == pytest.approx(35)
    assert world["performance_percent"] == pytest.approx(35 / 340 * 100)


def test_opening_dates_and_transfers_to_hidden_accounts_are_consistent(db) -> None:
    create_account(
        AccountCreate(
            name="Courant",
            initial_balance=1000,
            opening_balance_date=date(2026, 1, 10),
            is_visible=True,
        ),
        db=db,
    )
    create_account(
        AccountCreate(
            name="Caché",
            initial_balance=5000,
            opening_balance_date=date(2026, 1, 1),
            is_visible=False,
        ),
        db=db,
    )
    with db.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO transfers(id, date, source_account, target_account, amount, comment)
            VALUES ('t1', '2026-01-11', 'Courant', 'Caché', 100, '')
            """
        )
    result = wealth_evolution(date(2026, 1, 1), date(2026, 1, 12), db=db)
    by_date = {row["date"]: row for row in result["items"]}
    assert by_date["2026-01-09"]["total_wealth"] == 0
    assert by_date["2026-01-10"]["total_wealth"] == 1000
    assert by_date["2026-01-11"]["total_wealth"] == 900
    snapshot = portfolio_snapshot(date(2026, 1, 9), db=db)
    assert not any(item["account"] == "Courant" for item in snapshot)

    with pytest.raises(ValueError, match="first Courant operation"):
        update_account(
            "Courant",
            AccountUpdate(
                initial_balance=1000,
                opening_balance_date=date(2026, 1, 12),
                is_visible=True,
                revision=1,
            ),
            db=db,
        )


def projection_payload() -> ProjectionRequest:
    return ProjectionRequest(
        current_age=30,
        retirement_age=65,
        start_capital=10000,
        monthly_savings=500,
        monthly_expenses=1500,
        years=20,
        annual_return_rate=0.07,
        inflation_rate=0.02,
        salary_growth_rate=0.01,
        tax_rate=0.3,
        volatility=0.15,
        simulations=100,
        seed=7,
    )


def test_projection_and_monte_carlo_are_complete_and_reproducible() -> None:
    payload = projection_payload()
    deterministic = calculate_projection(payload)
    assert len(deterministic["items"]) == 240
    assert deterministic["metrics"]["final_wealth"] > payload.start_capital
    first = calculate_monte_carlo(payload)
    second = calculate_monte_carlo(payload)
    assert first == second
    nominal = calculate_monte_carlo(payload.model_copy(update={"show_real": False}))
    assert nominal["metrics"]["success_probability"] == first["metrics"]["success_probability"]
    assert len(first["items"]) == 240
    assert 0 <= first["metrics"]["success_probability"] <= 100


def test_legacy_projection_scenario_gets_safe_defaults(db) -> None:
    with db.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO projections(id, name, created_at, parameters_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                "legacy",
                "Ancien",
                "2025-01-01T00:00:00Z",
                json.dumps(
                    {
                        "start_capital": 10000,
                        "monthly_savings": 700,
                        "years": 25,
                        "annual_return_rate": 0.06,
                        "inflation_rate": 0.02,
                        "salary_growth_rate": 0.01,
                        "volatility": 0.15,
                        "life_events": [],
                    }
                ),
            ),
        )
    scenario = list_scenarios(db=db)[0]
    assert scenario["parameters"]["start_capital"] == 10000
    assert scenario["parameters"]["current_age"] == 30
    assert scenario["parameters"]["tax_rate"] == pytest.approx(0.3)
