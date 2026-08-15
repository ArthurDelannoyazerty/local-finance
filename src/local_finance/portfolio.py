from __future__ import annotations

import sqlite3
import threading
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

from .db import Database, database, row_to_dict, utc_now

_market_refresh_lock = threading.Lock()


def _iso(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _visible_accounts(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        row_to_dict(row)
        for row in connection.execute(
            """
            SELECT name, initial_balance, opening_balance_date
            FROM accounts WHERE is_visible = 1 ORDER BY name
            """
        )
    ]


def _event_data(connection: sqlite3.Connection, end: date) -> dict[str, Any]:
    accounts = _visible_accounts(connection)
    names = [account["name"] for account in accounts]
    if not names:
        return {"accounts": [], "transactions": [], "transfers": [], "trades": [], "prices": []}
    placeholders = ",".join("?" for _ in names)
    end_text = end.isoformat()
    transactions = connection.execute(
        f"""
        SELECT date, account, amount, type FROM transactions
        WHERE is_excluded = 0 AND account IN ({placeholders}) AND date <= ?
        ORDER BY date, id
        """,
        [*names, end_text],
    ).fetchall()
    transfers = connection.execute(
        f"""
        SELECT date, source_account, target_account, amount FROM transfers
        WHERE date <= ? AND (
            source_account IN ({placeholders}) OR target_account IN ({placeholders})
        ) ORDER BY date, id
        """,
        [end_text, *names, *names],
    ).fetchall()
    trades = connection.execute(
        f"""
        SELECT date, account, ticker, name, action, quantity, unit_price, fees,
               currency
        FROM investments WHERE account IN ({placeholders}) AND date <= ?
        ORDER BY date, CASE action WHEN 'BUY' THEN 0 ELSE 1 END, id
        """,
        [*names, end_text],
    ).fetchall()
    tickers = sorted({str(row["ticker"]) for row in trades})
    prices: list[sqlite3.Row] = []
    if tickers:
        ticker_placeholders = ",".join("?" for _ in tickers)
        prices = connection.execute(
            f"""
            SELECT date, ticker, price, currency FROM market_prices
            WHERE ticker IN ({ticker_placeholders}) AND date <= ?
            ORDER BY ticker, date
            """,
            [*tickers, end_text],
        ).fetchall()
    return {
        "accounts": accounts,
        "transactions": transactions,
        "transfers": transfers,
        "trades": trades,
        "prices": prices,
    }


def _price_series(rows: list[sqlite3.Row]) -> dict[str, list[tuple[date, float]]]:
    result: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for row in rows:
        result[str(row["ticker"])].append(
            (date.fromisoformat(str(row["date"])), float(row["price"]))
        )
    return result


def _latest_price(
    ticker: str,
    target: date,
    prices: dict[str, list[tuple[date, float]]],
    fallback: dict[str, float],
) -> float:
    available = prices.get(ticker, [])
    for price_date, value in reversed(available):
        if price_date <= target:
            return value
    return fallback.get(ticker, 0.0)


def _simulation_start(data: dict[str, Any], requested: date) -> date:
    dates: list[date] = [requested]
    for account in data["accounts"]:
        if account["opening_balance_date"]:
            dates.append(date.fromisoformat(str(account["opening_balance_date"])))
    for collection in (data["transactions"], data["transfers"], data["trades"]):
        dates.extend(date.fromisoformat(str(row["date"])) for row in collection)
    return min(dates)


def _account_opening_dates(
    data: dict[str, Any],
    fallback: date,
) -> dict[str, date]:
    account_names = {str(account["name"]) for account in data["accounts"]}
    first_event: dict[str, date] = {}
    for row in [*data["transactions"], *data["trades"]]:
        account = str(row["account"])
        event_date = date.fromisoformat(str(row["date"]))
        first_event[account] = min(first_event.get(account, event_date), event_date)
    for row in data["transfers"]:
        event_date = date.fromisoformat(str(row["date"]))
        for account in (str(row["source_account"]), str(row["target_account"])):
            if account in account_names:
                first_event[account] = min(first_event.get(account, event_date), event_date)
    return {
        str(account["name"]): (
            date.fromisoformat(str(account["opening_balance_date"]))
            if account["opening_balance_date"]
            else first_event.get(str(account["name"]), fallback)
        )
        for account in data["accounts"]
    }


def wealth_evolution(
    start: date,
    end: date,
    *,
    db: Database = database,
) -> dict[str, Any]:
    with db.read() as connection:
        data = _event_data(connection, end)
    if not data["accounts"]:
        return {"items": [], "accounts": []}

    sim_start = _simulation_start(data, start)
    account_names = [str(account["name"]) for account in data["accounts"]]
    account_openings: dict[date, list[dict[str, Any]]] = defaultdict(list)
    opening_dates = _account_opening_dates(data, sim_start)
    for account in data["accounts"]:
        account_openings[opening_dates[str(account["name"])]].append(account)

    transactions_by_date: dict[date, list[sqlite3.Row]] = defaultdict(list)
    transfers_by_date: dict[date, list[sqlite3.Row]] = defaultdict(list)
    trades_by_date: dict[date, list[sqlite3.Row]] = defaultdict(list)
    for row in data["transactions"]:
        transactions_by_date[date.fromisoformat(str(row["date"]))].append(row)
    for row in data["transfers"]:
        transfers_by_date[date.fromisoformat(str(row["date"]))].append(row)
    for row in data["trades"]:
        trades_by_date[date.fromisoformat(str(row["date"]))].append(row)

    prices = _price_series(data["prices"])
    cash = {account: 0.0 for account in account_names}
    holdings: dict[str, dict[str, float]] = {
        account: defaultdict(float) for account in account_names
    }
    fallback_prices: dict[str, float] = {}
    output: list[dict[str, Any]] = []
    day = sim_start
    while day <= end:
        for account in account_openings.get(day, []):
            cash[str(account["name"])] += float(account["initial_balance"])
        for row in transactions_by_date.get(day, []):
            multiplier = 1 if row["type"] == "INCOME" else -1
            cash[str(row["account"])] += multiplier * float(row["amount"])
        for row in transfers_by_date.get(day, []):
            source, target, amount = (
                str(row["source_account"]),
                str(row["target_account"]),
                float(row["amount"]),
            )
            if source in cash:
                cash[source] -= amount
            if target in cash:
                cash[target] += amount
        for row in trades_by_date.get(day, []):
            account, ticker = str(row["account"]), str(row["ticker"])
            quantity, price, fees = (
                float(row["quantity"]),
                float(row["unit_price"]),
                float(row["fees"]),
            )
            fallback_prices[ticker] = price
            if row["action"] == "BUY":
                cash[account] -= quantity * price + fees
                holdings[account][ticker] += quantity
            else:
                cash[account] += quantity * price - fees
                holdings[account][ticker] -= quantity

        if day >= start:
            account_values: dict[str, float] = {}
            investment_total = 0.0
            for account in account_names:
                investment_value = sum(
                    quantity * _latest_price(ticker, day, prices, fallback_prices)
                    for ticker, quantity in holdings[account].items()
                    if abs(quantity) > 1e-9
                )
                account_values[account] = cash[account] + investment_value
                investment_total += investment_value
            output.append(
                {
                    "date": day.isoformat(),
                    "total_wealth": sum(account_values.values()),
                    "total_investment": investment_total,
                    "accounts": account_values,
                }
            )
        day += timedelta(days=1)
    return {"items": output, "accounts": account_names}


def portfolio_snapshot(
    target: date,
    *,
    db: Database = database,
) -> list[dict[str, Any]]:
    with db.read() as connection:
        data = _event_data(connection, target)
    if not data["accounts"]:
        return []
    start = _simulation_start(data, target)
    account_names = [str(account["name"]) for account in data["accounts"]]
    cash = {account: 0.0 for account in account_names}
    opening_dates = _account_opening_dates(data, start)
    for account in data["accounts"]:
        if opening_dates[str(account["name"])] <= target:
            cash[str(account["name"])] += float(account["initial_balance"])
    for row in data["transactions"]:
        multiplier = 1 if row["type"] == "INCOME" else -1
        cash[str(row["account"])] += multiplier * float(row["amount"])
    for row in data["transfers"]:
        amount = float(row["amount"])
        if row["source_account"] in cash:
            cash[str(row["source_account"])] -= amount
        if row["target_account"] in cash:
            cash[str(row["target_account"])] += amount

    holdings: dict[tuple[str, str], float] = defaultdict(float)
    names: dict[str, str] = {}
    currencies: dict[str, str] = {}
    fallback: dict[str, float] = {}
    for row in data["trades"]:
        account, ticker = str(row["account"]), str(row["ticker"])
        quantity, price, fees = float(row["quantity"]), float(row["unit_price"]), float(row["fees"])
        names[ticker], currencies[ticker], fallback[ticker] = (
            str(row["name"]),
            str(row["currency"]),
            price,
        )
        if row["action"] == "BUY":
            cash[account] -= quantity * price + fees
            holdings[(account, ticker)] += quantity
        else:
            cash[account] += quantity * price - fees
            holdings[(account, ticker)] -= quantity

    prices = _price_series(data["prices"])
    rows: list[dict[str, Any]] = [
        {
            "account": account,
            "type": "CASH",
            "ticker": "CASH",
            "name": "Liquidités",
            "quantity": 1,
            "unit_price": value,
            "value": value,
            "currency": "EUR",
        }
        for account, value in cash.items()
        if abs(value) > 0.005
    ]
    for (account, ticker), quantity in holdings.items():
        if quantity <= 1e-9:
            continue
        price = _latest_price(ticker, target, prices, fallback)
        rows.append(
            {
                "account": account,
                "type": "INVESTMENT",
                "ticker": ticker,
                "name": names.get(ticker, ticker),
                "quantity": quantity,
                "unit_price": price,
                "value": quantity * price,
                "currency": currencies.get(ticker, "EUR"),
            }
        )
    return rows


def portfolio_summary(*, db: Database = database) -> dict[str, Any]:
    snapshot = portfolio_snapshot(datetime.now(UTC).date(), db=db)
    current_value = sum(row["value"] for row in snapshot if row["type"] == "INVESTMENT")
    with db.read() as connection:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN action = 'BUY' THEN quantity * unit_price + fees ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN action = 'SELL' THEN quantity * unit_price - fees ELSE 0 END), 0),
                MAX(updated_at)
            FROM investments
            WHERE account IN (SELECT name FROM accounts WHERE is_visible = 1)
            """
        ).fetchone()
        market = connection.execute(
            "SELECT MAX(fetched_at), MAX(date) FROM market_prices"
        ).fetchone()
    net_invested = float(row[0]) - float(row[1])
    pnl = current_value - net_invested
    return {
        "net_invested": net_invested,
        "current_value": current_value,
        "pnl": pnl,
        "pnl_percent": pnl / net_invested * 100 if net_invested > 0 else None,
        "total_wealth": sum(item["value"] for item in snapshot),
        "market_data": {"fetched_at": market[0], "latest_price_date": market[1]},
    }


def allocation_snapshot(
    target: date,
    start: date | None = None,
    *,
    db: Database = database,
) -> dict[str, Any]:
    end_rows = portfolio_snapshot(target, db=db)
    start_rows = portfolio_snapshot(start, db=db) if start else []
    start_values = {(row["account"], row["ticker"]): row["value"] for row in start_rows}
    flows: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"buys": 0.0, "sells": 0.0}
    )
    if start:
        with db.read() as connection:
            rows = connection.execute(
                """
                SELECT account, ticker, action, quantity, unit_price, fees
                FROM investments WHERE date > ? AND date <= ?
                """,
                (start.isoformat(), target.isoformat()),
            ).fetchall()
        for row in rows:
            key = (str(row["account"]), str(row["ticker"]))
            if row["action"] == "BUY":
                flows[key]["buys"] += float(row["quantity"]) * float(row["unit_price"]) + float(
                    row["fees"]
                )
            else:
                flows[key]["sells"] += float(row["quantity"]) * float(row["unit_price"]) - float(
                    row["fees"]
                )
    items: list[dict[str, Any]] = []
    for row in end_rows:
        key = (row["account"], row["ticker"])
        start_value = start_values.get(key, 0.0)
        flow = flows[key]
        denominator = start_value + flow["buys"]
        profit = row["value"] + flow["sells"] - start_value - flow["buys"]
        items.append(
            {
                **row,
                "start_value": start_value,
                "net_contribution": flow["buys"] - flow["sells"],
                "performance_percent": profit / denominator * 100
                if denominator > 0 and row["type"] == "INVESTMENT"
                else None,
            }
        )
    return {
        "date": target.isoformat(),
        "start": start.isoformat() if start else None,
        "items": items,
    }


def refresh_market_data(*, db: Database = database) -> dict[str, Any]:
    if not _market_refresh_lock.acquire(blocking=False):
        raise RuntimeError("A market-data refresh is already running")
    try:
        with db.read() as connection:
            tickers = [
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT ticker FROM investments ORDER BY ticker"
                )
            ]
            latest = {
                row[0]: row[1]
                for row in connection.execute(
                    "SELECT ticker, MAX(date) FROM market_prices GROUP BY ticker"
                )
            }
            first_trades = {
                row[0]: row[1]
                for row in connection.execute(
                    "SELECT ticker, MIN(date) FROM investments GROUP BY ticker"
                )
            }
            currencies = {
                row[0]: row[1]
                for row in connection.execute(
                    "SELECT ticker, MAX(currency) FROM investments GROUP BY ticker"
                )
            }
        now = utc_now()
        today = datetime.now(UTC).date()
        result = {"updated": {}, "errors": {}}
        for ticker in tickers:
            start = (
                date.fromisoformat(latest[ticker]) + timedelta(days=1)
                if latest.get(ticker)
                else date.fromisoformat(first_trades[ticker])
            )
            if start > today:
                result["updated"][ticker] = 0
                continue
            try:
                instrument = yf.Ticker(ticker)
                quote_currency = str(instrument.fast_info.currency).upper()
                expected_currency = str(currencies.get(ticker, "EUR")).upper()
                if quote_currency != expected_currency:
                    raise ValueError(
                        f"Yahoo quotes {ticker} in {quote_currency}, not {expected_currency}"
                    )
                frame = instrument.history(
                    start=start,
                    end=today + timedelta(days=1),
                    auto_adjust=True,
                    actions=False,
                    raise_errors=True,
                )
                if frame.empty:
                    result["updated"][ticker] = 0
                    continue
                close = frame["Close"]
                records = [
                    (
                        index.date().isoformat() if hasattr(index, "date") else str(index)[:10],
                        ticker,
                        float(value),
                        expected_currency,
                        now,
                    )
                    for index, value in close.items()
                    if not pd.isna(value)
                ]
                with db.transaction(immediate=True) as connection:
                    connection.executemany(
                        """
                        INSERT INTO market_prices(date, ticker, price, currency, fetched_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(date, ticker) DO UPDATE SET
                            price = excluded.price,
                            currency = excluded.currency,
                            fetched_at = excluded.fetched_at
                        """,
                        records,
                    )
                result["updated"][ticker] = len(records)
            except Exception as exc:  # noqa: BLE001 - isolate failures per external ticker
                result["errors"][ticker] = str(exc)
        return result
    finally:
        _market_refresh_lock.release()
