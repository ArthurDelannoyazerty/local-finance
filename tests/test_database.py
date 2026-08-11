from __future__ import annotations

import sqlite3
from pathlib import Path

from local_finance.db import Database


def test_legacy_database_is_backed_up_and_migrated(tmp_path: Path) -> None:
    path = tmp_path / "finance.db"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE accounts (
                name TEXT PRIMARY KEY,
                initial_balance REAL DEFAULT 0,
                is_visible INTEGER DEFAULT 1
            );
            CREATE TABLE transactions (
                id TEXT PRIMARY KEY, date TEXT, category TEXT, account TEXT,
                amount REAL, currency TEXT, comment TEXT, type TEXT,
                is_excluded INTEGER DEFAULT 0
            );
            CREATE TABLE transfers (
                id TEXT PRIMARY KEY, date TEXT, source_account TEXT,
                target_account TEXT, amount REAL, comment TEXT
            );
            CREATE TABLE investments (
                id TEXT PRIMARY KEY, date TEXT, ticker TEXT, name TEXT,
                action TEXT, quantity REAL, unit_price REAL, fees REAL,
                account TEXT, comment TEXT
            );
            CREATE TABLE market_prices (
                date TEXT, ticker TEXT, price REAL, PRIMARY KEY(date, ticker)
            );
            CREATE TABLE projections (
                id TEXT PRIMARY KEY, name TEXT, created_at TEXT,
                parameters_json TEXT
            );
            INSERT INTO accounts VALUES ('Courant', 100, 1);
            """
        )

    database = Database(path)
    database.initialize()

    backups = list((tmp_path / "backups").glob("finance-pre-v2-*.db"))
    assert len(backups) == 1
    with database.read() as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(investments)")}
        assert {"currency", "revision", "updated_at"} <= columns
        assert connection.execute("SELECT initial_balance FROM accounts").fetchone()[0] == 100
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


def test_failed_write_transaction_rolls_back(db) -> None:
    try:
        with db.transaction(immediate=True) as connection:
            connection.execute("INSERT INTO accounts(name, initial_balance) VALUES ('A', 10)")
            raise RuntimeError("stop")
    except RuntimeError:
        pass
    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0] == 0
