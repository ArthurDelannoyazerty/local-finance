"""Additive broker-import tables; deliberately separate from Android-owned data."""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime


def migrate_brokers(connection: sqlite3.Connection) -> None:
    # Do not use executescript: it would implicitly commit the surrounding migration.
    statements = """
    CREATE TABLE IF NOT EXISTS broker_batches (
        id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        account TEXT NOT NULL REFERENCES accounts(name),
        filename TEXT NOT NULL,
        state_hash TEXT NOT NULL,
        preview_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        applied_at TEXT
    );
    CREATE TABLE IF NOT EXISTS broker_cash_movements (
        id TEXT PRIMARY KEY,
        date TEXT NOT NULL,
        account TEXT NOT NULL REFERENCES accounts(name),
        kind TEXT NOT NULL CHECK(kind IN ('DEPOSIT', 'FEE_REFUND')),
        amount REAL NOT NULL CHECK(amount > 0),
        description TEXT NOT NULL,
        created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS broker_receipts (
        source TEXT NOT NULL,
        account TEXT NOT NULL REFERENCES accounts(name),
        fingerprint TEXT NOT NULL,
        occurrence INTEGER NOT NULL CHECK(occurrence > 0),
        batch_id TEXT NOT NULL REFERENCES broker_batches(id),
        trade_id TEXT REFERENCES investments(id) ON DELETE SET NULL,
        cash_id TEXT REFERENCES broker_cash_movements(id) ON DELETE SET NULL,
        row_json TEXT NOT NULL,
        PRIMARY KEY(source, account, fingerprint, occurrence)
    );
    CREATE TABLE IF NOT EXISTS broker_mappings (
        source TEXT NOT NULL,
        account TEXT NOT NULL REFERENCES accounts(name),
        instrument TEXT NOT NULL,
        ticker TEXT NOT NULL,
        PRIMARY KEY(source, account, instrument)
    );
    CREATE INDEX IF NOT EXISTS idx_broker_cash_date
        ON broker_cash_movements(account, date);
    """
    for statement in statements.split(";"):
        if statement.strip():
            connection.execute(statement)
    connection.execute(
        "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (3, ?)",
        (datetime.now(UTC).isoformat(),),
    )
