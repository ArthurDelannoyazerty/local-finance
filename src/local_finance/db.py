from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .settings import settings


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class Database:
    """The only gateway to the application's SQLite database."""

    def __init__(self, path: Path | str | None = None) -> None:
        self.path = Path(path or settings.database_path).resolve()
        self._write_lock = threading.RLock()

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            self.path,
            timeout=15,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def read(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        with self._write_lock:
            connection = self.connect()
            try:
                connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    @contextmanager
    def exclusive(self) -> Iterator[None]:
        """Serialize a multi-step write workflow inside this application process."""
        with self._write_lock:
            yield

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
        self._backup_before_first_migration()
        with self.transaction(immediate=True) as connection:
            self._apply_schema(connection)

    def _backup_before_first_migration(self) -> None:
        if not self.path.exists() or self.path.stat().st_size == 0:
            return
        with self.read() as connection:
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        if "schema_migrations" in tables or not tables:
            return
        backup_dir = self.path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        backup_path = backup_dir / f"finance-pre-v2-{timestamp}.db"
        with sqlite3.connect(self.path) as source, sqlite3.connect(backup_path) as target:
            source.backup(target)

    @staticmethod
    def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
        return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}

    @classmethod
    def _add_column(
        cls,
        connection: sqlite3.Connection,
        table: str,
        definition: str,
    ) -> None:
        name = definition.split()[0]
        if name not in cls._columns(connection, table):
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {definition}")

    @staticmethod
    def _execute_script(connection: sqlite3.Connection, script: str) -> None:
        """Execute simple DDL statements without sqlite3.executescript's implicit commit."""
        for statement in script.split(";"):
            if statement.strip():
                connection.execute(statement)

    @classmethod
    def _apply_schema(cls, connection: sqlite3.Connection) -> None:
        cls._execute_script(
            connection,
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS accounts (
                name TEXT PRIMARY KEY,
                initial_balance REAL NOT NULL DEFAULT 0,
                is_visible INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                category TEXT NOT NULL,
                account TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                comment TEXT NOT NULL DEFAULT '',
                type TEXT NOT NULL,
                is_excluded INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS transfers (
                id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                source_account TEXT NOT NULL,
                target_account TEXT NOT NULL,
                amount REAL NOT NULL,
                comment TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS investments (
                id TEXT PRIMARY KEY,
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                name TEXT NOT NULL,
                action TEXT NOT NULL,
                quantity REAL NOT NULL,
                unit_price REAL NOT NULL,
                fees REAL NOT NULL DEFAULT 0,
                account TEXT NOT NULL,
                comment TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS market_prices (
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                price REAL NOT NULL,
                PRIMARY KEY (date, ticker)
            );

            CREATE TABLE IF NOT EXISTS projections (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                parameters_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS import_batches (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                file_sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL,
                applied_at TEXT,
                status TEXT NOT NULL,
                sheets_json TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                base_state_hash TEXT NOT NULL,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS import_rows (
                batch_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                source_key TEXT NOT NULL,
                row_json TEXT NOT NULL,
                PRIMARY KEY (batch_id, kind, source_key),
                FOREIGN KEY (batch_id) REFERENCES import_batches(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS categories (
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                color TEXT,
                PRIMARY KEY (name, type)
            );
            """,
        )

        additions: dict[str, Sequence[str]] = {
            "accounts": (
                "opening_balance_date TEXT",
                "revision INTEGER NOT NULL DEFAULT 1",
                "updated_at TEXT",
            ),
            "transactions": (
                "source_key TEXT",
                "source_occurrence INTEGER NOT NULL DEFAULT 0",
                "import_batch_id TEXT",
                "imported_at TEXT",
            ),
            "transfers": (
                "source_key TEXT",
                "source_occurrence INTEGER NOT NULL DEFAULT 0",
                "import_batch_id TEXT",
                "imported_at TEXT",
            ),
            "investments": (
                "currency TEXT NOT NULL DEFAULT 'EUR'",
                "revision INTEGER NOT NULL DEFAULT 1",
                "created_at TEXT",
                "updated_at TEXT",
            ),
            "market_prices": (
                "currency TEXT NOT NULL DEFAULT 'EUR'",
                "fetched_at TEXT",
            ),
            "projections": ("updated_at TEXT",),
        }
        for table, definitions in additions.items():
            for definition in definitions:
                cls._add_column(connection, table, definition)

        now = utc_now()
        connection.execute("UPDATE accounts SET updated_at = COALESCE(updated_at, ?)", (now,))
        connection.execute(
            "UPDATE investments SET created_at = COALESCE(created_at, ?), "
            "updated_at = COALESCE(updated_at, ?)",
            (now, now),
        )
        connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (2, ?)",
            (now,),
        )
        cls._execute_script(
            connection,
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(date);
            CREATE INDEX IF NOT EXISTS idx_transactions_filters
                ON transactions(type, account, category, date);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_source_key
                ON transactions(source_key) WHERE source_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_transfers_date ON transfers(date);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transfers_source_key
                ON transfers(source_key) WHERE source_key IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_investments_date ON investments(date);
            CREATE INDEX IF NOT EXISTS idx_investments_ticker_account
                ON investments(ticker, account, date);
            CREATE INDEX IF NOT EXISTS idx_import_batches_created
                ON import_batches(created_at DESC);
            """,
        )

    def backup(self, label: str = "manual") -> Path:
        if not self.path.exists():
            raise FileNotFoundError("The finance database does not exist yet")
        backup_dir = self.path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        safe_label = "".join(c for c in label if c.isalnum() or c in "-_") or "backup"
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        target_path = backup_dir / f"finance-{safe_label}-{timestamp}.db"
        with (
            self._write_lock,
            sqlite3.connect(self.path) as source,
            sqlite3.connect(target_path) as target,
        ):
            source.backup(target)
        return target_path


database = Database()


def row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return dict(row)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
