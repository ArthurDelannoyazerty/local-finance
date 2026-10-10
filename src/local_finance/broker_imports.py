"""Transactional, occurrence-aware imports into the existing investment ledger."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from collections import Counter, defaultdict
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from .broker_sources import MAX_BYTES, SOURCES, DateFormat, decimal_text, normalized
from .db import Database, database, json_dumps, utc_now
from .instrument_catalog import instrument_suggestions


class StaleBrokerPreview(RuntimeError):
    pass


class BrokerImportRequest(BaseModel):
    source: str = Field(min_length=1, max_length=40)
    account: str = Field(min_length=1, max_length=120)
    csv_text: str = Field(min_length=1, max_length=MAX_BYTES)
    filename: str = Field(default="texte.csv", max_length=255)
    date_format: DateFormat
    deposit_policy: Literal["android", "external"] = "android"
    mappings: dict[str, str] = Field(default_factory=dict, max_length=1000)

    @field_validator("source", "account", mode="before")
    @classmethod
    def trim_required(cls, value: Any) -> Any:
        return value.strip() if isinstance(value, str) else value

    @field_validator("mappings")
    @classmethod
    def validate_mappings(cls, values: dict[str, str]) -> dict[str, str]:
        result = {}
        for key, value in values.items():
            key, value = normalized(key), value.strip().upper()
            if not key or len(key) > 160:
                raise ValueError("Désignation de titre invalide")
            if value and not re.fullmatch(r"[A-Z0-9^][A-Z0-9.^=_-]{0,31}", value):
                raise ValueError("Ticker invalide (32 caractères maximum)")
            if key in result and result[key] != value:
                raise ValueError("Deux associations contradictoires pour le même titre")
            result[key] = value
        return result


def _hash(value: Any) -> str:
    return hashlib.sha256(json_dumps(value).encode("utf-8")).hexdigest()


def _state(connection: sqlite3.Connection) -> str:
    tables = (
        ("accounts", "name"),
        ("investments", "id"),
        ("transfers", "id"),
        ("transactions", "id"),
        ("broker_cash_movements", "id"),
        ("broker_receipts", "source, account, fingerprint, occurrence"),
        ("broker_mappings", "source, account, instrument"),
    )
    return _hash({
        table: [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY {order}")]
        for table, order in tables
    })


def _fingerprint(row: dict[str, Any]) -> str:
    return _hash({key: row[key] for key in (
        "date", "designation", "kind", "quantity", "unit_price", "net"
    )})


def _raw_fingerprint(row: dict[str, Any]) -> str:
    raw_date = row["raw_date"]
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", raw_date):
        raw_date = "/".join(str(int(part)) for part in raw_date.split("/"))
    return _fingerprint({**row, "date": raw_date})


def _trade_key(row: dict[str, Any]) -> tuple:
    return (
        row["date"], row["ticker"], row.get("action", row.get("kind")),
        *(decimal_text(row[key]) for key in ("quantity", "unit_price", "fees")),
    )


def _inventory_errors(existing: list[dict], rows: list[dict]) -> list[str]:
    positions: dict[str, Decimal] = defaultdict(Decimal)
    events = [
        (row["date"], row["ticker"], row["action"], Decimal(str(row["quantity"])))
        for row in existing
    ]
    events.extend(
        (row["date"], row["ticker"], row["kind"], Decimal(row["quantity"]))
        for row in rows if row["status"] == "add" and row["kind"] in {"BUY", "SELL"}
    )
    errors = []
    for day, ticker, kind, quantity in sorted(events):
        # BUY sorts before SELL on the same day, matching the existing ledger.
        positions[ticker] += quantity if kind == "BUY" else -quantity
        if positions[ticker] < Decimal("-0.000000001"):
            errors.append(f"Position négative pour {ticker} le {day} : historique incomplet ou dates incorrectes")
            break
    return errors


def create_broker_preview(request: BrokerImportRequest, *, db: Database = database) -> dict:
    if request.source not in SOURCES:
        raise ValueError("Source d'import inconnue")
    parsed = SOURCES[request.source].parse(request.csv_text, request.date_format)
    with db.transaction(immediate=True) as connection:
        account = connection.execute("SELECT * FROM accounts WHERE name = ?", (request.account,)).fetchone()
        if account is None:
            raise ValueError("Créer ou sélectionner un compte existant")
        receipts = {
            (row["fingerprint"], row["occurrence"]): dict(row)
            for row in connection.execute(
                "SELECT * FROM broker_receipts WHERE source = ? AND account = ?",
                (request.source, request.account),
            )
        }
        raw_dates: dict[str, set[str]] = defaultdict(set)
        for receipt in receipts.values():
            original = json.loads(receipt["row_json"])
            raw_dates[_raw_fingerprint(original)].add(original["date"])
        trades = [dict(row) for row in connection.execute(
            "SELECT * FROM investments WHERE account = ? ORDER BY date, id", (request.account,)
        )]
        mappings = {row["instrument"]: row["ticker"] for row in connection.execute(
            "SELECT instrument, ticker FROM broker_mappings WHERE source = ? AND account = ?",
            (request.source, request.account),
        )}
        named: dict[str, set[str]] = defaultdict(set)
        for trade in trades:
            named[normalized(trade["name"])].add(trade["ticker"])
        for name, tickers in named.items():
            if len(tickers) == 1:
                mappings.setdefault(name, next(iter(tickers)))
        mappings.update(request.mappings)
        claimed = {row[0] for row in connection.execute(
            "SELECT trade_id FROM broker_receipts WHERE trade_id IS NOT NULL"
        )}
        manual: dict[tuple, list[str]] = defaultdict(list)
        for trade in trades:
            if trade["id"] not in claimed:
                manual[_trade_key(trade)].append(trade["id"])
        occurrences: Counter[str] = Counter()
        errors = list(parsed.errors)
        instruments: set[str] = set()
        for row in parsed.rows:
            fingerprint = _fingerprint(row)
            occurrences[fingerprint] += 1
            row.update(fingerprint=fingerprint, occurrence=occurrences[fingerprint], status="add", ticker="")
            receipt = receipts.get((fingerprint, occurrences[fingerprint]))
            if receipt:
                row["status"] = "duplicate"
                row["ticker"] = json.loads(receipt["row_json"]).get("ticker", "")
                continue
            previous = raw_dates.get(_raw_fingerprint(row), set())
            if previous and row["date"] not in previous:
                row["status"] = "error"
                errors.append(f"Ligne {row['line']} : déjà importée avec une autre interprétation de date ({', '.join(sorted(previous))})")
                continue
            if row["kind"] == "DEPOSIT" and request.deposit_policy == "android":
                row["status"] = "ignored"
                continue
            opening = account["opening_balance_date"]
            if opening and row["date"] < opening:
                row["status"] = "error"
                errors.append(f"Ligne {row['line']} : antérieure au solde initial du compte ({opening})")
            if row["kind"] == "DEPOSIT":
                transfer = connection.execute(
                    "SELECT 1 FROM transfers WHERE target_account = ? AND date = ? AND ABS(amount - ?) < 0.000001",
                    (request.account, row["date"], float(row["net"])),
                ).fetchone()
                if transfer:
                    row["status"] = "error"
                    errors.append(f"Ligne {row['line']} : transfert Android correspondant, utiliser le mode Android pour les versements")
            if row["kind"] in {"BUY", "SELL"}:
                instruments.add(row["instrument"])
                row["ticker"] = mappings.get(row["instrument"], "")
                if not row["ticker"]:
                    row["status"] = "error"
                    errors.append(f"Ligne {row['line']} : associer un ticker à {row['instrument']}")
                elif row["status"] == "add":
                    matching = manual[_trade_key(row)]
                    if matching:
                        row["status"], row["trade_id"] = "link", matching.pop(0)
        errors.extend(_inventory_errors(trades, parsed.rows))
        summary = {name: sum(row["status"] == name for row in parsed.rows) for name in (
            "add", "duplicate", "link", "ignored", "error"
        )}
        summary["fees"] = decimal_text(sum((Decimal(row["fees"]) for row in parsed.rows if row["status"] == "add"), Decimal(0)))
        summary["cash_change"] = decimal_text(sum((Decimal(row["net"]) for row in parsed.rows if row["status"] == "add"), Decimal(0)))
        preview = {
            "id": str(uuid.uuid4()), "source": request.source, "account": request.account,
            "date_format": request.date_format, "deposit_policy": request.deposit_policy,
            "rows": parsed.rows, "errors": errors, "warnings": parsed.warnings,
            "instruments": sorted(instruments),
            "suggestions": {
                name: instrument_suggestions(request.source, name)
                for name in sorted(instruments)
            },
            "mappings": {name: mappings.get(name, "") for name in sorted(instruments)},
            "summary": summary,
        }
        preview["warnings"].append(
            "Versements ignorés : Android et le solde initial restent la source de vérité."
            if request.deposit_policy == "android" else
            "Versements externes : ne pas les compter aussi dans Android ou le solde initial. Les rapprochements à des dates différentes ne sont pas automatiques."
        )
        connection.execute(
            "INSERT INTO broker_batches(id, source, account, filename, state_hash, preview_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (preview["id"], request.source, request.account, request.filename, _state(connection), json_dumps(preview), utc_now()),
        )
        return preview


def apply_broker_preview(batch_id: str, *, db: Database = database) -> dict:
    with db.transaction(immediate=True) as connection:
        batch = connection.execute("SELECT * FROM broker_batches WHERE id = ?", (batch_id,)).fetchone()
        if batch is None:
            raise KeyError("Aperçu introuvable")
        preview = json.loads(batch["preview_json"])
        if batch["applied_at"]:
            return {"id": batch_id, "already_applied": True, "summary": preview["summary"]}
        if preview["errors"]:
            raise ValueError("Corriger toutes les erreurs puis comparer à nouveau")
        if _state(connection) != batch["state_hash"]:
            raise StaleBrokerPreview("Le registre a changé : comparer à nouveau avant de confirmer")
        now = utc_now()
        for row in preview["rows"]:
            if row["status"] not in {"add", "link"}:
                continue
            trade_id, cash_id = row.get("trade_id"), None
            if row["status"] == "add":
                record_id = str(uuid.uuid4())
                if row["kind"] in {"BUY", "SELL"}:
                    trade_id = record_id
                    connection.execute(
                        "INSERT INTO investments(id, date, ticker, name, action, quantity, unit_price, fees, currency, account, comment, revision, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'EUR', ?, ?, 1, ?, ?)",
                        (trade_id, row["date"], row["ticker"], row["instrument"], row["kind"], float(row["quantity"]), float(row["unit_price"]), float(row["fees"]), batch["account"], row["description"], now, now),
                    )
                else:
                    cash_id = record_id
                    connection.execute(
                        "INSERT INTO broker_cash_movements(id, date, account, kind, amount, description, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (cash_id, row["date"], batch["account"], row["kind"], float(row["net"]), row["description"], now),
                    )
            connection.execute(
                "INSERT INTO broker_receipts(source, account, fingerprint, occurrence, batch_id, trade_id, cash_id, row_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (batch["source"], batch["account"], row["fingerprint"], row["occurrence"], batch_id, trade_id, cash_id, json_dumps(row)),
            )
        for instrument, ticker in preview["mappings"].items():
            if ticker:
                connection.execute(
                    "INSERT INTO broker_mappings(source, account, instrument, ticker) VALUES (?, ?, ?, ?) ON CONFLICT(source, account, instrument) DO UPDATE SET ticker = excluded.ticker",
                    (batch["source"], batch["account"], instrument, ticker),
                )
        connection.execute("UPDATE broker_batches SET applied_at = ? WHERE id = ?", (now, batch_id))
        return {"id": batch_id, "already_applied": False, "summary": preview["summary"]}


def cancel_broker_preview(batch_id: str, *, db: Database = database) -> None:
    with db.transaction(immediate=True) as connection:
        batch = connection.execute("SELECT applied_at FROM broker_batches WHERE id = ?", (batch_id,)).fetchone()
        if batch is None:
            raise KeyError("Aperçu introuvable")
        if batch["applied_at"]:
            raise StaleBrokerPreview("Un import appliqué ne peut pas être annulé via cet aperçu")
        connection.execute("DELETE FROM broker_batches WHERE id = ?", (batch_id,))
