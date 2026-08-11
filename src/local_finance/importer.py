from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import fastexcel
import polars as pl

from .db import Database, database, json_dumps, row_to_dict, utc_now

SHEET_TO_KIND = {
    "Revenus": "INCOME",
    "Dépenses": "EXPENSE",
    "Transferts": "TRANSFER",
}


class ImportValidationError(ValueError):
    pass


class StaleImportPreview(RuntimeError):
    pass


def _parse_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value is None:
        raise ImportValidationError("A row is missing its date")
    raw = str(value).strip()
    for pattern in (
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(raw, pattern).replace(tzinfo=UTC).date().isoformat()
        except ValueError:
            continue
    raise ImportValidationError(f"Unsupported date value: {raw}")


def _decimal(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    raw = str(value).strip().replace("\u202f", "").replace("\u00a0", "").replace(" ", "")
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    else:
        raw = raw.replace(",", ".")
    raw = raw.replace("€", "")
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise ImportValidationError(f"Unsupported amount value: {value}") from exc


def _decimal_text(value: Any) -> str:
    parsed = _decimal(value)
    normalized = parsed.normalize()
    return format(normalized, "f") if normalized != 0 else "0"


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _required(row: dict[str, Any], column: str, sheet: str) -> Any:
    if column not in row:
        raise ImportValidationError(f"Column '{column}' is missing from '{sheet}'")
    return row[column]


def _normalise_row(sheet: str, row: dict[str, Any]) -> dict[str, Any]:
    kind = SHEET_TO_KIND[sheet]
    if kind in {"INCOME", "EXPENSE"}:
        account = _text(_required(row, "Compte", sheet))
        category = _text(_required(row, "Catégorie", sheet))
        currency = _text(_required(row, "Devise par défaut", sheet)).upper() or "EUR"
        if not account or not category:
            raise ImportValidationError(f"'{sheet}' contains a blank account or category")
        if currency != "EUR":
            raise ImportValidationError(
                f"'{sheet}' uses {currency}; this version requires an export whose default currency is EUR"
            )
        return {
            "date": _parse_date(_required(row, "Date et heure", sheet)),
            "category": category,
            "account": account,
            "amount": _decimal_text(_required(row, "Montant dans la devise par défaut", sheet)),
            "currency": currency,
            "comment": _text(row.get("Commentaire")),
            "type": kind,
        }

    source = _text(_required(row, "Sortantes", sheet))
    target = _text(_required(row, "Entrantes", sheet))
    if not source or not target:
        raise ImportValidationError("'Transferts' contains a blank source or target account")
    return {
        "date": _parse_date(_required(row, "Date et heure", sheet)),
        "source_account": source,
        "target_account": target,
        "amount": _decimal_text(_required(row, "Montant en devise sortante", sheet)),
        "comment": _text(row.get("Commentaire")),
        "type": kind,
    }


def _base_signature(kind: str, row: dict[str, Any]) -> str:
    canonical = {key: value for key, value in row.items() if key != "source_key"}
    return hashlib.sha256(f"{kind}|{json_dumps(canonical)}".encode()).hexdigest()


def _key(base_signature: str, occurrence: int) -> str:
    return hashlib.sha256(f"{base_signature}:{occurrence}".encode()).hexdigest()


def _assign_source_keys(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    occurrences: Counter[str] = Counter()
    keyed: list[dict[str, Any]] = []
    for row in rows:
        kind = str(row["type"])
        base = _base_signature(kind, row)
        occurrence = occurrences[base]
        occurrences[base] += 1
        keyed.append({**row, "source_occurrence": occurrence, "source_key": _key(base, occurrence)})
    return keyed


def parse_workbook(file_bytes: bytes) -> tuple[list[str], dict[str, list[dict[str, Any]]]]:
    try:
        reader = fastexcel.read_excel(file_bytes)
    except Exception as exc:
        raise ImportValidationError("The uploaded file is not a readable Excel workbook") from exc

    available = set(reader.sheet_names)
    sheets = [sheet for sheet in SHEET_TO_KIND if sheet in available]
    if not sheets:
        raise ImportValidationError(
            "No supported sheet was found. Expected Revenus, Dépenses or Transferts."
        )

    parsed: dict[str, list[dict[str, Any]]] = {}
    for sheet_name in sheets:
        try:
            frame: pl.DataFrame = reader.load_sheet(sheet_name, header_row=1).to_polars()
        except Exception as exc:
            raise ImportValidationError(f"Could not read the '{sheet_name}' sheet") from exc
        frame.columns = [column.strip() for column in frame.columns]
        rows = [_normalise_row(sheet_name, row) for row in frame.to_dicts()]
        parsed[SHEET_TO_KIND[sheet_name]] = _assign_source_keys(rows)
    return sheets, parsed


def _existing_rows(
    connection: sqlite3.Connection,
    kinds: Iterable[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    requested = set(kinds)
    result: dict[str, dict[str, dict[str, Any]]] = {kind: {} for kind in requested}

    if requested & {"INCOME", "EXPENSE"}:
        placeholders = ",".join("?" for _ in requested & {"INCOME", "EXPENSE"})
        tx_kinds = sorted(requested & {"INCOME", "EXPENSE"})
        records = connection.execute(
            f"""
            SELECT id, date, category, account, amount, currency, comment, type,
                   source_key, source_occurrence
            FROM transactions
            WHERE type IN ({placeholders})
            ORDER BY type, date, id
            """,
            tx_kinds,
        ).fetchall()
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for record in records:
            row = row_to_dict(record)
            normalised = {
                "date": str(row["date"]),
                "category": _text(row["category"]),
                "account": _text(row["account"]),
                "amount": _decimal_text(row["amount"]),
                "currency": _text(row["currency"]).upper() or "EUR",
                "comment": _text(row["comment"]),
                "type": str(row["type"]),
            }
            grouped[str(row["type"])].append({**normalised, "id": str(row["id"])})
        for kind, rows in grouped.items():
            occurrences: Counter[str] = Counter()
            for row in rows:
                content = {key: value for key, value in row.items() if key != "id"}
                base = _base_signature(kind, content)
                occurrence = occurrences[base]
                occurrences[base] += 1
                source_key = _key(base, occurrence)
                result[kind][source_key] = {
                    **content,
                    "id": row["id"],
                    "source_key": source_key,
                    "source_occurrence": occurrence,
                }

    if "TRANSFER" in requested:
        records = connection.execute(
            """
            SELECT id, date, source_account, target_account, amount, comment,
                   source_key, source_occurrence
            FROM transfers ORDER BY date, id
            """
        ).fetchall()
        occurrences: Counter[str] = Counter()
        for record in records:
            row = row_to_dict(record)
            content = {
                "date": str(row["date"]),
                "source_account": _text(row["source_account"]),
                "target_account": _text(row["target_account"]),
                "amount": _decimal_text(row["amount"]),
                "comment": _text(row["comment"]),
                "type": "TRANSFER",
            }
            base = _base_signature("TRANSFER", content)
            occurrence = occurrences[base]
            occurrences[base] += 1
            source_key = _key(base, occurrence)
            result["TRANSFER"][source_key] = {
                **content,
                "id": str(row["id"]),
                "source_key": source_key,
                "source_occurrence": occurrence,
            }
    return result


def _state_hash(existing: dict[str, dict[str, dict[str, Any]]]) -> str:
    material = [f"{kind}:{key}" for kind in sorted(existing) for key in sorted(existing[kind])]
    return hashlib.sha256("|".join(material).encode()).hexdigest()


def _display_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if key not in {"id", "source_key", "source_occurrence", "type"}
    }


def create_import_preview(
    filename: str,
    file_bytes: bytes,
    *,
    db: Database = database,
) -> dict[str, Any]:
    sheets, parsed = parse_workbook(file_bytes)
    kinds = list(parsed)
    batch_id = str(uuid.uuid4())
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    with db.transaction(immediate=True) as connection:
        existing = _existing_rows(connection, kinds)
        base_hash = _state_hash(existing)
        summary: dict[str, Any] = {
            "total": {"added": 0, "removed": 0, "unchanged": 0},
            "sheets": {},
        }
        connection.execute(
            """
            INSERT INTO import_batches(
                id, filename, file_sha256, created_at, status, sheets_json,
                summary_json, base_state_hash
            ) VALUES (?, ?, ?, ?, 'PREVIEW', ?, ?, ?)
            """,
            (
                batch_id,
                filename,
                file_hash,
                utc_now(),
                json_dumps(sheets),
                json_dumps(summary),
                base_hash,
            ),
        )

        for kind, rows in parsed.items():
            staged = {str(row["source_key"]): row for row in rows}
            current = existing.get(kind, {})
            added_keys = sorted(staged.keys() - current.keys())
            removed_keys = sorted(current.keys() - staged.keys())
            unchanged_keys = sorted(staged.keys() & current.keys())
            section = {
                "added": len(added_keys),
                "removed": len(removed_keys),
                "unchanged": len(unchanged_keys),
                "added_preview": [_display_row(staged[key]) for key in added_keys[:50]],
                "removed_preview": [_display_row(current[key]) for key in removed_keys[:50]],
            }
            summary["sheets"][kind] = section
            for metric in ("added", "removed", "unchanged"):
                summary["total"][metric] += section[metric]
            connection.executemany(
                """
                INSERT INTO import_rows(batch_id, kind, source_key, row_json)
                VALUES (?, ?, ?, ?)
                """,
                [(batch_id, kind, key, json_dumps(row)) for key, row in staged.items()],
            )

        connection.execute(
            "UPDATE import_batches SET summary_json = ? WHERE id = ?",
            (json_dumps(summary), batch_id),
        )
    return {"id": batch_id, "filename": filename, "status": "PREVIEW", **summary}


def _stage_rows(
    connection: sqlite3.Connection,
    batch_id: str,
) -> dict[str, dict[str, dict[str, Any]]]:
    rows = connection.execute(
        "SELECT kind, source_key, row_json FROM import_rows WHERE batch_id = ?",
        (batch_id,),
    ).fetchall()
    staged: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        staged[str(row["kind"])][str(row["source_key"])] = json.loads(row["row_json"])
    return dict(staged)


def apply_import_preview(
    batch_id: str,
    *,
    allow_deletions: bool,
    db: Database = database,
) -> dict[str, Any]:
    with db.transaction(immediate=True) as connection:
        batch = connection.execute(
            "SELECT * FROM import_batches WHERE id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            raise KeyError("Import preview not found")
        if batch["status"] != "PREVIEW":
            raise RuntimeError("This import preview has already been applied or cancelled")

        batch_summary = json.loads(batch["summary_json"])
        kinds = list(batch_summary["sheets"])
        staged_rows = _stage_rows(connection, batch_id)
        staged = {kind: staged_rows.get(kind, {}) for kind in kinds}
        existing = _existing_rows(connection, kinds)
        if _state_hash(existing) != batch["base_state_hash"]:
            raise StaleImportPreview(
                "The ledger changed after this preview. Create a new preview before importing."
            )

        applied = {"added": 0, "removed": 0, "unchanged": 0, "deletions_skipped": 0}
        now = utc_now()
        for kind, staged_rows in staged.items():
            current = existing.get(kind, {})
            added_keys = staged_rows.keys() - current.keys()
            removed_keys = current.keys() - staged_rows.keys()
            unchanged_keys = staged_rows.keys() & current.keys()

            for key in unchanged_keys:
                current_row = current[key]
                table = "transfers" if kind == "TRANSFER" else "transactions"
                connection.execute(
                    f"""
                    UPDATE {table}
                    SET source_key = ?, source_occurrence = ?, import_batch_id = ?,
                        imported_at = COALESCE(imported_at, ?)
                    WHERE id = ?
                    """,
                    (
                        key,
                        staged_rows[key]["source_occurrence"],
                        batch_id,
                        now,
                        current_row["id"],
                    ),
                )

            for key in added_keys:
                row = staged_rows[key]
                if kind == "TRANSFER":
                    connection.execute(
                        """
                        INSERT INTO transfers(
                            id, date, source_account, target_account, amount, comment,
                            source_key, source_occurrence, import_batch_id, imported_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            row["date"],
                            row["source_account"],
                            row["target_account"],
                            float(row["amount"]),
                            row["comment"],
                            key,
                            row["source_occurrence"],
                            batch_id,
                            now,
                        ),
                    )
                    accounts = {row["source_account"], row["target_account"]}
                else:
                    connection.execute(
                        """
                        INSERT INTO transactions(
                            id, date, category, account, amount, currency, comment,
                            type, is_excluded, source_key, source_occurrence,
                            import_batch_id, imported_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            row["date"],
                            row["category"],
                            row["account"],
                            float(row["amount"]),
                            row["currency"],
                            row["comment"],
                            kind,
                            key,
                            row["source_occurrence"],
                            batch_id,
                            now,
                        ),
                    )
                    connection.execute(
                        "INSERT OR IGNORE INTO categories(name, type) VALUES (?, ?)",
                        (row["category"], kind),
                    )
                    accounts = {row["account"]}

                for account in accounts:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO accounts(name, initial_balance, updated_at)
                        VALUES (?, 0, ?)
                        """,
                        (account, now),
                    )

            if allow_deletions:
                table = "transfers" if kind == "TRANSFER" else "transactions"
                connection.executemany(
                    f"DELETE FROM {table} WHERE id = ?",
                    [(current[key]["id"],) for key in removed_keys],
                )
                applied["removed"] += len(removed_keys)
            else:
                applied["deletions_skipped"] += len(removed_keys)

            applied["added"] += len(added_keys)
            applied["unchanged"] += len(unchanged_keys)

        final_summary = {**batch_summary, "applied": applied}
        connection.execute(
            """
            UPDATE import_batches
            SET status = 'APPLIED', applied_at = ?, summary_json = ?
            WHERE id = ? AND status = 'PREVIEW'
            """,
            (now, json_dumps(final_summary), batch_id),
        )
    return {"id": batch_id, "status": "APPLIED", **final_summary}


def list_import_batches(*, db: Database = database, limit: int = 20) -> list[dict[str, Any]]:
    with db.read() as connection:
        rows = connection.execute(
            """
            SELECT id, filename, file_sha256, created_at, applied_at, status,
                   summary_json, error
            FROM import_batches ORDER BY created_at DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    result = []
    for row in rows:
        item = row_to_dict(row)
        item["summary"] = json.loads(item.pop("summary_json"))
        result.append(item)
    return result


def cancel_import_preview(batch_id: str, *, db: Database = database) -> None:
    with db.transaction(immediate=True) as connection:
        cursor = connection.execute(
            "UPDATE import_batches SET status = 'CANCELLED' WHERE id = ? AND status = 'PREVIEW'",
            (batch_id,),
        )
        if cursor.rowcount != 1:
            exists = connection.execute(
                "SELECT 1 FROM import_batches WHERE id = ?", (batch_id,)
            ).fetchone()
            if exists is None:
                raise KeyError("Import preview not found")
            raise RuntimeError("This import preview is no longer pending")
        connection.execute("DELETE FROM import_rows WHERE batch_id = ?", (batch_id,))
