from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Response

from .db import Database, database

# Application safety limit, not a claim about the platform clipboard capacity.
MAX_CLIPBOARD_BYTES = 5 * 1024 * 1024
router = APIRouter(prefix="/api")

HEADERS = [
    "ID",
    "Date",
    "Type",
    "Compte",
    "Compte destinataire",
    "Catégorie",
    "Montant",
    "Devise",
    "Ticker",
    "Actif",
    "Quantité",
    "Prix unitaire",
    "Frais",
    "Commentaire",
    "Exclue des statistiques",
]


class ClipboardTooLarge(ValueError):
    pass


def _safe_cell(value: Any) -> Any:
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


def export_ledger_clipboard(
    *, db: Database = database, max_bytes: int = MAX_CLIPBOARD_BYTES
) -> dict[str, Any]:
    """Export every ledger row from one snapshot, or fail without truncation."""
    parts: list[str] = []
    size = 0
    count = 0
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\r\n")

    def append(values: Any) -> None:
        nonlocal size
        buffer.seek(0)
        buffer.truncate(0)
        writer.writerow([_safe_cell(value) for value in values])
        line = buffer.getvalue()
        size += len(line.encode("utf-8"))
        if size > max_bytes:
            raise ClipboardTooLarge("Le registre dépasse la limite de copie de 5 Mio.")
        parts.append(line)

    append(HEADERS)
    with db.read() as connection:
        # One read transaction: concurrent imports cannot mix ledger versions.
        connection.execute("BEGIN")
        rows = connection.execute(
            """
            SELECT id, date, type, account, '' AS target_account, category,
                   amount, currency, '' AS ticker, '' AS name,
                   NULL AS quantity, NULL AS unit_price, NULL AS fees,
                   comment, is_excluded
            FROM transactions
            UNION ALL
            SELECT id, date, 'TRANSFER', source_account, target_account, '',
                   amount, 'EUR', '', '', NULL, NULL, NULL, comment, NULL
            FROM transfers
            UNION ALL
            SELECT id, date, action, account, '', '',
                   quantity * unit_price + CASE WHEN action = 'BUY' THEN fees ELSE -fees END,
                   currency, ticker, name, quantity, unit_price, fees, comment, NULL
            FROM investments
            ORDER BY date, type, id
            """
        )
        for row in rows:
            append(row)
            count += 1
    return {"text": "".join(parts), "row_count": count, "byte_count": size}


@router.get("/ledger/clipboard")
def ledger_clipboard(response: Response) -> dict[str, Any]:
    response.headers["Cache-Control"] = "no-store"
    try:
        return export_ledger_clipboard()
    except ClipboardTooLarge as exc:
        raise HTTPException(
            status_code=413,
            detail=str(exc),
            headers={"Cache-Control": "no-store"},
        ) from exc
