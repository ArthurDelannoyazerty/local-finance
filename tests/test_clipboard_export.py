from __future__ import annotations

import csv
import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from local_finance import clipboard_export
from local_finance.clipboard_export import ClipboardTooLarge, export_ledger_clipboard


def seed_ledger(db) -> None:
    with db.transaction(immediate=True) as connection:
        connection.execute("INSERT INTO accounts(name, is_visible) VALUES ('Hidden', 0)")
        connection.executemany(
            """
            INSERT INTO transactions(id, date, category, account, amount, currency,
                                     comment, type, is_excluded)
            VALUES (?, '2020-01-01', 'Courses', 'Hidden', 12.34, 'EUR', ?, ?, 1)
            """,
            [(f"tx-{i:04}", 'Été, "courses"\nSuite', "INCOME" if i % 2 else "EXPENSE")
             for i in range(251)],
        )
        connection.execute(
            """
            INSERT INTO transfers(id, date, source_account, target_account, amount, comment)
            VALUES ('transfer', '2021-01-01', 'Hidden', 'PEA', 300, '=1+1')
            """
        )
        connection.executemany(
            """
            INSERT INTO investments(id, date, ticker, name, action, quantity, unit_price,
                                    fees, account, comment)
            VALUES (?, '2022-01-01', 'ETF', 'Mon ETF', ?, 2, 50, 1.5, 'PEA', '')
            """,
            [("buy", "BUY"), ("sell", "SELL")],
        )


def test_complete_export_is_not_filtered_or_paginated_and_preserves_duplicates(db) -> None:
    seed_ledger(db)
    result = export_ledger_clipboard(db=db)
    rows = list(csv.DictReader(io.StringIO(result["text"])))
    assert result["row_count"] == len(rows) == 254
    assert result["byte_count"] == len(result["text"].encode("utf-8"))
    assert {row["Type"] for row in rows} == {"INCOME", "EXPENSE", "TRANSFER", "BUY", "SELL"}
    assert len({row["ID"] for row in rows}) == 254
    assert sum(row["Compte"] == "Hidden" for row in rows) == 252
    assert sum(row["Exclue des statistiques"] == "1" for row in rows) == 251
    assert rows[0]["Commentaire"] == 'Été, "courses"\nSuite'
    assert rows[0]["Date"] == "2020-01-01"
    transfer = next(row for row in rows if row["Type"] == "TRANSFER")
    assert transfer["Compte destinataire"] == "PEA"
    assert transfer["Commentaire"] == "'=1+1"
    buy = next(row for row in rows if row["Type"] == "BUY")
    sell = next(row for row in rows if row["Type"] == "SELL")
    assert float(buy["Montant"]) == 101.5
    assert float(sell["Montant"]) == 98.5
    assert buy["Ticker"] == "ETF"
    assert float(buy["Quantité"]) == 2
    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 251
        assert connection.execute("SELECT COUNT(*) FROM import_batches").fetchone()[0] == 0


def test_size_limit_counts_utf8_and_never_returns_partial_csv(db) -> None:
    seed_ledger(db)
    full = export_ledger_clipboard(db=db)
    assert len(full["text"]) < full["byte_count"]
    with pytest.raises(ClipboardTooLarge):
        export_ledger_clipboard(db=db, max_bytes=len(full["text"]))
    exact = export_ledger_clipboard(db=db, max_bytes=full["byte_count"])
    assert exact == full


def test_empty_ledger_has_only_headers(db) -> None:
    result = export_ledger_clipboard(db=db)
    assert result["row_count"] == 0
    assert len(list(csv.reader(io.StringIO(result["text"])))) == 1


def test_endpoint_returns_complete_snapshot_without_http_caching(db, monkeypatch) -> None:
    seed_ledger(db)
    monkeypatch.setattr(
        clipboard_export, "export_ledger_clipboard", lambda: export_ledger_clipboard(db=db)
    )
    app = FastAPI()
    app.include_router(clipboard_export.router)
    with TestClient(app) as client:
        response = client.get("/api/ledger/clipboard")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["row_count"] == 254


def test_endpoint_refuses_oversized_snapshot(db, monkeypatch) -> None:
    def oversized():
        raise ClipboardTooLarge("Too large")

    monkeypatch.setattr(clipboard_export, "export_ledger_clipboard", oversized)
    app = FastAPI()
    app.include_router(clipboard_export.router)
    with TestClient(app) as client:
        response = client.get("/api/ledger/clipboard")
    assert response.status_code == 413
    assert "text" not in response.json()
    assert response.headers["cache-control"] == "no-store"
