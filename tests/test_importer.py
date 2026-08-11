from __future__ import annotations

import pytest

from local_finance.importer import (
    ImportValidationError,
    StaleImportPreview,
    apply_import_preview,
    cancel_import_preview,
    create_import_preview,
)
from tests.conftest import TRANSACTION_HEADERS


def test_preview_is_read_only_and_income_expense_keys_do_not_collide(
    db,
    workbook_bytes,
) -> None:
    identical = ["15/03/2026", "Salaire", "Courant", "1 234,56 €", "EUR", ""]
    contents = workbook_bytes(
        {
            "Revenus": (TRANSACTION_HEADERS, [identical]),
            "Dépenses": (TRANSACTION_HEADERS, [identical]),
        }
    )

    preview = create_import_preview("export.xlsx", contents, db=db)
    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0

    assert preview["total"] == {"added": 2, "removed": 0, "unchanged": 0}
    result = apply_import_preview(preview["id"], allow_deletions=False, db=db)
    assert result["applied"]["added"] == 2
    with db.read() as connection:
        rows = connection.execute(
            "SELECT type, amount, source_key FROM transactions ORDER BY type"
        ).fetchall()
    assert [row["type"] for row in rows] == ["EXPENSE", "INCOME"]
    assert all(row["amount"] == pytest.approx(1234.56) for row in rows)
    assert rows[0]["source_key"] != rows[1]["source_key"]


def test_duplicate_source_rows_are_preserved(db, workbook_bytes) -> None:
    row = ["15/03/2026", "Courses", "Courant", 42.0, "EUR", ""]
    contents = workbook_bytes({"Dépenses": (TRANSACTION_HEADERS, [row, row])})
    preview = create_import_preview("duplicates.xlsx", contents, db=db)
    apply_import_preview(preview["id"], allow_deletions=False, db=db)
    with db.read() as connection:
        keys = [row[0] for row in connection.execute("SELECT source_key FROM transactions")]
    assert len(keys) == 2
    assert len(set(keys)) == 2


def test_missing_rows_are_not_deleted_without_a_confirmed_sync(db, workbook_bytes) -> None:
    rows = [
        ["01/01/2026", "Salaire", "Courant", 2000, "EUR", ""],
        ["01/02/2026", "Salaire", "Courant", 2000, "EUR", ""],
    ]
    full = workbook_bytes({"Revenus": (TRANSACTION_HEADERS, rows)})
    first = create_import_preview("full.xlsx", full, db=db)
    apply_import_preview(first["id"], allow_deletions=False, db=db)

    partial = workbook_bytes({"Revenus": (TRANSACTION_HEADERS, rows[:1])})
    preview = create_import_preview("partial.xlsx", partial, db=db)
    assert preview["total"]["removed"] == 1
    apply_import_preview(preview["id"], allow_deletions=False, db=db)
    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 2

    confirmed_preview = create_import_preview("confirmed.xlsx", partial, db=db)
    apply_import_preview(confirmed_preview["id"], allow_deletions=True, db=db)
    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 1


def test_confirmed_empty_sheet_can_remove_all_rows(db, workbook_bytes) -> None:
    populated = workbook_bytes(
        {
            "Dépenses": (
                TRANSACTION_HEADERS,
                [["01/01/2026", "Courses", "Courant", 42, "EUR", ""]],
            )
        }
    )
    first = create_import_preview("populated.xlsx", populated, db=db)
    apply_import_preview(first["id"], allow_deletions=False, db=db)

    empty = workbook_bytes({"Dépenses": (TRANSACTION_HEADERS, [])})
    preview = create_import_preview("empty.xlsx", empty, db=db)
    assert preview["total"]["removed"] == 1
    apply_import_preview(preview["id"], allow_deletions=True, db=db)

    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


def test_stale_preview_cannot_overwrite_a_newer_ledger(db, workbook_bytes) -> None:
    contents = workbook_bytes(
        {"Revenus": (TRANSACTION_HEADERS, [["01/01/2026", "Salaire", "Courant", 2000, "EUR", ""]])}
    )
    preview = create_import_preview("preview.xlsx", contents, db=db)
    with db.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO transactions(
                id, date, category, account, amount, currency, comment, type,
                is_excluded
            ) VALUES ('newer', '2026-02-01', 'Salaire', 'Courant', 2000,
                      'EUR', '', 'INCOME', 0)
            """
        )
    with pytest.raises(StaleImportPreview):
        apply_import_preview(preview["id"], allow_deletions=True, db=db)


def test_malformed_workbook_stages_nothing(db, workbook_bytes) -> None:
    contents = workbook_bytes({"Revenus": (["Mauvaise colonne"], [["x"]])})
    with pytest.raises(ImportValidationError):
        create_import_preview("broken.xlsx", contents, db=db)
    with db.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM import_batches").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0


def test_non_euro_default_currency_is_rejected(db, workbook_bytes) -> None:
    contents = workbook_bytes(
        {
            "Revenus": (
                TRANSACTION_HEADERS,
                [["01/01/2026", "Salaire", "Courant", 2000, "USD", ""]],
            )
        }
    )
    with pytest.raises(ImportValidationError, match="requires.*EUR"):
        create_import_preview("usd.xlsx", contents, db=db)


def test_cancelled_preview_releases_staged_rows(db, workbook_bytes) -> None:
    contents = workbook_bytes(
        {"Revenus": (TRANSACTION_HEADERS, [["01/01/2026", "Salaire", "Courant", 2000, "EUR", ""]])}
    )
    preview = create_import_preview("cancel.xlsx", contents, db=db)
    cancel_import_preview(preview["id"], db=db)
    with db.read() as connection:
        batch = connection.execute(
            "SELECT status FROM import_batches WHERE id = ?", (preview["id"],)
        ).fetchone()
        assert batch["status"] == "CANCELLED"
        assert connection.execute("SELECT COUNT(*) FROM import_rows").fetchone()[0] == 0
