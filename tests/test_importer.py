import sqlite3
import tempfile
import unittest
from datetime import date
from io import BytesIO
from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.importer import generate_deterministic_id, import_excel_file, process_sheet


def create_database(path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE accounts (
            name TEXT PRIMARY KEY,
            initial_balance REAL DEFAULT 0.0,
            is_visible BOOLEAN DEFAULT 1
        );
        CREATE TABLE transactions (
            id TEXT PRIMARY KEY,
            date DATE,
            category TEXT,
            account TEXT,
            amount REAL,
            currency TEXT,
            comment TEXT,
            type TEXT,
            is_excluded BOOLEAN DEFAULT 0
        );
        CREATE TABLE transfers (
            id TEXT PRIMARY KEY,
            date DATE,
            source_account TEXT,
            target_account TEXT,
            amount REAL,
            comment TEXT
        );
        """
    )
    return conn


def income_sheet(comment: str | None = "") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "Date et heure": [date(2026, 3, 15)],
            "Catégorie": ["Épargne salariale"],
            "Compte": ["PEE"],
            "Montant dans la devise par défaut": [1_000.0],
            "Devise par défaut": ["EUR"],
            "Commentaire": [comment],
        },
        schema_overrides={"Commentaire": pl.String},
    )


class ImporterTests(unittest.TestCase):
    def test_reimport_replaces_stale_row_in_imported_month(self) -> None:
        conn = create_database()
        old_row = {
            "date": date(2026, 3, 15),
            "category": "Épargne salariale",
            "account": "PEE",
            "amount": 1_000.0,
            "comment": "ancienne valeur",
        }
        old_id = generate_deterministic_id(old_row, "Revenus")
        conn.execute(
            """
            INSERT INTO transactions
                (id, date, category, account, amount, currency, comment, type, is_excluded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (old_id, date(2026, 3, 15), "Épargne salariale", "PEE", 1_000.0, "EUR", "ancienne valeur", "INCOME"),
        )

        process_sheet(income_sheet("valeur corrigée"), "Revenus", conn)

        rows = conn.execute("SELECT id, account, amount, comment FROM transactions").fetchall()
        self.assertEqual(1, len(rows))
        self.assertEqual(("PEE", 1_000.0, "valeur corrigée"), rows[0][1:])
        self.assertNotEqual(old_id, rows[0][0])

    def test_reimport_does_not_delete_history_outside_imported_month(self) -> None:
        conn = create_database()
        conn.execute(
            """
            INSERT INTO transactions
                (id, date, category, account, amount, currency, comment, type, is_excluded)
            VALUES ('older', '2026-02-10', 'Salaire', 'PEE', 500, 'EUR', '', 'INCOME', 0)
            """
        )

        process_sheet(income_sheet(), "Revenus", conn)

        ids = {row[0] for row in conn.execute("SELECT id FROM transactions")}
        self.assertIn("older", ids)
        self.assertEqual(2, len(ids))

    def test_blank_comments_have_stable_ids(self) -> None:
        base = {
            "date": date(2026, 3, 15),
            "category": "Épargne salariale",
            "account": "PEE",
            "amount": 1_000.0,
        }
        with_none = generate_deterministic_id({**base, "comment": None}, "Revenus")
        with_empty = generate_deterministic_id({**base, "comment": ""}, "Revenus")

        # process_sheet normalizes the value before generating the persisted ID.
        conn = create_database()
        process_sheet(income_sheet(None), "Revenus", conn)
        persisted_id = conn.execute("SELECT id FROM transactions").fetchone()[0]

        self.assertNotEqual(with_none, with_empty)
        self.assertEqual(with_empty, persisted_id)

    def test_duplicate_source_rows_do_not_abort_synchronization(self) -> None:
        conn = create_database()
        duplicate_rows = pl.concat([income_sheet(), income_sheet()])

        process_sheet(duplicate_rows, "Revenus", conn)

        count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        self.assertEqual(1, count)

    def test_malformed_sheet_rolls_back_the_whole_workbook(self) -> None:
        class FakeSheet:
            def __init__(self, frame: pl.DataFrame) -> None:
                self.frame = frame

            def to_polars(self) -> pl.DataFrame:
                return self.frame

        class FakeReader:
            sheet_names = ["Revenus", "Dépenses"]

            def load_sheet(self, name: str, header_row: int) -> FakeSheet:
                self.assert_header_row(header_row)
                if name == "Revenus":
                    return FakeSheet(income_sheet())
                return FakeSheet(pl.DataFrame({"Mauvaise colonne": ["invalide"]}))

            @staticmethod
            def assert_header_row(header_row: int) -> None:
                if header_row != 1:
                    raise AssertionError("unexpected header row")

        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = str(Path(tmp_dir) / "finance.db")
            create_database(db_path).close()

            with (
                patch("src.importer.get_db_path", return_value=db_path),
                patch("src.importer.fastexcel.read_excel", return_value=FakeReader()),
            ):
                with self.assertRaisesRegex(ValueError, "Date et heure"):
                    import_excel_file(BytesIO(b"fake workbook"))

            with sqlite3.connect(db_path) as conn:
                count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            self.assertEqual(0, count)


if __name__ == "__main__":
    unittest.main()
