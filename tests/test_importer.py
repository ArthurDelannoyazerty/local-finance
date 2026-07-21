import sqlite3
import tempfile
import unittest
from contextlib import closing, contextmanager, redirect_stdout
from datetime import date, datetime
from io import BytesIO, StringIO
from pathlib import Path
from unittest.mock import patch

import polars as pl

from src.importer import generate_deterministic_id, import_excel_file, process_sheet
from tests.import_scenarios import (
    TRANSACTION_IMPORT_SCENARIOS,
    TRANSACTION_SYNC_SCENARIOS,
    TRANSFER_IMPORT_SCENARIOS,
    TRANSFER_SYNC_SCENARIOS,
    WORKBOOK_SCENARIOS,
    TransactionImportScenario,
    TransferImportScenario,
)


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


@contextmanager
def open_database(path: str = ":memory:"):
    conn = create_database(path)
    try:
        yield conn
    finally:
        conn.close()


def transaction_sheet(
    scenario: TransactionImportScenario | None = None,
    *,
    sheet_name: str = "Revenus",
    input_date: date | datetime | str = date(2026, 3, 15),
    account: str = "PEE",
    amount: float | str = 1_000.0,
    category: str = "Épargne salariale",
    currency: str = "EUR",
    comment: str | None = "",
) -> pl.DataFrame:
    if scenario is not None:
        sheet_name = scenario.sheet_name
        input_date = scenario.input_date
        account = scenario.account
        amount = scenario.amount
        category = scenario.category
        currency = scenario.currency
        comment = scenario.comment

    return pl.DataFrame(
        {
            "Date et heure": [input_date],
            "Catégorie": [category],
            "Compte": [account],
            "Montant dans la devise par défaut": [amount],
            "Devise par défaut": [currency],
            "Commentaire": [comment],
        },
        schema_overrides={"Commentaire": pl.String},
    )


def transfer_sheet(
    scenario: TransferImportScenario | None = None,
    *,
    input_date: date | datetime | str = date(2026, 3, 15),
    source_account: str = "LCL Compte Courant",
    target_account: str = "PEE",
    amount: float | str = 1_000.0,
    comment: str | None = "",
) -> pl.DataFrame:
    if scenario is not None:
        input_date = scenario.input_date
        source_account = scenario.source_account
        target_account = scenario.target_account
        amount = scenario.amount
        comment = scenario.comment

    return pl.DataFrame(
        {
            "Date et heure": [input_date],
            "Sortantes": [source_account],
            "Entrantes": [target_account],
            "Montant en devise sortante": [amount],
            "Commentaire": [comment],
        },
        schema_overrides={"Commentaire": pl.String},
    )


class FakeSheet:
    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame

    def to_polars(self) -> pl.DataFrame:
        return self.frame


class FakeReader:
    def __init__(self, sheets: dict[str, pl.DataFrame]) -> None:
        self.sheets = sheets
        self.sheet_names = list(sheets)

    def load_sheet(self, name: str, header_row: int) -> FakeSheet:
        if header_row != 1:
            raise AssertionError("unexpected header row")
        return FakeSheet(self.sheets[name])


class ImporterScenarioTests(unittest.TestCase):
    def test_transaction_import_scenarios(self) -> None:
        for scenario in TRANSACTION_IMPORT_SCENARIOS:
            with self.subTest(scenario=scenario.name):
                with open_database() as conn:
                    processed = process_sheet(
                        transaction_sheet(scenario), scenario.sheet_name, conn
                    )
                    row = conn.execute(
                        """
                        SELECT date, category, account, amount, currency, comment, type
                        FROM transactions
                        """
                    ).fetchone()
                    accounts = {
                        value[0] for value in conn.execute("SELECT name FROM accounts")
                    }

                self.assertEqual(1, processed)
                self.assertEqual(scenario.expected_date.isoformat(), row[0])
                self.assertEqual(scenario.category, row[1])
                self.assertEqual(scenario.account, row[2])
                self.assertAlmostEqual(scenario.expected_amount, row[3])
                self.assertEqual(scenario.currency, row[4])
                self.assertEqual(scenario.comment or "", row[5])
                expected_type = (
                    "INCOME" if scenario.sheet_name == "Revenus" else "EXPENSE"
                )
                self.assertEqual(expected_type, row[6])
                self.assertEqual({scenario.account}, accounts)

    def test_transfer_import_scenarios(self) -> None:
        for scenario in TRANSFER_IMPORT_SCENARIOS:
            with self.subTest(scenario=scenario.name):
                with open_database() as conn:
                    processed = process_sheet(
                        transfer_sheet(scenario), "Transferts", conn
                    )
                    row = conn.execute(
                        """
                        SELECT date, source_account, target_account, amount, comment
                        FROM transfers
                        """
                    ).fetchone()
                    accounts = {
                        value[0] for value in conn.execute("SELECT name FROM accounts")
                    }

                self.assertEqual(1, processed)
                self.assertEqual(scenario.expected_date.isoformat(), row[0])
                self.assertEqual(scenario.source_account, row[1])
                self.assertEqual(scenario.target_account, row[2])
                self.assertAlmostEqual(scenario.expected_amount, row[3])
                self.assertEqual(scenario.comment or "", row[4])
                self.assertEqual(
                    {scenario.source_account, scenario.target_account}, accounts
                )

    def test_transaction_synchronization_scenarios(self) -> None:
        for scenario in TRANSACTION_SYNC_SCENARIOS:
            with self.subTest(scenario=scenario.name):
                with open_database() as conn:
                    conn.execute(
                        """
                        INSERT INTO transactions
                            (id, date, category, account, amount, currency, comment,
                             type, is_excluded)
                        VALUES ('legacy', ?, 'Ancienne', ?, 10, 'EUR', '', ?, 0)
                        """,
                        (
                            scenario.existing_date.isoformat(),
                            scenario.existing_account,
                            scenario.existing_type,
                        ),
                    )
                    process_sheet(
                        transaction_sheet(
                            sheet_name=scenario.sheet_name,
                            input_date=scenario.imported_date,
                            account=scenario.imported_account,
                            amount=20.0,
                            category="Nouvelle",
                        ),
                        scenario.sheet_name,
                        conn,
                    )
                    existing_ids = {
                        value[0] for value in conn.execute("SELECT id FROM transactions")
                    }

                self.assertEqual(scenario.keep_existing, "legacy" in existing_ids)

    def test_transfer_synchronization_scenarios(self) -> None:
        for scenario in TRANSFER_SYNC_SCENARIOS:
            with self.subTest(scenario=scenario.name):
                with open_database() as conn:
                    conn.execute(
                        """
                        INSERT INTO transfers
                            (id, date, source_account, target_account, amount, comment)
                        VALUES ('legacy', ?, ?, ?, 10, '')
                        """,
                        (
                            scenario.existing_date.isoformat(),
                            scenario.existing_source,
                            scenario.existing_target,
                        ),
                    )
                    process_sheet(
                        transfer_sheet(
                            input_date=scenario.imported_date,
                            source_account=scenario.imported_source,
                            target_account=scenario.imported_target,
                            amount=20.0,
                        ),
                        "Transferts",
                        conn,
                    )
                    existing_ids = {
                        value[0] for value in conn.execute("SELECT id FROM transfers")
                    }

                self.assertEqual(scenario.keep_existing, "legacy" in existing_ids)

    def test_supported_workbook_combinations(self) -> None:
        frame_for_sheet = {
            "Revenus": transaction_sheet(),
            "Dépenses": transaction_sheet(sheet_name="Dépenses", amount=50.0),
            "Transferts": transfer_sheet(),
            "Notes": pl.DataFrame({"Texte": ["ignored"]}),
        }

        for scenario in WORKBOOK_SCENARIOS:
            with self.subTest(scenario=scenario.name):
                sheets = {name: frame_for_sheet[name] for name in scenario.sheet_names}
                with tempfile.TemporaryDirectory() as tmp_dir:
                    db_path = str(Path(tmp_dir) / "finance.db")
                    create_database(db_path).close()
                    with (
                        patch("src.importer.get_db_path", return_value=db_path),
                        patch(
                            "src.importer.fastexcel.read_excel",
                            return_value=FakeReader(sheets),
                        ),
                    ):
                        stats = import_excel_file(BytesIO(b"fake workbook"))

                    with closing(sqlite3.connect(db_path)) as conn:
                        transaction_count = conn.execute(
                            "SELECT COUNT(*) FROM transactions"
                        ).fetchone()[0]
                        transfer_count = conn.execute(
                            "SELECT COUNT(*) FROM transfers"
                        ).fetchone()[0]

                self.assertEqual(scenario.expected_transactions, transaction_count)
                self.assertEqual(scenario.expected_transfers, transfer_count)
                for sheet_name in ("Revenus", "Dépenses", "Transferts"):
                    expected = 1 if sheet_name in scenario.sheet_names else 0
                    self.assertEqual(expected, stats[sheet_name])


class ImporterRegressionTests(unittest.TestCase):
    def test_reimport_replaces_corrected_pee_row(self) -> None:
        old_row = {
            "date": date(2026, 3, 15),
            "category": "Épargne salariale",
            "account": "PEE",
            "amount": 1_000.0,
            "comment": "ancienne valeur",
        }
        old_id = generate_deterministic_id(old_row, "Revenus")

        with open_database() as conn:
            conn.execute(
                """
                INSERT INTO transactions
                    (id, date, category, account, amount, currency, comment, type,
                     is_excluded)
                VALUES (?, '2026-03-15', 'Épargne salariale', 'PEE', 1000,
                        'EUR', 'ancienne valeur', 'INCOME', 0)
                """,
                (old_id,),
            )
            process_sheet(
                transaction_sheet(comment="valeur corrigée"), "Revenus", conn
            )
            rows = conn.execute(
                "SELECT id, account, amount, comment FROM transactions"
            ).fetchall()

        self.assertEqual(1, len(rows))
        self.assertEqual(("PEE", 1_000.0, "valeur corrigée"), rows[0][1:])
        self.assertNotEqual(old_id, rows[0][0])

    def test_blank_comments_have_stable_persisted_ids(self) -> None:
        base = {
            "date": date(2026, 3, 15),
            "category": "Épargne salariale",
            "account": "PEE",
            "amount": 1_000.0,
        }
        empty_id = generate_deterministic_id({**base, "comment": ""}, "Revenus")

        with open_database() as conn:
            process_sheet(transaction_sheet(comment=None), "Revenus", conn)
            persisted_id = conn.execute("SELECT id FROM transactions").fetchone()[0]

        self.assertEqual(empty_id, persisted_id)

    def test_identical_source_rows_are_preserved_and_stable(self) -> None:
        duplicate_rows = pl.concat(
            [transaction_sheet(amount=4.20), transaction_sheet(amount=4.20)]
        )

        with open_database() as conn:
            process_sheet(duplicate_rows, "Revenus", conn)
            first_import_ids = {
                value[0] for value in conn.execute("SELECT id FROM transactions")
            }
            process_sheet(duplicate_rows, "Revenus", conn)
            second_import_ids = {
                value[0] for value in conn.execute("SELECT id FROM transactions")
            }

        self.assertEqual(2, len(first_import_ids))
        self.assertEqual(first_import_ids, second_import_ids)

    def test_reimport_with_fewer_identical_rows_removes_extra_occurrence(self) -> None:
        duplicate_rows = pl.concat(
            [transaction_sheet(amount=4.20), transaction_sheet(amount=4.20)]
        )

        with open_database() as conn:
            process_sheet(duplicate_rows, "Revenus", conn)
            process_sheet(transaction_sheet(amount=4.20), "Revenus", conn)
            count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

        self.assertEqual(1, count)

    def test_identical_transfer_rows_are_preserved_and_stable(self) -> None:
        duplicate_rows = pl.concat(
            [transfer_sheet(amount=40.0), transfer_sheet(amount=40.0)]
        )

        with open_database() as conn:
            process_sheet(duplicate_rows, "Transferts", conn)
            first_import_ids = {
                value[0] for value in conn.execute("SELECT id FROM transfers")
            }
            process_sheet(duplicate_rows, "Transferts", conn)
            second_import_ids = {
                value[0] for value in conn.execute("SELECT id FROM transfers")
            }

        self.assertEqual(2, len(first_import_ids))
        self.assertEqual(first_import_ids, second_import_ids)

    def test_column_whitespace_is_normalized(self) -> None:
        frame = transaction_sheet()
        frame.columns = [f"  {column}  " for column in frame.columns]

        with open_database() as conn:
            process_sheet(frame, "Revenus", conn)
            count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

        self.assertEqual(1, count)

    def test_empty_sheet_is_a_noop(self) -> None:
        with open_database() as conn:
            processed = process_sheet(pl.DataFrame(), "Revenus", conn)
            count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

        self.assertEqual(0, processed)
        self.assertEqual(0, count)

    def test_malformed_sheet_rolls_back_the_whole_workbook(self) -> None:
        sheets = {
            "Revenus": transaction_sheet(),
            "Dépenses": pl.DataFrame({"Mauvaise colonne": ["invalide"]}),
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = str(Path(tmp_dir) / "finance.db")
            create_database(db_path).close()
            with (
                patch("src.importer.get_db_path", return_value=db_path),
                patch(
                    "src.importer.fastexcel.read_excel",
                    return_value=FakeReader(sheets),
                ),
            ):
                with redirect_stdout(StringIO()):
                    with self.assertRaisesRegex(ValueError, "Date et heure"):
                        import_excel_file(BytesIO(b"fake workbook"))

            with closing(sqlite3.connect(db_path)) as conn:
                count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

        self.assertEqual(0, count)


if __name__ == "__main__":
    unittest.main()
