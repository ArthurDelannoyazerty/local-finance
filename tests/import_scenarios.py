"""Reusable scenario catalogue for the manual finance import test suite."""

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class TransactionImportScenario:
    name: str
    sheet_name: str
    input_date: date | datetime | str
    expected_date: date
    category: str
    account: str
    amount: float | str
    expected_amount: float
    currency: str = "EUR"
    comment: str | None = ""


@dataclass(frozen=True)
class TransferImportScenario:
    name: str
    input_date: date | datetime | str
    expected_date: date
    source_account: str
    target_account: str
    amount: float | str
    expected_amount: float
    comment: str | None = ""


@dataclass(frozen=True)
class TransactionSyncScenario:
    name: str
    sheet_name: str
    imported_date: date
    imported_account: str
    existing_date: date
    existing_account: str
    existing_type: str
    keep_existing: bool


@dataclass(frozen=True)
class TransferSyncScenario:
    name: str
    imported_date: date
    imported_source: str
    imported_target: str
    existing_date: date
    existing_source: str
    existing_target: str
    keep_existing: bool


@dataclass(frozen=True)
class WorkbookScenario:
    name: str
    sheet_names: tuple[str, ...]
    expected_transactions: int
    expected_transfers: int


TRANSACTION_IMPORT_SCENARIOS = (
    TransactionImportScenario(
        name="direct_pee_contribution_with_empty_cell",
        sheet_name="Revenus",
        input_date=date(2026, 3, 15),
        expected_date=date(2026, 3, 15),
        category="Épargne salariale",
        account="PEE",
        amount=1_000.0,
        expected_amount=1_000.0,
        comment=None,
    ),
    TransactionImportScenario(
        name="salary_on_current_account",
        sheet_name="Revenus",
        input_date=date(2026, 7, 1),
        expected_date=date(2026, 7, 1),
        category="Salaire",
        account="LCL Compte Courant",
        amount=2_850.42,
        expected_amount=2_850.42,
        comment="Salaire juillet",
    ),
    TransactionImportScenario(
        name="excel_datetime_income",
        sheet_name="Revenus",
        input_date=datetime(2026, 7, 2, 8, 45),
        expected_date=date(2026, 7, 2),
        category="Prime",
        account="LCL Compte Courant",
        amount=300.0,
        expected_amount=300.0,
        comment="Horodatage Excel",
    ),
    TransactionImportScenario(
        name="refund_with_decimal_comma",
        sheet_name="Revenus",
        input_date="14/02/2026",
        expected_date=date(2026, 2, 14),
        category="Remboursement",
        account="LCL Compte Courant",
        amount="125,50",
        expected_amount=125.50,
    ),
    TransactionImportScenario(
        name="dividend_with_string_date",
        sheet_name="Revenus",
        input_date="31/01/2026",
        expected_date=date(2026, 1, 31),
        category="Dividendes",
        account="PEA",
        amount="18,73",
        expected_amount=18.73,
        comment="CW8.PA",
    ),
    TransactionImportScenario(
        name="gift_with_unicode_comment",
        sheet_name="Revenus",
        input_date=date(2026, 5, 9),
        expected_date=date(2026, 5, 9),
        category="Cadeaux",
        account="Espèces",
        amount=75.25,
        expected_amount=75.25,
        comment="Anniversaire 🎁",
    ),
    TransactionImportScenario(
        name="zero_value_income",
        sheet_name="Revenus",
        input_date=date(2026, 6, 30),
        expected_date=date(2026, 6, 30),
        category="Ajustement",
        account="PEE",
        amount=0.0,
        expected_amount=0.0,
    ),
    TransactionImportScenario(
        name="groceries_expense",
        sheet_name="Dépenses",
        input_date=date(2026, 3, 16),
        expected_date=date(2026, 3, 16),
        category="Courses",
        account="LCL Compte Courant",
        amount=84.63,
        expected_amount=84.63,
        comment="Marché",
    ),
    TransactionImportScenario(
        name="rent_with_decimal_comma",
        sheet_name="Dépenses",
        input_date="01/04/2026",
        expected_date=date(2026, 4, 1),
        category="Logement",
        account="LCL Compte Courant",
        amount="950,00",
        expected_amount=950.0,
        comment="Loyer",
    ),
    TransactionImportScenario(
        name="pee_management_fee",
        sheet_name="Dépenses",
        input_date=date(2026, 3, 31),
        expected_date=date(2026, 3, 31),
        category="Frais bancaires",
        account="PEE",
        amount=12.34,
        expected_amount=12.34,
    ),
    TransactionImportScenario(
        name="leap_day_expense",
        sheet_name="Dépenses",
        input_date=date(2024, 2, 29),
        expected_date=date(2024, 2, 29),
        category="Transport",
        account="Carte",
        amount=42.0,
        expected_amount=42.0,
    ),
    TransactionImportScenario(
        name="unicode_category_and_currency",
        sheet_name="Dépenses",
        input_date="20/12/2026",
        expected_date=date(2026, 12, 20),
        category="Santé & bien-être",
        account="Compte étranger",
        amount="19,99",
        expected_amount=19.99,
        currency="CHF",
        comment="Crème solaire",
    ),
    TransactionImportScenario(
        name="zero_value_expense",
        sheet_name="Dépenses",
        input_date=date(2026, 12, 31),
        expected_date=date(2026, 12, 31),
        category="Ajustement",
        account="LCL Compte Courant",
        amount=0.0,
        expected_amount=0.0,
        comment=None,
    ),
)


TRANSFER_IMPORT_SCENARIOS = (
    TransferImportScenario(
        name="current_account_to_pee",
        input_date=date(2026, 3, 15),
        expected_date=date(2026, 3, 15),
        source_account="LCL Compte Courant",
        target_account="PEE",
        amount=1_000.0,
        expected_amount=1_000.0,
        comment="Versement volontaire",
    ),
    TransferImportScenario(
        name="current_account_to_pea",
        input_date="10/04/2026",
        expected_date=date(2026, 4, 10),
        source_account="LCL Compte Courant",
        target_account="PEA",
        amount="500,50",
        expected_amount=500.50,
    ),
    TransferImportScenario(
        name="savings_to_current_account",
        input_date=date(2026, 5, 5),
        expected_date=date(2026, 5, 5),
        source_account="Livret A",
        target_account="LCL Compte Courant",
        amount=250.0,
        expected_amount=250.0,
    ),
    TransferImportScenario(
        name="excel_datetime_transfer",
        input_date=datetime(2026, 5, 6, 17, 30),
        expected_date=date(2026, 5, 6),
        source_account="LCL Compte Courant",
        target_account="Livret A",
        amount=90.0,
        expected_amount=90.0,
    ),
    TransferImportScenario(
        name="pee_to_pea_with_unicode_comment",
        input_date=date(2026, 6, 18),
        expected_date=date(2026, 6, 18),
        source_account="PEE",
        target_account="PEA",
        amount=125.75,
        expected_amount=125.75,
        comment="Réallocation été ☀️",
    ),
    TransferImportScenario(
        name="leap_day_transfer",
        input_date=date(2024, 2, 29),
        expected_date=date(2024, 2, 29),
        source_account="Compte joint",
        target_account="Livret A",
        amount=80.0,
        expected_amount=80.0,
    ),
    TransferImportScenario(
        name="blank_comment_transfer",
        input_date="31/12/2026",
        expected_date=date(2026, 12, 31),
        source_account="Espèces",
        target_account="LCL Compte Courant",
        amount="20,00",
        expected_amount=20.0,
        comment=None,
    ),
)


TRANSACTION_SYNC_SCENARIOS = (
    TransactionSyncScenario(
        "same_month_same_income_scope",
        "Revenus",
        date(2026, 3, 15),
        "PEE",
        date(2026, 3, 5),
        "PEE",
        "INCOME",
        False,
    ),
    TransactionSyncScenario(
        "first_day_of_imported_month",
        "Revenus",
        date(2026, 3, 15),
        "PEE",
        date(2026, 3, 1),
        "PEE",
        "INCOME",
        False,
    ),
    TransactionSyncScenario(
        "last_day_of_imported_month",
        "Revenus",
        date(2026, 3, 15),
        "PEE",
        date(2026, 3, 31),
        "PEE",
        "INCOME",
        False,
    ),
    TransactionSyncScenario(
        "previous_month_is_preserved",
        "Revenus",
        date(2026, 3, 15),
        "PEE",
        date(2026, 2, 28),
        "PEE",
        "INCOME",
        True,
    ),
    TransactionSyncScenario(
        "next_month_is_preserved",
        "Revenus",
        date(2026, 3, 15),
        "PEE",
        date(2026, 4, 1),
        "PEE",
        "INCOME",
        True,
    ),
    TransactionSyncScenario(
        "different_account_is_preserved",
        "Revenus",
        date(2026, 3, 15),
        "PEE",
        date(2026, 3, 5),
        "LCL Compte Courant",
        "INCOME",
        True,
    ),
    TransactionSyncScenario(
        "opposite_type_is_preserved",
        "Revenus",
        date(2026, 3, 15),
        "PEE",
        date(2026, 3, 5),
        "PEE",
        "EXPENSE",
        True,
    ),
    TransactionSyncScenario(
        "same_month_same_expense_scope",
        "Dépenses",
        date(2026, 3, 15),
        "LCL Compte Courant",
        date(2026, 3, 8),
        "LCL Compte Courant",
        "EXPENSE",
        False,
    ),
)


TRANSFER_SYNC_SCENARIOS = (
    TransferSyncScenario(
        "shared_source_in_same_month",
        date(2026, 3, 15),
        "LCL Compte Courant",
        "PEE",
        date(2026, 3, 2),
        "LCL Compte Courant",
        "Livret A",
        False,
    ),
    TransferSyncScenario(
        "shared_target_in_same_month",
        date(2026, 3, 15),
        "LCL Compte Courant",
        "PEE",
        date(2026, 3, 10),
        "Livret A",
        "PEE",
        False,
    ),
    TransferSyncScenario(
        "reversed_accounts_in_same_month",
        date(2026, 3, 15),
        "LCL Compte Courant",
        "PEE",
        date(2026, 3, 22),
        "PEE",
        "LCL Compte Courant",
        False,
    ),
    TransferSyncScenario(
        "unrelated_accounts_are_preserved",
        date(2026, 3, 15),
        "LCL Compte Courant",
        "PEE",
        date(2026, 3, 10),
        "Compte joint",
        "Livret A",
        True,
    ),
    TransferSyncScenario(
        "previous_month_is_preserved",
        date(2026, 3, 15),
        "LCL Compte Courant",
        "PEE",
        date(2026, 2, 28),
        "LCL Compte Courant",
        "PEE",
        True,
    ),
    TransferSyncScenario(
        "next_month_is_preserved",
        date(2026, 3, 15),
        "LCL Compte Courant",
        "PEE",
        date(2026, 4, 1),
        "LCL Compte Courant",
        "PEE",
        True,
    ),
)


WORKBOOK_SCENARIOS = (
    WorkbookScenario("income_only", ("Revenus",), 1, 0),
    WorkbookScenario("expense_only", ("Dépenses",), 1, 0),
    WorkbookScenario("transfer_only", ("Transferts",), 0, 1),
    WorkbookScenario("all_supported_sheets", ("Revenus", "Dépenses", "Transferts"), 2, 1),
    WorkbookScenario("no_supported_sheets", ("Notes",), 0, 0),
)
