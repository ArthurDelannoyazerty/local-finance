from __future__ import annotations

import csv
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from local_finance import broker_api
from local_finance.broker_imports import (
    BrokerImportRequest,
    StaleBrokerPreview,
    apply_broker_preview,
    cancel_broker_preview,
    create_broker_preview,
)
from local_finance.broker_sources import SOURCES, parse_bourse_direct, parse_number
from local_finance.db import Database
from local_finance.portfolio import portfolio_snapshot, portfolio_summary, wealth_evolution

HEADERS = ["Date", "Désignation", "Crédit (€)", "Qté", "Cours", "Débit (€)"]
BUY = ["2020-02-16", "ACH CPT TEST ETF", "", "5", "41.017", "206.08"]
SELL = ["2020-02-17", "VTE CPT TEST ETF", "210", "-5", "42.1", ""]
DEPOSIT = ["2020-02-15", "INVESTISSEMENT ESPECES VIRT TEST", "1000", "", "", ""]
REFUND = ["2020-02-18", "REGULARISATION PEA INT. RBT TEST 202002", "0.99", "", "", ""]


def csv_text(rows, delimiter=";", headers=HEADERS):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, delimiter=delimiter)
    writer.writerow(headers)
    writer.writerows(rows)
    return buffer.getvalue()


def request(rows, **kwargs):
    options = dict(source="bourse-direct", account="PEA", csv_text=csv_text(rows), date_format="dmy", mappings={"TEST ETF": "TEST.PA"})
    options.update(kwargs)
    return BrokerImportRequest(**options)


@pytest.fixture
def broker_db(tmp_path):
    db = Database(tmp_path / "finance.db")
    db.initialize()
    with db.transaction(immediate=True) as connection:
        connection.executemany("INSERT INTO accounts(name) VALUES (?)", [("PEA",), ("CTO",)])
    return db


def count(db, table):
    with db.read() as connection:
        return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def apply_rows(db, rows, **kwargs):
    preview = create_broker_preview(request(rows, **kwargs), db=db)
    assert preview["errors"] == []
    apply_broker_preview(preview["id"], db=db)
    return preview


@pytest.mark.parametrize("delimiter", [";", ",", "\t"])
def test_delimiters_and_fee_precision(delimiter):
    result = parse_bourse_direct(csv_text([BUY, SELL, DEPOSIT, REFUND], delimiter), "dmy")
    assert result.errors == []
    assert [row["kind"] for row in result.rows] == ["BUY", "SELL", "DEPOSIT", "FEE_REFUND"]
    assert result.rows[0]["fees"] == "0.995"
    assert result.rows[1]["fees"] == "0.5"
    assert result.rows[1]["quantity"] == "5"


@pytest.mark.parametrize(("raw", "expected"), [("1 234,56 €", "1234.56"), ("1.234,56", "1234.56"), ("1,234.56", "1234.56"), ("0,0001", "0.0001"), ("-5", "-5")])
def test_numbers(raw, expected):
    assert parse_number(raw) == Decimal(expected)


@pytest.mark.parametrize("raw", ["NaN", "Infinity", "1e3", "12.34,56", "2.123456789", "999999999999999"])
def test_invalid_numbers(raw):
    with pytest.raises(ValueError):
        parse_number(raw)


def test_dates_are_explicit_and_raw_date_is_retained():
    row = ["2/15/2025", *BUY[1:]]
    assert parse_bourse_direct(csv_text([row]), "dmy").errors
    result = parse_bourse_direct(csv_text([row]), "mdy")
    assert result.rows[0]["date"] == "2025-02-15"
    assert result.rows[0]["raw_date"] == "2/15/2025"
    ambiguous = ["1/12/2025", *BUY[1:]]
    assert parse_bourse_direct(csv_text([ambiguous]), "dmy").rows[0]["date"] == "2025-12-01"
    assert parse_bourse_direct(csv_text([ambiguous]), "mdy").rows[0]["date"] == "2025-01-12"


def test_bom_sep_hint_repeated_header_and_multiline():
    row = list(DEPOSIT)
    row[1] += '\nREFERENCE; "quoted"'
    text = "\ufeffsep=;\r\n" + csv_text([row, HEADERS, REFUND])
    result = parse_bourse_direct(text, "dmy")
    assert not result.errors
    assert len(result.rows) == 2


def test_whole_history_repeat_rename_reorder_and_extend(broker_db):
    first = apply_rows(broker_db, [BUY, SELL, REFUND, DEPOSIT])
    assert first["summary"]["add"] == 3
    repeated = apply_rows(broker_db, [DEPOSIT, REFUND, SELL, BUY], filename="renamed.csv")
    assert repeated["summary"]["duplicate"] == 3
    assert count(broker_db, "investments") == 2
    extra = ["2020-03-01", *BUY[1:]]
    extended = apply_rows(broker_db, [BUY, SELL, REFUND, DEPOSIT, extra])
    assert extended["summary"]["add"] == 1
    assert extended["summary"]["duplicate"] == 3


def test_identical_rows_are_counted_not_collapsed(broker_db):
    apply_rows(broker_db, [BUY, BUY])
    assert count(broker_db, "investments") == 2
    repeated = apply_rows(broker_db, [BUY, BUY])
    assert repeated["summary"]["duplicate"] == 2
    extended = apply_rows(broker_db, [BUY, BUY, BUY])
    assert extended["summary"]["add"] == 1
    assert count(broker_db, "investments") == 3


def test_ignoring_deposits_preserves_android_ownership(broker_db):
    apply_rows(broker_db, [DEPOSIT, BUY, REFUND])
    assert count(broker_db, "transactions") == 0
    assert count(broker_db, "transfers") == 0
    assert count(broker_db, "broker_cash_movements") == 1
    later = apply_rows(broker_db, [DEPOSIT, BUY, REFUND], deposit_policy="external")
    assert later["summary"]["add"] == 1
    assert count(broker_db, "broker_cash_movements") == 2


def test_cash_refunds_portfolio_and_wealth(broker_db):
    apply_rows(broker_db, [DEPOSIT, BUY, REFUND], deposit_policy="external")
    snapshot = portfolio_snapshot(date(2020, 2, 20), db=broker_db)
    cash = next(row["value"] for row in snapshot if row["type"] == "CASH")
    assert cash == pytest.approx(1000 - 206.08 + 0.99)
    summary = portfolio_summary(db=broker_db)
    assert summary["net_invested"] == pytest.approx(206.08 - 0.99)
    assert summary["total_wealth"] == pytest.approx(999.995)
    evolution = wealth_evolution(date(2020, 2, 15), date(2020, 2, 20), db=broker_db)
    assert evolution["items"][-1]["total_wealth"] == pytest.approx(999.995)


def test_future_trades_do_not_distort_today_summary(broker_db):
    apply_rows(broker_db, [["2099-01-01", *BUY[1:]]])
    assert portfolio_summary(db=broker_db)["net_invested"] == 0


def test_existing_android_transfer_blocks_external_deposit(broker_db):
    with broker_db.transaction() as connection:
        connection.execute("INSERT INTO transfers(id,date,source_account,target_account,amount) VALUES ('t','2020-02-15','CTO','PEA',1000)")
    preview = create_broker_preview(request([DEPOSIT], deposit_policy="external"), db=broker_db)
    assert preview["errors"]
    with pytest.raises(ValueError):
        apply_broker_preview(preview["id"], db=broker_db)


def test_unknown_operation_blocks_entire_batch(broker_db):
    preview = create_broker_preview(request([BUY, ["2020-02-19", "DIVIDENDE INCONNU", "5", "", "", ""]]), db=broker_db)
    assert preview["errors"]
    with pytest.raises(ValueError):
        apply_broker_preview(preview["id"], db=broker_db)
    assert count(broker_db, "investments") == 0


def test_overselling_and_wrong_opening_date_block_import(broker_db):
    preview = create_broker_preview(request([SELL]), db=broker_db)
    assert preview["errors"]
    with broker_db.transaction() as connection:
        connection.execute("UPDATE accounts SET opening_balance_date = '2021-01-01' WHERE name='PEA'")
    preview = create_broker_preview(request([BUY]), db=broker_db)
    assert any("solde initial" in error for error in preview["errors"])


def test_tickers_are_explicit_and_persist(broker_db):
    preview = create_broker_preview(request([BUY], mappings={}), db=broker_db)
    assert preview["errors"] and preview["instruments"] == ["TEST ETF"]
    apply_rows(broker_db, [BUY])
    next_buy = ["2020-03-01", *BUY[1:]]
    preview = create_broker_preview(request([next_buy], mappings={}), db=broker_db)
    assert preview["errors"] == []
    assert preview["rows"][0]["ticker"] == "TEST.PA"


def test_account_and_source_scoped_receipts(broker_db, monkeypatch):
    apply_rows(broker_db, [REFUND])
    apply_rows(broker_db, [REFUND], account="CTO")
    monkeypatch.setitem(SOURCES, "another-broker", SOURCES["bourse-direct"])
    apply_rows(broker_db, [REFUND], source="another-broker")
    assert count(broker_db, "broker_cash_movements") == 3


def test_stale_preview_and_idempotent_apply(broker_db):
    a = create_broker_preview(request([BUY]), db=broker_db)
    b = create_broker_preview(request([BUY]), db=broker_db)
    apply_broker_preview(a["id"], db=broker_db)
    assert apply_broker_preview(a["id"], db=broker_db)["already_applied"]
    with pytest.raises(StaleBrokerPreview):
        apply_broker_preview(b["id"], db=broker_db)
    assert count(broker_db, "investments") == 1


def test_concurrent_same_batch_applies_once(broker_db):
    preview = create_broker_preview(request([BUY]), db=broker_db)
    def apply_again(_):
        return apply_broker_preview(preview["id"], db=Database(broker_db.path))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(apply_again, range(2)))
    assert sum(not result["already_applied"] for result in results) == 1
    assert count(broker_db, "investments") == 1


def test_late_failure_rolls_back_everything(broker_db):
    preview = create_broker_preview(request([BUY, REFUND]), db=broker_db)
    with broker_db.transaction() as connection:
        connection.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON broker_receipts BEGIN SELECT RAISE(ABORT,'test rollback'); END")
    with pytest.raises(Exception, match="test rollback"):
        apply_broker_preview(preview["id"], db=broker_db)
    for table in ("investments", "broker_cash_movements", "broker_receipts", "broker_mappings"):
        assert count(broker_db, table) == 0
    with broker_db.read() as connection:
        assert connection.execute("SELECT applied_at FROM broker_batches").fetchone()[0] is None


def test_receipts_survive_manual_delete_and_edit(broker_db):
    apply_rows(broker_db, [BUY, BUY])
    with broker_db.transaction() as connection:
        ids = [row[0] for row in connection.execute("SELECT id FROM investments ORDER BY id")]
        connection.execute("DELETE FROM investments WHERE id = ?", (ids[0],))
        connection.execute("UPDATE investments SET unit_price=50 WHERE id = ?", (ids[1],))
    repeat = apply_rows(broker_db, [BUY, BUY])
    assert repeat["summary"]["duplicate"] == 2
    assert count(broker_db, "investments") == 1


def test_exact_manual_trade_is_linked_not_duplicated(broker_db):
    with broker_db.transaction() as connection:
        connection.execute("INSERT INTO investments(id,date,ticker,name,action,quantity,unit_price,fees,account) VALUES ('manual','2020-02-16','TEST.PA','Manual','BUY',5,41.017,0.995,'PEA')")
    preview = apply_rows(broker_db, [BUY, BUY])
    assert preview["summary"]["link"] == 1
    assert preview["summary"]["add"] == 1
    assert count(broker_db, "investments") == 2


def test_changed_date_interpretation_cannot_duplicate(broker_db):
    row = ["04/05/2020", *BUY[1:]]
    apply_rows(broker_db, [row], date_format="mdy")
    preview = create_broker_preview(request([row], date_format="dmy"), db=broker_db)
    assert preview["errors"]
    reformatted = ["05/04/2020", *BUY[1:]]
    repeat = apply_rows(broker_db, [reformatted], date_format="dmy")
    assert repeat["summary"]["duplicate"] == 1


def test_cancel_does_not_delete_applied_history(broker_db):
    preview = create_broker_preview(request([BUY]), db=broker_db)
    cancel_broker_preview(preview["id"], db=broker_db)
    with pytest.raises(KeyError):
        apply_broker_preview(preview["id"], db=broker_db)
    applied = apply_rows(broker_db, [BUY])
    with pytest.raises(StaleBrokerPreview):
        cancel_broker_preview(applied["id"], db=broker_db)


def test_routes_and_source_menu(broker_db, monkeypatch):
    monkeypatch.setattr(broker_api, "create_broker_preview", lambda payload: create_broker_preview(payload, db=broker_db))
    monkeypatch.setattr(broker_api, "apply_broker_preview", lambda value: apply_broker_preview(value, db=broker_db))
    app = FastAPI()
    app.include_router(broker_api.router)
    client = TestClient(app)
    sources = client.get("/api/broker-imports/sources").json()
    assert {source["id"] for source in sources} == {"bourse-direct", "android-excel"}
    created = client.post("/api/broker-imports/preview", json=request([BUY]).model_dump())
    assert created.status_code == 201
    batch = created.json()["id"]
    assert client.post(f"/api/broker-imports/{batch}/apply").status_code == 200
    assert client.post("/api/broker-imports/missing/apply").status_code == 404
    assert client.post("/api/broker-imports/preview", json={}).status_code == 422


def test_cash_dates_are_in_bounds_and_protect_account_opening(broker_db):
    from local_finance.ledger import get_date_bounds, update_account
    from local_finance.schemas import AccountUpdate
    apply_rows(broker_db, [REFUND])
    assert get_date_bounds(db=broker_db)["min"] == "2020-02-18"
    with pytest.raises(ValueError):
        update_account("PEA", AccountUpdate(initial_balance=0, opening_balance_date=date(2021, 1, 1), is_visible=True, revision=1), db=broker_db)


def test_moving_purchase_does_not_orphan_sale(broker_db):
    from local_finance.ledger import InventoryError, update_trade
    from local_finance.schemas import TradeUpdate
    apply_rows(broker_db, [BUY, SELL])
    with broker_db.read() as connection:
        old = dict(connection.execute("SELECT * FROM investments WHERE action='BUY'").fetchone())
    for change in ({"account": "CTO"}, {"ticker": "OTHER.PA"}):
        with pytest.raises(InventoryError):
            update_trade(old["id"], TradeUpdate(**{**old, **change}), db=broker_db)
    with broker_db.read() as connection:
        current = dict(connection.execute("SELECT * FROM investments WHERE id=?", (old["id"],)).fetchone())
        assert current == old


def test_manual_trades_cannot_precede_account_opening(broker_db):
    from local_finance.ledger import create_trade
    from local_finance.schemas import TradeInput
    with broker_db.transaction() as connection:
        connection.execute("UPDATE accounts SET opening_balance_date='2021-01-01' WHERE name='PEA'")
    with pytest.raises(ValueError, match="opening balance"):
        create_trade(TradeInput(date=date(2020, 1, 1), ticker="TEST.PA", name="Test", action="BUY", quantity=1, unit_price=10, account="PEA"), db=broker_db)


@pytest.mark.parametrize("balance", [float("inf"), float("-inf"), float("nan")])
def test_financial_models_reject_nonfinite(balance):
    from pydantic import ValidationError
    from local_finance.schemas import AccountCreate
    with pytest.raises(ValidationError):
        AccountCreate(name="Test", initial_balance=balance)


def test_financial_models_reject_blank_after_trim():
    from pydantic import ValidationError
    from local_finance.schemas import AccountCreate
    with pytest.raises(ValidationError):
        AccountCreate(name="   ")


def test_formula_safe_exports_preserve_numeric_cells(broker_db):
    from openpyxl import load_workbook
    from local_finance.ledger import export_trades
    apply_rows(broker_db, [BUY])
    with broker_db.transaction() as connection:
        connection.execute("UPDATE investments SET name='=1+1', comment='@SUM(A1)'")
    rows = list(csv.reader(io.StringIO(export_trades(file_format="csv", db=broker_db).decode("utf-8-sig"))))
    assert rows[1][3] == "'=1+1"
    assert rows[1][-1] == "'@SUM(A1)"
    workbook = load_workbook(io.BytesIO(export_trades(file_format="xlsx", db=broker_db)))
    assert workbook.active["D2"].data_type == "s"
    assert workbook.active["E2"].data_type == "n"


def test_market_refresh_updates_last_cached_day(broker_db, monkeypatch):
    import pandas as pd
    from types import SimpleNamespace
    from local_finance.portfolio import refresh_market_data
    apply_rows(broker_db, [BUY])
    with broker_db.transaction() as connection:
        connection.execute("INSERT INTO market_prices(date,ticker,price) VALUES ('2020-02-17','TEST.PA',40)")
    class Ticker:
        fast_info = SimpleNamespace(currency="EUR")
        def history(self, **kwargs):
            assert kwargs["start"] == date(2020, 2, 17)
            return pd.DataFrame({"Close": [45]}, index=pd.to_datetime(["2020-02-17"]))
    monkeypatch.setattr("local_finance.portfolio.yf.Ticker", lambda _: Ticker())
    assert not refresh_market_data(db=broker_db)["errors"]
    with broker_db.read() as connection:
        assert connection.execute("SELECT price FROM market_prices").fetchone()[0] == 45


def test_migration_is_additive_and_repeatable(tmp_path):
    db = Database(tmp_path / "upgrade.db")
    with db.transaction(immediate=True) as connection:
        db._apply_schema(connection)
        connection.execute("INSERT INTO accounts(name,initial_balance) VALUES ('Legacy',125)")
    db.initialize()
    db.initialize()
    with db.read() as connection:
        assert connection.execute("SELECT initial_balance FROM accounts WHERE name='Legacy'").fetchone()[0] == 125
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 3
        assert not connection.execute("PRAGMA foreign_key_check").fetchall()
    assert count(db, "broker_receipts") == 0


def test_row_limit_is_enforced(monkeypatch):
    import local_finance.broker_sources as sources
    monkeypatch.setattr(sources, "MAX_ROWS", 2)
    assert sources.parse_bourse_direct(csv_text([BUY, BUY, BUY]), "dmy").errors


@pytest.mark.parametrize(("label", "ticker"), [
    ("BNPETF STOXX 600", "ETZ.PA"),
    ("AM.SP 500 ETF ACC", "PSP5.PA"),
    ("AMUNDI CAC40 U.ACC", "CACC.PA"),
    ("AM.M.WOR.ETF EUR C", "CW8.PA"),
    ("AM.PEA EM.ES.T.ACC", "PAEEM.PA"),
    (r"IS.MS.W\.S.P.UC.EUR", "WPEA.PA"),
])
def test_catalog_matches_sample_labels_but_requires_confirmation(label, ticker):
    from local_finance.instrument_catalog import instrument_suggestions

    suggestions = instrument_suggestions("bourse-direct", "ACH CPT " + label.lower())
    assert len(suggestions) == 1
    assert suggestions[0]["ticker"] == ticker
    assert suggestions[0]["requires_confirmation"] is True
    assert len(suggestions[0]["isin"]) == 12
    assert suggestions[0]["currency"] == "EUR"
    assert suggestions[0]["evidence_url"].startswith("https://")


def test_catalog_never_guesses_unknown_label_or_another_source():
    from local_finance.instrument_catalog import instrument_suggestions

    assert not instrument_suggestions("bourse-direct", "AMUNDI WORLD")
    assert not instrument_suggestions("other-broker", "IS.MS.W.S.P.UC.EUR")
    assert not instrument_suggestions("bourse-direct", "IS.MS.W.S.P.UC.USD")


def test_suggestion_confirm_persist_reimport(broker_db):
    name = "IS.MS.W.S.P.UC.EUR"
    row = ["2020-02-16", "ACH CPT " + name, "", "5", "6", "30.5"]
    first = create_broker_preview(request([row], mappings={}), db=broker_db)
    assert first["mappings"][name] == ""
    assert first["suggestions"][name][0]["ticker"] == "WPEA.PA"
    assert first["errors"]
    with pytest.raises(ValueError):
        apply_broker_preview(first["id"], db=broker_db)
    assert count(broker_db, "investments") == 0

    confirmed = create_broker_preview(request([row], mappings={name: "WPEA.PA"}), db=broker_db)
    assert not confirmed["errors"]
    apply_broker_preview(confirmed["id"], db=broker_db)
    another = ["2020-02-17", "ACH CPT " + name, "", "1", "6", "6.1"]
    extended = create_broker_preview(request([row, another], mappings={}), db=broker_db)
    assert extended["mappings"][name] == "WPEA.PA"
    assert extended["summary"]["duplicate"] == 1
    assert extended["summary"]["add"] == 1
    assert not extended["errors"]
    apply_broker_preview(extended["id"], db=broker_db)
    repeated = create_broker_preview(request([another, row], mappings={}), db=broker_db)
    assert repeated["summary"]["duplicate"] == 2
    assert repeated["summary"]["add"] == 0
    assert count(broker_db, "investments") == 2


def test_user_mapping_takes_precedence_over_catalog(broker_db):
    name = "BNPETF STOXX 600"
    row = ["2020-02-16", "ACH CPT " + name, "", "1", "10", "10.1"]
    preview = create_broker_preview(request([row], mappings={name: "CUSTOM.PA"}), db=broker_db)
    assert preview["rows"][0]["ticker"] == "CUSTOM.PA"
    assert preview["suggestions"][name][0]["ticker"] == "ETZ.PA"
    assert not preview["errors"]
