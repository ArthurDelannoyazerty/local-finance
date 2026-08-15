from __future__ import annotations

import io
import sqlite3
from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, File, HTTPException, Query, Response, UploadFile, status
from fastapi.responses import FileResponse, StreamingResponse

from .db import database
from .importer import (
    ImportValidationError,
    StaleImportPreview,
    apply_import_preview,
    cancel_import_preview,
    create_import_preview,
    list_import_batches,
)
from .ledger import (
    InventoryError,
    RevisionConflict,
    create_account,
    create_trade,
    dashboard_summary,
    delete_trade,
    export_trades,
    flow_summary,
    get_date_bounds,
    list_accounts,
    list_trades,
    list_transactions,
    update_account,
    update_trade,
)
from .portfolio import (
    allocation_snapshot,
    portfolio_snapshot,
    portfolio_summary,
    refresh_market_data,
    wealth_evolution,
)
from .projections import (
    calculate_monte_carlo,
    calculate_projection,
    delete_scenario,
    list_scenarios,
    projection_defaults,
    save_scenario,
)
from .schemas import (
    AccountCreate,
    AccountUpdate,
    ImportApplyRequest,
    ProjectionRequest,
    ScenarioInput,
    TradeInput,
    TradeUpdate,
)

router = APIRouter(prefix="/api")


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc).strip("'"))
    if isinstance(exc, (RevisionConflict, StaleImportPreview)):
        return HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, RuntimeError):
        return HTTPException(status_code=409, detail=str(exc))
    if isinstance(exc, (ValueError, InventoryError, ImportValidationError)):
        return HTTPException(status_code=422, detail=str(exc))
    if isinstance(exc, sqlite3.IntegrityError):
        return HTTPException(status_code=409, detail="This record already exists")
    return HTTPException(status_code=500, detail="Unexpected server error")


@router.get("/health")
def health() -> dict[str, str]:
    with database.read() as connection:
        connection.execute("SELECT 1").fetchone()
    return {"status": "ok"}


@router.get("/meta/date-bounds")
def date_bounds() -> dict[str, str]:
    return get_date_bounds()


@router.get("/dashboard")
def dashboard(start: date, end: date) -> dict:
    if start > end:
        raise HTTPException(status_code=422, detail="Start date must precede end date")
    return dashboard_summary(start, end)


@router.get("/transactions")
def transactions(
    start: date | None = None,
    end: date | None = None,
    transaction_type: Literal["INCOME", "EXPENSE"] | None = Query(None, alias="type"),
    account: str | None = None,
    category: str | None = None,
    q: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort: str = "date",
    direction: Literal["asc", "desc"] = "desc",
) -> dict:
    return list_transactions(
        start=start,
        end=end,
        transaction_type=transaction_type,
        account=account,
        category=category,
        query=q,
        page=page,
        page_size=page_size,
        sort=sort,
        direction=direction,
    )


@router.get("/flows")
def flows(month: Annotated[list[str] | None, Query()] = None) -> dict:
    return flow_summary(month or [])


@router.get("/accounts")
def accounts() -> list[dict]:
    return list_accounts()


@router.post("/accounts", status_code=status.HTTP_201_CREATED)
def account_create(payload: AccountCreate) -> dict:
    try:
        return create_account(payload)
    except Exception as exc:
        raise _http_error(exc) from exc


@router.put("/accounts/{name}")
def account_update(name: str, payload: AccountUpdate) -> dict:
    try:
        return update_account(name, payload)
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/imports/preview", status_code=status.HTTP_201_CREATED)
async def import_preview(file: Annotated[UploadFile, File()]) -> dict:
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=422, detail="Please upload an .xlsx file")
    contents = await file.read()
    if len(contents) > 100 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="The workbook exceeds the 100 MB limit")
    try:
        return create_import_preview(file.filename, contents)
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/imports/{batch_id}/apply")
def import_apply(batch_id: str, payload: ImportApplyRequest) -> dict:
    try:
        with database.exclusive():
            if payload.allow_deletions:
                database.backup("before-import")
            return apply_import_preview(batch_id, allow_deletions=payload.allow_deletions)
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/imports")
def imports() -> list[dict]:
    return list_import_batches()


@router.delete("/imports/{batch_id}", status_code=status.HTTP_204_NO_CONTENT)
def import_cancel(batch_id: str) -> Response:
    try:
        cancel_import_preview(batch_id)
    except Exception as exc:
        raise _http_error(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/investments")
def investments(
    start: date | None = None,
    end: date | None = None,
    action: Literal["BUY", "SELL"] | None = None,
    account: str | None = None,
    ticker: str | None = None,
    q: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
) -> dict:
    return list_trades(
        start=start,
        end=end,
        action=action,
        account=account,
        ticker=ticker,
        query=q,
        page=page,
        page_size=page_size,
    )


@router.post("/investments", status_code=status.HTTP_201_CREATED)
def investment_create(payload: TradeInput) -> dict:
    try:
        return create_trade(payload)
    except Exception as exc:
        raise _http_error(exc) from exc


@router.put("/investments/{trade_id}")
def investment_update(trade_id: str, payload: TradeUpdate) -> dict:
    try:
        return update_trade(trade_id, payload)
    except Exception as exc:
        raise _http_error(exc) from exc


@router.delete("/investments/{trade_id}", status_code=status.HTTP_204_NO_CONTENT)
def investment_delete(trade_id: str, revision: int) -> Response:
    try:
        delete_trade(trade_id, revision)
    except Exception as exc:
        raise _http_error(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/investments/export")
def investments_export(
    format: Literal["csv", "xlsx"] = "csv",
    start: date | None = None,
    end: date | None = None,
    action: Literal["BUY", "SELL"] | None = None,
    account: str | None = None,
    ticker: str | None = None,
    q: str | None = None,
) -> StreamingResponse:
    content = export_trades(
        file_format=format,
        start=start,
        end=end,
        action=action,
        account=account,
        ticker=ticker,
        query=q,
    )
    media_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if format == "xlsx"
        else "text/csv; charset=utf-8"
    )
    return StreamingResponse(
        io.BytesIO(content),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="investments.{format}"'},
    )


@router.get("/portfolio/summary")
def portfolio_summary_endpoint() -> dict:
    return portfolio_summary()


@router.get("/portfolio/evolution")
def portfolio_evolution(start: date, end: date) -> dict:
    if start > end:
        raise HTTPException(status_code=422, detail="Start date must precede end date")
    return wealth_evolution(start, end)


@router.get("/portfolio/snapshot")
def portfolio_snapshot_endpoint(at: Annotated[date, Query(alias="date")]) -> dict:
    return {"date": at.isoformat(), "items": portfolio_snapshot(at)}


@router.get("/portfolio/allocation")
def portfolio_allocation(
    at: Annotated[date, Query(alias="date")],
    start: date | None = None,
) -> dict:
    return allocation_snapshot(at, start)


@router.post("/market-data/refresh")
def market_refresh() -> dict:
    try:
        return refresh_market_data()
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/projections/defaults")
def projections_defaults() -> dict:
    return projection_defaults()


@router.post("/projections/calculate")
def projections_calculate(payload: ProjectionRequest) -> dict:
    return calculate_projection(payload)


@router.post("/projections/monte-carlo")
def projections_monte_carlo(payload: ProjectionRequest) -> dict:
    return calculate_monte_carlo(payload)


@router.get("/scenarios")
def scenarios() -> list[dict]:
    return list_scenarios()


@router.post("/scenarios", status_code=status.HTTP_201_CREATED)
def scenario_create(payload: ScenarioInput) -> dict:
    return save_scenario(payload)


@router.delete("/scenarios/{scenario_id}", status_code=status.HTTP_204_NO_CONTENT)
def scenario_delete(scenario_id: str) -> Response:
    try:
        delete_scenario(scenario_id)
    except Exception as exc:
        raise _http_error(exc) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/backups", status_code=status.HTTP_201_CREATED)
def create_backup() -> dict[str, str]:
    path = database.backup("manual")
    return {"filename": path.name}


@router.get("/backups/database")
def download_database_backup() -> FileResponse:
    path = database.backup("download")
    return FileResponse(
        path,
        media_type="application/vnd.sqlite3",
        filename=path.name,
    )
