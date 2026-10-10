"""Broker endpoints are separate from the existing Android Excel routes."""

from __future__ import annotations

import sqlite3

from fastapi import APIRouter, HTTPException, Response

from .broker_imports import (
    BrokerImportRequest,
    StaleBrokerPreview,
    apply_broker_preview,
    cancel_broker_preview,
    create_broker_preview,
)
from .broker_sources import SOURCES

router = APIRouter(prefix="/api/broker-imports", tags=["Broker imports"])


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(404, str(exc).strip("'"))
    if isinstance(exc, (StaleBrokerPreview, sqlite3.IntegrityError)):
        return HTTPException(409, str(exc))
    return HTTPException(422, str(exc))


@router.get("/sources")
def sources() -> list[dict]:
    return [
        {"id": key, "label": source.label, "mode": "csv"} for key, source in SOURCES.items()
    ] + [
        {"id": "android-excel", "label": "Android — Excel", "mode": "existing", "path": "/donnees"}
    ]


@router.post("/preview", status_code=201)
def preview(payload: BrokerImportRequest) -> dict:
    try:
        return create_broker_preview(payload)
    except (ValueError, KeyError, sqlite3.IntegrityError) as exc:
        raise _error(exc) from exc


@router.post("/{batch_id}/apply")
def apply(batch_id: str) -> dict:
    try:
        return apply_broker_preview(batch_id)
    except (ValueError, KeyError, StaleBrokerPreview, sqlite3.IntegrityError) as exc:
        raise _error(exc) from exc


@router.delete("/{batch_id}", status_code=204)
def cancel(batch_id: str) -> Response:
    try:
        cancel_broker_preview(batch_id)
    except (KeyError, StaleBrokerPreview) as exc:
        raise _error(exc) from exc
    return Response(status_code=204)
