from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from openpyxl import Workbook

from local_finance.db import Database


@pytest.fixture
def db(tmp_path: Path) -> Database:
    instance = Database(tmp_path / "finance.db")
    instance.initialize()
    return instance


@pytest.fixture
def workbook_bytes():
    def build(sheets: dict[str, tuple[list[str], list[list[Any]]]]) -> bytes:
        workbook = Workbook()
        workbook.remove(workbook.active)
        for name, (headers, rows) in sheets.items():
            sheet = workbook.create_sheet(name)
            sheet.append(["Export Local Finance"])
            sheet.append(headers)
            for row in rows:
                sheet.append(row)
        output = BytesIO()
        workbook.save(output)
        return output.getvalue()

    return build


TRANSACTION_HEADERS = [
    "Date et heure",
    "Catégorie",
    "Compte",
    "Montant dans la devise par défaut",
    "Devise par défaut",
    "Commentaire",
]

TRANSFER_HEADERS = [
    "Date et heure",
    "Sortantes",
    "Entrantes",
    "Montant en devise sortante",
    "Commentaire",
]
