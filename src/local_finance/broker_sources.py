"""Pure source adapters: parse input, but never change the ledger."""

from __future__ import annotations

import csv
import io
import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

MAX_BYTES = 5 * 1024 * 1024
MAX_ROWS = 10_000
DateFormat = Literal["dmy", "mdy"]


@dataclass
class ParsedImport:
    rows: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def normalized(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(character for character in value if not unicodedata.combining(character))
    return " ".join(value.replace("\\.", ".").upper().split())


def decimal_text(value: Decimal | str | float) -> str:
    number = Decimal(str(value))
    return format(number.normalize(), "f") if number else "0"


def parse_number(value: str) -> Decimal:
    raw = re.sub(r"[\s\u00a0\u202f€]", "", value).replace("−", "-")
    if not raw:
        return Decimal(0)
    if len(raw) > 64:
        raise ValueError("Nombre trop long")
    if "," in raw and "." in raw:
        grouping, decimal = (".", ",") if raw.rfind(",") > raw.rfind(".") else (",", ".")
        integer, fraction = raw.rsplit(decimal, 1)
        if not re.fullmatch(r"[+-]?\d{1,3}(?:" + re.escape(grouping) + r"\d{3})+", integer):
            raise ValueError(f"Groupement numérique invalide : {value}")
        raw = integer.replace(grouping, "") + "." + fraction
    else:
        raw = raw.replace(",", ".")
    if not re.fullmatch(r"[+-]?\d+(?:\.\d{1,8})?", raw):
        raise ValueError(f"Nombre invalide (8 décimales maximum) : {value}")
    try:
        number = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"Nombre invalide : {value}") from exc
    if not number.is_finite() or abs(number) > Decimal(1000000000000):
        raise ValueError("Nombre hors limites")
    return number


def parse_date(raw: str, date_format: DateFormat) -> str:
    raw = raw.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return date.fromisoformat(raw).isoformat()
    if date_format not in {"dmy", "mdy"}:
        raise ValueError("Choisir explicitement JJ/MM/AAAA ou MM/JJ/AAAA")
    if not re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", raw):
        raise ValueError(f"Date non reconnue : {raw}")
    try:
        first, second, year = (int(part) for part in raw.split("/"))
        day, month = (first, second) if date_format == "dmy" else (second, first)
        return date(year, month, day).isoformat()
    except ValueError as exc:
        raise ValueError(f"Date {raw} incompatible avec le format choisi") from exc


def _headers(row: list[str]) -> list[str]:
    aliases = {"quantite": "qte"}
    names = [re.sub("[^a-z]", "", normalized(value).lower()) for value in row]
    return [aliases.get(name, name) for name in names]


def _operation(values: dict[str, str], line: int, date_format: DateFormat) -> dict[str, Any]:
    description = " ".join(values["designation"].split())
    designation = normalized(description)
    if not description or len(description) > 500:
        raise ValueError("Désignation vide ou trop longue")
    credit, debit = parse_number(values["credit"]), parse_number(values["debit"])
    quantity, price = parse_number(values["qte"]), parse_number(values["cours"])
    if credit < 0 or debit < 0 or bool(credit) == bool(debit):
        raise ValueError("Une seule colonne crédit/débit doit être strictement positive")
    fees = Decimal(0)
    instrument = ""
    if designation.startswith(("ACH CPT ", "VTE CPT ")):
        kind = "BUY" if designation.startswith("ACH CPT ") else "SELL"
        instrument = designation[8:].strip()
        if not instrument or len(instrument) > 160 or price <= 0 or quantity == 0:
            raise ValueError("Titre, quantité ou cours invalide")
        if kind == "BUY":
            if quantity < 0 or not debit:
                raise ValueError("Achat : quantité et débit positifs requis")
            fees = debit - quantity * price
        else:
            if not credit:
                raise ValueError("Vente : crédit positif requis")
            quantity = abs(quantity)
            fees = quantity * price - credit
        if fees < 0:
            raise ValueError(
                "Frais calculés négatifs : vérifier le cours, le montant et les décimales"
            )
    elif designation.startswith("INVESTISSEMENT ESPECES "):
        kind = "DEPOSIT"
    elif designation.startswith("REGULARISATION PEA INT. RBT "):
        kind = "FEE_REFUND"
    else:
        raise ValueError(f"Opération non prise en charge : {description}")
    if kind in {"DEPOSIT", "FEE_REFUND"} and (not credit or quantity or price):
        raise ValueError("Mouvement espèces : crédit positif sans quantité ni cours requis")
    return {
        "line": line,
        "raw_date": values["date"].strip(),
        "date": parse_date(values["date"], date_format),
        "description": description,
        "designation": designation,
        "instrument": instrument,
        "kind": kind,
        "quantity": decimal_text(quantity),
        "unit_price": decimal_text(price),
        "fees": decimal_text(fees),
        "net": decimal_text(credit - debit),
    }


def parse_bourse_direct(text: str, date_format: DateFormat) -> ParsedImport:
    result = ParsedImport()
    if len(text.encode("utf-8")) > MAX_BYTES or "\0" in text:
        raise ValueError("CSV invalide ou supérieur à 5 Mo")
    text = text.lstrip("\ufeff")
    # Excel's optional separator hint is not a data row.
    lines = text.splitlines(keepends=True)
    if lines and re.fullmatch(r"sep=[;,\t]\s*", lines[0], re.IGNORECASE):
        text = "".join(lines[1:])
    required = {"date", "designation", "credit", "qte", "cours", "debit"}
    reader = None
    headers: list[str] = []
    for delimiter in (";", ",", "\t"):
        candidate = csv.reader(
            io.StringIO(text, newline=""), delimiter=delimiter, strict=True, skipinitialspace=True
        )
        try:
            first = next(candidate)
        except (StopIteration, csv.Error):
            continue
        names = _headers(first)
        if len(names) == 6 and set(names) == required:
            reader, headers = candidate, names
            break
    if reader is None:
        raise ValueError("En-tête requis : Date, Désignation, Crédit (€), Qté, Cours, Débit (€)")
    count = 0
    try:
        for record in reader:
            if not record or not any(value.strip() for value in record):
                continue
            if _headers(record) == headers:
                continue
            count += 1
            if count > MAX_ROWS:
                result.errors.append("Limite de 10 000 opérations dépassée")
                break
            try:
                if len(record) != len(headers):
                    raise ValueError(
                        "Nombre de colonnes incorrect ; vérifier le séparateur et les guillemets"
                    )
                result.rows.append(
                    _operation(dict(zip(headers, record)), reader.line_num, date_format)
                )
            except ValueError as exc:
                result.errors.append(f"Ligne {reader.line_num} : {exc}")
    except csv.Error as exc:
        result.errors.append(f"CSV mal formé, ligne {reader.line_num} : {exc}")
    if not count:
        result.errors.append("Le CSV ne contient aucune opération")
    if any(
        "/" in row["raw_date"] and all(int(x) <= 12 for x in row["raw_date"].split("/")[:2])
        for row in result.rows
    ):
        result.warnings.append(
            "Dates ambiguës : vérifier les dates ISO de l'aperçu. Ne pas mélanger JJ/MM et MM/JJ."
        )
    today = datetime.now(UTC).date().isoformat()
    if any(row["date"] > today for row in result.rows):
        result.warnings.append(
            "Des opérations sont datées dans le futur : vérifier le format choisi."
        )
    result.warnings.append(
        "Les frais sont le résidu montant net / quantité × cours : ils peuvent inclure taxes et arrondis."
    )
    return result


@dataclass(frozen=True)
class BrokerSource:
    label: str
    parse: Callable[[str, DateFormat], ParsedImport]


SOURCES: dict[str, BrokerSource] = {
    "bourse-direct": BrokerSource("Bourse Direct — CSV", parse_bourse_direct),
}
