"""Offline instrument suggestions. A user must confirm each inferred association.

The CSV does not include ISINs. Published ISIN/listing data confirms that the
candidates exist, not that a shortened broker label uniquely identifies them.
Do not use this normaliser for the import receipt's identity.
"""
from __future__ import annotations

import re
from typing import Any

from .broker_sources import normalized

CATALOG: tuple[dict[str, str], ...] = (
    {
        "alias": "BNPETF STOXX 600",
        "ticker": "ETZ.PA",
        "isin": "FR0011550193",
        "name": "BNP Paribas Easy STOXX Europe 600 UCITS ETF Capitalisation",
        "note": "Le libellé ne précise pas la part capitalisante ou distribuante. Confirmer la part avec l’ISIN.",
        "evidence_url": "https://www.borsaitaliana.it/borsa/etf/scheda/FR0011550193-XPAR.html",
        "exchange": "XPAR",
        "currency": "EUR",
        "reviewed_on": "2026-10-10",
    },
    {
        "alias": "AM.SP 500 ETF ACC",
        "ticker": "PSP5.PA",
        "isin": "FR0011871128",
        "name": "Amundi PEA S&P 500 UCITS ETF Acc",
        "note": "Plusieurs ETF Amundi S&P 500 existent. Cette suggestion n’est pas une identification certaine : vérifier l’ISIN.",
        "evidence_url": "https://www.borsaitaliana.it/borsa/etf/scheda/FR0011871128-XPAR.html",
        "exchange": "XPAR",
        "currency": "EUR",
        "reviewed_on": "2026-10-10",
    },
    {
        "alias": "AMUNDI CAC40 U.ACC",
        "ticker": "CACC.PA",
        "isin": "FR0013380607",
        "name": "Amundi CAC 40 UCITS ETF Acc",
        "note": "Vérifier la part Acc et l’ISIN sur l’avis d’opération.",
        "evidence_url": "https://live.euronext.com/en/product/etfs/FR0013380607-XPAR/company-information",
        "exchange": "XPAR",
        "currency": "EUR",
        "reviewed_on": "2026-10-10",
    },
    {
        "alias": "AM.M.WOR.ETF EUR C",
        "ticker": "CW8.PA",
        "isin": "LU1681043599",
        "name": "Amundi MSCI World Swap UCITS ETF EUR Acc",
        "note": "Vérifier la part EUR et l’ISIN sur l’avis d’opération.",
        "evidence_url": "https://live.euronext.com/en/product/etfs/LU1681043599-XPAR",
        "exchange": "XPAR",
        "currency": "EUR",
        "reviewed_on": "2026-10-10",
    },
    {
        "alias": "AM.PEA EM.ES.T.ACC",
        "ticker": "PAEEM.PA",
        "isin": "FR0013412020",
        "name": "Amundi PEA Emergent (MSCI Emerging) ESG Transition UCITS ETF Acc",
        "note": "Vérifier l’ISIN ; un changement de nom ne signifie pas nécessairement un nouveau titre.",
        "evidence_url": "https://www.borsaitaliana.it/borsa/etf/scheda/FR0013412020-XPAR.html",
        "exchange": "XPAR",
        "currency": "EUR",
        "reviewed_on": "2026-10-10",
    },
    {
        "alias": "IS.MS.W.S.P.UC.EUR",
        "ticker": "WPEA.PA",
        "isin": "IE0002XZSHO1",
        "name": "iShares MSCI World Swap PEA UCITS ETF EUR Acc",
        "note": "Vérifier l’ISIN sur l’avis d’opération.",
        "evidence_url": "https://www.ishares.com/ch/professionelle-anleger/de/produkte/335178/ishares-msci-world-swap-pea-ucits-etf",
        "exchange": "XPAR",
        "currency": "EUR",
        "reviewed_on": "2026-10-10",
    },
)


def alias_key(label: str) -> str:
    # Accept case, whitespace, punctuation and Markdown-escaped periods only.
    label = normalized(label)
    label = re.sub(r"^(?:ACH|VTE)\s+CPT\s+", "", label)
    return re.sub(r"[^A-Z0-9]", "", label)


def instrument_suggestions(source: str, label: str) -> list[dict[str, Any]]:
    if source != "bourse-direct":
        return []
    key = alias_key(label)
    return [
        {**item, "requires_confirmation": True}
        for item in CATALOG
        if key == alias_key(item["alias"]) or key == item["isin"]
    ]
