# Broker imports and instrument suggestions

The global **Importer** button opens a source selector. Bourse Direct CSV supports dropping a file, dropping a text selection, and pasting CSV. Android Excel links to the existing two-step import page. Parsing and confirmation remain separate: invalid rows block the entire batch.

## Import procedure

1. Select the source and destination account.
2. Explicitly choose the CSV date convention. `2/15/2025` requires month/day; `1/12/2025` is ambiguous. Inspect the original date and ISO date side by side. Do not mix date conventions in one file.
3. Leave deposits in **Android / opening balance** mode when those sources already account for funding. Only use external funding for cash arriving from an untracked account. Otherwise cash could be counted twice.
4. Drop or paste the six-column export and compare.
5. Review instrument suggestions against the ISIN on the broker contract note. Click a suggestion or enter a ticker manually. Compare again, then confirm additions.

Accepted columns are Date, Désignation, Crédit (€), Qté, Cours and Débit (€), in any order. CSV comma/semicolon and TSV delimiters are supported. Files may be UTF-8, Windows-1252 or BOM-labelled UTF-16. Limits: 5 MiB decoded UTF-8, 10,000 operations, eight input decimal places. Repeated headers and quoted multiline descriptions are supported.

## Instrument identity

A shortened name is not an identifier. The offline catalogue in `instrument_catalog.py` provides candidates, not guaranteed matches:

| Broker label | Candidate Yahoo ticker | Candidate ISIN |
| --- | --- | --- |
| BNPETF STOXX 600 | ETZ.PA | FR0011550193 |
| AM.SP 500 ETF ACC | PSP5.PA | FR0011871128 |
| AMUNDI CAC40 U.ACC | CACC.PA | FR0013380607 |
| AM.M.WOR.ETF EUR C | CW8.PA | LU1681043599 |
| AM.PEA EM.ES.T.ACC | PAEEM.PA | FR0013412020 |
| IS.MS.W.S.P.UC.EUR | WPEA.PA | IE0002XZSHO1 |

Listing evidence links and review dates are stored with every candidate. The first two labels particularly need share-class verification. Names, tickers and listings can change; verify the fund and EUR Paris listing rather than blindly appending `.PA` to arbitrary text.

Only known normalized aliases or exact ISINs receive suggestions. Case, accents, extra whitespace, punctuation, the ACH/VTE CPT prefix and Markdown-escaped periods are tolerated. Unknown or merely similar names are not guessed. User-entered and saved account/source mappings take precedence. The CSV is never sent to an external instrument-search service.

Confirmed mappings are saved when the import is applied and reused on later operations. Catalogue normalization is intentionally separate from receipt normalization, so adding a suggestion does not invalidate existing deduplication keys.

## Fees, cash and ownership

Purchases and sales enter the existing `investments` table. Quantity is positive for both actions. Effective fees are calculated with Decimal:

- buy: debit minus quantity times quoted price;
- sell: absolute quantity times quoted price minus credit.

The full residual is retained, including sub-cent amounts from quoted-price precision. It may include taxes or rounding, not only brokerage commission. Negative inferred fees are rejected for review.

Fee refunds and explicitly external deposits live in `broker_cash_movements`. They affect cash/wealth; they do not become ordinary budget income, and Android Excel synchronization cannot delete them. Fee refunds also reduce aggregate net investment cost. Dividend and other unrecognized operations currently block the preview instead of being silently dropped.

## Re-import behavior

Receipts have a unique key `(source, account, fingerprint, occurrence)`. The fingerprint uses normalized operation content, not filename or row order. Identical repeated operations are counted rather than collapsed. Re-importing a full cumulative history adds only new occurrences. Exact matches to manually entered trades can be linked without duplicating them.

Receipts survive manual trade edits/deletions, so re-importing does not silently undo an intentional edit/delete. Confirmed apply requests are idempotent. A changed ledger invalidates an unapplied preview. Confirmation is one SQLite transaction; validation/insertion failures roll back the batch. No import deletes financial history.

Without broker transaction IDs, corrected rows and identical events split across unrelated partial exports remain ambiguous. Use cumulative exports. Cross-source funding reconciliation is not automatic; an exact same-date Android deposit match blocks external funding, but differently dated transfers still require user review.

## Extending sources

Register a `BrokerSource(label, parse)` in `broker_sources.SOURCES`. Its pure parser returns `ParsedImport` rows, warnings and errors using the existing normalized model. The source dropdown is populated by the API. Add adapter and re-import tests; add separately reviewed catalogue entries where appropriate. New corporate-action/cash types require explicit accounting support rather than treating unknown credits as income.

Back up the SQLite database before upgrading or importing live financial history. Migration 3 is additive and leaves the Android-owned tables separate.
