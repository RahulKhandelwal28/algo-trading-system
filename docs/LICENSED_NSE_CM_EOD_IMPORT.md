# Licensed NSE CM EOD Import

## Authorization gate

This importer reads **local files only**. It does not contact `nseindia.com`, call the public security-history endpoint, or download All Reports/bhavcopy files. Import actual NSE data only after NSE has authorized this project's intended use, including automated ingestion, retention, research/backtesting, and paper-trading simulation. Record the executed license or authorization identifier in the import manifest.

No licensed NSE sample or production file was used to build or test this pipeline. The paid EOD product's actual delivered encoding, container, header names, date representation, and technical requirements still need confirmation from NSE. Do not claim 2020–2026 coverage or proceed to Phase 3C based on the synthetic fixtures.

## Supported local delivery shape

The importer currently supports local UTF-8 CSV, `.csv.gz`, and `.csv.zip` containers. A ZIP may contain one or more `.csv` members. Other encodings or binary product formats fail closed until NSE supplies a sample and technical specification and a format-specific parser is added. `discover()` searches the configured local directory recursively and returns supported files in deterministic path order; it does not infer date ranges from filenames.

Source columns must be mapped explicitly using `NseCmEodSchema`. There are no default aliases because the licensed product's exact format has not been verified. After receiving an authorized sample, confirm every field against its product specification and configure the mapping, date formats, and numeric separators accordingly.

Example configuration (illustrative field labels only; **not a claim about NSE's licensed file headers**):

```python
from nse_research.data.licensed_nse_cm_eod import (
    LicensedNseCmEodProvider,
    NseCmEodSchema,
)

schema = NseCmEodSchema(
    date_column="<exact date header from licensed spec>",
    symbol_column="<exact symbol header from licensed spec>",
    series_column="<exact series header from licensed spec>",
    open_column="<exact open header from licensed spec>",
    high_column="<exact high header from licensed spec>",
    low_column="<exact low header from licensed spec>",
    close_column="<exact close header from licensed spec>",
    volume_column="<exact traded quantity header from licensed spec>",
)
provider = LicensedNseCmEodProvider("data/licensed_nse_cm_eod", schema)
manifest = provider.import_to_dataset(
    start_date,
    end_date,
    authorization_reference="<NSE agreement/order reference>",
)
```

The caller must set date parsing and numeric formatting to match the authorized product sample where the defaults do not apply. `thousands_separator` and `decimal_separator` are configurable. The importer never falls back to Yahoo or another provider.

## Normalized contract and validation

Successful imports are atomically written to the canonical columns:

```text
Symbol,Series,Date,Open Price,High Price,Low Price,Close Price,Total Traded Quantity
```

`Series` must be exactly `EQ` after trimming/case normalization. Dates are normalized to `DD-Mon-YYYY`; symbols are trimmed and uppercased; numeric comma grouping is removed by default while decimal scale is retained. Prices are never adjusted. The parser does not fill prices, create sessions, merge series, or map a renamed symbol to a predecessor.

The import audit checks date parsing, required source columns, finite numeric values, positive OHLC prices, OHLC consistency, non-negative volume, and duplicate `(symbol, date)` keys. Non-EQ rows and rows outside the requested date range are excluded and counted. Any malformed EQ record, duplicate, parse failure, or invalid OHLCV row fails the import and leaves an existing normalized dataset untouched. A failed validation report and manifest still record the failure details. Zero volume is retained and counted.

The provider implements the project's common `HistoricalDataProvider.history()` interface and returns the same `open, high, low, close, volume` daily bars as other providers. The research/backtest layer does not need to know the source.

## Symbol history and coverage

Ticker changes, renames, mergers, delistings, suspensions, and corporate actions are represented by caller-supplied `SymbolHistoryEvent` metadata. Events marked `known` require a source reference. Unknown relationships remain unknown; no corporate identity mapping is applied. Coverage ending before the requested end is reported as a coverage observation, not proof of a delisting.

The report lists overall and per-symbol first/last dates, row counts, longest histories, symbols starting after 2020, symbols ending before the requested end, and close-to-close jumps above a configurable threshold. Price jumps are warnings for investigation only; they do not trigger adjustment or corporate-action classification. Low-coverage dates are compared with the median observed symbol count. Missing-session statistics are calculated only when the caller supplies an exchange-session calendar; the importer never invents a calendar or fills a row.

## Outputs

By default, files are written beside the configured local source directory:

- `nse_cm_eod_dataset.csv` — normalized dataset, written only when data validation passes.
- `nse_cm_eod_import_manifest.json` — requested/actual range, file and row counts, symbols, validation counters, file SHA-256 values, import time, and authorization reference.
- `nse_cm_eod_validation_report.json` — detailed symbol coverage, rejected-row sample, missing sessions, low-coverage dates, abnormal jumps, and symbol-history metadata.

The manifest explicitly records that source access was local-only. A license reference is required before import, but supplying text is not an automated check that the license is valid or covers the intended use; the project owner must verify the agreement.

## Current gate

Only synthetic local fixtures are used in tests. An authorized sample and NSE technical specification are still required to confirm the format and determine whether a CSV parser is sufficient. Full history availability, historical symbol coverage, completeness, and permission remain unverified. Do not proceed to Phase 3C until a sufficiently long licensed dataset passes validation and coverage review.
