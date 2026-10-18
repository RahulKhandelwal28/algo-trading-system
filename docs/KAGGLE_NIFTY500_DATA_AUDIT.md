# Kaggle NIFTY 500 Dataset Audit

Audit date: 2026-09-26

## Verdict

**RESEARCH_READY_WITH_LIMITATIONS** for exploratory daily-bar research in a
separate Kaggle data path. Do not treat these bars as official NSE CM EOD
records, verified EQ-series securities, point-in-time NIFTY 500 membership, or
confirmed adjusted/unadjusted prices. Provenance and corporate-action
adjustedness remain unverified. The identified discontinuities must be
reviewed before using affected symbols or return series for performance
claims.

## Acquisition and dataset metadata

The source is the public Kaggle dataset
[Nifty500 companies 5 years stock market data](https://www.kaggle.com/datasets/shreyashautomation/nifty500-companies-5-years-stock-market-data),
owner handle `shreyashautomation` (the Kaggle CLI slug). It was downloaded on
2026-09-26 through Kaggle's CLI public-dataset download command. No alternate
market-data mirror was downloaded or used as a data source, and no
credentialed access was used.

| Metadata | Observed value |
|---|---|
| Dataset version | Unavailable; Kaggle's metadata command required authentication |
| License | MIT, as reported by the Kaggle CLI file listing |
| Kaggle description | Not available from the anonymous page/CLI output inspected |
| Stated underlying source | Unknown; not stated in metadata available during this audit |
| Stated collection method | Unknown; not stated in metadata available during this audit |
| Stated date range | Unknown; the primary CSV itself covers 2021-06-09 through 2026-06-09 |

The source archive was 30,884,542 bytes (29.5 MiB). It contains these two
flat CSV files:

| File | Bytes | Rows | Symbols | Unique dates | Observed dates | SHA-256 |
|---|---:|---:|---:|---:|---|---|
| `nifty500_5yr_data.csv` | 54,678,684 | 551,643 | 500 | 1,235 | 2021-06-09 to 2026-06-09 | `74ad16d3135685880ccde748bffe58da0d13b255f190443a9f7a3b0200b648eb` |
| `nifty500_complete_daily_data.csv` | 49,182,229 | 619,236 | 501 | 1,236 | 2021-06-24 to 2026-06-22 | `05791b2055569ff0d005b6a8093f816d8b71f29c376bd97be185ea23a31c6680` |

The downloaded ZIP SHA-256 is
`f9dd9a0f9ff264df0f85ca0cebd37be27537e9e0ebfc11e289c7b4f2a755ef54`.
The Kaggle file listing gave both CSVs a 2026-06-23 creation date. The full
acquisition record is in
[`kaggle_download_manifest.json`](../data/kaggle_nifty500/kaggle_download_manifest.json).
The available evidence does not support choosing NSE, Yahoo/yfinance, another
API or website, or manual collection as the actual source.

The primary CSV columns and pandas dtypes are `Date` (string), `Ticker`
(string), `Open`, `High`, `Low`, `Close`, `Volume` (float64), and `Notes`
(string). `Notes` is blank on 551,178 rows and contains
`Union Budget Day (Special Trading Session)` on 465 rows. The second CSV has
`Date`, `Ticker`, `Open`, `High`, `Low`, `Close`, `Volume`; its date/ticker
columns are strings and market fields are float64.

## Field mapping and provenance

| Research field | Kaggle source field | Result |
|---|---|---|
| Symbol | `Ticker` | Mapped without adding an exchange suffix |
| Series | — | **Series unavailable in source dataset**; `EQ` is not fabricated |
| Date | `Date` | ISO `YYYY-MM-DD` |
| Open Price | `Open` | Mapped as provided |
| High Price | `High` | Mapped as provided |
| Low Price | `Low` | Mapped as provided |
| Close Price | `Close` | Mapped as provided |
| Total Traded Quantity | `Volume` | Mapped as provided |

Neither CSV contains an `Adj Close` or adjusted-price indicator. The Kaggle
description/source details were not available through the unauthenticated
metadata path, so this audit cannot determine whether OHLC values are adjusted
for splits, dividends, or other corporate actions. The normalizer leaves all
price and volume values unchanged. A missing `Series` field also means these
rows cannot be represented as verified NSE `EQ` records.

## Data quality

### Primary file: `nifty500_5yr_data.csv`

| Check | Result |
|---|---|
| Date parsing / null symbol | 0 invalid dates; 0 null or blank symbols |
| Duplicate `(Ticker, Date)` rows | 0 |
| Ordering | Every symbol's records are date-ordered; file is grouped by symbol |
| Numeric missing / non-finite | 0 missing and 0 non-finite values across OHLCV |
| Positive OHLC | All prices are positive |
| OHLC bounds | One strict floating-point inequality violation, in TRIDENT on 2021-07-05; `Close` exceeds `High` by about `3.6e-15`. No violations remain with a `1e-9` precision tolerance. The row is preserved. |
| Volume | 0 missing, zero, or negative values; all observed quantities are integers. Range: 237 to 1,807,991,128 shares. No external source was used to confirm the largest volumes. |
| Row count per symbol | 113 to 1,235; listings and per-symbol coverage vary |
| Per-symbol missing NSE sessions | 2,600 within each symbol's own first/last date span; median 5, maximum 79; all 500 symbols have at least one such gap |

The official calendar used for comparison is the local
[`index_NSEI.csv`](../data/nse_exports/index_NSEI.csv), which has 1,673 dates
from 2020-01-01 through 2026-09-25. Within the Kaggle file's observed period,
four official index dates are absent for all symbols: 2023-11-12,
2024-01-20, 2024-05-18, and 2026-02-01. These are weekend sessions present in
the NSE index calendar. The Kaggle primary file has **zero dates that are
absent** from the official index calendar. The other 434 index-calendar dates
outside the Kaggle date span are not treated as file gaps.

All requested sample symbols exist. Their source histories begin on
2021-06-09 and end on 2026-06-09. RELIANCE, TCS, INFY, HDFCBANK, SBIN,
ICICIBANK, LT, AXISBANK, and BHARTIARTL each have 1,234 rows and five missing
in-span NSE sessions; ITC has 1,235 rows and four missing sessions. Those
sample rows have positive OHLC and volume and pass the OHLC bounds check.

### Second file: `nifty500_complete_daily_data.csv`

This file has no duplicate `(Ticker, Date)` keys, but 65,279 rows (10.5% of
all rows) have all five OHLCV fields null; no rows have only partial nulls.
It also contains three dates absent from the official NSE index calendar:
2026-01-15 (458 symbols have values), 2026-05-01 (464), and 2026-05-28
(500). It omits the same four special weekend index sessions listed above.
Because of its null-padded rows and unexpected dates, this file is not used
for normalization or the provider.

## Universe and survivorship assessment

The Kaggle symbol set overlaps with 498 of the 501 symbols in the project's
current `data/universe.csv`. The CSVs do not include dated constituent
snapshots, join/removal dates, or a membership history. Accordingly, the
working classification is **CURRENT_CONSTITUENT_HISTORICAL_DATA**, not
verified point-in-time NIFTY 500 history. Historical backtests using this
fixed roster are subject to survivorship and membership-selection bias. The
498/500 overlap supports the cautionary classification; it does not prove
when the data provider selected its roster.

## Extreme moves and corporate actions

The primary CSV has 50 close-to-close returns with absolute size over 20%,
across 40 symbols (20 positive and 30 negative flags). The complete list,
with previous close and a cautious event classification, is in
[`extreme_returns_over_20pct.csv`](../data/kaggle_nifty500/audit/extreme_returns_over_20pct.csv).
No flagged move was removed or adjusted. The three synchronized 2024-01-01
drops in GPIL (-79.5%), CGCL (-74.8%), and MOTILALOFS (-74.6%) are classified
as possible data/adjustment artifacts, unverified. ABFRL (-66.6%) and VEDL
(-64.9%) are possible corporate-action or data-error candidates, unverified.
The remaining flagged records are unknown without authoritative event
records. No split or merger classification is confirmed. These moves, along
with absent adjusted-close metadata, are a material limitation for return
research.

## Separate normalization and provider

The validated primary CSV is normalized, without row deletion or price
adjustment, to:

`data/kaggle_nifty500/normalized/nifty500_kaggle.csv`

It contains 551,643 rows and these columns: `Symbol`, `Date`, `Open Price`,
`High Price`, `Low Price`, `Close Price`, `Total Traded Quantity`. It does not
contain `Series`. Its sidecar manifest records `provider: "kaggle"`, dataset
slug, source and normalized checksums, row/symbol counts, and adjustedness and
Series limitations.

The separate adapter is
[`kaggle_nifty500.py`](../src/nse_research/data/kaggle_nifty500.py). It reads
only a caller-supplied normalized Kaggle CSV and makes no network requests.
It is not wired as a default provider, and the licensed NSE provider was not
changed. The raw archive, both source files, and both official NSE index CSVs
were left unchanged. No strategy, optimization, Phase 3 result, or trading
workflow was run.

## Tests

Synthetic local tests cover field mapping, provenance metadata, absent
`Series`, daily history filtering, invalid interval/date ranges, duplicate
keys, OHLC rejection, and preservation of tiny floating-point rounding
difference. Tests do not access Kaggle or NSE. The full repository suite
collected 146 tests; `pytest -q` exited successfully (146 passed, 0 failed).
