# Phase 4B Intraday Data Audit

Audit date: 2026-09-27  
Project: `algo_trading_system`

## Result

```text
PHASE 4B INTRADAY DATA STATUS: UNAVAILABLE

Datasets inspected: 568 inventory entries (including source files and aggregate audit records)
Genuine intraday datasets: 0
Tick datasets: 0
Daily-only datasets: 544 inventory entries
Unknown/non-price/empty entries: 24 inventory entries

Best validated intraday source: NONE
Coverage: N/A
Resolution: N/A
Symbols: N/A
Timezone: N/A
Exchange: N/A
Series: N/A
Volume: N/A
Provenance: no validated intraday source
```

The inventory examines the repository's configured data, dataset, result, and test-fixture locations, including CSV, ZIP/CSV, Parquet, JSON/JSONL, SQLite, YAML, and derived result artifacts. The `datasets/` directory is absent. `results/phase4b_intraday_dataset_inventory.csv` records the per-file or aggregate entry, schema facts, coverage, identity, duplicate count, provenance, and classification. Its 568 rows classify 544 entries as daily-only and 24 as unknown, empty, missing, metadata, or aggregate artifacts; none qualifies as genuine intraday or tick data.

## Relevant market-data findings

| Source inspected | Rows/files and observed coverage | Resolution and identity | Classification |
|---|---|---|---|
| `data/cache/*_1d.csv` | 501 files: 500 equities plus NIFTY; 148,070 rows total; 2025-07-21–2026-09-25 | Date-only daily OHLCV. `.NS` is a ticker suffix, not an explicit row-level NSE/EQ identity. Separate `Adj Close` exists; the cached data configuration says `auto_adjust=false`. | `DAILY_ONLY` |
| `data/kaggle_nifty500/normalized/nifty500_kaggle.csv` and source CSVs in the Kaggle ZIP | Normalized primary file: 551,643 rows, 500 symbols, 1,235 dates, 2021-06-09–2026-06-09. The archive also contains a daily CSV with null-padded rows. | Date-only daily bars; exchange/series are not explicit in the normalized schema. Adjustment and provenance limitations are described in `docs/KAGGLE_NIFTY500_DATA_AUDIT.md`. | `DAILY_ONLY` |
| `data/tejhq/raw/nse/year=YYYY/nse_YYYY.parquet` | 17 annual partitions; 7,208,132 rows; 4,364 symbols; 2010-01-04–2026-09-25 | Date-only raw NSE EOD OHLCV. `series` is explicit (including EQ, BE, BZ); exchange is established by the source tree/manifest. TejHQ keeps adjusted-price data separately. | `DAILY_ONLY` |
| `data/tejhq/audit/official_cm/` | 13 NSE CM bhavcopy ZIP archives; 35,760 rows; 2020-02-01–2022-10-24 | Single-session EOD records with explicit symbol/series and daily traded quantity; not minute bars. | `DAILY_ONLY` |
| `data/nse_exports/index_NSEI.csv` | 1,673 rows; 2020-01-01–2026-09-25 | Official NSE daily index observations, date-only. | `DAILY_ONLY` |
| `data/nse_exports/index_INDIAVIX.csv` | 1,421 rows; 2021-01-01–2026-09-25 | Official NSE daily India VIX observations, date-only. | `DAILY_ONLY` |
| `tests/fixtures/nse_cm_eod/*_synthetic.csv` | 7 synthetic EOD fixtures | Daily fixtures for importer tests, explicitly not market observations. | `DAILY_ONLY` (test fixtures) |

TejHQ corporate-action Parquet files contain event metadata rather than price observations. TejHQ symbol-history and universe Parquet files are reference data. The licensed NSE CM EOD intake and normalized directories contain only `.gitkeep` files. The results tree contains backtest, paper-trading, ledger, and reporting artifacts, not raw intraday OHLC streams. The two SQLite databases under the yfinance cache are provider metadata; result SQLite files are paper ledgers. None is an intraday price dataset.

## Identity, time, and volume conclusions

No validated intraday source exists, so there is no intraday source timezone, timestamp resolution, coverage, session count, or bar-volume meaning to report. Daily observations do not establish the within-session sequence and are not converted into bars.

Some daily sources do identify NSE/EQ explicitly: TejHQ raw EOD rows and CM bhavcopies carry series, with NSE established by their source context. They remain daily-only. The yfinance and Kaggle equity datasets do not explicitly establish both NSE and EQ in each row. NIFTY and India VIX are separate daily index series, not equity EQ bars.

The audit found no qualifying observations from which to compute intraday duplicate timestamps or missing intraday intervals. For the daily sources, their dates are not treated as intraday intervals. The inventory records duplicate symbol/timestamp observations where the source schema supports that check.

## Decision and limitations

`INTRADAY_DATA_AVAILABLE=false`. One-minute data is **UNAVAILABLE** and five-minute data is **UNAVAILABLE**. No source was ranked or selected because none met the genuine-intraday definition: multiple actual timestamps for a symbol in one NSE trading session. No provider/API was contacted, no scraping was performed, and no data was synthesized. This audit does not establish free-data availability or live-market readiness.

The next data step is to supply an authorized, genuine intraday dataset with documented timestamp/timezone semantics, explicit NSE and EQ identity, valid 1m or 5m OHLC, and defensible bar-volume semantics. Place it in the canonical normalized schema documented in `PHASE4B_INTRADAY_SCANNER.md`, then rerun the audit and data validation before replay. Do not transform any daily source into intraday data.
