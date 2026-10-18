# TejHQ Historical NSE Dataset Audit

Audit timestamp (UTC): 2026-09-26T16:04:27.129844+00:00

## 1. Executive summary

**Verdict: NOT_RESEARCH_READY**

This is third-party historical market data derived from NSE/BSE bhavcopy data, subject to independent audit. This report audits the frozen NSE raw-price tree; it does not run strategies, a backtest, or Phase 3C. It leaves the Kaggle snapshot, official NSE index files, licensed NSE CM EOD files, and all existing results untouched.

| Snapshot metric | Observed |
|---|---:|
| Revision | `12cf222405044b511b28730d071e7693a53e5a52` |
| Snapshot SHA-256 | `1a01bbf7a22f9dc5d84948df5af307f6f337d7317a77e47aaa998fd8059d6655` |
| Raw NSE rows | 7,208,132 |
| Explicit EQ rows | 6,577,021 |
| EQ symbols | 4,117 |
| Date range | 2010-01-04 → 2026-09-25 |
| Raw series values | BE, BZ, EQ |
| Duplicate key excess rows | 0 |
| Invalid EQ rows | 0 |
| Phase 3C fold date coverage | FAIL |

The source card states that raw-tree data is cleaned, deduplicated, and typed, and that some anomalous source rows are dropped before publishing. Thus, zero anomalies in this snapshot cannot establish that the original exchange files had no anomalies or that no records were removed upstream.

## 2. Source and provenance

- Provider: TejHQ, dataset `tejhq/indian-markets`.
- Source URL: [https://huggingface.co/datasets/tejhq/indian-markets](https://huggingface.co/datasets/tejhq/indian-markets)
- Source classification: third-party historical market data derived from NSE/BSE bhavcopy data, subject to independent audit. This is **not** called official NSE data.
- Immutable Hugging Face revision: `12cf222405044b511b28730d071e7693a53e5a52`.
- Repository head reported during acquisition: `12cf222405044b511b28730d071e7693a53e5a52`.
- Source updated: `2026-09-26T05:16:18.000Z`; downloaded: `2026-09-26T15:56:08.101831+00:00`.
- Exact downloaded repository files, byte sizes, SHA-256 values, and object IDs are listed in `data/tejhq/tejhq_dataset_manifest.json`.
- Pinned dataset card and Hugging Face metadata/tree response are preserved under `data/tejhq/metadata/`.

## 3. License and use information

The pinned Hugging Face card labels the dataset `MIT`. It states that the data is exchange-published and redistribution is permitted, and explicitly says to verify exchange terms before commercial use. This records TejHQ's published statement; this audit does not independently determine underlying exchange rights. References and pinned documentation are preserved in `data/tejhq/metadata/tejhq_dataset_card.md`.

## 4. Dataset snapshot and schema

The acquisition includes the pinned NSE raw annual Parquet partitions for 17 years, NSE corporate-action partitions, `symbol_history/nse.parquet`, `universe/nse_liquid.parquet`, and the pinned source README. Raw files are not rewritten. The TejHQ raw tree is itself a typed/cleaned publication, not the original NSE files.

| Field | Present | Type | Null % | Notes |
|---|---|---|---:|---|
| date | Yes | date32[day] | 0.0000% | Trading date; raw tree field. |
| symbol | Yes | large_string | 0.0000% | Source ticker; preserved without symbol-history rewriting. |
| series | Yes | large_string | 0.0000% | Explicit NSE series; only exact EQ is admitted to normalized equity output. |
| isin | Yes | large_string | 7.1531% | Null before 2012 is documented by TejHQ; no values are fabricated. |
| name | Yes | large_string | 0.0000% | Source instrument/company name. |
| open | Yes | double | 0.0000% | Raw-tree OHLC price, INR. |
| high | Yes | double | 0.0000% | Raw-tree OHLC price, INR. |
| low | Yes | double | 0.0000% | Raw-tree OHLC price, INR. |
| close | Yes | double | 0.0000% | Raw-tree close, INR. |
| last | Yes | double | 0.0000% | Last traded price. |
| prev_close | Yes | double | 0.0000% | Previous close. |
| volume | Yes | int64 | 0.0000% | Shares traded. |
| turnover | Yes | double | 0.0000% | Turnover, INR. |
| trades | Yes | int64 | 7.1531% | Trade count; null before 2012 is documented by TejHQ. |

Actual series counts:

| Series | Rows | Symbols | Dates | First date | Last date |
|---|---:|---:|---:|---|---|
| BE | 563,840 | 2,395 | 4,122 | 2010-01-04 | 2026-09-25 |
| BZ | 67,271 | 303 | 2,992 | 2014-07-09 | 2026-09-25 |
| EQ | 6,577,021 | 4,117 | 4,122 | 2010-01-04 | 2026-09-25 |

`EQ` exists as an explicit source `series` value and its rows contain positive OHLC prices and traded share volumes where validated below. It is kept distinct from `BE` and `BZ`. An NSE-hosted BAJAJELEC filing labels that security's series as `EQ` ([NSE filing](https://nsearchives.nseindia.com/corporate/BAJAJELEC_16092023182415_BELSEIntimation.pdf)); this is a spot confirmation for one security, not an independent comparison of every source row against an NSE security master. No series is fabricated.

## 5. Date coverage and index alignment

- Unique NSE dates: 4,122; unique EQ dates: 4,122.
- Inclusive calendar span: 6,109 days.
- Missing Monday–Friday calendar dates across the full observed union: 245. These are **not** called errors because exchange holidays and unscheduled closures are not inferred from weekdays.
- Official NIFTY calendar file: [existing official NSE index export](https://www.nseindia.com/reports-indices-historical-index-data) saved as `data/nse_exports/index_NSEI.csv` (2020-01-01 → 2026-09-25).
- Overlap: 2020-01-01 → 2026-09-25; 1,673 official NIFTY sessions and 1,660 TejHQ EQ dates.
- Official NIFTY sessions absent from TejHQ EQ: 13 (2020-02-01, 2020-11-14, 2021-11-04, 2022-10-24, 2023-11-12, 2024-05-18, 2024-11-01, 2025-10-21, 2026-02-01, 2026-06-05, 2026-06-15, 2026-06-16, 2026-06-22); TejHQ EQ dates not in the NIFTY calendar: 0.
- Official India VIX comparison: 2021-01-01 → 2026-09-25; 1,421 VIX dates, 11 not represented by a TejHQ EQ date (2021-11-04, 2022-10-24, 2023-11-12, 2024-05-18, 2024-11-01, 2025-10-21, 2026-02-01, 2026-06-05, 2026-06-15, 2026-06-16, 2026-06-22). Index CSVs were read only and left unchanged. Full NIFTY/VIX alignment is in `data/tejhq/audit/index_alignment.csv`.
- Date-level rows/symbols are in `data/tejhq/audit/date_coverage.csv`; exact unique symbols by year and month are in the two `symbols_by_*.csv` files.

For 2010–2019 the project has no official NSE index calendar, so the per-symbol missing-session denominator uses the observed NSE date union and can undercount a date omitted for every security. For 2020 onward, the official index date calendar is used where available.

## 6. Symbol coverage and identity

- EQ symbols observed: 4,117.
- Symbols with at least one ISIN transition: 544.
- ISIN values observed with multiple symbols: 430.
- Per-symbol row counts, first/last dates, observed ISINs, expected-session gaps, longest gap, and coverage percentage are in `audit/symbol_coverage.csv` and `audit/missing_data_by_symbol.csv`.
- `audit/security_identity_changes.csv` reports only observed ISIN↔symbol/date mappings. It does not infer a rename, merger, relisting, or delisting from those mappings.
- Newly appearing/disappearing symbols are observable first/last-bar facts, not verified listing/delisting events.

## 7. Series analysis

The raw data contains 3 distinct series: BE, BZ, EQ. Full rows/symbols/date coverage is in `audit/series_coverage.csv`. The intended normalized output contains only explicit `EQ`; non-equity rows are never passed through it.

## 8. OHLCV validation

- Invalid EQ records: 0.
- OHLC relationship/non-positive price rows: 0; tolerance: 1e-09.
- Missing volume: 0; negative volume: 0; zero volume: 0 (allowed by the stated `Volume >= 0` test, reported separately).
- Malformed dates, non-finite numbers, and OHLCV row anomalies are written to `audit/data_quality_anomalies.csv`; source values are not corrected or deleted.
- Since Parquet stores typed columns, malformed text numerics are not expected in the published files. Nulls are measured per field in the schema table; TejHQ documents pre-2012 null ISIN/trade-count fields.

## 9. Duplicate analysis

- Natural key: `date + symbol + series`; duplicate excess rows: 0.
- Duplicate key groups with identical payloads: 0; conflicting payload groups: 0.
- `date + ISIN + series` duplicate rows: 0.
- Examples: `audit/duplicate_examples.csv`. No deduplication was performed by this audit.

## 10. Missing-data analysis

Symbols with any expected-session gap: 3,642; symbols with a longest consecutive gap of at least 20 expected sessions: 1,785; symbols with 20 or fewer observations: 69. Expected sessions use the official NIFTY calendar from 2020 where available and the observed union of TejHQ NSE dates before 2020, so pre-2020 gap rates are not a definitive exchange-completeness measure. No values are filled.

## 11. Extreme-return analysis

EQ close-to-close moves exceeding absolute 10%, 20%, and 50% are counted as 88,344, 7,856, and 1,642. Every move above 10% is listed in `audit/extreme_returns.csv`. Nearby action events are labeled as correlations only; price jumps are not automatically assigned a cause or removed.

## 12. Corporate actions and adjustment methodology

- NSE action records acquired: 34,611; type counts:

| Type | Events | Symbols | First ex-date | Last ex-date |
|---|---:|---:|---|---|
| agm | 11108 | 2008 | 2010-01-04 | 2025-07-21 |
| bonus | 587 | 442 | 2010-02-01 | 2026-09-11 |
| buyback | 347 | 190 | 2012-06-07 | 2026-09-04 |
| demerger | 118 | 95 | 2010-11-25 | 2026-09-07 |
| dividend | 21043 | 2156 | 2010-01-12 | 2026-09-25 |
| merger | 4 | 4 | 2016-03-22 | 2019-07-11 |
| other | 395 | 278 | 2010-01-04 | 2025-06-25 |
| rights | 348 | 275 | 2010-01-15 | 2026-09-15 |
| split | 661 | 616 | 2010-01-04 | 2026-09-22 |

- TejHQ documents a separate action tree and an adjusted-price tree. Its card describes `adj_close = close × cumulative factor` over later corporate actions; splits/bonuses and a dividend factor `(prev_close - cash) / prev_close` are included, while demergers, rights, buybacks and mergers are not scaled.
- The raw tree and normalized output here remain raw/unadjusted values; the separate adjusted tree is not used.
- `audit/corporate_action_examples.csv` independently computes expected mechanical split/bonus/dividend ratios from the action fields and compares them with raw close ratios around selected ex-dates. The comparison is a consistency check, not a test of the final ex-date close: the residual includes market movement. External NSE event-by-event verification remains UNKNOWN. A nearby move does not prove causation.
- Some raw source action rows can carry a stale post-merger ISIN, which TejHQ itself notes; use the published identity history for review, not automatic symbol mapping.

## 13. Symbol/ISIN continuity

The source documents one contiguous interval per ISIN/symbol in `symbol_history/nse.parquet`. The audit compared its rows with identity pairs observed in raw data; see `audit/symbol_history_audit.json` for interval checks and unmatched pair counts, and `audit/security_identity_changes.csv` for observed mappings. No mapping is applied. The card notes pre-2012 NSE data has no ISIN and that ISIN can change around face-value splits/re-listings. Old rows without ISIN remain null.

## 14. Delisted and historical security behavior

Observed EQ last-date facts: {"symbols_ending_2022": 84, "symbols_ending_2023": 111, "symbols_ending_2024": 74, "symbols_ending_2025": 77, "symbols_ending_2026": 2931, "symbols_ending_before_2022": 840, "symbols_ending_prior_to_snapshot_last_date": 1457}. A security ending before the snapshot's last date may be delisted, renamed, inactive, newly assigned another ISIN, or missing from the provider snapshot; this data alone does not distinguish those states. TejHQ claims it covers all listings since 2010 and preserves delisted members in qualifying historical liquidity-universe months. Complete delisted-security coverage is therefore **PARTIAL/UNKNOWN**, not proven survivorship-free.

## 15. Survivorship assessment

**PARTIAL** — historical EQ securities with earlier last observations are present, but completeness of inactive/delisted securities cannot be independently established. The monthly PIT liquidity universe is a distinct artifact, not a guarantee that every raw security history is complete.

## 16. Point-in-time universe assessment

The separately published NSE liquidity universe has 97,000 rows and 194 rebalance dates from 2010-05-03 to 2026-09-01. TejHQ documents first-trading-day monthly top 500 by trailing 63-session turnover and says membership uses information available on the rebalance date. Duplicate/date/rank row checks are in `audit/universe_coverage.csv`. The ranking computation was not independently recalculated; status is **PARTIAL**. The universe was not used in a strategy or backtest. It is a distinct concept from applying the current NIFTY 500 list to all prior history.

## 17. Phase 3C fold coverage

**FAIL** — 2/15 train/validation/test windows contain all dates in the official NSE NIFTY calendar. Exact requested boundaries and availability are in `audit/phase3c_fold_coverage.csv`.

| Fold | Segment | Requested window | Official sessions | EQ-covered sessions | Missing | Status |
|---:|---|---|---:|---:|---:|---|
| 1 | train | 2021-06-09 → 2023-06-08 | 497 | 495 | 2 (2021-11-04;2022-10-24) | FAIL |
| 1 | validation | 2023-06-09 → 2023-12-08 | 125 | 124 | 1 (2023-11-12) | FAIL |
| 1 | test | 2023-12-11 → 2024-06-07 | 122 | 121 | 1 (2024-05-18) | FAIL |
| 2 | train | 2021-12-09 → 2023-12-08 | 497 | 495 | 2 (2022-10-24;2023-11-12) | FAIL |
| 2 | validation | 2023-12-11 → 2024-06-07 | 122 | 121 | 1 (2024-05-18) | FAIL |
| 2 | test | 2024-06-10 → 2024-12-06 | 124 | 123 | 1 (2024-11-01) | FAIL |
| 3 | train | 2022-06-09 → 2024-06-07 | 495 | 492 | 3 (2022-10-24;2023-11-12;2024-05-18) | FAIL |
| 3 | validation | 2024-06-10 → 2024-12-06 | 124 | 123 | 1 (2024-11-01) | FAIL |
| 3 | test | 2024-12-09 → 2025-06-06 | 123 | 123 | 0 (none) | PASS |
| 4 | train | 2022-12-09 → 2024-12-06 | 494 | 491 | 3 (2023-11-12;2024-05-18;2024-11-01) | FAIL |
| 4 | validation | 2024-12-09 → 2025-06-06 | 123 | 123 | 0 (none) | PASS |
| 4 | test | 2025-06-09 → 2025-12-08 | 126 | 125 | 1 (2025-10-21) | FAIL |
| 5 | train | 2023-06-09 → 2025-06-06 | 494 | 491 | 3 (2023-11-12;2024-05-18;2024-11-01) | FAIL |
| 5 | validation | 2025-06-09 → 2025-12-08 | 126 | 125 | 1 (2025-10-21) | FAIL |
| 5 | test | 2025-12-09 → 2026-06-08 | 121 | 119 | 2 (2026-02-01;2026-06-05) | FAIL |

This is calendar-coverage verification only. No Phase 3C strategy ran.

## 18. Lookahead and leakage assessment

**PARTIAL** — the raw source tree does not contain adjusted close, future corporate-action factors, future membership, or derived future-looking fields. The intended raw EQ projection would preserve that property, but it was withheld because date coverage failed. TejHQ's separate back-adjusted close uses factors from actions after each row, so that tree can encode future corporate actions relative to historical rows. The PIT universe is separate and is not selected in this task. Later corrections to old snapshots were not compared; revision risk remains.

Data quality and lookahead safety are distinct: clean bars do not establish point-in-time safety, and a point-in-time membership claim does not establish raw data completeness.

## 19. Known limitations

1. The published raw tree is already cleaned/deduplicated and drops some detected anomalies; the original NSE files and omitted rows are not available for this audit.
2. Official index calendar comparison begins only in 2020; earlier per-symbol completeness denominators are proxies.
3. TejHQ's stated source license is not an independent determination of the exchange's terms for commercial use.
4. Historical security disappearance cannot be equated with a delisting, and total dead-security completeness remains unknown.
5. Corporate action examples have not been externally checked against original NSE action notices.
6. Raw OHLC prices are not corporate-action adjusted. The dataset card documents adjusted close only; using raw bars for return comparisons can create action-related discontinuities. The adjusted close has future-action back-adjustment risk.
7. The point-in-time universe formula is documented but not recalculated independently.
8. The normalized dataset was withheld because the exact five-fold date-coverage gate failed; the provider adapter is isolated and not wired into the Phase 3C runner.

## 20. Final suitability verdict

**NOT_RESEARCH_READY** for the existing five-fold Phase 3C walk-forward requirement. Although the overall history spans all requested boundaries, the dataset misses official NSE EQ dates inside the required windows, so exact session coverage fails. The raw published rows pass local OHLCV/key checks, subject to source-side filtering and survivorship/adjustment limits. No normalized Phase 3C candidate was created because required coverage did not pass. No strategy results were generated or reinterpreted here.

### Normalized data candidate

Not created: the dataset misses official NSE EQ dates inside the required five-fold windows. The reason is also recorded in `data/tejhq/normalized/README.md`.


### Phase 3C execution approval

**Do not run Phase 3C until the user explicitly approves after reviewing this audit.**
