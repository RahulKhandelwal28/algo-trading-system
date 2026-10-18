# TejHQ Missing Session Investigation

Audit date: 2026-09-26  
Final classification: **TEJHQ_HAS_CONFIRMED_DATA_GAPS**

## 1. Objective and scope

This investigation checked the 13 official NSE NIFTY dates absent from the TejHQ NSE `EQ` date set. For each date, the question was whether NSE Capital Market (CM) equity securities actually traded. The analysis reads the existing TejHQ Parquet files, official NIFTY export, existing Phase 3C fold coverage file, and NSE CM bhavcopies downloaded from NSE's public All Reports archive.

No strategy or backtest was run. Phase 3C was not run. Phase 2/3/3C results, strategy logic, risk and cost settings, Kaggle data, licensed NSE provider, official index CSVs, and raw TejHQ data were not changed. The official CM source archives and investigation outputs are stored under `data/tejhq/audit/`.

## 2. Previous audit finding

The existing [TejHQ data audit](TEJHQ_DATA_AUDIT.md) found 1,673 official NIFTY dates and 1,660 TejHQ `EQ` dates from 2020-01-01 through 2026-09-25. Its date-alignment check flagged the 13 differences below. That comparison alone could not distinguish an equity-data omission from an index-only or non-equity date.

## 3. Dates investigated and result

All 13 dates are present in `data/nse_exports/index_NSEI.csv`. The exact NSE CM archive for every date exists and contains positive `EQ` counts. TejHQ has zero `EQ` rows on each date. Thus, none is a legitimate no-equity session, and none is an index/derivatives-only session.

| Date | NIFTY row | TejHQ EQ rows | NSE CM rows | NSE EQ | NSE BE | NSE BZ | Unique EQ symbols | NSE CM session context |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 2020-02-01 | Yes | 0 | 1,886 | 1,471 | 168 | 28 | 1,471 | Saturday; official CM report confirms EQ trading. Special-session reason not independently documented here. |
| 2020-11-14 | Yes | 0 | 1,878 | 1,606 | 72 | 39 | 1,606 | Saturday Muhurat session. |
| 2021-11-04 | Yes | 0 | 2,017 | 1,668 | 129 | 34 | 1,668 | Diwali Muhurat session. |
| 2022-10-24 | Yes | 0 | 2,241 | 1,797 | 173 | 29 | 1,797 | Diwali Muhurat session. |
| 2023-11-12 | Yes | 0 | 2,463 | 1,805 | 265 | 24 | 1,805 | Sunday Muhurat session. |
| 2024-05-18 | Yes | 0 | 2,512 | 1,892 | 245 | 18 | 1,892 | Saturday special live CM session with an intraday switch to the Disaster Recovery site. |
| 2024-11-01 | Yes | 0 | 2,746 | 1,912 | 308 | 27 | 1,912 | Diwali Muhurat session. |
| 2025-10-21 | Yes | 0 | 3,039 | 2,291 | 139 | 30 | 2,291 | Diwali Muhurat session. |
| 2026-02-01 | Yes | 0 | 3,229 | 2,411 | 96 | 34 | 2,411 | Sunday live CM session for the Union Budget presentation. |
| 2026-06-05 | Yes | 0 | 3,417 | 2,443 | 219 | 30 | 2,443 | Friday CM report with EQ records. |
| 2026-06-15 | Yes | 0 | 3,455 | 2,428 | 244 | 37 | 2,428 | Monday CM report with EQ records. |
| 2026-06-16 | Yes | 0 | 3,420 | 2,431 | 238 | 26 | 2,431 | Tuesday CM report with EQ records. |
| 2026-06-22 | Yes | 0 | 3,457 | 2,419 | 259 | 39 | 2,419 | Monday CM report with EQ records. |

`total rows` counts the records in the downloaded CM CSV. `EQ`, `BE`, and `BZ` count the explicit NSE series values. Each date's EQ row count equals its unique EQ symbol count, so the reports contain one EQ row per symbol for these dates.

The machine-readable row-level results are in [missing_session_investigation.csv](../data/tejhq/audit/missing_session_investigation.csv). Original official NSE ZIPs and their SHA-256 digests are retained in [data/tejhq/audit/official_cm/](../data/tejhq/audit/official_cm/README.md).

## 4. Evidence and method

The source was the official [NSE All Reports](https://www.nseindia.com/all-reports) page, under Equities / Capital Market reports. NSE's page lists the legacy `CM - Bhavcopy(csv)` and says it was discontinued effective 2024-07-08 in favor of `CM-UDiFF Common Bhavcopy Final(zip)`. I used the page's report/date controls to download the files; no unofficial data, endpoint scraping, CAPTCHA circumvention, or authentication bypass was used.

The six files through 2024-05-18 use the legacy report and expose an explicit `SERIES` column. The seven files from 2024-11-01 onward use UDiFF and expose `SctySrs`. Counts above filter only explicit `EQ`, `BE`, and `BZ` values. Each CSV's trade date matched the requested date. No EQ series was inferred or fabricated.

The NIFTY comparison uses the existing official export [index_NSEI.csv](../data/nse_exports/index_NSEI.csv), read without modifying it. TejHQ counts come from the raw yearly Parquet partitions, filtered to `series == "EQ"`; no raw file was rewritten. The retained NSE ZIPs make the row counts independently checkable.

## 5. Special-session evidence

NSE notices directly document the following calendar exceptions:

| Date | Official NSE notice |
|---|---|
| 2020-11-14 | [CMTR/46230 — Muhurat Trading session on Saturday, November 14, 2020](https://nsearchives.nseindia.com/content/circulars/CMTR46230.pdf) |
| 2021-11-04 | [CMPT/50078 — Muhurat Trading on November 04, 2021](https://nsearchives.nseindia.com/content/circulars/CMPT50078.pdf) |
| 2022-10-24 | [ISC/54105 — special trading session on Monday, October 24, 2022](https://nsearchives.nseindia.com/content/circulars/ISC54105.pdf) |
| 2023-11-12 | [ISC/59313 — special Sunday Muhurat session](https://nsearchives.nseindia.com/content/circulars/ISC59313.pdf) |
| 2024-05-18 | [MSD/61893 — special live session on Saturday in Equity and Equity Derivatives, with CM timings](https://nsearchives.nseindia.com/content/circulars/MSD61893.pdf) |
| 2024-11-01 | [CMTR/64628 — special Friday Muhurat session](https://nsearchives.nseindia.com/content/circulars/CMTR64628.pdf) |
| 2025-10-21 | [CMTR/70319 — special Tuesday Muhurat session](https://nsearchives.nseindia.com/content/circulars/CMTR70319.pdf) |
| 2026-02-01 | [CMTR/72349 — live Sunday session for the Union Budget presentation](https://nsearchives.nseindia.com/content/circulars/CMTR72349.pdf) |

For 2020-02-01, the official CM bhavcopy itself proves EQ activity on the Saturday, but a directly retrievable official notice explaining that date's special-session reason was not included in the evidence set. The reason is therefore left unspecified rather than inferred. The remaining four dates in June 2026 were weekdays; the official CM report and explicit EQ rows prove equity activity without needing a special-session explanation.

These circumstances explain why weekday/weekend heuristics are unsafe. They do not explain away the TejHQ absence: all nine weekend/festival/budget/DR dates listed above still have official EQ rows.

## 6. Per-date classification

All 13 dates are classified **`TEJHQ_MISSING_EQ_DATA`**. NSE CM archives exist for 13 of 13 dates and report 1,471–2,443 EQ rows per date. TejHQ contains zero EQ rows on all 13.

Counts by requested category:

| Category | Count |
|---|---:|
| `LEGITIMATE_NO_EQ_SESSION` | 0 |
| `LEGITIMATE_SPECIAL_SESSION` as a reason to treat the gap as non-missing | 0 |
| `TEJHQ_MISSING_EQ_DATA` | 13 |
| `OFFICIAL_SOURCE_UNAVAILABLE` | 0 |
| `UNKNOWN` for whether NSE EQ traded | 0 |

Special-session context is documented separately; it does not override the official CM row evidence. A session can be a legitimate exchange calendar exception and still be a genuine missing-data date in TejHQ.

## 7. Overall coverage reassessment

For 2020-01-01 through 2026-09-25:

| Measure | Sessions |
|---|---:|
| Official NIFTY dates | 1,673 |
| TejHQ EQ dates present | 1,660 |
| Questioned dates absent from TejHQ EQ | 13 |
| TejHQ EQ dates outside the NIFTY date set | 0 |
| NIFTY-date coverage | 1,660 / 1,673 = **99.22%** |

The valid-equity-session denominator for this comparison remains 1,673: the 1,660 dates with TejHQ EQ bars are observed EQ dates, and all 13 absent dates have official NSE CM reports with EQ rows. Therefore, none of the 13 dates can be removed as an index-only or no-equity date. This is a combined-evidence session count; the investigation directly re-downloaded official NSE CM reports for all 13 questioned dates, but did not re-download and independently recount official bhavcopies for each of the other 1,660 dates. It does not claim an exhaustive official-file audit of every date in the period.

Accordingly, the valid-session coverage estimate remains 1,660 / 1,673 = **99.22%**. The previous raw calendar comparison was not falsely failing because weekends or Muhurat sessions lacked equity activity: all 13 differences are real date-level omissions.

## 8. Five-fold reassessment

The original fold windows and date counts in `data/tejhq/audit/phase3c_fold_coverage.csv` were retained. Every unique missing date that falls inside a fold window has an official CM report with EQ records. The NIFTY denominator therefore does not decrease after the investigation. The table treats other dates already present in TejHQ EQ as observed EQ sessions; the eight unique disputed dates are individually supported by NSE CM evidence.

| Fold | Segment | NIFTY / supported EQ sessions | TejHQ EQ sessions | Confirmed missing sessions | Coverage |
|---:|---|---:|---:|---:|---:|
| 1 | Train | 497 | 495 | 2 | 99.60% |
| 1 | Validation | 125 | 124 | 1 | 99.20% |
| 1 | Test | 122 | 121 | 1 | 99.18% |
| 2 | Train | 497 | 495 | 2 | 99.60% |
| 2 | Validation | 122 | 121 | 1 | 99.18% |
| 2 | Test | 124 | 123 | 1 | 99.19% |
| 3 | Train | 495 | 492 | 3 | 99.39% |
| 3 | Validation | 124 | 123 | 1 | 99.19% |
| 3 | Test | 123 | 123 | 0 | 100.00% |
| 4 | Train | 494 | 491 | 3 | 99.39% |
| 4 | Validation | 123 | 123 | 0 | 100.00% |
| 4 | Test | 126 | 125 | 1 | 99.21% |
| 5 | Train | 494 | 491 | 3 | 99.39% |
| 5 | Validation | 126 | 125 | 1 | 99.21% |
| 5 | Test | 121 | 119 | 2 | 98.35% |

Eight unique confirmed gaps fall in the fold windows; because the windows overlap, they account for 22 missing segment-days across 15 segments. Only 2 of 15 segments meet exact date coverage; 13 remain below that gate. This investigation does not rerun folds or strategies.

## 9. Daily strategy impact

The existing daily experiment counts sessions from the benchmark date index and compares each symbol's observed dates to those sessions; it records omissions and states that no bars are filled (`src/nse_research/experiment.py`, `_data_manifest` and its warning path). A missing whole-market date removes the synchronized daily equity observation for all securities from TejHQ. A daily process that requires a bar for each exchange session cannot treat these as non-trading dates; rolling bar counts or session alignment can otherwise shift.

This is a data-availability assessment only. No hypothetical P&L, trades, or strategy outputs were calculated. No bars were forward-filled, interpolated, or fabricated.

## 10. TejHQ daily symbol-count quality check

Across the raw TejHQ EQ dataset there are 4,122 dates and 6,577,021 EQ rows. Independently grouping the raw EQ rows by date and symbol found zero duplicate date/symbol rows. Symbols per observed EQ date have:

| Statistic | EQ symbols/date |
|---|---:|
| Median | 1,499 |
| 1st percentile | 1,187 |
| 5th percentile | 1,265 |
| Minimum | 1,139 (2010-02-08) |
| Maximum | 2,668 (2026-09-23) |

No arbitrary sparse-session cutoff was set. The observed minimum is an early 2010 date, not one of the questioned 2020–2026 sessions. The 13 questioned dates are materially different: TejHQ has zero securities for the entire date while each official NSE report has at least 1,471 EQ securities.

## 11. Weekend and special-session calendar assessment

`NseTradingCalendar.classify()` already gives precedence to observed market data, so an observed special Saturday/Sunday is classified as `TRADING_DAY`; configured special sessions without data fail closed as `DATA_UNAVAILABLE`. Ordinary weekends with no data are classified as non-trading days.

I found a separate gap in `latest_completed_candidate()`: before 15:45 IST it walked backward over every Saturday/Sunday, even if the calendar explicitly marked that weekend date as `SPECIAL_SESSION`. I made a calendar-only correction so it preserves a configured special session while skipping ordinary weekends and holidays. A regression assertion now verifies that Monday morning selects the configured prior Sunday session, while a normal weekend still resolves to Friday. The check does not cause a run or create a trade. The correction is in `src/nse_research/forward/calendar.py`, with coverage in `tests/test_forward_paper.py`.

The checked-in calendar file covers 2026. Special sessions in other historical years are not automatically inferred from NIFTY rows; any calendar-dependent backtest or paper process must use authoritative special-session dates for its requested period.

## 12. Remaining uncertainty

- The official NSE archive reports for all 13 disputed dates were retrieved and parsed; there is no uncertainty about whether those dates had NSE EQ records.
- The exact reason for the 2020-02-01 special Saturday session is not documented in this report because an official notice with retrievable evidence was not obtained. This does not affect the direct CM EQ evidence or the missing-data classification.
- The overall and per-fold session denominator combines observed TejHQ EQ dates with direct NSE CM validation of the previously absent dates. It is not a fresh, day-by-day official bhavcopy census of all 1,673 dates.
- TejHQ is a third-party published dataset. This check establishes that its snapshot omits the dates, not why the upstream TejHQ publication omitted them or whether the omissions can be corrected in a future licensed release.

## 13. Recommendation for Phase 3C eligibility

The original date-coverage failure is **not** an index-calendar false positive. It is confirmed for 13 genuine NSE CM EQ dates, including special and weekend sessions. Keep Phase 3C withheld under the existing complete-session coverage requirement until the data source supplies these dates or a separately approved, explicit missing-session methodology changes that requirement. Do not drop the dates from the denominator, add synthetic rows, forward-fill, or edit existing Phase 3C results.

Final classification: **`TEJHQ_HAS_CONFIRMED_DATA_GAPS`**.
