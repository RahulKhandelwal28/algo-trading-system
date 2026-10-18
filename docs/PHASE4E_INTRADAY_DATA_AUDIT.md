# Phase 4E — Historical Intraday Data Audit

Audit date: 2026-09-27  
Project result: **BLOCKED — no candidate is currently research-ready**  
Data-only scope: no strategy, risk, fill, Phase 3C/3D result, Phase 4B/4C/4D artifact, realtime setting, paper ledger, or broker integration was changed.

## 1. Executive Summary

The repository had no historical intraday dataset or `data/intraday/` tree before this audit. It already had a provider-independent `IntradayBar` model, validation, store, and `CsvIntradayDataProvider` in `src/nse_research/intraday/model.py`; that is the existing canonical path for future bars.

Nine sources were investigated: eight public dataset/library candidates and official NSE Data & Analytics product availability. One bounded five-symbol candidate sample was downloaded to `/private/tmp` only. The sample is real intraday-shaped data, but it fails the audit gates: 702 rows have blank numeric fields, its CSV omits exchange and series identity, volume and corporate-action adjustment are not documented, price/volume aggregates disagree materially with the local EOD reference on selected dates, and rights to the underlying data are not established. No sample or full archive was copied into this repository.

**PHASE4E_STATUS = BLOCKED_DATA_UNAVAILABLE.** No dataset is accepted. Phase 4B and Phase 4D data integration/replay remain blocked. This is a data-readiness result, not a strategy-performance result.

## 2. Candidates Investigated

| Candidate | What was inspected | Outcome |
|---|---|---|
| [Hugging Face `indian-stock-market-minute-data`](https://huggingface.co/datasets/xxparthparekhxx/indian-stock-market-minute-data) | Dataset card, public viewer schema and preview rows | Minute rows and explicit UTC timestamps are visible; full archive not downloaded. Exchange/series, provider entitlement, volume semantics, adjustment method and membership history remain unverified. Public row counts conflict (about 713M/715M versus 720,421,072). |
| [GitHub `nse-fno-1min-data`](https://github.com/voletiramu/nse-fno-1min-data) | README and release metadata | Claims 214 F&O underlyings and 5.5M one-minute bars for 2024-04-01–2026-04-30, attributed to Kite. The 464 MB release was not downloaded. Spot cash EQ identity and rights are not established. |
| [GitHub `NSE-Data`](https://github.com/ShabbirHasan1/NSE-Data), attributed to [Kaggle `stock-market-india`](https://www.kaggle.com/hk7797/stock-market-india) | README, file listing, five single-symbol minute CSVs | Five files were downloaded to `/private/tmp` and measured below. TCS's expected raw path returned 404; ICICIBANK request timed out. No full repository/Kaggle download was made. The repo identifies Kaggle as the minute-data source and has a GPL-3.0 repository license, which does not establish rights in the data. |
| [GitHub `ragnartrades/nse-data-1`](https://github.com/ragnartrades/nse-data-1) | README and actual raw RELIANCE CSV | Despite the minute-by-minute repository description, the inspected file has date-only daily records from 1996-01-01 to 2021-05-12 (6,375 rows). It is not intraday. |
| [GitHub `ramamet/nse1minR`](https://github.com/ramamet/nse1minR) | README and data directory/sample descriptions | README claims about 40M rows and 502 companies. Package is about 223 MB; no full package or RData sample was downloaded. Sample descriptions show 2013–2016 examples; timezone, explicit EQ identity, adjustment status, completeness and data rights remain unresolved. |
| [GitHub `Arpitjain1234/openchart`](https://github.com/Arpitjain1234/openchart) | README/API description | Library advertises NSE IDX/EQ/FO and several intervals, including 1m/5m. It is a request library, not a versioned historical archive. No endpoint was called. Retention, reproducibility, rate limits and data entitlement are not established. |
| [Kaggle `nifty-50-minute-data`](https://www.kaggle.com/datasets/debashis74017/nifty-50-minute-data) | Dataset page/search metadata and author discussion | Describes NIFTY 50 index minute data, not a cash-equity EQ universe. Files, rights, volume and timezone were not inspected. |
| [Kaggle `equiml`](https://www.kaggle.com/datasets/manognap2505/equiml) | Public card metadata | Actual files/schema were inaccessible in this audit environment. Card does not establish exact dates, symbols, exchange/series, volume or source rights. |
| Official NSE Data & Analytics | Official real-time, historical-product and data-policy pages | NSE lists 1-minute and 5-minute snapshot products and paid historical products; no complete free historical equity-minute archive was located or acquired. No subscription or access attempt was made. |

The detailed row-by-row inventory is [phase4e_intraday_dataset_inventory.csv](../results/phase4e_intraday_dataset_inventory.csv). Its `research_readiness` field uses only `RESEARCH_READY`, `RESEARCH_READY_WITH_LIMITATIONS`, `NOT_RESEARCH_READY`, or `UNAVAILABLE`.

## 3. Dataset Inventory

There are 9 inventory entries: 0 `RESEARCH_READY`, 0 `RESEARCH_READY_WITH_LIMITATIONS`, 7 `NOT_RESEARCH_READY`, and 2 `UNAVAILABLE`. The seven rejected entries are the Hugging Face dataset, F&O release, sampled NSE-Data files, ragnartrades file, ramamet package, OpenChart library, and Kaggle NIFTY 50 index dataset. The unavailable entries are the inaccessible equiml files and official NSE data not locally licensed/acquired.

The inventory records source URLs, measurable/claimed coverage with labels, identities, timezone/volume/rights limitations, provenance status, survivorship-bias status and the reason for each classification. Unverified facts are marked `UNKNOWN`, unavailable, or explicitly as source claims. No subjective quality score is used.

## 4. Schema Validation

The sampled `NSE-Data` file schema is `timestamp,open,high,low,close,volume`. It has no `symbol`, `exchange`, `series`, timeframe, volume-kind, status or source fields in each row; the symbol is represented only by the filename. Therefore it cannot be passed directly to the project's canonical provider without an explicit, documented conversion contract, and exchange/series must not be fabricated.

The new provider-independent validator is [intraday_validator.py](../src/nse_research/data/intraday_validator.py). It requires the existing provider's canonical CSV fields, rejects missing identity/timezone/price/volume information, reports duplicates/order/session gaps, and never inserts rows, shifts timestamps, or infers series. Gaps are reported rather than automatically filled. Explicit expected session dates are required to label absent dates as missing sessions; absent dates are not guessed to be holidays.

## 5. Timestamp Validation

All five sampled CSVs contain explicit `+05:30` timestamps. The first regular-session bar is timestamped `09:15:00+05:30`; times advance on minute boundaries and regular bars end at `15:29:00+05:30`. This is consistent with interval-start labels for regular-session one-minute bars. No timezone conversion was applied to source samples.

The Hugging Face viewer preview instead shows UTC-aware timestamps; an example at `03:45 UTC` corresponds to `09:15` India time. The new validator converts explicitly zoned timestamps to `Asia/Kolkata` while preserving the instant and rejects naive timestamps.

## 6. NSE Session Validation

Each sampled symbol contains 992 observed dates. Per-symbol bar counts have a minimum of 60, median 375 and maximum 376. 986 dates have 375 rows; 2017-07-10 has 180 rows; four dates have 60 evening rows; and four of five files have an additional `15:30` boundary row on 2020-04-27. Four evening groups run 18:30–19:29, 17:30–18:29, 18:15–19:14 and 18:15–19:14 across 2017-10-19, 2018-11-07, 2019-10-27 and 2020-11-14. These are consistent with special-session-shaped observations, but their NSE session status was not independently matched to official calendars in this sample audit.

If all 992 observed dates are provisionally treated as normal 09:15–15:30 sessions, there are 1,695 absent regular-minute slots per symbol. This count includes the four 60-row evening sessions, so it is not a measure of unexplained data loss. The 180-row date and extra 15:30 row remain unexplained by this source. For Phase 4E, their market-holiday/special-session/suspension cause is not inferred. No bar is added or removed from the sample.

The existing project calendar is not a complete historical NSE calendar. The validator accepts explicit special-session windows and caller-supplied expected sessions; it does not pretend the local 2026 calendar proves historical coverage.

## 7. OHLC Validation

Across the five temporary CSVs, 702 rows (141 each for RELIANCE, INFY, HDFCBANK and SBIN; 138 for BHARTIARTL) have all five numeric columns blank. Among the remaining parseable records, the sample audit found no nonpositive prices and no `high`/`low` relationship violations. The blank rows are invalid records; they were not dropped from or repaired in a project dataset because none was created.

## 8. Volume Validation

The source provides a `volume` number but does not say whether it is bar share volume, cumulative volume, or another quantity. Across the five files there are 222 zero-volume rows. The selected daily sum comparisons below do not consistently agree with the local daily reference, so volume is `UNKNOWN`; it is not safe for volume-dependent strategy inputs. No transformation from cumulative to incremental volume was attempted.

## 9. Symbol/Series Validation

The repo README says the minute folder contains NIFTY 50, next 100 and index data, and filenames use forms such as `RELIANCE__EQ__NSE__NSE__MINUTE.csv`. But the sampled row schema does not contain exchange or series fields, and there is no pinned instrument-master mapping with the CSVs. This is contextual evidence, not row-level identity proof. The sample cannot establish `NSE`/`EQ` identity for every bar. TCS could not be retrieved from the inspected path, so the exact minimum target list was not fully sampled.

The F&O release also does not establish that every record is cash equity rather than a derivative contract. Index-only rows are excluded from the cash-equity dataset target.

## 10. Coverage

The five sampled files each contain 370,545 or 370,546 data rows, spanning 2017-01-02 09:15 IST through 2021-01-01 15:29 IST. Combined sample size is 1,852,729 rows. This is an older segment and does not cover the preferred 2021–2026 period. The repository's README claims a 2017–2020 dataset and more symbols, but this audit did not verify the full archive.

The candidate date set includes all 986 dates present for these symbols in the local TejHQ NSE `EQ` EOD tree, plus six additional dates. TejHQ is a third-party publication of cleaned/typed EOD data, not the official original exchange files; the additional dates are not classified here as trading sessions or data errors.

## 11. Missing Data

Duplicate timestamp count is zero in each of the five files; timestamp order is monotonic. The regular-session slot calculation above reports gaps but cannot distinguish a no-trade minute, an omitted bar, a partial session, an exchange special session, or a source gap without a reliable session calendar and the vendor's bar-generation policy. The 2017-07-10 180-row session and the 2020-04-27 15:30 timestamp need source-owner explanation before use.

The sample's 702 blank numeric records are independently invalid regardless of their session cause. No missing interval or session has been filled.

## 12. Cross-Source Checks

No second historical minute source with overlapping audited rows was available. A sanity comparison was made for the five sampled symbols on 2017-01-02, 2019-01-02 and 2020-12-31 against the local TejHQ raw-price `EQ` EOD tree. This is a third-party reference, not an official NSE daily cross-check and not proof of candidate correctness.

On 2017-01-02, candidate daily OHLC aggregates were approximately half the TejHQ OHLC values for RELIANCE (ratios 0.495–0.496), INFY (~0.500), and HDFCBANK (~0.500); SBIN was within about 0.04%, while BHARTIARTL was close except its open (~1% difference). HDFCBANK remained around half on 2019-01-02. Most 2020-12-31 OHLC values were much closer, but selected closes still differed (for example RELIANCE by ₹3.50 and INFY by ₹0.80). On 2017-01-02 the aggregated minute volume differed from the daily value by +1,643,328 RELIANCE shares and +1,161,145 INFY shares; other sampled differences vary by symbol/date.

The price ratios could reflect adjustment-basis differences or source errors; the source does not document an adjustment methodology, so this audit does not assign a cause. Since the TejHQ raw EOD source also says it cleans/deduplicates data and drops certain anomalous rows, these comparisons are a discrepancy flag only. The exact dates in the repo's official NSE CM bhavcopy sample archive do not overlap the selected three dates, so no matching official daily comparison was available for this sample.

## 13. Corporate Actions

The sampled minute source does not declare prices raw, split-adjusted, dividend-adjusted or otherwise adjusted. It also does not supply an action factor or adjustment manifest. Cross-source half-price ratios on selected historical dates make the unknown adjustment basis material. No adjustment was applied, and the minute source was not mixed into any strategy/research dataset.

The local TejHQ EOD tree is documented as raw OHLCV with a separate adjusted-price tree. That documentation does not make it official original data and does not settle the sampled minute source's basis.

## 14. Survivorship Bias

No candidate provides a verified point-in-time NSE security master and historical membership files as part of the inspected sample. The NSE-Data README describes constituents but does not establish historical constituent membership for each observation. Survivorship bias for equity datasets is therefore `UNKNOWN`; any later research would need point-in-time membership and a security-identity/delisting history. The index-only NIFTY dataset is not a substitute for EQ constituents.

## 15. License/Provenance

The sampled repository points to Kaggle for its minute data; the GitHub repository's GPL-3.0 license is a code/repository license and does not prove rights from the original data provider or NSE. The Kite release and `ramamet/nse1minR` similarly expose code/research descriptions without a verified market-data grant. Hugging Face's MIT claim and `equiml` card text were recorded as publisher statements, not legal determinations. OpenChart's code/API terms do not grant rights to data served by an upstream exchange.

Official NSE pages state that market-data products include 1-minute/5-minute snapshot data and paid historical products. NSE's data-use policy applies to realtime, snapshot, end-of-day and historical data, and use/distribution is governed by a subscriber agreement. This audit found no complete free historical 1m/5m cash-equity archive on official pages and did not attempt to access subscription-only data. See [NSE real-time data products](https://www.nseindia.com/static/market-data/real-time-data-subscription), [NSE EOD/historical data products](https://www.nseindia.com/static/market-data/eod-historical-data-subscription), [NSE products tariff](https://www.nseindia.com/static/market-data/products-tariff), and [NSE data sharing policy](https://www.nseindia.com/static/market-data/nse-data-policy).

Sample files were downloaded on 2026-09-27 from the public GitHub raw path into temporary storage. The branch/revision was not pinned, so it is mutable-source provenance. SHA-256 checksums:

| Temporary sample | Rows (excluding header) | SHA-256 |
|---|---:|---|
| RELIANCE | 370,546 | `f81734065e748f3828f0decfb9c400955b362d109b4de5643cac4aacd04045d2` |
| INFY | 370,546 | `ac3d7a7f0acef50e71787563eaaa42f4eef36863c24148f92837710b6ea4a8ee` |
| HDFCBANK | 370,546 | `52fb6ca29dcad49d366d10045b4755b06b9b0b4cfd6745087674527ecdaf5eb1` |
| SBIN | 370,545 | `fb655672bb415d8c94ff1b08dba316d26575df4c65d37c594b70615ea1c27c7d` |
| BHARTIARTL | 370,546 | `64dfe5519d0859641e8d05a7cf107ae4f739a69d3f68ff38e238736f3129e1a7` |

No dataset manifest was written because no dataset was accepted. No provenance fact was inferred to make one pass.

## 16. Accepted Dataset

**NONE.** No `data/intraday/normalized` bars or accepted-dataset manifest were created. The five temporary sample CSVs remain outside the project at `/private/tmp/phase4e_*_minute.csv` and must not be treated as production/research inputs.

## 17. Limitations

- Public dataset metadata is not a license to use or redistribute underlying market data.
- Some candidates were only inspectable through public metadata because the full archive was large, inaccessible, or not licensed locally.
- The TejHQ EOD comparison is not an official matching-date daily source.
- There is no pinned candidate revision, validated historical EQ security master, historical holiday/special-session calendar, or adjustment specification for an accepted provider.
- This audit makes no claim about strategy profitability or trading suitability.

## 18. Integration Instructions

The current `CsvIntradayDataProvider` already consumes canonical bars. If an authorized source is later supplied, preserve its raw copy under `data/intraday/raw/` (ignored by Git), record actual source/licensing/revision/checksum facts in a manifest, normalize only explicit source fields to the existing canonical columns, and run `validate_intraday_dataset` with explicit expected symbols/session dates and documented special sessions. Keep the source timestamp instant, convert aware timestamps to `Asia/Kolkata`, and do not invent exchange/series/volume meaning or fill gaps.

Only after the dataset passes schema, identity, coverage, quality, provenance and research-rights checks should a canonical sample be staged, then a Phase 4B data-load/scanner integration test be considered. Phase 4D replay remains blocked until that gate passes. This audit did not invoke Phase 4B scanner signals or Phase 4D replay.

## 19. Rejected Candidates

- Hugging Face minute dataset: actual preview rows exist, but key data identity, rights, adjustment and volume facts are unresolved, and public row counts conflict.
- F&O 1m release: cash EQ identity is not established and the large archive was not sample-inspected.
- NSE-Data sample: 702 blank numeric records, omitted row-level identity, unknown volume and adjustment semantics, discrepancies and unresolved data rights.
- ragnartrades: inspected file is daily, not intraday.
- ramamet package: claimed one-minute data not independently sampled; critical identity/timezone/rights/completeness fields unknown.
- OpenChart: API/library without a stable, versioned historical dataset or proven upstream rights.
- Kaggle NIFTY 50 minute data: index bars do not meet the cash-equity EQ requirement.

Equiml files/schema and official NSE products were unavailable locally for this audit; official NSE subscription products remain a legitimate procurement path, not a free archive discovered here.

## 20. Next Step

Obtain an authorized NSE cash-market 1m/5m historical delivery with written allowed-use terms and sample bars for at least the required symbols, including explicit instrument identity, volume semantics, timestamp convention, adjustment status, historical universe/security mapping, session calendar, and a stable source revision. Submit a small sample first. Until then:

```text
PHASE4E_STATUS = BLOCKED_DATA_UNAVAILABLE
Phase 4B data integration = BLOCKED
Phase 4D historical replay = BLOCKED
Realtime = REALTIME_PROVIDER_NOT_VALIDATED
Real orders = 0
Broker calls = 0
Live trading = DISABLED
```
