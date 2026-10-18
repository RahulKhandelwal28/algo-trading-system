# Phase 4F Final Code & Data-Integrity Audit

Audit date: 2026-09-27  
Repository: `/Users/rahul.khandelwal/Documents/personal/algo_trading_system`  
Final status: **PASS**

## Executive Summary

Phase 4F's intake and replay path is fail-closed for unregistered, unvalidated, non-research-ready, altered, or incomplete datasets. It does not acquire data, create missing bars, use synthetic fallback data, or call a broker. No authorized production intraday dataset is present.

The initial audit found a legacy entry point that accepted arbitrary `--data` and defaulted to the persistent Phase 4C paper ledger. Phase 4F.1 closes that bypass: the legacy `REPLAY` mode now returns `REPLAY_DATA_MUST_BE_PHASE4F_REGISTERED`, and the supported replay command requires Phase 4F registration/readiness, canonical output, observed session coverage, and an explicit temporary ledger. The Phase 4D execution engine and Phase 4C paper rules were not changed.

Phase 4F.1 implementation and verification details are recorded in [PHASE4F_1_REPLAY_SAFETY_FIX.md](PHASE4F_1_REPLAY_SAFETY_FIX.md).

Five Phase 4F gate weaknesses were small enough to fix within the stated audit exception: a subclass could bypass the canonical-provider constructor; registry/certificate flags could promote a dataset despite the Phase 4E report; historical session evidence was optional despite the 2026-only local holiday table; delivery-metadata changes were not rechecked by readiness; and UNKNOWN adjustment basis could reach readiness. These are now guarded and covered by tests. No strategy, Phase 4C risk rule, Phase 4D execution behavior, Phase 3C/3D result, broker integration, or live control was changed.

## Files Reviewed

The repository has no `.git` directory, so a tracked Phase 4F diff could not be generated. The review covered the complete Phase 4F file set present in the workspace and its direct ingestion, scanner, paper-replay, calendar, and safety boundaries: **41 files**.

Phase 4F implementation and artifacts (18):

- `.gitignore`
- `data/intraday/README.md`, `data/intraday/datasets.json`
- `docs/PHASE4F_DATA_INGESTION_CONTRACT.md`, `docs/INTRADAY_DATA_DELIVERY_SPEC.md`, `docs/INTRADAY_PROVIDER_CHECKLIST.md`
- `results/phase4f/README.md`
- `src/nse_research/data/intraday_ingestion.py`
- `src/nse_research/intraday/phase4f_adapter.py`
- `scripts/register_intraday_dataset.py`, `scripts/validate_intraday_dataset.py`, `scripts/normalize_intraday_dataset.py`, `scripts/check_intraday_readiness.py`, `scripts/replay_intraday_session.py`
- `tests/test_phase4f_ingestion.py`
- `tests/fixtures/intraday/README.md`, `tests/fixtures/intraday/synthetic_1m.csv`, `tests/fixtures/intraday/synthetic_5m.csv`

Boundary and regression files (23):

- `src/nse_research/intraday/model.py`, `src/nse_research/intraday/scanner.py`, `src/nse_research/data/intraday_validator.py`
- `src/nse_research/execution/phase4c.py`, `src/nse_research/execution/phase4c_models.py`, `src/nse_research/execution/phase4d.py`, `src/nse_research/execution/broker.py`
- `src/nse_research/forward/calendar.py`, `data/paper_trading/nse_cash_holidays_2026.csv`
- `scripts/run_phase4d_intraday_paper.py`, `requirements.txt`
- `tests/test_intraday_scanner.py`, `tests/test_intraday_validator.py`, `tests/test_phase4d_orchestrator.py`, `tests/test_phase4c_execution.py`, `tests/test_costs_and_risk.py`
- `tests/test_phase3_validation.py`, `tests/test_phase3c_kaggle.py`, `tests/test_phase3d_daily_acquisition.py`, `tests/test_phase3d_personal_intake.py`, `tests/test_phase3d_personal_preflight.py`, `tests/test_phase3d_personal_reports.py`, `tests/test_phase3d_preflight.py`

## Data Gate Audit

**Phase 4F intake path:**

1. Registration requires an explicit input path, identifiable CSV/CSV.GZ/CSV.ZIP/Parquet schema, and a declared `1m`, `5m`, or raw-only tick type. Daily `1d` registration is rejected.
2. Raw input bytes and delivery metadata are copied and hashed. Validation stages bounded row batches in SQLite and runs the existing Phase 4E validator.
3. Normalization requires Phase 4E `PASS`, rechecks raw hashes, revalidates before writing, writes atomically, and records `IN_PROGRESS`, `INCOMPLETE`, or `COMPLETE` state.
4. Canonical provider construction and Phase 4F Phase 4B/4D adapters require Phase 4E research readiness, accepted certificates, hash-pinned reports/manifests, intact raw/metadata and canonical files, and a complete normalized manifest.

Audit fixes now require the report itself to say Phase 4E `PASS` and `research_ready=true`; require the normalized manifest and accepted certificate to agree; recheck the delivery-metadata checksum; reject provider subclasses at the Phase 4B adapter; and require source-supplied expected sessions plus a documented common `RAW` or `ADJUSTED` basis. Rows whose normalized session date is outside `expected_sessions` are rejected. UNKNOWN or undocumented price basis blocks research readiness.

Normalization may still produce a canonical artifact after row validation when research readiness is false. That artifact cannot pass the Phase 4F canonical provider or replay readiness checks.

**Phase 4F.1 replay-boundary fix:** `scripts/run_phase4d_intraday_paper.py --mode REPLAY` now exits before constructing `Phase4DOrchestrator`. If `--data` is supplied it emits `REPLAY_DATA_MUST_BE_PHASE4F_REGISTERED`; no data file is opened and no ledger is created. The supported `scripts/replay_intraday_session.py` route checks registry membership, Phase 4E readiness, canonical provider hashes, observed session coverage, an explicit temporary ledger, and rejects the configured persistent ledger. The existing orchestrator remains unchanged.

## Synthetic Data Isolation

Checked-in Phase 4F fixtures are clearly labeled `SYNTHETIC_TEST_FIXTURE` and kept under `tests/fixtures`. The Phase 4F.1 end-to-end test builds its additional labeled fixture only inside pytest's temporary project. Neither can be selected by the production registry as a fallback. The separate legacy Phase 4D `SYNTHETIC_TEST` mode is explicit. Production replay retains its synthetic-source block; the end-to-end test temporarily overrides that block inside the test process only to exercise the whole registered→ready→canonical→paper path with labeled test data.

The actual readiness CLI returned `PHASE4F_NO_RESEARCH_READY_DATA` with exit code 2. No CLI generated signals or orders during this audit.

## Daily/Intra-day Boundary

Phase 4F accepts bars only at `1m` or `5m`, or tick records that remain raw-only. The existing test verifies `1d` fails registration with `TIMEFRAME_REQUIRED`. No daily-to-intraday resampling, timestamp fabrication, or conversion exists in the Phase 4F normalizer. Phase 3D/Yahoo daily data cannot enter the Phase 4F provider without an explicit intraday registration and all readiness gates. The legacy Phase 4D direct-CSV replay route is disabled.

## Timezone Audit

- Canonical timestamps are offset-aware ISO-8601 in `Asia/Kolkata`; UTC is converted as an instant, and BAR_END is shifted only when explicitly declared.
- Existing tests cover UTC conversion, IST representation, naive timestamps without documentation, and malformed/unknown conventions.
- An additional check confirmed `2026-09-28T03:45:00Z` remains the same instant and becomes `09:15+05:30` in India.
- Naive values are rejected unless the source timezone and its documentation are supplied. Ambiguous and nonexistent `America/New_York` DST local times both fail with `TIMESTAMP_AMBIGUOUS_LOCAL_TIME`; there is no silent fold choice.
- Session tests cover 09:15, 15:29, 15:30, weekends, and a listed holiday.

## Session Audit

Phase 4F reuses `default_nse_calendar()` and the existing `NseTradingCalendar`; it creates no competing calendar. The checked-in holiday file covers 2026, not the full historical period. The audit fix therefore makes delivery-supplied `expected_sessions` mandatory for research readiness and rejects out-of-list bar dates. It reports absent sessions/gaps without treating missing data as a holiday. Special-session bars require explicit hours. Session-list truth and source authority remain provider-documentation responsibilities.

## OHLC Audit

The validator rejects non-finite/malformed and non-positive OHLC values and enforces `high >= max(open, close)`, `low <= min(open, close)`, and `high >= low`; valid fixture bars pass. These rules cover each requested relationship, including high below open/close/low and low above open/close.

## Duplicate Audit

Canonical uniqueness is `(symbol, timeframe, normalized timestamp)`. SQLite reconciliation detects duplicates across validation batches; duplicate rows fail validation and are never aggregated. Timezone normalization means equivalent instants with different source offsets collide on the same canonical identity.

## Gap Audit

Coverage reports missing intervals, missing sessions, and missing symbol sessions; it does not forward-fill, interpolate, fabricate volume, or add bars. Zero-volume bars remain present. Gaps can remain in a dataset classified `RESEARCH_READY_WITH_LIMITATIONS`; callers must inspect the coverage report before research. Phase 4B receives only observed canonical bars.

## Volume Audit

Incremental shares, cumulative shares, trade count, and UNKNOWN remain distinct. Negative/malformed volume is rejected; zero is retained. Cumulative-to-incremental conversion requires an explicit documented rule and decreases/resets fail. UNKNOWN and trade-count semantics do not satisfy the usable-share-volume readiness gate, so volume-dependent research is blocked.

## Symbol/Exchange/Series Audit

Rows require explicit NSE/EQ identity. Tests reject BSE and non-EQ `BE`, `BZ`, `FUT`, `OPT`, and missing series. Missing identity can only be supplied by an explicit documented mapping; conflicting row identity fails. Duplicate canonical keys catch same-time alias collisions. There is no filename-based symbol inference.

## Manifest/Hash Audit

Raw file manifests include relative paths, sizes, and SHA-256; dataset fingerprints change when input bytes change. Normalized CSV values preserve source decimal text and the normalized hash is checked against both manifest and registry. Readiness now also checks the Phase 4E report hash and delivery-metadata checksum. Relative per-file paths are stable within a multi-file delivery; source manifests also retain the absolute intake path for provenance. Hashes detect changes relative to local registry state; they do not authenticate the provider or prove licensing.

## Idempotency Audit

An isolated `/tmp` synthetic test-only dataset was registered twice (`ALREADY_REGISTERED`), validated twice (`PASS` both times with a stable report hash), and normalized twice (`COMPLETE`, then `ALREADY_COMPLETE`). It retained one registry entry and byte-identical normalized manifest. These operations did not touch project data.

## Recovery Audit

An isolated ready test fixture was changed to `IN_PROGRESS`; readiness returned `NORMALIZED_DATASET_INCOMPLETE`. Atomic output writes and incomplete markers prevent partial output from authorizing use. A crash after the complete manifest is written but before the registry pins its hash also fails closed; operator repair may be needed rather than automatic recovery.

## Memory Audit

Raw CSV and Parquet ingestion is streamed; Parquet uses `iter_batches`, and SQLite staging bounds row batches. However `_validation_report()` retains every per-batch `IntradayValidationReport` in `phase_reports`, and `normalize_dataset()` separately retains the `reports` list before merging. Those reports include per-batch session-coverage dictionaries. Memory therefore grows with the number of batches and symbol/session keys, despite bounded raw rows. A multi-year, broad-universe dataset with hundreds of millions of bars could use substantial RAM or be killed by the OS. The available large-ish test is only 30 symbols × 375 bars; production-scale behavior was not measured. This is a **HIGH** scalability risk, not a silent-data-corruption path.

There is also no registry-wide lock around concurrent registrations. Two simultaneous distinct registrations could race when atomically replacing `datasets.json`; rerunning registration recovers an orphaned raw copy, but one registry entry could be lost. Avoid concurrent registration commands. This is **LOW** operational risk.

## Phase 4B Boundary

`CanonicalIntradayDatasetProvider` verifies readiness and delegates bar parsing/validation to the existing CSV provider. `Phase4BScannerAdapter` feeds sorted canonical bars into the existing scanner and does not duplicate indicators, strategies, or signal logic. The adapter now requires the exact provider type and rechecks its accepted dataset/path. A future-bar shock test confirms earlier scanner signals are unchanged; shuffled source rows normalize to identical ordered CSV bytes. Direct use of the existing Phase 4B scanner API remains possible for unit tests and is not itself a Phase 4F data-ingestion route.

## Phase 4D Boundary

The Phase 4F `Phase4DReplayAdapter` requires a registered, ready dataset and a ledger path under the system temporary directory, verifies that the requested date has observed session coverage, then passes the accepted canonical path to the existing Phase 4D orchestrator. It rejects the persistent Phase 4C ledger. That adapter has no broker provider. The standalone Phase 4D `REPLAY` CLI is disabled and cannot pass arbitrary `--data` into the orchestrator. The Phase 4D orchestrator and execution behavior remain unchanged.

## Live-Safety Audit

Phase 4F adds no broker, order-submission, credential, or realtime-provider code. Phase 4D paper configuration rejects live mode, realtime mode, and an enabled broker provider; the `LIVE` CLI mode reports unavailable. No broker was contacted, no live order was submitted, and no live-trading setting was changed. **Live execution remains disabled; broker calls: 0.**

## Lookahead Audit

The existing future-bar shock test was reviewed and passed. Adding an extreme later bar does not change earlier signal IDs. Scanner behavior remains owned by Phase 4B.

## Determinism Audit

Sorted and shuffled copies of the same fixture produce byte-identical canonical CSVs with the same event ordering. Registration IDs are stable for the same declared revision and conflicting source bytes under that ID fail. Lifecycle timestamps (`registered_at`, `accepted_at`, `complete_at`) are intentionally time-dependent; canonical data ordering and bytes are stable.

## Regression Tests

- Full suite after Phase 4F.1: **539 collected, 539 passed; 0 failed** (`PYTHONPATH=src .venv/bin/pytest -q`, exit 0).
- `python -m compileall src scripts tests`: passed.
- All six Phase 4F and Phase 4D CLI `--help` checks: passed.
- Actual blocked CLI checks: arbitrary legacy `--data`, missing ledger, unregistered dataset, and persistent ledger each exited 2 with the required status; no temporary ledger was created.
- Phase 4F.1 synthetic end-to-end replay: registered and Phase 4E-validated test fixture passed through canonical provider, Phase 4B scanner, Phase 4C paper engine, and Phase 4D. Two separate temporary-ledger runs produced matching signals, orders, fills, and trades. This fixture is test-only; the source-label block remains active in production.
- Actual readiness CLI: `PHASE4F_NO_RESEARCH_READY_DATA`, exit 2; the production registry has zero datasets.

## Protected Artifact Integrity

SHA-256 was compared before and after Phase 4F.1 for **363 protected files**: all result artifacts, Phase 4B/4C/4D source modules, the Phase 3C/3D universe files, configuration, and the persistent paper ledger. Changed: **0**; missing: **0**; unexpected additions in the protected set: **0**. A second comparison against the saved **368-file pre-Phase-4F protected baseline** also found 0 changed and 0 missing files. The Phase 4F.1 before/after manifests are `/private/tmp/phase4f_1_protected_before.json` and `/private/tmp/phase4f_1_protected_after.json`.

## Findings

### CRITICAL — fixed in Phase 4F.1

1. **Standalone Phase 4D replay bypassed Phase 4F readiness.** The legacy CLI now blocks `REPLAY` before orchestrator construction. The supported Phase 4F command enforces the registered dataset, Phase 4E readiness, canonical provider, observed session, and temporary-ledger requirements. Regression tests cover unknown/non-ready data, arbitrary `--data`, missing and persistent ledgers, missing sessions, full synthetic test-only execution, and idempotent outputs.

### CRITICAL — fixed during this audit

1. The Phase 4B adapter accepted subclasses that could skip canonical-provider initialization. It now requires the exact provider type and rechecks readiness/path.
2. Registry and certificate flags alone could promote a non-ready fixture. Readiness now verifies the hash-pinned Phase 4E report, report classification, and normalized-manifest readiness state.
3. Historical weekdays outside the local 2026 calendar could be misclassified. Readiness now requires provider-supplied `expected_sessions`, and rows outside it fail.
4. Readiness did not recheck the copied delivery-metadata sidecar. It now verifies its registered checksum.
5. UNKNOWN adjustment basis could reach readiness. It now requires a documented, dataset-wide `RAW` or `ADJUSTED` basis.

### HIGH

1. Batch validation report lists can grow with the full dataset size and retain coverage dictionaries; a very large multi-year universe may exhaust memory.
2. Price adjustment is represented by one common sidecar declaration. The importer cannot independently detect a provider mixing adjusted and raw partitions if the delivery documentation falsely claims a uniform basis.

### MEDIUM

1. `RESEARCH_READY_WITH_LIMITATIONS` can still include substantial missing intraday intervals. Coverage is reported and no bars are synthesized, but the caller must review it before research replay.
2. No authorized sample was available, so actual provider schema, licensing terms, adjustment claims, and production-scale memory behavior remain unverified.

### LOW

1. Concurrent registrations are not serialized across the registry file; run registration commands sequentially.

## Final Status

**PASS** — supported historical replay requires the Phase 4F research-ready boundary and an explicit temporary/custom ledger; the legacy direct-CSV replay route is blocked. This status confirms the safety boundary and regression checks, not the availability of production intraday data. No production intraday dataset is present, so actual-data replay remains unavailable.

```text
PHASE 4F FINAL AUDIT: PASS
Files reviewed: 41-file baseline audit plus Phase 4F.1 replay boundary, tests, and docs
Tests: 539 passed / 0 failed
Critical issues: 0 unresolved (5 Phase 4F gate fixes and 1 Phase 4D replay-boundary fix)
High issues: 2
Medium issues: 2
Low issues: 1
Protected artifacts: PASS (363 Phase 4F.1 hashes and 368 earlier protected hashes unchanged)
Live execution: DISABLED
Broker calls: 0
Production intraday dataset: NONE
Actual-data replay available: NO (no production dataset is registered)
```
