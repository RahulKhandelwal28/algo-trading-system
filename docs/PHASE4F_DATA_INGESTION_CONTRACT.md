# Phase 4F — Authorized Intraday Data Ingestion Contract

Review date: 2026-09-27  
Status: **PIPELINE IMPLEMENTED; research use blocked because no authorized intraday dataset is registered**

## Scope and current data state

Phase 4F provides an intake path for authorized NSE cash-equity 1-minute or 5-minute bars. The existing IntradayBar, IntradayDataProvider, CsvIntradayDataProvider, Phase 4E validator, NSE calendar, Phase 4B scanner, and Phase 4D paper orchestrator remain authoritative. Phase 4F adds registration, validation/reporting, deterministic normalization, readiness checks, and thin boundary adapters.

The checked-in data/intraday/datasets.json is empty. There are no licensed raw or normalized observations. The synthetic fixtures under tests/fixtures/intraday/ are test-only and are never copied or used as a fallback. Phase 4F does not acquire data or convert daily bars or ticks into intraday bars.

## Data flow and gates

    authorized delivery → immutable raw copy + SHA-256 → Phase 4E validation
        ├─ FAIL → rejection certificate + quarantine metadata; no canonical output
        └─ PASS → deterministic canonical CSV + completion manifest
                      ├─ research-ready → Phase 4B scanner adapter
                      └─ research-ready → Phase 4D paper replay adapter → Phase 4C

PASS from the Phase 4E row-quality validator permits normalization. Research replay additionally requires Phase 4E's research_ready result, the accepted certificate, a complete normalized manifest, matching source/output hashes, and a temporary ledger. A dataset can be structurally valid and normalized while still being blocked from research because its provenance, data rights, volume meaning, or coverage is incomplete.

## Intake files and metadata

Accepted source formats are CSV, CSV.GZ, CSV.ZIP containing CSV members, and Parquet (using the repository's existing PyArrow dependency). A directory can hold multiple files with one common schema. Ticks are registered and preserved raw-only; tick aggregation is not implemented. Daily data is not an accepted bar timeframe.

Provide a dataset_metadata.json inside an input directory or an adjacent <file>.metadata.json. Registration preserves the complete metadata object and does not fill unknown values. The delivery specification and a JSON example are in [INTRADAY_DATA_DELIVERY_SPEC.md](INTRADAY_DATA_DELIVERY_SPEC.md).

Important metadata keys include provider, dataset_name, revision, source_url (or the actual supplied source path), acquisition_timestamp, publisher, data_type, timeframe, timestamp_convention, original_timezone, volume_semantics, adjustment_status and adjustment_status_documentation, license_status/license, data_rights, research_allowed, commercial_allowed, redistribution_terms, universe_bias, expected_symbols, expected_sessions, and source documentation. Supply symbol_mapping plus symbol_mapping_documentation for source aliases or missing row identity. No mapping is inferred from a file name. Unknown source revision remains UNKNOWN in provenance; the deterministic dataset ID uses a content-hash suffix when no source revision is supplied. Unknown facts must be "UNKNOWN" and will block any Phase 4E gate that needs the fact.

Each raw file is copied to data/intraday/raw/<dataset_id>/ without changing its bytes. The source manifest records each relative path, size, SHA-256, deterministic dataset fingerprint, source schema, format, metadata checksum, and provenance. A repeated registration of the same ID and bytes is idempotent; different bytes or metadata under the same ID fail with DATASET_ID_CONFLICT. Raw files are never normalized in place.

## Validation and coverage

validate_dataset() stages the input in a temporary SQLite database and sends bounded row batches through the existing Phase 4E validate_intraday_dataset() implementation. It also reconciles duplicates across batch boundaries and uses the existing NseTradingCalendar and validate_session_timestamp() for session decisions. validate_session_coverage() is re-exported from the Phase 4F data module for callers that need the existing Phase 4E gap calculator.

Validation rejects malformed/naive/ambiguous timestamps, unverified timezone assumptions, unknown bar-start/bar-end convention, invalid OHLC, nonpositive prices, negative or malformed volume, duplicate (symbol, timeframe, timestamp) records, non-NSE identity, non-EQ series, invalid session bars, unclosed bars, and source-file/metadata hash changes. UTC timestamps are converted as instants to Asia/Kolkata; BAR_END timestamps are shifted backward only when the delivery metadata explicitly declares BAR_END. Raw timestamps remain untouched.

Coverage outputs are written under results/phase4f/<dataset_id>:

- validation_report.json — Phase 4E status, research-readiness classification, errors, warnings and counts.
- coverage_report.json — first/last date, observed sessions, symbols and rows, complete/partial/missing sessions, per-symbol/session counts, expected coverage when supplied, zero/negative volume counts, missing intervals, and the maximum multiple between consecutive observed positive volume values within one symbol/session. The volume-change metric is diagnostic only; it has no implicit threshold and never filters or changes volume.
- coverage_by_symbol.csv and coverage_by_session.csv — tabular coverage views.
- ACCEPTED_DATASET.json when Phase 4E validation passes, or REJECTED_DATASET.json when it fails.

For Phase 4F research readiness, a source-verified `expected_sessions` list is required. Without it, Phase 4E row validation and canonical normalization may complete, but research readiness remains blocked. Every source bar's normalized session date must belong to the declared list. `expected_symbols` enables a complete symbol comparison, and `no_trade_symbol_sessions` can classify documented symbol/session no-trade cases separately. Without source evidence, missing intervals remain gaps or unknown no-trade periods; the pipeline never forward-fills, interpolates, creates bars, removes zero-volume bars, or claims a holiday from absent data. Coverage calendars and vendor session rules are not invented.

## Canonical output and manifest

The adapter writes:

    data/intraday/normalized/<dataset_id>/canonical_intraday_bars.csv
    data/intraday/normalized/<dataset_id>/manifest.json

The normalized CSV matches the existing provider contract:

    timestamp,symbol,exchange,series,timeframe,open,high,low,close,volume,volume_kind,status,source

Timestamps are timezone-aware ISO-8601 values in Asia/Kolkata, and represent interval start in the existing IntradayBar model. Canonical bars are ordered by event close, symbol, timeframe, then interval start. OHLC and volume retain their source decimal values in canonical CSV; the existing float-based project model is used to validate bars at its boundary, and no price adjustment is applied. Research readiness requires one documented `RAW` or `ADJUSTED` basis for the whole delivery; `UNKNOWN` remains a blocker. The provider documentation must confirm that all files in a multi-file delivery share this basis because the importer cannot infer per-file adjustment history from OHLC values. `volume_kind` and `status` are set only from explicit row values or documented source metadata. Cumulative-to-incremental conversion is allowed only when volume_semantics=CUMULATIVE_SHARES, cumulative_to_incremental=true, and cumulative_increment_rule_documentation is supplied. Resets/decreases fail validation.

The manifest records dataset/source and normalized hashes, row/symbol counts, symbol list, coverage, timeframe, source and canonical timezones, exchange/series values, volume and adjustment status, source timestamp convention, PASS status, ordering, and completion time. The registry pins the manifest SHA-256. Re-normalization is idempotent; a concurrent normalization returns DATASET_LOCKED. Output is written to a temporary file and atomically renamed. IN_PROGRESS/INCOMPLETE manifests do not authorize use. Missing/corrupted output or changed raw input fails closed with NORMALIZED_DATASET_INCOMPLETE or SOURCE_HASH_MISMATCH.

## Provider and integrations

CanonicalIntradayDatasetProvider implements the existing read-only provider shape (available_symbols, has_symbol, available_dates, get_bars, iter_bars, coverage, metadata) and verifies readiness and hashes before iteration. It delegates parsing and bar validation to CsvIntradayDataProvider; it does not implement indicators or strategies.

Phase4BScannerAdapter streams chronologically ordered bars into the existing IntradayScanner for a caller-supplied frozen 100/200/486-symbol universe. It does not select or duplicate a universe, alter scanner rules, or create orders.

Phase4DReplayAdapter delegates to the existing Phase4DOrchestrator, which continues to own Phase 4C paper risk, fills, accounting, and lifecycle. Historical replay requires a registered dataset, Phase 4E research readiness, the canonical Phase 4F output, a date present in observed session coverage, and an explicit ledger path under the operating system temporary directory. The configured persistent Phase 4C ledger is rejected. This intake change does not configure a broker, realtime provider, live mode, or persistent paper ledger. Live execution remains unavailable.

## CLI workflow

    python scripts/register_intraday_dataset.py --path <PATH>
    python scripts/validate_intraday_dataset.py --dataset <ID>
    python scripts/normalize_intraday_dataset.py --dataset <ID>
    python scripts/check_intraday_readiness.py --dataset <ID>
    python scripts/replay_intraday_session.py \
      --dataset <ID> --date YYYY-MM-DD --ledger /tmp/phase4f_test_ledger.json

The legacy `scripts/run_phase4d_intraday_paper.py --mode REPLAY` entry point is blocked with `REPLAY_DATA_MUST_BE_PHASE4F_REGISTERED`; its `--data` argument cannot reach execution. `--data` remains available only for read-only PRECHECK. The Phase 4F replay command reports `PHASE4D_TEMPORARY_LEDGER_REQUIRED` when `--ledger` is omitted, rejects the configured persistent ledger, and reports `PHASE4F_SESSION_NOT_AVAILABLE` when the requested date has no observed bars in the registered dataset.

Registration and normalization accept --dry-run. Every script has --help. Validation and normalization do not run a scanner or paper execution. Readiness is read-only. Replay is paper-only, one session at a time, and is unavailable until all data gates pass. Current production behavior is PHASE4F_NO_RESEARCH_READY_DATA; no production replay was run because the registry is empty. The Phase 4F.1 synthetic end-to-end replay exists only as an isolated pytest fixture.

## Error/status codes

| Code | Meaning |
|---|---|
| DATASET_FILE_MISSING | Input path does not exist. |
| DATASET_SCHEMA_UNIDENTIFIABLE / DATASET_UNREADABLE | Required schema cannot be read or identified. |
| DATASET_ID_CONFLICT | A deterministic ID already refers to different bytes or metadata. |
| PHASE4F_DATA_REJECTED | Phase 4E validation did not pass; normalization is blocked. |
| TICK_AGGREGATION_NOT_IMPLEMENTED | Tick data was retained raw-only. |
| PHASE4F_NO_RESEARCH_READY_DATA | No registered dataset passed Phase 4E research readiness. |
| PHASE4F_DATASET_NOT_REGISTERED | Requested dataset ID is absent from the Phase 4F registry. |
| PHASE4F_DATA_NOT_RESEARCH_READY | Registered dataset is not cleared by Phase 4E. |
| PHASE4F_SESSION_NOT_AVAILABLE | Requested session has no observed bars in the ready canonical dataset. |
| SOURCE_HASH_MISMATCH | Immutable raw bytes differ from the registered fingerprint. |
| NORMALIZED_DATASET_INCOMPLETE | Completion marker, manifest, normalized hash, or output is inconsistent. |
| DATASET_LOCKED | Another normalization process owns the dataset lock. |
| REPLAY_LEDGER_MUST_BE_TEMPORARY | Replay ledger is outside the system temporary directory. |
| PHASE4D_TEMPORARY_LEDGER_REQUIRED | Replay requires an explicit ledger path. |
| PHASE4D_PERSISTENT_LEDGER_REJECTED_FOR_REPLAY | Historical replay cannot use the persistent Phase 4C ledger. |
| REPLAY_DATA_MUST_BE_PHASE4F_REGISTERED | Legacy direct-CSV replay is disabled; use the Phase 4F dataset-ID command. |

## Integrity and verification limits

Checksums detect accidental or uncoordinated file changes relative to the checked-in registry; they do not cryptographically authenticate provider identity or replace a vendor license. An accepted certificate says what local Phase 4E validation found; it does not prove the source publisher's claims. Rights and provenance must be supplied and supported by the dataset owner. Phase 4F reports data quality and integration status only; it makes no profitability or strategy-performance claim.

The Phase 3C/3D results, Phase 4B scanner/strategies, Phase 4C risk/fill rules, Phase 4D execution lifecycle and existing paper ledger, realtime providers, and broker integrations were not modified. No authorized source file was available during this implementation.

    REAL INTRADAY DATA = NONE
    PHASE4F = PHASE4F_NO_RESEARCH_READY_DATA
    REALTIME = REALTIME_PROVIDER_NOT_VALIDATED
    BROKER CALLS = 0
    LIVE TRADING = DISABLED
