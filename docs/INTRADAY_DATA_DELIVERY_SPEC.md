# NSE Intraday Data Delivery Specification

This specification is for delivery of authorized NSE cash-equity intraday bars to this project. A schema match alone does not establish data authenticity, coverage, or rights. Submit actual licensing/provenance documents and a small representative sample alongside a full delivery.

## Required observations

- Exchange: explicit NSE.
- Series: explicit cash-equity EQ.
- Bar interval: explicit 1m or 5m; no daily bars and no conversion from daily data.
- Symbol: canonical NSE trading symbol, or a source symbol with a documented source-to-canonical mapping.
- Timestamp: ISO-8601 with an explicit offset or Z; document the source timezone and whether the timestamp labels bar start or bar end.
- OHLC: finite positive values in the source's documented adjustment basis.
- Volume: nonnegative numeric value with explicit semantics: bar/incremental share volume, cumulative shares, trade count, or unknown. Unknown volume blocks volume-dependent research.
- Status: bars must be final/closed. If omitted, provide documentation that the delivered bars are finalized.

Preferred row fields:

    timestamp,symbol,exchange,series,open,high,low,close,volume
    2026-01-02T09:15:00+05:30,RELIANCE,NSE,EQ,....,....,....,....,....

**Schema example only. The ellipses are placeholders, this row is not a market observation, and it must not be used as test or production data.** The normalized project file additionally contains timeframe, volume_kind, status, source.

## Allowed files and layout

Supported input: .csv, .csv.gz, .csv.zip containing CSV members, or .parquet. Multiple files can be delivered in one directory if they share a schema and one metadata sidecar. Do not include unrelated daily files or index data in the same dataset directory.

Example delivery:

    vendor_delivery_2026-09-27/
    ├── bars_2026_q1.csv.gz
    ├── bars_2026_q2.csv.gz
    └── dataset_metadata.json

Pass the directory to registration. The pipeline copies raw bytes into data/intraday/raw/<dataset_id>/; it does not edit the delivered files.

## Required metadata sidecar

Use facts from the provider contract or delivery documentation. If a fact is unknown, set it to "UNKNOWN"; do not guess. A minimal structural example follows; all values are illustrative placeholders, not a license or source claim:

    {
      "provider": "<provider name>",
      "publisher": "<legal data publisher>",
      "dataset_name": "<dataset name>",
      "revision": "<immutable delivery revision>",
      "source_url": "<official delivery/source URL, or the actual supplied source path if no URL is available>",
      "acquisition_timestamp": "<ISO-8601 timestamp or UNKNOWN>",
      "data_type": "bars",
      "timeframe": "1m",
      "timestamp_convention": "BAR_START",
      "timestamp_origin": "<exchange/vendor timestamp semantics or UNKNOWN>",
      "original_timezone": "Asia/Kolkata",
      "timestamp_timezone_documentation": "<document reference or UNKNOWN>",
      "exchange": "NSE",
      "series": "EQ",
      "volume_semantics": "INCREMENTAL_SHARES",
      "adjustment_status": "UNKNOWN",
      "bars_finalized": true,
      "bars_finalized_documentation": "<provider documentation reference>",
      "identity_documentation": "<instrument identity source/reference>",
      "expected_symbols": ["<symbol>", "<symbol>"],
      "expected_sessions": ["YYYY-MM-DD"],
      "license_status": "<actual authorization status>",
      "license": "<license/agreement identifier or UNKNOWN>",
      "data_rights": "<actual granted rights or UNKNOWN>",
      "research_allowed": false,
      "commercial_allowed": false,
      "redistribution_terms": "<actual terms or UNKNOWN>",
      "universe_bias": "<membership history facts or UNKNOWN>",
      "column_map": {},
      "symbol_mapping": {},
      "symbol_mapping_documentation": "UNKNOWN"
    }

research_allowed must be explicitly true for Phase 4E to classify research readiness. A filename, exchange, series, timezone, price adjustment, license, or symbol map is never inferred. If input rows lack exchange/series identity, supply a documented source-to-instrument mapping in symbol_mapping, with symbol_mapping_documentation; mapped rows must resolve to canonical symbol, exchange, and series. A source identity that conflicts with its mapping must not be presented as equivalent. Provide an actual immutable provider revision when one exists; otherwise report UNKNOWN. The importer can still assign a deterministic content-hash dataset ID without calling that hash a provider revision. The license identifier and authorization status are separate facts; authorization status alone is not a license or rights reference.

For cumulative volume, use volume_semantics=CUMULATIVE_SHARES. Conversion to bar increments is permitted only if the provider documents reset and increment behavior, cumulative_to_incremental is exactly true, and cumulative_increment_rule_documentation cites that rule. Otherwise, values remain cumulative and Phase 4E research readiness is blocked. Do not mark a share volume field as trade count or vice versa.

## Coverage and instrument identity

Include the complete requested symbol set and source-verified session dates as expected_symbols and expected_sessions. `expected_sessions` is required for Phase 4F research readiness, and rows outside that list are rejected. Identify delisted securities and point-in-time universe membership if the delivery is intended for historical universe research. Include source-documented special-session windows in special_session_windows. If a symbol legitimately had no trades for a full session, identify it explicitly in no_trade_symbol_sessions; do not label unexplained missing records as no-trade.

Confirm that all files use the same timeframe, timezone, timestamp convention, volume meaning, adjustment basis, schema, and license. Set `adjustment_status` to `RAW` or `ADJUSTED` and cite the provider documentation in `adjustment_status_documentation`; Phase 4F research readiness is blocked while the basis is `UNKNOWN` or undocumented. The importer stores one basis per delivery and cannot detect a provider's undocumented per-file adjustment change from OHLC values alone. Keep original source files and provider documentation available. The importer records SHA-256 for each file and a deterministic dataset fingerprint.

## Intake commands

    python scripts/register_intraday_dataset.py --path <DELIVERY_DIRECTORY>
    python scripts/validate_intraday_dataset.py --dataset <DATASET_ID>
    python scripts/normalize_intraday_dataset.py --dataset <DATASET_ID>
    python scripts/check_intraday_readiness.py --dataset <DATASET_ID>

Registration and normalization support --dry-run. Do not run replay until readiness prints PHASE4F_READY. Replay is paper-only and requires a ledger path under the system temporary directory. Current status is no production dataset; test fixtures are not an alternative data source.
