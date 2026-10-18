# Phase 4A — Realtime market data and intraday candles

## Scope and safety

Phase 4A adds a data-only foundation: provider-neutral normalized quotes, validation, feed health, optional local recording, and intraday candles. This module does not call strategies, risk sizing, paper execution, broker code, or the Phase 3D daily runner. The separate Phase 4G Jugaad paper session can pass its accepted closed sampled bars to the existing analytical scanner, but has no order or ledger path. `LIVE_TRADING_ENABLED` remains false. Shared realtime defaults remain disabled.

The dashboard is read-only. It can read an optional feed-health JSON snapshot written by an explicitly run feed check, but does not start or poll a provider. `scripts/start_local.sh` starts only the dashboard. No source data or 2026-09-28 records are created by startup.

## Existing architecture and provider suitability

The project already had `RealtimeDataProvider`, `NormalizedQuote`, freshness checks, serial batching, bounded HTTP retry/backoff, latency counters, and a circuit breaker in `src/nse_research/realtime/providers.py`. The current `IndianStockMarketAPIProvider` is a read-only adapter for the Yahoo-derived API investigated in [REALTIME_PROVIDER_EVALUATION.md](REALTIME_PROVIDER_EVALUATION.md). Its response-generation timestamp is not a quote timestamp, and its response does not establish `Series=EQ`. It is therefore rejected by the Phase 4A provider factory as `DATA_SOURCE_NOT_SUITABLE`; it is not silently promoted or contacted by the safe command.

`RealtimeMarketDataProvider` is an alias for the existing provider abstraction. A future audited live or replay adapter can implement that contract and register a provider factory without changing candle aggregation or any later strategy/execution modules.

The user-authorized `jugaad_data` adapter is now registered in `realtime/jugaad.py`. It calls the package's serial NSE quote method with `series="EQ"`, uses the NSE `lastUpdateTime` field as source time, and treats reported day volume as cumulative. It can request at most five symbols per poll. This API returns snapshots rather than a tick stream, so candles built from it are sampled and can miss trades and intrabar extrema. See [PHASE4A8B_FREE_REALTIME_PROVIDER_EVALUATION.md](PHASE4A8B_FREE_REALTIME_PROVIDER_EVALUATION.md#follow-up-jugaad-data-2026-10-03) for the authorization note and source evaluation.

## Quote contract

`NormalizedQuote` carries:

- `symbol`, `exchange`, and explicit `series`;
- `timestamp` / `source_timestamp`: the timestamp supplied for the market observation;
- `received_at`: local adapter receipt time;
- `last_price`, optional `volume`, `volume_kind`, and optional source `tick_id`;
- `source`, original adapter validity, and age diagnostics.

The generic gate in `realtime/engine.py` requires a valid symbol matching the requested NSE symbol, explicit `exchange=NSE`, exact `series=EQ`, finite positive price, finite non-negative optional volume, non-empty source, and timezone-aware source and receipt timestamps. It rejects a missing series; it never fills one in. A supplied non-EQ series is rejected. It rejects timestamps beyond the configured future-skew tolerance and quotes older than the configured maximum age. Failure codes include `DATA_SOURCE_NOT_CONFIGURED`, `DATA_SOURCE_NOT_SUITABLE`, `QUOTE_STALE`, `QUOTE_TIMESTAMP_INVALID`, `QUOTE_PRICE_INVALID`, `QUOTE_SERIES_INVALID`, `QUOTE_SYMBOL_INVALID`, and `QUOTE_DUPLICATE`.

`quote_age_seconds` is measured from source timestamp to the current timezone-aware clock. The YAML setting `realtime.max_quote_age_seconds` defaults to 5 seconds as a strict starting limit; it must be set to a value supported by the chosen provider's documented timestamp and delivery behavior before enabling that provider. It is not evidence that any feed meets the limit.

## Universe and session handling

If `realtime.symbols` is empty, the safe test reads the configured `universe.symbols_file`. That frozen roster must include `Symbol` and `Series` columns; each requested row must explicitly be `EQ`, and duplicates or malformed symbols fail. An explicit configured list does not prove EQ identity on its own; the provider quote must still return `Series=EQ`. Missing and unsupported provider symbols are reported, not replaced.

Intraday session state extends the existing `NseTradingCalendar`; no second holiday calendar is introduced. Defaults are pre-open 09:00, regular session 09:15–15:30, and Asia/Kolkata. Holidays and weekends are closed. On an otherwise unknown weekday, the clock alone does not establish that the exchange is trading: the status remains `DATA_UNAVAILABLE` until a validated current-session quote supplies evidence. The calendar lists special-session dates but has no intraday schedule field, so those dates report `SPECIAL_SESSION` and candle creation fails closed until hours are explicitly represented. Pre-open and closed periods do not create candles.

## Candle aggregation

`IntradayCandleBuilder` is provider-independent and supports configured minute intervals, including 1m and 5m. Buckets are anchored at the configured NSE open (09:15 by default), so the 5-minute bars are 09:15–09:20, 09:20–09:25, and so on. Candle start/end are timezone-aware Asia/Kolkata timestamps. No empty bars are synthesized.

Candles remain `FORMING` until the local clock reaches their end; `advance()` then emits `CLOSED`. Closed bars are immutable. Open/high/low/close are deterministic for out-of-order updates inside an active bucket: open and close follow source timestamp, receipt timestamp, and tick ID ordering; highs and lows use all accepted prices. A late tick for a closed interval is rejected and logged. Repeated source tick IDs are rejected. If an earlier quote belongs to a still-forming bucket, the engine records `OUT_OF_ORDER_TICK_ACCEPTED` while recomputing that bucket's OHLC. No source tick ID means identity-level duplicate detection is unavailable.

The candle `volume` field is aggregated only when the provider explicitly marks each quote's volume as `INCREMENTAL`. A cumulative session-volume field is never summed as if it were per-tick volume; in that case candle volume is `null` until a source supplies a valid incremental observation contract. Missing ticks and unavailable volume are not filled.

Recent closed candles are kept in a bounded in-memory history for deterministic consumers. There are no threads per symbol; a single bounded batch/poll feeds dictionary-backed per-symbol aggregators.

## Lifecycle, health, and reconnects

The monitor reports `CONNECTING`, `CONNECTED`, `DEGRADED`, `DISCONNECTED`, or `RECONNECTING`. A poll retries provider exceptions at most `realtime.max_retries` times with capped exponential delay. Provider adapters may also apply their own bounded retry/circuit-breaker rules. The monitor tracks provider, generated/update/source/receipt times, observed latency, received/accepted/rejected/stale counts, missing/invalid/stale symbols, retries, reconnects, and errors. If disconnected, the last-known quote ages to `STALE`; it is not passed to candle processing as a new observation.

Feed events use the module logger with event names such as `QUOTE_REJECTED`, `CANDLE_STARTED`, `CANDLE_CLOSED`, `REALTIME_FEED_DEGRADED`, and `REALTIME_PROVIDER_DISCONNECTED`. Provider credentials and raw auth headers are not part of the normalized quote or health schema.

## Optional recording and replay

Recording is disabled by default. When `realtime.recording: true`, the monitor appends normalized quote observations and validation outcomes to date-partitioned JSONL files under `realtime.recording_dir`. Each record has source/receipt timestamps and a SHA-256 over its normalized content. Credentials and tokens are not stored. Recording does not write the paper ledger.

The normalized quote interface can be implemented by a future `HistoricalReplayProvider`. A complete replay engine is not included in Phase 4A.

## Configuration and safe connectivity check

Realtime values live under `realtime` in `config/phase3d_personal.yaml`. Shared settings default to disabled. The personal profile selects the user-authorized `jugaad_data` provider, five NSE EQ symbols, and `enabled: true`. This lets the explicitly started feed run and lets the dashboard match its health snapshot. The dashboard remains read-only and starts no network connection. Quotes still pass the shared validation and freshness gates.

Run the one-shot data-only check only after an audited adapter is configured:

```bash
source .venv/bin/activate
export PYTHONPATH=src
python scripts/test_realtime_feed.py
```

This one-shot command requests configured/frozen symbols, validates the returned quotes, prints feed/candle status, and exits. It uses no strategy, order, broker, or Phase 3D runner.

For the Jugaad paper-signal session, first run its local-only preflight, then start the runner during a regular NSE session:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --preflight
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --max-polls 1
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --interval-seconds 15
```

The optional dependency is declared in `requirements-realtime.txt` and is installed in the project environment. The runner requires the personal profile's `jugaad_data` provider to be enabled, validates symbols against the frozen EQ universe, serializes NSE calls, applies a five-second HTTP timeout, writes feed health and a separate paper recommendation snapshot, and stops at session close or with Ctrl+C. The existing scanner may emit deterministic paper candidates with reference entry, stop, target, reasons, and expiry. It does not create orders, fills, quantities, or paper-ledger records. Add `--max-polls 1` for one batch or `--symbols RELIANCE,TCS` for a smaller subset. See [PHASE4G_JUGAAD_PAPER_QUICKSTART.md](PHASE4G_JUGAAD_PAPER_QUICKSTART.md) for the session steps and sampled-OHLC caveat.

## Dashboard

`GET /api/dashboard/realtime` powers the existing dashboard's **Realtime Data** section. It displays configured feed state, market session, source/receipt freshness, provider statistics, universe coverage, validated quotes, and actual forming/closed candles. Without a suitable published health snapshot it displays `REALTIME FEED: NOT CONFIGURED` (or `REALTIME DATA SOURCE NOT SUITABLE`) and empty quote/candle tables. The route only reads config and the optional snapshot.

## Current limitations

- Jugaad Data is available as an opt-in polling snapshot source under the user's stated authorization; no live response has yet passed empirical validation.
- The previously evaluated public API is Yahoo-derived and cannot prove source-time freshness or exact EQ identity.
- A local special-session date has no associated intraday hours; the candle engine blocks that date.
- The dashboard starts no background feed; the explicitly started Phase 4G runner is the only Jugaad polling loop.
- Incremental trade-volume observations are required for non-null candle volume.
- The current NSE frozen universe and calendar are local files and can be updated only through their existing controlled workflows.
