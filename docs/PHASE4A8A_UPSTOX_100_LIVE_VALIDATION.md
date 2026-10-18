# Phase 4A.8A — Upstox V3 100-Stock Read-Only Validation

Review date: 2026-09-27  
Runtime status: **BLOCKED — no Upstox access token; mappings remain unverified**

## Environment and scope

This work prepares the existing Upstox V3 adapter for a 100-symbol, read-only validation. The selected feed mode is LTPC. The frozen 486-symbol Phase 3D roster remains the master; the realtime selection is isolated under `results/phase4a8a_*`. No strategy, scanner, signal, position, risk-sizing, order, broker, or ledger path was invoked. No live feed was started.

`REALTIME_UNIVERSE_SIZE` accepts `100`, `200`, or `486` and defaults to `100`. The local project roster contains no NIFTY 100 membership field or separate membership file. The deterministic selection ranks the brief's five required smoke symbols first, in their prescribed order, and fills the remaining ranks from the frozen roster file order. The selection uses no current prices and does not change during a run.

## Universe and mapping

| Measure | Result |
|---|---:|
| Selected | 100 / 100 |
| Verified | 0 |
| Missing | 0 |
| Duplicate | 0 |
| Invalid | 0 |
| Unverified | 100 |
| Required five included in selection | 5 / 5 |
| Required five mapped and live-validated | 0 / 5 |

The selection artifact is `results/phase4a8a_upstox_live_universe_100.csv` with the requested six-column schema. Its first five rows are `RELIANCE`, `TCS`, `INFY`, `HDFCBANK`, and `SBIN`. The separate `results/phase4a8a_upstox_instrument_mapping_100.csv` has the existing 14-column mapping schema and exactly 100 rows.

The ISIN Search implementation is serial, defaults to a 0.25 second inter-symbol delay and three bounded retries, and exponentially backs off only for timeouts, connection failures, HTTP 408/425/429, and HTTP 5xx. Permanent HTTP errors stop the pass. On a stop, completed rows are retained and the failed/unqueried rows stay `UNVERIFIED`; they are not mislabeled `MISSING`. Mapping requires exact ISIN, NSE, NSE_EQ, EQ, and an NSE_EQ instrument key. If Search supplies a trading symbol, it must match; if omitted, the exact ISIN and other explicit identity fields still must pass. Duplicate strict candidates are rejected.

The Search command was run with `UPSTOX_ACCESS_TOKEN` unset. It wrote the deterministic selection and 100 `UNVERIFIED` mapping rows, returned `UPSTOX_PROVIDER_NOT_CONFIGURED`, and made no request. BOD was not retried. The access token was neither read from a file nor displayed or persisted.

## Five-symbol gate

| Symbol | In universe | Mapping | Production gate |
|---|---|---|---|
| RELIANCE | Yes | UNVERIFIED | Not run |
| TCS | Yes | UNVERIFIED | Not run |
| INFY | Yes | UNVERIFIED | Not run |
| HDFCBANK | Yes | UNVERIFIED | Not run |
| SBIN | Yes | UNVERIFIED | Not run |

No five-symbol validation CSV was created because no production connection or observations occurred. During a future explicitly gated session, the launcher must receive snapshot and live LTPC updates for all five, normalize their quotes, and produce both 1-minute and 5-minute candles before it subscribes to the rest of the selected universe.

## Live feed, timestamps, volume, candles, coverage, and reconnect

| Item | Result / evidence class |
|---|---|
| Upstox connection and subscription | **Not attempted**; token absent and mapping is not verified. |
| Live ticks / coverage | None observed; `phase4a8a_live_coverage_100.csv` was not generated. |
| LTT unit | Configured as strict 13-digit epoch milliseconds; this interpretation is **not empirically validated**. No unit guessing or fallback is allowed. |
| Source and receipt times | The adapter keeps raw LTT, UTC source timestamp, UTC local receipt timestamp, and calculated latency separate. |
| Timestamp semantics | `CONFIGURED_EPOCH_MS_NOT_EMPIRICALLY_VALIDATED`; it may be changed to `EMPIRICALLY_VALIDATED` only after plausible production samples pass date, timezone, future-skew, and market-activity checks. |
| LTQ / aggregate volume | LTQ remains last-trade quantity. LTPC has no cumulative traded-volume field, so normalized quote and candle volume remain unknown. No LTQ-to-volume conversion is made. |
| 1-minute and 5-minute candles | No live candles generated. The existing session-anchored candle builder remains the only implementation. |
| Reconnect | The live launcher will request one controlled socket reconnect only after the five-symbol gate, then require a bounded reconnect, resubscription, and fresh five-symbol ticks before it adds the other 95. Existing retry/resubscription behavior is covered by mocked offline tests; no production reconnect test was run. |
| Dashboard | Shows configured universe size and LTPC mode dynamically, including mapped/subscribed/receiving denominators and provider health. Preflight checks only `http://127.0.0.1:8765/health`; it does not start a worker. The dashboard never serializes the token. |
| Coverage window | Default observation is 15 minutes after the five-symbol and reconnect gates. A symbol with no first tick remains `SUBSCRIBED_NO_TICK_YET`; it is not called missing. A previously receiving symbol becomes `STALE` only after 60 seconds without a locally received update. |

## Preflight

The offline command `python scripts/test_upstox_realtime.py --mode PREFLIGHT` returned `PHASE4A8A_PREFLIGHT_BLOCKED`. It made no market-data request, created no order or signal, and did not mutate the ledger. The blocking checks in the current shell were:

- `token_present`: false
- `realtime_enabled_for_live`: false
- `provider_is_upstox`: false (the process environment has no provider selection)
- `live_trading_disabled`: false because the process environment does not explicitly contain `LIVE_TRADING=false` (the project configuration itself remains disabled)
- `mapping_100_verified`: false (0 / 100)
- `dashboard_http_ready`: false (`127.0.0.1:8765/health` is not currently serving)

Universe selection, smoke-symbol presence, LTPC mode, dependencies, calendar files, baseline manifest, broker-provider absence, feed-idle state, and duplicate-run checks passed. `DASHBOARD_READY` requires the existing local read-only `/health` endpoint to respond; the endpoint is currently unavailable. Preflight never starts the dashboard or feed and is read-only.

The live launcher also refuses to connect outside the regular NSE weekday session. `LIVE_TRADING` must be explicitly set to `false`, `REALTIME_PROVIDER=upstox`, and `REALTIME_ENABLED=true` in the launcher process. The example environment keeps realtime disabled and live trading disabled by default.

## Safety and research preservation

- Signals: 0
- Orders: 0
- Broker API calls: 0
- Ledger mutations: 0
- Live trading: disabled in project configuration; explicit process value is still required by the live gate
- Phase 3C/3D result files: checksummed before implementation and verified unchanged afterward
- Phase 3C/3D strategy, experiment count, parameters, and backtester: unchanged

The before/after result checksum manifests cover all 342 result files that existed before this work. The new Phase 4A.8A roster and mapping artifacts are additive.

## Evidence labels

- **DOCUMENTED:** Upstox V3 supports protobuf LTPC updates; the existing adapter uses the documented authorization and websocket flow. The API schema identifies LTP, LTT, LTQ, and CP. LTQ is not cumulative volume.
- **EMPIRICALLY VERIFIED:** No production behavior was empirically verified in this phase.
- **INFERRED / CONFIGURED:** LTT is parsed as epoch milliseconds because the official example shows a 13-digit value. The adapter rejects other widths and does not reinterpret them.
- **UNKNOWN:** Production timestamp unit/semantics, latency distribution, actual symbol mappings, subscriptions, coverage, candles, and reconnect behavior.

Official Upstox references: [Instrument Search](https://upstox.com/developer/api-documentation/instrument-search/), [Market Data Feed V3](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/), and [Instruments](https://upstox.com/developer/api-documentation/instruments/).

## Tests and next action

The offline suite exercises deterministic selection, all supported universe sizes, required-symbol inclusion, strict ISIN mappings, invalid/duplicate candidates, bounded retries, permanent-error handling, partial-result safety, exact launcher statuses, token redaction, timestamp/session gates, coverage classification, controlled reconnect requests, and read-only dashboard/preflight behavior. No test contacts Upstox. Final checks: **372 passed**; `compileall` and `node --check forward/dashboard/app.js` passed.

The smallest next action is to configure a valid `UPSTOX_ACCESS_TOKEN` locally through the approved secret mechanism (do not paste it into source or chat), then run:

```bash
REALTIME_UNIVERSE_SIZE=100 \
UPSTOX_MAPPING_REQUEST_DELAY_SECONDS=0.25 \
UPSTOX_MAPPING_MAX_RETRIES=3 \
python scripts/build_upstox_instrument_mapping.py --universe-size 100 --search-api
```

Proceed only if that produces **100 / 100 VERIFIED**. Then set `REALTIME_PROVIDER=upstox`, `REALTIME_ENABLED=true`, and `LIVE_TRADING=false`, run `--mode PREFLIGHT`, and start `LIVE_READ_ONLY` only after preflight passes during the regular NSE session. Do not enable strategies after this phase without a separate authorization and review.

**PHASE 4A.8A STATUS: BLOCKED** — the mapping API was not called because no token was configured, and no production observations exist.
