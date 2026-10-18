# Phase 4A.8 — Upstox V3 Read-Only Market Data Validation

Review date: 2026-09-27 (Asia/Kolkata)  
Status: **PARTIAL — offline implementation and fail-closed preflight complete; production validation blocked**

## Safety and scope

This phase adds a read-only Upstox V3 market-data adapter, an explicit NSE/EQ mapping artifact, a safe launcher with `OFFLINE`, `PREFLIGHT`, and `LIVE_READ_ONLY` modes, and dashboard health fields. It does not import or invoke strategy, signal, risk-sizing, paper-order, broker-order, or ledger-writing code. The only API requests the live mode can make are the documented Upstox market-data authorization request and WebSocket subscription. Instrument search is a separately explicit, read-only mapping command.

`REALTIME_ENABLED` and `LIVE_TRADING` remain `false` in the example configuration. The Phase 3C/3D result files and paper ledger were not changed. No production feed was opened, no live tick was observed, no order or signal was created, and the ledger was not touched.

Integrity verification compared 89 pre-work Phase 3C/3D result-file checksums and sizes: **89 unchanged, 0 changed**.

## Source contract and evidence

| Area | Finding | Evidence status |
|---|---|---|
| V3 stream | The feed is a WebSocket using Protobuf response frames. LTPC mode is the initial subscription. The request is sent as UTF-8 JSON bytes, following Upstox's official example. | DOCUMENTED; request encoder also covered by a mocked test |
| Authorization | The official V3 authorize endpoint returns a one-use `wss://` redirect URI. The implementation keeps that URI and bearer token out of logs, CSV, health snapshots, and reports. | DOCUMENTED; no production authorization call made |
| V3 schema | `LTPC` carries `ltp`, `ltt`, `ltq`, and `cp`; feed responses contain a message type, per-key feeds, `currentTs`, and market info. The vendored schema is generated into the project package and tests decode serialized messages. | DOCUMENTED + offline test verified |
| `ltt` unit | Official examples show 13-digit timestamp values and protobuf defines `ltt` as `int64`; the page calls it a timestamp but does not spell out the unit in prose. The parser enforces the requested `epoch_ms` contract and never guesses seconds. | INFERRED from the official example and requested contract; not empirically verified with a production tick |
| `currentTs` | Retained separately as feed/message metadata. It never replaces `ltt`. | DOCUMENTED + offline test verified |
| Tick identity | No provider sequence/tick ID is established in the accessible V3 LTPC schema. The adapter uses a local signature of instrument key, LTT, LTP, and LTQ for repeat-frame suppression; this is an inferred dedup key, not an Upstox event ID. | INFERRED; production duplicate behavior UNKNOWN |
| LTPC volume | LTPC has `ltq`, which is last-trade quantity. The V3 full-feed schema contains `vtt`; it is not present in the LTPC schema. LTPC quotes therefore expose `last_trade_quantity` and leave aggregate volume `null`. | DOCUMENTED; no volume is fabricated |
| Full-feed cumulative volume | The official page describes `vtt` as volume traded today; the full-feed protobuf field is present. If received in a future approved full-mode check, the adapter establishes a baseline, emits only nonnegative deltas, marks decreases as `VOLUME_RESET_OR_ANOMALY`, and does not assign reconnect-gap volume to a candle. | DOCUMENTED structure + offline synthetic unit tests; actual NSE EQ cash semantics remain UNKNOWN until a production sample is inspected |
| LTP REST volume | The separate LTP V3 REST endpoint documents cumulative current-day `volume`. This implementation does not poll it or combine REST values with LTPC ticks. | DOCUMENTED; unused |
| Instrument identity | Official BOD JSON and Search API fields support exact ISIN matching and explicit `exchange=NSE`, `segment=NSE_EQ`, `instrument_type=EQ`, and `trading_symbol` checks. The Search fallback is serial and rate-limited. | DOCUMENTED; local mapping remains unverified |
| Instrument limits | Upstox currently documents LTPC capacity above this 486-symbol roster. | DOCUMENTED; no 486-key subscription attempted |
| Session and candles | The existing NSE calendar and existing 1m/5m candle builder remain the only candle implementation. | Offline project tests verified; no live candle observed |
| Reconnect | Reconnect count is bounded to at most five retries, with exponential backoff and resubscription of the preserved key set. | Mocked reconnect test passed; live reconnect UNKNOWN |

Official references:

- [Market Data Feed V3](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/)
- [Market Data Feed Authorize V3](https://upstox.com/developer/api-documentation/get-market-data-feed-authorize-v3/)
- [Upstox instruments and NSE BOD JSON](https://upstox.com/developer/api-documentation/instruments/)
- [Instrument Search API](https://upstox.com/developer/api-documentation/instrument-search/)
- [LTP Quotes V3](https://upstox.com/developer/api-documentation/ltp-v3/)
- [Official Upstox V3 protobuf schema](https://github.com/upstox/upstox-python/blob/master/examples/websocket/market_data/v3/MarketDataFeedV3.proto)
- [Official Upstox V3 WebSocket example](https://github.com/upstox/upstox-python/blob/master/examples/websocket/market_data/v3/websocket_client.py)

## Mapping

The frozen Phase 3D universe contains **486** explicit-EQ rows with unique project symbols and ISINs. Two conservative requests were attempted against Upstox's official NSE BOD JSON.GZ URL (`https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz`). Both ended with a `ConnectionError`; no HTTP response or status was received and no mapping conclusion was drawn.

`results/phase4a8_upstox_instrument_mapping.csv` has the required columns and 486 rows. Current counts:

| Mapping status | Count |
|---|---:|
| VERIFIED | 0 |
| UNVERIFIED | 486 |
| MISSING / DUPLICATE / INVALID | 0 |

The 486 live subscription gate remains closed. The alternative ISIN search is implemented in `scripts/build_upstox_instrument_mapping.py --search-api`, uses a one-second request interval, and requires `UPSTOX_ACCESS_TOKEN` from the process environment. It was not called because no token was configured.

## Current validation results

| Check | Result |
|---|---|
| Five-symbol gate (`RELIANCE`, `TCS`, `INFY`, `HDFCBANK`, `SBIN`) | NOT RUN — no verified map and no token |
| 486-symbol subscription | NOT RUN — requires 486/486 verified mappings and a passing five-symbol gate |
| Source timestamp parsing | Offline tests pass for strict epoch-ms conversion; production LTT semantics UNKNOWN |
| Source vs receipt time | Separate timezone-aware fields; raw LTT, declared unit/origin, instrument key, `currentTs`, receipt time, and latency survive into the health snapshot and optional normalized-quote JSONL; offline test verified |
| Volume | LTPC aggregate volume unavailable; `ltq` remains last-trade quantity. Full `vtt` delta guard is covered offline only |
| 1-minute candles | Existing candle builder integration verified offline; live result unavailable |
| 5-minute candles | Existing candle builder integration verified offline; live result unavailable |
| Latency | No live samples; latency sample count 0 |
| Reconnect | Mocked bounded reconnect and resubscribe test passes; no production disconnect/reconnect test |
| Dashboard | Local read-only UI displays feed, map, subscription, receiving, latency, rejection, volume, and candle health. Existing port remains `127.0.0.1:8765` |
| Credential handling | Token is checked for presence and passed only in the Authorization header. It is not displayed or persisted by the provider or launcher |
| Live trading / broker / ledger | Disabled / not configured / not mutated |

### Preflight

Command:

```bash
PYTHONPATH=src python scripts/test_upstox_realtime.py --mode PREFLIGHT
```

Current status: **`PHASE4A8_PREFLIGHT_BLOCKED`**. Exact blockers:

- `token_present`: false (`UPSTOX_ACCESS_TOKEN` is not configured)
- `realtime_enabled_for_live`: false (safe default)
- `mapping_486_verified`: false (0/486)

Preflight made no market-data request, wrote no health or run marker, created no order, generated no signal, and did not mutate the paper ledger.

## Offline commands

```bash
PYTHONPATH=src python scripts/test_upstox_realtime.py --mode OFFLINE
PYTHONPATH=src pytest -q
python -m compileall -q src scripts
node --check forward/dashboard/app.js
```

Results: **338 passed, 0 failed**. Compileall and `node --check` passed. `OFFLINE` returned `OFFLINE_READY`. These checks made no Upstox market-data connection.

## Monday read-only start gate

The production connection remains explicit. First obtain and validate a complete official map. When the public BOD file is reachable:

```bash
python scripts/build_upstox_instrument_mapping.py --download-bod
```

If the public BOD file remains unavailable, provide the authorized token through a secure process environment and explicitly run the serial ISIN search:

```bash
python scripts/build_upstox_instrument_mapping.py --search-api
```

Only after the map reports `verified=486`, supply the token through the approved secret mechanism and run this single read-only feed command during the regular NSE session:

```bash
REALTIME_PROVIDER=upstox REALTIME_ENABLED=true LIVE_TRADING=false \
  PYTHONPATH=src python scripts/test_upstox_realtime.py --mode LIVE_READ_ONLY
```

The launcher checks token presence, realtime opt-in, live-trading off, provider selection, dependencies, complete verified mapping, calendar, baseline, dashboard files, broker-provider absence, and duplicate run state before authorizing a feed request. It subscribes to five symbols first; any failed five-symbol gate stops before the remaining 481 subscriptions. It does not invoke the Phase 3D forward trader or create orders. The default smoke window is five minutes; after the gate, the default coverage observation is fifteen minutes.

## Implementation files

- `src/nse_research/realtime/upstox.py` — mapping validation, official source requests, protobuf quote decoding, timestamp checks, guarded cumulative-volume deltas, bounded reconnect, provider health.
- `src/nse_research/realtime/proto/MarketDataFeedV3.proto` and generated `MarketDataFeedV3_pb2.py` — Upstox V3 wire schema.
- `scripts/build_upstox_instrument_mapping.py` — official BOD and explicit Search fallback.
- `scripts/test_upstox_realtime.py` — offline, no-network preflight, and opt-in live read-only modes.
- `results/phase4a8_upstox_instrument_mapping.csv` — explicit mapping status for all 486 project rows.
- `forward/dashboard/app.js` and `src/nse_research/dashboard.py` — read-only feed-health display and environment-only runtime enablement.

## Final classification

**PHASE 4A.8 STATUS: PARTIAL.** The offline adapter, strict map path, fail-closed modes, dashboard fields, and mocked tests are implemented. Live validation remains **UNKNOWN / NOT RUN**. Do not treat this as approval to trade or as a verified production feed. The blockers are the unavailable official BOD response, zero verified instrument mappings, and absent access token. Live trading remains disabled.
