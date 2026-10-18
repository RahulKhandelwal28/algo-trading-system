# Phase 4A.7 — Upstox V3 Read-Only Market-Data Validation

Review date: 2026-09-27  
Result: **BLOCKED**  
UAT status: **UPSTOX_MARKET_DATA_UAT_UNAVAILABLE** (no market-data UAT is present in the currently published Upstox sandbox capability list)  
Timestamp status: **UPSTOX_TIMESTAMP_UNKNOWN**

## Scope and safety

This phase reviewed current official Upstox documentation, inspected the existing realtime and candle contracts, attempted the official public instrument-master download, and added an isolated, synthetic-tested cumulative-volume delta helper. No Upstox credentials or environment variables were read. No access token was generated or used. No market-data WebSocket, quote, account, position, or order endpoint was called. No realtime provider was registered or selected.

The project configuration still has `realtime.enabled: false` and `live_trading.enabled: false` in `config/phase3d_personal.yaml`. No strategies, signals, orders, positions, paper fills, Phase 3C/3D datasets/results, paper ledger, historical data, dashboard, or risk settings were changed.

## UAT availability

**UPSTOX_MARKET_DATA_UAT_UNAVAILABLE** for the currently published sandbox contract. Upstox's [Sandbox documentation](https://upstox.com/developer/api-documentation/sandbox/) says sandbox capability is identified by an API-page flag and lists the enabled sandbox APIs: place order, place order V3, multi-order, modify order, and cancel order variants. The [Market Data Feed V3 page](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/) does not identify a sandbox flag or a UAT feed endpoint. The sandbox page also says its rollout is phased and invites checking for additional APIs, so this finding is specifically about the public documentation reviewed; it cannot rule out a private arrangement that Upstox has not documented publicly.

This is sufficient to stop before production. No production market-data connection was attempted.

## Authentication review

The [Analytics Token documentation](https://upstox.com/developer/api-documentation/analytics-token/) describes a read-only token that supports the WebSocket API category, is valid for one year, is limited to one token per account, and does not require a static IP for the listed market-data/websocket categories. It permits GET APIs and cannot place or modify orders. This is the safest documented credential class for a future read-only production test, subject to explicit authorization. No token was created or inspected.

The standard [Get Token API](https://upstox.com/developer/api-documentation/get-token/) uses an OAuth authorization-code exchange; its access token expires at 03:30 AM the following day. The V3 feed authorization contract requires a Bearer access token and redirects to a one-time WebSocket URL. No separate refresh grant for this feed was established in the reviewed Get Token documentation; plan on renewing through the documented authorization flow unless Upstox supplies a current refresh contract. The sandbox page says sandbox tokens last 30 days and are exclusively for sandbox orders. No market-data UAT token behavior or separate UAT scopes are documented. Analytics Token support is described by API category rather than named OAuth scopes.

## Official instrument master and frozen-universe mapping

Upstox's [Instruments documentation](https://upstox.com/developer/api-documentation/instruments/) recommends the JSON BOD instrument file and says to use `instrument_key` as the unique identifier. For an NSE equity record it documents these fields:

| Field | Meaning in the official file | Required match |
|---|---|---|
| `trading_symbol` | Trading symbol | Compare as a consistency check; not the primary identity key |
| `instrument_key` | Unique Upstox identifier | Output key used to subscribe |
| `exchange` | Exchange | Must be `NSE` |
| `segment` | Segment | Must be `NSE_EQ` |
| `instrument_type` | Instrument type | Must be `EQ` |
| `isin` | ISIN | Exact-match against the frozen universe ISIN |

The official NSE BOD file linked by the docs is `https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz`. The attempted download failed before an HTTP response because this environment could not resolve the host (`Errno 8: nodename nor servname provided`). The web reader also could not decode the `.gz` content type. No master rows were available for matching.

The frozen universe contains 486 rows, 486 unique symbols, and 486 explicit `EQ` series. The output file [phase4a7_upstox_instrument_mapping.csv](../results/phase4a7_upstox_instrument_mapping.csv) contains one row for every frozen symbol. Each row is marked `UNVERIFIED`; its `isin` column preserves the target ISIN from the project universe, while the Upstox instrument fields are blank. No symbol is represented as mapped, missing from Upstox, duplicate, or invalid because the official BOD data could not be read.

**Mapping: 0 / 486 verified.** Actual missing, duplicate, and invalid mapping counts remain unknown. Full-universe subscription is blocked.

## V3 market-data contract

The [V3 feed documentation](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/) describes a Protobuf WebSocket feed. It documents the subscription `instrumentKeys`, `mode`, `sub`/`unsub` methods, market-status message, an initial data snapshot, then live messages, and server ping frames when idle. The incoming message is Protobuf/binary; the sample's `feeds[InstrumentKey]` key can be joined to the BOD master to recover exchange, segment, and instrument type. The sample feed does not itself document separate `exchange=NSE` or `series=EQ` fields per quote.

| Field | Documented type and meaning | Current validation status |
|---|---|---|
| `feeds[InstrumentKey]` | Object keyed by subscribed instrument key | Documented; no live mapping checked |
| `ltpc.ltp` | Number, last traded price | Field documented; no quote received |
| `ltpc.ltt` | String, last traded time (timestamp) | Unit, epoch origin, timezone, and exchange/source semantics not documented |
| `ltpc.ltq` | String, last traded quantity | Field documented; no observation |
| `vtt` | String, “volume traded today (in contracts)” in Full feed | Day-volume field documented; NSE cash-equity unit and reset behavior not empirically checked |
| `marketOHLC.ohlc[]` | Open/high/low/close numbers; `vol` and `ts` strings; examples include `1d` and `I1` | Schema examples documented; no values validated |
| `market_info.segmentStatus` | Segment-to-status map, including `NSE_EQ` | Documented; no feed status received |
| `currentTs` | Timestamp of the received tick in the feed | Provider/feed timestamp, not local receive time; its precise time basis is not established |

Full mode documents five market-depth levels and extended metadata. The standard limits are two connections per user and up to 2,000 individual Full subscriptions (1,500 combined when mixing modes), so 486 instruments fit the published capacity. Capacity limits do not establish data availability or coverage.

## Timestamp gate

**UPSTOX_TIMESTAMP_UNKNOWN — provider validation stopped.** Official documentation labels `ltpc.ltt` as a last-traded timestamp but does not specify its unit, epoch origin, timezone, or whether it is exchange time or another source timestamp. The examples look numeric and millisecond-sized, but that appearance is not sufficient to infer the conversion. `currentTs` is described as the received tick timestamp; it cannot replace the trade/source time or the project's local `received_at` clock. The `.proto` schema is linked by Upstox but was not readable through the available documentation fetcher. No timestamp parser or Upstox provider was added.

Without documented timestamp semantics, the project cannot construct a trustworthy timezone-aware `NormalizedQuote.source_timestamp`, enforce the existing one-second future-skew gate, or test candle timing. This is an independent blocker even if a production read-only connection is later authorized.

## Volume conversion

The official schema names `vtt` as volume traded today. Treating a day-to-date field as cumulative is an inference from that wording, not an empirically observed Upstox NSE cash tick. The schema's unit wording says “contracts”; it does not clarify cash-equity share units, session-reset details, or gap correction behavior. Therefore no Upstox value was assigned a `volume_kind` or sent into the candle builder.

Added [volume.py](../src/nse_research/realtime/volume.py), an offline generic guard that accepts a validated cumulative integer, timezone-aware ordered source time, and exchange-calendar session date. It returns no delta for the first observation/session reset, volume-counter decrease, or caller-detected feed gap; it records a quality event and rebases safely. Equal volume yields zero; an older source timestamp is ignored without changing state. The helper is not connected to an Upstox provider or the candle engine. Synthetic coverage is in [test_realtime_volume.py](../tests/test_realtime_volume.py).

## 5-symbol, reconnect, candles, universe feed, latency, and feed health

| Validation | Result |
|---|---|
| Five-symbol read-only test (`RELIANCE`, `TCS`, `INFY`, `HDFCBANK`, `SBIN`) | Not run; public market-data UAT unavailable and production connection not authorized |
| Authentication and WebSocket subscription | Not run |
| Reconnect/resubscribe and duplicate behavior | Not tested; documentation alone does not establish empirical recovery |
| 1-minute candle | Not tested; no normalized quote with a verified source timestamp was available |
| 5-minute candle | Not tested; no normalized quote with a verified source timestamp was available |
| 486-symbol stream | Not run; mapping is 0/486 verified and five-symbol gate did not pass |
| Feed coverage and missing live symbols | Not measured |
| Connection/first-quote/average/maximum latency, message rate, CPU, memory | Not measured |
| Feed-health integration | Not changed; no Upstox provider exists in the registry |

## Tests and failures

Added synthetic tests for normal delta, unchanged cumulative volume, counter decrease, explicit feed gap, session rollover, out-of-order observations, and invalid metadata. No test depends on Upstox or the internet. The focused test module passed: **11 passed**. Existing provider, candle, strategy, and risk implementations were not changed. The full suite was not run.

Critical blockers:

1. No market-data sandbox/UAT is exposed by the current public sandbox capability list.
2. `ltpc.ltt` timestamp semantics are unknown, preventing quote normalization and candle validation.
3. The official BOD `.json.gz` file could not be downloaded in this environment, leaving all 486 mappings unverified.
4. No production market-data connection was explicitly authorized.

## Final status

**BLOCKED.** Upstox V3 is not validated as compatible with the project's fail-closed realtime engine. No signals, orders, positions, paper fills, broker requests, or live trading occurred.

## Official sources

- [Market Data Feed V3](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/)
- [V3 feed authorization](https://upstox.com/developer/api-documentation/get-market-data-feed-authorize-v3/)
- [Instrument master](https://upstox.com/developer/api-documentation/instruments/)
- [Sandbox API capability list](https://upstox.com/developer/api-documentation/sandbox/)
- [Analytics Token](https://upstox.com/developer/api-documentation/analytics-token/)
- [Standard Get Token](https://upstox.com/developer/api-documentation/get-token/)
