# Phase 4A.5 — Choice FinX Realtime Provider Review

Review date: 2026-09-27  
Project phase: Choice FinX compatibility investigation  
Result: **PARTIAL — integration blocked pending the documented UAT socket contract**

## Scope and safety

This review checked Choice's official product/API pages and the official May 2026 OpenAPI Integration Guide. It did not change provider code, configuration, the dashboard, the frozen universe, strategy or risk code, Phase 3C/3D results, or the Phase 3D paper ledger. No Choice connection was attempted. No order, account, position, or broker endpoint was called.

The existing realtime configuration remains disabled by default. There is no Choice provider registered in the project. The existing provider interface and Phase 4A validation/candle path remain the only realtime implementation.

## Official documentation reviewed

The official Choice FinX integration guide identifies:

| Item | What the public guide says | What remains unverified |
|---|---|---|
| UAT API documentation | `https://uat.jiffy.in/api/OpenAPI/Info` | Detailed endpoint schemas were not retrievable in this review environment. |
| Production API documentation | `https://finx.choiceindia.com/api/OpenAPI/Info` | Production is intentionally not used for this integration. |
| Market-data categories | `/quote`, `/depth`, `/ohlc`, `/instruments` | Request/response fields and NSE cash-equity behavior. |
| WebSocket host | `wss://stream.jiffy.in/` | Whether this host is UAT or production, any path/query parameters, handshake, authentication, frames, heartbeat, limits, and reconnect rules. |
| Authentication modes | OAuth mode (authorization code then access/refresh token) and non-auth API-key mode are described at a high level. | Exact socket authentication and the required UAT credential form. The guide describes vendor UAT Vendor ID + Vendor Key and direct-client API-key access; it does not define the prompt's proposed `client code` field as a socket field. |
| Instrument lookup | An `/instruments` market-data endpoint is listed. | Instrument-master format and fields proving `exchange=NSE`, `series=EQ`, symbol identity, and a unique instrument identifier. |
| Exchanges | The guide describes Choice as supporting NSE, BSE, and MCX. | Whether the available UAT feed streams this project's NSE cash EQ instruments and includes explicit series identity. |

The guide points to a separate “Interactive Socket Integration” Google document and says complete request/response schemas are on the developer portal. Attempts to open the UAT/production OpenAPI pages and the linked socket document returned tool-level internal/unavailable errors; no HTTP status or authentication response was obtained. Therefore this review cannot claim that those sources are absent or that access was denied. It can only say their protocol contents were not inspectable here.

Official sources reviewed:

- [Choice FinX OpenAPI Integration Guide, May 2026](https://choiceindia.com/stock-market-trading-api/files/Choice_OpenAPI_Integration_Guide_May_2026.pdf)
- [Choice FinX API product page](https://choiceindia.com/stock-market-trading-api/)
- [Choice FinX API documentation page](https://choiceindia.com/stock-market-trading-api/docs)

## Protocol fields not established

The public integration guide does **not** provide enough detail to implement a conforming socket client. The following remain unknown from accessible official documentation:

- UAT WebSocket URL and any path/query parameters.
- WebSocket authentication handshake and required headers or message fields.
- Subscription and unsubscription frame shapes, exchange identifiers, and instrument-token field.
- Tick event schema, including an explicit NSE exchange and EQ series field.
- LTP field name and numeric representation.
- Source timestamp field, unit/epoch precision, timezone, and exchange-versus-provider semantics.
- Whether volume is cumulative, incremental, or another measure.
- Tick sequence/event identity for deduplication.
- Heartbeat, ping/pong, disconnect, reconnect, and resubscription behavior.
- Per-connection/per-message subscription limits, batching rules, and market-data rate limits.
- Availability and schema of an authoritative instrument master for deterministic 486-symbol mapping.

The guide's order-rate threshold is an order-routing rule and is not evidence of a market-data subscription or quote rate limit.

## Compatibility gate

| Requirement | Review status | Reason |
|---|---|---|
| NSE market-data candidate exists | Candidate only | Choice lists NSE support and market-data APIs, but UAT NSE cash tick behavior is not shown. |
| Explicit EQ identity | Unknown | No accessible tick or instrument schema establishes series. |
| Deterministic instrument mapping | Unknown | `/instruments` is listed without a schema or sample. |
| UAT WebSocket handshake and subscription | Unknown | Separate socket guide and API portal schemas could not be read. |
| Source timestamp and freshness | Unknown | No tick timestamp field or semantics documented in accessible material. |
| LTP | Unknown | `/quote` is listed, but no response field schema was accessible. |
| Volume semantics | Unknown | No realtime volume definition was accessible. |
| Duplicate identity | Unknown | No sequence/tick ID contract was accessible. |
| Reconnect/heartbeat | Unknown | Not documented in the accessible guide. |
| 486-symbol capacity/batching | Unknown | No market-data subscription limits were found in accessible documentation. |
| Existing generic validation | Existing tests pass | Phase 4A accepts only explicit NSE/EQ, positive finite LTP, timezone-aware timestamps, and fresh quotes. |
| Existing 1m/5m candle engine | Existing deterministic tests pass | This verifies the generic engine, not Choice-fed ticks. |
| Safe provider failure | Preserved | No provider selection, fallback, or automatic dashboard connection was added. |

**CHOICE_FINX_COMPATIBILITY = PARTIAL**

This is not a compatibility pass. A live adapter, credentials/configuration, socket implementation, Choice-specific dashboard fields, and 5/full-universe UAT tests were not added because the exact official wire and instrument contracts could not be established. Implementing them now would require guessing prohibited by the Phase 4A.5 request.

## Existing project state

- Historical/research provider selection remains unchanged; yfinance is not a realtime fallback.
- `realtime.enabled` remains `false`; provider selection remains unset.
- The dashboard does not start a market-data worker automatically.
- No Choice credential values were printed or used; no external connection was attempted. A presence-only environment check found the expected Choice variables unset.
- The existing 486-symbol frozen universe was not changed.
- No signals, candles from provider data, paper orders, broker requests, or ledger updates were generated by this review.

## Information needed to resume implementation

Implementation can proceed after the official Choice UAT material is available locally or through an accessible official portal:

1. UAT OpenAPI definition or official endpoint schemas for authentication, instrument lookup, and market data.
2. The official Interactive Socket Integration guide, including its UAT endpoint and exact frame examples.
3. An official instrument-master sample/schema that explicitly identifies exchange, series, symbol, and unique instrument ID.
4. UAT credentials provisioned for the applicable direct-client or vendor integration mode, stored outside source/config artifacts.

After those are available, the intended safe sequence is the existing frozen-universe five-symbol smoke test in UAT, then the explicitly requested bounded full-universe test only if the first test passes. Production remains disabled.

## Phase 4A.5B follow-up

The official Developer Hub and the linked view-only FINX Interactive Socket reference were subsequently opened in Chrome, and the rendered Choice OpenAPI reference was inspected. The socket document gives a JWT query-token URL and heartbeat contract but lists only `MKT_STAT`, `ORD_NRML`, and `TRD_MSG`; it does not provide quote subscription frames or a market tick schema. The integration guide and detailed socket reference also publish different WebSocket URLs without identifying the UAT URL. The result remains **PARTIAL**. See [CHOICE_FINX_MARKET_DATA_CONTRACT.md](CHOICE_FINX_MARKET_DATA_CONTRACT.md) and [PHASE4A5_CHOICE_FINX_MARKET_DATA.md](PHASE4A5_CHOICE_FINX_MARKET_DATA.md) for the updated matrix and final report.

## Phase 4A.5C REST follow-up

The May 2026 guide lists `/quote`, `/depth`, `/ohlc`, and `/instruments`, but the accessible material still did not provide their operation schemas. No authoritative symbol-to-token/EQ map, quote source timestamp, volume semantics, batching limit, or quote rate limit was established. The REST and WebSocket paths both remain **UNKNOWN** for Phase 4B suitability. See [CHOICE_FINX_REST_VS_WEBSOCKET.md](CHOICE_FINX_REST_VS_WEBSOCKET.md) for the comparison. No API endpoint was called and live trading remains disabled.
