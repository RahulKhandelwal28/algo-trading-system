# Phase 4A.5B — Choice FinX Market-Data Contract Review

Review date: 2026-09-27  
**CHOICE FINX MARKET-DATA STATUS: PARTIAL**

No Choice provider was implemented because the official material does not document a usable quote subscription/tick message. The precise findings and per-field evidence are in [CHOICE_FINX_MARKET_DATA_CONTRACT.md](CHOICE_FINX_MARKET_DATA_CONTRACT.md).

## Documentation reviewed

- [Choice FinX Developer Hub](https://choiceindia.com/stock-market-trading-api/docs)
- [Choice OpenAPI Integration Guide, May 2026](https://choiceindia.com/stock-market-trading-api/files/Choice_OpenAPI_Integration_Guide_May_2026.pdf)
- [FINX Interactive Socket WebSocket API — Technical Reference, May 2026](https://docs.google.com/document/d/1wIptRZZYr0dgQiqX_FvnFap8KDo-vtG0J2GVa1XMQkg/edit?usp=sharing), linked from the Developer Hub and read in Chrome
- [Choice OpenAPI reference](https://finx.choiceindia.com/api/OpenAPI/Info), rendered production REST API specification

The Guide lists UAT API docs at `https://uat.jiffy.in/api/OpenAPI/Info` and a general socket URL `wss://stream.jiffy.in/`. The detailed socket reference instead gives `wss://finxsocket.choiceindia.com/ws/?token=<JWT_TOKEN_FROM_LOGON_RESPONSE>`. The docs do not state how those socket URLs relate or provide a UAT WebSocket URL. No API endpoint or socket was called.

## Authentication

The detailed socket reference says the client obtains an RS256 JWT from a Logon/2FA API response and sends it as the `token` query parameter. It documents token-expired/invalid as 401, maximum connections exceeded as 429, and server exception as 500. The rendered REST reference includes TOTP login operations and vendor/API-key headers, but the exact JWT response field and UAT socket authentication are not established. No credentials were configured or used.

## WebSocket and quote schema

The socket reference describes JSON messages and a heartbeat: client sends plain text/integer `2`, server acknowledges `3`, with a 30-second timeout and one-second check interval. It lists only:

- `MKT_STAT`: market/segment status. `Segment=1` means NSE Cash.
- `ORD_NRML`: normal-order status.
- `TRD_MSG`: trade execution confirmation.

These do not define a market quote stream. `TRD_MSG.TradedPrice` is an executed trade price, while `TradeQty` and `QuantityTradedToday` are execution quantities. They are not documented as market LTP and market-wide volume. `TradeTime`/`TradeTimeStamp` are trade times; the example has no timezone and does not establish a quote timestamp.

Choice's Developer Hub advertises tick and depth streaming, but the linked socket reference contains no market-data tick type, LTP, bid/ask/depth schema, subscribe/unsubscribe frame, or market-tick identifier. The public Integration Guide's `/quote`, `/depth`, `/ohlc`, `/instruments` categories are not enough to infer those payloads. The rendered REST API reference exposes market-status, scrip-details, and chart-data operations but no quote subscription schema or reverse symbol-to-token mapping.

## NSE/EQ and instrument mapping

The socket reference documents exchange ID `1` as NSE on order/trade messages and `Series=EQ` on example order/trade records. `MKT_STAT` and the REST reference identify segment ID `1` as NSE Cash. These facts validate those respective message types only; they do not prove that an arbitrary quote tick is NSE EQ. The REST `ScripDetails` operation accepts a `SegmentId` and `Token`, but no official symbol-to-token instrument-master schema was found.

## Subscription and 486-symbol coverage

The socket guide's 429 error means the account's maximum connection count was exceeded; it does not specify the count or instruments per connection. No subscribe/unsubscribe messages, per-connection quote limits, or batching rules are documented.

The local frozen universe contains **486 unique symbols**, all with explicit `EQ` series. Choice coverage was not tested: **0 mapped, 0 subscribed, 0 receiving** because no deterministic Choice mapping contract is available. No full-universe request was made.

## Normalization, candles, and feed health

The existing Phase 4A path requires explicit NSE and EQ identity, positive finite LTP, timezone-aware source and receipt timestamps, freshness checks, and documented volume semantics before a quote can form a candle. The generic deterministic 1m/5m candle tests validate that existing engine, but Choice quote normalization and Choice-fed candles remain **untested** because no compatible tick schema is documented.

No Choice connection state, authentication status, quote metrics, or Choice sample rows were added to the dashboard. The existing dashboard remains read-only, does not start a feed worker, and reports the realtime feed as not configured when no provider is configured.

## UAT and safety

- **Five-symbol UAT:** not run. No UAT socket URL or quote subscription contract is available; no Choice credentials are configured.
- **486-symbol UAT:** not run. The five-symbol test has not passed.
- **Candle result:** generic 1m/5m tests pass; Choice-fed candle result is not established.
- **Feed health:** no Choice feed was started; no latency or quote-quality metrics exist.
- **Trading safety:** realtime remains disabled by default; live trading remains disabled. No signal, order, broker request, or ledger update was generated.

## Tests and files

The documentation-gate script is designed to report the incomplete contract without opening a network connection. Offline tests cover that fail-closed behavior. No socket protocol tests are claimed because there is no documented quote protocol to mock faithfully.

No strategy, risk, cost, historical data, Phase 3C result, Phase 3D result, or paper ledger changes are part of this phase.

## Remaining blockers

Choice must provide a single applicable UAT market-data contract specifying the UAT WebSocket URL, authentication response/token field, quote subscribe and unsubscribe frames, market tick event schema, explicit NSE Cash/EQ instrument identity and mapping, LTP, source timestamp and timezone, market volume semantics, bid/ask/depth fields if required, tick identity/ordering, subscription limits, heartbeat, reconnect/resubscribe behavior, and quote error messages.

Until these are documented or demonstrated by an official Choice UAT example, the compatibility result stays **PARTIAL** and no speculative provider code should be added.

## Phase 4A.5C REST follow-up

The May 2026 Integration Guide lists `/quote`, `/depth`, `/ohlc`, and `/instruments` as market-data categories, but does not include their operation schemas. The production OpenAPI UI inspected during Phase 4A.5B did not expose schemas for those four operations in the material available then. Follow-up attempts to read the exact production and guide-published UAT OpenAPI URLs could not retrieve content in this review environment; no underlying JSON/YAML schema or official Postman collection was obtained. This is not evidence that Choice blocks access or that the actual authenticated developer portal lacks the schemas.

REST method/path details, authentication headers for each operation, request/response/error fields, quote timestamp semantics, volume semantics, batching, and quote-specific rate limits therefore remain unknown. No data request was made. REST is a possible polling candidate but is not validated as suitable for Phase 4B; WebSocket quote suitability also remains unknown. See [CHOICE_FINX_REST_VS_WEBSOCKET.md](CHOICE_FINX_REST_VS_WEBSOCKET.md) for the endpoint-by-endpoint findings and comparison.

No provider, parser, mocked response schema, or Choice-specific test was added because no official response example was available to reproduce without guessing. Realtime and live trading remain disabled.
