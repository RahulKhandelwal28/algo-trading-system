# Choice FinX Market-Data Contract Matrix

Review date: 2026-09-27  
**CONTRACT_STATUS: PARTIAL**

This matrix distinguishes a fact documented for an order, trade, or market-status event from a field documented for a market quote tick. Fields on order/trade messages do not establish a market-feed quote contract.

## Official sources

- **[HUB]** [Choice FinX Developer Hub](https://choiceindia.com/stock-market-trading-api/docs) — lists a “Live Market Feed” for real-time tick, depth, and segment streaming via WebSocket, and links the socket reference below.
- **[GUIDE]** [Choice FinX OpenAPI Integration Guide, May 2026](https://choiceindia.com/stock-market-trading-api/files/Choice_OpenAPI_Integration_Guide_May_2026.pdf) — public guide lists `wss://stream.jiffy.in/`, market-data API categories, and the UAT API documentation URL.
- **[SOCKET]** [FINX Interactive Socket WebSocket API — Technical Reference, May 2026](https://docs.google.com/document/d/1wIptRZZYr0dgQiqX_FvnFap8KDo-vtG0J2GVa1XMQkg/edit?usp=sharing) — linked by the Developer Hub as its Live Market Feed documentation. Read in Chrome as a view-only document.
- **[REST]** [Choice OpenAPI reference](https://finx.choiceindia.com/api/OpenAPI/Info) — the rendered reference inspected in Chrome identifies itself as OpenAPI Interface API V1 and shows V1/V2 REST operations. It is not a UAT socket specification.

## Contract matrix

| Requirement | Found? | Official source | Exact field/message | Status |
|---|---|---|---|---|
| WebSocket URL | YES | [GUIDE], [SOCKET] | Guide: `wss://stream.jiffy.in/`. Socket reference: `wss://finxsocket.choiceindia.com/ws/?token=<JWT_TOKEN_FROM_LOGON_RESPONSE>`. | UNKNOWN — two URLs are published; their relationship and UAT designation are not stated. |
| UAT WebSocket URL | NO | [GUIDE], [SOCKET] | Guide gives UAT REST docs at `https://uat.jiffy.in/api/OpenAPI/Info`, but no UAT WebSocket URL. | UNKNOWN |
| Authentication | YES | [SOCKET], [REST] | Socket reference says Logon/2FA returns an RS256 JWT, supplied as query parameter `token`. OpenAPI reference describes TOTP login and vendor/API-key headers. | PASS for the high-level socket mechanism; UAT applicability and the exact login response remain unknown. |
| JWT acquisition | YES | [SOCKET], [REST] | “Logon / 2FA Authentication API” response supplies the JWT; no exact response field or complete response example is provided in the socket reference. | UNKNOWN |
| NSE exchange identifier | YES | [SOCKET] | `Exchange=1` is NSE in `ORD_NRML` and `TRD_MSG`; `Segment=1` is NSE Cash in `MKT_STAT`. | UNKNOWN for a quote tick; those identifiers appear in non-quote events. |
| NSE Cash segment | YES | [SOCKET], [REST] | `MKT_STAT.Data.Segment="1"` means NSE Cash; REST reference also lists segment master `NSE - CASH : 1`. | PASS for the segment/status identifier; no quote subscription uses it in the docs. |
| EQ series | YES | [SOCKET] | `Series=EQ` appears in the `ORD_NRML` and `TRD_MSG` field references; the example instrument name is `RBA-EQ`. | UNKNOWN for market-data ticks; no tick schema establishes series. |
| Instrument identifier | YES | [SOCKET], [REST] | `ScripCode` appears in order/trade events. REST `ScripDetails` takes `SegmentId` and `Token`; no symbol-to-token master schema is shown. | UNKNOWN — no deterministic frozen-universe mapping is documented. |
| Symbol | YES | [SOCKET] | `Symbol` appears on order/trade events (`RBA`, `RBA-EQ`). | UNKNOWN for market-data ticks. |
| LTP | NO | [HUB], [SOCKET], [REST] | The Hub promises real-time ticks, but the linked socket message list has no LTP/quote message or LTP field. | UNKNOWN |
| Trade price | YES | [SOCKET] | `TRD_MSG.TradedPrice` is described as the price at which that trade executed. | UNKNOWN as a market quote; it is an execution update, not documented LTP. |
| Source timestamp | YES | [SOCKET] | `TRD_MSG.TradeTime` and `TradeTimeStamp`; sample is `2026-05-08 11:55:20`, described as ISO format, with no timezone. | UNKNOWN for quote ticks; no quote timestamp or timezone semantics. |
| Volume | YES | [SOCKET] | `TRD_MSG.TradeQty` and `QuantityTradedToday` are execution quantities. | UNKNOWN for market-wide quote volume. |
| Volume semantics | YES | [SOCKET] | `QuantityTradedToday` is described as cumulative quantity for the day on a trade-update message. | UNKNOWN for market-feed volume; this does not document cumulative/incremental market volume. |
| Bid | NO | [HUB], [SOCKET], [REST] | The Hub mentions depth; no bid field/schema appears in the linked socket reference or rendered OpenAPI operations. | UNKNOWN |
| Ask | NO | [HUB], [SOCKET], [REST] | The Hub mentions depth; no ask field/schema appears in the linked socket reference or rendered OpenAPI operations. | UNKNOWN |
| Market depth | YES | [HUB], [GUIDE] | Hub says the Live Market Feed includes depth; guide lists `/depth`. No depth payload or level schema was accessible. | UNKNOWN |
| Market-data tick message | NO | [HUB], [SOCKET] | Socket reference enumerates only `MKT_STAT`, `ORD_NRML`, and `TRD_MSG`; it provides no tick/quote event. | UNKNOWN |
| `MKT_STAT` message | YES | [SOCKET] | Market status open/close events. Segment ID `1` is NSE Cash. Its sample request/response is not documented as a quote subscription. | PASS as a market-status event only. |
| `ORD_NRML` message | YES | [SOCKET] | Order status; includes order fields such as `OrderPrice`, `Series`, `ScripCode`, and `Exchange`. | PASS as an order update only. |
| `TRD_MSG` message | YES | [SOCKET] | Trade confirmation; includes `TradeQty`, `TradedPrice`, `TradeTime`, `Series`, `ScripCode`, and `Exchange`. | PASS as a trade update only. |
| Subscribe frame | NO | [HUB], [SOCKET] | No market quote subscribe payload or method is shown. The `MKT_STAT` example is not defined as a quote subscription. | UNKNOWN |
| Unsubscribe frame | NO | [HUB], [SOCKET] | No unsubscribe payload or method is shown. | UNKNOWN |
| Subscription limits | NO | [SOCKET] | Socket errors mention HTTP 429 when the account’s maximum connections are exceeded, but give no connection count or instrument-subscription capacity. | UNKNOWN |
| Heartbeat | YES | [SOCKET] | Send plain text/integer `2`; server responds `3`; timeout 30 seconds; check interval 1 second. | PASS for the documented socket. |
| Reconnect behavior | NO | [SOCKET] | No retry delay, reconnect procedure, or resubscription rule. Heartbeat timeout closes the connection. | UNKNOWN |
| Connection error messages | YES | [SOCKET] | Expired/invalid token: HTTP 401; maximum connections exceeded: HTTP 429; server exception: HTTP 500. | PASS for these documented connection errors. |
| Duplicate/out-of-order market ticks | NO | [SOCKET] | Order/trade messages include `TradeNumber` or `MessageSequenceNumber`; no market-tick identity/order contract is documented. | UNKNOWN |

## What the three documented message types establish

- `MKT_STAT` establishes market/segment status, not a security price. The documented `Segment=1` means NSE Cash, but it does not identify a symbol, EQ series, LTP, or market volume.
- `ORD_NRML` is an order-status update. Its `Series`, `ScripCode`, and `Exchange` fields describe an order's instrument; `OrderPrice` is an order price (and is zero for market orders), not an LTP feed.
- `TRD_MSG` is a trade execution update. `TradedPrice`, `TradeQty`, and `TradeTime` describe an execution; they do not constitute a continuously streamed quote for arbitrary subscribed securities. `QuantityTradedToday` is trade-message cumulative quantity, not documented market-wide volume.

The socket reference's overview says the connection receives market data, and the Developer Hub markets real-time ticks/depth, but the reference's supported message list and connection-flow summary contain no market-data quote event. The available docs therefore do not explain how to subscribe to or receive the advertised tick/depth stream.

## Findings and gate

1. The general Integration Guide and the detailed socket reference publish different WebSocket URLs. Neither identifies the UAT socket endpoint.
2. The detailed socket reference documents JWT-in-query authentication and heartbeat, but not a quote subscription protocol.
3. The three documented message types are insufficient to produce an LTP/timestamp/market-volume `NormalizedQuote`. Do not reinterpret `TRD_MSG` as a quote tick.
4. The rendered OpenAPI reference shows login/TOTP, market status, scrip details, historical/intraday chart data, order, and account operations; it does not show a market quote WebSocket schema or a reverse symbol-to-token master.
5. The frozen 486-symbol universe can be checked locally for its requested identities, but Choice mapping and subscription coverage cannot be assessed from this contract.

**CHOICE_FINX_MARKET_DATA_STATUS = PARTIAL**

No Choice provider, WebSocket frames, quote parser, credential flow, or market-data dashboard extension is implemented. This is the required stop point: critical quote/tick protocol fields remain unknown.

For the separate REST market-data contract investigation and REST-vs-WebSocket comparison, see [CHOICE_FINX_REST_VS_WEBSOCKET.md](CHOICE_FINX_REST_VS_WEBSOCKET.md).
