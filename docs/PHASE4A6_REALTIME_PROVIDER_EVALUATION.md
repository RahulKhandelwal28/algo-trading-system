# Phase 4A.6 — NSE Realtime Provider Evaluation

Review date: 2026-09-27  
Scope: official-documentation review for read-only NSE cash market data  
Result: **two candidates pass the documentation gate to UAT; no provider has been tested**

## Scope and evidence standard

This is a documentation evaluation, not an integration or reliability claim. Sources below are provider-published documentation and support material reviewed on 2026-09-27. No provider login, credential/environment-variable check, market-data request, WebSocket connection, or broker endpoint was used. No quotes, candles, orders, signals, positions, or ledger records were produced.

“PASS” means the cited documentation explicitly describes the named field or limit. “PARTIAL” means only part of the contract is documented or the current 486-symbol coverage has not been cross-checked. “UNKNOWN” means the reviewed official material does not establish the fact. No provider is marked empirically tested.

Provider classifications follow the requested gate:

- **IMPLEMENTATION_READY** — all technical contract and coverage requirements are established and verified.
- **UAT_REQUIRED** — the published contract is adequate to build a read-only adapter, but feed behavior and full-universe coverage still require controlled validation.
- **DOCUMENTATION_BLOCKED** — a critical mapping, timestamp, or volume detail is missing.
- **UNSUITABLE** — a documented hard requirement cannot be met.

## Existing project contract

The frozen universe file, **data/paper_trading/phase3d_personal_universe_v2.csv**, has 486 data rows. Each row has an explicit series column; this review did not download current provider masters or cross-match the 486 symbols.

The existing **NormalizedQuote** carries symbol, exchange, last_price, source timestamp/source_timestamp, local received_at, series, optional tick_id, volume, and volume_kind. The shared gate requires explicit exchange NSE and series EQ, a finite positive price, timezone-aware source and receipt times, and a fresh source timestamp. The instrument ID and bid/ask are not fields on the current dataclass; an adapter would retain the ID in its own symbol map and may expose depth separately.

The existing candle builder constructs its own 1m and 5m candles from normalized prices. It only adds quote volume when volume_kind is INCREMENTAL; a CUMULATIVE value is deliberately omitted. Therefore a provider's day-to-date volume cannot be passed through as candle volume. An adapter must derive validated increments, reset its baseline at session boundaries, and fail closed over gaps or counter regressions. Provider OHLC/candles are not a replacement for the project's candle builder.

The current Phase 3D config remains realtime.enabled=false, realtime.provider='', and live_trading.enabled=false. No provider adapter or provider-specific configuration was added.

## Quote contract compatibility matrix

Each provider cell starts with the required status token; details follow the em dash. “486 capacity” refers to documented subscription capacity. Actual current symbol-master coverage of the frozen list is marked separately.

| Requirement | Choice FinX | Upstox V3 | DhanHQ V2 | Angel One SmartAPI | FYERS API V3 |
|---|---|---|---|---|---|
| Exchange NSE | PARTIAL — NSE is advertised/supported; cash-equity wire identity remains absent. | PASS — master has exchange=NSE. | PASS — segment and master identify NSE. | PASS — NSE is exchange type 1 in the socket and NSE in REST/master. | PASS — market symbols use the explicit NSE prefix. |
| NSE Cash segment | UNKNOWN — no accessible quote contract establishes cash segment. | PASS — segment=NSE_EQ. | PASS — ExchangeSegment=NSE_EQ. | PASS — exchangeType=1 is nse_cm. | PARTIAL — examples use NSE cash symbols; exact segment field contract is not exposed in the reviewed data page. |
| Explicit EQ series | UNKNOWN — no quote/master schema with series. | PASS — instrument_type=EQ and segment=NSE_EQ. | PASS — master has exchange-defined SERIES/SEM_SERIES. | PASS — master trading symbol carries explicit -EQ suffix; feed mapping must preserve it. | PASS — documented symbols include -EQ; symbol master includes exchange instrument type. |
| Instrument ID | UNKNOWN — not specified. | PASS — instrument_key combines NSE_EQ and ISIN. | PASS — SecurityId. | PASS — token/symboltoken. | PARTIAL — current symbol master and subscription identifiers are linked from the docs, but the reviewed docs did not expose a full field contract. |
| Deterministic symbol mapping | UNKNOWN — no accessible master schema. | PARTIAL — ISIN plus explicit NSE_EQ/EQ provides deterministic mapping; current 486-key crosswalk not run. | PARTIAL — exchange, series, symbol and Security ID are in the official master; current 486 crosswalk not run. | PARTIAL — NSE master token plus exact -EQ symbol supports deterministic mapping; current 486 crosswalk not run. | PARTIAL — official CSV master and NSE:SYMBOL-EQ examples exist; current 486 crosswalk not run. |
| LTP | UNKNOWN — endpoint category only. | PASS — ltpc.ltp. | PASS — ticker/quote packet LTP. | PASS — websocket payload and REST FULL quote. | PASS — Lite and SymbolUpdate modes describe LTP. |
| Source timestamp | UNKNOWN — not established. | PARTIAL — ltpc.ltt is “last traded time”; examples look like epoch milliseconds, but prose does not state unit/timezone. currentTs is receipt time and must not substitute. | UNKNOWN — LTT is labelled EPOCH in the packet table, with no unit or timezone stated. | PASS — exchange timestamp is explicitly epoch milliseconds; a separate last-traded timestamp is provided. | UNKNOWN — a generic timestamp example exists, but exchange/source-time meaning and precision are not established. |
| Volume field | UNKNOWN — not established. | PASS — full feed has vtt, “volume traded today”; OHLC feed also includes traded volume. | PARTIAL — stream packet contains Volume; REST market quote describes total traded volume for the day. | PASS — stream field is “Volume traded for the day”; also includes last traded quantity. | PARTIAL — SymbolUpdate advertises OHLCV; exact field-level feed schema is not exposed in reviewed official docs. |
| Volume semantics | UNKNOWN — not established. | PASS — vtt is cumulative day volume; convert to increments before candle use. | UNKNOWN — daily-total wording appears in REST quote docs, but the V2 stream packet table calls its field only “Volume.” Confirm stream semantics before mapping it. | PASS — day volume is cumulative; convert to increments before candle use. | UNKNOWN — volume is exposed/advertised, but cumulative versus per-update semantics are not defined. |
| Bid/ask | UNKNOWN — not established. | PASS — Full feed includes five depth levels. | PASS — Full packet includes five bid/ask levels. | PASS — Snap Quote includes best-five depth. | PARTIAL — support material advertises bid/ask/depth without the reviewed full packet field schema. |
| OHLC | UNKNOWN — category listed, schema unavailable. | PASS — full feed has 1m I1, 30m I30 and 1d OHLC; do not use these as project candles. | PASS — quote/full packet has day OHLC. | PASS — quote/snap quote has day OHLC. | PASS — SymbolUpdate advertises OHLCV. |
| WebSocket push | PARTIAL — official materials advertise a live market WebSocket, but quote stream contract is still missing. | PASS — supported Market Data Feed V3. | PASS — documented Live Market Feed V2. | PASS — documented WebSocket Streaming 2.0. | PASS — documented API V3 Data WebSocket. |
| Subscription protocol | UNKNOWN — market quote frames/schema unavailable. | PASS — V3 subscribe/change_mode/unsub with instrumentKeys; Protobuf feed decoder required. | PASS — request JSON, binary little-endian responses; at most 100 instruments per subscribe message. | PASS — JSON tokenList subscribe/unsubscribe, binary little-endian response. | PARTIAL — modes and symbol examples documented; detailed fields/messages link to a dynamic reference. |
| Reconnect behavior | UNKNOWN — quote-stream reconnect contract unavailable. | UNKNOWN — SDK connection exists; recovery/resubscribe guarantees were not established. | PARTIAL — docs state reconnect is needed after server closes a missed-pong connection; general recovery contract not established. | PARTIAL — live subscribe/unsubscribe needs no reconnect; post-disconnect recovery/resubscribe behavior not established. | PARTIAL — support advises ping/retry; deterministic replay/resubscribe behavior is not specified. |
| Heartbeat | UNKNOWN — not established for quote stream. | PASS — server sends ping when idle; standard client pong. | PASS — server ping every 10s; close after 40s without pong. | PASS — client sends text ping every 30s and receives pong. | PARTIAL — support mentions ping checks; exact current V3 heartbeat contract not established. |
| 486 capacity | UNKNOWN — no documented total. | PASS — Full limit 2,000 individual / 1,500 combined; 486 fits one normal connection in Full mode. | PASS — 5,000 instruments per connection; 486 fits one connection and requires five 100-item subscription messages. | PASS — 1,000 token-mode subscriptions per WebSocket; one mode for 486 fits one connection. | PASS — 5,000 unique symbols per API key; 486 fits within the published cap. |
| Rate limit | UNKNOWN — no market-data limits established. | PARTIAL — V3 subscription limits are explicit; a separate market-data REST request rate was not established in reviewed docs. | PARTIAL — connection, total-instrument and per-message caps are explicit; WebSocket message-per-second limit not established. | PARTIAL — WebSocket subscription quota is explicit; REST quote docs say 50 symbols/request at 1 request/sec while the global table says 10 requests/sec for the route. Use the stricter 1/sec if REST is ever considered; resolve the conflict. | PARTIAL — total symbol cap is explicit; WebSocket message/request rate not established. |
| Python support | PARTIAL — guide lists Python samples, but no usable quote protocol schema. | PASS — official Python SDK exposes MarketDataStreamerV3. | PASS — official DhanHQ-py MarketFeed supports V2. | PASS — official SmartAPI docs include Python integration examples/SDK access. | PASS — official FYERS V3 Python SDK/sample is published. |
| Market-data UAT | PARTIAL — guide documents a UAT environment, but does not establish quote-feed availability there. | UNKNOWN — public sandbox documentation currently lists order lifecycle endpoints; market-feed UAT is not established. | UNKNOWN — no public market-feed sandbox/UAT contract found. | UNKNOWN — no market-feed UAT contract found. | UNKNOWN — no market-feed UAT contract found. |
| Documentation completeness | PARTIAL — marketing/API categories exist, critical quote contract remains unavailable. | PARTIAL — core fields/capacity are strong; timestamp unit/semantics, event identity and recovery need validation. | PARTIAL — core packet/mapping/capacity exist; timestamp unit and stream volume semantics block mapping. | PASS — current socket and master specify the core fields and limits; UAT still required for real feed behavior and coverage. | PARTIAL — overall feed exists but critical timestamp, volume, and packet details remain unknown. |
| Overall classification | DOCUMENTATION_BLOCKED | UAT_REQUIRED | DOCUMENTATION_BLOCKED | UAT_REQUIRED | DOCUMENTATION_BLOCKED |

## Provider findings

### Choice FinX — DOCUMENTATION_BLOCKED

The official Choice developer hub now describes a live market feed with tick, depth, and segment streaming and the May 2026 guide lists REST categories such as quote, depth, OHLC, and instruments. This is not a material compatibility change from the existing Phase 4A.5B/4A.5C review: no accessible official quote schema, deterministic NSE/EQ instrument map, source timestamp, volume semantics, subscription quota, or usable UAT feed contract was found. The public guide names a UAT documentation environment, but does not demonstrate that the market-data socket is available there.

Existing investigation: [Phase 4A.5 review](PHASE4A5_CHOICE_FINX_REALTIME.md), [market-data contract](CHOICE_FINX_MARKET_DATA_CONTRACT.md), and [REST/WebSocket comparison](CHOICE_FINX_REST_VS_WEBSOCKET.md). Current official sources: [Choice FinX API docs](https://choiceindia.com/stock-market-trading-api/docs), [May 2026 integration guide](https://choiceindia.com/stock-market-trading-api/files/Choice_OpenAPI_Integration_Guide_May_2026.pdf).

### Upstox V3 — UAT_REQUIRED

- **Instrument identity:** official BOD JSON fields include exchange=NSE, segment=NSE_EQ, instrument_type=EQ, ISIN, trading_symbol, and instrument_key. Matching this project's stored ISIN against NSE_EQ/EQ is deterministic. The current BOD file has not been compared with the frozen 486-symbol file.
- **Quote contract:** V3 provides LTP, last-traded time and last-traded quantity. Full mode adds five depth levels, extended metadata including day-to-date vtt volume, and market OHLC. Treat vtt as a cumulative session counter; convert to guarded increments for the existing candle builder. Preserve the source LTT, not currentTs (which is the provider's receive timestamp).
- **Transport/capacity:** V3 uses Protobuf messages, redirects after authorization, sends an initial market-status message and a market snapshot, and supports ping/pong. Normal account limits are two connections and 2,000 individual Full subscriptions (1,500 combined); 486 symbols fit on one connection in Full mode. No V2 feed is proposed.
- **Python/auth/access:** official Python SDK offers MarketDataStreamerV3. Standard access tokens expire at 03:30 the next day; the official Analytics Token is read-only, listed as free, valid one year, and supports WebSocket market data without a static-IP requirement for that read-only category. That API access statement does not establish exchange redistribution rights or any exchange-level market-data charge.
- **UAT and quality:** Upstox documents a sandbox, but its current sandbox page describes place/modify/cancel order APIs, not a market-data WebSocket sandbox. No live reliability is claimed. Exact LTT unit/time basis, event identity, post-disconnect resubscription, dropped-update handling, and full 486 mapping remain UAT gates.

Sources: [Market Data Feed V3](https://upstox.com/developer/api-documentation/v3/get-market-data-feed/), [V3 authorization](https://upstox.com/developer/api-documentation/get-market-data-feed-authorize-v3/), [instrument master](https://upstox.com/developer/api-documentation/instruments/), [Python streamer](https://upstox.com/developer/api-documentation/streamer-function/), [token lifecycle](https://upstox.com/developer/api-documentation/get-token/), [Analytics Token](https://upstox.com/developer/api-documentation/analytics-token/), [sandbox scope](https://upstox.com/developer/api-documentation/sandbox/).

### DhanHQ V2 — DOCUMENTATION_BLOCKED

- **Instrument identity:** official detailed master documents exchange ID, equity segment, symbol, ISIN, exchange instrument type, exchange-defined SERIES (SEM_SERIES), and security ID. This is enough to define a deterministic NSE/EQ crosswalk in principle, but the frozen 486 were not matched to a downloaded current master.
- **Quote contract:** ticker provides LTP and LTT; Quote/Full packets provide last-traded quantity, LTT, Volume, day OHLC, and (Full) five market-depth levels. The packet table labels LTT “EPOCH” but does not state units/timezone. The V2 WebSocket packet table labels Volume but does not say if it is per-trade or cumulative. REST market-quote documentation calls its volume total traded volume for the day, but that does not conclusively define the separate stream packet. These are critical unknowns.
- **Transport/capacity:** one connection supports 5,000 instruments; each JSON request carries at most 100, so the 486 list needs five subscribe messages. The response is little-endian binary. The server pings every 10 seconds and closes after 40 seconds without pong.
- **Python/auth/access:** official DhanHQ-py documents V2 MarketFeed. A manually generated access token is valid for 24 hours and an active token can be renewed; API key and secret are valid 12 months. Dhan says Data APIs have additional charges; the reviewed docs direct users to the platform for price, so the current amount is unknown.
- **UAT and quality:** no public market-data UAT contract was found. Reconnection/replay, exact timestamp units, stream volume semantics, and full symbol coverage must be resolved before the feed can be mapped to the quote/candle contract.

Sources: [Live Market Feed V2](https://dhanhq.co/docs/v2/live-market-feed/), [instrument master](https://dhanhq.co/docs/v2/instruments/), [V2 annexure](https://dhanhq.co/docs/v2/annexure/), [market quote](https://dhanhq.co/docs/v2/market-quote/), [authentication and token renewal](https://dhanhq.co/docs/v2/authentication/), [official Python SDK MarketFeed](https://dhanhq.co/docs/DhanHQ-py/api/marketfeed/).

### Angel One SmartAPI — UAT_REQUIRED

- **Instrument identity:** official instrument-master documentation describes an importable master. NSE cash tokens use exchange type 1/nse_cm; exact symbols such as RELIANCE-EQ and SBIN-EQ carry an explicit EQ designation. Adapter mapping must match the full explicit EQ symbol and NSE segment, never select merely by root symbol. The current 486 list has not been cross-matched.
- **Quote contract:** WebSocket 2.0 provides LTP, sequence number, exchange timestamp in epoch milliseconds, last-traded quantity, day volume, day OHLC and best-five depth. Day volume is explicitly cumulative (“volume traded for the day”), so the adapter must derive guarded increments. Use exchange timestamp, not local receive time or the documented dummy packet-received-time field.
- **Transport/capacity:** JSON subscriptions map NSE cash to exchangeType 1; binary little-endian responses. Up to three concurrent connections per client code and 1,000 token-mode subscriptions per session; one subscription mode for 486 fits. The client sends ping every 30 seconds. Subscriptions can be changed without reconnecting; disconnect recovery is not specified.
- **Python/auth/access:** official SmartAPI material provides Python examples/SDK access. Login uses a client account code, PIN and TOTP and returns JWT, refresh token and feed token; documented session expiry is midnight, with a token-generation endpoint. WebSocket requires JWT, API key, client code, and feed token. We did not inspect local credential variables or connect.
- **UAT and costs:** no market-data UAT contract or SmartAPI market-data price was established in reviewed official docs. REST FULL quote has up to 50 symbols/request at 1 request/sec, while the global rate table lists 10/sec for that route; this is a published conflict, so 1/sec is the conservative value. WebSocket is preferable for the project's 486-symbol stream.
- **Quality:** sequence and exchange timestamp support de-duplication and ordering, but message loss, resubscription after disconnect, auction/pre-open meaning, and actual market performance need bounded UAT validation.

Sources: [SmartAPI WebSocket and market-data docs](https://smartapi.angelone.in/docs), [instrument master](https://smartapi.angelone.in/docs/Instruments), [authentication/session](https://smartapi.angelone.in/docs/User), [rate limits](https://smartapi.angelone.in/docs/RateLimit).

### FYERS API V3 — DOCUMENTATION_BLOCKED

- Official support docs describe V3 Lite and SymbolUpdate streams, NSE:SYMBOL-EQ examples, a 5,000 unique-symbol cap per API key, and a regularly updated CSV symbol master with instrument type and ISIN. Those facts make FYERS a plausible research candidate.
- The reviewed official documentation does not establish the exact market-data packet schema for a complete source timestamp, whether the timestamp is exchange trade time or provider time, or whether SymbolUpdate volume is cumulative or per-update. It also does not expose message-rate limits or a market-data UAT environment. A generic timestamp example is insufficient for the project's fail-closed source-time gate.
- FYERS support says API market data is free to clients and documents an app/authentication flow; the refresh token is stated to last 15 days. A separate 2026 API app activation article describes static-IP setup for API trading; whether the same requirement applies to a read-only market-data-only application is not established. No exchange-level market-data fee or redistribution right was established.
- Official support advises retry/ping checks; actual reconnect, packet loss, opening/closing, special-session, and high-volatility behavior remain untested.

Sources: [API V3 Data WebSocket](https://support.fyers.in/portal/en/kb/articles/how-can-i-use-the-data-websocket-in-api-v3-to-access-real-time-data), [5000-symbol limit](https://support.fyers.in/portal/en/kb/articles/is-there-a-limit-to-the-number-of-symbols-i-can-track-using-the-data-websocket-in-api-v3), [official symbol-master page](https://support.fyers.in/portal/en/kb/articles/where-can-i-find-the-most-up-to-date-symbols-for-different-exchanges), [API pricing/datafeed access](https://support.fyers.in/portal/en/kb/articles/do-i-need-to-pay-for-datafeeds), [refresh token](https://support.fyers.in/portal/en/kb/articles/what-is-the-function-of-the-refresh-token-in-fyers), [2026 app activation/static IP](https://support.fyers.in/portal/en/kb/articles/how-do-i-activate-the-new-app-for-api-trading-after-april-1-2026).

## NormalizedQuote and candle fit

| Candidate | NormalizedQuote mapping | Existing 1m/5m candle builder |
|---|---|---|
| Upstox V3 | Map instrument_key through the verified master, set NSE/EQ from the master (not the tick), set timestamp from LTT and local receipt separately. Retain vtt as cumulative until delta logic is validated. No documented unique tick ID was found. | Price ticks can feed the builder after source-time validation. vtt cannot be passed as incremental volume. Derive deltas with session reset, duplicate/reconnect handling, and no attribution across an unobserved gap. Do not use Upstox I1 candles as project candles. |
| Angel One | Map by exact NSE cash master row and explicit -EQ trading symbol; use token to recover that row. Map exchange timestamp and local receipt separately; use the sequence field as tick identity. | Price ticks fit after validation. Day volume is cumulative; compute deltas and guard session resets/gaps. Keep project-built candles as source of truth. |
| DhanHQ V2 | Mapping fields exist, but cannot set a trustworthy timezone-aware source timestamp or volume_kind for the stream without unit/semantics clarification. | Blocked until Dhan documents or UAT confirms timestamp units and V2 stream Volume semantics. |
| FYERS V3 | Symbol identity can be explicit, but no reviewed source-time/volume contract suffices to populate the required fields without assumptions. | Blocked until full source timestamp and volume semantics are established. |
| Choice FinX | Required normalized identity and quote fields are not established. | Blocked. |

The candle builder accepts timezone-aware source time, orders observations by source time, handles late observations without mutating already closed candles, and de-duplicates only when a tick_id is supplied. Provider sequence IDs are therefore useful; Upstox's documented contract does not show one. No provider's reliability during normal/open/high-volatility/close/special sessions was empirically measured.

## Shortlist and next phase

| Provider | Status | Reason | Critical unknowns | 486 capacity | WebSocket | NSE/EQ mapping | UAT availability | Next action |
|---|---|---|---|---|---|---|---|---|
| Upstox V3 | UAT_REQUIRED | Explicit NSE_EQ/EQ master, LTP/LTT/LTQ, cumulative day volume, Python V3 feed and capacity are documented. | Actual 486 master coverage; exact LTT epoch interpretation; tick/reconnect behavior; guarded volume deltas. Market-feed sandbox availability is unknown. | 486 fits one Full-mode connection (2,000 per user). | Yes, Protobuf V3. | Deterministic via ISIN + NSE_EQ + EQ; run current master crosswalk. | Sandbox exists for order APIs; market-data UAT not documented. | First choice for Phase 4A.7 documentation-gated, read-only five-symbol UAT/market-data smoke test. Confirm a feed UAT exists before use. |
| Angel One SmartAPI | UAT_REQUIRED | Socket gives exchange timestamp in epoch ms, sequence, cumulative day volume, NSE cash token mapping and 1,000 subscriptions/session. | Current 486 master coverage; market-data UAT; post-disconnect recovery; volume delta gap behavior; REST quote-rate discrepancy if polling is considered. | 486 fits one session in one mode. | Yes, binary WebSocket 2.0. | Deterministic via NSE cash segment + explicit -EQ symbol + token; run current master crosswalk. | No public market-feed UAT established. | Alternate Phase 4A.7 candidate if Upstox feed UAT/access is unavailable. |

No subjective broker ranking is intended. Upstox is listed first because the official instrument master explicitly separates NSE_EQ and instrument_type EQ and the V3 Python feed contract is specific; Angel One is a second technically documented option because it includes a sequence and explicit millisecond exchange time. Neither has passed live feed or 486-symbol coverage tests.

## Proposed Phase 4A.7 — Upstox V3 read-only UAT validation

Do not connect automatically. Before any connection, confirm whether Upstox offers the V3 market-data feed in a sandbox/UAT account. The published sandbox currently documents order APIs only. If no market-data UAT exists, pause for the user's explicit authorization and configured read-only access before attempting even a bounded live market-data connection.

1. Cross-match the frozen 486 universe against a fresh official Upstox BOD master by ISIN and require exchange=NSE, segment=NSE_EQ, instrument_type=EQ; report missing/duplicate mappings and stop on any mismatch.
2. With explicit authorization and a read-only feed scope, subscribe only to RELIANCE, TCS, INFY, HDFCBANK, and SBIN in V3 Full mode. No order, account, portfolio, or broker execution endpoint.
3. Capture a short bounded session; validate explicit source LTT, local receive time, identity/series from the frozen master, LTP, duplicate/out-of-order behavior, heartbeat, disconnect and resubscription.
4. Normalize one symbol's verified ticks and check the existing 1m then 5m builder. Validate cumulative vtt delta and ensure reconnect gaps are not assigned to the wrong candle. Do not compare a provider candle as the project's result.
5. Only after the five-symbol checks pass, obtain explicit authorization for a 486-symbol read-only capacity run. Phase 4B scanner work remains a later phase.

## Safety and preservation

No strategy, signal, risk, cost, backtester, paper-order, fill, position, ledger, realtime provider, dashboard, Phase 3C/3D dataset/result, or runtime configuration was changed. No credentials were read, requested, stored, or written. No provider connection or broker endpoint was attempted. REALTIME_ENABLED=false and LIVE_TRADING=false remain in force.
