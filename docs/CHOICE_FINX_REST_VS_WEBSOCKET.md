# Phase 4A.5C — Choice FinX REST vs WebSocket Market-Data Review

Review date: 2026-09-27  
**CHOICE FINX REST MARKET DATA STATUS: PARTIAL**

## Scope and safety

This is a documentation-only investigation. No Choice market-data endpoint, login endpoint, or WebSocket was called. No credentials were read or used. No provider, parser, configuration, Phase 4A validation logic, signal, order, position, or ledger code was changed.

## Official sources

- [Choice FinX Developer Hub](https://choiceindia.com/stock-market-trading-api/docs) — describes live market-feed capability at a high level.
- [Choice FinX Open API Integration Guide, May 2026](https://choiceindia.com/stock-market-trading-api/files/Choice_OpenAPI_Integration_Guide_May_2026.pdf) — pp. 11–12 describe authentication modes; p. 16 names market-data categories and UAT/production documentation locations.
- [Choice FinX OpenAPI reference](https://finx.choiceindia.com/api/OpenAPI/Info) — production API documentation URL named by Choice.
- [Choice Interactive Socket reference](https://docs.google.com/document/d/1wIptRZZYr0dgQiqX_FvnFap8KDo-vtG0J2GVa1XMQkg/edit?usp=sharing) — WebSocket reference linked from the Developer Hub; detailed findings are also in [CHOICE_FINX_MARKET_DATA_CONTRACT.md](CHOICE_FINX_MARKET_DATA_CONTRACT.md).

The Integration Guide lists `/quote`, `/depth`, `/ohlc`, and `/instruments` under Market Data and describes them as live quotes, order-book depth, OHLC data, and instrument search. It says full request/response schemas, error codes, and examples are on the developer portal. Those descriptions establish product categories only; they do not establish callable operation schemas.

### OpenAPI inspection and access limits

The rendered OpenAPI UI inspected during Phase 4A.5B showed operations including market status, `ScripDetails`, and historical/chart data. It did not expose operation schemas for `/quote`, `/depth`, `/ohlc`, or `/instruments` in the material available for that review. The `ScripDetails` inputs included `SegmentId` and `Token`, but that is not a reverse symbol-to-token instrument master.

For this follow-up, neither the production OpenAPI documentation URL nor the guide's official UAT documentation URL (`https://uat.jiffy.in/api/OpenAPI/Info`) could be read by the web reader. Read-only fetches of both exact documentation URLs failed before receiving an HTTP response because hostname resolution was unavailable in the review environment. This is not evidence of an NSE/Choice block or of an HTTP error. No underlying OpenAPI JSON/YAML URL was exposed to inspect, so no machine-readable specification or REST endpoint example could be validated here. The guide says a Postman collection is available by request from Choice; it was not obtained.

Accordingly, operation details below are marked **UNKNOWN** where the developer portal schema was not accessible. The findings do not assert that Choice's private or authenticated documentation lacks those details.

## `/instruments`

| Requested field or behavior | Finding |
|---|---|
| Endpoint category | Listed as `/instruments`, described as instrument search. |
| HTTP method and full path | **UNKNOWN** — only the category name is published in the accessible guide. |
| Authentication header or OAuth scope | **UNKNOWN** for this operation. The guide describes API-key and OAuth modes generally. |
| Request parameters/body | **UNKNOWN**. |
| Response schema, field types, errors, examples | **UNKNOWN**. |
| `symbol`, `exchange`, `series` | **UNKNOWN** in `/instruments` response. |
| instrument ID, `ScripCode`, `Token` | **UNKNOWN** as `/instruments` response fields and mapping keys. `ScripCode` occurs in documented order/trade socket events; `Token` is an input to the separate `ScripDetails` operation. Neither proves a `/instruments` mapping. |
| segment | A separate rendered-reference segment master identified NSE Cash as segment ID `1`; whether `/instruments` returns or accepts that value is **UNKNOWN**. Segment identity alone does not prove EQ series. |
| expiry and instrument type | **UNKNOWN** for `/instruments`. |
| Unique NSE Cash EQ mapping | **UNKNOWN**. The available contract cannot deterministically map `RELIANCE`, `TCS`, `INFY`, `HDFCBANK`, or `SBIN` to a Choice identifier while proving `exchange=NSE`, cash segment, and `series=EQ`. |

Do not infer an instrument mapping from a symbol name, an order/trade event, or `ScripDetails(SegmentId, Token)`. Do not convert BE/BZ/SM/ST/ETF/derivative records to EQ.

## `/quote`

| Requested field or behavior | Finding |
|---|---|
| Endpoint category | Listed as `/quote`, described as live quotes. |
| HTTP method and full path | **UNKNOWN**. |
| Authentication/header | High-level modes are documented (OAuth or API key), but the operation's exact header, token scope, and authentication requirements are **UNKNOWN**. |
| Request parameters/body and batch behavior | **UNKNOWN** — no documented symbol/token input, maximum symbols, or batch example was accessible. |
| Response and error schemas/examples | **UNKNOWN**. |
| LTP / last traded price | **UNKNOWN** — no field name or numeric format established. |
| Timestamp and its meaning | **UNKNOWN** — no quote timestamp field or evidence that a timestamp is exchange/source time rather than server response time. |
| Volume / LTQ / volume semantics | **UNKNOWN** — no field or cumulative/incremental meaning established. |
| OHLC, bid/ask, market status | **UNKNOWN** on the quote response. The separate category names do not establish quote fields. |
| Instrument ID, symbol, exchange, series | **UNKNOWN** on the quote response. |

The word “live” in a category description is not a freshness guarantee. Without a source timestamp and observable timestamp semantics, the existing Phase 4A age/future-skew checks cannot validate REST quote freshness. An HTTP response timestamp must not be substituted for exchange/source time.

## `/ohlc` and `/depth`

| API | What is documented | What remains unknown |
|---|---|---|
| `/ohlc` | Category is listed as OHLC data. | HTTP method/full path; historical versus current data; interval/timeframe support; 1m/5m support; forming-candle behavior; timestamp timezone/source semantics; volume semantics; parameters, response/error schema, and examples. It is not established as a replacement for tick streaming. |
| `/depth` | Category is listed as order-book depth. The Developer Hub and WebSocket guide market depth at a high level. | HTTP method/full path; bid/ask field names; depth levels; quantity/order-count fields; timestamp semantics; instrument mapping; request/response/error schemas and examples. |

## Authentication, limits, latency, and scale

The May 2026 guide describes vendor OAuth (authorization code exchanged for access/refresh tokens) and non-auth/API-key mode for direct clients or bring-your-own-key arrangements. It states that the API key is sent using a standard API-key header and is bound to declared static IPs. It does not provide the market-data operation's exact header name, per-operation requirements, quote-specific OAuth scope, or complete token/request example in the accessible guide. The exact contract must come from the OpenAPI spec or Choice-provided integration material.

The guide's threshold of 10 orders per second concerns order strategy registration. It is **not** a quote request limit. Quote rate limits, per-minute quotas, maximum symbols per request, batching, and market-data error responses are **UNKNOWN**.

No quote calls were made, so latency, observed freshness, timeout behavior, and error rates are **UNKNOWN**. For 486 symbols, if requests were one symbol at a time, one full sweep would require 486 requests; the documentation available here does not establish whether calls can be batched or what throughput is permitted. Therefore 486-symbol polling duration and coverage cannot be calculated without inventing undocumented inputs.

## REST vs WebSocket

| Requirement | REST Quote | WebSocket |
|---|---|---|
| LTP | UNKNOWN | UNKNOWN |
| Source timestamp | UNKNOWN | UNKNOWN |
| Volume | UNKNOWN | UNKNOWN |
| NSE/EQ identity | UNKNOWN | UNKNOWN |
| Instrument mapping | UNKNOWN | UNKNOWN |
| Bid/Ask | UNKNOWN | UNKNOWN |
| 486 symbols | UNKNOWN | UNKNOWN |
| Streaming | NO — REST is request/response polling; quote polling behavior is not specified | UNKNOWN — Choice advertises streaming, but the documented socket messages do not define a market quote tick/subscription contract |
| Rate limits | UNKNOWN | UNKNOWN — connection error 429 is documented, but no market-data subscription capacity/rate is given |
| Reconnect | N/A | UNKNOWN |
| Suitable for Phase 4B | UNKNOWN | UNKNOWN |

The WebSocket evidence and its distinction between market-status/order/trade events and quote data are documented in [CHOICE_FINX_MARKET_DATA_CONTRACT.md](CHOICE_FINX_MARKET_DATA_CONTRACT.md). The socket's `TRD_MSG.TradedPrice` and execution quantities are not a documented substitute for `/quote` LTP and market volume.

## Conclusion

**REST is not currently validated as suitable for Phase 4B.** It remains a possible polling candidate, but all contract details needed by the Phase 4A validation path—NSE/EQ instrument identity, deterministic mapping, LTP, source timestamp, volume semantics, freshness, batch limits, and rate limits—remain unknown from accessible official schemas. WebSocket quote suitability also remains unknown; the REST investigation does not cure its missing quote event contract.

**WebSocket still required: UNKNOWN.** A complete REST quote schema plus documented batching/rate limits and source timestamp could establish whether polling is adequate. Those facts are not currently available. No `ChoiceFinXRealtimeProvider`, REST parser, or mocked Choice schema tests were added because there is no official response example to reproduce safely.

### Required material to resume

Obtain from Choice's official developer portal or Open API support:

1. The OpenAPI JSON/YAML or full Swagger schemas for `/instruments`, `/quote`, `/ohlc`, and `/depth`.
2. Exact HTTP methods, complete paths, authentication/header requirements, request parameters/bodies, response and error schemas, and official examples.
3. An authoritative instrument-master example that maps symbol to unique identifier and explicitly identifies exchange, cash segment, and series.
4. Quote source timestamp semantics, volume/LTQ semantics, and freshness guarantees.
5. Quote-specific rate limits, maximum symbols per request, and batching behavior.

After those are available, a mocked parser/validation prototype can be considered before any UAT polling. Production access remains unused.

**LIVE_TRADING_ENABLED = false**
