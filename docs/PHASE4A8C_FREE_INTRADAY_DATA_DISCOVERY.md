# Phase 4A.8C — Free No-Broker Intraday NSE Data Discovery

Review date: 2026-09-27  
Project: Phase 4A read-only realtime market-data foundation

## Executive result

**PHASE 4A.8C STATUS: PARTIAL**

`FREE_NO_BROKER_INTRADAY_DATA_NOT_VERIFIED`

Documentation discovery is complete for the candidates below. No source met the required Track B gate. The closest documented no-broker candidate is Indian API's free-plan service, whose current public documentation describes 5-minute intraday price observations and a separate delayed batch quote endpoint. An API key is required, plan quotas and the batch size are not published in the material reviewed, no `EQ` series is established, and no authorized source sample could be tested. The provider's terms allow free API use but leave the underlying market-data provenance/licensing unresolved for this use. No adapter was added.

The word **legitimate** is used here only for access and licensing evidence that is actually documented. A provider's claim to offer data is not treated as proof of exchange-data entitlement.

## Scope and method

- Reviewed official exchange/provider product pages, API documentation, pricing and terms for Indian API, BharatStock, Accelpix, Nubra, NSE Data & Analytics, and screened additional public API services.
- Reused the Phase 4A.8B source review for 0xramm/Indian-Stock-Market-API; no second quote-polling campaign was started.
- Checked whether provider credential environment variables were present without printing any values. Indian API, BharatStock, Accelpix and Nubra key variables were absent.
- Made one bounded, unauthenticated `GET https://bharatstockapi.com/v1/status`, which the provider documents as a public health endpoint. DNS resolution failed in this environment (`curl` exit 6, HTTP status `000`); this does not establish that BharatStock itself is down.
- Did not call an authenticated quote endpoint, use an API key, open a broker account, connect to a broker, scrape protected NSE pages, bypass an access control, or generate market observations. Authenticated 5-symbol validation and the 100-symbol tests were not run.
- No provider feed was started. No intraday bars, signals, paper orders, or fills were generated.

### Candidate matrix

The capability columns use only `YES`, `NO`, and `UNKNOWN`. The cost/payment column gives documented plan facts. “EQ explicit” means the quote/instrument contract contains an NSE cash-equity identity or equivalent explicit segment identity, not just a familiar ticker. `Status` uses the requested factual status vocabulary.

| Candidate | Free | Broker required | API key | Cost/payment | NSE | EQ explicit | Timestamp | 1m | 5m | WebSocket | 100 symbols | Status |
|---|---:|---:|---:|---|---:|---:|---|---:|---:|---:|---|---|
| Indian API | YES | NO | YES | Free tier; quota varies by plan | YES | UNKNOWN | YES | NO | YES | NO | UNKNOWN | `DOCUMENTATION_BLOCKED` |
| BharatStock | YES | NO | YES | Free tier, 50 requests/day | YES | NO | YES | NO | NO | NO | YES | `DATA_SOURCE_NOT_SUITABLE` |
| NSE Data & Analytics direct | NO | NO | NO | Paid subscription; subscriber FTP/SFTP credentials | YES | UNKNOWN | UNKNOWN | YES | YES | NO | UNKNOWN | `PAID_ONLY` |
| Accelpix Pix APIs | NO | NO | YES | API pricing quoted; Pix Connect starts at ₹1,599/month + GST for 10 symbols | YES | YES | YES | YES | YES | YES | UNKNOWN | `PAID_ONLY` |
| Nubra API | YES | YES | YES | Market data advertised free; broker account required | YES | UNKNOWN | YES | YES | YES | YES | YES | `BROKER_REQUIRED` |
| 0xramm/Indian-Stock-Market-API | UNKNOWN | NO | NO | Public service claimed; availability/data rights unclear | UNKNOWN | NO | NO | NO | NO | NO | UNKNOWN | `NETWORK_BLOCKED` |
| HF Data Library | YES | NO | YES | Free key; expires every 30 days | UNKNOWN | UNKNOWN | YES | YES | YES | UNKNOWN | UNKNOWN | `DATA_SOURCE_NOT_SUITABLE` |
| Business Quant | YES | NO | YES | Free use / free API plan | NO | NO | YES | YES | NO | YES | NO | `DATA_SOURCE_NOT_SUITABLE` |
| API Stocks | YES | NO | YES | $0 Basic tier; 5,000 requests/month | NO | NO | UNKNOWN | YES | UNKNOWN | UNKNOWN | NO | `DATA_SOURCE_NOT_SUITABLE` |
| Drishti | YES | NO | YES | Sandbox: 1,000 one-time credits; paid plans for wider access | NO | NO | NO | NO | NO | NO | NO | `DATA_SOURCE_NOT_SUITABLE` |
| Broker API category (Upstox, Choice, Angel One, Dhan, FYERS, Zerodha) | UNKNOWN | YES | YES | Varies by broker/product | YES | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | `BROKER_REQUIRED` |

## Candidate details

### Indian API — closest documented free/no-broker Track B candidate

- **Source:** [Indian Stock Market API docs](https://dev.indianapi.in/), [plan and access FAQ](https://indianapi.in/indian-stock-market), [terms](https://indianapi.in/legal/terms).
- **Access / account:** The provider says a free plan can be subscribed to and an API key is issued from its dashboard. No broker login is described. All endpoints require `X-API-Key`. The free-tier host is documented as `https://stock.indianapi.in`; the OpenAPI examples on `dev.indianapi.in` use a host that the FAQ says is for Developer-plan access. That host distinction must be confirmed using an entitled key.
- **Cost:** The terms say API use is free; request quota depends on plan and must be checked in the account dashboard. No numeric free-tier daily quota was present in the reviewed FAQ/docs.
- **NSE / EQ:** `nse_stock_batch_live_price` is explicitly an NSE endpoint. The docs do not return a `series` value or point to an authoritative `EQ` instrument master. `/static/all_stocks.json` supplies provider stock IDs, but the reviewed contract does not establish the exchange series for those IDs.
- **LTP / OHLC / volume:** The delayed batch endpoint documents LTP, day OHLC, total traded volume, `timestamp`, `last_trade_qty`, and `last_trade_time`. Its volume field is “total traded volume”; the exact reset/cumulative semantics are not specified. The separate `1D_intraday_data` endpoint returns timestamped `price` points, not intraday OHLCV.
- **Intervals / timestamps:** The docs describe `1D_intraday_data` at 5-minute intervals during the session, updated to 1-minute intervals after market close. They state timestamps are IST and show `timeStamp` values. The batch quote endpoint is described as delayed and its `timestamp` as last-trade time; the delay duration and actual freshness were not established. Documentation examples contain 2024 sample observations and are not current observed market data.
- **WebSocket:** No NSE equity price WebSocket contract was found in the current public API docs.
- **Polling feasibility:** `1D_intraday_data` accepts one `stock_id`, so one full-universe call round would require 100, 200, or 486 requests. If polled every 5 minutes for each of 75 five-minute intervals in a 09:15–15:30 session, this would be 7,500 / 15,000 / 36,450 calls per day. This is only the arithmetic for repeated per-symbol polling; the docs do not say this polling is needed or allowed by the free-tier quota. The batch endpoint accepts an array, but its maximum array length is undocumented, so requests per refresh and 100/200/486-symbol feasibility remain **UNKNOWN**. No request-frequency limit is published in the reviewed pages.
- **Five-symbol / 100-symbol validation:** Not run. No provider key was present and the actual quote payload could not be inspected. Observed: **0/5 symbols**, **0/100 symbols**, **0 observations**. No coverage CSV was created.
- **Terms / provenance:** The provider's terms state free use and allow integration, but also say marketplace users are responsible for third-party API terms. The reviewed API contract did not identify the exchange-data licensor or establish a market-data redistribution/license grant for this feed. This is a legitimacy blocker for use as an NSE market-data source, even for a private scanner, until clarified.
- **Status:** `DOCUMENTATION_BLOCKED`. The documented Track B shape is a candidate, not a pass: no live observation, no proven `EQ`, no published free quota/batch cap, and unclear underlying data rights.

### BharatStock — NSE daily/EOD research, not intraday

- **Source:** [API reference](https://bharatstockapi.com/reference), [terms](https://bharatstockapi.com/terms), [public status endpoint](https://bharatstockapi.com/v1/status).
- **Access / account:** Free API key; no broker required. Data endpoints require `X-API-Key`.
- **Cost / rate:** Free plan is documented at 50 requests per UTC day; `GET /v1/status` is documented as public and unauthenticated.
- **NSE / EQ:** Security metadata explicitly returns `exchange: NSE` and ISIN, but does not return a series field. `EQ` is not established by the quote contract.
- **Price contract:** `/v1/stocks/{ticker}` returns the latest available EOD close/volume with `trade_date`; `/prices` returns daily OHLCV; `/quotes` returns latest close and change only, max 50 symbols per call. The site labels one live display as cached “≤5 min,” but the terms explicitly say the service data is EOD/historical/cached and does not provide or claim realtime, live, or tick data. A response-cache interval does not provide 5-minute market observations.
- **Polling / universe arithmetic:** At 50 tickers per batch call: 100 symbols require 2 requests per refresh; 200 require 4; 486 require 10. With a 50-request daily quota this permits at most 25/12/5 complete refresh rounds if every request is reserved for that quote endpoint. It remains unsuitable for intraday because the documented quote is EOD/cached, not an intraday time series.
- **Freshness / live test:** No quote request was made. One public status request failed DNS resolution locally (`curl` exit 6, HTTP `000`); this is an environment network failure, not a provider outage claim. No market observations were obtained.
- **Terms:** Terms permit internal analytics, algorithmic research, backtesting, and application development subject to tier; prohibit raw re-serving/resale/bulk redistribution and require attribution for public display. They reserve third-party exchange data rights and say the user receives only limited use rights under the tier.
- **Status:** `DATA_SOURCE_NOT_SUITABLE` for intraday paper trading; daily/EOD-only research remains a documented product capability.

### NSE Data & Analytics — official but paid

- **Source:** [NSE realtime and snapshot products](https://www.nseindia.com/static/market-data/real-time-data-subscription), [2026 product tariff](https://www.nseindia.com/static/market-data/products-tariff), [CM snapshot specification](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/Snapshot_MDR_RT_CM_v1.27.pdf), [data sharing policy](https://www.nseindia.com/static/market-data/nse-data-policy).
- **Access / cost:** The product page lists real-time stock-wise, realtime, 1-minute/5-minute snapshot, and delayed products as subscriptions. Snapshot file access uses SFTP and subscriber credentials; the 15-minute delayed product uses FTP. The current tariff page labels the catalogue as paid. No anonymous free public intraday endpoint was documented.
- **Data contract:** NSE is the source. Product-level descriptions establish stock-wise feeds and 1/5-minute snapshot products; the reviewed summaries do not establish the precise project-specific EQ schema or 100-symbol entitlement. Feed details depend on the purchased product.
- **Polling / observed freshness:** No subscription, credential, feed access, or quote test was attempted. No market observations were produced. NSE's policy says use/transmission/display/distribution are governed by the subscriber's agreement; redistribution is controlled by the agreement.
- **Status:** `PAID_ONLY` under the current no-paid-subscription preference.

### Accelpix Pix APIs — licensed vendor product, paid ongoing access

- **Source:** [Pix API reference](https://accelpix.com/pix-api-test/), [product and API access](https://accelpix.com/products/pix-api/), [pricing](https://accelpix.com/pricing/), [terms](https://accelpix.com/legal/), [NSE authorized vendor list](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/List%20of%20Authoized%20Vendors_2.pdf).
- **Access / cost:** API keys are provisioned after registration and entitlement review. Pix API price is quoted and depends on symbols, history, live/history access, and license class. The listed Pix Connect feed begins at ₹1,599/month plus GST for one segment/10 symbols; this is not a published Pix API price. A five-day trial is time-limited and does not satisfy ongoing free access. No broker account is required.
- **Exchange / series:** Accelpix documents NSE cash equity, and NSE's vendor list includes Accelpix with CM service. The instrument master documents `xid=1` as `EQ`, and `inst=EQUITY`; a current entitled master/crosswalk still needs validation for the frozen symbols.
- **Quote/bar fields:** API docs provide LTP (`pr`), `sid=1` for EQ, cumulative session volume (`vol`), per-trade quantity (`qty`), last-trade time (`tm`, Unix epoch), OHLC references, and intraday OHLCV bars with bar datetime. Documented 1/3/5/7/10-minute resolutions exist. The provider markets live WebSocket trade streams; the product page describes source timestamps as second-resolution IST.
- **Scale / freshness:** Pricing and entitlements determine stream capacity. Pix Connect plans list up to 300 symbols on one segment, but Pix API symbol limits/rates are quoted and were not established. No API key was used and no live sample or latency was measured.
- **Terms:** Accelpix identifies itself as an authorized vendor; the NSE vendor list corroborates authorized realtime CM service. Accelpix terms grant a personal/internal-use license during the subscription term, prohibit redistribution, and reserve exchange compliance/KYC conditions.
- **Status:** `PAID_ONLY`. The technical contract is much more complete than the free candidates, but ongoing API access and exact 100-symbol entitlement are not free/verified.

### Nubra — free market-data API behind a broker account

- **Source:** [Nubra API](https://nubra.io/products/api), [REST authentication](https://nubra.io/products/api/docs/rest-api-v3/authentication.html), [market quote schema](https://nubra.io/products/api/docs/rest-api-v3/market-data/market-quotes.html), [market-data FAQ](https://nubra.io/products/api/docs/faq/market_data/index.html).
- **Access / cost:** Nubra advertises market-data APIs as free. Access requires a Nubra account, credentials, and authenticated session (OTP/TOTP and MPIN); Nubra identifies its operator as a SEBI-registered stock broker. It therefore requires the broker account the user explicitly does not want.
- **Data contract:** Docs support NSE instrument masters and `exchange=NSE` stock queries. Quote snapshots contain `ltp`, `ltq`, day `volume`, and a nanosecond timestamp; the FAQ describes exchange-sourced timestamped ticks, live WebSocket, and local 1m/5m tick aggregation. The reviewed stock examples do not expose `series=EQ` by that field name.
- **Scale:** FAQ states regular tick subscriptions have no explicit hard cap; order-book depth has a 10,000-instrument session limit. Subscription modification/unsubscribe is not currently supported. Docs state historical data limit of 60 requests/minute and current live market-data limits in the FAQ. No subscription test was run.
- **Terms / observations:** No account session or data request was made. No live observations or freshness measurements were collected. Data use is subject to Nubra's account/API terms.
- **Status:** `BROKER_REQUIRED`; excluded under the current no-broker preference.

### 0xramm Indian-Stock-Market-API — prior Phase 4A.8B candidate

- **Source:** [current repository README](https://github.com/0xramm/Indian-Stock-Market-API/blob/main/Readme.md), [current route/source](https://github.com/0xramm/Indian-Stock-Market-API/blob/main/src/index.js), and [Phase 4A.8B evaluation](PHASE4A8B_FREE_REALTIME_PROVIDER_EVALUATION.md).
- **Access / cost:** Repository describes a public no-key service; no broker account. The current deployed host was not reachable in the prior bounded test; the present phase did not retry it.
- **Data contract:** The reviewed Worker obtains prices from Yahoo Finance. It does not expose explicit `series=EQ`; the single-quote source time is truncated to a date and the batch timestamp is response-generated rather than a per-symbol quote time. No trustworthy volume semantics or source entitlement for the Yahoo-derived data was established.
- **Freshness / legal / scale:** Prior request got no HTTP response. No 5-symbol sample, 100-symbol capacity, SLA, source freshness, or data rights were established. No WebSocket or 1m/5m OHLCV was found.
- **Status:** `NETWORK_BLOCKED` for live validation; the documented schema/source gaps remain blockers even if the host later responds.

### Other services screened out for NSE intraday prices

| Service | Facts reviewed | Why it does not qualify | Status |
|---|---|---|---|
| HF Data Library | [API docs](https://hfdatalibrary.com/pages/api) document 1-minute Parquet bars, 1/5/15/30-minute downloads, free key, 100 data downloads/minute, 300 general requests/minute, and a public symbol directory. | Reviewed docs/examples do not establish NSE/India coverage, NSE exchange identity, `EQ`, or an India symbol crosswalk. No India quote was observed. | `DATA_SOURCE_NOT_SUITABLE` |
| Business Quant | [Intraday docs](https://businessquant.com/docs/api/quotes-intraday) document 1-minute OHLCV, free API use, multi-ticker requests, and cumulative session volume. | Explicitly limited to US-listed equities and ETFs. | `DATA_SOURCE_NOT_SUITABLE` |
| API Stocks | [Product page](https://apistocks.com/) documents a $0 Basic tier, 5,000 requests/month, 60 requests/minute, API key through RapidAPI, and intraday products. | Product coverage states US markets, not NSE. | `DATA_SOURCE_NOT_SUITABLE` |
| Drishti | [Docs and pricing](https://drishti.manasija.in/docs/pricing) document 1,000 one-time sandbox credits and market-intelligence/event APIs. | Reviewed docs do not provide NSE LTP/1m/5m stock quote endpoints; the described realtime streams concern events, not a verified price feed. | `DATA_SOURCE_NOT_SUITABLE` |
| Broker APIs | Existing Phase 4A.8A/4A.8B work covers Upstox; Choice and other retail broker products are account-authenticated services. | An account/broker login is outside the present preference. No new account was opened or provider contacted. | `BROKER_REQUIRED` |

## Five-symbol gate and observed data

Required symbols: `RELIANCE`, `TCS`, `INFY`, `HDFCBANK`, `SBIN`.

| Measure | Result |
|---|---|
| Authenticated current quote endpoints called | 0 |
| 5-symbol candidates with 5/5 valid NSE-equity observations | 0 |
| Provider quote observations collected | 0 |
| Source/receipt timestamp pairs and latency samples | 0 |
| 100-symbol coverage files created | 0 |
| Intraday candles built | 0 |

The missing keys explain why the gated authenticated endpoints were not tested; they do not imply provider failure. BharatStock's separate public `/v1/status` endpoint was attempted once and failed DNS resolution before an HTTP request could be made. No source's access controls were bypassed.

### Safe local validation commands

After independently obtaining a free-plan key from Indian API, the documented endpoint can be tested from the user's network. First use the free-plan host from the provider FAQ, not a guessed host. Keep the test limited to the five required stock IDs and use the IDs returned by the provider's documented `/static/all_stocks.json` mapping:

```bash
source .venv/bin/activate
python - <<'PY'
import getpass
import json
from urllib.request import Request, urlopen

key = getpass.getpass("Indian API key: ")
request = Request(
    "https://stock.indianapi.in/nse_stock_batch_live_price",
    data=json.dumps({"stock_symbols": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN"]}).encode(),
    headers={"X-API-Key": key, "Content-Type": "application/json"},
    method="POST",
)
with urlopen(request, timeout=8) as response:
    print("http_status:", response.status)
    print("content_type:", response.headers.get("Content-Type"))
    print(response.read().decode())
PY
```

This is a single, bounded quote request. A response must still be checked for all five symbols, timestamp timezone/meaning, actual data delay, explicit series identity, and the provider's free-tier quota/terms before it can be accepted. Do not use a development host unless the key's plan entitles it. For BharatStock's public non-quote status endpoint only:

```bash
curl --connect-timeout 3 --max-time 8 -sS -D - \
  'https://bharatstockapi.com/v1/status'
```

## Track A — Realtime

**Free no-broker realtime source verified: NO.**

No tested, no-broker source passed NSE identity, explicit series, trustworthy source time/freshness, live observations, and usable access rights together. Indian API documents delayed batch quotes, not a realtime WebSocket. Accelpix documents a live feed but ongoing API access is paid/quoted. Nubra advertises free realtime market data but requires the user's broker account. The zero-auth 0xramm service remains network-blocked and its schema/source contract does not pass.

## Track B — Intraday paper trading

**Free no-broker intraday source verified: NO.**

Indian API is the closest documented Track B candidate: 5-minute timestamped price points and delayed NSE batch data are described. It does not yet qualify to back the scanner because `EQ` identity is undocumented, 5/5 responses were not observed, 100-symbol batch/polling capacity and free quota are unknown, source freshness was not measured, and underlying exchange-data rights were not established. Its 5-minute endpoint is price points rather than OHLCV bars; no 1-minute or 5-minute candle series was built. BharatStock is explicitly an EOD/historical/cached API and is not an intraday feed.

Accordingly, the existing `RealtimeDataProvider`, `NormalizedQuote`, `RealtimeEngine`, candle builder, NSE calendar, strategy/risk/order code, and provider selection were not changed. No free provider adapter was created. No data was added to the project from an unvalidated source.

## Dashboard and project safety

The Realtime Data dashboard now displays a static, read-only “Free no-broker source discovery” block with the candidate, documented classification, access status (including the unresolved underlying data rights), universe status, timestamp status, intraday capability, and last validation. It does not contact the candidate or start a feed. `config/phase3d_personal.yaml` still has realtime disabled, provider unset, and recording disabled; `live_trading.enabled` remains false. The dashboard server was not started.

## Integrity and validation

- Before the dashboard/report change, SHA-256 hashes were recorded for all 83 files in Phase 3C and Phase 3D result trees, including Phase 3D manifests and paper-ledger databases.
- The same paths/hashes were checked after the change; Phase 3C and Phase 3D integrity passes.
- Paper ledger changed: **NO**.
- Phase 3C results or Kaggle dataset changed: **NO**.
- Phase 3D results changed: **NO**.
- Signals generated: **0**.
- Orders generated: **0**.
- Live trading: **DISABLED**.
- Requested checks: `PYTHONPATH=src pytest -q`, `python -m compileall src scripts tests`, and `node --check forward/dashboard/app.js`.

## Final decision tree

No Track A or Track B provider passes the requested free/no-broker validation gate. The resulting status is:

```text
FREE_NO_BROKER_INTRADAY_DATA_NOT_VERIFIED
```

Factual paths remain open:

1. Use delayed/public data for scanner interface development, keeping it labelled as delayed and unvalidated. BharatStock is daily/EOD; Indian API requires a bounded authenticated validation and data-rights clarification before any use.
2. Use a broker's free market-data API only if the user later changes the no-broker-account constraint.
3. Use licensed NSE market data if the user later changes the no-paid-subscription constraint.

No path was selected on the user's behalf.

## Sources

- [NSE real-time, snapshot and delayed products](https://www.nseindia.com/static/market-data/real-time-data-subscription)
- [NSE product tariff](https://www.nseindia.com/static/market-data/products-tariff)
- [NSE CM snapshot technical specification](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/Snapshot_MDR_RT_CM_v1.27.pdf)
- [NSE Data Usage and Sharing Policy](https://www.nseindia.com/static/market-data/nse-data-policy)
- [NSE authorized realtime vendor list](https://nsearchives.nseindia.com/web/sites/default/files/inline-files/List%20of%20Authoized%20Vendors_2.pdf)
- [Indian API current OpenAPI docs](https://dev.indianapi.in/), [free-plan FAQ](https://indianapi.in/indian-stock-market), [terms of service](https://indianapi.in/legal/terms)
- [BharatStock API reference](https://bharatstockapi.com/reference), [terms](https://bharatstockapi.com/terms), [status endpoint](https://bharatstockapi.com/v1/status)
- [Accelpix API reference](https://accelpix.com/pix-api-test/), [API product page](https://accelpix.com/products/pix-api/), [pricing](https://accelpix.com/pricing/), [terms](https://accelpix.com/legal/)
- [Nubra API product page](https://nubra.io/products/api), [authentication](https://nubra.io/products/api/docs/rest-api-v3/authentication.html), [market quote schema](https://nubra.io/products/api/docs/rest-api-v3/market-data/market-quotes.html), [market-data FAQ](https://nubra.io/products/api/docs/faq/market_data/index.html)
- [HF Data Library API docs](https://hfdatalibrary.com/pages/api)
- [Business Quant intraday docs](https://businessquant.com/docs/api/quotes-intraday)
- [API Stocks product page](https://apistocks.com/)
- [Drishti pricing/docs](https://drishti.manasija.in/docs/pricing)
- [0xramm source repository](https://github.com/0xramm/Indian-Stock-Market-API)
