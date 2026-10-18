# NSE Equity Data Acquisition Report

Generated: 2026-09-26T10:17:02Z

## Scope and source

- Official page: https://www.nseindia.com/historical/price-and-volume-data-per-security
- Official endpoint: `/api/historicalOR/priceAndVolumeDataPerSecurity`
- Range: 2020-01-01 through 2026-09-25 (inclusive).
- Series requested: `EQ`; the current endpoint has no series query parameter, so non-EQ rows are excluded locally.
- Price basis: NSE values are retained without corporate-action adjustment, gap filling, or synthetic rows.
- Official CSV source fields normalized: Symbol, Series, Date, Open Price, High Price, Low Price, Close Price, and Total Traded Quantity. Comma grouping is removed from numeric values; decimal scale is retained.
- Current project universe has survivorship bias; this run does not provide point-in-time constituents.

## Acquisition summary

- Symbols requested: 499
- Existing universe: 501 rows; eligible EQ symbols: 499.
- Project-universe exclusions: DUMMYHEG: dummy/test symbol; HFCL: universe series is BE, requested series is EQ
- Successful: 0
- Partial: 0
- Unavailable: 0
- Failed: 0
- Not attempted: 499
- Rows: 0
- Per-symbol coverage distribution: {"0 rows": 499, "1 to under 3 years": 0, "3 to under 5 years": 0, "5 years or more": 0, "under 1 year": 0}
- Symbols spanning at least two years between first and last rows: 0 (majority target: 250).
- Overall equity data coverage: no validated equity rows.
- CSV files present: 0

## Data-quality audit

- Duplicate rows encountered: 0
- Invalid OHLC rows: 0
- Nonpositive price fields: 0
- Negative volume rows: 0
- Zero-volume rows: 0
- Malformed numeric fields: 0
- Suspicious discontinuities (40% close/open gap heuristic): 0
- Non-EQ rows excluded: 0
- Discontinuities are warnings only and are retained; they are not automatically classified as corporate actions.
- Missing weekdays are not classified as trading-date gaps because no official exchange-session calendar is joined here.

## NSE endpoint preflight and limitations

The page's served JavaScript calls the endpoint with `symbol`, `from`, and `to`; its CSV action appends `csv=true`. It passes the selected autocomplete `symbol` value, and the current page enforces a maximum range of 365 days. No `series` request parameter is sent. Recorded source-preflight findings:

- Official security-history page returned HTTP 200 HTML. Its served all-historic-report-v2.js?v=25092026 calls /api/historicalOR/priceAndVolumeDataPerSecurity with symbol, from, and to; the CSV action appends csv=true. Page check limits requests to 365 days. No series parameter is sent.
- Official autocomplete returned RELIANCE as symbol=RELIANCE, series=EQ.
- RELIANCE CSV for 24–25 Sep 2026 returned two rows, both series XD, with Open/High/Low/Close 0.40; this does not match the selected EQ listing.
- TCS CSV for 24–25 Sep 2026 returned five rows in series O1, XN, and X9, with prices approximately 0.50–2.15; no EQ row was returned.
- RELIANCE JSON endpoint returned data=[] for 24–25 Sep 2025.
- RELIANCE JSON endpoint returned data=[] for 4–5 Jan 2021.

Bulk acquisition and historical-cache import were held because the source preflight failed integrity checks. No alternate endpoint or access-control workaround was attempted.

NSE's autocomplete currently supplies symbols from its available security catalogue. The endpoint accepts a symbol string, not an ISIN or former-symbol mapping; this inspection therefore cannot establish recovery of delisted, renamed, or merged issues that are absent from the current catalogue. Historical coverage of old symbols must be audited separately when NSE offers an authorized lookup. The project universe itself contains only current names and cannot remove survivorship bias.

## Index prerequisites and Phase 3B gate

NIFTY 50 and India VIX files/manifests are maintained by the separate index downloader. They are not changed by this equity downloader.
- NIFTY: 2020-01-01..2026-09-25; 1673 rows
- India VIX: 2021-01-01..2026-09-25; 1421 rows
- Equity data gate: **BLOCKED** (requires majority-universe multi-year EQ history plus both NIFTY and VIX inputs; source-integrity hold is active).
- Historical cache: not imported because the integrity preflight held bulk acquisition.

## Request/session status

- NSE page session: SUCCESS_PRECHECK
- CAPTCHA or explicit access block: No CAPTCHA or block page observed. Correctly formatted autocomplete and historical requests returned HTTP 200; the ISO-date probe returned HTTP 500 with a short HTML `Error` body.

See `data/nse_exports/nse_equity_download_manifest.json` for per-symbol chunk status and provenance.

## Focused endpoint investigation — 2026-09-26

This follow-up inspected the live official page and its served JavaScript, then replayed a small set of same-origin requests after opening the page and retaining its cookies. It did not use an actual browser DevTools/network capture: local browser automation timed out. The HTTP replay followed the URL and selection flow defined by the current NSE page source.

### Page request and selection flow

- Method and endpoint: `GET https://www.nseindia.com/api/historicalOR/priceAndVolumeDataPerSecurity`.
- The page calls it with `symbol`, `from`, and `to`; each value is passed through JavaScript `encodeURIComponent`. The CSV action adds `csv=true` to this same route. There is no `series`, security ID, or ISIN parameter.
- The served date picker specifies `dd-mm-yyyy`. A `01-09-2026` to `25-09-2026` request succeeded. An ISO `2026-09-25` probe returned HTTP 500, `text/html; charset=utf-8`, body `Error` (5 bytes).
- The page's served JavaScript obtains the selected value from the autocomplete item's `symbol` field and stores it as `searchedSymbolFromAutocomplete`. The autocomplete endpoint is `/api/smart-search/eqEtf?q=...`.
- RELIANCE and TCS autocomplete objects had `series: EQ`, their ticker symbol, company name, equity segment, quote URLs, and zero-valued quote fields. Neither object contained a security ID or ISIN. The historical request sent only the ticker symbol; the autocomplete `series` was not sent separately.
- The page source uses a same-origin XHR GET for JSON and opens the same endpoint with `csv=true` for the equity CSV. Its security-history code assigns `respJSON.data || []` directly to the grid; it does not filter records by series client-side.
- The request replay first opened the official page and then called autocomplete and history in the same cookie jar. Observed NSE cookies were `AKA_A2`, `bm_sz`, and `_abck` (the precise set changed during the session). No application token was found. The replay sent the page as `Referer` and NSE as `Origin`; static page JavaScript adds no custom API token or series header. Cookie/header necessity was not isolated from other session state.
- The page source rejects date ranges whose computed difference exceeds 366 days. No pagination behavior for this report was found in the inspected page code.

### Short-range controlled responses

The requests used `01-09-2026` through `25-09-2026`, with autocomplete first. Each JSON/CSV pair used the same symbol and range in the same NSE page session. JSON rows use `SLBMH_SERIES`; CSV uses `Series`.

| Symbol | Autocomplete series | JSON / CSV rows | Series and row counts in both modes | EQ rows | Response dates |
|---|---:|---:|---|---:|---|
| RELIANCE | EQ | 28 / 28 | XD 7, XN 2, O1 3, X3 8, XO 5, R3 3 | 0 | 2026-09-01..2026-09-25 |
| TCS | EQ | 34 / 34 | XN 9, O1 6, X9 2, XO 12, X7 3, X8 1, X4 1 | 0 | 2026-09-01..2026-09-25 |
| INFY | EQ | 30 / 30 | O1 6, XN 9, XD 2, XO 11, X9 2 | 0 | 2026-09-01..2026-09-25 |
| HDFCBANK | EQ | 37 / 37 | XD 9, XN 13, XO 6, R3 1, O1 1, X3 7 | 0 | 2026-09-01..2026-09-25 |
| SBIN | EQ | 2 / 2 | XO 2 | 0 | 2026-09-09..2026-09-10 |

For this short range, JSON and CSV returned the same row counts and series distribution for all five symbols. Their responses contain no `EQ` records. The `SLBMH_*` field names and the sampled fractional prices are part of the response and were not normalized into project equity history.

### Longer recent-range comparison

For `01-01-2026` through `25-09-2026` (still within the page's stated range limit), autocomplete again returned the requested ticker with `series: EQ`. JSON and CSV both returned non-EQ rows, and their response sets differed:

| Symbol | JSON rows | CSV rows | EQ rows in either mode |
|---|---:|---:|---:|
| RELIANCE | 70 | 193 | 0 |
| TCS | 70 | 284 | 0 |

The CSV response was `text/csv`; the JSON response was `application/json`. Since the counts and series sets diverge over this wider interval, `csv=true` is not just a client-side serialization of the JSON array and the two response modes cannot be treated as interchangeable. On the September short range, however, both modes agreed on the same non-EQ set.

### Finding and gate

Confirmed client-side cause of the importer's zero accepted rows: the official autocomplete returns the requested current symbols as `EQ`, but this endpoint's tested response set contains only non-EQ `SLBMH_SERIES` rows, so the existing EQ integrity guard correctly rejects every row. The ticker lookup and the documented `dd-mm-yyyy` date input worked. The live page code does not filter those rows for the user. The wider-range JSON/CSV divergence is an additional endpoint inconsistency. Public client code does not establish why NSE's server maps this request to these response records; that server-side cause remains unresolved.

The exact wire request from a browser was not captured, so browser reproduction is **NO**; the official page's current source flow was replayed over HTTP. No alternate NSE endpoint that returns verified EQ history was identified in this investigation. Do not change the EQ guard or normalize these responses into equity data. Keep the existing safeguards, and keep acquisition blocked until an NSE-authorized route/export produces explicitly EQ records with valid dates and OHLC values.

- Controlled current-period EQ prerequisite: **FAIL** (five-symbol September sample has zero EQ rows). The 2020-01-01..2026-09-25 RELIANCE/TCS requests were deliberately not sent after that failure.
- 499-symbol acquisition and equity-cache import: **BLOCKED**.
- Project tests: 109 passing / 0 failing were reported as the existing baseline; they were not rerun for this documentation-only follow-up.
- No strategy logic, backtester, risk engine, Phase 3 result, downloader code, manifest, or market-data CSV was modified in this investigation.
