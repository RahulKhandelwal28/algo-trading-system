# Official NSE historical index downloader

`scripts/download_nse_indices.py` downloads the NIFTY 50 and India VIX history exposed by NSE's official historical pages. It does not use Yahoo Finance, a third-party dataset, a proxy, or generated prices. The downloader is separate from strategy and backtest code.

## Verified NSE request flow

The official page and its served JavaScript were checked on 2026-09-26. They call:

- NIFTY and other historical indices: `/api/historicalOR/indicesHistory?indexType=<INDEX>&from=<DD-MM-YYYY>&to=<DD-MM-YYYY>`
- India VIX: `/api/historicalOR/vixhistory?from=<DD-MM-YYYY>&to=<DD-MM-YYYY>`

The page adds `csv=true` for its download action but receives JSON; it builds the CSV from NSE's published field metadata. This downloader uses the same query fields and normalizes those official JSON rows into the expected CSV headers. The index page's current script limits a query to approximately one year, so requests are split into inclusive chunks of no more than 365 calendar days.

Official pages:

- [Historical Index Data](https://www.nseindia.com/reports-indices-historical-index-data)
- [Historical Data – India VIX](https://www.nseindia.com/reports-indices-historical-vix)

The current NSE page scripts identify `NIFTY 50` as the index type. NSE's published field metadata maps the NIFTY columns to `EOD_TIMESTAMP`, `EOD_OPEN_INDEX_VAL`, `EOD_HIGH_INDEX_VAL`, `EOD_LOW_INDEX_VAL`, `EOD_CLOSE_INDEX_VAL`, `HIT_TRADED_QTY`, and `HIT_TURN_OVER`. The VIX metadata additionally maps previous close, point change, and percentage change. The downloader rejects HTML challenge pages, unexpected JSON structures, and empty chunks. It preserves malformed numeric and OHLC values from NSE and reports them as data-quality warnings. It does not try to get around an NSE access restriction.

## Download

From the project root, with `.venv` active:

```bash
python scripts/download_nse_indices.py \
  --start 2021-01-01 \
  --end 2026-09-26
```

This writes:

- `data/nse_exports/index_NSEI.csv`: `Date,Open,High,Low,Close,Shares Traded,Turnover`
- `data/nse_exports/index_INDIAVIX.csv`: `Date,Open,High,Low,Close,Prev. Close,Change,% Change`
- `data/nse_exports/nse_index_download_manifest.json`

Use `--start 2020-01-01` for the preferred wider range. Use `--index nifty50` or `--index indiavix` to fetch one series. `--timeout`, `--retries`, `--interval-seconds`, and `--chunk-days` configure conservative request behavior; chunks cannot exceed the page's 365-day limit. The default spacing is 1.25 seconds between HTTP requests. Transient timeouts, HTTP 429, and server errors use bounded exponential backoff.

Each successful chunk is atomically merged into its CSV and recorded in the manifest before the next chunk. A later invocation reuses completed chunks only when the output checksum still matches. An interrupted download can therefore resume without requesting already recorded chunks. Values are normalized for CSV formatting only; price levels are neither adjusted nor rounded, and no dates are synthesized.

## Validation notes

The manifest records each official source URL and endpoint, requested and observed range, row count, HTTP status, response content type when available, retries, duplicate rows encountered, validation details, candidate weekday gaps, request chunks, and SHA-256 checksum. Duplicate dates are resolved to one output row and counted. Invalid or non-positive OHLC and malformed numeric cells are preserved as returned (a JSON `null` becomes an empty CSV cell) and flagged as `WARN_DATA_QUALITY`; the downloader does not fill, adjust, or silently discard them. Unexpected index identity, out-of-range rows, malformed response structures, or HTML responses fail the request.

Candidate missing weekdays are reported for review, but the downloader does not fetch or infer a historical exchange-holiday calendar. Those dates can include NSE holidays and are not labelled as confirmed missing trading sessions. When both series are downloaded, their observed trading dates are also compared against each other. No missing dates are filled.
