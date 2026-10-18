# Data and cost assumptions

## Data source

The initial adapter implements the `HistoricalDataProvider` interface for yfinance NSE equity and index OHLCV. It keeps a local CSV cache and records the provider, package version, requested interval, coverage dates, normalization audit counts, missing session/candle estimates, action fields, and failed symbols. Duplicate timestamps keep the last provider row; missing OHLCV, invalid prices/OHLC, and out-of-session intraday bars are excluded and counted. Timestamps with a timezone are converted to Asia/Kolkata and stored timezone-naive; timezone-naive provider timestamps are assumed to already represent exchange-local time. It does not claim exchange-certified quality. Missing data is skipped rather than synthesized.

Phase 3B adds `NseOfficialHistoryCsvProvider`, which imports per-security CSV exports saved from NSE's official [security history page](https://www.nseindia.com/historical/price-and-volume-data-per-security). NSE also exposes separate CSV downloads for [historical index data](https://www.nseindia.com/reports-indices-historical-index-data) and [India VIX](https://www.nseindia.com/reports-indices-historical-vix). The data-only expansion report was run against the pre-existing Yahoo-derived cache because no NSE export files were staged in this environment. The request to NSE's published Bhavcopy MCP endpoint timed out. See the [NSE historical reports directory](https://www.nseindia.com/static/resources/historical-reports-capital-market-daily-monthly-archives) and [NSE Bhavcopy MCP terms](https://www.nseindia.com/nse-mcp).

The NSE website CSV exports contain unadjusted OHLC and traded quantity, not a complete split/dividend event feed. Index exports may omit volume; the importer stores zero in the required OHLCV volume field for non-traded index series and marks index volume as not applicable in its source limitations. It does not adjust OHLC for splits, bonus issues, rights, dividends, mergers, or delistings. Price changes over 20% without a same-date action in the source are flagged as review candidates only. Do not interpret an absent action field as proof that no corporate action occurred. For bulk EOD/history datasets and commercial use, check the current NSE subscription and data-use terms on its [EOD/Historical Data page](https://www.nseindia.com/static/market-data/eod-historical-data-subscription). NSE's Bhavcopy MCP states that access is for informational, non-commercial purposes and does not grant commercial exploitation rights.

Daily Yahoo history accepts requested start/end dates; the current short history was caused by the local request window and cache, not a date cutoff in the adapter. The backtest runner requested the two-month evaluation period plus the configured 370-calendar-day warm-up, yielding a cache beginning 21 July 2025. The historical import uses a separate cache so previous results remain reproducible.

The current constituent list is read from the Nifty 500 CSV endpoint. This is useful for a broad liquid universe but is not point-in-time historical membership; survivorship bias remains. Symbol normalization only changes case and exchange suffix; no corporate identity transitions are inferred. The historical universe manifest marks symbol history as unknown when no verified security master is present, and `historical_symbol_mapping.csv` is an empty template until verified mappings are supplied. Use point-in-time constituent files, an NSE security master, and a corporate-action feed for serious historical evaluation.

NIFTY 50 and India VIX have separate historical CSV pages on NSE. India VIX is applied to the benchmark regime when available, using only same-date completed bars. If that series is missing, the regime engine falls back to NIFTY realized volatility. The local cache audited in Phase 3B contains no India VIX rows, so the report marks `VIX_UNAVAILABLE`. Benchmark continuity is compared against observed NIFTY sessions; without an independent official NSE session calendar, this check cannot certify that the NIFTY series itself has no missing sessions. The optional `--run-baseline` path requires a passing data-range audit and per-symbol NSE CSV-import provenance; it runs the fixed 11 daily baselines only and does not run parameter searches.

## Modeled costs

Defaults in config.yaml approximate a resident individual Zerodha NSE cash-equity account. The official charge schedule can change; validate every rate against the broker's contract note and current exchange rules before interpreting results.

| Charge | Delivery | Intraday |
|---|---:|---:|
| Brokerage | 0 | 0.03% per order, capped at ₹20 |
| STT | 0.1% on buy and sell | 0.025% on sell |
| NSE transaction charge | 0.00307% per order | 0.00307% per order |
| SEBI turnover fee | ₹10/crore | ₹10/crore |
| GST | 18% on brokerage + transaction + SEBI charges | same |
| Stamp duty | 0.015% on buy | 0.003% on buy |
| DP debit charge | ₹15.34 on delivery sell | none |
| Slippage | 5 bps per side | 5 bps per side |

The provider's charges page lists these brokerage, STT, transaction, GST, SEBI, stamp, and DP amounts: [Zerodha charges](https://zerodha.com/charges). These settings model cash equities only. They do not include account-specific charges, auction/physical settlement, exchange IPFT charges, market impact beyond configured slippage, or impact from order-book depth. Historical adjusted-price bars do not contain historical bid/ask quotes.

## Signal and fill timing

Indicators at bar T use data only through that completed bar. Signals fill at the next bar's open, with adverse slippage. Queued orders are sized at the open using cash and holdings marked at observed opens; that candle's high, low, and close are evaluated after entry. OHLC cannot reveal the order of stop and target touches; when both occur in one bar the backtest exits at the stop. Gaps through a stop are filled at the adverse opening price, plus modeled slippage. Open trades are marked to market with estimated liquidation costs for risk gates and are closed at the period end for final P&L. The portfolio has a 25% single-position cap and 25% sector exposure cap by default.
