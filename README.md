# NSE Equity Research and Paper Trading

A modular Python research platform for NSE cash equities. It downloads historical OHLCV through a replaceable provider, screens a configurable Nifty 500 universe, evaluates deterministic equity strategies, simulates conservative next-bar fills and Indian trading costs, and writes reproducible trade, metric, equity, drawdown, Monte Carlo, walk-forward, and HTML report artifacts.

This is research software, not investment advice. A two-month backtest is a small initial experiment and does not establish a durable edge. Never infer future profitability from a backtest.

## Quick start

Requires Python 3.11 or later. From this directory:

    python3 -m venv .venv
    source .venv/bin/activate
    python -m pip install -r requirements.txt
    python scripts/run_backtest.py

By default the run uses the latest two complete months available from the data provider, with an additional indicator warm-up period. It reports the observed period and stores each run under results/. The script runs capital scenarios of ₹25,000, ₹50,000, and ₹100,000; set --capital to run one amount. Set --start-date and --end-date to override dates.

The daily scanner and existing Phase 2/3 cache use Yahoo Finance and may be delayed, incomplete, revised, or changed without notice. Phase 3B imports CSVs exported manually from NSE's official [security history](https://www.nseindia.com/historical/price-and-volume-data-per-security), [historical index](https://www.nseindia.com/reports-indices-historical-index-data), and [India VIX](https://www.nseindia.com/reports-indices-historical-vix) pages. Save one export per symbol under `data/nse_exports/`, using filenames such as `RELIANCE.csv`, `index_NSEI.csv`, and `index_INDIAVIX.csv`. The importer stores normalized files in a separate `data/historical_cache/` directory and does not alter the Phase 2/3 cache. The current constituent list still creates survivorship bias; supply point-in-time constituents to study older periods.

## Data and strategies

The initial universe is fetched from the Nifty 500 constituent CSV and screened on trailing average volume, turnover, price, and available history. Placeholder symbols prefixed `DUMMY` are excluded and reported. Daily OHLC data supports trend following, momentum, previous-day and 20/50-day breakouts, pullback, regime-filtered mean reversion, and four swing holding windows. VWAP includes opening-range and previous-day levels and is only run against intraday bars. Yahoo intraday history has a rolling retention limit and may not support a complete two-month 5-minute test. Raw OHLC is used by default; provider split/dividend fields are audited where present. Invalid candles are counted and excluded; missing candles are reported and never synthesized.

Signals use information through a completed bar and enter at the next bar's open. At each open the engine applies queued fills and gap exits before evaluating the rest of the candle. If a candle reaches both a stop and target, the simulator assumes the stop was first. The cash ledger accounts for entry debits, exit proceeds, and open marked positions. Position sizing uses stop distance and is limited by cash, per-position, gross, sector, daily-loss, and portfolio-drawdown rules. Costs are charged on each execution leg and include configurable brokerage, STT, exchange fees, GST, SEBI fees, stamp duty, DP charges, and slippage. Cost schedules are estimates and must be matched to the trader's broker, segment, and current exchange schedule.

The default exit is a fixed R target. The research run also compares stop-only, partial-profit/trailing, ATR-trailing, and max-holding behavior at reference capital. A separate trend-following sensitivity table compares 1R, 1.5R, 2R, 2.5R, and 3R targets. These are predeclared comparisons, not parameter optimization.

The portfolio drawdown control trips one per-trade risk unit before the configured limit, blocks new entries, and closes remaining positions at observed bar closes. Overnight gaps can still make realized drawdown exceed the configured limit.

Options are intentionally out of scope in this equity-first phase. The platform must not estimate or invent historical options trades.

## Commands

    python scripts/download_data.py --start 2020-01-01 --end 2026-09-25 --source-dir data/nse_exports
    python scripts/run_data_expansion.py --start 2020-01-01 --end 2026-09-25 --cache-dir data/historical_cache
    python scripts/run_data_expansion.py --start 2020-01-01 --end 2026-09-25 --cache-dir data/historical_cache --run-baseline
    python scripts/run_backtest.py --capital 100000 --start-date 2026-07-01 --end-date 2026-09-24
    python scripts/run_phase3_validation.py
    python scripts/run_daily_scan.py
    python scripts/run_daily_scan.py --interval 5m
    python scripts/paper_trade.py
    pytest -q

`download_data.py` imports official NSE security, NIFTY, and India VIX CSV exports. It resumes from valid normalized cache files, merges new date ranges, and writes `historical_download_manifest.json`; the website export step is separate. `run_data_expansion.py` performs data-only checks and writes per-symbol coverage and quality reports. Adding `--run-baseline` enables only the unchanged 11 daily strategy baselines after both the coverage check and official-source provenance check pass; otherwise it records that the baseline was skipped. This path does not run parameter searches. NSE's public Bhavcopy MCP is restricted to informational, non-commercial use; review the terms for website exports and licensed products before other use.

To fetch only the official NIFTY 50 and India VIX history, run `python scripts/download_nse_indices.py --start 2021-01-01 --end 2026-09-26`. It writes the two normalized index CSVs and a resumable download manifest under `data/nse_exports/`. See [docs/NSE_INDEX_DOWNLOADER.md](docs/NSE_INDEX_DOWNLOADER.md) for the verified NSE endpoints, chunking, and validation details.

The official security-wise equity downloader is `python scripts/download_nse_equities.py --start 2020-01-01 --end 2026-09-25`. It uses the project's EQ universe, writes one CSV per symbol, and records a resumable manifest and coverage report. The current NSE endpoint passed HTTP checks but returned non-EQ/mismatched rows in live preflight; see [docs/NSE_EQUITY_DATA_REPORT.md](docs/NSE_EQUITY_DATA_REPORT.md) before treating its output as research data.

Daily scan outputs candidate entries as next-bar/open estimates, rejected symbols and reasons, and a machine-readable CSV. Paper trading records signals and simulated fills only; it does not submit broker orders. Live trading is disabled in configuration and no live order placement is implemented.

## Run artifacts

Each experiment records a config snapshot, data manifest, trades, daily and monthly P&L, equity and drawdown curves, factual strategy metrics/comparison, exit and target sensitivity, capital milestones, rejected signals, Monte Carlo samples, walk-forward slices, and an HTML report. Empty or missing data is reported as unavailable; the program never substitutes invented prices or results. The implementation audit is in [docs/PHASE2_CHECKLIST.md](docs/PHASE2_CHECKLIST.md).

See docs/REFERENCE_REVIEW.md for the external project review and LICENSE for this project's license.

The personal profile has a no-broker Jugaad paper-signal experiment for up to five NSE EQ stocks. It polls quote snapshots, builds approximate sampled 1m/5m bars, and feeds accepted closed bars to the existing deterministic scanner. It is not a tick stream, AI-model output, or exchange-native OHLC; cumulative volume is not synthesized. The result file contains paper candidate levels only; there are no orders, fills, or ledger writes. Run preflight before the next session:

```bash
source .venv/bin/activate
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --preflight
```

At an open NSE session, make one quote check and then start continuous monitoring:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --max-polls 1
PYTHONPATH=src .venv/bin/python scripts/run_jugaad_realtime.py --interval-seconds 15
```

Outputs are written to `results/jugaad_paper/<date>/paper_recommendations.json`. A fresh quote is still required before a setup is marked active. See [docs/PHASE4G_JUGAAD_PAPER_QUICKSTART.md](docs/PHASE4G_JUGAAD_PAPER_QUICKSTART.md) for operating limits and the source-data caveat.

An optional Groww market-data feed is also wired into the same paper-only scanner using `scripts/run_groww_paper.py`. It requires an active Groww Trading API subscription, a locally supplied access token, and Groww's instrument CSV; no Groww order API is called. The output remains rule-based (no AI model is configured), uses approximate sampled-LTP candles, and covers NSE cash equities only. See [docs/PHASE4G_GROWW_PAPER_QUICKSTART.md](docs/PHASE4G_GROWW_PAPER_QUICKSTART.md) and [docs/PHASE4G_GROWW_AI_RECOMMENDATION_ARCHITECTURE.md](docs/PHASE4G_GROWW_AI_RECOMMENDATION_ARCHITECTURE.md).

## Phase 4G-YF forward paper check

Phase 4G-YF is an isolated paper-only Yahoo Finance experiment. It has no broker connection or live order route, and Yahoo is a third-party source rather than an authorized NSE feed. The BTC/USD connectivity check is optional and does not enter the NSE scanner, generate signals, or write a ledger:

    .venv/bin/python scripts/smoke_check_yahoo_crypto.py

For the planned NSE session on 2026-10-05, run `./scripts/check_monday_paper_ready.sh` first. It reports a local preflight only; it does not test Yahoo's NSE data. Follow [docs/PHASE4G_YF_MONDAY_START.md](docs/PHASE4G_YF_MONDAY_START.md) for the saved Yahoo failure history, start command, and official NSE calendar check. The monitor must pass its actual feed, freshness, coverage, and OHLC checks before any paper signal is trusted. The crypto smoke check does not validate NSE data, strategy quality, or profitability.

## Phase 3D forward paper trading

The default licensed-provider Phase 3D workflow reuses the frozen 11-strategy daily baseline and existing risk/cost rules for forward-only paper research. It validates the create-once baseline, local NSE session calendar, exact-date licensed CM EOD equity data, and official NSE NIFTY/VIX exports before signal generation. There is no provider fallback, broker connection, or live order path. The licensed canonical equity dataset is currently absent, so that workflow has not started and fails closed until authorized data is supplied.

A separate **personal paper-only experiment** is also configured in `config/phase3d_personal.yaml`. Corrected V2 uses the previously audited Yahoo-derived local OHLCV cache through 2026-09-25 (third-party market data, not official/licensed NSE equity data), a frozen 486-symbol explicit-EQ universe, and the existing official NSE NIFTY/VIX exports. It filters and records 2,331 cache rows outside the official NIFTY session calendar without editing source files. It does not use Kaggle or TejHQ for the forward equity series. V2 has one processed session with five simulated next-session entry orders pending; it is in progress and has no monthly performance result. V2's manifest identifies the preserved original 490-symbol run as superseded. The daily acquisition adapter retains and validates responses, but rejects sources that omit explicit NSE `Series`; Yahoo chart data is therefore not suitable for automatic daily acquisition. See [docs/PHASE3D_DAILY_PAPER_TRADING.md](docs/PHASE3D_DAILY_PAPER_TRADING.md#separate-personal-one-month-experiment) for acquisition, intake, preflight, and run instructions.

Start the local, read-only research and Phase 3D dashboard from the repository root with `./scripts/start_local.sh`, then open <http://127.0.0.1:8765/>. This starts only the Python dashboard/API service; it does not start a realtime feed or the daily paper trader and does not modify the 1/21-session experiment. The UI includes system/experiment status, forward-paper records, risk and cost configuration, data sources, Phase 3C research reports, and the Phase 4A realtime status view. See [docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md) for routes, safety behavior, tests, and troubleshooting.

After the licensed NSE CM EOD dataset and current official index exports are present, run:

    source .venv/bin/activate
    export PYTHONPATH=src PAPER_TRADING_ENABLED=true PAPER_STARTING_CAPITAL=100000 LIVE_TRADING_ENABLED=false
    python -m nse_research.daily_paper_trader

The default runner uses `data/paper_trading/PHASE3C_BASELINE.json`, stores the SQLite ledger and append-only events under `data/paper_trading/`, and writes date reports under `results/phase3d_forward/`. Historical `results/phase3c_kaggle_20260926_171429/` remains separate. See [docs/PHASE3D_DAILY_PAPER_TRADING.md](docs/PHASE3D_DAILY_PAPER_TRADING.md) for input formats, replay isolation, idempotency, cost records, scheduling, and dashboard startup. `PAPER_TRADING_ENABLED` is opt-in; live trading remains disabled.

## Phase 3 validation

`python scripts/run_phase3_validation.py` runs the robustness study from the local daily cache only. It does not refresh market data. The run folder contains `phase3_validation_report.html` plus strategy, yearly, monthly, regime, concentration, parameter, cost, exit, sizing, portfolio, walk-forward, and Monte Carlo artifacts. A result is labelled out-of-sample only when a complete chronological train/validation/test window is available. See [docs/PHASE3_CHECKLIST.md](docs/PHASE3_CHECKLIST.md) for the current data boundary and findings.
