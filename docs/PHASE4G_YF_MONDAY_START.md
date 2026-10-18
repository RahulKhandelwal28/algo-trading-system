# Phase 4G-YF Monday paper test

Target session: **Monday, 2026-10-05**  
Mode: **paper only**  
Market data: **Yahoo Finance 1-minute bars; third-party, not an authorized NSE feed**

This is a best-effort **cash-equity paper-engine/data-quality check**, not a promise of exchange-real-time prices. The free `nsepython`/`nselib` website-scraping route is not used: NSE's [Terms of Use](https://www.nseindia.com/static/nse-terms-of-use) prohibit automated collection and website-content use for virtual trading/simulation. Options are not part of this run; no options feed or validated options contract model is configured. See [the provider decision](PHASE4A8B_FREE_REALTIME_PROVIDER_EVALUATION.md#follow-up-nsepython-and-nselib-candidates-2026-10-02).

## 1. Check readiness

From the repository root, run:

```bash
./scripts/check_monday_paper_ready.sh
```

This local-only preflight prints a short status, checks the 486-symbol mapping and safety settings, and writes its report under `results/phase4g_yf_20261005_batched/`. It does not request market data, produce signals, or modify a paper ledger.

This checks local configuration only. It does not test Yahoo's NSE feed or guarantee Monday's data. Proceed to the monitor only when it says:

```text
Result          : LOCAL PREFLIGHT PASS - MARKET DATA UNTESTED
Checks          : 16/16 passed
```

The September 28 saved run did encounter real Yahoo data failures: the batched collector rejected malformed OHLC rows for 10 symbols, and a later monitor cycle ended with `BrokenProcessPool` failures and `PHASE4G_YF_BLOCKED_DATA_QUALITY`. A Yahoo “possibly delisted/no price data” warning for one symbol is not proof that NSE delisted it. The frozen universe remains unchanged; unavailable or invalid Yahoo rows must stay excluded, and the monitor must fail closed when its coverage and quality gates fail.

The script accepts an optional session date, for example `./scripts/check_monday_paper_ready.sh 2026-10-06`.

## 2. Check the Yahoo/NSE sample once

After the market has opened and several one-minute bars have closed, run this bounded source diagnostic once:

```bash
PYTHONPATH=src .venv/bin/python scripts/check_nse_yahoo_feed.py \
  --session-date 2026-10-05
```

It requests at most RELIANCE, TCS, INFY, HDFCBANK, and SBIN through the existing yfinance library, with per-ticker threading disabled, a 15-second per-request timeout, and no application-level retry or polling loop. It checks the project's frozen NSE/EQ mapping, source timestamps, numeric OHLCV, and a configurable 180-second completed-bar age threshold. `PASS_SAMPLE_ONLY` applies only to those sample symbols. It is not an exchange feed certification and does not prove full-universe coverage. If the check returns `DATA_UNAVAILABLE_OR_INVALID`, stop here and do not start the 486-symbol monitor.

This is a bounded Yahoo request, not an IP-block avoidance mechanism. Yahoo does not provide this project a freshness or rate-limit guarantee; do not increase polling or concurrency to work around provider errors.

## 3. Start the paper monitor

After readiness passes and only during the October 5 NSE session, run:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode monitor \
  --session-date 2026-10-05 \
  --batch-size 50 \
  --max-concurrent-batches 2 \
  --refresh-seconds 60
```

The monitor runs the safety preflight again, uses the existing strategy and risk settings, and records simulated paper activity in the isolated date-specific result folder only after its actual Yahoo refresh passes data-quality checks. If it prints a Yahoo warning, blocked status, poor coverage, invalid bars, or worker-pool error, treat that session as blocked; a passing local preflight is not a data pass. No Groww, OpenAlgo, Upstox, other broker, API key, or live order path is used. Stop the monitor with `Ctrl+C`; the runner records a safe shutdown checkpoint.

Recheck the [official NSE holiday calendar](https://www.nseindia.com/resources/exchange-communication-holidays) on the morning of the session. Yahoo can return stale, incomplete, or unavailable NSE data, so a passing preflight does not guarantee that the market-data quality gates will pass after collection starts. The separate BTC/USD smoke check only verifies Yahoo connectivity and parsing; it does not validate NSE data or strategy performance.
