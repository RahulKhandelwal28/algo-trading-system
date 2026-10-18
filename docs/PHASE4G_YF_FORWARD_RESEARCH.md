# Phase 4G-YF — Forward Intraday Paper Research

Status: **ORIGINAL SESSION: 2026-09-28; NEW RUNS REQUIRE AN EXPLICIT SESSION DATE**  
Original target session: **2026-09-28**  
Data source: **Yahoo Finance through yfinance; third-party research source, not an NSE-authorized feed**

## Batching follow-up

The initial preparation recorded here used the legacy serial output namespace. The current runner defaults to a separate `results/phase4g_yf_20260928_batched/` directory, partitions all 486 frozen symbols into 50-symbol batches, and caps acquisition concurrency at two. Receipt-time filtering and current CLI options are documented in [PHASE4G_YF_BATCHING.md](PHASE4G_YF_BATCHING.md). The legacy `results/phase4g_yf_20260928/` artifacts are retained and are not reused by the default commands.

## Scope and safety

This runner acquires genuine Yahoo `.NS` intraday observations and sends closed, Phase 4E-validated bars through the existing Phase 4B scanner and Phase 4C paper engine. It does not call a broker or provide a live-order path. Paper execution uses a temporary ledger at `results/phase4g_yf_20260928/paper_ledger.json`; the persistent Phase 4C ledger and prior Phase 3C/3D or Phase 4B–4F results are outside this experiment.

The experiment requests the repository's frozen 486-symbol universe. A symbol's `.NS` suffix is only a Yahoo lookup key. NSE/EQ/ISIN identity is taken from the repository's NSE security mapping and frozen-universe crosswalk. Missing symbols are listed in `reports/acquisition_report.json`; current-snapshot coverage below 80% blocks processing. Missing bars are reported and never filled.

Yahoo OHLC is requested with `auto_adjust=False`, `back_adjust=False`, and `repair=False`. This means the client does not apply an adjustment. Upstream adjustment treatment is not independently established, so output metadata retains `adjustment_status=UNKNOWN`. Yahoo volume semantics are also not assumed: the runner waits for empirical non-monotonic interval-volume evidence before volume-dependent ranking and signal processing. This is empirical evidence, not a Yahoo data contract. If the evidence remains ambiguous, the run blocks.

## Commands

For any new session, pass its date explicitly. The default remains the original 2026-09-28 experiment for backward compatibility. The selected date must be a `TRADING_DAY` in `data/paper_trading/nse_cash_holidays_2026.csv`; unknown weekdays and special sessions remain blocked. Run only the read-only preflight first:

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode preflight --session-date 2026-10-05
```

Expected status before the target day, if local checks pass: `PHASE4G_YF_PREPARED_NOT_RUN`. Preflight imports yfinance and checks the explicit 486-symbol NSE/EQ identity mapping, paper/live settings, fixed Phase 4B/4C configuration, target-session evidence, and isolated session-ledger path. It does not contact Yahoo or create a ledger. The local calendar records 2026-09-28 as the original validated session and 2026-10-05 as the next regular session based on the official NSE 2026 equity holiday page reviewed 2026-10-02; verify that page again before collection.

During the target session, after confirming it is an NSE session, start and leave the collector running:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode collect --session-date 2026-10-05
```

Use the same explicit date for collection. A passed preflight prints the first-session banner; a failed gate prints its exact `PHASE4G_YF_BLOCKED_*` status and exits before any Yahoo request. Starting before 09:15 waits without requesting bars.

The selected date and 09:15–15:30 Asia/Kolkata session are checked on every run. It uses Yahoo's 1m bars only. Requests are sequential (`threads=False`), use a 15-second timeout, do not retry or change providers, and are separated by a 60-second wait after each refresh completes. With the installed yfinance bulk-download implementation, a 486-symbol call may exceed the nominal poll interval. If response latency makes a scheduled ranking late, that ranking is skipped. Same-response bars are treated as co-observed: signals are submitted only after those bars are processed, delayed signals are suppressed, and no candle in that response can fill a newly submitted order. A response arriving at or after 15:30 is archived and reported but marked unused for forward signals.

After 15:30, the collector closes the paper session and registers the first-observed dataset in an isolated Phase 4F workspace under the result directory. Then run data-only reconciliation:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode reconcile --session-date 2026-10-05
```

Reconciliation compares the session observations with a post-close Yahoo download. It cannot invoke the scanner or paper engine and cannot rewrite signals, fills, positions, or the ledger. One session remains `NOT_RESEARCH_READY` under Phase 4F's existing history gate.

## Ranking and execution rules

The fixed ranking times are 10:00, 10:30, 11:00, 11:30, 12:00, 12:30, 13:00, 13:30, 14:00, 14:30, and 15:00 IST. A rank uses only 1m bars whose end time is no later than its decision time. The deterministic score weights are 30% momentum, 25% relative volume, 20% signed VWAP distance, 15% range expansion, and 10% data-quality/liquidity. Cross-sectional ties share the average percentile. The output describes **top-ranked candidates**, not objectively best stocks.

Only the existing Phase 4B scanner strategies are used. 5m bars are derived only from five actual, consecutive closed 1m bars and are stored separately from the primary 1m bars. The first available bars warm indicators without generating retrospective signals. Orders, fills, position sizing, stop/target handling, risk limits, costs, and slippage are handled by the existing Phase 4C engine. Experiment settings are ₹100,000 initial capital, 1% risk per trade, 2% daily loss limit, 10% drawdown limit, five positions, and 25% symbol/sector exposure limits. Execution remains paper-only.

The one-session report is descriptive and does not calculate Sharpe or claim strategy validation or profitability. It separates the top-ranked-candidate summary from actual strategy signals and paper outcomes. No 20–30-session study starts automatically.

## Output files

The experiment writes only under `results/phase4g_yf_20260928/`:

- `raw/`: timestamped yfinance-returned CSV snapshots and hash log.
- `canonical/forward_bars_1m.csv`: first-observed closed session bars; later revisions are logged, not substituted.
- `canonical/derived_bars_5m.csv`: complete five-bar aggregates from observed 1m data.
- `reports/acquisition_report.json` and `acquisition_snapshots.jsonl`: requested, successful, failed, missing, duplicate, and invalid symbols, plus volume/validation observations.
- `rankings/` and `signals/`: chronological candidate rankings and generated signals.
- `orders/`, `fills/`, `positions/`, `risk/`, `paper_ledger.json`: temporary Phase 4C paper state.
- `reconciliation/`: after-close comparison, kept separate from forward decisions.
- `phase4f_workspace/`: isolated registration, Phase 4E validation, and normalized copy. It does not update the project-wide Phase 4F registry.
- `manifest.json` and `reports/`: preflight, status, descriptive results, and failure details.

The experiment manifest includes source classification, dataset and paper counts, timestamps, raw hashes, Phase 4E status, Phase 4F registration status, and reconciliation status. Provider failure, no usable bars, invalid OHLC/timestamps, unusable volume, excessive gaps, insufficient coverage, or registration failure stop the pipeline without a fallback.

## Implementation references

- yfinance's [`Ticker.history` API](https://ranaroussi.github.io/yfinance/reference/api/yfinance.Ticker.history.html) documents intraday intervals and the adjustment option. The runner explicitly disables client-side adjustment and keeps upstream treatment unknown.
- The yfinance [`history.py` source](https://github.com/ranaroussi/yfinance/blob/main/yfinance/scrapers/history.py) describes `auto_adjust` OHLC behavior.
- Phase 4F's historical research-readiness requirement is unchanged; a one-day forward session is not promoted to a multi-session research dataset.
