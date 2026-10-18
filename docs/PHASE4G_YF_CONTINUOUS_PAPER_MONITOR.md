# Phase 4G-YF Continuous Paper Monitor

## Scope and safety

This is a forward research and paper-execution monitor. It reads Yahoo Finance 1-minute observations for the already frozen 486-symbol universe and passes eligible observations through the existing Phase 4E validator, Phase 4B intraday scanner, and Phase 4C paper execution engine.

```text
PAPER_ONLY=YES
LIVE_TRADING=DISABLED
BROKER=NONE
BROKER_CALLS=0
LIVE_ORDERS=0
```

No broker integration or live-order path is used. The local dashboard only reads these files and never starts acquisition or execution. Yahoo Finance is a third-party research source; NSE authorization is not claimed.

## Session and refresh cycle

The original audited Phase 4G-YF artifacts remain pinned to **2026-09-28**. The runner now accepts a separate `--session-date YYYY-MM-DD` for a new isolated run, but only if that date is explicitly classified `TRADING_DAY` in the local NSE calendar. Unknown weekdays, holidays, and special sessions fail closed. The regular-session data window remains 09:15–15:30 Asia/Kolkata. Each date writes to a date-specific result directory, so the original run is not overwritten. Recheck the official NSE calendar before each run and add a date only when its status is confirmed.

Monitor mode defaults to a 60-second minimum interval between refresh starts. `--refresh-seconds` accepts 30 seconds or more. It never runs overlapping cycles. If a refresh takes longer than the configured interval, the next refresh begins after the current cycle completes; the monitor does not assume Yahoo responds once per minute. Each cycle requests all 486 symbols using the existing 50-symbol batches and at most two concurrent workers by default.

On startup, current-session data is validated and used to warm indicator state. Historical bars returned by a late first response do not generate retroactive strategy signals. Until empirical interval-volume checks pass and warm-up completes, the display remains `WARMING_UP`.

## Timestamp protection and lookahead

Each normalized bar retains `market_timestamp`, `received_timestamp`, request start, and response receipt metadata. A decision can use a bar only when:

```text
received_timestamp <= decision_timestamp
market_timestamp < decision_timestamp
market_timestamp + bar_interval <= decision_timestamp
```

The monitor also checks this proof immediately before order submission. Missing or inconsistent provenance is audited as `REJECTED / TIMESTAMP_PROVENANCE_INSUFFICIENT`. First-observed values win; later revisions are retained only for reconciliation. Bars received after a scheduled decision are not backfilled into that decision.

The automated test covers the required case: a 09:25 bar received at 09:31 is excluded from the 09:30 decision and is eligible only for a later decision.

## Strategy and BUY qualification

The existing Phase 4B strategy settings and scanner are unchanged. The monitor uses the already configured VWAP trend, VWAP pullback, 5-minute 5-minute and 15-minute opening-range breakouts, 5-minute breakout, momentum, mean reversion, and volume breakout strategies. Strategies requiring volume remain unavailable until interval volume semantics are empirically classified as bar volume.

The existing cross-sectional ranker evaluates the current eligible data at its configured scheduled decision times. It produces up to ten ranked candidates after its 21-bar and data-quality requirements. In monitor mode, a strategy signal becomes a BUY eligible for Phase 4C only if:

1. the strategy emitted a genuine closed-bar LONG signal;
2. the symbol appears in an eligible prior top-ten candidate ranking;
3. receipt-time and closed-market-bar proof passes;
4. Phase 4C accepts the signal under its existing risk and portfolio checks.

Other strategy events are written as `REJECTED` with a specific reason. Ranking-only symbols are recorded as `CANDIDATE`; eligible observed symbols outside the top ten are `WATCH`. No technically possible signal is promoted to BUY by the display layer.

The scanner already suppresses repeated active setups, and Phase 4C deduplicates signal identities and blocks additional exposure in an already active symbol. The monitor audit also uses deterministic IDs so a restart does not duplicate audit entries.

## Entry, stop, target, and risk

The strategy signal uses the closed signal bar close as its reference price. The Phase 4C engine receives the order after the signal event is processed, so a bar in the same Yahoo response cannot fill it. The pending paper order can fill only from a subsequent eligible bar under the existing Phase 4C fill rules.

Stops are produced by the existing strategy calculation from the existing ATR stop setting. Targets use the existing `StrategySettings.target_r_multiple`, currently **1.5R**. The example 2R target in the request does not override this supported existing setting. No strategy parameters, risk engine, cost assumptions, or position-sizing rules were changed.

The Phase 4C engine controls quantity, capital, symbol/sector exposure, maximum positions, daily loss halt, drawdown halt, fees, slippage, fills, stop/target exits, and session close. Existing defaults verified by Phase 4G preflight are ₹100,000 capital, 1% risk per trade, 2% daily loss limit, 10% portfolio drawdown limit, five positions, 25% symbol exposure, and 25% sector exposure. Trailing stops remain disabled by the existing paper configuration.

The monitor does not choose which intrabar threshold happened first. It delegates stop/target handling, including any same-candle ambiguity policy, to the existing Phase 4C implementation.

## Audit and outputs

Monitor artifacts use a separate directory and do not overwrite the existing one-shot Phase 4G result:

```text
results/phase4g_yf_20260928_monitor/
  manifest.json
  batch_manifest.json
  canonical/forward_bars_1m.csv
  rankings/candidate_rankings.csv
  signals/signals.csv
  orders/orders.csv
  fills/fills.csv
  positions/open_positions.csv
  paper_ledger.json
  run_state.json
  reports/signal_audit.json
  reports/decision_snapshots.jsonl
  reports/acquisition_report.json
```

`reports/signal_audit.json` records each scheduled symbol ranking state and each actual strategy signal event. BUY records contain decision, market and receipt timestamps, entry reference, strategy stop and target, risk/share, risk/reward, Phase 4C quantity and planned risk, candidate rank/score, paper-order identity/status, and evidence/reason codes. Events that fail qualification include a rejection reason and do not create an order.

## Dashboard

The existing `Phase 4G-YF` dashboard tab now includes a read-only continuous-monitor panel with session state, refresh times, symbol and bar coverage, last market and response times, late-bar counts, signal and position counts, current paper BUY signals, candidates/watchlist states, Phase 4E quality state, and source classification. With no monitor artifacts it displays `NOT STARTED` and `NO QUALIFIED BUY SIGNALS`; it never fabricates prices or starts work.

Start the local dashboard separately with the project's existing `scripts/start_local.sh` command. The monitor and dashboard are independent processes.

## Shutdown and close

Ctrl+C stops the next acquisition cycle, checkpoints the current canonical views and run state, and records `PHASE4G_YF_MONITOR_INTERRUPTED`. It creates no shutdown order. Already submitted paper orders and open positions remain in the monitor's Phase 4G ledger. In-flight bounded provider calls may finish before the worker pool exits; no later polling cycle is started.

At 15:30 the runner stops new signal generation, calls the existing Phase 4C session-close handling, registers the closed observations through the existing Phase 4F adapter, and runs the existing post-close data reconciliation. Reconciliation is descriptive only and cannot enter the signal or order path.

## Testing and limitations

Tests use synthetic fixtures and temporary output directories; they do not call Yahoo and do not modify the persistent Phase 4C/Phase 3D ledgers. Coverage includes late receipt exclusion, strict timestamp proof, ranked-candidate gating, paper orders remaining pending until a later snapshot, risk/position control regressions from the existing Phase 4C suite, minimum refresh cadence, shutdown checkpointing, and dashboard read-only behavior.

The system remains a one-session forward research monitor, not profitability evidence. Yahoo's availability, latency, price revisions, interval-volume meaning, symbol mapping, and data rights are not represented as an NSE-authorized feed. The first refresh may take longer than 60 seconds; the monitor will not backfill late arrivals into earlier decisions. A missing or failed provider response blocks the affected decision or stops the run.

## Run command

Run during the validated NSE session:

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
source .venv/bin/activate

PYTHONPATH=src .venv/bin/python \
scripts/run_phase4g_yf_forward.py \
--mode monitor \
--batch-size 50 \
--max-concurrent-batches 2 \
--refresh-seconds 60
```

For the original date, the default output is `results/phase4g_yf_20260928_monitor/`. A later explicitly selected date uses `results/phase4g_yf_YYYYMMDD_monitor/`. The process is intentionally continuous until market close or Ctrl+C. Do not use `--once` with monitor mode.

For the next validated session, run preflight first, then start the monitor only during that session:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode preflight --session-date 2026-10-05

PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode monitor --session-date 2026-10-05 \
  --batch-size 50 --max-concurrent-batches 2 --refresh-seconds 60
```

`2026-10-05` is listed as a regular session in the local calendar based on NSE's published 2026 equities holiday schedule. Verify the NSE page again before starting the monitor. Yahoo remains a third-party research source: this path is broker-free and paper-only, but it does not guarantee exchange-real-time delivery or establish NSE data authorization.

**This system produces research/paper-trading signals only. It does not place live trades. Yahoo Finance is a third-party research source and is not represented as an authorized NSE feed.**
