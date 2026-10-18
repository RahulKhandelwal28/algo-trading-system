# Phase 4G-YF — First Session Readiness

Prepared: 2026-09-27  
Target session: **2026-09-28**  
Preparation status: **READY**  
Execution status: **PREPARED — NOT ACTUALLY RUN**

## Readiness result

```text
PHASE4G_YF_STATUS=READY_FOR_2026_09_28
TARGET_SESSION=2026-09-28, 09:15–15:30 Asia/Kolkata
PREFLIGHT=PHASE4G_YF_PREPARED_NOT_RUN; all 16 checks passed
SYMBOLS=486 frozen NSE EQ symbols with explicit SYMBOL.NS mapping
DATA_ACQUIRED=NO
SIGNALS=NOT_RUN
ORDERS=NOT_RUN
FILLS=NOT_RUN
P&L=NOT_RUN
PAPER_ONLY=YES
BROKER_CALLS=0
LIVE_ORDERS=0
```

The preflight imported the installed yfinance package and read local configuration, universe, and calendar files. It made **zero Yahoo/network requests** and **zero ledger mutations**. No target-day bars or market results are claimed.

## Session calendar evidence

The repository calendar classifies `2026-09-28` as `TRADING_DAY`. The entry records the official NSE 2026 equities holiday calendar reviewed on September 27. NSE's calendar states regular equity trading occurs on weekdays except weekends and exchange-declared holidays; September 28 is a Monday and is not in the published 2026 equity holiday list. This session classification is based on that official calendar and should be rechecked on the morning of collection in case NSE publishes a later change. See [NSE Market Timings & Holidays](https://www.nseindia.com/resources/exchange-communication-holidays).

The preflight output records the session status, source reason, and each check at:

`results/phase4g_yf_20260928/reports/preflight.json`

## Source, universe, and processing

- **Data source:** Yahoo Finance through yfinance, classified as `THIRD_PARTY_RESEARCH_SOURCE`; it is not an NSE-authorized feed.
- **Universe:** the existing frozen 486-symbol NSE/EQ roster. Each canonical symbol must have an explicit NSE/EQ/ISIN identity and one-to-one `SYMBOL.NS` Yahoo lookup mapping.
- **Interval and timezone:** native `1m` bars, normalized to `Asia/Kolkata`. Daily data is never converted into intraday bars.
- **Chronology:** ranking uses bars closed by the decision time. Late/stale windows are skipped; future rows fail closed. The 15:30 cutoff prevents post-close data from generating forward signals. Orders from the same response cannot fill against that response.
- **Strategies:** the existing Phase 4B scanner and its configured strategy set are reused without optimization or parameter changes.
- **Risk:** the existing Phase 4C paper engine uses ₹100,000 capital; 1% risk per trade; five positions; 2% daily loss; 10% portfolio drawdown; and 25% symbol and sector exposure caps. Live execution and realtime feeds are disabled.
- **Ledger:** `results/phase4g_yf_20260928/paper_ledger.json`, isolated from persistent Phase 3D and Phase 4C ledgers. Repeated events are idempotently keyed by source event identity.

Per-symbol acquisition failures are recorded. The run blocks on a failed global quality gate, insufficient current snapshot coverage, unverified volume semantics, invalid session, unavailable yfinance, timezone errors, unsafe ledger configuration, or any live-execution setting. It never fabricates or forward-fills bars and never switches providers.

## Commands

The historical September 28 run remains frozen in its own output directory. For a later, explicitly validated NSE session, select the session with `--session-date`. Run preflight first:

```bash
cd /Users/rahul.khandelwal/Documents/personal/algo_trading_system
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py --mode preflight --session-date 2026-10-05
```

On October 5, recheck the official NSE session calendar and rerun preflight before any acquisition. Only if preflight passes, start the paper monitor with:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py --mode monitor --session-date 2026-10-05 --batch-size 50 --max-concurrent-batches 2 --refresh-seconds 60
```

The monitor waits until 09:15 if started before the open and stops forward signal generation at 15:30. A failed preflight prints its exact `PHASE4G_YF_BLOCKED_*` status and exits before requesting Yahoo data. On a pass, startup prints the date, paper-only mode, 486-symbol universe, disabled live execution, and `PREFLIGHT: PASS`. Yahoo remains a third-party research source with no guaranteed freshness or NSE authorization; the run fails closed on unavailable or invalid data.

After the session close, data-only reconciliation is separate:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py --mode reconcile --session-date 2026-10-05 --batch-size 50 --max-concurrent-batches 2
```

Reconciliation cannot call the scanner or mutate the session ledger.

## Artifacts and dashboard

All session files are isolated under `results/phase4g_yf_20260928/`. The existing Phase 4G naming convention uses `manifest.json`, `reports/preflight.json`, `reports/acquisition_report.json`, canonical bar CSVs, candidate-ranking and signal CSVs, paper order/fill/position/risk CSVs, and `paper_ledger.json`; no fake or empty market-result files were created during preparation.

The existing local dashboard remains at [http://127.0.0.1:8765/](http://127.0.0.1:8765/). A separate **Phase 4G-YF** tab and read-only `GET /api/dashboard/phase4g-yf` view display preparation/session status, data quality, candidates, paper results, and zero broker/live counts. Starting or refreshing the dashboard does not start collection or paper execution.

## Validation and protected artifacts

- Full suite: **564 collected; passed**.
- Python compile: `PYTHONPATH=src .venv/bin/python -m compileall src scripts` — **passed**.
- Dashboard JavaScript: `node --check forward/dashboard/app.js` — **passed**.
- CLI help: **passed**.
- CLI preflight: **passed**, with all 16 checks `PASS`, `network_calls=0`, and `ledger_mutations=0`.
- Protected SHA-256 baseline: [protected_artifacts_before.json](../results/phase4g_yf_20260928/protected_artifacts_before.json), 1,166 pre-existing files.
- Post-change audit: [protected_artifact_audit.json](../results/phase4g_yf_20260928/protected_artifact_audit.json) — **PASS**. All 347 pre-existing result files were unchanged; no unexpected modified or missing baseline files were found.

## Prepared versus actually run

**PREPARED:** the configuration, session evidence, frozen universe mapping, isolated ledger path, startup checks, dashboard view, and fail-closed statuses are ready for the target session.

**ACTUALLY RUN:** no Yahoo market-data collection, Phase 4B signal generation, Phase 4C paper orders/fills, reconciliation, or P&L calculation has occurred for September 28. Any of those fields remain `NOT_RUN` until the target session is operated.
