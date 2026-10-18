# Phase 4D — Intraday Paper Session Orchestrator

Review date: 2026-09-27  
Status: **IMPLEMENTED; real replay is blocked until a validated intraday dataset is supplied**

## Safety and current availability

Phase 4D is paper only. It imports no broker adapter, does not authenticate, and never submits a live order. The CLI defaults to `PRECHECK`. `LIVE` always returns `LIVE_EXECUTION_NOT_AVAILABLE`; `REALTIME_PAPER` returns `REALTIME_PROVIDER_NOT_VALIDATED` because no project provider is approved for this path.

Current state:

```text
REAL INTRADAY DATA = UNAVAILABLE
REALTIME PROVIDER = NOT VALIDATED
BROKER = NOT CONNECTED
LIVE_TRADING = DISABLED
```

The existing frozen 100/200/486 universes, Phase 4B scanner/strategies, Phase 4C risk settings and fill model, Phase 3C/3D artifacts, and Phase 3D ledger remain separate and unchanged. Phase 4D does not derive intraday bars from daily data and does not forward-fill, interpolate, smooth, or fabricate market data.

## Data contract and preflight

Replay consumes one normalized CSV. Default location:

```text
data/intraday/canonical_intraday_bars.csv
```

Override it with `--data PATH` or `INTRADAY_DATA_PATH`. Required case-insensitive columns:

```text
symbol,exchange,series,timeframe,timestamp,open,high,low,close,volume,volume_kind,status,source
```

Each record must explicitly say `exchange=NSE`, `series=EQ`, a `1m` or `5m` timeframe, a timezone-aware timestamp, `status=CLOSED`, and a non-empty source. OHLC must be finite, positive, aligned to the NSE session, and internally consistent. The existing NSE calendar is reused; holidays and weekends are rejected, and special sessions are blocked until their hours are represented. Explicit duplicate intervals, malformed rows, and clearly marked synthetic/fixture/mock sources are rejected. Missing symbols are never substituted.

The expected-symbol default is 100. Set `INTRADAY_UNIVERSE_SIZE` or pass `--universe-size 100|200|486`. The existing frozen-universe files are read without re-ranking. Minimum symbol coverage defaults to 95% and can be changed with `MIN_INTRADAY_COVERAGE` or `--min-coverage`. A replay also requires at least one closed event at the 15:30 session boundary so a partial day cannot be labeled complete. Reports include missing bar intervals; gaps are not filled.

`PRECHECK` is read-only. With the current missing dataset it prints `PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE`, zero counts, and zero ledger mutations. A missing ledger is only initialized after a valid dataset passes the data/session gates. If a ledger already exists, Phase 4C's hash chain, snapshot, runtime configuration, accounting state, record timestamps, quantities, prices, risk state, session state, and checkpoint linkage are checked. Invalid or unsupported ledgers are not repaired.

## Session lifecycle and processing

Session IDs are deterministic: `PAPER-NSE-YYYYMMDD`. Session state is stored inside the hash-chained Phase 4C snapshot and uses `NOT_STARTED`, `PRECHECK`, `RUNNING`, `HALTED`, `COMPLETED`, `BLOCKED`, or `FAILED` statuses. A completed date cannot be run twice; replay dates must advance in order.

Bars are validated before the session starts, then sorted once by event close, symbol, timeframe, and bar start. The scanner and Phase 4C engine process one closed bar at a time. Existing Phase 4C positions and orders see a completed bar before Phase 4B evaluates it. The Phase 4C engine remains the only risk, fill, exit, and accounting implementation. When both intervals are present, the 5-minute record is usable only at its close boundary, as enforced by the existing closed-bar contract.

The session lock uses an OS advisory file lock. A competing writer receives `SESSION_LOCKED`; the operating system releases the lock after a process exits or crashes. After each completed event, an atomic checkpoint records the session, dataset checksum, last processed timestamp/event ID, and corresponding Phase 4C ledger event hash. Checkpoint mismatches block replay. On restart, Phase 4B state is rebuilt by scanning the same validated file; committed Phase 4C bar events are skipped by deterministic event ID, and Phase 4C signal/order idempotency prevents duplicate accounting.

The Phase 4C ledger remains at:

```text
data/paper_trading/phase4c/ledger.json
```

It preserves the project's Phase 4C `schema_version: 1` convention and adds the explicit `schema_id: phase4c_v1`, document checksum, creation/update timestamps, top-level accounting projections (`initial_capital`, `cash`, `positions`, `orders`, `trades`, `equity_history`, `risk_state`, `session_state`), and the hash-protected session state. The projections must match the Phase 4C snapshot. Writes use a temporary file, `fsync`, and atomic rename. Phase 3D SQLite ledgers are never opened by the orchestrator.

Only a completed data-backed replay creates:

```text
results/phase4d/PAPER-NSE-YYYYMMDD/
  session.json
  signals.csv
  orders.csv
  fills.csv
  positions.csv
  trades.csv
  risk_events.csv
  equity_curve.csv
  data_quality.json
```

The report records source labels supplied by the dataset, file name, checksum, coverage, session/event counts, paper accounting, risk halts, and runtime/bars-per-second. Missing-data blocks do not create a report. Synthetic software runs do not create results reports or publish strategy return, win-rate, Sharpe, profit-factor, or other performance claims.

## Commands

```bash
# Safe default: read-only preflight
PYTHONPATH=src python scripts/run_phase4d_intraday_paper.py

# Isolated software fixture; defaults to a temporary ledger
PYTHONPATH=src python scripts/run_phase4d_intraday_paper.py --mode SYNTHETIC_TEST

# Historical replay, only after authorized normalized bars are supplied
PYTHONPATH=src python scripts/run_phase4d_intraday_paper.py \
  --mode REPLAY --date 2026-09-28 --data data/intraday/canonical_intraday_bars.csv

# Explicit blocked modes
PYTHONPATH=src python scripts/run_phase4d_intraday_paper.py --mode REALTIME_PAPER
PYTHONPATH=src python scripts/run_phase4d_intraday_paper.py --mode LIVE
```

`SYNTHETIC_TEST` uses the existing deterministic Phase 4B→Phase 4C software fixture and a temporary ledger. An explicit `--ledger` in that mode is accepted only under a temporary directory. It verifies scanner signals, paper orders, fills, a closed trade, and reload without publishing performance metrics.

## Dashboard

The local dashboard adds **Intraday Paper Session** and a read-only `GET /api/dashboard/phase4d-session` view. It reads the ledger, checkpoint, and completed report only. It does not start data acquisition, replay, scanning, or execution. Existing mutation methods remain rejected, and no order, broker, or live-enable controls are added.

## Tests and known limits

Synthetic tests cover missing/malformed/duplicate data, explicit series, coverage, calendar decisions, schema/corruption, atomic writes, preflight non-mutation, full scanner-to-ledger execution in isolated temporary storage, checkpoint mismatch/recovery, idempotent reload after committed order/fill/close/bar events, duplicate sessions, locks, blocked modes, and dashboard read-only behavior.

REPLAY accepts the project's canonical normalized CSV contract and rejects obvious synthetic source labels. The file's provenance fields and checksum are recorded as supplied; the software cannot independently authenticate a provider claim embedded in a local CSV. The operator must supply an authorized source file and preserve its source documentation. There is no live-data adapter in this phase.
