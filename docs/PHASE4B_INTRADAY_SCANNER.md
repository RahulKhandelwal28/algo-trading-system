# Phase 4B Intraday Scanner and Signal Engine

Audit and implementation date: 2026-09-27

## Status

```text
PHASE 4B STATUS: COMPLETE (scanner/replay infrastructure complete; genuine intraday data unavailable)

Intraday data: UNAVAILABLE
Validated source: NONE
1m: UNAVAILABLE
5m: UNAVAILABLE
Universe: 100 default (supports 100 / 200 / 486)
Scanner: IMPLEMENTED, default disabled
Strategies: VWAP trend; VWAP pullback; 5m opening-range breakout (5m and 15m windows); prior completed 5m breakout; momentum; mean reversion; volume breakout
Signals: 0 real-data signals; deterministic unit fixtures only
Synthetic scanner fixture: 2 emitted test signals (1 VWAP trend, 1 momentum)
Orders: 0
Ledger mutations: 0
Live trading: DISABLED
Tests: 411 passed / 0 failed
```

Phase 4B completes the audit and provider-independent scanner infrastructure. The dashboard reports `INTRADAY_DATA_UNAVAILABLE` and `BLOCKED_NO_VALIDATED_INTRADAY_DATA`; no real-data signal report or performance result is produced.

## Data boundary

The audit found no genuine intraday bars. See [PHASE4B_INTRADAY_DATA_AUDIT.md](PHASE4B_INTRADAY_DATA_AUDIT.md) and [phase4b_intraday_dataset_inventory.csv](../results/phase4b_intraday_dataset_inventory.csv). Daily OHLCV is never resampled, interpolated, or used to infer a VWAP, opening range, minute timestamp, or intraday volume. Synthetic values are used only inside tests and are labeled `SYNTHETIC_TEST_DATA`.

The scanner consumes normalized bars through `IntradayDataProvider`; it does not know or select a market-data vendor. An existing realtime candle can cross the boundary through `intraday_bar_from_realtime_candle`, but the caller must provide explicit exchange and series because the current realtime `Candle` model does not carry them. No realtime connection, provider, or dashboard-triggered scanner was added.

## Normalized bar contract

`IntradayBar.timestamp` is the timezone-aware **interval start**. Accepted timestamps are normalized to `Asia/Kolkata` only after parsing an explicit offset. Bars must identify `exchange=NSE` and `series=EQ`, use `1m` or `5m`, align to the 09:15 NSE session anchor, fit wholly inside 09:15–15:30, and pass the existing project `NseTradingCalendar`. The calendar fails closed for holidays, closed sessions, and special sessions whose regular-session semantics are not confirmed.

The canonical CSV reader requires these columns:

```text
symbol,exchange,series,timeframe,timestamp,open,high,low,close,volume,volume_kind,status,source
```

`timestamp` must include an offset; `volume_kind` is one of `BAR`, `INCREMENTAL`, `CUMULATIVE`, `LTQ`, or `UNKNOWN`; `status` is `CLOSED` or `FORMING`; and `source` is required provenance text. Numeric OHLC must be finite and positive and satisfy the OHLC bounds. Volume may be unavailable, otherwise it must be finite and non-negative. VWAP and the volume breakout strategy require valid `BAR` volume; VWAP remains unavailable for that session if volume semantics become unknown. Daily rows fail interval/session validation.

Exact duplicates are counted and ignored; a conflicting duplicate raises `ConflictingDuplicateBar`. Out-of-order bars are counted and retained by the store, but they do not update scanner state. Gaps between observed aligned bars are counted and never filled. Forming bars are ignored for signals. Indicator and signal timestamps refer to the completed interval end, so a closed bar is evaluated only after all of its own data is available.

## Indicators and warmup

The incremental, session-scoped state calculates EMA 9/20/50, Wilder RSI 14, Wilder ATR 14, session VWAP, current/prior 20-bar volume averages, and actual-data 5m/15m opening ranges. Missing intervals remain missing. VWAP is volume-weighted from the typical price of actual bars and requires bar-volume semantics. `indicator_ready=false` until EMA 9/20/50, RSI, ATR, and VWAP are available; indicators reset at the next session and do not carry prior-day values.

## Strategies and signals

All implemented strategies are long-only and evaluate only closed bars:

1. `vwap_trend`: close above session VWAP, EMA 9 above EMA 20, and positive closed-bar momentum.
2. `vwap_pullback`: confirmed fast/slow trend, a prior bar touching the VWAP zone, and a closed-bar reclaim.
3. `orb_5m_5min` and `orb_5m_15min`: breakouts above the completed opening range, confirmed by a later closed 5m bar.
4. `breakout_5m`: close above the high of prior completed 5m bars; the current bar does not set its own threshold.
5. `momentum`: configurable lookback, EMA relationship, and optional RSI threshold.
6. `mean_reversion`: prior oversold/extended conditions plus a bullish closed-bar reversal.
7. `volume_breakout`: confirmed range breakout and current valid bar volume above a configurable multiple of the prior 20 valid bars. It is unavailable when bar volume is unavailable.

Signals carry explicit NSE/EQ identity, side, strategy, timeframe, reference price, analytical stop/target references, reason codes, metadata, and deterministic expiry. They contain no quantity or order identifiers. The signal ID hashes symbol, strategy, timeframe, side, timestamp, and setup ID. A stable symbol/strategy/timeframe/session setup ID latches a continuously valid setup, suppressing repeated same-setup signals until the condition clears. Signal storage is in memory only.

The optional regime gate defaults off. When enabled, it fails closed unless a same-session, timezone-aware `RegimeContext` at or before the bar end explicitly permits a long. No automatic NIFTY/VIX intraday context is available because the repository only has daily index data.

No train/validation/test partition was created because there is no genuine intraday dataset to partition. No parameter optimization or profitability statistic was run.

## Replay and dashboard

`IntradayReplay` streams provider bars in event-time order (interval end), defaults to `1x`, supports `5x` and `max`, and rejects out-of-order provider streams instead of sorting future input. It feeds one bar to the scanner at a time; it does not preload later bars into state. A forming bar does not produce a signal.

The dashboard's `/api/dashboard/intraday-scanner` endpoint is a read-only status payload. The “Intraday Scanner” tab shows source classification, frozen-universe status, last bar, 1m/5m counts, readiness, signal count, health counters, and an empty signal table. Loading the page never starts a provider, replay, scanner, or order path. There are no broker or order controls.

## Safety and current output

`INTRADAY_SCANNER_ENABLED=false`, `REGIME_FILTER_ENABLED=false`, `REALTIME_ENABLED=false`, and `live_trading.enabled=false` are the defaults. Configuration supports frozen universe sizes 100, 200, and 486 without changing the existing universe files. Phase 3C/3D strategies, risk settings, costs, result files, and paper ledgers were not intentionally changed. No broker was contacted; no orders were created; no ledger was mutated. `results/phase4b_intraday_signal_report.csv` is intentionally absent because there is no genuine input data.

The implementation is research infrastructure only. It does not claim strategy profitability or live-market readiness.

## Validation and handoff

Validation commands:

```bash
PYTHONPATH=src .venv/bin/pytest -q
PYTHONPATH=src .venv/bin/python -m compileall src scripts tests
node --check forward/dashboard/app.js
```

The fixed 60-bar synthetic scanner integration fixture emitted 2 analytical test signals: one `vwap_trend` and one `momentum`. This count is not a real-data result or a performance claim. The full suite passed 411 tests; `compileall` and the dashboard JavaScript syntax check also passed.

**Next step:** acquire/receive authorized genuine NSE intraday bars with timestamp and volume semantics and explicit NSE/EQ identity; map them to the normalized schema above; rerun the audit and validation; only then perform chronological signal replay. Phase 4C execution remains separate.
