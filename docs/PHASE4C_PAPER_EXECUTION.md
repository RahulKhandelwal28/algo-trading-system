# Phase 4C — Paper Execution and Risk Engine

## PHASE 4C STATUS

**IMPLEMENTED for deterministic software validation.** The Phase 4C engine accepts normalized Phase 4B signals and bars, applies paper risk controls, records simulated fills and positions, and stores a separate integrity-checked paper ledger.

```text
REAL INTRADAY DATA AVAILABLE = NO
REAL TRADING = NO
BROKER CONNECTION = NO
LIVE TRADING = DISABLED
```

The synthetic replay validates software flow only. Its fills and P&L are not market results or evidence of strategy performance. No return, win-rate, profit-factor, Sharpe, or Sortino result is produced from the fixture.

## PAPER EXECUTION

`PaperExecutionEngine` lives in `src/nse_research/execution/phase4c.py`. It consumes the Phase 4B `IntradaySignal` contract without adding quantity, order, or fill fields to that model. `SignalEnvelope` carries the provider-neutral source label and source metadata at the Phase 4C boundary. The scanner/replay provider and original bar source remain available on orders, positions, trades, and audit events; likely credential keys are filtered from metadata.

The default mode is `PAPER`, but paper execution is disabled unless explicitly enabled. `Phase4CPaperConfig.from_env()` reads the environment and remains disabled by default. The named synthetic test/replay command may opt into PAPER for that run; it does not override `EXECUTION_MODE`, `REALTIME_ENABLED`, `LIVE_TRADING`, or `LIVE_TRADING_ENABLED`. Any `LIVE` mode, live flag, or realtime execution flag rejects entry with `LIVE_EXECUTION_NOT_AVAILABLE_IN_PHASE4C`. It never falls back to PAPER.

Supported starting capital is ₹25,000, ₹50,000, or ₹100,000. The default is ₹100,000. The maximum configured risk per trade is 1% of current equity. There are no shorts, options, futures, margin, leverage, or borrowed funds in Phase 4C.

## RISK ENGINE

Sizing calls the existing `nse_research.risk.controls.position_size` helper, then applies exact whole-share/lot affordability against cash, gross exposure, symbol exposure, sector exposure, entry costs, slippage, and planned loss at the stop. Quantity is reduced to the largest affordable lot; zero quantity rejects the order. Pending orders reserve position capacity and their notional is reserved when admitting later orders.

The default limits are:

| Control | Default |
|---|---:|
| Risk per trade | 1% of current equity |
| Open plus pending position slots | 5 |
| Daily loss | 2% of start-of-session equity |
| Portfolio drawdown | 10% from the preserved high-water equity |
| Symbol exposure | 25% of current equity |
| Sector exposure | 25% of current equity |
| Gross exposure | 100% of equity; no leverage |
| Duplicate symbols | Disallowed |

The replay caller must provide an explicit symbol-to-sector mapping. The engine does not infer a sector from a ticker. Missing sector metadata rejects entry. Exposure caps are checked at admission using then-current marks; subsequent market moves can change marked exposure, but the engine will not add exposure while halted or above the cap.

## ORDER STATE MACHINE

Orders use `CREATED`, `VALIDATED`, `PENDING`, `PARTIALLY_FILLED`, `FILLED`, `REJECTED`, `CANCELLED`, or `EXPIRED`. The checked transition helper rejects invalid transitions. Entry admission records the `CREATED → VALIDATED → PENDING` transition in its audit event. Phase 4C bar/quote simulation is all-or-nothing; it does not invent partial fills or queue priority.

Signals are validated for a stable signal ID, symbol, explicit NSE/EQ identity, timezone-aware timestamp, strategy, timeframe, supported long side, price references, stop/target order, replay-relative age, and expiry. Reprocessing the same signal ID and contents returns the existing order. Reusing that ID with different contents creates a deterministic duplicate-signal rejection.

## POSITION MANAGEMENT

Paper positions maintain quantity, entry and current marks, stop, target, optional ATR trailing stop, sector, source metadata, and realized/unrealized P&L. Long-only positions close once. A closed position cannot be changed again. A caller can submit an explicit analytical exit using a normalized current market context.

## FILL MODEL

- A market entry queued after a completed signal bar fills at the next valid event: the next interval's open for bar replay, or the next valid quote for quote replay. Entry slippage is adverse.
- A buy limit fills only when the bar low touches the requested limit; it fills exactly at that limit, without favorable opening-price improvement.
- Long stop loss triggers when the completed bar low reaches the stop. If the bar opens below the stop, the exit uses the open before adverse slippage.
- A target triggers when the completed bar high reaches the target and fills at the target as a sell limit, with no assumed price improvement.
- If one OHLC bar reaches both stop and target, the engine exits at the stop first. OHLC does not reveal intrabar ordering, and stop-first is the conservative fixed rule.
- ATR trailing levels use the completed bar and indicator state, ratchet only in the favorable direction, and become effective on the next event. The bar that computes a new trailing stop cannot retroactively trigger it.
- With `FORCE_INTRADAY_EXIT=true`, the 15:30 IST square-off uses that session's latest observed mark. If the next session arrives first, the same last same-session mark is used. It never reads a future bar. Pending orders are canceled at session close regardless of square-off configuration.
- Setting `FORCE_INTRADAY_EXIT=false` permits positions to remain open across sessions; the default does not.

Stops are evaluated before targets inside one completed OHLC event. Trailing updates follow those checks; session close is a separate later event. The current Phase 4B scanner emits long entry signals only, so replay does not derive exits from a scanner signal.

## COST MODEL

Phase 4C reuses `nse_research.backtest.costs.CostModel` and the project's `CostSettings`. It does not define a second set of NSE taxes or brokerage rates. The existing cost settings apply to both entry and exit legs. Slippage defaults to the configured basis-point value; a fixed or percentage override is available. `PAPER_SLIPPAGE_VALUE` means rupees per share in `fixed` mode, basis points in `basis_points` mode, and percentage points in `percentage` mode (for example, `0.1` means 0.1%). P&L keeps gross P&L, transaction costs, slippage, total costs, and net P&L separate.

## RISK LIMITS

`MAX_POSITIONS`, `MAX_DAILY_LOSS`, `MAX_PORTFOLIO_DRAWDOWN`, `MAX_SYMBOL_EXPOSURE`, `MAX_SECTOR_EXPOSURE`, `MAX_GROSS_EXPOSURE`, and `RISK_PER_TRADE` are configurable through environment variables. Starting capital is selected with `PAPER_INITIAL_CAPITAL`. Risk inputs are validated when configuration is constructed.

## HALT RULES

Risk states are `ACTIVE`, `DAILY_LOSS_HALT`, `PORTFOLIO_DRAWDOWN_HALT`, `MANUAL_HALT`, and `DATA_QUALITY_HALT`. A daily loss halt blocks new entries and clears at the next session boundary; open positions remain eligible for stop, target, trailing, explicit, and session-close exits. The portfolio high-water mark is persisted and is never reset by a session rollover. Portfolio, manual, and data-quality halts require explicit confirmed recovery; a data-quality halt additionally requires a valid market event after the fault. Risk halts reject queued entry orders but do not suppress position exits.

Invalid, stale, out-of-order, wrong-session, or wrong-identity market data sets the data-quality halt. Replay freshness compares event timestamps, not wall-clock time. Quote freshness defaults to five seconds. Signal expiry is also measured relative to replay time, with a default maximum signal age of sixty seconds.

## LEDGER

The default Phase 4C ledger is:

```text
data/paper_trading/phase4c/ledger.json
```

It is separate from the Phase 3D SQLite paper ledger. Each mutation appends an event containing its sequence, deterministic ID, timestamp, type, linked entities, reason, before/after summaries, payload, previous hash, and event hash. The persisted state is separately hashed. Writes use a temporary file, `fsync`, and atomic replacement. On restart, the event chain, state hash, runtime risk configuration, and state invariants are checked. A corrupt or mismatched ledger raises `PAPER_STATE_CORRUPTED`; the engine does not repair or delete it. Pass `ledger_path=None` only for explicitly in-memory analysis/tests.

## REPLAY

`IntradayPaperReplay` interleaves Phase 4B scanner processing and Phase 4C event processing chronologically. Existing positions and orders see each completed bar before the scanner emits signals from it, preventing a signal from filling on its own completed bar. The next interval can fill it. The replay validates chronological provider ordering and accepts `1x`, `5x`, or `max` speed. Sector metadata is an explicit input mapping.

No daily bars are converted to intraday bars. No missing intervals, prices, or volume are synthesized. The Phase 4C engine has no provider-specific classes or broker execution port.

## SYNTHETIC TESTS

Run the fixture with:

```bash
PYTHONPATH=src python scripts/test_phase4c_paper_execution.py
```

It passes the unchanged Phase 4B 60-bar fixture through the scanner and verifies its two signals (`vwap_trend`, `momentum`) in Phase 4C. The fixture uses synthetic bars labelled `SYNTHETIC_TEST_DATA`, source metadata `SYNTHETIC_EXECUTION_TEST`, and an ambiguous stop/target bar. It defaults to a temporary ledger directory; `--ledger-dir` opts into persisting the synthetic test ledger at a caller-selected location.

## SAFETY

The Phase 4C execution module does not import a broker client or a realtime provider. There is no authentication, broker session, broker request, live-order API, or live-order submission code. The dashboard's `/api/dashboard/phase4c-paper` endpoint only validates and reads the ledger. The “Paper Execution” view has no execution controls and never creates an engine or advances replay.

The synthetic CLI reports software counts and asserts zero live orders and zero broker calls. It does not display synthetic trade profitability. Phase 3C results, Phase 3D results/ledger/configuration, and Phase 4B signal definitions/reports are not changed by the Phase 4C fixture.

## LIMITATIONS

- Genuine intraday data remains unavailable, so real-data replay and forward paper execution were not run.
- Paper fills are deterministic bar/quote simulations, not executable broker fills. Liquidity, queue position, halts, partial fills, and impact beyond configured slippage are not modeled.
- The execution side is long-only and cash equities only. Phase 4B currently provides long entry signals, not a full strategy exit contract.
- Sector coverage must be supplied by the caller; unknown sector data fails closed.
- Existing configured NSE cost rates are reused. Their appropriateness for a future broker/account must be reviewed independently before any later phase.
- This implementation does not establish live readiness or strategy performance.
