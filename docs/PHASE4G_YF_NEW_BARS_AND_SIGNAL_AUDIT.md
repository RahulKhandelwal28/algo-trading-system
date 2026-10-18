# Phase 4G-YF New-Bar and Signal Audit

Audit date: 2026-09-28  
Implementation verification: **PASS**  
Forward data-integrity status: **BLOCKED — Yahoo returned material revisions to the prior canonical bars**

## Scope and safety

This audit hardens Phase 4G-YF bar accounting, timestamp eligibility, duplicate handling, paper-signal deduplication, logging, and presentation. Phase 4B strategy formulas/settings, Phase 4C risk/cost settings, the frozen 486-symbol universe, Phase 3C/3D, Phase 4F/4F.1, broker configuration, and live execution were not changed. The pre-existing `results/phase4g_yf_20260928_monitor/` was read only. Controlled Yahoo acquisition ran in an isolated temporary output directory; no continuous monitor was started.

## 1. Legacy implementation audit

Before hardening, `new_bars_processed` was assigned from `len(combined)` after assembling strategy scanner work. `combined` could include 1-minute inputs and derived 5-minute events; it was not a count of first-seen `(symbol, market_timestamp)` observations. The observed `17,633` values therefore did not prove 17,633 new one-minute market bars.

Legacy processing also kept `processed_event_ids` and `warmup_event_ids` for scanner events, but had no shared per-refresh canonical market-bar store. Its persisted output did not contain a refresh-level row-accounting ledger, so the exact old count cannot be reconstructed from saved records. A read-only audit of the active legacy output found:

- 105,328 canonical CSV rows and 105,328 unique `(symbol, market_timestamp)` keys.
- No duplicate keys in that CSV.
- Zero `refresh_audit.jsonl` records, 110,778 warm-up event IDs, 2,189 processed event IDs, and 12,762 recorded bar revisions.
- The terminal values in the request are preserved as observed logs; their per-refresh market-bar counts cannot be independently verified from the legacy directory.

The old canonical CSV retained both market and receipt timestamps. The run-state revision records also retained receipt timestamps, but the missing refresh audit means the old `new_bars` output cannot prove when each market key first arrived or whether a later Yahoo response changed its payload.

The audited data path is: yfinance batch response → normalized 1-minute rows with source market time and response receipt time → validation and bar eligibility → current new 1-minute observations and completed 5-minute buckets → Phase 4B scanner/ranking → Phase 4C paper risk and execution. Acquisition workers return rows and metadata only; the main process performs all state mutations deterministically.

## 2. Canonical identity and timestamps

`CanonicalBarStore` uses `BarKey = (uppercase_symbol, market_timestamp_in_IST)`. `received_timestamp` is retained provenance and is excluded from both market-bar and signal identity. A later receipt of an identical key is a duplicate; it cannot become a new scanner input just because receipt time changed.

Every stored bar retains `market_timestamp` and `received_timestamp` independently. The normalized `timestamp` alias must agree with `market_timestamp`. Store validation also requires a timezone-aware receipt, a closed 1-minute bar, receipt no earlier than bar close, finite positive OHLC, non-negative volume, and valid OHLC relationships.

## 3. Duplicate and conflict policy

In each refresh, the store reports rows seen, unique response keys, new keys, identical duplicates, conflicting duplicates, invalid rows, late bars, eligible bars, and per-symbol accounting. Price fields use a very small serialization tolerance (`rel_tol=1e-12`, `abs_tol=1e-8`); volume must match exactly.

An identical payload increments `duplicate_identical`; it does not replace the first stored row or receipt. A changed OHLCV payload is recorded with old/new values and first/second receipt times as `DUPLICATE_CONFLICT`. Any conflict or invalid row blocks the entire refresh before store commit, scanner evaluation, paper-order processing, or ledger mutation. The current policy deliberately does not choose a newer Yahoo revision silently.

## 4. Receipt-time eligibility, late data, and decision snapshots

Eligibility is centralized in `bar_eligibility_reason`. A bar can enter a decision snapshot only if:

- `received_timestamp <= decision_timestamp`;
- `market_timestamp < decision_timestamp`;
- the one-minute interval has fully ended by the decision time; and
- the source row is explicitly marked `CLOSED`.

Late rows behind the per-symbol high-water mark or preceding the earlier decision boundary are counted and excluded from strategy inputs. A duplicate never refreshes the stored first-receipt timestamp, so later receipt cannot promote previously unavailable information into an earlier decision.

Before scanner work, the processor records a decision snapshot with its decision timestamp, eligible bar keys/count, and digest. Existing snapshots for the same decision are not rewritten. Late-arriving or future bars cannot retroactively alter an earlier snapshot.

## 5. New-bar accounting and warm-up

`new_market_bars` is the number of distinct keys not already in the canonical store. `duplicate_market_bars` is identical plus conflicting duplicates. `current_session_new_bars` is a separate metric for incremental rows after an established warm-up/high-water mark. `warmup_bars` is separate from current-session new bars. The old combined scanner-event count is retained only as the compatibility alias `new_bars_processed`, now equal to `current_session_new_bars`.

Per-symbol records include rows seen, first-seen new bars, duplicates, last market and receipt timestamps, and fetch-failure details. The dashboard separately shows frozen-universe size, symbols with valid session data, symbols with new bars, and symbols unavailable on the current refresh; a symbol remaining in the universe does not imply it returned fresh data.

## 6. Signal and paper-order audit

Only newly observed, closed, receipt-eligible 1-minute rows and newly completed eligible 5-minute buckets reach strategy evaluation. Signal identity is `(symbol, strategy, signal_bar_market_timestamp, direction)` and excludes receipt time. The scanner and Phase 4C paper engine retain their own setup/signal idempotency checks.

Signal audit records include decision, market, and receipt timestamps; strategy and candidate-rank evidence; timestamp/strategy/risk validity; entry, stop, target, risk/share, reward/share, R:R, quantity, planned risk; rejection reason; and paper order ID/status. Timestamp, ranking, risk/reward, duplicate-signal, and Phase 4C order rejections are machine-readable. No BUY is emitted when data-quality validation blocks the refresh.

For a closed-bar BUY, the signal reference and stop are those produced by the unchanged strategy framework from data available at the decision. The target is frozen in the signal. The configured Phase 4B `target_r_multiple` is **1.5**, so entry 100 and stop 95 imply target 107.50 and R:R 1.5:1. The prompt's 110 / 2R example does not apply to the current configuration; target settings were not changed.

Phase 4C configuration remains capital ₹100,000, 1% risk per trade, five maximum positions, 2% daily loss limit, 10% portfolio drawdown limit, and 25% sector/symbol exposure limits. Phase 4C sizing accounts for stop distance, costs, cash, and available exposure; the synthetic size regression confirms that ₹1,000 risk budget and ₹5/share risk cannot exceed 200 shares before tighter capital/exposure/cost limits. Invalid or non-positive risk/reward is rejected.

Existing pending paper orders are processed against eligible market events first. A new signal is submitted only after all rows in its response have been processed, so that same response cannot fill the new order. Subsequent fills, stop/target exits, costs, and net P&L come from the existing Phase 4C ledger. The dashboard/console distinguish market observation, paper recommendation, and paper position, and render position figures from paper state only.

## 7. Yahoo refresh failures

Yahoo warnings such as “possibly delisted” and “no timezone found” are classified as `DATA_UNAVAILABLE_FOR_THIS_REFRESH`, with symbol, attempt time, error type, and message. They do not remove a symbol from the frozen universe, establish delisting status, or create substitute bars. No alternate provider, forward fill, or synthetic fallback was introduced.

## 8. Refresh audit and performance

The processor writes `reports/refresh_audit.jsonl`, `reports/bar_accounting.jsonl`, conflict/invalid-row logs, decision snapshots, and signal audit rows when a signal is evaluated. Acquisition manifests retain batch status, attempts, warnings, retry counts, request timing, response-processing timing, and hashes. Accepted collection manifests report request/response processing, deduplication, indicator, ranking, strategy, paper execution, and total collection timing separately. No speedup claim is made.

For the controlled refresh that stopped at the conflict gate, measured components were 37.304 s request/batch duration, 26.821 s response processing, and 5.918 s deduplication. Indicator, ranking, strategy, paper-execution, and complete post-decision timing were not measured for that blocked refresh because no strategy or execution work ran.

## 9. Dashboard and monitor presentation

The Phase 4G dashboard reports unique bars, new bars, duplicates, late bars, failed symbols, WATCH/BUY/REJECTED counts, and open paper positions separately. BUY rows include symbol, strategy, entry, stop, target, R:R, quantity, and market/receipt/decision timestamps. Yahoo failure or data-quality block statuses render as blocked. Continuous mode is named `CONTINUOUS PAPER MONITOR`; live trading remains disabled.

## 10. Tests and integrity checks

Verification on the final implementation:

- Full suite: **608 passed, 0 failed** (`PYTHONPATH=src .venv/bin/pytest -q`).
- Compile: **PASS** (`.venv/bin/python -m compileall src scripts tests`).
- CLI help: **PASS**.
- Preflight: **PHASE4G_YF_READY_FOR_FORWARD_COLLECTION**, all 16 checks passed, 486/486 instrument mappings, paper only, zero broker calls, live trading disabled.
- Legacy diagnostic: **LEGACY_COUNT_NOT_PROVABLE_FROM_REFRESH_ARTIFACTS**, read-only.
- Required regression cases cover 10 symbols × 20 bars plus one incremental bar each, the 17,633-overlap/486-new example, deterministic input order, receipt-time late/future bars, conflict fail-closed without ledger mutation, repeated-signal/order deduplication, Yahoo unavailable symbols, Phase 4C sizing/target behavior, empty-refresh audit, and preflight read-only behavior.
- 100/100 protected SHA-256 files (251,462,978 bytes) remained unchanged, covering Phase 3C, Phase 3D, Phase 4B, Phase 4F, Phase 4F.1, and the Phase 3C Kaggle dataset.

## 11. Controlled live-session result

The first sandboxed 486-symbol run returned no bars and generic yfinance “possibly delisted; no timezone found” warnings across all 10 batches; this was recorded as a transient refresh failure, not delisting. A single read-only Yahoo probe for `RELIANCE.NS` succeeded with 236 one-minute rows from 09:15 through 13:10 IST. The subsequently approved, one-shot 486-symbol refresh completed all 10 batches successfully (batch size 50, concurrency 2) and returned 111,490 unique response keys for all 486 symbols.

It was compared with a byte-identical, isolated copy of the legacy canonical CSV (105,328 keys; SHA-256 `2e45e5eb5ed8fc6eecb5381666b5beb67d6fc71d18cf21b05371ebcf15531c0f`). Refresh accounting found:

| Metric | Controlled refresh |
|---|---:|
| Rows seen / unique response keys | 111,490 / 111,490 |
| New keys relative to the seed | 6,162 across all 486 symbols (3–14 per symbol) |
| Duplicate keys | 105,328 |
| Identical duplicates | 104,301 |
| Conflicting duplicates | **1,027** across 188 symbols |
| Invalid rows / late bars | 0 / 4 |
| Current refresh fetch failures | 0 of 486 |
| OHLC-changing conflicts | 643 keys |
| Old zero-volume to new positive-volume conflicts | 1,004 keys |

The OHLC and volume conflict subsets overlap. Since the prior canonical payload cannot be presumed correct and the new payload cannot silently overwrite it, the processor returned `PHASE4G_YF_BLOCKED_DATA_QUALITY` / `DUPLICATE_MARKET_BAR_CONFLICT`. It did not commit the 6,162 staged keys, generate strategy signals, create a paper ledger, or create paper orders. The original monitor output and canonical file were not modified. `signal_audit.jsonl` is absent for this refresh because evaluation stopped before any candidate was evaluated.

The durable, isolated summary and per-symbol/conflict records are in [the controlled audit results](../results/phase4g_yf_20260928_correctness_audit/AUDIT_README.md). The raw acquisition and full diagnostic run remain isolated under `/private/tmp/phase4g_yf_controlled_20260928/`.

## 12. Limitations and current readiness

Yahoo Finance is a third-party research source, not an authorized NSE feed, and can return incomplete or revised intraday bars. The legacy data contains thousands of recorded revisions and zero-volume payloads that conflict with the successful current response. The new first-seen policy correctly blocks this refresh but does not decide which Yahoo version is authoritative.

**Software checks pass; forward paper recommendations remain blocked pending an explicit data reconciliation/re-baselining decision.** Do not resume continuous paper monitoring or trust BUY recommendations from this legacy canonical baseline until its 1-minute Yahoo revisions are reconciled under a documented policy and a controlled refresh passes with zero unexplained conflicts. No profitability claim is made.
