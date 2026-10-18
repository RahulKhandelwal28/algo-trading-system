# Phase 3 validation checklist

## Phase 2 audit

The run `results/20260925_234118/` was checked before Phase 3 changes. It covered 1 July–31 August 2026 using 494 eligible equities and ₹100,000 reference capital. Pullback returned +2.60% over 17 trades (PF 1.49); swing 10–20 returned +5.35% over 18 trades (PF 2.57). Both were explicitly low sample. No Phase 2 strategy reached a 90% win rate or 2× capital. Existing Phase 2 CSV, JSON, data, and run manifests remain unchanged.

## Data boundary

The Phase 3 run is `results/phase3_20260926_000037/` and uses only cached daily bars. The observed benchmark period is 21 July 2025–25 September 2026 (432 calendar days, 293 NIFTY sessions), short of the 3-year minimum and 5-year preference. Of 500 current tradable constituent symbols with cached bars, 494 passed the 220-row minimum-history requirement. Six were excluded for shorter history: EMMVEE.NS (217 rows), ICICIAMC.NS (194), MEESHO.NS (201), PINELABS.NS (219), PWL.NS (217), and TENNIND.NS (216). The DUMMYHEG placeholder was excluded; India VIX was unavailable. No missing benchmark sessions were found inside observed symbol spans.

The default EMA200 first becomes valid on 14 May 2026, further shortening the period usable by trend-following and swing rules. Other signals have shorter warm-ups, but their available period also cannot supply the declared 24-month training + 6-month validation + 6-month test window.

## Results and scope

All 11 daily equity strategies were run over the full cached range. This generated 515 closed strategy-trade records across independent single-strategy runs. These are descriptive full-window results, not out-of-sample results. Year rows are partial calendar years (2025 begins on 21 July; 2026 ends on 25 September).

The Phase 2 candidates were retained for one-factor parameter, cost/slippage, exit, and risk-sizing sensitivity. Those checks use the same available window and are not OOS validation. Four union-of-signals portfolios were simulated under the Phase 2 portfolio risk limits. Individual and portfolio thresholds at 1.25×, 1.5×, 1.75×, and 2× were not achieved. No strategy recorded a 90% win rate.

Only pullback was net positive in the Phase 3 full-window baseline (+2.22%, PF 1.12, 43 trades). Swing 10–20, positive in the two-month Phase 2 window, returned −8.64% over 36 available-window trades (PF 0.45). The 2025 partial-year results were positive for several breakout/momentum rules and pullback, while every strategy had negative net P&L in 2026 YTD. Pullback's neighboring ATR/ADX/target settings changed sign; 2× modeled costs also changed its net return negative. All four portfolio combinations lost between 3.10% and 4.74%. These are still short, non-OOS samples and do not qualify any strategy for advancement.

Regime results are thin: all 43 pullback trades occurred in BULL conditions; swing 10–20 lost across the sampled bear regimes and gained modestly in SIDEWAYS. No high-volatility trade regime occurred, and VIX was absent. Exit outcomes varied materially (pullback ranged from −6.82% at 1R to +3.72% with ATR trailing); this does not justify selecting an exit from the same sample. Position-sizing increases did not produce monotonic return improvement and drew down more. Monte Carlo ran zero OOS simulations because the required test sample does not exist.

No complete walk-forward test window exists, so Phase 3's OOS metrics and OOS-only Monte Carlo are marked `INSUFFICIENT DATA`; full-window trades were not substituted. Every strategy's research status is `INSUFFICIENT DATA`. No strategy is ready to proceed to a later research phase.

## Bias and measurement limits

- The universe and industry labels are current snapshots; point-in-time membership, delistings, and historic sector classifications are unavailable.
- Yahoo Finance is not an exchange-certified source. Data can be revised or incomplete. Raw OHLC was used (`auto_adjust: false`); action columns are audited where present, but corporate-action reconstruction is incomplete.
- The cache has no India VIX bars. Volatility rows use annualized 20-session NIFTY realized volatility: low at or below 15%, high at or above 30%, with India VIX ≥25 also defined as high when available.
- Daily OHLC cannot reconstruct intraday ordering. The simulator uses the configured conservative stop-first assumption when stop and target both occur within a bar.
- Cost schedules are modeled estimates. Slippage sensitivity scenarios above the normal configured gate raised that gate to the tested slippage level; the CSV records this choice.

## Implementation and verification

The Phase 3 runner is `scripts/run_phase3_validation.py`. It reads local caches without network refresh, preserves the Phase 2 result folder, and writes an independent timestamped Phase 3 folder. The new reusable helpers cover rolling windows, training-only parameter selection, cost scaling, regime classes, concentration, and milestones. Four breakout lookback features (20/30/50/100) are supported for sensitivity runs. Combined strategy signals share one event-driven portfolio and retain member-strategy attribution.

The complete test suite passes: **48 passed**. No options, live execution, or broker orders were implemented.

## Phase 3B — official history expansion

The official-source import attempt is recorded in `results/data_expansion_20260926_121901/`. No NSE CSV exports were present under `data/nse_exports/`; the importer found 0 of 502 requested symbols, and the data-only report is `NO_DATA`. A request to the NSE Bhavcopy MCP endpoint timed out. The NSE export/import workflow is documented in [DATA_EXPANSION_REPORT.md](DATA_EXPANSION_REPORT.md).

A separate diagnostic audit of the existing Yahoo-derived cache is `results/data_expansion_20260926_121712/`. It still spans only 21 July 2025–25 September 2026, so 0 of 500 equities meet the complete 2021-minimum window and no 24/6/6 walk-forward is possible. The unchanged 11-strategy baseline was not run. Previous Phase 2 and Phase 3 result folders remain unchanged.

The updated complete test suite passes: **59 tests**. The baseline CLI requires a passing coverage check and recorded NSE official CSV provenance before it can run; it performs no parameter search.
