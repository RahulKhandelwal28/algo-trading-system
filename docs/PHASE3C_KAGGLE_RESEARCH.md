# Phase 3C — Kaggle NIFTY 500 Historical Research Validation

Run: `20260926_171429`  
Status: **RESULTS_NOT_ROBUST** — Net P&L changed sign across the non-overlapping OOS test windows for pullback 1/5, swing_10_20 2/5; this status describes outcome instability, not a strategy approval decision.

## 1. Executive summary

This fixed-rule study used the **Kaggle NIFTY 500 historical research dataset** as a separate source
(`provider=kaggle`) and official NSE NIFTY 50 and India VIX CSVs only for
market-regime context. It ran 500 symbols and 11
daily strategies over 5 rolling 24/6/6 windows. The latest
six-month test period is 2025-12-09 to 2026-06-08; the latest
test produced 346 closed trades across the 11 independent
strategy runs. The run is research-only, with `LIVE_TRADING_ENABLED=false`.

No strategy is approved by this experiment. Train, validation and test figures
are kept separate; the test-window table below also retains all five test
windows so deterioration is visible.

## 2. Dataset description

- Name: Kaggle NIFTY 500 historical research dataset
- Provider: `kaggle`
- Dataset slug: `shreyashautomation/nifty500-companies-5-years-stock-market-data`
- Normalized path: `data/kaggle_nifty500/normalized/nifty500_kaggle.csv`
- Date range: 2021-06-09 to 2026-06-09
- Symbols / rows: 500 / 551,643
- Universe label: `CURRENT_CONSTITUENT_HISTORICAL_DATA`; `SURVIVORSHIP_BIAS_PRESENT`
- `Series`: unavailable in source; no `EQ` value was created.
- Adjustedness: unknown. Source OHLC prices were not adjusted during this run.

## 3. Provenance, survivorship and data limitations

The Kaggle dataset has an MIT license as reported by the Kaggle CLI, but its
underlying data source, collection method and version were not available from
the unauthenticated Kaggle metadata response. The dataset must not be described
as official NSE equity data. It has no point-in-time constituent history and
its 500-symbol set overlaps 498 of 501 entries in the current project universe;
the experiment therefore uses the supplied fixed roster and retains
survivorship/current-constituent selection bias.

The source spans approximately five years and ends 2026-06-09, not
the current date. It contains no series or adjusted-close field. It has
2,600 per-symbol missing official
NSE sessions within symbol date spans (median
5, maximum
79). No missing bars were synthesized.

## 4. Exact walk-forward boundaries

Each fold resets to ₹100,000 and starts flat. Bounds are
calendar-month 24/6/6 partitions snapped inward to observed Kaggle sessions.
The first two years provide the initial train period; the five six-month test
periods are chronological and non-overlapping. The latest period ends at
2026-06-08; the 2026-06-09 source observation falls after
that exact six-month boundary and is not included in the final fold.

| Fold | Train | Validation | Test |
|---|---|---|---|
| 1 | 2021-06-09–2023-06-08 (497) | 2023-06-09–2023-12-08 (124) | 2023-12-11–2024-06-07 (120) |
| 2 | 2021-12-09–2023-12-08 (496) | 2023-12-11–2024-06-07 (120) | 2024-06-10–2024-12-06 (124) |
| 3 | 2022-06-09–2024-06-07 (492) | 2024-06-10–2024-12-06 (124) | 2024-12-09–2025-06-06 (123) |
| 4 | 2022-12-09–2024-12-06 (491) | 2024-12-09–2025-06-06 (123) | 2025-06-09–2025-12-08 (126) |
| 5 | 2023-06-09–2025-06-06 (491) | 2025-06-09–2025-12-08 (126) | 2025-12-09–2026-06-08 (120) |

The train/validation/test session sets are disjoint within each fold. Rolling
train and validation samples overlap across folds by design; test periods do
not. Full date/session details are in `walk_forward_boundaries.json`.

## 5. Strategy methodology and lookahead audit

The study used the existing fixed strategy objects, default parameters, exit
mode `fixed_r`, event-driven `BacktestEngine`, and
next-bar-open fills. The enabled daily rules were Trend (`trend_following`),
Momentum, Breakout 20-day, Breakout 50-day, Previous-day breakout, Pullback,
Mean reversion, Swing 1–3, Swing 3–5, Swing 5–10, and Swing 10–20. No daily
rule was unavailable. VWAP intraday was unavailable because the Kaggle source
contains daily bars only.

Indicators use chronological EWM/rolling calculations and prior levels shifted
by one bar. Signals are calculated from a completed bar and the engine retrieves
that candidate only when processing the next bar's open; current high/low/close
are evaluated after the entry. Exits use only the bar at their exit timestamp,
with the configured stop-first rule when stop and target both occur in a daily
bar. No parameter is selected from training; robustness values are predeclared
neighbors evaluated only on the latest test window. All 500 source symbols are
retained; the existing rolling liquidity/price signal filters use only data
available at that historical bar, not future history.

## 6. Train results — latest fold

All required metrics (including gross profit/loss, turnover, holding sessions,
Sharpe, Sortino, streaks and cost fields) are in `train_results.csv`.

| Strategy | Trades | Wins | Losses | Win % | Gross profit | Gross loss | PF | Expectancy | Net % | Max DD | Sharpe | Sortino | Costs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| breakout_20 | 49 | 19 | 30 | 38.775510204081634 | 18196.034206099434 | 17539.711514716408 | 0.86 | -57.02667370217216 | -2.79% | -8.83% | -0.2636600964290277 | -0.1928740648233129 | ₹3,451 |
| breakout_50 | 52 | 24 | 28 | 46.15384615384615 | 21816.97874065594 | 18051.1369725512 | 1.01 | 2.287549082617543 | 0.12% | -8.85% | 0.0392998306130438 | 0.0326699101003619 | ₹3,647 |
| breakout_previous_day | 154 | 73 | 81 | 47.4025974025974 | 72493.49764766126 | 55967.01046932278 | 1.11 | 42.397468374622726 | 6.53% | -8.35% | 0.3546398823820695 | 0.3745865055646366 | ₹9,997 |
| mean_reversion | 33 | 18 | 15 | 54.54545454545454 | 16874.630830316797 | 10157.643277527672 | 1.42 | 142.50350026486967 | 4.70% | -4.68% | 0.4732398017592332 | 0.2735272409648351 | ₹2,014 |
| momentum | 49 | 19 | 30 | 38.775510204081634 | 18196.034206099434 | 17539.711514716408 | 0.86 | -57.02667370217216 | -2.79% | -8.83% | -0.2636600964290277 | -0.1928740648233129 | ₹3,451 |
| pullback | 36 | 14 | 22 | 38.88888888888889 | 15963.635110133418 | 16216.348585779137 | 0.85 | -73.76659358267561 | -2.66% | -8.48% | -0.2632221123573372 | -0.1837946672351353 | ₹2,403 |
| swing_10_20 | 52 | 24 | 28 | 46.15384615384615 | 27074.40572102293 | 18976.15797856788 | 1.20 | 80.64360776759783 | 4.19% | -8.93% | 0.4407929778544873 | 0.2807506409828386 | ₹3,905 |
| swing_1_3 | 82 | 32 | 50 | 39.02439024390244 | 17721.603564501253 | 17163.85254631096 | 0.76 | -61.21066890483436 | -5.02% | -9.40% | -0.5786180463588267 | -0.3217082584872232 | ₹5,577 |
| swing_3_5 | 97 | 41 | 56 | 42.2680412371134 | 30260.685372343567 | 25343.96973520873 | 0.94 | -18.1752388561401 | -1.76% | -8.95% | -0.1053626086935159 | -0.0634353530351712 | ₹6,680 |
| swing_5_10 | 66 | 33 | 33 | 50.0 | 27179.670928552052 | 21290.46226698522 | 1.04 | 15.87208124301755 | 1.05% | -9.90% | 0.1332668244020943 | 0.0894801212292017 | ₹4,842 |
| trend_following | 101 | 49 | 52 | 48.51485148514851 | 50604.10545128417 | 41662.51450462025 | 1.05 | 21.18780083973993 | 2.14% | -8.69% | 0.259425704804195 | 0.2296507445135746 | ₹6,802 |

## 7. Validation results — latest fold

Validation metrics use unchanged strategy rules and the same risk/cost model.
No validation result was used to tune or select a rule.

| Strategy | Trades | Wins | Losses | Win % | Gross profit | Gross loss | PF | Expectancy | Net % | Max DD | Sharpe | Sortino | Costs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| breakout_20 | 47 | 19 | 28 | 40.42553191489361 | 15038.830193937772 | 14703.64309242608 | 0.85 | -52.1477178684977 | -2.45% | -6.67% | -0.4435677053324393 | -0.6207622717533567 | ₹2,786 |
| breakout_50 | 40 | 15 | 25 | 37.5 | 12147.125412888749 | 13555.704463064489 | 0.75 | -94.26837287505212 | -3.77% | -6.33% | -0.923247232526184 | -1.248567165382602 | ₹2,362 |
| breakout_previous_day | 26 | 6 | 20 | 23.076923076923077 | 4020.818481727027 | 11740.094558166797 | 0.29 | -355.76829624496133 | -9.25% | -9.25% | -3.1693394670884065 | -2.952966714010695 | ₹1,531 |
| mean_reversion | 19 | 9 | 10 | 47.368421052631575 | 9216.7203584292 | 7385.473321219769 | 1.07 | 28.88397048132676 | 0.55% | -7.09% | 0.1869869369667747 | 0.2328863944136412 | ₹1,282 |
| momentum | 46 | 19 | 27 | 41.30434782608695 | 15033.585594406302 | 15391.231521408094 | 0.82 | -67.98347415045956 | -3.13% | -6.67% | -0.5998814788715119 | -0.8437098635002283 | ₹2,770 |
| pullback | 38 | 15 | 23 | 39.473684210526315 | 10733.635100321057 | 14877.478360121087 | 0.61 | -167.18756492815072 | -6.35% | -8.71% | -1.3195762006510614 | -1.7228822954763172 | ₹2,209 |
| swing_10_20 | 54 | 21 | 33 | 38.88888888888889 | 18019.50192976032 | 16199.238971769752 | 0.92 | -25.80970508164549 | -1.39% | -6.90% | -0.1199712046528908 | -0.2286634051505815 | ₹3,214 |
| swing_1_3 | 133 | 47 | 86 | 35.338345864661655 | 21152.51831904339 | 22505.92689973171 | 0.67 | -68.01927363243867 | -9.05% | -9.05% | -2.242125180514185 | -2.5131075283759885 | ₹7,693 |
| swing_3_5 | 85 | 28 | 57 | 32.94117647058823 | 15916.922429082217 | 20703.501762944044 | 0.59 | -116.08757450755644 | -9.87% | -9.87% | -2.5215430344603487 | -4.305079029593804 | ₹5,081 |
| swing_5_10 | 85 | 39 | 46 | 45.88235294117647 | 30481.81959559506 | 18867.497609678452 | 1.31 | 76.59646016480735 | 6.51% | -4.35% | 1.2555541109675483 | 2.4514156398524296 | ₹5,104 |
| trend_following | 44 | 19 | 25 | 43.18181818181818 | 14851.517261866758 | 12745.882792674083 | 0.96 | -14.520348419252612 | -0.64% | -5.58% | -0.04013127701931 | -0.0603734080492707 | ₹2,745 |

## 8. Test results — latest fold

This is the primary six-month evaluation period. Exact values for every
strategy and fold are in `test_results.csv`.

| Strategy | Trades | Wins | Losses | Win % | Gross profit | Gross loss | PF | Expectancy | Net % | Max DD | Sharpe | Sortino | Costs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| breakout_20 | 10 | 3 | 7 | 30.0 | 1688.96253174945 | 5869.513341678555 | 0.23 | -487.5276921654443 | -4.88% | -5.19% | -2.321053639290206 | -1.462284043199737 | ₹695 |
| breakout_50 | 12 | 6 | 6 | 50.0 | 6246.008810847329 | 3458.5953111757826 | 1.52 | 167.13444515086186 | 2.01% | -1.87% | 0.8655535662217213 | 0.5930905733686865 | ₹782 |
| breakout_previous_day | 10 | 2 | 8 | 20.0 | 1698.5943839059553 | 5304.696873143609 | 0.27 | -420.05665039693 | -4.20% | -4.52% | -3.012513305791616 | -1.982393501991318 | ₹594 |
| mean_reversion | 18 | 5 | 13 | 27.77777777777778 | 4591.408827144291 | 8512.01209276325 | 0.45 | -284.6940968099286 | -5.12% | -9.11% | -1.4405107811737294 | -1.4527554980789643 | ₹1,204 |
| momentum | 9 | 3 | 6 | 33.33333333333333 | 1840.9031660943117 | 3822.402812361589 | 0.40 | -280.74249029158864 | -2.53% | -3.18% | -1.6137721105317424 | -0.9946748812627824 | ₹545 |
| pullback | 7 | 2 | 5 | 28.57142857142857 | 1901.7342554530703 | 4117.877560180167 | 0.39 | -384.82644471348056 | -2.69% | -5.21% | -0.9861177674061176 | -0.377131907649734 | ₹478 |
| swing_10_20 | 40 | 11 | 29 | 27.500000000000004 | 13578.648119514628 | 17009.76331854135 | 0.68 | -148.65297757326007 | -5.95% | -9.20% | -1.1930495355878863 | -1.203322380056103 | ₹2,515 |
| swing_1_3 | 96 | 40 | 56 | 41.66666666666667 | 18547.455256540048 | 16624.748420814645 | 0.80 | -42.28108929727565 | -4.06% | -9.72% | -0.9101395449201286 | -0.923165860724274 | ₹5,982 |
| swing_3_5 | 79 | 33 | 46 | 41.77215189873418 | 21338.028789481497 | 19002.84606495931 | 0.88 | -31.442368425252425 | -2.48% | -9.40% | -0.4703185876834519 | -0.4798400798148201 | ₹4,819 |
| swing_5_10 | 53 | 20 | 33 | 37.735849056603776 | 16766.60909110493 | 18980.362538678648 | 0.74 | -102.60503105697948 | -5.44% | -9.03% | -0.9463174482051514 | -1.0114762254018537 | ₹3,224 |
| trend_following | 12 | 5 | 7 | 41.66666666666667 | 4782.763175261416 | 5079.062642578817 | 0.80 | -92.3959113542848 | -1.11% | -2.74% | 0.2452419653639404 | 0.2347207162988802 | ₹812 |

### OOS results across all non-overlapping six-month test folds

| Fold | Strategy | Trades | Net % | PF | Max DD | Costs |
|---|---|---|---|---|---|---|
| 1 | breakout_20 | 61 | 2.69% | 1.0990261399996764 | -6.96% | ₹4,073 |
| 1 | breakout_50 | 59 | -4.47% | 0.8321842881014578 | -8.96% | ₹3,656 |
| 1 | breakout_previous_day | 58 | -4.34% | 0.8396529149019203 | -9.02% | ₹3,547 |
| 1 | mean_reversion | 17 | 4.80% | 1.787892715336559 | -4.68% | ₹1,014 |
| 1 | momentum | 60 | 1.20% | 1.046242816846236 | -7.26% | ₹3,802 |
| 1 | pullback | 63 | 7.89% | 1.3043661927902472 | -5.90% | ₹3,879 |
| 1 | swing_10_20 | 61 | 5.08% | 1.2188366639132535 | -8.12% | ₹3,755 |
| 1 | swing_1_3 | 77 | -9.49% | 0.5408758577739863 | -10.57% | ₹4,730 |
| 1 | swing_3_5 | 79 | 1.99% | 1.077614467841384 | -9.41% | ₹5,364 |
| 1 | swing_5_10 | 87 | 9.15% | 1.2845201712088958 | -4.67% | ₹5,779 |
| 1 | trend_following | 63 | 4.30% | 1.1566755690463817 | -8.59% | ₹4,082 |
| 2 | breakout_20 | 36 | -0.11% | 0.992551054293336 | -6.18% | ₹2,278 |
| 2 | breakout_50 | 37 | -1.94% | 0.8913436539828579 | -9.02% | ₹2,454 |
| 2 | breakout_previous_day | 36 | 6.77% | 1.5412581168475363 | -4.90% | ₹2,465 |
| 2 | mean_reversion | 5 | 1.59% | 3.6674150690267098 | -1.00% | ₹289 |
| 2 | momentum | 35 | -3.00% | 0.7977677188449981 | -7.75% | ₹2,227 |
| 2 | pullback | 31 | -1.79% | 0.851550945958062 | -4.71% | ₹1,826 |
| 2 | swing_10_20 | 45 | 5.22% | 1.2986802055271534 | -9.24% | ₹2,856 |
| 2 | swing_1_3 | 162 | -2.56% | 0.9242317792010372 | -9.47% | ₹9,934 |
| 2 | swing_3_5 | 107 | -7.48% | 0.7259372661276277 | -9.88% | ₹6,240 |
| 2 | swing_5_10 | 66 | 0.04% | 1.0017080991225398 | -9.38% | ₹3,954 |
| 2 | trend_following | 38 | -3.08% | 0.8043499247285326 | -7.78% | ₹1,997 |
| 3 | breakout_20 | 17 | 2.22% | 1.4819958946901286 | -2.39% | ₹891 |
| 3 | breakout_50 | 17 | 2.45% | 1.5460076407123833 | -2.24% | ₹891 |
| 3 | breakout_previous_day | 13 | 0.47% | 1.1538927052399697 | -2.35% | ₹645 |
| 3 | mean_reversion | 2 | -1.67% | 0.0 | -1.67% | ₹110 |
| 3 | momentum | 15 | 2.09% | 1.5783641978372474 | -2.39% | ₹856 |
| 3 | pullback | 19 | -0.91% | 0.8488853720747395 | -4.43% | ₹978 |
| 3 | swing_10_20 | 14 | -8.92% | 0.128139959657509 | -9.25% | ₹829 |
| 3 | swing_1_3 | 50 | -8.93% | 0.426696350563816 | -9.27% | ₹2,786 |
| 3 | swing_3_5 | 24 | -8.55% | 0.2385919802677622 | -8.88% | ₹1,414 |
| 3 | swing_5_10 | 17 | -10.20% | 0.0135660566892191 | -10.53% | ₹940 |
| 3 | trend_following | 14 | -0.67% | 0.7919285522523422 | -2.57% | ₹711 |
| 4 | breakout_20 | 47 | -2.45% | 0.8497958634381144 | -6.67% | ₹2,786 |
| 4 | breakout_50 | 40 | -3.77% | 0.748581942713819 | -6.33% | ₹2,362 |
| 4 | breakout_previous_day | 26 | -9.25% | 0.2867048957484692 | -9.25% | ₹1,531 |
| 4 | mean_reversion | 19 | 0.55% | 1.0683176970505643 | -7.09% | ₹1,282 |
| 4 | momentum | 46 | -3.13% | 0.8159286118668218 | -6.67% | ₹2,770 |
| 4 | pullback | 38 | -6.35% | 0.6086888047754391 | -8.71% | ₹2,209 |
| 4 | swing_10_20 | 54 | -1.39% | 0.9224843902928224 | -6.90% | ₹3,214 |
| 4 | swing_1_3 | 133 | -9.05% | 0.6651116165457551 | -9.05% | ₹7,693 |
| 4 | swing_3_5 | 85 | -9.87% | 0.5852200544815409 | -9.87% | ₹5,081 |
| 4 | swing_5_10 | 85 | 6.51% | 1.3065557871621063 | -4.35% | ₹5,104 |
| 4 | trend_following | 44 | -0.64% | 0.955041489169058 | -5.58% | ₹2,745 |
| 5 | breakout_20 | 10 | -4.88% | 0.2343225282127028 | -5.19% | ₹695 |
| 5 | breakout_50 | 12 | 2.01% | 1.5204752649732545 | -1.87% | ₹782 |
| 5 | breakout_previous_day | 10 | -4.20% | 0.2742146197621107 | -4.52% | ₹594 |
| 5 | mean_reversion | 18 | -5.12% | 0.4489683710866688 | -9.11% | ₹1,204 |
| 5 | momentum | 9 | -2.53% | 0.396905298045461 | -3.18% | ₹545 |
| 5 | pullback | 7 | -2.69% | 0.3946349726459721 | -5.21% | ₹478 |
| 5 | swing_10_20 | 40 | -5.95% | 0.6816175417097012 | -9.20% | ₹2,515 |
| 5 | swing_1_3 | 96 | -4.06% | 0.7963414236139446 | -9.72% | ₹5,982 |
| 5 | swing_3_5 | 79 | -2.48% | 0.8848881003946251 | -9.40% | ₹4,819 |
| 5 | swing_5_10 | 53 | -5.44% | 0.74004335033853 | -9.03% | ₹3,224 |
| 5 | trend_following | 12 | -1.11% | 0.8012262200949659 | -2.74% | ₹812 |

### Train-to-validation/test changes — latest fold

Changes are net-return percentage-point differences; a negative value means
the later segment's return was lower.

| Strategy | Train→validation Δ pp | Validation→test Δ pp | Train→test Δ pp |
|---|---|---|---|
| trend_following | -2.78% | -0.47% | -3.25% |
| momentum | -0.33% | 0.60% | 0.27% |
| breakout_20 | 0.34% | -2.42% | -2.08% |
| breakout_50 | -3.89% | 5.78% | 1.89% |
| breakout_previous_day | -15.78% | 5.05% | -10.73% |
| pullback | -3.70% | 3.66% | -0.04% |
| mean_reversion | -4.15% | -5.67% | -9.83% |
| swing_1_3 | -4.03% | 4.99% | 0.96% |
| swing_3_5 | -8.10% | 7.38% | -0.72% |
| swing_5_10 | 5.46% | -11.95% | -6.49% |
| swing_10_20 | -5.59% | -4.55% | -10.14% |

## 9. Costs and turnover — latest test

The canonical cost schedule was not changed: delivery brokerage 0%, delivery
STT 0.10% per side, exchange transaction charge 0.00307%, SEBI charge 0.0001%,
GST 18% on brokerage/exchange/SEBI, buy stamp duty 0.015%, sell DP charge
₹15.34, and 5 bps adverse slippage
per execution. Costs are recomputed from the same model. Gross-before-costs
reverses estimated slippage; explicit taxes/fees and slippage are reported
separately so net P&L reconciles. `cost_analysis.csv` contains detailed fee
components for every strategy segment and portfolio test.

| Strategy | Gross | Fees/taxes/DP | Slippage est. | Total costs | Cost/gross | Cost/turnover | Net |
|---|---|---|---|---|---|---|---|
| trend_following | ₹-296 | ₹618 | ₹195 | ₹812 | 274.20% | 0.2085% | ₹-1,109 |
| momentum | ₹-1,981 | ₹419 | ₹126 | ₹545 | 27.51% | 0.2160% | ₹-2,527 |
| breakout_20 | ₹-4,181 | ₹527 | ₹168 | ₹695 | 16.62% | 0.2071% | ₹-4,875 |
| breakout_50 | ₹2,787 | ₹596 | ₹185 | ₹782 | 28.05% | 0.2108% | ₹2,006 |
| breakout_previous_day | ₹-3,606 | ₹458 | ₹137 | ₹594 | 16.48% | 0.2175% | ₹-4,201 |
| pullback | ₹-2,216 | ₹363 | ₹115 | ₹478 | 21.55% | 0.2081% | ₹-2,694 |
| mean_reversion | ₹-3,921 | ₹916 | ₹288 | ₹1,204 | 30.71% | 0.2093% | ₹-5,124 |
| swing_1_3 | ₹1,923 | ₹4,583 | ₹1,398 | ₹5,982 | 311.11% | 0.2139% | ₹-4,059 |
| swing_3_5 | ₹2,335 | ₹3,701 | ₹1,119 | ₹4,819 | 206.37% | 0.2154% | ₹-2,484 |
| swing_5_10 | ₹-2,214 | ₹2,477 | ₹748 | ₹3,224 | 145.65% | 0.2156% | ₹-5,438 |
| swing_10_20 | ₹-3,431 | ₹1,925 | ₹590 | ₹2,515 | 73.30% | 0.2133% | ₹-5,946 |

## 10. Drawdown and portfolio results

Maximum drawdown is from the engine's daily marked-equity path for each
independent segment, including the 10% configured portfolio drawdown control
and the existing pre-limit tripwire behavior. It is not aggregated across
reset segments. The existing Phase 3 engine's four union-of-signals portfolios
were run on each test fold with the same risk limits:

| Portfolio | Trades | Net % | PF | Max DD | Costs |
|---|---|---|---|---|---|
| pullback + swing_10_20 | 40 | -5.95% | 0.6816175417097012 | -9.20% | ₹2,515 |
| trend_following + pullback | 12 | -1.11% | 0.8012262200949659 | -2.74% | ₹812 |
| trend_following + breakout_20 + pullback | 13 | -1.38% | 0.7544678473281083 | -2.99% | ₹778 |
| trend_following + pullback + swing_10_20 | 39 | -7.83% | 0.5764763859139562 | -8.97% | ₹2,325 |

## 11. Robustness analysis

The existing Phase 3 focus candidates, Pullback and Swing 10–20, were checked
on the latest test window using predeclared neighboring ATR stop multiples
(±10%), holding caps (±1 or ±2 sessions), breakout buffer (±0.05 ATR), volume
threshold (±0.1), total modeled-cost multipliers (0.8×/1.2×), and slippage
(3/7 bps). These are sensitivity observations only; no setting was selected.

| Strategy | Predeclared dimension | Baseline net | Neighbor net range | Sign changed |
|---|---|---|---|---|
| pullback | atr_multiple | -2.69% | -3.47% to -0.63% | False |
| pullback | breakout_buffer_atr | -2.69% | -2.69% to -2.69% | False |
| pullback | cost_multiplier | -2.69% | -2.68% to -2.68% | False |
| pullback | max_holding_bars | -2.69% | -2.06% to -1.09% | False |
| pullback | slippage_bps | -2.69% | -2.72% to -2.67% | False |
| pullback | volume_multiple | -2.69% | -2.69% to -2.69% | False |
| swing_10_20 | atr_multiple | -5.95% | -7.45% to -6.09% | False |
| swing_10_20 | breakout_buffer_atr | -5.95% | -5.95% to -5.95% | False |
| swing_10_20 | cost_multiplier | -5.95% | -6.19% to -5.98% | False |
| swing_10_20 | max_holding_bars | -5.95% | -6.35% to -6.22% | False |
| swing_10_20 | slippage_bps | -5.95% | -6.07% to -5.77% | False |
| swing_10_20 | volume_multiple | -5.95% | -5.95% to -4.62% | False |

## 12. Monte Carlo analysis

The existing trade-order permutation method was used, with 5,000 simulations,
the configured 1% risk fraction, 10% drawdown halt, and the project seed. It
requires at least 30 closed trades. Only trades in the latest six-month test
were eligible; full-sample trades were not substituted. Final-return and
maximum-drawdown p5/p25/p50/p75/p95 fields plus existing capital-milestone
probabilities are in `monte_carlo_results.csv` when estimable.

| Strategy | Status | Trades | P(positive) | P(loss) | Final return p5/p50/p95 | Median max DD |
|---|---|---|---|---|---|---|
| trend_following | INSUFFICIENT_TRADES_FOR_MONTE_CARLO | 12 | — | — | — / — / — | — |
| momentum | INSUFFICIENT_TRADES_FOR_MONTE_CARLO | 9 | — | — | — / — / — | — |
| breakout_20 | INSUFFICIENT_TRADES_FOR_MONTE_CARLO | 10 | — | — | — / — / — | — |
| breakout_50 | INSUFFICIENT_TRADES_FOR_MONTE_CARLO | 12 | — | — | — / — / — | — |
| breakout_previous_day | INSUFFICIENT_TRADES_FOR_MONTE_CARLO | 10 | — | — | — / — / — | — |
| pullback | INSUFFICIENT_TRADES_FOR_MONTE_CARLO | 7 | — | — | — / — / — | — |
| mean_reversion | INSUFFICIENT_TRADES_FOR_MONTE_CARLO | 18 | — | — | — / — / — | — |
| swing_1_3 | COMPUTED_5000_SIMULATIONS | 96 | 0.00% | 100.00% | -8.86% / -5.62% / -5.62% | 8.31% |
| swing_3_5 | COMPUTED_5000_SIMULATIONS | 79 | 0.00% | 100.00% | -10.31% / -7.91% / -6.15% | 10.10% |
| swing_5_10 | COMPUTED_5000_SIMULATIONS | 53 | 0.00% | 100.00% | -10.53% / -8.83% / -6.30% | 10.23% |
| swing_10_20 | COMPUTED_5000_SIMULATIONS | 40 | 0.00% | 100.00% | -10.76% / -9.96% / -7.33% | 10.40% |

For strategies with at least 30 latest-test trades, all requested return and
drawdown percentiles are shown as p5 / p25 / p50 / p75 / p95:

| Strategy | Final return p5 / p25 / p50 / p75 / p95 | Maximum drawdown p5 / p25 / p50 / p75 / p95 |
|---|---|---|
| swing_1_3 | -8.86% / -5.62% / -5.62% / -5.62% / -5.62% | 6.24% / 7.33% / 8.31% / 9.42% / 10.32% |
| swing_3_5 | -10.31% / -9.01% / -7.91% / -7.91% / -6.15% | 8.31% / 9.49% / 10.10% / 10.35% / 10.75% |
| swing_5_10 | -10.53% / -9.78% / -8.83% / -8.30% / -6.30% | 8.92% / 10.03% / 10.23% / 10.50% / 10.80% |
| swing_10_20 | -10.76% / -10.37% / -9.96% / -8.94% / -7.33% | 10.04% / 10.19% / 10.40% / 10.63% / 10.86% |

Existing framework capital-milestone probabilities are shown as probability
of reaching / probability of finishing above each threshold:

| Strategy | 1.25× reach / final | 1.5× reach / final | 1.75× reach / final | 2× reach / final |
|---|---|---|---|---|
| swing_1_3 | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% |
| swing_3_5 | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% |
| swing_5_10 | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% |
| swing_10_20 | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% | 0.00% / 0.00% |

## 13. Extreme-return analysis

The pre-run dataset scan found 50 close
moves over 20% across 40 symbols. No
extreme prices were removed, adjusted, winsorized or smoothed. Trades whose
signal or holding interval includes a flagged symbol/date are explicitly marked
in `trade_log.csv`. 1 closed trade across all
recorded segments includes at least one such observation;
0 are in
the latest primary test. The table and `extreme_return_impact.csv` report the
observed gross P&L, costs and net P&L for affected trades alongside the full
run's strategy/portfolio totals. This is trade attribution, not a re-priced
counterfactual; the effect of replacing these source bars cannot be established
without verified corporate-action data. No corporate action is presumed or confirmed.
`extreme_return_observations.csv` contains the dates/returns and labels their
event classification unknown.

### Extreme observations in the latest test

No latest-test strategy or portfolio trade overlapped a flagged signal/holding date.

### Observed trade attribution across all folds

| Strategy/portfolio | Fold | Segment | Trades | Affected trades | Affected gross | Affected costs | Affected net | Total net | Affected net / total net |
|---|---|---|---|---|---|---|---|---|---|
| trend_following + pullback + swing_10_20 | 1 | test_portfolio | 69 | 1 | ₹-1,016 | ₹41 | ₹-1,057 | ₹6,533 | -16.18% |

## 14. Official NSE NIFTY/VIX alignment

NIFTY 50 comes from official NSE `index_NSEI.csv`; India VIX comes from official
NSE `index_INDIAVIX.csv`. Both files are date-parsed as ISO `YYYY-MM-DD` and
joined on exact dates. There is no forward fill. Within the equity period,
1235 of 1235 Kaggle
sessions align with official NIFTY and 1235 align with official VIX.
Kaggle dates missing from NIFTY: 0;
Kaggle dates missing from VIX: 0.
Official weekend/special sessions absent from the Kaggle equity date union are
listed in `data_quality_report.json`. Official NIFTY is used for regime
classification; official VIX is joined by exact date to the existing regime
feature path. An unavailable VIX observation remains missing.
The local NSE VIX export contains 2 rows with
invalid Open/High/Low fields (2021-02-12; 2021-03-30); the existing regime logic
consumes only Close, which is positive on these rows. Source values were kept
unchanged, and this anomaly is recorded in the data manifest. Both dates
precede the Kaggle dataset's 2021-06-09 first session and fall outside every
Phase 3C fold.

## 15. Comparison with Phase 2 and Phase 3

Phase 2 pullback was +2.60% over 17 trades (PF 1.49); Phase 3 full-window pullback was +2.22% over 43 trades (PF 1.12). Phase 2 swing_10_20 was +5.35% over 18 trades (PF 2.57); Phase 3 full-window swing_10_20 was −8.64% over 36 trades (PF 0.45). Phase 2/Phase 3 figures are descriptive and use different data, dates and sampling than the Kaggle walk-forward results; they are not pooled or overwritten.

| Strategy | Period/scope | Trades | Win % | Profit factor | Net return | Max drawdown |
|---|---|---|---|---|---|---|
| pullback | Phase 2 | 17 | 41.18% | 1.49 | 2.60% | -2.63% |
| pullback | Phase 3 full window | 43 | 41.86% | 1.12 | 2.22% | -6.19% |
| pullback | Phase 3C latest 6-month test (2025-12-09 to 2026-06-08) | 7 | 28.57% | 0.39 | -2.69% | -5.21% |
| swing_10_20 | Phase 2 | 18 | 61.11% | 2.57 | 5.35% | -2.51% |
| swing_10_20 | Phase 3 full window | 36 | 30.56% | 0.45 | -8.64% | -9.15% |
| swing_10_20 | Phase 3C latest 6-month test (2025-12-09 to 2026-06-08) | 40 | 27.50% | 0.68 | -5.95% | -9.20% |

The latest Kaggle test rows are primary OOS observations. Pullback's positive
Phase 2/Phase 3 descriptive returns did not persist in the latest Kaggle test;
Swing 10–20's Phase 2 positive and Phase 3 negative behavior is followed by a
negative latest Kaggle test. The separate rolling test folds also change sign
for both focus rules. Win rate, profit factor and drawdown levels therefore
vary across samples. Provider, dates, coverage and universe construction differ,
so these figures are descriptive and are not pooled. Earlier reports/results
were not modified.

## 16. Limitations

- Unknown underlying provider and collection method; no `Series` field.
- Unknown corporate-action adjustment status; 50 extreme close moves remain
  untreated and may affect individual strategy trades.
- Current-constituent historical roster creates survivorship/membership bias.
- Daily bars cannot reconstruct intraday path; the existing stop-first
  ambiguity assumption applies.
- Four NSE weekend index sessions are absent from the Kaggle equity calendar;
  other per-symbol session gaps remain visible.
- Segment simulations reset capital and positions; they are independent
  segment estimates, not a single continuously compounded portfolio path.
- Cost rates are the existing model and may differ from a broker's actual
  charges. Slippage is estimated at the configured 5 bps each way.
- No Phase 3C-specific strategy approval threshold exists; this report makes
  no approval claim.

## 17. Reproducibility

- Run id: `20260926_171429`
- Application version: `0.1.0`
- Config file: `config.yaml`; SHA-256 is in `run_manifest.json`
- Data SHA-256: `data_manifest.json`
- Seed: `20260925`
- Universe: the exact 500 normalized Kaggle symbols; current sector labels are
  used for the existing 25% sector-risk control, with unmatched labels set to
  `UNKNOWN`.
- Capital: ₹100,000; risk: 1.0%
  per trade, 2.0% daily loss, 10% portfolio
  drawdown, 5 positions, 25% maximum position
  and 25% sector exposure.
- Live trading disabled; no broker contacted; paper ledger and earlier result
  folders were not modified.
- Detailed rows: `train_results.csv`, `validation_results.csv`,
  `test_results.csv`, `strategy_results.csv`, `trade_log.csv`,
  `portfolio_results.csv`, `cost_analysis.csv`, `robustness_results.csv`,
  and `monte_carlo_results.csv`.

## 18. Final research conclusion

**RESULTS_NOT_ROBUST.** Net P&L changed sign across the non-overlapping OOS test windows for pullback 1/5, swing_10_20 2/5; this status describes outcome instability, not a strategy approval decision. The experiment completed on a structurally
validated, separate Kaggle source and produced chronological train,
validation and test evidence. The source provenance, price adjustedness and
point-in-time universe are not verified. Results therefore describe only this
fixed current-constituent historical dataset under the existing engine,
configuration and modeled costs; they do not establish an official NSE
history or approve any strategy.
