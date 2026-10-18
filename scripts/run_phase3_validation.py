"""Run the Phase 3 robustness study strictly from the existing local cache."""

from __future__ import annotations

import html
import json
import sys
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nse_research import __version__  # noqa: E402
from nse_research.backtest.costs import CostModel  # noqa: E402
from nse_research.backtest.engine import BacktestEngine  # noqa: E402
from nse_research.backtest.phase3_validation import (  # noqa: E402
    classify_volatility,
    losing_or_winning_streak,
    metrics_for_trades,
    portfolio_milestones,
    rolling_walk_forward_windows,
    scale_cost_settings,
    select_training_candidate,
    symbol_contribution,
)
from nse_research.config import AppConfig  # noqa: E402
from nse_research.data.providers import validate_and_normalize_bars  # noqa: E402
from nse_research.data.universe import load_sector_map, remove_placeholder_symbols  # noqa: E402
from nse_research.indicators.features import add_features  # noqa: E402
from nse_research.reporting.performance import performance_metrics  # noqa: E402
from nse_research.strategies.composite import CombinedEquityStrategy  # noqa: E402
from nse_research.strategies.equity import EquityStrategy, build_strategies  # noqa: E402

CAPITAL = 100_000.0
PHASE2_CANDIDATES = ("pullback", "swing_10_20")
PORTFOLIO_COMBINATIONS = (
    ("pullback", "swing_10_20"),
    ("trend_following", "pullback"),
    ("trend_following", "breakout_20", "pullback"),
    ("trend_following", "pullback", "swing_10_20"),
)


def _cache_file(cache_dir: Path, symbol: str) -> Path:
    safe = symbol.replace("^", "index_").replace("/", "_")
    return cache_dir / f"{safe}_1d.csv"


def _load_cache(root: Path, config: AppConfig) -> tuple[dict[str, pd.DataFrame], dict[str, Any], list[str], list[str]]:
    universe = pd.read_csv(root / config.universe.symbols_file)
    raw_symbols = universe["Symbol"].dropna().astype(str).str.strip().str.upper().tolist()
    symbols = [symbol if symbol.endswith(".NS") else f"{symbol}.NS" for symbol in raw_symbols]
    symbols, placeholders = remove_placeholder_symbols(symbols)
    requested = list(dict.fromkeys([*symbols, config.universe.benchmark, config.regime.india_vix_symbol]))
    bars: dict[str, pd.DataFrame] = {}
    coverage: dict[str, Any] = {}
    missing = []
    for symbol in requested:
        path = _cache_file(root / config.data.cache_dir, symbol)
        if not path.exists():
            missing.append(symbol)
            continue
        try:
            raw = pd.read_csv(path, parse_dates=["timestamp"], index_col="timestamp")
            normalized, audit = validate_and_normalize_bars(raw, "1d")
        except Exception as exc:  # malformed inputs stay visible in the coverage manifest
            coverage[symbol] = {"path": str(path.relative_to(root)), "error": str(exc)}
            missing.append(symbol)
            continue
        if normalized.empty:
            missing.append(symbol)
            coverage[symbol] = {"path": str(path.relative_to(root)), "normalization_audit": audit, "rows": 0}
            continue
        bars[symbol] = normalized
        coverage[symbol] = {
            "path": str(path.relative_to(root)),
            "rows": len(normalized),
            "first_bar": normalized.index.min().date().isoformat(),
            "last_bar": normalized.index.max().date().isoformat(),
            "normalization_audit": audit,
            "corporate_action_rows": {
                "dividend_events": int(pd.to_numeric(normalized.get("dividends", pd.Series(dtype=float)), errors="coerce").fillna(0).ne(0).sum()),
                "split_events": int(pd.to_numeric(normalized.get("stock_splits", pd.Series(dtype=float)), errors="coerce").fillna(0).ne(0).sum()),
            },
        }
    return bars, coverage, missing, placeholders


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if pd.isna(value) if not isinstance(value, (str, bytes)) else False:
        return None
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def _result_metrics(result: Any, capital: float) -> dict[str, Any]:
    return performance_metrics(result.trades, result.equity_curve, capital)


def _run_with_config(
    engine: BacktestEngine,
    config: AppConfig,
    cost_model: CostModel,
    strategy: Any,
    bars: dict[str, pd.DataFrame],
    benchmark: pd.DataFrame,
    start: date,
    end: date,
    sectors: dict[str, str],
    exit_mode: str,
) -> Any:
    """Temporarily vary costs/risk while retaining causal indicator caches."""
    previous_config, previous_cost = engine.config, engine.cost_model
    previous_tripwire = engine._drawdown_tripwire
    engine.config, engine.cost_model = config, cost_model
    from nse_research.risk.controls import portfolio_drawdown_tripwire

    engine._drawdown_tripwire = portfolio_drawdown_tripwire(config.risk)
    try:
        return engine.run(strategy, bars, benchmark, CAPITAL, start, end, sectors, exit_mode)
    finally:
        engine.config, engine.cost_model = previous_config, previous_cost
        engine._drawdown_tripwire = previous_tripwire


def _month_rows(
    trades: list[dict[str, Any]],
    strategy_name: str,
    start: date,
    end: date,
    scope: str,
) -> list[dict[str, Any]]:
    frame = pd.DataFrame(trades)
    if not frame.empty:
        frame["month"] = pd.to_datetime(frame["exit_time"]).dt.to_period("M").astype(str)
        grouped = {month: part.to_dict("records") for month, part in frame.groupby("month")}
    else:
        grouped = {}
    months = pd.period_range(start, end, freq="M").astype(str)
    output = []
    pnl_values = []
    for month in months:
        subset = grouped.get(month, [])
        metric = metrics_for_trades(subset, CAPITAL)
        pnl = float(metric["net_pnl"])
        pnl_values.append(pnl)
        output.append({
            "strategy": strategy_name,
            "month": month,
            "trades": metric["trades"],
            "profitable_month": bool(pnl > 0),
            "losing_month": bool(pnl < 0),
            "gross_pnl": metric["gross_pnl"],
            "trading_costs": metric["trading_costs"],
            "net_pnl": pnl,
            "simple_return_pct": pnl / CAPITAL * 100,
            "win_rate_pct": metric["win_rate_pct"],
            "profit_factor": metric["profit_factor"],
            "expectancy": metric["expectancy_per_trade"],
            "maximum_drawdown_pct": metric["maximum_drawdown_pct"],
            "scope": scope,
        })
    summary = {
        "strategy": strategy_name,
        "month": "ALL_MONTHS_SUMMARY",
        "months_in_period": len(pnl_values),
        "profitable_months": sum(value > 0 for value in pnl_values),
        "losing_months": sum(value < 0 for value in pnl_values),
        "best_month_pnl": max(pnl_values, default=0.0),
        "worst_month_pnl": min(pnl_values, default=0.0),
        "average_month_pnl": float(np.mean(pnl_values)) if pnl_values else 0.0,
        "median_month_pnl": float(np.median(pnl_values)) if pnl_values else 0.0,
        "maximum_consecutive_losing_months": losing_or_winning_streak(pnl_values, winning=False),
        "maximum_consecutive_profitable_months": losing_or_winning_streak(pnl_values, winning=True),
        "scope": scope,
    }
    output.append(summary)
    return output


def _regime_rows(
    trades: list[dict[str, Any]],
    strategy_name: str,
    benchmark_features: pd.DataFrame,
    vix: pd.DataFrame | None,
) -> list[dict[str, Any]]:
    frame = pd.DataFrame(trades)
    if frame.empty:
        frame = pd.DataFrame(columns=["entry_time", "exit_time", "net_pnl", "market_regime"])
    else:
        frame["entry_day"] = pd.to_datetime(frame["entry_time"]).dt.date
        realized = benchmark_features["realized_vol20"]
        daily_features = benchmark_features.copy()
        daily_features["day"] = pd.to_datetime(daily_features.index).date
        vix_by_day = {}
        if vix is not None and not vix.empty:
            vix_by_day = {pd.Timestamp(stamp).date(): float(close) for stamp, close in vix["close"].items() if pd.notna(close)}
        vol_by_day = {day: float(vol) for day, vol in zip(daily_features["day"], realized) if pd.notna(vol)}
        frame["volatility_regime"] = frame["entry_day"].map(
            lambda day: classify_volatility(vol_by_day.get(day), vix_by_day.get(day))
        )
    rows = []
    for dimension, label_column, labels in [
        ("trend_regime", "market_regime", ["STRONG_BULL", "BULL", "SIDEWAYS", "BEAR", "STRONG_BEAR", "HIGH_VOLATILITY", "UNKNOWN"]),
        ("volatility_regime", "volatility_regime", ["LOW_VOLATILITY", "NORMAL_VOLATILITY", "HIGH_VOLATILITY", "UNKNOWN"]),
    ]:
        if label_column not in frame:
            frame[label_column] = "UNKNOWN"
        for label in labels:
            subset = frame.loc[frame[label_column].fillna("UNKNOWN").eq(label)].to_dict("records")
            metrics = metrics_for_trades(subset, CAPITAL)
            rows.append({
                "strategy": strategy_name,
                "regime_dimension": dimension,
                "regime": label,
                "trades": metrics["trades"],
                "win_rate_pct": metrics["win_rate_pct"],
                "profit_factor": metrics["profit_factor"],
                "expectancy": metrics["expectancy_per_trade"],
                "net_pnl": metrics["net_pnl"],
                "return_pct_of_initial_capital": metrics["net_return_pct"],
                "maximum_drawdown_pct": metrics["maximum_drawdown_pct"],
                "trading_costs": metrics["trading_costs"],
                "sample_status": "LOW SAMPLE (<30 trades)" if metrics["trades"] < 30 else "descriptive only",
            })
    return rows


def _yearly_rows(trades: list[dict[str, Any]], strategy_name: str, start: date, end: date) -> list[dict[str, Any]]:
    frame = pd.DataFrame(trades)
    if not frame.empty:
        frame["exit_year"] = pd.to_datetime(frame["exit_time"]).dt.year
    output = []
    for year in range(start.year, end.year + 1):
        subset = frame.loc[frame["exit_year"].eq(year)].to_dict("records") if not frame.empty else []
        metrics = metrics_for_trades(subset, CAPITAL)
        year_start = max(start, date(year, 1, 1))
        year_end = min(end, date(year, 12, 31))
        output.append({
            "strategy": strategy_name,
            "year": year,
            "period_start": year_start.isoformat(),
            "period_end": year_end.isoformat(),
            "trades": metrics["trades"],
            "win_rate_pct": metrics["win_rate_pct"],
            "profit_factor": metrics["profit_factor"],
            "expectancy": metrics["expectancy_per_trade"],
            "gross_pnl": metrics["gross_pnl"],
            "net_pnl": metrics["net_pnl"],
            "return_pct_of_initial_capital": metrics["net_return_pct"],
            "maximum_drawdown_pct": metrics["maximum_drawdown_pct"],
            "trading_costs": metrics["trading_costs"],
            "scope": "partial calendar year; available-window descriptive, not out-of-sample",
        })
    return output


def _sector_contribution(trades: list[dict[str, Any]], strategy_name: str) -> pd.DataFrame:
    frame = pd.DataFrame(trades)
    cols = ["strategy", "sector", "trades", "win_rate_pct", "net_pnl", "positive_pnl", "positive_pnl_contribution_pct"]
    if frame.empty:
        return pd.DataFrame(columns=cols)
    frame["net_pnl"] = pd.to_numeric(frame["net_pnl"], errors="coerce").fillna(0)
    grouped = frame.groupby("sector", dropna=False).agg(
        trades=("net_pnl", "size"),
        win_rate_pct=("net_pnl", lambda values: float((values > 0).mean() * 100)),
        net_pnl=("net_pnl", "sum"),
    ).reset_index()
    grouped["positive_pnl"] = grouped["net_pnl"].clip(lower=0)
    positive = float(grouped.positive_pnl.sum())
    grouped["positive_pnl_contribution_pct"] = grouped.positive_pnl / positive * 100 if positive else np.nan
    grouped.insert(0, "strategy", strategy_name)
    return grouped[cols].sort_values("net_pnl", ascending=False, kind="stable").reset_index(drop=True)


def _testable_windows(start: date, end: date, first_signal_ready: date) -> list[dict[str, date]]:
    return rolling_walk_forward_windows(first_signal_ready, end, 24, 6, 6, 6)


def _research_status(
    trades: int,
    windows: list[dict[str, Any]],
    metrics: dict[str, Any],
) -> tuple[str, str]:
    if trades < 30 or len(windows) < 2:
        return "INSUFFICIENT DATA", "Need at least 30 unseen trades across two complete test windows."
    if metrics["expectancy_per_trade"] <= 0 or metrics["profit_factor"] <= 1:
        return "NEGATIVE", "Aggregate unseen-test expectancy is non-positive or profit factor is at/below one."
    strategy_windows = [row for row in windows if row.get("test_trades", 0) > 0]
    positive_windows = sum(float(row.get("oos_net_pnl", 0) or 0) > 0 for row in strategy_windows)
    if len(strategy_windows) < 2 or positive_windows < len(strategy_windows):
        return "MIXED", "Positive aggregate OOS results are not consistent across test windows."
    return "PROMISING — REQUIRES FURTHER VALIDATION", "OOS criteria are positive, but multi-year cost, parameter, regime and concentration evidence remains required."


def run() -> Path:
    config = AppConfig.load(ROOT / "config.yaml")
    now = datetime.now(ZoneInfo(config.project.timezone))
    run_id = now.strftime("%Y%m%d_%H%M%S")
    result_dir = ROOT / config.project.result_root / f"phase3_{run_id}"
    result_dir.mkdir(parents=True, exist_ok=False)

    bars, coverage, missing, placeholders = _load_cache(ROOT, config)
    benchmark = bars.get(config.universe.benchmark)
    vix = bars.get(config.regime.india_vix_symbol)
    if benchmark is None or benchmark.empty:
        raise RuntimeError("Cached NIFTY 50 benchmark is unavailable; Phase 3 cannot run without network-free benchmark bars.")
    start, end = benchmark.index.min().date(), benchmark.index.max().date()
    benchmark_features = add_features(
        benchmark,
        atr_period=config.strategies.atr_period,
        rsi_period=config.strategies.rsi_period,
        ema_fast=config.regime.ema_fast,
        ema_slow=config.regime.ema_slow,
        ema_long=config.regime.ema_long,
        adx_period=config.regime.adx_period,
        india_vix=vix["close"] if vix is not None else None,
        india_vix_high_threshold=config.regime.india_vix_high_threshold,
    )
    ready_index = benchmark_features["ema200"].first_valid_index()
    first_signal_ready = pd.Timestamp(ready_index).date() if ready_index is not None else end
    eligible = {
        symbol: frame for symbol, frame in bars.items()
        if symbol not in {config.universe.benchmark, config.regime.india_vix_symbol}
        and len(frame) >= config.universe.min_history_rows
    }
    excluded_insufficient_history = [
        {"symbol": symbol, "rows": len(frame), "required_rows": config.universe.min_history_rows}
        for symbol, frame in bars.items()
        if symbol not in {config.universe.benchmark, config.regime.india_vix_symbol}
        and len(frame) < config.universe.min_history_rows
    ]
    for symbol, frame in bars.items():
        if symbol in coverage:
            coverage[symbol]["eligible_for_backtest"] = bool(
                symbol not in {config.universe.benchmark, config.regime.india_vix_symbol}
                and len(frame) >= config.universe.min_history_rows
            )
    sector_path = ROOT / config.universe.symbols_file
    sectors = load_sector_map(sector_path)
    sector_count = len({sectors.get(symbol, "UNKNOWN") for symbol in eligible})
    strategies = build_strategies(
        [name for name in config.strategies.enabled if name != "vwap_intraday"],
        config.strategies.reward_risk,
        config.strategies.atr_stop_multiple,
        config.strategies.adx_minimum,
        config.strategies.volume_multiple,
        config.strategies.entry_cutoff,
        "1d",
        False,
        config.universe.min_avg_volume,
        config.universe.min_avg_traded_value,
        config.universe.min_price,
        config.strategies.stop_mode,
        config.strategies.signal_score_weights,
    )
    engine = BacktestEngine(config, CostModel(config.costs))
    strategy_by_name = {strategy.name: strategy for strategy in strategies}
    results: dict[str, Any] = {}
    metrics_by_strategy: dict[str, dict[str, Any]] = {}
    trades_by_strategy: dict[str, list[dict[str, Any]]] = {}
    all_trade_rows = []
    yearly_rows: list[dict[str, Any]] = []
    monthly_rows: list[dict[str, Any]] = []
    regime_rows: list[dict[str, Any]] = []
    symbol_frames: list[pd.DataFrame] = []
    sector_frames: list[pd.DataFrame] = []
    milestone_rows = []

    for strategy in strategies:
        print(f"Phase 3 baseline: {strategy.name} ({len(eligible)} symbols)", flush=True)
        result = engine.run(strategy, eligible, benchmark, CAPITAL, start, end, sectors, config.strategies.exit_mode)
        results[strategy.name] = result
        metrics = _result_metrics(result, CAPITAL)
        metrics_by_strategy[strategy.name] = metrics
        trades = result.trades
        trades_by_strategy[strategy.name] = trades
        all_trade_rows.extend({**trade, "analysis_strategy": strategy.name, "initial_capital": CAPITAL, "sample_scope": "full_available_descriptive_not_oos"} for trade in trades)
        yearly_rows.extend(_yearly_rows(trades, strategy.name, start, end))
        monthly_rows.extend(_month_rows(trades, strategy.name, start, end, "full_available_descriptive_not_oos"))
        regime_rows.extend(_regime_rows(trades, strategy.name, benchmark_features, vix))
        contribution = symbol_contribution(trades)
        if not contribution.empty:
            contribution.insert(0, "strategy", strategy.name)
            symbol_frames.append(contribution)
        sector_frame = _sector_contribution(trades, strategy.name)
        if not sector_frame.empty:
            sector_frames.append(sector_frame)
        for milestone in portfolio_milestones(result.equity_curve, CAPITAL):
            milestone_rows.append({"strategy": strategy.name, **milestone, "scope": "full_available_descriptive_not_oos"})

    # The current cache supports no complete 24m train + 6m validation + 6m test
    # after the 200-session indicator warm-up. Do not fill test rows from in-sample data.
    windows = _testable_windows(start, end, first_signal_ready)
    walk_rows = []
    oos_by_strategy: dict[str, list[dict[str, Any]]] = {strategy.name: [] for strategy in strategies}
    if not windows:
        requested_window_end = (pd.Timestamp(start) + pd.DateOffset(months=36) - pd.Timedelta(days=1)).date()
        wfo_reason = (
            f"Insufficient history: the cache covers {(end - start).days + 1} calendar days "
            f"({start} to {end}), but a 24m train + 6m validation + 6m test requires 36 months "
            f"(a complete first window from this start would end {requested_window_end}). "
            f"The 200-session EMA first becomes usable {first_signal_ready}, shortening trend/swing history further."
        )
        for strategy in strategies:
            walk_rows.append({
                "strategy": strategy.name,
                "window": 0,
                "train_start": None,
                "train_end": None,
                "validation_start": None,
                "validation_end": None,
                "test_start": None,
                "test_end": None,
                "selected_parameters": json.dumps({"baseline": True}),
                "train_trades": 0,
                "validation_trades": 0,
                "test_trades": 0,
                "oos_net_pnl": None,
                "oos_return_pct": None,
                "oos_profit_factor": None,
                "oos_expectancy": None,
                "oos_maximum_drawdown_pct": None,
                "status": "INSUFFICIENT DATA",
                "reason": wfo_reason,
            })
    else:
        # Kept for future data refreshes: selection sees training metrics only;
        # validation is a gate and test is measured after selection is frozen.
        for window_number, window in enumerate(windows, start=1):
            for strategy in strategies:
                base_parameters = {"adx_minimum": strategy.adx_minimum}
                candidates = []
                for adx in sorted(set([max(5, strategy.adx_minimum - 5), strategy.adx_minimum, strategy.adx_minimum + 5])):
                    variant = replace(strategy, adx_minimum=adx)
                    training = engine.run(variant, eligible, benchmark, CAPITAL, window["train_start"], window["train_end"], sectors, config.strategies.exit_mode)
                    train_metrics = _result_metrics(training, CAPITAL)
                    candidates.append({"parameters": {"adx_minimum": adx}, **train_metrics})
                selected, selection_reason = select_training_candidate(candidates, base_parameters, min_trades=30)
                frozen = replace(strategy, **selected)
                validation = engine.run(frozen, eligible, benchmark, CAPITAL, window["validation_start"], window["validation_end"], sectors, config.strategies.exit_mode)
                validation_metrics = _result_metrics(validation, CAPITAL)
                if selected != base_parameters and (
                    validation_metrics["trades"] < 10
                    or validation_metrics["expectancy_per_trade"] <= 0
                    or validation_metrics["profit_factor"] <= 1
                ):
                    selected = base_parameters
                    frozen = strategy
                    selection_reason += "; reverted to baseline after validation gate failed"
                    validation = engine.run(frozen, eligible, benchmark, CAPITAL, window["validation_start"], window["validation_end"], sectors, config.strategies.exit_mode)
                test = engine.run(frozen, eligible, benchmark, CAPITAL, window["test_start"], window["test_end"], sectors, config.strategies.exit_mode)
                test_metrics = _result_metrics(test, CAPITAL)
                oos_by_strategy[strategy.name].extend(test.trades)
                walk_rows.append({
                    "strategy": strategy.name, "window": window_number, **window,
                    "selected_parameters": json.dumps(selected),
                    "selection_method": selection_reason,
                    "train_trades": next((row["trades"] for row in candidates if row["parameters"] == selected), 0),
                    "validation_trades": len(validation.trades), "test_trades": len(test.trades),
                    "oos_net_pnl": test_metrics["net_pnl"], "oos_return_pct": test_metrics["net_return_pct"],
                    "oos_profit_factor": test_metrics["profit_factor"], "oos_expectancy": test_metrics["expectancy_per_trade"],
                    "oos_maximum_drawdown_pct": test_metrics["maximum_drawdown_pct"],
                    "status": "LOW SAMPLE" if test_metrics["trades"] < 30 else "DESCRIPTIVE ONLY",
                    "reason": "Selection used training rows only; validation and test were untouched until parameters were frozen.",
                })

    # Stability uses small, predeclared one-factor neighborhoods around Phase 2 rules.
    stability_rows = []
    for strategy_name in PHASE2_CANDIDATES:
        strategy = strategy_by_name[strategy_name]
        dimensions = {
            "atr_stop_multiple": ("atr_multiple", [1.5, 2.0, 2.5]),
            "adx_minimum": ("adx_minimum", [15.0, 20.0, 25.0]),
            "volume_multiple": ("volume_multiple", [1.0, 1.3, 1.6]),
            "reward_risk": ("reward_risk", [1.0, 1.5, 2.0]),
        }
        for dimension, (attribute, values) in dimensions.items():
            for value in values:
                variant = replace(strategy, **{attribute: value})
                result = engine.run(variant, eligible, benchmark, CAPITAL, start, end, sectors, config.strategies.exit_mode)
                m = _result_metrics(result, CAPITAL)
                stability_rows.append({
                    "strategy": strategy_name, "parameter": dimension, "value": value,
                    "trades": m["trades"], "win_rate_pct": m["win_rate_pct"], "average_win": m["average_win"],
                    "average_loss": m["average_loss"], "payoff_ratio": m["payoff_ratio"], "profit_factor": m["profit_factor"],
                    "expectancy": m["expectancy_per_trade"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
                    "maximum_drawdown_pct": m["maximum_drawdown_pct"], "sample_scope": "full_available_descriptive_not_oos",
                })
    for name in ("breakout_20", "breakout_50"):
        strategy = strategy_by_name[name]
        for lookback in (20, 30, 50, 100):
            variant = replace(strategy, name=f"breakout_{lookback}")
            result = engine.run(variant, eligible, benchmark, CAPITAL, start, end, sectors, config.strategies.exit_mode)
            m = _result_metrics(result, CAPITAL)
            stability_rows.append({
                "strategy": name, "parameter": "breakout_lookback", "value": lookback,
                "trades": m["trades"], "win_rate_pct": m["win_rate_pct"], "average_win": m["average_win"],
                "average_loss": m["average_loss"], "payoff_ratio": m["payoff_ratio"], "profit_factor": m["profit_factor"],
                "expectancy": m["expectancy_per_trade"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
                "maximum_drawdown_pct": m["maximum_drawdown_pct"], "sample_scope": "full_available_descriptive_not_oos",
            })
    ema_sets = ((10, 20, 50), (20, 50, 100), (20, 50, 200), (50, 100, 200))
    for fast, slow, long in ema_sets:
        ema_config = config.model_copy(deep=True)
        ema_config.regime.ema_fast, ema_config.regime.ema_slow, ema_config.regime.ema_long = fast, slow, long
        ema_engine = BacktestEngine(ema_config, CostModel(ema_config.costs))
        strategy = strategy_by_name["trend_following"]
        result = ema_engine.run(strategy, eligible, benchmark, CAPITAL, first_signal_ready, end, sectors, config.strategies.exit_mode)
        m = _result_metrics(result, CAPITAL)
        stability_rows.append({
            "strategy": "trend_following", "parameter": "ema_fast_slow_long", "value": f"{fast}/{slow}/{long}",
            "trades": m["trades"], "win_rate_pct": m["win_rate_pct"], "average_win": m["average_win"],
            "average_loss": m["average_loss"], "payoff_ratio": m["payoff_ratio"], "profit_factor": m["profit_factor"],
            "expectancy": m["expectancy_per_trade"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
            "maximum_drawdown_pct": m["maximum_drawdown_pct"], "sample_scope": "common post-baseline-warmup available window; descriptive, not OOS",
        })

    cost_rows = []
    exit_rows = []
    sizing_rows = []
    for name in PHASE2_CANDIDATES:
        strategy = strategy_by_name[name]
        for factor in (0.5, 1.0, 1.5, 2.0):
            if factor == 1.0:
                result = results[name]
            else:
                scenario = config.model_copy(deep=True)
                scenario.costs = scale_cost_settings(config.costs, factor)
                result = _run_with_config(engine, scenario, CostModel(scenario.costs), strategy, eligible, benchmark, start, end, sectors, config.strategies.exit_mode)
            m = _result_metrics(result, CAPITAL)
            cost_rows.append({
                "strategy": name, "sensitivity": "all_costs_multiplier", "setting": factor,
                "slippage_bps": config.costs.slippage_bps * factor,
                "trades": m["trades"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
                "profit_factor": m["profit_factor"], "expectancy": m["expectancy_per_trade"],
                "maximum_drawdown_pct": m["maximum_drawdown_pct"], "trading_costs": m["trading_costs"],
                "sample_scope": "full_available_descriptive_not_oos",
            })
        for slip_bps in (5, 10, 20, 30, 50):
            if slip_bps == config.costs.slippage_bps:
                result = results[name]
            else:
                scenario = config.model_copy(deep=True)
                scenario.costs = config.costs.model_copy(update={"slippage_bps": float(slip_bps)})
                scenario.risk = scenario.risk.model_copy(update={"max_expected_slippage_bps": float(slip_bps)})
                result = _run_with_config(engine, scenario, CostModel(scenario.costs), strategy, eligible, benchmark, start, end, sectors, config.strategies.exit_mode)
            m = _result_metrics(result, CAPITAL)
            cost_rows.append({
                "strategy": name, "sensitivity": "slippage_bps", "setting": slip_bps,
                "slippage_bps": slip_bps,
                "trades": m["trades"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
                "profit_factor": m["profit_factor"], "expectancy": m["expectancy_per_trade"],
                "maximum_drawdown_pct": m["maximum_drawdown_pct"], "trading_costs": m["trading_costs"],
                "sample_scope": "full_available_descriptive_not_oos; slippage gate raised to scenario level",
            })
        for risk_fraction in (0.005, 0.01, 0.015, 0.02):
            if risk_fraction == config.risk.risk_per_trade:
                result = results[name]
            else:
                scenario = config.model_copy(deep=True)
                scenario.risk = scenario.risk.model_copy(update={"risk_per_trade": risk_fraction})
                result = _run_with_config(engine, scenario, CostModel(scenario.costs), strategy, eligible, benchmark, start, end, sectors, config.strategies.exit_mode)
            m = _result_metrics(result, CAPITAL)
            sizing_rows.append({
                "strategy": name, "risk_per_trade_pct": risk_fraction * 100,
                "trades": m["trades"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
                "maximum_drawdown_pct": m["maximum_drawdown_pct"], "sharpe": m["sharpe"], "sortino": m["sortino"],
                "longest_losing_streak": m["longest_losing_streak"], "average_r_multiple": m["average_r_multiple"],
                "capital_utilization_pct": m["max_gross_exposure"] / CAPITAL * 100,
                "sample_scope": "full_available_descriptive_not_oos",
            })
        for mode in ("stop_loss", "atr_trailing", "partial_trailing", "max_holding"):
            result = _run_with_config(engine, config, engine.cost_model, strategy, eligible, benchmark, start, end, sectors, mode)
            m = _result_metrics(result, CAPITAL)
            exit_rows.append({
                "strategy": name, "exit_mode": mode, "reward_risk_target": strategy.reward_risk,
                "trades": m["trades"], "win_rate_pct": m["win_rate_pct"], "average_win": m["average_win"],
                "average_loss": m["average_loss"], "payoff_ratio": m["payoff_ratio"], "profit_factor": m["profit_factor"],
                "expectancy": m["expectancy_per_trade"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
                "maximum_drawdown_pct": m["maximum_drawdown_pct"], "sharpe": m["sharpe"], "sortino": m["sortino"],
                "trading_costs": m["trading_costs"], "sample_scope": "full_available_descriptive_not_oos",
            })
        for rr in (1.0, 1.5, 2.0, 2.5, 3.0):
            variant = replace(strategy, reward_risk=rr)
            result = _run_with_config(engine, config, engine.cost_model, variant, eligible, benchmark, start, end, sectors, "fixed_r")
            m = _result_metrics(result, CAPITAL)
            exit_rows.append({
                "strategy": name, "exit_mode": "fixed_r", "reward_risk_target": rr,
                "trades": m["trades"], "win_rate_pct": m["win_rate_pct"], "average_win": m["average_win"],
                "average_loss": m["average_loss"], "payoff_ratio": m["payoff_ratio"], "profit_factor": m["profit_factor"],
                "expectancy": m["expectancy_per_trade"], "net_pnl": m["net_pnl"], "net_return_pct": m["net_return_pct"],
                "maximum_drawdown_pct": m["maximum_drawdown_pct"], "sharpe": m["sharpe"], "sortino": m["sortino"],
                "trading_costs": m["trading_costs"], "sample_scope": "full_available_descriptive_not_oos",
            })

    portfolio_rows = []
    portfolio_monthly_rows = []
    correlation_rows = []
    daily_by_strategy = {}
    for name, result in results.items():
        if result.trades:
            frame = pd.DataFrame(result.trades)
            frame["day"] = pd.to_datetime(frame["exit_time"]).dt.date
            daily_by_strategy[name] = frame.groupby("day")["net_pnl"].sum()
    for left_index, left in enumerate(sorted(daily_by_strategy)):
        for right in sorted(daily_by_strategy)[left_index + 1:]:
            pair = pd.concat([daily_by_strategy[left].rename(left), daily_by_strategy[right].rename(right)], axis=1).fillna(0)
            corr = float(pair[left].corr(pair[right])) if len(pair) > 1 and pair[left].std() and pair[right].std() else None
            correlation_rows.append({"strategy_a": left, "strategy_b": right, "daily_pnl_correlation": corr})
    correlation_map = {(row["strategy_a"], row["strategy_b"]): row["daily_pnl_correlation"] for row in correlation_rows}
    for names in PORTFOLIO_COMBINATIONS:
        members = tuple(strategy_by_name[name] for name in names)
        combined = CombinedEquityStrategy(" + ".join(names), members)
        result = engine.run(combined, eligible, benchmark, CAPITAL, start, end, sectors, config.strategies.exit_mode)
        m = _result_metrics(result, CAPITAL)
        source_frame = pd.DataFrame(result.trades)
        contributions = {}
        if not source_frame.empty:
            contributions = source_frame.groupby("strategy")["net_pnl"].sum().to_dict()
        member_corr = []
        for index, left in enumerate(sorted(names)):
            for right in sorted(names)[index + 1:]:
                member_corr.append(correlation_map.get((left, right)))
        corr_avg = float(np.nanmean(member_corr)) if member_corr and np.isfinite(np.nanmean(member_corr)) else None
        portfolio_rows.append({
            "portfolio": combined.name, "member_strategies": ",".join(names),
            "trades": m["trades"], "win_rate_pct": m["win_rate_pct"], "profit_factor": m["profit_factor"],
            "expectancy": m["expectancy_per_trade"], "gross_pnl": m["gross_pnl"], "net_pnl": m["net_pnl"],
            "return_pct": m["net_return_pct"], "maximum_drawdown_pct": m["maximum_drawdown_pct"],
            "sharpe": m["sharpe"], "sortino": m["sortino"], "trading_costs": m["trading_costs"],
            "average_member_daily_pnl_correlation": corr_avg,
            "strategy_contribution_net_pnl_json": json.dumps(contributions, sort_keys=True),
            "scope": "full_available_descriptive_not_oos; same portfolio risk limits as Phase 2",
        })
        portfolio_monthly_rows.extend(_month_rows(result.trades, combined.name, start, end, "combined-portfolio descriptive, not OOS"))
        for milestone in portfolio_milestones(result.equity_curve, CAPITAL):
            milestone_rows.append({"strategy": combined.name, **milestone, "scope": "full_available_descriptive_not_oos"})

    # OOS-only Monte Carlo is intentionally unavailable when the WFO test is not estimable.
    monte_carlo = {
        strategy.name: {
            "status": "INSUFFICIENT DATA",
            "oos_trades": 0,
            "required_trades": 30,
            "simulations_requested": 5000,
            "simulations_run": 0,
            "reason": "No complete walk-forward test window exists after indicator warm-up; full-sample trades are not substituted for OOS trades.",
        }
        for strategy in strategies
    }

    strategy_rows = []
    oos_metrics_by_strategy = {
        name: metrics_for_trades(trades, CAPITAL) for name, trades in oos_by_strategy.items()
    }
    for strategy in strategies:
        m = metrics_by_strategy[strategy.name]
        oos_metrics = oos_metrics_by_strategy[strategy.name]
        trades = trades_by_strategy[strategy.name]
        strategy_walk_rows = [row for row in walk_rows if row["strategy"] == strategy.name and row.get("window", 0)]
        status, status_reason = _research_status(oos_metrics["trades"], strategy_walk_rows, oos_metrics)
        sym = symbol_contribution(trades)
        top5 = float(sym.head(5)["positive_pnl"].sum() / sym["positive_pnl"].sum() * 100) if len(sym) and sym["positive_pnl"].sum() else None
        top10 = float(sym.head(10)["positive_pnl"].sum() / sym["positive_pnl"].sum() * 100) if len(sym) and sym["positive_pnl"].sum() else None
        month_summary = next((row for row in monthly_rows if row.get("strategy") == strategy.name and row.get("month") == "ALL_MONTHS_SUMMARY"), {})
        strategy_rows.append({
            "strategy": strategy.name,
            **{key: m.get(key) for key in [
                "trades", "win_rate_pct", "average_win", "average_loss", "payoff_ratio", "profit_factor",
                "expectancy_per_trade", "gross_pnl", "net_pnl", "net_return_pct", "maximum_drawdown_pct",
                "sharpe", "sortino", "longest_losing_streak", "average_holding_days", "trading_costs",
                "capital_multiple", "max_gross_exposure", "win_rate_90pct_plus", "win_rate_sample_sufficient",
            ]},
            "oos_trades": oos_metrics["trades"],
            "oos_net_pnl": oos_metrics["net_pnl"] if windows else None,
            "oos_return_pct": oos_metrics["net_return_pct"] if windows else None,
            "oos_win_rate_pct": oos_metrics["win_rate_pct"] if windows else None,
            "oos_profit_factor": oos_metrics["profit_factor"] if windows else None,
            "oos_expectancy": oos_metrics["expectancy_per_trade"] if windows else None,
            "oos_maximum_drawdown_pct": oos_metrics["maximum_drawdown_pct"] if windows else None,
            "oos_sharpe": oos_metrics["sharpe"] if windows else None,
            "oos_sortino": oos_metrics["sortino"] if windows else None,
            "oos_status": "INSUFFICIENT DATA — no complete train/validation/test window" if not windows else (
                "INSUFFICIENT DATA — fewer than 30 OOS trades" if oos_metrics["trades"] < 30 else "DESCRIPTIVE ONLY"
            ),
            "profitable_months": month_summary.get("profitable_months", 0),
            "losing_months": month_summary.get("losing_months", 0),
            "best_month_pnl": month_summary.get("best_month_pnl", 0.0),
            "worst_month_pnl": month_summary.get("worst_month_pnl", 0.0),
            "average_month_pnl": month_summary.get("average_month_pnl", 0.0),
            "median_month_pnl": month_summary.get("median_month_pnl", 0.0),
            "max_consecutive_losing_months": month_summary.get("maximum_consecutive_losing_months", 0),
            "max_consecutive_profitable_months": month_summary.get("maximum_consecutive_profitable_months", 0),
            "top5_symbol_positive_pnl_contribution_pct": top5,
            "top10_symbol_positive_pnl_contribution_pct": top10,
            "90pct_win_rate_assessment": (
                "90%+ observed; inspect average win/loss, payoff, target size, stops, partial exits, and OOS sample before interpretation"
                if m["win_rate_90pct_plus"] else
                "No 90% win-rate behavior observed in this available sample"
            ),
            "research_status": status,
            "research_status_reason": status_reason if windows else "No complete multi-year out-of-sample sample; available-window metrics are descriptive.",
            "sample_scope": "full_available_descriptive_not_oos",
        })

    # Detailed cache-quality comparison against benchmark sessions, with no filling.
    benchmark_days = set(pd.Timestamp(stamp).date() for stamp in benchmark.index)
    gap_total = 0
    for symbol, frame in bars.items():
        observed = set(pd.Timestamp(stamp).date() for stamp in frame.index)
        if observed:
            span = {day for day in benchmark_days if min(observed) <= day <= max(observed)}
            gaps = sorted(span - observed)
        else:
            gaps = []
        if symbol in coverage:
            coverage[symbol]["missing_benchmark_sessions_inside_symbol_span"] = len(gaps)
            coverage[symbol]["missing_session_dates_sample"] = [day.isoformat() for day in gaps[:25]]
        gap_total += len(gaps)
    manifest = {
        "provider": "local Yahoo Finance cache; no network request made during Phase 3",
        "interval": "1d",
        "requested_history_target_years": 3,
        "preferred_history_target_years": 5,
        "actual_start": start.isoformat(),
        "actual_end": end.isoformat(),
        "calendar_days": (end - start).days + 1,
        "benchmark_sessions": len(benchmark),
        "first_ema200_signal_ready_date": first_signal_ready.isoformat(),
        "universe_method": "current constituent list in data/universe.csv; same current-membership methodology as Phase 2",
        "universe_rows": len(pd.read_csv(ROOT / config.universe.symbols_file)),
        "eligible_equity_symbols": len(eligible),
        "excluded_insufficient_history_symbols": excluded_insufficient_history,
        "symbols_with_cached_bars": len([symbol for symbol in bars if symbol not in {config.universe.benchmark, config.regime.india_vix_symbol}]),
        "missing_symbols": missing,
        "excluded_placeholders": placeholders,
        "india_vix_available": vix is not None and not vix.empty,
        "symbols_with_inspected_quality": len(coverage),
        "missing_benchmark_sessions_total": gap_total,
        "corporate_action_notes": "Raw OHLCV cache uses auto_adjust=false. Yahoo dividends/splits fields are counted where present; delisted securities and historic symbol identity are not reconstructed.",
        "known_biases": [
            "Survivorship/index-membership bias: universe is a current constituent snapshot, not point-in-time membership.",
            "The 14-month cache starts 2025-07-21; it does not meet the requested 3-year target, much less the preferred 5 years.",
            "The default 200-session EMA becomes valid only on 2026-05-14, leaving fewer than five months for trend/swing rules; other strategies have shorter warm-ups but still lack a multi-year window.",
            "Yahoo Finance data may contain revisions, omissions, adjustment differences, symbol changes, and delisted-stock gaps; it is not exchange-certified.",
            "India VIX was not cached; high-volatility classification uses benchmark realized volatility and reports VIX unavailable.",
            "Daily OHLC bars cannot resolve intraday order path; stop-first ambiguity remains the configured conservative fill assumption.",
            "Current constituent sector labels are not point-in-time historical sector membership.",
        ],
        "volatility_regime_method": "LOW_VOLATILITY at annualized 20-session realized volatility <=15%; HIGH_VOLATILITY at >=30% or India VIX >=25; NORMAL_VOLATILITY otherwise. VIX was unavailable.",
        "symbol_coverage": coverage,
    }

    walk_frame = pd.DataFrame(walk_rows)
    # WFO test results, if any, must be written separately and never filled from full-sample trades.
    oos_result_rows = []
    for row in strategy_rows:
        oos_metrics = oos_metrics_by_strategy[row["strategy"]]
        oos_result_rows.append({
            "strategy": row["strategy"], "oos_trades": row["oos_trades"],
            "status": row["oos_status"], "net_pnl": None, "return_pct": None,
            "win_rate_pct": None, "profit_factor": None, "expectancy": None,
            "maximum_drawdown_pct": None, "sharpe": None, "sortino": None,
            "average_holding_days": None, "trading_costs": None,
            "reason": wfo_reason if not windows else "see phase3_walk_forward.csv",
        })
        if windows:
            oos_result_rows[-1].update({
                "net_pnl": oos_metrics["net_pnl"],
                "return_pct": oos_metrics["net_return_pct"],
                "win_rate_pct": oos_metrics["win_rate_pct"],
                "profit_factor": oos_metrics["profit_factor"],
                "expectancy": oos_metrics["expectancy_per_trade"],
                "maximum_drawdown_pct": oos_metrics["maximum_drawdown_pct"],
                "sharpe": oos_metrics["sharpe"],
                "sortino": oos_metrics["sortino"],
                "average_holding_days": oos_metrics["average_holding_days"],
                "trading_costs": oos_metrics["trading_costs"],
            })

    pd.DataFrame(strategy_rows).to_csv(result_dir / "phase3_strategy_results.csv", index=False)
    walk_frame.to_csv(result_dir / "phase3_walk_forward.csv", index=False)
    pd.DataFrame(oos_result_rows).to_csv(result_dir / "walk_forward_strategy_results.csv", index=False)
    pd.DataFrame(yearly_rows).to_csv(result_dir / "phase3_yearly_results.csv", index=False)
    pd.DataFrame(monthly_rows).to_csv(result_dir / "phase3_monthly_results.csv", index=False)
    pd.DataFrame(regime_rows).to_csv(result_dir / "phase3_regime_results.csv", index=False)
    pd.concat(symbol_frames, ignore_index=True).to_csv(result_dir / "phase3_symbol_contribution.csv", index=False) if symbol_frames else pd.DataFrame().to_csv(result_dir / "phase3_symbol_contribution.csv", index=False)
    pd.concat(sector_frames, ignore_index=True).to_csv(result_dir / "phase3_sector_contribution.csv", index=False) if sector_frames else pd.DataFrame().to_csv(result_dir / "phase3_sector_contribution.csv", index=False)
    pd.DataFrame(cost_rows).to_csv(result_dir / "phase3_cost_sensitivity.csv", index=False)
    pd.DataFrame(exit_rows).to_csv(result_dir / "phase3_exit_sensitivity.csv", index=False)
    pd.DataFrame(sizing_rows).to_csv(result_dir / "phase3_position_sizing.csv", index=False)
    pd.DataFrame(portfolio_rows).to_csv(result_dir / "phase3_portfolio_results.csv", index=False)
    pd.DataFrame(portfolio_monthly_rows).to_csv(result_dir / "phase3_portfolio_monthly.csv", index=False)
    pd.DataFrame(correlation_rows).to_csv(result_dir / "phase3_strategy_correlation.csv", index=False)
    pd.DataFrame(stability_rows).to_csv(result_dir / "phase3_parameter_stability.csv", index=False)
    pd.DataFrame(all_trade_rows).to_csv(result_dir / "phase3_available_window_trades.csv", index=False)
    pd.DataFrame(milestone_rows).to_csv(result_dir / "phase3_capital_milestones.csv", index=False)
    _write_json(result_dir / "phase3_monte_carlo.json", monte_carlo)
    _write_json(result_dir / "data_manifest.json", manifest)
    _write_json(result_dir / "phase3_run_manifest.json", {
        "status": "COMPLETED_WITH_HISTORY_LIMITATION",
        "created_at": now.isoformat(),
        "software_version": __version__,
        "period": {"start": start, "end": end, "sessions": len(benchmark)},
        "eligible_equity_symbols": len(eligible),
        "strategies_run": [strategy.name for strategy in strategies],
        "available_window_trades": {name: len(items) for name, items in trades_by_strategy.items()},
        "total_available_window_trades": sum(map(len, trades_by_strategy.values())),
        "walk_forward_windows": len(windows),
        "walk_forward_status": "INSUFFICIENT DATA" if not windows else "COMPLETED",
        "walk_forward_requested_window_months": {"train": 24, "validation": 6, "test": 6, "roll_step": 6},
        "monte_carlo": "not run because no OOS window is estimable; full-sample trades were not substituted",
        "options": "NOT IMPLEMENTED — Phase 3 boundary",
        "live_execution": "NOT IMPLEMENTED — Phase 3 boundary",
        "warnings": manifest["known_biases"],
    })

    _write_html_report(
        result_dir / "phase3_validation_report.html",
        manifest,
        pd.DataFrame(strategy_rows),
        walk_frame,
        pd.DataFrame(yearly_rows),
        pd.DataFrame(monthly_rows),
        pd.DataFrame(regime_rows),
        pd.DataFrame(stability_rows),
        pd.DataFrame(cost_rows),
        pd.DataFrame(exit_rows),
        pd.DataFrame(sizing_rows),
        pd.DataFrame(portfolio_rows),
        pd.concat(symbol_frames, ignore_index=True) if symbol_frames else pd.DataFrame(),
        pd.concat(sector_frames, ignore_index=True) if sector_frames else pd.DataFrame(),
        pd.DataFrame(milestone_rows),
        monte_carlo,
        wfo_reason if not windows else "Complete windows were generated; see walk-forward artifacts.",
    )
    print(f"Phase 3 artifacts: {result_dir}")
    print(f"History: {start} to {end}; {len(eligible)} equities; EMA200 ready {first_signal_ready}; walk-forward windows: {len(windows)}")
    print(f"Available-window closed trades across strategy runs: {sum(map(len, trades_by_strategy.values()))}")
    return result_dir


def _table(frame: pd.DataFrame, max_rows: int = 200) -> str:
    if frame.empty:
        return "<p>No estimable rows.</p>"
    safe = frame.head(max_rows).copy()
    return safe.to_html(index=False, border=0, classes="data", escape=True, na_rep="—")


def _write_html_report(
    path: Path,
    manifest: dict[str, Any],
    strategy: pd.DataFrame,
    walk: pd.DataFrame,
    yearly: pd.DataFrame,
    monthly: pd.DataFrame,
    regimes: pd.DataFrame,
    stability: pd.DataFrame,
    costs: pd.DataFrame,
    exits: pd.DataFrame,
    sizing: pd.DataFrame,
    portfolios: pd.DataFrame,
    symbols: pd.DataFrame,
    sectors: pd.DataFrame,
    milestones: pd.DataFrame,
    monte_carlo: dict[str, Any],
    wfo_reason: str,
) -> None:
    files = [
        "phase3_strategy_results.csv", "phase3_walk_forward.csv", "walk_forward_strategy_results.csv",
        "phase3_yearly_results.csv", "phase3_monthly_results.csv", "phase3_regime_results.csv",
        "phase3_symbol_contribution.csv", "phase3_sector_contribution.csv", "phase3_parameter_stability.csv",
        "phase3_cost_sensitivity.csv", "phase3_exit_sensitivity.csv", "phase3_position_sizing.csv",
        "phase3_portfolio_results.csv", "phase3_portfolio_monthly.csv", "phase3_strategy_correlation.csv",
        "phase3_capital_milestones.csv", "phase3_monte_carlo.json", "data_manifest.json", "phase3_available_window_trades.csv",
    ]
    links = " ".join(f'<a href="{html.escape(item)}">{html.escape(item)}</a>' for item in files)
    sections = [
        ("Available-window strategy results", strategy),
        ("Rolling walk-forward results", walk),
        ("Year-by-year results", yearly),
        ("Monthly results and streak summary", monthly),
        ("Trend and volatility regimes", regimes),
        ("Parameter stability (one factor at a time)", stability),
        ("Cost and slippage sensitivity", costs),
        ("Exit sensitivity", exits),
        ("Position-sizing sensitivity", sizing),
        ("Combined portfolio results", portfolios),
        ("Capital milestones", milestones),
        ("Symbol concentration", symbols),
        ("Sector concentration", sectors),
    ]
    body = "\n".join(f"<section><h2>{html.escape(title)}</h2>{_table(frame)}</section>" for title, frame in sections)
    limitations = "".join(f"<li>{html.escape(item)}</li>" for item in manifest["known_biases"])
    short_history = ", ".join(
        f"{html.escape(item['symbol'])} ({item['rows']} rows)"
        for item in manifest.get("excluded_insufficient_history_symbols", [])
    ) or "none"
    trade_records = int(pd.to_numeric(strategy.get("trades", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    mc_json = html.escape(json.dumps(_jsonable(monte_carlo), indent=2, sort_keys=True))
    content = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Phase 3 Equity Strategy Validation</title>
<style>
body{{font:15px/1.5 -apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;color:#17212b;margin:32px;max-width:1400px}}
h1,h2{{color:#12344d}} section{{margin:36px 0}} .notice{{background:#fff4d7;padding:16px;border-left:5px solid #d89c16}}
.data{{border-collapse:collapse;width:100%;font-size:12px}} .data th,.data td{{border:1px solid #d8e0e6;padding:6px;text-align:right;white-space:nowrap}}
.data th{{background:#edf3f6;position:sticky;top:0}} .data td:first-child,.data th:first-child{{text-align:left}}
.scroll{{overflow-x:auto}} a{{margin-right:12px;line-height:2}}
</style></head><body>
<h1>Phase 3 — Multi-year Robustness, Walk-Forward &amp; Strategy Validation</h1>
<p>Local data period: <strong>{manifest['actual_start']}</strong> to <strong>{manifest['actual_end']}</strong> ·
{manifest['eligible_equity_symbols']} eligible equities · {manifest['benchmark_sessions']} benchmark sessions ·
{manifest['symbols_with_inspected_quality']} cached symbols audited.</p>
<p>{trade_records} closed trade records across the 11 independent strategy baselines (records can represent overlapping signals across strategies).</p>
<p>Excluded under the 220-row history floor: {short_history}. Current constituent data also contains one excluded placeholder; India VIX is unavailable.</p>
<div class="notice"><strong>History limitation:</strong> {html.escape(wfo_reason)}
No available-window full-sample result is labelled out-of-sample. All strategy research statuses remain INSUFFICIENT DATA.</div>
<h2>Data and bias limitations</h2><ul>{limitations}</ul>
<h2>Artifacts</h2><p>{links}</p>
{body}
<section><h2>Out-of-sample Monte Carlo</h2><p>No simulations were run because no complete walk-forward test window exists. The JSON below records zero eligible OOS trades and the 30-trade minimum.</p><pre>{mc_json}</pre></section>
<footer><p>Research diagnostics only. Historical backtest results do not guarantee future performance. No options, live trading, or broker orders are included.</p></footer>
</body></html>"""
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    run()
