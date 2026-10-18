"""Leakage-aware walk-forward and robustness-analysis helpers."""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable

import numpy as np
import pandas as pd

from nse_research.config import CostSettings
from nse_research.reporting.performance import performance_metrics


def _add_months(value: date, months: int) -> date:
    return (pd.Timestamp(value) + pd.DateOffset(months=months)).date()


def rolling_walk_forward_windows(
    start: date,
    end: date,
    train_months: int = 24,
    validation_months: int = 6,
    test_months: int = 6,
    step_months: int = 6,
) -> list[dict[str, date]]:
    """Make contiguous rolling windows; incomplete test windows are omitted."""
    if min(train_months, validation_months, test_months, step_months) <= 0:
        raise ValueError("walk-forward window lengths must be positive")
    windows: list[dict[str, date]] = []
    train_start = start
    while True:
        train_end = _add_months(train_start, train_months) - pd.Timedelta(days=1)
        validation_start = train_end + pd.Timedelta(days=1)
        validation_end = _add_months(validation_start, validation_months) - pd.Timedelta(days=1)
        test_start = validation_end + pd.Timedelta(days=1)
        test_end = _add_months(test_start, test_months) - pd.Timedelta(days=1)
        if test_end > end:
            break
        windows.append({
            "train_start": train_start,
            "train_end": train_end,
            "validation_start": validation_start,
            "validation_end": validation_end,
            "test_start": test_start,
            "test_end": test_end,
        })
        train_start = _add_months(train_start, step_months)
    return windows


def select_training_candidate(
    training_metrics: Iterable[dict[str, Any]],
    baseline_parameters: dict[str, Any],
    min_trades: int = 30,
) -> tuple[dict[str, Any], str]:
    """Select only from training rows; otherwise keep the predeclared baseline.

    Eligible alternatives must have the requested training sample, positive
    expectancy, and profit factor above one. Sorting is explicit and limited to
    training expectancy, then PF, drawdown proximity to zero, then parameters.
    Validation and test rows are deliberately not accepted by this function.
    """
    eligible = []
    for row in training_metrics:
        trades = int(row.get("trades", 0) or 0)
        expectancy = pd.to_numeric(pd.Series([row.get("expectancy_per_trade")]), errors="coerce").iloc[0]
        profit_factor = pd.to_numeric(pd.Series([row.get("profit_factor")]), errors="coerce").iloc[0]
        if trades >= min_trades and pd.notna(expectancy) and expectancy > 0 and pd.notna(profit_factor) and profit_factor > 1:
            eligible.append(row)
    if not eligible:
        return dict(baseline_parameters), "baseline_fallback_insufficient_or_negative_training_sample"
    eligible.sort(key=lambda row: (
        -float(row["expectancy_per_trade"]),
        -float(row["profit_factor"]),
        abs(float(row.get("maximum_drawdown_pct", 0.0) or 0.0)),
        str(row.get("parameters", {})),
    ))
    return dict(eligible[0].get("parameters", baseline_parameters)), "selected_on_training_expectancy_only"


def scale_cost_settings(settings: CostSettings, multiplier: float) -> CostSettings:
    """Return a copy with all explicit fee and slippage assumptions scaled."""
    if multiplier <= 0:
        raise ValueError("cost multiplier must be positive")
    values = settings.model_dump()
    scaled = {
        key: value * multiplier
        for key, value in values.items()
        if key.endswith("_rate") or key in {"brokerage_cap_per_order", "delivery_dp_charge_per_sell", "slippage_bps"}
    }
    return settings.model_copy(update=scaled)


def classify_volatility(realized_vol_annualized: float | None, india_vix: float | None = None) -> str:
    """Classify realized annualized volatility plus VIX using declared cutoffs."""
    if india_vix is not None and np.isfinite(india_vix) and india_vix >= 25:
        return "HIGH_VOLATILITY"
    if realized_vol_annualized is None or not np.isfinite(realized_vol_annualized):
        return "UNKNOWN"
    if realized_vol_annualized >= 0.30:
        return "HIGH_VOLATILITY"
    if realized_vol_annualized <= 0.15:
        return "LOW_VOLATILITY"
    return "NORMAL_VOLATILITY"


def metrics_for_trades(trades: Iterable[dict[str, Any]], initial_capital: float) -> dict[str, Any]:
    """Calculate portfolio metrics for a closed trade subset."""
    ordered = sorted(list(trades), key=lambda trade: pd.Timestamp(trade["exit_time"]))
    curve = []
    running = initial_capital
    for trade in ordered:
        running += float(trade.get("net_pnl", 0.0) or 0.0)
        curve.append({"timestamp": trade["exit_time"], "equity": running})
    return performance_metrics(ordered, curve, initial_capital)


def losing_or_winning_streak(values: Iterable[float], winning: bool) -> int:
    best = current = 0
    for value in values:
        qualifies = value > 0 if winning else value < 0
        current = current + 1 if qualifies else 0
        best = max(best, current)
    return best


def symbol_contribution(trades: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Summarize per-symbol P&L and positive-profit concentration."""
    frame = pd.DataFrame(list(trades))
    columns = ["symbol", "trades", "win_rate_pct", "net_pnl", "positive_pnl", "positive_pnl_contribution_pct"]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame["net_pnl"] = pd.to_numeric(frame["net_pnl"], errors="coerce").fillna(0.0)
    grouped = frame.groupby("symbol", dropna=False).agg(
        trades=("net_pnl", "size"),
        win_rate_pct=("net_pnl", lambda values: float((values > 0).mean() * 100)),
        net_pnl=("net_pnl", "sum"),
    ).reset_index()
    grouped["positive_pnl"] = grouped["net_pnl"].clip(lower=0)
    denominator = float(grouped["positive_pnl"].sum())
    grouped["positive_pnl_contribution_pct"] = grouped["positive_pnl"] / denominator * 100 if denominator else np.nan
    return grouped[columns].sort_values("net_pnl", ascending=False, kind="stable").reset_index(drop=True)


def portfolio_milestones(
    equity_curve: Iterable[dict[str, Any]], initial_capital: float, multiples: tuple[float, ...] = (1.25, 1.5, 1.75, 2.0)
) -> list[dict[str, Any]]:
    """Return first observed threshold times without extrapolating beyond data."""
    curve = pd.DataFrame(list(equity_curve))
    if not curve.empty:
        curve["equity"] = pd.to_numeric(curve["equity"], errors="coerce")
        curve["timestamp"] = pd.to_datetime(curve["timestamp"], errors="coerce")
        curve = curve.dropna(subset=["equity", "timestamp"]).sort_values("timestamp")
    rows = []
    for multiple in multiples:
        threshold = initial_capital * multiple
        reached = curve.loc[curve["equity"] >= threshold] if not curve.empty else pd.DataFrame()
        first = reached.iloc[0]["timestamp"] if not reached.empty else None
        rows.append({
            "target_multiple": multiple,
            "target_capital": threshold,
            "first_reached": first.isoformat() if first is not None else None,
            "status": "ACHIEVED" if first is not None else "NOT ACHIEVED",
        })
    return rows
