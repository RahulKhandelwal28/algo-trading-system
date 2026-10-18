"""Trade-level and portfolio-level performance statistics."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import numpy as np
import pandas as pd


def performance_metrics(
    trades: Iterable[dict[str, Any]],
    equity_curve: Iterable[dict[str, Any]],
    initial_capital: float,
) -> dict[str, Any]:
    trade_list = list(trades)
    curve = pd.DataFrame(list(equity_curve))
    frame = pd.DataFrame(trade_list)
    if not frame.empty:
        frame["net_pnl"] = pd.to_numeric(frame["net_pnl"], errors="coerce").fillna(0.0)
        if "exit_time" in frame:
            frame = frame.sort_values("exit_time", kind="stable").reset_index(drop=True)
    wins = frame.loc[frame["net_pnl"] > 0, "net_pnl"] if not frame.empty else pd.Series(dtype=float)
    losses = frame.loc[frame["net_pnl"] < 0, "net_pnl"] if not frame.empty else pd.Series(dtype=float)
    total_net = float(frame["net_pnl"].sum()) if not frame.empty else 0.0
    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = abs(float(losses.sum())) if not losses.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)
    average_win = float(wins.mean()) if len(wins) else 0.0
    average_loss = float(losses.mean()) if len(losses) else 0.0
    payoff_ratio = average_win / abs(average_loss) if average_loss else (float("inf") if average_win > 0 else 0.0)
    gross_pnl = float(pd.to_numeric(frame.get("gross_pnl", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not frame.empty else 0.0
    turnover = float(pd.to_numeric(frame.get("turnover", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not frame.empty else 0.0
    final_capital = initial_capital + total_net

    if not curve.empty and "equity" in curve:
        equity = pd.to_numeric(curve["equity"], errors="coerce").dropna()
        equity = pd.concat([pd.Series([initial_capital]), equity], ignore_index=True)
        running_peak = equity.cummax()
        drawdowns = equity / running_peak - 1
        max_drawdown = float(drawdowns.min())
        average_drawdown = float(drawdowns.mean())
        if "timestamp" in curve:
            dates = pd.to_datetime(curve["timestamp"], errors="coerce")
            daily = pd.DataFrame({"date": dates.dt.date, "equity": pd.to_numeric(curve["equity"], errors="coerce")}).dropna()
            daily = daily.groupby("date", as_index=False).last()
            daily_returns = daily["equity"].pct_change().dropna()
        else:
            daily_returns = pd.Series(dtype=float)
    else:
        max_drawdown = 0.0
        average_drawdown = 0.0
        daily_returns = pd.Series(dtype=float)
    sharpe = _sharpe(daily_returns)
    sortino = _sortino(daily_returns)
    if not frame.empty:
        entry_dates = pd.to_datetime(frame["entry_time"], errors="coerce")
        exit_dates = pd.to_datetime(frame["exit_time"], errors="coerce")
        span_days = max(1, int((exit_dates.max() - entry_dates.min()).total_seconds() / 86400))
        duration = pd.to_numeric(frame.get("holding_period", pd.Series(dtype=float)), errors="coerce")
        daily_pnl = frame.assign(day=exit_dates.dt.date).groupby("day")["net_pnl"].sum()
        best_day = float(daily_pnl.max()) if len(daily_pnl) else 0.0
        worst_day = float(daily_pnl.min()) if len(daily_pnl) else 0.0
        losing_streak = _longest_streak((frame["net_pnl"] < 0).tolist(), True)
        winning_streak = _longest_streak((frame["net_pnl"] > 0).tolist(), True)
    else:
        span_days = 1
        duration = pd.Series(dtype=float)
        best_day = worst_day = 0.0
        losing_streak = winning_streak = 0
    if not curve.empty and "timestamp" in curve:
        curve_times = pd.to_datetime(curve["timestamp"], errors="coerce").dropna()
        if len(curve_times):
            span_days = max(1, int((curve_times.max() - curve_times.min()).total_seconds() / 86400))
    cagr = (final_capital / initial_capital) ** (365.25 / span_days) - 1 if final_capital > 0 else -1.0
    return {
        "initial_capital": initial_capital,
        "final_capital": final_capital,
        "net_pnl": total_net,
        "net_return_pct": (final_capital / initial_capital - 1) * 100 if initial_capital else 0.0,
        "annualized_return_pct": cagr * 100,
        "trades": len(frame),
        "trade_sample_status": "LOW SAMPLE (<30 trades)" if len(frame) < 30 else "30+ trades; descriptive only",
        "winning_trades": int((frame["net_pnl"] > 0).sum()) if not frame.empty else 0,
        "losing_trades": int((frame["net_pnl"] < 0).sum()) if not frame.empty else 0,
        "win_rate_pct": float((frame["net_pnl"] > 0).mean() * 100) if not frame.empty else None,
        "average_win": average_win,
        "average_loss": average_loss,
        "payoff_ratio": payoff_ratio,
        "profit_factor": profit_factor,
        "expectancy_per_trade": float(frame["net_pnl"].mean()) if not frame.empty else 0.0,
        "gross_pnl": gross_pnl,
        "turnover": turnover,
        "average_r_multiple": _average_r(frame),
        "maximum_drawdown_pct": max_drawdown * 100,
        "average_drawdown_pct": average_drawdown * 100,
        "sharpe": sharpe,
        "sortino": sortino,
        "longest_winning_streak": winning_streak,
        "longest_losing_streak": losing_streak,
        "average_holding_days": float(duration.mean()) if len(duration) else 0.0,
        "trading_costs": float(pd.to_numeric(frame.get("costs", pd.Series(dtype=float)), errors="coerce").fillna(0).sum()) if not frame.empty else 0.0,
        "best_trade": float(frame["net_pnl"].max()) if not frame.empty else 0.0,
        "worst_trade": float(frame["net_pnl"].min()) if not frame.empty else 0.0,
        "best_day": best_day,
        "worst_day": worst_day,
        "win_rate_90pct_plus": bool((frame["net_pnl"] > 0).mean() >= 0.90) if not frame.empty else False,
        "win_rate_sample_sufficient": len(frame) >= 30,
        "capital_multiple": final_capital / initial_capital if initial_capital else 0.0,
        "max_gross_exposure": float(pd.to_numeric(curve.get("gross_exposure", pd.Series(dtype=float)), errors="coerce").fillna(0).max()) if not curve.empty else 0.0,
    }


def _sharpe(returns: pd.Series) -> float:
    if len(returns) < 2 or returns.std(ddof=1) == 0:
        return 0.0
    return float(returns.mean() / returns.std(ddof=1) * np.sqrt(252))


def _sortino(returns: pd.Series) -> float:
    if len(returns) < 2:
        return 0.0
    downside = returns.loc[returns < 0]
    deviation = downside.std(ddof=1) if len(downside) > 1 else 0.0
    if not deviation:
        return 0.0
    return float(returns.mean() / deviation * np.sqrt(252))


def _longest_streak(values: list[bool], target: bool) -> int:
    best = current = 0
    for value in values:
        current = current + 1 if value == target else 0
        best = max(best, current)
    return best


def _average_r(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    risk_column = "risk_amount" if "risk_amount" in frame else "risk_budget"
    if risk_column not in frame:
        return 0.0
    multiples = frame["net_pnl"] / pd.to_numeric(frame[risk_column], errors="coerce").replace(0, np.nan)
    return float(multiples.replace([np.inf, -np.inf], np.nan).mean())
