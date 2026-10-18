"""Chronological fixed-parameter walk-forward slices and trade-order Monte Carlo."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from nse_research.reporting.performance import performance_metrics


def walk_forward_slices(
    trades: list[dict[str, Any]],
    start: date,
    end: date,
    initial_capital: float,
) -> dict[str, Any]:
    """Report fixed-parameter chronological slices; no fitting occurs in these slices."""
    begin = pd.Timestamp(start)
    finish = pd.Timestamp(end) + pd.Timedelta(days=1)
    duration = finish - begin
    boundaries = [begin, begin + duration * 0.6, begin + duration * 0.8, finish]
    frame = pd.DataFrame(trades)
    results: dict[str, Any] = {
        "method": "fixed-parameter chronological 60/20/20 slices; no tuning was performed",
        "parameter_fitting": False,
        "slices": {},
        "warning": None,
    }
    if frame.empty:
        results["warning"] = "No trades available for walk-forward analysis."
        return results
    entry = pd.to_datetime(frame["entry_time"], errors="coerce")
    names = ["train", "validation", "out_of_sample_test"]
    for index, name in enumerate(names):
        mask = (entry >= boundaries[index]) & (entry < boundaries[index + 1])
        subset = frame.loc[mask].to_dict("records")
        ordered = pd.DataFrame(subset)
        if not ordered.empty:
            ordered = ordered.sort_values("exit_time")
            running = initial_capital + ordered["net_pnl"].cumsum()
            curve = [
                {"timestamp": stamp, "equity": value}
                for stamp, value in zip(pd.to_datetime(ordered["exit_time"]), running)
            ]
        else:
            curve = []
        metric = performance_metrics(subset, curve, initial_capital)
        metric["status"] = "usable" if len(subset) >= 10 else "insufficient trades for a stable estimate"
        results["slices"][name] = {
            "start": boundaries[index].date().isoformat(),
            "end": (boundaries[index + 1] - pd.Timedelta(days=1)).date().isoformat(),
            "metrics": metric,
        }
    if len(frame) < 30:
        results["warning"] = "INSUFFICIENT SAMPLE SIZE for meaningful walk-forward inference (<30 trades)."
    return results


def monte_carlo_trade_order(
    trades: list[dict[str, Any]],
    initial_capital: float,
    risk_fraction: float,
    simulations: int = 5000,
    seed: int = 20260925,
    max_portfolio_drawdown: float = 0.10,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Permute historical trade R-multiples and compound at the fixed risk fraction."""
    frame = pd.DataFrame(trades)
    if frame.empty or len(frame) < 30:
        return {
            "status": "insufficient_sample",
            "trades": len(frame),
            "required_trades": 30,
            "simulations": simulations,
            "warning": "INSUFFICIENT SAMPLE SIZE; Monte Carlo percentiles would be misleading.",
        }, pd.DataFrame()
    risk_column = "risk_amount" if "risk_amount" in frame else "risk_budget"
    risk = pd.to_numeric(frame[risk_column], errors="coerce").replace(0, np.nan)
    multiples = (pd.to_numeric(frame["net_pnl"], errors="coerce") / risk).replace([np.inf, -np.inf], np.nan).dropna().to_numpy()
    if len(multiples) < 30:
        return {"status": "insufficient_sample", "trades": len(multiples), "simulations": simulations}, pd.DataFrame()
    rng = np.random.default_rng(seed)
    outcomes: list[dict[str, float]] = []
    for _ in range(simulations):
        equity = initial_capital
        maximum_capital = equity
        peak = equity
        maximum_drawdown = 0.0
        stopped = False
        for multiple in rng.permutation(multiples):
            equity = max(0.0, equity * (1 + risk_fraction * multiple))
            peak = max(peak, equity)
            maximum_capital = max(maximum_capital, equity)
            if peak:
                maximum_drawdown = max(maximum_drawdown, 1 - equity / peak)
            if maximum_drawdown >= max_portfolio_drawdown:
                stopped = True
                break
        outcomes.append({"final_capital": equity, "maximum_capital": maximum_capital, "maximum_drawdown_pct": maximum_drawdown * 100, "stopped_after_drawdown": float(stopped)})
    samples = pd.DataFrame(outcomes)
    final = samples["final_capital"]
    drawdown = samples["maximum_drawdown_pct"]
    percentiles = {f"p{percentile}": float(final.quantile(percentile / 100)) for percentile in [5, 25, 50, 75, 95]}
    summary = {
        "status": "computed_trade_order_permutations",
        "trades": len(multiples),
        "simulations": simulations,
        "seed": seed,
        "drawdown_halt_threshold_pct": max_portfolio_drawdown * 100,
        "final_capital_percentiles": percentiles,
        "maximum_drawdown_percentiles": {f"p{p}": float(drawdown.quantile(p / 100)) for p in [5, 25, 50, 75, 95]},
        "probability_of_losing_capital_pct": float((final < initial_capital).mean() * 100),
        "probability_of_ending_profitable_pct": float((final > initial_capital).mean() * 100),
        "probability_of_drawdown_halt_pct": float(samples["stopped_after_drawdown"].mean() * 100),
        "capital_milestones": {
            f"{multiple:.2f}x": {
                "probability_reached_pct": float((samples["maximum_capital"] >= initial_capital * multiple).mean() * 100),
                "probability_final_at_or_above_pct": float((final >= initial_capital * multiple).mean() * 100),
            }
            for multiple in [1.25, 1.5, 1.75, 2.0]
        },
        "simulation_method": "5,000 (or configured count) random permutations of observed net trade R-multiples, compounded at fixed risk fraction; stops a path at the configured drawdown tripwire",
    }
    return summary, samples
