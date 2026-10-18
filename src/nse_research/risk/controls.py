"""Position sizing that caps cash risk and portfolio exposure."""

from __future__ import annotations

import math
from dataclasses import dataclass

from nse_research.config import RiskSettings


@dataclass(frozen=True)
class SizingResult:
    quantity: int
    risk_budget: float
    estimated_risk: float
    reason: str | None = None


def position_size(
    equity: float,
    entry: float,
    stop_distance: float,
    risk: RiskSettings,
    available_exposure: float,
    estimated_cost_per_share: float = 0.0,
    fixed_costs: float = 0.0,
) -> SizingResult:
    if min(equity, entry, stop_distance) <= 0:
        return SizingResult(0, max(0.0, equity * risk.risk_per_trade), 0.0, "invalid equity, entry, or stop distance")
    budget = equity * risk.risk_per_trade
    loss_per_share = stop_distance + estimated_cost_per_share
    by_risk = math.floor(max(0.0, budget - fixed_costs) / loss_per_share)
    by_value = math.floor(min(available_exposure, equity * risk.max_single_position_fraction) / entry)
    quantity = max(0, min(by_risk, by_value))
    if quantity == 0:
        return SizingResult(0, budget, 0.0, "risk-sized quantity is zero or portfolio exposure is exhausted")
    return SizingResult(quantity, budget, quantity * loss_per_share + fixed_costs)


def portfolio_drawdown_tripwire(risk: RiskSettings) -> float:
    """Trip before the configured drawdown cap to reserve one risk unit for bar gaps."""
    reserve = min(risk.risk_per_trade, risk.max_portfolio_drawdown * 0.1)
    return max(0.0, risk.max_portfolio_drawdown - reserve)


def kill_switch_active(path: str) -> bool:
    """A present kill-switch file blocks live order admission."""
    from pathlib import Path

    return Path(path).exists()
