from datetime import date

import numpy as np
import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.backtest.engine import BacktestEngine
from nse_research.backtest.phase3_validation import (
    classify_volatility,
    rolling_walk_forward_windows,
    scale_cost_settings,
    select_training_candidate,
    symbol_contribution,
)
from nse_research.backtest.validation import monte_carlo_trade_order
from nse_research.config import AppConfig, CostSettings, RiskSettings
from nse_research.strategies.base import Candidate
from nse_research.strategies.composite import CombinedEquityStrategy


def test_walk_forward_windows_are_contiguous_and_chronological() -> None:
    windows = rolling_walk_forward_windows(
        date(2020, 1, 1), date(2024, 12, 31), train_months=24,
        validation_months=6, test_months=6, step_months=6,
    )
    assert len(windows) >= 3
    first = windows[0]
    assert first["train_start"] == date(2020, 1, 1)
    assert first["validation_start"] == first["train_end"] + pd.Timedelta(days=1)
    assert first["test_start"] == first["validation_end"] + pd.Timedelta(days=1)
    for left, right in zip(windows, windows[1:]):
        assert left["test_end"] < right["test_start"]
        assert right["train_start"] > left["train_start"]


def test_parameter_selection_uses_only_training_metrics_and_falls_back_safely() -> None:
    training_rows = [
        {"parameters": {"adx": 15}, "trades": 45, "expectancy_per_trade": 10, "profit_factor": 1.2, "maximum_drawdown_pct": -8},
        {"parameters": {"adx": 20}, "trades": 44, "expectancy_per_trade": 25, "profit_factor": 1.3, "maximum_drawdown_pct": -9},
    ]
    chosen, reason = select_training_candidate(training_rows, {"adx": 20}, min_trades=30)
    assert chosen == {"adx": 20}
    assert reason == "selected_on_training_expectancy_only"

    # Hypothetical validation/test outcomes cannot enter this selector's inputs.
    training_rows[0]["test_expectancy"] = 100000
    training_rows[0]["validation_expectancy"] = 100000
    chosen_again, _ = select_training_candidate(training_rows, {"adx": 20}, min_trades=30)
    assert chosen_again == chosen
    fallback, fallback_reason = select_training_candidate(
        [{"parameters": {"adx": 10}, "trades": 4, "expectancy_per_trade": 100, "profit_factor": 8}],
        {"adx": 20}, min_trades=30,
    )
    assert fallback == {"adx": 20}
    assert fallback_reason.startswith("baseline_fallback")


def test_cost_sensitivity_scales_explicit_fees_and_slippage() -> None:
    base = CostSettings()
    doubled = scale_cost_settings(base, 2)
    assert doubled.delivery_stt_rate == base.delivery_stt_rate * 2
    assert doubled.brokerage_cap_per_order == base.brokerage_cap_per_order * 2
    assert doubled.delivery_dp_charge_per_sell == base.delivery_dp_charge_per_sell * 2
    assert doubled.slippage_bps == base.slippage_bps * 2


def test_regime_classifier_separates_low_normal_high_and_vix_override() -> None:
    assert classify_volatility(0.12) == "LOW_VOLATILITY"
    assert classify_volatility(0.20) == "NORMAL_VOLATILITY"
    assert classify_volatility(0.31) == "HIGH_VOLATILITY"
    assert classify_volatility(0.10, india_vix=28) == "HIGH_VOLATILITY"
    assert classify_volatility(None) == "UNKNOWN"


def test_symbol_contribution_handles_concentration_and_no_positive_pnl() -> None:
    trades = [
        {"symbol": "AAA", "net_pnl": 100.0},
        {"symbol": "AAA", "net_pnl": 50.0},
        {"symbol": "BBB", "net_pnl": -25.0},
    ]
    summary = symbol_contribution(trades).set_index("symbol")
    assert summary.loc["AAA", "trades"] == 2
    assert summary.loc["AAA", "win_rate_pct"] == 100
    assert summary.loc["AAA", "positive_pnl_contribution_pct"] == 100
    losing = symbol_contribution([{"symbol": "CCC", "net_pnl": -1.0}])
    assert np.isnan(losing.loc[0, "positive_pnl_contribution_pct"])


class _Member:
    intraday_only = False
    max_holding_bars = 5

    def __init__(self, name: str, score: float) -> None:
        self.name = name
        self.score = score

    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        return [Candidate(features.index[-2], 1, 1.0, 1.5, self.score, "BULL", "fixture")]


def test_portfolio_combination_executes_one_shared_candidate_and_keeps_source() -> None:
    config = AppConfig.load("config.yaml")
    config.risk.max_open_positions = 1
    index = pd.bdate_range("2026-09-21", periods=2)
    bars = pd.DataFrame({
        "open": [100.0, 100.0], "high": [100.5, 100.5],
        "low": [99.5, 99.5], "close": [100.0, 100.0], "volume": [700000, 700000],
    }, index=index)
    strategy = CombinedEquityStrategy("alpha+beta", (_Member("alpha", 60), _Member("beta", 90)))
    result = BacktestEngine(config, CostModel(config.costs)).run(
        strategy, {"TEST.NS": bars}, None, 100000, index[-1].date(), index[-1].date(), exit_mode="stop_loss"
    )
    assert len(result.trades) == 1
    assert result.trades[0]["strategy"] == "beta"
    assert result.trades[0]["reason_for_signal"].startswith("source=beta;")
    assert abs(result.final_capital - (100000 + result.trades[0]["net_pnl"])) < 0.02


def test_requested_two_percent_position_risk_is_available_for_sensitivity() -> None:
    # The ordinary app config remains capped at 1%; Phase 3 creates a temporary
    # sensitivity-only model copy to inspect the requested 2% scenario.
    risk = RiskSettings().model_copy(update={"risk_per_trade": 0.02})
    assert risk.risk_per_trade == 0.02


def test_monte_carlo_uses_only_samples_with_at_least_thirty_trades() -> None:
    too_few = [{"net_pnl": 10.0, "risk_amount": 100.0} for _ in range(29)]
    summary, samples = monte_carlo_trade_order(too_few, 100000, 0.01, simulations=5000, seed=13)
    assert summary["status"] == "insufficient_sample"
    assert samples.empty

    adequate = [{"net_pnl": 10.0 if index % 3 else -20.0, "risk_amount": 100.0} for index in range(30)]
    summary, samples = monte_carlo_trade_order(adequate, 100000, 0.01, simulations=5000, seed=13)
    assert summary["status"] == "computed_trade_order_permutations"
    assert summary["simulations"] == 5000
    assert len(samples) == 5000
    assert set(summary["capital_milestones"]) == {"1.25x", "1.50x", "1.75x", "2.00x"}
