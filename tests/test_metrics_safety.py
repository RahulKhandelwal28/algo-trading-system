from datetime import datetime, timedelta

from nse_research.config import AppConfig
from nse_research.execution.broker import LiveExecutionGuard
from nse_research.reporting.performance import performance_metrics
from nse_research.backtest.validation import monte_carlo_trade_order
from nse_research.reporting.report import json_safe
from nse_research.reporting.report import make_comparison


def test_metrics_separate_win_rate_expectancy_drawdown_and_costs() -> None:
    now = datetime(2026, 9, 1)
    trades = [
        {"entry_time": now, "exit_time": now + timedelta(days=1), "net_pnl": 100, "gross_pnl": 120, "costs": 20, "risk_budget": 100, "holding_period": 1, "turnover": 1000},
        {"entry_time": now + timedelta(days=2), "exit_time": now + timedelta(days=3), "net_pnl": -150, "gross_pnl": -130, "costs": 20, "risk_budget": 100, "holding_period": 1, "turnover": 2000},
    ]
    result = performance_metrics(trades, [{"timestamp": now, "equity": 100000}, {"timestamp": now + timedelta(days=3), "equity": 99950}], 100000)
    assert result["trades"] == 2
    assert result["win_rate_pct"] == 50
    assert result["expectancy_per_trade"] == -25
    assert result["trading_costs"] == 40
    assert result["gross_pnl"] == -10
    assert result["payoff_ratio"] == 100 / 150
    assert result["turnover"] == 3000
    assert result["maximum_drawdown_pct"] < 0


def test_live_execution_fails_closed_when_disabled(tmp_path) -> None:
    config = AppConfig.load("config.yaml")
    guard = LiveExecutionGuard(config, str(tmp_path))
    try:
        guard.place({"symbol": "TEST.NS"})
    except RuntimeError as exc:
        assert "enabled is false" in str(exc)
    else:
        raise AssertionError("live execution guard did not fail closed")


def test_monte_carlo_refuses_tiny_samples() -> None:
    result, samples = monte_carlo_trade_order([], 100000, 0.01, simulations=5000)
    assert result["status"] == "insufficient_sample"
    assert samples.empty


def test_monte_carlo_reports_profit_and_capital_milestone_probabilities() -> None:
    trades = [{"net_pnl": 100, "risk_amount": 100} for _ in range(30)]
    summary, samples = monte_carlo_trade_order(trades, 100000, 0.01, simulations=200, seed=3)
    assert summary["status"] == "computed_trade_order_permutations"
    assert summary["probability_of_ending_profitable_pct"] == 100
    assert summary["capital_milestones"]["1.25x"]["probability_reached_pct"] == 100
    assert summary["capital_milestones"]["2.00x"]["probability_reached_pct"] == 0
    assert len(samples) == 200


def test_json_manifest_serializes_python_dates_and_nonfinite_values() -> None:
    from datetime import date

    result = json_safe({"day": date(2026, 9, 25), "score": float("inf")})
    assert result == {"day": "2026-09-25", "score": None}


def test_strategy_comparison_is_factual_and_has_no_subjective_rank_score() -> None:
    rows = [{
        "strategy": "trend_following", "initial_capital": 100000, "trades": 4,
        "win_rate_pct": 50, "average_win": 100, "average_loss": -50,
        "payoff_ratio": 2, "profit_factor": 2, "expectancy_per_trade": 25,
        "gross_pnl": 100, "net_pnl": 100, "net_return_pct": 0.1,
        "maximum_drawdown_pct": -1, "sharpe": 0.2, "sortino": 0.3,
        "trading_costs": 20, "turnover": 10000,
    }]
    table, selected = make_comparison(rows, 100000)
    assert selected is None
    assert "risk_adjusted_score" not in table
    assert {"average_win", "average_loss", "profit_factor", "expectancy_per_trade", "maximum_drawdown_pct", "trading_costs"} <= set(table.columns)
