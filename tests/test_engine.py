from datetime import date

import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.backtest.engine import BacktestEngine
from nse_research.config import AppConfig
from nse_research.strategies.base import Candidate


class OneSignalStrategy:
    name = "test_strategy"
    max_holding_bars = 10
    intraday_only = False

    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        signal_at = features.index[-2]
        return [Candidate(signal_at, 1, 1.0, 1.5, 80.0, "BULL", "test-only signal")]


class FirstBarSignalStrategy:
    name = "test_strategy"
    max_holding_bars = 10
    intraday_only = False

    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        return [Candidate(features.index[0], 1, 1.0, 1.5, 80.0, "BULL", "test-only signal")]


class DifferentTimingStrategy:
    name = "test_strategy"
    max_holding_bars = 10
    intraday_only = False

    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        marker = int(features.volume.iloc[-1])
        signal_index = 0 if marker == 1 else 1
        return [Candidate(features.index[signal_index], 1, 1.0, 1.5, 80.0, "BULL", "test-only signal")]


class ShortHoldingStrategy(FirstBarSignalStrategy):
    max_holding_bars = 2


class LongHoldingStrategy(FirstBarSignalStrategy):
    max_holding_bars = 50


class IntradaySignalStrategy(FirstBarSignalStrategy):
    name = "vwap_intraday"
    max_holding_bars = 1
    intraday_only = True


class InvalidSignalStrategy(FirstBarSignalStrategy):
    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        return [Candidate(features.index[0], 1, 0.0, 1.5, 80.0, "BULL", "invalid stop")]


def test_signal_is_filled_at_next_bar_open_and_same_bar_ambiguity_stops_first() -> None:
    config = AppConfig.load("config.yaml")
    index = pd.bdate_range("2026-09-23", periods=2)
    bars = pd.DataFrame({
        "open": [99.0, 100.0], "high": [100.0, 102.0],
        "low": [98.0, 98.0], "close": [99.0, 100.0], "volume": [700000, 700000],
    }, index=index)
    engine = BacktestEngine(config, CostModel(config.costs))
    result = engine.run(OneSignalStrategy(), {"TEST.NS": bars}, None, 100000, index[-1].date(), index[-1].date())
    assert len(result.trades) == 1
    trade = result.trades[0]
    assert trade["entry_time"] == index[-1]
    assert trade["entry"] > 100.0  # adverse entry slippage
    assert trade["reason_for_exit"] == "stop"  # stop-first when stop and target both trade
    assert trade["net_pnl"] < 0
    assert trade["costs"] > 0
    assert abs(result.final_capital - (result.initial_capital + trade["net_pnl"])) < 0.02
    assert result.equity_curve[-1]["equity"] == result.final_capital


def test_empty_test_range_produces_no_trades() -> None:
    config = AppConfig.load("config.yaml")
    bars = pd.DataFrame({"open": [10], "high": [11], "low": [9], "close": [10], "volume": [1]}, index=pd.DatetimeIndex(["2026-09-01"]))
    result = BacktestEngine(config, CostModel(config.costs)).run(OneSignalStrategy(), {"TEST.NS": bars}, None, 100000, date(2026, 9, 2), date(2026, 9, 3))
    assert result.trades == []


def test_portfolio_drawdown_kill_closes_open_position() -> None:
    config = AppConfig.load("config.yaml")
    config.risk.max_portfolio_drawdown = 0.0005
    index = pd.bdate_range("2026-09-23", periods=2)
    bars = pd.DataFrame({
        "open": [99.0, 100.0], "high": [100.0, 100.2],
        "low": [98.0, 99.5], "close": [99.0, 99.5], "volume": [700000, 700000],
    }, index=index)
    result = BacktestEngine(config, CostModel(config.costs)).run(
        OneSignalStrategy(), {"TEST.NS": bars}, None, 100000, index[-1].date(), index[-1].date()
    )
    assert result.trades[0]["reason_for_exit"] == "portfolio_drawdown_kill"
    assert result.equity_curve[-1]["equity"] == result.final_capital


def _three_bars(rows: list[tuple[float, float, float, float]], index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame({
        "open": [row[0] for row in rows], "high": [row[1] for row in rows],
        "low": [row[2] for row in rows], "close": [row[3] for row in rows],
        "volume": [700000] * len(rows),
    }, index=index)


def test_gap_through_stop_exits_at_gap_open_and_reconciles_cash() -> None:
    config = AppConfig.load("config.yaml")
    index = pd.bdate_range("2026-09-21", periods=3)
    bars = _three_bars([(100, 101, 99, 100), (100, 100.5, 99.5, 100), (90, 92, 89, 91)], index)
    result = BacktestEngine(config, CostModel(config.costs)).run(
        FirstBarSignalStrategy(), {"TEST.NS": bars}, None, 100000, index[1].date(), index[2].date()
    )
    assert len(result.trades) == 1
    assert result.trades[0]["reason_for_exit"] == "gap_stop"
    assert result.trades[0]["exit"] < result.trades[0]["stop"]
    assert abs(result.final_capital - (100000 + result.trades[0]["net_pnl"])) < 0.02


def test_fixed_r_target_is_recorded_as_exit_reason() -> None:
    config = AppConfig.load("config.yaml")
    index = pd.bdate_range("2026-09-21", periods=3)
    bars = _three_bars([(100, 101, 99, 100), (100, 100.5, 99.5, 100), (100, 104, 99.5, 103)], index)
    result = BacktestEngine(config, CostModel(config.costs)).run(
        FirstBarSignalStrategy(), {"TEST.NS": bars}, None, 100000, index[1].date(), index[2].date()
    )
    assert len(result.trades) == 1
    assert result.trades[0]["reason_for_exit"] == "target"
    assert result.trades[0]["gross_pnl"] > 0


def test_sector_cap_and_max_positions_block_excess_entries() -> None:
    config = AppConfig.load("config.yaml")
    index = pd.bdate_range("2026-09-21", periods=2)
    bars = _three_bars([(100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100)], index)
    config.risk.max_open_positions = 5
    engine = BacktestEngine(config, CostModel(config.costs))
    result = engine.run(
        FirstBarSignalStrategy(), {"AAA.NS": bars, "BBB.NS": bars.copy()}, None,
        100000, index[1].date(), index[1].date(), {"AAA.NS": "FINANCE", "BBB.NS": "FINANCE"}, "stop_loss",
    )
    assert len(result.trades) == 1
    assert any("sector exposure" in row["reason"] for row in result.rejected)


def test_max_positions_gate_is_enforced() -> None:
    config = AppConfig.load("config.yaml")
    config.risk.max_open_positions = 1
    index = pd.bdate_range("2026-09-21", periods=2)
    bars = _three_bars([(100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100)], index)
    result = BacktestEngine(config, CostModel(config.costs)).run(
        FirstBarSignalStrategy(), {"AAA.NS": bars, "BBB.NS": bars.copy()}, None,
        100000, index[1].date(), index[1].date(), {"AAA.NS": "FINANCE", "BBB.NS": "IT"}, "stop_loss",
    )
    assert len(result.trades) == 1
    assert any(row["reason"] == "maximum open positions reached" for row in result.rejected)


def test_daily_loss_drawdown_and_invalid_signal_gates_report_reasons() -> None:
    config = AppConfig.load("config.yaml")
    engine = BacktestEngine(config, CostModel(config.costs))
    index = pd.bdate_range("2026-09-21", periods=2)
    candidate = Candidate(index[0], 1, 1.0, 1.5, 80.0, "BULL", "test")
    strategy = FirstBarSignalStrategy()
    day_loss = engine._admission_reason("A", candidate, pd.Timestamp(index[1]), 98000, 100000, 100000, -2000, {}, strategy, "IT", {})
    drawdown = engine._admission_reason("A", candidate, pd.Timestamp(index[1]), 91000, 100000, 91000, 0, {}, strategy, "IT", {})
    assert day_loss == "daily loss limit reached"
    assert drawdown == "portfolio drawdown limit reached"

    bars = _three_bars([(100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100)], index)
    invalid = BacktestEngine(config, CostModel(config.costs)).run(
        InvalidSignalStrategy(), {"TEST.NS": bars}, None, 100000, index[1].date(), index[1].date()
    )
    assert invalid.trades == []
    assert invalid.rejected[0]["reason"] == "invalid stop distance or risk/reward"


def test_next_open_sizing_does_not_use_same_candle_close_for_existing_holdings() -> None:
    config = AppConfig.load("config.yaml")
    config.risk.max_sector_exposure_fraction = 0.8
    index = pd.bdate_range("2026-09-21", periods=3)
    first = _three_bars([(100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (100, 200, 100, 200)], index)
    first["volume"] = 1
    second = _three_bars([(100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100)], index)
    second["volume"] = 2
    result = BacktestEngine(config, CostModel(config.costs)).run(
        DifferentTimingStrategy(), {"AAA.NS": first, "BBB.NS": second}, None,
        100000, index[1].date(), index[2].date(), {"AAA.NS": "A", "BBB.NS": "B"}, "stop_loss",
    )
    second_trade = next(trade for trade in result.trades if trade["symbol"] == "BBB.NS")
    assert second_trade["quantity"] < 300  # sizing sees AAA at its 100 open, not its 200 close


def test_partial_profit_closes_a_leg_and_keeps_one_aggregated_trade() -> None:
    config = AppConfig.load("config.yaml")
    index = pd.bdate_range("2026-09-21", periods=3)
    bars = _three_bars([(100, 100.5, 99.5, 100), (100, 101.5, 99.5, 101.2), (101, 101.2, 100.2, 100.8)], index)
    result = BacktestEngine(config, CostModel(config.costs)).run(
        LongHoldingStrategy(), {"TEST.NS": bars}, None, 100000,
        index[1].date(), index[2].date(), exit_mode="partial_trailing",
    )
    assert len(result.trades) == 1
    assert result.trades[0]["partial_exits"] == 1
    assert result.trades[0]["quantity"] > 1
    assert result.trades[0]["exit_cost"] > 0  # partial and final exit fees both included
    assert abs(result.final_capital - (100000 + result.trades[0]["net_pnl"])) < 0.02


def test_atr_trailing_stop_only_tightens_and_then_exits() -> None:
    config = AppConfig.load("config.yaml")
    index = pd.bdate_range("2026-08-20", periods=17)
    rows = [(100, 101, 99, 100), (100, 100.5, 99.5, 100)]
    for close in range(101, 115):
        rows.append((close - 0.2, close + 1, close - 1, close))
    rows.append((114, 115, 108, 109))
    bars = _three_bars(rows, index)
    result = BacktestEngine(config, CostModel(config.costs)).run(
        LongHoldingStrategy(), {"TEST.NS": bars}, None, 100000,
        index[1].date(), index[-1].date(), exit_mode="atr_trailing",
    )
    trade = result.trades[0]
    assert trade["reason_for_exit"] == "stop"
    assert trade["last_stop"] > trade["stop"]


def test_maximum_holding_period_and_intraday_session_end_exits() -> None:
    config = AppConfig.load("config.yaml")
    days = pd.bdate_range("2026-09-21", periods=4)
    swing = _three_bars([(100, 100.5, 99.5, 100)] * 4, days)
    held = BacktestEngine(config, CostModel(config.costs)).run(
        ShortHoldingStrategy(), {"TEST.NS": swing}, None, 100000,
        days[1].date(), days[3].date(), exit_mode="max_holding",
    )
    assert held.trades[0]["reason_for_exit"] == "time_exit"

    intraday_index = pd.DatetimeIndex([
        "2026-09-25 14:35", "2026-09-25 14:45", "2026-09-25 15:25",
    ])
    intraday = _three_bars([(100, 100.2, 99.8, 100), (100, 100.2, 99.8, 100), (100, 100.2, 99.8, 100)], intraday_index)
    day = intraday_index[0].date()
    day_result = BacktestEngine(config, CostModel(config.costs)).run(
        IntradaySignalStrategy(), {"TEST.NS": intraday}, None, 100000, day, day, exit_mode="stop_loss",
    )
    assert day_result.trades[0]["reason_for_exit"] == "session_end"
