from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from nse_research.config import AppConfig
from nse_research.paper.trader import active_paper_positions, paper_portfolio_drawdown, paper_realized_pnl, record_paper_candidates, update_paper_positions
from nse_research.scanner.scanner import _apply_portfolio_limits
from nse_research.utils.dates import resolve_backtest_period


def test_default_period_uses_latest_two_complete_calendar_months() -> None:
    index = pd.bdate_range("2026-06-01", "2026-09-25")
    start, end = resolve_backtest_period(index, 2, now=datetime(2026, 9, 25, 17, 0, tzinfo=ZoneInfo("Asia/Kolkata")))
    assert start.isoformat() == "2026-07-01"
    assert end.isoformat() == "2026-08-31"


def test_paper_journal_waits_for_next_bar_and_closes_stop_first(tmp_path) -> None:
    config = AppConfig.load("config.yaml")
    database = tmp_path / "paper.sqlite"
    signal_time = pd.Timestamp("2026-09-23")
    candidate = {
        "signal_time": signal_time,
        "symbol": "TEST.NS",
        "strategy": "trend_following",
        "direction": "LONG",
        "entry_estimate": 100.0,
        "stop_loss": 99.0,
        "target": 102.0,
        "quantity": 10,
        "maximum_loss": 100.0,
        "signal_score": 80.0,
        "reason": "fixture",
    }
    assert record_paper_candidates(database, [candidate]) == 1
    assert record_paper_candidates(database, [candidate]) == 0
    assert len(active_paper_positions(database)) == 1
    bars = pd.DataFrame({
        "open": [100.0, 100.0], "high": [101.0, 103.0],
        "low": [99.5, 98.0], "close": [100.0, 100.5], "volume": [1, 1],
    }, index=pd.DatetimeIndex(["2026-09-23", "2026-09-24"]))
    assert update_paper_positions(database, {"TEST.NS": bars}, config) == 1
    with __import__("sqlite3").connect(database) as connection:
        status, exit_reason, pnl = connection.execute("SELECT status, reason_exit, pnl FROM paper_signals").fetchone()
    assert status == "CLOSED"
    assert exit_reason == "stop"
    assert pnl < 0
    assert active_paper_positions(database) == []
    assert paper_realized_pnl(database, "2026-09-24") < 0


def test_paper_portfolio_admission_respects_daily_loss_sector_and_position_limits(tmp_path) -> None:
    config = AppConfig.load("config.yaml")
    config.universe.symbols_file = str(tmp_path / "universe.csv")
    pd.DataFrame({"Symbol": ["AAA", "BBB"], "Industry": ["Finance", "Finance"]}).to_csv(config.universe.symbols_file, index=False)
    candidates = [
        {"symbol": name, "strategy": "trend_following", "signal_time": pd.Timestamp("2026-09-25"),
         "entry_estimate": 100.0, "quantity": 250, "maximum_loss": 100.0,
         "direction": "LONG", "signal_score": 80.0}
        for name in ["AAA.NS", "BBB.NS"]
    ]
    accepted, rejected = _apply_portfolio_limits(candidates, [], {}, config, 100000, [], 0)
    assert len(accepted) == 1
    assert accepted[0]["sector"] == "Finance"
    assert any(row["reason"] == "sector exposure limit reached" for row in rejected)

    blocked, daily_rejections = _apply_portfolio_limits(candidates, [], {}, config, 100000, [], -2000)
    assert blocked == []
    assert all(row["reason"] == "daily loss limit reached in paper portfolio" for row in daily_rejections)


def test_paper_portfolio_drawdown_halts_new_entries(tmp_path) -> None:
    database = tmp_path / "paper.sqlite"
    candidate = {
        "signal_time": pd.Timestamp("2026-09-23"), "symbol": "TEST.NS", "strategy": "trend_following",
        "direction": "LONG", "entry_estimate": 100.0, "stop_loss": 99.0, "target": 102.0,
        "quantity": 1, "maximum_loss": 1.0, "signal_score": 80.0, "reason": "fixture",
    }
    record_paper_candidates(database, [candidate])
    with __import__("sqlite3").connect(database) as connection:
        connection.execute("UPDATE paper_signals SET status='CLOSED', exit_time='2026-09-24', pnl=-10000 WHERE symbol='TEST.NS'")
    config = AppConfig.load("config.yaml")
    drawdown = paper_portfolio_drawdown(database, 100000, {}, config)
    assert abs(drawdown - 0.10) < 1e-9
    config.universe.symbols_file = str(tmp_path / "universe.csv")
    pd.DataFrame({"Symbol": ["AAA"], "Industry": ["IT"]}).to_csv(config.universe.symbols_file, index=False)
    candidate_row = [{
        "symbol": "AAA.NS", "strategy": "trend_following", "signal_time": pd.Timestamp("2026-09-25"),
        "entry_estimate": 100.0, "quantity": 10, "maximum_loss": 100.0, "direction": "LONG",
    }]
    accepted, rejected = _apply_portfolio_limits(candidate_row, [], {}, config, 100000, [], 0, drawdown)
    assert accepted == []
    assert rejected[0]["reason"] == "portfolio drawdown limit reached in paper portfolio"
