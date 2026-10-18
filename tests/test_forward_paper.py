from __future__ import annotations

import csv
import json
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from nse_research.config import AppConfig
from nse_research.data.providers import HistoricalDataProvider
from nse_research.forward.data import LocalEodCsvProvider, validate_daily_bars
from nse_research.forward.baseline import (
    BaselineMismatchError,
    create_baseline_manifest,
    validate_baseline_manifest,
)
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.forward.engine import DailyPaperTrader, _ensure_schema, load_local_universe
from nse_research.forward.reports import update_cumulative_research
from nse_research.strategies.base import Candidate


@pytest.fixture(autouse=True)
def _paper_safety_environment(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")


class MemoryProvider(HistoricalDataProvider):
    provider_name = "synthetic_fixture"

    def __init__(self, bars: dict[str, pd.DataFrame]):
        self.bars = bars

    def history(self, symbol, start, end, interval="1d"):
        if symbol not in self.bars:
            raise FileNotFoundError(f"fixture missing {symbol}")
        frame = self.bars[symbol]
        mask = [(pd.Timestamp(stamp).date() >= start and pd.Timestamp(stamp).date() <= end) for stamp in frame.index]
        return frame.loc[mask].copy()


class BrokerCapableProvider(MemoryProvider):
    def __init__(self, bars):
        super().__init__(bars)
        self.order_method_called = False

    def place(self, order):
        self.order_method_called = True
        raise AssertionError("the paper workflow must never call a broker method")


class FixedSignalStrategy:
    name = "trend_following"
    intraday_only = False
    max_holding_bars = 20

    def evaluate(self, features, regimes):
        if features.empty:
            return []
        timestamp = features.index[-1]
        return [Candidate(timestamp, 1, 2.0, 1.5, 80.0, str(regimes.iloc[-1]), "synthetic deterministic signal")]


class ScheduledSignalStrategy:
    name = "trend_following"
    intraday_only = False
    max_holding_bars = 20

    def __init__(self, schedule):
        self.schedule = schedule

    def evaluate(self, features, regimes):
        if features.empty:
            return []
        timestamp = features.index[-1]
        symbol_label = "TEST2" if float(features["close"].iloc[-1]) > 150 else "TEST"
        if symbol_label not in self.schedule.get(timestamp.date(), set()):
            return []
        return [Candidate(timestamp, 1, 2.0, 1.5, 80.0, str(regimes.iloc[-1]), "scheduled synthetic signal")]


class FailOnceStrategy(FixedSignalStrategy):
    def __init__(self):
        self.failed = False

    def evaluate(self, features, regimes):
        if not self.failed:
            self.failed = True
            raise RuntimeError("synthetic one-time strategy failure")
        return super().evaluate(features, regimes)


def _bars(dates, *, start=100.0, volume=1_000_000, last_open=None, last_high=None, last_low=None, last_close=None):
    close = [start + i * 0.02 for i in range(len(dates))]
    rows = []
    for i, day in enumerate(dates):
        value = close[i]
        op = value
        high, low = value + 0.5, value - 0.5
        if i == len(dates) - 1:
            if last_close is not None:
                value = last_close
                close[i] = value
            op = last_open if last_open is not None else value
            high = last_high if last_high is not None else value + 0.5
            low = last_low if last_low is not None else value - 0.5
        rows.append({"open": op, "high": high, "low": low, "close": value, "volume": volume})
    return pd.DataFrame(rows, index=pd.DatetimeIndex(dates))


def _config(tmp_path: Path) -> AppConfig:
    config = AppConfig.load(Path(__file__).parents[1] / "config.yaml")
    config.forward_paper.enabled = True
    config.universe.symbols_file = str(tmp_path / "universe.csv")
    config.universe.min_history_rows = 220
    config.universe.min_avg_volume = 1
    config.universe.min_avg_traded_value = 1
    pd.DataFrame({"Symbol": ["TEST", "TEST2"], "Industry": ["Technology", "Technology"]}).to_csv(config.universe.symbols_file, index=False)
    create_baseline_manifest(
        config,
        root=Path(__file__).parents[1],
        path=_baseline_path(config),
        phase3c_source_path=None,
        created_at=datetime(2026, 9, 26, tzinfo=timezone.utc),
    )
    return config


def _baseline_path(config: AppConfig) -> Path:
    return Path(config.universe.symbols_file).parent / "PHASE3C_BASELINE.json"


def _provider_bars(dates, symbols=("TEST.NS",)):
    bars = {
        "^NSEI": _bars(dates, start=20000, volume=0),
        "^INDIAVIX": _bars(dates, start=15, volume=0),
    }
    for symbol in symbols:
        bars[symbol] = _bars(dates)
    return MemoryProvider(bars)


def test_fresh_daily_data_is_accepted_and_stale_data_fails_closed():
    days = pd.bdate_range("2026-01-01", periods=3)
    bars = _bars(days)
    valid, fresh = validate_daily_bars(bars, provider="fixture", symbol="TEST.NS", market_date=days[-1].date())
    assert fresh.valid and fresh.freshness_status == "FRESH"
    assert len(valid) == 3
    stale, stale_check = validate_daily_bars(bars.iloc[:-1], provider="fixture", symbol="TEST.NS", market_date=days[-1].date())
    assert stale.empty
    assert not stale_check.valid and stale_check.freshness_status == "DATA_STALE"


@pytest.mark.parametrize("defect", ["duplicate", "invalid_ohlc", "missing_value", "nonfinite", "abnormal_jump"])
def test_invalid_daily_data_is_rejected(defect):
    days = pd.bdate_range("2026-01-01", periods=3)
    bars = _bars(days)
    if defect == "duplicate":
        bars = pd.concat([bars, bars.iloc[[-1]]])
    elif defect == "invalid_ohlc":
        bars.iloc[-1, bars.columns.get_loc("high")] = bars.iloc[-1]["low"] - 1
    elif defect == "missing_value":
        bars.iloc[-1, bars.columns.get_loc("close")] = float("nan")
    elif defect == "nonfinite":
        bars.iloc[-1, bars.columns.get_loc("close")] = float("inf")
    elif defect == "abnormal_jump":
        bars.iloc[-1, bars.columns.get_loc("close")] = 200
        bars.iloc[-1, bars.columns.get_loc("high")] = 201
    clean, check = validate_daily_bars(bars, provider="fixture", symbol="TEST.NS", market_date=days[-1].date())
    assert clean.empty
    assert not check.valid
    assert check.freshness_status == "DATA_INVALID"


def test_local_provider_reads_only_the_explicit_forward_csv_directory(tmp_path):
    market = tmp_path / "market"
    index = tmp_path / "indexes"
    market.mkdir()
    index.mkdir()
    file = market / "TEST.NS_1d.csv"
    file.write_text("timestamp,open,high,low,close,volume\n2026-01-02,10,11,9,10,100\n", encoding="utf-8")
    provider = LocalEodCsvProvider(market, index)
    bars = provider.history("TEST.NS", date(2026, 1, 2), date(2026, 1, 2))
    assert list(bars.columns) == ["open", "high", "low", "close", "volume"]
    assert bars.iloc[0]["close"] == 10
    assert provider.provider_for("TEST.NS") == "local_eod_csv"
    assert provider.provider_for("^NSEI") == "nse_official_index_csv"
    with pytest.raises(FileNotFoundError):
        provider.history("MISSING.NS", date(2026, 1, 2), date(2026, 1, 2))


def test_licensed_nse_canonical_dataset_is_read_without_import_or_network(tmp_path):
    dataset = tmp_path / "licensed" / "nse_cm_eod_dataset.csv"
    dataset.parent.mkdir()
    dataset.write_text(
        "Symbol,Series,Date,Open Price,High Price,Low Price,Close Price,Total Traded Quantity\n"
        "TEST,EQ,02-Jan-2026,10,11,9,10.5,1000\n"
        "TEST,BE,02-Jan-2026,10,11,9,10.5,50\n",
        encoding="utf-8",
    )
    provider = LocalEodCsvProvider(tmp_path / "unused", tmp_path / "indexes", licensed_dataset_path=dataset)
    bars = provider.history("TEST.NS", date(2026, 1, 2), date(2026, 1, 2))
    assert len(bars) == 1
    assert bars.iloc[0]["close"] == 10.5
    assert bars.iloc[0]["volume"] == 1000
    assert provider.provider_name == "licensed_nse_cm_eod"
    assert provider.provider_for("TEST.NS") == "licensed_nse_cm_eod"


def test_forward_index_adapter_preserves_duplicate_dates_for_validation(tmp_path):
    market = tmp_path / "market"
    indexes = tmp_path / "indexes"
    market.mkdir()
    indexes.mkdir()
    (indexes / "index_NSEI.csv").write_text(
        "Index Name,Date,Open,High,Low,Close\n"
        "NIFTY 50,25-Sep-2026,100,102,99,101\n"
        "NIFTY 50,25-Sep-2026,100,103,99,102\n",
        encoding="utf-8",
    )
    provider = LocalEodCsvProvider(market, indexes)
    bars = provider.history("^NSEI", date(2026, 9, 25), date(2026, 9, 25))
    assert len(bars) == 2
    clean, health = validate_daily_bars(bars, provider=provider.provider_for("^NSEI"),
        symbol="^NSEI", market_date=date(2026, 9, 25))
    assert clean.empty
    assert health.freshness_status == "DATA_INVALID"
    assert health.duplicate_records == 2


def test_local_universe_loader_never_needs_a_network_request(tmp_path):
    path = tmp_path / "universe.csv"
    path.write_text("Symbol,Industry\nTEST,Software\nDUMMYHEG,Other\nTEST,Software\n", encoding="utf-8")
    assert load_local_universe(path) == ["TEST.NS"]


def test_forward_workflow_persists_signal_then_fills_exits_and_accounts_pnl(tmp_path):
    dates = pd.bdate_range("2025-01-01", periods=251)
    target = dates[-1].date()
    first_provider = _provider_bars(dates)
    first_provider.bars["TEST.NS"] = _bars(dates, last_close=100.0)
    config = _config(tmp_path)
    historical_fixture = tmp_path / "research" / "historical" / "phase3_result.csv"
    historical_fixture.parent.mkdir(parents=True)
    historical_fixture.write_text("protected historical result\n", encoding="utf-8")
    db = tmp_path / "forward" / "portfolio" / "paper.sqlite"
    trader = DailyPaperTrader(
        config, first_provider, db, 50000, output_root=tmp_path / "forward", provider_name="synthetic_fixture",
        clock=lambda: datetime(2026, 9, 25, 11, tzinfo=timezone.utc), strategies=[FixedSignalStrategy()],
        baseline_manifest=_baseline_path(config),
    )

    day_one = trader.run(target, ["TEST.NS"])
    assert day_one["signals"]["generated"] == 1
    assert day_one["signals"]["accepted"] == 1
    assert day_one["orders"]["created"] == 1
    assert day_one["orders"]["filled"] == 0  # EOD signal is filled no earlier than the next session.
    with sqlite3.connect(db) as connection:
        status = connection.execute("SELECT status FROM orders").fetchone()[0]
        signal_count = connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    assert status == "CREATED"
    assert signal_count == 1

    next_dates = dates.append(pd.DatetimeIndex([dates[-1] + pd.offsets.BDay(1)]))
    next_date = next_dates[-1].date()
    next_bars = {
        "^NSEI": _bars(next_dates, start=20000, volume=0),
        "^INDIAVIX": _bars(next_dates, start=15, volume=0),
        "TEST.NS": _bars(next_dates, last_open=100.0, last_high=103.0, last_low=96.0, last_close=101.0),
    }
    next_trader = DailyPaperTrader(
        config, MemoryProvider(next_bars), db, 50000, output_root=tmp_path / "forward",
        provider_name="synthetic_fixture", clock=lambda: datetime(2026, 9, 26, 11, tzinfo=timezone.utc),
        strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config),
    )
    day_two = next_trader.run(next_date, ["TEST.NS"])
    assert day_two["orders"]["filled"] == 1
    assert day_two["signals"]["generated"] == 1
    assert day_two["orders"]["exited"] == 1
    assert day_two["closed_positions_today"][0]["realized_pnl"] < 0
    assert day_two["portfolio"]["realized_pnl_today"] < 0
    assert day_two["portfolio"]["ending_equity"] < 50000
    assert day_two["portfolio"]["position_count"] == 0
    assert day_two["report_path"]
    assert Path(day_two["report_path"]).is_file()
    assert Path(tmp_path / "forward" / "journal" / "daily_journal.jsonl").read_text().count("\n") == 2
    with sqlite3.connect(db) as connection:
        entry = connection.execute("SELECT status,simulated_fill_price,slippage,costs FROM orders WHERE order_kind='ENTRY'").fetchone()
        exit_order = connection.execute("SELECT status,costs FROM orders WHERE order_kind='EXIT'").fetchone()
        position = connection.execute("SELECT status,realized_pnl,holding_sessions FROM positions").fetchone()
    assert entry[0] == "FILLED" and entry[1] > 0 and entry[2] != 0 and entry[3] > 0
    assert exit_order[0] == "FILLED" and exit_order[1] > 0
    assert position[0] == "CLOSED" and position[2] == 1
    assert day_two["portfolio"]["drawdown"] > 0
    with (db.parent / "events" / f"{next_date.isoformat()}.jsonl").open(encoding="utf-8") as stream:
        day_two_events = [json.loads(line)["event_type"] for line in stream if line.strip()]
    assert day_two_events.index("ORDER_FILLED") < day_two_events.index("SIGNALS_GENERATED")
    assert day_two["historical_backtests_included"] is False
    assert historical_fixture.read_text(encoding="utf-8") == "protected historical result\n"
    assert day_two["live_trading_enabled"] is False and day_two["broker_contacted"] is False
    with sqlite3.connect(db) as connection:
        journal = connection.execute("SELECT payload_json FROM daily_journal ORDER BY trading_date").fetchall()
    assert len(journal) == 2
    assert '"signals_generated": 1' in journal[0][0]
    assert '"positions_closed": 1' in journal[1][0]


def test_daily_exit_prefers_stop_if_stop_and_target_are_both_touched_and_supports_target():
    stop_and_target = {"open": 100.0, "high": 103.0, "low": 96.0}
    target_only = {"open": 100.0, "high": 103.0, "low": 99.0}
    position = {"stop_loss": 97.0, "target": 102.0}
    assert DailyPaperTrader._exit_condition(position, pd.Series(stop_and_target), 1) == ("stop", 97.0)
    assert DailyPaperTrader._exit_condition(position, pd.Series(target_only), 1) == ("target", 102.0)


def test_existing_risk_engine_rejections_are_persisted_with_reason(tmp_path):
    dates = pd.bdate_range("2025-01-01", periods=251)
    config = _config(tmp_path)
    provider = _provider_bars(dates, symbols=("TEST.NS", "TEST2.NS"))
    trader = DailyPaperTrader(config, provider, tmp_path / "risk.sqlite", 50000,
        output_root=tmp_path / "forward", strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config))

    report = trader.run(dates[-1].date(), ["TEST.NS", "TEST2.NS"])

    assert report["signals"]["generated"] == 2
    assert report["signals"]["accepted"] == 1
    assert report["signals"]["rejected"] == 1
    with sqlite3.connect(tmp_path / "risk.sqlite") as connection:
        rejected = connection.execute("SELECT status,rejection_reason,risk_amount,risk_percentage FROM signals WHERE accepted=0").fetchone()
    assert rejected[0] == "REJECTED_RISK"
    assert rejected[1] == "sector exposure limit reached"
    assert rejected[2] is not None and rejected[3] is not None


def test_stale_symbol_is_journaled_as_data_stale_without_orders(tmp_path):
    days = pd.bdate_range("2025-01-01", periods=251)
    config = _config(tmp_path)
    bars = _provider_bars(days).bars
    bars["TEST.NS"] = bars["TEST.NS"].iloc[:-1]
    trader = DailyPaperTrader(config, MemoryProvider(bars), tmp_path / "stale.sqlite", 50000,
        output_root=tmp_path / "forward", strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config))

    report = trader.run(days[-1].date(), ["TEST.NS"])

    assert report["signals"]["generated"] == 0
    assert report["data"]["stale_symbols"] == ["TEST.NS"]
    assert report["orders"]["created"] == 0
    with sqlite3.connect(tmp_path / "stale.sqlite") as connection:
        status, reason = connection.execute("SELECT status,rejection_reason FROM signals").fetchone()
    assert status == "DATA_STALE" and "latest bar" in reason


def test_live_setting_refuses_to_start_even_for_paper_workflow(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=3)
    config = _config(tmp_path)
    config.live_trading.enabled = True
    trader = DailyPaperTrader(config, _provider_bars(dates), tmp_path / "live.sqlite", 50000,
        output_root=tmp_path / "forward", strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config))
    with pytest.raises(RuntimeError, match="LIVE_TRADING_ENABLED must remain false"):
        trader.run(dates[-1].date(), ["TEST.NS"])
    assert not (tmp_path / "live.sqlite").exists()


def test_broker_capable_provider_is_refused_without_calling_order_method(tmp_path):
    dates = pd.bdate_range("2026-01-01", periods=3)
    config = _config(tmp_path)
    provider = BrokerCapableProvider(_provider_bars(dates).bars)
    database = tmp_path / "broker.sqlite"
    trader = DailyPaperTrader(config, provider, database, 50000,
        output_root=tmp_path / "forward", strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config))
    with pytest.raises(RuntimeError, match="broker/live-order interface"):
        trader.run(dates[-1].date(), ["TEST.NS"])
    assert provider.order_method_called is False
    assert not database.exists()


def test_required_paper_environment_and_live_environment_guards(tmp_path, monkeypatch):
    days = pd.bdate_range("2026-01-01", periods=3)
    config = _config(tmp_path)
    database = tmp_path / "environment.sqlite"
    trader = DailyPaperTrader(config, _provider_bars(days), database, 50000,
        output_root=tmp_path / "forward", strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config))
    monkeypatch.delenv("PAPER_TRADING_ENABLED")
    with pytest.raises(RuntimeError, match="PAPER_TRADING_ENABLED=true is required"):
        trader.run(days[-1].date(), ["TEST.NS"])
    monkeypatch.setenv("PAPER_TRADING_ENABLED", "true")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    with pytest.raises(RuntimeError, match="LIVE_TRADING_ENABLED must remain false"):
        trader.run(days[-1].date(), ["TEST.NS"])
    assert not database.exists()


def test_future_market_date_is_rejected_before_creating_the_ledger(tmp_path):
    config = _config(tmp_path)
    dates = pd.bdate_range("2025-01-01", periods=3)
    database = tmp_path / "future.sqlite"
    trader = DailyPaperTrader(config, _provider_bars(dates), database, 50000,
        output_root=tmp_path / "forward", strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config))
    with pytest.raises(ValueError, match="is in the future"):
        trader.run(date.today() + timedelta(days=2), ["TEST.NS"])
    assert not database.exists()


def test_dashboard_api_is_read_only_and_filters_forward_signals(tmp_path):
    import json
    from http import HTTPStatus
    from types import SimpleNamespace
    from unittest.mock import Mock

    from nse_research.dashboard import dashboard_payload, make_handler
    from nse_research.forward.engine import _ensure_schema

    database = tmp_path / "forward" / "portfolio" / "paper.sqlite"
    _ensure_schema(database, 50000)
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(database) as connection:
        connection.execute(
            """INSERT INTO portfolio_snapshots
            (run_id,trading_date,starting_capital,cash,invested_capital,realized_pnl,unrealized_pnl,
             equity,daily_pnl,daily_return,cumulative_return,drawdown,maximum_drawdown,gross_exposure,
             sector_exposure_json,open_positions,pending_orders,created_at)
            VALUES ('run1','2025-01-02',50000,50000,0,0,0,50000,0,0,0,0,0,0,'{}',0,0,?)""",
            (now,),
        )
        connection.execute(
            "INSERT INTO health_snapshots(run_id,trading_date,payload_json,created_at) VALUES (?,?,?,?)",
            ("run1", "2025-01-02", json.dumps({"system_data_status": "FRESH", "provider": "test"}), now),
        )
        connection.execute(
            "INSERT INTO daily_journal(run_id,trading_date,payload_json,created_at) VALUES (?,?,?,?)",
            ("run1", "2025-01-02", json.dumps({"market_regime": "TRENDING"}), now),
        )
        connection.execute(
            """INSERT INTO signals
            (signal_id,trading_date,timestamp,symbol,strategy,direction,signal_price,stop_loss,target,
             risk_amount,reason,features_json,market_regime,sector,data_provider,freshness_status,
             accepted,status,payload_json)
            VALUES ('sig1','2025-01-02',?,'TEST.NS','trend_following','BUY',100,98,103,500,
                    'fixture','{}','TRENDING','Technology','fixture','FRESH',1,'ACCEPTED','{}')""",
            (now,),
        )

    overview = dashboard_payload(database, "/api/dashboard/overview")
    signals = dashboard_payload(database, "/api/dashboard/signals", {
        "date": ["2025-01-02"], "symbol": ["TEST.NS"], "status": ["accepted"]
    })["signals"]
    performance = dashboard_payload(database, "/api/dashboard/performance")
    strategy_view = dashboard_payload(database, "/api/dashboard/strategies")
    activity = dashboard_payload(database, "/api/dashboard/activity")
    handler = make_handler(database)
    response = SimpleNamespace(_send=Mock())
    handler.do_POST(response)
    assert overview["scope"] == "FORWARD PAPER"
    assert overview["live_trading_enabled"] is False
    assert [row["symbol"] for row in signals] == ["TEST.NS"]
    assert performance["backtest_included"] is False
    assert strategy_view["strategy_accounts_are_separate"] is True
    assert activity["scope"] == "FORWARD PAPER"
    dashboard_js = (Path(__file__).parents[1] / "forward" / "dashboard" / "app.js").read_text(encoding="utf-8")
    assert '"Forward Paper Trading"' in dashboard_js
    assert "Place order" not in dashboard_js and "place_order" not in dashboard_js
    response._send.assert_called_once()
    assert response._send.call_args.args[0] == HTTPStatus.METHOD_NOT_ALLOWED
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 0


def test_official_calendar_classifies_holidays_weekends_unknown_days_and_special_sessions():
    calendar = NseTradingCalendar(Path(__file__).parents[1] / "data" / "paper_trading" / "nse_cash_holidays_2026.csv")
    assert calendar.classify(date(2026, 1, 26), observed_market_data=False).status == "NON_TRADING_DAY"
    assert calendar.classify(date(2026, 9, 26), observed_market_data=False).status == "NON_TRADING_DAY"
    assert calendar.classify(date(2026, 9, 28), observed_market_data=False).status == "TRADING_DAY"
    assert calendar.classify(date(2026, 10, 1), observed_market_data=False).status == "DATA_UNAVAILABLE"
    assert calendar.classify(date(2026, 11, 8), observed_market_data=False).status == "DATA_UNAVAILABLE"
    assert calendar.classify(date(2026, 11, 8), observed_market_data=True).status == "TRADING_DAY"
    assert calendar.latest_completed_candidate(datetime(2026, 11, 9, 4, 30, tzinfo=timezone.utc)) == date(2026, 11, 8)
    assert calendar.latest_completed_candidate(datetime(2026, 11, 16, 4, 30, tzinfo=timezone.utc)) == date(2026, 11, 13)


def test_baseline_is_create_once_and_risk_drift_fails_closed(tmp_path):
    config = _config(tmp_path)
    path = _baseline_path(config)
    frozen_bytes = path.read_bytes()
    with pytest.raises(FileExistsError, match="refusing to replace frozen baseline"):
        create_baseline_manifest(config, root=Path(__file__).parents[1], path=path)
    config.risk.risk_per_trade += 0.001
    with pytest.raises(BaselineMismatchError, match="risk_configuration differs"):
        validate_baseline_manifest(config, root=Path(__file__).parents[1], path=path)
    assert path.read_bytes() == frozen_bytes


def test_five_day_forward_acceptance_lifecycle_and_duplicate_run_protection(tmp_path):
    days = pd.bdate_range("2024-01-02", periods=260)
    day1, day2, day3, day4, day5 = [stamp.date() for stamp in days[-5:]]
    test = _bars(days, start=100)
    test2 = _bars(days, start=200)
    day1_close = float(test.loc[pd.Timestamp(day1), "close"])
    test.loc[pd.Timestamp(day2), ["open", "high", "low", "close"]] = [day1_close, day1_close + 0.4, day1_close - 0.4, day1_close]
    test.loc[pd.Timestamp(day3), ["open", "high", "low", "close"]] = [day1_close + 0.1, day1_close + 4.0, day1_close - 0.1, day1_close + 3.4]
    test.loc[pd.Timestamp(day4), ["open", "high", "low", "close"]] = [day1_close + 3.5, day1_close + 4.1, day1_close + 3.2, day1_close + 3.8]
    test.loc[pd.Timestamp(day5), ["open", "high", "low", "close"]] = [day1_close + 3.8, day1_close + 4.2, day1_close + 3.5, day1_close + 3.9]
    provider = MemoryProvider({
        "^NSEI": _bars(days, start=20000, volume=0),
        "^INDIAVIX": _bars(days, start=15, volume=0),
        "TEST.NS": test,
        "TEST2.NS": test2,
    })
    config = _config(tmp_path)
    database = tmp_path / "forward" / "forward.sqlite"
    output = tmp_path / "results" / "phase3d_forward"
    strategy = ScheduledSignalStrategy({day1: {"TEST"}, day4: {"TEST", "TEST2"}})
    trader = DailyPaperTrader(
        config, provider, database, 50000, output_root=output, provider_name="synthetic_fixture",
        clock=lambda: datetime(2026, 9, 26, 11, tzinfo=timezone.utc), strategies=[strategy],
        baseline_manifest=_baseline_path(config),
    )

    first = trader.run(day1, ["TEST.NS", "TEST2.NS"])
    second = trader.run(day2, ["TEST.NS", "TEST2.NS"])
    third = trader.run(day3, ["TEST.NS", "TEST2.NS"])
    fourth = trader.run(day4, ["TEST.NS", "TEST2.NS"])
    fifth = trader.run(day5, ["TEST.NS", "TEST2.NS"])
    event_path = database.parent / "events" / f"{day5.isoformat()}.jsonl"
    prefix = event_path.read_text(encoding="utf-8").splitlines()
    repeated = trader.run(day5, ["TEST.NS", "TEST2.NS"])
    final_events = [json.loads(line) for line in event_path.read_text(encoding="utf-8").splitlines()]

    assert first["signals"]["generated"] == 1 and first["orders"]["created"] == 1
    assert second["orders"]["filled"] == 1 and second["portfolio"]["position_count"] == 1
    assert second["portfolio"]["drawdown"] > 0
    assert second["portfolio"]["cash"] + second["portfolio"]["market_value"] == pytest.approx(second["portfolio"]["ending_equity"], abs=0.02)
    assert third["orders"]["exited"] == 1 and third["portfolio"]["position_count"] == 0
    assert third["closed_positions_today"][0]["realized_pnl"] > 0
    assert third["portfolio"]["ending_equity"] == pytest.approx(50000 + third["portfolio"]["realized_pnl"], abs=0.02)
    assert fourth["signals"]["generated"] == 2
    assert fourth["signals"]["accepted"] == 1 and fourth["signals"]["rejected"] == 1
    assert fifth["orders"]["filled"] == 1
    assert repeated["duplicate_run_skipped"] is True
    assert len(final_events) == len(prefix) + 3  # run start, preflight and explicit duplicate skip
    assert final_events[-1]["event_type"] == "RUN_SKIPPED_DUPLICATE"

    with sqlite3.connect(database) as connection:
        entry_orders = connection.execute(
            "SELECT status,brokerage,stt,exchange_charges,gst,sebi_charges,stamp_duty,dp_charge,costs,filled_at FROM orders WHERE order_kind='ENTRY' ORDER BY trading_date"
        ).fetchall()
        exit_order = connection.execute(
            "SELECT status,brokerage,stt,exchange_charges,gst,sebi_charges,stamp_duty,dp_charge,costs FROM orders WHERE order_kind='EXIT'"
        ).fetchone()
        position = connection.execute("SELECT status,total_costs,realized_pnl FROM positions WHERE exit_date=?", (day3.isoformat(),)).fetchone()
        snapshot_count = connection.execute("SELECT COUNT(*) FROM portfolio_snapshots").fetchone()[0]
        journal_count = connection.execute("SELECT COUNT(*) FROM daily_journal").fetchone()[0]
        order_count = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        duplicate_signals = connection.execute(
            "SELECT COUNT(*) FROM (SELECT trading_date,symbol,strategy,COUNT(*) n FROM signals GROUP BY trading_date,symbol,strategy HAVING n>1)"
        ).fetchone()[0]
    assert len(entry_orders) == 2 and entry_orders[0][0] == "FILLED" and entry_orders[1][0] == "FILLED"
    assert entry_orders[0][1:8] and sum(entry_orders[0][1:8]) == pytest.approx(entry_orders[0][8], abs=0.01)
    assert entry_orders[0][9] is not None and exit_order[0] == "FILLED"
    assert sum(exit_order[1:8]) == pytest.approx(exit_order[8], abs=0.01)
    assert position[0] == "CLOSED" and position[1] > 0 and position[2] > 0
    assert snapshot_count == journal_count == 5 and order_count == 3 and duplicate_signals == 0
    expected_files = {
        "daily_report.json", "daily_report.md", "signals.csv", "orders.csv", "trades.csv",
        "positions.csv", "portfolio.csv", "risk_events.csv", "data_quality.json", "run_manifest.json",
    }
    for market_day in (day1, day2, day3, day4, day5):
        report_dir = output / market_day.isoformat()
        assert {path.name for path in report_dir.iterdir()} == expected_files
        assert not list(report_dir.glob("*.tmp"))
        manifest = json.loads((report_dir / "run_manifest.json").read_text(encoding="utf-8"))
        assert manifest["live_trading_enabled"] is False and manifest["broker_contacted"] is False
    run_manifest = json.loads((output / day5.isoformat() / "run_manifest.json").read_text(encoding="utf-8"))
    assert run_manifest["provider"] == "synthetic_fixture"
    exit_order_csv = output / day3.isoformat() / "orders.csv"
    with exit_order_csv.open(newline="", encoding="utf-8") as stream:
        assert {"brokerage", "stt", "exchange_charges", "gst", "sebi_charges", "stamp_duty", "dp_charge", "costs"}.issubset(
            set(next(csv.reader(stream)))
        )
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM signals WHERE accepted=0 AND trading_date=?", (day4.isoformat(),)).fetchone()[0] == 1
    risk_lines = (output / day4.isoformat() / "risk_events.csv").read_text(encoding="utf-8").splitlines()
    assert len(risk_lines) >= 2
    assert any(json.loads(line)["event_type"] == "RECONCILIATION_PASSED" for line in (database.parent / "events" / f"{day3.isoformat()}.jsonl").read_text().splitlines())
    research_doc = tmp_path / "PHASE3D_FORWARD_RESEARCH.md"
    update_cumulative_research(database, output, research_doc, phase3c_results=tmp_path / "no_historical_inputs")
    cumulative = research_doc.read_text(encoding="utf-8")
    assert "Trading sessions: 5" in cumulative and "Closed trades: 1" in cumulative
    assert "EARLY_FORWARD_SAMPLE" in cumulative
    assert (output / "monthly").is_dir()
    assert not (output / "forward_vs_phase3c_comparison.md").exists()


def test_failed_run_can_retry_without_rewriting_append_only_events_or_duplicating_orders(tmp_path):
    days = pd.bdate_range("2025-01-01", periods=251)
    market_day = days[-1].date()
    config = _config(tmp_path)
    database = tmp_path / "retry" / "forward.sqlite"
    output = tmp_path / "retry" / "reports"
    strategy = FailOnceStrategy()
    trader = DailyPaperTrader(
        config, _provider_bars(days), database, 50000, output_root=output,
        clock=lambda: datetime(2026, 9, 26, 11, tzinfo=timezone.utc), strategies=[strategy],
        baseline_manifest=_baseline_path(config),
    )
    with pytest.raises(RuntimeError, match="synthetic one-time strategy failure"):
        trader.run(market_day, ["TEST.NS"])
    event_path = database.parent / "events" / f"{market_day.isoformat()}.jsonl"
    original_lines = event_path.read_text(encoding="utf-8").splitlines()
    original_events = [json.loads(line) for line in original_lines]
    assert original_events[-1]["event_type"] == "RUN_FAILED"
    assert original_events[-1]["stage"] == "strategy_generation"
    assert original_events[-1]["execution_status"] == "EXECUTION_ERROR"
    assert not list((output / market_day.isoformat()).glob("daily_report.json"))

    retried = trader.run(market_day, ["TEST.NS"])
    retry_lines = event_path.read_text(encoding="utf-8").splitlines()
    retry_events = [json.loads(line) for line in retry_lines]
    assert retry_lines[:len(original_lines)] == original_lines
    assert retry_events[-1]["event_type"] == "RUN_COMPLETED"
    assert retried["signals"]["generated"] == 1 and retried["orders"]["created"] == 1
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM signals WHERE strategy!='DATA_HEALTH'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM portfolio_snapshots").fetchone()[0] == 1


def test_existing_drawdown_tripwire_rejects_new_signal_without_changing_risk_settings(tmp_path):
    days = pd.bdate_range("2025-01-01", periods=251)
    previous_day, market_day = days[-2].date(), days[-1].date()
    config = _config(tmp_path)
    database = tmp_path / "drawdown" / "forward.sqlite"
    _ensure_schema(database, 50000)
    now = datetime(2026, 9, 26, 11, tzinfo=timezone.utc).isoformat()
    with sqlite3.connect(database) as connection:
        connection.execute(
            """INSERT INTO signals(signal_id,trading_date,timestamp,symbol,strategy,direction,signal_price,
               stop_loss,target,reason,features_json,market_regime,sector,data_provider,freshness_status,
               accepted,status,payload_json)
               VALUES ('prior-signal',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (previous_day.isoformat(), now, "OLD.NS", "trend_following", "BUY", 100, 98, 103,
             "synthetic loss", "{}", "TRENDING", "Technology", "fixture", "FRESH", 1, "ACCEPTED", "{}"),
        )
        connection.execute(
            """INSERT INTO orders(order_id,signal_id,created_at,trading_date,symbol,side,quantity,
               intended_price,status,strategy,order_kind)
               VALUES ('prior-order','prior-signal',?,?,?,?,?,?,?,'trend_following','ENTRY')""",
            (now, previous_day.isoformat(), "OLD.NS", "BUY", 1000, 100, "FILLED"),
        )
        connection.execute(
            """INSERT INTO orders(order_id,signal_id,created_at,trading_date,symbol,side,quantity,
               intended_price,simulated_fill_price,filled_at,status,strategy,order_kind)
               VALUES ('prior-exit','prior-signal',?,?,?,?,?,95,95,?,'FILLED','trend_following','EXIT')""",
            (now, previous_day.isoformat(), "OLD.NS", "SELL", 1000, now),
        )
        connection.execute(
            """INSERT INTO positions(position_id,signal_id,entry_order_id,exit_order_id,symbol,strategy,direction,sector,
               quantity,entry_price,entry_date,entry_time,stop_loss,target,entry_costs,total_costs,mark_price,
               status,exit_price,exit_date,realized_pnl,holding_sessions)
               VALUES ('prior-position','prior-signal','prior-order','prior-exit','OLD.NS','trend_following',1,'Technology',
               1000,100,?,?,90,110,0,0,100,'CLOSED',95,?,-5000,1)""",
            (previous_day.isoformat(), now, previous_day.isoformat()),
        )
        connection.execute(
            """INSERT INTO portfolio_snapshots
               (run_id,trading_date,starting_capital,cash,invested_capital,realized_pnl,unrealized_pnl,
                equity,daily_pnl,daily_return,cumulative_return,drawdown,maximum_drawdown,gross_exposure,
                sector_exposure_json,open_positions,pending_orders,created_at)
               VALUES ('prior-run',?,50000,45000,0,-5000,0,45000,-5000,-0.1,-0.1,0.1,0.1,0,'{}',0,0,?)""",
            (previous_day.isoformat(), now),
        )

    trader = DailyPaperTrader(
        config, _provider_bars(days), database, 50000, output_root=tmp_path / "drawdown" / "reports",
        strategies=[FixedSignalStrategy()], baseline_manifest=_baseline_path(config),
    )
    report = trader.run(market_day, ["TEST.NS"])
    assert report["portfolio"]["drawdown"] == pytest.approx(0.10)
    assert report["signals"]["generated"] == 1
    assert report["signals"]["accepted"] == 0 and report["signals"]["rejected"] == 1
    assert "drawdown" in report["risk_events"][0]["reason"].lower()
    assert config.risk.max_portfolio_drawdown == pytest.approx(0.10)
    assert config.risk.risk_per_trade == pytest.approx(0.01)
