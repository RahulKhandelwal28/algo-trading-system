from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import shutil
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nse_research.dashboard import dashboard_payload
from nse_research.execution.phase4c import PaperExecutionEngine, inspect_paper_ledger
from nse_research.execution.phase4c_models import Phase4CPaperConfig
from nse_research.execution.phase4d import (
    CANONICAL_COLUMNS,
    Phase4DOrchestrator,
    Phase4DError,
    SessionLock,
    _atomic_json,
    synthetic_test,
)
from nse_research.intraday.model import load_frozen_universe


IST = ZoneInfo("Asia/Kolkata")
TRADING_DATE = date(2026, 9, 28)
SOURCE_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def safe_environment(monkeypatch):
    for name in ("LIVE_TRADING", "LIVE_TRADING_ENABLED", "REALTIME_ENABLED", "EXECUTION_MODE",
                 "PAPER_TRADING_ENABLED", "BROKER_PROVIDER", "INTRADAY_DATA_PATH"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LIVE_TRADING", "false")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("REALTIME_ENABLED", "false")
    monkeypatch.setenv("EXECUTION_MODE", "PAPER")


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    universe_file = root / "results/phase4a8a_upstox_live_universe_100.csv"
    universe_file.parent.mkdir(parents=True)
    shutil.copyfile(SOURCE_ROOT / "results/phase4a8a_upstox_live_universe_100.csv", universe_file)
    sectors_file = root / "data/paper_trading/phase3d_personal_universe_v2.csv"
    sectors_file.parent.mkdir(parents=True)
    shutil.copyfile(SOURCE_ROOT / "data/paper_trading/phase3d_personal_universe_v2.csv", sectors_file)
    return root


def _universe(root: Path) -> list[str]:
    return load_frozen_universe(root, 100)


def _row(symbol: str, *, timestamp: str | None = None, exchange: str = "NSE", series: str = "EQ",
         high: str = "101", low: str = "99", volume: str = "100", volume_kind: str = "BAR",
         source: str = "UNIT_TEST_SOURCE") -> dict[str, str]:
    stamp = timestamp or datetime.combine(TRADING_DATE, time(9, 15), IST).isoformat()
    return {"symbol": symbol, "exchange": exchange, "series": series, "timeframe": "1m", "timestamp": stamp,
            "open": "100", "high": high, "low": low, "close": "100", "volume": volume,
            "volume_kind": volume_kind, "status": "CLOSED", "source": source}


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=sorted(CANONICAL_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def _init_ledger(path: Path) -> None:
    engine = PaperExecutionEngine(Phase4CPaperConfig.from_env(explicit_paper_command=True), ledger_path=path)
    assert engine.initialize_ledger()


def _bars_for_full_universe(root: Path, *, long_reliance_path: bool = False) -> list[dict[str, str]]:
    symbols = _universe(root)
    close_stamp = datetime.combine(TRADING_DATE, time(15, 29), IST).isoformat()
    rows = [row for symbol in symbols for row in (_row(symbol), _row(symbol, timestamp=close_stamp))]
    if long_reliance_path:
        rows = [row for row in rows if row["symbol"] != "RELIANCE"]
        for offset in range(60):
            stamp = datetime.combine(TRADING_DATE, time(9, 15), IST) + timedelta(minutes=offset)
            price = 100.0 + offset * 0.5
            if offset == 20:
                opening, high, low, close = 109.59, 113.0, 107.0, 109.59
            else:
                opening, high, low, close = price, price + 1, price - 1, price
            rows.append({"symbol": "RELIANCE", "exchange": "NSE", "series": "EQ", "timeframe": "1m",
                         "timestamp": stamp.isoformat(), "open": str(opening), "high": str(high),
                         "low": str(low), "close": str(close), "volume": "100", "volume_kind": "BAR",
                         "status": "CLOSED", "source": "UNIT_TEST_SOURCE"})
        rows.append({"symbol": "RELIANCE", "exchange": "NSE", "series": "EQ", "timeframe": "1m",
                     "timestamp": close_stamp, "open": "129", "high": "130", "low": "128", "close": "129",
                     "volume": "100", "volume_kind": "BAR", "status": "CLOSED", "source": "UNIT_TEST_SOURCE"})
    return rows


def test_missing_intraday_data_blocks_without_ledger_mutation(tmp_path: Path):
    root = _root(tmp_path)
    ledger = tmp_path / "ledger.json"
    _init_ledger(ledger)
    before = hashlib.sha256(ledger.read_bytes()).hexdigest()
    orchestrator = Phase4DOrchestrator(project_root=root, data_path=tmp_path / "missing.csv", ledger_path=ledger)

    result = orchestrator.preflight(trading_date=TRADING_DATE)

    assert result.status == "PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE"
    assert result.data_quality["bars_received"] == 0
    assert result.data_quality["coverage_percent"] == 0
    assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before
    assert not orchestrator.checkpoint_path(result.session_id).exists()


def test_real_replay_block_does_not_mutate_ledger_or_create_session_report(tmp_path: Path):
    root = _root(tmp_path)
    ledger = tmp_path / "ledger.json"
    _init_ledger(ledger)
    before = hashlib.sha256(ledger.read_bytes()).hexdigest()
    orchestrator = Phase4DOrchestrator(project_root=root, data_path=tmp_path / "missing.csv", ledger_path=ledger)

    result = orchestrator.run_replay(trading_date=TRADING_DATE)

    assert result["status"] == "PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE"
    assert result["orders"] == result["fills"] == result["trades"] == result["live_orders"] == result["broker_calls"] == 0
    assert result["ledger_mutations"] == 0
    assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before
    assert not (root / "results/phase4d/PAPER-NSE-20260928").exists()


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda rows: rows[0].update(series="BE"), "DATA_RECORD_INVALID"),
        (lambda rows: rows[0].update(exchange="BSE"), "DATA_RECORD_INVALID"),
        (lambda rows: rows[0].update(high="not-a-number"), "DATA_RECORD_INVALID"),
        (lambda rows: rows[0].update(high="98"), "DATA_RECORD_INVALID"),
        (lambda rows: rows[0].update(volume="-1"), "DATA_RECORD_INVALID"),
        (lambda rows: rows[0].update(timestamp="2026-09-28T09:15:00"), "DATA_RECORD_INVALID"),
    ],
)
def test_invalid_series_timestamp_numeric_and_ohlc_are_rejected(tmp_path: Path, mutate, code):
    root = _root(tmp_path)
    rows = _bars_for_full_universe(root)
    mutate(rows)
    data = tmp_path / "bars.csv"
    _write_csv(data, rows)
    result = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=tmp_path / "ledger.json").preflight(
        trading_date=TRADING_DATE,
    )
    assert result.status == "PHASE4D_FORWARD_BLOCKED"
    assert any(code in reason for reason in result.reasons)
    assert result.data_quality["validation_status"] == code


def test_duplicate_rows_and_insufficient_symbol_coverage_fail_closed(tmp_path: Path):
    root = _root(tmp_path)
    rows = _bars_for_full_universe(root)
    rows.append(dict(rows[0]))
    duplicate_data = tmp_path / "duplicates.csv"
    _write_csv(duplicate_data, rows)
    duplicate = Phase4DOrchestrator(project_root=root, data_path=duplicate_data,
                                    ledger_path=tmp_path / "ledger.json").preflight(trading_date=TRADING_DATE)
    assert any("DATA_DUPLICATE_BARS" in reason for reason in duplicate.reasons)
    assert duplicate.data_quality["duplicate_intervals"] == 1

    partial_data = tmp_path / "partial.csv"
    partial_symbols = set(_universe(root)[:90])
    _write_csv(partial_data, [row for row in _bars_for_full_universe(root) if row["symbol"] in partial_symbols])
    partial = Phase4DOrchestrator(project_root=root, data_path=partial_data,
                                  ledger_path=tmp_path / "ledger.json").preflight(trading_date=TRADING_DATE)
    assert any("DATA_COVERAGE_HALT" in reason for reason in partial.reasons)


def test_calendar_rejects_weekends_holidays_and_unrepresented_special_sessions(tmp_path: Path):
    root = _root(tmp_path)
    data = tmp_path / "missing.csv"
    orchestrator = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=tmp_path / "ledger.json")
    holiday = orchestrator.preflight(trading_date=date(2026, 1, 26))
    weekend = orchestrator.preflight(trading_date=date(2026, 9, 27))
    special = orchestrator.preflight(trading_date=date(2026, 11, 8))
    assert any("SESSION_NOT_TRADING_DAY" in reason for reason in holiday.reasons)
    assert any("SESSION_NOT_TRADING_DAY" in reason for reason in weekend.reasons)
    assert any("SPECIAL_SESSION_HOURS_UNVERIFIED" in reason for reason in special.reasons)


def test_required_trading_date_must_have_its_own_bars(tmp_path: Path):
    root = _root(tmp_path)
    next_day_rows = []
    for symbol in _universe(root):
        row = _row(symbol)
        row["timestamp"] = datetime(2026, 9, 29, 9, 15, tzinfo=IST).isoformat()
        next_day_rows.append(row)
    data = tmp_path / "next-day-only.csv"
    _write_csv(data, next_day_rows)
    result = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=tmp_path / "ledger.json").preflight(
        trading_date=TRADING_DATE,
    )
    assert result.status == "PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE"
    assert result.data_quality["bars_received"] == 0


def test_partial_day_without_a_close_boundary_is_not_a_completed_replay(tmp_path: Path):
    root = _root(tmp_path)
    close_stamp = datetime.combine(TRADING_DATE, time(15, 29), IST).isoformat()
    rows = [row for row in _bars_for_full_universe(root) if row["timestamp"] != close_stamp]
    data = tmp_path / "partial-session.csv"
    _write_csv(data, rows)
    result = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=tmp_path / "ledger.json").preflight(
        trading_date=TRADING_DATE,
    )
    assert result.status == "PHASE4D_FORWARD_BLOCKED"
    assert result.data_quality["session_close_present"] is False
    assert any("DATA_SESSION_INCOMPLETE" in reason for reason in result.reasons)


def test_preflight_passes_for_valid_canonical_sample_without_mutating_ledger(tmp_path: Path):
    root = _root(tmp_path)
    data = tmp_path / "bars.csv"
    ledger = tmp_path / "ledger.json"
    _write_csv(data, _bars_for_full_universe(root))
    _init_ledger(ledger)
    before = hashlib.sha256(ledger.read_bytes()).hexdigest()

    result = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=ledger).preflight(
        trading_date=TRADING_DATE,
    )

    assert result.status == "PHASE4D_READY"
    assert result.session_id == "PAPER-NSE-20260928"
    assert result.data_quality["symbols_with_data"] == 100
    assert result.data_quality["coverage_percent"] == 1
    assert result.data_quality["available_timeframes"] == ["1m"]
    assert result.provenance["sha256"] == result.dataset_checksum
    assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before


def test_live_flags_and_unsupported_ledger_schema_block_without_repair(tmp_path: Path, monkeypatch):
    root = _root(tmp_path)
    data = tmp_path / "bars.csv"
    _write_csv(data, _bars_for_full_universe(root))
    ledger = tmp_path / "ledger.json"
    _init_ledger(ledger)
    before = ledger.read_bytes()
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    blocked = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=ledger).preflight(
        trading_date=TRADING_DATE,
    )
    assert any("LIVE_TRADING_MUST_REMAIN_DISABLED" in reason for reason in blocked.reasons)
    assert ledger.read_bytes() == before

    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
    document = json.loads(before)
    document["schema_version"] = 999
    ledger.write_text(json.dumps(document), encoding="utf-8")
    corrupt_bytes = ledger.read_bytes()
    unsupported = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=ledger).preflight(
        trading_date=TRADING_DATE,
    )
    assert any("LEDGER_SCHEMA_UNSUPPORTED" in reason for reason in unsupported.reasons)
    assert ledger.read_bytes() == corrupt_bytes


def test_atomic_checkpoint_write_failure_preserves_previous_file(tmp_path: Path, monkeypatch):
    import nse_research.execution.phase4d as phase4d

    target = tmp_path / "checkpoint.json"
    target.write_text('{"old":true}\n', encoding="utf-8")
    before = target.read_bytes()
    monkeypatch.setattr(phase4d.os, "replace", lambda *_args: (_ for _ in ()).throw(OSError("fixture")))
    with pytest.raises(OSError, match="fixture"):
        _atomic_json(target, {"schema_version": 1, "new": True})
    assert target.read_bytes() == before


def test_phase4c_ledger_initialization_is_versioned_and_reloadable(tmp_path: Path):
    ledger = tmp_path / "ledger.json"
    engine = PaperExecutionEngine(Phase4CPaperConfig.from_env(explicit_paper_command=True), ledger_path=ledger)
    assert engine.initialize_ledger() is True
    original = ledger.read_bytes()
    assert engine.initialize_ledger() is False
    assert ledger.read_bytes() == original
    document = json.loads(original)
    assert document["schema_version"] == 1
    assert document["schema_id"] == "phase4c_v1"
    assert document["state"]["config"]["initial_capital"] == 100000
    assert document["state"]["positions"] == []
    assert document["state"]["orders"] == []
    assert document["state"]["trades"] == []
    assert document["state"]["equity_curve"] == []
    assert document["state"]["session_state"]["active_session"] is None
    assert document["initial_capital"] == 100000
    assert document["cash"] == 100000
    assert document["positions"] == document["orders"] == document["trades"] == document["equity_history"] == []
    assert document["risk_state"] == "ACTIVE"
    assert document["session_state"]["active_session"] is None
    assert PaperExecutionEngine(Phase4CPaperConfig.from_env(explicit_paper_command=True),
                                ledger_path=ledger).session_state["completed_session_ids"] == []


def test_checkpoint_must_match_a_hash_chained_ledger_event(tmp_path: Path):
    root = _root(tmp_path)
    data = tmp_path / "bars.csv"
    _write_csv(data, _bars_for_full_universe(root))
    ledger = tmp_path / "ledger.json"
    _init_ledger(ledger)
    orchestrator = Phase4DOrchestrator(project_root=root, data_path=data, ledger_path=ledger)
    checkpoint = orchestrator.checkpoint_path("PAPER-NSE-20260928")
    checkpoint.parent.mkdir(parents=True)
    checkpoint.write_text(json.dumps({
        "schema_version": 1, "session_id": "PAPER-NSE-20260928", "trading_date": "2026-09-28",
        "last_processed_timestamp": "2026-09-28T09:16:00+05:30", "last_processed_event_id": "BAR4D-MISSING",
        "last_ledger_event_id": "EVT-MISSING", "ledger_event_hash": "0" * 64,
        "dataset_sha256": hashlib.sha256(data.read_bytes()).hexdigest(),
    }), encoding="utf-8")
    result = orchestrator.preflight(trading_date=TRADING_DATE)
    assert any("CHECKPOINT_LEDGER_MISMATCH" in reason for reason in result.reasons)


def test_session_file_lock_excludes_second_writer_and_releases_cleanly(tmp_path: Path):
    path = tmp_path / "PAPER-NSE-20260928.lock"
    with SessionLock(path):
        with pytest.raises(Phase4DError, match="SESSION_LOCKED"):
            with SessionLock(path):
                pass
    with SessionLock(path):
        assert path.is_file()


def test_replay_persists_session_report_and_dashboard_is_read_only(tmp_path: Path):
    root = _root(tmp_path)
    data = root / "data/intraday/canonical_intraday_bars.csv"
    _write_csv(data, _bars_for_full_universe(root))
    orchestrator = Phase4DOrchestrator(project_root=root, data_path=data)

    result = orchestrator.run_replay(trading_date=TRADING_DATE)

    assert result["status"] == "PHASE4D_SESSION_COMPLETED"
    assert result["session_id"] == "PAPER-NSE-20260928"
    assert result["bars_processed"] == 200
    assert result["orders"] == result["fills"] == result["trades"] == 0
    report = Path(result["report_dir"])
    assert {"session.json", "signals.csv", "orders.csv", "fills.csv", "positions.csv", "trades.csv",
            "risk_events.csv", "equity_curve.csv", "data_quality.json"}.issubset({p.name for p in report.iterdir()})
    ledger = root / "data/paper_trading/phase4c/ledger.json"
    before = hashlib.sha256(ledger.read_bytes()).hexdigest()
    dashboard = dashboard_payload(root / "unused.sqlite", "/api/dashboard/phase4d-session", project_root=root)
    assert dashboard["dashboard_read_only"] is True
    assert dashboard["session"]["status"] == "COMPLETED"
    assert dashboard["live_trading_enabled"] is False
    assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before
    duplicate = orchestrator.preflight(trading_date=TRADING_DATE)
    assert any("DUPLICATE_SESSION" in reason for reason in duplicate.reasons)


@pytest.mark.parametrize("crash_event", ["ORDER_PENDING", "ENTRY_FILLED", "POSITION_CLOSED", "MARKET_BAR_PROCESSED"])
def test_restart_recovers_after_committed_phase4c_event_without_duplicate_execution(
    tmp_path: Path, monkeypatch, crash_event: str,
):
    import nse_research.execution.phase4c as phase4c

    root = _root(tmp_path)
    data = root / "data/intraday/canonical_intraday_bars.csv"
    _write_csv(data, _bars_for_full_universe(root, long_reliance_path=True))
    orchestrator = Phase4DOrchestrator(project_root=root, data_path=data)
    original = phase4c.PaperExecutionEngine._record_event
    crashed = {"value": False}

    def commit_then_crash(self, event_type, *args, **kwargs):
        event_id = original(self, event_type, *args, **kwargs)
        if event_type == crash_event and not crashed["value"]:
            crashed["value"] = True
            raise RuntimeError(f"simulated crash after {crash_event}")
        return event_id

    monkeypatch.setattr(phase4c.PaperExecutionEngine, "_record_event", commit_then_crash)
    with pytest.raises(Phase4DError, match="PHASE4D_SESSION_FAILED"):
        orchestrator.run_replay(trading_date=TRADING_DATE)
    assert crashed["value"] is True
    monkeypatch.setattr(phase4c.PaperExecutionEngine, "_record_event", original)

    result = orchestrator.run_replay(trading_date=TRADING_DATE)
    ledger = root / "data/paper_trading/phase4c/ledger.json"
    engine = PaperExecutionEngine(Phase4CPaperConfig.from_env(explicit_paper_command=True), ledger_path=ledger)
    event_types = [event["event_type"] for event in engine.events]
    assert result["status"] == "PHASE4D_SESSION_COMPLETED"
    assert event_types.count("MARKET_BAR_PROCESSED") == len(_bars_for_full_universe(root, long_reliance_path=True))
    assert len(engine.all_orders) == len({order.order_id for order in engine.all_orders})
    assert len(engine.all_trades) == len({trade.trade_id for trade in engine.all_trades})
    assert inspect_paper_ledger(ledger)["status"] == "OK"


def test_synthetic_test_uses_isolated_ledger_and_publishes_no_performance_metrics(tmp_path: Path):
    production_ledger = tmp_path / "production-ledger.json"
    _init_ledger(production_ledger)
    before = hashlib.sha256(production_ledger.read_bytes()).hexdigest()
    result = synthetic_test()
    assert result["status"] == "SYNTHETIC_TEST_PASS"
    assert result["performance_metrics"] == "NOT_CALCULATED"
    assert result["live_orders"] == result["broker_calls"] == 0
    assert hashlib.sha256(production_ledger.read_bytes()).hexdigest() == before


def test_live_and_unvalidated_realtime_modes_are_rejected_without_execution(capsys):
    spec = importlib.util.spec_from_file_location("phase4d_cli", SOURCE_ROOT / "scripts/run_phase4d_intraday_paper.py")
    assert spec and spec.loader
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)

    assert cli.main(["--mode", "LIVE"]) != 0
    assert "LIVE_EXECUTION_NOT_AVAILABLE" in capsys.readouterr().out
    assert cli.main(["--mode", "REALTIME_PAPER"]) != 0
    assert "REALTIME_PROVIDER_NOT_VALIDATED" in capsys.readouterr().out
