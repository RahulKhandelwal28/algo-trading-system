from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nse_research.dashboard import dashboard_payload
from nse_research.execution.phase4c import (
    IntradayPaperReplay,
    PaperExecutionEngine,
    PaperExecutionError,
    PaperStateCorrupted,
    inspect_paper_ledger,
    transition_order,
    verify_paper_ledger_document,
)
from nse_research.execution.phase4c_models import (
    ExecutionMode,
    OrderStatus,
    OrderType,
    PaperQuote,
    Phase4CPaperConfig,
    RejectCode,
    RiskState,
    SignalEnvelope,
)
from nse_research.intraday.model import IntradayBar
from nse_research.intraday.scanner import IntradayScanner, IntradayScannerConfig
from nse_research.intraday.strategies import IntradaySignal


IST = ZoneInfo("Asia/Kolkata")
SESSION = date(2026, 9, 28)
SYNTHETIC_SOURCE = "SYNTHETIC_EXECUTION_TEST"


def stamp(offset: int, *, day: date = SESSION) -> datetime:
    return datetime.combine(day, time(9, 15), IST) + timedelta(minutes=offset)


def make_signal(
    *, signal_id: str = "signal-1", symbol: str = "RELIANCE", timestamp: datetime | None = None,
    valid_until: datetime | None = None, reference: float = 100.0, stop: float = 95.0,
    target: float = 110.0, exchange: str = "NSE", series: str = "EQ", side: str = "LONG",
    strategy: str = "test_strategy", timeframe: str = "1m",
) -> IntradaySignal:
    at = timestamp or stamp(0)
    return IntradaySignal(
        signal_id=signal_id, timestamp=at, valid_until=valid_until or at + timedelta(minutes=3),
        symbol=symbol, exchange=exchange, series=series, side=side, strategy=strategy,
        timeframe=timeframe, reference_price=reference, stop_reference=stop,
        target_reference=target, confidence_metadata={"fixture": SYNTHETIC_SOURCE},
        reason_codes=("SOFTWARE_TEST_ONLY",), setup_id="test-setup",
    )


def make_bar(
    *, symbol: str = "RELIANCE", offset: int = 0, day: date = SESSION,
    open_price: float = 100.0, high: float = 101.0, low: float = 99.0,
    close: float = 100.0, source: str = SYNTHETIC_SOURCE,
) -> IntradayBar:
    return IntradayBar(
        symbol=symbol, exchange="NSE", series="EQ", timeframe="1m", timestamp=stamp(offset, day=day),
        open=open_price, high=high, low=low, close=close, volume=100,
        status="CLOSED", source=source, volume_kind="BAR",
    )


def engine(tmp_path: Path, **config_changes) -> PaperExecutionEngine:
    config = Phase4CPaperConfig(enabled=True, **config_changes)
    return PaperExecutionEngine(config, ledger_path=None)


def submit(
    target_engine: PaperExecutionEngine, signal: IntradaySignal | None = None, *,
    replay_time: datetime | None = None, sector: str | None = "TEST_SECTOR",
    order_type: OrderType = OrderType.MARKET, limit_price: float | None = None,
):
    current = signal or make_signal()
    return target_engine.submit_signal(
        SignalEnvelope(current, source=SYNTHETIC_SOURCE,
                       source_metadata={"fixture": SYNTHETIC_SOURCE, "token": "must-not-persist",
                                        "auth": {"access_token": "never-persist", "provider": "fixture"}}),
        replay_time=replay_time or current.timestamp,
        sector=sector, order_type=order_type, limit_price=limit_price,
    )


def test_safe_defaults_and_live_mode_fail_closed(tmp_path: Path):
    defaults = Phase4CPaperConfig()
    assert defaults.execution_mode is ExecutionMode.PAPER
    assert defaults.enabled is False
    assert defaults.realtime_enabled is False
    assert defaults.live_trading_enabled is False
    assert defaults.max_positions == 5
    assert defaults.max_daily_loss == 0.02
    assert defaults.max_portfolio_drawdown == 0.10
    assert defaults.max_symbol_exposure == defaults.max_sector_exposure == 0.25

    live = engine(tmp_path, execution_mode=ExecutionMode.LIVE)
    order = submit(live)
    assert order.status == OrderStatus.REJECTED.value
    assert order.rejection_reason == "LIVE_EXECUTION_NOT_AVAILABLE_IN_PHASE4C"
    assert live.open_positions == []
    disabled = PaperExecutionEngine(defaults, ledger_path=None)
    assert submit(disabled).rejection_reason == RejectCode.PAPER_TRADING_DISABLED.value


def test_from_env_explicit_command_enables_only_paper(monkeypatch):
    monkeypatch.setenv("PAPER_TRADING_ENABLED", "false")
    monkeypatch.setenv("REALTIME_ENABLED", "false")
    monkeypatch.setenv("LIVE_TRADING", "true")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
    config = Phase4CPaperConfig.from_env(explicit_paper_command=True)
    assert config.enabled is True
    assert config.realtime_enabled is False
    assert config.live_trading_enabled is True


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({"signal_id": ""}, RejectCode.INVALID_SIGNAL.value),
        ({"exchange": "BSE"}, RejectCode.INVALID_SIGNAL.value),
        ({"series": "BE"}, RejectCode.INVALID_SIGNAL.value),
        ({"reference": 0}, RejectCode.INVALID_PRICE.value),
        ({"stop": 100}, RejectCode.INVALID_STOP.value),
        ({"timestamp": stamp(2)}, RejectCode.FUTURE_TIMESTAMP.value),
        ({"timestamp": stamp(-2)}, RejectCode.STALE_SIGNAL.value),
        ({"valid_until": stamp(0)}, RejectCode.EXPIRED_SIGNAL.value),
    ],
)
def test_signal_validation_rejections(tmp_path: Path, changes, expected):
    target = engine(tmp_path)
    signal = make_signal(**changes)
    result = submit(target, signal, replay_time=stamp(0))
    assert result.status == OrderStatus.REJECTED.value
    assert result.rejection_reason == expected


def test_signal_idempotency_and_conflicting_duplicate_rejection(tmp_path: Path):
    target = engine(tmp_path)
    first = make_signal(signal_id="stable-id")
    accepted = submit(target, first)
    repeated = submit(target, first)
    assert repeated.order_id == accepted.order_id
    assert len(target.all_orders) == 1

    conflict = make_signal(signal_id="stable-id", reference=101, stop=96, target=111)
    rejected = submit(target, conflict)
    assert rejected.status == OrderStatus.REJECTED.value
    assert rejected.rejection_reason == RejectCode.DUPLICATE_SIGNAL.value
    assert len(target.all_orders) == 2


def test_position_sizing_obeys_risk_budget_and_symbol_cap(tmp_path: Path):
    target = engine(tmp_path)
    order = submit(target, make_signal(reference=100, stop=98, target=104))
    assert order.status == OrderStatus.PENDING.value
    assert order.quantity > 0
    assert order.planned_risk <= 1000
    assert order.quantity * order.requested_price <= 25_000
    assert order.risk_budget == pytest.approx(1000)


@pytest.mark.parametrize(
    ("mode", "value", "expected"),
    [("fixed", 0.05, 100.05), ("basis_points", 10, 100.1), ("percentage", 0.1, 100.1)],
)
def test_slippage_modes_use_documented_units(tmp_path: Path, mode, value, expected):
    target = engine(tmp_path, slippage_mode=mode, slippage_value=value)
    assert target.risk_engine._slipped(100, 1, True) == pytest.approx(expected)


def test_capital_limit_can_reject_when_one_share_is_unaffordable(tmp_path: Path):
    target = engine(tmp_path, max_symbol_exposure=0.25, max_sector_exposure=0.25, max_gross_exposure=1)
    for index in range(4):
        submit(target, make_signal(signal_id=f"capital-{index}", symbol=f"CAP{index}",
                                  reference=4990, stop=4989, target=5000), sector=f"CAP_SECTOR{index}")
    order = submit(target, make_signal(signal_id="capital-final", symbol="CAP_FINAL",
                                       reference=1000, stop=999, target=1010), sector="CAP_SECTOR_FINAL")
    assert order.status == OrderStatus.REJECTED.value
    assert order.rejection_reason == RejectCode.INSUFFICIENT_CAPITAL.value
    assert target.portfolio_snapshot()["cash"] >= 0
    assert target.portfolio_snapshot()["available_cash"] >= 0


def test_fill_price_gap_rechecks_exposure_limits(tmp_path: Path):
    target = engine(tmp_path)
    signal = make_signal(reference=100, stop=99.9, target=110)
    order = submit(target, signal)
    assert order.status == OrderStatus.PENDING.value
    target.process_bar(make_bar(open_price=101, high=102, low=100.5, close=101), event_time=stamp(1))
    assert target.orders[order.order_id].status == OrderStatus.REJECTED.value
    assert target.orders[order.order_id].rejection_reason == RejectCode.SYMBOL_EXPOSURE_LIMIT.value
    assert target.open_positions == []


def test_max_positions_and_duplicate_symbol_are_enforced(tmp_path: Path):
    target = engine(tmp_path, max_positions=5, max_symbol_exposure=0.05, max_sector_exposure=0.05)
    for index in range(5):
        symbol = f"SYM{index}"
        item = make_signal(signal_id=f"id-{index}", symbol=symbol, reference=100, stop=99, target=104)
        accepted = submit(target, item, sector=f"SECTOR{index}")
        assert accepted.status == OrderStatus.PENDING.value
    sixth = submit(target, make_signal(signal_id="id-5", symbol="SYM5", reference=100, stop=99, target=104), sector="SECTOR5")
    assert sixth.rejection_reason == RejectCode.MAX_POSITIONS.value
    duplicate = submit(target, make_signal(signal_id="same-symbol", symbol="SYM0", reference=100, stop=99, target=104), sector="OTHER")
    assert duplicate.rejection_reason == RejectCode.EXISTING_POSITION.value


def test_sector_metadata_is_required_and_sector_limit_is_enforced(tmp_path: Path):
    target = engine(tmp_path, max_symbol_exposure=1, max_sector_exposure=0.0149, max_gross_exposure=1)
    unknown = submit(target, make_signal(), sector=None)
    assert unknown.rejection_reason == RejectCode.SECTOR_EXPOSURE_UNKNOWN.value
    first = submit(target, make_signal(signal_id="sector-a", reference=100, stop=99, target=104), sector="ONE")
    assert first.status == OrderStatus.PENDING.value
    second = submit(target, make_signal(signal_id="sector-b", symbol="OTHER", reference=100, stop=99, target=104), sector="ONE")
    assert second.rejection_reason == RejectCode.SECTOR_EXPOSURE_LIMIT.value


def test_next_bar_market_fill_stop_first_on_ambiguous_bar_and_cost_accounting(tmp_path: Path):
    target = engine(tmp_path)
    signal = make_signal(reference=109.5, stop=107.5, target=112.5)
    order = submit(target, signal)
    assert order.status == OrderStatus.PENDING.value
    # The signal is known at 09:15; this next interval opens at 09:15. Both
    # barriers are touched, so stop-first is the conservative deterministic rule.
    trades = target.process_bar(make_bar(open_price=109.59, high=113, low=107, close=109.59), event_time=stamp(1))
    assert len(trades) == 1
    filled = target.orders[order.order_id]
    assert filled.status == OrderStatus.FILLED.value
    assert filled.filled_at == stamp(0)
    trade = trades[0]
    assert trade.exit_reason == "STOP_LOSS"
    assert trade.raw_exit_price == pytest.approx(107.5)
    assert trade.gross_pnl == pytest.approx((107.5 - 109.59) * trade.quantity)
    assert trade.total_cost > 0
    assert trade.net_pnl == pytest.approx(trade.gross_pnl - trade.total_cost)
    snapshot = target.portfolio_snapshot()
    assert snapshot["cash"] >= 0
    assert snapshot["total_equity"] == pytest.approx(100_000 + trade.net_pnl)
    assert target.positions[trade.position_id].status == "CLOSED"
    assert target.positions[trade.position_id].source == SYNTHETIC_SOURCE
    assert target.positions[trade.position_id].source_metadata == {
        "fixture": SYNTHETIC_SOURCE, "auth": {"provider": "fixture"},
    }


def test_limit_fill_uses_requested_price_and_cancel_expire_transitions(tmp_path: Path):
    target = engine(tmp_path)
    signal = make_signal(reference=102, stop=98, target=108)
    limit = submit(target, signal, order_type=OrderType.LIMIT, limit_price=100.5)
    target.process_bar(make_bar(open_price=102, high=103, low=100, close=101), event_time=stamp(1))
    assert target.orders[limit.order_id].filled_price == pytest.approx(100.5)

    pending_signal = make_signal(signal_id="cancel-me", symbol="CANCEL", timestamp=stamp(1), reference=100, stop=95, target=110)
    pending = submit(target, pending_signal, sector="CANCEL_SECTOR")
    cancelled = target.cancel_order(pending.order_id, at=stamp(1))
    assert cancelled.status == OrderStatus.CANCELLED.value
    assert cancelled.rejection_reason == "CANCELLED_BY_REPLAY"

    expiring_signal = make_signal(signal_id="expire-me", symbol="EXPIRE", timestamp=stamp(1), reference=100, stop=95, target=110)
    expiring = submit(target, expiring_signal, sector="EXPIRE_SECTOR")
    target.process_bar(make_bar(symbol="EXPIRE", offset=3), event_time=stamp(4))
    assert target.orders[expiring.order_id].status == OrderStatus.EXPIRED.value


def test_target_exit_uses_exact_target_and_single_trade(tmp_path: Path):
    target = engine(tmp_path)
    signal = make_signal(reference=100, stop=95, target=105)
    order = submit(target, signal)
    target.process_bar(make_bar(offset=0, open_price=100, high=102, low=99, close=101), event_time=stamp(1))
    position = target.open_positions[0]
    trade = target.process_bar(make_bar(offset=1, open_price=101, high=106, low=100, close=105), event_time=stamp(2))[0]
    assert trade.exit_reason == "TARGET"
    assert trade.raw_exit_price == pytest.approx(105)
    assert len(target.trades) == 1
    assert target.orders[order.order_id].status == OrderStatus.FILLED.value
    with pytest.raises(PaperExecutionError, match="closed position"):
        target.exit_position(position.position_id, market_context=make_bar(offset=2), at=stamp(3))


def test_explicit_exit_requires_a_current_normalized_context(tmp_path: Path):
    valid = engine(tmp_path / "valid")
    submit(valid, make_signal())
    valid.process_bar(make_bar(open_price=100, high=101, low=99.5, close=100), event_time=stamp(1))
    position = valid.open_positions[0]
    quote_at = stamp(2)
    trade = valid.exit_position(
        position.position_id,
        market_context=PaperQuote("RELIANCE", "NSE", "EQ", quote_at, 100.5, SYNTHETIC_SOURCE),
        at=quote_at,
    )
    assert trade.exit_reason == "EXPLICIT_EXIT"
    assert valid.open_positions == []

    stale = engine(tmp_path / "stale")
    submit(stale, make_signal())
    stale.process_bar(make_bar(open_price=100, high=101, low=99.5, close=100), event_time=stamp(1))
    stale_position = stale.open_positions[0]
    with pytest.raises(ValueError, match="stale or in the future"):
        stale.exit_position(
            stale_position.position_id,
            market_context=PaperQuote("RELIANCE", "NSE", "EQ", stamp(0), 100.5, SYNTHETIC_SOURCE),
            at=stamp(2),
        )
    assert stale.risk_state is RiskState.DATA_QUALITY_HALT
    assert stale.open_positions


def test_atr_trailing_stop_only_ratchets_forward(tmp_path: Path):
    target = engine(tmp_path, trailing_atr_multiple=2)
    submit(target, make_signal(reference=100, stop=95, target=120))
    target.process_bar(make_bar(open_price=100, high=101, low=99, close=100), event_time=stamp(1))
    position = target.open_positions[0]
    target._update_trailing_stops(make_bar(offset=1, open_price=100, high=115, low=100, close=114), 5.0, stamp(2))
    raised = target.positions[position.position_id].trailing_stop
    assert raised == pytest.approx(105)
    target._update_trailing_stops(make_bar(offset=2, open_price=114, high=116, low=113, close=115), 10.0, stamp(3))
    assert target.positions[position.position_id].trailing_stop == pytest.approx(105)
    trailing_trade = target.process_bar(
        make_bar(offset=3, open_price=105.5, high=106, low=104.5, close=105), event_time=stamp(4),
    )[0]
    assert trailing_trade.exit_reason == "TRAILING_STOP"
    assert trailing_trade.raw_exit_price == pytest.approx(105)


def test_session_close_uses_same_day_mark_and_cancels_pending_even_without_forced_squareoff(tmp_path: Path):
    target = engine(tmp_path, force_intraday_exit=False)
    pending = submit(target, make_signal(signal_id="pending-close", symbol="PENDING"))
    open_order = submit(target, make_signal(signal_id="open-close", symbol="OPEN"))
    target.process_bar(make_bar(symbol="OPEN", open_price=100, high=101, low=99, close=100), event_time=stamp(1))
    trades = target.close_session(SESSION, datetime.combine(SESSION, time(15, 30), IST))
    assert target.orders[pending.order_id].status == OrderStatus.CANCELLED.value
    assert trades == []  # Explicitly configured to permit carrying positions.
    assert target.positions[next(iter(target.positions))].status == "OPEN"

    squareoff = engine(tmp_path / "second", force_intraday_exit=True)
    squareoff_order = submit(squareoff, make_signal(signal_id="squareoff", symbol="CLOSE"))
    squareoff.process_bar(make_bar(symbol="CLOSE", open_price=100, high=101, low=99, close=100), event_time=stamp(1))
    closed = squareoff.close_session(SESSION, datetime.combine(SESSION, time(15, 30), IST))
    assert len(closed) == 1
    assert closed[0].exit_reason == "SESSION_CLOSE"
    assert squareoff.orders[squareoff_order.order_id].status == OrderStatus.FILLED.value


@pytest.mark.parametrize("halt", ["daily", "drawdown"])
def test_daily_and_portfolio_halts_block_new_entries_and_preserve_high_water(tmp_path: Path, halt):
    changes = ({"max_daily_loss": 0.001, "max_portfolio_drawdown": 0.10} if halt == "daily"
               else {"max_daily_loss": 0.10, "max_portfolio_drawdown": 0.001})
    target = engine(tmp_path, **changes)
    submit(target, make_signal(reference=100, stop=99, target=105))
    target.process_bar(make_bar(open_price=100, high=101, low=99.5, close=100), event_time=stamp(1))
    peak = target.portfolio_snapshot()["peak_equity"]
    quote_time = stamp(2)
    target.process_quote(PaperQuote("RELIANCE", "NSE", "EQ", quote_time, 99.0, SYNTHETIC_SOURCE), event_time=quote_time)
    assert target.risk_state.value == (RiskState.DAILY_LOSS_HALT.value if halt == "daily" else RiskState.PORTFOLIO_DRAWDOWN_HALT.value)
    assert target._peak_equity == pytest.approx(peak)
    blocked = submit(target, make_signal(signal_id=f"blocked-{halt}", symbol="BLOCKED"))
    assert blocked.status == OrderStatus.REJECTED.value
    assert blocked.rejection_reason in {RejectCode.DAILY_LOSS_LIMIT.value, RejectCode.PORTFOLIO_DRAWDOWN_LIMIT.value}
    next_day_bar = make_bar(symbol="NEXT", offset=0, day=date(2026, 9, 29))
    target.process_bar(next_day_bar, event_time=next_day_bar.end)
    if halt == "daily":
        assert RiskState.DAILY_LOSS_HALT.value not in target._halts
    else:
        assert RiskState.PORTFOLIO_DRAWDOWN_HALT.value in target._halts
        assert target._peak_equity == pytest.approx(peak)


def test_manual_and_data_quality_halts_are_fail_closed_until_explicit_resume(tmp_path: Path):
    target = engine(tmp_path)
    pending = submit(target)
    target.set_manual_halt(stamp(0), reason="fixture halt")
    assert target.orders[pending.order_id].status == OrderStatus.REJECTED.value
    blocked = submit(target, make_signal(signal_id="manual-block", symbol="BLOCK"))
    assert blocked.rejection_reason == RejectCode.MANUAL_HALT.value
    with pytest.raises(ValueError, match="explicit confirmation"):
        target.clear_manual_halt(stamp(1))
    target.clear_manual_halt(stamp(1), confirmed=True)

    invalid = replace(make_bar(offset=1), exchange="BSE")
    target.process_bar(invalid, event_time=stamp(2))
    assert target.risk_state is RiskState.DATA_QUALITY_HALT
    target.process_bar(make_bar(offset=2), event_time=stamp(3))
    target.clear_data_quality_halt(stamp(3), confirmed=True)
    assert target.risk_state is RiskState.ACTIVE


def test_stale_quote_halts_new_entries_using_replay_time(tmp_path: Path):
    target = engine(tmp_path)
    stale_at = stamp(0)
    event_at = stamp(1)
    target.process_quote(
        PaperQuote("RELIANCE", "NSE", "EQ", stale_at, 100, SYNTHETIC_SOURCE), event_time=event_at,
    )
    assert target.risk_state is RiskState.DATA_QUALITY_HALT
    rejected = submit(target, make_signal(signal_id="after-stale", symbol="AFTER"), replay_time=event_at)
    assert rejected.rejection_reason == RejectCode.DATA_QUALITY.value


def test_transition_helper_rejects_invalid_state_changes(tmp_path: Path):
    pending = submit(engine(tmp_path))
    with pytest.raises(PaperExecutionError, match="invalid paper-order transition"):
        transition_order(replace(pending, status=OrderStatus.CANCELLED.value), OrderStatus.FILLED)
    cancelled = transition_order(pending, OrderStatus.CANCELLED)
    assert cancelled.status == OrderStatus.CANCELLED.value
    created = replace(pending, status=OrderStatus.CREATED.value)
    validated = transition_order(created, OrderStatus.VALIDATED)
    queued = transition_order(validated, OrderStatus.PENDING)
    partial = transition_order(queued, OrderStatus.PARTIALLY_FILLED)
    assert transition_order(partial, OrderStatus.FILLED).status == OrderStatus.FILLED.value


def test_hash_chained_ledger_restores_state_and_rejects_corruption(tmp_path: Path):
    ledger = tmp_path / "ledger.json"
    config = Phase4CPaperConfig(enabled=True)
    target = PaperExecutionEngine(config, ledger_path=ledger)
    order = submit(target)
    raw = json.loads(ledger.read_text())
    verify_paper_ledger_document(raw)
    restored = PaperExecutionEngine(config, ledger_path=ledger)
    assert restored.orders[order.order_id].status == OrderStatus.PENDING.value
    before_events = len(restored.events)
    assert submit(restored).order_id == order.order_id
    assert len(restored.events) == before_events
    restored.set_manual_halt(stamp(1), reason="persisted safety halt")
    halted = PaperExecutionEngine(config, ledger_path=ledger)
    assert halted.risk_state is RiskState.MANUAL_HALT
    assert inspect_paper_ledger(ledger)["status"] == "OK"
    raw["last_event_hash"] = "0" * 64
    ledger.write_text(json.dumps(raw))
    assert inspect_paper_ledger(ledger)["status"] == "PAPER_STATE_CORRUPTED"
    with pytest.raises(PaperStateCorrupted):
        PaperExecutionEngine(config, ledger_path=ledger)
    ledger.write_text("[]", encoding="utf-8")
    assert inspect_paper_ledger(ledger)["status"] == "PAPER_STATE_CORRUPTED"


def test_atomic_persistence_failure_rolls_back_state(tmp_path: Path, monkeypatch):
    import nse_research.execution.phase4c as module

    target = PaperExecutionEngine(Phase4CPaperConfig(enabled=True), ledger_path=tmp_path / "ledger.json")
    monkeypatch.setattr(module.os, "replace", lambda *_args: (_ for _ in ()).throw(OSError("fixture write failure")))
    with pytest.raises(OSError, match="fixture write failure"):
        submit(target)
    assert target.all_orders == []
    assert target.events == ()
    assert not (tmp_path / "phase4c" / "ledger.json").exists()


def test_dashboard_reads_only_and_reports_missing_or_valid_ledger(tmp_path: Path):
    before = hashlib.sha256((Path(__file__).resolve().parents[1] / "results/phase3d_forward_20260926_225042/paper_ledger.sqlite").read_bytes()).hexdigest() if (Path(__file__).resolve().parents[1] / "results/phase3d_forward_20260926_225042/paper_ledger.sqlite").is_file() else None
    missing = dashboard_payload(tmp_path / "unused.sqlite", "/api/dashboard/phase4c-paper", project_root=tmp_path)
    assert missing["status"] == "DATA_UNAVAILABLE"
    assert missing["risk_status"] == "DATA_UNAVAILABLE"
    ledger = tmp_path / "data/paper_trading/phase4c/ledger.json"
    target = PaperExecutionEngine(Phase4CPaperConfig(enabled=True), ledger_path=ledger)
    submit(target)
    digest = hashlib.sha256(ledger.read_bytes()).hexdigest()
    payload = dashboard_payload(tmp_path / "unused.sqlite", "/api/dashboard/phase4c-paper", project_root=tmp_path)
    assert payload["status"] == "OK"
    assert payload["execution_mode"] == "PAPER"
    assert payload["live_trading_enabled"] is False
    assert payload["pending_orders"] == 1
    assert payload["risk_status"] == "OK"
    assert payload["symbol_exposures"]["RELIANCE"] > 0
    assert payload["sector_exposures"]["TEST_SECTOR"] > 0
    assert hashlib.sha256(ledger.read_bytes()).hexdigest() == digest
    project_root = Path(__file__).resolve().parents[1]
    phase3d = project_root / "results/phase3d_forward_20260926_225042/paper_ledger.sqlite"
    if before is not None:
        assert hashlib.sha256(phase3d.read_bytes()).hexdigest() == before


def test_execution_module_has_no_broker_or_provider_adapter_imports():
    source_path = Path(__file__).resolve().parents[1] / "src/nse_research/execution/phase4c.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden = ("broker", "upstox", "choice", "angel", "kite")
    assert not [name for name in imported if any(token in name.lower() for token in forbidden)]


class _SyntheticProvider:
    provider_name = SYNTHETIC_SOURCE

    def __init__(self, rows):
        self.rows = rows

    def iter_bars(self, symbols, timeframes, *, start=None, end=None):
        for row in self.rows:
            yield row


def test_replay_interleaves_existing_phase4b_two_signals_with_paper_execution(tmp_path: Path):
    symbols = ["RELIANCE"] + [f"SYM{index:03}" for index in range(99)]
    scanner = IntradayScanner(
        symbols, config=IntradayScannerConfig(enabled=True, universe_size=100, timeframes=("1m",)),
    )
    fixture_bars = []
    for index in range(60):
        if index == 20:
            fixture_bars.append(make_bar(offset=index, open_price=109.59, high=113, low=107, close=109.59,
                                         source="SYNTHETIC_TEST_DATA"))
        else:
            price = 100 + index * 0.5
            fixture_bars.append(make_bar(offset=index, open_price=price, high=price + 1, low=price - 1,
                                         close=price, source="SYNTHETIC_TEST_DATA"))
    target = engine(tmp_path)
    result = IntradayPaperReplay(scanner, target).run(
        _SyntheticProvider(fixture_bars), speed="max", sectors={"RELIANCE": "SYNTHETIC_TEST_SECTOR"},
    )
    assert result["signals_processed"] == 2
    assert {signal.strategy for signal in scanner.signals} == {"vwap_trend", "momentum"}
    assert result["orders"] == 3  # accepted entry, rejected duplicate entry, and exit order
    assert result["fills"] == 2  # entry fill and stop exit fill
    assert result["trades"] == 1
    assert result["portfolio"]["live_trading_enabled"] is False
    assert all(order.source in {"SYNTHETIC_TEST_DATA", SYNTHETIC_SOURCE} for order in target.all_orders)
