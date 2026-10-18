from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nse_research.data.yfinance_intraday import load_yahoo_instruments
from nse_research.intraday.bar_store import CanonicalBarStore, bar_eligibility_reason
from nse_research.intraday.phase4g_yf import Phase4GYFError, Phase4GYFForwardProcessor


IST = ZoneInfo("Asia/Kolkata")
SESSION_START = datetime(2026, 9, 28, 9, 15, tzinfo=IST)


def _bar(symbol: str, minute: int, *, receipt_delay: int = 1, close: float = 100.0) -> dict[str, object]:
    market = SESSION_START + timedelta(minutes=minute)
    received = market + timedelta(minutes=receipt_delay)
    return {
        "symbol": symbol,
        "timestamp": market.isoformat(),
        "market_timestamp": market.isoformat(),
        "received_timestamp": received.isoformat(),
        "timeframe": "1m",
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": float(100 + minute),
        "source": "mock_yahoo",
        "exchange": "NSE",
        "series": "EQ",
        "status": "CLOSED",
        "bar_status": "IMMUTABLE_CLOSED",
        "volume_kind": "BAR",
    }


def test_mandatory_ten_symbols_twenty_bars_overlap_and_incremental_addition():
    symbols = [f"S{index:02d}" for index in range(10)]
    first_rows = [_bar(symbol, minute) for symbol in symbols for minute in range(20)]
    store = CanonicalBarStore()
    first = store.plan_ingest(
        first_rows, decision_timestamp=SESSION_START + timedelta(minutes=25),
        expected_symbols=symbols, warmed_up=False,
    )
    assert first.accounting["rows_seen"] == 200
    assert first.accounting["unique_market_bars"] == 200
    assert first.accounting["new_market_bars"] == 200
    store.commit(first)

    second_rows = first_rows + [_bar(symbol, 20) for symbol in symbols]
    second = store.plan_ingest(
        second_rows, decision_timestamp=SESSION_START + timedelta(minutes=22),
        expected_symbols=symbols, warmed_up=True,
    )
    assert second.accounting["rows_seen"] == 210
    assert second.accounting["unique_market_bars"] == 210
    assert second.accounting["new_market_bars"] == 10
    assert second.accounting["duplicate_identical"] == 200
    assert second.accounting["duplicate_conflict"] == 0
    assert second.accounting["duplicate_market_bars"] == 200
    assert second.accounting["current_session_new_bars"] == 10
    assert all(item["new_bars"] == 1 and item["duplicates"] == 20
               for item in second.accounting["per_symbol"])


def test_refresh_merge_order_does_not_change_canonical_output():
    symbols = ["S01", "S02"]
    rows = [_bar(symbol, minute) for symbol in symbols for minute in range(4)]
    decision = SESSION_START + timedelta(minutes=6)
    ordered_store, reversed_store = CanonicalBarStore(), CanonicalBarStore()
    ordered = ordered_store.plan_ingest(rows, decision_timestamp=decision,
                                        expected_symbols=symbols, warmed_up=False)
    reversed_plan = reversed_store.plan_ingest(list(reversed(rows)), decision_timestamp=decision,
                                                expected_symbols=symbols, warmed_up=False)
    ordered_store.commit(ordered)
    reversed_store.commit(reversed_plan)
    assert ordered_store.ordered_rows() == reversed_store.ordered_rows()
    assert ordered.accounting["per_symbol"] == reversed_plan.accounting["per_symbol"]


def test_reproduces_17633_overlapping_bars_plus_486_new_bars():
    symbols = [f"S{index:03d}" for index in range(486)]
    first_rows: list[dict[str, object]] = []
    next_minute: dict[str, int] = {}
    for index, symbol in enumerate(symbols):
        count = 37 if index < 137 else 36
        first_rows.extend(_bar(symbol, minute) for minute in range(count))
        next_minute[symbol] = count
    assert len(first_rows) == 17_633
    store = CanonicalBarStore()
    first = store.plan_ingest(
        first_rows, decision_timestamp=SESSION_START + timedelta(hours=1),
        expected_symbols=symbols, warmed_up=False,
    )
    assert first.accounting["new_market_bars"] == 17_633
    store.commit(first)

    second_rows = first_rows + [_bar(symbol, next_minute[symbol]) for symbol in symbols]
    second = store.plan_ingest(
        second_rows, decision_timestamp=SESSION_START + timedelta(minutes=40),
        expected_symbols=symbols, warmed_up=True,
    )
    assert second.accounting["unique_market_bars"] == 18_119
    assert second.accounting["new_market_bars"] == 486
    assert second.accounting["duplicate_market_bars"] == 17_633
    assert second.accounting["duplicate_conflict"] == 0


def test_bar_key_ignores_receipt_timestamp_and_conflicts_fail_closed_without_mutation():
    first_row = _bar("RELIANCE", 200, receipt_delay=2)
    store = CanonicalBarStore()
    initial = store.plan_ingest(
        [first_row], decision_timestamp=datetime(2026, 9, 28, 12, 40, tzinfo=IST),
        expected_symbols=["RELIANCE"], warmed_up=False,
    )
    store.commit(initial)
    duplicate = dict(first_row, received_timestamp="2026-09-28T12:39:00+05:30")
    identical = store.plan_ingest(
        [duplicate], decision_timestamp=datetime(2026, 9, 28, 12, 40, tzinfo=IST),
        expected_symbols=["RELIANCE"], warmed_up=True,
    )
    assert identical.accounting["new_market_bars"] == 0
    assert identical.accounting["duplicate_market_bars"] == 1
    assert len(store.records) == 1

    conflict = dict(duplicate, close=101.0)
    rejected = store.plan_ingest(
        [conflict], decision_timestamp=datetime(2026, 9, 28, 12, 40, tzinfo=IST),
        expected_symbols=["RELIANCE"], warmed_up=True,
    )
    assert rejected.accounting["duplicate_conflict"] == 1
    assert rejected.conflicts[0]["old_close"] == 100.0
    assert rejected.conflicts[0]["new_close"] == 101.0
    assert rejected.conflicts[0]["first_received_timestamp"] == first_row["received_timestamp"]
    assert rejected.conflicts[0]["second_received_timestamp"] == duplicate["received_timestamp"]
    with pytest.raises(ValueError, match="invalid or conflicting"):
        store.commit(rejected)
    assert store.records[("RELIANCE", first_row["market_timestamp"])] ["close"] == 100.0


def test_receipt_time_late_bar_can_become_eligible_only_for_a_later_decision():
    row = {
        "symbol": "RELIANCE", "market_timestamp": "2026-09-28T12:35:00+05:30",
        "timestamp": "2026-09-28T12:35:00+05:30",
        "received_timestamp": "2026-09-28T12:38:00+05:30", "timeframe": "1m",
        "status": "CLOSED", "bar_status": "IMMUTABLE_CLOSED",
    }
    assert bar_eligibility_reason(row, datetime(2026, 9, 28, 12, 36, tzinfo=IST)) == "late_receipt"
    assert bar_eligibility_reason(row, datetime(2026, 9, 28, 12, 39, tzinfo=IST)) is None


def test_future_or_not_yet_closed_market_bar_is_ineligible():
    row = {
        "symbol": "RELIANCE", "market_timestamp": "2026-09-28T12:39:00+05:30",
        "timestamp": "2026-09-28T12:39:00+05:30",
        "received_timestamp": "2026-09-28T12:39:00+05:30", "timeframe": "1m",
        "status": "CLOSED", "bar_status": "IMMUTABLE_CLOSED",
    }
    assert bar_eligibility_reason(row, datetime(2026, 9, 28, 12, 39, tzinfo=IST)) == "market_bar_not_closed_before_decision"
    assert bar_eligibility_reason(row, datetime(2026, 9, 28, 12, 40, tzinfo=IST)) == "bar_settling_period_not_elapsed"
    assert bar_eligibility_reason(row, datetime(2026, 9, 28, 12, 40, 5, tzinfo=IST)) is None


def test_failed_symbol_remains_in_universe_and_is_reported_without_synthetic_bar():
    store = CanonicalBarStore()
    failures = {"BLUEDART": {"error_type": "YAHOO_PROVIDER_WARNING",
                             "error_message": "possibly delisted; no price data found"}}
    plan = store.plan_ingest(
        [], decision_timestamp=datetime(2026, 9, 28, 12, 40, tzinfo=IST),
        expected_symbols=["BLUEDART", "RELIANCE"], warmed_up=True,
        refresh_failures=failures,
    )
    bluedart = next(item for item in plan.accounting["per_symbol"] if item["symbol"] == "BLUEDART")
    assert plan.accounting["universe_symbols"] == 2
    assert plan.accounting["symbols_with_refresh_failure"] == 1
    assert bluedart["fetch_failure"] is True
    assert bluedart["fetch_status"] == "DATA_UNAVAILABLE_FOR_THIS_REFRESH"
    assert bluedart["rows_seen"] == bluedart["new_bars"] == bluedart["duplicates"] == 0
    assert not store.records


def test_duplicate_payload_does_not_update_first_receipt_or_become_new_information():
    original = _bar("RELIANCE", 100, receipt_delay=2)
    store = CanonicalBarStore()
    first = store.plan_ingest(
        [original], decision_timestamp=datetime(2026, 9, 28, 11, 2, tzinfo=IST),
        expected_symbols=["RELIANCE"], warmed_up=False,
    )
    store.commit(first)
    later_duplicate = dict(original, received_timestamp="2026-09-28T11:05:00+05:30")
    second = store.plan_ingest(
        [later_duplicate], decision_timestamp=datetime(2026, 9, 28, 11, 6, tzinfo=IST),
        expected_symbols=["RELIANCE"], warmed_up=True,
    )
    assert second.accounting["new_market_bars"] == 0
    assert second.accounting["duplicate_market_bars"] == 1
    assert second.accounting["eligible_bars"] == 1
    assert store.records[("RELIANCE", original["market_timestamp"])]["received_timestamp"] == original["received_timestamp"]


def test_processor_conflict_blocks_before_paper_ledger_mutation(tmp_path):
    class NoSignalScanner:
        def handle_bar(self, _bar):
            return []

    instruments = load_yahoo_instruments(Path(__file__).resolve().parents[1], size=100)
    reliance = {"RELIANCE": instruments["RELIANCE"]}
    processor = Phase4GYFForwardProcessor(
        project_root=Path(__file__).resolve().parents[1],
        output_dir=tmp_path, instruments=reliance, scanner=NoSignalScanner(),
    )
    original = _bar("RELIANCE", 0)
    first = processor.process_snapshot(
        [original], now=datetime(2026, 9, 28, 9, 17, 5, tzinfo=IST),
    )
    assert first["status"] == "PHASE4G_YF_WARMED_UP_NO_RETRO_SIGNALS"
    ledger_path = tmp_path / "paper_ledger.json"
    assert not ledger_path.exists()
    conflicting = dict(original, high=103.0, close=102.0,
                       received_timestamp="2026-09-28T09:18:00+05:30")
    with pytest.raises(Phase4GYFError, match="PHASE4G_YF_IMMUTABLE_BAR_READINESS_BLOCKED"):
        processor.process_snapshot(
            [conflicting], now=datetime(2026, 9, 28, 9, 19, tzinfo=IST),
        )
    assert not ledger_path.exists()
    stored = next(iter(processor.bar_rows.values()))
    assert stored["close"] == 100.0
    assert stored["received_timestamp"] == original["received_timestamp"]


def test_empty_provider_refresh_is_audited_and_does_not_mutate_ledger(tmp_path):
    class NoSignalScanner:
        def handle_bar(self, _bar):
            return []

    project_root = Path(__file__).resolve().parents[1]
    instruments = load_yahoo_instruments(project_root, size=100)
    processor = Phase4GYFForwardProcessor(
        project_root=project_root, output_dir=tmp_path,
        instruments={"BLUEDART": instruments["BLUEDART"]}, scanner=NoSignalScanner(),
    )
    with pytest.raises(Phase4GYFError, match="YAHOO_NO_INTRADAY_DATA"):
        processor.process_snapshot(
            [], now=datetime(2026, 9, 28, 12, 40, tzinfo=IST),
            refresh_failures={"BLUEDART": {"error_type": "YAHOO_PROVIDER_WARNING",
                                            "error_message": "possibly delisted; no price data found"}},
        )
    refresh_audit = (tmp_path / "reports/refresh_audit.jsonl").read_text(encoding="utf-8").splitlines()
    bar_audit = (tmp_path / "reports/bar_accounting.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(refresh_audit) == len(bar_audit) == 1
    assert '"rows_seen": 0' in refresh_audit[0]
    assert '"fetch_status": "DATA_UNAVAILABLE_FOR_THIS_REFRESH"' in bar_audit[0]
    assert '"error_message": "possibly delisted; no price data found"' in bar_audit[0]
    assert not (tmp_path / "paper_ledger.json").exists()


def test_phase4c_risk_budget_caps_one_percent_risk_sizing(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    instruments = load_yahoo_instruments(project_root, size=100)
    processor = Phase4GYFForwardProcessor(
        project_root=project_root, output_dir=tmp_path,
        instruments=instruments,
    )
    quantity, budget, planned_risk, rejection = processor.engine.risk_engine.size(
        equity=100_000, cash_available=100_000, exposure_available=100_000,
        entry=100, stop=95,
    )
    assert rejection is None
    assert budget == 1_000
    assert 0 < quantity <= 200
    assert planned_risk <= budget
    assert not (tmp_path / "paper_ledger.json").exists()
