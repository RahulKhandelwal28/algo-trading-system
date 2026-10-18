from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nse_research.intraday.yahoo_revision_reconciler import YahooRevisionReconciler
from nse_research.intraday.bar_store import bar_eligibility_reason


IST = ZoneInfo("Asia/Kolkata")
SESSION = date(2026, 9, 28)


def _row(received: str, *, close: float = 100, volume: float = 0,
         market: str = "2026-09-28T12:35:00+05:30") -> dict[str, object]:
    return {
        "symbol": "TEST", "timestamp": market, "market_timestamp": market,
        "received_timestamp": received, "timeframe": "1m", "open": 100,
        "high": max(101, close), "low": min(99, close), "close": close,
        "volume": volume, "status": "CLOSED", "source": "mock_yahoo",
        "exchange": "NSE", "series": "EQ", "volume_kind": "BAR",
    }


def test_forming_and_settling_revisions_reconcile_then_freeze_latest_version():
    reconciler = YahooRevisionReconciler(session_date=SESSION, settling_seconds=5,
                                         expected_symbols=["TEST"])
    first = _row("2026-09-28T12:35:30+05:30", close=100, volume=0)
    reconciler.observe_many([first], decision_timestamp=datetime(2026, 9, 28, 12, 36, 2, tzinfo=IST))
    assert reconciler.mutable_status_counts()["SETTLING_BAR"] == 1
    assert reconciler.immutable_rows() == []

    revision = _row("2026-09-28T12:36:02+05:30", close=100, volume=1000)
    reconciler.observe_many([revision], decision_timestamp=datetime(2026, 9, 28, 12, 36, 4, tzinfo=IST))
    assert reconciler.revision_records[0]["classification"] == "PRE_IMMUTABILITY_REVISION"
    assert reconciler.immutable_rows() == []

    frozen = reconciler.advance(datetime(2026, 9, 28, 12, 36, 5, tzinfo=IST))
    assert len(frozen) == 1
    assert frozen[0]["bar_status"] == "IMMUTABLE_CLOSED"
    assert frozen[0]["volume"] == 1000
    assert frozen[0]["immutable_eligible_at"] == "2026-09-28T12:36:05+05:30"
    assert reconciler.readiness_report()["status"] == "READY"


def test_different_payload_after_freeze_blocks_and_keeps_immutable_snapshot():
    reconciler = YahooRevisionReconciler(session_date=SESSION, settling_seconds=5,
                                         expected_symbols=["TEST"])
    reconciler.observe_many(
        [_row("2026-09-28T12:36:02+05:30", close=100, volume=1000)],
        decision_timestamp=datetime(2026, 9, 28, 12, 36, 5, tzinfo=IST),
    )
    frozen = reconciler.immutable_rows()[0]
    reconciler.observe_many(
        [_row("2026-09-28T12:37:00+05:30", close=101, volume=1200)],
        decision_timestamp=datetime(2026, 9, 28, 12, 37, tzinfo=IST),
    )
    after = reconciler.immutable_rows()[0]
    report = reconciler.readiness_report()
    assert after["observation_hash"] == frozen["observation_hash"]
    assert after["close"] == 100 and after["volume"] == 1000
    assert report["status"] == "BLOCKED"
    assert "POST_IMMUTABILITY_REVISION" in report["blocking_reasons"]
    assert reconciler.revision_records[-1]["classification"] == "POST_IMMUTABILITY_REVISION"
    assert reconciler.revision_records[-1]["previous_close"] == 100
    assert reconciler.revision_records[-1]["new_close"] == 101


def test_canonical_identity_ignores_receipt_time_and_preserves_every_duplicate_observation():
    reconciler = YahooRevisionReconciler(session_date=SESSION, settling_seconds=5,
                                         expected_symbols=["TEST"])
    rows = [
        _row("2026-09-28T12:36:02+05:30", volume=1000),
        _row("2026-09-28T12:36:03+05:30", volume=1000),
    ]
    reconciler.observe_many(rows, decision_timestamp=datetime(2026, 9, 28, 12, 36, 5, tzinfo=IST))
    assert len(reconciler.timelines) == 1
    assert len(reconciler.observations) == 2
    assert reconciler.observations[1]["observation_status"] == "IDENTICAL_DUPLICATE"
    assert reconciler.observations[1]["first_seen"] == "2026-09-28T12:36:02+05:30"
    assert reconciler.observations[1]["last_seen"] == "2026-09-28T12:36:03+05:30"


def test_naive_timestamps_and_same_receipt_different_payloads_block_readiness():
    reconciler = YahooRevisionReconciler(session_date=SESSION, settling_seconds=5,
                                         expected_symbols=["TEST"])
    malformed = _row("2026-09-28T12:36:00")
    same_receipt = [
        _row("2026-09-28T12:36:02+05:30", volume=1000),
        _row("2026-09-28T12:36:02+05:30", volume=1200),
    ]
    reconciler.observe_many([malformed, *same_receipt],
                            decision_timestamp=datetime(2026, 9, 28, 12, 36, 4, tzinfo=IST))
    report = reconciler.readiness_report()
    assert report["status"] == "BLOCKED"
    assert "INVALID_OR_UNTRUSTED_OBSERVATION" in report["blocking_reasons"]
    assert "UNRESOLVED_SAME_RECEIPT_CONFLICT" in report["blocking_reasons"]
    assert any(item["classification"] == "UNKNOWN" for item in reconciler.revision_records)


def test_reconciliation_artifacts_are_machine_readable_and_capture_provenance(tmp_path: Path):
    reconciler = YahooRevisionReconciler(session_date=SESSION, settling_seconds=5,
                                         expected_symbols=["TEST"])
    reconciler.observe_many([_row("2026-09-28T12:36:02+05:30", volume=1000)],
                            decision_timestamp=datetime(2026, 9, 28, 12, 36, 5, tzinfo=IST))
    summary = reconciler.write_artifacts(tmp_path, decision_timestamp=datetime(2026, 9, 28, 12, 36, 5, tzinfo=IST))
    assert summary["status"] == "READY"
    assert json.loads((tmp_path / "reports/readiness_report.json").read_text())["status"] == "READY"
    observation = json.loads((tmp_path / "reports/observations.jsonl").read_text().splitlines()[0])
    assert observation["received_timestamp"] == "2026-09-28T12:36:02+05:30"
    assert observation["observation_hash"]
    immutable = json.loads((tmp_path / "reports/immutable_bar_audit.jsonl").read_text().splitlines()[0])
    assert immutable["bar_status"] == "IMMUTABLE_CLOSED"


def test_network_completion_order_does_not_change_latest_pre_freeze_snapshot():
    early = _row("2026-09-28T12:35:30+05:30", close=100, volume=0)
    middle = _row("2026-09-28T12:35:50+05:30", close=100, volume=1000)
    late = _row("2026-09-28T12:36:03+05:30", close=100, volume=1500)
    decision = datetime(2026, 9, 28, 12, 36, 5, tzinfo=IST)
    results = []
    for delivery_order in ([early, middle, late], [late, early, middle], [middle, late, early]):
        reconciler = YahooRevisionReconciler(session_date=SESSION, settling_seconds=5,
                                             expected_symbols=["TEST"])
        reconciler.observe_many(delivery_order, decision_timestamp=decision)
        results.append((reconciler.immutable_rows()[0], reconciler.revision_records))
    assert all(item[0]["observation_hash"] == results[0][0]["observation_hash"] for item in results)
    assert all(bar["volume"] == 1500 for bar, _ in results)
    assert [item["classification"] for item in results[0][1]] == [
        item["classification"] for item in results[1][1]
    ] == ["PRE_CLOSE_REVISION", "PRE_IMMUTABILITY_REVISION"]


def test_late_data_is_excluded_from_past_decision_but_available_to_later_decision():
    row = _row("2026-09-28T12:40:00+05:30", volume=1000)
    row["bar_status"] = "IMMUTABLE_CLOSED"
    assert bar_eligibility_reason(
        row, datetime(2026, 9, 28, 12, 36, tzinfo=IST), settling_seconds=5,
    ) == "late_receipt"
    assert bar_eligibility_reason(
        row, datetime(2026, 9, 28, 12, 41, tzinfo=IST), settling_seconds=5,
    ) is None


def test_market_bar_at_or_after_decision_is_never_available():
    row = _row("2026-09-28T12:41:00+05:30", market="2026-09-28T12:41:00+05:30", volume=1000)
    reconciler = YahooRevisionReconciler(session_date=SESSION, settling_seconds=5,
                                         expected_symbols=["TEST"])
    reconciler.observe_many(
        [row], decision_timestamp=datetime(2026, 9, 28, 12, 41, tzinfo=IST),
    )
    assert reconciler.immutable_rows() == []
    assert "FUTURE_DATA_VIOLATION" in reconciler.readiness_report()["blocking_reasons"]
