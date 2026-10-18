from __future__ import annotations

import json
import hashlib
import sys
import csv
import importlib.util
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from nse_research.data.yfinance_intraday import (
    NormalizedYahooBars, YahooInstrument, YFinanceDataError, download_yfinance_history,
    infer_interval_volume_semantics, load_yahoo_instruments, normalize_yfinance_frame,
)
from nse_research.execution.phase4c import inspect_paper_ledger
from nse_research.data.yfinance_batching import BatchAttempt, BatchFetchResult
from nse_research.intraday.phase4g_yf import (
    SESSION_DATE, Phase4GYFError, Phase4GYFForwardProcessor,
    aggregate_complete_1m_to_5m, build_strategy_signal_summary,
    build_decision_snapshot, build_top_candidate_summary, check_missing_bar_fraction, paper_config,
    phase4g_preflight, rank_candidates, reconciliation_report, register_phase4g_dataset,
)
from nse_research.intraday.indicators import IndicatorSnapshot
from nse_research.intraday.model import IntradayBar
from nse_research.intraday.strategies import (
    IntradaySignal, StrategyCandidate, StrategySettings, candidate_to_signal,
)


ROOT = Path(__file__).resolve().parents[1]
IST = ZoneInfo("Asia/Kolkata")
_RUNNER_SPEC = importlib.util.spec_from_file_location(
    "phase4g_yf_runner_test", ROOT / "scripts/run_phase4g_yf_forward.py",
)
phase4g_runner = importlib.util.module_from_spec(_RUNNER_SPEC)
assert _RUNNER_SPEC and _RUNNER_SPEC.loader
_RUNNER_SPEC.loader.exec_module(phase4g_runner)


def _instrument(symbol: str = "TEST") -> YahooInstrument:
    return YahooInstrument(symbol, f"{symbol}.NS", "NSE", "EQ", "INE000A01000")


def _frame(stamps, *, values=None, tz="Asia/Kolkata"):
    index = pd.DatetimeIndex(stamps, tz=tz) if tz else pd.DatetimeIndex(stamps)
    count = len(index)
    vals = values or {}
    return pd.DataFrame({
        "Open": vals.get("open", [100.0] * count),
        "High": vals.get("high", [102.0] * count),
        "Low": vals.get("low", [99.0] * count),
        "Close": vals.get("close", [101.0] * count),
        "Volume": vals.get("volume", [10.0] * count),
    }, index=index)


def _ranking_rows(symbol: str, *, slope: float = 0.1, volume_scale: float = 1.0):
    rows = []
    start = datetime.combine(SESSION_DATE, time(9, 15), IST)
    for index in range(21):
        close = 100 + slope * index
        rows.append({
            "timestamp": (start + timedelta(minutes=index)).isoformat(),
            "market_timestamp": (start + timedelta(minutes=index)).isoformat(),
            "received_timestamp": (start + timedelta(minutes=index + 1)).isoformat(),
            "request_started_at": (start + timedelta(minutes=index)).isoformat(),
            "response_received_at": (start + timedelta(minutes=index + 1)).isoformat(),
            "batch_id": 1, "symbol": symbol,
            "exchange": "NSE", "series": "EQ", "timeframe": "1m",
            "open": close, "high": close + 0.5, "low": close - 0.5, "close": close,
            "volume": (100 + index) * volume_scale, "volume_kind": "BAR", "status": "CLOSED",
            "bar_status": "IMMUTABLE_CLOSED",
        })
    return rows


def test_frozen_universe_yahoo_map_is_explicit_nse_eq_and_complete():
    mapped = load_yahoo_instruments(ROOT, size=486)
    assert len(mapped) == 486
    assert len({item.yahoo_symbol for item in mapped.values()}) == 486
    assert all(item.exchange == "NSE" and item.series == "EQ" and item.isin for item in mapped.values())
    assert all(item.yahoo_symbol == f"{item.symbol}.NS" for item in mapped.values())


def test_yfinance_downloader_is_bounded_unadjusted_and_mock_only():
    calls = []
    expected = _frame(["2026-09-28 09:15"], tz="Asia/Kolkata")

    def fake_download(**kwargs):
        calls.append(kwargs)
        return expected

    result = download_yfinance_history(["TEST.NS"], start="2026-09-28", end="2026-09-29",
                                       interval="1m", downloader=fake_download)
    assert result is expected
    assert calls[0]["auto_adjust"] is False
    assert calls[0]["back_adjust"] is False
    assert calls[0]["repair"] is False
    assert calls[0]["prepost"] is False
    assert calls[0]["threads"] is False
    assert calls[0]["timeout"] == 15.0


def test_yfinance_downloader_reports_timeout_and_empty_intraday():
    with pytest.raises(YFinanceDataError, match="YAHOO_TIMEOUT"):
        download_yfinance_history(["TEST.NS"], start="2026-09-28", end="2026-09-29",
                                  downloader=lambda **_: (_ for _ in ()).throw(TimeoutError("mock timeout")))
    with pytest.raises(YFinanceDataError, match="YAHOO_NO_INTRADAY_DATA"):
        download_yfinance_history(["TEST.NS"], start="2026-09-28", end="2026-09-29",
                                  downloader=lambda **_: pd.DataFrame())


def test_yfinance_normalization_preserves_source_time_and_converts_ist():
    frame = _frame(["2026-09-28 03:45", "2026-09-28 03:46", "2026-09-28 03:47"], tz="UTC")
    frame["Adj Close"] = [98.0, 99.0, 100.0]
    result = normalize_yfinance_frame(frame, {"TEST": _instrument()}, interval="1m",
                                      now=datetime(2026, 9, 28, 9, 17, 30, tzinfo=IST),
                                      session_date=SESSION_DATE)
    assert [row["timestamp"] for row in result.rows] == [
        "2026-09-28T09:15:00+05:30", "2026-09-28T09:16:00+05:30",
    ]
    assert result.rows[0]["source_timestamp"].endswith("+00:00")
    assert result.rows[0]["market_timestamp"] == "2026-09-28T09:15:00+05:30"
    assert result.rows[0]["received_timestamp"] == "2026-09-28T09:17:30+05:30"
    assert result.rows[0]["source_timezone"] == "UTC"
    assert result.rows[0]["adjustment_status"] == "UNKNOWN"
    assert result.rows[0]["close"] == result.rows[0]["unadjusted_close"] == 101.0
    assert result.rows[0]["adjusted_close"] == 98.0
    assert result.forming_rows_ignored == 1


def test_yfinance_skips_empty_union_index_cells_without_filling_missing_bars():
    frame = _frame(
        ["2026-09-28 09:15", "2026-09-28 09:16", "2026-09-28 09:17"],
        values={"open": [100.0, None, None], "high": [102.0, None, None],
                "low": [99.0, None, None], "close": [101.0, None, None],
                "volume": [10.0, 0.0, None]},
    )
    result = normalize_yfinance_frame(
        frame, {"TEST": _instrument()}, interval="1m",
        now=datetime(2026, 9, 28, 9, 19, tzinfo=IST), session_date=SESSION_DATE,
    )
    assert [row["timestamp"] for row in result.rows] == ["2026-09-28T09:15:00+05:30"]
    assert result.empty_no_trade_cells_ignored == 2
    assert result.invalid_symbols == []
    assert result.missing_symbols == []


def test_yfinance_normalization_rejects_naive_and_duplicate_timestamps():
    naive = _frame(["2026-09-28 09:15"], tz=None)
    with pytest.raises(YFinanceDataError, match="UNKNOWN_TIMEZONE"):
        normalize_yfinance_frame(naive, {"TEST": _instrument()}, interval="1m",
                                 now=datetime(2026, 9, 28, 9, 17, tzinfo=IST), session_date=SESSION_DATE)
    duplicate = _frame(["2026-09-28 09:15", "2026-09-28 09:15"], tz="Asia/Kolkata")
    with pytest.raises(YFinanceDataError, match="YAHOO_DUPLICATE_TIMESTAMP"):
        normalize_yfinance_frame(duplicate, {"TEST": _instrument()}, interval="1m",
                                 now=datetime(2026, 9, 28, 9, 17, tzinfo=IST), session_date=SESSION_DATE)


@pytest.mark.parametrize(
    ("values", "timestamp", "now", "expected_code"),
    [
        ({"high": [99.0]}, "2026-09-28 09:15", datetime(2026, 9, 28, 9, 17, tzinfo=IST), "YAHOO_OHLC_INVALID"),
        ({"open": [None]}, "2026-09-28 09:15", datetime(2026, 9, 28, 9, 17, tzinfo=IST), "YAHOO_NUMERIC_FIELD_INVALID"),
        ({"volume": [-1.0]}, "2026-09-28 09:15", datetime(2026, 9, 28, 9, 17, tzinfo=IST), "YAHOO_VOLUME_INVALID"),
        ({}, "2026-09-28 09:14", datetime(2026, 9, 28, 9, 16, tzinfo=IST), "YAHOO_SESSION_BOUNDARY_INVALID"),
    ],
)
def test_yfinance_normalization_marks_invalid_symbols_without_admitting_rows(values, timestamp, now, expected_code):
    frame = _frame([timestamp], values=values, tz="Asia/Kolkata")
    result = normalize_yfinance_frame(frame, {"TEST": _instrument()}, interval="1m", now=now,
                                      session_date=SESSION_DATE)
    assert result.rows == []
    assert result.invalid_symbols == ["TEST"]
    assert result.issues[0]["code"] == expected_code


def test_partial_yahoo_response_has_explicit_failed_and_missing_symbols():
    frame = _frame(["2026-09-28 09:15"], tz="Asia/Kolkata")
    frame.columns = pd.MultiIndex.from_product([["TEST.NS"], frame.columns])
    result = normalize_yfinance_frame(frame, {"TEST": _instrument(), "ABSENT": _instrument("ABSENT")},
                                      interval="1m", now=datetime(2026, 9, 28, 9, 17, tzinfo=IST),
                                      session_date=SESSION_DATE)
    assert result.symbols_with_data == ["TEST"]
    assert result.missing_symbols == ["ABSENT"]
    assert result.failed_symbols == ["ABSENT"]
    assert result.rows[0]["symbol"] == "TEST"


def test_possibly_delisted_warning_is_transient_unavailability_not_a_delisting_fact():
    stamp = "2026-09-28T12:40:00+05:30"
    warning = "$BLUEDART.NS: possibly delisted; no price data found"
    batch = BatchFetchResult(
        batch_id=1, symbols=("BLUEDART",), yahoo_symbols=("BLUEDART.NS",),
        status="FAILED", frame=None,
        attempts=(BatchAttempt(1, stamp, stamp, None, 0.0, "FAILED", "YFinanceDataError", "no data"),),
        request_started_at=stamp, request_finished_at=stamp, response_received_at=None,
        error_type="YFinanceDataError", error_message="YAHOO_NO_INTRADAY_DATA",
        provider_warnings=(warning,),
    )
    normalized = NormalizedYahooBars(
        rows=[], requested_symbols=["BLUEDART"], symbols_with_data=[], missing_symbols=["BLUEDART"],
        forming_rows_ignored=0, source_timezone="UNKNOWN", interval="1m",
        raw_index_first=None, raw_index_last=None,
        issues=[{"symbol": "BLUEDART", "code": "YAHOO_NO_INTRADAY_DATA", "detail": "no data"}],
        failed_symbols=["BLUEDART"], invalid_symbols=[], duplicate_symbols=[],
    )
    failures, issues = phase4g_runner._current_refresh_failures(
        [batch], normalized, attempt_timestamp=datetime(2026, 9, 28, 12, 40, tzinfo=IST),
    )
    assert failures["BLUEDART"]["error_type"] == "YAHOO_PROVIDER_WARNING"
    assert failures["BLUEDART"]["error_message"] == warning
    assert all(item.get("delisting_conclusion") == "NONE" for item in issues if item.get("symbol") == "BLUEDART"
               or "symbols" in item)
    assert not normalized.rows


def test_zero_volume_is_retained_and_interval_volume_inference_is_explicitly_empirical():
    frame = _frame(["2026-09-28 09:15"], values={"volume": [0]}, tz="Asia/Kolkata")
    result = normalize_yfinance_frame(frame, {"TEST": _instrument()}, interval="1m",
                                      now=datetime(2026, 9, 28, 9, 17, tzinfo=IST), session_date=SESSION_DATE)
    assert result.rows[0]["volume"] == 0
    assert result.rows[0]["volume_kind"] == "UNKNOWN"
    observations = [{"timestamp": f"2026-09-28T09:{15 + i:02d}:00+05:30", "symbol": "TEST", "volume": v}
                   for i, v in enumerate([100, 50, 75, 40, 80])]
    evidence = infer_interval_volume_semantics(observations, minimum_pairs=4, minimum_decrease_ratio=0.05)
    assert evidence["volume_kind"] == "BAR"
    assert evidence["classification"] == "INTERVAL_BAR_EMPIRICAL"
    assert "not a provider guarantee" in evidence["evidence_limit"]


def test_5m_aggregation_requires_five_closed_contiguous_real_1m_bars():
    rows = _ranking_rows("TEST")[:5]
    derived = aggregate_complete_1m_to_5m(rows)
    assert len(derived) == 1
    assert derived[0]["timestamp"] == "2026-09-28T09:15:00+05:30"
    assert derived[0]["open"] == rows[0]["open"]
    assert derived[0]["close"] == rows[-1]["close"]
    assert derived[0]["volume"] == sum(row["volume"] for row in rows)
    assert aggregate_complete_1m_to_5m(rows[:2] + rows[3:]) == []
    with pytest.raises(Phase4GYFError, match="AGGREGATION_FORMING_BAR_REJECTED"):
        aggregate_complete_1m_to_5m([dict(rows[0], status="FORMING")])


def test_candidate_ranking_is_deterministic_cross_sectional_and_chronological():
    rows = _ranking_rows("AAA", slope=0.2) + _ranking_rows("BBB", slope=-0.1, volume_scale=2)
    as_of = datetime(2026, 9, 28, 9, 36, 5, tzinfo=IST)
    first = rank_candidates(rows, as_of=as_of, top_n=2, minimum_bars=20)
    second = rank_candidates(list(reversed(rows)), as_of=as_of, top_n=2, minimum_bars=20)
    assert first == second
    assert [row["rank"] for row in first] == [1, 2]
    assert set(first[0]) >= {"momentum", "relative_volume", "vwap", "vwap_distance", "range_expansion", "score"}
    assert first[0]["decision_timestamp"] == as_of.isoformat()
    later_bar = dict(rows[20], timestamp="2026-09-28T09:36:00+05:30",
                     market_timestamp="2026-09-28T09:36:00+05:30",
                     received_timestamp="2026-09-28T09:37:00+05:30",
                     response_received_at="2026-09-28T09:37:00+05:30")
    later = rank_candidates(rows + [later_bar], as_of=as_of + timedelta(minutes=1),
                             top_n=2, minimum_bars=20)
    assert later


def test_candidate_ranking_rejects_future_rows_and_unverified_volume():
    rows = _ranking_rows("AAA")
    with pytest.raises(Phase4GYFError, match="FUTURE_DATA_ACCESS"):
        rank_candidates(rows + [dict(rows[-1], timestamp="2026-09-28T09:36:00+05:30",
                                    market_timestamp="2026-09-28T09:36:00+05:30")],
                        as_of=datetime(2026, 9, 28, 9, 36, 5, tzinfo=IST))
    with pytest.raises(Phase4GYFError, match="VOLUME_SEMANTICS_UNVERIFIED"):
        rank_candidates([dict(row, volume_kind="UNKNOWN") for row in rows],
                        as_of=datetime(2026, 9, 28, 9, 36, 5, tzinfo=IST))


def test_0930_decision_excludes_0925_bar_received_at_0931_then_allows_later_decision():
    late_bar = _ranking_rows("TEST")[10]
    assert late_bar["market_timestamp"] == "2026-09-28T09:25:00+05:30"
    late_bar["received_timestamp"] = "2026-09-28T09:31:00+05:30"
    at_0930 = build_decision_snapshot(
        [late_bar], decision=datetime(2026, 9, 28, 9, 30, tzinfo=IST), expected_symbols=["TEST"],
    )
    assert at_0930["bars_eligible_by_decision_time"] == 0
    assert at_0930["bars_excluded_by_receipt_time"] == 1
    assert at_0930["symbols_available"] == 0
    at_0932 = build_decision_snapshot(
        [late_bar], decision=datetime(2026, 9, 28, 9, 32, tzinfo=IST), expected_symbols=["TEST"],
    )
    assert at_0932["bars_eligible_by_decision_time"] == 1
    assert at_0932["symbols_available"] == 1


def test_strategy_target_uses_existing_configured_target_multiple_without_future_bars():
    bar = IntradayBar(
        symbol="TEST", exchange="NSE", series="EQ", timeframe="1m",
        timestamp=datetime(2026, 9, 28, 9, 20, tzinfo=IST), open=99, high=101,
        low=98, close=100, volume=100, status="CLOSED", source="yahoo_finance_yfinance",
        volume_kind="BAR",
    )
    snapshot = IndicatorSnapshot(
        symbol="TEST", timeframe="1m", timestamp=bar.end, close=100, vwap=99,
        vwap_status="READY", ema9=100, ema20=99, ema50=98, rsi14=60, atr14=5,
        volume_avg_20=100, prior_volume_avg_20=100, opening_range_5m_high=101,
        opening_range_5m_low=98, opening_range_15m_high=101, opening_range_15m_low=98,
        bars_in_session=21, indicator_ready=True,
    )
    candidate = StrategyCandidate("momentum", "1m", "setup", ("CLOSED_BAR",), {})
    settings = StrategySettings()
    signal = candidate_to_signal(candidate, bar, snapshot, settings)
    assert signal is not None
    assert signal.reference_price == 100
    assert signal.stop_reference == 95
    # Preserve the existing Phase 4B 1.5R setting rather than changing strategy parameters.
    assert settings.target_r_multiple == 1.5
    assert signal.target_reference == 107.5
    assert (signal.target_reference - signal.reference_price) / (signal.reference_price - signal.stop_reference) == 1.5


def test_missing_bar_report_does_not_fill_gaps():
    rows = [
        {"timestamp": "2026-09-28T09:15:00+05:30", "symbol": "TEST"},
        {"timestamp": "2026-09-28T09:17:00+05:30", "symbol": "TEST"},
    ]
    report = check_missing_bar_fraction(rows, session_date=SESSION_DATE)
    assert report["expected_slots"] == 3
    assert report["observed_slots"] == 2
    assert report["missing_slots"] == 1
    assert report["within_limit"] is False
    assert report["gap_policy"] == "report only; never fill"


def test_reconciliation_reports_late_and_changed_bars_without_mutating_inputs():
    forward = _ranking_rows("AAA")[:1]
    final = [dict(forward[0], close=forward[0]["close"] + 1),
             dict(forward[0], timestamp="2026-09-28T09:16:00+05:30")]
    before = deepcopy(forward)
    report = reconciliation_report(forward, final, requested_symbols=["AAA", "BBB"])
    assert report["price_difference_count"] == 1
    assert report["late_arriving_bars"] == 1
    assert report["bars_available_after_close_not_seen_during_session"] == 1
    assert report["missing_forward_symbols"] == ["BBB"]
    assert report["signal_mutation"] == "PROHIBITED_AND_NOT_PERFORMED"
    assert forward == before


def test_candidate_and_signal_summary_keep_ranks_and_trade_results_separate():
    rankings = [
        {"symbol": "AAA", "rank": 1, "score": 0.9, "decision_timestamp": "2026-09-28T10:00:00+05:30"},
        {"symbol": "AAA", "rank": 3, "score": 0.4, "decision_timestamp": "2026-09-28T10:30:00+05:30"},
    ]
    summary = build_top_candidate_summary(rankings)[0]
    assert summary["best_rank"] == 1 and summary["maximum_rank"] == 1 and summary["worst_rank"] == 3
    signal_rows = [{"signal_id": "s1", "symbol": "AAA", "strategy": "momentum", "timestamp": "t",
                    "direction": "LONG", "entry_reference": 10, "stop_reference": 9,
                    "target_reference": 12, "candidate_rank": 1}]
    table = build_strategy_signal_summary(
        signal_rows, [{"signal_id": "s1", "status": "FILLED"}], [{"signal_id": "s1", "net_pnl": 1.5}],
    )
    assert table[0]["paper_order_status"] == "FILLED"
    assert table[0]["paper_result_net_pnl"] == 1.5


def test_preflight_is_read_only_reports_future_session_and_486_symbols(monkeypatch, tmp_path):
    report = phase4g_preflight(ROOT, now=datetime(2026, 9, 27, 12, 0, tzinfo=IST), session_date=SESSION_DATE)
    assert report["status"] == "PHASE4G_YF_PREPARED_NOT_RUN"
    assert report["symbols_requested"] == 486
    assert report["symbols_expected"] == 486
    assert report["authorized_nse_feed"] is False
    assert report["paper_only"] is True
    assert report["broker_calls"] == report["live_orders"] == report["network_calls"] == 0
    assert report["calendar_check"] == "TRADING_DAY"
    assert report["checks"]["nse_session"]["status"] == "PASS"
    assert report["checks"]["yahoo_identity_mapping"]["status"] == "PASS"
    assert report["checks"]["phase4b_strategy_configuration"]["status"] == "PASS"
    assert report["checks"]["phase4c_risk_configuration"]["status"] == "PASS"
    assert report["checks"]["temporary_session_ledger"]["status"] == "PASS"
    assert report["interval"] == "1m"
    assert report["temporary_ledger"].endswith("results/phase4g_yf_20260928_batched/paper_ledger.json")
    assert report["persistent_phase3d_ledger_used"] is False
    assert report["synthetic_data_fallback"] is False
    assert report["daily_to_intraday_fallback"] is False
    assert report["future_data_fallback"] is False
    assert not (tmp_path / "paper_ledger.json").exists()


def test_preflight_blocks_weekday_not_explicitly_validated_by_nse_calendar():
    report = phase4g_preflight(
        ROOT, now=datetime(2026, 9, 27, 12, 0, tzinfo=IST), session_date=date(2026, 9, 29),
    )
    assert report["status"] == "PHASE4G_YF_BLOCKED_SESSION_NOT_VALIDATED"
    assert report["calendar_check"] == "DATA_UNAVAILABLE"
    assert any(reason.startswith("SESSION_NOT_VALIDATED") for reason in report["reasons"])


def test_preflight_fails_closed_when_yfinance_unavailable(monkeypatch):
    monkeypatch.setitem(sys.modules, "yfinance", None)
    report = phase4g_preflight(ROOT, now=datetime(2026, 9, 27, 12, 0, tzinfo=IST), session_date=SESSION_DATE)
    assert report["status"] == "PHASE4G_YF_BLOCKED_YFINANCE_UNAVAILABLE"
    assert any(item.startswith("YFINANCE_UNAVAILABLE") for item in report["reasons"])


def test_collection_blocks_unvalidated_session_before_any_yahoo_request(monkeypatch, tmp_path):
    requests = []
    monkeypatch.setattr(phase4g_runner, "acquire_batches", lambda *args, **kwargs: requests.append(kwargs))
    status = phase4g_runner.run_collection(
        tmp_path,
        session_date=date(2026, 9, 29),
        now_fn=lambda _timezone: datetime(2026, 9, 29, 9, 15, tzinfo=IST),
        sleep_fn=lambda _seconds: None,
    )
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert status == 2
    assert manifest["status"] == "PHASE4G_YF_BLOCKED_SESSION_NOT_VALIDATED"
    assert requests == []


def _session_rows(symbols, last_minute: int):
    start = datetime.combine(SESSION_DATE, time(9, 15), IST)
    rows = []
    for symbol in symbols:
        for minute in range(last_minute + 1):
            price = 100 + minute * 0.1
            stamp = start + timedelta(minutes=minute)
            rows.append({
                "timestamp": stamp.isoformat(), "market_timestamp": stamp.isoformat(),
                "received_timestamp": (stamp + timedelta(minutes=1)).isoformat(),
                "request_started_at": stamp.isoformat(),
                "response_received_at": (stamp + timedelta(minutes=1)).isoformat(),
                "batch_id": 1, "symbol": symbol, "exchange": "NSE", "series": "EQ",
                "timeframe": "1m", "open": price, "high": price + 0.2, "low": price - 0.2,
                "close": price, "volume": 100 + minute, "volume_kind": "BAR", "status": "CLOSED",
                "source": "yahoo_finance_yfinance", "source_timestamp": stamp.isoformat(),
                "source_timezone": "Asia/Kolkata", "yahoo_symbol": f"{symbol}.NS", "isin": "INE000A01000",
                "adjustment_status": "UNKNOWN",
            })
    return rows


class _SignalScanner:
    def __init__(self, signal_symbol: str):
        self.signal_symbol = signal_symbol

    def handle_bar(self, bar):
        if bar.symbol != self.signal_symbol or bar.timestamp.minute != 18:
            return []
        at = bar.end
        return [IntradaySignal(
            signal_id=f"signal-{bar.symbol}-{at.isoformat()}", timestamp=at,
            valid_until=at + timedelta(minutes=3), symbol=bar.symbol, exchange="NSE", series="EQ",
            side="LONG", strategy="test_strategy", timeframe="1m", reference_price=bar.close,
            stop_reference=bar.close - 5, target_reference=bar.close + 7.5,
            confidence_metadata={"software_test_only": True}, reason_codes=("TEST",), setup_id="test",
        )]


def test_forward_processor_defers_orders_until_later_snapshot_and_persists_idempotently(tmp_path):
    instruments = load_yahoo_instruments(ROOT, size=100)
    signal_symbol = "RELIANCE"
    assert signal_symbol in instruments
    processor = Phase4GYFForwardProcessor(project_root=ROOT, output_dir=tmp_path,
                                          instruments=instruments, scanner=_SignalScanner(signal_symbol))
    first_rows = _session_rows(list(instruments), 0)
    warm = processor.process_snapshot(first_rows, now=datetime(2026, 9, 28, 9, 16, 5, tzinfo=IST))
    assert warm["status"] == "PHASE4G_YF_WARMED_UP_NO_RETRO_SIGNALS"
    assert not (tmp_path / "paper_ledger.json").exists()

    second_rows = _session_rows(list(instruments), 3)
    second = processor.process_snapshot(second_rows, now=datetime(2026, 9, 28, 9, 19, 5, tzinfo=IST))
    assert second["new_signals"] == 1
    emitted = processor.signals[0]
    assert emitted["timestamp"] == "2026-09-28T09:19:05+05:30"
    assert emitted["signal_bar_timestamp"] == "2026-09-28T09:18:00+05:30"
    assert emitted["received_timestamp"] == "2026-09-28T09:19:00+05:30"
    assert emitted["decision_timestamp"] == "2026-09-28T09:19:05+05:30"
    assert emitted["bar_status"] == "IMMUTABLE_CLOSED"
    assert emitted["signal_bar_timestamp"] < emitted["decision_timestamp"]
    assert emitted["received_timestamp"] <= emitted["decision_timestamp"]
    assert len(processor.engine.all_orders) == 1
    assert processor.engine.all_orders[0].status == "PENDING"
    # The 09:18 candle came in the same snapshot as the signal and cannot fill it.
    assert processor.engine.all_orders[0].filled_price is None
    assert Path(processor.ledger_path).is_file()

    repeated = processor.process_snapshot(
        second_rows, now=datetime(2026, 9, 28, 9, 19, 30, tzinfo=IST),
    )
    assert repeated["status"] == "PHASE4G_YF_WAITING_FOR_IMMUTABLE_BARS"
    assert repeated["new_market_bars"] == 0
    assert repeated["duplicate_identical"] == len(second_rows)
    assert repeated["new_signals"] == 0
    assert len(processor.engine.all_orders) == 1
    assert processor.engine.all_orders[0].status == "PENDING"

    third_rows = _session_rows(list(instruments), 5)
    # Keep the next-bar opening at the signal reference so the synthetic test
    # exercises timing/fill ordering without inventing a gap-risk rejection.
    next_entry = next(row for row in third_rows if row["symbol"] == signal_symbol
                      and row["market_timestamp"] == "2026-09-28T09:20:00+05:30")
    next_entry["open"] = 100.3
    processor.process_snapshot(third_rows, now=datetime(2026, 9, 28, 9, 21, 5, tzinfo=IST))
    assert processor.engine.all_orders[0].status == "FILLED"
    ledger_before = inspect_paper_ledger(processor.ledger_path)
    assert ledger_before["status"] == "OK"
    event_count = ledger_before["event_count"]

    restored = Phase4GYFForwardProcessor(project_root=ROOT, output_dir=tmp_path,
                                          instruments=instruments, scanner=_SignalScanner(signal_symbol))
    restored.process_snapshot(third_rows, now=datetime(2026, 9, 28, 9, 22, 5, tzinfo=IST))
    ledger_after = inspect_paper_ledger(processor.ledger_path)
    assert ledger_after["event_count"] == event_count
    assert len(restored.engine.all_orders) == 1


@pytest.mark.parametrize("ranked", [False, True])
def test_monitor_requires_ranked_candidate_and_audits_signal_with_receipt_proof(tmp_path: Path, ranked: bool):
    instruments = load_yahoo_instruments(ROOT, size=486)
    output = tmp_path / ("ranked" if ranked else "unranked")
    processor = Phase4GYFForwardProcessor(
        project_root=ROOT, output_dir=output, instruments=instruments,
        scanner=_SignalScanner("RELIANCE"), require_ranked_candidate=True,
    )
    warm = processor.process_snapshot(
        _session_rows(list(instruments), 1), now=datetime(2026, 9, 28, 9, 17, 5, tzinfo=IST),
    )
    assert warm["status"] == "PHASE4G_YF_WARMED_UP_NO_RETRO_SIGNALS"
    if ranked:
        processor.rankings.append({
            "decision_timestamp": "2026-09-28T09:16:00+05:30", "symbol": "RELIANCE",
            "rank": 1, "score": 0.9,
        })
    result = processor.process_snapshot(
        _session_rows(list(instruments), 3), now=datetime(2026, 9, 28, 9, 19, 5, tzinfo=IST),
    )
    assert result["new_signals"] == int(ranked)
    assert len(processor.engine.all_orders) == int(ranked)
    audit = next(item for item in processor.signal_audit if item.get("strategy") == "test_strategy")
    assert audit["signal"] == ("BUY" if ranked else "REJECTED")
    assert audit["timestamp_proof"] is True
    assert audit["market_timestamp"] < audit["decision_timestamp"]
    assert audit["received_timestamp"] <= audit["decision_timestamp"]
    if ranked:
        assert processor.engine.all_orders[0].status == "PENDING"
        assert audit["paper_order_status"] == "PENDING"
        assert audit["risk_reward"] == 1.5
        assert audit["quantity"] > 0
        assert audit["risk_amount"] <= 1_000  # risk budget is checked by the existing Phase 4C engine
    else:
        assert audit["rejection_reason"] == "NOT_IN_TOP_RANKED_CANDIDATES"
        assert processor.engine.all_orders == []
    persisted = json.loads((output / "reports/signal_audit.json").read_text())
    assert any(item["audit_id"] == audit["audit_id"] for item in persisted)


def test_monitor_shutdown_checkpoint_preserves_state_without_creating_orders(tmp_path: Path):
    instruments = load_yahoo_instruments(ROOT, size=486)
    processor = Phase4GYFForwardProcessor(
        project_root=ROOT, output_dir=tmp_path, instruments=instruments,
        scanner=_SignalScanner("RELIANCE"), require_ranked_candidate=True,
    )
    checkpoint = processor.checkpoint_shutdown()
    assert checkpoint["no_order_generation"] is True
    assert checkpoint["orders"] == checkpoint["ledger_events"] == 0
    assert (tmp_path / "run_state.json").is_file()
    assert not (tmp_path / "paper_ledger.json").exists()


def test_paper_defaults_match_the_requested_risk_and_capital():
    config = paper_config()
    assert config.initial_capital == 100_000
    assert config.risk_per_trade == 0.01
    assert config.max_daily_loss == 0.02
    assert config.max_portfolio_drawdown == 0.10
    assert config.max_positions == 5
    assert config.max_sector_exposure == config.max_symbol_exposure == 0.25
    assert config.realtime_enabled is False and config.live_trading_enabled is False


def test_refresh_interval_never_starts_cycles_faster_than_configured():
    assert phase4g_runner.refresh_sleep_seconds(12.0, 60) == 48.0
    assert phase4g_runner.refresh_sleep_seconds(79.0, 60) == 0.0
    with pytest.raises(ValueError, match="at least 30"):
        phase4g_runner.refresh_sleep_seconds(0.0, 29)


def test_monitor_cli_rejects_refresh_below_30_seconds():
    with pytest.raises(SystemExit):
        phase4g_runner.build_argument_parser().parse_args(["--mode", "monitor", "--refresh-seconds", "29"])


def test_monitor_once_is_a_supported_single_refresh_and_diagnostic_is_read_only(tmp_path, capsys):
    args = phase4g_runner.build_argument_parser().parse_args(["--mode", "monitor", "--once"])
    assert args.mode == "monitor" and args.once is True
    source = tmp_path / "legacy-monitor"
    (source / "canonical").mkdir(parents=True)
    (source / "reports").mkdir()
    (source / "manifest.json").write_text(json.dumps({
        "latest_refresh_accounting": {"new_bars_processed": 17_633},
    }), encoding="utf-8")
    (source / "run_state.json").write_text(json.dumps({
        "warmup_event_ids": ["warm-1"], "processed_event_ids": ["processed-1"],
        "bar_revisions": [{"market_timestamp": "2026-09-28T09:15:00+05:30"}],
    }), encoding="utf-8")
    with (source / "canonical/forward_bars_1m.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["symbol", "market_timestamp", "received_timestamp"])
        writer.writeheader()
        writer.writerow({"symbol": "TEST", "market_timestamp": "2026-09-28T09:15:00+05:30",
                         "received_timestamp": "2026-09-28T09:16:00+05:30"})
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest()
              for path in source.rglob("*") if path.is_file()}
    report = phase4g_runner.phase4g_diagnostic_report(source)
    assert report["status"] == "LEGACY_COUNT_NOT_PROVABLE_FROM_REFRESH_ARTIFACTS"
    assert report["legacy_reported_new_bars"] == 17_633
    assert report["canonical_unique_market_bars"] == 1
    assert report["legacy_bar_revision_records"] == 1
    assert phase4g_runner.main(["--mode", "diagnostic", "--source-dir", str(source)]) == 0
    capsys.readouterr()
    assert before == {path: hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in source.rglob("*") if path.is_file()}


def test_monitor_ctrl_c_finalizes_manifest_without_network_or_broker(monkeypatch, tmp_path: Path):
    fixed_now = datetime(2026, 9, 28, 9, 16, tzinfo=IST)

    def interrupting_acquirer(*args, **kwargs):
        raise KeyboardInterrupt

    status = phase4g_runner.run_collection(
        tmp_path, now_fn=lambda _timezone: fixed_now, sleep_fn=lambda _seconds: None,
        batch_acquirer=interrupting_acquirer, monitor_mode=True,
    )
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    shutdown = json.loads((tmp_path / "reports/shutdown.json").read_text())
    assert status == 130
    assert manifest["status"] == "PHASE4G_YF_MONITOR_INTERRUPTED"
    assert shutdown["checkpoint"]["no_order_generation"] is True
    assert manifest["broker_calls"] == manifest["live_orders"] == 0
    assert shutdown["checkpoint"]["orders"] == 0


def test_phase4f_registration_is_isolated_and_one_session_is_not_research_ready(tmp_path):
    output = tmp_path / "results/phase4g_yf_20260928"
    canonical = output / "canonical/forward_bars_1m.csv"
    canonical.parent.mkdir(parents=True)
    row = _session_rows(["RELIANCE"], 0)[0]
    with canonical.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    result = register_phase4g_dataset(
        project_root=tmp_path / "project", output_dir=output,
        instruments={"RELIANCE": load_yahoo_instruments(ROOT, size=100)["RELIANCE"]},
        volume_evidence={"classification": "INTERVAL_BAR_EMPIRICAL", "volume_kind": "BAR"},
    )
    assert result["status"] == "REGISTERED_AND_PHASE4E_VALIDATED"
    assert result["phase4e_validation_status"] == "PASS"
    assert result["research_ready"] is False
    assert result["research_readiness"] == "NOT_RESEARCH_READY"
    assert Path(result["registry_path"]).is_relative_to(output)
    assert Path(result["normalized_path"]).is_file()


def test_snapshot_returned_after_close_never_enters_forward_signal_or_ledger_path(monkeypatch, tmp_path):
    instruments = load_yahoo_instruments(ROOT, size=486)
    tickers = list(item.yahoo_symbol for item in instruments.values())
    fields = ["Open", "High", "Low", "Close", "Volume"]
    columns = pd.MultiIndex.from_product([tickers, fields])
    index = pd.DatetimeIndex(["2026-09-28 15:29"], tz="Asia/Kolkata")
    one_row = [value for _ in tickers for value in (100.0, 102.0, 99.0, 101.0, 100.0)]
    frame = pd.DataFrame([one_row], index=index, columns=columns)
    times = iter([
        datetime(2026, 9, 28, 15, 29, 40, tzinfo=IST),  # initial preflight
        datetime(2026, 9, 28, 15, 29, 40, tzinfo=IST),  # collector loop start
        datetime(2026, 9, 28, 15, 30, 30, tzinfo=IST),  # actual response arrival
        datetime(2026, 9, 28, 15, 30, 30, tzinfo=IST),  # session close handling
    ])
    monkeypatch.setattr(phase4g_runner, "acquire_batches", lambda *args, **kwargs: [])
    result_code = phase4g_runner.run_collection(
        output=tmp_path, now_fn=lambda *_: next(times), sleep_fn=lambda _: None,
    )
    report = json.loads((tmp_path / "reports/acquisition_report.json").read_text())
    assert report["latest_snapshot"]["used_for_forward_signals"] is False
    assert (tmp_path / "paper_ledger.json").exists() is False
    assert not (tmp_path / "signals/signals.csv").exists() or (tmp_path / "signals/signals.csv").read_text().strip() == ""
    assert result_code == 2  # no first-observed forward bars remained to register


def test_failed_acquisition_snapshot_is_not_marked_as_used_for_forward_signals(tmp_path):
    report = phase4g_runner._write_acquisition_report(
        tmp_path, ["TEST"], failure={"reason_code": "YAHOO_NO_INTRADAY_DATA"},
        retrieved_at=datetime(2026, 9, 28, 13, 59, 10, tzinfo=IST),
        used_for_forward=False,
    )
    assert report["latest_snapshot"]["used_for_forward_signals"] is False
    persisted = json.loads((tmp_path / "reports/acquisition_snapshots.jsonl").read_text())
    assert persisted["used_for_forward_signals"] is False
