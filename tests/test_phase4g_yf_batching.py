from __future__ import annotations

import json
import logging
import pickle
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from nse_research.data.yfinance_batching import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_MAX_CONCURRENT_BATCHES,
    BatchAttempt,
    BatchDefinition,
    BatchFetchResult,
    acquire_batches,
    _fetch_batch_job,
    fetch_yfinance_batch,
    partition_frozen_universe,
    validate_batch_configuration,
)
from nse_research.data.yfinance_intraday import YahooInstrument, YFinanceDataError, normalize_yfinance_frame
from nse_research.intraday.model import load_frozen_universe
from nse_research.intraday.phase4g_yf import (
    SESSION_DATE,
    Phase4GYFError,
    build_decision_snapshot,
    rank_candidates,
)


ROOT = Path(__file__).resolve().parents[1]
IST = ZoneInfo("Asia/Kolkata")


def _instruments(count: int) -> dict[str, YahooInstrument]:
    return {
        f"S{index:03d}": YahooInstrument(f"S{index:03d}", f"S{index:03d}.NS", "NSE", "EQ", "INE000A01000")
        for index in range(count)
    }


def _batch_result(batch: BatchDefinition, *, receipt: str, status: str = "SUCCESS") -> BatchFetchResult:
    started = "2026-09-28T09:20:00+05:30"
    attempt = BatchAttempt(1, started, receipt, receipt if status == "SUCCESS" else None,
                          0.1, status, None if status == "SUCCESS" else "TimeoutError",
                          None if status == "SUCCESS" else "mock timeout")
    return BatchFetchResult(batch.batch_id, batch.symbols, batch.yahoo_symbols, status,
                            pd.DataFrame() if status == "SUCCESS" else None, (attempt,), started,
                            receipt, receipt if status == "SUCCESS" else None,
                            None if status == "SUCCESS" else "TimeoutError",
                            None if status == "SUCCESS" else "mock timeout")


def _bar(symbol: str, market: str, received: str) -> dict[str, object]:
    return {
        "timestamp": market,
        "market_timestamp": market,
        "received_timestamp": received,
        "request_started_at": received,
        "response_received_at": received,
        "batch_id": 1,
        "symbol": symbol,
        "timeframe": "1m",
        "status": "CLOSED",
        "bar_status": "IMMUTABLE_CLOSED",
    }


def test_batch_partition_default_486_is_deterministic_and_preserves_order():
    instruments = partition_frozen_universe(load_yahoo_instruments_for_test(), batch_size=DEFAULT_BATCH_SIZE)
    assert DEFAULT_MAX_CONCURRENT_BATCHES == 2
    assert len(instruments) == 10
    assert [len(item.symbols) for item in instruments] == [50] * 9 + [36]
    repeated = partition_frozen_universe(load_yahoo_instruments_for_test(), batch_size=50)
    assert instruments == repeated
    flattened = [symbol for batch in instruments for symbol in batch.symbols]
    assert len(flattened) == 486
    assert len(set(flattened)) == 486
    assert flattened == list(load_frozen_universe(ROOT, 486))


def load_yahoo_instruments_for_test():
    from nse_research.data.yfinance_intraday import load_yahoo_instruments

    return load_yahoo_instruments(ROOT, size=486)


@pytest.mark.parametrize("batch_size,concurrency", [(0, 1), (-1, 1), (1, 0), (1, -1)])
def test_invalid_batch_configuration_fails_before_worker_creation(batch_size, concurrency):
    with pytest.raises(YFinanceDataError):
        validate_batch_configuration(batch_size, concurrency)


def test_production_process_worker_and_batch_payload_are_spawn_picklable():
    batch = partition_frozen_universe(_instruments(2), batch_size=2)[0]
    assert pickle.loads(pickle.dumps(batch)) == batch
    assert pickle.loads(pickle.dumps(_fetch_batch_job)) is _fetch_batch_job


def test_bounded_concurrency_failure_and_out_of_order_completion(tmp_path):
    batches = partition_frozen_universe(_instruments(4), batch_size=1)
    lock = threading.Lock()
    active = 0
    maximum_active = 0
    completed = []
    ledger_path = tmp_path / "paper_ledger.json"

    def worker(batch, **_kwargs):
        nonlocal active, maximum_active
        with lock:
            active += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0.03 if batch.batch_id == 1 else 0.005)
        # Batch 2 finishes first but declares a later response receipt. The
        # main result order must follow the recorded receipt, not completion.
        receipt = "2026-09-28T09:20:03+05:30" if batch.batch_id == 1 else "2026-09-28T09:20:10+05:30"
        status = "FAILED" if batch.batch_id == 3 else "SUCCESS"
        result = _batch_result(batch, receipt=receipt, status=status)
        with lock:
            completed.append(batch.batch_id)
            active -= 1
        return result

    results = acquire_batches(
        batches, start="2026-09-28", end="2026-09-29", max_concurrent_batches=2,
        worker=worker, executor_type=ThreadPoolExecutor,
    )
    assert maximum_active <= 2 and maximum_active == 2
    assert completed[0] != 1  # confirms a later-submitted result finished before batch 1
    assert [item.batch_id for item in results[:2]] == [1, 2]  # receipt order is authoritative
    assert results[2].status == "FAILED" and results[2].frame is None
    assert not ledger_path.exists()  # acquisition workers have no ledger path/write API


def test_batch_failure_does_not_fabricate_bars_and_manifest_is_paper_only():
    batch = partition_frozen_universe(_instruments(1), batch_size=1)[0]
    calls = []

    def failing_downloader(**_kwargs):
        calls.append(True)
        raise TimeoutError("mock timeout")

    instants = iter([
        datetime(2026, 9, 28, 9, 20, tzinfo=IST), datetime(2026, 9, 28, 9, 20, 1, tzinfo=IST),
        datetime(2026, 9, 28, 9, 20, 2, tzinfo=IST), datetime(2026, 9, 28, 9, 20, 3, tzinfo=IST),
        datetime(2026, 9, 28, 9, 20, 4, tzinfo=IST), datetime(2026, 9, 28, 9, 20, 5, tzinfo=IST),
    ])
    sleeps = []
    result = fetch_yfinance_batch(
        batch, start="2026-09-28", end="2026-09-29", max_retries=2,
        downloader=failing_downloader, now_fn=lambda *_: next(instants),
        sleep_fn=sleeps.append,
    )
    assert result.status == "FAILED"
    assert result.frame is None
    assert len(calls) == 3 and len(result.attempts) == 3
    assert result.retry_count == 2
    assert sleeps == [0.25, 0.5]
    manifest = result.to_manifest(bars_returned=0)
    assert manifest["bars_returned"] == 0
    assert manifest["error_type"]
    assert manifest["batch_symbol_count"] == 1


def test_yahoo_possibly_delisted_warning_is_retained_as_provider_evidence():
    batch = BatchDefinition(1, ("BLUEDART",), ("BLUEDART.NS",))

    def unavailable(**_kwargs):
        logging.getLogger("yfinance").warning(
            "$BLUEDART.NS: possibly delisted; no price data found"
        )
        return pd.DataFrame()

    fixed = datetime(2026, 9, 28, 12, 40, tzinfo=IST)
    result = fetch_yfinance_batch(
        batch, start="2026-09-28", end="2026-09-29", max_retries=0,
        downloader=unavailable, now_fn=lambda *_: fixed,
    )
    assert result.status == "FAILED"
    assert result.frame is None
    assert result.provider_warnings == (
        "$BLUEDART.NS: possibly delisted; no price data found",
    )
    assert result.to_manifest(bars_returned=0)["bars_returned"] == 0


def test_successful_batch_carries_attempt_receipt_time_and_retries_are_bounded():
    batch = partition_frozen_universe(_instruments(1), batch_size=1)[0]
    calls = []

    def downloader(**_kwargs):
        calls.append(True)
        if len(calls) < 2:
            raise TimeoutError("first attempt timeout")
        index = pd.DatetimeIndex(["2026-09-28 09:15"], tz="Asia/Kolkata")
        return pd.DataFrame({"Open": [100], "High": [102], "Low": [99], "Close": [101], "Volume": [50]}, index=index)

    instants = iter([
        datetime(2026, 9, 28, 9, 20, tzinfo=IST), datetime(2026, 9, 28, 9, 20, 1, tzinfo=IST),
        datetime(2026, 9, 28, 9, 20, 2, tzinfo=IST), datetime(2026, 9, 28, 9, 20, 3, tzinfo=IST),
    ])
    result = fetch_yfinance_batch(
        batch, start="2026-09-28", end="2026-09-29", max_retries=2,
        downloader=downloader, now_fn=lambda *_: next(instants), sleep_fn=lambda _: None,
    )
    assert result.status == "SUCCESS"
    assert result.retry_count == 1
    assert result.response_received_at == "2026-09-28T09:20:03+05:30"
    assert result.attempts[0].response_received_at is None
    assert result.attempts[1].response_received_at == result.response_received_at


def test_normalized_rows_keep_market_timestamp_and_response_receipt_timestamp():
    instrument = YahooInstrument("TEST", "TEST.NS", "NSE", "EQ", "INE000A01000")
    frame = pd.DataFrame(
        {"Open": [100], "High": [102], "Low": [99], "Close": [101], "Volume": [50]},
        index=pd.DatetimeIndex(["2026-09-28 09:20"], tz="Asia/Kolkata"),
    )
    receipt = datetime(2026, 9, 28, 9, 24, 30, tzinfo=IST)
    result = normalize_yfinance_frame(
        frame, {"TEST": instrument}, interval="1m", now=receipt,
        request_started_at=datetime(2026, 9, 28, 9, 24, tzinfo=IST),
        response_received_at=receipt, batch_id=2, session_date=SESSION_DATE,
    )
    row = result.rows[0]
    assert row["market_timestamp"] == "2026-09-28T09:20:00+05:30"
    assert row["received_timestamp"] == row["response_received_at"] == receipt.isoformat()
    assert row["request_started_at"] == "2026-09-28T09:24:00+05:30"
    assert row["batch_id"] == 2


@pytest.mark.parametrize(
    ("market", "received", "decision", "available", "late", "market_excluded"),
    [
        ("09:20", "09:21", "09:21:30", 1, 0, 0),  # A: both clocks permit it
        ("09:20", "09:22", "09:21:30", 0, 1, 0),  # B: response arrived late
        ("09:22", "09:22:10", "09:22:30", 0, 0, 1),  # C: bar had not closed
        ("09:15", "10:00", "09:30", 0, 1, 0),  # D: late historical response
    ],
)
def test_decision_snapshot_requires_receipt_and_closed_market_time(
    market, received, decision, available, late, market_excluded
):
    row = _bar(
        "RELIANCE",
        f"2026-09-28T{market}:00+05:30",
        f"2026-09-28T{received}:00+05:30",
    )
    snapshot = build_decision_snapshot(
        [row], decision=datetime.fromisoformat(f"2026-09-28T{decision}:00+05:30"),
        expected_symbols=["RELIANCE"],
    )
    assert snapshot["symbols_available"] == available
    assert snapshot["late_bars_excluded"] == late
    assert snapshot["bars_excluded_by_market_time"] == market_excluded
    assert snapshot["symbols_missing_due_to_latency"] == late
    if late:
        assert snapshot["late_data_excluded"] is True
        assert snapshot["late_data_exclusions"][0]["received_timestamp"] == row["received_timestamp"]


def test_late_data_can_only_enter_a_later_decision_and_never_backfills_prior_ranking():
    row = _bar("RELIANCE", "2026-09-28T09:20:00+05:30", "2026-09-28T09:22:00+05:30")
    earlier = build_decision_snapshot(
        [row], decision=datetime(2026, 9, 28, 9, 21, 30, tzinfo=IST), expected_symbols=["RELIANCE"]
    )
    later = build_decision_snapshot(
        [row], decision=datetime(2026, 9, 28, 10, 0, tzinfo=IST), expected_symbols=["RELIANCE"]
    )
    assert earlier["symbols_available"] == 0 and earlier["late_data_excluded"] is True
    assert later["symbols_available"] == 1 and later["late_data_excluded"] is False

    late_rows = []
    first_market = datetime(2026, 9, 28, 9, 15, tzinfo=IST)
    for index in range(21):
        market = first_market + timedelta(minutes=index)
        row = _bar("RELIANCE", market.isoformat(), "2026-09-28T10:00:00+05:30")
        price = 100.0 + index * 0.1
        row.update({"open": price, "high": price + 0.5, "low": price - 0.5,
                    "close": price, "volume": 100 + index,
                    "volume_kind": "BAR"})
        late_rows.append(row)
    assert rank_candidates(late_rows, as_of=datetime(2026, 9, 28, 9, 36, tzinfo=IST)) == []


def test_ranking_fails_closed_when_response_receipt_time_is_missing():
    row = _bar("RELIANCE", "2026-09-28T09:20:00+05:30", "2026-09-28T09:21:00+05:30")
    row.pop("received_timestamp")
    with pytest.raises(Phase4GYFError, match="RANKING_RECEIPT_TIME_UNKNOWN"):
        rank_candidates([row], as_of=datetime(2026, 9, 28, 9, 22, tzinfo=IST))


def test_batch_cli_defaults_overrides_and_invalid_values():
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("phase4g_batching_cli_test", ROOT / "scripts/run_phase4g_yf_forward.py")
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    parser = module.build_argument_parser()
    defaults = parser.parse_args([])
    assert defaults.batch_size == 50 and defaults.max_concurrent_batches == 2
    override = parser.parse_args(["--batch-size", "100", "--max-concurrent-batches", "1"])
    assert override.batch_size == 100 and override.max_concurrent_batches == 1
    assert module.default_output_root(50, 2).name == "phase4g_yf_20260928_batched"
    assert module.default_output_root(100, 1).name == "phase4g_yf_20260928_batch100_c1"
    for arguments in (["--batch-size", "0"], ["--batch-size", "-1"],
                      ["--max-concurrent-batches", "0"], ["--max-concurrent-batches", "-2"]):
        with pytest.raises(SystemExit):
            parser.parse_args(arguments)


def test_session_batch_manifest_has_no_broker_live_path():
    batch = partition_frozen_universe(_instruments(1), batch_size=1)[0]
    result = _batch_result(batch, receipt="2026-09-28T09:21:00+05:30")
    manifest = result.to_manifest(bars_returned=0)
    assert manifest["bars_returned"] == 0
    assert manifest["status"] == "SUCCESS"
    with pytest.raises(FrozenInstanceError):
        batch.batch_id = 9


def test_preflight_batch_manifest_records_plan_without_fake_execution(tmp_path):
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("phase4g_batch_manifest_test", ROOT / "scripts/run_phase4g_yf_forward.py")
    module = module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    batches = partition_frozen_universe(_instruments(6), batch_size=5)
    manifest = module._write_batch_manifest(
        tmp_path, batches=batches, batch_size=5, max_concurrent_batches=2,
    )
    assert manifest["frozen_universe_size"] == 6
    assert manifest["session_universe_size"] == 6
    assert manifest["number_of_batches"] == 2
    assert manifest["planned_batches"][0]["batch_symbol_count"] == 5
    assert manifest["planned_batches"][1]["batch_symbol_count"] == 1
    assert manifest["executions"] == []
    assert manifest["execution_status"] == "NOT_STARTED"
    assert manifest["live_trading_enabled"] is False
    assert manifest["broker"] == "NONE"
