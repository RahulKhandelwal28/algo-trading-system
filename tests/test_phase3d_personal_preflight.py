from __future__ import annotations

import csv
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

from nse_research.config import AppConfig
from nse_research.forward import preflight
from nse_research.forward.preflight import run_preflight


ROOT = Path(__file__).resolve().parents[1]
MARKET_DATE = date(2026, 9, 25)


def _dates(count: int = 225) -> list[date]:
    out = []
    current = MARKET_DATE
    while len(out) < count:
        if current.weekday() < 5:
            out.append(current)
        current -= timedelta(days=1)
    return list(reversed(out))


def _write_indexes(folder: Path, days: list[date]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    for name, columns, base in (
        ("index_NSEI.csv", ["Date", "Open", "High", "Low", "Close", "Shares Traded", "Turnover"], 20000.0),
        ("index_INDIAVIX.csv", ["Date", "Open", "High", "Low", "Close", "Prev. Close", "Change", "% Change"], 15.0),
    ):
        with (folder / name).open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(columns)
            for i, day in enumerate(days):
                close = base + i * (0.2 if name == "index_NSEI.csv" else 0.002)
                if name == "index_NSEI.csv":
                    writer.writerow([day.isoformat(), close - 0.1, close + 1, close - 1, close, 100000, 2500])
                else:
                    previous = close - 0.02
                    writer.writerow([day.isoformat(), previous, close + 0.1, close - 0.1, close, previous, close - previous, 0.01])


def _write_equity(folder: Path, days: list[date], *, skip_target=False, duplicate=False, invalid_ohlc=False,
                  extra_official_date: date | None = None) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / "TEST.NS_1d.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for i, day in enumerate(days):
            if skip_target and day == MARKET_DATE:
                continue
            close = 100 + i * 0.05
            high, low = close + 1, close - 1
            if invalid_ohlc and i == 10:
                high, low = close - 2, close + 2
            row = [day.isoformat(), close - 0.1, high, low, close, 1000]
            writer.writerow(row)
            if duplicate and day == MARKET_DATE:
                writer.writerow(row)
        if extra_official_date:
            writer.writerow([extra_official_date.isoformat(), 100.0, 101.0, 99.0, 100.5, 1000])


@pytest.fixture
def personal_inputs(tmp_path, monkeypatch):
    config = AppConfig.load(ROOT / "config.yaml")
    config.forward_paper.provider = "personal_local_eod"
    config.forward_paper.baseline_name = "TEST_PERSONAL_BASELINE"
    config.forward_paper.identity_file = str(tmp_path / "universe.csv")
    config.universe.symbols_file = str(tmp_path / "universe.csv")
    config.universe.min_history_rows = 220
    with (tmp_path / "universe.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Symbol", "Series"])
        writer.writerow(["TEST", "EQ"])
    days = _dates()
    history = tmp_path / "cache"
    _write_equity(history, days)
    indexes = tmp_path / "indices"
    _write_indexes(indexes, days)
    baseline = tmp_path / "baseline.json"
    baseline.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(preflight, "validate_baseline_manifest", lambda *args, **kwargs: {})
    return {
        "config": config, "root": ROOT, "market_date": MARKET_DATE, "symbols": ["TEST.NS"],
        "dataset_path": history, "index_data_dir": indexes, "daily_data_dir": tmp_path / "normalized",
        "identity_file": tmp_path / "universe.csv", "database": tmp_path / "paper.sqlite",
        "baseline_manifest": baseline, "paper_mode_enabled": True, "live_mode_disabled": True,
        "provider_name": "personal_local_eod", "output_root": tmp_path / "reports", "dates": days,
    }


def _run(values, **updates):
    inputs = {key: value for key, value in values.items() if key != "dates"}
    inputs.update(updates)
    return run_preflight(**inputs)


def test_personal_preflight_passes_for_exact_eq_universe_and_is_read_only(personal_inputs):
    result = _run(personal_inputs)
    assert result.status == "PHASE3D_READY", result.format()
    assert all(row["passed"] for row in result.checks.values())
    assert result.dataset_summary["required_symbols_present_on_market_date"] == 1
    assert not personal_inputs["database"].exists()
    assert not personal_inputs["output_root"].exists()


def test_personal_preflight_excludes_and_reports_equity_rows_outside_official_nifty_sessions(personal_inputs):
    extra_day = date(2026, 8, 15)
    _write_equity(personal_inputs["dataset_path"], personal_inputs["dates"], extra_official_date=extra_day)
    result = _run(personal_inputs)
    assert result.status == "PHASE3D_READY", result.format()
    assert result.dataset_summary["excluded_non_nifty_session_rows"] == 1
    assert result.dataset_summary["excluded_non_nifty_session_dates"] == [extra_day.isoformat()]
    assert "off_calendar_equity_rows_excluded" in result.checks
    assert result.checks["off_calendar_equity_rows_excluded"]["passed"]


def test_personal_preflight_missing_dataset_blocks_with_exact_status(personal_inputs):
    personal_inputs["dataset_path"].rename(personal_inputs["dataset_path"].with_name("cache-missing"))
    personal_inputs["dataset_path"].mkdir()
    result = _run(personal_inputs)
    assert result.status == "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA"
    assert not result.checks["data_passes_validation"]["passed"]


def test_personal_preflight_requires_explicit_eq_series(personal_inputs):
    personal_inputs["identity_file"].write_text("Symbol,Series\nTEST,BE\n", encoding="utf-8")
    result = _run(personal_inputs)
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert not result.checks["explicit_eq_identity_file"]["passed"]


def test_personal_preflight_rejects_duplicate_records(personal_inputs):
    _write_equity(personal_inputs["dataset_path"], personal_inputs["dates"], duplicate=True)
    result = _run(personal_inputs)
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert result.dataset_summary["duplicate_rows"] > 0
    assert not result.checks["data_passes_validation"]["passed"]


def test_personal_preflight_rejects_malformed_ohlc(personal_inputs):
    _write_equity(personal_inputs["dataset_path"], personal_inputs["dates"], invalid_ohlc=True)
    result = _run(personal_inputs)
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert result.dataset_summary["invalid_rows"] > 0


def test_personal_preflight_requires_every_frozen_symbol(personal_inputs):
    result = _run(personal_inputs, symbols=["TEST.NS", "MISSING.NS"])
    assert result.status == "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA"
    assert "MISSING" in result.dataset_summary["missing_symbols"]


def test_personal_preflight_requires_exact_market_date(personal_inputs):
    _write_equity(personal_inputs["dataset_path"], personal_inputs["dates"], skip_target=True)
    result = _run(personal_inputs)
    assert result.status == "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA"
    assert "TEST" in result.dataset_summary["missing_symbols"] or not result.checks["required_dates_available"]["passed"]


def test_personal_preflight_uses_existing_realized_volatility_fallback_for_stale_vix(personal_inputs):
    path = personal_inputs["index_data_dir"] / "index_INDIAVIX.csv"
    frame = list(csv.reader(path.open(newline="", encoding="utf-8")))
    frame = [row for row in frame if row[0] != MARKET_DATE.isoformat()]
    with path.open("w", newline="", encoding="utf-8") as stream:
        csv.writer(stream).writerows(frame)
    result = _run(personal_inputs)
    assert result.status == "PHASE3D_READY", result.format()
    assert result.dataset_summary["india_vix_fallback"] is True
    assert result.checks["india_vix_exact_date_or_nifty_fallback"]["passed"]


def test_personal_preflight_does_not_fallback_from_malformed_vix(personal_inputs):
    path = personal_inputs["index_data_dir"] / "index_INDIAVIX.csv"
    frame = list(csv.reader(path.open(newline="", encoding="utf-8")))
    frame[-1][4] = "not-a-number"
    with path.open("w", newline="", encoding="utf-8") as stream:
        csv.writer(stream).writerows(frame)
    result = _run(personal_inputs)
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert result.dataset_summary["india_vix_fallback"] is False
    assert not result.checks["data_passes_validation"]["passed"]


def test_personal_failed_preflight_never_mutates_existing_ledger(personal_inputs):
    with sqlite3.connect(personal_inputs["database"]) as connection:
        connection.execute("CREATE TABLE sentinel (value TEXT)")
        connection.execute("INSERT INTO sentinel VALUES ('unchanged')")
    before = personal_inputs["database"].read_bytes()
    result = _run(personal_inputs, paper_mode_enabled=False)
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert not result.checks["paper_mode_enabled"]["passed"]
    assert personal_inputs["database"].read_bytes() == before
    assert not personal_inputs["output_root"].exists()
