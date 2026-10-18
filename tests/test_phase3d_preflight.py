from __future__ import annotations

import csv
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from nse_research.config import AppConfig
from nse_research.forward import preflight
from nse_research.forward.preflight import run_preflight


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MARKET_DATE = date(2026, 9, 25)


def _session_dates(count: int = 225) -> list[date]:
    output = []
    current = MARKET_DATE
    while len(output) < count:
        if current.weekday() < 5:
            output.append(current)
        current -= timedelta(days=1)
    return list(reversed(output))


def _write_indexes(folder: Path, dates: list[date]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / "index_NSEI.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Shares Traded", "Turnover"])
        for index, day in enumerate(dates):
            close = 1000 + index * 0.2
            writer.writerow([day.isoformat(), close - 0.1, close + 1, close - 1, close, 100000, 2500])
    with (folder / "index_INDIAVIX.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Date", "Open", "High", "Low", "Close", "Prev. Close", "Change", "% Change"])
        for index, day in enumerate(dates):
            close = 15 + index * 0.002
            previous = close - 0.002 if index else close
            writer.writerow([day.isoformat(), previous, close + 0.1, close - 0.1, close, previous, close - previous, 0.01])


def _write_equities(path: Path, dates: list[date], *, skip_target: bool = False,
                    bad_series: bool = False, bad_ohlc_at: int | None = None,
                    duplicate: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow([
            "Symbol", "Series", "Date", "Open Price", "High Price", "Low Price",
            "Close Price", "Total Traded Quantity",
        ])
        rows = []
        for index, day in enumerate(dates):
            if skip_target and day == MARKET_DATE:
                continue
            close = 100 + index * 0.03
            row = ["TEST", "EQ", day.strftime("%d-%b-%Y"), close - 0.1, close + 1, close - 1, close, 1000]
            if bad_series and day == MARKET_DATE:
                row[1] = "BE"
            if bad_ohlc_at == index:
                row[4], row[5] = close - 2, close + 2
            rows.append(row)
        writer.writerows(rows)
        if duplicate and rows:
            writer.writerow(rows[0])


@pytest.fixture
def preflight_inputs(tmp_path, monkeypatch):
    config = AppConfig.load(PROJECT_ROOT / "config.yaml")
    dates = _session_dates()
    index_dir = tmp_path / "nse_exports"
    _write_indexes(index_dir, dates)
    dataset = tmp_path / "licensed_nse_cm_eod" / "nse_cm_eod_dataset.csv"
    _write_equities(dataset, dates)
    baseline = tmp_path / "PHASE3C_BASELINE.json"
    baseline.write_text("{}\n", encoding="utf-8")
    # Baseline integrity is covered separately; this fixture focuses on the
    # read-only data and runtime readiness checks.
    monkeypatch.setattr(preflight, "validate_baseline_manifest", lambda *args, **kwargs: {})
    return {
        "config": config,
        "root": PROJECT_ROOT,
        "market_date": MARKET_DATE,
        "symbols": ["TEST.NS"],
        "dataset_path": dataset,
        "index_data_dir": index_dir,
        "database": tmp_path / "forward_paper.sqlite",
        "baseline_manifest": baseline,
        "paper_mode_enabled": True,
        "live_mode_disabled": True,
        "provider_name": "licensed_nse_cm_eod",
        "output_root": tmp_path / "reports",
        "dates": dates,
    }


def _run(inputs, **updates):
    values = {key: value for key, value in inputs.items() if key != "dates"}
    values.update(updates)
    return run_preflight(**values)


def test_preflight_blocks_when_licensed_dataset_is_missing(preflight_inputs):
    result = _run(preflight_inputs, dataset_path=preflight_inputs["dataset_path"].with_name("missing.csv"))
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert not result.checks["licensed_equity_dataset_exists"]["passed"]
    assert not result.checks["data_passes_validation"]["passed"]
    assert "licensed equity dataset is missing" in result.checks["required_dates_available"]["detail"]


def test_preflight_requires_explicit_eq_series(preflight_inputs):
    _write_equities(preflight_inputs["dataset_path"], preflight_inputs["dates"], bad_series=True)
    result = _run(preflight_inputs)
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert result.dataset_summary["non_eq_rows"] == 1
    assert not result.checks["data_passes_validation"]["passed"]


def test_preflight_rejects_duplicate_symbol_date_records(preflight_inputs):
    _write_equities(preflight_inputs["dataset_path"], preflight_inputs["dates"], duplicate=True)
    result = _run(preflight_inputs)
    assert result.dataset_summary["duplicate_rows"] == 1
    assert result.dataset_summary["invalid_rows"] >= 1
    assert not result.checks["data_passes_validation"]["passed"]


def test_preflight_rejects_malformed_ohlc(preflight_inputs):
    _write_equities(preflight_inputs["dataset_path"], preflight_inputs["dates"], bad_ohlc_at=12)
    result = _run(preflight_inputs)
    assert result.dataset_summary["invalid_ohlc_rows"] == 1
    assert not result.checks["data_passes_validation"]["passed"]


def test_preflight_reports_missing_required_symbols(preflight_inputs):
    result = _run(preflight_inputs, symbols=["TEST.NS", "MISSING.NS"])
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert result.dataset_summary["missing_symbols"] == ["MISSING"]
    assert not result.checks["required_symbols_available"]["passed"]


def test_preflight_blocks_when_required_trading_date_is_missing(preflight_inputs):
    _write_equities(preflight_inputs["dataset_path"], preflight_inputs["dates"], skip_target=True)
    result = _run(preflight_inputs)
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert not result.checks["required_dates_available"]["passed"]
    assert not result.checks["required_symbols_available"]["passed"]


def test_successful_preflight_is_read_only_and_does_not_create_ledger(preflight_inputs):
    result = _run(preflight_inputs)
    assert result.status == "PHASE3D_READY", result.format()
    assert all(check["passed"] for check in result.checks.values())
    assert not preflight_inputs["database"].exists()
    assert not preflight_inputs["output_root"].exists()


def test_failed_preflight_lists_runtime_and_duplicate_blockers(preflight_inputs):
    connection = sqlite3.connect(preflight_inputs["database"])
    connection.execute("CREATE TABLE daily_journal (trading_date TEXT)")
    connection.execute("INSERT INTO daily_journal VALUES (?)", (MARKET_DATE.isoformat(),))
    connection.commit()
    connection.close()
    result = _run(
        preflight_inputs,
        paper_mode_enabled=False,
        live_mode_disabled=False,
        provider_name="local_eod_csv",
        baseline_manifest=preflight_inputs["baseline_manifest"].with_name("absent.json"),
    )
    assert result.status == "PHASE3D_FORWARD_BLOCKED"
    assert not result.checks["paper_mode_enabled"]["passed"]
    assert not result.checks["live_mode_disabled"]["passed"]
    assert not result.checks["broker_provider_not_configured"]["passed"]
    assert not result.checks["baseline_manifest_exists_and_matches"]["passed"]
    assert not result.checks["no_duplicate_forward_run"]["passed"]


def test_preflight_does_not_mutate_existing_ledger(preflight_inputs):
    connection = sqlite3.connect(preflight_inputs["database"])
    connection.execute("CREATE TABLE daily_journal (trading_date TEXT)")
    connection.execute("CREATE TABLE positions (symbol TEXT, status TEXT)")
    connection.execute("INSERT INTO daily_journal VALUES ('2026-09-24')")
    connection.execute("INSERT INTO positions VALUES ('TEST.NS', 'CLOSED')")
    connection.commit()
    connection.close()
    before = preflight_inputs["database"].read_bytes()

    result = _run(preflight_inputs)

    after = preflight_inputs["database"].read_bytes()
    assert result.status == "PHASE3D_READY", result.format()
    assert before == after
