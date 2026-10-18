from __future__ import annotations

from datetime import date, timedelta

import pytest

from nse_research.data.intraday_validator import (
    classify_dataset,
    validate_intraday_dataset,
    validate_ohlc,
    validate_provenance,
    validate_session_coverage,
    validate_symbol_identity,
    validate_timezone,
    validate_volume,
)


def bar(timestamp: str, **overrides):
    record = {
        "timestamp": timestamp,
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "series": "EQ",
        "timeframe": "1m",
        "open": "100",
        "high": "102",
        "low": "99",
        "close": "101",
        "volume": "25",
        "volume_kind": "UNKNOWN",
        "status": "CLOSED",
        "source": "SYNTHETIC_TEST_ONLY",
    }
    record.update(overrides)
    return record


def test_schema_and_timezone_validation_normalize_aware_timestamp():
    report = validate_intraday_dataset([bar("2026-09-28T03:45:00Z")])
    assert report.validation_status == "PASS"
    assert report.first_timestamp == "2026-09-28T09:15:00+05:30"
    assert report.source_timezones == ["UTC"]
    assert validate_timezone("2026-09-28T03:45:00Z").isoformat() == "2026-09-28T09:15:00+05:30"
    with pytest.raises(ValueError, match="explicit timezone"):
        validate_timezone("2026-09-28 09:15:00")


def test_missing_schema_column_is_reported(tmp_path):
    import csv

    incomplete = {key: value for key, value in bar("2026-09-28T09:15:00+05:30").items() if key != "series"}
    path = tmp_path / "canonical_missing_series.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(incomplete))
        writer.writeheader()
        writer.writerow(incomplete)
    report = validate_intraday_dataset(path)
    assert report.validation_status == "FAIL"
    assert report.issue_counts["SCHEMA_COLUMN_MISSING"] == 1
    assert report.issue_counts["SERIES_MISMATCH"] == 1


def test_session_boundary_accepts_regular_start_and_rejects_close_boundary():
    valid = validate_intraday_dataset([bar("2026-09-28T09:15:00+05:30")])
    assert valid.validation_status == "PASS"
    invalid = validate_intraday_dataset([bar("2026-09-28T15:30:00+05:30")])
    assert invalid.validation_status == "FAIL"
    assert invalid.issue_counts["TIMESTAMP_OUTSIDE_SESSION"] == 1


def test_ohlc_validation_rejects_bad_relations_and_nonpositive_prices():
    assert validate_ohlc("100", "102", "99", "101") == ()
    assert "OHLC_RELATION_INVALID" in validate_ohlc("100", "100", "99", "101")
    assert "PRICE_NONPOSITIVE" in validate_ohlc("0", "2", "0", "1")
    assert "OHLC_MALFORMED" in validate_ohlc("bad", "2", "1", "1.5")
    report = validate_intraday_dataset([
        bar("2026-09-28T09:15:00+05:30", high="100", close="101"),
        bar("2026-09-28T09:16:00+05:30", low="0"),
    ])
    assert report.issue_counts["OHLC_RELATION_INVALID"] == 1
    assert report.issue_counts["PRICE_NONPOSITIVE"] == 1


def test_volume_validation_rejects_missing_malformed_and_negative_values():
    assert validate_volume("0", "UNKNOWN") == ()
    assert validate_volume("12", "BAD_KIND") == ("VOLUME_KIND_INVALID",)
    assert validate_volume("not-a-number") == ("VOLUME_MALFORMED",)
    assert validate_volume("-1") == ("VOLUME_NEGATIVE",)
    assert validate_volume("") == ("VOLUME_MALFORMED",)


def test_duplicate_and_out_of_order_bars_are_reported():
    report = validate_intraday_dataset([
        bar("2026-09-28T09:16:00+05:30"),
        bar("2026-09-28T09:16:00+05:30"),
        bar("2026-09-28T09:15:00+05:30"),
    ])
    assert report.duplicate_rows == 1
    assert report.out_of_order_rows == 1
    assert report.issue_counts["DUPLICATE_BAR"] == 1
    assert report.issue_counts["OUT_OF_ORDER_BAR"] == 1


def test_session_coverage_measures_gaps_and_missing_sessions_without_filling():
    records = [
        {"timestamp": "2026-09-28T09:15:00+05:30", "symbol": "RELIANCE", "timeframe": "1m"},
        {"timestamp": "2026-09-28T09:17:00+05:30", "symbol": "RELIANCE", "timeframe": "1m"},
    ]
    coverage = validate_session_coverage(
        records,
        timeframe="1m",
        expected_sessions=[date(2026, 9, 28), date(2026, 9, 29)],
        expected_symbols=["RELIANCE"],
        market_open="09:15",
        market_close="09:18",
    )
    assert coverage["missing_sessions"] == ["2026-09-29"]
    assert coverage["missing_symbol_sessions"] == ["RELIANCE:2026-09-29:1m"]
    assert coverage["expected_intervals"] == 6
    assert coverage["missing_intervals"] == 4
    assert coverage["unexpected_intervals"] == 0
    assert coverage["gap_policy"].endswith("no bars or sessions synthesized")


def test_explicit_series_exchange_and_expected_symbol_coverage_are_required():
    assert validate_symbol_identity("RELIANCE", "NSE", "EQ") == ()
    assert "SERIES_MISMATCH" in validate_symbol_identity("RELIANCE", "NSE", "BE")
    report = validate_intraday_dataset(
        [bar("2026-09-28T09:15:00+05:30", series="BE")],
        expected_symbols=["RELIANCE", "TCS"],
    )
    assert report.issue_counts["SERIES_MISMATCH"] == 1
    assert report.missing_symbols == ["TCS"]
    assert report.issue_counts["MISSING_SYMBOLS"] == 1


def _complete_provenance(start: date, end: date, rows: int) -> dict:
    return {
        "dataset": "synthetic-test-only",
        "source_url": "https://example.invalid/synthetic-test-only",
        "publisher": "test fixture",
        "revision": "fixture-v1",
        "downloaded_at": "2026-09-27T00:00:00Z",
        "sha256": "a" * 64,
        "coverage_start": start.isoformat(),
        "coverage_end": end.isoformat(),
        "symbols": 1,
        "rows": rows,
        "timeframe": "1m",
        "timezone": "Asia/Kolkata",
        "exchange": "NSE",
        "series": "EQ",
        "volume_semantics": "BAR_SHARE_VOLUME",
        "license": "CC-BY-4.0",
        "original_provider": "synthetic test fixture",
        "redistribution_terms": "synthetic test fixture only",
        "research_allowed": True,
        "commercial_allowed": False,
        "universe_bias": "ALL_AVAILABLE",
    }


def test_provenance_completeness_and_classification_gates():
    assert "PROVENANCE_MISSING" in validate_provenance(None)
    incomplete = _complete_provenance(date(2026, 1, 1), date(2026, 4, 30), 60)
    del incomplete["original_provider"]
    assert "PROVENANCE_MISSING_ORIGINAL_PROVIDER" in validate_provenance(incomplete)
    assert "PROVENANCE_SHA256_INVALID" in validate_provenance({**incomplete, "original_provider": "test", "sha256": "bad"})

    first = date(2026, 1, 1)
    records = []
    for offset in range(0, 120, 2):
        day = first + timedelta(days=offset)
        records.append(bar(f"{day.isoformat()}T09:15:00+05:30", volume_kind="BAR"))
    report = validate_intraday_dataset(records, expected_symbols=["RELIANCE"])
    provenance = _complete_provenance(date.fromisoformat(report.first_date), date.fromisoformat(report.last_date), len(records))
    assert classify_dataset(
        report, provenance, minimum_coverage_days=90, preferred_coverage_days=730,
        minimum_symbols=1, required_symbols=["RELIANCE"],
    ) == "RESEARCH_READY_WITH_LIMITATIONS"
    assert classify_dataset(report, {**provenance, "research_allowed": False}) == "NOT_RESEARCH_READY"
    assert classify_dataset(None, None, download_status="UNAVAILABLE") == "UNAVAILABLE"
