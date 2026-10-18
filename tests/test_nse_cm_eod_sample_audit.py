from __future__ import annotations

import csv
import gzip
import hashlib
import json
import shutil
import zipfile
from datetime import date
from pathlib import Path

from nse_research.data.nse_cm_eod_sample_audit import (
    OPTIONAL_FIELDS,
    REQUIRED_FIELDS,
    TARGET_COLUMNS,
    audit_sample,
    initial_sample_validation,
)

PROJECT = Path(__file__).parents[1]
AUTH_TEMPLATE = PROJECT / "data" / "licensed_nse_cm_eod" / "manifest" / "source_authorization.json"
MAP_TEMPLATE = PROJECT / "data" / "licensed_nse_cm_eod" / "manifest" / "source_column_mapping.json"
HEADER_BY_FIELD = {
    "date": "TRADE_DATE", "symbol": "TICKER", "series": "SERIES", "isin": "ISIN",
    "name": "NAME", "open": "OPEN", "high": "HIGH", "low": "LOW", "close": "CLOSE",
    "prev_close": "PREV_CLOSE", "volume": "VOLUME", "turnover": "TURNOVER", "trades": "TRADES",
}


def _authorization(*, confirmed: bool = True) -> dict[str, object]:
    return {
        "provider": "NSE", "product": "CM EOD Historical Data",
        "authorization_status": "CONFIRMED" if confirmed else "NOT_PROVIDED",
        "authorization_reference": "SYNTHETIC-TEST-ONLY" if confirmed else None,
        "authorization_date": "2026-09-26" if confirmed else None,
        "acquisition_date": "2026-09-26" if confirmed else None,
        "permitted_uses": [
            "quantitative_research", "historical_backtesting", "walk_forward_research",
            "paper_trading_simulation", "internal_analytical_use", "local_dataset_retention",
        ] if confirmed else [],
        "local_storage_permitted": confirmed,
        "backtesting_permitted": confirmed,
        "paper_trading_simulation_permitted": confirmed,
        "commercial_use_permitted": None,
        "terms_file": "synthetic fixture only" if confirmed else None,
    }


def _mapping(source_format: str = "csv") -> dict[str, object]:
    return {
        "mapping_status": "VERIFIED_AGAINST_SYNTHETIC_TEST_FIXTURE",
        "source_format": source_format,
        "source_version": "synthetic-test-format",
        "date_formats": ["%Y-%m-%d"],
        "decimal_separator": ".",
        "thousands_separator": ",",
        "source_columns": dict(HEADER_BY_FIELD),
        "optional_fields_confirmed_absent": [],
    }


def _row(**overrides: str) -> dict[str, str]:
    values = {
        "date": "2020-01-02", "symbol": "ALPHA", "series": "EQ", "isin": "INE000A01001",
        "name": "Alpha Ltd", "open": "100.00", "high": "110.00", "low": "90.00",
        "close": "105.00", "prev_close": "99.00", "volume": "1,000", "turnover": "105000.00",
        "trades": "75",
    }
    values.update(overrides)
    return {HEADER_BY_FIELD[field]: value for field, value in values.items()}


def _write_csv(path: Path, rows: list[dict[str, str]], headers: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if headers is None:
        headers = [HEADER_BY_FIELD[field] for field in TARGET_COLUMNS]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _run(tmp_path: Path, source: Path | None, *, confirmed: bool = True, mapping: dict | None = None) -> dict:
    tmp_path.mkdir(parents=True, exist_ok=True)
    auth_path = tmp_path / "source_authorization.json"
    map_path = tmp_path / "source_column_mapping.json"
    auth_path.write_text(json.dumps(_authorization(confirmed=confirmed)), encoding="utf-8")
    map_path.write_text(json.dumps(mapping or _mapping()), encoding="utf-8")
    return audit_sample(
        source_path=source,
        mapping_path=map_path,
        authorization_path=auth_path,
        audit_dir=tmp_path / "audit",
        normalized_path=tmp_path / "normalized" / "nse_cm_eod_sample.csv",
        provenance_path=tmp_path / "manifest" / "sample_provenance.json",
        nifty_path=tmp_path / "index_NSEI.csv",
        vix_path=tmp_path / "index_INDIAVIX.csv",
        project_version="0.1.0-test",
    )


def _write_reference(path: Path, days: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Date"])
        writer.writerows([[day] for day in days])


def test_no_sample_is_not_assessable_and_does_not_create_normalized_data(tmp_path) -> None:
    result = _run(tmp_path, None, confirmed=False)
    assert result["authorization"] == "FAIL"
    assert result["acceptance_status"] == "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED"
    assert result["overall"] == "NOT_ASSESSABLE"
    assert result["sample_present"] is False
    assert not (tmp_path / "normalized" / "nse_cm_eod_sample.csv").exists()
    assert (tmp_path / "audit" / "sample_validation.json").is_file()
    assert (tmp_path / "audit" / "invalid_ohlcv.csv").read_text().startswith("source_file,")


def test_checked_in_authorization_template_is_fail_closed() -> None:
    template = json.loads(AUTH_TEMPLATE.read_text(encoding="utf-8"))
    result = initial_sample_validation(template)
    assert result["authorization_status"] == "NOT_PROVIDED"
    assert result["authorization"] == "FAIL"
    assert result["acceptance_status"] == "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED"
    assert template["authorization_reference"] is None
    assert template["permitted_uses"] == []
    assert template["backtesting_permitted"] is None


def test_explicit_eq_filtering_and_exact_column_mapping(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row(), _row(date="2020-01-03", symbol="BETA", series="BE")])
    _write_reference(tmp_path / "index_NSEI.csv", ["2020-01-02", "2020-01-03"])
    result = _run(tmp_path, source)
    assert result["schema"] == "PASS"
    assert result["eq_series"] == "PASS"
    assert result["EQ_rows"] == 1
    assert result["non_EQ_rows"] == 1
    assert result["observed_series_values"] == ["BE", "EQ"]
    assert result["acceptance_status"] == "AUTHORIZED_RESEARCH_READY_SAMPLE"
    output = list(csv.DictReader((tmp_path / "normalized" / "nse_cm_eod_sample.csv").open(encoding="utf-8")))
    assert len(output) == 1
    assert output[0]["series"] == "EQ"
    assert tuple(output[0]) == TARGET_COLUMNS


def test_required_column_validation_fails_without_alias_guessing(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    row = _row()
    del row["SERIES"]
    headers = [header for header in row]
    _write_csv(source, [row], headers=headers)
    result = _run(tmp_path, source)
    assert result["schema"] == "FAIL"
    assert "missing explicitly mapped headers" in " ".join(result["errors"])
    assert not (tmp_path / "normalized" / "nse_cm_eod_sample.csv").exists()


def test_series_is_never_fabricated(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row()])
    mapping = _mapping()
    mapping["source_columns"]["series"] = None
    mapping["optional_fields_confirmed_absent"] = list(OPTIONAL_FIELDS)
    result = _run(tmp_path, source, mapping=mapping)
    assert result["schema"] == "FAIL"
    assert not (tmp_path / "normalized" / "nse_cm_eod_sample.csv").exists()


def test_malformed_dates_are_recorded_and_fail_date_gate(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row(date="31/31/2020", open="120.00")])
    result = _run(tmp_path, source)
    assert result["dates"] == "FAIL"
    assert result["malformed_date_rows"] == 1
    assert result["EQ_rows"] == 1
    diagnostic = list(csv.DictReader((tmp_path / "audit" / "malformed_dates.csv").open(encoding="utf-8")))
    assert diagnostic[0]["date_raw"] == "31/31/2020"
    invalid = list(csv.DictReader((tmp_path / "audit" / "invalid_ohlcv.csv").open(encoding="utf-8")))
    assert any(row["field"] == "OHLC" for row in invalid)
    assert not (tmp_path / "normalized" / "nse_cm_eod_sample.csv").exists()


def test_invalid_ohlc_relationship_is_recorded_not_repaired(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row(open="120.00")])
    result = _run(tmp_path, source)
    assert result["ohlcv"] == "FAIL"
    rows = list(csv.DictReader((tmp_path / "audit" / "invalid_ohlcv.csv").open(encoding="utf-8")))
    assert any(row["field"] == "OHLC" for row in rows)
    assert _row(open="120.00")["OPEN"] == "120.00"
    assert not (tmp_path / "normalized" / "nse_cm_eod_sample.csv").exists()


def test_nonpositive_prices_negative_volume_and_bad_numeric_are_rejected(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [
        _row(date="2020-01-02", symbol="ZERO", open="0"),
        _row(date="2020-01-03", symbol="NEG", volume="-1"),
        _row(date="2020-01-04", symbol="BAD", close="1,2,3"),
    ])
    result = _run(tmp_path, source)
    assert result["ohlcv"] == "FAIL"
    rows = list(csv.DictReader((tmp_path / "audit" / "invalid_ohlcv.csv").open(encoding="utf-8")))
    assert any(row["field"] == "open" and "positive" in row["reason"] for row in rows)
    assert any(row["field"] == "volume" and "non-negative" in row["reason"] for row in rows)
    assert any(row["field"] == "close" and "malformed" in row["reason"] for row in rows)


def test_duplicate_keys_and_conflicting_duplicates_are_not_silently_deduplicated(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row(), _row(), _row(close="106.00", high="111.00")])
    result = _run(tmp_path, source)
    assert result["duplicate_key_count"] == 1
    assert result["duplicate_row_count"] == 2
    assert result["conflicting_duplicate_count"] == 1
    assert result["duplicates"] == "FAIL"
    duplicates = list(csv.DictReader((tmp_path / "audit" / "duplicate_records.csv").open(encoding="utf-8")))
    assert len(duplicates) == 3
    assert all(row["conflicting_duplicate"] == "true" for row in duplicates)
    assert not (tmp_path / "normalized" / "nse_cm_eod_sample.csv").exists()


def test_nifty_and_vix_coverage_is_diagnostic_and_does_not_assume_weekday_sessions(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row(date="2020-02-01")])
    _write_reference(tmp_path / "index_NSEI.csv", ["2020-02-01", "2020-02-02"])
    _write_reference(tmp_path / "index_INDIAVIX.csv", ["2020-02-01", "2020-02-02"])
    result = _run(tmp_path, source)
    coverage = result["coverage_comparison"]
    assert coverage["NIFTY_reference_dates"] == 2
    assert coverage["EQ_source_dates"] == 1
    assert coverage["intersection"] == 1
    assert coverage["NIFTY_dates_missing_from_EQ"] == ["2020-02-02"]
    rows = list(csv.DictReader((tmp_path / "audit" / "coverage_comparison.csv").open(encoding="utf-8")))
    missing = next(row for row in rows if row["date"] == "2020-02-02" and row["source"] == "NIFTY")
    assert missing["classification"] == "POSSIBLE_EQ_DATA_GAP"
    assert result["coverage"] == "PASS_WITH_WARNINGS"


def test_symbol_isin_anomalies_are_reported_without_identity_merging(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [
        _row(date="2020-01-02", isin="INE000A01001"),
        _row(date="2020-01-03", isin="INE000A01002"),
        _row(date="2020-01-04", symbol="BETA", isin="INE000A01002"),
    ])
    result = _run(tmp_path, source)
    assert result["rows_with_ISIN"] == 3
    assert result["unique_ISINs"] == 2
    assert result["symbol_to_multiple_ISINs"] == {"ALPHA": ["INE000A01001", "INE000A01002"]}
    assert result["ISIN_to_multiple_symbols"] == {"INE000A01002": ["ALPHA", "BETA"]}
    anomalies = list(csv.DictReader((tmp_path / "audit" / "symbol_isin_anomalies.csv").open(encoding="utf-8")))
    assert {row["anomaly_type"] for row in anomalies} == {"symbol_to_multiple_ISINs", "ISIN_to_multiple_symbols"}


def test_extreme_return_thresholds_are_diagnostic_and_do_not_drop_rows(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [
        _row(date="2020-01-02", close="100.00", high="110.00"),
        _row(date="2020-01-03", close="120.00", high="125.00"),
        _row(date="2020-01-04", close="200.00", high="205.00"),
        _row(date="2020-01-05", close="301.00", high="305.00"),
    ])
    _write_reference(tmp_path / "index_NSEI.csv", [
        "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05",
    ])
    result = _run(tmp_path, source)
    extremes = list(csv.DictReader((tmp_path / "audit" / "extreme_returns.csv").open(encoding="utf-8")))
    assert len(extremes) == 3
    assert "abs_return_gt_10pct" in extremes[0]["threshold_exceeded"]
    assert "abs_return_gt_20pct" in extremes[1]["threshold_exceeded"]
    assert "abs_return_gt_50pct" in extremes[1]["threshold_exceeded"]
    normalized = list(csv.DictReader((tmp_path / "normalized" / "nse_cm_eod_sample.csv").open(encoding="utf-8")))
    assert len(normalized) == 4


def test_no_synthetic_dates_and_no_price_adjustment(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row(date="2020-01-02", close="105.00"), _row(date="2020-01-06", close="110.00")])
    _write_reference(tmp_path / "index_NSEI.csv", ["2020-01-02", "2020-01-03", "2020-01-06"])
    _run(tmp_path, source)
    normalized = list(csv.DictReader((tmp_path / "normalized" / "nse_cm_eod_sample.csv").open(encoding="utf-8")))
    assert [row["date"] for row in normalized] == ["2020-01-02", "2020-01-06"]
    assert [row["close"] for row in normalized] == ["105.00", "110.00"]


def test_authorization_missing_allows_audit_but_blocks_normalized_output(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row()])
    result = _run(tmp_path, source, confirmed=False)
    assert result["schema"] == "PASS"
    assert result["ohlcv"] == "PASS"
    assert result["acceptance_status"] == "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED"
    assert result["normalization_written"] is False
    assert not (tmp_path / "normalized" / "nse_cm_eod_sample.csv").exists()


def test_provenance_records_actual_sha256_and_file_size(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row()])
    _run(tmp_path, source)
    provenance = json.loads((tmp_path / "manifest" / "sample_provenance.json").read_text(encoding="utf-8"))
    assert provenance["source_sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert provenance["source_file_size_bytes"] == source.stat().st_size
    assert provenance["git_commit"] is None
    assert provenance["acquisition_date"] == "2026-09-26"
    assert "adjustment_status" not in provenance


def test_provenance_preserves_only_explicit_adjustment_status(tmp_path) -> None:
    source = tmp_path / "sample.csv"
    _write_csv(source, [_row()])
    mapping = _mapping()
    mapping["adjustment_status"] = "Unadjusted close prices, as stated in delivery notes"
    _run(tmp_path, source, mapping=mapping)
    provenance = json.loads((tmp_path / "manifest" / "sample_provenance.json").read_text(encoding="utf-8"))
    assert provenance["adjustment_status"] == "Unadjusted close prices, as stated in delivery notes"


def test_cli_rejects_sample_outside_incoming_directory(tmp_path, capsys) -> None:
    from nse_research.data.nse_cm_eod_sample_audit import main

    root = tmp_path / "project"
    incoming = root / "data" / "licensed_nse_cm_eod" / "incoming"
    incoming.mkdir(parents=True)
    outside = tmp_path / "other.csv"
    outside.write_text("A\n1\n", encoding="utf-8")
    assert main(["--root", str(root), "--source", str(outside)]) == 2
    assert "must be inside" in capsys.readouterr().out


def test_csv_gzip_and_zip_containers_are_read_with_explicit_mapping(tmp_path) -> None:
    row = _row()
    csv_path = tmp_path / "payload.csv"
    _write_csv(csv_path, [row])
    gzip_path = tmp_path / "payload.csv.gz"
    with csv_path.open("rb") as src, gzip.open(gzip_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    gzip_result = _run(tmp_path / "gz", gzip_path, mapping=_mapping("csv.gz"))
    assert gzip_result["EQ_rows"] == 1

    zip_path = tmp_path / "payload.csv.zip"
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.write(csv_path, "sample.csv")
    zip_result = _run(tmp_path / "zip", zip_path, mapping=_mapping("csv.zip"))
    assert zip_result["EQ_rows"] == 1


def test_mapping_manifest_keeps_all_target_fields_unmapped_until_sample_arrives() -> None:
    mapping = json.loads(MAP_TEMPLATE.read_text(encoding="utf-8"))
    assert tuple(mapping["target_schema"]) == TARGET_COLUMNS
    assert set(mapping["required_fields"]) == set(REQUIRED_FIELDS)
    assert all(mapping["source_columns"][field] is None for field in TARGET_COLUMNS)
    assert mapping["mapping_status"] == "UNVERIFIED_NO_SAMPLE"
