from __future__ import annotations

import csv
import json
import shutil
import zipfile
from datetime import date
from pathlib import Path

import pytest

from nse_research.data.licensed_nse_cm_eod import (
    CANONICAL_COLUMNS,
    LicensedNseCmEodProvider,
    NseCmEodAuthorizationError,
    NseCmEodSchema,
    NseCmEodSchemaError,
    SymbolHistoryEvent,
)
from nse_research.data.licensed_nse_cm_eod_setup import import_authorized_nse_cm_eod

FIXTURES = Path(__file__).parent / "fixtures" / "nse_cm_eod"
SCHEMA = NseCmEodSchema(
    date_column="Trading Date",
    symbol_column="Instrument Symbol",
    series_column="Security Series",
    open_column="Open",
    high_column="High",
    low_column="Low",
    close_column="Close",
    volume_column="Traded Qty",
)


def _provider(tmp_path: Path, fixture: str = "valid_synthetic.csv", **kwargs) -> LicensedNseCmEodProvider:
    source_dir = tmp_path / "licensed_delivery"
    source_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(FIXTURES / fixture, source_dir / fixture)
    return LicensedNseCmEodProvider(source_dir, SCHEMA, **kwargs)


def _import(provider: LicensedNseCmEodProvider, **kwargs) -> dict[str, object]:
    return provider.import_to_dataset(
        date(2020, 1, 1), date(2020, 1, 10), authorization_reference="TEST-FIXTURE-ONLY",
        **kwargs,
    )


def test_synthetic_valid_file_normalizes_to_compatible_schema_and_manifest(tmp_path) -> None:
    provider = _provider(tmp_path)

    assert len(provider.discover()) == 1
    file_audit = provider.validate_file(provider.discover()[0])
    assert file_audit["row_count"] == 5
    manifest = _import(provider)

    assert manifest["validation_status"] == "PASS"
    assert manifest["source_access"] == "local files only; no NSE network access"
    assert manifest["source_row_count"] == 5
    assert manifest["eq_rows_seen"] == 4
    assert manifest["row_count"] == 4
    assert manifest["rejected_row_count"] == 1
    assert manifest["duplicate_count"] == 0
    assert manifest["source_file_hashes"][0]["sha256"]
    with Path(provider.dataset_path).open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert tuple(reader.fieldnames) == CANONICAL_COLUMNS
    alpha = next(row for row in rows if row["Symbol"] == "ALPHA")
    assert alpha == {
        "Symbol": "ALPHA", "Series": "EQ", "Date": "02-Jan-2020",
        "Open Price": "100.00", "High Price": "110.00", "Low Price": "90.00",
        "Close Price": "105.00", "Total Traded Quantity": "1000",
    }
    saved_manifest = json.loads(Path(provider.manifest_path).read_text(encoding="utf-8"))
    saved_report = json.loads(Path(provider.report_path).read_text(encoding="utf-8"))
    assert saved_manifest["authorization_reference"] == "TEST-FIXTURE-ONLY"
    assert saved_report["symbols"] == ["ALPHA", "NEWCO", "OLDCO"]


def test_non_eq_rows_are_excluded_without_series_merging(tmp_path) -> None:
    provider = _provider(tmp_path)
    manifest = _import(provider)
    assert manifest["row_count"] == 4
    with Path(provider.dataset_path).open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert {row["Series"] for row in rows} == {"EQ"}
    assert sum(row["Symbol"] == "ALPHA" for row in rows) == 2


def test_duplicate_symbol_date_fails_and_does_not_replace_dataset(tmp_path) -> None:
    provider = _provider(tmp_path, "duplicate_synthetic.csv")
    existing = Path(provider.dataset_path)
    existing.write_text("previous verified dataset\n", encoding="utf-8")

    manifest = _import(provider)

    assert manifest["validation_status"] == "FAIL"
    assert manifest["duplicate_count"] == 1
    assert existing.read_text(encoding="utf-8") == "previous verified dataset\n"
    assert manifest["dataset_path"] is None


def test_invalid_ohlc_is_counted_and_import_is_blocked(tmp_path) -> None:
    provider = _provider(tmp_path, "invalid_ohlc_synthetic.csv")
    manifest = _import(provider)
    assert manifest["validation_status"] == "FAIL"
    assert manifest["invalid_ohlc_count"] == 1
    assert not Path(provider.dataset_path).exists()


def test_zero_and_negative_prices_or_volume_are_rejected(tmp_path) -> None:
    provider = _provider(tmp_path, "nonpositive_synthetic.csv")
    manifest = _import(provider)
    assert manifest["validation_status"] == "FAIL"
    assert manifest["normalization_counts"]["nonpositive_price_field_count"] >= 3
    assert manifest["normalization_counts"]["negative_volume_row_count"] == 1


def test_malformed_date_is_rejected(tmp_path) -> None:
    provider = _provider(tmp_path, "malformed_date_synthetic.csv")
    manifest = _import(provider)
    assert manifest["validation_status"] == "FAIL"
    assert manifest["normalization_counts"]["malformed_date_count"] == 1
    assert not Path(provider.dataset_path).exists()


def test_malformed_numeric_grouping_is_rejected(tmp_path) -> None:
    provider = _provider(tmp_path, "malformed_numeric_synthetic.csv")
    manifest = _import(provider)
    assert manifest["validation_status"] == "FAIL"
    assert manifest["normalization_counts"]["invalid_numeric_row_count"] == 1
    assert not Path(provider.dataset_path).exists()


def test_missing_required_volume_header_rejects_the_file(tmp_path) -> None:
    provider = _provider(tmp_path, "missing_volume_synthetic.csv")
    with pytest.raises(NseCmEodSchemaError, match="Traded Qty"):
        provider.validate_file(provider.discover()[0])
    manifest = _import(provider)
    assert manifest["validation_status"] == "FAIL"
    assert "Traded Qty" in manifest["file_errors"][0]


def test_authorization_reference_is_required_before_import(tmp_path) -> None:
    provider = _provider(tmp_path)
    with pytest.raises(NseCmEodAuthorizationError):
        provider.import_to_dataset(date(2020, 1, 1), date(2020, 1, 10), authorization_reference=" ")
    assert not Path(provider.dataset_path).exists()


def test_symbol_renames_are_kept_separate_and_unknown_mapping_is_reported(tmp_path) -> None:
    provider = _provider(
        tmp_path,
        symbol_events=[SymbolHistoryEvent("ticker_change", "OLDCO", effective_date=date(2020, 1, 3))],
    )
    _import(provider)
    report = json.loads(Path(provider.report_path).read_text(encoding="utf-8"))
    assert "OLDCO" in report["symbols"] and "NEWCO" in report["symbols"]
    assert report["symbol_history_metadata"]["identity_mappings_applied"] == []
    event = report["symbol_history_metadata"]["events"][0]
    assert event["status"] == "unknown"
    assert event["related_symbol"] is None


def test_delisting_metadata_requires_reference_and_is_preserved(tmp_path) -> None:
    event = SymbolHistoryEvent(
        "delisting", "OLDCO", effective_date=date(2020, 1, 4), status="known",
        source_reference="synthetic fixture metadata",
    )
    provider = _provider(tmp_path, symbol_events=[event])
    _import(provider)
    report = json.loads(Path(provider.report_path).read_text(encoding="utf-8"))
    assert report["symbol_history_metadata"]["events"][0]["event_type"] == "delisting"
    oldco = next(item for item in report["symbols_disappearing_before_end"] if item["symbol"] == "OLDCO")
    assert "delisting/rename is unknown" in oldco["disappearance_interpretation"]


def test_authoritative_calendar_and_large_jump_are_reported_without_adjustment(tmp_path) -> None:
    source_dir = tmp_path / "licensed_delivery"
    source_dir.mkdir()
    (source_dir / "jump.csv").write_text(
        "Trading Date,Instrument Symbol,Security Series,Open,High,Low,Close,Traded Qty\n"
        "2020-01-02,ALPHA,EQ,100,105,95,100,1000\n"
        "2020-01-03,ALPHA,EQ,150,155,145,150,1100\n",
        encoding="utf-8",
    )
    provider = LicensedNseCmEodProvider(
        source_dir, SCHEMA, exchange_sessions=[date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 6)]
    )
    manifest = _import(provider)
    report = json.loads(Path(provider.report_path).read_text(encoding="utf-8"))
    assert manifest["validation_status"] == "PASS_WITH_WARNINGS"
    assert len(report["abnormal_price_jumps"]) == 1
    assert report["missing_session_statistics"]["dataset_missing_sessions"] == 1
    assert report["missing_session_statistics"]["per_symbol"]["ALPHA"]["missing_session_count"] == 0


def test_discover_reads_a_local_zip_without_network_access(tmp_path) -> None:
    source_dir = tmp_path / "licensed_delivery"
    source_dir.mkdir()
    archive_path = source_dir / "daily.csv.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.write(FIXTURES / "valid_synthetic.csv", "delivery.csv")
    provider = LicensedNseCmEodProvider(source_dir, SCHEMA)
    assert provider.discover() == [archive_path]
    assert len(provider.parse_file(archive_path)) == 5


def test_history_uses_the_same_daily_ohlcv_provider_contract(tmp_path) -> None:
    provider = _provider(tmp_path)
    _import(provider)
    bars = provider.history("ALPHA.NS", date(2020, 1, 2), date(2020, 1, 3))
    assert list(bars.columns) == ["open", "high", "low", "close", "volume"]
    assert len(bars) == 2
    assert bars.iloc[0]["close"] == 105.0
    cached_dataset = provider._history_dataset_cache
    repeated = provider.history("ALPHA.NS", date(2020, 1, 2), date(2020, 1, 3))
    assert len(repeated) == 2
    assert provider._history_dataset_cache is cached_dataset


def test_authorized_import_writes_coverage_and_only_supplied_provenance(tmp_path) -> None:
    provider = _provider(
        tmp_path,
        exchange_sessions=[date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 6)],
    )
    outcome = import_authorized_nse_cm_eod(
        provider=provider,
        start=date(2020, 1, 2),
        end=date(2020, 1, 6),
        authorization_reference="SYNTHETIC-TEST-AUTH",
        expected_symbols=["ALPHA", "NEWCO", "OLDCO", "MISSING"],
        source_provider="Synthetic authorized fixture provider",
        source_identifier="FIXTURE-BATCH-1",
    )

    coverage_path = Path(provider.dataset_path).parent / "coverage_report.json"
    provenance_path = Path(provider.dataset_path).parent / "provenance.json"
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    assert outcome["import"]["validation_status"] == "PASS_WITH_WARNINGS"
    assert coverage == {
        "first_date": "2020-01-02",
        "last_date": "2020-01-03",
        "number_of_sessions": 3,
        "number_of_symbols": 3,
        "total_rows": 5,
        "eq_rows": 4,
        "non_eq_rows": 1,
        "missing_sessions": 1,
        "missing_session_dates": ["2020-01-06"],
        "missing_symbols": ["MISSING"],
        "duplicate_rows": 0,
        "invalid_rows": 0,
        "validation_status": "FAIL",
    }
    assert provenance["provider"] == "Synthetic authorized fixture provider"
    assert provenance["file_name"] == "valid_synthetic.csv"
    assert provenance["source_identifier"] == "FIXTURE-BATCH-1"
    assert provenance["authorization_reference"] == "SYNTHETIC-TEST-AUTH"
    assert "delivery_date" not in provenance
    assert "adjustment_status" not in provenance
