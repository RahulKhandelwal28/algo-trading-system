from __future__ import annotations

import csv
import gzip
import hashlib
import json
import random
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nse_research.data.intraday_ingestion import (
    DatasetLock,
    Phase4FError,
    assert_research_ready,
    normalize_dataset,
    register_dataset,
    sha256_file,
    validate_dataset,
    validate_session_coverage,
    validate_session_timestamp,
)
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.intraday.model import IntradayBar, IntradayDataProvider
from nse_research.intraday.phase4f_adapter import (
    CanonicalIntradayDatasetProvider,
    Phase4BScannerAdapter,
    Phase4DReplayAdapter,
)
from nse_research.intraday.scanner import IntradayScanner, IntradayScannerConfig


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "intraday"
IST = ZoneInfo("Asia/Kolkata")


def _rows(path: Path = FIXTURE_DIR / "synthetic_1m.csv") -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as stream:
        return list(csv.DictReader(stream))


def _write_rows(path: Path, rows: list[dict[str, object]], *, headers=None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = list(headers or (rows[0].keys() if rows else ()))
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=names)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _metadata(timeframe: str = "1m", **overrides) -> dict:
    value = {
        "provider": "SYNTHETIC_TEST_FIXTURE",
        "dataset_name": "Synthetic unit-test bars",
        "revision": "fixture-v1",
        "timeframe": timeframe,
        "data_type": "bars",
        "timestamp_convention": "BAR_START",
        "original_timezone": "Asia/Kolkata",
        "volume_semantics": "INCREMENTAL_SHARES",
        "adjustment_status": "UNKNOWN",
        "license_status": "TEST_ONLY",
        "bars_finalized": True,
        "bars_finalized_documentation": "synthetic test fixture contract",
        "identity_documentation": "fixture rows explicitly carry NSE and EQ fields",
        "source_url": "UNKNOWN",
        "publisher": "SYNTHETIC_TEST_FIXTURE",
        "research_allowed": False,
        "commercial_allowed": False,
        "redistribution_terms": "TEST_ONLY",
        "universe_bias": "SYNTHETIC_TEST_FIXTURE",
    }
    value.update(overrides)
    return value


def _register(tmp_path: Path, rows=None, *, timeframe="1m", metadata=None, name="input.csv") -> tuple[str, Path, Path]:
    source = tmp_path / name
    _write_rows(source, list(rows if rows is not None else _rows()))
    metadata_file = Path(str(source) + ".metadata.json")
    metadata_file.write_text(json.dumps(metadata or _metadata(timeframe)), encoding="utf-8")
    result = register_dataset(source, metadata_path=metadata_file, project_root=tmp_path)
    return result["dataset"]["dataset_id"], source, metadata_file


def _output(root: Path, dataset_id: str) -> Path:
    return root / "data/intraday/normalized" / dataset_id / "canonical_intraday_bars.csv"


def test_synthetic_fixtures_are_isolated_and_labelled():
    for name in ("synthetic_1m.csv", "synthetic_5m.csv"):
        content = (FIXTURE_DIR / name).read_text(encoding="utf-8")
        assert "SYNTHETIC_TEST_FIXTURE" in content
    assert "not NSE observations" in (FIXTURE_DIR / "README.md").read_text(encoding="utf-8")


def test_registration_hashes_copies_immutably_and_is_idempotent(tmp_path: Path):
    dataset_id, source, metadata = _register(tmp_path)
    registry = json.loads((tmp_path / "data/intraday/datasets.json").read_text())
    record = registry["datasets"][0]
    raw_dir = tmp_path / record["raw_path"]
    raw_copy = raw_dir / source.name
    assert record["source_hash"] == hashlib.sha256(json.dumps(record["file_manifest"], sort_keys=True,
        separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    assert sha256_file(source) == sha256_file(raw_copy)
    before = raw_copy.read_bytes()
    source.write_text(source.read_text() + "\n", encoding="utf-8")
    assert raw_copy.read_bytes() == before
    with pytest.raises(Phase4FError, match="DATASET_ID_CONFLICT"):
        register_dataset(source, metadata_path=metadata, project_root=tmp_path)
    # Restore the original external file and verify repeat registration is stable.
    source.write_bytes(before)
    result = register_dataset(source, metadata_path=metadata, project_root=tmp_path)
    assert result["status"] == "ALREADY_REGISTERED"
    assert result["dataset"]["dataset_id"] == dataset_id
    manifest = json.loads((tmp_path / record["source_manifest"]).read_text())
    assert manifest["source_hash"] == record["source_hash"]
    assert manifest["file_manifest"][0]["sha256"] == sha256_file(raw_copy)


def test_registration_dry_run_does_not_mutate_project(tmp_path: Path):
    source = tmp_path / "source.csv"
    _write_rows(source, _rows())
    metadata = Path(str(source) + ".metadata.json")
    metadata.write_text(json.dumps(_metadata()), encoding="utf-8")
    result = register_dataset(source, metadata_path=metadata, project_root=tmp_path, dry_run=True)
    assert result["status"] == "DRY_RUN"
    assert not (tmp_path / "data/intraday").exists()


def test_unknown_source_revision_stays_unknown_while_id_uses_content_fingerprint(tmp_path: Path):
    source = tmp_path / "revisionless.csv"
    _write_rows(source, _rows())
    metadata = Path(str(source) + ".metadata.json")
    values = _metadata()
    values.pop("revision")
    metadata.write_text(json.dumps(values), encoding="utf-8")
    registered = register_dataset(source, metadata_path=metadata, project_root=tmp_path)
    record = registered["dataset"]
    assert record["revision"] == "UNKNOWN"
    assert record["dataset_id"].endswith(record["source_hash"][:16])
    assert record["source"] == source.as_posix()


def test_missing_timeframe_or_unidentifiable_schema_fails_registration(tmp_path: Path):
    source = tmp_path / "bad.csv"
    _write_rows(source, [{"date": "2026-09-28", "close": "10"}])
    metadata = Path(str(source) + ".metadata.json")
    metadata.write_text(json.dumps(_metadata()), encoding="utf-8")
    with pytest.raises(Phase4FError, match="DATASET_SCHEMA_UNIDENTIFIABLE"):
        register_dataset(source, metadata_path=metadata, project_root=tmp_path)
    _write_rows(source, _rows())
    metadata.write_text(json.dumps({key: val for key, val in _metadata().items() if key != "timeframe"}), encoding="utf-8")
    with pytest.raises(Phase4FError, match="TIMEFRAME_REQUIRED"):
        register_dataset(source, metadata_path=metadata, project_root=tmp_path)
    metadata.write_text(json.dumps(_metadata("1d")), encoding="utf-8")
    with pytest.raises(Phase4FError, match="TIMEFRAME_REQUIRED"):
        register_dataset(source, metadata_path=metadata, project_root=tmp_path)


@pytest.mark.parametrize("kind", ["gzip", "zip", "parquet"])
def test_registration_supports_compressed_and_parquet_inputs(tmp_path: Path, kind: str):
    rows = _rows()
    if kind == "gzip":
        path = tmp_path / "delivery.csv.gz"
        with gzip.open(path, "wt", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    elif kind == "zip":
        path = tmp_path / "delivery.csv.zip"
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            text_path = tmp_path / "inner.csv"
            _write_rows(text_path, rows)
            archive.write(text_path, "year/part.csv")
    else:
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = tmp_path / "delivery.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
    metadata_file = Path(str(path) + ".metadata.json")
    metadata_file.write_text(json.dumps(_metadata()), encoding="utf-8")
    result = register_dataset(path, metadata_path=metadata_file, project_root=tmp_path)
    assert result["status"] == "REGISTERED"
    assert result["dataset"]["file_manifest"][0]["sha256"] == sha256_file(path)


def test_registration_supports_directory_as_multi_file_dataset(tmp_path: Path):
    folder = tmp_path / "delivery"
    folder.mkdir()
    rows = _rows()
    _write_rows(folder / "part-a.csv", rows[:3])
    _write_rows(folder / "part-b.csv", rows[3:])
    (folder / "dataset_metadata.json").write_text(json.dumps(_metadata()), encoding="utf-8")
    result = register_dataset(folder, project_root=tmp_path)
    assert len(result["dataset"]["file_manifest"]) == 2
    validation = validate_dataset(result["dataset"]["dataset_id"], project_root=tmp_path, batch_size=2)
    assert validation["result"]["phase4e_validation_status"] == "PASS"
    assert validation["result"]["statistics"]["phase4e"]["rows"] == len(rows)


def test_phase4e_validation_and_normalization_are_separate_gates(tmp_path: Path):
    dataset_id, _, _ = _register(tmp_path)
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_REJECTED"):
        normalize_dataset(dataset_id, project_root=tmp_path)
    result = validate_dataset(dataset_id, project_root=tmp_path, batch_size=2)
    assert result["result"]["phase4e_validation_status"] == "PASS"
    assert result["result"]["research_ready"] is False
    normalized = normalize_dataset(dataset_id, project_root=tmp_path, batch_size=2)
    assert normalized["status"] == "COMPLETE"
    manifest = json.loads((tmp_path / "data/intraday/normalized" / dataset_id / "manifest.json").read_text())
    assert manifest["status"] == "COMPLETE"
    assert manifest["row_count"] == len(_rows())
    assert manifest["timezone"] == "Asia/Kolkata"
    assert manifest["timestamp_convention_canonical"] == "BAR_START"
    assert normalize_dataset(dataset_id, project_root=tmp_path)["status"] == "ALREADY_COMPLETE"
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        assert_research_ready(dataset_id, project_root=tmp_path)


def test_valid_5m_dataset_normalizes_without_changing_bar_granularity(tmp_path: Path):
    rows = _rows(FIXTURE_DIR / "synthetic_5m.csv")
    dataset_id, _, _ = _register(tmp_path, rows, timeframe="5m")
    result = validate_dataset(dataset_id, project_root=tmp_path)
    assert result["result"]["phase4e_validation_status"] == "PASS"
    normalize_dataset(dataset_id, project_root=tmp_path)
    canonical = _rows(_output(tmp_path, dataset_id))
    assert len(canonical) == len(rows)
    assert {row["timeframe"] for row in canonical} == {"5m"}
    assert {row["timestamp"][11:16] for row in canonical} == {"09:15", "09:20"}


def test_documented_symbol_mapping_supplies_missing_identity_but_rejects_conflicts(tmp_path: Path):
    rows = _rows()
    mapping = {symbol: {"symbol": symbol, "exchange": "NSE", "series": "EQ"}
               for symbol in ("RELIANCE", "TCS", "INFY")}
    for row in rows:
        row["exchange"] = ""
        row["series"] = ""
    metadata = _metadata(symbol_mapping=mapping,
                         symbol_mapping_documentation="official instrument-master revision 2026-09")
    dataset_id, _, _ = _register(tmp_path / "mapped", rows, metadata=metadata)
    assert validate_dataset(dataset_id, project_root=tmp_path / "mapped")["result"]["phase4e_validation_status"] == "PASS"

    conflicting = _rows()
    conflicting[0]["exchange"] = "BSE"
    other_id, _, _ = _register(tmp_path / "conflict", conflicting,
                               metadata=_metadata(symbol_mapping=mapping,
                                                  symbol_mapping_documentation="documented test mapping"))
    failed = validate_dataset(other_id, project_root=tmp_path / "conflict")
    assert failed["result"]["phase4e_validation_status"] == "FAIL"
    assert any("SYMBOL_MAPPING_SOURCE_IDENTITY_MISMATCH" in json.dumps(error)
               for error in failed["result"]["errors"])


def test_normalization_dry_run_has_no_filesystem_mutation(tmp_path: Path):
    dataset_id, _, _ = _register(tmp_path)
    validate_dataset(dataset_id, project_root=tmp_path)
    normalized_dir = tmp_path / "data/intraday/normalized" / dataset_id
    before = sorted((str(path.relative_to(tmp_path)), sha256_file(path))
                    for path in (tmp_path / "data/intraday").rglob("*") if path.is_file())
    result = normalize_dataset(dataset_id, project_root=tmp_path, dry_run=True)
    after = sorted((str(path.relative_to(tmp_path)), sha256_file(path))
                   for path in (tmp_path / "data/intraday").rglob("*") if path.is_file())
    assert result["status"] == "DRY_RUN"
    assert before == after
    assert not normalized_dir.exists()


def test_utc_conversion_and_bar_end_convention_preserve_instant_and_interval(tmp_path: Path):
    rows = _rows()
    for row in rows:
        source_end = datetime.fromisoformat(row["timestamp"]) + timedelta(minutes=1)
        row["timestamp"] = source_end.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    # Source rows are bar ends in UTC; canonical starts should be the original IST starts.
    metadata = _metadata(original_timezone="UTC", timestamp_convention="BAR_END")
    dataset_id, _, _ = _register(tmp_path, rows, metadata=metadata)
    assert validate_dataset(dataset_id, project_root=tmp_path)["result"]["phase4e_validation_status"] == "PASS"
    normalize_dataset(dataset_id, project_root=tmp_path)
    output_rows = _rows(_output(tmp_path, dataset_id))
    assert output_rows[0]["timestamp"].endswith("+05:30")
    assert output_rows[0]["timestamp"].startswith("2026-09-28T09:15:00")


def test_normalization_preserves_source_decimal_price_text_numerically(tmp_path: Path):
    rows = _rows()
    rows[0].update({
        "open": "200.12345678901234567890",
        "high": "202.12345678901234567890",
        "low": "199.12345678901234567890",
        "close": "201.12345678901234567890",
    })
    dataset_id, _, _ = _register(tmp_path, rows)
    validate_dataset(dataset_id, project_root=tmp_path)
    normalize_dataset(dataset_id, project_root=tmp_path)
    canonical = _rows(_output(tmp_path, dataset_id))
    tcs = next(row for row in canonical if row["symbol"] == "TCS" and row["timestamp"].startswith("2026-09-28T09:16"))
    assert tcs["open"] == "200.1234567890123456789"
    assert tcs["close"] == "201.1234567890123456789"


def test_shuffled_input_normalizes_to_same_ordered_bytes(tmp_path: Path):
    rows = _rows()
    shuffled = rows[:]
    random.Random(42).shuffle(shuffled)
    left_root, right_root = tmp_path / "left", tmp_path / "right"
    left_id, _, _ = _register(left_root, rows, metadata=_metadata(revision="same-v1"))
    right_id, _, _ = _register(right_root, shuffled, metadata=_metadata(revision="same-v1"))
    assert left_id == right_id
    validate_dataset(left_id, project_root=left_root)
    validate_dataset(right_id, project_root=right_root)
    normalize_dataset(left_id, project_root=left_root)
    normalize_dataset(right_id, project_root=right_root)
    left = _output(left_root, left_id)
    right = _output(right_root, right_id)
    assert left.read_bytes() == right.read_bytes()
    stamps = [(row["timestamp"], row["symbol"], row["timeframe"]) for row in _rows(left)]
    assert stamps == sorted(stamps, key=lambda item: (item[0], item[1], item[2]))


@pytest.mark.parametrize(
    "change,expected_code",
    [
        ({"high": "90"}, "OHLC_RELATION_INVALID"),
        ({"low": "0"}, "PRICE_NONPOSITIVE"),
        ({"close": "bad"}, "OHLC_MALFORMED"),
        ({"open": "-1"}, "PRICE_NONPOSITIVE"),
        ({"series": "BE"}, "SERIES_MISMATCH"),
        ({"series": "BZ"}, "SERIES_MISMATCH"),
        ({"series": ""}, "SERIES_MISMATCH"),
        ({"series": "FUT"}, "SERIES_MISMATCH"),
        ({"series": "OPT"}, "SERIES_MISMATCH"),
        ({"exchange": "BSE"}, "EXCHANGE_MISMATCH"),
        ({"volume": "-1"}, "VOLUME_NEGATIVE"),
        ({"timestamp": ""}, "TIMESTAMP_MISSING"),
        ({"timestamp": "2026-09-28 09:15:00"}, "TIMESTAMP_NAIVE_UNDOCUMENTED"),
    ],
)
def test_invalid_records_are_rejected_without_normalization(tmp_path: Path, change: dict, expected_code: str):
    rows = _rows()
    rows[0].update(change)
    dataset_id, _, _ = _register(tmp_path, rows)
    result = validate_dataset(dataset_id, project_root=tmp_path, batch_size=2)
    assert result["result"]["phase4e_validation_status"] == "FAIL"
    assert any(expected_code in json.dumps(error) for error in result["result"]["errors"])
    assert not _output(tmp_path, dataset_id).exists()
    assert (tmp_path / "results/phase4f" / dataset_id / "REJECTED_DATASET.json").is_file()
    assert (tmp_path / "data/intraday/quarantine" / f"{dataset_id}.json").is_file()


def test_duplicate_across_validation_batches_is_rejected(tmp_path: Path):
    rows = _rows()
    rows.append(dict(rows[0]))
    dataset_id, _, _ = _register(tmp_path, rows)
    result = validate_dataset(dataset_id, project_root=tmp_path, batch_size=1)
    assert result["result"]["phase4e_validation_status"] == "FAIL"
    assert result["result"]["statistics"]["phase4e"]["duplicate_rows"] == 1
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_REJECTED"):
        normalize_dataset(dataset_id, project_root=tmp_path)


def test_missing_expected_symbol_and_session_are_reported(tmp_path: Path):
    rows = _rows()
    rows = [row for row in rows if row["symbol"] != "TCS"]
    metadata = _metadata(expected_symbols=["RELIANCE", "TCS", "INFY"],
                         expected_sessions=["2026-09-28", "2026-09-29"])
    dataset_id, _, _ = _register(tmp_path, rows, metadata=metadata)
    result = validate_dataset(dataset_id, project_root=tmp_path)
    stats = result["result"]["statistics"]["phase4e"]
    assert stats["missing_symbols"] == ["TCS"]
    assert stats["session_coverage"]["missing_sessions"] == ["2026-09-29"]
    assert result["result"]["phase4e_validation_status"] == "FAIL"
    coverage = json.loads((tmp_path / "results/phase4f" / dataset_id / "coverage_report.json").read_text())
    assert coverage["missing_sessions"] == ["2026-09-29"]
    # Missing entire symbol/session combinations contribute their expected
    # intervals; they are never filled into canonical output.
    assert coverage["missing_interval_count"] >= 2 * 375
    missing_day = next(row for row in coverage["coverage_by_session"] if row["date"] == "2026-09-29")
    assert missing_day["missing_intervals"] == 3 * 375
    assert missing_day["unclassified_missing_symbol_sessions"] == 3


def test_rows_outside_delivery_expected_sessions_are_rejected(tmp_path: Path):
    rows = _rows()
    rows[0]["timestamp"] = "2026-09-29T09:15:00+05:30"
    dataset_id, _, _ = _register(
        tmp_path, rows,
        metadata=_metadata(expected_sessions=["2026-09-28"]),
    )
    result = validate_dataset(dataset_id, project_root=tmp_path)
    assert result["result"]["phase4e_validation_status"] == "FAIL"
    assert any("SESSION_UNEXPECTED_DATE" in json.dumps(error)
               for error in result["result"]["errors"])


def test_documented_no_trade_is_separated_from_a_dataset_gap(tmp_path: Path):
    metadata = _metadata(expected_symbols=["RELIANCE", "TCS", "INFY"],
                         expected_sessions=["2026-09-28"],
                         no_trade_symbol_sessions=[{"symbol": "TCS", "date": "2026-09-28"}])
    rows = [row for row in _rows() if row["symbol"] != "TCS"]
    dataset_id, _, _ = _register(tmp_path, rows, metadata=metadata)
    coverage = validate_dataset(dataset_id, project_root=tmp_path)["result"]["statistics"]["coverage"]
    session = coverage["coverage_by_session"][0]
    assert session["symbol_specific_no_trade"] == 1
    assert "TCS:2026-09-28" not in session["missing_symbol_sessions"]
    assert session["bars_expected"] == 2 * 375
    assert session["missing_intervals"] == 2 * 375 - 4  # four observed bars


def test_missing_interval_is_reported_no_forward_fill_zero_volume_is_preserved(tmp_path: Path):
    rows = [row for row in _rows() if not (row["symbol"] == "RELIANCE" and row["timestamp"].startswith("2026-09-28T09:16"))]
    rows[0]["volume"] = "0"
    dataset_id, _, _ = _register(tmp_path, rows)
    report = validate_dataset(dataset_id, project_root=tmp_path)
    assert report["result"]["phase4e_validation_status"] == "PASS"
    coverage = report["result"]["statistics"]["coverage"]
    assert coverage["missing_interval_count"] > 0
    assert coverage["zero_volume_bars"] == 1
    normalize_dataset(dataset_id, project_root=tmp_path)
    canonical = _rows(_output(tmp_path, dataset_id))
    assert len(canonical) == len(rows)
    assert sum(float(row["volume"]) == 0 for row in canonical) == 1


def test_large_observed_volume_changes_are_reported_not_filtered(tmp_path: Path):
    rows = _rows()
    for row in rows:
        if row["symbol"] == "RELIANCE":
            row["volume"] = "10" if row["timestamp"].startswith("2026-09-28T09:15") else "1000"
    dataset_id, _, _ = _register(tmp_path, rows)
    coverage = validate_dataset(dataset_id, project_root=tmp_path)["result"]["statistics"]["coverage"]
    diagnostic = coverage["volume_discontinuity_diagnostic"]
    assert diagnostic["maximum_multiple_between_observed_rows"] == "100"
    assert diagnostic["maximum_change_location"]["symbol"] == "RELIANCE"
    assert diagnostic["threshold"] == "NOT_CONFIGURED"
    normalize_dataset(dataset_id, project_root=tmp_path)
    canonical = _rows(_output(tmp_path, dataset_id))
    assert sorted(int(row["volume"]) for row in canonical if row["symbol"] == "RELIANCE") == [10, 1000]


def test_unknown_volume_semantics_remain_unknown_and_do_not_pass_readiness(tmp_path: Path):
    metadata = _metadata(volume_semantics="UNKNOWN")
    dataset_id, _, _ = _register(tmp_path, metadata=metadata)
    result = validate_dataset(dataset_id, project_root=tmp_path)
    assert result["result"]["phase4e_validation_status"] == "PASS"
    assert result["result"]["research_ready"] is False
    normalize_dataset(dataset_id, project_root=tmp_path)
    canonical = _rows(_output(tmp_path, dataset_id))
    assert {row["volume_kind"] for row in canonical} == {"BAR"}
    manifest = json.loads((tmp_path / "data/intraday/normalized" / dataset_id / "manifest.json").read_text())
    assert manifest["volume_semantics"] == "UNKNOWN"


def test_cumulative_volume_conversion_requires_explicit_documented_rule(tmp_path: Path):
    rows = _rows()
    for row in rows:
        row["volume"] = "10" if row["timestamp"].endswith("09:15:00+05:30") else "15"
    metadata = _metadata(volume_semantics="CUMULATIVE_SHARES", cumulative_to_incremental=True,
                         cumulative_increment_rule_documentation="provider specifies session cumulative resets")
    dataset_id, _, _ = _register(tmp_path, rows, metadata=metadata)
    assert validate_dataset(dataset_id, project_root=tmp_path)["result"]["phase4e_validation_status"] == "PASS"
    normalize_dataset(dataset_id, project_root=tmp_path)
    values = _rows(_output(tmp_path, dataset_id))
    # Each symbol's cumulative volumes are differenced within the session only.
    reliance = [float(row["volume"]) for row in values if row["symbol"] == "RELIANCE"]
    assert reliance == [10.0, 5.0]
    assert {row["volume_kind"] for row in values} == {"BAR"}


def test_cumulative_decrease_is_rejected(tmp_path: Path):
    rows = _rows()
    for row in rows:
        row["volume"] = "10" if row["timestamp"].endswith("09:15:00+05:30") else "5"
    metadata = _metadata(volume_semantics="CUMULATIVE_SHARES", cumulative_to_incremental=True,
                         cumulative_increment_rule_documentation="provider specifies reset at session open")
    dataset_id, _, _ = _register(tmp_path, rows, metadata=metadata)
    result = validate_dataset(dataset_id, project_root=tmp_path)
    assert result["result"]["phase4e_validation_status"] == "FAIL"
    assert any("CUMULATIVE_VOLUME_RESET" in json.dumps(error) for error in result["result"]["errors"])


def test_wrong_declared_timezone_and_unknown_bar_convention_fail_closed(tmp_path: Path):
    rows = _rows()
    utc_metadata = _metadata(original_timezone="UTC")
    dataset_id, _, _ = _register(tmp_path / "zone", rows, metadata=utc_metadata)
    result = validate_dataset(dataset_id, project_root=tmp_path / "zone")
    assert result["result"]["phase4e_validation_status"] == "FAIL"
    assert any("TIMESTAMP_SOURCE_ZONE_MISMATCH" in json.dumps(error) for error in result["result"]["errors"])

    unknown_metadata = _metadata(timestamp_convention="UNKNOWN")
    other_id, _, _ = _register(tmp_path / "convention", rows, metadata=unknown_metadata)
    other = validate_dataset(other_id, project_root=tmp_path / "convention")
    assert other["result"]["phase4e_validation_status"] == "FAIL"
    assert any("TIMESTAMP_CONVENTION_UNKNOWN" in json.dumps(error) for error in other["result"]["errors"])


def test_holiday_and_session_boundaries_use_existing_calendar():
    calendar = NseTradingCalendar(Path(__file__).resolve().parents[1] / "data/paper_trading/nse_cash_holidays_2026.csv")
    holiday = next(iter(calendar.holidays))
    result = validate_session_timestamp(datetime.combine(holiday, time(9, 15), IST), calendar=calendar)
    assert result.status == "HOLIDAY"
    assert validate_session_timestamp("2026-09-28T09:15:00+05:30").status == "NORMAL_SESSION"
    assert validate_session_timestamp("2026-09-28T15:29:00+05:30").status == "NORMAL_SESSION"
    assert validate_session_timestamp("2026-09-28T15:30:00+05:30").status == "OUTSIDE_SESSION"
    assert validate_session_timestamp("2026-09-28 09:15:00").status == "INVALID"


def test_session_coverage_reports_gaps_without_synthesizing_bars():
    result = validate_session_coverage(
        [{"timestamp": "2026-09-28T09:15:00+05:30", "symbol": "RELIANCE", "timeframe": "1m"}],
        timeframe="1m", expected_sessions=["2026-09-28"], expected_symbols=["RELIANCE", "TCS"],
    )
    assert result["missing_intervals"] == 749
    assert result["missing_symbol_sessions"] == ["TCS:2026-09-28:1m"]
    assert result["gap_policy"].startswith("reported only")


def test_no_research_ready_data_is_a_valid_blocked_state(tmp_path: Path):
    with pytest.raises(Phase4FError, match="PHASE4F_NO_RESEARCH_READY_DATA"):
        assert_research_ready(project_root=tmp_path)


def test_corruption_is_detected_for_source_and_normalized_output(tmp_path: Path):
    dataset_id, _, _ = _register(tmp_path)
    validate_dataset(dataset_id, project_root=tmp_path)
    normalize_dataset(dataset_id, project_root=tmp_path)
    output = _output(tmp_path, dataset_id)
    output.write_text(output.read_text() + "\n", encoding="utf-8")
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        assert_research_ready(dataset_id, project_root=tmp_path)
    with pytest.raises(Phase4FError, match="NORMALIZED_DATASET_INCOMPLETE"):
        normalize_dataset(dataset_id, project_root=tmp_path)
    output.unlink()
    with pytest.raises(Phase4FError, match="NORMALIZED_DATASET_INCOMPLETE"):
        normalize_dataset(dataset_id, project_root=tmp_path)


def test_dataset_lock_reports_concurrent_normalization_attempt(tmp_path: Path):
    path = tmp_path / "normalized" / ".lock"
    with DatasetLock(path):
        with pytest.raises(Phase4FError, match="DATASET_LOCKED"):
            with DatasetLock(path):
                pass


def test_ticks_are_registered_raw_only_and_never_aggregated(tmp_path: Path):
    source = tmp_path / "ticks.csv"
    _write_rows(source, [{"timestamp": "2026-09-28T09:15:01+05:30", "symbol": "RELIANCE", "price": "100"}])
    metadata = Path(str(source) + ".metadata.json")
    metadata.write_text(json.dumps(_metadata("tick", data_type="tick")), encoding="utf-8")
    dataset_id = register_dataset(source, metadata_path=metadata, project_root=tmp_path)["dataset"]["dataset_id"]
    result = validate_dataset(dataset_id, project_root=tmp_path)
    assert result["result"]["phase4e_validation_status"] == "NOT_APPLICABLE"
    assert result["result"]["errors"][0]["code"] == "TICK_AGGREGATION_NOT_IMPLEMENTED"
    with pytest.raises(Phase4FError, match="TICK_AGGREGATION_NOT_IMPLEMENTED"):
        normalize_dataset(dataset_id, project_root=tmp_path)
    record = json.loads((tmp_path / "data/intraday/datasets.json").read_text())["datasets"][0]
    assert record["data_type"] == "tick"
    assert list((tmp_path / record["raw_path"]).glob("*.csv"))
    assert not (tmp_path / "data/intraday/normalized" / dataset_id).exists()


def test_registry_and_certificate_flags_cannot_promote_nonready_data(tmp_path: Path):
    dataset_id, _, _ = _register(tmp_path)
    validate_dataset(dataset_id, project_root=tmp_path)
    normalize_dataset(dataset_id, project_root=tmp_path)
    registry_path = tmp_path / "data/intraday/datasets.json"
    registry = json.loads(registry_path.read_text())
    dataset = registry["datasets"][0]
    # Test the ready-path integrity check with a temporary synthetic registry
    # promotion. The fixture remains TEST_ONLY and is never made production data.
    dataset["research_ready"] = True
    dataset["validation_status"] = "PASS"
    dataset["research_readiness"] = "RESEARCH_READY"
    registry_path.write_text(json.dumps(registry), encoding="utf-8")
    certificate_path = tmp_path / "results/phase4f" / dataset_id / "ACCEPTED_DATASET.json"
    certificate = json.loads(certificate_path.read_text())
    certificate.update({"research_ready": True, "validation_status": "PASS"})
    certificate_path.write_text(json.dumps(certificate), encoding="utf-8")
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        assert_research_ready(dataset_id, project_root=tmp_path)


def test_manifest_row_count_and_coverage_tampering_blocks_readiness(tmp_path: Path):
    symbols = [f"SYM{index:02d}" for index in range(10)]
    calendar = NseTradingCalendar(Path(__file__).resolve().parents[1] / "data/paper_trading/nse_cash_holidays_2026.csv")
    sessions = [
        day for day in (date(2026, 1, 5) + timedelta(days=offset) for offset in range(116))
        if day.weekday() < 5 and day not in calendar.holidays
    ]
    rows = []
    for day in sessions:
        for symbol in symbols:
            rows.append({
                "timestamp": datetime.combine(day, time(9, 15), IST).isoformat(),
                "symbol": symbol, "exchange": "NSE", "series": "EQ", "timeframe": "1m",
                "open": "100", "high": "101", "low": "99", "close": "100",
                "volume": "10", "volume_kind": "BAR", "status": "CLOSED",
                "source": "SYNTHETIC_TEST_FIXTURE",
            })
    assert len(sessions) >= 60 and (sessions[-1] - sessions[0]).days >= 90
    metadata = _metadata(
        research_allowed=True,
        license="TEST_ONLY",
        source_url="synthetic-test://readiness-fixture",
        acquisition_timestamp="2026-09-27T00:00:00+00:00",
        adjustment_status="RAW",
        adjustment_status_documentation="test fixture declares one common raw price basis for all rows",
        expected_symbols=symbols,
        expected_sessions=[day.isoformat() for day in sessions],
    )
    dataset_id, _, _ = _register(tmp_path, rows, metadata=metadata)
    result = validate_dataset(dataset_id, project_root=tmp_path)
    assert result["result"]["research_ready"] is True
    normalize_dataset(dataset_id, project_root=tmp_path)
    assert_research_ready(dataset_id, project_root=tmp_path)

    registry = json.loads((tmp_path / "data/intraday/datasets.json").read_text())
    raw_metadata = tmp_path / registry["datasets"][0]["raw_path"] / "delivery_metadata.json"
    original_metadata = raw_metadata.read_bytes()
    raw_metadata.write_bytes(original_metadata + b" ")
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        assert_research_ready(dataset_id, project_root=tmp_path)
    raw_metadata.write_bytes(original_metadata)
    assert_research_ready(dataset_id, project_root=tmp_path)

    without_sessions_root = tmp_path / "without_expected_sessions"
    without_sessions_metadata = {key: value for key, value in metadata.items() if key != "expected_sessions"}
    without_sessions_id, _, _ = _register(
        without_sessions_root, rows,
        metadata=without_sessions_metadata,
    )
    without_sessions_result = validate_dataset(without_sessions_id, project_root=without_sessions_root)
    assert without_sessions_result["result"]["phase4e_validation_status"] == "PASS"
    assert without_sessions_result["result"]["research_ready"] is False

    unknown_adjustment_root = tmp_path / "unknown_adjustment"
    unknown_adjustment_metadata = {**metadata, "adjustment_status": "UNKNOWN"}
    unknown_adjustment_metadata.pop("adjustment_status_documentation")
    unknown_adjustment_id, _, _ = _register(
        unknown_adjustment_root, rows,
        metadata=unknown_adjustment_metadata,
    )
    unknown_adjustment_result = validate_dataset(unknown_adjustment_id, project_root=unknown_adjustment_root)
    assert unknown_adjustment_result["result"]["phase4e_validation_status"] == "PASS"
    assert unknown_adjustment_result["result"]["research_ready"] is False

    manifest_path = tmp_path / "data/intraday/normalized" / dataset_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["row_count"] += 1
    manifest["coverage"]["coverage_start"] = "1900-01-01"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(Phase4FError, match="NORMALIZED_DATASET_INCOMPLETE"):
        assert_research_ready(dataset_id, project_root=tmp_path)


def test_raw_source_corruption_blocks_validation_and_readiness(tmp_path: Path):
    dataset_id, _, _ = _register(tmp_path)
    registry = json.loads((tmp_path / "data/intraday/datasets.json").read_text())
    raw_path = tmp_path / registry["datasets"][0]["raw_path"] / "input.csv"
    raw_path.write_text(raw_path.read_text() + "\n", encoding="utf-8")
    with pytest.raises(Phase4FError, match="SOURCE_HASH_MISMATCH"):
        validate_dataset(dataset_id, project_root=tmp_path)


def test_provider_api_is_read_only_and_streams_sorted_bars(tmp_path: Path, monkeypatch):
    from nse_research.intraday import phase4f_adapter

    canonical = tmp_path / "canonical.csv"
    rows = sorted(_rows(), key=lambda row: (row["timestamp"], row["symbol"]))
    _write_rows(canonical, [{**row, "timeframe": "1m"} for row in rows], headers=(
        "timestamp", "symbol", "exchange", "series", "timeframe", "open", "high", "low", "close",
        "volume", "volume_kind", "status", "source"))
    from nse_research.data.intraday_ingestion import sha256_file
    session = {"date": "2026-09-28", "bars_present": len(rows)}
    manifest = {"status": "COMPLETE", "normalized_hash": sha256_file(canonical), "symbols": ["INFY", "RELIANCE", "TCS"],
                "timeframe": "1m", "coverage": {"coverage_by_symbol": [{"symbol": "RELIANCE", "rows": 2}],
                                                       "coverage_by_session": [session]}}
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(phase4f_adapter, "assert_research_ready", lambda *_args, **_kwargs: {"normalized_path": str(canonical)})
    provider = CanonicalIntradayDatasetProvider("test", project_root=tmp_path)
    assert provider.available_symbols() == ["INFY", "RELIANCE", "TCS"]
    assert provider.has_symbol("reliance")
    assert provider.available_dates() == [date(2026, 9, 28)]
    assert provider.coverage("RELIANCE")["rows"] == 2
    bars = list(provider.iter_bars(["RELIANCE", "TCS", "INFY"], ["1m"]))
    assert [(bar.end, bar.symbol) for bar in bars] == sorted((bar.end, bar.symbol) for bar in bars)
    assert provider.get_bars("MISSING", timeframe="1m") == []
    with pytest.raises(ValueError, match="unsupported timeframe"):
        list(provider.iter_bars(["RELIANCE"], ["1d"]))


def test_largeish_csv_is_staged_validated_and_normalized_in_bounded_batches(tmp_path: Path):
    rows = []
    symbols = [f"SYM{index:03d}" for index in range(30)]
    start = datetime(2026, 9, 28, 9, 15, tzinfo=IST)
    for symbol_index, symbol in enumerate(symbols):
        for bar_index in range(375):
            stamp = start + timedelta(minutes=bar_index)
            close = 100 + symbol_index + bar_index / 100
            rows.append({
                "timestamp": stamp.isoformat(), "symbol": symbol, "exchange": "NSE", "series": "EQ",
                "open": str(close), "high": str(close + 1), "low": str(close - 1), "close": str(close),
                "volume": "100", "volume_kind": "BAR", "status": "CLOSED",
                "source": "SYNTHETIC_TEST_FIXTURE",
            })
    metadata = _metadata(expected_symbols=symbols, expected_sessions=["2026-09-28"])
    dataset_id, _, _ = _register(tmp_path, rows, metadata=metadata, name="largeish.csv")
    report = validate_dataset(dataset_id, project_root=tmp_path, batch_size=257)
    assert report["result"]["phase4e_validation_status"] == "PASS"
    normalized = normalize_dataset(dataset_id, project_root=tmp_path, batch_size=257)
    assert normalized["manifest"]["row_count"] == 30 * 375
    output = _output(tmp_path, dataset_id)
    with output.open("r", newline="", encoding="utf-8") as stream:
        assert sum(1 for _ in csv.DictReader(stream)) == 30 * 375


def test_replay_ledger_path_must_be_temporary():
    from nse_research.intraday.phase4f_adapter import _temporary_ledger

    assert _temporary_ledger("/private/tmp/phase4f-replay.json").is_absolute()
    protected_ledger = Path(__file__).resolve().parents[1] / "data/paper_trading/phase4c/ledger.json"
    with pytest.raises(Phase4FError, match="REPLAY_LEDGER_MUST_BE_TEMPORARY"):
        _temporary_ledger(protected_ledger)


def test_phase4b_adapter_uses_existing_scanner_without_future_bar_lookahead():
    day = date(2026, 9, 28)
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    bars = []
    for offset in range(40):
        stamp = datetime.combine(day, time(9, 15), IST) + timedelta(minutes=offset)
        close = 100 + offset * 0.25
        if offset == 39:
            close = 9999  # future shock must not alter earlier indicator/signal outputs
        bars.append(IntradayBar("RELIANCE", "NSE", "EQ", "1m", stamp,
                                close, close + 1, close - 1, close, 100, "CLOSED", "SYNTHETIC_TEST_FIXTURE", "BAR"))

    class SyntheticCanonicalProvider(CanonicalIntradayDatasetProvider):
        """Test-only ready-boundary stand-in; no production fixture is registered."""

        def __init__(self, values):
            self.values = values

        def iter_bars(self, _symbols, _timeframes, *, start=None, end=None):
            yield from self.values

    scanner_a = IntradayScanner(symbols, config=IntradayScannerConfig(enabled=True, universe_size=100, timeframes=("1m",)))
    scanner_b = IntradayScanner(symbols, config=IntradayScannerConfig(enabled=True, universe_size=100, timeframes=("1m",)))
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        Phase4BScannerAdapter(object(), symbols, timeframes=("1m",), scanner=scanner_a)
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        Phase4BScannerAdapter(SyntheticCanonicalProvider(bars), symbols,
                              timeframes=("1m",), scanner=scanner_a)
    # Exercise the existing scanner directly for the lookahead property. The
    # Phase 4B adapter itself must only accept a genuinely validated provider.
    for bar in bars[:30]:
        scanner_a.handle_bar(bar)
    for bar in bars:
        scanner_b.handle_bar(bar)
    prefix = {"signals": scanner_a.signals}
    full = {"signals": scanner_b.signals}
    first_signals = [item.signal_id for item in prefix["signals"]]
    extended_earlier_signals = [item.signal_id for item in full["signals"] if item.timestamp <= bars[29].end]
    assert first_signals == extended_earlier_signals


def test_phase4d_replay_requires_temporary_ledger_and_never_uses_project_ledger(tmp_path: Path):
    with pytest.raises(Phase4FError, match="PHASE4F_DATASET_NOT_REGISTERED"):
        Phase4DReplayAdapter("missing", ledger_path=tmp_path / "ledger.json", project_root=tmp_path)
    dataset_id, _, _ = _register(tmp_path)
    validate_dataset(dataset_id, project_root=tmp_path)
    normalize_dataset(dataset_id, project_root=tmp_path)
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        Phase4DReplayAdapter(dataset_id, ledger_path=Path("/private/tmp/phase4f_unit_ledger.json"), project_root=tmp_path)

    # The existing Phase 4D software-only path already uses a temporary ledger.
    # Exercise that bridge without touching the persistent Phase 4C ledger.
    from nse_research.execution.phase4d import synthetic_test
    temporary_ledger = Path("/private/tmp") / f"phase4f-synthetic-{tmp_path.name}.json"
    before = hashlib.sha256((Path(__file__).resolve().parents[1] / "data/paper_trading/phase4c/ledger.json").read_bytes()).hexdigest()
    outcome = synthetic_test(temporary_ledger)
    after = hashlib.sha256((Path(__file__).resolve().parents[1] / "data/paper_trading/phase4c/ledger.json").read_bytes()).hexdigest()
    assert outcome["status"] == "SYNTHETIC_TEST_PASS"
    assert outcome["live_orders"] == outcome["broker_calls"] == 0
    assert before == after
    temporary_ledger.unlink(missing_ok=True)
    Path(str(temporary_ledger) + ".lock").unlink(missing_ok=True)


def test_phase4d_adapter_delegates_to_existing_replay_only(tmp_path: Path, monkeypatch):
    from nse_research.intraday import phase4f_adapter
    from nse_research.execution import phase4d

    calls = {}

    class OrchestratorSpy:
        def __init__(self, **kwargs):
            calls["init"] = kwargs

        def run_replay(self, *, trading_date):
            calls["date"] = trading_date
            return {"status": "PHASE4D_SESSION_COMPLETED", "live_orders": 0, "broker_calls": 0}

    class CanonicalProviderSpy:
        def __init__(self, dataset_id, *, project_root):
            calls["provider"] = (dataset_id, Path(project_root))
            self.path = Path("/tmp/test-canonical.csv")

        def available_dates(self):
            return [date(2026, 9, 28)]

    monkeypatch.setattr(phase4f_adapter, "assert_research_ready",
                        lambda *_args, **_kwargs: {"normalized_path": "/tmp/test-canonical.csv"})
    monkeypatch.setattr(phase4f_adapter, "get_registered_dataset", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(phase4f_adapter, "CanonicalIntradayDatasetProvider", CanonicalProviderSpy)
    monkeypatch.setattr(phase4d, "Phase4DOrchestrator", OrchestratorSpy, raising=False)
    ledger = Path(tempfile.gettempdir()) / f"phase4f-adapter-{tmp_path.name}.json"
    adapter = Phase4DReplayAdapter("test-ready-dataset", ledger_path=ledger, project_root=tmp_path)
    result = adapter.run(date(2026, 9, 28))
    assert calls["init"]["data_path"] == Path("/tmp/test-canonical.csv")
    assert calls["init"]["ledger_path"] == ledger.resolve()
    assert calls["init"]["project_root"] == tmp_path.resolve()
    assert calls["date"] == date(2026, 9, 28)
    assert calls["provider"] == ("test-ready-dataset", tmp_path.resolve())
    assert result["live_orders"] == result["broker_calls"] == 0
    assert result["phase4f_dataset_id"] == "test-ready-dataset"


def test_sha256_changes_when_source_changes(tmp_path: Path):
    path = tmp_path / "same.csv"
    path.write_text("a\n", encoding="utf-8")
    first = sha256_file(path)
    assert sha256_file(path) == first
    path.write_text("b\n", encoding="utf-8")
    assert sha256_file(path) != first


def _phase4d_synthetic_research_fixture(tmp_path: Path) -> tuple[Path, str, date, list[str]]:
    """Create a test-only Phase 4F-ready fixture plus Phase 4D universe files."""
    root = tmp_path / "project"
    root.mkdir()
    repository = Path(__file__).resolve().parents[1]
    universe_file = root / "results/phase4a8a_upstox_live_universe_100.csv"
    universe_file.parent.mkdir(parents=True)
    shutil.copyfile(repository / "results/phase4a8a_upstox_live_universe_100.csv", universe_file)
    sectors_file = root / "data/paper_trading/phase3d_personal_universe_v2.csv"
    sectors_file.parent.mkdir(parents=True)
    shutil.copyfile(repository / "data/paper_trading/phase3d_personal_universe_v2.csv", sectors_file)

    with universe_file.open("r", newline="", encoding="utf-8") as stream:
        symbols = [row["symbol"] for row in csv.DictReader(stream)]
    replay_date = date(2026, 9, 28)
    calendar = NseTradingCalendar(repository / "data/paper_trading/nse_cash_holidays_2026.csv")
    sessions = []
    day = date(2026, 5, 1)
    while day <= replay_date:
        if day.weekday() < 5 and day not in calendar.holidays and day not in calendar.special_sessions:
            sessions.append(day)
        day += timedelta(days=1)
    assert sessions[-1] == replay_date
    assert len(sessions) >= 60 and (sessions[-1] - sessions[0]).days >= 90

    def row(symbol: str, stamp: datetime, opening=100, high=101, low=99, close=100) -> dict[str, str]:
        return {
            "timestamp": stamp.isoformat(), "symbol": symbol, "exchange": "NSE", "series": "EQ",
            "timeframe": "1m", "open": str(opening), "high": str(high), "low": str(low),
            "close": str(close), "volume": "100", "volume_kind": "BAR", "status": "CLOSED",
            "source": "SYNTHETIC_TEST_FIXTURE",
        }

    rows: list[dict[str, str]] = []
    for session in sessions:
        for symbol in symbols:
            if session == replay_date and symbol == "RELIANCE":
                for offset in range(60):
                    stamp = datetime.combine(session, time(9, 15), IST) + timedelta(minutes=offset)
                    price = 100 + offset * 0.5
                    if offset == 20:
                        rows.append(row(symbol, stamp, 109.59, 113, 107, 109.59))
                    else:
                        rows.append(row(symbol, stamp, price, price + 1, price - 1, price))
                rows.append(row(symbol, datetime.combine(session, time(15, 29), IST), 129, 130, 128, 129))
            else:
                rows.append(row(symbol, datetime.combine(session, time(15, 29), IST)))

    metadata = _metadata(
        provider="SYNTHETIC_TEST_FIXTURE",
        dataset_name="Phase 4F.1 paper replay integration test only",
        revision="phase4f-1-test-fixture-v1",
        source_url="synthetic-test://phase4f-1-not-market-data",
        publisher="SYNTHETIC_TEST_FIXTURE",
        acquisition_timestamp="2026-09-27T00:00:00+05:30",
        volume_semantics="BAR_SHARE_VOLUME",
        license="TEST_ONLY",
        license_status="TEST_ONLY",
        redistribution_terms="TEST_ONLY",
        research_allowed=True,
        expected_symbols=symbols,
        expected_sessions=[item.isoformat() for item in sessions],
        adjustment_status="RAW",
        adjustment_status_documentation="test fixture explicitly declares a common raw basis",
    )
    dataset_id, _, _ = _register(root, rows, metadata=metadata, name="phase4f_test_fixture.csv")
    validation = validate_dataset(dataset_id, project_root=root)
    assert validation["result"]["phase4e_validation_status"] == "PASS"
    assert validation["result"]["research_ready"] is True
    normalize_dataset(dataset_id, project_root=root)
    assert_research_ready(dataset_id, project_root=root)
    return root, dataset_id, replay_date, symbols


def test_phase4d_replay_boundary_blocks_unknown_nonready_and_persistent_ledger(tmp_path: Path):
    root = tmp_path / "project"
    temporary_ledger = tmp_path / "unknown" / "ledger.json"
    with pytest.raises(Phase4FError, match="PHASE4F_DATASET_NOT_REGISTERED"):
        Phase4DReplayAdapter("not-registered", ledger_path=temporary_ledger, project_root=root)
    assert not temporary_ledger.exists()

    dataset_id, _, _ = _register(root)
    blocked_ledger = tmp_path / "nonready" / "ledger.json"
    with pytest.raises(Phase4FError, match="PHASE4F_DATA_NOT_RESEARCH_READY"):
        Phase4DReplayAdapter(dataset_id, ledger_path=blocked_ledger, project_root=root)
    assert not blocked_ledger.exists()

    persistent_ledger = root / "data/paper_trading/phase4c/ledger.json"
    with pytest.raises(Phase4FError, match="PHASE4D_PERSISTENT_LEDGER_REJECTED_FOR_REPLAY"):
        Phase4DReplayAdapter(dataset_id, ledger_path=persistent_ledger, project_root=root)
    assert not persistent_ledger.exists()


def test_phase4d_replay_cli_real_entry_points_fail_closed(tmp_path: Path):
    repository = Path(__file__).resolve().parents[1]
    environment = dict(__import__("os").environ)
    environment["PYTHONPATH"] = str(repository / "src")
    ledger = tmp_path / "blocked-ledger.json"

    def run(script: str, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(repository / "scripts" / script), *args],
            cwd=repository, env=environment, capture_output=True, text=True, check=False,
        )

    help_result = run("replay_intraday_session.py", "--help")
    assert help_result.returncode == 0
    assert "--dataset" in help_result.stdout and "--ledger" in help_result.stdout

    no_ledger = run("replay_intraday_session.py", "--dataset", "missing-probe", "--date", "2026-09-28")
    assert no_ledger.returncode == 2
    assert "PHASE4D_TEMPORARY_LEDGER_REQUIRED" in no_ledger.stderr

    unknown = run("replay_intraday_session.py", "--dataset", "phase4f-unregistered-cli-probe",
                  "--date", "2026-09-28", "--ledger", str(ledger))
    assert unknown.returncode == 2
    assert "PHASE4F_DATASET_NOT_REGISTERED" in unknown.stderr
    assert not ledger.exists()

    arbitrary = run("run_phase4d_intraday_paper.py", "--mode", "REPLAY", "--date", "2026-09-28",
                    "--data", "/tmp/random.csv", "--ledger", str(tmp_path / "arbitrary-ledger.json"))
    assert arbitrary.returncode == 2
    assert "REPLAY_DATA_MUST_BE_PHASE4F_REGISTERED" in arbitrary.stderr
    assert not (tmp_path / "arbitrary-ledger.json").exists()

    persistent = repository / "data/paper_trading/phase4c/ledger.json"
    ledger_before = sha256_file(persistent)
    rejected_persistent = run("replay_intraday_session.py", "--dataset", "phase4f-unregistered-persistent-probe",
                              "--date", "2026-09-28", "--ledger", str(persistent))
    assert rejected_persistent.returncode == 2
    assert "PHASE4D_PERSISTENT_LEDGER_REJECTED_FOR_REPLAY" in rejected_persistent.stderr
    assert sha256_file(persistent) == ledger_before


def test_phase4d_replay_requires_observed_phase4f_session_before_execution(tmp_path: Path):
    root, dataset_id, replay_date, _ = _phase4d_synthetic_research_fixture(tmp_path)
    ledger = tmp_path / "session-check" / "ledger.json"
    adapter = Phase4DReplayAdapter(dataset_id, ledger_path=ledger, project_root=root)
    with pytest.raises(Phase4FError, match="PHASE4F_SESSION_NOT_AVAILABLE"):
        adapter.run(date(2026, 9, 29))
    assert not ledger.exists()


def test_phase4d_phase4f_synthetic_fixture_replay_isolated_and_idempotent(tmp_path: Path, monkeypatch):
    import nse_research.execution.phase4d as phase4d
    from nse_research.execution.phase4c import PaperExecutionEngine
    from nse_research.execution.phase4c_models import Phase4CPaperConfig

    root, dataset_id, replay_date, symbols = _phase4d_synthetic_research_fixture(tmp_path)
    # Phase 4D blocks fixture source labels in production. This override exists
    # only inside this test so the rest of the registered→validated→canonical→
    # scanner→paper path can be exercised with explicitly synthetic test data.
    monkeypatch.setattr(phase4d, "BLOCKED_SOURCE_PARTS", ())
    production_ledger = Path(__file__).resolve().parents[1] / "data/paper_trading/phase4c/ledger.json"
    production_ledger_before = sha256_file(production_ledger)

    def run_once(name: str) -> tuple[dict, dict[str, list[dict[str, str]]]]:
        ledger = tmp_path / name / "ledger.json"
        result = Phase4DReplayAdapter(
            dataset_id, ledger_path=ledger, project_root=root, universe_size=100,
        ).run(replay_date)
        report = Path(result["report_dir"])
        snapshots = {}
        for filename in ("signals.csv", "orders.csv", "fills.csv", "trades.csv"):
            with (report / filename).open("r", newline="", encoding="utf-8") as stream:
                snapshots[filename] = list(csv.DictReader(stream))
        return result, snapshots

    first, first_records = run_once("paper-run-one")
    second, second_records = run_once("paper-run-two")
    assert first["status"] == second["status"] == "PHASE4D_SESSION_COMPLETED"
    assert first["phase4f_dataset_id"] == second["phase4f_dataset_id"] == dataset_id
    assert first["broker_calls"] == second["broker_calls"] == 0
    assert first["live_orders"] == second["live_orders"] == 0
    assert first_records == second_records
    assert len(first_records["signals.csv"]) > 0
    assert len(first_records["orders.csv"]) > 0
    assert len(first_records["fills.csv"]) > 0
    assert len(first_records["trades.csv"]) > 0
    assert sha256_file(production_ledger) == production_ledger_before
    for name in ("paper-run-one", "paper-run-two"):
        ledger = tmp_path / name / "ledger.json"
        engine = PaperExecutionEngine(Phase4CPaperConfig.from_env(explicit_paper_command=True), ledger_path=ledger)
        assert len(engine.all_orders) > 0 and len(engine.all_trades) > 0
    assert len(symbols) == 100
