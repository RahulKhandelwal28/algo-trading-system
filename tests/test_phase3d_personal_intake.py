from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from nse_research.forward.data import PersonalPaperEodProvider
from nse_research.forward.intake import DAILY_COLUMNS, import_daily_eod


def _write_source(path: Path, rows: list[list[str]], *, spaced_headers: bool = False) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [f" {name} " for name in DAILY_COLUMNS] if spaced_headers else list(DAILY_COLUMNS)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
    return path.read_bytes()


def _valid_rows(day: str = "2026-09-28") -> list[list[str]]:
    return [
        [day, "AAA", "EQ", "100", "105", "98", "103", "1,000"],
        [day, "BBB.NS", "EQ", "200", "205", "198", "203", "250"],
    ]


def _import(tmp_path: Path, rows: list[list[str]], *, spaced_headers: bool = False):
    source = tmp_path / "incoming" / "2026-09-28.csv"
    original = _write_source(source, rows, spaced_headers=spaced_headers)
    result = import_daily_eod(
        source_path=source,
        expected_symbols={"AAA", "BBB"},
        normalized_dir=tmp_path / "normalized",
        manifest_dir=tmp_path / "manifests",
        source_provider="user-supplied local file",
        source_identifier="sample-1",
        adjustment_status="unadjusted as documented by source",
    )
    return source, original, result


def test_valid_daily_file_normalizes_and_records_provenance_without_changing_input(tmp_path):
    source, original, result = _import(tmp_path, _valid_rows(), spaced_headers=True)
    assert result["validation_status"] == "PASS"
    assert result["normalization_status"] == "WRITTEN"
    assert source.read_bytes() == original
    normalized = tmp_path / "normalized" / "2026-09-28.csv"
    rows = list(csv.DictReader(normalized.open(newline="", encoding="utf-8")))
    assert [row["symbol"] for row in rows] == ["AAA", "BBB"]
    assert rows[0]["volume"] == "1000"
    assert result["source_sha256"] == hashlib.sha256(original).hexdigest()
    assert result["source_provider"] == "user-supplied local file"
    assert result["source_identifier"] == "sample-1"
    assert result["adjustment_status"] == "unadjusted as documented by source"


def test_same_source_is_idempotent_but_different_source_cannot_overwrite_normalized_date(tmp_path):
    source, _, first = _import(tmp_path, _valid_rows())
    second = import_daily_eod(
        source_path=source,
        expected_symbols={"AAA", "BBB"},
        normalized_dir=tmp_path / "normalized",
        manifest_dir=tmp_path / "manifests",
    )
    assert second["validation_status"] == "PASS"
    assert second["normalization_status"] == "ALREADY_IMPORTED_IDENTICAL_SOURCE"
    before = (tmp_path / "normalized" / "2026-09-28.csv").read_bytes()
    source.write_text(source.read_text().replace(",103,", ",104,"), encoding="utf-8")
    third = import_daily_eod(
        source_path=source,
        expected_symbols={"AAA", "BBB"},
        normalized_dir=tmp_path / "normalized",
        manifest_dir=tmp_path / "manifests",
    )
    assert third["validation_status"] == "FAIL"
    assert "refusing overwrite" in third["errors"][-1]
    assert (tmp_path / "normalized" / "2026-09-28.csv").read_bytes() == before
    assert first["source_sha256"] != third["source_sha256"]


def test_non_eq_series_is_never_fabricated_and_missing_eq_symbol_blocks(tmp_path):
    rows = _valid_rows()
    rows[1][2] = "BE"
    _, _, result = _import(tmp_path, rows)
    assert result["validation_status"] == "FAIL"
    assert result["EQ_row_count"] == 1
    assert result["non_EQ_row_count"] == 1
    assert result["missing_symbols"] == ["BBB"]
    assert not (tmp_path / "normalized" / "2026-09-28.csv").exists()


def test_duplicate_symbol_date_rows_are_rejected_not_deduplicated(tmp_path):
    rows = _valid_rows()
    rows.append(rows[0].copy())
    _, _, result = _import(tmp_path, rows)
    assert result["validation_status"] == "FAIL"
    assert result["duplicate_key_count"] == 1
    assert result["duplicate_row_count"] == 1
    assert not (tmp_path / "normalized" / "2026-09-28.csv").exists()


@pytest.mark.parametrize(
    ("rows", "expected_error"),
    [
        ([ ["28/09/2026", "AAA", "EQ", "100", "105", "98", "103", "1000"], _valid_rows()[1] ], "malformed date"),
        ([ ["2026-09-28", "AAA", "EQ", "100", "95", "98", "103", "1000"], _valid_rows()[1] ], "OHLC"),
        ([ ["2026-09-28", "AAA", "EQ", "bad", "105", "98", "103", "1000"], _valid_rows()[1] ], "malformed"),
        ([ ["2026-09-28", "AAA", "EQ", "0", "105", "98", "103", "1000"], _valid_rows()[1] ], "positive"),
        ([ ["2026-09-28", "AAA", "EQ", "100", "105", "98", "103", ""], _valid_rows()[1] ], "empty"),
        ([ ["2026-09-28", "AAA", "EQ", "100", "105", "98", "103", "1,2"], _valid_rows()[1] ], "thousands separators"),
    ],
)
def test_malformed_date_numeric_and_ohlc_rows_fail_closed(tmp_path, rows, expected_error):
    _, _, result = _import(tmp_path, rows)
    assert result["validation_status"] == "FAIL"
    assert any(expected_error.lower() in error.lower() for error in result["errors"])
    assert not (tmp_path / "normalized" / "2026-09-28.csv").exists()


def test_unexpected_symbols_and_incomplete_coverage_block_import(tmp_path):
    rows = [_valid_rows()[0], ["2026-09-28", "OTHER", "EQ", "10", "11", "9", "10", "1"]]
    _, _, result = _import(tmp_path, rows)
    assert result["validation_status"] == "FAIL"
    assert result["missing_symbols"] == ["BBB"]
    assert result["unknown_symbols"] == ["OTHER"]
    assert not (tmp_path / "normalized" / "2026-09-28.csv").exists()


def test_source_date_must_match_filename(tmp_path):
    rows = _valid_rows()
    rows[0][0] = rows[1][0] = "2026-09-29"
    _, _, result = _import(tmp_path, rows)
    assert result["validation_status"] == "FAIL"
    assert any("does not match filename" in error for error in result["errors"])
    assert not (tmp_path / "normalized" / "2026-09-28.csv").exists()


def test_personal_provider_requires_passing_manifest_and_matching_normalized_checksum(tmp_path):
    _, _, result = _import(tmp_path, _valid_rows())
    assert result["validation_status"] == "PASS"
    provider = PersonalPaperEodProvider(tmp_path / "cache", tmp_path / "indices", tmp_path / "normalized")
    assert set(provider._load_daily_bars()) == {"AAA", "BBB"}
    normalized = tmp_path / "normalized" / "2026-09-28.csv"
    normalized.write_text(normalized.read_text() + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        PersonalPaperEodProvider(tmp_path / "cache", tmp_path / "indices", tmp_path / "normalized")._load_daily_bars()
