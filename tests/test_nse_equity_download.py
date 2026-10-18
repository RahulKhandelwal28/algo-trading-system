from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

import nse_research.data.nse_equity_download as module
from nse_research.data.nse_equity_download import (
    EQUITY_SOURCE_URL,
    NseEquityDownloader,
    _date_chunks,
    _normalize_date,
    _parse_source_csv,
    _safe_symbol,
)
from nse_research.data.nse_index_download import HttpResponse


class FakeSession:
    def __init__(self, *responses: HttpResponse | BaseException) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def get(self, url: str, *, headers, timeout: float) -> HttpResponse:
        self.calls.append((url, dict(headers), timeout))
        if not self.responses:
            raise AssertionError(f"unexpected NSE request: {url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _page() -> HttpResponse:
    return HttpResponse(200, {"Content-Type": "text/html; charset=utf-8"}, b"<html>NSE official security history</html>")


def _csv(rows: list[dict[str, str]] | None = None) -> HttpResponse:
    headers = [
        "Symbol ",
        "Date ",
        " Settlement Date ",
        "Series ",
        "Open Price ",
        "High Price ",
        "Low Price ",
        "PREV. CLOSE ",
        "Close Price ",
        "Total Traded Quantity ",
    ]
    data = rows or []
    text = '"' + '","'.join(headers) + '"\n'
    for row in data:
        text += '"' + '","'.join(row.get(name, "") for name in headers) + '"\n'
    return HttpResponse(200, {"Content-Type": "text/csv; charset=utf-8"}, ("\ufeff" + text).encode())


def _row(
    *, symbol: str = "RELIANCE", series: str = "EQ", day: str = "25-SEP-2026",
    open_price: str = "1234.00", high: str = "1240.00", low: str = "1225.00",
    close: str = "1230.00", volume: str = "1,23,456",
) -> dict[str, str]:
    return {
        "Symbol ": symbol,
        "Date ": day,
        " Settlement Date ": "28-SEP-2026",
        "Series ": series,
        "Open Price ": open_price,
        "High Price ": high,
        "Low Price ": low,
        "PREV. CLOSE ": "1220.00",
        "Close Price ": close,
        "Total Traded Quantity ": volume,
    }


def _universe(path: Path, *rows: tuple[str, str] | tuple[str, str, str]) -> Path:
    path.write_text("Company Name,Industry,Symbol,Series,ISIN Code\n" + "".join(
        f"Example Co,Other,{item[0]},{item[1]},{item[2] if len(item) > 2 else 'INE000A01000'}\n"
        for item in rows
    ), encoding="utf-8")
    return path


def _downloader(tmp_path: Path, session: FakeSession, **kwargs) -> NseEquityDownloader:
    universe_path = _universe(tmp_path / "universe.csv", ("RELIANCE", "EQ"), ("OTHER", "EQ"))
    return NseEquityDownloader(
        universe_path,
        tmp_path / "nse_exports",
        report_path=tmp_path / "docs" / "NSE_EQUITY_DATA_REPORT.md",
        session=session,
        min_request_interval_seconds=0,
        backoff_seconds=0,
        sleep=lambda _: None,
        monotonic=lambda: 1.0,
        **kwargs,
    )


def test_successful_equity_response_writes_importer_schema_and_manifest(tmp_path) -> None:
    session = FakeSession(_page(), _csv([_row()]))
    downloader = _downloader(tmp_path, session)

    manifest = downloader.download(date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",))

    output = tmp_path / "nse_exports" / "RELIANCE.csv"
    with output.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    assert reader.fieldnames == list(module.OUTPUT_COLUMNS)
    assert rows == [{
        "Symbol": "RELIANCE",
        "Series": "EQ",
        "Date": "25-Sep-2026",
        "Open Price": "1234.00",
        "High Price": "1240.00",
        "Low Price": "1225.00",
        "Close Price": "1230.00",
        "Total Traded Quantity": "123456",
    }]
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "SUCCESS"
    assert record["actual_first_date"] == record["actual_last_date"] == "2026-09-25"
    assert record["row_count"] == 1
    assert record["chunks_successful"] == 1
    assert record["sha256"]
    assert manifest["aggregate"]["successful_symbols"] == 1
    assert len(session.calls) == 2
    assert session.calls[0][0] == EQUITY_SOURCE_URL
    query = parse_qs(urlparse(session.calls[1][0]).query)
    assert query == {"symbol": ["RELIANCE"], "from": ["25-09-2026"], "to": ["25-09-2026"], "csv": ["true"]}
    assert "Origin" in session.calls[1][1]


def test_date_normalization_and_symbol_normalization() -> None:
    assert _normalize_date("01-Jan-2020") == ("2020-01-01", "01-Jan-2020")
    assert _normalize_date("2020-01-01") == ("2020-01-01", "01-Jan-2020")
    assert _normalize_date("01-01-2020") == ("2020-01-01", "01-Jan-2020")
    assert _safe_symbol("m&m") == "M&M"
    with pytest.raises(ValueError):
        _safe_symbol("../RELIANCE")


def test_non_eq_series_are_filtered_when_eq_rows_are_present(tmp_path) -> None:
    session = FakeSession(_page(), _csv([_row(series="BE"), _row()]))
    manifest = _downloader(tmp_path, session).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "SUCCESS"
    assert record["non_eq_rows_excluded"] == 1
    output = tmp_path / "nse_exports" / "RELIANCE.csv"
    assert len(list(csv.DictReader(output.open(encoding="utf-8")))) == 1


def test_chunking_uses_at_most_requested_inclusive_days(tmp_path) -> None:
    assert _date_chunks(date(2026, 9, 1), date(2026, 9, 6), 3) == [
        (date(2026, 9, 1), date(2026, 9, 3)),
        (date(2026, 9, 4), date(2026, 9, 6)),
    ]
    session = FakeSession(
        _page(),
        _csv([_row(day="01-SEP-2026")]),
        _csv([_row(day="04-SEP-2026")]),
    )
    manifest = _downloader(tmp_path, session, chunk_days=3).download(
        date(2026, 9, 1), date(2026, 9, 6), symbols=("RELIANCE",)
    )
    assert manifest["symbols"]["RELIANCE"]["chunks_successful"] == 2
    urls = [call[0] for call in session.calls if "/api/" in call[0]]
    assert len(urls) == 2
    assert parse_qs(urlparse(urls[0]).query)["from"] == ["01-09-2026"]
    assert parse_qs(urlparse(urls[0]).query)["to"] == ["03-09-2026"]


def test_retries_transient_http_error(tmp_path) -> None:
    session = FakeSession(
        _page(),
        HttpResponse(503, {"Content-Type": "text/plain"}, b"try later"),
        _csv([_row()]),
    )
    manifest = _downloader(tmp_path, session, retries=1).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    assert manifest["symbols"]["RELIANCE"]["status"] == "SUCCESS"
    assert manifest["symbols"]["RELIANCE"]["retry_count"] == 1
    assert len(session.calls) == 3


def test_timeout_is_recorded_without_losing_other_state(tmp_path) -> None:
    session = FakeSession(_page(), TimeoutError("socket timeout"), TimeoutError("socket timeout"))
    manifest = _downloader(tmp_path, session, retries=1).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "FAILED"
    assert record["http_failures"][0]["status"] == "TIMEOUT"
    assert record["http_failures"][0]["retry_count"] == 1


def test_nse_access_error_is_recorded_and_not_bypassed(tmp_path) -> None:
    session = FakeSession(_page(), HttpResponse(403, {"Content-Type": "text/html"}, b"Access Denied"))
    manifest = _downloader(tmp_path, session, retries=2).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "FAILED"
    assert record["http_failures"][0]["status"] == "BLOCKED"
    assert len(session.calls) == 2


def test_malformed_html_or_schema_is_rejected(tmp_path) -> None:
    session = FakeSession(_page(), HttpResponse(200, {"Content-Type": "text/html"}, b"<html>blocked</html>"))
    manifest = _downloader(tmp_path, session).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "FAILED"
    assert "expected CSV" in record["http_failures"][0]["error"]
    assert not (tmp_path / "nse_exports" / "RELIANCE.csv").exists()


def test_duplicate_records_are_counted_and_deduplicated(tmp_path) -> None:
    manifest = _downloader(tmp_path, FakeSession(_page(), _csv([_row(), _row()]))).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["duplicate_rows"] == 1
    assert record["row_count"] == 1
    assert manifest["aggregate"]["duplicate_rows"] == 1


def test_numeric_and_ohlc_validation_flags_but_preserves_source_values(tmp_path) -> None:
    bad = _row(open_price="-1", high="0", low="2", close="4", volume="-3")
    manifest = _downloader(tmp_path, FakeSession(_page(), _csv([bad]))).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["validation"]["invalid_ohlc_rows"] == 1
    assert record["validation"]["nonpositive_price_fields"] == 2
    assert record["validation"]["negative_volume_rows"] == 1
    assert record["validation_status"] == "WARN_DATA_QUALITY"
    saved = next(csv.DictReader((tmp_path / "nse_exports" / "RELIANCE.csv").open(encoding="utf-8")))
    assert saved["Open Price"] == "-1"
    assert saved["Total Traded Quantity"] == "-3"


def test_atomic_write_keeps_old_file_when_replace_fails(tmp_path, monkeypatch) -> None:
    downloader = _downloader(tmp_path, FakeSession())
    path = tmp_path / "nse_exports" / "RELIANCE.csv"
    path.parent.mkdir(parents=True)
    path.write_text("old content\n", encoding="utf-8")
    rows = {"2026-09-25": {
        "Symbol": "RELIANCE", "Series": "EQ", "Date": "25-Sep-2026",
        "Open Price": "1", "High Price": "1", "Low Price": "1", "Close Price": "1",
        "Total Traded Quantity": "1",
    }}
    original_replace = module.os.replace

    def fail_replace(source, target):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(module.os, "replace", fail_replace)
    with pytest.raises(OSError):
        downloader._atomic_write_symbol(path, rows)
    monkeypatch.setattr(module.os, "replace", original_replace)
    assert path.read_text(encoding="utf-8") == "old content\n"
    assert not list(path.parent.glob("*.tmp"))


def test_resume_skips_previously_completed_chunk(tmp_path) -> None:
    universe = _universe(tmp_path / "universe.csv", ("RELIANCE", "EQ"))
    output = tmp_path / "nse_exports"
    first = NseEquityDownloader(
        universe, output, session=FakeSession(_page(), _csv([_row()])),
        min_request_interval_seconds=0, backoff_seconds=0,
    )
    first.download(date(2026, 9, 25), date(2026, 9, 25))
    second_session = FakeSession()
    second = NseEquityDownloader(
        universe, output, session=second_session, min_request_interval_seconds=0,
    )
    manifest = second.download(date(2026, 9, 25), date(2026, 9, 25))
    assert not second_session.calls
    assert manifest["symbols"]["RELIANCE"]["status"] == "SUCCESS"


def test_manifest_and_universe_json_include_provenance(tmp_path) -> None:
    downloader = _downloader(tmp_path, FakeSession(_page(), _csv([_row()])))
    manifest = downloader.download(date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",))
    stored = json.loads((tmp_path / "nse_exports" / "nse_equity_download_manifest.json").read_text())
    universe = json.loads((tmp_path / "nse_exports" / "nse_equity_universe.json").read_text())
    assert stored["endpoint"] == "/api/historicalOR/priceAndVolumeDataPerSecurity"
    assert stored["request_contract"]["series_parameter"] is None
    assert stored["symbols"]["RELIANCE"]["source_endpoint"] == stored["endpoint"]
    assert universe["requested_series"] == "EQ"
    assert next(row for row in universe["symbols"] if row["symbol"] == "RELIANCE")["download_status"] == "SUCCESS"
    assert (tmp_path / "docs" / "NSE_EQUITY_DATA_REPORT.md").exists()
    assert manifest["aggregate"]["total_rows"] == 1


def test_empty_official_csv_marks_symbol_unavailable(tmp_path) -> None:
    manifest = _downloader(tmp_path, FakeSession(_page(), _csv())).download(
        date(2020, 1, 1), date(2020, 12, 30), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "UNAVAILABLE"
    assert record["chunks_successful"] == 1
    assert record["row_count"] == 0


def test_explicit_empty_nse_json_data_is_classified_as_no_data(tmp_path) -> None:
    empty_json = HttpResponse(200, {"Content-Type": "application/json; charset=utf-8"}, b'{"data":[]}')
    manifest = _downloader(tmp_path, FakeSession(_page(), empty_json)).download(
        date(2020, 1, 1), date(2020, 12, 30), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "UNAVAILABLE"
    assert record["chunks_successful"] == 1
    assert not record["http_failures"]


def test_partial_download_retains_successful_chunk(tmp_path) -> None:
    session = FakeSession(
        _page(),
        _csv([_row(day="24-SEP-2026")]),
        TimeoutError("socket timeout"),
        TimeoutError("socket timeout"),
    )
    manifest = _downloader(tmp_path, session, chunk_days=2, retries=1).download(
        date(2026, 9, 24), date(2026, 9, 27), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "PARTIAL"
    assert record["row_count"] == 1
    assert record["chunks_successful"] == 1
    assert record["chunks_failed"] == 1
    assert (tmp_path / "nse_exports" / "RELIANCE.csv").exists()


def test_one_invalid_local_symbol_file_does_not_stop_other_symbols(tmp_path) -> None:
    universe = _universe(tmp_path / "universe.csv", ("RELIANCE", "EQ"), ("OTHER", "EQ"))
    output = tmp_path / "nse_exports"
    output.mkdir()
    invalid_file = output / "RELIANCE.csv"
    invalid_file.write_text("unexpected header\n", encoding="utf-8")
    session = FakeSession(_page(), _csv([_row(symbol="OTHER")]))
    downloader = NseEquityDownloader(
        universe, output, report_path=tmp_path / "docs" / "report.md", session=session,
        min_request_interval_seconds=0, backoff_seconds=0,
    )

    manifest = downloader.download(date(2026, 9, 25), date(2026, 9, 25))

    assert manifest["symbols"]["RELIANCE"]["status"] == "FAILED"
    assert manifest["symbols"]["RELIANCE"]["request_status"] == "LOCAL_FILE_INVALID"
    assert manifest["symbols"]["OTHER"]["status"] == "SUCCESS"
    assert len(session.calls) == 2
    assert invalid_file.read_text(encoding="utf-8") == "unexpected header\n"


def test_only_non_eq_response_is_rejected_as_source_mismatch(tmp_path) -> None:
    mismatch = _row(symbol="RELIANCE", series="XD", open_price="0.40", high="0.40", low="0.40", close="0.40", volume="312")
    session = FakeSession(_page(), _csv([mismatch]))
    manifest = _downloader(tmp_path, session).download(
        date(2026, 9, 25), date(2026, 9, 25), symbols=("RELIANCE",)
    )
    record = manifest["symbols"]["RELIANCE"]
    assert record["status"] == "FAILED"
    assert record["http_failures"][0]["http_status"] == 200
    assert "no EQ rows" in record["http_failures"][0]["error"]
    assert not (tmp_path / "nse_exports" / "RELIANCE.csv").exists()


def test_universe_filters_dummy_and_non_eq_rows(tmp_path) -> None:
    universe = _universe(
        tmp_path / "universe.csv",
        ("RELIANCE", "EQ"),
        ("DUMMYFOO", "EQ"),
        ("OTHER", "BE"),
    )
    selected, excluded = module._read_universe(universe)
    assert [row["symbol"] for row in selected] == ["RELIANCE"]
    assert {row["symbol"] for row in excluded} == {"DUMMYFOO", "OTHER"}
