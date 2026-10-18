from __future__ import annotations

import csv
import json
import socket
from datetime import date
from pathlib import Path

import pytest

import nse_research.data.nse_index_download as module
from nse_research.data.nse_index_download import (
    HttpResponse,
    NseAccessBlocked,
    NseDownloadError,
    NseIndexDownloader,
    NseMalformedResponse,
    NseNoDataResponse,
    _normalize_date,
    _normalize_number,
)


class FakeSession:
    def __init__(self, *responses: HttpResponse | BaseException) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, str], float]] = []

    def get(self, url: str, *, headers, timeout: float) -> HttpResponse:
        self.calls.append((url, dict(headers), timeout))
        if not self.responses:
            raise AssertionError(f"unexpected request: {url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _page() -> HttpResponse:
    return HttpResponse(200, {"Content-Type": "text/html; charset=utf-8"}, b"<html>NSE historical page</html>")


def _api(payload: object, *, status: int = 200, content_type: str = "application/json; charset=utf-8") -> HttpResponse:
    body = json.dumps(payload).encode()
    return HttpResponse(status, {"Content-Type": content_type}, body)


def _nifty_row(day: str = "25-SEP-2026", **overrides) -> dict[str, object]:
    row: dict[str, object] = {
        "EOD_INDEX_NAME": "NIFTY 50",
        "EOD_TIMESTAMP": day,
        "EOD_OPEN_INDEX_VAL": 23035,
        "EOD_HIGH_INDEX_VAL": 23162.7,
        "EOD_LOW_INDEX_VAL": 23020.95,
        "EOD_CLOSE_INDEX_VAL": 23140.5,
        "HIT_TRADED_QTY": 242720711,
        "HIT_TURN_OVER": 18949.25,
    }
    row.update(overrides)
    return row


def _vix_row(day: str = "25-SEP-2026") -> dict[str, object]:
    return {
        "EOD_INDEX_NAME": "INDIA VIX",
        "EOD_TIMESTAMP": day,
        "EOD_OPEN_INDEX_VAL": 12.6875,
        "EOD_HIGH_INDEX_VAL": 12.835,
        "EOD_LOW_INDEX_VAL": 11.77,
        "EOD_CLOSE_INDEX_VAL": 12.16,
        "EOD_PREV_CLOSE": 12.6875,
        "VIX_PTS_CHG": -0.53,
        "VIX_PERC_CHG": -4.16,
    }


def _downloader(tmp_path: Path, session: FakeSession, **kwargs) -> NseIndexDownloader:
    return NseIndexDownloader(
        tmp_path,
        session=session,
        min_request_interval_seconds=0,
        backoff_seconds=0,
        **kwargs,
    )


def test_successful_nifty_response_writes_expected_csv_and_manifest(tmp_path) -> None:
    session = FakeSession(_page(), _api({"data": [_nifty_row()]}))
    downloader = _downloader(tmp_path, session)

    manifest = downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    output = tmp_path / "index_NSEI.csv"
    with output.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0] == {
        "Date": "2026-09-25",
        "Open": "23035",
        "High": "23162.7",
        "Low": "23020.95",
        "Close": "23140.5",
        "Shares Traded": "242720711",
        "Turnover": "18949.25",
    }
    assert list(rows[0]) == ["Date", "Open", "High", "Low", "Close", "Shares Traded", "Turnover"]
    record = manifest["indexes"]["nifty50"]
    assert record["request_status"] == "SUCCESS"
    assert record["validation_status"] == "PASS_WITH_CALENDAR_CAVEAT"
    assert record["actual_first_date"] == record["actual_last_date"] == "2026-09-25"
    assert record["rows"] == 1
    assert record["http_status"] == 200
    assert record["sha256"] == record["checksum_sha256"]
    assert record["sha256"] == module._sha256_file(output)
    assert len(record["completed_chunks"]) == 1
    assert len(session.calls) == 2
    assert "indexType=NIFTY+50" in session.calls[1][0]
    assert "from=25-09-2026" in session.calls[1][0]
    assert "csv=true" in session.calls[1][0]


def test_successful_vix_response_uses_distinct_output_schema(tmp_path) -> None:
    session = FakeSession(_page(), _api({"data": [_vix_row()]}))
    downloader = _downloader(tmp_path, session)

    manifest = downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("indiavix",))

    output = tmp_path / "index_INDIAVIX.csv"
    with output.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        assert reader.fieldnames == [
            "Date", "Open", "High", "Low", "Close", "Prev. Close", "Change", "% Change"
        ]
    assert rows[0]["Date"] == "2026-09-25"
    assert rows[0]["Prev. Close"] == "12.6875"
    assert rows[0]["Change"] == "-0.53"
    assert rows[0]["% Change"] == "-4.16"
    record = manifest["indexes"]["indiavix"]
    assert record["endpoint"] == "/api/historicalOR/vixhistory"
    assert record["request_status"] == "SUCCESS"
    assert "indexType" not in session.calls[1][0]


def test_date_range_is_chunked_with_inclusive_bounded_chunks(tmp_path) -> None:
    session = FakeSession(
        _page(),
        _api({"data": [_nifty_row("21-SEP-2026")]}),
        _api({"data": [_nifty_row("24-SEP-2026", EOD_CLOSE_INDEX_VAL=23141)]}),
    )
    downloader = _downloader(tmp_path, session, chunk_days=3)

    manifest = downloader.download(date(2026, 9, 21), date(2026, 9, 26), ("nifty50",))

    record = manifest["indexes"]["nifty50"]
    chunks = record["completed_chunks"]
    assert [(chunk["start"], chunk["end"]) for chunk in chunks] == [
        ("2026-09-21", "2026-09-23"),
        ("2026-09-24", "2026-09-26"),
    ]
    assert len([call for call in session.calls if "/api/" in call[0]]) == 2
    assert record["rows"] == 2


def test_malformed_response_fails_without_creating_csv_and_records_http_details(tmp_path) -> None:
    malformed = _nifty_row()
    del malformed["EOD_HIGH_INDEX_VAL"]
    session = FakeSession(_page(), _api({"data": [malformed]}))
    downloader = _downloader(tmp_path, session)

    with pytest.raises(NseMalformedResponse):
        downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    assert not (tmp_path / "index_NSEI.csv").exists()
    record = json.loads((tmp_path / "nse_index_download_manifest.json").read_text())["indexes"]["nifty50"]
    assert record["request_status"] == "FAILED"
    assert record["validation_status"] == "FAIL"
    assert record["http_status"] == 200
    assert record["requests"][-1]["status"] == "MALFORMED"


def test_official_null_and_nonpositive_vix_values_are_preserved_and_flagged(tmp_path) -> None:
    row = _vix_row("12-FEB-2021")
    row.update(
        {
            "EOD_OPEN_INDEX_VAL": 0,
            "EOD_HIGH_INDEX_VAL": 0,
            "EOD_LOW_INDEX_VAL": 0,
            "EOD_PREV_CLOSE": 0,
            "VIX_PTS_CHG": 23.05,
            "VIX_PERC_CHG": None,
        }
    )
    session = FakeSession(_page(), _api({"data": [row]}))
    downloader = _downloader(tmp_path, session)

    manifest = downloader.download(date(2021, 2, 12), date(2021, 2, 12), ("indiavix",))

    with (tmp_path / "index_INDIAVIX.csv").open(newline="", encoding="utf-8") as handle:
        stored = next(csv.DictReader(handle))
    assert stored["Open"] == stored["High"] == stored["Low"] == "0"
    assert stored["Prev. Close"] == "0"
    assert stored["% Change"] == ""
    record = manifest["indexes"]["indiavix"]
    assert record["request_status"] == "SUCCESS"
    assert record["validation_status"] == "WARN_DATA_QUALITY"
    assert record["validation"]["invalid_ohlc_rows"] == 1
    assert record["validation"]["nonpositive_price_rows"] == 1
    assert record["validation"]["malformed_numeric_fields"] == 1
    assert record["data_quality_anomalies"] == [
        {
            "date": "2021-02-12",
            "issues": [
                "malformed_numeric:% Change",
                "nonpositive_price:Open",
                "nonpositive_price:High",
                "nonpositive_price:Low",
                "nonpositive_price:Prev. Close",
                "invalid_ohlc",
            ],
        }
    ]


def test_nse_error_response_is_reported_as_blocked_without_retry_or_bypass(tmp_path) -> None:
    blocked = HttpResponse(
        403,
        {"Content-Type": "text/html"},
        b"<html><title>Access Denied</title>CAPTCHA required</html>",
    )
    session = FakeSession(_page(), blocked)
    downloader = _downloader(tmp_path, session, retries=4)

    with pytest.raises(NseAccessBlocked):
        downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    assert len(session.calls) == 2
    record = json.loads((tmp_path / "nse_index_download_manifest.json").read_text())["indexes"]["nifty50"]
    assert record["request_status"] == "BLOCKED"
    assert record["http_status"] == 403
    assert "CAPTCHA" in record["response_excerpt"]


def test_empty_nse_data_array_is_recorded_as_no_data(tmp_path) -> None:
    session = FakeSession(_page(), _api({"data": [], "message": "No records found"}))
    downloader = _downloader(tmp_path, session)

    with pytest.raises(NseNoDataResponse):
        downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    record = json.loads((tmp_path / "nse_index_download_manifest.json").read_text())["indexes"]["nifty50"]
    assert record["request_status"] == "NO_DATA"
    assert record["http_status"] == 200
    assert "No records found" in record["error"]


def test_timeout_is_reported_after_configured_attempts(tmp_path) -> None:
    session = FakeSession(_page(), socket.timeout("slow NSE response"))
    downloader = _downloader(tmp_path, session, retries=0)

    with pytest.raises(NseDownloadError, match="timeout") as exc_info:
        downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    assert getattr(exc_info.value, "request_status") == "TIMEOUT"
    assert len(session.calls) == 2
    record = json.loads((tmp_path / "nse_index_download_manifest.json").read_text())["indexes"]["nifty50"]
    assert record["request_status"] == "TIMEOUT"
    assert record["http_status"] is None


def test_timeout_retry_recovers_and_records_retry_count(tmp_path) -> None:
    delays: list[float] = []
    session = FakeSession(_page(), socket.timeout("transient"), _api({"data": [_nifty_row()]}))
    downloader = _downloader(tmp_path, session, retries=1, sleep=delays.append)

    manifest = downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    record = manifest["indexes"]["nifty50"]
    assert record["retry_count"] == 1
    assert record["requests"][-1]["retry_count"] == 1
    assert delays == [0]
    assert record["request_status"] == "SUCCESS"


def test_duplicate_dates_are_counted_and_output_has_one_row(tmp_path) -> None:
    session = FakeSession(_page(), _api({"data": [_nifty_row(), _nifty_row(EOD_CLOSE_INDEX_VAL=23140.5)]}))
    downloader = _downloader(tmp_path, session)

    manifest = downloader.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    record = manifest["indexes"]["nifty50"]
    assert record["duplicates_encountered"] == 1
    assert record["rows"] == 1
    with (tmp_path / "index_NSEI.csv").open(newline="", encoding="utf-8") as handle:
        assert len(list(csv.DictReader(handle))) == 1


def test_nse_date_formats_normalize_to_iso() -> None:
    assert _normalize_date("25-SEP-2026") == "2026-09-25"
    assert _normalize_date("2026-09-25") == "2026-09-25"
    assert _normalize_date("25-09-2026") == "2026-09-25"


def test_numeric_values_remove_grouping_commas_without_rounding() -> None:
    assert _normalize_number("242,720,711", "Shares Traded") == "242720711"
    assert _normalize_number("18,949.2500", "Turnover") == "18949.2500"
    assert _normalize_number(-0.53, "Change") == "-0.53"


def test_atomic_csv_write_preserves_previous_file_when_replace_fails(tmp_path, monkeypatch) -> None:
    output = tmp_path / "index_NSEI.csv"
    original = b"Date,Open,High,Low,Close,Shares Traded,Turnover\noriginal\n"
    output.write_bytes(original)
    downloader = _downloader(tmp_path, FakeSession())

    def fail_replace(*args, **kwargs):
        raise OSError("simulated atomic replace failure")

    monkeypatch.setattr(module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        downloader._atomic_write_csv(
            output,
            module.INDEX_DEFINITIONS["nifty50"].output_columns,
            {"2026-09-25": {"Date": "2026-09-25", "Open": "1", "High": "2", "Low": "1", "Close": "2", "Shares Traded": "5", "Turnover": "10"}},
        )

    assert output.read_bytes() == original
    assert list(tmp_path.glob(".index_NSEI.csv.*.tmp")) == []


def test_rerun_skips_verified_completed_chunks_and_keeps_manifest(tmp_path) -> None:
    first_session = FakeSession(_page(), _api({"data": [_nifty_row()]}))
    first = _downloader(tmp_path, first_session)
    first.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    second_session = FakeSession()
    second = _downloader(tmp_path, second_session)
    manifest = second.download(date(2026, 9, 25), date(2026, 9, 26), ("nifty50",))

    assert not second_session.calls
    record = manifest["indexes"]["nifty50"]
    assert record["request_status"] == "SUCCESS_CACHED"
    assert record["http_status"] == 200
    assert record["rows"] == 1
    assert record["checksum_sha256"] == record["sha256"]
