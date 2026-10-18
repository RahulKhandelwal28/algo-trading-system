from __future__ import annotations

import csv
import hashlib
import json
from datetime import date

import pytest
import requests

from nse_research.forward.acquisition import (
    DailyEodAcquisitionAdapter,
    EodAcquisitionError,
    EodHttpResponse,
    HttpDailyEodSource,
)


DAY = date(2026, 9, 28)
HEADERS = "Date,Symbol,Series,Open Price,High Price,Low Price,Close Price,Total Traded Quantity\n"
VALID_BODY = (
    HEADERS
    + "2026-09-28,AAA,EQ,100,105,98,103,1000\n"
    + "2026-09-28,BBB,EQ,200,205,198,203,250\n"
).encode()


def _index(path, dates=("2026-09-28",)):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["Date", "Open", "High", "Low", "Close"])
        for day in dates:
            writer.writerow([day, 100, 101, 99, 100])
    return path


class FakeSource:
    source_name = "third-party-test-provider"
    url_template = "https://provider.test/daily?date={date}&symbols={symbols}"

    def __init__(self, response=None, error=None):
        self.response = response or EodHttpResponse(
            200, {"Content-Type": "text/csv"}, VALID_BODY,
            "https://provider.test/daily?date=2026-09-28&symbols=AAA%2CBBB", 0,
        )
        self.error = error
        self.calls = 0

    def fetch(self, market_date, symbols):
        self.calls += 1
        if self.error:
            raise self.error
        return self.response


def _adapter(tmp_path, source=None, *, dates=("2026-09-28",)):
    source = source or FakeSource()
    raw = tmp_path / "data" / "paper_trading" / "raw"
    normalized = tmp_path / "data" / "paper_trading" / "normalized"
    manifests = tmp_path / "data" / "paper_trading" / "manifests"
    calendar = tmp_path / "data" / "paper_trading" / "holidays.csv"
    calendar.parent.mkdir(parents=True, exist_ok=True)
    calendar.write_text("Date,Status,Reason\n2026-10-02,HOLIDAY,Gandhi Jayanti\n", encoding="utf-8")
    adapter = DailyEodAcquisitionAdapter(
        source=source,
        official_nifty_path=_index(tmp_path / "data" / "nse_exports" / "index_NSEI.csv", dates),
        trading_calendar_path=calendar,
        raw_dir=raw,
        normalized_dir=normalized,
        intake_manifest_dir=manifests,
        daily_manifest_path=raw / "daily_manifest.jsonl",
    )
    return adapter, source, raw, normalized


def _manifest(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def test_successful_acquisition_retains_raw_hash_normalizes_and_records_frozen_coverage(tmp_path):
    adapter, source, raw, normalized = _adapter(tmp_path)
    result = adapter.acquire(DAY, {"AAA", "BBB"})

    assert result["validation_status"] == "PASS"
    assert result["source_date"] == "2026-09-28"
    assert result["expected_symbols"] == result["received_symbols"] == 2
    assert result["missing_symbols"] == result["unexpected_symbols"] == []
    assert result["raw_rows"] == result["eq_rows"] == 2
    raw_path = raw / result["raw_file"].split("/")[-1]
    assert raw_path.read_bytes() == VALID_BODY
    assert result["sha256"] == hashlib.sha256(VALID_BODY).hexdigest()
    output = normalized / "2026-09-28.csv"
    with output.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    assert [row["symbol"] for row in rows] == ["AAA", "BBB"]
    assert [row["series"] for row in rows] == ["EQ", "EQ"]
    assert result["normalized_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
    manifest = _manifest(raw / "daily_manifest.jsonl")
    assert len(manifest) == 1
    assert manifest[0]["validation_status"] == "PASS"
    assert manifest[0]["source_url"].startswith("https://provider.test/")
    assert manifest[0]["source_url_template"] == source.url_template


def test_explicit_non_eq_rows_are_reported_but_never_enter_normalized_data(tmp_path):
    body = (
        VALID_BODY.decode()
        + "2026-09-28,AAA,BE,100,105,98,103,10\n"
    ).encode()
    response = EodHttpResponse(200, {"Content-Type": "text/csv"}, body, "https://provider.test/data", 0)
    adapter, _, _, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "PASS"
    assert result["eq_rows"] == 2
    assert result["non_eq_rows"] == 1
    assert result["series_values"] == ["BE", "EQ"]
    with (normalized / "2026-09-28.csv").open(newline="", encoding="utf-8") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2
    assert {row["series"] for row in rows} == {"EQ"}


def test_missing_official_index_date_waits_without_request_or_normalized_file(tmp_path):
    adapter, source, raw, normalized = _adapter(tmp_path, dates=("2026-09-25",))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "WAITING_FOR_2026_09_28_EOD_DATA"
    assert source.calls == 0
    assert not normalized.exists()
    assert not list(raw.glob("*.csv"))
    assert result["response_status"] is None and result["acquired_at"] is None


def test_network_failure_is_recorded_without_normalized_data_or_ledger_access(tmp_path):
    adapter, source, raw, normalized = _adapter(
        tmp_path, FakeSource(error=EodAcquisitionError("NETWORK_ERROR", "connection failed", retry_count=2))
    )
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "NETWORK_ERROR"
    assert result["retry_count"] == 2
    assert result["raw_file"] is None
    assert not normalized.exists()
    assert source.calls == 1


def test_http_timeout_retries_with_bounded_backoff_then_succeeds(tmp_path):
    class Response:
        status_code = 200
        headers = {"Content-Type": "text/csv"}
        content = VALID_BODY
        url = "https://provider.test/daily?date=2026-09-28"

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            assert kwargs["allow_redirects"] is False
            assert kwargs["timeout"] == 4
            if self.calls < 3:
                raise requests.Timeout("slow endpoint")
            return Response()

    session = Session()
    waits = []
    source = HttpDailyEodSource(
        "third-party-test-provider",
        "https://provider.test/daily?date={date}&symbols={symbols}",
        session=session,
        timeout_seconds=4,
        retries=2,
        backoff_seconds=0.25,
        minimum_interval_seconds=0,
        sleep=waits.append,
        monotonic=lambda: 0.0,
    )
    adapter, _, _, _ = _adapter(tmp_path, source)
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "PASS"
    assert result["retry_count"] == 2
    assert session.calls == 3
    assert waits == [0.25, 0.5]


def test_source_date_mismatch_keeps_raw_but_does_not_publish_normalized_file(tmp_path):
    body = VALID_BODY.replace(b"2026-09-28", b"2026-09-25")
    response = EodHttpResponse(200, {"Content-Type": "text/csv"}, body, "https://provider.test/data", 0)
    adapter, _, raw, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "SOURCE_DATE_MISMATCH"
    assert result["source_date"] == "2026-09-25"
    assert result["raw_file"] is not None
    assert len(list(raw.glob("*.csv"))) == 1
    assert not normalized.exists()


def test_timezone_aware_eod_timestamp_normalizes_to_nse_trade_date(tmp_path):
    body = (
        HEADERS
        + "2026-09-27T18:30:00Z,AAA,EQ,100,105,98,103,1000\n"
        + "2026-09-27T18:30:00Z,BBB,EQ,200,205,198,203,250\n"
    ).encode()
    response = EodHttpResponse(200, {"Content-Type": "text/csv"}, body, "https://provider.test/data", 0)
    adapter, _, _, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "PASS"
    assert result["source_date"] == "2026-09-28"
    with (normalized / "2026-09-28.csv").open(newline="", encoding="utf-8") as stream:
        assert {row["date"] for row in csv.DictReader(stream)} == {"2026-09-28"}


def test_missing_and_unexpected_symbols_are_reported_and_block_normalization(tmp_path):
    body = (
        HEADERS
        + "2026-09-28,AAA,EQ,100,105,98,103,1000\n"
        + "2026-09-28,OTHER,EQ,200,205,198,203,250\n"
    ).encode()
    response = EodHttpResponse(200, {"Content-Type": "text/csv"}, body, "https://provider.test/data", 0)
    adapter, _, _, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA"
    assert result["missing_symbols"] == ["BBB"]
    assert result["unexpected_symbols"] == ["OTHER"]
    assert result["received_symbols"] == 2
    assert not normalized.exists()


@pytest.mark.parametrize(
    ("records", "duplicate_count", "conflict_count"),
    [
        (["2026-09-28,AAA,EQ,100,105,98,103,1000", "2026-09-28,AAA,EQ,100,105,98,103,1000", "2026-09-28,BBB,EQ,200,205,198,203,250"], 1, 0),
        (["2026-09-28,AAA,EQ,100,105,98,103,1000", "2026-09-28,AAA,EQ,100,106,98,104,1000", "2026-09-28,BBB,EQ,200,205,198,203,250"], 1, 1),
    ],
)
def test_duplicate_records_and_conflicting_duplicate_count_block(tmp_path, records, duplicate_count, conflict_count):
    body = (HEADERS + "\n".join(records) + "\n").encode()
    response = EodHttpResponse(200, {"Content-Type": "text/csv"}, body, "https://provider.test/data", 0)
    adapter, _, _, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "DATA_VALIDATION_FAILED"
    assert result["duplicate_count"] == duplicate_count
    assert result["conflicting_duplicate_count"] == conflict_count
    assert not normalized.exists()


@pytest.mark.parametrize(
    "row",
    [
        "2026-09-28,AAA,EQ,100,95,98,103,1000",
        "2026-09-28,AAA,EQ,0,105,98,103,1000",
        "2026-09-28,AAA,EQ,100,105,98,103,-1",
        "2026-09-28,AAA,EQ,100,105,98,103,not-a-number",
    ],
)
def test_invalid_ohlc_price_or_volume_keeps_raw_and_blocks_normalization(tmp_path, row):
    body = (HEADERS + row + "\n" + "2026-09-28,BBB,EQ,200,205,198,203,250\n").encode()
    response = EodHttpResponse(200, {"Content-Type": "text/csv"}, body, "https://provider.test/data", 0)
    adapter, _, raw, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "DATA_VALIDATION_FAILED"
    assert result["raw_file"] is not None
    assert not normalized.exists()
    assert len(list(raw.glob("*.csv"))) == 1


def test_explicit_series_is_required_and_yahoo_chart_json_is_not_suitable(tmp_path):
    yahoo = json.dumps({"chart": {"result": [{"timestamp": [1790553600], "indicators": {}}]}}).encode()
    response = EodHttpResponse(200, {"Content-Type": "application/json"}, yahoo, "https://query1.finance.yahoo.com/chart", 0)
    adapter, _, raw, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "DATA_SOURCE_NOT_SUITABLE"
    assert "explicit NSE Series" in result["errors"][0]
    assert result["raw_rows"] == 1
    assert result["raw_file"] is not None
    assert list(raw.glob("*.json"))
    assert not normalized.exists()


def test_access_block_is_retained_and_not_bypassed(tmp_path):
    body = b"<html><title>Access Denied</title><body>Request blocked</body></html>"
    response = EodHttpResponse(403, {"Content-Type": "text/html"}, body, "https://provider.test/data", 0)
    adapter, _, raw, normalized = _adapter(tmp_path, FakeSource(response=response))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "SOURCE_BLOCKED_ACCESS_CONTROL"
    assert list(raw.glob("*.html"))
    assert not normalized.exists()


def test_successful_cached_acquisition_is_idempotent_and_detects_universe_change(tmp_path):
    adapter, source, _, normalized = _adapter(tmp_path)
    first = adapter.acquire(DAY, {"AAA", "BBB"})
    second = adapter.acquire(DAY, {"AAA", "BBB"})
    assert first["validation_status"] == second["validation_status"] == "PASS"
    assert second["cache_hit"] is True
    assert source.calls == 1
    assert normalized.joinpath("2026-09-28.csv").is_file()
    changed = adapter.acquire(DAY, {"AAA"})
    assert changed["validation_status"] == "FROZEN_UNIVERSE_MISMATCH"
    assert source.calls == 1


def test_tampered_normalized_cache_is_not_reimported_or_reblessed(tmp_path):
    adapter, source, _, normalized = _adapter(tmp_path)
    first = adapter.acquire(DAY, {"AAA", "BBB"})
    assert first["validation_status"] == "PASS"
    path = normalized / "2026-09-28.csv"
    original = path.read_bytes()
    path.write_bytes(original.replace(b",103,", b",104,"))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "NORMALIZED_DATE_ALREADY_EXISTS_UNVERIFIED"
    assert source.calls == 2
    assert path.read_bytes() != original


def test_retry_after_is_bounded_and_respected_without_redirects(tmp_path):
    class Response:
        def __init__(self, status, headers, content):
            self.status_code, self.headers, self.content = status, headers, content
            self.url = "https://provider.test/data?date=2026-09-28"

    class Session:
        def __init__(self):
            self.calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                return Response(429, {"Retry-After": "2"}, b"rate limited")
            return Response(200, {"Content-Type": "text/csv"}, VALID_BODY)

    waits = []
    session = Session()
    source = HttpDailyEodSource(
        "third-party-test-provider", "https://provider.test/data?date={date}",
        session=session, retries=1, backoff_seconds=1, minimum_interval_seconds=0,
        sleep=waits.append, monotonic=lambda: 0.0,
    )
    adapter, _, _, _ = _adapter(tmp_path, source)
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "PASS"
    assert result["retry_count"] == 1
    assert session.calls == 2
    assert waits == [2.0]


def test_long_retry_after_stops_automatic_retries_instead_of_retrying_early():
    class Response:
        status_code = 429
        headers = {"Retry-After": "120", "Content-Type": "text/plain"}
        content = b"rate limited"
        url = "https://provider.test/data?date=2026-09-28"

    class Session:
        calls = 0

        def get(self, *args, **kwargs):
            self.calls += 1
            return Response()

    session = Session()
    waits = []
    source = HttpDailyEodSource(
        "third-party-test-provider",
        "https://provider.test/data?date={date}",
        session=session,
        retries=3,
        minimum_interval_seconds=0,
        sleep=waits.append,
    )

    response = source.fetch(DAY, {"AAA"})

    assert response.status_code == 429
    assert response.retry_count == 0
    assert session.calls == 1
    assert waits == []


def test_rate_limit_is_persisted_across_adapter_instances(tmp_path):
    class Response:
        status_code = 404
        headers = {"Content-Type": "text/plain"}
        content = b"not found"
        url = "https://provider.test/data?date=2026-09-28"

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    now = [100.0]
    waits = []

    def sleep(delay):
        waits.append(delay)
        now[0] += delay

    kwargs = dict(
        session=Session(), retries=0, minimum_interval_seconds=2,
        rate_limit_state_dir=tmp_path / "rate", sleep=sleep,
        monotonic=lambda: 0.0, wall_time=lambda: now[0],
    )
    url = "https://provider.test/data?date={date}"
    first = HttpDailyEodSource("source", url, **kwargs)
    first.fetch(DAY, {"AAA"})
    second = HttpDailyEodSource("source", url, **kwargs)
    second.fetch(DAY, {"AAA"})
    assert waits == [2.0]
    assert list((tmp_path / "rate").glob("*.json"))


def test_source_urls_reject_credentials_and_non_https_endpoints():
    with pytest.raises(ValueError, match="HTTPS"):
        HttpDailyEodSource("source", "http://provider.test/data?date={date}")
    with pytest.raises(ValueError, match="credentials"):
        HttpDailyEodSource("source", "https://provider.test/data?api_key=secret&date={date}")


def test_http_source_enforces_bounded_timeout_retries_and_backoff():
    url = "https://provider.test/data?date={date}"
    with pytest.raises(ValueError, match="timeout_seconds"):
        HttpDailyEodSource("source", url, timeout_seconds=61)
    with pytest.raises(ValueError, match="retries"):
        HttpDailyEodSource("source", url, retries=6)
    with pytest.raises(ValueError, match="backoff_seconds"):
        HttpDailyEodSource("source", url, backoff_seconds=31)


def test_nonofficial_missing_date_does_not_download_or_fabricate(tmp_path):
    adapter, source, raw, normalized = _adapter(tmp_path, dates=("2026-09-25",))
    result = adapter.acquire(DAY, {"AAA", "BBB"})
    assert result["validation_status"] == "WAITING_FOR_2026_09_28_EOD_DATA"
    assert result["raw_rows"] is None
    assert source.calls == 0
    assert not raw.exists() or not list(raw.glob("*.csv"))
    assert not normalized.exists()


def test_weekend_is_not_treated_as_a_trading_session_or_requested(tmp_path):
    adapter, source, raw, normalized = _adapter(tmp_path, dates=("2026-09-25",))
    result = adapter.acquire(date(2026, 9, 26), {"AAA", "BBB"})
    assert result["validation_status"] == "NON_TRADING_DAY"
    assert source.calls == 0
    assert not normalized.exists()
    assert not list(raw.glob("*.csv"))


def test_known_official_holiday_is_not_treated_as_a_trading_session(tmp_path):
    adapter, source, raw, normalized = _adapter(tmp_path, dates=("2026-09-25",))
    result = adapter.acquire(date(2026, 10, 2), {"AAA", "BBB"})
    assert result["validation_status"] == "NON_TRADING_DAY"
    assert source.calls == 0
    assert not normalized.exists()
    assert not list(raw.glob("*.csv"))
