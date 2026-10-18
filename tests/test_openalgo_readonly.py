from __future__ import annotations

import json
from datetime import date, datetime, timezone
from urllib.parse import parse_qs, urlsplit
from zoneinfo import ZoneInfo

import pytest

from nse_research.data.openalgo_readonly import (
    InstrumentIdentity,
    OpenAlgoDataError,
    OpenAlgoReadOnlyClient,
    OpenAlgoSettings,
)


IST = ZoneInfo("Asia/Kolkata")


def _settings(**kwargs) -> OpenAlgoSettings:
    values = {
        "base_url": "http://127.0.0.1:5000",
        "api_key": "test-secret",
        "request_interval_seconds": 0,
    }
    values.update(kwargs)
    return OpenAlgoSettings(**values)


def _json_response(value: object) -> tuple[int, bytes]:
    return 200, json.dumps(value).encode("utf-8")


def _epoch(hour: int, minute: int) -> int:
    return int(datetime(2026, 10, 1, hour, minute, tzinfo=IST).timestamp())


def _bar(timestamp: int) -> dict[str, object]:
    return {"timestamp": timestamp, "open": 100.0, "high": 102.0, "low": 99.0,
            "close": 101.0, "volume": 1234}


def test_settings_require_secure_remote_url_and_allow_loopback_http() -> None:
    _settings().validate()
    with pytest.raises(OpenAlgoDataError, match="HTTPS"):
        _settings(base_url="http://openalgo.example").validate()
    with pytest.raises(OpenAlgoDataError, match="HTTPS"):
        _settings(base_url="https://user:pass@example.com").validate()


def test_search_requires_one_explicit_nse_eq_match() -> None:
    payload = {"status": "success", "data": [
        {"symbol": "RELIANCE", "exchange": "NSE", "instrumenttype": "EQ", "token": "2885"},
    ]}
    client = OpenAlgoReadOnlyClient(_settings(), transport=lambda *_: _json_response(payload))
    identity = client.validate_equity_symbol("reliance")
    assert identity == InstrumentIdentity("RELIANCE", "NSE", "EQ", "2885")


def test_smoke_path_uses_only_search_and_ticker_read_endpoints() -> None:
    responses = iter([
        _json_response({"status": "success", "data": [
            {"symbol": "RELIANCE", "exchange": "NSE", "instrumenttype": "EQ", "token": "2885"},
        ]}),
        _json_response({"status": "success", "data": [_bar(_epoch(9, 15))]}),
    ])
    requests: list[tuple[str, str, bytes | None]] = []

    def transport(method: str, url: str, body: bytes | None, timeout: float):
        requests.append((method, url, body))
        return next(responses)

    client = OpenAlgoReadOnlyClient(
        _settings(), transport=transport,
        clock=lambda: datetime(2026, 10, 2, 12, 0, tzinfo=IST),
    )
    identity = client.validate_equity_symbol("RELIANCE")
    assert client.minute_bars(identity, date(2026, 10, 1))
    assert [method for method, _, _ in requests] == ["POST", "GET"]
    assert urlsplit(requests[0][1]).path.endswith("/api/v1/search")
    assert urlsplit(requests[1][1]).path.endswith("/api/v1/ticker/NSE:RELIANCE")
    assert parse_qs(urlsplit(requests[1][1]).query)["interval"] == ["1m"]
    assert "order" not in " ".join(url for _, url, _ in requests).lower()


@pytest.mark.parametrize("row", [
    {"symbol": "RELIANCE", "exchange": "NSE", "instrumenttype": "", "token": "1"},
    {"symbol": "RELIANCE", "exchange": "BSE", "instrumenttype": "EQ", "token": "1"},
    {"symbol": "RELIANCE", "exchange": "NSE", "instrumenttype": "FUT", "token": "1"},
])
def test_search_rejects_ambiguous_or_non_eq_identity(row: dict[str, str]) -> None:
    client = OpenAlgoReadOnlyClient(
        _settings(), transport=lambda *_: _json_response({"status": "success", "data": [row]}),
    )
    with pytest.raises(OpenAlgoDataError, match="explicit NSE/EQ"):
        client.validate_equity_symbol("RELIANCE")


def test_ticker_bars_normalize_documented_epoch_seconds() -> None:
    client = OpenAlgoReadOnlyClient(
        _settings(),
        transport=lambda *_: _json_response({"status": "success", "data": [_bar(_epoch(9, 15))]}),
        clock=lambda: datetime(2026, 10, 2, 12, 0, tzinfo=IST),
    )
    bars = client.minute_bars(InstrumentIdentity("RELIANCE", "NSE", "EQ", "2885"), date(2026, 10, 1))
    assert len(bars) == 1
    assert bars[0].timestamp == datetime(2026, 10, 1, 9, 15, tzinfo=IST)
    assert (bars[0].exchange, bars[0].series, bars[0].volume) == ("NSE", "EQ", 1234.0)


def test_ticker_rejects_duplicate_timestamps() -> None:
    duplicate = _bar(_epoch(9, 15))
    client = OpenAlgoReadOnlyClient(
        _settings(), transport=lambda *_: _json_response({"status": "success", "data": [duplicate, duplicate]}),
        clock=lambda: datetime(2026, 10, 2, 12, 0, tzinfo=IST),
    )
    with pytest.raises(OpenAlgoDataError, match="duplicate"):
        client.minute_bars(InstrumentIdentity("RELIANCE", "NSE", "EQ", "2885"), date(2026, 10, 1))


def test_ticker_rejects_malformed_ohlc_and_html() -> None:
    bad = _bar(_epoch(9, 15))
    bad["high"] = 98
    client = OpenAlgoReadOnlyClient(
        _settings(), transport=lambda *_: _json_response({"status": "success", "data": [bad]}),
        clock=lambda: datetime(2026, 10, 2, 12, 0, tzinfo=IST),
    )
    with pytest.raises(OpenAlgoDataError, match="OHLC"):
        client.minute_bars(InstrumentIdentity("RELIANCE", "NSE", "EQ", "2885"), date(2026, 10, 1))

    html_client = OpenAlgoReadOnlyClient(_settings(), transport=lambda *_: (200, b"<html>AirTunes</html>"))
    with pytest.raises(OpenAlgoDataError, match="JSON"):
        html_client.validate_equity_symbol("RELIANCE")


def test_http_and_auth_errors_do_not_disclose_api_key() -> None:
    client = OpenAlgoReadOnlyClient(_settings(), transport=lambda *_: (403, b"secret-test-secret"))
    with pytest.raises(OpenAlgoDataError) as caught:
        client.validate_equity_symbol("RELIANCE")
    assert "test-secret" not in str(caught.value)
    assert "apikey" not in str(caught.value).lower()
