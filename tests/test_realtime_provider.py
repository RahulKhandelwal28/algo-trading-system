import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from nse_research.realtime import (
    IndianStockMarketAPIProvider,
    QuoteValidity,
    RealtimeProviderSettings,
    create_realtime_provider,
    signal_eligible_quotes,
)


NOW = datetime(2026, 9, 26, 4, 30, tzinfo=timezone.utc)


def response(payload, status=200):
    return SimpleNamespace(status=status, body=json.dumps(payload).encode())


def quote_payload(
    *, symbol="RELIANCE", exchange="NSE", price="100.5", quote_timestamp=None,
):
    data = {
        "last_price": price,
        "change": "0.5",
        "percent_change": "0.5",
        "volume": "123456",
        "market_cap": None,
        "last_update": "2026-09-26",
    }
    if quote_timestamp is not None:
        data["quote_timestamp"] = quote_timestamp
    return {"status": "success", "symbol": symbol, "exchange": exchange, "data": data}


class FakeTransport:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []
        self.monotonic = 100.0
        self.sleeps = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        self.monotonic += 0.01
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.monotonic += seconds

    def make_provider(self, **kwargs):
        return IndianStockMarketAPIProvider(
            "https://quotes.example.test",
            http_get=self.get,
            clock=lambda: self.monotonic,
            sleeper=self.sleep,
            utcnow=lambda: NOW,
            request_interval_seconds=0,
            **kwargs,
        )


def test_single_quote_normalizes_fields_and_rejects_server_timestamp_as_quote_time():
    transport = FakeTransport([response({
        **quote_payload(),
        "data": {**quote_payload()["data"], "timestamp": "2026-09-26 04:30:00"},
    })])
    provider = transport.make_provider()

    quote = provider.get_quote("reliance.ns")

    assert quote.symbol == "RELIANCE"
    assert quote.exchange == "NSE"
    assert quote.last_price == 100.5
    assert quote.change == 0.5
    assert quote.change_percent == 0.5
    assert quote.volume == 123456
    assert quote.market_cap is None
    assert quote.timestamp is None
    assert quote.api_timestamp == NOW
    assert quote.source_quote_date == date(2026, 9, 26)
    assert quote.validity_status == QuoteValidity.UNVERIFIABLE_TIMESTAMP
    assert quote.data_age_seconds is None
    assert parse_qs(urlsplit(transport.calls[0][0]).query)["symbol"] == ["RELIANCE.NS"]


def test_exact_quote_timestamp_is_parsed_and_fresh_quote_is_eligible():
    transport = FakeTransport([response(quote_payload(quote_timestamp="2026-09-26T09:59:30+05:30"))])
    provider = transport.make_provider(max_quote_age_seconds=60)

    quote = provider.get_quote("RELIANCE")

    assert quote.timestamp == NOW - timedelta(seconds=30)
    assert quote.data_age_seconds == 30
    assert quote.validity_status == QuoteValidity.VALID
    assert signal_eligible_quotes([quote]) == [quote]


def test_stale_quote_is_blocked_from_strategy_inputs():
    stamp = (NOW - timedelta(seconds=121)).isoformat()
    transport = FakeTransport([response(quote_payload(quote_timestamp=stamp))])
    provider = transport.make_provider(max_quote_age_seconds=120)

    quote = provider.get_quote("RELIANCE")

    assert quote.validity_status == QuoteValidity.STALE_DATA
    assert signal_eligible_quotes([quote]) == []


@pytest.mark.parametrize(
    ("price", "expected_status"),
    [("0", QuoteValidity.INVALID_PRICE), ("-1", QuoteValidity.INVALID_PRICE), ("NaN", QuoteValidity.INVALID_NUMERIC)],
)
def test_bad_price_is_rejected(price, expected_status):
    transport = FakeTransport([response(quote_payload(price=price, quote_timestamp=NOW.isoformat()))])
    quote = transport.make_provider().get_quote("RELIANCE")
    assert quote.validity_status == expected_status
    assert signal_eligible_quotes([quote]) == []


def test_missing_optional_fields_are_none_but_missing_price_cannot_pass():
    payload = quote_payload(quote_timestamp=NOW.isoformat())
    payload["data"] = {"last_price": "100", "quote_timestamp": NOW.isoformat()}
    transport = FakeTransport([response(payload)])

    quote = transport.make_provider().get_quote("RELIANCE")

    assert quote.last_price == 100
    assert quote.change is None
    assert quote.change_percent is None
    assert quote.volume is None
    assert quote.market_cap is None
    assert quote.validity_status == QuoteValidity.VALID

    payload["data"].pop("last_price")
    provider = FakeTransport([response(payload)]).make_provider()
    assert provider.get_quote("RELIANCE").validity_status == QuoteValidity.INVALID_PRICE


def test_invalid_symbol_response_is_reported_as_no_data_without_fabricating_a_quote():
    transport = FakeTransport([response({"status": "error", "message": "No data found; symbol may not exist or market is closed"}, 404)])
    quote = transport.make_provider().get_quote("NOTREAL")

    assert quote.validity_status == QuoteValidity.NO_DATA
    assert quote.last_price is None
    assert quote.timestamp is None


def test_non_json_not_found_response_is_not_retried_as_a_provider_outage():
    transport = FakeTransport([SimpleNamespace(status=404, body=b"not found")])
    quote = transport.make_provider(max_retries=2).get_quote("NOTREAL")

    assert quote.validity_status == QuoteValidity.NO_DATA
    assert len(transport.calls) == 1


def test_batch_quotes_parse_per_symbol_rows_and_never_use_batch_timestamp_as_quote_time():
    payload = {
        "status": "success",
        "timestamp": "2026-09-26 04:30:00",
        "count": 2,
        "stocks": [
            {"symbol": "RELIANCE", "exchange": "NSE", "ticker": "RELIANCE.NS", "last_price": 100, "change": 1, "percent_change": 1, "volume": 10, "market_cap": 1000000},
            {"symbol": "TCS", "exchange": "NSE", "ticker": "TCS.NS", "error": "No data available"},
        ],
    }
    transport = FakeTransport([response(payload)])
    quotes = transport.make_provider(batch_size=5).get_quotes(["RELIANCE", "TCS"])

    assert len(transport.calls) == 1
    assert [quote.symbol for quote in quotes] == ["RELIANCE", "TCS"]
    assert quotes[0].last_price == 100
    assert quotes[0].api_timestamp == NOW
    assert quotes[0].timestamp is None
    assert quotes[0].validity_status == QuoteValidity.UNVERIFIABLE_TIMESTAMP
    assert quotes[1].validity_status == QuoteValidity.NO_DATA
    assert signal_eligible_quotes(quotes) == []
    query = parse_qs(urlsplit(transport.calls[0][0]).query)
    assert query["symbols"] == ["RELIANCE.NS,TCS.NS"]


def test_batch_size_splits_large_universe_into_serial_calls():
    batch = lambda symbol: {"status": "success", "timestamp": NOW.isoformat(), "stocks": [
        {"symbol": symbol, "exchange": "NSE", "ticker": f"{symbol}.NS", "last_price": 10}
    ]}
    transport = FakeTransport([response(batch("A")), response(batch("B")), response(batch("C"))])
    provider = transport.make_provider(batch_size=1)

    quotes = provider.get_quotes(["A", "B", "C"])

    assert len(quotes) == 3
    assert len(transport.calls) == 3
    assert all(quote.validity_status == QuoteValidity.UNVERIFIABLE_TIMESTAMP for quote in quotes)


def test_quote_identity_and_exchange_must_match_the_request():
    wrong_symbol = FakeTransport([response(quote_payload(symbol="TCS", quote_timestamp=NOW.isoformat()))])
    assert wrong_symbol.make_provider().get_quote("RELIANCE").validity_status == QuoteValidity.WRONG_SYMBOL

    wrong_exchange = FakeTransport([response(quote_payload(exchange="BSE", quote_timestamp=NOW.isoformat()))])
    quote = wrong_exchange.make_provider().get_quote("RELIANCE")
    assert quote.validity_status == QuoteValidity.WRONG_EXCHANGE


def test_timeout_retries_with_bounded_exponential_backoff_and_then_recovers():
    transport = FakeTransport([TimeoutError("timed out"), response(quote_payload(quote_timestamp=NOW.isoformat()))])
    provider = transport.make_provider(max_retries=1)

    quote = provider.get_quote("RELIANCE")

    assert quote.validity_status == QuoteValidity.VALID
    assert len(transport.calls) == 2
    assert transport.sleeps == [0.25]
    assert provider.health().consecutive_failures == 0
    assert provider.health().last_successful_request == NOW


def test_failure_retries_then_opens_circuit_and_fails_closed():
    transport = FakeTransport([TimeoutError("offline"), TimeoutError("offline"), TimeoutError("offline")])
    provider = transport.make_provider(max_retries=1, circuit_failure_threshold=1, circuit_cooldown_seconds=30)

    failed = provider.get_quote("RELIANCE")
    blocked = provider.get_quote("TCS")

    assert failed.validity_status == QuoteValidity.PROVIDER_ERROR
    assert blocked.validity_status == QuoteValidity.PROVIDER_ERROR
    assert len(transport.calls) == 2  # The open circuit prevents another HTTP call.
    health = provider.health()
    assert health.provider_status == "CIRCUIT_OPEN"
    assert health.consecutive_failures == 1
    assert health.symbols_failed == ("TCS",)
    assert health.symbols_successfully_retrieved == ()
    assert health.circuit_open_until == NOW + timedelta(seconds=30)


def test_health_counts_stale_and_invalid_quotes_and_request_latency():
    stale = quote_payload(quote_timestamp=(NOW - timedelta(seconds=5)).isoformat())
    bad = quote_payload(symbol="TCS", price="NaN", quote_timestamp=NOW.isoformat())
    transport = FakeTransport([response({"status": "success", "timestamp": NOW.isoformat(), "stocks": [
        {"symbol": "RELIANCE", "exchange": "NSE", "ticker": "RELIANCE.NS", **stale["data"]},
        {"symbol": "TCS", "exchange": "NSE", "ticker": "TCS.NS", **bad["data"]},
    ]})])
    provider = transport.make_provider(max_quote_age_seconds=1)

    quotes = provider.get_quotes(["RELIANCE", "TCS"])
    health = provider.health()

    assert [q.validity_status for q in quotes] == [QuoteValidity.STALE_DATA, QuoteValidity.INVALID_NUMERIC]
    assert health.provider_status == "DEGRADED"
    assert health.stale_quote_count == 1
    assert health.invalid_quote_count == 1
    assert health.symbols_successfully_retrieved == ()
    assert health.symbols_failed == ("RELIANCE", "TCS")
    assert health.last_request_latency_ms == pytest.approx(10)
    assert health.average_request_latency_ms == pytest.approx(10)


def test_unit_wrapped_numeric_fields_are_not_silently_misread():
    payload = quote_payload(quote_timestamp=NOW.isoformat())
    payload["data"]["market_cap"] = {"value": 100, "unit": "Crores INR"}
    quote = FakeTransport([response(payload)]).make_provider().get_quote("RELIANCE")
    assert quote.validity_status == QuoteValidity.INVALID_NUMERIC
    assert quote.market_cap is None


def test_provider_is_disabled_by_default_and_validates_configuration():
    assert RealtimeProviderSettings.from_env({}).provider == "disabled"
    assert create_realtime_provider({}) is None
    with pytest.raises(ValueError, match="HTTPS"):
        RealtimeProviderSettings.from_env({
            "REALTIME_PROVIDER": "indian_stock_market_api",
            "REALTIME_API_BASE_URL": "http://quotes.example.test",
        })
    with pytest.raises(ValueError, match="required"):
        RealtimeProviderSettings.from_env({"REALTIME_PROVIDER": "indian_stock_market_api"})
