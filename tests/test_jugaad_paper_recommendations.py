from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import requests

from nse_research.forward.calendar import NseTradingCalendar
from nse_research.realtime.jugaad import JugaadDataProvider
from nse_research.realtime.paper_recommendations import (
    JugaadPaperRecommendationSession,
    write_recommendation_snapshot,
)
from nse_research.realtime.providers import QuoteValidity


IST = ZoneInfo("Asia/Kolkata")
SESSION_START = datetime(2026, 9, 28, 9, 15, tzinfo=IST)


class FakeNseLive:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def stock_quote(self, symbol, *, market_type, series):
        self.requests.append((symbol, market_type, series))
        return self.response


def quote_response(*, series="EQ"):
    metadata = {"symbol": "RELIANCE"}
    if series is not None:
        metadata["series"] = series
    return {
        "metaData": metadata,
        "priceInfo": {"lastPrice": "101.50", "change": "1.25", "pChange": "1.24"},
        "tradeInfo": {"totalTradedVolume": "1,234"},
        "lastUpdateTime": "28-Sep-2026 09:15:01",
    }


def test_jugaad_provider_requests_explicit_eq_and_normalizes_snapshot_fields():
    client = FakeNseLive(quote_response())
    provider = JugaadDataProvider(nse_live=client, min_request_interval_seconds=0)

    result = provider.get_quote("RELIANCE")

    assert client.requests == [("RELIANCE", "N", "EQ")]
    assert result.validity_status == QuoteValidity.VALID
    assert result.series == "EQ"
    assert result.last_price == 101.5
    assert result.volume == 1234
    assert result.volume_kind == "CUMULATIVE"


def test_jugaad_provider_rejects_response_without_explicit_eq_series():
    provider = JugaadDataProvider(
        nse_live=FakeNseLive(quote_response(series=None)), min_request_interval_seconds=0,
    )

    result = provider.get_quote("RELIANCE")

    assert result.validity_status == QuoteValidity.QUOTE_SERIES_INVALID
    assert result.series is None


def test_jugaad_session_applies_http_timeout_and_restores_package_session_factory(monkeypatch):
    import jugaad_data.nse.live as live_module

    observed = []

    class FakeResponse:
        pass

    class FakeNseLive:
        def __init__(self):
            self.session = live_module.Session()
            self.session.get("https://example.invalid")

    original_session = live_module.Session
    monkeypatch.setattr(live_module, "NSELive", FakeNseLive)

    def fake_request(self, method, url, **kwargs):
        observed.append(kwargs.get("timeout"))
        return FakeResponse()

    monkeypatch.setattr(requests.Session, "request", fake_request)

    client = JugaadDataProvider()._get_client()

    assert isinstance(client, FakeNseLive)
    assert observed == [5.0]
    assert live_module.Session is original_session


def test_closed_snapshot_bars_produce_paper_only_levels_and_never_orders():
    universe = [f"SYM{index:03d}" for index in range(486)]
    calendar = NseTradingCalendar()
    session = JugaadPaperRecommendationSession(
        universe_symbols=universe,
        requested_symbols=[universe[0]],
        timeframes=["1m"],
        calendar=calendar,
    )
    candles = []
    for index in range(30):
        start = SESSION_START + timedelta(minutes=index)
        close = 100.0 + index * 0.25
        open_price = close - 0.1
        candles.append(SimpleNamespace(
            symbol=universe[0], interval="1m", start=start, end=start + timedelta(minutes=1),
            open=open_price, high=close + 0.1, low=open_price - 0.1, close=close,
            volume=None, status="CLOSED", source="jugaad_data_nse_live", tick_count=3,
        ))

    new_ids = session.ingest_closed_candles(candles)
    snapshot = session.snapshot(
        now=SESSION_START + timedelta(minutes=21), fresh_symbols=[universe[0]],
    )

    assert new_ids
    assert snapshot["active_candidate_count"] >= 1
    candidate = next(item for item in snapshot["recommendations"]
                     if item["status"] == "ACTIVE_PAPER_CANDIDATE")
    assert candidate["side"] == "LONG"
    assert candidate["entry_reference_price"] > candidate["stop_loss_reference"] > 0
    assert candidate["target_reference"] > candidate["entry_reference_price"]
    assert candidate["paper_only"] is True
    assert candidate["quantity"] is None
    assert candidate["broker_order"] is None
    assert snapshot["orders_created"] is False
    assert snapshot["ledger_mutated"] is False
    assert candidate["bar_quality"]["construction"] == "APPROXIMATE_LTP_SNAPSHOT_OHLC"
    stale_snapshot = session.snapshot(now=SESSION_START + timedelta(minutes=21), fresh_symbols=[])
    assert next(item for item in stale_snapshot["recommendations"]
                if item["signal_id"] == candidate["signal_id"])["status"] == "WATCH_ONLY_CURRENT_QUOTE_NOT_FRESH"


def test_snapshot_candles_need_multiple_observations_and_stale_setup_is_not_active():
    universe = [f"SYM{index:03d}" for index in range(486)]
    session = JugaadPaperRecommendationSession(
        universe_symbols=universe,
        requested_symbols=[universe[0]],
        timeframes=["1m"],
        calendar=NseTradingCalendar(),
    )
    single_sample = SimpleNamespace(
        symbol=universe[0], interval="1m", start=SESSION_START, open=100, high=100,
        low=100, close=100, volume=None, status="CLOSED", source="jugaad_data_nse_live",
        tick_count=1,
    )

    assert session.ingest_closed_candles([single_sample]) == []
    assert session.snapshot(now=SESSION_START + timedelta(minutes=1), fresh_symbols=[])["quality"][
        "candles_rejected_low_sample_count"
    ] == 1


def test_candle_processing_is_idempotent_and_snapshot_write_is_atomic(tmp_path: Path):
    universe = [f"SYM{index:03d}" for index in range(486)]
    session = JugaadPaperRecommendationSession(
        universe_symbols=universe,
        requested_symbols=[universe[0]],
        timeframes=["1m"],
        calendar=NseTradingCalendar(),
    )
    candle = SimpleNamespace(
        symbol=universe[0], interval="1m", start=SESSION_START, open=100, high=101,
        low=99, close=100.5, volume=None, status="CLOSED", source="jugaad_data_nse_live",
        tick_count=2,
    )
    session.ingest_closed_candles([candle])
    assert session.ingest_closed_candles([candle]) == []
    payload = session.snapshot(now=SESSION_START + timedelta(minutes=2), fresh_symbols=[])

    target = write_recommendation_snapshot(tmp_path / "paper.json", payload)

    assert json.loads(target.read_text())["orders_created"] is False
    assert sorted(path.name for path in tmp_path.iterdir()) == ["paper.json"]
