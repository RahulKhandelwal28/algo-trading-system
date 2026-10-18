from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nse_research.config import AppConfig, RealtimeSettings
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.realtime.engine import (
    FeedConfigurationError,
    IntradayCandleBuilder,
    RealtimeFeedMonitor,
    TickRecorder,
    configured_provider,
    initial_feed_status,
    validate_quote,
)
from nse_research.realtime.providers import NormalizedQuote, QuoteValidity


IST = ZoneInfo("Asia/Kolkata")
SESSION_DATE = "2026-09-28"


def stamp(value: str) -> datetime:
    return datetime.fromisoformat(f"{SESSION_DATE}T{value}+05:30")


def quote(
    symbol: str = "RELIANCE",
    price: float = 100.0,
    *,
    source_time: datetime | None = None,
    received_at: datetime | None = None,
    series: str | None = "EQ",
    exchange: str = "NSE",
    volume: float | None = 1.0,
    volume_kind: str | None = "INCREMENTAL",
    tick_id: str | None = None,
    status: str = QuoteValidity.VALID,
) -> NormalizedQuote:
    source_time = source_time or stamp("09:15:01")
    received_at = received_at or source_time
    return NormalizedQuote(
        symbol=symbol,
        exchange=exchange,
        last_price=price,
        change=None,
        change_percent=None,
        volume=volume,
        market_cap=None,
        timestamp=source_time,
        source="deterministic_test_fixture",
        received_at=received_at,
        data_age_seconds=max(0, (received_at - source_time).total_seconds()),
        validity_status=status,
        series=series,
        volume_kind=volume_kind,
        tick_id=tick_id,
    )


def validate(item: NormalizedQuote, *, now: datetime, max_age: float = 60.0):
    return validate_quote(item, item.symbol, now=now, max_quote_age_seconds=max_age)


def builder(intervals=("1m", "5m"), calendar: NseTradingCalendar | None = None):
    return IntradayCandleBuilder(intervals, calendar=calendar or NseTradingCalendar())


def test_valid_explicit_eq_quote_is_accepted_and_source_and_received_times_are_distinct():
    item = quote(source_time=stamp("09:15:01"), received_at=stamp("09:15:01.350"))
    result = validate(item, now=stamp("09:15:02"))
    assert result.accepted is True
    assert item.source_timestamp == stamp("09:15:01")
    assert item.received_at.microsecond == 350000
    assert result.quote_age_seconds == 1.0


@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf")])
def test_invalid_or_nonfinite_prices_are_rejected(price):
    result = validate(quote(price=price), now=stamp("09:15:01"))
    assert result.accepted is False
    assert result.code == QuoteValidity.QUOTE_PRICE_INVALID


def test_naive_source_timestamp_is_rejected():
    item = quote(source_time=datetime(2026, 9, 28, 9, 15, 1))
    result = validate(item, now=stamp("09:15:02"))
    assert result.code == QuoteValidity.QUOTE_TIMESTAMP_INVALID


def test_stale_quote_is_rejected_using_configured_threshold():
    item = quote(source_time=stamp("09:15:00"), received_at=stamp("09:15:09"))
    result = validate(item, now=stamp("09:15:09"), max_age=5)
    assert result.accepted is False
    assert result.code == QuoteValidity.QUOTE_STALE
    assert result.quote_age_seconds == 9


@pytest.mark.parametrize("series", ["BE", "BZ", None, ""])
def test_non_eq_or_missing_series_is_rejected(series):
    result = validate(quote(series=series), now=stamp("09:15:01"))
    assert result.code == QuoteValidity.QUOTE_SERIES_INVALID


def test_malformed_or_wrong_symbol_is_rejected():
    item = quote(symbol="BAD SYMBOL")
    assert validate(item, now=stamp("09:15:01")).code == QuoteValidity.QUOTE_SYMBOL_INVALID
    wrong = validate_quote(quote(symbol="TCS"), "RELIANCE", now=stamp("09:15:01"),
                           max_quote_age_seconds=60)
    assert wrong.code == QuoteValidity.QUOTE_SYMBOL_INVALID


def test_unreasonably_future_source_timestamp_is_rejected():
    now = stamp("09:15:01")
    item = quote(source_time=now + timedelta(seconds=3), received_at=now)
    result = validate(item, now=now)
    assert result.code == QuoteValidity.QUOTE_TIMESTAMP_INVALID


@pytest.mark.parametrize("volume", [-1, float("nan"), float("inf")])
def test_negative_or_nonfinite_volume_is_rejected(volume):
    item = quote(volume=volume)
    result = validate(item, now=stamp("09:15:01"))
    assert result.accepted is False
    assert result.code == "QUOTE_VOLUME_INVALID"


class FakeProvider:
    provider_name = "deterministic_fake"

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get_quote(self, symbol, exchange="NSE"):
        raise NotImplementedError

    def get_quotes(self, symbols):
        self.calls += 1
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def health(self):
        return {}


def monitor_settings(**overrides):
    values = {"enabled": True, "provider": "deterministic_fake", "max_quote_age_seconds": 60,
              "max_future_skew_seconds": 1, "max_retries": 0}
    values.update(overrides)
    return RealtimeSettings(**values)


def test_duplicate_tick_identity_is_rejected():
    engine = builder(("1m",))
    first = quote(tick_id="trade-1", source_time=stamp("09:15:05"))
    second = quote(price=101, tick_id="trade-1", source_time=stamp("09:15:10"))
    engine.add_quote(first, validate(first, now=stamp("09:15:05")), now=stamp("09:15:05"))
    events = engine.add_quote(second, validate(second, now=stamp("09:15:10")), now=stamp("09:15:10"))
    assert any(event.event_type == "DUPLICATE_TICK_REJECTED" for event in events)
    assert engine.candles()[0].tick_count == 1
    assert engine.candles()[0].close == 100


def test_deterministic_out_of_order_quotes_recalculate_active_candle():
    engine = builder(("1m",))
    later = quote(price=100, source_time=stamp("09:15:20"), received_at=stamp("09:15:21"))
    earlier = quote(price=99, source_time=stamp("09:15:10"), received_at=stamp("09:15:30"))
    engine.add_quote(later, validate(later, now=stamp("09:15:21")), now=stamp("09:15:21"))
    events = engine.add_quote(earlier, validate(earlier, now=stamp("09:15:30")), now=stamp("09:15:30"))
    candle = engine.candles()[0]
    assert any(event.event_type == "OUT_OF_ORDER_TICK_ACCEPTED" for event in events)
    assert (candle.open, candle.high, candle.low, candle.close) == (99, 100, 99, 100)


def test_fake_provider_integration_builds_exact_one_and_five_minute_ohlcv():
    engine = builder(("1m", "5m"))
    observations = [("09:15:01", 100), ("09:15:10", 101), ("09:15:30", 99), ("09:15:59", 100)]
    for clock_value, price in observations:
        current = stamp(clock_value)
        item = quote(price=price, source_time=current, received_at=current, volume=25)
        result = validate(item, now=current)
        assert result.accepted
        engine.add_quote(item, result, now=current)
    forming = engine.latest_candles()
    assert len(forming) == 2
    assert {candle.status for candle in forming} == {"FORMING"}
    one_minute = next(candle for candle in forming if candle.interval == "1m")
    five_minute = next(candle for candle in forming if candle.interval == "5m")
    assert (one_minute.open, one_minute.high, one_minute.low, one_minute.close) == (100, 101, 99, 100)
    assert one_minute.volume == 100
    assert (five_minute.open, five_minute.high, five_minute.low, five_minute.close) == (100, 101, 99, 100)
    assert five_minute.volume == 100


def test_candles_close_only_after_interval_and_keep_closed_status():
    engine = builder(("1m",))
    item = quote(source_time=stamp("09:15:10"))
    engine.add_quote(item, validate(item, now=stamp("09:15:10")), now=stamp("09:15:10"))
    assert engine.candles()[0].status == "FORMING"
    events = engine.advance(stamp("09:16:00"))
    assert any(event.event_type == "CANDLE_CLOSED" for event in events)
    assert engine.candles()[0].status == "CLOSED"


def test_late_tick_cannot_mutate_closed_candle():
    engine = builder(("1m",))
    item = quote(price=100, source_time=stamp("09:15:50"))
    engine.add_quote(item, validate(item, now=stamp("09:15:50")), now=stamp("09:15:50"))
    engine.advance(stamp("09:16:00"))
    before = engine.candles()[0]
    late = quote(price=50, source_time=stamp("09:15:59"), received_at=stamp("09:16:05"))
    events = engine.add_quote(late, validate(late, now=stamp("09:16:05")), now=stamp("09:16:05"))
    after = engine.candles()[0]
    assert any(event.event_type == "LATE_TICK_REJECTED" for event in events)
    assert after == before


def test_missing_ticks_do_not_create_empty_candles_or_forward_fill():
    engine = builder(("1m", "5m"))
    item = quote(source_time=stamp("09:15:05"))
    engine.add_quote(item, validate(item, now=stamp("09:15:05")), now=stamp("09:15:05"))
    engine.advance(stamp("09:18:00"))
    candles = engine.candles()
    assert len(candles) == 2
    assert {candle.start for candle in candles} == {stamp("09:15:00")}
    assert not any(candle.start == stamp("09:16:00") or candle.start == stamp("09:17:00") for candle in candles)


def test_cumulative_volume_is_not_misrepresented_as_per_candle_volume():
    engine = builder(("1m",))
    item = quote(volume=500, volume_kind="CUMULATIVE", source_time=stamp("09:15:05"))
    engine.add_quote(item, validate(item, now=stamp("09:15:05")), now=stamp("09:15:05"))
    assert engine.candles()[0].volume is None


def test_market_closed_weekend_does_not_create_candles():
    closed = datetime(2026, 9, 26, 10, 0, tzinfo=IST)
    item = quote(source_time=closed, received_at=closed)
    engine = builder(("1m",))
    events = engine.add_quote(item, validate(item, now=closed), now=closed)
    assert any(event.event_type == "CANDLE_SUPPRESSED" for event in events)
    assert engine.candles() == []


def test_market_calendar_reports_preopen_and_closed_and_blocks_special_session():
    calendar = NseTradingCalendar()
    assert calendar.intraday_status(stamp("09:05"), timezone_name="Asia/Kolkata").status == "PREOPEN"
    assert calendar.intraday_status(stamp("08:30"), timezone_name="Asia/Kolkata").status == "CLOSED"
    assert calendar.intraday_status(stamp("10:00"), timezone_name="Asia/Kolkata").status == "DATA_UNAVAILABLE"
    assert calendar.intraday_status(stamp("10:00"), timezone_name="Asia/Kolkata",
                                    observed_market_data=True).status == "OPEN"
    calendar.holidays[datetime(2026, 9, 28).date()] = "test holiday"
    assert calendar.intraday_status(stamp("10:00"), timezone_name="Asia/Kolkata",
                                    observed_market_data=True).status == "CLOSED"
    special = NseTradingCalendar()
    special.special_sessions[datetime(2026, 9, 28).date()] = "test special session"
    assert special.intraday_status(stamp("10:00"), timezone_name="Asia/Kolkata").status == "SPECIAL_SESSION"


def test_monitor_reports_missing_symbol_and_fresh_quote_metrics():
    now = stamp("09:15:10")
    provider = FakeProvider([[quote(source_time=now, received_at=now)]])
    monitor = RealtimeFeedMonitor(provider, requested_symbols=["RELIANCE", "TCS"],
                                  settings=monitor_settings(), candle_builder=builder(("1m",)),
                                  clock=lambda: now)
    health = monitor.poll_once()
    assert health["feed_status"] == "DEGRADED"
    assert health["received_symbols"] == ["RELIANCE"]
    assert health["accepted_symbols"] == ["RELIANCE"]
    assert health["missing_symbols"] == ["TCS"]
    assert health["accepted_quote_count"] == 1
    assert health["quotes"][0]["freshness_status"] == "FRESH"
    assert health["live_trading_enabled"] is False
    assert health["signals_generated"] is False


def test_stale_quote_is_not_accepted_and_is_reported_separately():
    now = stamp("09:15:10")
    stale = quote(source_time=stamp("09:15:00"), received_at=stamp("09:15:10"))
    provider = FakeProvider([[stale]])
    monitor = RealtimeFeedMonitor(provider, requested_symbols=["RELIANCE"],
                                  settings=monitor_settings(max_quote_age_seconds=5),
                                  candle_builder=builder(("1m",)), clock=lambda: now)
    health = monitor.poll_once()
    assert health["accepted_quote_count"] == 0
    assert health["stale_quote_count"] == 1
    assert health["stale_symbols"] == ["RELIANCE"]
    assert health["missing_symbols"] == ["RELIANCE"]
    assert health["candles"] == []


def test_provider_disconnect_uses_bounded_retries_and_marks_symbols_missing():
    now = stamp("09:15:10")
    provider = FakeProvider([TimeoutError("first"), TimeoutError("second"), TimeoutError("third")])
    sleeps = []
    settings = monitor_settings(max_retries=2, retry_backoff_seconds=0.25)
    monitor = RealtimeFeedMonitor(provider, requested_symbols=["RELIANCE"], settings=settings,
                                  candle_builder=builder(("1m",)), clock=lambda: now,
                                  sleeper=sleeps.append)
    health = monitor.poll_once()
    assert health["feed_status"] == "DISCONNECTED"
    assert health["missing_symbols"] == ["RELIANCE"]
    assert health["error_count"] == 3
    assert health["retry_count"] == 2
    assert sleeps == [0.25, 0.5]


def test_provider_reconnect_after_disconnect_is_reported():
    now = [stamp("09:15:10")]
    provider = FakeProvider([ConnectionError("offline"), [quote(source_time=now[0], received_at=now[0])]])
    settings = monitor_settings(max_retries=0)
    monitor = RealtimeFeedMonitor(provider, requested_symbols=["RELIANCE"], settings=settings,
                                  candle_builder=builder(("1m",)), clock=lambda: now[0], sleeper=lambda _: None)
    assert monitor.poll_once()["feed_status"] == "DISCONNECTED"
    now[0] = stamp("09:15:20")
    recovered = quote(source_time=now[0], received_at=now[0])
    provider.responses[0] = [recovered]
    health = monitor.poll_once()
    assert health["feed_status"] == "CONNECTED"
    assert health["reconnect_count"] == 1


def test_last_known_quote_becomes_stale_when_feed_disconnects():
    first_time = stamp("09:15:10")
    provider = FakeProvider([[quote(source_time=first_time, received_at=first_time)],
                             ConnectionError("disconnected")])
    now = [first_time]
    monitor = RealtimeFeedMonitor(provider, requested_symbols=["RELIANCE"],
                                  settings=monitor_settings(max_retries=0, max_quote_age_seconds=5),
                                  candle_builder=builder(("1m",)), clock=lambda: now[0])
    assert monitor.poll_once()["feed_status"] == "CONNECTED"
    now[0] = stamp("09:16:00")
    health = monitor.poll_once()
    assert health["feed_status"] == "DISCONNECTED"
    assert health["stale_symbols"] == ["RELIANCE"]
    assert health["quotes"][0]["freshness_status"] == "STALE"


def test_malformed_provider_response_degrades_without_crashing():
    now = stamp("09:15:10")
    monitor = RealtimeFeedMonitor(FakeProvider([{"price": 100}]), requested_symbols=["RELIANCE"],
                                  settings=monitor_settings(), candle_builder=builder(("1m",)),
                                  clock=lambda: now)
    health = monitor.poll_once()
    assert health["feed_status"] == "DEGRADED"
    assert health["error_count"] == 1
    assert health["invalid_symbols"] == ["(malformed provider response)"]
    assert health["candles"] == []


def test_feed_health_snapshot_is_atomic_json_and_recording_is_opt_in(tmp_path: Path):
    now = stamp("09:15:10")
    health_path = tmp_path / "health.json"
    item = quote(source_time=now, received_at=now)
    provider = FakeProvider([[item]])
    monitor = RealtimeFeedMonitor(provider, requested_symbols=["RELIANCE"],
                                  settings=monitor_settings(), candle_builder=builder(("1m",)),
                                  health_snapshot_path=health_path, clock=lambda: now)
    monitor.poll_once()
    snapshot = json.loads(health_path.read_text())
    assert snapshot["accepted_quote_count"] == 1
    assert not list(tmp_path.glob("*.tmp"))

    recorder = TickRecorder(tmp_path / "recordings")
    validation = validate(item, now=now)
    path = recorder.record(item, validation)
    row = json.loads(path.read_text().splitlines()[0])
    canonical = {key: value for key, value in row.items() if key != "record_sha256"}
    content = json.dumps(canonical, sort_keys=True, separators=(",", ":"), allow_nan=False)
    assert row["record_sha256"] == hashlib.sha256(content.encode()).hexdigest()
    assert "token" not in path.read_text().lower()


def test_no_suitable_provider_is_configured_by_default():
    settings = RealtimeSettings()
    status = initial_feed_status(settings, now=stamp("09:15:10"))
    assert status["status_label"] == "REALTIME FEED: NOT CONFIGURED"
    assert status["status_code"] == "DATA_SOURCE_NOT_CONFIGURED"
    with pytest.raises(FeedConfigurationError) as error:
        configured_provider(settings)
    assert error.value.code == "DATA_SOURCE_NOT_CONFIGURED"


def test_investigated_yahoo_derived_provider_is_blocked_before_network_access():
    settings = RealtimeSettings(enabled=True, provider="indian_stock_market_api")
    with pytest.raises(FeedConfigurationError) as error:
        configured_provider(settings)
    assert error.value.code == "DATA_SOURCE_NOT_SUITABLE"


def test_config_defaults_keep_realtime_and_live_trading_disabled():
    from nse_research.config import RealtimeSettings

    config = AppConfig.load(Path(__file__).resolve().parents[1] / "config/phase3d_personal.yaml")
    assert RealtimeSettings().enabled is False
    assert config.realtime.enabled is True
    assert config.realtime.provider == "jugaad_data"
    assert config.live_trading.enabled is False
    assert config.realtime.candle_intervals == ["1m", "5m"]
    assert config.live_trading.enabled is False


def test_frozen_universe_loader_requires_explicit_eq(tmp_path: Path):
    from nse_research.realtime.engine import load_eq_universe

    good = tmp_path / "good.csv"
    good.write_text("Symbol,Series\nRELIANCE,EQ\nTCS,EQ\n", encoding="utf-8")
    assert load_eq_universe(good) == ["RELIANCE", "TCS"]
    bad = tmp_path / "bad.csv"
    bad.write_text("Symbol,Series\nRELIANCE,BE\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Series=EQ"):
        load_eq_universe(bad)
