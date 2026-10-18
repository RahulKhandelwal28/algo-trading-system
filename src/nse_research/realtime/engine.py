"""Provider-neutral real-time quote validation and intraday candle foundation.

This module only accepts explicitly identified, timestamped NSE EQ quotes. It
does not know about strategies, signals, risk sizing, orders, or broker APIs.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import tempfile
import threading
import time as time_module
from collections import deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from nse_research.config import RealtimeSettings
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.realtime.providers import NormalizedQuote, QuoteValidity, RealtimeDataProvider, SOURCE_NAME

LOGGER = logging.getLogger(__name__)
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")
_PROVIDER_FACTORIES: dict[str, Callable[[RealtimeSettings], "RealtimeMarketDataProvider"]] = {}


# Public name for the existing replaceable provider interface. Keeping one
# abstraction means live adapters and a future replay adapter share the same
# normalized quote contract.
RealtimeMarketDataProvider = RealtimeDataProvider


@dataclass(frozen=True)
class QuoteValidation:
    accepted: bool
    code: str
    reason: str | None
    quote_age_seconds: float | None


def validate_quote(
    quote: NormalizedQuote,
    requested_symbol: str,
    *,
    now: datetime,
    max_quote_age_seconds: float,
    max_future_skew_seconds: float = 1.0,
) -> QuoteValidation:
    """Apply the shared identity, timestamp, numeric, and freshness gates."""
    symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
    requested = str(requested_symbol or "").strip().upper()
    if not requested or not _SYMBOL_RE.fullmatch(requested) or not _SYMBOL_RE.fullmatch(symbol):
        return QuoteValidation(False, QuoteValidity.QUOTE_SYMBOL_INVALID, "symbol is empty or malformed", None)
    if symbol != requested:
        return QuoteValidation(False, QuoteValidity.QUOTE_SYMBOL_INVALID,
                               f"provider returned {symbol} for {requested}", None)
    if str(getattr(quote, "exchange", "") or "").strip().upper() != "NSE":
        return QuoteValidation(False, QuoteValidity.DATA_SOURCE_NOT_SUITABLE,
                               "quote exchange must be explicitly NSE", None)
    series = str(getattr(quote, "series", "") or "").strip().upper()
    if series != "EQ":
        return QuoteValidation(False, QuoteValidity.QUOTE_SERIES_INVALID,
                               "quote must explicitly establish Series=EQ", None)

    try:
        price = float(quote.last_price)
    except (TypeError, ValueError, OverflowError):
        return QuoteValidation(False, QuoteValidity.QUOTE_PRICE_INVALID,
                               "last price is missing or non-numeric", None)
    if not math.isfinite(price) or price <= 0:
        return QuoteValidation(False, QuoteValidity.QUOTE_PRICE_INVALID,
                               "last price must be finite and positive", None)
    volume = quote.volume
    if volume is not None:
        try:
            volume_value = float(volume)
        except (TypeError, ValueError, OverflowError):
            return QuoteValidation(False, "QUOTE_VOLUME_INVALID", "volume is non-numeric", None)
        if not math.isfinite(volume_value) or volume_value < 0:
            return QuoteValidation(False, "QUOTE_VOLUME_INVALID", "volume must be finite and non-negative", None)
        volume_kind = str(getattr(quote, "volume_kind", "") or "").strip().upper()
        if volume_kind and volume_kind not in {"INCREMENTAL", "CUMULATIVE"}:
            return QuoteValidation(False, "QUOTE_VOLUME_INVALID", "volume kind is unsupported", None)
    if not str(getattr(quote, "source", "") or "").strip():
        return QuoteValidation(False, QuoteValidity.DATA_SOURCE_NOT_SUITABLE, "quote source is missing", None)

    received_at = getattr(quote, "received_at", None)
    source_timestamp = getattr(quote, "source_timestamp", None)
    if not _aware_datetime(now) or not _aware_datetime(received_at) or not _aware_datetime(source_timestamp):
        return QuoteValidation(False, QuoteValidity.QUOTE_TIMESTAMP_INVALID,
                               "now, received_at, and source_timestamp must include timezone offsets", None)
    now_utc = now.astimezone(timezone.utc)
    received_utc = received_at.astimezone(timezone.utc)
    source_utc = source_timestamp.astimezone(timezone.utc)
    if received_utc - now_utc > timedelta(seconds=max_future_skew_seconds):
        return QuoteValidation(False, QuoteValidity.QUOTE_TIMESTAMP_INVALID,
                               "local receipt timestamp is unreasonably far in the future", None)
    if source_utc - received_utc > timedelta(seconds=max_future_skew_seconds):
        return QuoteValidation(False, QuoteValidity.QUOTE_TIMESTAMP_INVALID,
                               "source timestamp is later than its local receipt time", None)
    age = (now_utc - source_utc).total_seconds()
    if age < -max_future_skew_seconds:
        return QuoteValidation(False, QuoteValidity.QUOTE_TIMESTAMP_INVALID,
                               "source timestamp is unreasonably far in the future", age)
    age_seconds = max(0.0, age)
    if age_seconds > max_quote_age_seconds:
        return QuoteValidation(False, QuoteValidity.QUOTE_STALE,
                               f"quote age {age_seconds:.3f}s exceeds configured limit {max_quote_age_seconds:.3f}s",
                               age_seconds)
    if not quote.is_valid:
        if quote.validity_status == QuoteValidity.STALE_DATA:
            code = QuoteValidity.QUOTE_STALE
        elif quote.validity_status in {QuoteValidity.INVALID_TIMESTAMP, QuoteValidity.UNVERIFIABLE_TIMESTAMP}:
            code = QuoteValidity.QUOTE_TIMESTAMP_INVALID
        elif quote.validity_status in {QuoteValidity.INVALID_PRICE, QuoteValidity.INVALID_NUMERIC}:
            code = QuoteValidity.QUOTE_PRICE_INVALID
        elif quote.validity_status in {QuoteValidity.WRONG_SYMBOL, QuoteValidity.INVALID_SYMBOL}:
            code = QuoteValidity.QUOTE_SYMBOL_INVALID
        else:
            code = QuoteValidity.DATA_SOURCE_NOT_SUITABLE
        return QuoteValidation(False, code, quote.reason or quote.validity_status, age_seconds)
    return QuoteValidation(True, "ACCEPTED", None, age_seconds)


def _aware_datetime(value: Any) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


@dataclass(frozen=True)
class Candle:
    symbol: str
    interval: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    status: str
    source: str
    last_source_timestamp: datetime
    updated_at: datetime
    tick_count: int


@dataclass(frozen=True)
class CandleEvent:
    event_type: str
    symbol: str
    interval: str | None
    timestamp: datetime
    reason: str | None = None


@dataclass
class _MutableCandle:
    symbol: str
    interval: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    open_key: tuple[datetime, datetime, str]
    close_key: tuple[datetime, datetime, str]
    volume_total: float | None
    volume_complete: bool
    source: str
    last_source_timestamp: datetime
    updated_at: datetime
    tick_count: int = 1

    def freeze(self, status: str) -> Candle:
        return Candle(
            symbol=self.symbol,
            interval=self.interval,
            start=self.start,
            end=self.end,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume_total if self.volume_complete else None,
            status=status,
            source=self.source,
            last_source_timestamp=self.last_source_timestamp,
            updated_at=self.updated_at,
            tick_count=self.tick_count,
        )


class IntradayCandleBuilder:
    """Aggregate validated quotes into session-anchored FORMING/CLOSED bars."""

    def __init__(
        self,
        intervals: Iterable[str] = ("1m", "5m"),
        *,
        calendar: NseTradingCalendar,
        timezone_name: str = "Asia/Kolkata",
        preopen_start: time = time(9, 0),
        market_open: time = time(9, 15),
        market_close: time = time(15, 30),
        history_limit_per_symbol: int = 1000,
        max_seen_tick_ids: int = 25000,
    ) -> None:
        self.intervals = tuple(dict.fromkeys(intervals))
        self.interval_minutes = {value: _interval_minutes(value) for value in self.intervals}
        if not self.interval_minutes:
            raise ValueError("at least one candle interval is required")
        self.calendar = calendar
        self.zone = ZoneInfo(timezone_name)
        self.preopen_start = preopen_start
        self.market_open = market_open
        self.market_close = market_close
        if not (self.preopen_start < self.market_open < self.market_close):
            raise ValueError("market session times must satisfy pre-open < open < close")
        if history_limit_per_symbol < 1 or max_seen_tick_ids < 1:
            raise ValueError("history and duplicate-id limits must be positive")
        self.history_limit_per_symbol = history_limit_per_symbol
        self.max_seen_tick_ids = max_seen_tick_ids
        self._active: dict[tuple[str, str], _MutableCandle] = {}
        self._history: dict[tuple[str, str], deque[Candle]] = {}
        self._closed_until: dict[tuple[str, str], datetime] = {}
        self._latest_source: dict[tuple[str, str], datetime] = {}
        self._seen_tick_ids: set[tuple[str, str]] = set()
        self._seen_tick_order: deque[tuple[str, str]] = deque()
        self._lock = threading.RLock()

    def add_quote(
        self,
        quote: NormalizedQuote,
        validation: QuoteValidation,
        *,
        now: datetime,
    ) -> list[CandleEvent]:
        """Add a validated quote; callers cannot accidentally bypass validation."""
        if not validation.accepted:
            return [CandleEvent("QUOTE_REJECTED", quote.symbol, None, now,
                                f"{validation.code}: {validation.reason or ''}".strip())]
        if not _aware_datetime(now) or not _aware_datetime(quote.source_timestamp):
            return [CandleEvent("QUOTE_TIMESTAMP_INVALID", quote.symbol, None, now,
                                "candle input requires timezone-aware clock and source timestamp")]
        with self._lock:
            events = self.advance(now)
            local_now = now.astimezone(self.zone)
            session = self.calendar.intraday_status(
                local_now,
                timezone_name=str(self.zone),
                preopen_start=self.preopen_start,
                market_open=self.market_open,
                market_close=self.market_close,
                observed_market_data=True,
            )
            if session.status != "OPEN":
                events.append(CandleEvent("CANDLE_SUPPRESSED", quote.symbol, None, local_now,
                                          f"{session.status}: {session.reason}"))
                return events

            source_local = quote.source_timestamp.astimezone(self.zone)
            if source_local.date() != local_now.date():
                events.append(CandleEvent("QUOTE_OUTSIDE_CURRENT_SESSION", quote.symbol, None, local_now,
                                          "source quote date differs from the current local market session"))
                return events
            if not (self.market_open <= source_local.timetz().replace(tzinfo=None) < self.market_close):
                events.append(CandleEvent("QUOTE_OUTSIDE_CURRENT_SESSION", quote.symbol, None, local_now,
                                          "source timestamp is outside the configured regular session"))
                return events

            identity = (quote.symbol, str(quote.tick_id)) if quote.tick_id else None
            if identity and identity in self._seen_tick_ids:
                events.append(CandleEvent("DUPLICATE_TICK_REJECTED", quote.symbol, None, local_now,
                                          "tick identity was already processed"))
                LOGGER.info("QUOTE_REJECTED symbol=%s code=QUOTE_DUPLICATE", quote.symbol)
                return events
            if identity:
                self._remember_tick_id(identity)

            for interval, minutes in self.interval_minutes.items():
                start, end = self._bucket(source_local, minutes)
                key = (quote.symbol, interval)
                if end <= local_now:
                    events.append(CandleEvent("LATE_TICK_REJECTED", quote.symbol, interval, local_now,
                                              "the source interval has ended; closed candles are immutable"))
                    LOGGER.info("QUOTE_REJECTED symbol=%s interval=%s code=LATE_TICK", quote.symbol, interval)
                    continue
                closed_until = self._closed_until.get(key)
                if closed_until is not None and start < closed_until:
                    events.append(CandleEvent("LATE_TICK_REJECTED", quote.symbol, interval, local_now,
                                              "tick would change a previously closed candle"))
                    continue
                active = self._active.get(key)
                if active is not None and active.start != start:
                    if active.end <= local_now:
                        events.extend(self._close_active(key, active, local_now))
                        active = None
                    else:
                        events.append(CandleEvent("CANDLE_SEQUENCE_REJECTED", quote.symbol, interval, local_now,
                                                  "new bucket arrived before the active bucket could close"))
                        continue

                event_type = "CANDLE_UPDATED"
                if active is None:
                    active = self._start_candle(quote, interval, start, end, local_now)
                    self._active[key] = active
                    event_type = "CANDLE_STARTED"
                    LOGGER.info("CANDLE_STARTED symbol=%s interval=%s start=%s", quote.symbol, interval, start.isoformat())
                else:
                    source_utc = quote.source_timestamp.astimezone(timezone.utc)
                    latest = self._latest_source.get(key)
                    if latest is not None and source_utc < latest:
                        event_type = "OUT_OF_ORDER_TICK_ACCEPTED"
                    self._update_candle(active, quote, local_now)
                source_utc = quote.source_timestamp.astimezone(timezone.utc)
                previous = self._latest_source.get(key)
                self._latest_source[key] = max(previous, source_utc) if previous else source_utc
                events.append(CandleEvent(event_type, quote.symbol, interval, local_now))
                LOGGER.info("%s symbol=%s interval=%s source_timestamp=%s", event_type, quote.symbol,
                            interval, quote.source_timestamp.isoformat())
            return events

    def advance(self, now: datetime) -> list[CandleEvent]:
        """Close elapsed intervals without generating empty or synthetic bars."""
        if not _aware_datetime(now):
            raise ValueError("candle clock must include a timezone")
        local_now = now.astimezone(self.zone)
        events: list[CandleEvent] = []
        for key, active in list(self._active.items()):
            if local_now >= active.end:
                events.extend(self._close_active(key, active, local_now))
        return events

    def candles(self, *, status: str | None = None) -> list[Candle]:
        """Return a snapshot of recent closed bars and current forming bars."""
        with self._lock:
            result: list[Candle] = []
            for key, history in self._history.items():
                result.extend(candle for candle in history if status is None or candle.status == status)
                active = self._active.get(key)
                if active is not None and (status is None or status == "FORMING"):
                    result.append(active.freeze("FORMING"))
            for key, active in self._active.items():
                if key not in self._history and (status is None or status == "FORMING"):
                    result.append(active.freeze("FORMING"))
            return sorted(result, key=lambda candle: (candle.start, candle.symbol, candle.interval))

    def latest_candles(self) -> list[Candle]:
        """Return the newest candle for each symbol and interval."""
        latest: dict[tuple[str, str], Candle] = {}
        for candle in self.candles():
            key = (candle.symbol, candle.interval)
            if key not in latest or candle.end > latest[key].end:
                latest[key] = candle
        return sorted(latest.values(), key=lambda candle: (candle.symbol, candle.interval))

    def _bucket(self, source_time: datetime, minutes: int) -> tuple[datetime, datetime]:
        session_open = datetime.combine(source_time.date(), self.market_open, tzinfo=self.zone)
        elapsed_seconds = int((source_time - session_open).total_seconds())
        bucket_seconds = minutes * 60
        offset_seconds = (elapsed_seconds // bucket_seconds) * bucket_seconds
        start = session_open + timedelta(seconds=offset_seconds)
        end = min(start + timedelta(seconds=bucket_seconds),
                  datetime.combine(source_time.date(), self.market_close, tzinfo=self.zone))
        return start, end

    def _start_candle(
        self, quote: NormalizedQuote, interval: str, start: datetime, end: datetime, now: datetime
    ) -> _MutableCandle:
        key = _quote_order_key(quote)
        volume_total, volume_complete = _quote_volume(quote)
        source_time = quote.source_timestamp.astimezone(timezone.utc)
        return _MutableCandle(
            symbol=quote.symbol, interval=interval, start=start, end=end,
            open=float(quote.last_price), high=float(quote.last_price), low=float(quote.last_price),
            close=float(quote.last_price), open_key=key, close_key=key,
            volume_total=volume_total, volume_complete=volume_complete,
            source=quote.source, last_source_timestamp=source_time,
            updated_at=now.astimezone(self.zone),
        )

    @staticmethod
    def _update_candle(active: _MutableCandle, quote: NormalizedQuote, now: datetime) -> None:
        price = float(quote.last_price)
        key = _quote_order_key(quote)
        active.high = max(active.high, price)
        active.low = min(active.low, price)
        if key < active.open_key:
            active.open_key = key
            active.open = price
        if key > active.close_key:
            active.close_key = key
            active.close = price
        increment, complete = _quote_volume(quote)
        if not active.volume_complete or not complete:
            active.volume_complete = False
            active.volume_total = None
        else:
            active.volume_total = float(active.volume_total or 0.0) + float(increment or 0.0)
        active.tick_count += 1
        source_time = quote.source_timestamp.astimezone(timezone.utc)
        active.last_source_timestamp = max(active.last_source_timestamp, source_time)
        active.updated_at = now.astimezone(active.start.tzinfo)

    def _close_active(
        self, key: tuple[str, str], active: _MutableCandle, now: datetime
    ) -> list[CandleEvent]:
        candle = active.freeze("CLOSED")
        history = self._history.setdefault(key, deque(maxlen=self.history_limit_per_symbol))
        history.append(candle)
        self._closed_until[key] = max(self._closed_until.get(key, candle.end), candle.end)
        self._active.pop(key, None)
        self._latest_source.pop(key, None)
        LOGGER.info("CANDLE_CLOSED symbol=%s interval=%s start=%s end=%s", candle.symbol,
                    candle.interval, candle.start.isoformat(), candle.end.isoformat())
        return [CandleEvent("CANDLE_CLOSED", candle.symbol, candle.interval, now, None)]

    def _remember_tick_id(self, identity: tuple[str, str]) -> None:
        self._seen_tick_ids.add(identity)
        self._seen_tick_order.append(identity)
        while len(self._seen_tick_order) > self.max_seen_tick_ids:
            old = self._seen_tick_order.popleft()
            self._seen_tick_ids.discard(old)


def _quote_order_key(quote: NormalizedQuote) -> tuple[datetime, datetime, str]:
    return (
        quote.source_timestamp.astimezone(timezone.utc),
        quote.received_at.astimezone(timezone.utc),
        str(quote.tick_id or ""),
    )


def _quote_volume(quote: NormalizedQuote) -> tuple[float | None, bool]:
    if quote.volume is None or str(quote.volume_kind or "").upper() != "INCREMENTAL":
        return None, False
    return float(quote.volume), True


def _interval_minutes(value: str) -> int:
    if not isinstance(value, str) or not value.endswith("m"):
        raise ValueError(f"unsupported candle interval: {value!r}")
    try:
        minutes = int(value[:-1])
    except ValueError as error:
        raise ValueError(f"unsupported candle interval: {value!r}") from error
    if not 1 <= minutes <= 60:
        raise ValueError("candle intervals must be between 1m and 60m")
    return minutes


def register_market_data_provider(
    name: str,
    factory: Callable[[RealtimeSettings], RealtimeMarketDataProvider],
) -> None:
    """Register an explicitly audited provider adapter for the common contract."""
    normalized = name.strip().lower()
    if not normalized or normalized == SOURCE_NAME:
        raise ValueError("provider name is empty or reserved for an unsuitable adapter")
    _PROVIDER_FACTORIES[normalized] = factory


class FeedConfigurationError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def configured_provider(settings: RealtimeSettings) -> RealtimeMarketDataProvider:
    """Create only a provider with an explicit suitability registration.

    The already-investigated Yahoo-derived API is intentionally rejected here:
    it does not guarantee source timestamps or explicit NSE EQ identity.
    """
    name = settings.provider.strip().lower()
    if not settings.enabled or not name:
        raise FeedConfigurationError(QuoteValidity.DATA_SOURCE_NOT_CONFIGURED,
                                     "REALTIME_ENABLED must be true and REALTIME_PROVIDER must name an audited provider")
    if name == SOURCE_NAME:
        raise FeedConfigurationError(
            QuoteValidity.DATA_SOURCE_NOT_SUITABLE,
            "Indian Stock Market API uses Yahoo-derived quotes and cannot establish the required source timestamp and Series=EQ identity",
        )
    factory = _PROVIDER_FACTORIES.get(name)
    if factory is None:
        raise FeedConfigurationError(QuoteValidity.DATA_SOURCE_NOT_SUITABLE,
                                     f"no audited adapter is registered for provider {name!r}")
    return factory(settings)


class TickRecorder:
    """Optional JSONL normalized-tick recorder with per-record provenance hash."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self._lock = threading.Lock()

    def record(
        self,
        quote: NormalizedQuote,
        validation: QuoteValidation,
        *,
        request_metadata: Mapping[str, Any] | None = None,
    ) -> Path:
        received = quote.received_at
        day = received.date().isoformat() if _aware_datetime(received) else "unknown-date"
        path = self.directory / f"quotes_{day}.jsonl"
        payload = {
            "source": quote.source,
            "symbol": quote.symbol,
            "exchange": quote.exchange,
            "series": quote.series,
            "source_timestamp": _iso(quote.source_timestamp),
            "source_timestamp_raw": quote.source_timestamp_raw,
            "source_timestamp_unit": quote.source_timestamp_unit,
            "source_timestamp_origin": quote.source_timestamp_origin,
            "received_at": _iso(quote.received_at),
            "instrument_key": quote.instrument_key,
            "last_trade_quantity": quote.last_trade_quantity,
            "feed_current_timestamp_raw": quote.feed_current_timestamp_raw,
            "feed_current_timestamp_utc": _iso(quote.feed_current_timestamp_utc),
            "latency_ms": quote.latency_ms,
            "last_price": _safe_record_number(quote.last_price),
            "volume": _safe_record_number(quote.volume),
            "volume_kind": quote.volume_kind,
            "tick_id": quote.tick_id,
            "request_metadata": dict(request_metadata or {}),
            "validation": asdict(validation),
        }
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        payload["record_sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        line = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
        with self._lock:
            self.directory.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as stream:
                stream.write(line)
        return path


class RealtimeFeedMonitor:
    """Bounded polling/validation coordinator; never invokes trading code."""

    def __init__(
        self,
        provider: RealtimeMarketDataProvider,
        *,
        requested_symbols: Iterable[str],
        settings: RealtimeSettings,
        candle_builder: IntradayCandleBuilder,
        health_snapshot_path: str | Path | None = None,
        recorder: TickRecorder | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleeper: Callable[[float], None] = time_module.sleep,
    ) -> None:
        symbols = [str(symbol).strip().upper() for symbol in requested_symbols]
        if any(not _SYMBOL_RE.fullmatch(symbol) for symbol in symbols):
            raise ValueError("requested symbols must use valid NSE ticker characters")
        if len(set(symbols)) != len(symbols):
            raise ValueError("requested symbols must be unique")
        self.provider = provider
        self.provider_name = str(getattr(provider, "provider_name", provider.__class__.__name__))
        self.requested_symbols = symbols
        self.settings = settings
        self.candle_builder = candle_builder
        self.health_snapshot_path = Path(health_snapshot_path) if health_snapshot_path else None
        self.recorder = recorder
        self.clock = clock
        self.sleeper = sleeper
        self._state = "DISCONNECTED"
        self._ever_connected = False
        self._has_polled = False
        self._counters = {
            "received_quote_count": 0, "accepted_quote_count": 0, "rejected_quote_count": 0,
            "stale_quote_count": 0, "reconnect_count": 0, "error_count": 0,
            "retry_count": 0,
        }
        self._last_successful_update: datetime | None = None
        self._last_quote_timestamp: datetime | None = None
        self._last_received_at: datetime | None = None
        self._last_error: str | None = None
        self._missing_symbols = list(symbols)
        self._invalid_symbols: list[str] = []
        self._stale_symbols: list[str] = []
        self._received_symbols: list[str] = []
        self._accepted_symbols: list[str] = []
        self._latest_quotes: dict[str, dict[str, Any]] = {}
        self._rejection_details: deque[dict[str, Any]] = deque(maxlen=1000)
        self._latencies: deque[float] = deque(maxlen=10000)
        self._seen_tick_ids: set[tuple[str, str]] = set()
        self._seen_tick_order: deque[tuple[str, str]] = deque()
        self._lock = threading.RLock()

    def poll_once(self) -> dict[str, Any]:
        """Fetch one configured symbol batch with bounded exception retries."""
        with self._lock:
            previous_state = self._state
            recovering = self._has_polled and previous_state == "DISCONNECTED"
            self._state = "RECONNECTING" if recovering else "CONNECTING"
            self._has_polled = True
            if recovering:
                self._counters["reconnect_count"] += 1
        quotes: list[NormalizedQuote] | None = None
        last_exception: Exception | None = None
        for attempt in range(self.settings.max_retries + 1):
            try:
                quotes = self.provider.get_quotes(list(self.requested_symbols))
                last_exception = None
                break
            except Exception as error:  # adapter failures become health, never a fake quote
                last_exception = error
                with self._lock:
                    self._counters["error_count"] += 1
                    if attempt < self.settings.max_retries:
                        self._counters["retry_count"] += 1
                        self._state = "RECONNECTING"
                LOGGER.warning("REALTIME_PROVIDER_DISCONNECTED provider=%s error=%s",
                               self.provider_name, type(error).__name__)
                if attempt < self.settings.max_retries:
                    delay = min(self.settings.retry_backoff_seconds * (2 ** attempt), 30.0)
                    self.sleeper(delay)
        now = self.clock()
        if not _aware_datetime(now):
            raise ValueError("feed clock must be timezone-aware")
        if last_exception is not None or quotes is None:
            with self._lock:
                self._state = "DISCONNECTED"
                self._last_error = f"{type(last_exception).__name__}: {last_exception}"
                self._missing_symbols = list(self.requested_symbols)
                self._received_symbols = []
                self._accepted_symbols = []
                self._invalid_symbols = []
                self._stale_symbols = []
            payload = self.health(now=now)
            self._write_health(payload)
            return payload
        if not isinstance(quotes, (list, tuple)) or any(not isinstance(item, NormalizedQuote) for item in quotes):
            with self._lock:
                self._state = "DEGRADED"
                self._last_error = "provider response did not contain normalized quote objects"
                self._counters["error_count"] += 1
                self._counters["rejected_quote_count"] += 1
                self._missing_symbols = list(self.requested_symbols)
                self._received_symbols = []
                self._accepted_symbols = []
                self._invalid_symbols = ["(malformed provider response)"]
                self._stale_symbols = []
            payload = self.health(now=now)
            self._write_health(payload)
            return payload

        self.candle_builder.advance(now)
        requested_set = set(self.requested_symbols)
        received: set[str] = set()
        accepted: set[str] = set()
        invalid: set[str] = set()
        stale: set[str] = set()
        with self._lock:
            self._last_successful_update = now
            self._last_error = None
            self._counters["received_quote_count"] += len(quotes)

        for quote in quotes:
            symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
            if symbol:
                received.add(symbol)
            source_candidate = getattr(quote, "source_timestamp", None)
            received_candidate = getattr(quote, "received_at", None)
            with self._lock:
                if _aware_datetime(source_candidate):
                    source_utc = source_candidate.astimezone(timezone.utc)
                    if self._last_quote_timestamp is None or source_utc > self._last_quote_timestamp.astimezone(timezone.utc):
                        self._last_quote_timestamp = source_candidate
                if _aware_datetime(received_candidate):
                    if self._last_received_at is None or received_candidate > self._last_received_at:
                        self._last_received_at = received_candidate
            validation = validate_quote(
                quote,
                symbol,
                now=now,
                max_quote_age_seconds=self.settings.max_quote_age_seconds,
                max_future_skew_seconds=self.settings.max_future_skew_seconds,
            )
            if symbol not in requested_set:
                validation = QuoteValidation(False, QuoteValidity.QUOTE_SYMBOL_INVALID,
                                              "provider returned a symbol that was not requested",
                                              validation.quote_age_seconds)
            if validation.accepted and quote.tick_id:
                identity = (symbol, str(quote.tick_id))
                if identity in self._seen_tick_ids:
                    validation = QuoteValidation(False, QuoteValidity.QUOTE_DUPLICATE,
                                                  "tick identity was already received", validation.quote_age_seconds)
                else:
                    self._remember_tick_id(identity)
            if self.recorder is not None:
                self.recorder.record(
                    quote,
                    validation,
                    request_metadata={
                        "requested_symbol": symbol,
                        "batch_symbols": list(self.requested_symbols),
                    },
                )
            if not validation.accepted:
                with self._lock:
                    self._rejection_details.append({
                        "symbol": symbol or "(unknown)",
                        "code": validation.code,
                        "reason": validation.reason,
                        "quote_age_seconds": validation.quote_age_seconds,
                    })
                if validation.code == QuoteValidity.QUOTE_STALE:
                    stale.add(symbol)
                    with self._lock:
                        self._counters["stale_quote_count"] += 1
                else:
                    invalid.add(symbol or "(unknown)")
                with self._lock:
                    self._counters["rejected_quote_count"] += 1
                LOGGER.warning("QUOTE_REJECTED symbol=%s code=%s reason=%s",
                               symbol or "(unknown)", validation.code, validation.reason)
                continue
            accepted.add(symbol)
            quote_row = {
                "symbol": symbol,
                "exchange": quote.exchange,
                "series": quote.series,
                "last_price": quote.last_price,
                "volume": quote.volume,
                "source": quote.source,
                "source_timestamp": _iso(quote.source_timestamp),
                "source_timestamp_raw": quote.source_timestamp_raw,
                "source_timestamp_unit": quote.source_timestamp_unit,
                "source_timestamp_origin": quote.source_timestamp_origin,
                "received_at": _iso(quote.received_at),
                "instrument_key": quote.instrument_key,
                "last_trade_quantity": quote.last_trade_quantity,
                "feed_current_timestamp_raw": quote.feed_current_timestamp_raw,
                "feed_current_timestamp_utc": _iso(quote.feed_current_timestamp_utc),
                "latency_ms": quote.latency_ms,
                "quote_age_seconds": validation.quote_age_seconds,
                "freshness_status": "FRESH",
                "tick_id": quote.tick_id,
            }
            with self._lock:
                self._counters["accepted_quote_count"] += 1
                self._latest_quotes[symbol] = quote_row
            source_stamp = quote.source_timestamp
            if source_stamp is not None:
                latency = max(0.0, (now.astimezone(timezone.utc) - source_stamp.astimezone(timezone.utc)).total_seconds())
                self._latencies.append(latency)
            self.candle_builder.add_quote(quote, validation, now=now)

        with self._lock:
            self._received_symbols = sorted(received & requested_set)
            self._accepted_symbols = sorted(accepted)
            self._missing_symbols = sorted(requested_set - accepted)
            self._invalid_symbols = sorted(invalid)
            self._stale_symbols = sorted(stale)
            self._ever_connected = True
            if accepted and not self._missing_symbols and not invalid and not stale:
                self._state = "CONNECTED"
            else:
                self._state = "DEGRADED"
        LOGGER.info("REALTIME_FEED_%s provider=%s accepted=%d requested=%d missing=%d",
                    self._state, self.provider_name, len(accepted), len(requested_set),
                    len(self._missing_symbols))
        payload = self.health(now=now)
        self._write_health(payload)
        return payload

    def health(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = now or self.clock()
        with self._lock:
            observed_market_data = bool(self._accepted_symbols)
        session = self.candle_builder.calendar.intraday_status(
            current,
            timezone_name=str(self.candle_builder.zone),
            preopen_start=self.candle_builder.preopen_start,
            market_open=self.candle_builder.market_open,
            market_close=self.candle_builder.market_close,
            observed_market_data=observed_market_data,
        )
        with self._lock:
            latencies = list(self._latencies)
            payload = {
                "feed_status": self._state,
                "status_label": f"REALTIME FEED: {self._state}",
                "provider": self.provider_name,
                "enabled": self.settings.enabled,
                "last_successful_update": _iso(self._last_successful_update),
                "last_received_at": _iso(self._last_received_at),
                "last_quote_timestamp": _iso(self._last_quote_timestamp),
                "average_observed_latency_seconds": sum(latencies) / len(latencies) if latencies else None,
                "maximum_observed_latency_seconds": max(latencies) if latencies else None,
                "requested_symbols": len(self.requested_symbols),
                "requested_symbol_list": list(self.requested_symbols),
                "received_symbols": list(self._received_symbols),
                "accepted_symbols": list(self._accepted_symbols),
                "quotes": [dict(self._latest_quotes[symbol]) for symbol in sorted(self._latest_quotes)],
                "missing_symbols": list(self._missing_symbols),
                "invalid_symbols": list(self._invalid_symbols),
                "stale_symbols": list(self._stale_symbols),
                "received_quote_count": self._counters["received_quote_count"],
                "accepted_quote_count": self._counters["accepted_quote_count"],
                "rejected_quote_count": self._counters["rejected_quote_count"],
                "stale_quote_count": self._counters["stale_quote_count"],
                "reconnect_count": self._counters["reconnect_count"],
                "error_count": self._counters["error_count"],
                "retry_count": self._counters["retry_count"],
                "last_error": self._last_error,
                "recent_rejections": list(self._rejection_details),
                "market_status": session.status,
                "market_status_reason": session.reason,
                "candle_intervals": list(self.candle_builder.intervals),
                "candles": [_candle_dict(candle) for candle in self.candle_builder.latest_candles()],
                "generated_at": _iso(current),
                "live_trading_enabled": False,
                "broker_execution_enabled": False,
                "signals_generated": False,
                "ledger_mutated": False,
            }
        provider_health = getattr(self.provider, "health_snapshot", None)
        if callable(provider_health):
            try:
                details = provider_health()
            except Exception:
                details = None
            if isinstance(details, dict):
                payload["provider_details"] = details
        for quote_row in payload["quotes"]:
            source_stamp = _parse_aware(quote_row.get("source_timestamp"))
            age = None if source_stamp is None else max(
                0.0, (current.astimezone(timezone.utc) - source_stamp.astimezone(timezone.utc)).total_seconds()
            )
            quote_row["quote_age_seconds"] = age
            quote_row["freshness_status"] = (
                "FRESH" if age is not None and age <= self.settings.max_quote_age_seconds else "STALE"
            )
        if payload["feed_status"] == "CONNECTED" and any(
            quote["freshness_status"] == "STALE" for quote in payload["quotes"]
        ):
            payload["feed_status"] = "DEGRADED"
            payload["status_label"] = "REALTIME FEED: DEGRADED"
        stale_now = {row["symbol"] for row in payload["quotes"] if row["freshness_status"] == "STALE"}
        payload["stale_symbols"] = sorted(set(payload["stale_symbols"]) | stale_now)
        return payload

    def _remember_tick_id(self, identity: tuple[str, str]) -> None:
        self._seen_tick_ids.add(identity)
        self._seen_tick_order.append(identity)
        while len(self._seen_tick_order) > 25000:
            self._seen_tick_ids.discard(self._seen_tick_order.popleft())

    def _write_health(self, payload: Mapping[str, Any]) -> None:
        if self.health_snapshot_path is None:
            return
        path = self.health_snapshot_path
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(payload, sort_keys=True, default=_json_default, allow_nan=False, indent=2) + "\n"
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def initial_feed_status(
    settings: RealtimeSettings,
    *,
    now: datetime | None = None,
    calendar: NseTradingCalendar | None = None,
    timezone_name: str = "Asia/Kolkata",
    observed_market_data: bool = False,
) -> dict[str, Any]:
    """Honest dashboard payload when no feed worker has published health."""
    provider = settings.provider.strip().lower()
    if not settings.enabled and not provider:
        code = QuoteValidity.DATA_SOURCE_NOT_CONFIGURED
        label = "REALTIME FEED: NOT CONFIGURED"
        reason = "Set realtime.enabled and an audited realtime.provider after a suitable source is available."
    elif not settings.enabled:
        code = "REALTIME_DISABLED"
        label = "REALTIME FEED: DISABLED"
        reason = "Realtime data is configured but remains disabled."
    elif not provider:
        code = QuoteValidity.DATA_SOURCE_NOT_CONFIGURED
        label = "REALTIME FEED: NOT CONFIGURED"
        reason = "Realtime is enabled without a configured provider; feed requests are blocked."
    elif provider == SOURCE_NAME:
        code = QuoteValidity.DATA_SOURCE_NOT_SUITABLE
        label = "REALTIME DATA SOURCE NOT SUITABLE"
        reason = "The investigated Yahoo-derived API cannot establish source timestamps and explicit Series=EQ identity."
    elif provider not in _PROVIDER_FACTORIES:
        code = QuoteValidity.DATA_SOURCE_NOT_SUITABLE
        label = "REALTIME DATA SOURCE NOT SUITABLE"
        reason = f"No audited realtime adapter is registered for provider {provider!r}."
    else:
        code = "FEED_NOT_STARTED"
        label = "REALTIME FEED: DISCONNECTED"
        reason = "A suitable provider is configured, but no feed health snapshot has been published."
    current = now or datetime.now(ZoneInfo(timezone_name))
    session = (calendar or NseTradingCalendar()).intraday_status(
        current,
        timezone_name=timezone_name,
        preopen_start=time.fromisoformat(settings.preopen_start),
        market_open=time.fromisoformat(settings.market_open),
        market_close=time.fromisoformat(settings.market_close),
        observed_market_data=observed_market_data,
    )
    return {
        "feed_status": "DISCONNECTED",
        "status_label": label,
        "status_code": code,
        "provider": provider or None,
        "provider_configured": bool(provider),
        "enabled": settings.enabled,
        "reason": reason,
        "last_successful_update": None,
        "last_received_at": None,
        "last_quote_timestamp": None,
        "average_observed_latency_seconds": None,
        "maximum_observed_latency_seconds": None,
        "requested_symbols": 0,
        "requested_symbol_list": [],
        "received_symbols": [],
        "accepted_symbols": [],
        "quotes": [],
        "missing_symbols": [],
        "invalid_symbols": [],
        "stale_symbols": [],
        "received_quote_count": 0,
        "accepted_quote_count": 0,
        "rejected_quote_count": 0,
        "stale_quote_count": 0,
        "reconnect_count": 0,
        "error_count": 0,
        "retry_count": 0,
        "last_error": None,
        "recent_rejections": [],
        "market_status": session.status,
        "market_status_reason": session.reason,
        "candle_intervals": list(settings.candle_intervals),
        "candles": [],
        "live_trading_enabled": False,
        "broker_execution_enabled": False,
        "signals_generated": False,
        "ledger_mutated": False,
    }


def load_eq_universe(path: str | Path) -> list[str]:
    """Read the frozen symbol roster and require its explicit Series=EQ field."""
    import csv

    symbols: list[str] = []
    with Path(path).open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        fields = {str(name).strip().casefold(): name for name in (reader.fieldnames or [])}
        symbol_column, series_column = fields.get("symbol"), fields.get("series")
        if not symbol_column or not series_column:
            raise ValueError("realtime universe requires explicit Symbol and Series columns")
        seen: set[str] = set()
        for line, row in enumerate(reader, start=2):
            symbol = str(row.get(symbol_column) or "").strip().upper()
            series = str(row.get(series_column) or "").strip().upper()
            if not symbol or not _SYMBOL_RE.fullmatch(symbol):
                raise ValueError(f"invalid symbol in realtime universe at line {line}")
            if series != "EQ":
                raise ValueError(f"realtime universe row {line} does not explicitly identify Series=EQ")
            if symbol in seen:
                raise ValueError(f"duplicate symbol {symbol!r} in realtime universe")
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def _candle_dict(candle: Candle) -> dict[str, Any]:
    return {key: _json_default(value) if isinstance(value, datetime) else value
            for key, value in asdict(candle).items()}


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _parse_aware(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if _aware_datetime(parsed) else None


def _safe_record_number(value: Any) -> float | str | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return str(value)
    return number if math.isfinite(number) else str(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(f"cannot JSON encode {type(value).__name__}")


def _upstox_provider_factory(settings: RealtimeSettings) -> RealtimeMarketDataProvider:
    from nse_research.realtime.upstox import create_upstox_provider

    return create_upstox_provider(settings)


def _jugaad_data_provider_factory(settings: RealtimeSettings) -> RealtimeMarketDataProvider:
    from nse_research.realtime.jugaad import JugaadDataProvider

    return JugaadDataProvider()


def _groww_feed_provider_factory(settings: RealtimeSettings) -> RealtimeMarketDataProvider:
    from nse_research.realtime.groww import GrowwFeedProvider

    return GrowwFeedProvider(
        instruments_csv=os.environ.get("GROWW_INSTRUMENTS_CSV", ""),
        symbols=settings.symbols,
        max_quote_age_seconds=settings.max_quote_age_seconds,
    )


register_market_data_provider("upstox", _upstox_provider_factory)
register_market_data_provider("jugaad_data", _jugaad_data_provider_factory)
register_market_data_provider("groww_feed", _groww_feed_provider_factory)
