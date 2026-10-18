"""Read-only real-time quote providers, isolated from historical OHLCV data.

The current Indian Stock Market API response does not expose a quote-time
timestamp with time-of-day precision. Its top-level ``timestamp`` is generated
when the API responds, so it must never be treated as the market-data time.
Such responses are retained for health and diagnostics but are marked
``UNVERIFIABLE_TIMESTAMP`` and excluded from strategy-eligible quotes.
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


SOURCE_NAME = "indian_stock_market_api"
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")


class QuoteValidity:
    """String values used to explain whether a quote can reach a strategy."""

    VALID = "VALID"
    STALE_DATA = "STALE_DATA"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    NO_DATA = "NO_DATA"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_NUMERIC = "INVALID_NUMERIC"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    UNVERIFIABLE_TIMESTAMP = "UNVERIFIABLE_TIMESTAMP"
    WRONG_SYMBOL = "WRONG_SYMBOL"
    WRONG_EXCHANGE = "WRONG_EXCHANGE"
    PROVIDER_ERROR = "PROVIDER_ERROR"
    DATA_SOURCE_NOT_CONFIGURED = "DATA_SOURCE_NOT_CONFIGURED"
    DATA_SOURCE_NOT_SUITABLE = "DATA_SOURCE_NOT_SUITABLE"
    QUOTE_STALE = "QUOTE_STALE"
    QUOTE_TIMESTAMP_INVALID = "QUOTE_TIMESTAMP_INVALID"
    QUOTE_PRICE_INVALID = "QUOTE_PRICE_INVALID"
    QUOTE_SERIES_INVALID = "QUOTE_SERIES_INVALID"
    QUOTE_SYMBOL_INVALID = "QUOTE_SYMBOL_INVALID"
    QUOTE_DUPLICATE = "QUOTE_DUPLICATE"


@dataclass(frozen=True)
class NormalizedQuote:
    symbol: str
    exchange: str
    last_price: float | None
    change: float | None
    change_percent: float | None
    volume: float | None
    market_cap: float | None
    timestamp: datetime | None
    source: str
    received_at: datetime
    data_age_seconds: float | None
    validity_status: str
    reason: str | None = None
    # This is the API response timestamp, not the quote timestamp.
    api_timestamp: datetime | None = None
    # The upstream single-quote route only returns this date, without a time.
    source_quote_date: date | None = None
    # Series is required by the generic intraday validation gate. The current
    # investigated public adapter does not establish it and stays ineligible.
    series: str | None = None
    tick_id: str | None = None
    # Candle volume is only aggregated when the adapter explicitly identifies
    # this value as an incremental trade-volume observation.
    volume_kind: str | None = None
    # Optional exchange/provider identity and timestamp provenance for streams.
    instrument_key: str | None = None
    source_timestamp_raw: str | None = None
    source_timestamp_unit: str | None = None
    source_timestamp_origin: str | None = None
    local_received_timestamp_utc: datetime | None = None
    last_trade_quantity: int | None = None
    feed_current_timestamp_raw: str | None = None
    feed_current_timestamp_utc: datetime | None = None
    feed_message_type: str | None = None
    latency_ms: float | None = None

    @property
    def is_valid(self) -> bool:
        return self.validity_status == QuoteValidity.VALID

    @property
    def source_timestamp(self) -> datetime | None:
        """Exchange/source time, distinct from the local ``received_at`` time."""
        return self.timestamp

    @property
    def quote_age_seconds(self) -> float | None:
        return self.data_age_seconds


@dataclass(frozen=True)
class QuoteProviderHealth:
    provider_status: str
    last_successful_request: datetime | None
    last_request_latency_ms: float | None
    average_request_latency_ms: float | None
    consecutive_failures: int
    stale_quote_count: int
    invalid_quote_count: int
    symbols_successfully_retrieved: tuple[str, ...]
    symbols_failed: tuple[str, ...]
    data_age_seconds: float | None
    last_http_status: int | None
    circuit_open_until: datetime | None


@dataclass(frozen=True)
class _HttpResponse:
    status: int
    body: bytes


@dataclass(frozen=True)
class RealtimeProviderSettings:
    provider: str = "disabled"
    base_url: str = ""
    max_quote_age_seconds: float = 120.0
    batch_size: int = 5
    timeout_seconds: float = 5.0
    max_retries: int = 2
    request_interval_seconds: float = 1.0
    circuit_failure_threshold: int = 3
    circuit_cooldown_seconds: float = 60.0

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "RealtimeProviderSettings":
        values = os.environ if environ is None else environ
        settings = cls(
            provider=values.get("REALTIME_PROVIDER", "disabled").strip().lower(),
            base_url=values.get("REALTIME_API_BASE_URL", "").strip(),
            max_quote_age_seconds=_env_float(values, "REALTIME_MAX_QUOTE_AGE_SECONDS", 120.0),
            batch_size=_env_int(values, "REALTIME_BATCH_SIZE", 5),
            timeout_seconds=_env_float(values, "REALTIME_TIMEOUT_SECONDS", 5.0),
            max_retries=_env_int(values, "REALTIME_MAX_RETRIES", 2),
            request_interval_seconds=_env_float(values, "REALTIME_REQUEST_INTERVAL_SECONDS", 1.0),
            circuit_failure_threshold=_env_int(values, "REALTIME_CIRCUIT_FAILURE_THRESHOLD", 3),
            circuit_cooldown_seconds=_env_float(values, "REALTIME_CIRCUIT_COOLDOWN_SECONDS", 60.0),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.provider not in {"disabled", SOURCE_NAME, "jugaad_data"}:
            raise ValueError(f"unsupported REALTIME_PROVIDER: {self.provider}")
        if self.max_quote_age_seconds <= 0:
            raise ValueError("REALTIME_MAX_QUOTE_AGE_SECONDS must be positive")
        if not 1 <= self.batch_size <= 25:
            raise ValueError("REALTIME_BATCH_SIZE must be between 1 and 25")
        if self.timeout_seconds <= 0:
            raise ValueError("REALTIME_TIMEOUT_SECONDS must be positive")
        if self.max_retries < 0 or self.max_retries > 5:
            raise ValueError("REALTIME_MAX_RETRIES must be between 0 and 5")
        if self.request_interval_seconds < 0:
            raise ValueError("REALTIME_REQUEST_INTERVAL_SECONDS cannot be negative")
        if self.circuit_failure_threshold < 1:
            raise ValueError("REALTIME_CIRCUIT_FAILURE_THRESHOLD must be positive")
        if self.circuit_cooldown_seconds <= 0:
            raise ValueError("REALTIME_CIRCUIT_COOLDOWN_SECONDS must be positive")
        if self.provider == SOURCE_NAME:
            if not self.base_url:
                raise ValueError("REALTIME_API_BASE_URL is required when the provider is enabled")
            parsed = urlsplit(self.base_url)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError("REALTIME_API_BASE_URL must be an HTTPS URL")


class RealtimeDataProvider(ABC):
    """Substitutable interface for live and future replay quote sources.

    Implementations normalize records into :class:`NormalizedQuote`, keeping
    exchange source time separate from local receipt time. Intraday consumers
    additionally require an explicit EQ series before accepting a quote.
    """

    @abstractmethod
    def get_quote(self, symbol: str, exchange: str = "NSE") -> NormalizedQuote:
        """Fetch, normalize, and validate one quote."""

    @abstractmethod
    def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        """Fetch, normalize, and validate quotes in configured batches."""

    @abstractmethod
    def health(self) -> QuoteProviderHealth:
        """Return a read-only snapshot of provider and data health."""


class IndianStockMarketAPIProvider(RealtimeDataProvider):
    """Read-only adapter for the public Indian Stock Market API.

    ``http_get`` and clock/sleeper functions are injectable for deterministic
    unit tests. No response is cached by this adapter. ``get_quotes`` makes
    serial requests and splits requests at ``batch_size``.
    """

    def __init__(
        self,
        base_url: str,
        *,
        max_quote_age_seconds: float = 120.0,
        batch_size: int = 5,
        timeout_seconds: float = 5.0,
        max_retries: int = 2,
        request_interval_seconds: float = 1.0,
        circuit_failure_threshold: int = 3,
        circuit_cooldown_seconds: float = 60.0,
        max_future_skew_seconds: float = 5.0,
        http_get: Callable[[str, float], _HttpResponse] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        utcnow: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.settings = RealtimeProviderSettings(
            provider=SOURCE_NAME,
            base_url=base_url.rstrip("/"),
            max_quote_age_seconds=max_quote_age_seconds,
            batch_size=batch_size,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            request_interval_seconds=request_interval_seconds,
            circuit_failure_threshold=circuit_failure_threshold,
            circuit_cooldown_seconds=circuit_cooldown_seconds,
        )
        self.settings.validate()
        if max_future_skew_seconds < 0:
            raise ValueError("max_future_skew_seconds cannot be negative")
        self.max_future_skew_seconds = max_future_skew_seconds
        self._http_get = http_get or _http_get
        self._clock = clock
        self._sleep = sleeper
        self._utcnow = utcnow
        self._lock = threading.RLock()
        self._last_request_monotonic: float | None = None
        self._last_successful_request: datetime | None = None
        self._last_request_latency_ms: float | None = None
        self._latency_total_ms = 0.0
        self._latency_count = 0
        self._consecutive_failures = 0
        self._circuit_open_until_monotonic: float | None = None
        self._last_http_status: int | None = None
        self._stale_quote_count = 0
        self._invalid_quote_count = 0
        self._symbols_successful: tuple[str, ...] = ()
        self._symbols_failed: tuple[str, ...] = ()
        self._latest_data_age_seconds: float | None = None
        self._latest_response_degraded = False

    def get_quote(self, symbol: str, exchange: str = "NSE") -> NormalizedQuote:
        requested_symbol, expected_exchange, ticker = _normalize_requested_symbol(symbol, exchange)
        path = "/stock?" + urlencode({"symbol": ticker, "res": "num"})
        payload = self._fetch_json(path)
        if payload is None:
            quote = self._failed_quote(requested_symbol, expected_exchange, "provider request failed")
        elif str(payload.get("status", "")).lower() != "success":
            quote = self._no_data_quote(requested_symbol, expected_exchange, payload.get("message", "provider returned no data"))
        else:
            raw = payload.get("data")
            if not isinstance(raw, dict):
                quote = self._failed_quote(requested_symbol, expected_exchange, "single-quote response has no data object")
            else:
                quote = self._normalize_quote(
                    requested_symbol,
                    expected_exchange,
                    raw,
                    api_symbol=payload.get("symbol"),
                    api_exchange=payload.get("exchange"),
                    api_timestamp=raw.get("timestamp"),
                    source_quote_date=raw.get("last_update"),
                )
        self._record_quotes([quote], [requested_symbol])
        return quote

    def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        normalized = [_normalize_requested_symbol(symbol, "NSE") for symbol in symbols]
        if not normalized:
            return []
        output: list[NormalizedQuote] = []
        requested_names = [item[0] for item in normalized]
        for offset in range(0, len(normalized), self.settings.batch_size):
            batch = normalized[offset : offset + self.settings.batch_size]
            tickers = [item[2] for item in batch]
            path = "/stock/list?" + urlencode({"symbols": ",".join(tickers), "res": "num"})
            payload = self._fetch_json(path)
            if payload is None:
                output.extend(self._failed_quote(item[0], item[1], "provider request failed") for item in batch)
                continue
            rows = payload.get("stocks") if str(payload.get("status", "")).lower() == "success" else None
            if not isinstance(rows, list):
                reason = payload.get("message", "batch response has no stocks list")
                output.extend(self._failed_quote(item[0], item[1], str(reason)) for item in batch)
                continue
            api_timestamp = payload.get("timestamp")
            by_ticker: dict[str, dict[str, Any]] = {}
            for row in rows:
                if isinstance(row, dict):
                    row_ticker = str(row.get("ticker") or "").strip().upper()
                    if row_ticker:
                        by_ticker[row_ticker] = row
            for requested_symbol, expected_exchange, ticker in batch:
                row = by_ticker.get(ticker)
                if row is None:
                    output.append(self._failed_quote(requested_symbol, expected_exchange, "requested symbol missing from batch response"))
                elif row.get("error"):
                    output.append(self._no_data_quote(requested_symbol, expected_exchange, str(row.get("error"))))
                else:
                    output.append(self._normalize_quote(
                        requested_symbol,
                        expected_exchange,
                        row,
                        api_symbol=row.get("symbol"),
                        api_exchange=row.get("exchange"),
                        api_timestamp=api_timestamp,
                    ))
        self._record_quotes(output, requested_names)
        return output

    def health(self) -> QuoteProviderHealth:
        now_mono = self._clock()
        now = self._utcnow()
        with self._lock:
            circuit_open = (
                self._circuit_open_until_monotonic is not None
                and now_mono < self._circuit_open_until_monotonic
            )
            if circuit_open:
                status = "CIRCUIT_OPEN"
            elif self._consecutive_failures >= self.settings.circuit_failure_threshold:
                status = "DOWN"
            elif self._last_successful_request is None:
                status = "UNKNOWN"
            elif self._latest_response_degraded:
                status = "DEGRADED"
            else:
                status = "UP"
            opened_until = None
            if circuit_open:
                remaining = self._circuit_open_until_monotonic - now_mono
                opened_until = now + timedelta(seconds=max(0.0, remaining))
            avg = self._latency_total_ms / self._latency_count if self._latency_count else None
            return QuoteProviderHealth(
                provider_status=status,
                last_successful_request=self._last_successful_request,
                last_request_latency_ms=self._last_request_latency_ms,
                average_request_latency_ms=avg,
                consecutive_failures=self._consecutive_failures,
                stale_quote_count=self._stale_quote_count,
                invalid_quote_count=self._invalid_quote_count,
                symbols_successfully_retrieved=self._symbols_successful,
                symbols_failed=self._symbols_failed,
                data_age_seconds=self._latest_data_age_seconds,
                last_http_status=self._last_http_status,
                circuit_open_until=opened_until,
            )

    def _fetch_json(self, path: str) -> dict[str, Any] | None:
        url = f"{self.settings.base_url}{path}"
        for attempt in range(self.settings.max_retries + 1):
            if not self._wait_for_request_slot():
                return None
            started = self._clock()
            try:
                response = self._http_get(url, self.settings.timeout_seconds)
            except Exception:
                response = None
            elapsed = max(0.0, (self._clock() - started) * 1000.0)
            if response is None:
                status = None
                payload = None
                retryable = True
            else:
                status = response.status
                try:
                    decoded = json.loads(response.body.decode("utf-8"))
                    payload = decoded if isinstance(decoded, dict) else None
                except (UnicodeDecodeError, json.JSONDecodeError):
                    payload = None
                retryable = status is None or status == 429 or status >= 500 or (
                    200 <= status < 300 and payload is None
                )
            with self._lock:
                self._last_http_status = status
                self._last_request_latency_ms = elapsed
                self._latency_total_ms += elapsed
                self._latency_count += 1
            if response is not None and status is not None and 200 <= status < 300 and payload is not None:
                self._mark_request_success()
                return payload
            if response is not None and status in {400, 404}:
                # These are returned by the API for invalid input/no-data cases;
                # they do not count as a transport/provider outage.
                self._mark_request_success()
                return payload or {"status": "error", "message": f"HTTP {status}: no data response"}
            if not retryable:
                break
            if attempt < self.settings.max_retries:
                self._sleep(min(0.25 * (2**attempt), 2.0))
        self._mark_request_failure()
        return None

    def _wait_for_request_slot(self) -> bool:
        with self._lock:
            now = self._clock()
            if self._circuit_open_until_monotonic is not None:
                if now < self._circuit_open_until_monotonic:
                    return False
                self._circuit_open_until_monotonic = None
            if self._last_request_monotonic is None:
                self._last_request_monotonic = now
                return True
            delay = self.settings.request_interval_seconds - (now - self._last_request_monotonic)
            if delay > 0:
                self._sleep(delay)
                now = self._clock()
            self._last_request_monotonic = now
            return True

    def _mark_request_success(self) -> None:
        now = self._utcnow()
        with self._lock:
            self._last_successful_request = now
            self._consecutive_failures = 0
            self._circuit_open_until_monotonic = None

    def _mark_request_failure(self) -> None:
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.settings.circuit_failure_threshold:
                self._circuit_open_until_monotonic = self._clock() + self.settings.circuit_cooldown_seconds

    def _normalize_quote(
        self,
        requested_symbol: str,
        expected_exchange: str,
        raw: Mapping[str, Any],
        *,
        api_symbol: Any = None,
        api_exchange: Any = None,
        api_timestamp: Any = None,
        source_quote_date: Any = None,
    ) -> NormalizedQuote:
        received_at = _as_utc(self._utcnow())
        symbol = str(api_symbol or raw.get("symbol") or requested_symbol).strip().upper()
        exchange = str(api_exchange or raw.get("exchange") or expected_exchange).strip().upper()
        if symbol.endswith((".NS", ".BO")):
            symbol = symbol[:-3]
        try:
            last_price = _optional_number(raw.get("last_price"), "last_price")
            change = _optional_number(raw.get("change"), "change")
            change_percent = _optional_number(raw.get("percent_change", raw.get("change_percent")), "percent_change")
            volume = _optional_number(raw.get("volume"), "volume")
            market_cap = _optional_number(raw.get("market_cap"), "market_cap")
        except ValueError as exc:
            return self._quote(
                requested_symbol, exchange, None, None, None, None, None,
                received_at, QuoteValidity.INVALID_NUMERIC, str(exc),
                api_timestamp=api_timestamp, source_quote_date=source_quote_date,
            )
        parsed_api_timestamp = _parse_datetime(api_timestamp, naive_is_utc=True)
        quote_timestamp_value = _quote_time_value(raw)
        source_date = _parse_date(source_quote_date)
        quote_timestamp = _parse_datetime(quote_timestamp_value, naive_is_utc=False)

        if symbol != requested_symbol:
            status, reason = QuoteValidity.WRONG_SYMBOL, f"provider returned {symbol} for {requested_symbol}"
        elif exchange != expected_exchange:
            status, reason = QuoteValidity.WRONG_EXCHANGE, f"provider returned {exchange} for {expected_exchange} request"
        elif last_price is None or last_price <= 0:
            status, reason = QuoteValidity.INVALID_PRICE, "last_price must be a finite positive number"
        elif volume is not None and volume < 0:
            status, reason = QuoteValidity.INVALID_NUMERIC, "volume cannot be negative"
        elif market_cap is not None and market_cap < 0:
            status, reason = QuoteValidity.INVALID_NUMERIC, "market_cap cannot be negative"
        elif quote_timestamp_value is not None and quote_timestamp is None:
            status, reason = QuoteValidity.INVALID_TIMESTAMP, "quote timestamp could not be parsed"
        elif quote_timestamp is None:
            # In this upstream API ``data.timestamp`` and the batch-level
            # ``timestamp`` are generated by the server at response time.
            status, reason = QuoteValidity.UNVERIFIABLE_TIMESTAMP, "provider response has no quote timestamp with time-of-day precision"
        else:
            age = (received_at - quote_timestamp).total_seconds()
            if age < -self.max_future_skew_seconds:
                status, reason = QuoteValidity.INVALID_TIMESTAMP, "quote timestamp is implausibly far in the future"
            elif age > self.settings.max_quote_age_seconds:
                status, reason = QuoteValidity.STALE_DATA, f"quote is {age:.1f}s old; limit is {self.settings.max_quote_age_seconds:.1f}s"
            else:
                status, reason = QuoteValidity.VALID, None
        age_seconds = None if quote_timestamp is None else max(0.0, (received_at - quote_timestamp).total_seconds())
        return self._quote(
            requested_symbol, exchange, last_price, change, change_percent, volume, market_cap,
            received_at, status, reason, timestamp=quote_timestamp, data_age_seconds=age_seconds,
            api_timestamp=parsed_api_timestamp, source_quote_date=source_date,
            series=raw.get("series"), tick_id=raw.get("tick_id"), volume_kind=raw.get("volume_kind"),
        )

    def _quote(
        self,
        symbol: str,
        exchange: str,
        last_price: float | None,
        change: float | None,
        change_percent: float | None,
        volume: float | None,
        market_cap: float | None,
        received_at: datetime,
        status: str,
        reason: str | None,
        *,
        timestamp: datetime | None = None,
        data_age_seconds: float | None = None,
        api_timestamp: datetime | None = None,
        source_quote_date: date | None = None,
        series: Any = None,
        tick_id: Any = None,
        volume_kind: Any = None,
    ) -> NormalizedQuote:
        return NormalizedQuote(
            symbol=symbol,
            exchange=exchange,
            last_price=last_price,
            change=change,
            change_percent=change_percent,
            volume=volume,
            market_cap=market_cap,
            timestamp=timestamp,
            source=SOURCE_NAME,
            received_at=received_at,
            data_age_seconds=data_age_seconds,
            validity_status=status,
            reason=reason,
            api_timestamp=api_timestamp,
            source_quote_date=source_quote_date,
            series=str(series).strip().upper() if series is not None and str(series).strip() else None,
            tick_id=str(tick_id) if tick_id is not None else None,
            volume_kind=str(volume_kind).strip().upper() if volume_kind is not None and str(volume_kind).strip() else None,
        )

    def _no_data_quote(self, symbol: str, exchange: str, reason: Any) -> NormalizedQuote:
        return self._quote(symbol, exchange, None, None, None, None, None, _as_utc(self._utcnow()),
                           QuoteValidity.NO_DATA, str(reason))

    def _failed_quote(self, symbol: str, exchange: str, reason: str) -> NormalizedQuote:
        return self._quote(symbol, exchange, None, None, None, None, None, _as_utc(self._utcnow()),
                           QuoteValidity.PROVIDER_ERROR, reason)

    def _record_quotes(self, quotes: list[NormalizedQuote], requested: list[str]) -> None:
        valid = tuple(quote.symbol for quote in quotes if quote.is_valid)
        failed = tuple(quote.symbol for quote in quotes if not quote.is_valid)
        with self._lock:
            self._symbols_successful = valid
            self._symbols_failed = failed
            self._stale_quote_count += sum(q.validity_status == QuoteValidity.STALE_DATA for q in quotes)
            self._invalid_quote_count += sum(q.validity_status not in {QuoteValidity.VALID, QuoteValidity.STALE_DATA} for q in quotes)
            ages = [q.data_age_seconds for q in quotes if q.data_age_seconds is not None]
            self._latest_data_age_seconds = max(ages) if ages else None
            self._latest_response_degraded = len(valid) != len(requested)


def create_realtime_provider(
    environ: Mapping[str, str] | None = None,
    **provider_overrides: Any,
) -> RealtimeDataProvider | None:
    """Build the optional quote adapter; default is deliberately disabled."""

    settings = RealtimeProviderSettings.from_env(environ)
    if settings.provider == "disabled":
        return None
    if settings.provider == "jugaad_data":
        from nse_research.realtime.jugaad import JugaadDataProvider

        return JugaadDataProvider(**provider_overrides)
    return IndianStockMarketAPIProvider(
        settings.base_url,
        max_quote_age_seconds=settings.max_quote_age_seconds,
        batch_size=settings.batch_size,
        timeout_seconds=settings.timeout_seconds,
        max_retries=settings.max_retries,
        request_interval_seconds=settings.request_interval_seconds,
        circuit_failure_threshold=settings.circuit_failure_threshold,
        circuit_cooldown_seconds=settings.circuit_cooldown_seconds,
        **provider_overrides,
    )


def signal_eligible_quotes(quotes: list[NormalizedQuote]) -> list[NormalizedQuote]:
    """Return only quotes that passed identity, numeric, and freshness checks."""

    return [quote for quote in quotes if quote.is_valid]


def _http_get(url: str, timeout: float) -> _HttpResponse:
    request = Request(url, headers={"Accept": "application/json", "User-Agent": "nse-equity-research/0.1"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return _HttpResponse(response.status, response.read(1_000_000))
    except HTTPError as response:
        return _HttpResponse(response.code, response.read(100_000))
    except URLError:
        raise


def _normalize_requested_symbol(symbol: str, exchange: str) -> tuple[str, str, str]:
    raw = str(symbol).strip().upper()
    if not raw or not _SYMBOL_RE.fullmatch(raw) or "," in raw:
        raise ValueError(f"invalid stock symbol: {symbol!r}")
    if raw.endswith(".NS"):
        raw, exchange = raw[:-3], "NSE"
    elif raw.endswith(".BO"):
        raw, exchange = raw[:-3], "BSE"
    exchange = str(exchange).strip().upper()
    if exchange not in {"NSE", "BSE"}:
        raise ValueError("exchange must be NSE or BSE")
    return raw, exchange, raw + (".NS" if exchange == "NSE" else ".BO")


def _optional_number(value: Any, field_name: str) -> float | None:
    if value is None or (isinstance(value, str) and value.strip().upper() in {"", "N/A", "NA", "NULL", "NONE"}):
        return None
    if isinstance(value, dict):
        raise ValueError(f"{field_name} was unit-wrapped; numeric-only response was expected")
    if isinstance(value, str):
        value = value.replace(",", "").strip()
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} is not numeric") from None
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be finite")
    return number


def _quote_time_value(raw: Mapping[str, Any]) -> Any:
    for key in ("quote_timestamp", "last_trade_time", "last_updated_at", "regular_market_time"):
        if raw.get(key) not in (None, ""):
            return raw[key]
    # The current API emits only a date here. Accept a timestamp only if a
    # future version includes a time component; a date alone is not fresh enough.
    value = raw.get("last_update")
    if isinstance(value, str) and re.search(r"\d[T ]\d{2}:\d{2}", value):
        return value
    return None


def _parse_datetime(value: Any, *, naive_is_utc: bool) -> datetime | None:
    if value is None or value == "" or (isinstance(value, str) and value.upper() == "N/A"):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except (TypeError, ValueError):
        try:
            parsed = parsedate_to_datetime(str(value))
        except (TypeError, ValueError, OverflowError):
            return None
    if parsed.tzinfo is None:
        if not naive_is_utc:
            return None
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_date(value: Any) -> date | None:
    if value is None or value == "" or (isinstance(value, str) and value.upper() == "N/A"):
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _env_float(values: Mapping[str, str], key: str, default: float) -> float:
    try:
        return float(values.get(key, default))
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be numeric") from None


def _env_int(values: Mapping[str, str], key: str, default: int) -> int:
    try:
        return int(values.get(key, default))
    except (TypeError, ValueError):
        raise ValueError(f"{key} must be an integer") from None
