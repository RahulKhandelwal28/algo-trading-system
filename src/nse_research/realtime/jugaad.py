"""Jugaad Data adapter for read-only NSE equity quote snapshots."""

from __future__ import annotations

import math
import re
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from nse_research.realtime.providers import (
    NormalizedQuote,
    QuoteProviderHealth,
    QuoteValidity,
    RealtimeDataProvider,
)


PROVIDER_NAME = "jugaad_data"
SOURCE_NAME = "jugaad_data_nse_live"
MAX_SYMBOLS_PER_POLL = 5
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")
_IST = ZoneInfo("Asia/Kolkata")
_NSE_LIVE_INIT_LOCK = threading.Lock()


class JugaadDataProvider(RealtimeDataProvider):
    """Fetch up to five serial NSE EQ snapshots through NSELive."""

    provider_name = PROVIDER_NAME

    def __init__(
        self,
        *,
        nse_live: Any | None = None,
        utcnow=lambda: datetime.now(timezone.utc),
        min_request_interval_seconds: float = 1.0,
        monotonic=time.monotonic,
        sleeper=time.sleep,
    ) -> None:
        if min_request_interval_seconds < 0:
            raise ValueError("min_request_interval_seconds cannot be negative")
        self._client = nse_live
        self._utcnow = utcnow
        self._min_request_interval_seconds = min_request_interval_seconds
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._request_lock = threading.Lock()
        self._health_lock = threading.RLock()
        self._last_successful_request: datetime | None = None
        self._last_request_started: float | None = None
        self._last_latency_ms: float | None = None
        self._latency_total_ms = 0.0
        self._request_count = 0
        self._consecutive_failures = 0
        self._invalid_quote_count = 0
        self._symbols_successful: tuple[str, ...] = ()
        self._symbols_failed: tuple[str, ...] = ()
        self._data_age_seconds: float | None = None
        self._last_error: str | None = None

    def get_quote(self, symbol: str, exchange: str = "NSE") -> NormalizedQuote:
        if str(exchange).strip().upper() != "NSE":
            return self._failure(
                symbol, QuoteValidity.WRONG_EXCHANGE, "Jugaad Data only supports NSE"
            )
        return self.get_quotes([symbol])[0]

    def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        requested = [str(symbol or "").strip().upper() for symbol in symbols]
        if len(requested) > MAX_SYMBOLS_PER_POLL:
            raise ValueError(f"Jugaad Data is limited to {MAX_SYMBOLS_PER_POLL} symbols per poll")
        if len(set(requested)) != len(requested):
            raise ValueError("quote symbols must be unique")
        if not requested:
            return []

        started = time.monotonic()
        quotes: list[NormalizedQuote] = []
        request_errors = 0
        for symbol in requested:
            if not _SYMBOL_RE.fullmatch(symbol):
                quotes.append(self._failure(
                    symbol, QuoteValidity.INVALID_SYMBOL, "symbol is empty or malformed"
                ))
                continue
            try:
                with self._request_lock:
                    request_started = self._monotonic()
                    if self._last_request_started is not None:
                        remaining = self._min_request_interval_seconds - (
                            request_started - self._last_request_started
                        )
                        if remaining > 0:
                            self._sleeper(remaining)
                    self._last_request_started = self._monotonic()
                    quote_data = self._get_client().stock_quote(
                        symbol, market_type="N", series="EQ"
                    )
                quotes.append(self._normalize_quote(symbol, quote_data))
            except Exception as error:
                request_errors += 1
                quotes.append(self._failure(
                    symbol, QuoteValidity.PROVIDER_ERROR,
                    f"Jugaad Data request failed ({type(error).__name__})",
                ))

        self._record_health(
            quotes, request_errors, (time.monotonic() - started) * 1000
        )
        return quotes

    def health(self) -> QuoteProviderHealth:
        with self._health_lock:
            if self._consecutive_failures:
                status = "DOWN"
            elif self._last_successful_request is None:
                status = "UNKNOWN"
            elif self._symbols_failed:
                status = "DEGRADED"
            else:
                status = "UP"
            average = (
                self._latency_total_ms / self._request_count
                if self._request_count else None
            )
            return QuoteProviderHealth(
                provider_status=status,
                last_successful_request=self._last_successful_request,
                last_request_latency_ms=self._last_latency_ms,
                average_request_latency_ms=average,
                consecutive_failures=self._consecutive_failures,
                stale_quote_count=0,
                invalid_quote_count=self._invalid_quote_count,
                symbols_successfully_retrieved=self._symbols_successful,
                symbols_failed=self._symbols_failed,
                data_age_seconds=self._data_age_seconds,
                last_http_status=None,
                circuit_open_until=None,
            )

    def health_snapshot(self) -> dict[str, Any]:
        result = asdict(self.health())
        with self._health_lock:
            result["last_error"] = self._last_error
            return result

    def _now(self) -> datetime:
        value = self._utcnow()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc)

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                import requests
                import jugaad_data.nse.live as live_module
            except ImportError as error:
                raise RuntimeError(
                    "Install the optional dependency with "
                    "python -m pip install -r requirements-realtime.txt"
                ) from error

            # jugaad-data 0.35.9 does not pass a timeout to its initial page
            # request or quote GETs. Give its own Session a conservative
            # timeout while constructing the client; restore the module symbol
            # immediately so no package-global behavior is changed afterwards.
            class _BoundedSession(requests.Session):
                def request(self, method, url, **kwargs):
                    kwargs.setdefault("timeout", 5.0)
                    return super().request(method, url, **kwargs)

            with _NSE_LIVE_INIT_LOCK:
                original_session = live_module.Session
                live_module.Session = _BoundedSession
                try:
                    self._client = live_module.NSELive()
                finally:
                    live_module.Session = original_session
        return self._client

    def _normalize_quote(
        self, requested_symbol: str, data: Any
    ) -> NormalizedQuote:
        received_at = self._now()
        if not isinstance(data, Mapping):
            return self._failure(
                requested_symbol, QuoteValidity.NO_DATA, "NSE returned no quote object"
            )

        metadata = _mapping(data.get("metaData") or data.get("metadata"))
        info = _mapping(data.get("info"))
        prices = _mapping(data.get("priceInfo"))
        trades = _mapping(data.get("tradeInfo"))
        symbol = str(metadata.get("symbol") or info.get("symbol") or requested_symbol).strip().upper()
        series = str(metadata.get("series") or "").strip().upper() or None

        if symbol != requested_symbol:
            return self._quote(
                symbol, received_at, QuoteValidity.WRONG_SYMBOL,
                f"NSE returned {symbol} for {requested_symbol}", series=series,
            )
        if series != "EQ":
            return self._quote(
                requested_symbol, received_at, QuoteValidity.QUOTE_SERIES_INVALID,
                f"NSE response did not explicitly identify series EQ (received {series or 'missing'})",
                series=series,
            )

        try:
            price = _number(prices.get("lastPrice"), "lastPrice")
            change = _number(prices.get("change"), "change")
            change_percent = _number(prices.get("pChange"), "pChange")
            volume = _number(
                trades.get("totalTradedVolume", prices.get("totalTradedVolume")),
                "totalTradedVolume",
            )
        except ValueError as error:
            return self._quote(
                requested_symbol, received_at, QuoteValidity.INVALID_NUMERIC,
                str(error), series=series,
            )

        raw_timestamp = data.get("lastUpdateTime") or metadata.get("lastUpdateTime")
        source_timestamp = _parse_timestamp(raw_timestamp)
        if price is None or price <= 0:
            status, reason = QuoteValidity.INVALID_PRICE, "lastPrice must be positive"
        elif raw_timestamp in (None, "", "-"):
            status, reason = QuoteValidity.UNVERIFIABLE_TIMESTAMP, "NSE has no lastUpdateTime"
        elif source_timestamp is None:
            status, reason = QuoteValidity.INVALID_TIMESTAMP, "NSE lastUpdateTime is invalid"
        else:
            status, reason = QuoteValidity.VALID, None

        tick_id = (
            f"{source_timestamp.isoformat()}:{price}:{volume}"
            if source_timestamp is not None and price is not None else None
        )
        return self._quote(
            requested_symbol, received_at, status, reason,
            last_price=price, change=change, change_percent=change_percent,
            volume=volume, timestamp=source_timestamp, series=series,
            tick_id=tick_id,
            volume_kind="CUMULATIVE" if volume is not None else None,
            source_timestamp_raw=str(raw_timestamp) if raw_timestamp is not None else None,
        )

    def _failure(
        self, symbol: str, status: str, reason: str
    ) -> NormalizedQuote:
        return self._quote(
            str(symbol or "").strip().upper(),
            self._now(),
            status, reason,
        )

    @staticmethod
    def _quote(
        symbol: str,
        received_at: datetime,
        status: str,
        reason: str | None,
        *,
        last_price: float | None = None,
        change: float | None = None,
        change_percent: float | None = None,
        volume: float | None = None,
        timestamp: datetime | None = None,
        series: str | None = None,
        tick_id: str | None = None,
        volume_kind: str | None = None,
        source_timestamp_raw: str | None = None,
    ) -> NormalizedQuote:
        age = (
            (received_at - timestamp.astimezone(timezone.utc)).total_seconds()
            if timestamp is not None else None
        )
        return NormalizedQuote(
            symbol=symbol,
            exchange="NSE",
            last_price=last_price,
            change=change,
            change_percent=change_percent,
            volume=volume,
            market_cap=None,
            timestamp=timestamp,
            source=SOURCE_NAME,
            received_at=received_at,
            data_age_seconds=age,
            validity_status=status,
            reason=reason,
            series=series,
            tick_id=tick_id,
            volume_kind=volume_kind,
            source_timestamp_raw=source_timestamp_raw,
            source_timestamp_unit="Asia/Kolkata",
            source_timestamp_origin="NSE lastUpdateTime via jugaad-data",
            local_received_timestamp_utc=received_at,
        )

    def _record_health(
        self, quotes: list[NormalizedQuote], request_errors: int, latency_ms: float
    ) -> None:
        valid = tuple(quote.symbol for quote in quotes if quote.is_valid)
        failed = tuple(quote.symbol for quote in quotes if not quote.is_valid)
        ages = [quote.data_age_seconds for quote in quotes if quote.data_age_seconds is not None]
        with self._health_lock:
            self._last_latency_ms = max(0.0, latency_ms)
            self._latency_total_ms += self._last_latency_ms
            self._request_count += 1
            self._symbols_successful = valid
            self._symbols_failed = failed
            self._data_age_seconds = max(ages) if ages else None
            self._invalid_quote_count += len(failed)
            if request_errors == len(quotes):
                self._consecutive_failures += 1
                self._last_error = "provider request failed"
            else:
                self._consecutive_failures = 0
                self._last_successful_request = self._now()
                self._last_error = "one or more quotes failed validation" if failed else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: Any, name: str) -> float | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        result = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError(f"{name} is not numeric") from error
    if not math.isfinite(result):
        raise ValueError(f"{name} is not finite")
    return result


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip() or value.strip() == "-":
        return None
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%d-%b-%Y %H:%M:%S")
        except ValueError:
            return None
    return parsed.replace(tzinfo=_IST) if parsed.tzinfo is None else parsed
