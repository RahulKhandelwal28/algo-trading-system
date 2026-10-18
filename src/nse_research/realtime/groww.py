"""Read-only Groww streaming market-data adapter for NSE cash EQ symbols.

Only Groww's documented LTP subscription is used here. The adapter contains no
order, portfolio, or account calls. It requires an explicit Groww instrument
master so NSE/EQ identity is established before a quote reaches the scanner.
"""

from __future__ import annotations

import csv
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from nse_research.realtime.providers import (
    NormalizedQuote,
    QuoteProviderHealth,
    QuoteValidity,
    RealtimeDataProvider,
)


PROVIDER_NAME = "groww_feed"
SOURCE_NAME = "groww_live_feed"
REQUIRED_COLUMNS = frozenset({
    "exchange", "exchange_token", "trading_symbol", "instrument_type",
    "segment", "series", "buy_allowed",
})


class GrowwFeedError(RuntimeError):
    """Sanitized provider error; never includes the Groww access token."""


@dataclass(frozen=True, slots=True)
class GrowwEquityInstrument:
    symbol: str
    exchange_token: str
    exchange: str = "NSE"
    segment: str = "CASH"
    series: str = "EQ"


def load_groww_eq_instruments(path: str | Path) -> dict[str, GrowwEquityInstrument]:
    """Read unique NSE/CASH/EQ identities from Groww's official instruments CSV."""
    source = Path(path).expanduser()
    if not source.is_file():
        raise GrowwFeedError(f"Groww instrument master does not exist: {source}")
    try:
        with source.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            columns = {str(name or "").strip().lower() for name in (reader.fieldnames or [])}
            missing = sorted(REQUIRED_COLUMNS - columns)
            if missing:
                raise GrowwFeedError(f"Groww instrument master is missing columns: {missing}")
            instruments: dict[str, GrowwEquityInstrument] = {}
            for raw in reader:
                row = {str(key or "").strip().lower(): str(value or "").strip() for key, value in raw.items()}
                if (row.get("exchange", "").upper() != "NSE"
                        or row.get("segment", "").upper() != "CASH"
                        or row.get("instrument_type", "").upper() != "EQ"
                        or row.get("series", "").upper() != "EQ"):
                    continue
                symbol = row.get("trading_symbol", "").upper()
                token = row.get("exchange_token", "")
                if not symbol or not token:
                    raise GrowwFeedError("Groww NSE/EQ instrument row has an empty symbol or exchange token")
                if row.get("buy_allowed", "").lower() not in {"1", "true", "yes"}:
                    continue
                if symbol in instruments:
                    raise GrowwFeedError(f"Groww NSE/EQ instrument master has duplicate symbol {symbol}")
                instruments[symbol] = GrowwEquityInstrument(symbol=symbol, exchange_token=token)
    except (OSError, UnicodeError, csv.Error) as error:
        raise GrowwFeedError(f"could not read Groww instrument master ({type(error).__name__})") from None
    if not instruments:
        raise GrowwFeedError("Groww instrument master contains no eligible NSE/CASH/EQ rows")
    return instruments


def validate_groww_configuration(
    *,
    instruments_csv: str | Path,
    symbols: Iterable[str],
    access_token: str | None = None,
) -> dict[str, Any]:
    """Local-only preflight; validates credentials' presence and mappings, no API call."""
    token_present = bool((access_token if access_token is not None else os.environ.get("GROWW_ACCESS_TOKEN", "")).strip())
    if not token_present:
        raise GrowwFeedError("GROWW_ACCESS_TOKEN is not configured")
    instruments = load_groww_eq_instruments(instruments_csv)
    requested = [str(symbol).strip().upper() for symbol in symbols]
    missing = sorted(set(requested) - set(instruments))
    if missing:
        raise GrowwFeedError(f"symbols lack an explicit NSE/CASH/EQ Groww mapping: {missing}")
    if not requested or len(set(requested)) != len(requested):
        raise GrowwFeedError("Groww preflight requires a non-empty list of unique symbols")
    return {
        "status": "GROWW_LOCAL_PREFLIGHT_READY",
        "symbols": requested,
        "mapped_symbols": len(requested),
        "instrument_master": str(Path(instruments_csv).expanduser()),
        "market_data_request_made": False,
        "order_api_called": False,
    }


class GrowwFeedProvider(RealtimeDataProvider):
    """Read a bounded, already-subscribed Groww LTP stream snapshot."""

    provider_name = PROVIDER_NAME

    def __init__(
        self,
        *,
        access_token: str | None = None,
        instruments_csv: str | Path | None = None,
        symbols: Iterable[str] | None = None,
        max_quote_age_seconds: float = 5.0,
        groww_api: Any | None = None,
        feed: Any | None = None,
        utcnow=lambda: datetime.now(timezone.utc),
        monotonic=time.monotonic,
    ) -> None:
        token = (access_token if access_token is not None else os.environ.get("GROWW_ACCESS_TOKEN", "")).strip()
        csv_path = instruments_csv or os.environ.get("GROWW_INSTRUMENTS_CSV", "")
        if not token:
            raise GrowwFeedError("GROWW_ACCESS_TOKEN is not configured")
        if not csv_path:
            raise GrowwFeedError("GROWW_INSTRUMENTS_CSV is not configured")
        if not math.isfinite(max_quote_age_seconds) or max_quote_age_seconds <= 0:
            raise ValueError("max_quote_age_seconds must be positive and finite")
        all_instruments = load_groww_eq_instruments(csv_path)
        requested = tuple(str(symbol).strip().upper() for symbol in (symbols or ()))
        if not requested or len(set(requested)) != len(requested):
            raise GrowwFeedError("Groww feed requires a non-empty list of unique NSE/EQ symbols")
        if len(requested) > 1000:
            raise GrowwFeedError("Groww feed supports at most 1000 instrument subscriptions")
        missing = sorted(set(requested) - set(all_instruments))
        if missing:
            raise GrowwFeedError(f"symbols lack an explicit Groww NSE/CASH/EQ mapping: {missing}")
        self.instruments = {symbol: all_instruments[symbol] for symbol in requested}
        self.max_quote_age_seconds = max_quote_age_seconds
        self._utcnow = utcnow
        self._monotonic = monotonic
        self._last_success: datetime | None = None
        self._last_latency_ms: float | None = None
        self._consecutive_failures = 0
        self._stale_count = 0
        self._invalid_count = 0
        self._symbols_successful: tuple[str, ...] = ()
        self._symbols_failed: tuple[str, ...] = ()
        self._last_age: float | None = None
        self._last_error: str | None = None
        self._subscribed = False
        try:
            if groww_api is None or feed is None:
                from growwapi import GrowwAPI, GrowwFeed  # optional, official Groww SDK
                groww_api = groww_api or GrowwAPI(token)
                feed = feed or GrowwFeed(groww_api)
            self._groww_api = groww_api
            self._feed = feed
            subscriptions = [
                {"exchange": item.exchange, "segment": item.segment, "exchange_token": item.exchange_token}
                for item in self.instruments.values()
            ]
            self._feed.subscribe_ltp(subscriptions)
            self._subscriptions = subscriptions
            self._subscribed = True
        except ImportError:
            raise GrowwFeedError("growwapi is not installed; run python -m pip install -r requirements-realtime.txt") from None
        except GrowwFeedError:
            raise
        except Exception as error:
            raise GrowwFeedError(f"Groww feed could not start ({type(error).__name__})") from None

    def get_quote(self, symbol: str, exchange: str = "NSE") -> NormalizedQuote:
        if str(exchange).strip().upper() != "NSE":
            return self._failure(symbol, QuoteValidity.WRONG_EXCHANGE, "Groww paper scanner accepts NSE only")
        return self.get_quotes([symbol])[0]

    def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        requested = [str(symbol or "").strip().upper() for symbol in symbols]
        if not requested:
            return []
        if len(set(requested)) != len(requested):
            raise ValueError("Groww quote symbols must be unique")
        now = self._now()
        started = self._monotonic()
        unknown = [symbol for symbol in requested if symbol not in self.instruments]
        valid_symbols = [symbol for symbol in requested if symbol in self.instruments]
        quotes: dict[str, NormalizedQuote] = {
            symbol: self._failure(symbol, QuoteValidity.INVALID_SYMBOL, "symbol has no explicit Groww NSE/EQ mapping")
            for symbol in unknown
        }
        request_error: str | None = None
        response: Any = {}
        if valid_symbols:
            try:
                response = self._feed.get_ltp()
                if not isinstance(response, Mapping):
                    raise GrowwFeedError("Groww feed returned an unexpected response structure")
                ltp_root = response.get("ltp", response)
                if not isinstance(ltp_root, Mapping):
                    raise GrowwFeedError("Groww feed returned an unexpected LTP structure")
                nse = ltp_root.get("NSE", {})
                cash = nse.get("CASH", {}) if isinstance(nse, Mapping) else {}
                if not isinstance(cash, Mapping):
                    raise GrowwFeedError("Groww feed did not return an NSE/CASH map")
                for symbol in valid_symbols:
                    instrument = self.instruments[symbol]
                    tick = cash.get(instrument.exchange_token)
                    quotes[symbol] = self._normalize(instrument, tick, now)
            except Exception as error:
                request_error = type(error).__name__
                for symbol in valid_symbols:
                    quotes[symbol] = self._failure(
                        symbol, QuoteValidity.PROVIDER_ERROR,
                        f"Groww market-data request failed ({request_error})",
                    )
        ordered = [quotes[symbol] for symbol in requested]
        self._last_latency_ms = max(0.0, (self._monotonic() - started) * 1000.0)
        accepted = [quote for quote in ordered if quote.is_valid]
        stale = [quote for quote in ordered if quote.validity_status == QuoteValidity.STALE_DATA]
        failed = [quote.symbol for quote in ordered if not quote.is_valid]
        self._symbols_successful = tuple(quote.symbol for quote in accepted)
        self._symbols_failed = tuple(failed)
        self._stale_count += len(stale)
        self._invalid_count += len(ordered) - len(accepted) - len(stale)
        if request_error or not accepted:
            self._consecutive_failures += 1
            self._last_error = f"market data unavailable ({request_error or 'no fresh mapped quotes'})"
        else:
            self._consecutive_failures = 0
            self._last_success = now
            self._last_error = None
        ages = [quote.data_age_seconds for quote in ordered if quote.data_age_seconds is not None]
        self._last_age = max(ages) if ages else None
        return ordered

    def health(self) -> QuoteProviderHealth:
        if self._consecutive_failures:
            status = "DOWN" if not self._symbols_successful else "DEGRADED"
        elif self._last_success is None:
            status = "UNKNOWN"
        elif self._symbols_failed:
            status = "DEGRADED"
        else:
            status = "UP"
        return QuoteProviderHealth(
            provider_status=status,
            last_successful_request=self._last_success,
            last_request_latency_ms=self._last_latency_ms,
            average_request_latency_ms=self._last_latency_ms,
            consecutive_failures=self._consecutive_failures,
            stale_quote_count=self._stale_count,
            invalid_quote_count=self._invalid_count,
            symbols_successfully_retrieved=self._symbols_successful,
            symbols_failed=self._symbols_failed,
            data_age_seconds=self._last_age,
            last_http_status=None,
            circuit_open_until=None,
        )

    def health_snapshot(self) -> dict[str, Any]:
        health = self.health()
        return {
            "provider": PROVIDER_NAME,
            "provider_status": health.provider_status,
            "last_successful_request": health.last_successful_request.isoformat() if health.last_successful_request else None,
            "last_request_latency_ms": health.last_request_latency_ms,
            "consecutive_failures": health.consecutive_failures,
            "stale_quote_count": health.stale_quote_count,
            "invalid_quote_count": health.invalid_quote_count,
            "symbols_successfully_retrieved": list(health.symbols_successfully_retrieved),
            "symbols_failed": list(health.symbols_failed),
            "data_age_seconds": health.data_age_seconds,
            "last_error": self._last_error,
            "orders_called": False,
        }

    def close(self) -> None:
        """Release only market-data subscriptions."""
        if self._subscribed:
            try:
                self._feed.unsubscribe_ltp(self._subscriptions)
            finally:
                self._subscribed = False

    def _normalize(self, instrument: GrowwEquityInstrument, raw: Any, received_at: datetime) -> NormalizedQuote:
        if not isinstance(raw, Mapping):
            return self._failure(instrument.symbol, QuoteValidity.NO_DATA, "Groww has no latest LTP for this token")
        price, timestamp_ms = raw.get("ltp"), raw.get("tsInMillis")
        if isinstance(price, bool) or not isinstance(price, (float, int)):
            return self._failure(instrument.symbol, QuoteValidity.INVALID_PRICE, "Groww LTP is missing or non-numeric")
        price = float(price)
        if not math.isfinite(price) or price <= 0:
            return self._failure(instrument.symbol, QuoteValidity.INVALID_PRICE, "Groww LTP must be finite and positive")
        if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, (float, int)):
            return self._failure(instrument.symbol, QuoteValidity.INVALID_TIMESTAMP, "Groww source timestamp is missing")
        timestamp_number = float(timestamp_ms)
        if not math.isfinite(timestamp_number) or timestamp_number <= 0:
            return self._failure(instrument.symbol, QuoteValidity.INVALID_TIMESTAMP, "Groww timestamp is invalid")
        source_timestamp = datetime.fromtimestamp(timestamp_number / 1000.0, tz=timezone.utc)
        age = (received_at - source_timestamp).total_seconds()
        if age < -1.0:
            validity, reason = QuoteValidity.INVALID_TIMESTAMP, "Groww source timestamp is in the future"
        elif age > self.max_quote_age_seconds:
            validity, reason = QuoteValidity.STALE_DATA, "Groww source timestamp is stale"
        else:
            validity, reason = QuoteValidity.VALID, None
        return NormalizedQuote(
            symbol=instrument.symbol,
            exchange=instrument.exchange,
            series=instrument.series,
            last_price=price,
            change=None,
            change_percent=None,
            volume=None,
            market_cap=None,
            timestamp=source_timestamp,
            source=SOURCE_NAME,
            received_at=received_at,
            data_age_seconds=max(0.0, age),
            validity_status=validity,
            reason=reason,
            tick_id=f"groww:{instrument.exchange_token}:{int(timestamp_number)}",
            volume_kind=None,
            instrument_key=instrument.exchange_token,
            source_timestamp_raw=str(timestamp_ms),
            source_timestamp_unit="epoch_milliseconds",
            source_timestamp_origin="Groww feed tsInMillis",
            local_received_timestamp_utc=received_at,
            latency_ms=self._last_latency_ms,
        )

    def _failure(self, symbol: str, status: str, reason: str) -> NormalizedQuote:
        now = self._now()
        return NormalizedQuote(
            symbol=str(symbol or "").strip().upper(), exchange="NSE", series=None,
            last_price=None, change=None, change_percent=None, volume=None, market_cap=None,
            timestamp=None, source=SOURCE_NAME, received_at=now, data_age_seconds=None,
            validity_status=status, reason=reason, source_timestamp_origin="Groww feed",
            local_received_timestamp_utc=now,
        )

    def _now(self) -> datetime:
        value = self._utcnow()
        if value.tzinfo is None or value.utcoffset() is None:
            raise GrowwFeedError("local clock must return a timezone-aware timestamp")
        return value.astimezone(timezone.utc)
