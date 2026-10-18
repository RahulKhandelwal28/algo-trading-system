"""Read-only OpenAlgo historical-candle client for a bounded feed smoke test.

This module intentionally exposes only instrument search and the ticker
history endpoint. It has no quote, account, position, order, or broker methods.
"""

from __future__ import annotations

import json
import math
import os
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as day_time, timedelta, timezone
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]{1,32}$")
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_MIN_EPOCH_SECONDS = 946684800  # 2000-01-01 UTC
_MAX_EPOCH_SECONDS = 4102444800  # 2100-01-01 UTC


class OpenAlgoDataError(RuntimeError):
    """Sanitized error; never contains an API key or full request URL."""


@dataclass(frozen=True, slots=True)
class OpenAlgoSettings:
    base_url: str
    api_key: str = field(repr=False)
    timeout_seconds: float = 10.0
    request_interval_seconds: float = 1.0

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "OpenAlgoSettings":
        values = os.environ if environ is None else environ
        settings = cls(
            base_url=str(values.get("OPENALGO_API_BASE_URL", "")).strip().rstrip("/"),
            api_key=str(values.get("OPENALGO_API_KEY", "")).strip(),
            timeout_seconds=_env_float(values, "OPENALGO_TIMEOUT_SECONDS", 10.0),
            request_interval_seconds=_env_float(values, "OPENALGO_REQUEST_INTERVAL_SECONDS", 1.0),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.base_url:
            raise OpenAlgoDataError("OPENALGO_API_BASE_URL is not configured")
        if not self.api_key:
            raise OpenAlgoDataError("OPENALGO_API_KEY is not configured")
        try:
            parsed = urlsplit(self.base_url)
        except ValueError:
            raise OpenAlgoDataError("OpenAlgo base URL is malformed") from None
        try:
            _ = parsed.port
        except ValueError:
            raise OpenAlgoDataError("OpenAlgo base URL has an invalid port") from None
        if (not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment
                or (parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in _LOCAL_HOSTS))):
            raise OpenAlgoDataError(
                "OpenAlgo base URL must use HTTPS; plain HTTP is allowed only for localhost"
            )
        if (not math.isfinite(self.timeout_seconds) or not math.isfinite(self.request_interval_seconds)
                or self.timeout_seconds <= 0 or self.request_interval_seconds < 0):
            raise OpenAlgoDataError("OpenAlgo timeout must be positive and request interval non-negative")


@dataclass(frozen=True, slots=True)
class OpenAlgoMinuteBar:
    symbol: str
    exchange: str
    series: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True, slots=True)
class InstrumentIdentity:
    symbol: str
    exchange: str
    series: str
    instrument_token: str


HttpTransport = Callable[[str, str, bytes | None, float], tuple[int, bytes]]


class OpenAlgoReadOnlyClient:
    """Small, serial, read-only client using OpenAlgo's documented APIs."""

    source = "openalgo_ticker_history"

    def __init__(
        self,
        settings: OpenAlgoSettings,
        *,
        transport: HttpTransport | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        settings.validate()
        self.settings = settings
        self._transport = transport or _http_request
        self._sleep = sleeper
        self._clock = clock
        self._last_request_monotonic: float | None = None

    def validate_equity_symbol(self, symbol: str) -> InstrumentIdentity:
        canonical = _validate_symbol(symbol)
        payload = self._json_request(
            "POST",
            "/api/v1/search",
            body={"apikey": self.settings.api_key, "query": canonical, "exchange": "NSE"},
        )
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise OpenAlgoDataError(f"OpenAlgo symbol search returned malformed data for {canonical}")
        matches = [
            row for row in rows
            if isinstance(row, dict)
            and str(row.get("symbol", "")).strip().upper() == canonical
            and str(row.get("exchange", "")).strip().upper() == "NSE"
            and str(row.get("instrumenttype", "")).strip().upper() == "EQ"
        ]
        if len(matches) != 1:
            raise OpenAlgoDataError(
                f"{canonical}: expected exactly one explicit NSE/EQ instrument; found {len(matches)}"
            )
        row = matches[0]
        token = str(row.get("token") or "").strip()
        if not token:
            raise OpenAlgoDataError(f"{canonical}: OpenAlgo did not return an instrument token")
        return InstrumentIdentity(canonical, "NSE", "EQ", token)

    def minute_bars(self, identity: InstrumentIdentity, session_date: date) -> list[OpenAlgoMinuteBar]:
        if identity.exchange != "NSE" or identity.series != "EQ":
            raise OpenAlgoDataError("ticker request requires a previously validated NSE/EQ identity")
        if _validate_symbol(identity.symbol) != identity.symbol:
            raise OpenAlgoDataError("instrument identity symbol is not canonical")
        local_today = self._clock().astimezone(IST).date()
        if session_date > local_today:
            raise OpenAlgoDataError("requested session date is in the future")
        endpoint_symbol = quote(f"NSE:{identity.symbol}", safe=":")
        params = urlencode({
            "apikey": self.settings.api_key,
            "interval": "1m",
            "from": session_date.isoformat(),
            "to": session_date.isoformat(),
            "format": "json",
        })
        payload = self._json_request("GET", f"/api/v1/ticker/{endpoint_symbol}?{params}")
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise OpenAlgoDataError(f"{identity.symbol}: ticker response data is not a list")
        parsed = [_parse_bar(identity, row, session_date) for row in rows]
        parsed.sort(key=lambda row: row.timestamp)
        seen: set[datetime] = set()
        for row in parsed:
            if row.timestamp in seen:
                raise OpenAlgoDataError(f"{identity.symbol}: duplicate one-minute timestamp")
            seen.add(row.timestamp)
        if not parsed:
            raise OpenAlgoDataError(f"{identity.symbol}: no one-minute bars returned for {session_date.isoformat()}")

        # Be conservative with the current session: the API does not document
        # whether a timestamp labels a candle's start or close. Exclude the
        # newest minute until it is certainly complete under either convention.
        if session_date == local_today:
            now_ist = self._clock().astimezone(IST)
            cutoff = now_ist.replace(second=0, microsecond=0)
            parsed = [row for row in parsed if row.timestamp + timedelta(minutes=1) <= cutoff]
        if not parsed:
            raise OpenAlgoDataError(f"{identity.symbol}: no completed one-minute bars available yet")
        return parsed

    def _json_request(self, method: str, path: str, *, body: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self._throttle()
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        url = self.settings.base_url + path
        try:
            status, raw = self._transport(method, url, data, self.settings.timeout_seconds)
        except OpenAlgoDataError:
            raise
        except Exception as exc:
            raise OpenAlgoDataError(f"OpenAlgo request failed ({type(exc).__name__})") from None
        if status != 200:
            raise OpenAlgoDataError(f"OpenAlgo returned HTTP {status}")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            raise OpenAlgoDataError("OpenAlgo response was not valid UTF-8 JSON") from None
        if not isinstance(payload, dict):
            raise OpenAlgoDataError("OpenAlgo response had an unexpected JSON structure")
        if str(payload.get("status", "")).strip().lower() != "success":
            raise OpenAlgoDataError("OpenAlgo returned a non-success response")
        return payload

    def _throttle(self) -> None:
        interval = self.settings.request_interval_seconds
        if self._last_request_monotonic is not None and interval:
            elapsed = time.monotonic() - self._last_request_monotonic
            if elapsed < interval:
                self._sleep(interval - elapsed)
        self._last_request_monotonic = time.monotonic()


def _parse_bar(identity: InstrumentIdentity, row: Any, session_date: date) -> OpenAlgoMinuteBar:
    if not isinstance(row, dict):
        raise OpenAlgoDataError(f"{identity.symbol}: ticker row is not an object")
    raw_timestamp = row.get("timestamp")
    if isinstance(raw_timestamp, bool) or not isinstance(raw_timestamp, (int, float)):
        raise OpenAlgoDataError(f"{identity.symbol}: timestamp must be documented epoch seconds")
    if not math.isfinite(float(raw_timestamp)) or not float(raw_timestamp).is_integer():
        raise OpenAlgoDataError(f"{identity.symbol}: timestamp is not an integer epoch-second value")
    epoch = int(raw_timestamp)
    if not _MIN_EPOCH_SECONDS <= epoch < _MAX_EPOCH_SECONDS:
        raise OpenAlgoDataError(f"{identity.symbol}: timestamp is outside supported epoch-second range")
    timestamp = datetime.fromtimestamp(epoch, timezone.utc).astimezone(IST)
    if timestamp.date() != session_date:
        raise OpenAlgoDataError(f"{identity.symbol}: ticker returned a bar from a different NSE session")
    if timestamp.second != 0 or timestamp.microsecond != 0:
        raise OpenAlgoDataError(f"{identity.symbol}: one-minute bar timestamp is not minute-aligned")
    if not (day_time(9, 15) <= timestamp.timetz().replace(tzinfo=None) <= day_time(15, 30)):
        raise OpenAlgoDataError(f"{identity.symbol}: bar timestamp is outside the regular NSE session")

    numbers: dict[str, float] = {}
    for field in ("open", "high", "low", "close", "volume"):
        value = row.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise OpenAlgoDataError(f"{identity.symbol}: {field} is missing or non-numeric")
        number = float(value)
        if not math.isfinite(number):
            raise OpenAlgoDataError(f"{identity.symbol}: {field} is not finite")
        numbers[field] = number
    if min(numbers[name] for name in ("open", "high", "low", "close")) <= 0:
        raise OpenAlgoDataError(f"{identity.symbol}: OHLC prices must be positive")
    if numbers["volume"] < 0:
        raise OpenAlgoDataError(f"{identity.symbol}: volume must be non-negative")
    if (numbers["high"] < max(numbers["open"], numbers["close"], numbers["low"])
            or numbers["low"] > min(numbers["open"], numbers["close"], numbers["high"])):
        raise OpenAlgoDataError(f"{identity.symbol}: OHLC relationships are invalid")
    return OpenAlgoMinuteBar(identity.symbol, "NSE", "EQ", timestamp, **numbers)


def _validate_symbol(symbol: str) -> str:
    canonical = str(symbol or "").strip().upper()
    if not _SYMBOL_RE.fullmatch(canonical):
        raise OpenAlgoDataError("symbol is empty or malformed")
    return canonical


def _env_float(values: Mapping[str, str], name: str, default: float) -> float:
    try:
        return float(values.get(name, str(default)))
    except (TypeError, ValueError):
        raise OpenAlgoDataError(f"{name} must be numeric") from None


def _http_request(method: str, url: str, body: bytes | None, timeout: float) -> tuple[int, bytes]:
    headers = {"Accept": "application/json", "User-Agent": "nse-research-readonly/1.0"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    request = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read()
    except HTTPError as exc:
        return int(exc.code), b""
    except (URLError, TimeoutError, OSError) as exc:
        raise OpenAlgoDataError(f"OpenAlgo connection failed ({type(exc).__name__})") from None
