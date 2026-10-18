"""Canonical, provider-independent intraday bars and validated storage."""

from __future__ import annotations

import csv
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Iterator, Protocol, Sequence
from zoneinfo import ZoneInfo

from nse_research.forward.calendar import NseTradingCalendar


IST = ZoneInfo("Asia/Kolkata")
TIMEFRAME_MINUTES = {"1m": 1, "5m": 5}
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")


def default_nse_calendar() -> NseTradingCalendar:
    """Load the project's existing local holiday/special-session file when present."""
    project_root = Path(__file__).resolve().parents[3]
    path = project_root / "data/paper_trading/nse_cash_holidays_2026.csv"
    return NseTradingCalendar(path if path.is_file() else None)


class BarValidationError(ValueError):
    """A normalized bar violated its identity, timestamp, or OHLCV contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class IntradayBar:
    """One interval bar. ``timestamp`` is the interval start, with an offset."""

    symbol: str
    exchange: str
    series: str
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    status: str = "CLOSED"
    source: str = "UNKNOWN"
    volume_kind: str = "UNKNOWN"
    bar_status: str = "IMMUTABLE_CLOSED"

    @property
    def trading_date(self) -> date:
        return self.timestamp.astimezone(IST).date()

    @property
    def end(self) -> datetime:
        return self.timestamp + timedelta(minutes=TIMEFRAME_MINUTES[self.timeframe])

    @property
    def identity(self) -> tuple[str, str, date, datetime]:
        return self.symbol, self.timeframe, self.trading_date, self.timestamp


def validate_intraday_bar(
    bar: IntradayBar,
    *,
    calendar: NseTradingCalendar | None = None,
    require_closed: bool = False,
) -> IntradayBar:
    """Validate and normalize a bar into Asia/Kolkata without guessing zones."""
    if not isinstance(bar.timestamp, datetime) or bar.timestamp.tzinfo is None or bar.timestamp.utcoffset() is None:
        raise BarValidationError("BAR_TIMESTAMP_INVALID", "bar timestamp must include a source timezone offset")
    symbol = str(bar.symbol or "").strip().upper()
    if not symbol or not _SYMBOL_RE.fullmatch(symbol):
        raise BarValidationError("BAR_SYMBOL_INVALID", "symbol is empty or malformed")
    exchange = str(bar.exchange or "").strip().upper()
    series = str(bar.series or "").strip().upper()
    if exchange != "NSE":
        raise BarValidationError("BAR_EXCHANGE_INVALID", "exchange must explicitly be NSE")
    if series != "EQ":
        raise BarValidationError("BAR_SERIES_INVALID", "series must explicitly be EQ")
    timeframe = str(bar.timeframe or "").strip().lower()
    if timeframe not in TIMEFRAME_MINUTES:
        raise BarValidationError("BAR_TIMEFRAME_INVALID", "timeframe must be 1m or 5m")
    status = str(bar.status or "").strip().upper()
    if status not in {"CLOSED", "FORMING"}:
        raise BarValidationError("BAR_STATUS_INVALID", "status must be CLOSED or FORMING")
    if require_closed and status != "CLOSED":
        raise BarValidationError("BAR_NOT_CLOSED", "scanner evaluation requires a CLOSED bar")
    bar_status = str(bar.bar_status or "").strip().upper()
    if require_closed and bar_status != "IMMUTABLE_CLOSED":
        raise BarValidationError("BAR_NOT_IMMUTABLE", "scanner evaluation requires an IMMUTABLE_CLOSED bar")

    local_timestamp = bar.timestamp.astimezone(IST)
    if local_timestamp.second or local_timestamp.microsecond:
        raise BarValidationError("BAR_TIMESTAMP_ALIGNMENT_INVALID", "bar start must align to a whole minute")
    if timeframe == "5m" and (local_timestamp.minute - 15) % 5:
        raise BarValidationError("BAR_TIMESTAMP_ALIGNMENT_INVALID", "5m bars must align to the NSE 09:15 session anchor")
    session_open = datetime.combine(local_timestamp.date(), time(9, 15), tzinfo=IST)
    session_close = datetime.combine(local_timestamp.date(), time(15, 30), tzinfo=IST)
    bar_end = local_timestamp + timedelta(minutes=TIMEFRAME_MINUTES[timeframe])
    if local_timestamp < session_open or bar_end > session_close:
        raise BarValidationError("BAR_OUTSIDE_SESSION", "bar must be wholly within the 09:15–15:30 NSE cash session")
    session = (calendar or default_nse_calendar()).intraday_status(
        local_timestamp,
        timezone_name="Asia/Kolkata",
        market_open=time(9, 15),
        market_close=time(15, 30),
        observed_market_data=True,
    )
    if session.status != "OPEN":
        raise BarValidationError("BAR_SESSION_INVALID", f"NSE calendar rejected session: {session.status} ({session.reason})")

    try:
        prices = [float(value) for value in (bar.open, bar.high, bar.low, bar.close)]
    except (TypeError, ValueError, OverflowError) as exc:
        raise BarValidationError("BAR_OHLC_INVALID", "OHLC fields must be numeric") from exc
    if any(not math.isfinite(value) or value <= 0 for value in prices):
        raise BarValidationError("BAR_PRICE_INVALID", "OHLC prices must be finite and positive")
    open_price, high, low, close = prices
    if high < max(open_price, close) or low > min(open_price, close) or high < low:
        raise BarValidationError("BAR_OHLC_RELATION_INVALID", "bar must satisfy high >= max(open, close), low <= min(open, close), high >= low")
    volume = bar.volume
    if volume is not None:
        try:
            volume = float(volume)
        except (TypeError, ValueError, OverflowError) as exc:
            raise BarValidationError("BAR_VOLUME_INVALID", "volume must be numeric or unavailable") from exc
        if not math.isfinite(volume) or volume < 0:
            raise BarValidationError("BAR_VOLUME_INVALID", "volume must be finite and non-negative")
    volume_kind = str(bar.volume_kind or "UNKNOWN").strip().upper()
    if volume_kind not in {"BAR", "INCREMENTAL", "CUMULATIVE", "LTQ", "UNKNOWN"}:
        raise BarValidationError("BAR_VOLUME_KIND_INVALID", "unsupported volume semantics")
    source = str(bar.source or "").strip()
    if not source:
        raise BarValidationError("BAR_SOURCE_MISSING", "source label is required")
    return IntradayBar(
        symbol=symbol,
        exchange=exchange,
        series=series,
        timeframe=timeframe,
        timestamp=local_timestamp,
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        status=status,
        source=source,
        volume_kind=volume_kind,
        bar_status=bar_status,
    )


def intraday_bar_from_realtime_candle(candle, *, exchange: str, series: str) -> IntradayBar:
    """Adapt the existing tick-built Candle model after explicit identity is supplied.

    The realtime candle engine reports only a non-null volume after it has
    aggregated valid incremental trade volume, so that value is a bar volume.
    Cumulative or unavailable quote volume remains unavailable in its Candle.
    """
    try:
        volume = candle.volume
        return IntradayBar(
            symbol=candle.symbol,
            exchange=exchange,
            series=series,
            timeframe=candle.interval,
            timestamp=candle.start,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=volume,
            status=candle.status,
            source=candle.source,
            volume_kind="BAR" if volume is not None else "UNKNOWN",
        )
    except AttributeError as exc:
        raise ValueError("realtime Candle is missing required OHLCV fields") from exc


class ConflictingDuplicateBar(ValueError):
    """Two different observations claimed the same symbol/timeframe/start."""


class IntradayBarStore:
    """Validated closed-bar store with explicit health counters and no gap fill."""

    def __init__(self, expected_symbols: Sequence[str] = (), *, calendar: NseTradingCalendar | None = None) -> None:
        self.expected_symbols = tuple(dict.fromkeys(str(s).strip().upper() for s in expected_symbols))
        self.calendar = calendar or default_nse_calendar()
        self._bars: dict[tuple[str, str, date], dict[datetime, IntradayBar]] = defaultdict(dict)
        self._last_seen: dict[tuple[str, str, date], datetime] = {}
        self.bars_received = 0
        self.bars_rejected = 0
        self.duplicate_bars = 0
        self.out_of_order_bars = 0
        self.timestamp_errors = 0
        self.forming_bars_ignored = 0

    def add(self, bar: IntradayBar) -> bool:
        self.bars_received += 1
        if str(bar.status).upper() != "CLOSED":
            self.forming_bars_ignored += 1
            return False
        try:
            item = validate_intraday_bar(bar, calendar=self.calendar, require_closed=True)
        except BarValidationError as exc:
            self.bars_rejected += 1
            if "TIMESTAMP" in exc.code or "SESSION" in exc.code or "OUTSIDE_SESSION" in exc.code:
                self.timestamp_errors += 1
            raise
        key = (item.symbol, item.timeframe, item.trading_date)
        latest = self._last_seen.get(key)
        existing = self._bars[key].get(item.timestamp)
        if existing is not None:
            self.duplicate_bars += 1
            if existing != item:
                self.bars_rejected += 1
                raise ConflictingDuplicateBar(f"conflicting duplicate bar for {item.symbol} {item.timeframe} {item.timestamp.isoformat()}")
            return False
        if latest is not None and item.timestamp < latest:
            self.out_of_order_bars += 1
        self._last_seen[key] = max(latest, item.timestamp) if latest else item.timestamp
        self._bars[key][item.timestamp] = item
        return True

    def bars(self, symbol: str | None = None, timeframe: str | None = None, trading_date: date | None = None) -> list[IntradayBar]:
        selected: list[IntradayBar] = []
        for (item_symbol, item_timeframe, item_date), values in self._bars.items():
            if symbol is not None and item_symbol != symbol.strip().upper():
                continue
            if timeframe is not None and item_timeframe != timeframe.strip().lower():
                continue
            if trading_date is not None and item_date != trading_date:
                continue
            selected.extend(values.values())
        return sorted(selected, key=lambda b: (b.timestamp, b.symbol, b.timeframe))

    @property
    def symbols_with_data(self) -> set[str]:
        return {key[0] for key in self._bars}

    def health(self) -> dict[str, int]:
        missing = 0
        unavailable_volume = 0
        for key, values in self._bars.items():
            ordered = sorted(values)
            interval = TIMEFRAME_MINUTES[key[1]]
            for previous, current in zip(ordered, ordered[1:]):
                delta_minutes = int((current - previous).total_seconds() // 60)
                if delta_minutes > interval and delta_minutes % interval == 0:
                    missing += delta_minutes // interval - 1
            unavailable_volume += sum(1 for bar in values.values() if bar.volume is None or bar.volume_kind != "BAR")
        return {
            "symbols_expected": len(self.expected_symbols),
            "symbols_with_data": len(self.symbols_with_data),
            "bars_received": self.bars_received,
            "bars_rejected": self.bars_rejected,
            "missing_intervals": missing,
            "duplicate_bars": self.duplicate_bars,
            "out_of_order_bars": self.out_of_order_bars,
            "timestamp_errors": self.timestamp_errors,
            "volume_unavailable": unavailable_volume,
            "forming_bars_ignored": self.forming_bars_ignored,
        }


class IntradayDataProvider(Protocol):
    """Provider contract: yield normalized bars, without strategy knowledge."""

    provider_name: str

    def iter_bars(
        self,
        symbols: Sequence[str],
        timeframes: Sequence[str],
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[IntradayBar]: ...


class CsvIntradayDataProvider:
    """Stream a canonical, timestamped bar CSV; never converts daily rows."""

    provider_name = "canonical_intraday_csv"
    REQUIRED_COLUMNS = {"symbol", "exchange", "series", "timeframe", "timestamp", "open", "high", "low", "close", "volume", "volume_kind", "status", "source"}

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def iter_bars(
        self,
        symbols: Sequence[str],
        timeframes: Sequence[str] = ("1m", "5m"),
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[IntradayBar]:
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        symbol_set = {s.strip().upper() for s in symbols}
        timeframe_set = {t.strip().lower() for t in timeframes}
        if not timeframe_set.issubset(TIMEFRAME_MINUTES):
            raise ValueError("provider requested an unsupported timeframe")
        for label, value in (("start", start), ("end", end)):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError(f"provider {label} bound must include a timezone offset")
        with self.path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if not reader.fieldnames or not self.REQUIRED_COLUMNS.issubset({n.strip().lower() for n in reader.fieldnames}):
                raise ValueError(f"canonical CSV requires columns: {', '.join(sorted(self.REQUIRED_COLUMNS))}")
            names = {n.strip().lower(): n for n in reader.fieldnames}
            for row_number, row in enumerate(reader, start=2):
                try:
                    raw_timestamp = datetime.fromisoformat(row[names["timestamp"]].strip().replace("Z", "+00:00"))
                    raw_volume = row[names["volume"]].strip()
                    item = IntradayBar(
                        symbol=row[names["symbol"]], exchange=row[names["exchange"]], series=row[names["series"]],
                        timeframe=row[names["timeframe"]], timestamp=raw_timestamp,
                        open=float(row[names["open"]]), high=float(row[names["high"]]),
                        low=float(row[names["low"]]), close=float(row[names["close"]]),
                        volume=None if not raw_volume else float(raw_volume),
                        volume_kind=row[names["volume_kind"]], status=row[names["status"]], source=row[names["source"]],
                    )
                except (ValueError, TypeError, KeyError) as exc:
                    raise ValueError(f"malformed canonical intraday CSV row {row_number}: {exc}") from exc
                item = validate_intraday_bar(item, require_closed=True)
                if item.symbol not in symbol_set or item.timeframe not in timeframe_set:
                    continue
                if start is not None and item.end <= start.astimezone(IST):
                    continue
                if end is not None and item.timestamp >= end.astimezone(IST):
                    continue
                yield item


def load_frozen_universe(project_root: str | Path, size: int) -> list[str]:
    """Load a previously frozen universe without modifying or re-ranking it."""
    root = Path(project_root)
    if size not in {100, 200, 486}:
        raise ValueError("intraday universe size must be 100, 200, or 486")
    if size == 100:
        path = root / "results/phase4a8a_upstox_live_universe_100.csv"
        symbol_field, series_field = "symbol", "series"
    else:
        path = root / "data/paper_trading/phase3d_personal_universe_v2.csv"
        symbol_field, series_field = "Symbol", "Series"
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not {symbol_field, series_field}.issubset(reader.fieldnames):
            raise ValueError(f"frozen universe schema is invalid: {path}")
        rows = [row for row in reader if str(row.get(series_field, "")).strip().upper() == "EQ"]
    symbols = [str(row[symbol_field]).strip().upper() for row in rows[:size]]
    if len(symbols) != size or len(set(symbols)) != size or any(not _SYMBOL_RE.fullmatch(s) for s in symbols):
        raise ValueError(f"frozen universe does not contain {size} unique explicit EQ symbols")
    return symbols
