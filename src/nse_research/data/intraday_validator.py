"""Provider-independent validation for canonical historical intraday bars.

The validator checks data as supplied. It never fills gaps, infers an exchange
or series, shifts timestamps, or converts a coarser bar into a finer one.
"""

from __future__ import annotations

import csv
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


IST = ZoneInfo("Asia/Kolkata")
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")
_TIMEFRAME_MINUTES = {"1m": 1, "5m": 5}
_VOLUME_KINDS = {"BAR", "INCREMENTAL", "CUMULATIVE", "LTQ", "UNKNOWN"}
CANONICAL_COLUMNS = frozenset(
    {
        "timestamp", "symbol", "exchange", "series", "timeframe", "open", "high",
        "low", "close", "volume", "volume_kind", "status", "source",
    }
)
PROVENANCE_FACTS = (
    "dataset", "source_url", "publisher", "revision", "downloaded_at", "sha256",
    "coverage_start", "coverage_end", "symbols", "rows", "timeframe", "timezone",
    "exchange", "series", "volume_semantics", "license", "original_provider",
    "redistribution_terms", "research_allowed", "commercial_allowed", "universe_bias",
)


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    code: str
    message: str
    row: int | None = None
    symbol: str | None = None
    timestamp: str | None = None


@dataclass(slots=True)
class IntradayValidationReport:
    rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    duplicate_rows: int = 0
    out_of_order_rows: int = 0
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    first_date: str | None = None
    last_date: str | None = None
    sessions: int = 0
    symbols: list[str] = field(default_factory=list)
    missing_symbols: list[str] = field(default_factory=list)
    source_timezones: list[str] = field(default_factory=list)
    canonical_timezone: str = "Asia/Kolkata"
    exchanges: list[str] = field(default_factory=list)
    series_values: list[str] = field(default_factory=list)
    timeframes: list[str] = field(default_factory=list)
    issue_counts: dict[str, int] = field(default_factory=dict)
    issues: list[ValidationIssue] = field(default_factory=list)
    session_coverage: dict[str, Any] = field(default_factory=dict)

    @property
    def validation_status(self) -> str:
        return "PASS" if self.rows > 0 and self.invalid_rows == 0 and not self.issue_counts else "FAIL"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable audit summary with bounded issue samples."""
        return {
            "rows": self.rows,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "duplicate_rows": self.duplicate_rows,
            "out_of_order_rows": self.out_of_order_rows,
            "first_timestamp": self.first_timestamp,
            "last_timestamp": self.last_timestamp,
            "first_date": self.first_date,
            "last_date": self.last_date,
            "sessions": self.sessions,
            "symbols": self.symbols,
            "missing_symbols": self.missing_symbols,
            "source_timezones": self.source_timezones,
            "canonical_timezone": self.canonical_timezone,
            "exchanges": self.exchanges,
            "series": self.series_values,
            "timeframes": self.timeframes,
            "issue_counts": self.issue_counts,
            "issues": [
                {"code": item.code, "message": item.message, "row": item.row,
                 "symbol": item.symbol, "timestamp": item.timestamp}
                for item in self.issues
            ],
            "session_coverage": self.session_coverage,
            "validation_status": self.validation_status,
        }


def validate_timezone(value: Any, *, canonical_timezone: str = "Asia/Kolkata") -> datetime:
    """Parse an explicitly zoned timestamp and convert its instant to the target zone.

    Naive timestamps are rejected. The conversion preserves the instant; it does
    not guess whether a naive value was UTC or exchange-local.
    """
    try:
        zone = ZoneInfo(canonical_timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown canonical timezone {canonical_timezone!r}") from exc
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("timestamp is missing")
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError("timestamp is malformed; expected ISO-8601 with timezone") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must contain an explicit timezone offset")
    return parsed.astimezone(zone)


def validate_ohlc(open_value: Any, high_value: Any, low_value: Any, close_value: Any) -> tuple[str, ...]:
    """Return OHLC validation codes; an empty tuple means the bar is valid."""
    values: list[float] = []
    for value in (open_value, high_value, low_value, close_value):
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return ("OHLC_MALFORMED",)
        if not math.isfinite(number):
            return ("OHLC_MALFORMED",)
        values.append(number)
    open_price, high, low, close = values
    issues: list[str] = []
    if min(values) <= 0:
        issues.append("PRICE_NONPOSITIVE")
    if high < max(open_price, close) or low > min(open_price, close) or high < low:
        issues.append("OHLC_RELATION_INVALID")
    return tuple(issues)


def validate_volume(volume: Any, volume_kind: Any = "UNKNOWN") -> tuple[str, ...]:
    """Validate a bar's numeric volume and declared canonical volume kind.

    ``UNKNOWN`` semantics are recorded as unknown, not treated as proof that the
    volume is usable by a volume-dependent strategy.
    """
    try:
        number = float(volume)
    except (TypeError, ValueError, OverflowError):
        return ("VOLUME_MALFORMED",)
    if not math.isfinite(number):
        return ("VOLUME_MALFORMED",)
    if number < 0:
        return ("VOLUME_NEGATIVE",)
    if str(volume_kind or "").strip().upper() not in _VOLUME_KINDS:
        return ("VOLUME_KIND_INVALID",)
    return ()


def validate_symbol_identity(
    symbol: Any,
    exchange: Any,
    series: Any,
    *,
    expected_symbols: Iterable[str] = (),
    expected_exchange: str = "NSE",
    expected_series: str = "EQ",
) -> tuple[str, ...]:
    """Require an explicit, syntactically valid NSE cash-equity identity."""
    issues: list[str] = []
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_exchange = str(exchange or "").strip().upper()
    normalized_series = str(series or "").strip().upper()
    if not normalized_symbol or not _SYMBOL_RE.fullmatch(normalized_symbol):
        issues.append("SYMBOL_INVALID")
    if normalized_exchange != expected_exchange.upper():
        issues.append("EXCHANGE_MISMATCH")
    if normalized_series != expected_series.upper():
        issues.append("SERIES_MISMATCH")
    allowed = {str(item).strip().upper() for item in expected_symbols}
    if allowed and normalized_symbol not in allowed:
        issues.append("SYMBOL_NOT_IN_EXPECTED_UNIVERSE")
    return tuple(issues)


def validate_provenance(metadata: Mapping[str, Any] | None) -> tuple[str, ...]:
    """Check that provenance facts are explicitly recorded without inferring them."""
    if not isinstance(metadata, Mapping):
        return ("PROVENANCE_MISSING",)
    missing = [
        key for key in PROVENANCE_FACTS
        if metadata.get(key) is None or (isinstance(metadata.get(key), str) and not metadata[key].strip())
    ]
    issues = [f"PROVENANCE_MISSING_{key.upper()}" for key in missing]
    if metadata.get("research_allowed") is not True:
        issues.append("RESEARCH_RIGHTS_NOT_VERIFIED")
    if str(metadata.get("license", "")).strip().upper() in {"UNKNOWN", "UNLICENSED", ""}:
        issues.append("LICENSE_NOT_VERIFIED")
    if str(metadata.get("redistribution_terms", "")).strip().upper() in {"UNKNOWN", ""}:
        issues.append("REDISTRIBUTION_TERMS_UNKNOWN")
    sha256 = str(metadata.get("sha256", "")).strip().lower()
    if sha256 and not re.fullmatch(r"[0-9a-f]{64}", sha256):
        issues.append("PROVENANCE_SHA256_INVALID")
    return tuple(dict.fromkeys(issues))


def _records(data: Any) -> tuple[Iterable[Mapping[str, Any]], Sequence[str] | None]:
    if isinstance(data, (str, Path)):
        path = Path(data)
        stream = path.open("r", newline="", encoding="utf-8-sig")
        reader = csv.DictReader(stream)
        # Keep the stream alive for generator consumption; it is closed by the
        # generator's finally block below.
        def rows():
            try:
                yield from reader
            finally:
                stream.close()
        return rows(), reader.fieldnames
    if hasattr(data, "to_dict") and hasattr(data, "columns"):
        return data.to_dict(orient="records"), [str(item) for item in data.columns]
    if isinstance(data, Mapping):
        return [data], list(data.keys())
    if isinstance(data, Sequence) and data and isinstance(data[0], Mapping):
        return data, list(data[0].keys())
    return data, None


def _issue(report: IntradayValidationReport, code: str, message: str, *, row: int | None = None,
           symbol: str | None = None, timestamp: str | None = None, sample_cap: int = 100) -> None:
    report.issue_counts[code] = report.issue_counts.get(code, 0) + 1
    if len(report.issues) < sample_cap:
        report.issues.append(ValidationIssue(code, message, row, symbol, timestamp))


def _normalized_source_timezone(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return "UNKNOWN"
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return "NAIVE"
    key = getattr(parsed.tzinfo, "key", None)
    return str(key or parsed.tzname() or parsed.utcoffset())


def _session_date(value: date | str) -> date:
    return value if isinstance(value, date) and not isinstance(value, datetime) else date.fromisoformat(str(value))


def _window_times(window: tuple[time | str, time | str]) -> tuple[time, time]:
    def parse(value: time | str) -> time:
        if isinstance(value, time):
            return value.replace(tzinfo=None)
        return time.fromisoformat(str(value))
    return parse(window[0]), parse(window[1])


def validate_session_coverage(
    records: Iterable[Mapping[str, Any] | datetime | str],
    *,
    timeframe: str | int = "1m",
    expected_sessions: Iterable[date | str] | None = None,
    expected_symbols: Iterable[str] = (),
    market_open: time | str = "09:15",
    market_close: time | str = "15:30",
    special_sessions: Mapping[date | str, Sequence[tuple[time | str, time | str]]] | None = None,
    canonical_timezone: str = "Asia/Kolkata",
) -> dict[str, Any]:
    """Measure gaps against explicit session windows without inserting bars.

    If ``expected_sessions`` is omitted, only dates observed in the input are
    counted; absent calendar dates are deliberately not guessed to be holidays
    or missing trading sessions. Special sessions can supply their actual
    window(s) by date.
    """
    if isinstance(timeframe, int):
        minutes = timeframe
        label = f"{minutes}m"
    else:
        label = str(timeframe).strip().lower()
        minutes = _TIMEFRAME_MINUTES.get(label, 0)
    if minutes <= 0:
        raise ValueError("timeframe must be 1m, 5m, or a positive minute count")
    regular_window = _window_times((market_open, market_close))
    special: dict[date, Sequence[tuple[time | str, time | str]]] = {
        _session_date(day): windows for day, windows in (special_sessions or {}).items()
    }
    observed: dict[tuple[str, date, str], set[datetime]] = defaultdict(set)
    malformed_timestamps = 0
    duplicate_rows = 0
    previous_by_symbol: dict[tuple[str, str], datetime] = {}
    out_of_order = 0
    for record in records:
        if isinstance(record, datetime):
            raw_timestamp = record
            symbol = ""
            record_timeframe = label
        elif isinstance(record, str):
            raw_timestamp = record
            symbol = ""
            record_timeframe = label
        else:
            raw_timestamp = record.get("timestamp")
            symbol = str(record.get("symbol") or "").strip().upper()
            record_timeframe = str(record.get("timeframe") or label).strip().lower()
        try:
            timestamp = validate_timezone(raw_timestamp, canonical_timezone=canonical_timezone)
        except ValueError:
            malformed_timestamps += 1
            continue
        if symbol:
            prior = previous_by_symbol.get((symbol, record_timeframe))
            if prior is not None and timestamp < prior:
                out_of_order += 1
            previous_by_symbol[(symbol, record_timeframe)] = max(prior, timestamp) if prior is not None else timestamp
        slots = observed[(symbol, timestamp.date(), record_timeframe)]
        if timestamp in slots:
            duplicate_rows += 1
        slots.add(timestamp)

    observed_dates = {day for _, day, _ in observed}
    expected_dates = {_session_date(day) for day in expected_sessions} if expected_sessions is not None else observed_dates
    symbols = {symbol for symbol, _, _ in observed if symbol}
    observed_timeframes = {item_timeframe for _, _, item_timeframe in observed} or {label}
    required_symbols = {str(item).strip().upper() for item in expected_symbols}
    expected_symbol_set = required_symbols or symbols
    expected_pairs: set[tuple[str, date, str]] | None = None
    if expected_sessions is not None and expected_symbol_set:
        expected_pairs = {
            (symbol, day, item_timeframe)
            for symbol in expected_symbol_set for day in expected_dates for item_timeframe in observed_timeframes
        }
        missing_pairs = sorted(expected_pairs - set(observed))
        missing_sessions = sorted({day for _, day, _ in missing_pairs})
        missing_session_pairs = [f"{symbol}:{day.isoformat()}:{item_timeframe}" for symbol, day, item_timeframe in missing_pairs]
    else:
        missing_sessions = sorted(expected_dates - observed_dates)
        missing_session_pairs = [day.isoformat() for day in missing_sessions]
    missing_intervals = 0
    unexpected_intervals = 0
    expected_intervals = 0
    per_session: dict[str, int] = {}
    coverage_keys = expected_pairs if expected_pairs is not None else set(observed)
    for symbol, day, item_timeframe in sorted(coverage_keys):
        timestamps = observed.get((symbol, day, item_timeframe), set())
        windows = special.get(day, (regular_window,))
        expected_slots: set[datetime] = set()
        for window in windows:
            window_start, window_end = _window_times(window)
            cursor = datetime.combine(day, window_start, tzinfo=ZoneInfo(canonical_timezone))
            end = datetime.combine(day, window_end, tzinfo=ZoneInfo(canonical_timezone))
            while cursor + timedelta(minutes=minutes) <= end:
                expected_slots.add(cursor)
                cursor += timedelta(minutes=minutes)
        expected_intervals += len(expected_slots)
        missing_intervals += len(expected_slots - timestamps)
        unexpected_intervals += len(timestamps - expected_slots)
        per_session[f"{symbol + ':' if symbol else ''}{day.isoformat()}:{item_timeframe}"] = len(timestamps)
    counts = list(per_session.values())
    return {
        "timeframe": label,
        "observed_sessions": len(observed_dates),
        "observed_symbol_sessions": len(observed),
        "expected_sessions": len(expected_dates),
        "missing_sessions": [day.isoformat() for day in missing_sessions],
        "missing_symbol_sessions": missing_session_pairs,
        "missing_session_count": len(missing_sessions),
        "missing_intervals": missing_intervals,
        "expected_intervals": expected_intervals,
        "observed_expected_intervals": expected_intervals - missing_intervals,
        "unexpected_intervals": unexpected_intervals,
        "duplicate_timestamps": duplicate_rows,
        "out_of_order_timestamps": out_of_order,
        "malformed_timestamps": malformed_timestamps,
        "bars_per_observed_session": {
            "minimum": min(counts) if counts else 0,
            "median": median(counts) if counts else 0,
            "maximum": max(counts) if counts else 0,
        },
        "observed_session_bar_counts": per_session,
        "gap_policy": "reported only; no bars or sessions synthesized",
    }


def validate_intraday_dataset(
    data: Any,
    *,
    expected_symbols: Iterable[str] = (),
    expected_sessions: Iterable[date | str] | None = None,
    timeframe: str | None = None,
    expected_exchange: str = "NSE",
    expected_series: str = "EQ",
    canonical_timezone: str = "Asia/Kolkata",
    special_sessions: Mapping[date | str, Sequence[tuple[time | str, time | str]]] | None = None,
    issue_sample_limit: int = 100,
) -> IntradayValidationReport:
    """Validate canonical OHLCV records and produce a factual coverage report."""
    records, columns = _records(data)
    report = IntradayValidationReport(canonical_timezone=canonical_timezone)
    if columns is not None:
        missing_columns = sorted(CANONICAL_COLUMNS - set(columns))
        for column in missing_columns:
            _issue(report, "SCHEMA_COLUMN_MISSING", f"required canonical column {column!r} is missing", sample_cap=issue_sample_limit)
    seen: set[tuple[str, str, datetime]] = set()
    previous: dict[tuple[str, str], datetime] = {}
    symbols: set[str] = set()
    source_timezones: set[str] = set()
    exchanges: set[str] = set()
    series_values: set[str] = set()
    timeframes: set[str] = set()
    parsed_records: list[dict[str, Any]] = []
    timestamps: list[datetime] = []
    expected = {str(item).strip().upper() for item in expected_symbols}

    for row_number, record in enumerate(records, start=2 if columns is not None else 1):
        report.rows += 1
        row_issue_count = 0
        symbol = str(record.get("symbol") or "").strip().upper()
        timestamp_raw = record.get("timestamp")
        timestamp_text = str(timestamp_raw or "")
        def add(code: str, message: str) -> None:
            nonlocal row_issue_count
            row_issue_count += 1
            _issue(report, code, message, row=row_number, symbol=symbol or None,
                   timestamp=timestamp_text or None, sample_cap=issue_sample_limit)

        if not symbol:
            add("SYMBOL_MISSING", "symbol is missing")
        else:
            symbols.add(symbol)
        exchange = str(record.get("exchange") or "").strip().upper()
        series = str(record.get("series") or "").strip().upper()
        exchanges.add(exchange or "MISSING")
        series_values.add(series or "MISSING")
        for code in validate_symbol_identity(symbol, exchange, series, expected_symbols=expected,
                                            expected_exchange=expected_exchange, expected_series=expected_series):
            add(code, f"symbol identity rejected ({symbol or 'missing'}, {exchange or 'missing'}, {series or 'missing'})")

        try:
            timestamp = validate_timezone(timestamp_raw, canonical_timezone=canonical_timezone)
            source_timezones.add(_normalized_source_timezone(timestamp_raw))
            if timestamp.second or timestamp.microsecond:
                add("TIMESTAMP_NOT_MINUTE_ALIGNED", "timestamp must align to a whole minute")
            timestamps.append(timestamp)
            timeframe_value = str(record.get("timeframe") or "").strip().lower()
            timeframes.add(timeframe_value or "MISSING")
            interval = _TIMEFRAME_MINUTES.get(timeframe_value)
            if interval is None:
                add("TIMEFRAME_INVALID", "timeframe must be explicitly 1m or 5m")
            else:
                if timeframe is not None and timeframe_value != timeframe.strip().lower():
                    add("TIMEFRAME_MISMATCH", f"row timeframe {timeframe_value!r} differs from requested {timeframe!r}")
                open_time = time(9, 15)
                close_time = time(15, 30)
                day_windows = (special_sessions or {}).get(timestamp.date())
                if day_windows is None:
                    day_windows = (special_sessions or {}).get(timestamp.date().isoformat())
                windows = tuple(_window_times(item) for item in day_windows) if day_windows else ((open_time, close_time),)
                bar_end = timestamp + timedelta(minutes=interval)
                if not any(timestamp.time().replace(tzinfo=None) >= start and bar_end.time().replace(tzinfo=None) <= end
                           for start, end in windows):
                    add("TIMESTAMP_OUTSIDE_SESSION", "bar start/end is outside the supplied NSE session window")
                anchor = windows[0][0] if windows else open_time
                minute_offset = (timestamp.hour * 60 + timestamp.minute) - (anchor.hour * 60 + anchor.minute)
                if minute_offset < 0 or minute_offset % interval:
                    add("TIMESTAMP_NOT_ALIGNED_TO_TIMEFRAME", f"timestamp is not aligned to {timeframe_value} bars")
            key_symbol = symbol or "MISSING"
            key_timeframe = timeframe_value or "MISSING"
            identity = (key_symbol, key_timeframe, timestamp)
            if identity in seen:
                report.duplicate_rows += 1
                add("DUPLICATE_BAR", "duplicate symbol/timeframe/timestamp record")
            else:
                seen.add(identity)
            order_key = (key_symbol, key_timeframe)
            prior = previous.get(order_key)
            if prior is not None and timestamp < prior:
                report.out_of_order_rows += 1
                add("OUT_OF_ORDER_BAR", "bar appears before a later timestamp for this symbol/timeframe")
            previous[order_key] = max(prior, timestamp) if prior is not None else timestamp
            parsed_records.append({"timestamp": timestamp, "symbol": symbol, "timeframe": timeframe_value})
        except ValueError as exc:
            add("TIMESTAMP_INVALID", str(exc))
            timestamp = None

        for code in validate_ohlc(record.get("open"), record.get("high"), record.get("low"), record.get("close")):
            add(code, "OHLC values are malformed or violate positive-price/price-relationship rules")
        volume_kind = str(record.get("volume_kind") or "UNKNOWN").strip().upper()
        for code in validate_volume(record.get("volume"), volume_kind):
            add(code, "volume is malformed, negative, or has an invalid kind")
        if volume_kind not in _VOLUME_KINDS:
            pass
        status = str(record.get("status") or "").strip().upper()
        if status != "CLOSED":
            add("BAR_NOT_CLOSED", "historical dataset rows must be explicitly CLOSED")
        if not str(record.get("source") or "").strip():
            add("SOURCE_MISSING", "source label is missing")
        if row_issue_count:
            report.invalid_rows += 1
        else:
            report.valid_rows += 1

    report.symbols = sorted(symbols)
    report.missing_symbols = sorted(expected - symbols)
    report.source_timezones = sorted(source_timezones)
    report.exchanges = sorted(exchanges)
    report.series_values = sorted(series_values)
    report.timeframes = sorted(timeframes)
    actual_timeframes = {value for value in timeframes if value not in {"MISSING", ""}}
    if len(actual_timeframes) > 1:
        _issue(report, "MULTIPLE_TIMEFRAMES", "validate one timeframe per dataset", sample_cap=issue_sample_limit)
    if report.missing_symbols:
        _issue(report, "MISSING_SYMBOLS", f"expected symbols absent: {', '.join(report.missing_symbols)}", sample_cap=issue_sample_limit)
    if timestamps:
        ordered = sorted(timestamps)
        report.first_timestamp = ordered[0].isoformat()
        report.last_timestamp = ordered[-1].isoformat()
        report.first_date = ordered[0].date().isoformat()
        report.last_date = ordered[-1].date().isoformat()
        report.sessions = len({item.date() for item in timestamps})
    if parsed_records and len(actual_timeframes) == 1 and next(iter(actual_timeframes)) in _TIMEFRAME_MINUTES:
        coverage_timeframe = next(iter(actual_timeframes))
        report.session_coverage = validate_session_coverage(
            parsed_records, timeframe=coverage_timeframe, expected_sessions=expected_sessions,
            expected_symbols=expected,
            special_sessions=special_sessions, canonical_timezone=canonical_timezone,
        )
        if report.session_coverage["missing_session_count"]:
            _issue(report, "MISSING_SESSIONS", "one or more explicitly expected trading sessions have no rows", sample_cap=issue_sample_limit)
    elif parsed_records:
        report.session_coverage = {
            "status": "NOT_COMPUTED",
            "reason": "session coverage requires exactly one valid timeframe per dataset",
        }
    if report.invalid_rows:
        # Row-level issues have already made issue_counts non-empty. This is a
        # separate count; coverage gaps remain descriptive unless sessions were
        # explicitly supplied as authoritative expectations.
        pass
    return report


def classify_dataset(
    report: IntradayValidationReport | None,
    provenance: Mapping[str, Any] | None,
    *,
    download_status: str = "AVAILABLE",
    minimum_coverage_days: int = 90,
    preferred_coverage_days: int = 730,
    minimum_symbols: int = 10,
    required_symbols: Iterable[str] = (),
) -> str:
    """Apply hard quality, identity, volume, rights, and coverage gates.

    Limitations can affect a passing dataset's scope, but cannot waive a failed
    integrity, NSE/EQ, usable-volume, timezone, provenance, or research-rights
    gate.
    """
    if report is None:
        return "UNAVAILABLE" if str(download_status).upper() == "UNAVAILABLE" else "NOT_RESEARCH_READY"
    if report.validation_status != "PASS" or report.missing_symbols:
        return "NOT_RESEARCH_READY"
    if report.exchanges != ["NSE"] or report.series_values != ["EQ"]:
        return "NOT_RESEARCH_READY"
    if report.canonical_timezone != "Asia/Kolkata" or any(value in {"NAIVE", "UNKNOWN"} for value in report.source_timezones):
        return "NOT_RESEARCH_READY"
    if validate_provenance(provenance):
        return "NOT_RESEARCH_READY"
    assert provenance is not None
    volume_semantics = str(provenance.get("volume_semantics", "")).strip().upper()
    usable_volume = {"BAR_SHARE_VOLUME", "INCREMENTAL_SHARE_VOLUME", "CUMULATIVE_SHARE_VOLUME_WITH_VERIFIED_INCREMENT_RULE"}
    if volume_semantics not in usable_volume:
        return "NOT_RESEARCH_READY"
    if report.first_date is None or report.last_date is None:
        return "NOT_RESEARCH_READY"
    coverage_days = (date.fromisoformat(report.last_date) - date.fromisoformat(report.first_date)).days + 1
    if coverage_days < minimum_coverage_days or report.sessions < 60:
        return "NOT_RESEARCH_READY"
    required = {str(symbol).strip().upper() for symbol in required_symbols}
    if required - set(report.symbols):
        return "NOT_RESEARCH_READY"
    preferred_start = str(provenance.get("preferred_coverage_start", "2021-01-01"))
    preferred_end = str(provenance.get("preferred_coverage_end", "2026-12-31"))
    target_scope_met = (
        coverage_days >= preferred_coverage_days
        and len(report.symbols) >= minimum_symbols
        and report.first_date <= preferred_start
        and report.last_date >= preferred_end
    )
    return "RESEARCH_READY" if target_scope_met else "RESEARCH_READY_WITH_LIMITATIONS"
