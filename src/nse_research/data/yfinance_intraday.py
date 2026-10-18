"""Conservative yfinance intraday acquisition for Phase 4G-YF.

Yahoo Finance is a third-party research source here.  This module never claims
that Yahoo is an NSE-authorized feed, does not infer identity from ``.NS``, and
never adjusts prices.  Source timestamps and provider-returned columns are
retained separately from the Phase 4E canonical representation.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from nse_research.data.intraday_validator import validate_ohlc
from nse_research.intraday.model import load_frozen_universe


IST = ZoneInfo("Asia/Kolkata")
YAHOO_SOURCE = "yahoo_finance_yfinance"
OHLCV_FIELDS = {"open", "high", "low", "close", "volume"}
OPTIONAL_FIELDS = {"adj close", "dividends", "stock splits", "capital gains"}


class YFinanceDataError(RuntimeError):
    """Fail-closed source or normalization error with a stable status code."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


@dataclass(frozen=True, slots=True)
class YahooInstrument:
    symbol: str
    yahoo_symbol: str
    exchange: str
    series: str
    isin: str


@dataclass(slots=True)
class NormalizedYahooBars:
    rows: list[dict[str, Any]]
    requested_symbols: list[str]
    symbols_with_data: list[str]
    missing_symbols: list[str]
    forming_rows_ignored: int
    source_timezone: str
    interval: str
    raw_index_first: str | None
    raw_index_last: str | None
    raw_duplicate_timestamps: int = 0
    issues: list[dict[str, Any]] | None = None
    failed_symbols: list[str] | None = None
    invalid_symbols: list[str] | None = None
    duplicate_symbols: list[str] | None = None
    empty_no_trade_cells_ignored: int = 0


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_yahoo_instruments(project_root: str | Path, *, size: int = 486) -> dict[str, YahooInstrument]:
    """Map the existing frozen Phase 3D universe using explicit NSE/EQ evidence.

    The Yahoo ticker is only an access symbol.  NSE/series/ISIN identity comes
    from the repository's NSE security-history-derived universe manifest and
    the existing frozen EQ universe; the ``.NS`` suffix itself proves nothing.
    """
    root = Path(project_root).resolve()
    frozen = load_frozen_universe(root, size)
    manifest_path = root / "data/nse_exports/nse_equity_universe.json"
    universe_path = root / "data/universe.csv"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise YFinanceDataError("NSE_IDENTITY_MANIFEST_UNAVAILABLE", str(exc)) from exc
    expected_hash = str(manifest.get("source_universe_sha256") or "").lower()
    if not expected_hash or not universe_path.is_file() or sha256_file(universe_path) != expected_hash:
        raise YFinanceDataError("NSE_IDENTITY_MANIFEST_MISMATCH", "official-universe source hash does not match data/universe.csv")
    identities: dict[str, YahooInstrument] = {}
    for row in manifest.get("symbols", []):
        symbol = str(row.get("symbol") or "").strip().upper()
        series = str(row.get("requested_series") or "").strip().upper()
        isin = str(row.get("isin") or "").strip().upper()
        if symbol and series == "EQ" and isin:
            identities[symbol] = YahooInstrument(symbol, f"{symbol}.NS", "NSE", "EQ", isin)
    missing = sorted(set(frozen) - set(identities))
    if missing:
        raise YFinanceDataError("NSE_EQ_IDENTITY_UNAVAILABLE", f"frozen symbols lack explicit NSE/EQ/ISIN mapping: {missing[:10]}")
    return {symbol: identities[symbol] for symbol in frozen}


def download_yfinance_history(
    yahoo_symbols: Sequence[str],
    *,
    start: date | datetime | str,
    end: date | datetime | str,
    interval: str = "1m",
    timeout_seconds: float = 15.0,
    downloader: Callable[..., Any] | None = None,
    request_started_callback: Callable[[], None] | None = None,
    response_received_callback: Callable[[], None] | None = None,
) -> pd.DataFrame:
    """Fetch one bounded, non-pre/post-market provider snapshot.

    ``threads=False`` avoids a burst of parallel per-symbol Yahoo requests.
    The caller controls refresh cadence; the function itself never retries or
    silently switches providers/data intervals.
    """
    tickers = list(dict.fromkeys(str(item).strip().upper() for item in yahoo_symbols))
    if not tickers or any(not item.endswith(".NS") for item in tickers):
        raise YFinanceDataError("YAHOO_SYMBOL_LIST_INVALID", "expected one or more explicit .NS access symbols")
    if interval not in {"1m", "5m"}:
        raise YFinanceDataError("INTRADAY_INTERVAL_INVALID", "only native 1m or 5m observations are accepted")
    if timeout_seconds <= 0:
        raise YFinanceDataError("TIMEOUT_INVALID", "timeout must be positive")
    if downloader is None:
        try:
            import yfinance as yf
        except Exception as exc:  # pragma: no cover - depends on local installation
            raise YFinanceDataError("PHASE4G_YF_BLOCKED_PROVIDER", f"yfinance is unavailable: {exc}") from exc
        downloader = yf.download
    try:
        if request_started_callback is not None:
            request_started_callback()
        frame = downloader(
            tickers=tickers,
            start=start,
            end=end,
            interval=interval,
            auto_adjust=False,
            back_adjust=False,
            repair=False,
            actions=True,
            prepost=False,
            threads=False,
            progress=False,
            keepna=True,
            timeout=timeout_seconds,
            group_by="ticker",
            multi_level_index=True,
        )
        if response_received_callback is not None:
            response_received_callback()
    except TimeoutError as exc:
        raise YFinanceDataError("YAHOO_TIMEOUT", str(exc)) from exc
    except Exception as exc:
        name = type(exc).__name__.lower()
        code = "YAHOO_TIMEOUT" if "timeout" in name or "timeout" in str(exc).lower() else "PHASE4G_YF_BLOCKED_PROVIDER"
        raise YFinanceDataError(code, str(exc)) from exc
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise YFinanceDataError("YAHOO_NO_INTRADAY_DATA", "provider returned no intraday rows")
    return frame


def _field_and_ticker(label: Any) -> tuple[str, str | None]:
    if isinstance(label, tuple):
        parts = [str(item).strip() for item in label if item not in (None, "")]
        field = next((part for part in parts if part.casefold() in OHLCV_FIELDS | OPTIONAL_FIELDS), "")
        ticker = next((part.upper() for part in parts if part.casefold() not in OHLCV_FIELDS | OPTIONAL_FIELDS), None)
        return field.casefold(), ticker
    return str(label).strip().casefold(), None


def _columns_for_ticker(frame: pd.DataFrame, yahoo_symbol: str, symbol_count: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    seen: set[str] = set()
    target = yahoo_symbol.strip().upper()
    for column in frame.columns:
        field, ticker = _field_and_ticker(column)
        if ticker is None:
            if symbol_count == 1:
                if field in seen:
                    raise YFinanceDataError("YAHOO_RESPONSE_DUPLICATE_FIELD", f"{yahoo_symbol} has duplicate {field} columns")
                seen.add(field)
                result[field] = column
        elif ticker == target:
            if field in seen:
                raise YFinanceDataError("YAHOO_RESPONSE_DUPLICATE_FIELD", f"{yahoo_symbol} has duplicate {field} columns")
            seen.add(field)
            result[field] = column
    if not OHLCV_FIELDS.issubset(result):
        raise YFinanceDataError(
            "YAHOO_RESPONSE_SCHEMA_INVALID",
            f"{yahoo_symbol} lacks fields {sorted(OHLCV_FIELDS - set(result))}",
        )
    return result


def _finite_number(value: Any, *, field: str, symbol: str, timestamp: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise YFinanceDataError("YAHOO_NUMERIC_FIELD_INVALID", f"{symbol} {timestamp} {field}: {value!r}") from exc
    if not math.isfinite(number):
        raise YFinanceDataError("YAHOO_NUMERIC_FIELD_INVALID", f"{symbol} {timestamp} {field}: non-finite")
    return number


def normalize_yfinance_frame(
    frame: pd.DataFrame,
    instruments: Mapping[str, YahooInstrument],
    *,
    interval: str,
    now: datetime,
    request_started_at: datetime | None = None,
    response_received_at: datetime | None = None,
    batch_id: int | None = None,
    session_date: date | None = None,
    volume_kind: str = "UNKNOWN",
) -> NormalizedYahooBars:
    """Convert provider observations to Phase 4E rows without filling or adjusting.

    Index values are bar starts, matching Phase 4E's ``BAR_START`` convention.
    A forming row is retained by the raw snapshot at the caller but omitted
    here until its interval end has passed in IST.
    """
    if interval not in {"1m", "5m"}:
        raise YFinanceDataError("INTRADAY_INTERVAL_INVALID", interval)
    if volume_kind not in {"BAR", "UNKNOWN"}:
        raise YFinanceDataError("VOLUME_SEMANTICS_UNSUPPORTED", volume_kind)
    if now.tzinfo is None or now.utcoffset() is None:
        raise YFinanceDataError("UNKNOWN_TIMEZONE", "collection clock must be timezone aware")
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        raise YFinanceDataError("YAHOO_NO_INTRADAY_DATA", "provider returned no rows")
    index = frame.index
    tz = getattr(index, "tz", None)
    if tz is None:
        raise YFinanceDataError("UNKNOWN_TIMEZONE", "Yahoo response index has no timezone")
    try:
        source_timezone = str(tz)
        stamps = [pd.Timestamp(item) for item in index]
    except Exception as exc:
        raise YFinanceDataError("YAHOO_TIMESTAMP_INVALID", str(exc)) from exc
    if any(item.tzinfo is None for item in stamps):
        raise YFinanceDataError("UNKNOWN_TIMEZONE", "at least one source timestamp is naive")
    if stamps != sorted(stamps):
        raise YFinanceDataError("YAHOO_TIMESTAMP_OUT_OF_ORDER", "provider rows are not monotonic")
    if len(set(stamps)) != len(stamps):
        raise YFinanceDataError("YAHOO_DUPLICATE_TIMESTAMP", "provider response has duplicate timestamp rows")

    now_ist = now.astimezone(IST)
    receipt = response_received_at or now
    if receipt.tzinfo is None or receipt.utcoffset() is None:
        raise YFinanceDataError("UNKNOWN_TIMEZONE", "response receipt timestamp must be timezone aware")
    receipt_ist = receipt.astimezone(IST)
    if request_started_at is not None and (request_started_at.tzinfo is None or request_started_at.utcoffset() is None):
        raise YFinanceDataError("UNKNOWN_TIMEZONE", "request start timestamp must be timezone aware")
    request_started_ist = request_started_at.astimezone(IST) if request_started_at is not None else None
    minutes = int(interval[:-1])
    rows: list[dict[str, Any]] = []
    forming = 0
    symbols_with_data: set[str] = set()
    issues: list[dict[str, Any]] = []
    failed_symbols: list[str] = []
    invalid_symbols: list[str] = []
    duplicate_symbols: list[str] = []
    empty_no_trade_cells_ignored = 0
    for canonical, instrument in instruments.items():
        try:
            cols = _columns_for_ticker(frame, instrument.yahoo_symbol, len(instruments))
        except YFinanceDataError as exc:
            failed_symbols.append(canonical)
            issues.append({"symbol": canonical, "code": exc.code, "detail": exc.detail})
            if exc.code == "YAHOO_RESPONSE_DUPLICATE_FIELD":
                duplicate_symbols.append(canonical)
            continue
        symbol_rows: list[dict[str, Any]] = []
        try:
            for row_index, stamp in enumerate(stamps):
                original_timestamp = stamp.isoformat()
                local = stamp.to_pydatetime().astimezone(IST)
                if session_date is not None and local.date() != session_date:
                    continue
                if local.second or local.microsecond:
                    raise YFinanceDataError("YAHOO_TIMESTAMP_PRECISION_INVALID", f"{instrument.yahoo_symbol} {original_timestamp}")
                if session_date is not None:
                    start_min = local.hour * 60 + local.minute
                    session_open_min = 9 * 60 + 15
                    session_close_min = 15 * 60 + 30
                    if start_min < session_open_min or start_min + minutes > session_close_min:
                        raise YFinanceDataError("YAHOO_SESSION_BOUNDARY_INVALID", f"bar outside regular session: {original_timestamp}")
                    if (start_min - session_open_min) % minutes:
                        raise YFinanceDataError("YAHOO_INTERVAL_ALIGNMENT_INVALID", f"bar not aligned to {interval}: {original_timestamp}")
                end = local + timedelta(minutes=minutes)
                if local > now_ist + timedelta(minutes=minutes):
                    raise YFinanceDataError("YAHOO_FUTURE_TIMESTAMP", f"provider row exceeds collection clock: {original_timestamp}")
                if end > now_ist:
                    forming += 1
                    continue
                raw_bar = frame.iloc[row_index]
                raw_ohlc = {field: raw_bar[column] for field, column in cols.items()
                            if field in {"open", "high", "low", "close"}}
                # Multi-symbol yfinance responses use a union timestamp index.
                # A ticker can therefore have an empty cell where another
                # ticker has a bar. A completely absent OHLC bar with zero
                # volume is missing data, not a malformed bar: omit it and
                # let coverage checks observe the gap. Never fill it.
                if all(pd.isna(value) for value in raw_ohlc.values()):
                    raw_volume = raw_bar[cols["volume"]]
                    try:
                        empty_cell_volume = float(raw_volume)
                    except (TypeError, ValueError, OverflowError):
                        empty_cell_volume = math.nan
                    if pd.isna(raw_volume) or (math.isfinite(empty_cell_volume) and empty_cell_volume == 0.0):
                        empty_no_trade_cells_ignored += 1
                        continue
                vals = {
                    field: _finite_number(raw_bar[column], field=field,
                                          symbol=canonical, timestamp=original_timestamp)
                    for field, column in cols.items() if field in OHLCV_FIELDS
                }
                adjusted_close = None
                if "adj close" in cols:
                    raw_adjusted = frame.iloc[row_index][cols["adj close"]]
                    if not pd.isna(raw_adjusted):
                        adjusted_close = _finite_number(raw_adjusted, field="adj close",
                                                        symbol=canonical, timestamp=original_timestamp)
                        if adjusted_close <= 0:
                            raise YFinanceDataError("YAHOO_ADJUSTED_CLOSE_INVALID",
                                                    f"{instrument.yahoo_symbol} {original_timestamp}: non-positive Adj Close")
                ohlc_issues = validate_ohlc(vals["open"], vals["high"], vals["low"], vals["close"])
                if ohlc_issues:
                    raise YFinanceDataError("YAHOO_OHLC_INVALID", f"{instrument.yahoo_symbol} {original_timestamp}: {ohlc_issues}")
                if vals["volume"] < 0:
                    raise YFinanceDataError("YAHOO_VOLUME_INVALID", f"{instrument.yahoo_symbol} {original_timestamp}: negative volume")
                symbol_rows.append({
                    "timestamp": local.isoformat(), "market_timestamp": local.isoformat(),
                    "received_timestamp": receipt_ist.isoformat(),
                    "request_started_at": request_started_ist.isoformat() if request_started_ist else None,
                    "response_received_at": receipt_ist.isoformat(), "batch_id": batch_id,
                    "symbol": instrument.symbol,
                    "exchange": instrument.exchange, "series": instrument.series,
                    "timeframe": interval, "open": vals["open"], "high": vals["high"],
                    "low": vals["low"], "close": vals["close"],
                    "unadjusted_close": vals["close"], "adjusted_close": adjusted_close,
                    "volume": vals["volume"],
                    "volume_kind": volume_kind, "status": "CLOSED", "source": YAHOO_SOURCE,
                    "yahoo_symbol": instrument.yahoo_symbol, "isin": instrument.isin,
                    "source_timestamp": original_timestamp, "source_timezone": source_timezone,
                    "adjustment_status": "UNKNOWN",
                })
        except YFinanceDataError as exc:
            invalid_symbols.append(canonical)
            issues.append({"symbol": canonical, "code": exc.code, "detail": exc.detail})
            continue
        rows.extend(symbol_rows)
        if symbol_rows:
            symbols_with_data.add(canonical)
    rows.sort(key=lambda item: (item["timestamp"], item["symbol"], item["timeframe"]))
    first = stamps[0].isoformat() if stamps else None
    last = stamps[-1].isoformat() if stamps else None
    present = sorted(symbols_with_data)
    return NormalizedYahooBars(
        rows=rows,
        requested_symbols=sorted(instruments),
        symbols_with_data=present,
        missing_symbols=sorted(set(instruments) - symbols_with_data),
        forming_rows_ignored=forming,
        source_timezone=source_timezone,
        interval=interval,
        raw_index_first=first,
        raw_index_last=last,
        issues=issues,
        failed_symbols=sorted(failed_symbols),
        invalid_symbols=sorted(invalid_symbols),
        duplicate_symbols=sorted(duplicate_symbols),
        empty_no_trade_cells_ignored=empty_no_trade_cells_ignored,
    )


def infer_interval_volume_semantics(rows: Sequence[Mapping[str, Any]], *, minimum_pairs: int = 20,
                                    minimum_decrease_ratio: float = 0.05) -> dict[str, Any]:
    """Empirically distinguish bar volume from cumulative volume without guessing.

    A cumulative session total should not fall between adjacent observations.
    Repeated decreases are evidence of interval volume, not proof of source
    licensing or an authoritative Yahoo contract.  Ambiguous evidence remains
    UNKNOWN and blocks relative-volume ranking/volume-breakout use.
    """
    by_symbol: dict[str, list[tuple[datetime, float]]] = {}
    for row in rows:
        try:
            stamp = datetime.fromisoformat(str(row["timestamp"]))
            volume = float(row["volume"])
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(volume) and volume >= 0:
            by_symbol.setdefault(str(row["symbol"]).upper(), []).append((stamp, volume))
    comparisons = decreases = 0
    for values in by_symbol.values():
        ordered = sorted(values)
        for (_, previous), (_, current) in zip(ordered, ordered[1:]):
            if previous > 0 and current > 0:
                comparisons += 1
                decreases += int(current < previous)
    ratio = decreases / comparisons if comparisons else 0.0
    supported = comparisons >= minimum_pairs and ratio >= minimum_decrease_ratio
    return {
        "classification": "INTERVAL_BAR_EMPIRICAL" if supported else "UNKNOWN",
        "volume_kind": "BAR" if supported else "UNKNOWN",
        "comparison_count": comparisons,
        "decrease_count": decreases,
        "decrease_ratio": ratio,
        "minimum_pairs": minimum_pairs,
        "minimum_decrease_ratio": minimum_decrease_ratio,
        "evidence_limit": "empirical non-monotonicity only; not a provider guarantee",
    }
