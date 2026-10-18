"""Replaceable market-data providers and audited CSV cache."""

from __future__ import annotations

import logging
import os
import re
import time
import zipfile
from abc import ABC, abstractmethod
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger(__name__)
REQUIRED_COLUMNS = ["open", "high", "low", "close", "volume"]


class HistoricalDataProvider(ABC):
    @abstractmethod
    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        """Return validated OHLCV bars indexed by exchange-local bar timestamp."""


MarketDataProvider = HistoricalDataProvider


class CsvDirectoryProvider(HistoricalDataProvider):
    """Read normalized OHLCV CSV files supplied by an authorized data source."""

    def __init__(self, data_dir: str | Path) -> None:
        self.data_dir = Path(data_dir)

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        path = self.data_dir / YahooFinanceProvider.cache_filename(symbol, interval)
        if not path.exists():
            return pd.DataFrame(columns=REQUIRED_COLUMNS)
        raw = pd.read_csv(path, parse_dates=["timestamp"], index_col="timestamp")
        bars = normalize_bars(raw, interval)
        return bars.loc[(bars.index.date >= start) & (bars.index.date <= end)]


class NseOfficialHistoryCsvProvider(HistoricalDataProvider):
    """Parse official NSE per-security historical CSV exports saved from NSE's site."""

    _ALIASES = {
        "timestamp": {"date", "timestamp", "tradedate", "tradingdate", "traddt", "date1", "indexdate", "historicaldate"},
        "symbol": {"symbol", "tradingsymbol", "tckrsymb"},
        "index_name": {"indexname", "index"},
        "series": {"series", "sctysrs"},
        "open": {"open", "openprice", "openingprice", "opnpric", "openindexvalue"},
        "high": {"high", "highprice", "hghpric", "highindexvalue"},
        "low": {"low", "lowprice", "lwpric", "lowindexvalue"},
        "close": {"close", "closeprice", "closingprice", "clspric", "closeindexvalue", "closingindexvalue"},
        "volume": {"volume", "quantity", "totaltradedquantity", "totaltradedqty", "tottrdqty", "ttltradgvol", "sharestraded"},
    }

    def __init__(self, export_dir: str | Path) -> None:
        self.export_dir = Path(export_dir)

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        bars = self.history_raw(symbol, start, end, interval)
        if bars.empty:
            return bars
        bars = bars.loc[~bars.index.duplicated(keep="last")]
        return bars

    def history_raw(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        """Return parsed source rows without hiding data-quality defects.

        The historical importer keeps its established last-row behavior through
        :meth:`history`; forward validation uses this method to reject duplicates,
        malformed timestamps, and malformed numeric values.
        """
        if start > end:
            raise ValueError("start date must be on or before end date")
        normalized_symbol = symbol.strip().upper()
        path = self.find_export(normalized_symbol, interval)
        if path is None:
            return pd.DataFrame(columns=REQUIRED_COLUMNS)
        frames = self._read_export(path, normalized_symbol, preserve_invalid=True)
        if not frames:
            return pd.DataFrame(columns=REQUIRED_COLUMNS)
        bars = pd.concat(frames).sort_index()
        keep = [pd.isna(stamp) or start <= pd.Timestamp(stamp).date() <= end for stamp in bars.index]
        return bars.loc[keep]

    def find_export(self, symbol: str, interval: str = "1d") -> Path | None:
        safe = YahooFinanceProvider.cache_filename(symbol, interval)
        base = symbol.upper().replace("^", "index_").replace(".NS", "").replace("/", "_")
        names = (safe, f"{base}_{interval}.csv", f"{base}.csv", f"{symbol}.csv", f"{symbol}.zip")
        for name in names:
            path = self.export_dir / name
            if path.is_file():
                return path
        return None

    def _read_export(
        self, path: Path, requested_symbol: str, *, preserve_invalid: bool = False
    ) -> list[pd.DataFrame]:
        sources: list[tuple[str, bytes | None]] = []
        if path.suffix.lower() == ".zip":
            with zipfile.ZipFile(path) as archive:
                sources = [(name, archive.read(name)) for name in archive.namelist() if name.lower().endswith(".csv")]
        else:
            sources = [(path.name, None)]
        result = []
        for name, payload in sources:
            try:
                frame = pd.read_csv(path if payload is None else BytesIO(payload))
                normalized = self._normalize_export_frame(
                    frame, requested_symbol, validate=not preserve_invalid,
                    fail_on_schema=preserve_invalid,
                )
                if not normalized.empty:
                    result.append(normalized)
            except Exception as exc:
                if preserve_invalid:
                    raise ValueError(f"cannot parse NSE export {path}!{name}: {exc}") from exc
                LOGGER.warning("Skipping unrecognized NSE export %s!%s: %s", path, name, exc)
        return result

    @classmethod
    def _normalize_export_frame(
        cls,
        source: pd.DataFrame,
        requested_symbol: str,
        *,
        validate: bool = True,
        fail_on_schema: bool = False,
    ) -> pd.DataFrame:
        columns = {_normalize_header(column): column for column in source.columns}
        resolved: dict[str, str] = {}
        for target, aliases in cls._ALIASES.items():
            for alias in aliases:
                normalized_alias = _normalize_header(alias)
                if normalized_alias in columns:
                    resolved[target] = columns[normalized_alias]
                    break
        required = {"timestamp", "open", "high", "low", "close"}
        index_volume_not_applicable = requested_symbol.startswith("^")
        if not required.issubset(resolved) or ("volume" not in resolved and not index_volume_not_applicable):
            if fail_on_schema:
                raise ValueError(f"NSE CSV is missing required daily columns for {requested_symbol}")
            return pd.DataFrame(columns=REQUIRED_COLUMNS)
        keep = pd.Series(True, index=source.index)
        requested = requested_symbol.upper().removesuffix(".NS")
        is_index = requested_symbol.startswith("^")
        expected_names = {
            "^NSEI": {"NIFTY 50", "NIFTY50", "NIFTY_50"},
            "^INDIAVIX": {"INDIA VIX", "INDIAVIX", "INDIA_VIX"},
        }.get(requested_symbol.upper(), {requested})
        if "symbol" in resolved:
            symbol_values = source[resolved["symbol"]].astype(str).str.strip().str.upper()
            keep &= symbol_values.isin(expected_names)
        if "index_name" in resolved:
            index_values = source[resolved["index_name"]].astype(str).str.strip().str.upper()
            keep &= index_values.isin(expected_names)
        if "series" in resolved and not is_index:
            series = source[resolved["series"]].astype(str).str.strip().str.upper()
            keep &= series.eq("EQ") | series.eq("EQUITY")
        source = source.loc[keep]
        date_values = source[resolved["timestamp"]].astype(str).str.strip()
        iso_dates = date_values.str.fullmatch(r"\d{4}-\d{2}-\d{2}")
        parsed_dates = pd.Series(pd.NaT, index=source.index, dtype="datetime64[ns]")
        if bool(iso_dates.any()):
            parsed_dates.loc[iso_dates] = pd.to_datetime(
                date_values.loc[iso_dates], errors="coerce", format="%Y-%m-%d"
            )
        if bool((~iso_dates).any()):
            parsed_dates.loc[~iso_dates] = pd.to_datetime(
                date_values.loc[~iso_dates], errors="coerce", dayfirst=True, format="mixed"
            )
        frame = pd.DataFrame(index=pd.DatetimeIndex(parsed_dates))
        for field in ("open", "high", "low", "close", "volume"):
            if field == "volume" and field not in resolved:
                # NSE index series are not traded securities; OHLCV validation
                # requires a volume slot, so zero records non-applicability.
                frame[field] = 0.0
            else:
                values = source[resolved[field]].astype(str).str.replace(",", "", regex=False).str.strip()
                frame[field] = pd.to_numeric(values, errors="coerce").to_numpy()
        if not validate:
            return frame
        frame = frame.loc[~frame.index.isna()]
        normalized, _ = validate_and_normalize_bars(frame, "1d")
        return normalized


def _normalize_header(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).strip().lower())


class YahooFinanceProvider(HistoricalDataProvider):
    """Research-data adapter. Yahoo data is not a substitute for licensed exchange feeds."""

    def __init__(
        self,
        cache_dir: str | Path,
        auto_adjust: bool = True,
        retries: int = 3,
        force_refresh: bool = False,
        min_request_interval_seconds: float = 1.0,
    ) -> None:
        if retries < 1:
            raise ValueError("retries must be at least 1")
        if min_request_interval_seconds < 0:
            raise ValueError("min_request_interval_seconds cannot be negative")
        self.cache_dir = Path(cache_dir)
        self.auto_adjust = auto_adjust
        self.retries = retries
        self.force_refresh = force_refresh
        self.min_request_interval_seconds = min_request_interval_seconds
        self.quality_reports: dict[str, dict[str, int | str]] = {}
        self.download_failures: dict[str, str] = {}
        self._last_request_at: float | None = None

    @staticmethod
    def cache_filename(symbol: str, interval: str) -> str:
        safe = symbol.replace("^", "index_").replace("/", "_")
        return f"{safe}_{interval}.csv"

    def _cache_path(self, symbol: str, interval: str) -> Path:
        return self.cache_dir / self.cache_filename(symbol, interval)

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        return self.histories([symbol], start, end, interval)[symbol]

    def histories(self, symbols: list[str], start: date, end: date, interval: str = "1d") -> dict[str, pd.DataFrame]:
        if start > end:
            raise ValueError("start date must be on or before end date")
        output: dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            path = self._cache_path(symbol, interval)
            cached = self._read_cache(path, interval, symbol)
            windows = self._missing_edge_ranges(cached, start, end, interval)
            if self.force_refresh:
                windows = [(start, end)]
            additions: list[pd.DataFrame] = []
            failures: list[str] = []
            for fetch_start, fetch_end in windows:
                try:
                    normalized, audit = self._download_with_retries(symbol, fetch_start, fetch_end, interval)
                    self.quality_reports[symbol] = audit
                    additions.append(normalized)
                except Exception as exc:
                    failures.append(f"{fetch_start}..{fetch_end}: {exc}")
                    LOGGER.warning("Download failed for %s over %s..%s: %s", symbol, fetch_start, fetch_end, exc)
            if additions:
                pieces = ([cached] if cached is not None and not cached.empty else []) + additions
                merged = pd.concat(pieces).sort_index()
                duplicate_rows = int(merged.index.duplicated(keep="last").sum())
                merged = merged.loc[~merged.index.duplicated(keep="last")]
                merged, audit = validate_and_normalize_bars(merged, interval)
                audit["duplicate_timestamps"] = int(audit.get("duplicate_timestamps", 0)) + duplicate_rows
                self.quality_reports[symbol] = audit
                self._write_cache(symbol, interval, merged)
                cached = merged
            if failures:
                self.download_failures[symbol] = "; ".join(failures)
            else:
                self.download_failures.pop(symbol, None)
            if cached is not None and not cached.empty:
                output[symbol] = cached.loc[(cached.index.date >= start) & (cached.index.date <= end)]
        return output

    def _read_cache(self, path: Path, interval: str, symbol: str) -> pd.DataFrame | None:
        if not path.exists():
            return None
        try:
            raw_cached = pd.read_csv(path, parse_dates=["timestamp"], index_col="timestamp")
            cached, audit = validate_and_normalize_bars(raw_cached, interval)
            self.quality_reports[symbol] = audit
            return cached
        except Exception as exc:
            LOGGER.warning("Ignoring invalid cache %s: %s", path, exc)
            return None

    @staticmethod
    def _missing_edge_ranges(
        cached: pd.DataFrame | None, start: date, end: date, interval: str
    ) -> list[tuple[date, date]]:
        if cached is None or cached.empty:
            return [(start, end)]
        if interval not in {"1d", "1wk", "1mo"}:
            complete = cached.index.min().date() <= start and cached.index.max().date() >= end - timedelta(days=2)
            return [] if complete else [(start, end)]
        first = cached.index.min().date()
        last = cached.index.max().date()
        if first <= start and last >= end - timedelta(days=2):
            return []
        # For short daily scans, a cache beginning inside the request commonly
        # means the security listed after the warm-up date. Long requests still
        # fetch the older edge, which prevents a recent cache from masquerading
        # as five years of history.
        short_request_with_late_listing = (end - start).days < 365 and first > start and last >= end - timedelta(days=2)
        if short_request_with_late_listing:
            return []
        ranges: list[tuple[date, date]] = []
        if first > start:
            prefix_end = min(end, first - timedelta(days=1))
            if prefix_end >= start:
                ranges.append((start, prefix_end))
        if last < end - timedelta(days=2):
            suffix_start = max(start, last + timedelta(days=1))
            if suffix_start <= end:
                ranges.append((suffix_start, end))
        return ranges

    def _download_with_retries(
        self, symbol: str, start: date, end: date, interval: str
    ) -> tuple[pd.DataFrame, dict[str, int | str]]:
        last_error: Exception | None = None
        for attempt in range(self.retries):
            self._respect_rate_limit()
            try:
                downloaded = self._download_symbol(symbol, start, end, interval)
                normalized, audit = validate_and_normalize_bars(downloaded, interval)
                if normalized.empty:
                    raise ValueError("provider returned no valid bars")
                return normalized, audit
            except Exception as exc:
                last_error = exc
                if attempt + 1 < self.retries:
                    time.sleep(min(2**attempt, 8))
        raise RuntimeError(f"all {self.retries} attempt(s) failed: {last_error}") from last_error

    def _respect_rate_limit(self) -> None:
        now = time.monotonic()
        if self._last_request_at is not None:
            remaining = self.min_request_interval_seconds - (now - self._last_request_at)
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.monotonic()

    def _download_symbol(self, symbol: str, start: date, end: date, interval: str) -> pd.DataFrame:
        try:
            import yfinance as yf
        except ImportError as exc:
            raise RuntimeError("yfinance is not installed; install requirements.txt before downloading data") from exc
        tz_cache = self.cache_dir / ".yfinance"
        tz_cache.mkdir(parents=True, exist_ok=True)
        yf.set_tz_cache_location(str(tz_cache))
        downloaded = yf.download(
            tickers=symbol,
            start=start.isoformat(),
            end=(end + timedelta(days=1)).isoformat(),
            interval=interval,
            auto_adjust=self.auto_adjust,
            actions=True,
            progress=False,
            threads=False,
            group_by="ticker",
            timeout=10,
        )
        if downloaded is None or downloaded.empty:
            raise ValueError("provider returned no bars")
        return _ticker_frame(downloaded, symbol, [symbol])

    def _write_cache(self, symbol: str, interval: str, frame: pd.DataFrame) -> None:
        path = self._cache_path(symbol, interval)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        frame.to_csv(temporary, index_label="timestamp")
        os.replace(temporary, path)


def normalize_bars(frame: pd.DataFrame, interval: str | None = None) -> pd.DataFrame:
    """Standardize OHLCV bars and reject malformed prices without synthesizing candles."""
    result, _ = validate_and_normalize_bars(frame, interval)
    return result


def validate_and_normalize_bars(
    frame: pd.DataFrame, interval: str | None = None
) -> tuple[pd.DataFrame, dict[str, int | str]]:
    """Normalize bars and return counts of rows excluded by basic quality checks."""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS), {
            "raw_rows": 0, "duplicate_timestamps": 0, "missing_ohlcv_rows": 0,
            "invalid_price_rows": 0, "invalid_ohlc_rows": 0, "out_of_session_rows": 0,
            "accepted_rows": 0,
        }
    result = frame.copy()
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = [column[-1] for column in result.columns]
    result.columns = [str(column).strip().lower().replace(" ", "_") for column in result.columns]
    result = result.rename(columns={"adj_close": "adj_close", "stock_splits": "stock_splits"})
    missing = [column for column in REQUIRED_COLUMNS if column not in result.columns]
    if missing:
        raise ValueError(f"market bars missing required columns: {missing}")
    index = pd.DatetimeIndex(pd.to_datetime(result.index))
    duplicate_count = int(index.duplicated(keep="last").sum())
    if index.tz is not None:
        index = index.tz_convert("Asia/Kolkata").tz_localize(None)
    result.index = index
    for column in result.columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    missing_rows = int(result[REQUIRED_COLUMNS].isna().any(axis=1).sum())
    invalid_price = (result[["open", "high", "low", "close"]] <= 0).any(axis=1) | (result["volume"] < 0)
    invalid_price_count = int(invalid_price.fillna(False).sum())
    invalid_ohlc = (result["high"] < result[["open", "close", "low"]].max(axis=1)) | (result["low"] > result[["open", "close", "high"]].min(axis=1))
    invalid_ohlc_count = int(invalid_ohlc.fillna(False).sum())
    result = result.dropna(subset=REQUIRED_COLUMNS).sort_index()
    result = result[~result.index.duplicated(keep="last")]
    result = result[(result["open"] > 0) & (result["high"] > 0) & (result["low"] > 0) & (result["close"] > 0) & (result["volume"] >= 0)]
    valid_ohlc = (result["high"] >= result[["open", "close", "low"]].max(axis=1)) & (result["low"] <= result[["open", "close", "high"]].min(axis=1))
    result = result.loc[valid_ohlc]
    session_invalid = pd.Series(False, index=result.index)
    if interval not in {None, "1d", "1wk", "1mo"} and isinstance(result.index, pd.DatetimeIndex):
        minute_of_day = result.index.hour * 60 + result.index.minute
        session_invalid = pd.Series((minute_of_day < 9 * 60 + 15) | (minute_of_day > 15 * 60 + 30), index=result.index)
        result = result.loc[~session_invalid]
    result = result.rename(columns={"dividends": "dividends", "stock_splits": "stock_splits"})
    report: dict[str, int | str] = {
        "raw_rows": int(len(frame)),
        "duplicate_timestamps": duplicate_count,
        "missing_ohlcv_rows": missing_rows,
        "invalid_price_rows": invalid_price_count,
        "invalid_ohlc_rows": invalid_ohlc_count,
        "out_of_session_rows": int(session_invalid.sum()),
        "accepted_rows": int(len(result)),
        "timezone_policy": "timestamps converted to Asia/Kolkata then stored timezone-naive",
    }
    return result, report


def _ticker_frame(downloaded: pd.DataFrame, symbol: str, batch: list[str]) -> pd.DataFrame:
    if not isinstance(downloaded.columns, pd.MultiIndex):
        if len(batch) == 1:
            return downloaded
        raise ValueError("multi-symbol response did not include ticker labels")
    for level in range(downloaded.columns.nlevels):
        if symbol in downloaded.columns.get_level_values(level):
            frame = downloaded.xs(symbol, axis=1, level=level, drop_level=True)
            if isinstance(frame.columns, pd.MultiIndex):
                frame.columns = frame.columns.get_level_values(-1)
            return frame
    raise ValueError(f"symbol {symbol} absent from batched provider response")


def read_bars(path: str | Path) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["timestamp"], index_col="timestamp")
    return normalize_bars(frame)


def download_many(
    provider: MarketDataProvider,
    symbols: list[str],
    start: date,
    end: date,
    interval: str = "1d",
) -> tuple[dict[str, pd.DataFrame], list[dict[str, str]]]:
    data: dict[str, pd.DataFrame] = {}
    failures: list[dict[str, str]] = []
    if hasattr(provider, "histories"):
        try:
            data.update(provider.histories(symbols, start, end, interval))  # type: ignore[attr-defined]
        except Exception as exc:
            LOGGER.warning("Batch download failed: %s", exc)
        for symbol in symbols:
            if symbol not in data:
                failures.append({"symbol": symbol, "reason": "provider did not return bars; inspect logs or check connectivity"})
        return data, failures
    for symbol in symbols:
        try:
            bars = provider.history(symbol, start, end, interval)
            if bars.empty:
                raise ValueError("no bars returned")
            data[symbol] = bars
        except Exception as exc:
            LOGGER.warning("Skipping %s: %s", symbol, exc)
            failures.append({"symbol": symbol, "reason": str(exc)})
    return data, failures
