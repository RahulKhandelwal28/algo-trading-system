"""Explicit local EOD providers and fail-closed daily data validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from nse_research.data.providers import (
    HistoricalDataProvider,
    NseOfficialHistoryCsvProvider,
    REQUIRED_COLUMNS,
)
from nse_research.data.licensed_nse_cm_eod import (
    LicensedNseCmEodProvider,
    NseCmEodSchema,
)


@dataclass(frozen=True)
class DataCheck:
    provider: str
    retrieved_at: str
    market_date: str
    symbol: str
    data_timestamp: str | None
    freshness_status: str
    valid: bool
    reason: str = ""
    rows: int = 0
    invalid_records: int = 0
    duplicate_records: int = 0
    abnormal_records: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class LocalEodCsvProvider(HistoricalDataProvider):
    """Read per-symbol normalized daily CSVs without hiding malformed rows.

    Expected filename: ``SYMBOL_1d.csv``. Columns are ``timestamp,open,high,
    low,close,volume``. No network calls or provider fallback are made.
    """

    def __init__(
        self,
        equity_dir: str | Path,
        index_dir: str | Path,
        *,
        licensed_dataset_path: str | Path | None = None,
    ) -> None:
        self.equity_dir = Path(equity_dir)
        self.index_provider = NseOfficialHistoryCsvProvider(index_dir)
        self.licensed_dataset_path = Path(licensed_dataset_path) if licensed_dataset_path else None
        self.licensed_provider = None
        if self.licensed_dataset_path is not None:
            canonical_schema = NseCmEodSchema(
                date_column="Date", symbol_column="Symbol", series_column="Series",
                open_column="Open Price", high_column="High Price", low_column="Low Price",
                close_column="Close Price", volume_column="Total Traded Quantity",
            )
            self.licensed_provider = LicensedNseCmEodProvider(
                self.licensed_dataset_path.parent,
                canonical_schema,
                dataset_path=self.licensed_dataset_path,
            )

    @property
    def provider_name(self) -> str:
        return "licensed_nse_cm_eod" if self.licensed_provider is not None else "local_eod_csv"

    @property
    def dataset_name(self) -> str:
        return "NSE licensed CM EOD canonical dataset" if self.licensed_provider is not None else "explicit local normalized EOD CSVs"

    @staticmethod
    def filename(symbol: str) -> str:
        safe = symbol.strip().upper().replace("^", "index_").replace("/", "_")
        return f"{safe}_1d.csv"

    def provider_for(self, symbol: str) -> str:
        if symbol.startswith("^"):
            return "nse_official_index_csv"
        return self.provider_name

    def dataset_path_for(self, symbol: str) -> Path:
        if symbol.startswith("^"):
            return self.index_provider.find_export(symbol, "1d") or self.index_provider.export_dir / self.index_provider.cache_filename(symbol, "1d")
        if self.licensed_provider is not None:
            return self.licensed_dataset_path
        return self.equity_dir / self.filename(symbol)

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        if start > end:
            raise ValueError("start date must be on or before end date")
        if interval != "1d":
            raise ValueError("forward paper trading currently accepts daily EOD bars only")
        if symbol.startswith("^"):
            return self.index_provider.history_raw(symbol, start, end, interval)
        if self.licensed_provider is not None:
            if not self.licensed_dataset_path.is_file():
                raise FileNotFoundError(f"licensed normalized NSE CM EOD dataset is missing: {self.licensed_dataset_path}")
            return self.licensed_provider.history(symbol, start, end, interval)
        path = self.equity_dir / self.filename(symbol)
        if not path.is_file():
            raise FileNotFoundError(f"no local EOD file for {symbol}: {path}")
        raw = pd.read_csv(path, dtype=str, keep_default_na=False)
        columns = {str(column).strip().lower(): column for column in raw.columns}
        required = ("timestamp", *REQUIRED_COLUMNS)
        missing = [name for name in required if name not in columns]
        if missing:
            raise ValueError(f"{path.name} is missing normalized columns {missing}")
        timestamps = pd.to_datetime(raw[columns["timestamp"]], errors="coerce", format="mixed")
        frame = pd.DataFrame(index=pd.DatetimeIndex(timestamps))
        for name in REQUIRED_COLUMNS:
            frame[name] = pd.to_numeric(raw[columns[name]].astype(str).str.replace(",", "", regex=False), errors="coerce").to_numpy()
        keep = [
            pd.isna(stamp) or start <= pd.Timestamp(stamp).date() <= end
            for stamp in frame.index
        ]
        return frame.loc[keep].sort_index(kind="stable")


class PersonalPaperEodProvider(HistoricalDataProvider):
    """Read audited local history plus explicitly supplied dated EQ files.

    Historical equity bars are read from the already-audited local CSV cache.
    Future files are loaded only from the deterministic normalized directory
    written by :mod:`nse_research.forward.intake`. Official NSE index exports
    remain the sole NIFTY/VIX source. No network access or provider fallback is
    implemented.
    """

    provider_name = "personal_local_eod"
    dataset_name = "Yahoo-derived audited local OHLC cache plus explicit local daily EQ files"

    def __init__(
        self,
        historical_equity_dir: str | Path,
        index_dir: str | Path,
        daily_data_dir: str | Path,
    ) -> None:
        self._local = LocalEodCsvProvider(historical_equity_dir, index_dir)
        self.equity_dir = Path(historical_equity_dir)
        self.index_provider = self._local.index_provider
        self.daily_data_dir = Path(daily_data_dir)
        self.daily_manifest_dir = self.daily_data_dir.parent / "manifests"
        self._daily_bars: dict[str, pd.DataFrame] | None = None
        self._official_session_dates: set[date] | None = None
        self._excluded_non_nifty_rows: dict[str, int] = {}
        self._excluded_non_nifty_dates: set[date] = set()

    @property
    def excluded_non_nifty_rows(self) -> dict[str, int]:
        return dict(self._excluded_non_nifty_rows)

    @property
    def excluded_non_nifty_dates(self) -> list[str]:
        return sorted(day.isoformat() for day in self._excluded_non_nifty_dates)

    def _nifty_session_dates(self, end: date) -> set[date]:
        if self._official_session_dates is None:
            frame = self._local.history("^NSEI", date(2000, 1, 1), end, "1d")
            self._official_session_dates = {
                pd.Timestamp(stamp).date() for stamp in frame.index if pd.notna(stamp)
            }
        return {day for day in self._official_session_dates if day <= end}

    def _filter_to_official_sessions(self, symbol: str, frame: pd.DataFrame, end: date) -> pd.DataFrame:
        if frame.empty:
            return frame
        sessions = self._nifty_session_dates(end)
        dates = pd.to_datetime(frame.index, errors="coerce", format="mixed")
        keep = [pd.isna(stamp) or pd.Timestamp(stamp).date() in sessions for stamp in dates]
        excluded = frame.loc[[not value for value in keep]]
        if not excluded.empty:
            base_symbol = symbol.strip().upper().removesuffix(".NS")
            self._excluded_non_nifty_rows[base_symbol] = int(self._excluded_non_nifty_rows.get(base_symbol, 0)) + len(excluded)
            self._excluded_non_nifty_dates.update(
                pd.Timestamp(stamp).date() for stamp in excluded.index if pd.notna(stamp)
            )
        return frame.loc[keep]

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    def provider_for(self, symbol: str) -> str:
        if symbol.startswith("^"):
            return "nse_official_index_csv"
        return self.provider_name

    def dataset_path_for(self, symbol: str) -> Path:
        if symbol.startswith("^"):
            return self.index_provider.find_export(symbol, "1d") or self.index_provider.export_dir / self.index_provider.cache_filename(symbol, "1d")
        return self.equity_dir / self._local.filename(symbol)

    def _load_daily_bars(self) -> dict[str, pd.DataFrame]:
        if self._daily_bars is not None:
            return self._daily_bars
        grouped: dict[str, list[pd.DataFrame]] = {}
        if self.daily_data_dir.is_dir():
            for path in sorted(self.daily_data_dir.glob("????-??-??.csv")):
                try:
                    filename_date = date.fromisoformat(path.stem)
                except ValueError as error:
                    raise ValueError(f"invalid normalized daily filename {path.name}") from error
                manifest_path = self.daily_manifest_dir / f"{filename_date.isoformat()}.json"
                if not manifest_path.is_file():
                    raise ValueError(f"normalized daily file has no intake manifest: {manifest_path}")
                try:
                    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError) as error:
                    raise ValueError(f"cannot read intake manifest {manifest_path}: {error}") from error
                if manifest.get("validation_status") != "PASS":
                    raise ValueError(f"normalized daily file is not backed by a passing intake manifest: {manifest_path}")
                expected_hash = manifest.get("normalized_sha256")
                if not expected_hash or self._sha256(path) != expected_hash:
                    raise ValueError(f"normalized daily file checksum does not match its intake manifest: {path}")
                raw = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
                columns = {str(column).strip().casefold(): column for column in raw.columns}
                missing = [name for name in ("date", "symbol", "series", *REQUIRED_COLUMNS) if name not in columns]
                if missing:
                    raise ValueError(f"normalized daily file {path} is missing columns {missing}")
                parsed_dates: list[pd.Timestamp] = []
                symbols: list[str] = []
                for line_number, row in enumerate(raw.to_dict(orient="records"), start=2):
                    value = str(row.get(columns["date"]) or "").strip()
                    try:
                        parsed = date.fromisoformat(value)
                    except ValueError as error:
                        raise ValueError(f"malformed date in {path.name}:{line_number}: {value!r}") from error
                    if parsed.isoformat() != value or parsed != filename_date:
                        raise ValueError(f"normalized row date does not match {path.name}:{line_number}")
                    series = str(row.get(columns["series"]) or "").strip().upper()
                    if series != "EQ":
                        raise ValueError(f"normalized paper source must contain explicit EQ rows: {path.name}:{line_number}")
                    symbol = str(row.get(columns["symbol"]) or "").strip().upper().removesuffix(".NS")
                    if not symbol:
                        raise ValueError(f"normalized row has an empty symbol: {path.name}:{line_number}")
                    parsed_dates.append(pd.Timestamp(parsed))
                    symbols.append(symbol)
                for symbol in sorted(set(symbols)):
                    selected = raw.loc[[value == symbol for value in symbols]]
                    frame = pd.DataFrame(index=pd.DatetimeIndex([
                        parsed_dates[index] for index, value in enumerate(symbols) if value == symbol
                    ]))
                    for name in REQUIRED_COLUMNS:
                        values = pd.to_numeric(selected[columns[name]].astype(str).str.replace(",", "", regex=False), errors="coerce")
                        frame[name] = values.to_numpy()
                    grouped.setdefault(symbol, []).append(frame)
        self._daily_bars = {
            symbol: pd.concat(frames).sort_index(kind="stable")
            for symbol, frames in grouped.items()
        }
        return self._daily_bars

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        if start > end:
            raise ValueError("start date must be on or before end date")
        if interval != "1d":
            raise ValueError("personal forward paper trading accepts daily EOD bars only")
        if symbol.startswith("^"):
            return self._local.history(symbol, start, end, interval)
        historical = self._local.history(symbol, start, end, interval)
        daily = self._load_daily_bars().get(symbol.strip().upper().removesuffix(".NS"))
        if daily is None or daily.empty:
            combined = historical
        else:
            keep = [start <= pd.Timestamp(stamp).date() <= end for stamp in daily.index]
            current = daily.loc[keep]
            if historical.empty:
                combined = current.sort_index(kind="stable")
            else:
                # Preserve overlaps so validate_daily_bars rejects ambiguous source rows.
                combined = pd.concat([historical, current]).sort_index(kind="stable")
        return self._filter_to_official_sessions(symbol, combined, end)


def validate_daily_bars(
    frame: pd.DataFrame | None,
    *,
    provider: str,
    symbol: str,
    market_date: date,
    retrieved_at: datetime | None = None,
    max_daily_price_jump_fraction: float = 0.40,
) -> tuple[pd.DataFrame, DataCheck]:
    """Validate raw provider bars; any defect blocks this symbol's signals."""
    retrieved = (retrieved_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    empty = pd.DataFrame(columns=REQUIRED_COLUMNS)

    def check(status: str, reason: str, *, timestamp: str | None = None, rows: int = 0,
              invalid: int = 0, duplicates: int = 0, abnormal: int = 0) -> tuple[pd.DataFrame, DataCheck]:
        return empty, DataCheck(provider, retrieved, market_date.isoformat(), symbol, timestamp,
                                status, False, reason, rows, invalid, duplicates, abnormal)

    if frame is None or frame.empty:
        return check("DATA_STALE", "provider returned no daily bars")
    missing = [name for name in REQUIRED_COLUMNS if name not in frame.columns]
    if missing:
        return check("DATA_INVALID", f"missing required OHLCV columns: {missing}", rows=len(frame), invalid=len(frame))
    bars = frame.copy()
    try:
        index = pd.DatetimeIndex(pd.to_datetime(bars.index, errors="coerce", format="mixed"))
    except (TypeError, ValueError) as error:
        return check("DATA_INVALID", f"invalid timestamp index: {error}", rows=len(frame), invalid=len(frame))
    if index.hasnans:
        return check("DATA_INVALID", "one or more data timestamps are missing or malformed", rows=len(frame), invalid=int(index.isna().sum()))
    # Daily datasets must contain one record per exchange market date.
    session_dates = pd.Index(index.date)
    duplicate_count = int(session_dates.duplicated(keep=False).sum())
    if duplicate_count:
        return check("DATA_INVALID", "duplicate daily records for the same market date", rows=len(frame), duplicates=duplicate_count)
    if any(value.date() > market_date for value in index):
        return check("DATA_INVALID", "provider returned data after the requested market date", rows=len(frame))
    bars.index = index
    bars = bars.sort_index(kind="stable")
    numeric = bars[list(REQUIRED_COLUMNS)].apply(pd.to_numeric, errors="coerce")
    missing_rows = int(numeric.isna().any(axis=1).sum())
    finite_values = pd.DataFrame(
        np.isfinite(numeric.to_numpy(dtype=float)), index=numeric.index, columns=numeric.columns
    )
    nonfinite_rows = int((~finite_values).any(axis=1).sum())
    invalid_prices = (numeric[["open", "high", "low", "close"]] <= 0).any(axis=1)
    negative_volume = numeric["volume"] < 0
    invalid_ohlc = (
        (numeric["high"] < numeric[["open", "close", "low"]].max(axis=1))
        | (numeric["low"] > numeric[["open", "close", "high"]].min(axis=1))
    )
    invalid_mask = numeric.isna().any(axis=1) | (~finite_values).any(axis=1) | invalid_prices | negative_volume | invalid_ohlc
    invalid_count = int(invalid_mask.sum())
    if invalid_count:
        reason = "invalid or missing OHLCV values"
        if invalid_ohlc.any():
            reason = "invalid OHLC range"
        elif invalid_prices.any():
            reason = "nonpositive OHLC price"
        elif negative_volume.any():
            reason = "negative volume"
        elif missing_rows or nonfinite_rows:
            reason = "missing or malformed OHLCV value"
        return check("DATA_INVALID", reason, timestamp=index[-1].isoformat(), rows=len(frame), invalid=invalid_count)
    bars = numeric
    close_changes = bars["close"].pct_change().abs()
    abnormal = close_changes > max_daily_price_jump_fraction
    abnormal_count = int(abnormal.sum())
    if abnormal_count:
        return check("DATA_INVALID", f"close-to-close move exceeds configured {max_daily_price_jump_fraction:.0%} review limit",
                     timestamp=index[-1].isoformat(), rows=len(frame), abnormal=abnormal_count)
    actual_market_date = index[-1].date()
    if actual_market_date < market_date:
        return check("DATA_STALE", f"latest bar is {actual_market_date.isoformat()}, requested {market_date.isoformat()}",
                     timestamp=index[-1].isoformat(), rows=len(frame))
    if actual_market_date > market_date:
        return check("DATA_INVALID", "latest bar exceeds requested market date", timestamp=index[-1].isoformat(), rows=len(frame))
    bars.index = index
    bars = bars.loc[:, REQUIRED_COLUMNS]
    return bars, DataCheck(provider, retrieved, market_date.isoformat(), symbol, index[-1].isoformat(),
                           "FRESH", True, "", len(bars), 0, 0, 0)
