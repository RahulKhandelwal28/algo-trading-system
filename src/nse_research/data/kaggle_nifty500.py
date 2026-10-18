"""Explicit, local-file adapter for the Kaggle NIFTY 500 research dataset.

This module is intentionally separate from the licensed NSE CM EOD provider.
It performs no network requests and never invents an NSE ``Series`` value.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from nse_research.data.providers import HistoricalDataProvider, REQUIRED_COLUMNS

DATASET_SLUG = "shreyashautomation/nifty500-companies-5-years-stock-market-data"
DATASET_URL = f"https://www.kaggle.com/datasets/{DATASET_SLUG}"
SOURCE_REQUIRED_COLUMNS = ("Date", "Ticker", "Open", "High", "Low", "Close", "Volume")
NORMALIZED_COLUMNS = (
    "Symbol",
    "Date",
    "Open Price",
    "High Price",
    "Low Price",
    "Close Price",
    "Total Traded Quantity",
)
PRICE_COLUMNS = ("Open Price", "High Price", "Low Price", "Close Price")
VOLUME_COLUMN = "Total Traded Quantity"
_OHLC_TOLERANCE = 1e-9


class KaggleNifty500DataError(ValueError):
    """The local Kaggle CSV does not meet the adapter's explicit schema/QA rules."""


@dataclass(frozen=True)
class KaggleNormalizationSummary:
    source_path: Path
    output_path: Path
    manifest_path: Path
    rows: int
    symbols: int
    first_date: date
    last_date: date
    source_sha256: str
    normalized_sha256: str


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonicalize(frame: pd.DataFrame) -> pd.DataFrame:
    missing = [column for column in NORMALIZED_COLUMNS if column not in frame.columns]
    if missing:
        raise KaggleNifty500DataError(f"normalized data is missing columns: {missing}")

    normalized = frame.loc[:, NORMALIZED_COLUMNS].copy()
    normalized["Symbol"] = normalized["Symbol"].astype("string").str.strip().str.upper()
    if normalized["Symbol"].isna().any() or normalized["Symbol"].eq("").any():
        raise KaggleNifty500DataError("symbol values must be present and non-empty")

    normalized["Date"] = pd.to_datetime(normalized["Date"], format="%Y-%m-%d", errors="coerce")
    if normalized["Date"].isna().any():
        raise KaggleNifty500DataError("Date must contain ISO YYYY-MM-DD values")

    numeric_columns = [*PRICE_COLUMNS, VOLUME_COLUMN]
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    values = normalized[numeric_columns].to_numpy(dtype="float64")
    if not np.isfinite(values).all():
        raise KaggleNifty500DataError("OHLCV fields must be present, numeric, and finite")

    prices = normalized[list(PRICE_COLUMNS)]
    if (prices <= 0).any(axis=None):
        raise KaggleNifty500DataError("OHLC prices must be strictly positive")
    if (normalized[VOLUME_COLUMN] < 0).any():
        raise KaggleNifty500DataError("volume must not be negative")

    high_floor = normalized[["Open Price", "Close Price", "Low Price"]].max(axis=1)
    low_ceiling = normalized[["Open Price", "Close Price", "High Price"]].min(axis=1)
    invalid_ohlc = (
        (normalized["High Price"] + _OHLC_TOLERANCE < high_floor)
        | (normalized["Low Price"] - _OHLC_TOLERANCE > low_ceiling)
    )
    if invalid_ohlc.any():
        raise KaggleNifty500DataError(f"{int(invalid_ohlc.sum())} rows violate OHLC bounds")

    duplicate_mask = normalized.duplicated(["Symbol", "Date"], keep=False)
    if duplicate_mask.any():
        raise KaggleNifty500DataError(
            f"duplicate (Symbol, Date) records: {int(duplicate_mask.sum())} rows"
        )

    return normalized.sort_values(["Symbol", "Date"], kind="mergesort").reset_index(drop=True)


def normalize_kaggle_nifty500(
    source_path: str | Path,
    output_path: str | Path,
    manifest_path: str | Path | None = None,
) -> KaggleNormalizationSummary:
    """Validate and write a separate canonical CSV plus provenance manifest.

    All source rows are retained. ``Series`` is deliberately absent because
    the Kaggle source files do not provide it. The price values are not
    adjusted, rescaled, or otherwise changed by this adapter.
    """
    source = Path(source_path)
    output = Path(output_path)
    manifest = Path(manifest_path) if manifest_path else output.with_suffix(".manifest.json")
    if not source.is_file():
        raise FileNotFoundError(source)

    source_frame = pd.read_csv(source, low_memory=False)
    missing_source = [column for column in SOURCE_REQUIRED_COLUMNS if column not in source_frame.columns]
    if missing_source:
        raise KaggleNifty500DataError(f"source CSV is missing columns: {missing_source}")

    raw = pd.DataFrame(
        {
            "Symbol": source_frame["Ticker"],
            "Date": source_frame["Date"],
            "Open Price": source_frame["Open"],
            "High Price": source_frame["High"],
            "Low Price": source_frame["Low"],
            "Close Price": source_frame["Close"],
            "Total Traded Quantity": source_frame["Volume"],
        }
    )
    normalized = _canonicalize(raw)

    output.parent.mkdir(parents=True, exist_ok=True)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    output_tmp = _temporary_path(output)
    manifest_tmp = _temporary_path(manifest)
    try:
        normalized.to_csv(output_tmp, index=False, date_format="%Y-%m-%d")
        output_digest = _sha256(output_tmp)
        source_digest = _sha256(source)
        metadata = {
            "provider": "kaggle",
            "dataset": DATASET_SLUG,
            "dataset_url": DATASET_URL,
            "source_file": source.name,
            "source_sha256": source_digest,
            "normalized_file": output.name,
            "normalized_sha256": output_digest,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "rows": int(len(normalized)),
            "symbols": int(normalized["Symbol"].nunique()),
            "first_date": normalized["Date"].min().date().isoformat(),
            "last_date": normalized["Date"].max().date().isoformat(),
            "series_field": "unavailable in source dataset; not fabricated",
            "normalization": "column mapping, ISO dates, numeric validation, sorted by Symbol and Date",
            "price_adjustment": "none performed; source adjustedness is unverified",
        }
        with manifest_tmp.open("w", encoding="utf-8") as stream:
            json.dump(metadata, stream, indent=2, sort_keys=True)
            stream.write("\n")
        os.replace(output_tmp, output)
        os.replace(manifest_tmp, manifest)
    finally:
        output_tmp.unlink(missing_ok=True)
        manifest_tmp.unlink(missing_ok=True)

    return KaggleNormalizationSummary(
        source_path=source,
        output_path=output,
        manifest_path=manifest,
        rows=int(len(normalized)),
        symbols=int(normalized["Symbol"].nunique()),
        first_date=normalized["Date"].min().date(),
        last_date=normalized["Date"].max().date(),
        source_sha256=source_digest,
        normalized_sha256=output_digest,
    )


def _temporary_path(destination: Path) -> Path:
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent, delete=False
    ) as stream:
        return Path(stream.name)


class KaggleNifty500Provider(HistoricalDataProvider):
    """Read one explicitly selected, normalized Kaggle daily dataset locally."""

    provider = "kaggle"
    dataset = DATASET_SLUG
    series_available = False

    def __init__(self, normalized_csv: str | Path) -> None:
        self.normalized_csv = Path(normalized_csv)
        if not self.normalized_csv.is_file():
            raise FileNotFoundError(self.normalized_csv)
        try:
            raw = pd.read_csv(self.normalized_csv, low_memory=False)
            self._frame = _canonicalize(raw)
        except (pd.errors.ParserError, UnicodeDecodeError) as exc:
            raise KaggleNifty500DataError(f"cannot read normalized Kaggle CSV: {exc}") from exc

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        if start > end:
            raise ValueError("start date must be on or before end date")
        if interval != "1d":
            raise ValueError("Kaggle NIFTY 500 source contains daily bars only")
        requested = str(symbol).strip().upper().removesuffix(".NS")
        if not requested:
            raise ValueError("symbol must be non-empty")

        selected = self._frame.loc[self._frame["Symbol"].eq(requested)].copy()
        if selected.empty:
            return pd.DataFrame(columns=REQUIRED_COLUMNS, index=pd.DatetimeIndex([], name="Date"))
        selected = selected.set_index("Date").sort_index()
        selected = selected.loc[(selected.index.date >= start) & (selected.index.date <= end)]
        return selected.rename(
            columns={
                "Open Price": "open",
                "High Price": "high",
                "Low Price": "low",
                "Close Price": "close",
                "Total Traded Quantity": "volume",
            }
        ).loc[:, REQUIRED_COLUMNS]

    def histories(self, start: date, end: date, interval: str = "1d") -> dict[str, pd.DataFrame]:
        """Return all symbol histories in one pass for reproducible batch research."""
        if start > end:
            raise ValueError("start date must be on or before end date")
        if interval != "1d":
            raise ValueError("Kaggle NIFTY 500 source contains daily bars only")
        selected = self._frame.loc[
            (self._frame["Date"].dt.date >= start) & (self._frame["Date"].dt.date <= end)
        ]
        output: dict[str, pd.DataFrame] = {}
        for symbol, symbol_rows in selected.groupby("Symbol", sort=False):
            output[str(symbol)] = symbol_rows.set_index("Date").rename(
                columns={
                    "Open Price": "open",
                    "High Price": "high",
                    "Low Price": "low",
                    "Close Price": "close",
                    "Total Traded Quantity": "volume",
                }
            ).loc[:, REQUIRED_COLUMNS]
        return output


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Normalize the local Kaggle NIFTY 500 CSV for separate research use.")
    parser.add_argument("--source", default="data/kaggle_nifty500/raw/extracted/nifty500_5yr_data.csv")
    parser.add_argument("--output", default="data/kaggle_nifty500/normalized/nifty500_kaggle.csv")
    parser.add_argument("--manifest", default=None)
    args = parser.parse_args()
    result = normalize_kaggle_nifty500(args.source, args.output, args.manifest)
    print(
        f"Normalized {result.rows} rows across {result.symbols} symbols "
        f"({result.first_date} to {result.last_date}) to {result.output_path}"
    )
