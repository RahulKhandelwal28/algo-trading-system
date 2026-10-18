"""Isolated local adapter for the audited TejHQ NSE snapshot.

The adapter never downloads data and is not selected by any default provider.
Its only input is an explicitly supplied normalized Parquet snapshot.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from nse_research.data.providers import HistoricalDataProvider, REQUIRED_COLUMNS

DATASET_ID = "tejhq/indian-markets"
DATASET_URL = "https://huggingface.co/datasets/tejhq/indian-markets"
RAW_REQUIRED_COLUMNS = (
    "date", "symbol", "series", "isin", "open", "high", "low", "close",
    "prev_close", "volume", "turnover",
)
NORMALIZED_COLUMNS = RAW_REQUIRED_COLUMNS
OHLC_TOLERANCE = 1e-9


class TejHQDataError(ValueError):
    """The local TejHQ snapshot or its normalized schema is invalid."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def equity_quality_anomalies(frame: pd.DataFrame) -> pd.DataFrame:
    """Return one unmodified-source diagnostic row per anomalous explicit EQ row."""
    missing_columns = [column for column in RAW_REQUIRED_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise TejHQDataError(f"TejHQ NSE raw schema is missing columns: {missing_columns}")

    series = frame["series"].astype("string")
    equity = frame.loc[series.eq("EQ").fillna(False)].copy()
    if equity.empty:
        return pd.DataFrame(columns=["source_index", "date", "symbol", "series", "issues"])

    issues: list[list[str]] = [[] for _ in range(len(equity))]

    def flag(mask: pd.Series | np.ndarray, label: str) -> None:
        for position in np.flatnonzero(np.asarray(mask, dtype=bool)):
            issues[int(position)].append(label)

    parsed_dates = pd.to_datetime(equity["date"], errors="coerce")
    flag(parsed_dates.isna(), "malformed_or_missing_date")
    symbols = equity["symbol"].astype("string")
    flag(symbols.isna() | symbols.fillna("").eq(""), "missing_symbol")
    flag(symbols.notna() & symbols.ne(symbols.str.strip()), "symbol_has_outer_whitespace")
    flag(equity["series"].isna() | equity["series"].astype("string").ne("EQ"), "series_not_explicit_EQ")

    numeric: dict[str, pd.Series] = {}
    for field in ("open", "high", "low", "close", "volume"):
        original = equity[field]
        parsed = pd.to_numeric(original, errors="coerce")
        numeric[field] = parsed.astype("float64")
        flag(original.isna(), f"missing_{field}")
        flag(original.notna() & parsed.isna(), f"malformed_{field}")
        flag(parsed.notna() & ~np.isfinite(parsed.to_numpy(dtype="float64", na_value=np.nan)), f"nonfinite_{field}")

    for field in ("open", "high", "low", "close"):
        flag(numeric[field].notna() & numeric[field].le(0), f"nonpositive_{field}")
    flag(numeric["volume"].notna() & numeric["volume"].lt(0), "negative_volume")

    valid_prices = pd.DataFrame({field: numeric[field] for field in ("open", "high", "low", "close")})
    high_floor = valid_prices[["open", "close", "low"]].max(axis=1)
    low_ceiling = valid_prices[["open", "close", "high"]].min(axis=1)
    flag(numeric["high"].notna() & high_floor.notna() & (numeric["high"] + OHLC_TOLERANCE < high_floor), "invalid_high_relationship")
    flag(numeric["low"].notna() & low_ceiling.notna() & (numeric["low"] - OHLC_TOLERANCE > low_ceiling), "invalid_low_relationship")

    output = []
    for position, row_issues in enumerate(issues):
        if row_issues:
            row = equity.iloc[position]
            output.append({
                "source_index": str(equity.index[position]),
                "date": str(row["date"]),
                "symbol": None if pd.isna(row["symbol"]) else str(row["symbol"]),
                "series": None if pd.isna(row["series"]) else str(row["series"]),
                "open": row["open"], "high": row["high"], "low": row["low"],
                "close": row["close"], "volume": row["volume"],
                "issues": ";".join(row_issues),
            })
    return pd.DataFrame(output, columns=[
        "source_index", "date", "symbol", "series", "open", "high", "low", "close", "volume", "issues",
    ])


def normalize_eq_rows(frame: pd.DataFrame) -> pd.DataFrame:
    """Select explicit EQ rows into the research schema without changing prices."""
    missing_columns = [column for column in RAW_REQUIRED_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise TejHQDataError(f"TejHQ NSE raw schema is missing columns: {missing_columns}")
    selected = frame.loc[frame["series"].astype("string").eq("EQ").fillna(False), list(RAW_REQUIRED_COLUMNS)].copy()
    if selected.empty:
        return pd.DataFrame(columns=NORMALIZED_COLUMNS)
    anomalies = equity_quality_anomalies(selected)
    if not anomalies.empty:
        raise TejHQDataError(f"cannot normalize {len(anomalies)} anomalous EQ rows")
    selected["date"] = pd.to_datetime(selected["date"], errors="raise").dt.normalize()
    for field in ("open", "high", "low", "close", "prev_close", "volume", "turnover"):
        selected[field] = pd.to_numeric(selected[field], errors="raise")
    selected["symbol"] = selected["symbol"].astype("string")
    selected["series"] = selected["series"].astype("string")
    selected["isin"] = selected["isin"].astype("string")
    duplicated = selected.duplicated(["date", "symbol", "series"], keep=False)
    if duplicated.any():
        raise TejHQDataError(f"cannot normalize duplicate (date, symbol, series) rows: {int(duplicated.sum())}")
    return selected.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)


class TejHQProvider(HistoricalDataProvider):
    """Read validated TejHQ EQ bars from an explicitly supplied local snapshot."""

    provider = "tejhq"
    dataset_id = DATASET_ID

    def __init__(
        self,
        normalized_path: str | Path,
        manifest_path: str | Path | None = None,
    ) -> None:
        self.normalized_path = Path(normalized_path)
        if not self.normalized_path.is_file():
            raise FileNotFoundError(self.normalized_path)
        self.manifest_path = Path(manifest_path) if manifest_path else self.normalized_path.parents[1] / "tejhq_dataset_manifest.json"
        if not self.manifest_path.is_file():
            raise FileNotFoundError(self.manifest_path)
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        if manifest.get("dataset_id") != DATASET_ID:
            raise TejHQDataError("manifest dataset_id does not identify tejhq/indian-markets")
        normalized = manifest.get("normalized_dataset") or {}
        recorded_path = normalized.get("path")
        if recorded_path and Path(recorded_path).name != self.normalized_path.name:
            raise TejHQDataError("manifest normalized dataset filename does not match the supplied file")
        recorded_digest = normalized.get("sha256")
        if not recorded_digest or _sha256(self.normalized_path) != recorded_digest:
            raise TejHQDataError("normalized dataset SHA-256 does not match its manifest")
        self.dataset_revision = manifest.get("dataset_revision")
        self.snapshot_sha256 = manifest.get("snapshot_sha256")
        self.metadata = {
            "provider": self.provider,
            "dataset_id": DATASET_ID,
            "dataset_revision": self.dataset_revision,
            "snapshot_sha256": self.snapshot_sha256,
        }
        if not self.dataset_revision or not self.snapshot_sha256:
            raise TejHQDataError("manifest is missing the source revision or snapshot SHA-256")

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        if start > end:
            raise ValueError("start date must be on or before end date")
        if interval != "1d":
            raise ValueError("TejHQ NSE source contains daily bars only")
        requested = str(symbol).strip().upper().removesuffix(".NS")
        if not requested:
            raise ValueError("symbol must be non-empty")
        try:
            selected = pd.read_parquet(
                self.normalized_path,
                columns=["date", "symbol", "open", "high", "low", "close", "volume"],
                filters=[
                    ("symbol", "==", requested),
                    ("date", ">=", pd.Timestamp(start)),
                    ("date", "<=", pd.Timestamp(end)),
                ],
            )
        except ImportError as exc:
            raise TejHQDataError("reading the TejHQ Parquet snapshot requires pyarrow") from exc
        if selected.empty:
            return pd.DataFrame(columns=REQUIRED_COLUMNS, index=pd.DatetimeIndex([], name="timestamp"))
        selected["date"] = pd.to_datetime(selected["date"], errors="raise")
        selected = selected.set_index("date").sort_index()
        if selected.index.duplicated().any():
            raise TejHQDataError(f"normalized TejHQ data contains duplicate dates for {requested}")
        selected.index.name = "timestamp"
        return selected.loc[:, REQUIRED_COLUMNS]

