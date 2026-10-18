#!/usr/bin/env python3
"""Read one Yahoo Finance BTC/USD 1-minute sample; never enters the NSE paper flow.

This is a connectivity and OHLCV parsing smoke check only. It creates no files,
signals, orders, candles in the project store, or ledger entries.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import yfinance as yf


TICKER = "BTC-USD"
INTERVAL = "1m"
MAX_DATA_AGE_SECONDS = 15 * 60
OHLCV_FIELDS = ("Open", "High", "Low", "Close", "Volume")


def _flatten_yfinance_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Select BTC/USD OHLCV columns from flat or yfinance MultiIndex output."""
    selected: dict[str, pd.Series] = {}
    for expected in OHLCV_FIELDS:
        matches = []
        for column in frame.columns:
            parts = column if isinstance(column, tuple) else (column,)
            if any(str(part).strip().casefold() == expected.casefold() for part in parts):
                matches.append(column)
        if len(matches) != 1:
            raise ValueError(f"expected one {expected} column, found {len(matches)}")
        selected[expected] = frame[matches[0]]
    result = pd.DataFrame(selected, index=frame.index)
    for field in OHLCV_FIELDS:
        result[field] = pd.to_numeric(result[field], errors="coerce")
    return result


def run_smoke_check() -> tuple[dict[str, Any], int]:
    requested_at = datetime.now(timezone.utc)
    try:
        frame = yf.download(
            tickers=TICKER,
            period="1d",
            interval=INTERVAL,
            auto_adjust=False,
            back_adjust=False,
            repair=False,
            actions=False,
            prepost=False,
            keepna=True,
            threads=False,
            progress=False,
            timeout=20,
            group_by="ticker",
            multi_level_index=True,
        )
        if frame is None or frame.empty:
            raise ValueError("Yahoo returned no BTC/USD 1-minute bars")
        numeric_bars = _flatten_yfinance_frame(frame)
        incomplete_rows = int(numeric_bars.isna().any(axis=1).sum())
        bars = numeric_bars.dropna(subset=list(OHLCV_FIELDS))
        if bars.empty:
            raise ValueError("Yahoo returned no complete numeric OHLCV bars")

        invalid_rows = incomplete_rows
        for row in bars.itertuples(index=False):
            open_, high, low, close, volume = map(float, row)
            valid = (
                all(math.isfinite(value) for value in (open_, high, low, close, volume))
                and min(open_, high, low, close) > 0
                and volume >= 0
                and high >= max(open_, close, low)
                and low <= min(open_, close, high)
            )
            invalid_rows += not valid

        latest = pd.Timestamp(bars.index[-1])
        if latest.tzinfo is None:
            latest = latest.tz_localize("UTC")
        latest_utc = latest.tz_convert("UTC")
        age_seconds = max(0.0, (requested_at - latest_utc.to_pydatetime()).total_seconds())
        status = "PASS" if invalid_rows == 0 and age_seconds <= MAX_DATA_AGE_SECONDS else "DATA_RECEIVED_BUT_NOT_FRESH_OR_VALID"
        report = {
            "status": status,
            "provider": "Yahoo Finance via yfinance",
            "classification": "THIRD_PARTY_INTRADAY_DATA_NOT_AN_AUTHORIZED_NSE_FEED",
            "ticker": TICKER,
            "interval": INTERVAL,
            "requested_at_utc": requested_at.isoformat(),
            "first_bar_utc": pd.Timestamp(bars.index[0]).tz_localize("UTC").isoformat()
            if pd.Timestamp(bars.index[0]).tzinfo is None else pd.Timestamp(bars.index[0]).tz_convert("UTC").isoformat(),
            "latest_bar_utc": latest_utc.isoformat(),
            "latest_bar_age_seconds": round(age_seconds, 2),
            "bars_received": int(len(bars)),
            "invalid_ohlcv_rows": int(invalid_rows),
            "latest_bar": {
                field: float(bars.iloc[-1][field]) for field in OHLCV_FIELDS
            },
            "scope": "connectivity and response parsing only; no NSE strategy, signal, paper order, or ledger is involved",
        }
        return report, 0 if status == "PASS" else 2
    except Exception as exc:
        return {
            "status": "DATA_UNAVAILABLE_OR_INVALID",
            "provider": "Yahoo Finance via yfinance",
            "ticker": TICKER,
            "interval": INTERVAL,
            "requested_at_utc": requested_at.isoformat(),
            "error_type": type(exc).__name__,
            "error": str(exc),
            "scope": "one read-only request; no retry, signals, orders, or ledger writes",
        }, 2


if __name__ == "__main__":
    result, exit_code = run_smoke_check()
    print(json.dumps(result, indent=2))
    raise SystemExit(exit_code)
