"""Liquid equity universe construction and liquidity screens."""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.request import Request, urlopen
from io import BytesIO

import pandas as pd

LOGGER = logging.getLogger(__name__)


def remove_placeholder_symbols(symbols: list[str]) -> tuple[list[str], list[dict[str, str]]]:
    """Exclude non-tradable placeholder rows whose NSE symbol begins with DUMMY."""
    tradable: list[str] = []
    excluded: list[dict[str, str]] = []
    for symbol in symbols:
        if symbol.upper().split(".", maxsplit=1)[0].startswith("DUMMY"):
            excluded.append({"symbol": symbol, "reason": "placeholder constituent row; not a tradable security"})
        else:
            tradable.append(symbol)
    return tradable, excluded


def load_constituents(url: str, cache_path: str | Path, max_symbols: int | None = None) -> list[str]:
    """Fetch NSE Indices constituents; fall back only to a previously saved official file."""
    cache = Path(cache_path)
    frame: pd.DataFrame | None = None
    try:
        request = Request(url, headers={"User-Agent": "Mozilla/5.0 NSE-equity-research/0.1"})
        with urlopen(request, timeout=12) as response:
            frame = pd.read_csv(BytesIO(response.read()))
        cache.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(cache, index=False)
    except Exception as exc:  # provider/network failures should leave an auditable message
        LOGGER.warning("Could not fetch constituent list from %s: %s", url, exc)
        if cache.exists():
            frame = pd.read_csv(cache)
    if frame is None or frame.empty:
        raise RuntimeError("NSE constituent list unavailable; provide a valid cached constituent CSV")
    symbol_column = next((column for column in frame.columns if str(column).strip().lower() in {"symbol", "ticker"}), None)
    if symbol_column is None:
        raise ValueError("Constituent CSV must contain a Symbol or Ticker column")
    symbols = frame[symbol_column].astype(str).str.strip().str.upper()
    symbols = [symbol for symbol in symbols if symbol and symbol != "NAN"]
    symbols = list(dict.fromkeys(f"{symbol}.NS" for symbol in symbols))
    return symbols[:max_symbols] if max_symbols else symbols


def load_sector_map(path: str | Path) -> dict[str, str]:
    """Read sector/industry labels from a configurable constituent CSV."""
    file_path = Path(path)
    if not file_path.exists():
        return {}
    frame = pd.read_csv(file_path)
    symbol_column = next((name for name in frame.columns if str(name).strip().lower() in {"symbol", "ticker"}), None)
    sector_column = next((name for name in frame.columns if str(name).strip().lower() in {"industry", "sector"}), None)
    if symbol_column is None or sector_column is None:
        return {}
    output: dict[str, str] = {}
    for symbol, sector in frame[[symbol_column, sector_column]].dropna().itertuples(index=False, name=None):
        raw = str(symbol).strip().upper()
        suffix = "" if raw.endswith(".NS") or raw.startswith("^") else ".NS"
        output[raw + suffix] = str(sector).strip() or "UNKNOWN"
    return output


def apply_liquidity_filter(
    bars: pd.DataFrame,
    min_rows: int,
    min_avg_volume: float,
    min_avg_value: float,
    min_price: float,
    lookback: int = 20,
) -> tuple[pd.DataFrame, str | None]:
    """Use only preceding observations when judging tradability at the current bar."""
    if len(bars) < min_rows:
        return bars, f"insufficient history: {len(bars)} rows, need {min_rows}"
    sample = bars.tail(lookback)
    avg_volume = float(sample["volume"].mean())
    avg_value = float((sample["close"] * sample["volume"]).mean())
    price = float(bars["close"].iloc[-1])
    if price < min_price:
        return bars, f"price ₹{price:.2f} below minimum ₹{min_price:.2f}"
    if avg_volume < min_avg_volume:
        return bars, f"average volume {avg_volume:.0f} below minimum {min_avg_volume:.0f}"
    if avg_value < min_avg_value:
        return bars, f"average traded value ₹{avg_value:.0f} below minimum ₹{min_avg_value:.0f}"
    return bars, None
