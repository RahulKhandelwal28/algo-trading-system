"""Indicator and market-regime calculations using past bars only."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def add_features(
    bars: pd.DataFrame,
    atr_period: int = 14,
    rsi_period: int = 14,
    ema_fast: int = 20,
    ema_slow: int = 50,
    ema_long: int = 200,
    adx_period: int = 14,
    india_vix: pd.Series | None = None,
    india_vix_high_threshold: float = 25.0,
) -> pd.DataFrame:
    """Return bars with causal indicators; all rolling signals include current bar only."""
    frame = bars.copy().sort_index()
    high, low, close = frame["high"], frame["low"], frame["close"]
    frame["ema20"] = close.ewm(span=ema_fast, adjust=False, min_periods=ema_fast).mean()
    frame["ema50"] = close.ewm(span=ema_slow, adjust=False, min_periods=ema_slow).mean()
    frame["ema200"] = close.ewm(span=ema_long, adjust=False, min_periods=ema_long).mean()
    frame["sma20"] = close.rolling(20, min_periods=20).mean()
    frame["sma50"] = close.rolling(50, min_periods=50).mean()

    previous_close = close.shift(1)
    true_range = pd.concat(
        [(high - low), (high - previous_close).abs(), (low - previous_close).abs()], axis=1
    ).max(axis=1)
    frame["atr"] = true_range.ewm(alpha=1 / atr_period, adjust=False, min_periods=atr_period).mean()
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    smoothed_tr = true_range.ewm(alpha=1 / adx_period, adjust=False, min_periods=adx_period).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / adx_period, adjust=False, min_periods=adx_period).mean() / smoothed_tr
    minus_di = 100 * minus_dm.ewm(alpha=1 / adx_period, adjust=False, min_periods=adx_period).mean() / smoothed_tr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    frame["adx"] = dx.ewm(alpha=1 / adx_period, adjust=False, min_periods=adx_period).mean()
    frame["plus_di"] = plus_di
    frame["minus_di"] = minus_di

    change = close.diff()
    average_gain = change.clip(lower=0).ewm(alpha=1 / rsi_period, adjust=False, min_periods=rsi_period).mean()
    average_loss = -change.clip(upper=0).ewm(alpha=1 / rsi_period, adjust=False, min_periods=rsi_period).mean()
    rs = average_gain / average_loss.replace(0, np.nan)
    frame["rsi"] = 100 - 100 / (1 + rs)
    frame.loc[(average_loss == 0) & (average_gain > 0), "rsi"] = 100.0
    frame["rsi"] = frame["rsi"].fillna(50.0)
    fast = close.ewm(span=12, adjust=False, min_periods=12).mean()
    slow = close.ewm(span=26, adjust=False, min_periods=26).mean()
    frame["macd"] = fast - slow
    frame["macd_signal"] = frame["macd"].ewm(span=9, adjust=False, min_periods=9).mean()

    middle = close.rolling(20, min_periods=20).mean()
    deviation = close.rolling(20, min_periods=20).std(ddof=0)
    frame["bb_middle"] = middle
    frame["bb_upper"] = middle + 2 * deviation
    frame["bb_lower"] = middle - 2 * deviation
    frame["prior_high20"] = high.rolling(20, min_periods=20).max().shift(1)
    frame["prior_high30"] = high.rolling(30, min_periods=30).max().shift(1)
    frame["prior_high50"] = high.rolling(50, min_periods=50).max().shift(1)
    frame["prior_high100"] = high.rolling(100, min_periods=100).max().shift(1)
    frame["prior_low20"] = low.rolling(20, min_periods=20).min().shift(1)
    frame["prior_low10"] = low.rolling(10, min_periods=10).min().shift(1)
    frame["prior_high1"] = high.shift(1)
    frame["prior_low1"] = low.shift(1)
    frame["volume_mean20"] = frame["volume"].rolling(20, min_periods=20).mean()
    frame["traded_value_mean20"] = (frame["close"] * frame["volume"]).rolling(20, min_periods=20).mean()
    frame["volume_ratio"] = frame["volume"] / frame["volume_mean20"].replace(0, np.nan)
    frame["atr_pct"] = frame["atr"] / close
    frame["return"] = close.pct_change()
    frame["realized_vol20"] = frame["return"].rolling(20, min_periods=20).std(ddof=0) * np.sqrt(252)
    frame["roc20"] = close.pct_change(20) * 100
    frame["momentum20"] = close - close.shift(20)
    frame["volatility"] = frame["realized_vol20"]

    # Session-reset intraday VWAP. With daily bars it is only the bar typical price.
    session_key = pd.Index(frame.index.date)
    typical = (high + low + close) / 3
    volume = frame["volume"].where(frame["volume"] > 0, np.nan)
    cumulative_pv = (typical * volume).groupby(session_key).cumsum()
    cumulative_volume = volume.groupby(session_key).cumsum()
    frame["vwap"] = cumulative_pv / cumulative_volume
    session_high = frame.groupby(session_key)["high"].transform("max")
    session_low = frame.groupby(session_key)["low"].transform("min")
    prior_session_high = pd.Series(session_high.to_numpy(), index=frame.index).groupby(session_key).first().shift(1)
    prior_session_low = pd.Series(session_low.to_numpy(), index=frame.index).groupby(session_key).first().shift(1)
    frame["prior_day_high"] = [prior_session_high.get(day, np.nan) for day in session_key]
    frame["prior_day_low"] = [prior_session_low.get(day, np.nan) for day in session_key]
    # Opening range is the first 30 minutes. It becomes usable only from 09:45.
    if isinstance(frame.index, pd.DatetimeIndex) and len(frame.index):
        minute_of_day = frame.index.hour * 60 + frame.index.minute
        opening = (minute_of_day >= 9 * 60 + 15) & (minute_of_day < 9 * 60 + 45)
        range_high = frame.loc[opening, "high"].groupby(pd.Index(frame.index[opening].date)).max()
        range_low = frame.loc[opening, "low"].groupby(pd.Index(frame.index[opening].date)).min()
        frame["opening_range_high"] = [range_high.get(day, np.nan) for day in session_key]
        frame["opening_range_low"] = [range_low.get(day, np.nan) for day in session_key]
        frame.loc[minute_of_day < 9 * 60 + 45, ["opening_range_high", "opening_range_low"]] = np.nan
    if india_vix is not None and not india_vix.empty:
        vix = india_vix.copy()
        vix.index = pd.Index(pd.to_datetime(vix.index).date)
        vix = vix[~vix.index.duplicated(keep="last")]
        frame["india_vix"] = [vix.get(pd.Timestamp(stamp).date(), np.nan) for stamp in frame.index]
    else:
        frame["india_vix"] = np.nan
    frame["regime"] = classify_regime(frame, india_vix_high_threshold=india_vix_high_threshold)
    return frame


def classify_regime(
    frame: pd.DataFrame,
    adx_minimum: float = 20,
    india_vix_high_threshold: float = 25.0,
) -> pd.Series:
    """Classify the supplied benchmark series without peeking beyond each row."""
    close = frame["close"]
    rsi = frame.get("rsi", pd.Series(50.0, index=frame.index))
    strong_bull = (close > frame["ema20"]) & (frame["ema20"] > frame["ema50"]) & (frame["ema50"] > frame["ema200"]) & (frame["adx"] >= adx_minimum + 8) & (rsi >= 55)
    bull = (close > frame["ema50"]) & (frame["ema20"] > frame["ema50"]) & (rsi >= 50)
    strong_bear = (close < frame["ema20"]) & (frame["ema20"] < frame["ema50"]) & (frame["ema50"] < frame["ema200"]) & (frame["adx"] >= adx_minimum + 8) & (rsi <= 45)
    bear = (close < frame["ema50"]) & (frame["ema20"] < frame["ema50"]) & (rsi <= 50)
    sideways = frame["adx"] < adx_minimum
    regime = pd.Series("UNKNOWN", index=frame.index, dtype="object")
    regime.loc[sideways] = "SIDEWAYS"
    regime.loc[bear] = "BEAR"
    regime.loc[strong_bear] = "STRONG_BEAR"
    regime.loc[bull] = "BULL"
    regime.loc[strong_bull] = "STRONG_BULL"
    high_volatility = frame["realized_vol20"] > 0.55
    vix = frame.get("india_vix")
    if vix is not None:
        high_volatility |= vix >= india_vix_high_threshold
    regime.loc[high_volatility.fillna(False)] = "HIGH_VOLATILITY"
    return regime


def align_regime(features: pd.DataFrame, benchmark_features: pd.DataFrame | None) -> pd.Series:
    """Map completed benchmark regime values onto stock bar dates."""
    if benchmark_features is None or benchmark_features.empty:
        return features["regime"].astype(str)
    benchmark = benchmark_features["regime"].copy()
    benchmark.index = pd.Index(benchmark.index.date)
    benchmark = benchmark[~benchmark.index.duplicated(keep="last")]
    return pd.Series([benchmark.get(pd.Timestamp(timestamp).date(), "UNKNOWN") for timestamp in features.index], index=features.index, dtype="object")


def finite(value: Any) -> bool:
    """True when an indicator value is numeric and finite."""
    try:
        return bool(np.isfinite(float(value)))
    except (TypeError, ValueError):
        return False
