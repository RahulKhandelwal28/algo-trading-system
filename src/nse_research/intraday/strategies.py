"""Closed-bar intraday strategies; all outputs are analytical signals only."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Mapping, Sequence

from .indicators import IndicatorSnapshot
from .model import IST, IntradayBar, TIMEFRAME_MINUTES


@dataclass(frozen=True, slots=True)
class StrategySettings:
    momentum_lookback_bars: int = 3
    pullback_tolerance_bps: float = 25.0
    breakout_lookback_5m_bars: int = 3
    mean_reversion_atr_multiple: float = 1.5
    mean_reversion_rsi_max: float = 35.0
    momentum_rsi_min: float = 50.0
    momentum_rsi_filter_enabled: bool = False
    volume_breakout_multiple: float = 1.5
    stop_atr_multiple: float = 1.0
    target_r_multiple: float = 1.5
    signal_ttl_bars: int = 3

    def __post_init__(self) -> None:
        if self.momentum_lookback_bars < 1 or self.breakout_lookback_5m_bars < 1 or self.signal_ttl_bars < 1:
            raise ValueError("strategy lookbacks and signal lifetime must be positive")
        for name in ("pullback_tolerance_bps", "mean_reversion_atr_multiple", "volume_breakout_multiple", "stop_atr_multiple", "target_r_multiple"):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if not 0 <= self.mean_reversion_rsi_max <= 100 or not 0 <= self.momentum_rsi_min <= 100:
            raise ValueError("RSI thresholds must be in [0, 100]")


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    strategy: str
    timeframe: str
    setup_id: str
    reason_codes: tuple[str, ...]
    confidence_metadata: Mapping[str, float | int | str | bool | None]


@dataclass(frozen=True, slots=True)
class IntradaySignal:
    signal_id: str
    timestamp: datetime
    valid_until: datetime
    symbol: str
    exchange: str
    series: str
    side: str
    strategy: str
    timeframe: str
    reference_price: float
    stop_reference: float
    target_reference: float
    confidence_metadata: Mapping[str, float | int | str | bool | None]
    reason_codes: tuple[str, ...]
    setup_id: str


@dataclass(frozen=True, slots=True)
class RegimeContext:
    """Optional, externally validated descriptive market context."""

    timestamp: datetime
    nifty_trend: str | None = None
    india_vix: float | None = None
    allow_long: bool = False
    source: str = ""


def _candidate(name: str, bar: IntradayBar, reasons: tuple[str, ...], values: dict) -> StrategyCandidate:
    # Stable during one uninterrupted setup. The signal identity still includes
    # event time; this key lets the scanner suppress repeated same-setup bars.
    setup = f"{name}:{bar.symbol}:{bar.trading_date.isoformat()}"
    return StrategyCandidate(name, bar.timeframe, setup, reasons, values)


def vwap_trend(bar: IntradayBar, snapshot: IndicatorSnapshot, previous: Sequence[IntradayBar]) -> StrategyCandidate | None:
    if snapshot.vwap is None or snapshot.ema9 is None or snapshot.ema20 is None or not previous:
        return None
    if bar.close > snapshot.vwap and snapshot.ema9 > snapshot.ema20 and bar.close > previous[-1].close:
        return _candidate("vwap_trend", bar, ("CLOSE_ABOVE_SESSION_VWAP", "EMA9_ABOVE_EMA20", "CLOSED_BAR_MOMENTUM_POSITIVE"),
                          {"close": bar.close, "vwap": snapshot.vwap, "ema9": snapshot.ema9, "ema20": snapshot.ema20})
    return None


def vwap_pullback(
    bar: IntradayBar,
    snapshot: IndicatorSnapshot,
    previous: Sequence[IntradayBar],
    previous_snapshots: Sequence[IndicatorSnapshot],
    settings: StrategySettings,
) -> StrategyCandidate | None:
    if snapshot.vwap is None or snapshot.ema9 is None or snapshot.ema20 is None or not previous or not previous_snapshots:
        return None
    prior = previous[-1]
    prior_snapshot = previous_snapshots[-1]
    tolerance = settings.pullback_tolerance_bps / 10_000.0
    touched = prior.low <= (prior_snapshot.vwap * (1.0 + tolerance)) if prior_snapshot.vwap is not None else False
    if snapshot.ema9 > snapshot.ema20 and bar.close > snapshot.vwap and bar.close > prior.close and touched:
        return _candidate("vwap_pullback", bar, ("UPTREND_CONFIRMED", "PRIOR_BAR_TOUCHED_VWAP_ZONE", "CLOSED_BAR_RECLAIM"),
                          {"close": bar.close, "vwap": snapshot.vwap, "pullback_tolerance_bps": settings.pullback_tolerance_bps})
    return None


def opening_range_breakout(bar: IntradayBar, snapshot: IndicatorSnapshot, previous: Sequence[IntradayBar], *, window_minutes: int) -> StrategyCandidate | None:
    if bar.timeframe != "5m" or not previous:
        return None
    high = snapshot.opening_range_5m_high if window_minutes == 5 else snapshot.opening_range_15m_high
    low = snapshot.opening_range_5m_low if window_minutes == 5 else snapshot.opening_range_15m_low
    if high is None or low is None or bar.timestamp < previous[0].timestamp:
        return None
    if bar.timestamp.astimezone(IST).time() < time(9, 20 if window_minutes == 5 else 30):
        return None
    if bar.close > high and previous[-1].close <= high:
        return _candidate(f"orb_5m_{window_minutes}min", bar, ("OPENING_RANGE_COMPLETE", "CLOSED_BAR_BREAKOUT_ABOVE_RANGE"),
                          {"opening_range_high": high, "opening_range_low": low, "close": bar.close})
    return None


def breakout_5m(bar: IntradayBar, snapshot: IndicatorSnapshot, previous: Sequence[IntradayBar], settings: StrategySettings) -> StrategyCandidate | None:
    if bar.timeframe != "5m" or len(previous) < settings.breakout_lookback_5m_bars:
        return None
    prior = previous[-settings.breakout_lookback_5m_bars:]
    level = max(item.high for item in prior)
    if bar.close > level and bar.close > prior[-1].close:
        return _candidate("breakout_5m", bar, ("PRIOR_COMPLETED_RANGE_ONLY", "CLOSED_BAR_BREAKOUT"),
                          {"prior_range_high": level, "close": bar.close, "lookback_bars": settings.breakout_lookback_5m_bars})
    return None


def momentum(bar: IntradayBar, snapshot: IndicatorSnapshot, previous: Sequence[IntradayBar], settings: StrategySettings) -> StrategyCandidate | None:
    if snapshot.ema9 is None or snapshot.ema20 is None or len(previous) < settings.momentum_lookback_bars:
        return None
    ref = previous[-settings.momentum_lookback_bars].close
    if bar.close <= ref or snapshot.ema9 <= snapshot.ema20:
        return None
    if settings.momentum_rsi_filter_enabled and (snapshot.rsi14 is None or snapshot.rsi14 < settings.momentum_rsi_min):
        return None
    return _candidate("momentum", bar, ("EMA9_ABOVE_EMA20", "CLOSED_BAR_POSITIVE_MOMENTUM"),
                      {"close": bar.close, "momentum_reference": ref, "rsi14": snapshot.rsi14,
                       "rsi_filter_enabled": settings.momentum_rsi_filter_enabled})


def mean_reversion(
    bar: IntradayBar, snapshot: IndicatorSnapshot, previous: Sequence[IntradayBar], previous_snapshots: Sequence[IndicatorSnapshot],
    settings: StrategySettings,
) -> StrategyCandidate | None:
    if snapshot.vwap is None or snapshot.atr14 is None or not previous or not previous_snapshots:
        return None
    prior = previous[-1]
    prior_snapshot = previous_snapshots[-1]
    if prior_snapshot.rsi14 is None or prior_snapshot.rsi14 > settings.mean_reversion_rsi_max:
        return None
    threshold = snapshot.vwap - snapshot.atr14 * settings.mean_reversion_atr_multiple
    if prior.close <= threshold and bar.close > prior.close and bar.close > bar.open:
        return _candidate("mean_reversion", bar, ("PRIOR_BAR_OVERSOLD_BELOW_VWAP", "CLOSED_BAR_REVERSAL_CONFIRMATION"),
                          {"close": bar.close, "vwap": snapshot.vwap, "atr14": snapshot.atr14,
                           "deviation_threshold": threshold, "prior_rsi14": prior_snapshot.rsi14})
    return None


def volume_breakout(bar: IntradayBar, snapshot: IndicatorSnapshot, previous: Sequence[IntradayBar], settings: StrategySettings) -> StrategyCandidate | None:
    if bar.volume is None or bar.volume_kind != "BAR" or snapshot.prior_volume_avg_20 is None:
        return None
    if len(previous) < settings.breakout_lookback_5m_bars:
        return None
    level = max(item.high for item in previous[-settings.breakout_lookback_5m_bars:])
    threshold = snapshot.prior_volume_avg_20 * settings.volume_breakout_multiple
    if bar.close > level and bar.volume > threshold:
        return _candidate("volume_breakout", bar, ("CLOSED_BAR_BREAKOUT", "BAR_VOLUME_ABOVE_PRIOR_20_BAR_AVERAGE"),
                          {"close": bar.close, "prior_range_high": level, "volume": bar.volume,
                           "prior_volume_average_20": snapshot.prior_volume_avg_20, "threshold": threshold})
    return None


def evaluate_candidates(
    bar: IntradayBar,
    snapshot: IndicatorSnapshot,
    previous: Sequence[IntradayBar],
    previous_snapshots: Sequence[IndicatorSnapshot],
    settings: StrategySettings,
) -> list[StrategyCandidate]:
    """Evaluate independent long-only strategies with closed-bar data only."""
    ordered = (
        vwap_trend(bar, snapshot, previous),
        vwap_pullback(bar, snapshot, previous, previous_snapshots, settings),
        opening_range_breakout(bar, snapshot, previous, window_minutes=5),
        opening_range_breakout(bar, snapshot, previous, window_minutes=15),
        breakout_5m(bar, snapshot, previous, settings),
        momentum(bar, snapshot, previous, settings),
        mean_reversion(bar, snapshot, previous, previous_snapshots, settings),
        volume_breakout(bar, snapshot, previous, settings),
    )
    return [item for item in ordered if item is not None]


def candidate_to_signal(candidate: StrategyCandidate, bar: IntradayBar, snapshot: IndicatorSnapshot, settings: StrategySettings) -> IntradaySignal | None:
    """Build analytical reference levels; deliberately calculates no quantity."""
    reference = bar.close
    risk_distance = snapshot.atr14 * settings.stop_atr_multiple if snapshot.atr14 is not None else bar.high - bar.low
    if not risk_distance or risk_distance <= 0:
        return None
    stop = max(0.0, reference - risk_distance)
    if stop <= 0 or stop >= reference:
        return None
    target = reference + (reference - stop) * settings.target_r_multiple
    timestamp = bar.end
    ttl = timedelta(minutes=TIMEFRAME_MINUTES[bar.timeframe] * settings.signal_ttl_bars)
    identity = "|".join((bar.symbol, candidate.strategy, bar.timeframe, "LONG", timestamp.isoformat(), candidate.setup_id))
    signal_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    return IntradaySignal(
        signal_id=signal_id,
        timestamp=timestamp,
        valid_until=timestamp + ttl,
        symbol=bar.symbol,
        exchange=bar.exchange,
        series=bar.series,
        side="LONG",
        strategy=candidate.strategy,
        timeframe=bar.timeframe,
        reference_price=reference,
        stop_reference=stop,
        target_reference=target,
        confidence_metadata=dict(candidate.confidence_metadata),
        reason_codes=candidate.reason_codes,
        setup_id=candidate.setup_id,
    )
