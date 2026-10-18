"""Incremental session-scoped intraday indicator calculations."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from statistics import mean
from typing import Iterable

from .model import IST, TIMEFRAME_MINUTES, IntradayBar, validate_intraday_bar


@dataclass(frozen=True, slots=True)
class IndicatorSnapshot:
    symbol: str
    timeframe: str
    timestamp: datetime
    close: float
    vwap: float | None
    vwap_status: str
    ema9: float | None
    ema20: float | None
    ema50: float | None
    rsi14: float | None
    atr14: float | None
    volume_avg_20: float | None
    prior_volume_avg_20: float | None
    opening_range_5m_high: float | None
    opening_range_5m_low: float | None
    opening_range_15m_high: float | None
    opening_range_15m_low: float | None
    bars_in_session: int
    indicator_ready: bool


class IntradayIndicatorState:
    """O(1) updates per bar; all state is cleared when the session changes."""

    EMA_PERIODS = (9, 20, 50)
    RSI_PERIOD = 14
    ATR_PERIOD = 14
    VOLUME_PERIOD = 20

    def __init__(self, timeframe: str) -> None:
        if timeframe not in TIMEFRAME_MINUTES:
            raise ValueError("indicator timeframe must be 1m or 5m")
        self.timeframe = timeframe
        self._reset(None)

    def _reset(self, session_date: date | None) -> None:
        self.session_date = session_date
        self.bars: deque[IntradayBar] = deque(maxlen=256)
        self._ema_values: dict[int, float | None] = {period: None for period in self.EMA_PERIODS}
        self._ema_seeds: dict[int, list[float]] = {period: [] for period in self.EMA_PERIODS}
        self._previous_close: float | None = None
        self._rsi_seed_gains: list[float] = []
        self._rsi_seed_losses: list[float] = []
        self._rsi_avg_gain: float | None = None
        self._rsi_avg_loss: float | None = None
        self._atr_seed: list[float] = []
        self._atr: float | None = None
        self._vwap_pv = 0.0
        self._vwap_volume = 0.0
        self._vwap_valid = True
        self._opening_5: tuple[float, float] | None = None
        self._opening_15: tuple[float, float] | None = None
        self._session_bar_count = 0

    @property
    def history(self) -> tuple[IntradayBar, ...]:
        return tuple(self.bars)

    def update(self, bar: IntradayBar) -> IndicatorSnapshot:
        item = validate_intraday_bar(bar, require_closed=True)
        if item.timeframe != self.timeframe:
            raise ValueError("bar timeframe does not match indicator state")
        if self.session_date != item.trading_date:
            self._reset(item.trading_date)
        if self.bars and item.timestamp <= self.bars[-1].timestamp:
            raise ValueError("indicator input bars must be strictly chronological within a session")

        typical = (item.high + item.low + item.close) / 3.0
        if item.volume is None or item.volume_kind != "BAR":
            self._vwap_valid = False
        elif self._vwap_valid:
            self._vwap_pv += typical * item.volume
            self._vwap_volume += item.volume
        vwap = self._vwap_pv / self._vwap_volume if self._vwap_valid and self._vwap_volume > 0 else None

        prior_volumes = [b.volume for b in list(self.bars)[-self.VOLUME_PERIOD:]]
        prior_volume_avg = None
        if len(prior_volumes) == self.VOLUME_PERIOD and all(v is not None and b.volume_kind == "BAR" for v, b in zip(prior_volumes, list(self.bars)[-self.VOLUME_PERIOD:])):
            prior_volume_avg = mean(float(v) for v in prior_volumes if v is not None)

        close = item.close
        for period in self.EMA_PERIODS:
            if self._ema_values[period] is None:
                seed = self._ema_seeds[period]
                seed.append(close)
                if len(seed) == period:
                    self._ema_values[period] = mean(seed)
                    seed.clear()
            else:
                previous = self._ema_values[period]
                alpha = 2.0 / (period + 1.0)
                self._ema_values[period] = alpha * close + (1.0 - alpha) * float(previous)

        if self._previous_close is None:
            true_range = item.high - item.low
        else:
            delta = close - self._previous_close
            gain, loss = max(delta, 0.0), max(-delta, 0.0)
            if self._rsi_avg_gain is None:
                self._rsi_seed_gains.append(gain)
                self._rsi_seed_losses.append(loss)
                if len(self._rsi_seed_gains) == self.RSI_PERIOD:
                    self._rsi_avg_gain = mean(self._rsi_seed_gains)
                    self._rsi_avg_loss = mean(self._rsi_seed_losses)
                    self._rsi_seed_gains.clear()
                    self._rsi_seed_losses.clear()
            else:
                self._rsi_avg_gain = ((self.RSI_PERIOD - 1) * self._rsi_avg_gain + gain) / self.RSI_PERIOD
                self._rsi_avg_loss = ((self.RSI_PERIOD - 1) * self._rsi_avg_loss + loss) / self.RSI_PERIOD
            true_range = max(item.high - item.low, abs(item.high - self._previous_close), abs(item.low - self._previous_close))
        if self._atr is None:
            self._atr_seed.append(true_range)
            if len(self._atr_seed) == self.ATR_PERIOD:
                self._atr = mean(self._atr_seed)
                self._atr_seed.clear()
        else:
            self._atr = ((self.ATR_PERIOD - 1) * self._atr + true_range) / self.ATR_PERIOD
        self._previous_close = close

        self.bars.append(item)
        self._session_bar_count += 1
        self._update_opening_ranges()
        current_volumes = [b.volume for b in list(self.bars)[-self.VOLUME_PERIOD:]]
        current_volume_avg = None
        current_volume_bars = list(self.bars)[-self.VOLUME_PERIOD:]
        if len(current_volumes) == self.VOLUME_PERIOD and all(v is not None and b.volume_kind == "BAR" for v, b in zip(current_volumes, current_volume_bars)):
            current_volume_avg = mean(float(v) for v in current_volumes if v is not None)

        rsi = None
        if self._rsi_avg_gain is not None and self._rsi_avg_loss is not None:
            rsi = 100.0 if self._rsi_avg_loss == 0 else 100.0 - 100.0 / (1.0 + self._rsi_avg_gain / self._rsi_avg_loss)
        ready = (all(self._ema_values[p] is not None for p in self.EMA_PERIODS)
                 and rsi is not None and self._atr is not None and vwap is not None)
        return IndicatorSnapshot(
            symbol=item.symbol,
            timeframe=item.timeframe,
            timestamp=item.end,
            close=close,
            vwap=vwap,
            vwap_status="AVAILABLE" if vwap is not None else "VWAP_UNAVAILABLE",
            ema9=self._ema_values[9],
            ema20=self._ema_values[20],
            ema50=self._ema_values[50],
            rsi14=rsi,
            atr14=self._atr,
            volume_avg_20=current_volume_avg,
            prior_volume_avg_20=prior_volume_avg,
            opening_range_5m_high=self._opening_5[0] if self._opening_5 else None,
            opening_range_5m_low=self._opening_5[1] if self._opening_5 else None,
            opening_range_15m_high=self._opening_15[0] if self._opening_15 else None,
            opening_range_15m_low=self._opening_15[1] if self._opening_15 else None,
            bars_in_session=self._session_bar_count,
            indicator_ready=bool(ready),
        )

    def _opening_range(self, window_minutes: int) -> tuple[float, float] | None:
        bars_needed, bar_minutes = window_minutes // TIMEFRAME_MINUTES[self.timeframe], TIMEFRAME_MINUTES[self.timeframe]
        if window_minutes % bar_minutes:
            return None
        opened = datetime.combine(self.session_date, time(9, 15), tzinfo=IST)
        candidates = [b for b in self.bars if opened <= b.timestamp and b.end <= opened + timedelta(minutes=window_minutes)]
        if len(candidates) < bars_needed:
            return None
        first = candidates[:bars_needed]
        if any(b.timestamp != opened + timedelta(minutes=i * bar_minutes) for i, b in enumerate(first)):
            return None
        return max(b.high for b in first), min(b.low for b in first)

    def _update_opening_ranges(self) -> None:
        if self._opening_5 is None:
            self._opening_5 = self._opening_range(5)
        if self._opening_15 is None:
            self._opening_15 = self._opening_range(15)


def calculate_intraday_indicators(bars: Iterable[IntradayBar]) -> list[IndicatorSnapshot]:
    """Calculate snapshots chronologically for tests/research; no daily data accepted."""
    materialized = [validate_intraday_bar(bar, require_closed=True) for bar in bars]
    ordered = sorted(materialized, key=lambda b: (b.timestamp, b.symbol, b.timeframe))
    states: dict[tuple[str, str], IntradayIndicatorState] = {}
    snapshots: list[IndicatorSnapshot] = []
    for bar in ordered:
        state = states.setdefault((bar.symbol, bar.timeframe), IntradayIndicatorState(bar.timeframe))
        snapshots.append(state.update(bar))
    return snapshots
