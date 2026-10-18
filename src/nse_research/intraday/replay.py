"""Chronological bar replay with event-time progress and configurable pacing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from time import sleep
from typing import Callable, Sequence

from .model import IntradayBar, IntradayDataProvider
from .scanner import IntradayScanner
from .strategies import IntradaySignal


REPLAY_SPEEDS = {"1x": 1.0, "5x": 5.0, "max": None}


@dataclass(frozen=True, slots=True)
class ReplayResult:
    bars_processed: int
    signals: tuple[IntradaySignal, ...]
    first_event_time: datetime | None
    last_event_time: datetime | None


class ReplayOrderError(ValueError):
    pass


class IntradayReplay:
    """Stream bars through scanner state in event-time order; forming bars do not signal."""

    def __init__(self, scanner: IntradayScanner, *, sleep_fn: Callable[[float], None] = sleep) -> None:
        self.scanner = scanner
        self.sleep_fn = sleep_fn

    def run(self, provider: IntradayDataProvider, *, speed: str = "1x", start=None, end=None) -> ReplayResult:
        if speed not in REPLAY_SPEEDS:
            raise ValueError("replay speed must be 1x, 5x, or max")
        factor = REPLAY_SPEEDS[speed]
        bars_processed = 0
        first_event: datetime | None = None
        previous_key = None
        previous_event: datetime | None = None
        for bar in provider.iter_bars(self.scanner.symbols, self.scanner.config.timeframes, start=start, end=end):
            if bar.status != "CLOSED":
                continue
            event_time = bar.end
            key = (event_time, bar.symbol, bar.timeframe, bar.timestamp)
            if previous_key is not None and key < previous_key:
                raise ReplayOrderError("provider bars must be yielded in chronological event-time order")
            if factor is not None and previous_event is not None:
                delay = (event_time - previous_event).total_seconds() / factor
                if delay > 0:
                    self.sleep_fn(delay)
            if first_event is None:
                first_event = event_time
            self.scanner.handle_bar(bar)
            bars_processed += 1
            previous_key = key
            previous_event = event_time
        return ReplayResult(bars_processed, tuple(self.scanner.signals), first_event, previous_event)
