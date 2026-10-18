"""Guarded conversion of a cumulative session-volume counter to deltas.

This module is provider-agnostic and does not parse or interpret provider
timestamps. Callers must pass a timezone-aware source timestamp that has
already passed the feed's timestamp validation, and a session date supplied
by the exchange calendar. A detected feed gap suppresses the unassignable
delta while rebasing the counter for subsequent observations.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class CumulativeVolumeUpdate:
    """One cumulative-to-incremental volume conversion result."""

    symbol: str
    session_date: date
    cumulative_volume: int
    incremental_volume: int | None
    source_timestamp: datetime
    quality_events: tuple[str, ...] = ()


@dataclass(frozen=True)
class _VolumeState:
    session_date: date
    cumulative_volume: int
    source_timestamp: datetime


class CumulativeVolumeDeltaTracker:
    """Convert ordered cumulative volume observations into safe increments.

    The first observation of a session establishes a baseline and returns no
    volume. A caller that detects a feed gap must set ``feed_gap=True``; the
    difference across that gap is then not assigned to any candle. Session
    dates must come from the exchange calendar, not from a guessed clock rule.
    """

    def __init__(self) -> None:
        self._states: dict[str, _VolumeState] = {}
        self._lock = threading.RLock()

    def update(
        self,
        symbol: str,
        cumulative_volume: int,
        source_timestamp: datetime,
        *,
        session_date: date,
        feed_gap: bool = False,
    ) -> CumulativeVolumeUpdate:
        """Return a nonnegative increment, or ``None`` when attribution is unsafe."""
        if not isinstance(symbol, str) or not symbol or symbol.strip() != symbol:
            raise ValueError("symbol must be a non-empty, trimmed string")
        if isinstance(cumulative_volume, bool) or not isinstance(cumulative_volume, int):
            raise ValueError("cumulative_volume must be a nonnegative integer")
        if cumulative_volume < 0:
            raise ValueError("cumulative_volume must be a nonnegative integer")
        if isinstance(session_date, datetime) or not isinstance(session_date, date):
            raise ValueError("session_date must be a date supplied by the exchange calendar")
        if not isinstance(source_timestamp, datetime) or source_timestamp.tzinfo is None:
            raise ValueError("source_timestamp must be timezone-aware")
        if source_timestamp.utcoffset() is None:
            raise ValueError("source_timestamp must be timezone-aware")
        if not isinstance(feed_gap, bool):
            raise ValueError("feed_gap must be a boolean")

        with self._lock:
            previous = self._states.get(symbol)
            events: list[str] = []

            if previous is not None and session_date < previous.session_date:
                events.append("OUT_OF_ORDER_SESSION_IGNORED")
                return self._result(symbol, session_date, cumulative_volume, None,
                                    source_timestamp, events)

            if previous is None or session_date > previous.session_date:
                self._states[symbol] = _VolumeState(
                    session_date, cumulative_volume, source_timestamp
                )
                events.append("BASELINE_ESTABLISHED" if previous is None else "SESSION_RESET_BASELINE")
                if feed_gap:
                    events.append("FEED_GAP_BASELINE_ONLY")
                return self._result(symbol, session_date, cumulative_volume, None,
                                    source_timestamp, events)

            if source_timestamp < previous.source_timestamp:
                events.append("OUT_OF_ORDER_VOLUME_OBSERVATION_IGNORED")
                return self._result(symbol, session_date, cumulative_volume, None,
                                    source_timestamp, events)

            self._states[symbol] = _VolumeState(
                session_date, cumulative_volume, source_timestamp
            )
            if feed_gap:
                events.append("FEED_GAP_VOLUME_UNATTRIBUTED")
                return self._result(symbol, session_date, cumulative_volume, None,
                                    source_timestamp, events)
            if cumulative_volume < previous.cumulative_volume:
                events.append("VOLUME_COUNTER_DECREASED")
                return self._result(symbol, session_date, cumulative_volume, None,
                                    source_timestamp, events)

            increment = cumulative_volume - previous.cumulative_volume
            return self._result(symbol, session_date, cumulative_volume, increment,
                                source_timestamp, events)

    @staticmethod
    def _result(
        symbol: str,
        session_date: date,
        cumulative_volume: int,
        incremental_volume: int | None,
        source_timestamp: datetime,
        events: list[str],
    ) -> CumulativeVolumeUpdate:
        return CumulativeVolumeUpdate(
            symbol=symbol,
            session_date=session_date,
            cumulative_volume=cumulative_volume,
            incremental_volume=incremental_volume,
            source_timestamp=source_timestamp,
            quality_events=tuple(events),
        )
