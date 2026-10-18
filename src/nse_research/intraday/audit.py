"""Structural classification helpers for repository intraday-data audits."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Iterable, Mapping


# Snapshot fact from the Phase 4B repository audit dated 2026-09-27. A later
# authorized file requires a fresh audit; this flag never infers availability.
INTRADAY_DATA_AVAILABLE = False


@dataclass(frozen=True, slots=True)
class ObservationAudit:
    classification: str
    row_count: int
    symbols: int
    timestamp_resolution: str
    timezone_awareness: str
    duplicate_symbol_timestamp: int
    multi_timestamp_symbol_sessions: int
    malformed_timestamps: int


def _parse_timestamp(value) -> tuple[datetime, bool]:
    if isinstance(value, datetime):
        stamp = value
        date_only = stamp.time() == time.min
    elif isinstance(value, date):
        stamp = datetime.combine(value, time.min)
        date_only = True
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError("empty timestamp")
        date_only = len(raw) == 10 and raw[4] == "-" and raw[7] == "-"
        stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return stamp, date_only


def classify_observations(
    records: Iterable[Mapping[str, object]],
    *,
    symbol_field: str = "symbol",
    timestamp_field: str = "timestamp",
) -> ObservationAudit:
    """Classify from actual within-session timestamp multiplicity, never names."""
    rows = list(records)
    if not rows:
        return ObservationAudit("UNKNOWN", 0, 0, "UNKNOWN", "UNKNOWN", 0, 0, 0)
    keys: set[tuple[str, datetime]] = set()
    sessions: dict[tuple[str, object], set[datetime]] = defaultdict(set)
    symbols: set[str] = set()
    stamps: list[datetime] = []
    date_only_all = True
    malformed = 0
    aware_count = 0
    naive_count = 0
    for row in rows:
        symbol = str(row.get(symbol_field) or "").strip().upper()
        if symbol:
            symbols.add(symbol)
        try:
            stamp, date_only = _parse_timestamp(row.get(timestamp_field))
        except (TypeError, ValueError, OverflowError):
            malformed += 1
            continue
        date_only_all &= date_only
        if stamp.tzinfo is not None and stamp.utcoffset() is not None:
            aware_count += 1
        else:
            naive_count += 1
        event_key = (symbol, stamp)
        keys.add(event_key)
        sessions[(symbol, stamp.date())].add(stamp)
        stamps.append(stamp)
    duplicates = len(rows) - malformed - len(keys)
    multi_sessions = sum(1 for values in sessions.values() if len(values) > 1)
    awareness = "AWARE" if aware_count and not naive_count else "NAIVE" if naive_count and not aware_count else "MIXED" if aware_count and naive_count else "UNKNOWN"
    intervals = []
    for values in sessions.values():
        ordered = sorted(values)
        intervals.extend((b - a).total_seconds() for a, b in zip(ordered, ordered[1:]) if b > a)
    if not intervals:
        resolution = "DATE_ONLY" if date_only_all else "ONE_OR_FEWER_TIMES_PER_SESSION"
    elif min(intervals) < 60:
        resolution = "SUB_MINUTE_OR_TICK"
    elif all(int(step) % 60 == 0 for step in intervals):
        resolution = f"MINUTE_MULTIPLE_{int(min(intervals) // 60)}M"
    else:
        resolution = "IRREGULAR_INTRADAY"
    if malformed:
        classification = "INVALID"
    elif multi_sessions:
        classification = "TICK_DATA" if min(intervals, default=60) < 60 else "GENUINE_INTRADAY"
    elif date_only_all or not multi_sessions:
        classification = "DAILY_ONLY"
    else:
        classification = "UNKNOWN"
    return ObservationAudit(classification, len(rows), len(symbols), resolution, awareness, duplicates, multi_sessions, malformed)
