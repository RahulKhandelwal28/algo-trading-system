"""Offline NSE equity session calendar with fail-closed unknown weekdays."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class SessionDecision:
    status: str
    reason: str


class NseTradingCalendar:
    """Classify observed sessions and explicit exchange holidays locally.

    Market data observations take precedence, so special weekend sessions are
    accepted when the approved provider actually contains that date. Unknown
    weekdays with no data remain DATA_UNAVAILABLE, never guessed holidays.
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self.holidays: dict[date, str] = {}
        self.special_sessions: dict[date, str] = {}
        self.trading_days: dict[date, str] = {}
        if path is None or not Path(path).is_file():
            return
        with Path(path).open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            required = {"Date", "Status"}
            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                raise ValueError("NSE trading calendar requires Date and Status columns")
            for row in reader:
                day = date.fromisoformat(str(row["Date"]).strip())
                status = str(row["Status"]).strip().upper()
                reason = str(row.get("Reason") or "official NSE calendar").strip()
                if status == "HOLIDAY":
                    self.holidays[day] = reason
                elif status == "SPECIAL_SESSION":
                    self.special_sessions[day] = reason
                elif status == "TRADING_DAY":
                    self.trading_days[day] = reason
                else:
                    raise ValueError(f"unsupported NSE calendar status {status!r} for {day}")
            conflicting = set(self.trading_days) & (set(self.holidays) | set(self.special_sessions))
            if conflicting:
                raise ValueError(f"conflicting NSE trading calendar entries for {sorted(conflicting)}")

    def classify(self, market_date: date, *, observed_market_data: bool) -> SessionDecision:
        if observed_market_data:
            return SessionDecision("TRADING_DAY", "approved local market data contains the requested date")
        if market_date in self.special_sessions:
            return SessionDecision("DATA_UNAVAILABLE", f"special session expected; no approved data: {self.special_sessions[market_date]}")
        if market_date in self.holidays:
            return SessionDecision("NON_TRADING_DAY", self.holidays[market_date])
        if market_date in self.trading_days:
            return SessionDecision("TRADING_DAY", self.trading_days[market_date])
        if market_date.weekday() >= 5:
            return SessionDecision("NON_TRADING_DAY", "weekend; no market data observation")
        return SessionDecision("DATA_UNAVAILABLE", "no approved market data and no official holiday entry for this weekday")

    def intraday_status(
        self,
        now: datetime,
        *,
        timezone_name: str = "Asia/Kolkata",
        preopen_start: time = time(9, 0),
        market_open: time = time(9, 15),
        market_close: time = time(15, 30),
        observed_market_data: bool = False,
    ) -> SessionDecision:
        """Classify a local intraday clock using this same holiday calendar.

        The calendar stores special-session dates and reasons, but not their
        intraday hours. Those dates are therefore identified and fail closed
        until an explicit schedule is represented.
        """
        zone = ZoneInfo(timezone_name)
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("intraday clock must include a timezone")
        local_now = now.astimezone(zone)
        market_date = local_now.date()
        if market_date in self.holidays:
            return SessionDecision("CLOSED", self.holidays[market_date])
        if market_date in self.special_sessions:
            return SessionDecision(
                "SPECIAL_SESSION",
                "special session is listed but its intraday hours are not represented in the local calendar",
            )
        if market_date.weekday() >= 5:
            return SessionDecision("CLOSED", "weekend")
        local_time = local_now.timetz().replace(tzinfo=None)
        if local_time < preopen_start:
            return SessionDecision("CLOSED", "before NSE pre-open")
        if local_time < market_open:
            return SessionDecision("PREOPEN", "NSE pre-open; regular-session candles are not emitted")
        if local_time < market_close:
            day = self.classify(market_date, observed_market_data=observed_market_data)
            if day.status != "TRADING_DAY":
                return SessionDecision("DATA_UNAVAILABLE", day.reason)
            return SessionDecision("OPEN", "regular NSE cash session with trading-session evidence")
        return SessionDecision("CLOSED", "regular NSE cash session has ended")

    def latest_completed_candidate(self, now: datetime, timezone_name: str = "Asia/Kolkata") -> date:
        local_now = now.astimezone(ZoneInfo(timezone_name)) if now.tzinfo else now.replace(tzinfo=ZoneInfo(timezone_name))
        if local_now.time() >= time(15, 45):
            # Let the session classifier journal today's holiday/weekend status
            # and recognize a special session if the provider has its data.
            return local_now.date()
        candidate = local_now.date() - timedelta(days=1)
        while candidate not in self.special_sessions and (
            candidate.weekday() >= 5 or candidate in self.holidays
        ):
            candidate -= timedelta(days=1)
        return candidate
