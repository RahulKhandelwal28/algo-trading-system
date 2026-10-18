"""NSE session period helpers."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd


def latest_completed_trading_date(index: pd.Index | None = None, now: datetime | None = None) -> date:
    """Use observed provider bars and the local exchange close to exclude partial sessions."""
    local_now = now or datetime.now(ZoneInfo("Asia/Kolkata"))
    if local_now.tzinfo is None:
        local_now = local_now.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    else:
        local_now = local_now.astimezone(ZoneInfo("Asia/Kolkata"))
    cutoff = local_now.date() if local_now.time() >= time(15, 30) else local_now.date() - timedelta(days=1)
    if index is None or len(index) == 0:
        return cutoff
    observed = [pd.Timestamp(value).date() for value in index if pd.Timestamp(value).date() <= cutoff]
    if not observed:
        raise ValueError("no completed market-data session found")
    return max(observed)


def resolve_backtest_period(
    observed_index: pd.Index,
    lookback_months: int = 2,
    start: str | None = None,
    end: str | None = None,
    now: datetime | None = None,
) -> tuple[date, date]:
    completed = latest_completed_trading_date(observed_index, now)
    if not start and not end:
        local_now = now or datetime.now(ZoneInfo("Asia/Kolkata"))
        if local_now.tzinfo is None:
            local_now = local_now.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        else:
            local_now = local_now.astimezone(ZoneInfo("Asia/Kolkata"))
        latest_full_month_end = local_now.date().replace(day=1) - timedelta(days=1)
        available_month_end = min(completed, latest_full_month_end)
        period_end = pd.Timestamp(available_month_end)
        first_month = period_end.to_period("M").start_time - pd.DateOffset(months=lookback_months - 1)
        start_date = first_month.date()
        end_date = max(
            pd.Timestamp(value).date()
            for value in observed_index
            if pd.Timestamp(value).date() <= available_month_end
        )
    else:
        end_date = min(pd.Timestamp(end).date(), completed) if end else completed
        start_date = pd.Timestamp(start).date() if start else (pd.Timestamp(end_date) - pd.DateOffset(months=lookback_months)).date()
    if start_date > end_date:
        raise ValueError("start date must be on or before end date")
    return start_date, end_date


def warmup_start(start: date, calendar_days: int = 370) -> date:
    return start - timedelta(days=calendar_days)
