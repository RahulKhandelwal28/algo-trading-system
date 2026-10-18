"""Persist scanner decisions and simulated fills; never calls a broker."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.config import AppConfig
from nse_research.execution.broker import PaperBroker


def record_paper_candidates(database: str | Path, candidates: list[dict[str, Any]]) -> int:
    path = Path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    broker = PaperBroker()
    inserted = 0
    with sqlite3.connect(path) as connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS paper_signals (
                id INTEGER PRIMARY KEY, recorded_at TEXT NOT NULL, signal_time TEXT NOT NULL,
                symbol TEXT NOT NULL, strategy TEXT NOT NULL, direction TEXT NOT NULL,
                sector TEXT NOT NULL DEFAULT 'UNKNOWN',
                entry REAL NOT NULL, simulated_fill REAL NOT NULL, stop REAL NOT NULL,
                target REAL NOT NULL, quantity INTEGER NOT NULL, max_loss REAL NOT NULL,
                signal_score REAL NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL, entry_time TEXT,
                exit_time TEXT, exit_price REAL, pnl REAL, costs REAL, reason_exit TEXT,
                UNIQUE(signal_time, symbol, strategy, direction)
            )"""
        )
        existing = {row[1] for row in connection.execute("PRAGMA table_info(paper_signals)")}
        for name, kind in [("exit_time", "TEXT"), ("exit_price", "REAL"), ("pnl", "REAL"), ("costs", "REAL"), ("reason_exit", "TEXT"), ("entry_time", "TEXT"), ("sector", "TEXT NOT NULL DEFAULT 'UNKNOWN'")]:
            if name not in existing:
                connection.execute(f"ALTER TABLE paper_signals ADD COLUMN {name} {kind}")
        for candidate in candidates:
            fill = broker.place(candidate)
            cursor = connection.execute(
                """INSERT OR IGNORE INTO paper_signals
                (recorded_at, signal_time, symbol, strategy, direction, sector, entry, simulated_fill,
                 stop, target, quantity, max_loss, signal_score, reason, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now().isoformat(timespec="seconds"),
                    str(candidate["signal_time"]), candidate["symbol"], candidate["strategy"],
                    candidate["direction"], candidate.get("sector", "UNKNOWN"), candidate["entry_estimate"], fill.price,
                    candidate["stop_loss"], candidate["target"], candidate["quantity"],
                    candidate["maximum_loss"], candidate["signal_score"], candidate["reason"], "PENDING",
                ),
            )
            inserted += max(0, cursor.rowcount)
    return inserted


def active_paper_positions(database: str | Path) -> list[dict[str, Any]]:
    """Return reserved pending/open paper positions for portfolio admission."""
    path = Path(database)
    if not path.exists():
        return []
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT symbol, sector, direction, entry, quantity, status FROM paper_signals WHERE status IN ('PENDING', 'OPEN')"
        ).fetchall()
    return [dict(row) for row in rows]


def paper_realized_pnl(
    database: str | Path,
    session_date: str,
    bars_by_symbol: dict[str, pd.DataFrame] | None = None,
) -> float:
    """Return current-session closed and marked-open P&L for the daily loss gate."""
    path = Path(database)
    if not path.exists():
        return 0.0
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM paper_signals WHERE status='CLOSED' AND substr(exit_time, 1, 10)=?",
            (session_date,),
        ).fetchone()
        active = connection.execute(
            "SELECT symbol, direction, quantity, simulated_fill FROM paper_signals WHERE status='OPEN'"
        ).fetchall()
    total = float(row[0] or 0.0)
    if bars_by_symbol is None:
        return total
    for symbol, direction_text, quantity, fill in active:
        bars = bars_by_symbol.get(symbol)
        if bars is None or bars.empty:
            continue
        session_bars = bars.loc[[pd.Timestamp(stamp).date().isoformat() == session_date for stamp in bars.index]]
        if session_bars.empty:
            continue
        previous = bars.loc[[pd.Timestamp(stamp).date().isoformat() < session_date for stamp in bars.index]]
        prior_close = float(previous.close.iloc[-1]) if not previous.empty else float(fill)
        direction = 1 if direction_text == "LONG" else -1
        total += direction * int(quantity) * (float(session_bars.close.iloc[-1]) - prior_close)
    return total


def paper_portfolio_drawdown(
    database: str | Path,
    initial_capital: float,
    bars_by_symbol: dict[str, pd.DataFrame],
    config: AppConfig,
) -> float:
    """Estimate paper equity drawdown from closed trades and marked open positions."""
    path = Path(database)
    if not path.exists():
        return 0.0
    with sqlite3.connect(path) as connection:
        closed = pd.read_sql_query(
            "SELECT exit_time, pnl FROM paper_signals WHERE status='CLOSED' AND exit_time IS NOT NULL ORDER BY exit_time, id",
            connection,
        )
        active = connection.execute(
            "SELECT symbol, direction, strategy, quantity, simulated_fill, status FROM paper_signals WHERE status IN ('PENDING', 'OPEN')"
        ).fetchall()
    equity = float(initial_capital)
    peak = equity
    maximum_drawdown = 0.0
    if not closed.empty:
        closed["day"] = pd.to_datetime(closed["exit_time"], errors="coerce").dt.date
        daily = pd.to_numeric(closed["pnl"], errors="coerce").fillna(0).groupby(closed["day"]).sum()
        for daily_pnl in daily:
            equity += float(daily_pnl)
            peak = max(peak, equity)
            if peak > 0:
                maximum_drawdown = max(maximum_drawdown, 1 - equity / peak)
    costs = CostModel(config.costs)
    marked_equity = equity
    for symbol, direction_text, strategy, quantity, fill, status in active:
        if status != "OPEN":
            continue
        bars = bars_by_symbol.get(symbol)
        if bars is None or bars.empty or pd.isna(fill):
            continue
        direction = 1 if direction_text == "LONG" else -1
        mark = float(bars.close.iloc[-1])
        exit_fill = costs.slippage_price(mark, direction, False)
        net_open_pnl = direction * int(quantity) * (exit_fill - float(fill))
        total_cost, _, _ = costs.round_trip(
            float(fill), exit_fill, int(quantity), direction, strategy == "vwap_intraday"
        )
        marked_equity += net_open_pnl - total_cost
    peak = max(peak, marked_equity)
    if peak > 0:
        maximum_drawdown = max(maximum_drawdown, 1 - marked_equity / peak)
    return float(maximum_drawdown)


def update_paper_positions(
    database: str | Path,
    bars_by_symbol: dict[str, pd.DataFrame],
    config: AppConfig,
) -> int:
    """Advance recorded simulated positions using later real bars, with stop-first exits."""
    path = Path(database)
    if not path.exists():
        return 0
    costs = CostModel(config.costs)
    exits = 0
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        positions = connection.execute("SELECT * FROM paper_signals WHERE status IN ('PENDING', 'OPEN')").fetchall()
        for position in positions:
            bars = bars_by_symbol.get(position["symbol"])
            if bars is None or bars.empty:
                continue
            pending = position["status"] == "PENDING"
            last_known_time = pd.Timestamp(position["signal_time"] if pending else position["entry_time"])
            future = bars.loc[bars.index > last_known_time]
            direction = 1 if position["direction"] == "LONG" else -1
            entry_fill = float(position["simulated_fill"])
            exit_price = None
            reason = None
            exit_time = None
            for stamp, bar in future.iterrows():
                opening, high, low = float(bar.open), float(bar.high), float(bar.low)
                stop, target = float(position["stop"]), float(position["target"])
                if pending:
                    # A close-of-bar signal can only fill at the next bar's open.
                    if (direction > 0 and (opening <= stop or opening >= target)) or (direction < 0 and (opening >= stop or opening <= target)):
                        connection.execute(
                            "UPDATE paper_signals SET status='CANCELLED', reason_exit=? WHERE id=?",
                            ("next-bar gap invalidated entry", position["id"]),
                        )
                        exit_price = None
                        reason = "cancelled"
                        break
                    fill = costs.slippage_price(opening, direction, is_entry=True)
                    intraday = position["strategy"] == "vwap_intraday"
                    risk_exit = costs.slippage_price(stop, direction, is_entry=False)
                    estimated_cost, _, _ = costs.round_trip(fill, risk_exit, int(position["quantity"]), direction, intraday)
                    maximum_loss = abs(fill - risk_exit) * int(position["quantity"]) + estimated_cost
                    if maximum_loss > float(position["max_loss"]) + 1.0:
                        connection.execute(
                            "UPDATE paper_signals SET status='CANCELLED', reason_exit=? WHERE id=?",
                            ("next-bar gap exceeds configured risk budget", position["id"]),
                        )
                        reason = "cancelled"
                        break
                    connection.execute(
                        "UPDATE paper_signals SET status='OPEN', entry=?, simulated_fill=?, entry_time=? WHERE id=?",
                        (opening, fill, pd.Timestamp(stamp).isoformat(), position["id"]),
                    )
                    entry_fill = fill
                    pending = False
                if direction > 0 and opening <= stop:
                    exit_price, reason = opening, "gap_stop"
                elif direction > 0 and opening >= target:
                    exit_price, reason = target, "target"
                elif direction < 0 and opening >= stop:
                    exit_price, reason = opening, "gap_stop"
                elif direction < 0 and opening <= target:
                    exit_price, reason = target, "target"
                elif direction > 0 and low <= stop:
                    exit_price, reason = stop, "stop"
                elif direction > 0 and high >= target:
                    exit_price, reason = target, "target"
                elif direction < 0 and high >= stop:
                    exit_price, reason = stop, "stop"
                elif direction < 0 and low <= target:
                    exit_price, reason = target, "target"
                if exit_price is not None:
                    exit_time = pd.Timestamp(stamp).isoformat()
                    break
            if exit_price is None:
                continue
            if reason == "cancelled":
                continue
            slipped = costs.slippage_price(exit_price, direction, is_entry=False)
            gross = direction * int(position["quantity"]) * (slipped - entry_fill)
            intraday = position["strategy"] == "vwap_intraday"
            total_cost, _, _ = costs.round_trip(
                entry_fill, slipped, int(position["quantity"]), direction, intraday
            )
            connection.execute(
                "UPDATE paper_signals SET status='CLOSED', exit_time=?, exit_price=?, pnl=?, costs=?, reason_exit=? WHERE id=?",
                (exit_time, slipped, gross - total_cost, total_cost, reason, position["id"]),
            )
            exits += 1
    return exits
