"""Atomic daily, cumulative, monthly and delayed comparison reports."""

from __future__ import annotations

import csv
import io
import json
import math
import sqlite3
import tempfile
import os
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from nse_research.forward.baseline import PHASE3C_DAILY_STRATEGIES


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _write_json(path: Path, payload: Any) -> None:
    _atomic_write(path, json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False, default=str) + "\n")


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    if columns is None:
        columns = list(rows[0]) if rows else []
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: json.dumps(value, sort_keys=True, default=str) if isinstance(value, (dict, list)) else value
                         for key, value in row.items()})
    _atomic_write(path, buffer.getvalue())


def _rows(connection: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    return [dict(row) for row in connection.execute(query, params).fetchall()]


def write_daily_artifacts(
    database: str | Path,
    report: dict[str, Any],
    output_root: str | Path,
    *,
    baseline_manifest: dict[str, Any],
    render_markdown,
) -> Path:
    """Write a date-scoped immutable artifact set from the reconciled ledger."""
    market_date = str(report["date"])
    target = Path(output_root) / market_date
    target.mkdir(parents=True, exist_ok=True)
    db_path = Path(database)
    if db_path.is_file():
        with sqlite3.connect(db_path) as connection:
            signals = _rows(connection, "SELECT * FROM signals WHERE trading_date=? ORDER BY timestamp,symbol,strategy", (market_date,))
            orders = _rows(connection, "SELECT * FROM orders WHERE trading_date=? ORDER BY created_at,order_id", (market_date,))
            trades = _rows(connection, "SELECT * FROM positions WHERE entry_date=? OR exit_date=? ORDER BY entry_date,position_id", (market_date, market_date))
            positions = _rows(connection, "SELECT * FROM positions WHERE status='OPEN' OR exit_date=? ORDER BY symbol,position_id", (market_date,))
            snapshots = _rows(connection, "SELECT * FROM portfolio_snapshots WHERE trading_date=? ORDER BY snapshot_id DESC LIMIT 1", (market_date,))
    else:
        signals, orders, trades, positions, snapshots = [], [], [], [], []

    _write_json(target / "daily_report.json", report)
    _atomic_write(target / "daily_report.md", render_markdown(report))
    _write_csv(target / "signals.csv", signals)
    _write_csv(target / "orders.csv", orders)
    _write_csv(target / "trades.csv", trades)
    _write_csv(target / "positions.csv", positions)
    _write_csv(target / "portfolio.csv", snapshots)
    risk_rows = []
    for event in report.get("risk_events", []):
        risk_rows.append({"event_type": event.get("event_type", "SIGNAL_REJECTED"), **event})
    _write_csv(
        target / "risk_events.csv",
        risk_rows,
        ["event_type", "symbol", "strategy", "reason", "risk_check", "portfolio_check",
         "capital_check", "position_limit", "sector_limit", "daily_loss_limit", "drawdown_limit"],
    )
    _write_json(target / "data_quality.json", report.get("data", {}))
    _write_json(target / "run_manifest.json", {
        "run_id": report.get("run_id"),
        "baseline_name": baseline_manifest.get("baseline_name"),
        "baseline_sha256": baseline_manifest.get("baseline_sha256"),
        "provider": report.get("data", {}).get("provider"),
        "dataset": report.get("data", {}).get("dataset"),
        "data_timestamps": report.get("data", {}).get("data_timestamps", {}),
        "market_date": market_date,
        "symbols_requested": report.get("data", {}).get("symbols_requested", 0),
        "symbols_received": report.get("data", {}).get("symbols_received", 0),
        "symbols_missing": report.get("data", {}).get("symbols_missing", []),
        "universe": report.get("universe", {}),
        "session_status": report.get("session_status", "TRADING_DAY"),
        "data_status": report.get("data", {}).get("system_data_status"),
        "reconciliation": report.get("reconciliation", {}),
        "live_trading_enabled": False,
        "broker_contacted": False,
    })
    return target


def _strategy_metrics(connection: sqlite3.Connection, starting_capital: float) -> list[dict[str, Any]]:
    signals = _rows(connection, "SELECT strategy,accepted,trading_date FROM signals WHERE strategy!='DATA_HEALTH'")
    positions = _rows(connection, "SELECT * FROM positions ORDER BY exit_date,entry_date,position_id")
    order_costs = {
        str(row[0]): float(row[1] or 0)
        for row in connection.execute("SELECT strategy,COALESCE(SUM(costs),0) FROM orders GROUP BY strategy")
    }
    observed_dates = {str(row[0]) for row in connection.execute("SELECT trading_date FROM portfolio_snapshots")}
    observed_months = {day[:7] for day in observed_dates}

    def new_account(name: str) -> dict[str, Any]:
        return {
            "strategy": name, "starting_capital": float(starting_capital),
            "signals": 0, "accepted_signals": 0, "rejected_signals": 0,
            "open_positions": 0, "closed_trades": 0, "gross_pnl": 0.0,
            "costs": 0.0, "net_pnl": 0.0, "realized_net_pnl": 0.0,
            "unrealized_pnl": 0.0, "equity": float(starting_capital), "cash": float(starting_capital),
            "gross_exposure": 0.0, "net_exposure": 0.0,
            "_pnls": [], "_open_basis": 0.0, "_open_gross": 0.0,
            "_sessions": set(observed_dates), "_months": set(observed_months),
        }

    grouped: dict[str, dict[str, Any]] = {name: new_account(name) for name in PHASE3C_DAILY_STRATEGIES}
    for signal in signals:
        item = grouped.setdefault(signal["strategy"], new_account(signal["strategy"]))
        item["signals"] += 1
        item["accepted_signals"] += int(bool(signal["accepted"]))
        item["rejected_signals"] += int(not signal["accepted"])
    for position in positions:
        item = grouped.setdefault(position["strategy"], new_account(position["strategy"]))
        direction = int(position["direction"])
        quantity = int(position["quantity"])
        if position["status"] == "OPEN":
            item["open_positions"] += 1
            item["unrealized_pnl"] += float(position["unrealized_pnl"] or 0.0)
            item["_open_gross"] += direction * quantity * (float(position["mark_price"]) - float(position["entry_price"]))
            item["_open_basis"] += direction * quantity * float(position["entry_price"]) + float(position["entry_costs"])
            mark_value = abs(float(position["mark_price"]) * quantity)
            item["gross_exposure"] += mark_value
            item["net_exposure"] += direction * mark_value
            continue
        if position["status"] != "CLOSED":
            continue
        item["closed_trades"] += 1
        net = float(position["realized_pnl"] or 0.0)
        gross = direction * quantity * (float(position["exit_price"]) - float(position["entry_price"]))
        item["gross_pnl"] += gross
        item["realized_net_pnl"] += net
        item["_pnls"].append(net)
    result = []
    for item in grouped.values():
        pnls = item.pop("_pnls")
        item["gross_pnl"] += item.pop("_open_gross")
        item["cash"] = float(starting_capital) + item["realized_net_pnl"] - item.pop("_open_basis")
        item["net_pnl"] = item["realized_net_pnl"] + item["unrealized_pnl"]
        item["equity"] = float(starting_capital) + item["net_pnl"]
        item["costs"] = order_costs.get(item["strategy"], 0.0)
        item["trades_observed"] = item["closed_trades"]
        item["trading_days_observed"] = len(item.pop("_sessions"))
        item["months_observed"] = len(item.pop("_months"))
        wins = [value for value in pnls if value > 0]
        losses = [value for value in pnls if value < 0]
        item["win_rate"] = len(wins) / len(pnls) if pnls else None
        item["profit_factor"] = sum(wins) / abs(sum(losses)) if losses and sum(losses) else None
        item["expectancy"] = float(np.mean(pnls)) if pnls else 0.0
        item["return"] = item["net_pnl"] / starting_capital
        equity = peak = float(starting_capital)
        max_drawdown = 0.0
        for value in pnls:
            equity += value
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, (peak - equity) / peak if peak else 0.0)
        current_drawdown = max(0.0, (peak - item["equity"]) / peak) if peak else 0.0
        max_drawdown = max(max_drawdown, current_drawdown)
        item["max_drawdown"] = max_drawdown
        if item["trades_observed"] >= 30 and item["months_observed"] >= 3:
            item["sample_status"] = "MEANINGFUL_FORWARD_SAMPLE"
        elif item["trades_observed"] or item["trading_days_observed"]:
            item["sample_status"] = "EARLY_FORWARD_SAMPLE"
        else:
            item["sample_status"] = "INSUFFICIENT_FORWARD_SAMPLE"
        result.append(item)
    return result


def _portfolio_summary(connection: sqlite3.Connection) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    snapshots = _rows(connection, "SELECT * FROM portfolio_snapshots ORDER BY trading_date,snapshot_id")
    closed = _rows(connection, "SELECT * FROM positions WHERE status='CLOSED' ORDER BY exit_date,position_id")
    return snapshots, closed


def update_cumulative_research(
    database: str | Path,
    output_root: str | Path,
    research_doc: str | Path,
    *,
    phase3c_results: str | Path,
) -> None:
    """Update cumulative research and current/finalized month snapshots."""
    db_path = Path(database)
    if not db_path.is_file():
        return
    with sqlite3.connect(db_path) as connection:
        journals = _rows(connection, "SELECT trading_date,payload_json FROM daily_journal ORDER BY trading_date")
        snapshots, closed = _portfolio_summary(connection)
        capital_row = connection.execute("SELECT value FROM settings WHERE key='starting_capital'").fetchone()
        capital = float(capital_row[0]) if capital_row else 1.0
        strategies = _strategy_metrics(connection, capital)
        signal_count = int(connection.execute("SELECT COUNT(*) FROM signals WHERE strategy!='DATA_HEALTH'").fetchone()[0])
        signal_risk_events = int(connection.execute("SELECT COUNT(*) FROM signals WHERE status='REJECTED_RISK'").fetchone()[0])
        order_risk_events = int(connection.execute("SELECT COUNT(*) FROM orders WHERE status='REJECTED'").fetchone()[0])
        risk_events = signal_risk_events + order_risk_events
        data_events = int(connection.execute("SELECT COUNT(*) FROM health_snapshots WHERE json_extract(payload_json,'$.system_data_status')!='FRESH'").fetchone()[0])
        order_costs = float(connection.execute("SELECT COALESCE(SUM(costs),0) FROM orders").fetchone()[0])

    sessions = [str(row["trading_date"]) for row in journals]
    trades = [position for position in closed if position["status"] == "CLOSED"]
    pnls = [float(trade["realized_pnl"] or 0.0) for trade in trades]
    wins = [value for value in pnls if value > 0]
    losses = [value for value in pnls if value < 0]
    overall = {
        "trading_sessions": len(sessions),
        "signals": signal_count,
        "trades": len(trades),
        "win_rate": len(wins) / len(pnls) if pnls else None,
        "profit_factor": sum(wins) / abs(sum(losses)) if losses and sum(losses) else None,
        "expectancy": float(np.mean(pnls)) if pnls else 0.0,
        "net_pnl": float(sum(pnls)),
        "costs": order_costs,
        "average_holding_sessions": float(np.mean([int(t["holding_sessions"] or 0) for t in trades])) if trades else 0.0,
        "risk_events": risk_events,
        "data_quality_events": data_events,
        "sample_status": "MEANINGFUL_FORWARD_SAMPLE" if len(trades) >= 30 and len({s[:7] for s in sessions}) >= 3
            else ("EARLY_FORWARD_SAMPLE" if trades or sessions else "INSUFFICIENT_FORWARD_SAMPLE"),
    }
    final_equity = float(snapshots[-1]["equity"]) if snapshots else capital
    max_drawdown = max((float(row["drawdown"]) for row in snapshots), default=0.0)
    monthly: dict[str, dict[str, float]] = defaultdict(lambda: {"pnl": 0.0, "trades": 0.0, "sessions": 0.0})
    for snapshot in snapshots:
        month = str(snapshot["trading_date"])[:7]
        monthly[month]["pnl"] += float(snapshot["daily_pnl"] or 0.0)
        monthly[month]["sessions"] += 1
    for trade in trades:
        monthly[str(trade["exit_date"])[:7]]["trades"] += 1

    ordered_pnls = pnls
    longest_win = longest_loss = current_win = current_loss = 0
    for value in ordered_pnls:
        current_win = current_win + 1 if value > 0 else 0
        current_loss = current_loss + 1 if value < 0 else 0
        longest_win = max(longest_win, current_win)
        longest_loss = max(longest_loss, current_loss)
    rolling = []
    for index in range(max(0, len(snapshots) - 19), len(snapshots)):
        chunk = snapshots[max(0, index - 19):index + 1]
        rolling.append({"end_date": snapshots[index]["trading_date"], "sessions": len(chunk),
                        "net_pnl": sum(float(row["daily_pnl"]) for row in chunk)})

    lines = [
        "# Phase 3D Forward Paper Research", "",
        "Cumulative, forward-only evidence from the frozen PHASE3C_BASELINE. It is kept separate from historical backtests.", "",
        f"Experiment start date: {sessions[0] if sessions else 'not started'}",
        f"Latest market date: {sessions[-1] if sessions else 'not started'}",
        f"Trading sessions: {overall['trading_sessions']}", f"Signals: {overall['signals']}",
        f"Closed trades: {overall['trades']}", f"Forward sample status: {overall['sample_status']}", "",
        "## Portfolio", "",
        f"Starting capital: {capital:.2f}", f"Latest equity: {final_equity:.2f}",
        f"Net realized P&L: {overall['net_pnl']:.2f}", f"Recorded transaction costs: {overall['costs']:.2f}",
        f"Maximum observed drawdown: {max_drawdown:.4%}",
        f"Win rate: {overall['win_rate']:.4%}" if overall["win_rate"] is not None else "Win rate: unavailable",
        f"Profit factor: {overall['profit_factor']:.4f}" if overall["profit_factor"] is not None else "Profit factor: unavailable",
        f"Expectancy per closed trade: {overall['expectancy']:.2f}",
        f"Average holding period: {overall['average_holding_sessions']:.2f} sessions",
        f"Longest win/loss streaks: {longest_win}/{longest_loss}",
        f"Risk events: {risk_events}", f"Data-quality events: {data_events}", "",
        "## Strategy accounts", "",
        "Each strategy row is an isolated descriptive account view using the configured starting capital as its own denominator; values are not summed into the portfolio account.", "",
        "| Strategy | Capital | Equity | Cash | Signals | Trades | Win rate | Gross P&L | Costs | Net P&L | Return | PF | Expectancy | Max DD | Open | Sample |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in strategies:
        pct = "—" if row["win_rate"] is None else f"{row['win_rate']:.1%}"
        pf = "—" if row["profit_factor"] is None else f"{row['profit_factor']:.2f}"
        lines.append(f"| {row['strategy']} | {row['starting_capital']:.2f} | {row['equity']:.2f} | {row['cash']:.2f} | {row['signals']} | {row['closed_trades']} | {pct} | {row['gross_pnl']:.2f} | {row['costs']:.2f} | {row['net_pnl']:.2f} | {row['return']:.2%} | {pf} | {row['expectancy']:.2f} | {row['max_drawdown']:.2%} | {row['open_positions']} | {row['sample_status']} |")
    lines += ["", "## Monthly performance", "", "| Month | Sessions | Trades closed | Net P&L |", "|---|---:|---:|---:|"]
    for month, values in sorted(monthly.items()):
        lines.append(f"| {month} | {int(values['sessions'])} | {int(values['trades'])} | {values['pnl']:.2f} |")
    lines += ["", "## Rolling 20-session performance", "", "| Through date | Sessions | Net P&L |", "|---|---:|---:|"]
    for row in rolling:
        lines.append(f"| {row['end_date']} | {row['sessions']} | {row['net_pnl']:.2f} |")
    lines += [
        "", "## Evidence boundary", "",
        "These figures are deterministic paper results under local EOD data and daily-bar assumptions. Small samples are not statistically meaningful. No broker orders were sent. Historical Phase 3C observations are not included in forward totals.",
        "",
    ]
    _atomic_write(Path(research_doc), "\n".join(lines))

    root = Path(output_root)
    latest_month = sessions[-1][:7] if sessions else None
    for month, values in sorted(monthly.items()):
        month_dir = root / "monthly" / month
        if month_dir.exists() and latest_month and month < latest_month:
            continue
        month_dir.mkdir(parents=True, exist_ok=True)
        month_snapshots = [row for row in snapshots if str(row["trading_date"])[:7] == month]
        month_trades = [row for row in trades if str(row["exit_date"])[:7] == month]
        prior_months = [key for key in monthly if key < month]
        previous_pnl = monthly[max(prior_months)]["pnl"] if prior_months else None
        comparison = "no previous month" if previous_pnl is None else f"previous month net P&L {previous_pnl:.2f}; change {values['pnl'] - previous_pnl:.2f}"
        monthly_md = (
            f"# Phase 3D Monthly Forward Paper Report — {month}\n\n"
            f"Sessions: {int(values['sessions'])}\n\nClosed trades: {int(values['trades'])}\n\n"
            f"Net P&L: {values['pnl']:.2f}\n\nComparison: {comparison}\n\n"
            "Forward paper evidence only; it does not include historical backtests.\n"
        )
        _atomic_write(month_dir / "monthly_report.md", monthly_md)
        _write_csv(month_dir / "monthly_metrics.csv", [{"month": month, **values, "previous_month_pnl": previous_pnl}])
        _write_csv(month_dir / "trade_log.csv", month_trades)
        _write_csv(month_dir / "equity_curve.csv", month_snapshots)

    if len(trades) >= 30 and len({session[:7] for session in sessions}) >= 3:
        _write_forward_comparison(Path(output_root), phase3c_results, trades, snapshots, strategies, capital)


def _write_forward_comparison(
    output_root: Path,
    phase3c_results: str | Path,
    trades: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    strategies: list[dict[str, Any]],
    capital: float,
) -> None:
    test_path = Path(phase3c_results) / "test_results.csv"
    if not test_path.is_file():
        return
    frame = pd.read_csv(test_path)
    if "is_primary_test" in frame:
        frame = frame[frame["is_primary_test"].astype(str).str.lower().isin({"true", "1"})]
    if "segment" in frame:
        frame = frame[frame["segment"].astype(str).str.lower() == "test"]
    if frame.empty:
        return
    phase3c_trades = int(pd.to_numeric(frame["total_trades"], errors="coerce").fillna(0).sum())
    phase3c_gross_profit = float(pd.to_numeric(frame["gross_profit"], errors="coerce").fillna(0).sum())
    phase3c_gross_loss = float(pd.to_numeric(frame["gross_loss"], errors="coerce").fillna(0).sum())
    phase3c_days = max(1, (pd.to_datetime(frame["date_range_end"]).max() - pd.to_datetime(frame["date_range_start"]).min()).days + 1)
    forward_pnls = [float(row["realized_pnl"] or 0) for row in trades]
    forward_wins = [value for value in forward_pnls if value > 0]
    forward_losses = [value for value in forward_pnls if value < 0]
    phase3c_avg_holding = float(pd.to_numeric(frame["average_holding_period_sessions"], errors="coerce").dropna().mean()) if "average_holding_period_sessions" in frame else math.nan
    forward_holding = float(np.mean([int(row["holding_sessions"] or 0) for row in trades])) if trades else 0.0
    phase3c_net = float(pd.to_numeric(frame["net_pnl"], errors="coerce").fillna(0).sum())
    phase3c_costs = float(pd.to_numeric(frame["total_transaction_costs"], errors="coerce").fillna(0).sum())
    phase3c_dd = float(pd.to_numeric(frame["maximum_drawdown_pct"], errors="coerce").max())
    forward_dd = max((float(row["drawdown"]) for row in snapshots), default=0.0) * 100
    first, last = snapshots[0]["trading_date"], snapshots[-1]["trading_date"]
    forward_days = max(1, (date.fromisoformat(last) - date.fromisoformat(first)).days + 1)
    content = [
        "# Phase 3C Historical OOS vs Phase 3D Forward Paper",
        "",
        "This comparison is generated only after at least 30 closed forward trades across at least three observed calendar months. It is retrospective and descriptive, not a daily prediction or a statistical approval.",
        "",
        "| Metric | Phase 3C primary OOS test | Phase 3D forward paper |",
        "|---|---:|---:|",
        f"| Trades | {phase3c_trades} | {len(trades)} |",
        f"| Trade frequency (per calendar day) | {phase3c_trades / phase3c_days:.4f} | {len(trades) / forward_days:.4f} |",
        f"| Win rate | {float(pd.to_numeric(frame['win_rate_pct'], errors='coerce').mean()):.2f}% | {len(forward_wins) / len(forward_pnls) * 100:.2f}% |" if forward_pnls else "| Win rate | available | unavailable |",
        f"| Expectancy/trade | {float(pd.to_numeric(frame['expectancy_per_trade'], errors='coerce').mean()):.2f} | {float(np.mean(forward_pnls)):.2f} |" if forward_pnls else "| Expectancy/trade | available | unavailable |",
        f"| Profit factor | {phase3c_gross_profit / abs(phase3c_gross_loss):.3f} | {sum(forward_wins) / abs(sum(forward_losses)):.3f} |" if forward_losses and phase3c_gross_loss else "| Profit factor | unavailable | unavailable |",
        f"| Average trade net P&L | {phase3c_net / phase3c_trades:.2f} | {sum(forward_pnls) / len(forward_pnls):.2f} |" if forward_pnls and phase3c_trades else "| Average trade net P&L | unavailable | unavailable |",
        f"| Recorded costs | {phase3c_costs:.2f} | {sum(float(row['costs']) for row in _orders_for_trades(output_root, trades)):.2f} |",
        f"| Maximum drawdown | {phase3c_dd:.2f}% | {forward_dd:.2f}% |",
        f"| Average holding sessions | {phase3c_avg_holding:.2f} | {forward_holding:.2f} |",
        "| Strategy distribution | see Phase 3C test_results.csv | " + ", ".join(f"{row['strategy']}={row['closed_trades']}" for row in strategies if row["closed_trades"]) + " |",
        "",
        "The datasets, observation windows, and effective universe availability differ. Do not interpret this table as evidence of statistical significance or live execution performance.",
        "",
    ]
    _atomic_write(output_root / "forward_vs_phase3c_comparison.md", "\n".join(content))


def _orders_for_trades(output_root: Path, trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Costs remain available in the immutable date reports; this placeholder
    # intentionally avoids treating a synthetic mark as a transaction cost.
    total = 0.0
    for trade in trades:
        entry_date = trade.get("entry_date")
        exit_date = trade.get("exit_date")
        for day in {entry_date, exit_date} - {None}:
            path = output_root / str(day) / "orders.csv"
            if path.is_file():
                with path.open("r", newline="", encoding="utf-8") as stream:
                    for row in csv.DictReader(stream):
                        if row.get("signal_id") == trade.get("signal_id"):
                            try:
                                total += float(row.get("costs") or 0)
                            except ValueError:
                                pass
    return [{"costs": total}]


def write_personal_experiment_exports(
    database: str | Path,
    output_root: str | Path,
    *,
    experiment_manifest_path: str | Path,
    daily_manifest_dir: str | Path,
    target_sessions: int,
) -> dict[str, Any]:
    """Refresh isolated Phase 3D exports without ranking strategies."""
    db_path, root = Path(database), Path(output_root)
    manifest_path = Path(experiment_manifest_path)
    if not db_path.is_file() or not manifest_path.is_file():
        raise FileNotFoundError("personal experiment ledger or manifest is missing")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    baseline_name = manifest.get("baseline_name", "PHASE3D_PERSONAL_BASELINE")
    config_hash = manifest.get("config_sha256")
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        snapshots = _rows(connection, "SELECT * FROM portfolio_snapshots ORDER BY trading_date,snapshot_id")
        signals = _rows(connection, "SELECT * FROM signals ORDER BY trading_date,timestamp,symbol,strategy")
        orders = _rows(connection, "SELECT * FROM orders ORDER BY trading_date,created_at,order_id")
        fills = _rows(connection, "SELECT * FROM orders WHERE filled_at IS NOT NULL ORDER BY filled_at,order_id")
        positions = _rows(connection, "SELECT * FROM positions ORDER BY entry_date,position_id")
        trades = [row for row in positions if row.get("status") == "CLOSED"]
        capital_row = connection.execute("SELECT value FROM settings WHERE key='starting_capital'").fetchone()
        capital = float(capital_row[0]) if capital_row else 0.0
        journal = _rows(connection, "SELECT trading_date,payload_json FROM daily_journal ORDER BY trading_date")

    reports: dict[str, dict[str, Any]] = {}
    for row in journal:
        payload = json.loads(row["payload_json"])
        reports[str(row["trading_date"])] = payload.get("full_report", payload)
    daily: list[dict[str, Any]] = []
    prior_cash, prior_costs, prior_realized, prior_equity = capital, 0.0, 0.0, capital
    daily_starting_equities: list[float] = []
    for snap in snapshots:
        day = str(snap["trading_date"])
        report = reports.get(day, {})
        daily_starting_equities.append(prior_equity)
        costs = float(snap.get("transaction_costs") or 0.0)
        realized = float(snap.get("realized_pnl") or 0.0)
        pnl = float(snap.get("daily_pnl") or 0.0)
        daily.append({
            "date": day, "starting_cash": prior_cash, "ending_cash": float(snap.get("cash") or 0.0),
            "gross_pnl": pnl + costs - prior_costs, "costs": costs - prior_costs,
            "net_pnl": pnl, "realized_pnl": realized - prior_realized,
            "unrealized_pnl": float(snap.get("unrealized_pnl") or 0.0),
            "portfolio_value": float(snap.get("equity") or 0.0),
            "open_positions": int(snap.get("open_positions") or 0),
            "new_positions": sum(str(pos.get("entry_date") or "") == day for pos in positions),
            "closed_positions": sum(str(pos.get("exit_date") or "") == day for pos in positions),
            "daily_drawdown": float(snap.get("drawdown") or 0.0),
            "cumulative_return": float(snap.get("cumulative_return") or 0.0),
            "data_source": report.get("data", {}).get("provider"), "data_as_of": day,
            "strategy_version": baseline_name, "config_hash": config_hash,
            "nifty_date_used": report.get("market", {}).get("nifty_date"),
            "vix_date_used": report.get("market", {}).get("india_vix_date"),
        })
        prior_cash = float(snap.get("cash") or prior_cash)
        prior_costs, prior_realized = costs, realized
        prior_equity = float(snap.get("equity") or prior_equity)

    for day in reports:
        source = root / day
        target = root / "daily" / day
        for filename in ("daily_report.md", "daily_report.json"):
            source_file = source / filename
            if source_file.is_file():
                _atomic_write(target / filename, source_file.read_text(encoding="utf-8"))

    _write_csv(root / "daily_snapshots.csv", daily, [
        "date", "starting_cash", "ending_cash", "gross_pnl", "costs", "net_pnl", "realized_pnl",
        "unrealized_pnl", "portfolio_value", "open_positions", "new_positions", "closed_positions",
        "daily_drawdown", "cumulative_return", "data_source", "data_as_of", "strategy_version",
        "config_hash", "nifty_date_used", "vix_date_used",
    ])
    _write_csv(root / "signals.csv", signals, list(signals[0]) if signals else [
        "signal_id", "trading_date", "symbol", "strategy", "direction", "signal_price", "stop_loss",
        "target", "position_size", "risk_amount", "market_regime", "reason", "accepted", "status",
        "rejection_reason", "data_provider", "freshness_status",
    ])
    _write_csv(root / "orders.csv", orders, list(orders[0]) if orders else [
        "order_id", "signal_id", "trading_date", "symbol", "side", "quantity", "intended_price",
        "simulated_fill_price", "filled_at", "status", "strategy", "order_kind", "costs",
    ])
    _write_csv(root / "fills.csv", fills, list(fills[0]) if fills else [
        "order_id", "signal_id", "filled_at", "trading_date", "symbol", "side", "quantity",
        "simulated_fill_price", "slippage", "costs", "strategy", "order_kind",
    ])
    _write_csv(root / "positions.csv", positions, list(positions[0]) if positions else [
        "position_id", "signal_id", "entry_order_id", "exit_order_id", "symbol", "strategy", "direction",
        "sector", "quantity", "entry_price", "entry_date", "stop_loss", "target", "mark_price",
        "unrealized_pnl", "status", "exit_price", "exit_date", "realized_pnl", "holding_sessions",
    ])
    _write_csv(root / "trades.csv", trades, list(trades[0]) if trades else [
        "position_id", "symbol", "strategy", "direction", "quantity", "entry_price", "entry_date",
        "exit_price", "exit_date", "realized_pnl", "total_costs", "holding_sessions",
    ])
    cost_columns = ["order_id", "signal_id", "trading_date", "filled_at", "symbol", "strategy", "order_kind",
                    "brokerage", "stt", "exchange_charges", "gst", "sebi_charges", "stamp_duty", "dp_charge", "costs"]
    _write_csv(root / "costs.csv", [{key: row.get(key) for key in cost_columns} for row in orders], cost_columns)
    event_files = sorted((db_path.parent / "events").glob("*.jsonl"))
    _atomic_write(root / "events.jsonl", "".join(path.read_text(encoding="utf-8") for path in event_files))

    strategy_results: list[dict[str, Any]] = []
    for strategy in PHASE3C_DAILY_STRATEGIES:
        ss = [row for row in signals if row.get("strategy") == strategy]
        so = [row for row in orders if row.get("strategy") == strategy]
        st = [row for row in trades if row.get("strategy") == strategy]
        open_st = [row for row in positions if row.get("status") == "OPEN" and row.get("strategy") == strategy]
        net_values = [float(row.get("realized_pnl") or 0.0) for row in st]
        gross_values = [int(row.get("direction") or 0) * int(row.get("quantity") or 0)
                        * (float(row.get("exit_price") or 0) - float(row.get("entry_price") or 0)) for row in st]
        gross_values.extend(
            int(row.get("direction") or 0) * int(row.get("quantity") or 0)
            * (float(row.get("mark_price") or 0) - float(row.get("entry_price") or 0))
            for row in open_st
        )
        strategy_results.append({
            "strategy": strategy, "signals": len(ss), "orders": len(so), "trades": len(st),
            "wins": sum(value > 0 for value in net_values), "losses": sum(value < 0 for value in net_values),
            "gross_pnl": sum(gross_values), "costs": sum(float(row.get("costs") or 0) for row in so),
            "net_pnl": sum(net_values) + sum(float(row.get("unrealized_pnl") or 0.0) for row in open_st),
        })

    sessions = [str(row["trading_date"]) for row in snapshots]
    last = snapshots[-1] if snapshots else {}
    ending = float(last.get("equity") or capital)
    all_trade_pnl = [float(row.get("realized_pnl") or 0) for row in trades]
    wins, losses = [x for x in all_trade_pnl if x > 0], [x for x in all_trade_pnl if x < 0]
    total_costs = float(last.get("transaction_costs") or 0.0)
    net_pnl = ending - capital
    largest_position = max((abs(float(row.get("mark_price") or row.get("entry_price") or 0) * int(row.get("quantity") or 0))
                            for row in positions), default=0.0)
    sector_exposure = 0.0
    for snap in snapshots:
        try:
            sector_exposure = max(sector_exposure, max((float(v) for v in json.loads(snap.get("sector_exposure_json") or "{}").values()), default=0.0))
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    worst_daily = min((float(row["net_pnl"]) for row in daily), default=0.0)
    max_daily_loss = max((max(0.0, -float(row["net_pnl"])) / denominator
                          for row, denominator in zip(daily, daily_starting_equities) if denominator), default=0.0)
    ordered_trade_pnls = [float(row.get("realized_pnl") or 0.0) for row in sorted(
        trades, key=lambda row: (row.get("exit_date") or "", row.get("position_id") or "")
    )]
    longest_winning, longest_losing = 0, 0
    current_winning = current_losing = 0
    for value in ordered_trade_pnls:
        current_winning = current_winning + 1 if value > 0 else 0
        current_losing = current_losing + 1 if value < 0 else 0
        longest_winning = max(longest_winning, current_winning)
        longest_losing = max(longest_losing, current_losing)
    status = "COMPLETE" if len(sessions) >= target_sessions else ("IN_PROGRESS" if sessions else "READY_WAITING_FOR_DATA")
    summary = {
        "experiment_id": manifest.get("experiment_id"), "status": status, "starting_capital": capital,
        "ending_capital": ending, "gross_pnl": net_pnl + total_costs, "total_costs": total_costs,
        "net_pnl": net_pnl, "return": net_pnl / capital if capital else None,
        "maximum_drawdown": max((float(row.get("maximum_drawdown") or 0) for row in snapshots), default=0.0),
        "maximum_daily_loss": abs(worst_daily), "maximum_daily_loss_fraction": max_daily_loss,
        "largest_position_notional": largest_position, "largest_sector_exposure_notional": sector_exposure,
        "maximum_concurrent_positions": max((int(row.get("open_positions") or 0) for row in snapshots), default=0),
        "sessions_processed": len(sessions), "sessions_blocked": len(manifest.get("blocked_sessions", [])),
        "missing_data_sessions": sum("MISSING_EQUITY_DATA" in str(item) for item in manifest.get("blocked_sessions", [])),
        "symbols_processed": max((int(reports.get(day, {}).get("data", {}).get("symbols_received", 0)) for day in sessions), default=0),
        "signals": sum(row.get("strategy") != "DATA_HEALTH" for row in signals),
        "signals_rejected": sum(row.get("strategy") != "DATA_HEALTH" and not bool(row.get("accepted")) for row in signals),
        "orders_created": len(orders), "fills": len(fills),
        "trades": len(trades), "winning_trades": len(wins), "losing_trades": len(losses),
        "win_rate": len(wins) / len(trades) if trades else None,
        "average_win": float(np.mean(wins)) if wins else None, "average_loss": float(np.mean(losses)) if losses else None,
        "profit_factor": sum(wins) / abs(sum(losses)) if losses and sum(losses) else None,
        "expectancy": float(np.mean(all_trade_pnl)) if all_trade_pnl else None,
        "largest_win": max(wins) if wins else None, "largest_loss": min(losses) if losses else None,
        "average_holding_sessions": float(np.mean([int(row.get("holding_sessions") or 0) for row in trades])) if trades else None,
        "longest_winning_streak": longest_winning, "longest_losing_streak": longest_losing,
        "first_processed_date": sessions[0] if sessions else None, "last_processed_date": sessions[-1] if sessions else None,
        "strategy_results": strategy_results,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(root / "summary.json", summary)

    # Record hashes for only those dated files whose intake manifest passed.
    daily_sources = list(manifest.get("daily_data_sources", []))
    source_hashes = {item.get("source_sha256") for item in daily_sources}
    for day in sessions:
        intake_path = Path(daily_manifest_dir) / f"{day}.json"
        if not intake_path.is_file():
            continue
        try:
            item = json.loads(intake_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if item.get("validation_status") == "PASS" and item.get("source_sha256") not in source_hashes:
            daily_sources.append({"date": day, "source_filename": item.get("source_filename"),
                                  "source_sha256": item.get("source_sha256"), "rows": item.get("total_rows"),
                                  "symbols": item.get("symbol_count"), "provider": item.get("source_provider"),
                                  "source_identifier": item.get("source_identifier"),
                                  "adjustment_status": item.get("adjustment_status")})
            source_hashes.add(item.get("source_sha256"))
    manifest.update({"status": status, "processed_sessions": sessions, "processed_session_count": len(sessions),
                     "last_processed_date": sessions[-1] if sessions else None, "daily_data_sources": daily_sources})
    if status == "COMPLETE":
        manifest.setdefault("completed_at_utc", datetime.now(timezone.utc).isoformat())
    _write_json(manifest_path, manifest)

    lines = [
        "# Phase 3D Forward Paper Trading Report", "", f"Status: **{status}**", "",
        f"Sessions processed: {len(sessions)} / {target_sessions}",
        f"Processed range: {sessions[0] if sessions else 'not started'} to {sessions[-1] if sessions else 'not started'}",
        f"Starting capital: ₹{capital:,.2f}", f"Ending capital: ₹{ending:,.2f}",
        f"Gross P&L: ₹{net_pnl + total_costs:,.2f}", f"Total costs: ₹{total_costs:,.2f}",
        f"Net P&L: ₹{net_pnl:,.2f}", f"Return: {net_pnl / capital:.2%}" if capital else "Return: unavailable",
        f"Maximum drawdown: {summary['maximum_drawdown']:.2%}",
        f"Maximum daily loss: ₹{abs(worst_daily):,.2f} ({max_daily_loss:.2%})",
        f"Largest position notional: ₹{largest_position:,.2f}",
        f"Largest sector exposure: ₹{sector_exposure:,.2f}",
        f"Maximum concurrent positions: {summary['maximum_concurrent_positions']}",
        f"Trades: {len(trades)}; wins: {len(wins)}; losses: {len(losses)}",
        f"Win rate: {summary['win_rate']:.2%}" if summary["win_rate"] is not None else "Win rate: unavailable",
        f"Profit factor: {summary['profit_factor']:.4f}" if summary["profit_factor"] is not None else "Profit factor: unavailable",
        f"Expectancy per closed trade: ₹{summary['expectancy']:,.2f}" if summary["expectancy"] is not None else "Expectancy per closed trade: unavailable",
        f"Average win: ₹{summary['average_win']:,.2f}; average loss: ₹{summary['average_loss']:,.2f}" if summary["average_win"] is not None and summary["average_loss"] is not None else "Average win/loss: unavailable",
        f"Largest win: ₹{summary['largest_win']:,.2f}; largest loss: ₹{summary['largest_loss']:,.2f}" if summary["largest_win"] is not None and summary["largest_loss"] is not None else "Largest win/loss: unavailable",
        f"Longest winning/losing streaks: {summary['longest_winning_streak']}/{summary['longest_losing_streak']}",
        f"Average holding period: {summary['average_holding_sessions']:.2f} sessions" if summary["average_holding_sessions"] is not None else "Average holding period: unavailable",
        f"Sessions blocked: {summary['sessions_blocked']}; missing-data sessions: {summary['missing_data_sessions']}",
        f"Symbols processed: {summary['symbols_processed']}; rejected signals: {summary['signals_rejected']}", "",
        "## Strategy-level results", "", "Strategies are listed in frozen baseline order; no ranking or selection is performed.", "",
        "| Strategy | Signals | Orders | Trades | Wins | Losses | Gross P&L | Costs | Net P&L |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in strategy_results:
        lines.append(f"| {row['strategy']} | {row['signals']} | {row['orders']} | {row['trades']} | {row['wins']} | {row['losses']} | ₹{row['gross_pnl']:,.2f} | ₹{row['costs']:,.2f} | ₹{row['net_pnl']:,.2f} |")
    lines.extend(["", "## Limitations", "", "Personal paper trading only; no broker was contacted. Fills use the configured next-session open model with existing costs and slippage.",
                  "Equity cache is Yahoo-derived and not official or licensed NSE data. Official NSE CSV exports are used for NIFTY and India VIX.",
                  "No Kaggle or TejHQ rows are merged into the forward equity series. No strategy ranking is produced.", ""])
    content = "\n".join(lines)
    _atomic_write(root / "REPORT.md", content)
    if status == "COMPLETE":
        _atomic_write(root / "PHASE3D_FORWARD_REPORT.md", content)
    return summary
