"""Read-only local dashboard API and static frontend server."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import numpy as np
from zoneinfo import ZoneInfo

from nse_research.config import AppConfig
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.forward.engine import _json_default
from nse_research.forward.reports import _strategy_metrics
from nse_research.intraday import IntradayScannerConfig, load_frozen_universe
from nse_research.realtime.engine import initial_feed_status

_STATIC = Path(__file__).resolve().parents[2] / "forward" / "dashboard"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class _LocalDashboardServer(ThreadingHTTPServer):
    """Allow quick local restarts when prior HTTP connections are in TIME_WAIT."""

    allow_reuse_address = True


def _connect_read_only(database: Path) -> sqlite3.Connection:
    quoted = quote(str(database.resolve()))
    return sqlite3.connect(f"file:{quoted}?mode=ro", uri=True)


def _dict_rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description or []]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _no_data() -> dict[str, Any]:
    return {"scope": "FORWARD PAPER", "status": "NO_FORWARD_DATA", "live_trading_enabled": False}


def _rooted(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _realtime_payload(root: Path, config: Any, *, now: datetime | None = None) -> dict[str, Any]:
    """Read-only view of current config and an optional feed health snapshot."""
    current = now or datetime.now(ZoneInfo(config.project.timezone))
    try:
        calendar = NseTradingCalendar(_rooted(root, config.forward_paper.trading_calendar_path))
    except (OSError, ValueError):
        calendar = NseTradingCalendar()
    payload = initial_feed_status(
        config.realtime,
        now=current,
        calendar=calendar,
        timezone_name=config.project.timezone,
    )
    # Static discovery metadata only. This is not a provider configuration and
    # never starts a network worker or supplies quotes to the realtime engine.
    payload["source_discovery"] = {
        "provider": "Indian API (free-plan candidate; not configured)",
        "source_classification": "DELAYED documented; not empirically verified",
        "access_status": "Free plan requires account/API key; underlying data rights unverified; no key used",
        "universe_capability": "UNKNOWN; 100-symbol validation not run and batch limit is unpublished",
        "timestamp_status": "IST timestamp fields documented; source freshness not empirically verified",
        "intraday_capability": "5-minute price observations documented; no observed payload or candles",
        "last_validation": "2026-09-27 documentation review; authenticated samples unavailable",
    }
    try:
        universe_size = int(os.environ.get("REALTIME_UNIVERSE_SIZE", "100"))
    except ValueError:
        universe_size = 100
    if universe_size not in {100, 200, 486}:
        universe_size = 100
    payload["universe_size"] = universe_size
    payload["mode"] = os.environ.get("UPSTOX_MODE", "ltpc").strip().upper()
    if (config.realtime.enabled and config.realtime.provider.strip().lower() == "upstox"
            and not os.environ.get("UPSTOX_ACCESS_TOKEN", "")):
        payload.update({
            "feed_status": "DISCONNECTED",
            "status_label": "REALTIME PROVIDER: NOT CONFIGURED",
            "status_code": "REALTIME_PROVIDER_NOT_CONFIGURED",
            "reason": "UPSTOX_ACCESS_TOKEN is not present in the dashboard process environment.",
        })
        return payload
    if payload.get("status_code") != "FEED_NOT_STARTED":
        return payload
    path = _rooted(root, config.realtime.health_snapshot_path)
    snapshot = _read_json(path)
    if snapshot is None:
        return payload
    # Do not surface a prior provider snapshot after it has been disabled or
    # changed. A snapshot is observational state and is never used by trading.
    snapshot_provider = str(snapshot.get("provider") or "").strip().lower()
    if not config.realtime.enabled or snapshot_provider != config.realtime.provider.strip().lower():
        return payload
    if snapshot_provider == "upstox":
        provider_details = snapshot.get("provider_details")
        if isinstance(provider_details, dict):
            try:
                snapshot_universe_size = int(provider_details.get(
                    "universe_size", provider_details.get("mapped_symbols", universe_size),
                ))
            except (TypeError, ValueError):
                snapshot_universe_size = -1
            if snapshot_universe_size != universe_size:
                payload.update({
                    "feed_status": "DISCONNECTED",
                    "status_label": "REALTIME FEED: UNIVERSE CONFIGURATION MISMATCH",
                    "status_code": "REALTIME_UNIVERSE_MISMATCH",
                    "reason": "The feed-health snapshot was recorded for a different realtime universe size.",
                })
                return payload
    safety_flags = ("live_trading_enabled", "broker_execution_enabled", "signals_generated", "ledger_mutated")
    if any(snapshot.get(flag) is not False for flag in safety_flags):
        return payload
    if snapshot.get("feed_status") not in {"CONNECTING", "CONNECTED", "DEGRADED", "DISCONNECTED", "RECONNECTING"}:
        return payload
    if any(not isinstance(snapshot.get(key), list) for key in (
        "quotes", "candles", "requested_symbol_list", "received_symbols", "accepted_symbols",
        "missing_symbols", "invalid_symbols", "stale_symbols",
    )):
        return payload
    try:
        generated = datetime.fromisoformat(str(snapshot.get("generated_at") or ""))
        if generated.tzinfo is None or generated.utcoffset() is None:
            return payload
    except ValueError:
        return payload
    payload.update(snapshot)
    payload["universe_size"] = universe_size
    payload["mode"] = os.environ.get("UPSTOX_MODE", "ltpc").strip().upper()
    age = (current.astimezone(ZoneInfo("UTC")) - generated.astimezone(ZoneInfo("UTC"))).total_seconds()
    if age < -config.realtime.max_future_skew_seconds:
        return _realtime_fallback(root, config, current, calendar)
    payload["health_snapshot_age_seconds"] = max(0.0, age)
    stale_symbols = set(payload.get("stale_symbols", []))
    quote_rows = []
    raw_quotes = payload.get("quotes", [])
    for row in raw_quotes if isinstance(raw_quotes, list) else []:
        if not isinstance(row, dict):
            continue
        quote = dict(row)
        try:
            source_stamp = datetime.fromisoformat(str(quote.get("source_timestamp") or ""))
            if source_stamp.tzinfo is None or source_stamp.utcoffset() is None:
                raise ValueError("timezone required")
            quote_age = max(0.0, (current.astimezone(ZoneInfo("UTC")) - source_stamp.astimezone(ZoneInfo("UTC"))).total_seconds())
        except ValueError:
            quote_age = None
        quote["quote_age_seconds"] = quote_age
        quote["freshness_status"] = (
            "FRESH" if quote_age is not None and quote_age <= config.realtime.max_quote_age_seconds else "STALE"
        )
        if quote["freshness_status"] == "STALE":
            stale_symbols.add(str(quote.get("symbol") or ""))
        quote_rows.append(quote)
    payload["quotes"] = quote_rows
    payload["stale_symbols"] = sorted(symbol for symbol in stale_symbols if symbol)
    stale_health = age > config.realtime.max_quote_age_seconds
    if age > config.realtime.max_quote_age_seconds:
        payload["feed_status"] = "DISCONNECTED"
        payload["status_label"] = "REALTIME FEED: DISCONNECTED"
        payload["reason"] = "The last feed-health snapshot is older than the configured quote freshness limit."
        payload["status_code"] = "FEED_HEALTH_STALE"
    elif payload.get("feed_status") == "CONNECTED" and payload["stale_symbols"]:
        payload["feed_status"] = "DEGRADED"
        payload["status_label"] = "REALTIME FEED: DEGRADED"
        payload["reason"] = "One or more last-known quotes exceeded the configured freshness limit."
        payload["status_code"] = "QUOTE_STALE"
    else:
        payload["status_code"] = "FEED_HEALTH_CURRENT"
    observed_data = payload.get("feed_status") in {"CONNECTED", "DEGRADED"} and any(
        row.get("freshness_status") == "FRESH" for row in quote_rows
    )
    market = initial_feed_status(
        config.realtime,
        now=current,
        calendar=calendar,
        timezone_name=config.project.timezone,
        observed_market_data=observed_data,
    )
    payload["market_status"] = market["market_status"]
    payload["market_status_reason"] = market["market_status_reason"]
    candles = payload.get("candles", [])
    for candle in candles if isinstance(candles, list) else []:
        if not isinstance(candle, dict) or candle.get("status") != "FORMING":
            continue
        try:
            candle_end = datetime.fromisoformat(str(candle.get("end") or ""))
        except ValueError:
            candle_end = None
        if (stale_health or candle_end is None or candle_end.tzinfo is None or
                candle_end.astimezone(ZoneInfo("UTC")) <= current.astimezone(ZoneInfo("UTC"))):
            candle["status"] = "STALE / CLOSE UNCONFIRMED"
    return payload


def _phase4c_paper_payload(root: Path) -> dict[str, Any]:
    """Read the Phase 4C paper ledger without constructing or advancing an engine."""
    from nse_research.execution.phase4c import inspect_paper_ledger

    ledger = root / "data/paper_trading/phase4c/ledger.json"
    inspected = inspect_paper_ledger(ledger)
    if inspected["status"] != "OK":
        return {
            "status": inspected["status"],
            "risk_status": "DATA_UNAVAILABLE",
            "ledger_path": str(ledger.relative_to(root)) if ledger.is_relative_to(root) else str(ledger),
            "reason": inspected.get("reason"),
            "execution_mode": "PAPER",
            "paper_trading_enabled": False,
            "realtime_enabled": False,
            "live_trading_enabled": False,
            "orders": [], "positions": [], "trades": [], "risk_events": [], "equity_curve": [],
        }
    state = inspected["state"]
    metrics = state.get("portfolio_metrics") or {}
    config = state.get("config") or {}
    daily_loss = max(0.0, -float(metrics.get("today_pnl") or 0.0))
    daily_limit = float(metrics.get("daily_loss_limit_amount") or 0.0)
    drawdown = float(metrics.get("drawdown") or 0.0)
    drawdown_limit = float(config.get("max_portfolio_drawdown") or 0.0)
    risk_state = metrics.get("risk_state", "ACTIVE")
    risk_status = "HALTED" if risk_state != "ACTIVE" else (
        "WARNING" if (daily_limit > 0 and daily_loss >= daily_limit * 0.8
                      or drawdown_limit > 0 and drawdown >= drawdown_limit * 0.8
                      or metrics.get("open_positions", 0) >= config.get("max_positions", 5)) else "OK"
    )
    return {
        "status": "OK",
        "risk_status": risk_status,
        "ledger_path": str(ledger.relative_to(root)) if ledger.is_relative_to(root) else str(ledger),
        "event_count": inspected["event_count"],
        "last_event_hash": inspected["last_event_hash"],
        "execution_mode": config.get("execution_mode", "PAPER"),
        "paper_trading_enabled": bool(config.get("paper_trading_enabled", False)),
        "realtime_enabled": bool(config.get("realtime_enabled", False)),
        "live_trading_enabled": bool(config.get("live_trading_enabled", False)),
        "initial_capital": metrics.get("initial_capital", config.get("initial_capital")),
        "current_equity": metrics.get("total_equity"),
        "available_cash": metrics.get("available_cash"),
        "cash": metrics.get("cash"),
        "used_capital": metrics.get("used_capital"),
        "gross_exposure": metrics.get("gross_exposure"),
        "realized_pnl": metrics.get("realized_pnl"),
        "unrealized_pnl": metrics.get("unrealized_pnl"),
        "today_pnl": metrics.get("today_pnl"),
        "current_daily_loss": daily_loss,
        "total_pnl": (metrics.get("total_equity", 0) - metrics.get("initial_capital", 0)),
        "drawdown": metrics.get("drawdown"),
        "peak_equity": metrics.get("peak_equity"),
        "risk_state": metrics.get("risk_state", "ACTIVE"),
        "halt_reasons": metrics.get("halt_reasons", {}),
        "risk_limits": {
            "risk_per_trade": config.get("risk_per_trade"),
            "daily_loss_limit": config.get("max_daily_loss"),
            "daily_loss_limit_amount": metrics.get("daily_loss_limit_amount"),
            "portfolio_drawdown_limit": config.get("max_portfolio_drawdown"),
            "max_positions": config.get("max_positions"),
            "max_symbol_exposure": config.get("max_symbol_exposure"),
            "max_sector_exposure": config.get("max_sector_exposure"),
        },
        "open_positions": metrics.get("open_positions", 0),
        "symbol_exposures": metrics.get("symbol_exposures", {}),
        "sector_exposures": metrics.get("sector_exposures", {}),
        "pending_orders": metrics.get("pending_orders", 0),
        "positions": state.get("positions", []),
        "orders": state.get("orders", []),
        "trades": state.get("trades", []),
        "risk_events": state.get("risk_events", []),
        "equity_curve": state.get("equity_curve", []),
    }


def _phase4d_session_payload(root: Path) -> dict[str, Any]:
    """Read the Phase 4D checkpoint and Phase 4C snapshot without advancing either."""
    from nse_research.execution.phase4c import inspect_paper_ledger

    ledger = root / "data/paper_trading/phase4c/ledger.json"
    data_path = _rooted(root, os.environ.get("INTRADAY_DATA_PATH", "data/intraday/canonical_intraday_bars.csv"))
    inspected = inspect_paper_ledger(ledger)
    checkpoint = None
    session = None
    portfolio: dict[str, Any] = {}
    if inspected.get("status") == "OK":
        state = inspected.get("state") or {}
        session_state = state.get("session_state") or {}
        session = session_state.get("active_session") or session_state.get("last_session")
        metrics = state.get("portfolio_metrics") or {}
        portfolio = {
            "equity": metrics.get("total_equity"), "cash": metrics.get("cash"),
            "exposure": metrics.get("gross_exposure"), "open_positions": metrics.get("open_positions", 0),
            "daily_pnl": metrics.get("today_pnl"), "total_pnl": metrics.get("total_equity", 0) - metrics.get("initial_capital", 0),
            "drawdown": metrics.get("drawdown"), "risk_state": metrics.get("risk_state", "ACTIVE"),
        }
        session_id = (session or {}).get("session_id")
        if session_id:
            try:
                value = json.loads((ledger.parent / "checkpoints" / f"{session_id}.json").read_text(encoding="utf-8"))
                checkpoint = value if isinstance(value, dict) else None
            except (OSError, UnicodeError, json.JSONDecodeError):
                checkpoint = None
    if session is None:
        session = {"status": "BLOCKED", "mode": "PRECHECK", "reason": (
            "PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE" if not data_path.is_file()
            else "No Phase 4D session has been run")}
    quality_path = root / "results/phase4d" / str(session.get("session_id") or "") / "data_quality.json"
    try:
        data_health = json.loads(quality_path.read_text(encoding="utf-8"))
        if not isinstance(data_health, dict):
            data_health = {}
    except (OSError, UnicodeError, json.JSONDecodeError):
        data_health = {
            "expected_symbols": session.get("universe_size", int(os.environ.get("INTRADAY_UNIVERSE_SIZE", "100"))),
            "symbols_with_data": 0, "coverage_percent": 0.0,
            "bars_received": checkpoint.get("bars_processed", 0) if checkpoint else 0,
            "missing_intervals": None, "duplicate_intervals": None, "rejected_rows": None,
        }
    return {
        "status": inspected.get("status", "PAPER_LEDGER_MISSING"),
        "session": session, "checkpoint": checkpoint, "portfolio": portfolio,
        "data_health": data_health, "data_path": str(data_path),
        "dataset_available": data_path.is_file(), "ledger_path": str(ledger),
        "ledger_event_count": inspected.get("event_count", 0),
        "live_trading_enabled": False, "broker_connected": False, "dashboard_read_only": True,
    }


def _intraday_scanner_payload(root: Path) -> dict[str, Any]:
    """Read-only empty/status view. Opening this endpoint never starts a scanner."""
    config_error = None
    try:
        settings = IntradayScannerConfig.from_env()
        universe_size = settings.universe_size
        configured_enabled = settings.enabled
        regime_filter = settings.regime_filter_enabled
    except (ValueError, TypeError) as exc:
        config_error = str(exc)
        universe_size = 100
        configured_enabled = False
        regime_filter = False
    universe_path = "results/phase4a8a_upstox_live_universe_100.csv" if universe_size == 100 else "data/paper_trading/phase3d_personal_universe_v2.csv"
    try:
        universe = load_frozen_universe(root, universe_size)
        expected = len(universe)
        universe_status = "FROZEN_UNIVERSE_AVAILABLE"
    except (OSError, ValueError, KeyError) as exc:
        expected = 0
        universe_status = f"UNIVERSE_UNAVAILABLE: {exc}"
    return {
        "status": "INTRADAY_DATA_UNAVAILABLE",
        "intraday_data_available": False,
        "scanner_status": "BLOCKED_NO_VALIDATED_INTRADAY_DATA",
        "configured_enabled": configured_enabled,
        "scanner_started": False,
        "data_source": None,
        "data_classification": "INTRADAY_DATA_UNAVAILABLE",
        "universe_size": universe_size,
        "universe_status": universe_status,
        "universe_path": universe_path,
        "last_bar": None,
        "bars_1m": 0,
        "bars_5m": 0,
        "indicators_ready": 0,
        "signals_today": 0,
        "signals": [],
        "orders": 0,
        "regime_filter_enabled": regime_filter,
        "configuration_error": config_error,
        "health": {
            "symbols_expected": expected,
            "symbols_with_data": 0,
            "bars_received": 0,
            "bars_rejected": 0,
            "missing_intervals": 0,
            "duplicate_bars": 0,
            "out_of_order_bars": 0,
            "timestamp_errors": 0,
            "volume_unavailable": 0,
        },
        "reason": "Repository audit found no genuine historical intraday price bars. The dashboard does not start a provider, replay, scanner, or order path.",
    }


def _phase4g_yf_payload(root: Path) -> dict[str, Any]:
    """Read Phase 4G-YF session artifacts without starting or advancing a run."""
    output = root / "results/phase4g_yf_20260928"
    manifest = _read_json(output / "manifest.json") or {}
    preflight = _read_json(output / "reports/preflight.json") or {}
    acquisition = _read_json(output / "reports/acquisition_report.json") or {}
    summary = _read_json(output / "reports/descriptive_one_day_summary.json") or {}

    def rows(relative: str) -> list[dict[str, str]]:
        try:
            with (output / relative).open("r", newline="", encoding="utf-8-sig") as stream:
                return [dict(row) for row in csv.DictReader(stream)]
        except (OSError, UnicodeError, csv.Error):
            return []

    bars = rows("canonical/forward_bars_1m.csv")
    signals = rows("signals/signals.csv")
    orders = rows("orders/orders.csv")
    fills = rows("fills/fills.csv")
    positions = rows("positions/open_positions.csv")
    trades = rows("reports/closed_trades.csv")
    risk_events = rows("risk/risk_events.csv")
    rankings = rows("rankings/candidate_rankings.csv")
    revision_root = root / "results/phase4g_yf_20260928_revision_reconciliation/reports"
    revision_readiness = _read_json(revision_root / "readiness_report.json") or {}
    revision_summary = _read_json(revision_root / "revision_summary.json") or {}
    if rankings:
        latest_decision = max((row.get("decision_timestamp") or "") for row in rankings)
        rankings = [row for row in rankings if (row.get("decision_timestamp") or "") == latest_decision]
        rankings.sort(key=lambda row: (int(row.get("rank") or 999999), row.get("symbol") or ""))
    else:
        rankings = rows("reports/top_ranked_candidates.csv")

    manifest_status = str(manifest.get("status") or "PREPARATION_ARTIFACTS_MISSING")
    preflight_passed = bool(preflight) and not preflight.get("reasons") and preflight.get("calendar_check") == "TRADING_DAY"
    # The canonical first-observed bar file is written only from provider data.
    # Its contents, not a mutable status label, establish that acquisition began.
    data_acquired = bool(bars)
    if "BLOCKED" in manifest_status:
        status = "BLOCKED"
    elif data_acquired and (manifest.get("reconciliation") == "RECONCILIATION_COMPLETE"
                            or manifest_status == "PHASE4G_YF_DATA_COMPLETE_DESCRIPTIVE_ONLY"):
        status = "COMPLETE"
    elif data_acquired:
        status = "RUNNING"
    elif preflight_passed:
        status = "READY"
    else:
        status = "BLOCKED"

    latest_snapshot = acquisition.get("latest_snapshot") or {}
    latest_symbols = latest_snapshot.get("successful_symbols") or []
    quality = acquisition.get("validation_status") or "NOT_ACQUIRED"
    valid_symbols = len(latest_symbols) if quality == "PASS" else None
    no_run = not data_acquired
    return {
        "status": status,
        "execution_status": manifest_status,
        "session_date": "2026-09-28",
        "session_open": "09:15",
        "session_close": "15:30",
        "timezone": "Asia/Kolkata",
        "provider": "Yahoo Finance / yfinance",
        "data_classification": "THIRD_PARTY_RESEARCH_SOURCE",
        "authorized_nse_feed": False,
        "interval": "1m",
        "universe_expected": 486,
        "symbols_received": len(latest_symbols) if data_acquired else 0,
        "valid_symbols": valid_symbols,
        "bars": len(bars),
        "data_quality": quality,
        "top_candidates": rankings[:10],
        "signals": None if no_run else len(signals),
        "paper_orders": None if no_run else len(orders),
        "fills": None if no_run else len(fills),
        "open_positions": None if no_run else len(positions),
        "closed_trades": None if no_run else len(trades),
        "gross_pnl": summary.get("gross_pnl_closed_trades"),
        "costs": summary.get("cost_total_closed_trades"),
        "net_pnl": summary.get("net_pnl_closed_trades"),
        "max_drawdown": summary.get("max_intraday_drawdown"),
        "risk_events": None if no_run else len(risk_events),
        "preflight": preflight,
        "immutable_bar_readiness": revision_readiness.get("status", "NOT_RUN"),
        "bar_settling_seconds": revision_readiness.get("settling_seconds"),
        "revision_summary": revision_summary,
        "block_reasons": list(preflight.get("reasons") or []),
        "data_acquired": data_acquired,
        "signals_run": not no_run,
        "paper_only": True,
        "broker_calls": 0,
        "live_orders": 0,
        "dashboard_read_only": True,
        "output_directory": "results/phase4g_yf_20260928",
        "temporary_ledger": "results/phase4g_yf_20260928/paper_ledger.json",
        "monitor": _phase4g_yf_monitor_payload(root),
    }


def _phase4g_yf_monitor_payload(root: Path) -> dict[str, Any]:
    """Read-only continuous-monitor state. This view never starts acquisition."""
    output = root / "results/phase4g_yf_20260928_monitor"
    manifest = _read_json(output / "manifest.json") or {}
    acquisition = _read_json(output / "reports/acquisition_report.json") or {}
    revision_root = root / "results/phase4g_yf_20260928_revision_reconciliation/reports"
    revision_readiness = _read_json(revision_root / "readiness_report.json") or {}
    revision_summary = _read_json(revision_root / "revision_summary.json") or {}

    def rows(relative: str) -> list[dict[str, str]]:
        try:
            with (output / relative).open("r", newline="", encoding="utf-8-sig") as stream:
                return [dict(item) for item in csv.DictReader(stream)]
        except (OSError, UnicodeError, csv.Error):
            return []

    audit: list[dict[str, Any]] = []
    try:
        value = json.loads((output / "reports/signal_audit.json").read_text(encoding="utf-8"))
        if isinstance(value, list):
            audit = [item for item in value if isinstance(item, dict)]
    except (OSError, UnicodeError, json.JSONDecodeError):
        pass
    bars = rows("canonical/forward_bars_1m.csv")
    orders = rows("orders/orders.csv")
    positions = rows("positions/open_positions.csv")
    rankings = rows("rankings/candidate_rankings.csv")
    latest = acquisition.get("latest_snapshot") or {}
    latest_decision = max((str(item.get("decision_timestamp") or "") for item in audit), default="")
    current_decisions = [item for item in audit if item.get("decision_timestamp") == latest_decision]
    watches = sorted((item for item in current_decisions if item.get("signal") == "WATCH"),
                     key=lambda item: item.get("symbol") or "")
    latest_rank_time = max((str(item.get("decision_timestamp") or "") for item in rankings), default="")
    latest_rankings = sorted(
        (item for item in rankings if str(item.get("decision_timestamp") or "") == latest_rank_time),
        key=lambda item: (int(float(item.get("rank") or 999999)), str(item.get("symbol") or "")),
    )
    candidates = [{**item, "signal": "WATCH"} for item in latest_rankings]
    buy_audit = sorted((item for item in audit if item.get("signal") == "BUY"),
                       key=lambda item: (item.get("decision_timestamp") or "", item.get("symbol") or ""), reverse=True)
    order_by_id = {item.get("order_id"): item for item in orders if item.get("order_id")}
    buy_rows = []
    for item in buy_audit:
        order = order_by_id.get(item.get("paper_order_id"), {})
        buy_rows.append({
            "timestamp": item.get("timestamp"), "decision_timestamp": item.get("decision_timestamp"),
            "symbol": item.get("symbol"),
            "market_timestamp": item.get("market_timestamp"),
            "received_timestamp": item.get("received_timestamp"),
            "strategy": item.get("strategy"), "entry": item.get("entry"),
            "stop_loss": item.get("stop_loss"), "target": item.get("target"),
            "quantity": item.get("quantity"), "risk_reward": item.get("risk_reward"),
            "status": order.get("status") or item.get("paper_order_status") or "PAPER",
            "paper_order_id": item.get("paper_order_id"),
        })
    decision_snapshots: list[dict[str, Any]] = []
    try:
        snapshot_path = output / "reports/decision_snapshots.jsonl"
        if snapshot_path.is_file():
            decision_snapshots = [json.loads(line) for line in snapshot_path.read_text(encoding="utf-8").splitlines()
                                  if line.strip()]
    except (OSError, UnicodeError, json.JSONDecodeError):
        decision_snapshots = []
    open_market = False
    try:
        local_now = datetime.now(ZoneInfo("Asia/Kolkata"))
        open_market = local_now.date().isoformat() == "2026-09-28" and local_now.strftime("%H:%M") >= "09:15" and local_now.strftime("%H:%M") < "15:30"
    except Exception:
        local_now = None
    if str(manifest.get("status") or "").startswith("PHASE4G_YF_BLOCKED"):
        status = "BLOCKED"
    elif not manifest:
        status = "NOT_STARTED"
    elif not manifest.get("monitor_mode"):
        status = "NOT STARTED"
    elif manifest.get("monitor_status") in {"LIVE_MONITORING", "PAPER_MONITORING_ACTIVE"}:
        status = "PAPER MONITORING"
    else:
        status = str(manifest.get("monitor_status") or manifest.get("status") or "NOT_STARTED")
    market_timestamps = [str(item.get("timestamp")) for item in bars if item.get("timestamp")]
    received_timestamps = [str(item.get("received_timestamp")) for item in bars if item.get("received_timestamp")]
    rejected_signal_count = sum(1 for item in audit if item.get("signal") == "REJECTED" and item.get("strategy"))
    accounting = manifest.get("latest_refresh_accounting") or {}
    current_late = accounting.get("late_bars")
    if current_late is None:
        current_late = sum(int(item.get("late_bars_excluded", 0) or 0) for item in decision_snapshots)
    return {
        "status": status,
        "execution_status": manifest.get("status", "NOT_STARTED"),
        "session_date": "2026-09-28",
        "market_status": "OPEN" if open_market else "CLOSED",
        "last_refresh": manifest.get("last_refresh_at"),
        "universe": int(manifest.get("frozen_universe_size") or 486),
        "symbols_valid": len(latest.get("successful_symbols") or []),
        "symbols_with_valid_session_data": accounting.get("symbols_with_valid_session_data"),
        "symbols_with_current_response_data": accounting.get("symbols_with_data", len(latest.get("successful_symbols") or [])),
        "symbols_with_new_bars": accounting.get("symbols_with_new_bars"),
        "symbols_with_refresh_failure": accounting.get("symbols_with_refresh_failure"),
        "bars_received": len(bars),
        "current_rows_seen": accounting.get("rows_seen"),
        "unique_market_bars": accounting.get("unique_market_bars"),
        "new_market_bars": accounting.get("new_market_bars"),
        "duplicate_market_bars": accounting.get("duplicate_market_bars"),
        "duplicate_conflict": accounting.get("duplicate_conflict"),
        "eligible_bars": accounting.get("eligible_bars"),
        "immutable_bar_readiness": revision_readiness.get("status", "NOT_RUN"),
        "bar_settling_seconds": revision_readiness.get("settling_seconds"),
        "forming_bars": (revision_readiness.get("bar_status_counts") or {}).get("FORMING_BAR", 0),
        "settling_bars": (revision_readiness.get("bar_status_counts") or {}).get("SETTLING_BAR", 0),
        "immutable_bars": (revision_readiness.get("bar_status_counts") or {}).get("IMMUTABLE_CLOSED_BAR", 0),
        "conflicting_bars": (revision_readiness.get("bar_status_counts") or {}).get("CONFLICTING_BAR", 0),
        "post_immutability_revisions": revision_summary.get("post_immutability_revisions", 0),
        "pre_immutability_revisions": revision_summary.get("pre_immutability_revisions", 0),
        "revision_gate_reasons": revision_readiness.get("blocking_reasons", []),
        "last_market_bar": max(market_timestamps) if market_timestamps else None,
        "last_response_received": max(received_timestamps) if received_timestamps else None,
        "late_bars_excluded": current_late,
        "valid_rows": len(bars),
        "invalid_rows": accounting.get("invalid_rows"),
        "duplicate_rows": accounting.get("duplicate_market_bars"),
        "missing_symbols": len(latest.get("missing_symbols") or []),
        "data_quality": latest.get("validation_status", "NOT_ACQUIRED"),
        "receipt_time_protection": "PASS" if bars and all(item.get("timestamp_proof") is not False for item in buy_audit) else ("PENDING" if not buy_audit else "FAIL"),
        "buy_signals_today": len(buy_audit),
        "open_paper_positions": len(positions),
        "open_positions_detail": positions,
        "watchlist_candidates": len(candidates) if candidates else len(watches),
        "watch_signals": len(candidates) if candidates else len(watches),
        "rejected_signals": rejected_signal_count,
        "buy_signals": buy_rows,
        "top_candidates": candidates[:10],
        "watchlist": (candidates if candidates else watches)[:20],
        "data_provider": "Yahoo Finance / yfinance",
        "data_classification": "THIRD_PARTY_RESEARCH_SOURCE",
        "nse_authorization": "NOT_CLAIMED",
        "authorized_nse_feed": False,
        "paper_only": True,
        "live_trading_enabled": False,
        "broker": "NONE",
        "broker_calls": 0,
        "live_orders": 0,
        "dashboard_read_only": True,
        "output_directory": "results/phase4g_yf_20260928_monitor",
        "ledger_path": "results/phase4g_yf_20260928_monitor/paper_ledger.json",
        "refresh_interval_seconds": manifest.get("refresh_interval_seconds", 60),
        "open_position_capacity": 5,
    }


def _realtime_fallback(root: Path, config: Any, now: datetime, calendar: NseTradingCalendar) -> dict[str, Any]:
    return initial_feed_status(config.realtime, now=now, calendar=calendar,
                               timezone_name=config.project.timezone)


def _load_dashboard_config(root: Path, config_path: str | Path | None):
    try:
        config = AppConfig.load(_rooted(root, config_path or "config/phase3d_personal.yaml"))
        realtime_updates: dict[str, Any] = {}
        if "REALTIME_ENABLED" in os.environ:
            realtime_updates["enabled"] = os.environ["REALTIME_ENABLED"].strip().lower() in {"1", "true", "yes", "on"}
        if "REALTIME_PROVIDER" in os.environ:
            realtime_updates["provider"] = os.environ["REALTIME_PROVIDER"].strip().lower()
        for variable, field in (
            ("REALTIME_MAX_QUOTE_AGE_SECONDS", "max_quote_age_seconds"),
            ("REALTIME_MAX_FUTURE_SKEW_SECONDS", "max_future_skew_seconds"),
        ):
            if variable in os.environ:
                realtime_updates[field] = float(os.environ[variable])
        if realtime_updates:
            realtime = config.realtime.model_copy(update=realtime_updates)
            config = config.model_copy(update={"realtime": realtime})
        return config
    except (OSError, ValueError):
        return None


def _parse_market_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    for pattern in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(text, pattern).date()
        except ValueError:
            continue
    return None


def _csv_date_coverage(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"available": False, "path": str(path), "rows": 0, "first_date": None, "last_date": None,
                "duplicate_dates": 0, "invalid_date_rows": 0}
    dates: list[date] = []
    seen: set[date] = set()
    duplicates = invalid = rows = 0
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            date_column = next((name for name in (reader.fieldnames or []) if name.strip().casefold() == "date"), None)
            if not date_column:
                return {"available": True, "path": str(path), "rows": 0, "first_date": None, "last_date": None,
                        "duplicate_dates": 0, "invalid_date_rows": 1, "error": "Date column is missing"}
            for row in reader:
                if not row or not any(value for value in row.values() if value):
                    continue
                rows += 1
                market_date = _parse_market_date(row.get(date_column))
                if market_date is None:
                    invalid += 1
                    continue
                dates.append(market_date)
                if market_date in seen:
                    duplicates += 1
                seen.add(market_date)
    except (OSError, UnicodeError, csv.Error) as error:
        return {"available": False, "path": str(path), "rows": 0, "first_date": None, "last_date": None,
                "duplicate_dates": 0, "invalid_date_rows": 0, "error": str(error)}
    return {"available": True, "path": str(path), "rows": rows,
            "first_date": min(dates).isoformat() if dates else None,
            "last_date": max(dates).isoformat() if dates else None,
            "duplicate_dates": duplicates, "invalid_date_rows": invalid}


def _identity_coverage(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"available": False, "path": str(path), "symbols": 0, "eq_symbols": 0,
                "non_eq_rows": 0, "missing_series_rows": 0, "duplicate_symbols": 0}
    symbols: list[str] = []
    series_values: list[str] = []
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            fields = {name.strip().casefold(): name for name in (reader.fieldnames or [])}
            symbol_col, series_col = fields.get("symbol"), fields.get("series")
            if not symbol_col or not series_col:
                return {"available": True, "path": str(path), "symbols": 0, "eq_symbols": 0,
                        "non_eq_rows": 0, "missing_series_rows": 0, "duplicate_symbols": 0,
                        "error": "Symbol or Series column is missing"}
            for row in reader:
                symbol = str(row.get(symbol_col) or "").strip().upper()
                if symbol:
                    symbols.append(symbol)
                    series_values.append(str(row.get(series_col) or "").strip().upper())
    except (OSError, UnicodeError, csv.Error) as error:
        return {"available": False, "path": str(path), "symbols": 0, "eq_symbols": 0,
                "non_eq_rows": 0, "missing_series_rows": 0, "duplicate_symbols": 0, "error": str(error)}
    counts = Counter(symbols)
    return {"available": True, "path": str(path), "symbols": len(symbols),
            "eq_symbols": sum(series == "EQ" for series in series_values),
            "non_eq_rows": sum(bool(series) and series != "EQ" for series in series_values),
            "missing_series_rows": sum(not series for series in series_values),
            "duplicate_symbols": sum(count - 1 for count in counts.values() if count > 1),
            "series_is_explicit": True}


def _experiment_payload(root: Path, config: Any) -> dict[str, Any]:
    manifest_path = _rooted(root, config.forward_paper.experiment_manifest_path or "")
    manifest = _read_json(manifest_path)
    if manifest is None:
        return {"status": "UNAVAILABLE", "manifest_available": False, "processed_sessions": 0,
                "target_sessions": config.forward_paper.target_sessions, "progress": None,
                "last_processed_date": None, "next_required_session": None,
                "next_session_status": "BLOCKED", "block_reasons": [f"Experiment manifest unavailable: {manifest_path.relative_to(root) if manifest_path.is_relative_to(root) else manifest_path}"]}

    processed_dates = manifest.get("processed_sessions") or []
    try:
        processed = int(manifest.get("processed_session_count", len(processed_dates)))
        target = int(manifest.get("target_sessions") or config.forward_paper.target_sessions or 0)
    except (TypeError, ValueError):
        return {"status": "INVALID_MANIFEST", "manifest_available": True, "processed_sessions": None,
                "target_sessions": config.forward_paper.target_sessions, "progress": None,
                "last_processed_date": None, "next_required_session": None,
                "next_session_status": "BLOCKED", "block_reasons": ["Experiment manifest has invalid session counts"]}
    last_date = manifest.get("last_processed_date") or (processed_dates[-1] if processed_dates else None)
    start_value = manifest.get("start_date") or config.forward_paper.experiment_start_date
    end_value = manifest.get("end_date") or config.forward_paper.experiment_end_date
    next_session: date | None = None
    block_reasons: list[str] = []
    next_status = "TARGET REACHED" if target and processed >= target else "NO NEXT SESSION"
    if target and processed < target and (last_date or start_value) and end_value:
        try:
            cursor = date.fromisoformat(last_date) + timedelta(days=1) if last_date else date.fromisoformat(start_value)
            end_date = date.fromisoformat(end_value)
            calendar = NseTradingCalendar(_rooted(root, config.forward_paper.trading_calendar_path))
            while cursor <= end_date:
                decision = calendar.classify(cursor, observed_market_data=False)
                if decision.status != "NON_TRADING_DAY":
                    next_session = cursor
                    break
                cursor += timedelta(days=1)
            if next_session:
                next_text = next_session.isoformat()
                equity_path = _rooted(root, config.forward_paper.daily_data_dir) / f"{next_text}.csv"
                nifty_path = _rooted(root, config.forward_paper.index_data_dir) / "index_NSEI.csv"
                vix_path = _rooted(root, config.forward_paper.index_data_dir) / "index_INDIAVIX.csv"
                for label, path in (("validated equity EOD file", equity_path),):
                    if not path.is_file():
                        block_reasons.append(f"{next_text}: {label} is missing")
                for label, path in (("official NIFTY row", nifty_path), ("official India VIX row", vix_path)):
                    coverage = _csv_date_coverage(path)
                    if not path.is_file() or not coverage.get("available") or _date_missing(path, next_session):
                        block_reasons.append(f"{next_text}: {label} is missing")
                next_status = "BLOCKED / WAITING FOR REQUIRED EOD DATA" if block_reasons else "DATA FILES PRESENT / PAPER PREFLIGHT REQUIRED"
        except (OSError, TypeError, ValueError) as error:
            block_reasons.append(f"Could not determine the next session from the configured calendar: {error}")
            next_status = "BLOCKED"
    elif target and processed < target:
        block_reasons.append("Experiment date range or latest processed date is unavailable")
        next_status = "BLOCKED"

    return {"status": manifest.get("status", "UNKNOWN"), "manifest_available": True,
            "experiment_id": manifest.get("experiment_id"), "provider": manifest.get("provider", config.forward_paper.provider),
            "start_date": start_value, "end_date": end_value, "processed_sessions": processed,
            "processed_session_dates": processed_dates, "target_sessions": target,
            "progress": f"{processed}/{target}" if target else None,
            "last_processed_date": last_date,
            "next_required_session": next_session.isoformat() if next_session else None,
            "next_session_status": next_status, "block_reasons": block_reasons,
            "blocked_sessions": manifest.get("blocked_sessions", []),
            "live_trading_enabled": manifest.get("live_trading_enabled") is True,
            "broker_contacted": manifest.get("broker_contacted") is True,
            "manifest_path": str(manifest_path.relative_to(root)) if manifest_path.is_relative_to(root) else str(manifest_path)}


def _date_missing(path: Path, requested: date) -> bool:
    if not path.is_file():
        return True
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            date_column = next((name for name in (reader.fieldnames or []) if name.strip().casefold() == "date"), None)
            if not date_column:
                return True
            return not any(_parse_market_date(row.get(date_column)) == requested for row in reader)
    except (OSError, UnicodeError, csv.Error):
        return True


def _data_sources_payload(root: Path, config: Any) -> dict[str, Any]:
    output_root = _rooted(root, config.forward_paper.output_root)
    data_manifest = _read_json(output_root / "data_manifest.json") or {}
    experiment = _experiment_payload(root, config)
    equity_source = data_manifest.get("equity_source") or {}
    official = data_manifest.get("official_indexes") or {}
    index_dir = _rooted(root, config.forward_paper.index_data_dir)
    index_rows: dict[str, Any] = {}
    for key, filename in (("NIFTY", "index_NSEI.csv"), ("INDIA_VIX", "index_INDIAVIX.csv")):
        record = _csv_date_coverage(index_dir / filename)
        provenance = official.get(key, {})
        index_rows[key] = {**record, "source": provenance.get("source", "configured official NSE CSV"),
                           "source_url": provenance.get("source_url"),
                           "manifest_sha256": provenance.get("sha256"),
                           "manifest_rows": provenance.get("rows")}
        if experiment.get("next_required_session"):
            required_date = date.fromisoformat(experiment["next_required_session"])
            index_rows[key]["next_required_session"] = required_date.isoformat()
            index_rows[key]["required_session_present"] = not _date_missing(index_dir / filename, required_date)
        if provenance.get("first_date"):
            index_rows[key]["manifest_first_date"] = provenance["first_date"]
        if provenance.get("last_date"):
            index_rows[key]["manifest_last_date"] = provenance["last_date"]
    identity = _identity_coverage(_rooted(root, config.forward_paper.identity_file))
    return {"equity": {"configured_provider": config.forward_paper.provider,
                       "historical_source": equity_source.get("provider"),
                       "path": equity_source.get("path", config.forward_paper.market_data_dir),
                       "first_date": equity_source.get("first_raw_date"),
                       "last_date": equity_source.get("last_raw_date", equity_source.get("as_of_date")),
                       "eligible_symbols": equity_source.get("eligible_symbols"),
                       "accepted_session_rows": equity_source.get("accepted_official_session_rows"),
                       "adjustment_status": equity_source.get("adjustment_status"),
                       "next_date_file_present": bool(experiment.get("next_required_session") and
                           (_rooted(root, config.forward_paper.daily_data_dir) /
                            f"{experiment['next_required_session']}.csv").is_file())},
            "indexes": index_rows,
            "frozen_universe": {**identity, "configured_path": config.forward_paper.identity_file,
                                "expected_daily_symbols": (data_manifest.get("coverage_window") or {}).get("expected_daily_symbols"),
                                "status": "VALIDATED EXPLICIT EQ ROSTER" if identity.get("available") and identity.get("eq_symbols") == identity.get("symbols") and not identity.get("duplicate_symbols") and not identity.get("missing_series_rows") else "CHECK REQUIRED"},
            "coverage_window": data_manifest.get("coverage_window"),
            "data_gaps_policy": data_manifest.get("data_gaps_policy"),
            "next_required_session": experiment.get("next_required_session")}


def _risk_payload(database: Path, config: Any, root: Path) -> dict[str, Any]:
    state: dict[str, Any] = {"available": False}
    if database.is_file():
        try:
            with _connect_read_only(database) as connection:
                connection.row_factory = sqlite3.Row
                snapshot = connection.execute("SELECT * FROM portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1").fetchone()
                open_positions = connection.execute("SELECT COUNT(*) FROM positions WHERE status='OPEN'").fetchone()[0]
                orders = _dict_rows(connection.execute("SELECT status,COUNT(*) AS count FROM orders GROUP BY status ORDER BY status"))
                if snapshot:
                    state = {"available": True, "trading_date": snapshot["trading_date"],
                             "equity": snapshot["equity"], "daily_pnl": snapshot["daily_pnl"],
                             "drawdown": snapshot["drawdown"], "maximum_drawdown": snapshot["maximum_drawdown"],
                             "gross_exposure": snapshot["gross_exposure"], "net_exposure": snapshot["net_exposure"],
                             "open_positions": open_positions, "orders_by_status": orders}
        except sqlite3.Error as error:
            state = {"available": False, "error": str(error)}
    risk_events: list[dict[str, Any]] = []
    events_dir = database.resolve().parent / "events"
    if events_dir.is_dir():
        for event_file in sorted(events_dir.glob("*.jsonl"), reverse=True):
            try:
                for line in event_file.read_text(encoding="utf-8").splitlines()[-250:]:
                    if line.strip():
                        event = json.loads(line)
                        if event.get("event_type") == "RISK_LIMIT_TRIGGERED":
                            risk_events.append(event)
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            if len(risk_events) >= 10:
                break
    return {"configured_limits": config.risk.model_dump(), "observed_state": state,
            "risk_events": list(reversed(risk_events[-10:])),
            "risk_event_status": "TRIGGER RECORDED" if risk_events else "NO RISK TRIGGER EVENT RECORDED",
            "cost_configuration": config.costs.model_dump(),
            "source": "phase3d_personal.yaml and read-only paper ledger"}


def _research_report_catalog(root: Path) -> list[dict[str, Any]]:
    results = root / "results"
    reports: list[dict[str, Any]] = []
    if not results.is_dir():
        return reports
    for directory in sorted((item for item in results.iterdir() if item.is_dir()), reverse=True):
        names = []
        if directory.name.startswith("phase3c_kaggle_"):
            names.append(("Phase 3C Kaggle report", "PHASE3C_KAGGLE_REPORT.md", "phase3c-kaggle"))
        if directory.name.startswith("phase3_"):
            names.append(("Phase 3 walk-forward report", "phase3_validation_report.html", "phase3-validation"))
        if directory.name.startswith("phase3d_forward_"):
            names.append(("Phase 3D experiment report", "REPORT.md", "phase3d-" + directory.name))
        if directory.name.startswith("data_expansion_"):
            names.append(("Historical data expansion report", "DATA_EXPANSION_REPORT.md", "data-expansion-" + directory.name))
        if directory.name[:8].isdigit() and (directory / "report.html").is_file():
            names.append(("Historical backtest report", "report.html", "backtest-" + directory.name))
        for title, filename, report_id in names:
            path = directory / filename
            if path.is_file():
                reports.append({"title": title, "run_id": directory.name, "filename": filename,
                                "path": path.relative_to(root).as_posix(),
                                "href": f"/research-report/{report_id}", "bytes": path.stat().st_size})
    return reports


def _resolve_research_report(root: Path, report_id: str) -> Path | None:
    if report_id == "phase3c-kaggle":
        matches = sorted((root / "results").glob("phase3c_kaggle_*/PHASE3C_KAGGLE_REPORT.md"), reverse=True)
        return matches[0] if matches else None
    if report_id == "phase3-validation":
        matches = sorted((root / "results").glob("phase3_*/phase3_validation_report.html"), reverse=True)
        return matches[0] if matches else None
    if report_id.startswith("phase3d-"):
        folder = report_id[len("phase3d-"):]
        if re.fullmatch(r"phase3d_forward_[A-Za-z0-9_-]+", folder):
            path = root / "results" / folder / "REPORT.md"
            return path if path.is_file() else None
    if report_id.startswith("data-expansion-"):
        folder = report_id[len("data-expansion-"):]
        if re.fullmatch(r"data_expansion_[A-Za-z0-9_-]+", folder):
            path = root / "results" / folder / "DATA_EXPANSION_REPORT.md"
            return path if path.is_file() else None
    if report_id.startswith("backtest-"):
        folder = report_id[len("backtest-"):]
        if re.fullmatch(r"[A-Za-z0-9_-]+", folder):
            path = root / "results" / folder / "report.html"
            return path if path.is_file() and path.resolve().parent == (root / "results" / folder).resolve() else None
    return None


def _research_payload(root: Path, config: Any) -> dict[str, Any]:
    phase3c_manifests = sorted((root / "results").glob("phase3c_kaggle_*/run_manifest.json"), reverse=True)
    phase3c = _read_json(phase3c_manifests[0]) if phase3c_manifests else None
    mc_statuses: Counter[str] = Counter()
    mc_rows = 0
    simulations_run = 0
    if phase3c_manifests:
        mc_path = phase3c_manifests[0].parent / "monte_carlo_results.csv"
        try:
            with mc_path.open("r", newline="", encoding="utf-8-sig") as stream:
                for row in csv.DictReader(stream):
                    mc_rows += 1
                    mc_statuses[str(row.get("status") or "UNKNOWN")] += 1
                    try:
                        simulations_run += int(float(row.get("simulations_run") or 0))
                    except (ValueError, TypeError):
                        continue
        except (OSError, UnicodeError, csv.Error):
            pass
    validation_manifests = sorted((root / "results").glob("phase3_*/phase3_run_manifest.json"), reverse=True)
    validation = _read_json(validation_manifests[0]) if validation_manifests else None
    summary_path = _rooted(root, config.forward_paper.output_root) / "summary.json"
    phase3d_summary = _read_json(summary_path)
    phase3c_view = None
    if phase3c:
        phase3c_view = {"run_id": phase3c.get("run_id"), "status": phase3c.get("status"),
                        "status_reason": phase3c.get("status_reason"), "completed_at": phase3c.get("completed_at"),
                        "provider": phase3c.get("provider"), "dataset_name": phase3c.get("dataset_name"),
                        "date_range": phase3c.get("dataset_date_range"), "symbols": phase3c.get("symbols_used"),
                        "strategies": len(phase3c.get("strategies_run") or []),
                        "walk_forward_windows": phase3c.get("walk_forward_windows"),
                        "monte_carlo": {"status_counts": dict(mc_statuses), "rows": mc_rows,
                                        "simulations_run": simulations_run,
                                        "status": "NOT RECORDED" if not mc_rows else (
                                            "COMPLETED" if mc_statuses and all(k.startswith("COMPUTED") for k in mc_statuses)
                                            else "PARTIAL / INSUFFICIENT TRADES")},
                        "report_href": "/research-report/phase3c-kaggle"}
    validation_view = None
    if validation:
        validation_view = {"status": validation.get("status"), "period": validation.get("period"),
                           "eligible_equity_symbols": validation.get("eligible_equity_symbols"),
                           "walk_forward_status": validation.get("walk_forward_status"),
                           "walk_forward_windows": validation.get("walk_forward_windows"),
                           "walk_forward_reason": validation.get("walk_forward_reason"),
                           "monte_carlo_status": "NOT RUN" if "not run" in str(validation.get("monte_carlo", "")).casefold() else validation.get("monte_carlo", "UNKNOWN"),
                           "monte_carlo_reason": validation.get("monte_carlo"),
                           "report_href": "/research-report/phase3-validation"}
    summary_view = None
    if phase3d_summary:
        summary_view = {key: phase3d_summary.get(key) for key in (
            "status", "sessions_processed", "signals", "signals_rejected", "orders_created", "fills",
            "trades", "gross_pnl", "net_pnl", "total_costs", "maximum_drawdown", "last_processed_date")}
    return {"phase3c": phase3c_view, "phase3_validation": validation_view,
            "phase3d": summary_view, "reports": _research_report_catalog(root)}


def _system_payload(database: Path, root: Path, config: Any) -> dict[str, Any]:
    experiment = _experiment_payload(root, config)
    live_config = bool(config.live_trading.enabled)
    live_manifest = experiment.get("live_trading_enabled") is True
    return {"status": "ok" if database.is_file() else "degraded",
            "system_status": experiment.get("next_session_status", "UNKNOWN"),
            "mode": "PAPER / RESEARCH ONLY" if config.paper.enabled and config.forward_paper.enabled else "RESEARCH",
            "paper_mode_enabled": bool(config.paper.enabled and config.forward_paper.enabled),
            "live_trading_enabled": live_config or live_manifest,
            "broker_connected": False, "broker_contacted": experiment.get("broker_contacted", False),
            "broker_status": "NOT CONNECTED — dashboard has no broker client",
            "data_provider": config.forward_paper.provider, "dashboard_read_only": True,
            "ledger_available": database.is_file(),
            "ledger_path": str(database.relative_to(root)) if database.is_relative_to(root) else str(database),
            "experiment": experiment}


def dashboard_payload(database: str | Path, endpoint: str, query: dict[str, list[str]] | None = None,
                      *, project_root: str | Path | None = None, config_path: str | Path | None = None) -> Any:
    """Read dashboard views using SQLite read-only mode; there are no trade APIs."""
    path = Path(database)
    query = query or {}
    root = Path(project_root).resolve() if project_root else _PROJECT_ROOT
    if endpoint == "/api/dashboard/phase4c-paper":
        return _phase4c_paper_payload(root)
    if endpoint == "/api/dashboard/phase4d-session":
        return _phase4d_session_payload(root)
    if endpoint == "/api/dashboard/phase4g-yf":
        return _phase4g_yf_payload(root)
    config = _load_dashboard_config(root, config_path)
    if endpoint == "/health":
        if config is None:
            return {"status": "degraded", "reason": "dashboard configuration is unavailable",
                    "mode": "unknown", "live_trading_enabled": None, "broker_connected": False,
                    "dashboard_read_only": True}
        system = _system_payload(path, root, config)
        realtime = _realtime_payload(root, config)
        health = {key: system[key] for key in ("status", "mode", "paper_mode_enabled", "live_trading_enabled",
                                                "broker_connected", "broker_contacted", "dashboard_read_only",
                                                "system_status", "data_provider", "ledger_available", "experiment")}
        health["realtime_feed_status"] = realtime["status_label"]
        health["realtime_feed_code"] = realtime["status_code"]
        return health
    if endpoint in {"/api/dashboard/system", "/api/dashboard/experiment", "/api/dashboard/data-sources",
                    "/api/dashboard/risk", "/api/dashboard/research", "/api/dashboard/realtime",
                    "/api/dashboard/intraday-scanner"}:
        if config is None:
            return {"error": "Dashboard configuration is unavailable"}
        if endpoint == "/api/dashboard/system":
            return _system_payload(path, root, config)
        if endpoint == "/api/dashboard/experiment":
            return _experiment_payload(root, config)
        if endpoint == "/api/dashboard/data-sources":
            return _data_sources_payload(root, config)
        if endpoint == "/api/dashboard/risk":
            return _risk_payload(path, config, root)
        if endpoint == "/api/dashboard/realtime":
            return _realtime_payload(root, config)
        if endpoint == "/api/dashboard/intraday-scanner":
            return _intraday_scanner_payload(root)
        return _research_payload(root, config)
    if not path.is_file():
        return _no_data()
    try:
        connection = _connect_read_only(path)
    except sqlite3.Error:
        return _no_data()
    with connection:
        connection.row_factory = sqlite3.Row
        latest_snapshot = connection.execute("SELECT * FROM portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1").fetchone()
        latest_health = connection.execute("SELECT payload_json FROM health_snapshots ORDER BY created_at DESC LIMIT 1").fetchone()
        latest_journal = connection.execute("SELECT payload_json FROM daily_journal ORDER BY created_at DESC LIMIT 1").fetchone()

        if endpoint == "/api/dashboard/overview":
            snapshot = dict(latest_snapshot) if latest_snapshot else {}
            today = snapshot.get("trading_date")
            signals = _dict_rows(connection.execute(
                "SELECT signal_id,trading_date,timestamp,symbol,strategy,direction,signal_price,stop_loss,target,risk_amount,status,rejection_reason FROM signals WHERE trading_date=? ORDER BY timestamp,symbol,strategy",
                (today or "",),
            ))
            positions = _dict_rows(connection.execute("SELECT * FROM positions WHERE status='OPEN' ORDER BY symbol"))
            health = json.loads(latest_health[0]) if latest_health else {}
            journal = json.loads(latest_journal[0]) if latest_journal else {}
            return {
                "scope": "FORWARD PAPER", "status": health.get("system_data_status", "NO_FORWARD_DATA"),
                "starting_capital": snapshot.get("starting_capital"), "current_equity": snapshot.get("equity"),
                "cash": snapshot.get("cash"), "reserved_cash": snapshot.get("reserved_cash"),
                "market_value": snapshot.get("market_value"), "gross_exposure": snapshot.get("gross_exposure"),
                "net_exposure": snapshot.get("net_exposure"),
                "today_pnl": snapshot.get("daily_pnl"), "cumulative_pnl": (snapshot.get("equity", 0) - snapshot.get("starting_capital", 0)) if snapshot else None,
                "cumulative_return": snapshot.get("cumulative_return"),
                "drawdown": snapshot.get("drawdown"), "maximum_drawdown": snapshot.get("maximum_drawdown"),
                "open_positions": positions, "today_signals": signals,
                "market_regime": journal.get("market_regime", "UNKNOWN"),
                "data_freshness": health.get("system_data_status", "UNKNOWN"),
                "last_successful_run": journal.get("created_at"),
                "last_market_date": journal.get("date"),
                "reconciliation_status": journal.get("reconciliation_status", "UNKNOWN"),
                "system_health": health, "live_trading_enabled": False,
            }

        if endpoint == "/api/dashboard/signals":
            filters, values = [], []
            mapping = {"date": "trading_date", "strategy": "strategy", "symbol": "symbol"}
            for key, column in mapping.items():
                value = (query.get(key) or [""])[0].strip()
                if value:
                    filters.append(f"{column}=?")
                    values.append(value.upper() if key == "symbol" else value)
            side = (query.get("side") or [""])[0].upper()
            if side in {"BUY", "SELL"}:
                filters.append("direction=?")
                values.append(side)
            status = (query.get("status") or [""])[0].lower()
            if status == "accepted":
                filters.append("accepted=1")
            elif status == "rejected":
                filters.append("accepted=0")
            sql = "SELECT * FROM signals" + (" WHERE " + " AND ".join(filters) if filters else "") + " ORDER BY trading_date DESC,timestamp DESC,symbol,strategy LIMIT 1000"
            rows = _dict_rows(connection.execute(sql, values))
            for row in rows:
                row["features"] = json.loads(row.pop("features_json"))
                row.pop("payload_json", None)
            return {"scope": "FORWARD PAPER", "signals": rows}

        if endpoint == "/api/dashboard/positions":
            positions = _dict_rows(connection.execute("SELECT * FROM positions WHERE status='OPEN' ORDER BY symbol"))
            closed = _dict_rows(connection.execute("SELECT * FROM positions WHERE status='CLOSED' ORDER BY exit_date DESC LIMIT 1000"))
            return {"scope": "FORWARD PAPER", "open": positions, "closed": closed}

        if endpoint == "/api/dashboard/performance":
            snapshots = _dict_rows(connection.execute("SELECT trading_date,equity,daily_pnl,daily_return,drawdown,gross_exposure FROM portfolio_snapshots ORDER BY snapshot_id"))
            trades = _dict_rows(connection.execute("SELECT strategy,exit_date,realized_pnl FROM positions WHERE status='CLOSED' ORDER BY exit_date,position_id"))
            monthly: dict[str, float] = defaultdict(float)
            for trade in trades:
                monthly[str(trade["exit_date"] or "")[:7]] += float(trade["realized_pnl"] or 0)
            pnls = [float(trade["realized_pnl"] or 0) for trade in trades]
            wins = [value for value in pnls if value > 0]
            losses = [value for value in pnls if value < 0]
            gross_loss = abs(sum(losses))
            return {
                "scope": "FORWARD PAPER", "backtest_included": False, "equity_curve": snapshots,
                "daily_pnl": [{"date": row["trading_date"], "pnl": row["daily_pnl"]} for row in snapshots],
                "monthly_pnl": [{"month": key, "pnl": monthly[key]} for key in sorted(monthly)],
                "metrics": {
                    "number_of_trades": len(trades), "win_rate": len(wins) / len(pnls) if pnls else None,
                    "profit_factor": sum(wins) / gross_loss if gross_loss else None,
                    "expectancy": float(np.mean(pnls)) if pnls else 0.0,
                    "maximum_drawdown": max((float(row["drawdown"] or 0) for row in snapshots), default=0.0),
                    "average_win": float(np.mean(wins)) if wins else 0.0,
                    "average_loss": float(np.mean(losses)) if losses else 0.0,
                },
            }

        if endpoint == "/api/dashboard/strategies":
            capital_row = connection.execute("SELECT value FROM settings WHERE key='starting_capital'").fetchone()
            strategy_capital = float(capital_row[0]) if capital_row else 1.0
            payload = {"scope": "FORWARD PAPER", "strategy_accounts_are_separate": True,
                       "strategies": _strategy_metrics(connection, strategy_capital)}
            if config is not None:
                payload["configured_strategies"] = list(config.strategies.enabled)
                payload["strategy_parameters"] = config.strategies.model_dump()
            return payload

        if endpoint == "/api/dashboard/activity":
            signals = _dict_rows(connection.execute(
                "SELECT signal_id,trading_date,timestamp,symbol,strategy,direction,signal_price,status,rejection_reason FROM signals WHERE strategy!='DATA_HEALTH' ORDER BY timestamp DESC LIMIT 10"
            ))
            orders = _dict_rows(connection.execute(
                "SELECT o.order_id,o.signal_id,o.created_at,o.filled_at,o.trading_date,o.symbol,o.side,o.quantity,"
                "o.intended_price,o.requested_price,o.simulated_fill_price,o.costs,o.status,o.strategy,o.order_kind,"
                "s.stop_loss,s.target FROM orders AS o LEFT JOIN signals AS s ON s.signal_id=o.signal_id "
                "ORDER BY o.created_at DESC LIMIT 10"
            ))
            trades = _dict_rows(connection.execute(
                "SELECT position_id,signal_id,symbol,strategy,quantity,entry_price,entry_date,exit_price,exit_date,"
                "realized_pnl,entry_costs,total_costs,holding_sessions FROM positions WHERE status='CLOSED' "
                "ORDER BY exit_date DESC,position_id DESC LIMIT 10"
            ))
            event_directory = path.resolve().parent / "events"
            events = []
            for event_file in sorted(event_directory.glob("*.jsonl"), reverse=True):
                try:
                    lines = event_file.read_text(encoding="utf-8").splitlines()[-250:]
                    events.extend(json.loads(line) for line in lines if line.strip())
                except (OSError, json.JSONDecodeError):
                    continue
                if len(events) >= 1000:
                    break
            risks = [event for event in events if event.get("event_type") == "RISK_LIMIT_TRIGGERED"]
            return {"scope": "FORWARD PAPER", "signals": signals, "orders": orders,
                    "closed_trades": trades, "risk_events": list(reversed(risks[-10:]))}

        if endpoint == "/api/dashboard/data-health":
            if not latest_health:
                return _no_data()
            health = json.loads(latest_health[0])
            journal = json.loads(latest_journal[0]) if latest_journal else {}
            health["last_successful_run"] = journal.get("created_at")
            health["last_market_date"] = journal.get("date")
            health["reconciliation_status"] = journal.get("reconciliation_status", "UNKNOWN")
            return health

        if endpoint == "/api/dashboard/daily-report":
            requested_date = (query.get("date") or [""])[0]
            if requested_date and _parse_market_date(requested_date) and requested_date == _parse_market_date(requested_date).isoformat():
                report_dir = Path(database).resolve().parents[1] / "reports" / requested_date
                # Default forward path is sibling to the configured portfolio DB.
                paths = sorted(report_dir.glob("*.json")) if report_dir.is_dir() else []
                if paths:
                    return json.loads(paths[-1].read_text(encoding="utf-8"))
            if latest_journal:
                return json.loads(latest_journal[0])
            return _no_data()
    return {"error": "Not found"}


def make_handler(database: Path, static_dir: Path = _STATIC, *, project_root: Path | None = None,
                 config_path: Path | None = None):
    root = project_root.resolve() if project_root else _PROJECT_ROOT

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._handle_request(head_only=False)

        def do_HEAD(self) -> None:
            self._handle_request(head_only=True)

        def _handle_request(self, *, head_only: bool) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                payload = dashboard_payload(database, "/health", project_root=root, config_path=config_path)
                self._send(HTTPStatus.OK, json.dumps(payload, default=_json_default, allow_nan=False).encode(),
                           "application/json; charset=utf-8", head_only=head_only)
                return
            if parsed.path.startswith("/api/"):
                payload = dashboard_payload(database, parsed.path, parse_qs(parsed.query),
                                            project_root=root, config_path=config_path)
                self._send(HTTPStatus.OK if "error" not in payload else HTTPStatus.NOT_FOUND,
                           json.dumps(payload, default=_json_default, allow_nan=False).encode(),
                           "application/json; charset=utf-8", head_only=head_only)
                return
            if parsed.path.startswith("/research-report/"):
                report_id = parsed.path.removeprefix("/research-report/")
                target = _resolve_research_report(root, report_id)
                if target is None:
                    self._send(HTTPStatus.NOT_FOUND, b"Research report not found", "text/plain; charset=utf-8", head_only=head_only)
                    return
                content_type = "text/html; charset=utf-8" if target.suffix.casefold() == ".html" else "text/plain; charset=utf-8"
                self._send(HTTPStatus.OK, target.read_bytes(), content_type, head_only=head_only)
                return
            allowed = {"/": "index.html", "/index.html": "index.html", "/app.js": "app.js", "/styles.css": "styles.css"}
            name = allowed.get(parsed.path)
            if not name:
                self._send(HTTPStatus.NOT_FOUND, b"Not found", "text/plain; charset=utf-8", head_only=head_only)
                return
            target = static_dir / name
            if not target.is_file():
                self._send(HTTPStatus.SERVICE_UNAVAILABLE, b"Dashboard frontend files are missing", "text/plain; charset=utf-8", head_only=head_only)
                return
            content_type = "text/javascript; charset=utf-8" if name.endswith(".js") else "text/css; charset=utf-8" if name.endswith(".css") else "text/html; charset=utf-8"
            self._send(HTTPStatus.OK, target.read_bytes(), content_type, head_only=head_only)

        def do_POST(self) -> None:
            self._send(HTTPStatus.METHOD_NOT_ALLOWED, b"Read-only dashboard", "text/plain; charset=utf-8")

        def do_PUT(self) -> None:
            self._send(HTTPStatus.METHOD_NOT_ALLOWED, b"Read-only dashboard", "text/plain; charset=utf-8")

        def do_DELETE(self) -> None:
            self._send(HTTPStatus.METHOD_NOT_ALLOWED, b"Read-only dashboard", "text/plain; charset=utf-8")

        def do_PATCH(self) -> None:
            self._send(HTTPStatus.METHOD_NOT_ALLOWED, b"Read-only dashboard", "text/plain; charset=utf-8")

        def _send(self, status: HTTPStatus, content: bytes, content_type: str, *, head_only: bool = False) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            if not head_only:
                self.wfile.write(content)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve read-only forward paper dashboard on localhost")
    parser.add_argument("--config", default="config/phase3d_personal.yaml")
    parser.add_argument("--database")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("dashboard is local-only; bind to 127.0.0.1 or localhost")
    root = Path(__file__).resolve().parents[2]
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = root / config_path
    config = AppConfig.load(config_path)
    database = Path(args.database or config.forward_paper.database)
    if not database.is_absolute():
        database = root / database
    server = _LocalDashboardServer((args.host, args.port), make_handler(
        database, project_root=root, config_path=config_path,
    ))
    print(f"Read-only FORWARD PAPER dashboard at http://127.0.0.1:{server.server_port}/")
    print("REALTIME PAPER DATA MODE: " + ("CONFIGURED; no feed worker is started by the dashboard" if config.realtime.enabled else "DISABLED"))
    print("LIVE TRADING: DISABLED")
    print("BROKER EXECUTION: DISABLED")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
