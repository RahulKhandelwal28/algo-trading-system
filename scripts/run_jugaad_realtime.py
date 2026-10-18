#!/usr/bin/env python3
"""Create paper-only NSE EQ recommendations from a selected realtime feed."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from datetime import datetime, time as clock_time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nse_research.config import AppConfig  # noqa: E402
from nse_research.forward.calendar import NseTradingCalendar  # noqa: E402
from nse_research.realtime.engine import (  # noqa: E402
    IntradayCandleBuilder,
    RealtimeFeedMonitor,
    TickRecorder,
    configured_provider,
    load_eq_universe,
)
from nse_research.realtime.jugaad import MAX_SYMBOLS_PER_POLL  # noqa: E402
from nse_research.realtime.paper_recommendations import (  # noqa: E402
    JugaadPaperRecommendationSession,
    write_recommendation_snapshot,
)

MIN_POLL_INTERVAL_SECONDS = 6


def _path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def _emit(payload: dict) -> None:
    print(json.dumps(payload, sort_keys=True, allow_nan=False), flush=True)


def _session(calendar: NseTradingCalendar, config: AppConfig) -> tuple[bool, str]:
    settings = config.realtime
    result = calendar.intraday_status(
        datetime.now(ZoneInfo(config.project.timezone)),
        timezone_name=config.project.timezone,
        preopen_start=clock_time.fromisoformat(settings.preopen_start),
        market_open=clock_time.fromisoformat(settings.market_open),
        market_close=clock_time.fromisoformat(settings.market_close),
    )
    return result.status == "OPEN", result.reason


def _load_provider_dependency(provider: str, symbols: list[str]) -> str | None:
    """Check the optional package and its provider class without making a request."""
    if provider == "jugaad_data":
        if importlib.util.find_spec("jugaad_data") is None:
            return "jugaad-data is not installed; run .venv/bin/pip install -r requirements-realtime.txt"
        try:
            from jugaad_data.nse import NSELive  # noqa: F401
        except Exception as error:
            return f"jugaad-data cannot load NSELive ({type(error).__name__}): {error}"
        return None
    if provider == "groww_feed":
        if importlib.util.find_spec("growwapi") is None:
            return "growwapi is not installed; run .venv/bin/pip install -r requirements-realtime.txt"
        instruments_csv = os.environ.get("GROWW_INSTRUMENTS_CSV", "").strip()
        if not instruments_csv:
            return "GROWW_INSTRUMENTS_CSV is not configured"
        try:
            from nse_research.realtime.groww import validate_groww_configuration

            validate_groww_configuration(instruments_csv=instruments_csv, symbols=symbols)
        except Exception as error:
            return str(error)
        return None
    return f"unsupported realtime provider: {provider}"


def _recommendation_path(value: str | None, config: AppConfig) -> Path:
    if value:
        return _path(value)
    today = datetime.now(ZoneInfo(config.project.timezone)).date().isoformat()
    prefix = "groww_paper" if config.realtime.provider.strip().lower() == "groww_feed" else "jugaad_paper"
    return ROOT / "results" / prefix / today / "paper_recommendations.json"


def _preflight(config: AppConfig, universe: list[str], symbols: list[str], output_path: Path,
               dependency_error: str | None) -> dict:
    settings = config.realtime
    reasons: list[str] = []
    if not settings.enabled:
        reasons.append("config realtime.enabled must be true")
    provider = settings.provider.strip().lower()
    if provider not in {"jugaad_data", "groww_feed"}:
        reasons.append("realtime.provider must be jugaad_data or groww_feed")
    if config.live_trading.enabled:
        reasons.append("live_trading.enabled must remain false")
    if len(symbols) > MAX_SYMBOLS_PER_POLL:
        reasons.append(f"paper recommendation session supports at most {MAX_SYMBOLS_PER_POLL} symbols")
    if not symbols:
        reasons.append("at least one configured or selected symbol is required")
    if len(set(symbols)) != len(symbols):
        reasons.append("symbols must be unique")
    if sorted(set(symbols) - set(universe)):
        reasons.append("all requested symbols must exist in the frozen EQ universe")
    if not set(settings.candle_intervals).issubset({"1m", "5m"}):
        reasons.append("paper scanner supports only 1m and 5m intervals")
    if dependency_error:
        reasons.append(dependency_error)
    output_parent = output_path.parent
    existing_parent = output_parent
    while not existing_parent.exists() and existing_parent != existing_parent.parent:
        existing_parent = existing_parent.parent
    if not existing_parent.is_dir() or not os.access(existing_parent, os.W_OK):
        reasons.append(f"recommendation output directory is not writable: {output_parent}")
    calendar = NseTradingCalendar(_path(config.forward_paper.trading_calendar_path))
    open_now, market_reason = _session(calendar, config)
    return {
        "status": "PAPER_PREFLIGHT_BLOCKED" if reasons else "PAPER_PREFLIGHT_READY",
        "reasons": reasons,
        "provider": settings.provider,
        "configured_symbols": symbols,
        "frozen_universe_size": len(universe),
        "poll_interval_minimum_seconds": MIN_POLL_INTERVAL_SECONDS,
        "max_quote_age_seconds": settings.max_quote_age_seconds,
        "candle_intervals": settings.candle_intervals,
        "market_session_open_now": open_now,
        "market_session_status": "OPEN" if open_now else "CLOSED",
        "market_session_reason": market_reason,
        "recommendation_output": str(output_path),
        "recommendations_tested": False,
        "market_data_request_made": False,
        "paper_mode": True,
        "live_trading_enabled": False,
        "groww_market_data_session_required": provider == "groww_feed",
        "broker_order_api_called": False,
        "broker_orders_enabled": False,
        "orders_created": False,
        "ledger_mutated": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/phase3d_personal.yaml")
    parser.add_argument("--provider", choices=("jugaad_data", "groww_feed"),
                        help="override the configured read-only feed for this run")
    parser.add_argument("--symbols", help="comma-separated subset of the configured EQ universe")
    parser.add_argument("--interval-seconds", type=float, default=15)
    parser.add_argument("--paper-capital", type=float,
                        help="optional capital used only to estimate paper share quantity; never used for orders")
    parser.add_argument("--max-polls", type=int, help="stop after this many polls")
    parser.add_argument("--preflight", action="store_true",
                        help="validate local paper-session configuration without requesting data")
    parser.add_argument("--recommendations-out",
                        help="recommendation JSON snapshot (default: results/jugaad_paper/<date>/paper_recommendations.json)")
    args = parser.parse_args(argv)
    if args.interval_seconds < MIN_POLL_INTERVAL_SECONDS:
        parser.error("--interval-seconds must be at least 6")
    if args.max_polls is not None and args.max_polls < 1:
        parser.error("--max-polls must be positive")
    if args.paper_capital is not None and args.paper_capital <= 0:
        parser.error("--paper-capital must be positive")

    try:
        config = AppConfig.load(_path(args.config))
        settings = config.realtime
        if args.provider:
            settings = settings.model_copy(update={"provider": args.provider})
            config = config.model_copy(update={"realtime": settings})
        if not settings.enabled:
            raise ValueError("enable realtime in the config")
        universe = load_eq_universe(_path(config.universe.symbols_file))
        symbols = (
            [part.strip().upper() for part in args.symbols.split(",") if part.strip()]
            if args.symbols else list(settings.symbols or universe[:MAX_SYMBOLS_PER_POLL])
        )
        if not symbols or len(symbols) > MAX_SYMBOLS_PER_POLL:
            raise ValueError(f"choose between one and {MAX_SYMBOLS_PER_POLL} symbols")
        if len(set(symbols)) != len(symbols):
            raise ValueError("symbols must be unique")
        unsupported = sorted(set(symbols) - set(universe))
        if unsupported:
            raise ValueError(f"symbols are not in the frozen EQ universe: {', '.join(unsupported)}")
        settings = settings.model_copy(update={"symbols": symbols})
        config = config.model_copy(update={"realtime": settings})
        dependency_error = _load_provider_dependency(settings.provider.strip().lower(), symbols)
        calendar = NseTradingCalendar(_path(config.forward_paper.trading_calendar_path))
        recommendation_path = _recommendation_path(args.recommendations_out, config)
    except Exception as error:
        _emit({"status": "REALTIME_CONFIGURATION_ERROR", "reason": str(error),
               "market_data_request_made": False})
        return 2

    if args.preflight:
        payload = _preflight(config, universe, symbols, recommendation_path, dependency_error)
        _emit(payload)
        return 0 if not payload["reasons"] else 2

    if dependency_error:
        _emit({"status": "REALTIME_CONFIGURATION_ERROR", "reason": dependency_error,
               "market_data_request_made": False})
        return 2
    if config.live_trading.enabled:
        _emit({"status": "PAPER_SAFETY_BLOCKED", "reason": "live_trading.enabled must remain false",
               "market_data_request_made": False, "orders_created": False, "broker_request_made": False})
        return 2

    open_now, reason = _session(calendar, config)
    if not open_now:
        _emit({"status": "NSE_SESSION_CLOSED", "reason": reason,
               "market_data_request_made": False})
        return 3

    try:
        provider = configured_provider(settings)
    except Exception as error:
        _emit({"status": "REALTIME_PROVIDER_ERROR", "reason": str(error),
               "market_data_request_made": False})
        return 2

    builder = IntradayCandleBuilder(
        settings.candle_intervals,
        calendar=calendar,
        timezone_name=config.project.timezone,
        preopen_start=clock_time.fromisoformat(settings.preopen_start),
        market_open=clock_time.fromisoformat(settings.market_open),
        market_close=clock_time.fromisoformat(settings.market_close),
    )
    recorder = TickRecorder(_path(settings.recording_dir)) if settings.recording else None
    monitor = RealtimeFeedMonitor(
        provider,
        requested_symbols=symbols,
        settings=settings,
        candle_builder=builder,
        health_snapshot_path=_path(settings.health_snapshot_path),
        recorder=recorder,
    )
    try:
        source = "groww_live_feed" if settings.provider.strip().lower() == "groww_feed" else "jugaad_data_nse_live"
        recommendation_session = JugaadPaperRecommendationSession(
            universe_symbols=universe,
            requested_symbols=symbols,
            timeframes=settings.candle_intervals,
            calendar=calendar,
            strategy_settings=None,
            source=source,
            paper_capital=args.paper_capital,
            risk_per_trade_fraction=config.risk.risk_per_trade,
            max_single_position_fraction=config.risk.max_single_position_fraction,
        )
    except Exception as error:
        close_provider = getattr(provider, "close", None)
        if callable(close_provider):
            try:
                close_provider()
            except Exception:
                pass
        _emit({"status": "PAPER_SCANNER_CONFIGURATION_ERROR", "reason": str(error),
               "market_data_request_made": False, "orders_created": False, "broker_request_made": False})
        return 2

    _emit({
        "status": "PAPER_SESSION_STARTED",
        "provider": settings.provider,
        "symbols": symbols,
        "poll_interval_seconds": args.interval_seconds,
        "recommendation_output": str(recommendation_path),
        "live_trading_enabled": False,
        "broker_request_made": False,
        "orders_created": False,
        "ledger_mutated": False,
    })

    def publish_recommendations(now: datetime, fresh_symbols: list[str], health: dict) -> dict:
        recommendation_session.ingest_closed_candles(builder.candles(status="CLOSED"))
        latest_quotes = {
            str(item.get("symbol", "")).upper(): item
            for item in health.get("quotes", [])
            if isinstance(item, dict) and item.get("freshness_status") == "FRESH"
        }
        snapshot = recommendation_session.snapshot(
            now=now, fresh_symbols=fresh_symbols, latest_quotes=latest_quotes,
        )
        snapshot["feed_status"] = health.get("feed_status")
        snapshot["feed_missing_symbols"] = health.get("missing_symbols", [])
        snapshot["feed_invalid_symbols"] = health.get("invalid_symbols", [])
        snapshot["feed_stale_symbols"] = health.get("stale_symbols", [])
        snapshot["sampled_bars_caveat"] = (
            "Bars aggregate periodic NSE LTP snapshots. They are approximate OHLC, "
            "not exchange-native candles; cumulative quote volume is not converted to bar volume."
        )
        write_recommendation_snapshot(recommendation_path, snapshot)
        return snapshot

    polls = 0
    any_accepted = False
    try:
        while args.max_polls is None or polls < args.max_polls:
            open_now, reason = _session(calendar, config)
            if not open_now:
                now = datetime.now(timezone.utc)
                builder.advance(now)
                health = monitor.health(now=now)
                paper_snapshot = publish_recommendations(now, [], health)
                _emit({"status": "NSE_SESSION_CLOSED", "reason": reason,
                       "polls_completed": polls, "market_data_request_made": bool(polls),
                       "paper_recommendations": paper_snapshot,
                       "orders_created": False, "broker_request_made": False, "ledger_mutated": False})
                break
            started = time.monotonic()
            health = monitor.poll_once()
            polls += 1
            accepted = health.get("accepted_symbols", [])
            any_accepted = any_accepted or bool(accepted)
            paper_snapshot = publish_recommendations(datetime.now(timezone.utc), accepted, health)
            _emit({
                "status": "REALTIME_QUOTES_ACCEPTED" if accepted else "REALTIME_QUOTES_NOT_VALIDATED",
                "poll": polls,
                "accepted_symbols": accepted,
                "missing_symbols": health.get("missing_symbols", []),
                "invalid_symbols": health.get("invalid_symbols", []),
                "stale_symbols": health.get("stale_symbols", []),
                "quotes": health.get("quotes", []),
                "candles": health.get("candles", []),
                "paper_recommendations": paper_snapshot,
                "market_data_request_made": True,
                "signals_generated": bool(paper_snapshot["recommendation_count"]),
                "orders_created": False,
                "broker_request_made": False,
                "ledger_mutated": False,
            })
            if args.max_polls is None or polls < args.max_polls:
                time.sleep(max(0, args.interval_seconds - (time.monotonic() - started)))
    except KeyboardInterrupt:
        now = datetime.now(timezone.utc)
        health = monitor.health(now=now)
        paper_snapshot = publish_recommendations(now, health.get("accepted_symbols", []), health)
        _emit({"status": "REALTIME_PAPER_SESSION_STOPPED", "polls_completed": polls,
               "any_quotes_accepted": any_accepted,
               "paper_recommendations": paper_snapshot,
               "orders_created": False, "broker_request_made": False, "ledger_mutated": False})
    finally:
        close_provider = getattr(provider, "close", None)
        if callable(close_provider):
            try:
                close_provider()
            except Exception as error:
                _emit({"status": "REALTIME_PROVIDER_CLOSE_WARNING",
                       "reason": type(error).__name__, "orders_created": False})
    return 0 if any_accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
