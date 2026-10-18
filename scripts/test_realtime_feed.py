#!/usr/bin/env python3
"""One-shot realtime data connectivity/validation check; never trades."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nse_research.config import AppConfig
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.realtime.engine import (
    FeedConfigurationError,
    IntradayCandleBuilder,
    RealtimeFeedMonitor,
    TickRecorder,
    configured_provider,
    load_eq_universe,
)

_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")


def _rooted(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Test configured realtime market data without trading")
    parser.add_argument("--config", default="config/phase3d_personal.yaml")
    parser.add_argument("--symbols", help="comma-separated subset of the configured/frozen EQ universe")
    parser.add_argument("--max-symbols", type=int, default=5)
    args = parser.parse_args(argv)
    if not 1 <= args.max_symbols <= 25:
        parser.error("--max-symbols must be between 1 and 25")
    try:
        config = AppConfig.load(_rooted(args.config))
        if config.realtime.symbols:
            universe = list(config.realtime.symbols)
        else:
            universe = load_eq_universe(_rooted(config.universe.symbols_file))
    except Exception as error:
        print(json.dumps({"status": "REALTIME_FEED_CONFIGURATION_INVALID",
                          "reason": f"{type(error).__name__}: {error}",
                          "market_data_request_made": False,
                          "signals_generated": False, "orders_created": False,
                          "ledger_mutated": False}, sort_keys=True))
        return 2

    if args.symbols:
        requested = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
        if len(set(requested)) != len(requested) or any(not _SYMBOL_RE.fullmatch(item) for item in requested):
            print(json.dumps({"status": "REALTIME_FEED_CONFIGURATION_INVALID",
                              "reason": "requested symbols are malformed or duplicated",
                              "market_data_request_made": False}, sort_keys=True))
            return 2
        unsupported = sorted(set(requested) - set(universe))
        if unsupported:
            print(json.dumps({"status": "REALTIME_SYMBOLS_UNSUPPORTED",
                              "invalid_symbols": unsupported,
                              "market_data_request_made": False}, sort_keys=True))
            return 2
    else:
        requested = universe[: args.max_symbols]
    requested = requested[: args.max_symbols]

    try:
        provider = configured_provider(config.realtime)
    except FeedConfigurationError as error:
        status = ("REALTIME_FEED_NOT_CONFIGURED" if error.code == "DATA_SOURCE_NOT_CONFIGURED"
                  else "DATA_SOURCE_NOT_SUITABLE")
        print(json.dumps({"status": status, "failure_code": error.code,
                          "reason": str(error), "provider": config.realtime.provider or None,
                          "requested_symbols": requested,
                          "market_data_request_made": False,
                          "signals_generated": False, "orders_created": False,
                          "broker_request_made": False, "ledger_mutated": False}, sort_keys=True))
        return 2 if status == "REALTIME_FEED_NOT_CONFIGURED" else 3

    calendar = NseTradingCalendar(_rooted(config.forward_paper.trading_calendar_path))
    builder = IntradayCandleBuilder(
        config.realtime.candle_intervals,
        calendar=calendar,
        timezone_name=config.project.timezone,
        preopen_start=time.fromisoformat(config.realtime.preopen_start),
        market_open=time.fromisoformat(config.realtime.market_open),
        market_close=time.fromisoformat(config.realtime.market_close),
    )
    recorder = TickRecorder(_rooted(config.realtime.recording_dir)) if config.realtime.recording else None
    monitor = RealtimeFeedMonitor(
        provider,
        requested_symbols=requested,
        settings=config.realtime,
        candle_builder=builder,
        health_snapshot_path=_rooted(config.realtime.health_snapshot_path),
        recorder=recorder,
    )
    health = monitor.poll_once()
    accepted = int(health.get("accepted_quote_count", 0))
    status = "REALTIME_FEED_CONNECTED" if accepted else "REALTIME_DATA_NOT_VALIDATED"
    payload: dict[str, Any] = {
        "status": status,
        "provider": health.get("provider"),
        "requested_symbols": health.get("requested_symbol_list", []),
        "received_symbols": health.get("received_symbols", []),
        "accepted_symbols": health.get("accepted_symbols", []),
        "missing_symbols": health.get("missing_symbols", []),
        "invalid_symbols": health.get("invalid_symbols", []),
        "stale_symbols": health.get("stale_symbols", []),
        "received_quote_count": health.get("received_quote_count", 0),
        "accepted_quote_count": accepted,
        "rejected_quote_count": health.get("rejected_quote_count", 0),
        "last_quote_timestamp": health.get("last_quote_timestamp"),
        "quotes": health.get("quotes", []),
        "candles": health.get("candles", []),
        "market_status": health.get("market_status"),
        "error_count": health.get("error_count", 0),
        "retry_count": health.get("retry_count", 0),
        "market_data_request_made": True,
        "signals_generated": False,
        "orders_created": False,
        "broker_request_made": False,
        "ledger_mutated": False,
    }
    print(json.dumps(payload, sort_keys=True, allow_nan=False))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
