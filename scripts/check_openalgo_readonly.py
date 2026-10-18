#!/usr/bin/env python3
"""Read-only OpenAlgo feed smoke test; never calls an order or account API."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_research.data.openalgo_readonly import (  # noqa: E402
    OpenAlgoDataError,
    OpenAlgoReadOnlyClient,
    OpenAlgoSettings,
)


DEFAULT_SMOKE_SYMBOLS = ("RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Validate read-only OpenAlgo NSE/EQ one-minute bars for at most five smoke symbols."
    )
    result.add_argument("--date", required=True, help="NSE session date (YYYY-MM-DD)")
    result.add_argument(
        "--symbols", default=",".join(DEFAULT_SMOKE_SYMBOLS),
        help="Comma-separated smoke symbols; maximum five, default is RELIANCE,TCS,INFY,HDFCBANK,SBIN",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        session_date = date.fromisoformat(args.date)
    except ValueError:
        print("OPENALGO_READONLY_PREFLIGHT_BLOCKED: --date must use YYYY-MM-DD")
        return 2
    symbols = list(dict.fromkeys(item.strip().upper() for item in args.symbols.split(",") if item.strip()))
    if not symbols or len(symbols) > 5:
        print("OPENALGO_READONLY_PREFLIGHT_BLOCKED: provide one to five unique smoke symbols")
        return 2

    try:
        settings = OpenAlgoSettings.from_env()
        client = OpenAlgoReadOnlyClient(settings)
    except OpenAlgoDataError as exc:
        print(f"OPENALGO_READONLY_PREFLIGHT_BLOCKED: {exc}")
        return 2

    rows: list[dict[str, object]] = []
    for symbol in symbols:
        try:
            identity = client.validate_equity_symbol(symbol)
            bars = client.minute_bars(identity, session_date)
            rows.append({
                "symbol": symbol,
                "exchange": identity.exchange,
                "series": identity.series,
                "status": "PASS",
                "bars": len(bars),
                "first_bar_ist": bars[0].timestamp.isoformat(),
                "last_bar_ist": bars[-1].timestamp.isoformat(),
            })
        except OpenAlgoDataError as exc:
            rows.append({"symbol": symbol, "status": "BLOCKED", "reason": str(exc)})

    passed = all(row["status"] == "PASS" for row in rows)
    parsed = urlsplit(settings.base_url)
    report = {
        "status": "OPENALGO_READONLY_PREFLIGHT_READY" if passed else "OPENALGO_READONLY_PREFLIGHT_BLOCKED",
        "provider": "openalgo_ticker_history",
        "base_host": parsed.netloc,
        "session_date": session_date.isoformat(),
        "interval": "1m",
        "symbols_requested": len(symbols),
        "symbols_passed": sum(row["status"] == "PASS" for row in rows),
        "symbols": rows,
        "strategy_signals_generated": False,
        "paper_ledger_modified": False,
        "order_api_called": False,
        "live_trading_enabled": False,
    }
    print(json.dumps(report, indent=2))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
