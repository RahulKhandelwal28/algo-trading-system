#!/usr/bin/env python3
"""Run one small, read-only Yahoo 1-minute NSE equity feed diagnostic.

This makes one yfinance download invocation for at most five predeclared
symbols. It has no application-level retry or polling loop, writes no data,
creates no signals, and does not touch any paper ledger. The result is a
source/quality smoke check, not proof of an exchange-authorized or real-time
feed.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from nse_research.data.yfinance_intraday import (
    IST,
    load_yahoo_instruments,
    normalize_yfinance_frame,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SYMBOLS = ("RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN")
MAX_BAR_AGE_SECONDS_DEFAULT = 180
REQUEST_TIMEOUT_SECONDS = 15.0


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    if not symbols:
        raise argparse.ArgumentTypeError("provide at least one symbol")
    if len(symbols) > len(DEFAULT_SYMBOLS) or any(item not in DEFAULT_SYMBOLS for item in symbols):
        raise argparse.ArgumentTypeError(
            "this bounded diagnostic accepts only up to five symbols from " + ",".join(DEFAULT_SYMBOLS)
        )
    return symbols


def run_check(
    *,
    session_date: date,
    symbols: Sequence[str] = DEFAULT_SYMBOLS,
    max_bar_age_seconds: int = MAX_BAR_AGE_SECONDS_DEFAULT,
    now_fn: Any = lambda: datetime.now(IST),
    downloader: Any = None,
) -> dict[str, Any]:
    """Fetch and validate a single selected NSE session sample; never writes."""
    requested_at = now_fn()
    if requested_at.tzinfo is None or requested_at.utcoffset() is None:
        raise ValueError("diagnostic clock must include a timezone")
    requested_at = requested_at.astimezone(IST)

    report: dict[str, Any] = {
        "status": "DATA_UNAVAILABLE_OR_INVALID",
        "provider": "Yahoo Finance via yfinance",
        "classification": "THIRD_PARTY_RESEARCH_SOURCE_NOT_AUTHORIZED_NSE_FEED",
        "interval": "1m",
        "requested_at_ist": requested_at.isoformat(),
        "session_date": session_date.isoformat(),
        "symbols_requested": list(symbols),
        "max_symbols": len(DEFAULT_SYMBOLS),
        "max_bar_age_seconds": max_bar_age_seconds,
        "paper_ledger_touched": False,
        "signals_generated": False,
        "application_level_retries": 0,
        "scope": "one bounded source diagnostic; no strategy, signal, order, or ledger processing",
    }

    try:
        if max_bar_age_seconds <= 0:
            raise ValueError("max-bar-age-seconds must be positive")
        if any(symbol not in DEFAULT_SYMBOLS for symbol in symbols) or not symbols or len(symbols) > 5:
            raise ValueError("symbols must be a non-empty subset of the bounded five-symbol sample")

        universe = load_yahoo_instruments(PROJECT_ROOT, size=486)
        selected = {symbol: universe[symbol] for symbol in symbols}
        started = datetime.now(IST)
        if downloader is None:
            from nse_research.data.yfinance_intraday import download_yfinance_history

            frame = download_yfinance_history(
                [item.yahoo_symbol for item in selected.values()],
                start=session_date.isoformat(),
                end=(session_date + timedelta(days=1)).isoformat(),
                interval="1m",
                timeout_seconds=REQUEST_TIMEOUT_SECONDS,
            )
        else:
            frame = downloader(
                tickers=[item.yahoo_symbol for item in selected.values()],
                start=session_date.isoformat(),
                end=(session_date + timedelta(days=1)).isoformat(),
                interval="1m",
                auto_adjust=False,
                back_adjust=False,
                repair=False,
                actions=True,
                prepost=False,
                threads=False,
                progress=False,
                keepna=True,
                timeout=REQUEST_TIMEOUT_SECONDS,
                group_by="ticker",
                multi_level_index=True,
            )
        received = datetime.now(IST)
        normalized = normalize_yfinance_frame(
            frame,
            selected,
            interval="1m",
            now=received,
            request_started_at=started,
            response_received_at=received,
            batch_id=1,
            session_date=session_date,
            volume_kind="UNKNOWN",
        )

        by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in symbols}
        for row in normalized.rows:
            by_symbol[row["symbol"]].append(row)

        symbol_reports: list[dict[str, Any]] = []
        all_valid = True
        for symbol in symbols:
            rows = by_symbol[symbol]
            if not rows:
                all_valid = False
                symbol_reports.append({
                    "symbol": symbol,
                    "status": "NO_VALID_BAR",
                    "bars": 0,
                    "error_codes": sorted({
                        str(issue.get("code")) for issue in normalized.issues or []
                        if issue.get("symbol") == symbol
                    }),
                })
                continue
            latest = max(rows, key=lambda row: row["timestamp"])
            bar_end = datetime.fromisoformat(latest["timestamp"]) + timedelta(minutes=1)
            age_seconds = (received - bar_end).total_seconds()
            fresh = 0 <= age_seconds <= max_bar_age_seconds
            all_valid = all_valid and fresh
            symbol_reports.append({
                "symbol": symbol,
                "status": "PASS_SAMPLE" if fresh else "STALE_OR_FUTURE_BAR",
                "bars": len(rows),
                "first_bar_ist": rows[0]["timestamp"],
                "latest_bar_ist": latest["timestamp"],
                "latest_completed_bar_age_seconds": round(age_seconds, 2),
                "latest_ohlcv": {
                    field: latest[field] for field in ("open", "high", "low", "close", "volume")
                },
                "source_timestamp": latest["source_timestamp"],
                "project_identity_mapping": f"{latest['exchange']}/{latest['series']} from frozen NSE manifest",
                "volume_semantics": latest["volume_kind"],
            })

        report.update({
            "status": "PASS_SAMPLE_ONLY" if all_valid and len(symbol_reports) == len(symbols) else "DATA_UNAVAILABLE_OR_INVALID",
            "request_started_at_ist": started.isoformat(),
            "response_received_at_ist": received.isoformat(),
            "raw_latest_timestamp": normalized.raw_index_last,
            "forming_rows_ignored": normalized.forming_rows_ignored,
            "missing_symbols": normalized.missing_symbols,
            "failed_symbols": normalized.failed_symbols,
            "invalid_symbols": normalized.invalid_symbols,
            "symbol_results": symbol_reports,
        })
    except Exception as exc:
        report.update({
            "status": "DATA_UNAVAILABLE_OR_INVALID",
            "error_type": type(exc).__name__,
            "error": str(exc),
        })
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-date", type=_parse_date, default=datetime.now(IST).date(),
                        help="NSE session date YYYY-MM-DD; default is today's IST date")
    parser.add_argument("--symbols", type=_parse_symbols, default=DEFAULT_SYMBOLS,
                        help="comma-separated subset of the fixed five-symbol sample")
    parser.add_argument("--max-bar-age-seconds", type=int, default=MAX_BAR_AGE_SECONDS_DEFAULT,
                        help="diagnostic freshness threshold; this is not a Yahoo freshness guarantee")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run_check(session_date=args.session_date, symbols=args.symbols,
                       max_bar_age_seconds=args.max_bar_age_seconds)
    print(json.dumps(report, indent=2, default=str))
    return 0 if report.get("status") == "PASS_SAMPLE_ONLY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
