#!/usr/bin/env python3
"""Fail-closed Phase 4D intraday paper session command line."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_research.execution.phase4d import (  # noqa: E402
    Phase4DOrchestrator,
    Phase4DError,
    preflight_text,
    synthetic_test,
)


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("PRECHECK", "SYNTHETIC_TEST", "REPLAY", "REALTIME_PAPER", "LIVE"),
                        default="PRECHECK")
    parser.add_argument("--date", type=_date)
    parser.add_argument("--universe-size", type=int, choices=(100, 200, 486))
    parser.add_argument("--ledger", type=Path)
    parser.add_argument("--data", type=Path, help="read-only PRECHECK input; REPLAY is Phase 4F-only")
    parser.add_argument("--min-coverage", type=float)
    args = parser.parse_args(argv)

    if args.mode == "LIVE":
        print("LIVE_EXECUTION_NOT_AVAILABLE")
        return 2
    if args.mode == "REALTIME_PAPER":
        print("REALTIME_PROVIDER_NOT_VALIDATED")
        print("Realtime paper mode is blocked until a provider passes the documented data and safety validation.")
        return 2
    if args.mode == "SYNTHETIC_TEST":
        try:
            outcome = synthetic_test(args.ledger)
            print(json.dumps(outcome, indent=2, default=str))
            return 0
        except (Phase4DError, OSError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 1

    if args.mode == "REPLAY":
        print("REPLAY_DATA_MUST_BE_PHASE4F_REGISTERED", file=sys.stderr)
        print("Use scripts/replay_intraday_session.py with a registered Phase 4F dataset and temporary ledger.",
              file=sys.stderr)
        return 2

    try:
        orchestrator = Phase4DOrchestrator(
            data_path=args.data, ledger_path=args.ledger,
            universe_size=args.universe_size, min_coverage=args.min_coverage,
        )
        result = orchestrator.preflight(trading_date=args.date)
        print(preflight_text(result))
        return 0 if result.status == "PHASE4D_READY" else 2
    except (Phase4DError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
