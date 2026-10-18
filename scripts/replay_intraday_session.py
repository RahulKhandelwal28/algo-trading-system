#!/usr/bin/env python3
"""Run one Phase 4D paper replay with a Phase 4F-ready dataset and temp ledger."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nse_research.data.intraday_ingestion import (  # noqa: E402
    PROJECT_ROOT,
    Phase4FError,
)
from nse_research.intraday.phase4f_adapter import Phase4DReplayAdapter  # noqa: E402


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
    parser.add_argument("--dataset", required=True, help="Phase 4E research-ready dataset ID")
    parser.add_argument("--date", required=True, type=_date, help="one NSE session date, YYYY-MM-DD")
    parser.add_argument("--ledger", type=Path, help="required temporary ledger path; system temporary directory only")
    parser.add_argument("--universe-size", type=int, choices=(100, 200, 486), default=100)
    parser.add_argument("--minimum-coverage", type=float)
    args = parser.parse_args(argv)
    if args.ledger is None:
        print("PHASE4D_TEMPORARY_LEDGER_REQUIRED", file=sys.stderr)
        return 2
    ledger_candidate = args.ledger.expanduser()
    if not ledger_candidate.is_absolute():
        ledger_candidate = PROJECT_ROOT / ledger_candidate
    if ledger_candidate.resolve() == (PROJECT_ROOT / "data/paper_trading/phase4c/ledger.json").resolve():
        print("PHASE4D_PERSISTENT_LEDGER_REJECTED_FOR_REPLAY", file=sys.stderr)
        return 2
    try:
        adapter = Phase4DReplayAdapter(args.dataset, ledger_path=args.ledger,
                                       project_root=PROJECT_ROOT, universe_size=args.universe_size,
                                       minimum_coverage=args.minimum_coverage)
        result = adapter.run(args.date)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0 if result.get("status") == "PHASE4D_SESSION_COMPLETED" else 2
    except (Phase4FError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
