#!/usr/bin/env python3
"""Run Phase 4E validation and write Phase 4F reports/certificate."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nse_research.data.intraday_ingestion import (  # noqa: E402
    PROJECT_ROOT,
    Phase4FError,
    validate_dataset,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="registered dataset ID")
    parser.add_argument("--batch-size", type=int, default=10_000, help="bounded Phase 4E validation batch size")
    args = parser.parse_args(argv)
    try:
        result = validate_dataset(args.dataset, project_root=PROJECT_ROOT, batch_size=args.batch_size)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0 if result["result"]["phase4e_validation_status"] == "PASS" else 2
    except (Phase4FError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
