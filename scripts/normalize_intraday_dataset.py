#!/usr/bin/env python3
"""Normalize a Phase 4E-PASS dataset into immutable-manifest canonical bars."""

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
    normalize_dataset,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="registered dataset ID")
    parser.add_argument("--batch-size", type=int, default=10_000, help="bounded normalization batch size")
    parser.add_argument("--dry-run", action="store_true", help="check gates and report outputs without writing normalized data")
    args = parser.parse_args(argv)
    try:
        result = normalize_dataset(args.dataset, project_root=PROJECT_ROOT,
                                   dry_run=args.dry_run, batch_size=args.batch_size)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0
    except (Phase4FError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
