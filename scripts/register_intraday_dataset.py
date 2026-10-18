#!/usr/bin/env python3
"""Register and hash an authorized intraday dataset without running trading."""

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
    register_dataset,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True, type=Path, help="raw CSV, CSV.GZ, CSV.ZIP, Parquet file, or directory")
    parser.add_argument("--metadata", type=Path, help="delivery metadata JSON; otherwise discover an adjacent metadata file")
    parser.add_argument("--dry-run", action="store_true", help="hash and inspect only; do not copy or register")
    args = parser.parse_args(argv)
    try:
        result = register_dataset(args.path, metadata_path=args.metadata, dry_run=args.dry_run, project_root=PROJECT_ROOT)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0
    except (Phase4FError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
