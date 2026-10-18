#!/usr/bin/env python3
"""Fail-closed Phase 4F research-readiness check; creates no signals or orders."""

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
    assert_research_ready,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", help="registered dataset ID; omit to locate any research-ready normalized dataset")
    args = parser.parse_args(argv)
    try:
        result = assert_research_ready(args.dataset, project_root=PROJECT_ROOT)
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0
    except Phase4FError as exc:
        print(json.dumps({"status": exc.code, "reasons": [exc.detail] if exc.detail else []}, indent=2))
        return 2
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "PHASE4F_READINESS_CHECK_FAILED", "reasons": [str(exc)]}, indent=2))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
