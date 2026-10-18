#!/usr/bin/env python3
"""Run the fixed-rule Phase 3C Kaggle research validation."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_research.backtest.phase3c_kaggle import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main(["--project-root", str(PROJECT_ROOT), *sys.argv[1:]]))
