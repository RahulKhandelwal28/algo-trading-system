#!/usr/bin/env python3
"""Run the read-only Groww feed through the deterministic paper scanner."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from run_jugaad_realtime import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main(["--provider", "groww_feed", *sys.argv[1:]]))
