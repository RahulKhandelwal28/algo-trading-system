#!/usr/bin/env python3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nse_research.data.nse_index_download import download_nse_indices_main

if __name__ == "__main__":
    raise SystemExit(download_nse_indices_main(sys.argv[1:]))
