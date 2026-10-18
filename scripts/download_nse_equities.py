"""CLI entry point for the official NSE security-wise equity downloader."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nse_research.data.nse_equity_download import download_nse_equities_main


if __name__ == "__main__":
    raise SystemExit(download_nse_equities_main(sys.argv[1:]))
