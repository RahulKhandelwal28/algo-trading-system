#!/usr/bin/env python3
"""Print the path to a generated report or write a concise metrics summary."""

from pathlib import Path
import argparse
import pandas as pd

parser = argparse.ArgumentParser(description="Inspect a completed backtest report")
parser.add_argument("run_dir", help="Experiment result directory")
args = parser.parse_args()
run_dir = Path(args.run_dir)
report = run_dir / "report.html"
if not report.exists():
    raise SystemExit(f"No report.html found in {run_dir}")
metrics = pd.read_csv(run_dir / "strategy_metrics.csv")
print(metrics.to_string(index=False) if not metrics.empty else "No data-backed metrics were produced.")
print(f"HTML report: {report.resolve()}")
