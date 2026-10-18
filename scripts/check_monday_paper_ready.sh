#!/bin/sh
set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
SESSION_DATE=${1:-2026-10-05}

cd "$PROJECT_DIR"
if [ ! -x .venv/bin/python ]; then
  echo "Missing .venv/bin/python. Create the project virtual environment first." >&2
  exit 2
fi

exec env PYTHONPATH=src .venv/bin/python scripts/run_phase4g_yf_forward.py \
  --mode preflight \
  --session-date "$SESSION_DATE" \
  --batch-size 50 \
  --max-concurrent-batches 2 \
  --summary
