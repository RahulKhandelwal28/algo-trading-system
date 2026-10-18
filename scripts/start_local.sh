#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

VENV_PYTHON="$PROJECT_ROOT/.venv/bin/python"
CONFIG_PATH="$PROJECT_ROOT/config/phase3d_personal.yaml"
DASHBOARD_PORT="${DASHBOARD_PORT:-8765}"

if [[ ! -x "$VENV_PYTHON" ]]; then
  echo "Missing executable: $VENV_PYTHON" >&2
  echo "Create the project environment and install requirements.txt first." >&2
  exit 1
fi

for required_file in \
  "$CONFIG_PATH" \
  "$PROJECT_ROOT/src/nse_research/dashboard.py" \
  "$PROJECT_ROOT/forward/dashboard/index.html" \
  "$PROJECT_ROOT/forward/dashboard/styles.css" \
  "$PROJECT_ROOT/forward/dashboard/app.js"; do
  if [[ ! -f "$required_file" ]]; then
    echo "Required local dashboard file is missing: $required_file" >&2
    exit 1
  fi
done

export PYTHONPATH="$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
# This process only serves GET-only dashboard views. Pin the inherited safety
# flag and remove the opt-in flag used by the separate paper-run command.
export LIVE_TRADING_ENABLED=false
unset PAPER_TRADING_ENABLED || true

"$VENV_PYTHON" - "$PROJECT_ROOT" "$CONFIG_PATH" "$DASHBOARD_PORT" <<'PY'
import csv
import socket
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from nse_research.config import AppConfig
from nse_research.forward.calendar import NseTradingCalendar

root = Path(sys.argv[1]).resolve()
config_path = Path(sys.argv[2]).resolve()
try:
    port = int(sys.argv[3])
except ValueError:
    raise SystemExit("DASHBOARD_PORT must be an integer")
if not 1 <= port <= 65535:
    raise SystemExit("DASHBOARD_PORT must be between 1 and 65535")

if sys.version_info < (3, 11):
    raise SystemExit("Python 3.11 or later is required")

try:
    # Import the actual API module to validate the dashboard's complete Python
    # dependency chain without starting the trading runner.
    import nse_research.dashboard  # noqa: F401
except Exception as error:
    raise SystemExit(f"Dashboard dependencies are unavailable: {type(error).__name__}: {error}")

config = AppConfig.load(config_path)
if config.live_trading.enabled:
    raise SystemExit("Refusing startup: config/phase3d_personal.yaml enables live trading")

def rooted(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path

database = rooted(config.forward_paper.database)
manifest_path = rooted(config.forward_paper.experiment_manifest_path)
calendar_path = rooted(config.forward_paper.trading_calendar_path)
index_dir = rooted(config.forward_paper.index_data_dir)
normalized_dir = rooted(config.forward_paper.daily_data_dir)
if not database.is_file():
    raise SystemExit(f"Phase 3D paper ledger is missing: {database}")
if not manifest_path.is_file():
    raise SystemExit(f"Phase 3D experiment manifest is missing: {manifest_path}")

import json

manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
if manifest.get("live_trading_enabled") is not False or manifest.get("broker_contacted") is not False:
    raise SystemExit("Refusing startup: experiment manifest does not confirm paper-only/no-broker state")
processed = int(manifest.get("processed_session_count", len(manifest.get("processed_sessions", []))))
target = int(manifest.get("target_sessions", config.forward_paper.target_sessions))
processed_dates = list(manifest.get("processed_sessions", []))
last_date = manifest.get("last_processed_date") or (processed_dates[-1] if processed_dates else None)

def has_date(path: Path, requested: date) -> bool:
    if not path.is_file():
        return False
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            date_column = next((name for name in (reader.fieldnames or []) if name.casefold() == "date"), None)
            if not date_column:
                return False
            for row in reader:
                value = str(row.get(date_column) or "").strip()
                if value == requested.isoformat():
                    return True
                for pattern in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"):
                    try:
                        if datetime.strptime(value, pattern).date() == requested:
                            return True
                    except ValueError:
                        continue
    except (OSError, UnicodeError, csv.Error):
        return False
    return False

next_day = None
next_status = "EXPERIMENT TARGET REACHED" if processed >= target else "NO NEXT SESSION"
if processed < target and last_date:
    try:
        cursor = date.fromisoformat(last_date) + timedelta(days=1)
        end_value = manifest.get("end_date") or str(config.forward_paper.experiment_end_date)
        end_date = date.fromisoformat(end_value)
        calendar = NseTradingCalendar(calendar_path)
        while cursor <= end_date:
            decision = calendar.classify(cursor, observed_market_data=False)
            if decision.status != "NON_TRADING_DAY":
                next_day = cursor
                break
            cursor += timedelta(days=1)
        if next_day:
            missing = []
            if not (normalized_dir / f"{next_day.isoformat()}.csv").is_file():
                missing.append("validated equity file")
            if not has_date(index_dir / "index_NSEI.csv", next_day):
                missing.append("official NIFTY row")
            if not has_date(index_dir / "index_INDIAVIX.csv", next_day):
                missing.append("official India VIX row")
            next_status = (
                "BLOCKED / WAITING FOR VALID EOD DATA (missing: " + ", ".join(missing) + ")"
                if missing else "EOD FILES PRESENT; PAPER PREFLIGHT STILL REQUIRED"
            )
    except (OSError, ValueError, TypeError) as error:
        raise SystemExit(f"Could not read Phase 3D session state safely: {error}")

try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
except OSError as error:
    raise SystemExit(f"Cannot bind dashboard to 127.0.0.1:{port}: {error}")

print("========================================", flush=True)
print("NSE Research System - Local Startup", flush=True)
print("========================================", flush=True)
print("Environment: .venv", flush=True)
print(f"Python: {sys.version.split()[0]}", flush=True)
print("Backend/API + dashboard: http://127.0.0.1:" + str(port) + "/", flush=True)
print("Mode: PAPER RESEARCH / READ-ONLY DASHBOARD", flush=True)
print("REALTIME PAPER DATA MODE: " + ("CONFIGURED; dashboard will not start a feed worker" if config.realtime.enabled else "DISABLED"), flush=True)
print("LIVE TRADING: DISABLED", flush=True)
print("BROKER EXECUTION: DISABLED", flush=True)
print("BROKER: NOT CONNECTED", flush=True)
print(f"Phase 3D progress: {processed} / {target} sessions", flush=True)
print(f"Latest session: {last_date or 'none'}", flush=True)
if next_day:
    print(f"Next session: {next_day.isoformat()} — {next_status}", flush=True)
else:
    print(f"Next session: {next_status}", flush=True)
print("No trading runner or data acquisition job will be started.", flush=True)
print("Press Ctrl+C to stop.", flush=True)
PY

exec "$VENV_PYTHON" -m nse_research.dashboard \
  --config "$CONFIG_PATH" \
  --host 127.0.0.1 \
  --port "$DASHBOARD_PORT"
