"""Import, preflight, and run exactly one local Phase 3D personal paper date."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

from nse_research.config import AppConfig
from nse_research.forward.acquisition import DailyEodAcquisitionAdapter, HttpDailyEodSource
from nse_research.forward.intake import DailyEodIntakeError, expected_eq_symbols, import_daily_eod


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _record_block(root: Path, config: AppConfig, day: date, status: str, output: str) -> None:
    manifest_setting = config.forward_paper.experiment_manifest_path
    if not manifest_setting:
        return
    manifest_path = Path(manifest_setting)
    manifest_path = manifest_path if manifest_path.is_absolute() else root / manifest_path
    if not manifest_path.is_file():
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    attempts = list(manifest.get("blocked_sessions", []))
    attempts.append({
        "date": day.isoformat(), "status": status, "reason": output.strip(),
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    manifest["blocked_sessions"] = attempts
    _atomic_json(manifest_path, manifest)
    output_root = Path(config.forward_paper.output_root)
    output_root = output_root if output_root.is_absolute() else root / output_root
    _atomic_json(output_root / "daily" / day.isoformat() / "status.json", {
        "date": day.isoformat(), "status": status, "reason": output.strip(),
        "paper_ledger_mutated": False,
    })


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Process one validated personal Phase 3D paper session")
    parser.add_argument("--config", default="config/phase3d_personal.yaml")
    parser.add_argument("--market-date", required=True, help="completed NSE session YYYY-MM-DD")
    parser.add_argument("--source-provider", help="source fact supplied with the incoming file")
    parser.add_argument("--source-identifier", help="source batch/reference supplied with the incoming file")
    parser.add_argument("--adjustment-status", help="adjustment status only if explicitly documented")
    parser.add_argument("--source-name", help="name of the explicit HTTPS daily EOD source")
    parser.add_argument("--source-url", help="HTTPS URL template containing {date}; optional {symbols}")
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    try:
        market_date = date.fromisoformat(args.market_date)
        if market_date.isoformat() != args.market_date:
            raise ValueError("market date must use strict YYYY-MM-DD format")
        config_path = Path(args.config)
        config_path = config_path if config_path.is_absolute() else root / config_path
        config = AppConfig.load(config_path)
    except Exception as error:
        print(f"FORWARD_RUN_BLOCKED\n- {type(error).__name__}: {error}")
        return 2

    if config.forward_paper.provider != "personal_local_eod":
        print("FORWARD_RUN_BLOCKED\n- this workflow requires the explicitly configured personal_local_eod provider")
        return 2
    if os.getenv("PAPER_TRADING_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
        print("FORWARD_RUN_BLOCKED\n- PAPER_TRADING_ENABLED=true is required")
        return 2
    if os.getenv("LIVE_TRADING_ENABLED", "").strip().lower() not in {"0", "false", "no", "off"} or config.live_trading.enabled:
        print("FORWARD_RUN_BLOCKED\n- LIVE_TRADING_ENABLED must remain false")
        return 2

    incoming_dir = root / "data" / "paper_trading" / "incoming"
    source = incoming_dir / f"{market_date.isoformat()}.csv"
    if bool(args.source_name) != bool(args.source_url):
        print("FORWARD_RUN_BLOCKED\n- --source-name and --source-url must be supplied together")
        return 2
    if args.source_url:
        if source.is_file():
            print("FORWARD_RUN_BLOCKED\n- both an incoming CSV and an HTTP acquisition were supplied for this date")
            return 2
        try:
            identity_path = Path(config.forward_paper.identity_file)
            identity_path = identity_path if identity_path.is_absolute() else root / identity_path
            expected_symbols = expected_eq_symbols(identity_path)
            index_dir = Path(config.forward_paper.index_data_dir)
            index_dir = index_dir if index_dir.is_absolute() else root / index_dir
            trading_calendar_path = Path(config.forward_paper.trading_calendar_path)
            trading_calendar_path = trading_calendar_path if trading_calendar_path.is_absolute() else root / trading_calendar_path
            http_source = HttpDailyEodSource(
                args.source_name,
                args.source_url,
                timeout_seconds=args.timeout_seconds,
                retries=args.retries,
                rate_limit_state_dir=root / "data" / "paper_trading" / "raw" / ".rate_limit",
            )
            raw_dir = root / "data" / "paper_trading" / "raw"
            normalized_dir = Path(config.forward_paper.daily_data_dir)
            normalized_dir = normalized_dir if normalized_dir.is_absolute() else root / normalized_dir
            adapter = DailyEodAcquisitionAdapter(
                source=http_source,
                official_nifty_path=index_dir / "index_NSEI.csv",
                trading_calendar_path=trading_calendar_path,
                raw_dir=raw_dir,
                normalized_dir=normalized_dir,
                intake_manifest_dir=normalized_dir.parent / "manifests",
                daily_manifest_path=raw_dir / "daily_manifest.jsonl",
            )
            acquired = adapter.acquire(market_date, expected_symbols)
        except Exception as error:
            reason = f"DATA_ACQUISITION_FAILED: {type(error).__name__}: {error}"
            _record_block(root, config, market_date, "DATA_VALIDATION_FAILED", reason)
            print(reason)
            return 2
        print(f"DAILY_EOD_{acquired['validation_status']}: {market_date.isoformat()}")
        if acquired["validation_status"] != "PASS":
            reason = "\n".join([acquired["validation_status"], *[f"- {item}" for item in acquired.get("errors", [])]])
            if acquired["validation_status"] == "NON_TRADING_DAY":
                for item in acquired.get("errors", []):
                    print(f"- {item}")
                return 0
            _record_block(root, config, market_date, acquired["validation_status"], reason)
            for item in acquired.get("errors", []):
                print(f"- {item}")
            return 2
        print(f"Raw: {acquired['raw_file']} SHA256={acquired['sha256']}")
        print(f"Normalized: {acquired['normalized_file']} ({acquired['received_symbols']}/{acquired['expected_symbols']} symbols)")
    elif source.is_file():
        try:
            symbols = expected_eq_symbols(root / config.forward_paper.identity_file)
            imported = import_daily_eod(
                source_path=source,
                expected_symbols=symbols,
                normalized_dir=root / config.forward_paper.daily_data_dir,
                manifest_dir=(root / config.forward_paper.daily_data_dir).parent / "manifests",
                source_provider=args.source_provider,
                source_identifier=args.source_identifier,
                adjustment_status=args.adjustment_status,
            )
        except Exception as error:
            reason = f"DATA_VALIDATION_FAILED: {type(error).__name__}: {error}"
            _record_block(root, config, market_date, "DATA_VALIDATION_FAILED", reason)
            print(reason)
            return 2
        if imported.get("validation_status") != "PASS":
            reason = "\n".join(["DATA_VALIDATION_FAILED", *[f"- {item}" for item in imported.get("errors", [])]])
            _record_block(root, config, market_date, "DATA_VALIDATION_FAILED", reason)
            print(reason)
            return 2
        print(f"DAILY_EOD_PASS: {market_date.isoformat()} ({imported['EQ_row_count']} explicit EQ rows)")

    common = [sys.executable, "-m", "nse_research.daily_paper_trader", "--config", str(config_path),
              "--provider", "personal_local_eod", "--market-date", market_date.isoformat()]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    preflight = subprocess.run([*common, "--preflight"], cwd=root, env=env, capture_output=True, text=True, check=False)
    print(preflight.stdout, end="")
    if preflight.stderr:
        print(preflight.stderr, end="", file=sys.stderr)
    if preflight.returncode != 0:
        status = "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA" if "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA" in preflight.stdout else "PHASE3D_FORWARD_BLOCKED"
        _record_block(root, config, market_date, status, preflight.stdout + preflight.stderr)
        return preflight.returncode

    run = subprocess.run(common, cwd=root, env=env, capture_output=True, text=True, check=False)
    print(run.stdout, end="")
    if run.stderr:
        print(run.stderr, end="", file=sys.stderr)
    if run.returncode:
        _record_block(root, config, market_date, "FORWARD_RUN_BLOCKED", run.stdout + run.stderr)
    return run.returncode


if __name__ == "__main__":
    raise SystemExit(main())
