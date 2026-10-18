"""Create-once frozen strategy/risk/cost/universe baseline for Phase 3D."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from nse_research.config import AppConfig


PHASE3C_DAILY_STRATEGIES = (
    "trend_following",
    "momentum",
    "breakout_20",
    "breakout_50",
    "breakout_previous_day",
    "pullback",
    "mean_reversion",
    "swing_1_3",
    "swing_3_5",
    "swing_5_10",
    "swing_10_20",
)

_CODE_FILES = (
    "src/nse_research/strategies/equity.py",
    "src/nse_research/indicators/features.py",
    "src/nse_research/risk/controls.py",
    "src/nse_research/scanner/scanner.py",
    "src/nse_research/backtest/costs.py",
)

_PERSONAL_PHASE3D_CODE_FILES = (
    "src/nse_research/config.py",
    "src/nse_research/daily_paper_trader.py",
    "src/nse_research/forward/data.py",
    "src/nse_research/forward/engine.py",
    "src/nse_research/forward/intake.py",
    "src/nse_research/forward/preflight.py",
    "src/nse_research/forward/reports.py",
)


class BaselineMismatchError(RuntimeError):
    """Raised when Phase 3D inputs differ from the create-once baseline."""


def _canonical_hash(value: Any) -> str:
    content = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def _universe_snapshot(config: AppConfig, root: Path) -> dict[str, Any]:
    path = _resolve(root, config.universe.symbols_file)
    if not path.is_file():
        raise FileNotFoundError(f"Phase 3D universe file does not exist: {path}")
    with path.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError("Phase 3D universe CSV has no header")
        columns = {name.strip().lower(): name for name in reader.fieldnames}
        symbol_column = columns.get("symbol") or columns.get("ticker")
        if not symbol_column:
            raise ValueError("Phase 3D universe CSV must contain Symbol or Ticker")
        symbols = []
        for row in reader:
            raw = str(row.get(symbol_column) or "").strip().upper()
            if not raw or raw.startswith("DUMMY"):
                continue
            symbols.append(raw if raw.endswith(".NS") else f"{raw}.NS")
    symbols = list(dict.fromkeys(symbols))
    if config.universe.max_symbols:
        symbols = symbols[: config.universe.max_symbols]
    return {
        "source": config.universe.source,
        "symbols_file": str(path.relative_to(root)) if path.is_relative_to(root) else str(path),
        "symbols_file_sha256": _sha256(path),
        "symbol_count": len(symbols),
        "symbols": symbols,
        "benchmark": config.universe.benchmark,
        "selection": {
            "max_symbols": config.universe.max_symbols,
            "min_history_rows": config.universe.min_history_rows,
            "min_avg_volume": config.universe.min_avg_volume,
            "min_avg_traded_value": config.universe.min_avg_traded_value,
            "min_price": config.universe.min_price,
            "liquidity_lookback": config.universe.liquidity_lookback,
        },
    }


def baseline_inputs(
    config: AppConfig,
    root: str | Path,
    *,
    baseline_name: str = "PHASE3C_BASELINE",
) -> dict[str, Any]:
    project_root = Path(root).resolve()
    enabled = list(config.strategies.enabled)
    daily = [name for name in enabled if name in PHASE3C_DAILY_STRATEGIES]
    if tuple(daily) != PHASE3C_DAILY_STRATEGIES:
        raise BaselineMismatchError(
            "daily strategy set/order differs from Phase 3C; create a separately versioned experiment"
        )
    code_files = _CODE_FILES
    if baseline_name != "PHASE3C_BASELINE":
        code_files = (*_CODE_FILES, *_PERSONAL_PHASE3D_CODE_FILES)
    return {
        "strategy_version": {
            "name": baseline_name,
            "phase3c_run_id": "20260926_171429" if baseline_name == "PHASE3C_BASELINE" else None,
            "daily_strategies": list(PHASE3C_DAILY_STRATEGIES),
            "enabled_strategies": enabled,
            "parameters": config.strategies.model_dump(mode="json"),
        },
        "risk_configuration": config.risk.model_dump(mode="json"),
        "cost_configuration": config.costs.model_dump(mode="json"),
        "slippage_configuration": {"slippage_bps": config.costs.slippage_bps},
        "universe_configuration": _universe_snapshot(config, project_root),
        "data_provider": config.forward_paper.provider,
        "code_version": {
            "git_commit": _git_commit(project_root),
            "application_version": "0.1.0",
            "source_sha256": {
                relative: _sha256(project_root / relative)
                for relative in code_files
            },
        },
    }


def _git_commit(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = result.stdout.strip()
    return commit or None


def create_baseline_manifest(
    config: AppConfig,
    *,
    root: str | Path,
    path: str | Path,
    created_at: datetime | None = None,
    phase3c_source_path: str | Path | None = None,
    baseline_name: str = "PHASE3C_BASELINE",
) -> dict[str, Any]:
    """Create a baseline only when the destination does not yet exist."""
    destination = Path(path)
    if destination.exists():
        raise FileExistsError(f"refusing to replace frozen baseline: {destination}")
    inputs = baseline_inputs(config, root, baseline_name=baseline_name)
    source_path = Path(phase3c_source_path) if phase3c_source_path else None
    source_manifest = None
    if source_path is not None:
        source = json.loads(source_path.read_text(encoding="utf-8"))
        if source.get("run_id") != "20260926_171429":
            raise BaselineMismatchError("Phase 3C source manifest run id does not match the frozen run")
        source_manifest = {
            "path": str(source_path),
            "sha256": _sha256(source_path),
            "run_id": source.get("run_id"),
            "status": source.get("status"),
            "provider": source.get("provider"),
            "strategies_run": source.get("strategies_run"),
        }
        if source.get("strategies_run") != inputs["strategy_version"]["daily_strategies"]:
            raise BaselineMismatchError("configured daily strategy names differ from the Phase 3C source manifest")
        if source.get("risk_configuration") != inputs["risk_configuration"]:
            raise BaselineMismatchError("configured risk values differ from the Phase 3C source manifest")
        if source.get("cost_model") != inputs["cost_configuration"]:
            raise BaselineMismatchError("configured cost values differ from the Phase 3C source manifest")

    timestamp = (created_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    document = {
        "baseline_name": baseline_name,
        "created_at": timestamp.isoformat(),
        "created_at_ist": timestamp.astimezone(ZoneInfo("Asia/Kolkata")).isoformat(),
        "phase3c_source": source_manifest,
        **inputs,
    }
    document["baseline_sha256"] = _canonical_hash(document)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(document, stream, indent=2, sort_keys=True, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, destination)
    except FileExistsError as error:
        raise FileExistsError(f"refusing to replace frozen baseline: {destination}") from error
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return document


def validate_baseline_manifest(
    config: AppConfig,
    *,
    root: str | Path,
    path: str | Path,
    baseline_name: str = "PHASE3C_BASELINE",
) -> dict[str, Any]:
    """Fail closed if live configuration, universe, or strategy code drifted."""
    manifest_path = Path(path)
    if not manifest_path.is_file():
        raise BaselineMismatchError(f"frozen Phase 3D baseline is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    expected_hash = manifest.get("baseline_sha256")
    body = {key: value for key, value in manifest.items() if key != "baseline_sha256"}
    if expected_hash != _canonical_hash(body):
        raise BaselineMismatchError("frozen baseline manifest checksum is invalid")
    if manifest.get("baseline_name") != baseline_name:
        raise BaselineMismatchError("unexpected baseline name")
    expected = baseline_inputs(config, root, baseline_name=baseline_name)
    for field in (
        "strategy_version",
        "risk_configuration",
        "cost_configuration",
        "slippage_configuration",
        "universe_configuration",
        "data_provider",
        "code_version",
    ):
        if manifest.get(field) != expected.get(field):
            raise BaselineMismatchError(
                f"{field} differs from {baseline_name}; start a new versioned experiment"
            )
    return manifest
