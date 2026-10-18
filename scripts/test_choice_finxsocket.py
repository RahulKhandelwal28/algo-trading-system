#!/usr/bin/env python3
"""Fail-closed Choice contract/configuration gate; this script never connects."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
_CONTRACT_STATUS = re.compile(r"^\*\*CONTRACT_STATUS:\s*(PASS|PARTIAL|FAIL)\*\*\s*$", re.MULTILINE)


def _read_contract_status(path: Path) -> str:
    content = path.read_text(encoding="utf-8")
    match = _CONTRACT_STATUS.search(content)
    if match is None:
        raise ValueError("contract document has no recognized CONTRACT_STATUS marker")
    return match.group(1)


def _read_safety_flags(path: Path) -> tuple[bool, bool]:
    # YAML parsing is local-only; this module does not import any network client.
    import yaml

    config: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("configuration must be a YAML mapping")
    realtime = config.get("realtime")
    live = config.get("live_trading")
    if not isinstance(realtime, dict) or not isinstance(live, dict):
        raise ValueError("configuration is missing realtime or live_trading settings")
    if not isinstance(realtime.get("enabled"), bool) or not isinstance(live.get("enabled"), bool):
        raise ValueError("realtime.enabled and live_trading.enabled must be explicit booleans")
    return realtime["enabled"], live["enabled"]


def evaluate_gate(
    contract_path: str | Path,
    config_path: str | Path,
    *,
    uat_opt_in: bool = False,
) -> dict[str, Any]:
    """Return a sanitized local status. No credentials or network APIs are read or used."""
    try:
        contract_status = _read_contract_status(Path(contract_path))
        realtime_enabled, live_trading_enabled = _read_safety_flags(Path(config_path))
    except Exception as error:
        return {
            "status": "CHOICE_FINX_FAILED",
            "reason": f"Local documentation/configuration check failed: {type(error).__name__}: {error}",
            "contract_status": "UNKNOWN",
            "uat_opt_in": uat_opt_in,
            "realtime_enabled": None,
            "live_trading_enabled": None,
            "provider_implemented": False,
            "connection_attempted": False,
            "network_request_made": False,
            "production_selected": False,
            "market_data_validated": False,
        }

    result: dict[str, Any] = {
        "contract_status": contract_status,
        "uat_opt_in": uat_opt_in,
        "realtime_enabled": realtime_enabled,
        "live_trading_enabled": live_trading_enabled,
        "provider_implemented": False,
        "connection_attempted": False,
        "network_request_made": False,
        "production_selected": False,
        "market_data_validated": False,
    }

    if realtime_enabled or live_trading_enabled:
        result.update({
            "status": "CHOICE_FINX_FAILED",
            "reason": "Safety configuration is not fail-closed: realtime and live trading must both be disabled.",
        })
    elif contract_status == "PARTIAL":
        result.update({
            "status": "CHOICE_FINX_DOCUMENTATION_INCOMPLETE",
            "reason": (
                "Official documentation does not define a quote subscription/tick schema. "
                "No UAT test or network request was attempted, including when UAT is explicitly selected."
            ),
        })
    elif contract_status == "FAIL":
        result.update({
            "status": "CHOICE_FINX_FAILED",
            "reason": "The official documentation review reports no usable market-data contract.",
        })
    elif uat_opt_in:
        result.update({
            "status": "CHOICE_FINX_FAILED",
            "reason": "Documentation is marked complete, but no reviewed Choice provider/UAT test is implemented.",
        })
    else:
        result.update({
            "status": "CHOICE_FINX_NOT_CONFIGURED",
            "reason": "No UAT opt-in was requested; this is a local documentation/configuration check only.",
        })
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check the Choice FinX market-data contract and safety config without connecting."
    )
    parser.add_argument("--contract", default="docs/CHOICE_FINX_MARKET_DATA_CONTRACT.md")
    parser.add_argument("--config", default="config/phase3d_personal.yaml")
    args = parser.parse_args(argv)

    def rooted(value: str) -> Path:
        path = Path(value)
        return path if path.is_absolute() else ROOT / path

    # This is the only Choice-related environment input. Credentials and tokens are
    # intentionally neither read nor printed; even this opt-in cannot bypass the
    # documentation gate or cause a connection from this script.
    uat_opt_in = os.environ.get("CHOICE_FINX_UAT", "").strip().lower() == "true"
    result = evaluate_gate(rooted(args.contract), rooted(args.config), uat_opt_in=uat_opt_in)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0 if result["status"] == "CHOICE_FINX_NOT_CONFIGURED" else (
        2 if result["status"] == "CHOICE_FINX_DOCUMENTATION_INCOMPLETE" else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
