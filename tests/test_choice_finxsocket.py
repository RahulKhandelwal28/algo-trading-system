import json
import importlib.util
import socket
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs/CHOICE_FINX_MARKET_DATA_CONTRACT.md"
CONFIG = ROOT / "config/phase3d_personal.yaml"
SPEC = importlib.util.spec_from_file_location(
    "test_choice_finxsocket_script", ROOT / "scripts/test_choice_finxsocket.py"
)
SCRIPT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCRIPT)
evaluate_gate = SCRIPT.evaluate_gate
main = SCRIPT.main


def test_partial_contract_is_documentation_only_and_fails_closed(tmp_path):
    safe_config = tmp_path / "choice_finx_disabled_config.yaml"
    safe_config.write_text("realtime:\n  enabled: false\nlive_trading:\n  enabled: false\n", encoding="utf-8")
    result = evaluate_gate(CONTRACT, safe_config)

    assert result["status"] == "CHOICE_FINX_DOCUMENTATION_INCOMPLETE"
    assert result["contract_status"] == "PARTIAL"
    assert result["realtime_enabled"] is False
    assert result["live_trading_enabled"] is False
    assert result["provider_implemented"] is False
    assert result["connection_attempted"] is False
    assert result["network_request_made"] is False
    assert result["production_selected"] is False
    assert result["market_data_validated"] is False


def test_explicit_uat_opt_in_still_cannot_bypass_incomplete_contract(monkeypatch, capsys, tmp_path):
    def forbidden(*args, **kwargs):
        raise AssertionError("the documentation gate must never open a network connection")

    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setenv("CHOICE_FINX_UAT", "true")

    safe_config = tmp_path / "choice_finx_disabled_config.yaml"
    safe_config.write_text("realtime:\n  enabled: false\nlive_trading:\n  enabled: false\n", encoding="utf-8")
    exit_code = main(["--config", str(safe_config)])
    result = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert result["status"] == "CHOICE_FINX_DOCUMENTATION_INCOMPLETE"
    assert result["uat_opt_in"] is True
    assert result["connection_attempted"] is False
    assert result["network_request_made"] is False


def test_unknown_contract_status_fails_closed(tmp_path):
    contract = tmp_path / "contract.md"
    contract.write_text("No status marker\n", encoding="utf-8")

    result = evaluate_gate(contract, CONFIG)

    assert result["status"] == "CHOICE_FINX_FAILED"
    assert result["contract_status"] == "UNKNOWN"
    assert result["network_request_made"] is False


def test_non_disabled_safety_config_fails_closed(tmp_path):
    config = tmp_path / "config.yaml"
    config.write_text("realtime:\n  enabled: true\nlive_trading:\n  enabled: false\n", encoding="utf-8")

    result = evaluate_gate(CONTRACT, config)

    assert result["status"] == "CHOICE_FINX_FAILED"
    assert result["network_request_made"] is False
    assert "both be disabled" in result["reason"]
