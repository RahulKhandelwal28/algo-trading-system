from __future__ import annotations

import csv
import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import yaml

from nse_research.config import AppConfig
from nse_research.dashboard import dashboard_payload, make_handler


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)


def _make_project(root: Path, *, next_day_available: bool = False) -> tuple[Path, Path]:
    config_data = AppConfig.load(PROJECT_ROOT / "config/phase3d_personal.yaml").model_dump(mode="json")
    # Dashboard read-only tests need an explicitly unconfigured runtime profile;
    # the personal paper profile intentionally opts into the five-symbol Jugaad monitor.
    config_data["realtime"].update({"enabled": False, "provider": ""})
    config_data["forward_paper"].update({
        "database": "results/phase3d_forward_test/paper_ledger.sqlite",
        "daily_data_dir": "data/paper_trading/normalized",
        "index_data_dir": "data/nse_exports",
        "identity_file": "data/paper_trading/universe.csv",
        "trading_calendar_path": "data/paper_trading/holidays.csv",
        "output_root": "results/phase3d_forward_test",
        "experiment_manifest_path": "results/phase3d_forward_test/experiment_manifest.json",
    })
    config_path = root / "config/phase3d_personal.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.safe_dump(config_data), encoding="utf-8")

    out = root / "results/phase3d_forward_test"
    out.mkdir(parents=True, exist_ok=True)
    manifest = {
        "experiment_id": "phase3d_forward_test", "provider": "personal_local_eod", "status": "IN_PROGRESS",
        "start_date": "2026-09-25", "end_date": "2026-10-27", "target_sessions": 21,
        "processed_session_count": 1, "processed_sessions": ["2026-09-25"],
        "last_processed_date": "2026-09-25", "live_trading_enabled": False, "broker_contacted": False,
        "blocked_sessions": [],
    }
    (out / "experiment_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    next_rows = [["2026-09-28", "100", "102", "99", "101", "1000", "100"]] if next_day_available else []
    _write_csv(root / "data/nse_exports/index_NSEI.csv",
               ["Date", "Open", "High", "Low", "Close", "Shares Traded", "Turnover"],
               [["2026-09-25", "100", "102", "99", "101", "1000", "100"], *next_rows])
    _write_csv(root / "data/nse_exports/index_INDIAVIX.csv",
               ["Date", "Open", "High", "Low", "Close", "Prev. Close", "Change", "% Change"],
               [["2026-09-25", "20", "21", "19", "20", "20", "0", "0"],
                *([["2026-09-28", "20", "21", "19", "20", "20", "0", "0"]] if next_day_available else [])])
    if next_day_available:
        _write_csv(root / "data/paper_trading/normalized/2026-09-28.csv",
                   ["Symbol", "Series", "Date", "Open", "High", "Low", "Close", "Volume"],
                   [["TEST", "EQ", "2026-09-28", "100", "102", "99", "101", "1000"]])
    _write_csv(root / "data/paper_trading/universe.csv",
               ["Symbol", "Series"], [["TEST", "EQ"]])
    (out / "data_manifest.json").write_text(json.dumps({
        "equity_source": {"provider": "Authorized sample records", "path": "data/cache",
                          "first_raw_date": "2026-09-25", "last_raw_date": "2026-09-25",
                          "eligible_symbols": 1, "accepted_official_session_rows": 1},
        "official_indexes": {
            "NIFTY": {"source": "official NSE CSV export", "source_url": "https://example.invalid/nifty"},
            "INDIA_VIX": {"source": "official NSE CSV export", "source_url": "https://example.invalid/vix"},
        },
        "coverage_window": {"start": "2026-09-25", "end": "2026-09-25", "expected_daily_symbols": 1},
        "data_gaps_policy": "No fill or synthesis.",
    }), encoding="utf-8")
    return config_path, out / "paper_ledger.sqlite"


def _make_ledger(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE portfolio_snapshots (
                snapshot_id INTEGER PRIMARY KEY, trading_date TEXT, equity REAL, daily_pnl REAL,
                drawdown REAL, maximum_drawdown REAL, gross_exposure REAL, net_exposure REAL
            );
            INSERT INTO portfolio_snapshots VALUES (1, '2026-09-25', 100000, 0, 0, 0, 0, 0);
            CREATE TABLE positions (status TEXT);
            CREATE TABLE orders (status TEXT);
            INSERT INTO orders VALUES ('CREATED');
        """)


def _payload(root: Path, database: Path, config_path: Path, endpoint: str):
    return dashboard_payload(database, endpoint, project_root=root, config_path=config_path)


def test_health_reports_next_session_blockers_and_does_not_mutate_ledger(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    before = hashlib.sha256(database.read_bytes()).hexdigest()

    health = _payload(tmp_path, database, config_path, "/health")
    experiment = _payload(tmp_path, database, config_path, "/api/dashboard/experiment")

    assert health["status"] == "ok"
    assert health["mode"] == "PAPER / RESEARCH ONLY"
    assert health["live_trading_enabled"] is False
    assert health["broker_connected"] is False
    assert health["dashboard_read_only"] is True
    assert experiment["progress"] == "1/21"
    assert experiment["last_processed_date"] == "2026-09-25"
    assert experiment["next_required_session"] == "2026-09-28"
    assert len(experiment["block_reasons"]) == 3
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_data_sources_report_official_index_coverage_and_explicit_eq_universe(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)

    sources = _payload(tmp_path, database, config_path, "/api/dashboard/data-sources")

    assert sources["equity"]["configured_provider"] == "personal_local_eod"
    assert sources["indexes"]["NIFTY"]["source"] == "official NSE CSV export"
    assert sources["indexes"]["NIFTY"]["last_date"] == "2026-09-25"
    assert sources["indexes"]["NIFTY"]["required_session_present"] is False
    assert sources["indexes"]["INDIA_VIX"]["required_session_present"] is False
    assert sources["frozen_universe"]["symbols"] == 1
    assert sources["frozen_universe"]["eq_symbols"] == 1
    assert sources["frozen_universe"]["status"] == "VALIDATED EXPLICIT EQ ROSTER"


def test_all_required_next_date_files_present_still_requires_paper_preflight(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path, next_day_available=True)

    experiment = _payload(tmp_path, database, config_path, "/api/dashboard/experiment")
    sources = _payload(tmp_path, database, config_path, "/api/dashboard/data-sources")

    assert experiment["next_session_status"] == "DATA FILES PRESENT / PAPER PREFLIGHT REQUIRED"
    assert experiment["block_reasons"] == []
    assert sources["equity"]["next_date_file_present"] is True
    assert sources["indexes"]["NIFTY"]["required_session_present"] is True
    assert sources["indexes"]["INDIA_VIX"]["required_session_present"] is True


def test_risk_view_uses_config_and_read_only_ledger(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    before = hashlib.sha256(database.read_bytes()).hexdigest()

    risk = _payload(tmp_path, database, config_path, "/api/dashboard/risk")

    assert risk["configured_limits"]["risk_per_trade"] == 0.01
    assert risk["configured_limits"]["max_open_positions"] == 5
    assert risk["observed_state"]["trading_date"] == "2026-09-25"
    assert risk["observed_state"]["open_positions"] == 0
    assert risk["observed_state"]["orders_by_status"] == [{"status": "CREATED", "count": 1}]
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_research_view_reads_phase3c_results_and_report_catalog(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)
    run = tmp_path / "results/phase3c_kaggle_20260926_000000"
    run.mkdir(parents=True)
    (run / "run_manifest.json").write_text(json.dumps({
        "run_id": "20260926_000000", "status": "RESULTS_NOT_ROBUST", "provider": "kaggle",
        "dataset_name": "Kaggle NIFTY 500", "dataset_date_range": {"start": "2021-01-01", "end": "2026-01-01"},
        "symbols_used": 500, "strategies_run": ["momentum"], "walk_forward_windows": 5,
        "status_reason": "Existing test outcome.",
    }), encoding="utf-8")
    (run / "monte_carlo_results.csv").write_text(
        "strategy,status,simulations_run\nmomentum,COMPUTED_5000_SIMULATIONS,5000\n", encoding="utf-8")
    (run / "PHASE3C_KAGGLE_REPORT.md").write_text("Existing report", encoding="utf-8")

    research = _payload(tmp_path, database, config_path, "/api/dashboard/research")

    assert research["phase3c"]["status"] == "RESULTS_NOT_ROBUST"
    assert research["phase3c"]["walk_forward_windows"] == 5
    assert research["phase3c"]["monte_carlo"]["simulations_run"] == 5000
    assert any(report["href"] == "/research-report/phase3c-kaggle" for report in research["reports"])


def test_upstox_dashboard_reports_not_configured_without_token_and_stays_read_only(tmp_path: Path, monkeypatch) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    monkeypatch.setenv("REALTIME_ENABLED", "true")
    monkeypatch.setenv("REALTIME_PROVIDER", "upstox")
    monkeypatch.setenv("REALTIME_UNIVERSE_SIZE", "100")
    monkeypatch.delenv("UPSTOX_ACCESS_TOKEN", raising=False)

    realtime = _payload(tmp_path, database, config_path, "/api/dashboard/realtime")

    assert realtime["status_code"] == "REALTIME_PROVIDER_NOT_CONFIGURED"
    assert realtime["feed_status"] == "DISCONNECTED"
    assert realtime["enabled"] is True
    assert realtime["universe_size"] == 100
    assert realtime["mode"] == "LTPC"
    assert "UPSTOX_ACCESS_TOKEN is not present" in realtime["reason"]
    assert realtime["source_discovery"]["provider"].startswith("Indian API")
    assert "not empirically verified" in realtime["source_discovery"]["source_classification"]
    assert "underlying data rights unverified" in realtime["source_discovery"]["access_status"]
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_intraday_scanner_dashboard_is_unavailable_read_only_and_does_not_start_work(tmp_path: Path, monkeypatch) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    universe_copy = tmp_path / "results/phase4a8a_upstox_live_universe_100.csv"
    shutil.copyfile(PROJECT_ROOT / "results/phase4a8a_upstox_live_universe_100.csv", universe_copy)
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    for name in ("INTRADAY_SCANNER_ENABLED", "INTRADAY_UNIVERSE_SIZE", "INTRADAY_TIMEFRAMES", "REGIME_FILTER_ENABLED"):
        monkeypatch.delenv(name, raising=False)

    payload = _payload(tmp_path, database, config_path, "/api/dashboard/intraday-scanner")

    assert payload["status"] == "INTRADAY_DATA_UNAVAILABLE"
    assert payload["scanner_status"] == "BLOCKED_NO_VALIDATED_INTRADAY_DATA"
    assert payload["scanner_started"] is False
    assert payload["configured_enabled"] is False
    assert payload["data_source"] is None
    assert payload["universe_size"] == 100
    assert payload["health"]["symbols_expected"] == 100
    assert payload["health"]["bars_received"] == 0
    assert payload["signals"] == []
    assert payload["orders"] == 0
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_phase4g_dashboard_separates_prepared_from_run_and_reads_without_mutation(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    db_before = hashlib.sha256(database.read_bytes()).hexdigest()
    output = tmp_path / "results/phase4g_yf_20260928"
    (output / "reports").mkdir(parents=True)
    manifest = {
        "status": "PHASE4G_YF_PREPARED_NOT_RUN", "session_date": "2026-09-28",
        "data_acquisition": "NOT_STARTED", "paper_only": True,
        "broker_calls": 0, "live_orders": 0,
    }
    preflight = {
        "status": "PHASE4G_YF_PREPARED_NOT_RUN", "session_date": "2026-09-28",
        "calendar_check": "TRADING_DAY", "reasons": [], "symbols_expected": 486,
        "network_calls": 0, "ledger_mutations": 0,
    }
    (output / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (output / "reports/preflight.json").write_text(json.dumps(preflight), encoding="utf-8")
    preflight_before = hashlib.sha256((output / "reports/preflight.json").read_bytes()).hexdigest()

    payload = _payload(tmp_path, database, config_path, "/api/dashboard/phase4g-yf")

    assert payload["status"] == "READY"
    assert payload["execution_status"] == "PHASE4G_YF_PREPARED_NOT_RUN"
    assert payload["session_date"] == "2026-09-28"
    assert payload["data_acquired"] is False
    assert payload["data_classification"] == "THIRD_PARTY_RESEARCH_SOURCE"
    assert payload["authorized_nse_feed"] is False
    assert payload["universe_expected"] == 486
    assert payload["signals"] is None and payload["paper_orders"] is None and payload["fills"] is None
    assert payload["paper_only"] is True and payload["broker_calls"] == payload["live_orders"] == 0
    assert payload["dashboard_read_only"] is True
    assert hashlib.sha256(database.read_bytes()).hexdigest() == db_before
    assert hashlib.sha256((output / "reports/preflight.json").read_bytes()).hexdigest() == preflight_before


def test_phase4g_monitor_dashboard_reads_monitor_artifacts_without_mutation(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    db_before = hashlib.sha256(database.read_bytes()).hexdigest()
    output = tmp_path / "results/phase4g_yf_20260928_monitor"
    (output / "reports").mkdir(parents=True)
    (output / "canonical").mkdir()
    (output / "orders").mkdir()
    (output / "positions").mkdir()
    (output / "manifest.json").write_text(json.dumps({
        "status": "PHASE4G_YF_COLLECTING", "monitor_status": "PAPER_MONITORING_ACTIVE",
        "monitor_mode": True,
        "session_date": "2026-09-28", "last_refresh_at": "2026-09-28T11:52:13+05:30",
        "frozen_universe_size": 486, "refresh_interval_seconds": 60,
        "latest_refresh_accounting": {
            "rows_seen": 93145, "unique_market_bars": 93145, "new_market_bars": 486,
            "duplicate_market_bars": 92659, "duplicate_conflict": 0, "late_bars": 2,
            "eligible_bars": 92500, "symbols_with_valid_session_data": 486,
            "symbols_with_data": 480, "symbols_with_new_bars": 486,
            "symbols_with_refresh_failure": 6, "warmup_bars": 73798,
            "current_session_new_bars": 486, "invalid_rows": 0,
        },
        "paper_only": True, "live_trading_enabled": False, "broker": "NONE",
        "broker_calls": 0, "live_orders": 0,
    }), encoding="utf-8")
    (output / "reports/acquisition_report.json").write_text(json.dumps({
        "latest_snapshot": {"successful_symbols": ["TEST"], "missing_symbols": [],
                            "invalid_symbols": [], "duplicate_symbols": []},
    }), encoding="utf-8")
    _write_csv(output / "canonical/forward_bars_1m.csv", ["timestamp", "received_timestamp"], [
        ["2026-09-28T11:51:00+05:30", "2026-09-28T11:52:00+05:30"],
    ])
    _write_csv(output / "orders/orders.csv", ["order_id", "status"], [["O-1", "PENDING"]])
    _write_csv(output / "positions/open_positions.csv", ["position_id"], [])
    _write_csv(output / "rankings/candidate_rankings.csv",
               ["decision_timestamp", "rank", "symbol", "score"],
               [["2026-09-28T11:52:00+05:30", 1, "TEST", 0.75]])
    (output / "reports/signal_audit.json").write_text(json.dumps([{
        "audit_id": "signal-1", "decision_timestamp": "2026-09-28T11:52:13+05:30",
        "symbol": "TEST", "strategy": "vwap_pullback", "signal": "BUY",
        "market_timestamp": "2026-09-28T11:51:00+05:30",
        "received_timestamp": "2026-09-28T11:52:00+05:30", "timestamp_proof": True,
        "entry": 100, "stop_loss": 95, "target": 107.5, "quantity": 200,
        "risk_reward": 1.5, "paper_order_id": "O-1", "paper_order_status": "PENDING",
    }]), encoding="utf-8")

    payload = _payload(tmp_path, database, config_path, "/api/dashboard/phase4g-yf")
    monitor = payload["monitor"]
    assert monitor["status"] == "PAPER MONITORING"
    assert monitor["symbols_valid"] == 1
    assert monitor["symbols_with_valid_session_data"] == 486
    assert monitor["symbols_with_current_response_data"] == 480
    assert monitor["symbols_with_new_bars"] == 486
    assert monitor["symbols_with_refresh_failure"] == 6
    assert monitor["unique_market_bars"] == 93145
    assert monitor["new_market_bars"] == 486
    assert monitor["duplicate_market_bars"] == 92659
    assert monitor["late_bars_excluded"] == 2
    assert monitor["top_candidates"][0]["signal"] == "WATCH"
    assert monitor["buy_signals_today"] == 1
    assert monitor["buy_signals"][0]["status"] == "PENDING"
    assert monitor["nse_authorization"] == "NOT_CLAIMED"
    assert monitor["dashboard_read_only"] is True
    assert hashlib.sha256(database.read_bytes()).hexdigest() == db_before


def test_dashboard_never_serializes_configured_upstox_token(tmp_path: Path, monkeypatch) -> None:
    config_path, database = _make_project(tmp_path)
    secret = "dashboard-token-must-not-appear"
    monkeypatch.setenv("REALTIME_ENABLED", "true")
    monkeypatch.setenv("REALTIME_PROVIDER", "upstox")
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", secret)

    realtime = _payload(tmp_path, database, config_path, "/api/dashboard/realtime")

    assert secret not in json.dumps(realtime)
    assert realtime["quotes"] == []


def test_dashboard_hides_snapshot_from_a_different_realtime_universe(tmp_path: Path, monkeypatch) -> None:
    config_path, database = _make_project(tmp_path)
    health_path = tmp_path / "data/realtime/feed_health.json"
    health_path.parent.mkdir(parents=True, exist_ok=True)
    health_path.write_text(json.dumps({
        "provider": "upstox",
        "feed_status": "CONNECTED",
        "provider_details": {"universe_size": 486, "mapped_symbols": 486},
    }), encoding="utf-8")
    monkeypatch.setenv("REALTIME_ENABLED", "true")
    monkeypatch.setenv("REALTIME_PROVIDER", "upstox")
    monkeypatch.setenv("REALTIME_UNIVERSE_SIZE", "100")
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "private-token")

    realtime = _payload(tmp_path, database, config_path, "/api/dashboard/realtime")

    assert realtime["status_code"] == "REALTIME_UNIVERSE_MISMATCH"
    assert realtime["feed_status"] == "DISCONNECTED"
    assert realtime["quotes"] == []
    assert "private-token" not in json.dumps(realtime)


def test_realtime_dashboard_endpoint_is_read_only_and_shows_unconfigured_without_fake_quotes(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    before = hashlib.sha256(database.read_bytes()).hexdigest()

    realtime = _payload(tmp_path, database, config_path, "/api/dashboard/realtime")
    health = _payload(tmp_path, database, config_path, "/health")

    assert realtime["status_label"] == "REALTIME FEED: NOT CONFIGURED"
    assert realtime["status_code"] == "DATA_SOURCE_NOT_CONFIGURED"
    assert realtime["quotes"] == []
    assert realtime["candles"] == []
    assert realtime["live_trading_enabled"] is False
    assert realtime["broker_execution_enabled"] is False
    assert realtime["signals_generated"] is False
    assert realtime["ledger_mutated"] is False
    assert health["realtime_feed_status"] == "REALTIME FEED: NOT CONFIGURED"
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before


def test_realtime_http_route_loads_and_rejects_mutation_methods(tmp_path: Path) -> None:
    config_path, database = _make_project(tmp_path)
    _make_ledger(database)
    before = hashlib.sha256(database.read_bytes()).hexdigest()
    handler = make_handler(database, project_root=tmp_path, config_path=config_path)

    class RequestHarness:
        path = "/api/dashboard/realtime"
        result = None

        def _send(self, status, content, content_type, *, head_only=False):
            self.result = (status, content, content_type)

    harness = RequestHarness()
    handler._handle_request(harness, head_only=False)
    status, body, content_type = harness.result
    payload = json.loads(body)
    assert status == 200
    assert content_type.startswith("application/json")
    assert payload["status_label"] == "REALTIME FEED: NOT CONFIGURED"
    handler.do_POST(harness)
    assert harness.result[0] == 405
    assert hashlib.sha256(database.read_bytes()).hexdigest() == before
