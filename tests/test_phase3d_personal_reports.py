from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from nse_research.forward.baseline import PHASE3C_DAILY_STRATEGIES
from nse_research.forward.reports import write_personal_experiment_exports


def _ledger(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO settings VALUES('starting_capital','100000');
            CREATE TABLE portfolio_snapshots(
                trading_date TEXT, snapshot_id INTEGER, starting_capital REAL, cash REAL,
                realized_pnl REAL, unrealized_pnl REAL, equity REAL, daily_pnl REAL,
                cumulative_return REAL, drawdown REAL, maximum_drawdown REAL,
                transaction_costs REAL, open_positions INTEGER, sector_exposure_json TEXT
            );
            INSERT INTO portfolio_snapshots VALUES(
                '2026-09-25',1,100000,100000,0,0,100000,0,0,0,0,0,0,'{}'
            );
            CREATE TABLE signals(
                signal_id TEXT, trading_date TEXT, timestamp TEXT, symbol TEXT, strategy TEXT,
                accepted INTEGER, status TEXT, payload_json TEXT
            );
            INSERT INTO signals VALUES('s1','2026-09-25','2026-09-25T10:00:00','AAA.NS','trend_following',0,'REJECTED','{}');
            CREATE TABLE orders(
                order_id TEXT, signal_id TEXT, created_at TEXT, trading_date TEXT, symbol TEXT, side TEXT,
                quantity INTEGER, intended_price REAL, raw_signal_price REAL, requested_price REAL,
                simulated_fill_price REAL, slippage REAL, filled_at TEXT, brokerage REAL, stt REAL,
                exchange_charges REAL, gst REAL, sebi_charges REAL, stamp_duty REAL, dp_charge REAL,
                costs REAL, status TEXT, strategy TEXT, order_kind TEXT
            );
            INSERT INTO orders VALUES('o1','s1','2026-09-25T10:00:00','2026-09-25','AAA.NS','BUY',1,100,100,100,NULL,NULL,NULL,0,0,0,0,0,0,0,0,'CREATED','trend_following','ENTRY');
            CREATE TABLE positions(
                position_id TEXT, signal_id TEXT, entry_order_id TEXT, exit_order_id TEXT, symbol TEXT,
                strategy TEXT, direction INTEGER, sector TEXT, quantity INTEGER, entry_price REAL,
                entry_date TEXT, entry_time TEXT, stop_loss REAL, target REAL, entry_costs REAL,
                total_costs REAL, mark_price REAL, unrealized_pnl REAL, status TEXT, exit_price REAL,
                exit_date TEXT, realized_pnl REAL, holding_sessions INTEGER
            );
            CREATE TABLE daily_journal(trading_date TEXT, payload_json TEXT);
            INSERT INTO daily_journal VALUES(
                '2026-09-25',
                '{"full_report":{"data":{"provider":"personal_local_eod","symbols_received":490},"market":{"nifty_date":"2026-09-25","india_vix_date":"2026-09-25"}}}'
            );
            """
        )


def test_personal_exports_write_daily_and_root_artifacts_without_early_month_end(tmp_path):
    database = tmp_path / "paper.sqlite"
    _ledger(database)
    output = tmp_path / "results" / "phase3d_forward_test"
    output.mkdir(parents=True)
    manifest_path = output / "experiment_manifest.json"
    manifest_path.write_text(json.dumps({
        "experiment_id": "test", "status": "READY_WAITING_FOR_DATA",
        "processed_sessions": [], "blocked_sessions": [], "config_sha256": "config-hash",
        "baseline_name": "PHASE3D_PERSONAL_BASELINE",
    }), encoding="utf-8")
    date_report = output / "2026-09-25"
    date_report.mkdir()
    (date_report / "daily_report.md").write_text("date report", encoding="utf-8")

    summary = write_personal_experiment_exports(
        database,
        output,
        experiment_manifest_path=manifest_path,
        daily_manifest_dir=tmp_path / "manifests",
        target_sessions=21,
    )
    assert summary["status"] == "IN_PROGRESS"
    assert summary["sessions_processed"] == 1
    assert "strategy_ranking" not in summary
    assert len(summary["strategy_results"]) == len(PHASE3C_DAILY_STRATEGIES) == 11
    for filename in ("daily_snapshots.csv", "signals.csv", "orders.csv", "fills.csv", "positions.csv",
                     "trades.csv", "costs.csv", "events.jsonl", "summary.json", "REPORT.md"):
        assert (output / filename).is_file()
    assert (output / "daily" / "2026-09-25" / "daily_report.md").read_text(encoding="utf-8") == "date report"
    assert not (output / "PHASE3D_FORWARD_REPORT.md").exists()
    report = (output / "REPORT.md").read_text(encoding="utf-8")
    assert "No strategy ranking" in report
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["processed_sessions"] == ["2026-09-25"]


def test_blocked_acquisition_does_not_increment_processed_session_count(tmp_path):
    database = tmp_path / "paper.sqlite"
    _ledger(database)
    output = tmp_path / "results" / "phase3d_forward_test"
    output.mkdir(parents=True)
    manifest_path = output / "experiment_manifest.json"
    blocked = {"date": "2026-09-28", "status": "WAITING_FOR_2026_09_28_EOD_DATA"}
    manifest_path.write_text(json.dumps({
        "experiment_id": "test", "processed_sessions": ["2026-09-25"],
        "blocked_sessions": [blocked], "config_sha256": "config-hash",
        "baseline_name": "PHASE3D_PERSONAL_BASELINE",
    }), encoding="utf-8")
    write_personal_experiment_exports(
        database,
        output,
        experiment_manifest_path=manifest_path,
        daily_manifest_dir=tmp_path / "manifests",
        target_sessions=21,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["processed_session_count"] == 1
    assert manifest["processed_sessions"] == ["2026-09-25"]
    assert manifest["blocked_sessions"] == [blocked]


def test_personal_export_only_emits_final_report_when_target_session_count_met(tmp_path):
    database = tmp_path / "paper.sqlite"
    _ledger(database)
    output = tmp_path / "results" / "phase3d_forward_test"
    output.mkdir(parents=True)
    manifest = output / "experiment_manifest.json"
    manifest.write_text(json.dumps({"experiment_id": "test", "blocked_sessions": [], "config_sha256": "x"}), encoding="utf-8")
    summary = write_personal_experiment_exports(
        database,
        output,
        experiment_manifest_path=manifest,
        daily_manifest_dir=tmp_path / "manifests",
        target_sessions=1,
    )
    assert summary["status"] == "COMPLETE"
    assert (output / "PHASE3D_FORWARD_REPORT.md").is_file()
    report = (output / "PHASE3D_FORWARD_REPORT.md").read_text(encoding="utf-8")
    for strategy in PHASE3C_DAILY_STRATEGIES:
        assert strategy in report
