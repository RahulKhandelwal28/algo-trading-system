"""Forward-only daily paper workflow using the existing strategy/risk scan."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.config import AppConfig
from nse_research.data.providers import HistoricalDataProvider
from nse_research.data.universe import load_sector_map
from nse_research.forward.baseline import validate_baseline_manifest
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.forward.data import DataCheck, validate_daily_bars
from nse_research.forward.reports import (
    update_cumulative_research,
    write_daily_artifacts,
    write_personal_experiment_exports,
)
from nse_research.indicators.features import add_features, align_regime
from nse_research.risk.controls import position_size
from nse_research.scanner.scanner import config_cost_slippage, scan_universe
from nse_research.strategies.base import Candidate
from nse_research.strategies.equity import EquityStrategy, build_strategies


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if value is None or isinstance(value, (str, int, bool)):
        return value
    return str(value)


def _json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, default=_json_default, allow_nan=False)


def _cost_fields(breakdown: Any) -> dict[str, float]:
    """Map the shared cost model breakdown to the persisted paper-order schema."""
    return {
        "brokerage": float(breakdown.brokerage),
        "stt": float(breakdown.stt),
        "exchange_charges": float(breakdown.exchange),
        "gst": float(breakdown.gst),
        "sebi_charges": float(breakdown.sebi),
        "stamp_duty": float(breakdown.stamp),
        "dp_charge": float(breakdown.dp),
        "costs": float(breakdown.total),
    }


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(_json(payload) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _ensure_schema(database: Path, starting_capital: float) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS signals (
                signal_id TEXT PRIMARY KEY, trading_date TEXT NOT NULL, timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL, strategy TEXT NOT NULL, direction TEXT,
                signal_price REAL, stop_loss REAL, target REAL, atr REAL,
                position_size INTEGER, risk_amount REAL, risk_percentage REAL,
                reason TEXT NOT NULL, features_json TEXT NOT NULL, market_regime TEXT,
                sector TEXT NOT NULL DEFAULT 'UNKNOWN',
                data_provider TEXT NOT NULL, freshness_status TEXT NOT NULL,
                accepted INTEGER NOT NULL, status TEXT NOT NULL, rejection_reason TEXT,
                payload_json TEXT NOT NULL,
                UNIQUE(trading_date, symbol, strategy, direction)
            );
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL, created_at TEXT NOT NULL,
                trading_date TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
                quantity INTEGER NOT NULL, intended_price REAL NOT NULL,
                raw_signal_price REAL, requested_price REAL,
                simulated_fill_price REAL, slippage REAL, filled_at TEXT,
                brokerage REAL NOT NULL DEFAULT 0, stt REAL NOT NULL DEFAULT 0,
                exchange_charges REAL NOT NULL DEFAULT 0, gst REAL NOT NULL DEFAULT 0,
                sebi_charges REAL NOT NULL DEFAULT 0, stamp_duty REAL NOT NULL DEFAULT 0,
                dp_charge REAL NOT NULL DEFAULT 0, costs REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL, strategy TEXT NOT NULL, order_kind TEXT NOT NULL,
                FOREIGN KEY(signal_id) REFERENCES signals(signal_id)
            );
            CREATE TABLE IF NOT EXISTS order_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL,
                signal_id TEXT NOT NULL, timestamp TEXT NOT NULL, event TEXT NOT NULL,
                details_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS positions (
                position_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL, entry_order_id TEXT NOT NULL,
                exit_order_id TEXT, symbol TEXT NOT NULL, strategy TEXT NOT NULL,
                direction INTEGER NOT NULL, sector TEXT NOT NULL, quantity INTEGER NOT NULL,
                entry_price REAL NOT NULL, entry_date TEXT NOT NULL, entry_time TEXT NOT NULL,
                stop_loss REAL NOT NULL, target REAL NOT NULL, entry_costs REAL NOT NULL,
                total_costs REAL NOT NULL DEFAULT 0,
                mark_price REAL NOT NULL, unrealized_pnl REAL NOT NULL DEFAULT 0,
                status TEXT NOT NULL, exit_price REAL, exit_date TEXT,
                realized_pnl REAL, holding_sessions INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS position_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT, position_id TEXT NOT NULL,
                signal_id TEXT NOT NULL, timestamp TEXT NOT NULL, event TEXT NOT NULL,
                details_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
                trading_date TEXT NOT NULL, starting_capital REAL NOT NULL,
                cash REAL NOT NULL, invested_capital REAL NOT NULL, realized_pnl REAL NOT NULL,
                unrealized_pnl REAL NOT NULL, equity REAL NOT NULL, daily_pnl REAL NOT NULL,
                daily_return REAL NOT NULL, cumulative_return REAL NOT NULL,
                drawdown REAL NOT NULL, maximum_drawdown REAL NOT NULL,
                gross_exposure REAL NOT NULL, sector_exposure_json TEXT NOT NULL,
                open_positions INTEGER NOT NULL, pending_orders INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS daily_journal (
                run_id TEXT PRIMARY KEY, trading_date TEXT NOT NULL,
                payload_json TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS health_snapshots (
                run_id TEXT PRIMARY KEY, trading_date TEXT NOT NULL,
                payload_json TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(trading_date);
            CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
            CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
            CREATE INDEX IF NOT EXISTS idx_journal_date ON daily_journal(trading_date);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_signal_kind ON orders(signal_id,order_kind);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_positions_signal ON positions(signal_id);
            """
        )
        # Keep databases created by early Phase 3D builds usable if their schema
        # predates the sector field required by the existing portfolio risk scan.
        signal_columns = {row[1] for row in connection.execute("PRAGMA table_info(signals)")}
        if "sector" not in signal_columns:
            connection.execute("ALTER TABLE signals ADD COLUMN sector TEXT NOT NULL DEFAULT 'UNKNOWN'")
        snapshot_columns = {row[1] for row in connection.execute("PRAGMA table_info(portfolio_snapshots)")}
        for name in ("market_value", "net_exposure", "reserved_cash", "transaction_costs"):
            if name not in snapshot_columns:
                connection.execute(f"ALTER TABLE portfolio_snapshots ADD COLUMN {name} REAL NOT NULL DEFAULT 0")
        order_columns = {row[1] for row in connection.execute("PRAGMA table_info(orders)")}
        order_numeric_columns = (
            "raw_signal_price", "requested_price", "brokerage", "stt", "exchange_charges", "gst",
            "sebi_charges", "stamp_duty", "dp_charge", "costs",
        )
        for name in order_numeric_columns:
            if name not in order_columns:
                connection.execute(f"ALTER TABLE orders ADD COLUMN {name} REAL NOT NULL DEFAULT 0")
        for name in ("filled_at",):
            if name not in order_columns:
                connection.execute(f"ALTER TABLE orders ADD COLUMN {name} TEXT")
        position_columns = {row[1] for row in connection.execute("PRAGMA table_info(positions)")}
        if "total_costs" not in position_columns:
            connection.execute("ALTER TABLE positions ADD COLUMN total_costs REAL NOT NULL DEFAULT 0")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_trading_date ON portfolio_snapshots(trading_date)")
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_journal_trading_date ON daily_journal(trading_date)")
        configured = connection.execute("SELECT value FROM settings WHERE key='starting_capital'").fetchone()
        if configured is None:
            connection.execute("INSERT INTO settings(key,value) VALUES ('starting_capital',?)", (str(starting_capital),))
        elif not np.isclose(float(configured[0]), starting_capital):
            raise ValueError(
                f"database uses starting capital {configured[0]}; use a new forward database to change capital"
            )


def _normalize_symbol(symbol: str) -> str:
    value = str(symbol).strip().upper()
    if not value:
        return ""
    if value.startswith("^") or value.endswith(".NS"):
        return value
    return f"{value}.NS"


def load_local_universe(path: str | Path, max_symbols: int | None = None) -> list[str]:
    """Load only a local universe file. This function never refreshes it online."""
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"local universe file not found: {source}")
    frame = pd.read_csv(source)
    column = next((name for name in frame.columns if str(name).strip().lower() in {"symbol", "ticker"}), None)
    if column is None:
        raise ValueError("local universe CSV must contain Symbol or Ticker")
    symbols = [_normalize_symbol(value) for value in frame[column].dropna().astype(str)]
    symbols = [value for value in dict.fromkeys(symbols) if value and not value.startswith("DUMMY")]
    return symbols[:max_symbols] if max_symbols else symbols


class DailyPaperTrader:
    """Validate local EOD data, run unchanged strategies, and maintain paper state."""

    def __init__(
        self,
        config: AppConfig,
        provider: HistoricalDataProvider,
        database: str | Path,
        starting_capital: float,
        *,
        output_root: str | Path = "forward",
        provider_name: str | None = None,
        clock=lambda: datetime.now(timezone.utc),
        strategies: list[EquityStrategy] | None = None,
        baseline_manifest: str | Path | None = None,
        trading_calendar: str | Path | None = None,
    ) -> None:
        if starting_capital <= 0:
            raise ValueError("starting_capital must be configured as a positive amount")
        self.config = config
        self.provider = provider
        self.database = Path(database)
        self.starting_capital = float(starting_capital)
        self.output_root = Path(output_root)
        self.provider_name = provider_name or getattr(provider, "provider_name", type(provider).__name__)
        self.clock = clock
        self.strategies = strategies
        project_root = Path(__file__).resolve().parents[3]
        self.project_root = project_root
        self.baseline_manifest = Path(baseline_manifest) if baseline_manifest else Path(self.config.forward_paper.baseline_manifest_path)
        if not self.baseline_manifest.is_absolute():
            self.baseline_manifest = project_root / self.baseline_manifest
        configured_calendar = trading_calendar or self.config.forward_paper.trading_calendar_path
        calendar_path = Path(configured_calendar)
        self.trading_calendar_path = calendar_path if calendar_path.is_absolute() else project_root / calendar_path
        self.calendar = NseTradingCalendar(self.trading_calendar_path)
        self.event_root = self.database.parent / "events"
        self._active_run_id: str | None = None
        self._stage = "preflight"
        for folder in ("signals", "orders", "positions", "portfolio", "reports", "journal"):
            (self.output_root / folder).mkdir(parents=True, exist_ok=True)

    def run(self, market_date: date, symbols: Iterable[str]) -> dict[str, Any]:
        self._active_run_id = uuid4().hex
        self._stage = "preflight"
        now = self.clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        self._emit_event("RUN_STARTED", market_date, now, mode="PAPER TRADING", provider=self.provider_name)
        try:
            if not self.config.forward_paper.enabled:
                raise RuntimeError("forward paper trading is disabled in config")
            if os.getenv("PAPER_TRADING_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
                raise RuntimeError("PAPER_TRADING_ENABLED=true is required")
            live_mode = os.getenv("LIVE_TRADING_ENABLED", "").strip().lower()
            if self.config.live_trading.enabled or live_mode not in {"0", "false", "no", "off"}:
                raise RuntimeError("LIVE_TRADING_ENABLED must remain false; refusing to start forward workflow")
            order_methods = ("place", "place_order", "submit_order", "send_order", "create_live_order")
            if any(callable(getattr(self.provider, name, None)) for name in order_methods):
                raise RuntimeError("broker/live-order interface is configured on the market-data provider")
            validate_baseline_manifest(
                self.config,
                root=self.project_root,
                path=self.baseline_manifest,
                baseline_name=self.config.forward_paper.baseline_name,
            )
            self._emit_event("PREFLIGHT_PASSED", market_date, now,
                             baseline=self.config.forward_paper.baseline_name, live_trading_enabled=False,
                             broker_orders_enabled=False)
            self._stage = "session_detection"
            report = self._run_workflow(market_date, symbols, now)
        except Exception as error:
            self._emit_event(
                "RUN_FAILED",
                market_date,
                now,
                stage=self._stage,
                execution_status="EXECUTION_ERROR",
                error_type=type(error).__name__,
                error_message=str(error),
                retry_count=0,
            )
            raise
        if report.get("duplicate_run_skipped"):
            self._emit_event("RUN_SKIPPED_DUPLICATE", market_date, now, prior_run_id=report.get("run_id"))
        elif report.get("session_status") == "DATA_UNAVAILABLE":
            self._stage = "data_loading"
            self._emit_event("RUN_FAILED", market_date, now, stage="data_loading",
                             error_type="RequiredDataUnavailable",
                             error_message=report.get("session_reason", "required market data unavailable"),
                             retry_count=0)
        elif report.get("data", {}).get("system_data_status") != "FRESH" and report.get("session_status") == "TRADING_DAY":
            self._stage = "data_validation"
            self._emit_event("RUN_FAILED", market_date, now, stage="data_validation",
                             error_type="RequiredDataUnavailable",
                             error_message=report.get("data", {}).get("system_data_reason", "required data unavailable"),
                             retry_count=0)
        else:
            self._emit_event("RUN_COMPLETED", market_date, now, session_status=report.get("session_status", "TRADING_DAY"))
        manifest_setting = self.config.forward_paper.experiment_manifest_path
        if (manifest_setting and report.get("session_status") == "TRADING_DAY"
                and report.get("data", {}).get("system_data_status") == "FRESH"):
            manifest_path = Path(manifest_setting)
            if not manifest_path.is_absolute():
                manifest_path = self.project_root / manifest_path
            daily_manifest_dir = Path(self.config.forward_paper.daily_data_dir).parent / "manifests"
            if not daily_manifest_dir.is_absolute():
                daily_manifest_dir = self.project_root / daily_manifest_dir
            write_personal_experiment_exports(
                self.database,
                self.output_root,
                experiment_manifest_path=manifest_path,
                daily_manifest_dir=daily_manifest_dir,
                target_sessions=self.config.forward_paper.target_sessions or 21,
            )
        return report

    def _emit_event(self, event_type: str, market_date: date, now: datetime, **payload: Any) -> None:
        _append_jsonl(
            self.event_root / f"{market_date.isoformat()}.jsonl",
            {
                "event_type": event_type,
                "run_id": self._active_run_id,
                "market_date": market_date.isoformat(),
                "timestamp": now.astimezone(timezone.utc).isoformat(),
                "stage": self._stage,
                **payload,
            },
        )

    def _run_workflow(self, market_date: date, symbols: Iterable[str], now: datetime) -> dict[str, Any]:
        local_market_date = now.astimezone(ZoneInfo(self.config.project.timezone)).date()
        if market_date > local_market_date:
            raise ValueError(f"market date {market_date.isoformat()} is in the future")
        if self.config.forward_paper.experiment_start_date:
            start_date = date.fromisoformat(self.config.forward_paper.experiment_start_date)
            if market_date < start_date:
                raise ValueError(f"market date {market_date.isoformat()} is before the configured experiment start {start_date.isoformat()}")
        if self.config.forward_paper.experiment_end_date:
            end_date = date.fromisoformat(self.config.forward_paper.experiment_end_date)
            if market_date > end_date:
                raise ValueError(f"market date {market_date.isoformat()} is after the configured experiment end {end_date.isoformat()}")
        _ensure_schema(self.database, self.starting_capital)
        run_id = self._active_run_id or uuid4().hex
        previous_run = self._successful_report_for_date(market_date)
        if previous_run is not None:
            return {**previous_run, "duplicate_run_skipped": True}
        start = market_date - timedelta(days=self.config.backtest.warmup_calendar_days)
        requested_symbols = list(dict.fromkeys(_normalize_symbol(value) for value in symbols if str(value).strip()))
        if not requested_symbols:
            raise ValueError("the frozen Phase 3D universe contains no symbols")
        required_symbols = list(dict.fromkeys([
            *requested_symbols,
            self.config.universe.benchmark,
            self.config.regime.india_vix_symbol,
            *self._open_position_symbols(),
        ]))
        bars_by_symbol: dict[str, pd.DataFrame] = {}
        checks: dict[str, DataCheck] = {}
        observed_market_data = False
        self._stage = "data_loading"
        for symbol in required_symbols:
            provider_for = getattr(self.provider, "provider_for", lambda _symbol: self.provider_name)(symbol)
            try:
                raw = self.provider.history(symbol, start, market_date, "1d")
                if raw is not None and not raw.empty:
                    index = pd.to_datetime(raw.index, errors="coerce", format="mixed")
                    observed_market_data = observed_market_data or any(
                        pd.notna(stamp) and pd.Timestamp(stamp).date() == market_date for stamp in index
                    )
                bars, check = validate_daily_bars(
                    raw,
                    provider=provider_for,
                    symbol=symbol,
                    market_date=market_date,
                    retrieved_at=now,
                    max_daily_price_jump_fraction=self.config.forward_paper.max_daily_price_jump_fraction,
                )
                checks[symbol] = check
                if check.valid:
                    bars_by_symbol[symbol] = bars
            except FileNotFoundError as error:
                checks[symbol] = DataCheck(provider_for, now.isoformat(), market_date.isoformat(), symbol,
                                           None, "DATA_STALE", False, str(error))
            except Exception as error:
                checks[symbol] = DataCheck(provider_for, now.isoformat(), market_date.isoformat(), symbol,
                                           None, "DATA_INVALID", False, f"{type(error).__name__}: {error}")

        excluded_official_session_rows = getattr(self.provider, "excluded_non_nifty_rows", {})
        excluded_official_session_dates = getattr(self.provider, "excluded_non_nifty_dates", [])
        self._emit_event(
            "DATA_LOADED",
            market_date,
            now,
            provider=self.provider_name,
            dataset=getattr(self.provider, "dataset_name", self.provider_name),
            dataset_paths=self._dataset_paths(required_symbols),
            symbols_requested=requested_symbols,
            symbols_received=[symbol for symbol in requested_symbols if symbol in checks and checks[symbol].valid],
            symbols_missing=[symbol for symbol in requested_symbols if symbol not in checks or not checks[symbol].valid],
            data_timestamps={symbol: check.data_timestamp for symbol, check in checks.items()},
            excluded_non_nifty_session_rows=excluded_official_session_rows,
            excluded_non_nifty_session_dates=excluded_official_session_dates,
        )
        session = self.calendar.classify(market_date, observed_market_data=observed_market_data)
        self._stage = "session_detection"
        self._emit_event("SESSION_DETECTED", market_date, now,
                         status=session.status, reason=session.reason,
                         observed_market_data=observed_market_data)
        if session.status != "TRADING_DAY":
            event = "NON_TRADING_DAY" if session.status == "NON_TRADING_DAY" else "DATA_UNAVAILABLE"
            self._emit_event(event, market_date, now, reason=session.reason)
            report = self._write_non_session_report(market_date, now, session.status, session.reason, requested_symbols)
            return report

        health = self._health_payload(run_id, market_date, requested_symbols, required_symbols, checks)
        health["excluded_non_nifty_session_rows"] = excluded_official_session_rows
        health["excluded_non_nifty_session_dates"] = excluded_official_session_dates
        benchmark_symbol = self.config.universe.benchmark
        vix_symbol = self.config.regime.india_vix_symbol
        benchmark_check = checks.get(benchmark_symbol)
        vix_check = checks.get(vix_symbol)
        vix_fallback = bool(vix_check is None or (
            not vix_check.valid and vix_check.freshness_status == "DATA_STALE"
        ))
        core_failures = [check for check in (benchmark_check,) if check is None or not check.valid]
        if not vix_fallback and (vix_check is None or not vix_check.valid):
            core_failures.append(vix_check)
        benchmark = bars_by_symbol.get(benchmark_symbol)
        vix = bars_by_symbol.get(vix_symbol)
        health["india_vix_fallback"] = vix_fallback
        health["nifty_date_used"] = benchmark_check.data_timestamp[:10] if benchmark_check and benchmark_check.data_timestamp else None
        health["vix_date_used"] = vix_check.data_timestamp[:10] if vix_check and vix_check.valid and vix_check.data_timestamp else None
        health["vix_source"] = "official NSE India VIX" if not vix_fallback else "causal NIFTY realized_vol20 fallback"
        valid_equities = {
            symbol: bars_by_symbol[symbol]
            for symbol in requested_symbols
            if symbol in bars_by_symbol
        }

        required_equity_symbols = [
            symbol for symbol in required_symbols
            if symbol not in {self.config.universe.benchmark, self.config.regime.india_vix_symbol}
        ]
        equity_failures = [symbol for symbol in required_equity_symbols if symbol not in bars_by_symbol]
        all_required_available = not core_failures and not equity_failures
        if all_required_available:
            lifecycle = self._advance_orders_and_positions(market_date, now, bars_by_symbol)
        else:
            lifecycle = {"orders_filled": 0, "orders_exited": 0, "positions_opened": 0,
                         "positions_closed": 0, "events": []}
        before_scan = self._portfolio_state(market_date)
        if not core_failures:
            for symbol in requested_symbols:
                check = checks.get(symbol)
                if check is not None and not check.valid:
                    self._persist_data_block_signal(
                        run_id, market_date, now, symbol, check, check.freshness_status, check.reason
                    )
        scan_result: dict[str, Any] = {
            "signals_generated": 0, "signals_accepted": 0, "signals_rejected": 0,
            "orders_created": 0, "raw_signals": [], "scanner_rejections": [],
            "market_regime": "REGIME_DATA_UNAVAILABLE",
        }
        if core_failures or equity_failures:
            reason = "; ".join(
                f"{item.symbol}: {item.freshness_status} ({item.reason})" for item in core_failures if item
            )
            if equity_failures:
                reason = "; ".join(filter(None, [reason, "required equity symbols unavailable: " + ", ".join(equity_failures)]))
            relevant_checks = [checks.get(symbol) for symbol in [*equity_failures, benchmark_symbol, vix_symbol]]
            status = "DATA_STALE" if any(item and item.freshness_status == "DATA_STALE" for item in relevant_checks) else "DATA_INVALID"
            for symbol in requested_symbols:
                self._persist_data_block_signal(run_id, market_date, now, symbol, checks.get(symbol), status, reason)
            health["system_data_status"] = status
            health["system_data_reason"] = reason
            self._emit_event("DATA_VALIDATION_FAILED", market_date, now, reason=reason, system_data_status=status)
        elif not valid_equities:
            health["system_data_status"] = "DATA_STALE"
            health["system_data_reason"] = "no equity symbols have fresh, valid local EOD data"
            health["symbols_scanned"] = 0
        else:
            self._stage = "strategy_generation"
            self._emit_event("STRATEGY_GENERATION_STARTED", market_date, now,
                             strategy_count=len(self.strategies or self.config.strategies.enabled),
                             symbols=len(valid_equities))
            strategies = self.strategies or build_strategies(
                self.config.strategies.enabled,
                self.config.strategies.reward_risk,
                self.config.strategies.atr_stop_multiple,
                self.config.strategies.adx_minimum,
                self.config.strategies.volume_multiple,
                self.config.strategies.entry_cutoff,
                "1d",
                self.config.risk.allow_short_intraday,
                self.config.universe.min_avg_volume,
                self.config.universe.min_avg_traded_value,
                self.config.universe.min_price,
                self.config.strategies.stop_mode,
                self.config.strategies.signal_score_weights,
            )
            scan_result = self._scan(
                market_date, now, valid_equities, benchmark, vix, strategies,
                before_scan, checks,
            )
            health["symbols_scanned"] = len(valid_equities)
            health["symbols_scanned_set"] = list(valid_equities)
            health["system_data_status"] = "FRESH"
            health["system_data_reason"] = ""
            self._emit_event("DATA_VALIDATION_PASSED", market_date, now, symbols_scanned=len(valid_equities))
            self._emit_event("SIGNALS_GENERATED", market_date, now,
                             generated=scan_result["signals_generated"],
                             accepted=scan_result["signals_accepted"],
                             rejected=scan_result["signals_rejected"])
            self._emit_event("RISK_CHECK_COMPLETED", market_date, now,
                             accepted=scan_result["signals_accepted"],
                             rejected=scan_result["signals_rejected"],
                             risk_events=len(scan_result["scanner_rejections"]))

        self._stage = "portfolio_accounting"
        self._persist_health(run_id, market_date, health, now)
        portfolio = self._portfolio_state(market_date)
        self._emit_event("PORTFOLIO_ACCOUNTED", market_date, now,
                         cash=portfolio["cash"], equity=portfolio["equity"],
                         realized_pnl=portfolio["realized_pnl"],
                         unrealized_pnl=portfolio["unrealized_pnl"],
                         gross_exposure=portfolio["gross_exposure"])
        # Compare against the previous market date; repeated scans on one date
        # must not reset that date's daily-loss baseline.
        data_valid = health["system_data_status"] == "FRESH"
        if data_valid:
            previous_equity = self._previous_equity(exclude_run_id=run_id, before_date=market_date)
            if previous_equity is None:
                previous_equity = self.starting_capital
            daily_pnl = portfolio["equity"] - previous_equity
            daily_return = daily_pnl / previous_equity if previous_equity else 0.0
            peak = max(self.starting_capital, self._historical_peak(exclude_run_id=run_id), portfolio["equity"])
            drawdown = max(0.0, 1 - portfolio["equity"] / peak) if peak else 0.0
            max_drawdown = max(drawdown, self._historical_max_drawdown(exclude_run_id=run_id))
            self._stage = "reconciliation"
            reconciliation = self._reconcile(market_date, portfolio)
            self._emit_event("RECONCILIATION_PASSED", market_date, now, **reconciliation)
            self._save_portfolio_snapshot(run_id, market_date, portfolio, daily_pnl, daily_return, drawdown, max_drawdown)
        else:
            drawdown = float(self._latest_snapshot_value("drawdown", 0.0))
            max_drawdown = float(self._latest_snapshot_value("maximum_drawdown", drawdown))
            daily_pnl = daily_return = 0.0
            reconciliation = {"status": "NOT_RUN", "reason": "required market data failed closed"}

        closed_today = self._closed_positions(market_date)
        open_positions = self._open_positions()
        strategy_activity = self._strategy_activity(market_date)
        risk_events = list(scan_result["scanner_rejections"])
        risk_events.extend(
            {"event_type": "ORDER_REJECTED", **event}
            for event in lifecycle["events"]
            if event.get("event") in {"REJECTED", "REJECTED_RISK"}
        )
        report = {
            "report_type": "FORWARD PAPER",
            "run_id": run_id,
            "date": market_date.isoformat(),
            "session_status": "TRADING_DAY",
            "generated_at": now.isoformat(),
            "market": {
                "nifty_close": float(benchmark["close"].iloc[-1]) if benchmark is not None and not benchmark.empty else None,
                "nifty_daily_return": self._latest_return(benchmark) if benchmark is not None and not benchmark.empty else None,
                "india_vix": float(vix["close"].iloc[-1]) if vix is not None and not vix.empty else None,
                "nifty_date": benchmark_check.data_timestamp[:10] if benchmark_check and benchmark_check.data_timestamp else None,
                "india_vix_date": vix_check.data_timestamp[:10] if vix is not None and vix_check and vix_check.data_timestamp else None,
                "india_vix_source": "official NSE India VIX" if vix is not None else "causal NIFTY realized_vol20 fallback",
                "regime": scan_result.get("market_regime", "UNKNOWN"),
            },
            "data": health,
            "universe": {"universe_date": market_date.isoformat(), "symbol_count": len(requested_symbols),
                         "symbols": requested_symbols},
            "signals": {
                "generated": scan_result["signals_generated"],
                "accepted": scan_result["signals_accepted"],
                "rejected": scan_result["signals_rejected"],
                "details": scan_result["raw_signals"],
            },
            "orders": {
                "created": scan_result["orders_created"],
                "filled": lifecycle["orders_filled"],
                "exited": lifecycle["orders_exited"],
                "events": lifecycle["events"],
            },
            "portfolio": {
                "starting_equity": self.starting_capital,
                "ending_equity": portfolio["equity"],
                "cash": portfolio["cash"],
                "available_cash": portfolio["available_cash"],
                "reserved_cash": portfolio["reserved_cash"],
                "market_value": portfolio["market_value"],
                "invested_capital": portfolio["invested_capital"],
                "realized_pnl": portfolio["realized_pnl"],
                "realized_pnl_today": portfolio["realized_pnl_today"],
                "unrealized_pnl": portfolio["unrealized_pnl"],
                "daily_pnl": daily_pnl,
                "daily_return": daily_return,
                "cumulative_return": portfolio["equity"] / self.starting_capital - 1,
                "drawdown": drawdown,
                "maximum_drawdown": max_drawdown,
                "gross_exposure": portfolio["gross_exposure"],
                "net_exposure": portfolio["net_exposure"],
                "transaction_costs": portfolio["transaction_costs"],
                "sector_exposure": portfolio["sector_exposure"],
                "open_positions": open_positions,
                "position_count": len(open_positions),
                "pending_orders": self._pending_order_count(),
            },
            "closed_positions_today": closed_today,
            "strategy_activity": strategy_activity,
            "risk_events": risk_events,
            "reconciliation": reconciliation,
            "historical_backtests_included": False,
            "live_trading_enabled": False,
            "broker_contacted": False,
            "broker_orders_enabled": False,
        }
        baseline = validate_baseline_manifest(
            self.config, root=self.project_root, path=self.baseline_manifest,
            baseline_name=self.config.forward_paper.baseline_name,
        )
        report_dir = self.output_root / market_date.isoformat()
        report["report_path"] = str(report_dir / "daily_report.md")
        report["report_json_path"] = str(report_dir / "daily_report.json")
        write_daily_artifacts(
            self.database,
            report,
            self.output_root,
            baseline_manifest=baseline,
            render_markdown=self._render_report,
        )
        self._emit_event("DAILY_REPORT_GENERATED", market_date, now, report_path=report["report_path"])
        if data_valid:
            self._persist_journal(run_id, market_date, report, now)
        if self.output_root.resolve() == (self.project_root / "results" / "phase3d_forward").resolve():
            update_cumulative_research(
                self.database,
                self.output_root,
                self.project_root / "docs" / "PHASE3D_FORWARD_RESEARCH.md",
                phase3c_results=self.project_root / "results" / "phase3c_kaggle_20260926_171429",
            )
        return report

    @staticmethod
    def _latest_return(frame: pd.DataFrame) -> float | None:
        if frame is None or len(frame) < 2:
            return None
        previous = float(frame["close"].iloc[-2])
        current = float(frame["close"].iloc[-1])
        return current / previous - 1 if previous else None

    def _reconcile(self, market_date: date, portfolio: dict[str, Any]) -> dict[str, Any]:
        errors: list[str] = []
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            orphan = connection.execute(
                """SELECT COUNT(*) FROM positions p
                   LEFT JOIN signals s ON s.signal_id=p.signal_id
                   LEFT JOIN orders o ON o.order_id=p.entry_order_id
                   WHERE s.signal_id IS NULL OR o.order_id IS NULL"""
            ).fetchone()[0]
            bad_positions = connection.execute(
                """SELECT COUNT(*) FROM positions WHERE quantity<=0 OR entry_price<=0 OR mark_price<=0
                   OR status NOT IN ('OPEN','CLOSED') OR (exit_date IS NOT NULL AND exit_date<entry_date)"""
            ).fetchone()[0]
            bad_orders = connection.execute(
                """SELECT COUNT(*) FROM orders WHERE quantity<=0 OR intended_price<=0
                   OR COALESCE(raw_signal_price,intended_price)<=0 OR status NOT IN
                   ('CREATED','PENDING','FILLED','REJECTED','CANCELLED','EXPIRED','PARTIALLY_FILLED','CLOSED')"""
            ).fetchone()[0]
            duplicate_snapshots = connection.execute(
                "SELECT COUNT(*) FROM (SELECT trading_date FROM portfolio_snapshots GROUP BY trading_date HAVING COUNT(*)>1)"
            ).fetchone()[0]
            duplicate_orders = connection.execute(
                "SELECT COUNT(*) FROM (SELECT signal_id,order_kind FROM orders GROUP BY signal_id,order_kind HAVING COUNT(*)>1)"
            ).fetchone()[0]
            broken_exit_links = connection.execute(
                """SELECT COUNT(*) FROM positions p LEFT JOIN orders o ON o.order_id=p.exit_order_id
                   WHERE p.status='CLOSED' AND (p.exit_order_id IS NULL OR o.order_id IS NULL)"""
            ).fetchone()[0]
            bad_fill_timestamps = connection.execute(
                """SELECT COUNT(*) FROM orders WHERE filled_at IS NOT NULL
                   AND datetime(filled_at)<datetime(created_at)"""
            ).fetchone()[0]
            inconsistent_realized = connection.execute(
                """SELECT COUNT(*) FROM positions p JOIN orders o ON o.order_id=p.exit_order_id
                   WHERE p.status='CLOSED' AND ABS(
                     p.realized_pnl - (p.direction*p.quantity*(p.exit_price-p.entry_price)
                     - p.entry_costs - o.costs)
                   )>0.02"""
            ).fetchone()[0]
            inconsistent_order_costs = connection.execute(
                """SELECT COUNT(*) FROM orders WHERE filled_at IS NOT NULL AND ABS(
                   costs-(brokerage+stt+exchange_charges+gst+sebi_charges+stamp_duty+dp_charge)
                   )>0.02"""
            ).fetchone()[0]
            inconsistent_position_costs = connection.execute(
                """SELECT COUNT(*) FROM positions p JOIN orders entry ON entry.order_id=p.entry_order_id
                   JOIN orders exit ON exit.order_id=p.exit_order_id
                   WHERE p.status='CLOSED' AND entry.filled_at IS NOT NULL AND exit.filled_at IS NOT NULL
                   AND ABS(p.total_costs-entry.costs-exit.costs)>0.02"""
            ).fetchone()[0]
        if orphan:
            errors.append(f"{orphan} orphan position(s)")
        if bad_positions:
            errors.append(f"{bad_positions} position(s) have impossible quantity or price")
        if bad_orders:
            errors.append(f"{bad_orders} order(s) have impossible quantity or price")
        if duplicate_snapshots:
            errors.append(f"{duplicate_snapshots} duplicated daily portfolio snapshot(s)")
        if duplicate_orders:
            errors.append(f"{duplicate_orders} duplicated signal/order-kind pair(s)")
        if broken_exit_links:
            errors.append(f"{broken_exit_links} closed position(s) have no linked exit order")
        if bad_fill_timestamps:
            errors.append(f"{bad_fill_timestamps} order fill timestamp(s) precede creation")
        if inconsistent_realized:
            errors.append(f"{inconsistent_realized} closed trade(s) have unexplained realized P&L")
        if inconsistent_order_costs:
            errors.append(f"{inconsistent_order_costs} order(s) have inconsistent itemized costs")
        if inconsistent_position_costs:
            errors.append(f"{inconsistent_position_costs} closed position(s) have inconsistent total costs")
        if portfolio["cash"] < -0.01:
            errors.append(f"unexplained negative cash {portfolio['cash']:.2f}")
        if abs(portfolio["cash"] + portfolio["market_value"] - portfolio["equity"]) > 0.02:
            errors.append("cash plus net liquidation market value does not equal equity")
        if abs(portfolio["equity"] - (portfolio["starting_capital"] + portfolio["realized_pnl"] + portfolio["unrealized_pnl"])) > 0.02:
            errors.append("starting capital plus realized/unrealized P&L does not equal equity")
        if errors:
            self._emit_event("RECONCILIATION_FAILED", market_date, self.clock(), checks=errors)
            raise RuntimeError("RECONCILIATION_FAILED: " + "; ".join(errors))
        return {
            "status": "PASSED",
            "cash": portfolio["cash"],
            "market_value": portfolio["market_value"],
            "equity": portfolio["equity"],
            "reconciled_market_date": market_date.isoformat(),
            "checks": ["cash_plus_market_value", "pnl_bridge", "orders_unique", "positions_linked",
                       "quantities_and_prices", "timestamps", "realized_pnl", "cost_components"],
        }

    def _successful_report_for_date(self, market_date: date) -> dict[str, Any] | None:
        if not self.database.exists():
            return None
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(
                "SELECT payload_json FROM daily_journal WHERE trading_date=? ORDER BY created_at DESC LIMIT 1",
                (market_date.isoformat(),),
            ).fetchone()
        if not row:
            return None
        payload = json.loads(row[0])
        report = payload.get("full_report")
        if report:
            return report
        report_path = self.output_root / market_date.isoformat() / "daily_report.json"
        return json.loads(report_path.read_text(encoding="utf-8")) if report_path.is_file() else payload

    def _latest_snapshot_value(self, column: str, default: float) -> float:
        allowed = {"drawdown", "maximum_drawdown", "equity", "cash", "market_value"}
        if column not in allowed or not self.database.is_file():
            return default
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(f"SELECT {column} FROM portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1").fetchone()
        return float(row[0]) if row and row[0] is not None else default

    def _write_non_session_report(
        self, market_date: date, now: datetime, status: str, reason: str, symbols: list[str]
    ) -> dict[str, Any]:
        latest = {}
        if self.database.is_file():
            with sqlite3.connect(self.database) as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute("SELECT * FROM portfolio_snapshots ORDER BY snapshot_id DESC LIMIT 1").fetchone()
                if row:
                    latest = dict(row)
        capital = float(latest.get("starting_capital", self.starting_capital))
        equity = float(latest.get("equity", capital))
        report = {
            "report_type": "FORWARD PAPER",
            "run_id": self._active_run_id,
            "date": market_date.isoformat(),
            "session_status": status,
            "session_reason": reason,
            "generated_at": now.isoformat(),
            "market": {"nifty_close": None, "nifty_daily_return": None, "nifty_date": None,
                       "india_vix": None, "india_vix_date": None, "regime": "REGIME_DATA_UNAVAILABLE"},
            "data": {"provider": self.provider_name, "dataset": getattr(self.provider, "dataset_name", self.provider_name),
                     "market_date": market_date.isoformat(), "system_data_status": status,
                     "system_data_reason": reason, "symbols_requested": len(symbols), "symbols_received": 0,
                     "symbols_missing": symbols, "missing_symbols": symbols, "stale_symbols": [],
                     "invalid_symbols": [], "symbols_scanned": 0, "data_timestamps": {}},
            "universe": {"universe_date": market_date.isoformat(), "symbol_count": len(symbols), "symbols": symbols},
            "signals": {"generated": 0, "accepted": 0, "rejected": 0, "details": []},
            "orders": {"created": 0, "filled": 0, "exited": 0, "events": []},
            "portfolio": {"starting_equity": capital, "ending_equity": equity,
                          "cash": float(latest.get("cash", capital)), "market_value": float(latest.get("market_value", 0)),
                          "available_cash": float(latest.get("cash", capital)) - float(latest.get("reserved_cash", 0)),
                          "reserved_cash": float(latest.get("reserved_cash", 0)), "gross_exposure": float(latest.get("gross_exposure", 0)),
                          "net_exposure": float(latest.get("net_exposure", 0)), "transaction_costs": float(latest.get("transaction_costs", 0)),
                          "realized_pnl": float(latest.get("realized_pnl", 0)),
                          "realized_pnl_today": 0.0,
                          "unrealized_pnl": float(latest.get("unrealized_pnl", 0)),
                          "daily_pnl": 0.0, "daily_return": 0.0, "cumulative_return": equity / capital - 1 if capital else 0.0,
                          "drawdown": float(latest.get("drawdown", 0)), "maximum_drawdown": float(latest.get("maximum_drawdown", 0)),
                          "sector_exposure": json.loads(latest.get("sector_exposure_json", "{}")),
                          "open_positions": self._open_positions() if self.database.exists() else [],
                          "position_count": int(latest.get("open_positions", 0)), "pending_orders": int(latest.get("pending_orders", 0))},
            "strategy_activity": [], "risk_events": [], "reconciliation": {"status": "NOT_RUN", "reason": "no trading session"},
            "historical_backtests_included": False, "live_trading_enabled": False,
            "broker_contacted": False, "broker_orders_enabled": False,
        }
        report_dir = self.output_root / market_date.isoformat()
        report["report_path"] = str(report_dir / "daily_report.md")
        report["report_json_path"] = str(report_dir / "daily_report.json")
        try:
            baseline = validate_baseline_manifest(
                self.config, root=self.project_root, path=self.baseline_manifest,
                baseline_name=self.config.forward_paper.baseline_name,
            )
        except Exception:
            baseline = {"baseline_name": self.config.forward_paper.baseline_name, "baseline_sha256": None}
        write_daily_artifacts(self.database, report, self.output_root, baseline_manifest=baseline,
                              render_markdown=self._render_report)
        return report

    def _open_position_symbols(self) -> list[str]:
        if not self.database.exists():
            return []
        with sqlite3.connect(self.database) as connection:
            positions = [row[0] for row in connection.execute("SELECT symbol FROM positions WHERE status='OPEN'")]
            pending = [row[0] for row in connection.execute("SELECT symbol FROM orders WHERE status IN ('CREATED','PENDING')")]
        return list(dict.fromkeys([*positions, *pending]))

    def _health_payload(
        self, run_id: str, market_date: date, symbols: list[str], required: list[str], checks: dict[str, DataCheck]
    ) -> dict[str, Any]:
        serial = {symbol: check.to_dict() for symbol, check in checks.items()}
        missing = [symbol for symbol in symbols if symbol not in checks or not checks[symbol].valid]
        return {
            "run_id": run_id,
            "provider": self.provider_name,
            "dataset": getattr(self.provider, "dataset_name", self.provider_name),
            "dataset_paths": self._dataset_paths(required),
            "last_update": max((check.retrieved_at for check in checks.values()), default=None),
            "market_date": market_date.isoformat(),
            "symbols_requested": len(symbols),
            "symbols_received": sum(1 for symbol in symbols if symbol in checks and checks[symbol].valid),
            "symbols_scanned": 0,
            "symbols_missing": missing,
            "missing_symbols": missing,
            "stale_symbols": [symbol for symbol in symbols if symbol in checks and checks[symbol].freshness_status == "DATA_STALE"],
            "invalid_symbols": [symbol for symbol in symbols if symbol in checks and checks[symbol].freshness_status == "DATA_INVALID"],
            "data_timestamps": {symbol: check.data_timestamp for symbol, check in checks.items()},
            "invalid_records": sum(check.invalid_records for check in checks.values()),
            "duplicate_records": sum(check.duplicate_records for check in checks.values()),
            "abnormal_records": sum(check.abnormal_records for check in checks.values()),
            "symbols_scanned_set": [],
            "required_symbols": required,
            "checks": serial,
        }

    def _dataset_paths(self, symbols: Iterable[str]) -> dict[str, str]:
        get_path = getattr(self.provider, "dataset_path_for", None)
        if not callable(get_path):
            return {}
        paths: dict[str, str] = {}
        for symbol in symbols:
            try:
                paths[symbol] = str(Path(get_path(symbol)).resolve())
            except (OSError, TypeError, ValueError):
                paths[symbol] = "UNAVAILABLE"
        return paths

    def _scan(
        self,
        market_date: date,
        now: datetime,
        equities: dict[str, pd.DataFrame],
        benchmark: pd.DataFrame | None,
        vix: pd.DataFrame | None,
        strategies: list[EquityStrategy],
        portfolio: dict[str, Any],
        checks: dict[str, DataCheck],
    ) -> dict[str, Any]:
        if benchmark is None:
            return {"signals_generated": 0, "signals_accepted": 0, "signals_rejected": 0,
                    "orders_created": 0, "raw_signals": [], "scanner_rejections": [], "market_regime": "REGIME_DATA_UNAVAILABLE"}
        vix_close = vix["close"] if vix is not None and not vix.empty else None
        benchmark_features = add_features(benchmark, india_vix=vix_close,
                                           india_vix_high_threshold=self.config.regime.india_vix_high_threshold)
        regime_value = str(benchmark_features["regime"].iloc[-1]) if not benchmark_features.empty else "UNKNOWN"
        raw: dict[tuple[str, str], dict[str, Any]] = {}
        for symbol, bars in equities.items():
            features = add_features(bars)
            regimes = align_regime(features, benchmark_features)
            for strategy in strategies:
                emitted = strategy.evaluate(features, regimes)
                signal = next((item for item in reversed(emitted) if pd.Timestamp(item.signal_time).date() == market_date), None)
                if signal is None:
                    continue
                row = features.loc[signal.signal_time]
                raw[(symbol, strategy.name)] = {
                    "candidate": signal,
                    "features": {name: _json_default(row.get(name)) for name in ("close", "atr", "ema20", "ema50", "ema200", "rsi", "adx", "volume_ratio", "realized_vol20")},
                    "signal_price": float(row["close"]),
                    "atr": float(row["atr"]) if pd.notna(row.get("atr")) else None,
                    "regime": str(regimes.loc[signal.signal_time]),
                }
        active = self._risk_positions(equities)
        accepted, rejected = scan_universe(
            equities,
            benchmark,
            strategies,
            self.config,
            capital=max(0.01, portfolio["equity"]),
            existing_positions=active,
            realized_day_pnl=portfolio["daily_pnl_before_scan"],
            india_vix_bars=vix,
            portfolio_drawdown_pct=portfolio["drawdown_before_scan"],
        )
        accepted_by_key = {(item["symbol"], item["strategy"]): item for item in accepted}
        reject_by_key = {(item.get("symbol"), item.get("strategy")): str(item.get("reason", "risk admission rejected"))
                         for item in rejected if item.get("strategy")}
        created = generated = rejected_count = 0
        signal_output: list[dict[str, Any]] = []
        for key, item in sorted(raw.items()):
            generated += 1
            admitted = accepted_by_key.get(key)
            reason = reject_by_key.get(key)
            signal_payload = self._signal_payload(
                market_date, now, key[0], key[1], item, admitted, reason, checks[key[0]], portfolio["equity"]
            )
            inserted = self._persist_signal(signal_payload)
            if inserted:
                _append_jsonl(self.output_root / "signals" / "signals.jsonl", signal_payload)
                self._emit_event("SIGNAL_GENERATED", market_date, now, **signal_payload)
            signal_output.append(signal_payload)
            if admitted:
                if self._create_entry_order(signal_payload, admitted, now):
                    created += 1
            else:
                rejected_count += 1
                if inserted:
                    self._emit_event(
                        "SIGNAL_REJECTED",
                        market_date,
                        now,
                        signal_id=signal_payload["signal_id"],
                        symbol=key[0],
                        strategy=key[1],
                        reason=reason or "risk admission rejected",
                        risk_check=reason,
                    )
                    if reason and any(token in reason.lower() for token in ("risk", "limit", "sector", "position", "capital", "drawdown")):
                        self._emit_event("RISK_LIMIT_TRIGGERED", market_date, now,
                                         signal_id=signal_payload["signal_id"], symbol=key[0], strategy=key[1], reason=reason)
        return {
            "signals_generated": generated,
            "signals_accepted": len(accepted_by_key),
            "signals_rejected": rejected_count,
            "orders_created": created,
            "raw_signals": signal_output,
            "scanner_rejections": rejected,
            "market_regime": regime_value,
        }

    def _signal_payload(
        self,
        market_date: date,
        now: datetime,
        symbol: str,
        strategy: str,
        context: dict[str, Any],
        admitted: dict[str, Any] | None,
        rejection: str | None,
        check: DataCheck,
        equity: float,
    ) -> dict[str, Any]:
        candidate: Candidate = context["candidate"]
        direction = candidate.direction
        direction_name = "BUY" if direction > 0 else "SELL"
        entry = float(admitted["entry_estimate"]) if admitted else config_cost_slippage(self.config, context["signal_price"], direction)
        stop = float(admitted["stop_loss"]) if admitted else entry - direction * candidate.stop_distance
        target = float(admitted["target"]) if admitted else entry + direction * candidate.stop_distance * candidate.reward_risk
        if admitted:
            quantity = int(admitted["quantity"])
            risk_amount = float(admitted["maximum_loss"])
        else:
            sizing = self._candidate_sizing(entry, stop, direction, strategy, equity)
            quantity = sizing.quantity
            risk_amount = sizing.estimated_risk
        signal_id = hashlib.sha256(f"{market_date}|{symbol}|{strategy}|{direction_name}".encode()).hexdigest()[:24]
        status = "ACCEPTED" if admitted else "REJECTED_RISK"
        reason = str(candidate.reason)
        if rejection:
            reason = f"{reason}; risk rejection: {rejection}"
        return {
            "signal_id": signal_id,
            "trading_date": market_date.isoformat(),
            "timestamp": now.isoformat(),
            "symbol": symbol,
            "strategy": strategy,
            "direction": direction_name,
            "signal_price": context["signal_price"],
            "stop_loss": stop,
            "target": target,
            "atr": context["atr"],
            "position_size": quantity,
            "risk_amount": risk_amount,
            "risk_percentage": risk_amount / equity if equity > 0 else None,
            "reason": reason,
            "features": context["features"],
            "market_regime": context["regime"],
            "data_provider": check.provider,
            "data_freshness": check.freshness_status,
            "accepted": bool(admitted),
            "status": status,
            "rejection_reason": rejection,
            "entry_estimate": entry,
            "sector": admitted.get("sector", "UNKNOWN") if admitted else "UNKNOWN",
        }

    def _candidate_sizing(self, entry: float, stop: float, direction: int, strategy: str, equity: float):
        costs = CostModel(self.config.costs)
        intraday = strategy == "vwap_intraday"
        side = "buy" if direction > 0 else "sell"
        close_side = "sell" if direction > 0 else "buy"
        per_share = costs.order_cost(entry, 1, side, intraday).total
        per_share += costs.order_cost(max(stop, 0.01), 1, close_side, intraday, is_exit=False).total
        stop_risk = abs(entry - stop) + entry * self.config.costs.slippage_bps / 10000 * 2
        return position_size(
            equity, entry, stop_risk, self.config.risk,
            equity * self.config.risk.max_single_position_fraction,
            per_share,
            fixed_costs=0.0 if intraday else self.config.costs.delivery_dp_charge_per_sell,
        )

    def _persist_data_block_signal(
        self, run_id: str, market_date: date, now: datetime, symbol: str,
        check: DataCheck | None, status: str, reason: str,
    ) -> None:
        provider = check.provider if check else self.provider_name
        freshness = check.freshness_status if check else status
        signal_id = hashlib.sha256(f"{market_date}|{symbol}|DATA_HEALTH|{status}".encode()).hexdigest()[:24]
        payload = {
            "signal_id": signal_id, "trading_date": market_date.isoformat(), "timestamp": now.isoformat(),
            "symbol": symbol, "strategy": "DATA_HEALTH", "direction": None,
            "signal_price": None, "stop_loss": None, "target": None, "atr": None,
            "position_size": None, "risk_amount": None, "risk_percentage": None,
            "reason": reason, "features": {}, "market_regime": "UNKNOWN", "data_provider": provider,
            "data_freshness": freshness, "accepted": False, "status": status,
            "rejection_reason": reason, "run_id": run_id,
        }
        if self._persist_signal(payload):
            _append_jsonl(self.output_root / "signals" / "signals.jsonl", payload)

    def _persist_signal(self, payload: dict[str, Any]) -> bool:
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO signals
                (signal_id,trading_date,timestamp,symbol,strategy,direction,signal_price,stop_loss,target,atr,
                 position_size,risk_amount,risk_percentage,reason,features_json,market_regime,sector,data_provider,
                 freshness_status,accepted,status,rejection_reason,payload_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (payload["signal_id"], payload["trading_date"], payload["timestamp"], payload["symbol"],
                 payload["strategy"], payload.get("direction"), payload.get("signal_price"), payload.get("stop_loss"),
                 payload.get("target"), payload.get("atr"), payload.get("position_size"), payload.get("risk_amount"),
                 payload.get("risk_percentage"), payload["reason"], _json(payload.get("features", {})),
                 payload.get("market_regime"), payload.get("sector", "UNKNOWN"), payload["data_provider"], payload["data_freshness"],
                 int(bool(payload.get("accepted"))), payload["status"], payload.get("rejection_reason"), _json(payload)),
            )
            return cursor.rowcount == 1

    def _create_entry_order(self, signal: dict[str, Any], admitted: dict[str, Any], now: datetime) -> bool:
        order_id = f"PAPER-{signal['signal_id']}"
        order = {
            "order_id": order_id, "signal_id": signal["signal_id"], "timestamp": now.isoformat(),
            "trading_date": signal["trading_date"], "symbol": signal["symbol"],
            "side": "BUY" if signal["direction"] == "BUY" else "SELL",
            "quantity": int(admitted["quantity"]), "raw_signal_price": float(signal["signal_price"]),
            "requested_price": float(admitted["entry_estimate"]),
            "intended_price": float(admitted["entry_estimate"]),
            "simulated_fill_price": None, "slippage": None, "filled_at": None,
            "status": "CREATED", "strategy": signal["strategy"], "order_kind": "ENTRY",
        }
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute(
                """INSERT INTO orders(order_id,signal_id,created_at,trading_date,symbol,side,quantity,
                intended_price,raw_signal_price,requested_price,status,strategy,order_kind)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(signal_id,order_kind) DO NOTHING""",
                (order_id, signal["signal_id"], order["timestamp"], order["trading_date"], order["symbol"],
                 order["side"], order["quantity"], order["intended_price"], order["raw_signal_price"],
                 order["requested_price"], "CREATED", order["strategy"], "ENTRY"),
            )
            if cursor.rowcount == 1:
                connection.execute(
                    "INSERT INTO order_events(order_id,signal_id,timestamp,event,details_json) VALUES (?,?,?,?,?)",
                    (order_id, signal["signal_id"], order["timestamp"], "ORDER_CREATED", _json(order)),
                )
            else:
                return False
        _append_jsonl(self.output_root / "orders" / "orders.jsonl", {"event": "ORDER_CREATED", **order})
        self._emit_event("ORDER_CREATED", date.fromisoformat(order["trading_date"]), now, **order)
        return True

    def _advance_orders_and_positions(
        self, market_date: date, now: datetime, bars: dict[str, pd.DataFrame]
    ) -> dict[str, Any]:
        costs = CostModel(self.config.costs)
        events: list[dict[str, Any]] = []
        filled = exited = opened = closed = 0
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            pending = connection.execute(
                """SELECT o.*,s.stop_loss,s.target,s.risk_amount,s.direction,s.sector,s.market_regime,
                          COALESCE(o.raw_signal_price,s.signal_price) AS signal_raw_price
                   FROM orders o JOIN signals s ON s.signal_id=o.signal_id
                   WHERE o.status IN ('CREATED','PENDING') AND o.order_kind='ENTRY'
                     AND o.trading_date < ? ORDER BY o.created_at""",
                (market_date.isoformat(),),
            ).fetchall()
        for order in pending:
            frame = bars.get(order["symbol"])
            current = self._row_for_date(frame, market_date)
            if current is None:
                continue
            direction = 1 if order["side"] == "BUY" else -1
            raw_open = float(current["open"])
            fill = costs.slippage_price(raw_open, direction, is_entry=True)
            entry_side = "buy" if direction > 0 else "sell"
            exit_side = "sell" if direction > 0 else "buy"
            entry_cost_breakdown = costs.order_cost(fill, int(order["quantity"]), entry_side, False)
            entry_cost_fields = _cost_fields(entry_cost_breakdown)
            entry_cost = entry_cost_breakdown.total
            stop_fill = costs.slippage_price(float(order["stop_loss"]), direction, is_entry=False)
            exit_estimate = costs.order_cost(stop_fill, int(order["quantity"]), exit_side, False, is_exit=True).total
            actual_risk = abs(fill - stop_fill) * int(order["quantity"]) + entry_cost + exit_estimate
            risk_budget = float(order["risk_amount"] or self.starting_capital * self.config.risk.risk_per_trade)
            if actual_risk > risk_budget + 0.01:
                reason = f"next-session opening gap raises estimated stop risk to {actual_risk:.2f}, above risk budget"
                self._order_status(order["order_id"], order["signal_id"], "REJECTED", now, reason)
                events.append({"order_id": order["order_id"], "event": "REJECTED", "reason": reason})
                self._emit_event("ORDER_REJECTED", market_date, now, order_id=order["order_id"],
                                 signal_id=order["signal_id"], reason=reason)
                self._emit_event("RISK_LIMIT_TRIGGERED", market_date, now,
                                 order_id=order["order_id"], reason=reason)
                continue
            slippage = fill - raw_open
            with sqlite3.connect(self.database) as connection:
                connection.execute(
                    """UPDATE orders SET status='FILLED',simulated_fill_price=?,slippage=?,filled_at=?,
                       brokerage=?,stt=?,exchange_charges=?,gst=?,sebi_charges=?,stamp_duty=?,dp_charge=?,costs=?
                       WHERE order_id=?""",
                    (fill, slippage, now.isoformat(), entry_cost_fields["brokerage"], entry_cost_fields["stt"],
                     entry_cost_fields["exchange_charges"], entry_cost_fields["gst"],
                     entry_cost_fields["sebi_charges"], entry_cost_fields["stamp_duty"],
                     entry_cost_fields["dp_charge"], entry_cost_fields["costs"], order["order_id"]),
                )
                connection.execute(
                    "INSERT INTO order_events(order_id,signal_id,timestamp,event,details_json) VALUES (?,?,?,?,?)",
                    (order["order_id"], order["signal_id"], now.isoformat(), "FILLED",
                     _json({"raw_signal_price": order["signal_raw_price"], "requested_price": order["intended_price"],
                            "raw_open": raw_open, "fill_price": fill, "slippage": slippage,
                            "cost_breakdown": entry_cost_fields, "entry_costs": entry_cost})),
                )
                connection.execute(
                    """INSERT OR IGNORE INTO positions
                    (position_id,signal_id,entry_order_id,symbol,strategy,direction,sector,quantity,entry_price,
                     entry_date,entry_time,stop_loss,target,entry_costs,total_costs,mark_price,status)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN')""",
                    (order["order_id"], order["signal_id"], order["order_id"], order["symbol"], order["strategy"],
                     direction, order["sector"] or "UNKNOWN", order["quantity"], fill, market_date.isoformat(),
                     now.isoformat(), order["stop_loss"], order["target"], entry_cost, entry_cost, fill),
                )
                connection.execute(
                    "INSERT INTO position_events(position_id,signal_id,timestamp,event,details_json) VALUES (?,?,?,?,?)",
                    (order["order_id"], order["signal_id"], now.isoformat(), "POSITION_OPENED",
                     _json({"entry_price": fill, "quantity": order["quantity"], "costs": entry_cost})),
                )
            fill_event = {"order_id": order["order_id"], "signal_id": order["signal_id"],
                          "event": "FILLED", "raw_signal_price": order["signal_raw_price"],
                          "requested_price": order["intended_price"], "simulated_fill_price": fill,
                          "slippage": slippage, "filled_at": now.isoformat(), **entry_cost_fields}
            _append_jsonl(self.output_root / "orders" / "orders.jsonl", fill_event)
            _append_jsonl(self.output_root / "positions" / "position_events.jsonl",
                          {"position_id": order["order_id"], "signal_id": order["signal_id"], "event": "POSITION_OPENED", **fill_event})
            self._emit_event("ORDER_FILLED", market_date, now, **fill_event)
            self._emit_event("POSITION_OPENED", market_date, now,
                             position_id=order["order_id"], signal_id=order["signal_id"],
                             symbol=order["symbol"], quantity=order["quantity"], entry_price=fill)
            events.append(fill_event)
            filled += 1
            opened += 1

        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            positions = connection.execute(
                """SELECT p.*,s.signal_price AS raw_signal_price FROM positions p
                   JOIN signals s ON s.signal_id=p.signal_id
                   WHERE p.status='OPEN' ORDER BY p.entry_date,p.position_id"""
            ).fetchall()
        for position in positions:
            frame = bars.get(position["symbol"])
            current = self._row_for_date(frame, market_date)
            if current is None:
                continue
            direction = int(position["direction"])
            reason, raw_exit = self._exit_condition(position, current, direction)
            if reason:
                exit_fill = costs.slippage_price(raw_exit, direction, is_entry=False)
                exit_side = "sell" if direction > 0 else "buy"
                exit_cost_breakdown = costs.order_cost(exit_fill, int(position["quantity"]), exit_side, False, is_exit=True)
                exit_cost_fields = _cost_fields(exit_cost_breakdown)
                exit_cost = exit_cost_breakdown.total
                gross = direction * int(position["quantity"]) * (exit_fill - float(position["entry_price"]))
                net = gross - float(position["entry_costs"]) - exit_cost
                exit_order_id = f"PAPER-EXIT-{position['signal_id']}"
                side = "SELL" if direction > 0 else "BUY"
                with sqlite3.connect(self.database) as connection:
                    connection.execute(
                        """INSERT INTO orders(order_id,signal_id,created_at,trading_date,symbol,side,quantity,
                        intended_price,raw_signal_price,requested_price,simulated_fill_price,slippage,filled_at,
                        brokerage,stt,exchange_charges,gst,
                        sebi_charges,stamp_duty,dp_charge,costs,status,strategy,order_kind)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (exit_order_id, position["signal_id"], now.isoformat(), market_date.isoformat(),
                         position["symbol"], side, position["quantity"], raw_exit,
                         position["raw_signal_price"], raw_exit, exit_fill,
                         exit_fill - raw_exit, now.isoformat(), exit_cost_fields["brokerage"],
                         exit_cost_fields["stt"], exit_cost_fields["exchange_charges"], exit_cost_fields["gst"],
                         exit_cost_fields["sebi_charges"], exit_cost_fields["stamp_duty"],
                         exit_cost_fields["dp_charge"], exit_cost_fields["costs"],
                         "FILLED", position["strategy"], "EXIT"),
                    )
                    connection.execute(
                        "INSERT INTO order_events(order_id,signal_id,timestamp,event,details_json) VALUES (?,?,?,?,?)",
                        (exit_order_id, position["signal_id"], now.isoformat(), "EXIT_FILLED",
                         _json({"reason": reason, "raw_price": raw_exit, "fill_price": exit_fill,
                                "cost_breakdown": exit_cost_fields, "costs": exit_cost})),
                    )
                    connection.execute(
                        """UPDATE positions SET status='CLOSED',exit_order_id=?,exit_price=?,exit_date=?,
                        realized_pnl=?,unrealized_pnl=0,total_costs=?,mark_price=?,holding_sessions=? WHERE position_id=?""",
                        (exit_order_id, exit_fill, market_date.isoformat(), net,
                         float(position["entry_costs"]) + exit_cost, exit_fill,
                         self._holding_sessions(frame, position["entry_date"], market_date), position["position_id"]),
                    )
                    connection.execute(
                        "INSERT INTO position_events(position_id,signal_id,timestamp,event,details_json) VALUES (?,?,?,?,?)",
                        (position["position_id"], position["signal_id"], now.isoformat(), "POSITION_CLOSED",
                         _json({"exit_order_id": exit_order_id, "reason": reason, "realized_pnl": net,
                                "gross_pnl": gross, "exit_costs": exit_cost})),
                    )
                    # An order remains FILLED after its position later exits. The
                    # immutable order lifecycle is distinct from position status.
                exit_event = {"order_id": exit_order_id, "signal_id": position["signal_id"], "symbol": position["symbol"],
                              "side": side, "quantity": position["quantity"], "intended_price": raw_exit,
                              "simulated_fill_price": exit_fill, "slippage": exit_fill - raw_exit,
                              "filled_at": now.isoformat(), **exit_cost_fields,
                              "status": "FILLED", "strategy": position["strategy"],
                              "reason": reason, "realized_pnl": net}
                _append_jsonl(self.output_root / "orders" / "orders.jsonl", {"event": "EXIT_FILLED", **exit_event})
                _append_jsonl(self.output_root / "positions" / "position_events.jsonl",
                              {"position_id": position["position_id"], "event": "POSITION_CLOSED", **exit_event})
                self._emit_event("ORDER_FILLED", market_date, now, **exit_event)
                self._emit_event("POSITION_CLOSED", market_date, now,
                                 position_id=position["position_id"], signal_id=position["signal_id"],
                                 symbol=position["symbol"], exit_reason=reason, exit_price=exit_fill,
                                 realized_pnl=net, costs=exit_cost)
                events.append({"order_id": exit_order_id, "event": "EXIT_FILLED", "reason": reason, "pnl": net})
                exited += 1
                closed += 1
                continue
            close = float(current["close"])
            exit_mark = costs.slippage_price(close, direction, is_entry=False)
            exit_side = "sell" if direction > 0 else "buy"
            estimated_exit_cost = costs.order_cost(exit_mark, int(position["quantity"]), exit_side, False, is_exit=True).total
            unrealized = direction * int(position["quantity"]) * (exit_mark - float(position["entry_price"]))
            unrealized -= float(position["entry_costs"]) + estimated_exit_cost
            held = self._holding_sessions(frame, position["entry_date"], market_date)
            with sqlite3.connect(self.database) as connection:
                connection.execute(
                    "UPDATE positions SET mark_price=?,unrealized_pnl=?,holding_sessions=? WHERE position_id=?",
                    (close, unrealized, held, position["position_id"]),
                )
                connection.execute(
                    "INSERT INTO position_events(position_id,signal_id,timestamp,event,details_json) VALUES (?,?,?,?,?)",
                    (position["position_id"], position["signal_id"], now.isoformat(), "POSITION_MARKED",
                     _json({"mark_price": close, "unrealized_pnl": unrealized, "holding_sessions": held})),
                )
            _append_jsonl(self.output_root / "positions" / "position_events.jsonl",
                          {"position_id": position["position_id"], "signal_id": position["signal_id"],
                           "event": "POSITION_MARKED", "mark_price": close, "unrealized_pnl": unrealized})
            self._emit_event("MARK_TO_MARKET", market_date, now,
                             position_id=position["position_id"], signal_id=position["signal_id"],
                             symbol=position["symbol"], mark_price=close, unrealized_pnl=unrealized)
        return {"orders_filled": filled, "orders_exited": exited, "positions_opened": opened,
                "positions_closed": closed, "events": events}

    @staticmethod
    def _row_for_date(frame: pd.DataFrame | None, market_date: date) -> pd.Series | None:
        if frame is None or frame.empty:
            return None
        matches = [pd.Timestamp(value).date() == market_date for value in frame.index]
        selected = frame.loc[matches]
        return None if selected.empty else selected.iloc[-1]

    @staticmethod
    def _exit_condition(position: sqlite3.Row, row: pd.Series, direction: int) -> tuple[str | None, float | None]:
        opening, high, low = float(row["open"]), float(row["high"]), float(row["low"])
        stop, target = float(position["stop_loss"]), float(position["target"])
        if direction > 0:
            if opening <= stop:
                return "gap_stop", opening
            if opening >= target:
                return "target", target
            if low <= stop:
                return "stop", stop
            if high >= target:
                return "target", target
        else:
            if opening >= stop:
                return "gap_stop", opening
            if opening <= target:
                return "target", target
            if high >= stop:
                return "stop", stop
            if low <= target:
                return "target", target
        return None, None

    @staticmethod
    def _holding_sessions(frame: pd.DataFrame, entry_date: str, current_date: date) -> int:
        # A position filled at the next session's open was exposed during that
        # session, even when its stop or target was hit before the close.
        return sum(1 for stamp in frame.index if date.fromisoformat(entry_date) <= pd.Timestamp(stamp).date() <= current_date)

    def _order_status(self, order_id: str, signal_id: str, status: str, now: datetime, reason: str) -> None:
        details = {"status": status, "reason": reason}
        with sqlite3.connect(self.database) as connection:
            connection.execute("UPDATE orders SET status=? WHERE order_id=?", (status, order_id))
            connection.execute(
                "INSERT INTO order_events(order_id,signal_id,timestamp,event,details_json) VALUES (?,?,?,?,?)",
                (order_id, signal_id, now.isoformat(), status, _json(details)),
            )
        _append_jsonl(self.output_root / "orders" / "orders.jsonl",
                      {"order_id": order_id, "signal_id": signal_id, "event": status, **details})

    def _risk_positions(self, bars: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
        sectors = self._sectors()
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM positions WHERE status='OPEN'").fetchall()
            pending = connection.execute(
                """SELECT o.symbol,o.quantity,s.direction,s.sector,o.intended_price
                   FROM orders o JOIN signals s ON s.signal_id=o.signal_id
                   WHERE o.status IN ('CREATED','PENDING') AND o.order_kind='ENTRY'"""
            ).fetchall()
        result = [{"symbol": row["symbol"], "sector": row["sector"],
                   "direction": "LONG" if row["direction"] > 0 else "SHORT",
                   "entry": row["entry_price"], "quantity": row["quantity"], "status": "OPEN"} for row in rows]
        result.extend({"symbol": row["symbol"], "sector": row["sector"] or sectors.get(row["symbol"], "UNKNOWN"),
                       "direction": row["direction"], "entry": row["intended_price"],
                       "quantity": row["quantity"], "status": "PENDING"} for row in pending)
        return result

    def _sectors(self) -> dict[str, str]:
        return load_sector_map(self.config.universe.symbols_file)

    def _portfolio_state(self, market_date: date) -> dict[str, Any]:
        costs = CostModel(self.config.costs)
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            positions = connection.execute("SELECT * FROM positions WHERE status='OPEN'").fetchall()
            pending = connection.execute(
                "SELECT * FROM orders WHERE status IN ('CREATED','PENDING') AND order_kind='ENTRY'"
            ).fetchall()
            realized = connection.execute("SELECT COALESCE(SUM(realized_pnl),0) FROM positions WHERE status='CLOSED'").fetchone()[0]
            realized_today = connection.execute(
                "SELECT COALESCE(SUM(realized_pnl),0) FROM positions WHERE status='CLOSED' AND exit_date=?",
                (market_date.isoformat(),),
            ).fetchone()[0]
            transaction_costs = connection.execute("SELECT COALESCE(SUM(costs),0) FROM orders").fetchone()[0]
        unrealized = sum(float(row["unrealized_pnl"]) for row in positions)
        invested = sum(max(0, int(row["direction"])) * float(row["entry_price"]) * int(row["quantity"]) for row in positions)
        cash = self.starting_capital + float(realized or 0) - sum(
            int(row["direction"]) * float(row["entry_price"]) * int(row["quantity"])
            + float(row["entry_costs"])
            for row in positions
        )
        market_value = 0.0
        exposure = net_exposure = 0.0
        sectors: dict[str, float] = {}
        for row in positions:
            mark = float(row["mark_price"])
            quantity = int(row["quantity"])
            direction = int(row["direction"])
            value = abs(mark * quantity)
            exposure += value
            net_exposure += direction * value
            liquidation_mark = costs.slippage_price(mark, direction, is_entry=False)
            exit_side = "sell" if direction > 0 else "buy"
            estimated_exit_cost = costs.order_cost(
                liquidation_mark, quantity, exit_side, False, is_exit=True
            ).total
            market_value += direction * liquidation_mark * quantity - estimated_exit_cost
            sectors[row["sector"]] = sectors.get(row["sector"], 0.0) + value
        reserved_cash = 0.0
        for row in pending:
            if row["side"] == "BUY":
                entry = float(row["intended_price"])
                quantity = int(row["quantity"])
                reserved_cash += entry * quantity + costs.order_cost(entry, quantity, "buy", False).total
        equity = self.starting_capital + float(realized or 0) + unrealized
        # Cash plus the net liquidation value of open positions is the account
        # equity. Keep estimated exit costs outside the execution price.
        accounting_equity = cash + market_value
        previous_equity = self._previous_equity(before_date=market_date)
        if previous_equity is None:
            previous_equity = self.starting_capital
        daily_pnl = equity - previous_equity
        peak = max(self.starting_capital, self._historical_peak(), equity)
        drawdown = max(0.0, 1 - equity / peak) if peak else 0.0
        return {
            "starting_capital": self.starting_capital,
            "cash": cash,
            "available_cash": cash - reserved_cash,
            "reserved_cash": reserved_cash,
            "market_value": market_value,
            "invested_capital": invested,
            "realized_pnl": float(realized or 0),
            "realized_pnl_today": float(realized_today or 0),
            "unrealized_pnl": unrealized,
            "equity": equity,
            "accounting_equity": accounting_equity,
            "gross_exposure": exposure,
            "net_exposure": net_exposure,
            "transaction_costs": float(transaction_costs or 0),
            "sector_exposure": sectors,
            "daily_pnl_before_scan": daily_pnl,
            "drawdown_before_scan": drawdown,
        }

    def _previous_equity(self, exclude_run_id: str | None = None, before_date: date | None = None) -> float | None:
        query = "SELECT equity FROM portfolio_snapshots WHERE 1=1"
        params_list: list[Any] = []
        if exclude_run_id:
            query += " AND run_id != ?"
            params_list.append(exclude_run_id)
        if before_date:
            query += " AND trading_date < ?"
            params_list.append(before_date.isoformat())
        query += " ORDER BY snapshot_id DESC LIMIT 1"
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(query, tuple(params_list)).fetchone()
        return float(row[0]) if row else None

    def _historical_peak(self, exclude_run_id: str | None = None) -> float:
        query = "SELECT MAX(equity) FROM portfolio_snapshots"
        params: tuple[Any, ...] = ()
        if exclude_run_id:
            query += " WHERE run_id != ?"
            params = (exclude_run_id,)
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(query, params).fetchone()
        return float(row[0] or self.starting_capital)

    def _historical_max_drawdown(self, exclude_run_id: str | None = None) -> float:
        query = "SELECT MAX(drawdown) FROM portfolio_snapshots"
        params: tuple[Any, ...] = ()
        if exclude_run_id:
            query += " WHERE run_id != ?"
            params = (exclude_run_id,)
        with sqlite3.connect(self.database) as connection:
            row = connection.execute(query, params).fetchone()
        return float(row[0] or 0.0)

    def _save_portfolio_snapshot(
        self, run_id: str, market_date: date, state: dict[str, Any], daily_pnl: float,
        daily_return: float, drawdown: float, maximum_drawdown: float,
    ) -> None:
        with sqlite3.connect(self.database) as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO portfolio_snapshots
                (run_id,trading_date,starting_capital,cash,invested_capital,realized_pnl,unrealized_pnl,
                 equity,daily_pnl,daily_return,cumulative_return,drawdown,maximum_drawdown,gross_exposure,
                 sector_exposure_json,open_positions,pending_orders,created_at,market_value,net_exposure,
                 reserved_cash,transaction_costs)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (run_id, market_date.isoformat(), self.starting_capital, state["cash"], state["invested_capital"],
                 state["realized_pnl"], state["unrealized_pnl"], state["equity"], daily_pnl, daily_return,
                 state["equity"] / self.starting_capital - 1, drawdown, maximum_drawdown,
                 state["gross_exposure"], _json(state["sector_exposure"]), len(self._open_positions()),
                 self._pending_order_count(), self.clock().astimezone(timezone.utc).isoformat(),
                 state["market_value"], state["net_exposure"], state["reserved_cash"], state["transaction_costs"]),
            )
        if cursor.rowcount != 1:
            return
        _append_jsonl(self.output_root / "portfolio" / "portfolio_ledger.jsonl", {
            "run_id": run_id, "trading_date": market_date.isoformat(), "starting_capital": self.starting_capital,
            "cash": state["cash"], "invested_capital": state["invested_capital"],
            "realized_pnl": state["realized_pnl"], "unrealized_pnl": state["unrealized_pnl"],
            "equity": state["equity"], "daily_pnl": daily_pnl, "daily_return": daily_return,
            "cumulative_return": state["equity"] / self.starting_capital - 1,
            "drawdown": drawdown, "maximum_drawdown": maximum_drawdown,
            "market_value": state["market_value"],
            "reserved_cash": state["reserved_cash"], "gross_exposure": state["gross_exposure"],
            "net_exposure": state["net_exposure"], "transaction_costs": state["transaction_costs"],
            "sector_exposure": state["sector_exposure"],
        })

    def _persist_health(self, run_id: str, market_date: date, health: dict[str, Any], now: datetime) -> None:
        health["invalid_records"] = sum(record["invalid_records"] for record in health["checks"].values())
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "INSERT INTO health_snapshots(run_id,trading_date,payload_json,created_at) VALUES (?,?,?,?)",
                (run_id, market_date.isoformat(), _json(health), now.isoformat()),
            )
        _append_jsonl(self.output_root / "portfolio" / "data_health.jsonl", health)

    def _persist_journal(self, run_id: str, market_date: date, report: dict[str, Any], now: datetime) -> None:
        journal = {
            "run_id": run_id,
            "date": market_date.isoformat(),
            "market_regime": report["market"]["regime"],
            "data_status": report["data"]["system_data_status"],
            "symbols_scanned": report["data"]["symbols_scanned"],
            "signals_generated": report["signals"]["generated"],
            "signals_accepted": report["signals"]["accepted"],
            "signals_rejected": report["signals"]["rejected"],
            "orders_created": report["orders"]["created"],
            "orders_filled": report["orders"]["filled"],
            "positions_opened": sum(event.get("event") == "FILLED" for event in report["orders"]["events"]),
            "positions_closed": len(report["closed_positions_today"]),
            "realized_pnl": report["portfolio"]["realized_pnl_today"],
            "unrealized_pnl": report["portfolio"]["unrealized_pnl"],
            "total_pnl": report["portfolio"]["daily_pnl"],
            "drawdown": report["portfolio"]["drawdown"],
            "session_status": report.get("session_status", "TRADING_DAY"),
            "reconciliation_status": report.get("reconciliation", {}).get("status"),
            "full_report": report,
            "created_at": now.isoformat(),
        }
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "INSERT INTO daily_journal(run_id,trading_date,payload_json,created_at) VALUES (?,?,?,?)",
                (run_id, market_date.isoformat(), _json(journal), now.isoformat()),
            )
        _append_jsonl(self.output_root / "journal" / "daily_journal.jsonl", journal)

    def _open_positions(self) -> list[dict[str, Any]]:
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM positions WHERE status='OPEN' ORDER BY symbol").fetchall()
        return [{key: row[key] for key in row.keys()} for row in rows]

    def _closed_positions(self, market_date: date) -> list[dict[str, Any]]:
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT * FROM positions WHERE status='CLOSED' AND exit_date=? ORDER BY symbol", (market_date.isoformat(),)).fetchall()
        return [{key: row[key] for key in row.keys()} for row in rows]

    def _pending_order_count(self) -> int:
        with sqlite3.connect(self.database) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM orders WHERE status IN ('CREATED','PENDING')").fetchone()[0])

    def _strategy_activity(self, market_date: date) -> list[dict[str, Any]]:
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                "SELECT strategy,status,accepted,rejection_reason,payload_json FROM signals WHERE trading_date=? ORDER BY strategy,symbol",
                (market_date.isoformat(),),
            ).fetchall()
            trades = connection.execute(
                "SELECT strategy,realized_pnl FROM positions WHERE status='CLOSED' AND exit_date=?",
                (market_date.isoformat(),),
            ).fetchall()
        output: dict[str, dict[str, Any]] = {}
        for row in rows:
            if row["strategy"] == "DATA_HEALTH":
                continue
            item = output.setdefault(row["strategy"], {"strategy": row["strategy"], "signals": 0,
                "accepted": 0, "rejected": 0, "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
            item["signals"] += 1
            item["accepted"] += int(row["accepted"])
            item["rejected"] += int(not row["accepted"])
        for trade in trades:
            item = output.setdefault(trade["strategy"], {"strategy": trade["strategy"], "signals": 0,
                "accepted": 0, "rejected": 0, "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0})
            pnl = float(trade["realized_pnl"] or 0)
            item["trades"] += 1
            item["wins"] += int(pnl > 0)
            item["losses"] += int(pnl < 0)
            item["pnl"] += pnl
        for item in output.values():
            item["win_rate"] = item["wins"] / item["trades"] if item["trades"] else None
        return [output[key] for key in sorted(output)]

    def _render_report(self, report: dict[str, Any]) -> str:
        market = report["market"]
        data = report["data"]
        signals = report["signals"]
        orders = report["orders"]
        portfolio = report["portfolio"]
        lines = [
            "# DAILY PAPER TRADING REPORT", "", f"Date: {report['date']}",
            f"Session: {report.get('session_status', 'TRADING_DAY')}",
            f"Session reason: {report.get('session_reason', '')}", "",
            "## Market", f"NIFTY close: {market['nifty_close']}",
            f"NIFTY daily return: {market.get('nifty_daily_return')}", f"India VIX: {market['india_vix']}",
            f"Regime: {market['regime']}", "", "## Data",
            f"Provider: {data['provider']}", f"Status: {data['system_data_status']}",
            f"Symbols received: {data['symbols_received']}", f"Valid: {data['symbols_scanned']}",
            f"Universe symbols: {report.get('universe', {}).get('symbol_count', 0)}",
            f"Missing: {len(data.get('symbols_missing', data['missing_symbols']))}", f"Stale: {len(data['stale_symbols'])}",
            f"Invalid: {len(data['invalid_symbols'])}", "", "## Signals",
            f"Generated: {signals['generated']}", f"Accepted: {signals['accepted']}",
            f"Rejected: {signals['rejected']}", "", "## Orders",
            f"Created: {orders['created']}", f"Filled: {orders['filled']}", f"Exited: {orders['exited']}",
            "", "## Portfolio", f"Starting equity: {portfolio['starting_equity']:.2f}",
            f"Ending equity: {portfolio['ending_equity']:.2f}",
            f"Cash: {portfolio['cash']:.2f}", f"Reserved cash: {portfolio.get('reserved_cash', 0):.2f}",
            f"Market value: {portfolio.get('market_value', 0):.2f}",
            f"Gross/net exposure: {portfolio.get('gross_exposure', 0):.2f} / {portfolio.get('net_exposure', 0):.2f}",
            f"Realized P&L today: {portfolio['realized_pnl_today']:.2f}",
            f"Unrealized P&L: {portfolio['unrealized_pnl']:.2f}",
            f"Transaction costs: {portfolio.get('transaction_costs', 0):.2f}",
            f"Daily P&L: {portfolio.get('daily_pnl', 0):.2f}",
            f"Daily return: {portfolio['daily_return']:.4%}", f"Drawdown: {portfolio['drawdown']:.4%}",
            f"Position count: {portfolio.get('position_count', 0)}",
            f"Sector exposure: {json.dumps(portfolio.get('sector_exposure', {}), sort_keys=True)}",
            f"Reconciliation: {report.get('reconciliation', {}).get('status', 'NOT_RUN')}",
            "", "## Open positions",
        ]
        for position in report["portfolio"]["open_positions"]:
            lines.append(f"- {position['symbol']} {position['strategy']} qty {position['quantity']} mark {position['mark_price']:.2f} P&L {position['unrealized_pnl']:.2f}")
        if not report["portfolio"]["open_positions"]:
            lines.append("- None")
        lines += ["", "## Strategy activity"]
        for item in report["strategy_activity"]:
            lines.append(f"- {item['strategy']}: signals {item['signals']}, accepted {item['accepted']}, rejected {item['rejected']}, trades {item['trades']}, P&L {item['pnl']:.2f}")
        lines += ["", "## Risk events"]
        for event in report.get("risk_events", []):
            lines.append(f"- {event.get('symbol', '')} {event.get('strategy', '')}: {event.get('reason', '')}")
        if not report.get("risk_events"):
            lines.append("- None")
        lines += ["", "LIVE TRADING: DISABLED", "BROKER ORDERS: DISABLED", "Broker contacted: no", ""]
        return "\n".join(lines)


def run_daily_paper_workflow(
    *,
    config: AppConfig,
    provider: HistoricalDataProvider,
    market_date: date,
    symbols: Iterable[str],
    database: str | Path,
    starting_capital: float,
    output_root: str | Path = "forward",
    strategies: list[EquityStrategy] | None = None,
    baseline_manifest: str | Path | None = None,
    trading_calendar: str | Path | None = None,
) -> dict[str, Any]:
    """Functional entry point shared by CLI and end-to-end tests."""
    return DailyPaperTrader(config, provider, database, starting_capital,
                            output_root=output_root, strategies=strategies,
                            baseline_manifest=baseline_manifest,
                            trading_calendar=trading_calendar).run(market_date, symbols)
