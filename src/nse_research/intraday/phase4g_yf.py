"""Phase 4G-YF chronological, paper-only intraday research components.

This is a forward observer, not a historical replay adapter.  It accepts only
Phase 4E-validated closed bars, records them as first observed, and delegates
strategy decisions and paper accounting to the existing Phase 4B/4C engines.
The Phase 4F historical research-readiness gate remains unchanged.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import shutil
import tempfile
import time as time_module
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from nse_research.config import AppConfig
from nse_research.data.intraday_validator import validate_intraday_dataset
from nse_research.data.yfinance_intraday import IST, YAHOO_SOURCE, YahooInstrument, YFinanceDataError
from nse_research.execution.phase4c import PaperExecutionEngine, inspect_paper_ledger
from nse_research.execution.phase4c_models import ExecutionMode, Phase4CPaperConfig, SignalEnvelope
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.intraday.bar_store import (
    BarIngestPlan, CanonicalBarStore, bar_eligibility_reason, canonical_bar_key,
)
from nse_research.intraday.model import IntradayBar, default_nse_calendar
from nse_research.intraday.scanner import IntradayScanner, IntradayScannerConfig
from nse_research.intraday.strategies import StrategySettings
from nse_research.intraday.yahoo_revision_reconciler import (
    BAR_SETTLING_SECONDS, YahooRevisionReconciler,
)


SESSION_DATE = date(2026, 9, 28)
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)
DECISION_TIMES = tuple(time(hour, minute) for hour, minute in (
    (10, 0), (10, 30), (11, 0), (11, 30), (12, 0), (12, 30),
    (13, 0), (13, 30), (14, 0), (14, 30), (15, 0),
))
MINIMUM_UNIVERSE_COVERAGE = 0.80
MAX_MISSING_INTERVAL_FRACTION = 0.20
MINIMUM_RANKING_BARS = 21
SCORE_WEIGHTS = {
    "momentum": 0.30, "relative_volume": 0.25, "vwap_strength": 0.20,
    "range_expansion": 0.15, "data_quality_liquidity": 0.10,
}
PHASE4B_STRATEGIES = (
    "vwap_trend", "vwap_pullback", "orb_5m_5min", "orb_5m_15min",
    "breakout_5m", "momentum", "mean_reversion", "volume_breakout",
)


class Phase4GYFError(RuntimeError):
    """Fail-closed error emitted by Phase 4G-YF."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(value, sort_keys=True, indent=2, default=str, allow_nan=False) + "\n"
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    except Exception:
        try:
            os.unlink(name)
        except OSError:
            pass
        raise


def _atomic_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            for row in rows:
                stream.write(json.dumps(row, sort_keys=True, default=str, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    except Exception:
        try:
            os.unlink(name)
        except OSError:
            pass
        raise


def _append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, sort_keys=True, default=str, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _atomic_csv(path: Path, rows: Sequence[Mapping[str, Any]], fieldnames: Sequence[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0]) if rows else []
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(fieldnames), extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    except Exception:
        try:
            os.unlink(name)
        except OSError:
            pass
        raise


def phase4g_preflight(project_root: str | Path, *, now: datetime, session_date: date = SESSION_DATE,
                      universe_size: int = 486, output_dir: str | Path | None = None) -> dict[str, Any]:
    """Read-only checks; never opens Yahoo, the ledger, or a broker endpoint."""
    root = Path(project_root).resolve()
    reasons: list[str] = []
    checks: dict[str, dict[str, Any]] = {}

    def record(name: str, passed: bool, detail: str) -> None:
        checks[name] = {"status": "PASS" if passed else "FAIL", "detail": detail}

    # The original 2026-09-28 run remains the module default, while the CLI
    # may select another date. The explicit local NSE calendar below is the
    # gate: unknown weekdays and special sessions still fail closed.
    record("target_date", isinstance(session_date, date), f"requested={session_date.isoformat()}")
    if now.tzinfo is None or now.utcoffset() is None:
        reasons.append("CLOCK_TIMEZONE_UNKNOWN")
        local = None
    else:
        local = now.astimezone(IST)
    record("timezone", local is not None, "Asia/Kolkata with timezone-aware acquisition and decision timestamps")
    try:
        from nse_research.data.yfinance_intraday import load_yahoo_instruments
        instruments = load_yahoo_instruments(root, size=universe_size)
        symbols = sorted(instruments)
    except (OSError, ValueError, YFinanceDataError) as exc:
        symbols = []
        instruments = {}
        reasons.append(f"NSE_EQ_UNIVERSE_INVALID: {exc}")
    universe_ok = len(symbols) == 486 and universe_size == 486
    record("frozen_universe", universe_ok, f"expected=486, mapped={len(symbols)}")
    identity_ok = bool(symbols) and len(set(symbols)) == len(symbols) and all(
        item.symbol == symbol and item.yahoo_symbol == f"{symbol}.NS"
        and item.exchange == "NSE" and item.series == "EQ" and bool(item.isin)
        for symbol, item in instruments.items()
    )
    record("yahoo_identity_mapping", identity_ok, "Every frozen NSE/EQ/ISIN symbol maps one-to-one to canonical SYMBOL.NS")
    if not universe_ok or not identity_ok:
        reasons.append("FROZEN_UNIVERSE_OR_YAHOO_IDENTITY_INVALID")
    try:
        config = AppConfig.load(root / "config.yaml")
    except Exception as exc:
        config = None
        reasons.append(f"PROJECT_CONFIG_INVALID: {exc}")
    if config is not None:
        if config.live_trading.enabled:
            reasons.append("LIVE_TRADING_ENABLED")
        if config.realtime.enabled:
            reasons.append("REALTIME_PROVIDER_ENABLED_UNEXPECTEDLY")
        if not config.paper.enabled:
            reasons.append("PAPER_MODE_DISABLED")
        # No broker adapter is selected by this runner; any configured provider
        # outside this data-only workflow is treated as a preflight failure.
        if config.forward_paper.provider not in {"licensed_nse_cm_eod", ""}:
            reasons.append("UNEXPECTED_BROKER_OR_FORWARD_PROVIDER_CONFIGURED")
    paper_only_ok = config is not None and config.paper.enabled
    live_disabled = config is not None and not config.live_trading.enabled
    record("paper_only_mode", paper_only_ok, "Paper mode is enabled; the Phase 4G runner constructs ExecutionMode.PAPER")
    record("live_execution", live_disabled, "LIVE_TRADING_ENABLED is false")
    record("broker_dependency", True, "No broker client, broker authentication, or live-order endpoint is used by this runner")
    try:
        import yfinance as yf
        yfinance_version = str(getattr(yf, "__version__", "UNKNOWN"))
    except Exception as exc:
        yfinance_version = None
        reasons.append(f"YFINANCE_UNAVAILABLE: {exc}")
    record("yfinance_dependency", yfinance_version is not None, f"version={yfinance_version or 'unavailable'}; import only, no network request")
    calendar = NseTradingCalendar(root / "data/paper_trading/nse_cash_holidays_2026.csv")
    session_decision = calendar.classify(session_date, observed_market_data=False)
    session_valid = session_decision.status == "TRADING_DAY"
    record("nse_session", session_valid, f"{session_decision.status}: {session_decision.reason}")
    if not session_valid:
        reasons.append(f"SESSION_NOT_VALIDATED: {session_decision.status}: {session_decision.reason}")
    day_state = "NOT_STARTED" if local is None or local.date() < session_date else (
        "TARGET_DATE" if local.date() == session_date else "TARGET_DATE_PASSED"
    )
    # Each selected date gets its own result directory; callers can still
    # explicitly pass another isolated output.
    expected_output = (root / "results" / f"phase4g_yf_{session_date:%Y%m%d}_batched").resolve()
    output = Path(output_dir).resolve() if output_dir is not None else expected_output
    # The CLI uses the fixed session-specific result directory. An explicit
    # directory override is retained for isolated tests, but must already exist.
    output_ok = output.is_dir()
    record("output_directory", output_ok, f"active={output}; cli_default={expected_output}")
    ledger_path = output / "paper_ledger.json"
    configured_forward_ledger = (
        (root / config.forward_paper.database).resolve()
        if config is not None and not Path(config.forward_paper.database).is_absolute()
        else (Path(config.forward_paper.database).resolve() if config is not None else None)
    )
    phase4c_ledger = root / "data/paper_trading/phase4c/ledger.json"
    phase3d_ledgers = list((root / "results").glob("phase3d_forward_*/paper_ledger.sqlite"))
    temporary_ledger_ok = (
        ledger_path == output / "paper_ledger.json"
        and ledger_path != configured_forward_ledger
        and ledger_path != phase4c_ledger
        and not output.name.startswith("phase3d_forward_")
        and all(ledger_path != item.resolve() for item in phase3d_ledgers)
    )
    record("temporary_session_ledger", temporary_ledger_ok, str(ledger_path))
    if not output_ok or not temporary_ledger_ok:
        reasons.append("PHASE4G_TEMPORARY_OUTPUT_OR_LEDGER_INVALID")

    scanner_config = IntradayScannerConfig(enabled=True, universe_size=486, timeframes=("1m", "5m"))
    strategy_settings = StrategySettings()
    strategy_ok = scanner_config.enabled and scanner_config.universe_size == 486 and set(scanner_config.timeframes) == {"1m", "5m"}
    record("phase4b_strategy_configuration", strategy_ok, f"strategies={list(PHASE4B_STRATEGIES)}; settings={asdict(strategy_settings)}")
    risk = paper_config()
    risk_ok = (
        risk.enabled and risk.execution_mode == ExecutionMode.PAPER and risk.initial_capital == 100_000.0
        and risk.risk_per_trade == 0.01 and risk.max_positions == 5 and risk.max_daily_loss == 0.02
        and risk.max_portfolio_drawdown == 0.10 and risk.max_sector_exposure == 0.25
        and risk.max_symbol_exposure == 0.25 and risk.live_trading_enabled is False
        and risk.realtime_enabled is False
    )
    record("phase4c_risk_configuration", risk_ok, f"capital={risk.initial_capital}; risk_per_trade={risk.risk_per_trade}; max_positions={risk.max_positions}; daily_loss={risk.max_daily_loss}; drawdown={risk.max_portfolio_drawdown}; sector={risk.max_sector_exposure}; symbol={risk.max_symbol_exposure}")
    if not strategy_ok:
        reasons.append("PHASE4B_STRATEGY_CONFIGURATION_INVALID")
    if not risk_ok:
        reasons.append("PHASE4C_RISK_CONFIGURATION_INVALID")
    for name, detail in (
        ("one_minute_interval", "Only native Yahoo 1m observations are requested; no daily-to-intraday conversion"),
        ("no_synthetic_fallback", "No synthetic bars, forward-filled prices, or alternate-provider fallback"),
        ("no_future_data_fallback", "Chronological decisions reject bars not closed by decision time; late data is skipped"),
    ):
        record(name, True, detail)
    if not session_valid:
        status = "PHASE4G_YF_BLOCKED_SESSION_NOT_VALIDATED"
    elif any(reason.startswith("YFINANCE_UNAVAILABLE") for reason in reasons):
        status = "PHASE4G_YF_BLOCKED_YFINANCE_UNAVAILABLE"
    elif "CLOCK_TIMEZONE_UNKNOWN" in reasons:
        status = "PHASE4G_YF_BLOCKED_TIMEZONE"
    elif not universe_ok or not identity_ok:
        status = "PHASE4G_YF_BLOCKED_UNIVERSE"
    elif "LIVE_TRADING_ENABLED" in reasons:
        status = "PHASE4G_YF_BLOCKED_LIVE_EXECUTION"
    elif "PAPER_MODE_DISABLED" in reasons or not temporary_ledger_ok or not output_ok:
        status = "PHASE4G_YF_BLOCKED_PAPER_LEDGER"
    elif reasons:
        status = "PHASE4G_YF_BLOCKED_DATA_QUALITY"
    elif day_state == "NOT_STARTED":
        status = "PHASE4G_YF_PREPARED_NOT_RUN"
    elif day_state == "TARGET_DATE_PASSED":
        status = "PHASE4G_YF_COLLECTION_WINDOW_PASSED"
    elif local is not None and local.timetz().replace(tzinfo=None) >= SESSION_CLOSE:
        status = "PHASE4G_YF_RECONCILIATION_ONLY"
    elif local is not None and local.timetz().replace(tzinfo=None) < SESSION_OPEN:
        status = "PHASE4G_YF_WAITING_FOR_SESSION_OPEN"
    else:
        status = "PHASE4G_YF_READY_FOR_FORWARD_COLLECTION"
    return {
        "status": status,
        "reasons": reasons,
        "session_date": session_date.isoformat(),
        "timezone": "Asia/Kolkata",
        "local_now": local.isoformat() if local else None,
        "calendar_check": session_decision.status,
        "calendar_reason": session_decision.reason,
        "calendar_status_source": "data/paper_trading/nse_cash_holidays_2026.csv",
        "session_open": SESSION_OPEN.isoformat(),
        "session_close": SESSION_CLOSE.isoformat(),
        "interval": "1m",
        "checks": checks,
        "symbols_requested": len(symbols),
        "symbols_expected": 486,
        "symbols": symbols,
        "provider": "Yahoo Finance",
        "access_method": "yfinance",
        "classification": "THIRD_PARTY_RESEARCH_SOURCE",
        "authorized_nse_feed": False,
        "paper_only": True,
        "live_trading_enabled": bool(config.live_trading.enabled) if config else None,
        "broker_calls": 0,
        "live_orders": 0,
        "yfinance_version": yfinance_version,
        "network_calls": 0,
        "ledger_mutations": 0,
        "output_directory": str(output),
        "temporary_ledger": str(ledger_path),
        "persistent_phase3d_ledger_used": False,
        "synthetic_data_fallback": False,
        "daily_to_intraday_fallback": False,
        "future_data_fallback": False,
    }


def _rank_percentiles(values: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted(values.items(), key=lambda item: (item[1], item[0]))
    if len(ordered) <= 1:
        return {symbol: 0.5 for symbol, _ in ordered}
    # Equal component values receive the same average cross-sectional rank.
    result: dict[str, float] = {}
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        percentile = ((start + end - 1) / 2) / (len(ordered) - 1)
        for symbol, _ in ordered[start:end]:
            result[symbol] = percentile
        start = end
    return result


def _bar_decision_exclusion(row: Mapping[str, Any], decision: datetime, *,
                            settling_seconds: int = BAR_SETTLING_SECONDS) -> str | None:
    """Return why a bar is unavailable at a decision, enforcing event and receipt time."""
    try:
        return bar_eligibility_reason(row, decision, settling_seconds=settling_seconds)
    except (KeyError, TypeError, ValueError) as exc:
        raise Phase4GYFError("RANKING_RECEIPT_TIME_UNKNOWN", str(exc)) from exc


def build_decision_snapshot(rows: Sequence[Mapping[str, Any]], *, decision: datetime,
                            expected_symbols: Sequence[str],
                            data_quality_missing_symbols: Sequence[str] = (),
                            decision_kind: str = "CURRENT_STRATEGY_EVALUATION",
                            settling_seconds: int = BAR_SETTLING_SECONDS) -> dict[str, Any]:
    """Describe precisely what was known at a decision cutoff; never backfill it."""
    if decision.tzinfo is None or decision.utcoffset() is None:
        raise Phase4GYFError("RANKING_TIMEZONE_UNKNOWN")
    local_decision = decision.astimezone(IST)
    expected = sorted({str(symbol).strip().upper() for symbol in expected_symbols})
    available_symbols: set[str] = set()
    late_symbols: set[str] = set()
    late_details: list[dict[str, str]] = []
    eligible_keys: set[tuple[str, str]] = set()
    eligible_rows = 0
    excluded_receipt = 0
    excluded_market = 0
    quality_symbols = {str(symbol).strip().upper() for symbol in data_quality_missing_symbols}
    for row in rows:
        reason = _bar_decision_exclusion(row, local_decision, settling_seconds=settling_seconds)
        symbol = str(row.get("symbol", "")).strip().upper()
        if reason == "late_receipt":
            excluded_receipt += 1
            try:
                market_stamp = datetime.fromisoformat(str(row.get("market_timestamp") or row["timestamp"]))
                market_end = market_stamp.astimezone(IST) + timedelta(minutes=int(str(row["timeframe"])[:-1]))
                receipt_stamp = datetime.fromisoformat(str(row["received_timestamp"]))
                if market_stamp.astimezone(IST) < local_decision and market_end <= local_decision:
                    late_symbols.add(symbol)
                    late_details.append({
                        "symbol": symbol,
                        "market_timestamp": market_stamp.astimezone(IST).isoformat(),
                        "received_timestamp": receipt_stamp.astimezone(IST).isoformat(),
                    })
            except (KeyError, TypeError, ValueError):
                pass
        elif reason == "market_bar_not_closed_before_decision":
            excluded_market += 1
        elif reason == "bar_not_closed_status":
            quality_symbols.add(symbol)
        else:
            eligible_rows += 1
            available_symbols.add(symbol)
            try:
                eligible_keys.add(canonical_bar_key(row))
            except (KeyError, TypeError, ValueError):
                # The bar-store validator will reject this before a signal path.
                pass
    missing_latency = sorted((set(expected) - available_symbols) & late_symbols)
    missing_quality = sorted((set(expected) - available_symbols) & quality_symbols - set(missing_latency))
    missing_unknown = sorted(set(expected) - available_symbols - set(missing_latency) - set(missing_quality))
    ordered_eligible_keys = [[symbol, market] for symbol, market in sorted(eligible_keys)]
    eligible_key_digest = hashlib.sha256(
        json.dumps(ordered_eligible_keys, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "decision_timestamp": local_decision.isoformat(),
        "decision_kind": decision_kind,
        "symbols_available": len(available_symbols),
        "symbols_available_list": sorted(available_symbols),
        "symbols_expected": len(expected),
        "symbols_expected_list": expected,
        "symbols_missing_due_to_latency": len(missing_latency),
        "symbols_missing_due_to_latency_list": missing_latency,
        "symbols_missing_due_to_data_quality": len(missing_quality),
        "symbols_missing_due_to_data_quality_list": missing_quality,
        "symbols_missing_unclassified": missing_unknown,
        "late_bars_excluded": excluded_receipt,
        "late_data_excluded": bool(late_details),
        "late_data_exclusions": late_details,
        "bars_eligible_by_decision_time": eligible_rows,
        "eligible_market_bar_keys": ordered_eligible_keys,
        "eligible_market_bar_keys_sha256": eligible_key_digest,
        "eligibility_rule": "received_timestamp <= decision_timestamp AND bar_end + settling_seconds <= decision_timestamp AND bar_status=IMMUTABLE_CLOSED",
        "bars_excluded_by_receipt_time": excluded_receipt,
        "bars_excluded_by_market_time": excluded_market,
    }


def rank_candidates(rows: Sequence[Mapping[str, Any]], *, as_of: datetime,
                    top_n: int = 10, minimum_bars: int = MINIMUM_RANKING_BARS,
                    settling_seconds: int = BAR_SETTLING_SECONDS) -> list[dict[str, Any]]:
    """Rank candidates from bars that have closed by ``as_of``; future input fails.

    Momentum is a fixed blend of session return, 5-bar return and 15-bar return.
    Relative volume compares the latest interval with the preceding 20 observed
    intervals from the same session.  No future or final-session value enters a
    component.  Component percentiles are cross-sectional at this timestamp.
    """
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise Phase4GYFError("RANKING_TIMEZONE_UNKNOWN")
    decision = as_of.astimezone(IST)
    grouped: dict[str, list[tuple[datetime, Mapping[str, Any]]]] = defaultdict(list)
    for row in rows:
        try:
            stamp = datetime.fromisoformat(str(row.get("market_timestamp") or row["timestamp"]))
            timeframe = str(row["timeframe"]).strip().lower()
            interval = int(timeframe[:-1])
        except (KeyError, TypeError, ValueError) as exc:
            raise Phase4GYFError("RANKING_INPUT_INVALID", str(exc)) from exc
        if timeframe != "1m":
            raise Phase4GYFError("RANKING_TIMEFRAME_INVALID", timeframe)
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise Phase4GYFError("RANKING_TIMEZONE_UNKNOWN", str(row.get("timestamp")))
        if row.get("market_timestamp") and str(row["market_timestamp"]) != str(row["timestamp"]):
            raise Phase4GYFError("RANKING_MARKET_TIMESTAMP_MISMATCH", str(row.get("symbol")))
        if stamp.astimezone(IST) >= decision or stamp.astimezone(IST) + timedelta(minutes=interval) > decision:
            raise Phase4GYFError("FUTURE_DATA_ACCESS", f"bar {row.get('symbol')} {stamp.isoformat()} is not closed by {decision.isoformat()}")
        availability = _bar_decision_exclusion(row, decision, settling_seconds=settling_seconds)
        if availability == "late_receipt":
            continue
        if availability in {"bar_not_closed_status", "bar_not_immutable",
                            "bar_settling_period_not_elapsed"}:
            raise Phase4GYFError("RANKING_NON_IMMUTABLE_BAR_REJECTED", str(row.get("symbol")))
        if str(row.get("volume_kind", "UNKNOWN")).upper() != "BAR":
            raise Phase4GYFError("VOLUME_SEMANTICS_UNVERIFIED", "relative-volume ranking needs empirically verified interval volumes")
        grouped[str(row["symbol"]).strip().upper()].append((stamp.astimezone(IST), row))

    features: dict[str, dict[str, float]] = {}
    for symbol, pairs in grouped.items():
        pairs.sort(key=lambda item: item[0])
        stamps = [item[0] for item in pairs]
        if len(stamps) != len(set(stamps)):
            raise Phase4GYFError("RANKING_DUPLICATE_TIMESTAMP", symbol)
        if any(right < left for left, right in zip(stamps, stamps[1:])):
            raise Phase4GYFError("RANKING_OUT_OF_ORDER", symbol)
        bars = [item[1] for item in pairs]
        if len(bars) < max(minimum_bars, 21):
            continue
        volumes = [float(item["volume"]) for item in bars]
        if any(not math.isfinite(value) or value < 0 for value in volumes):
            raise Phase4GYFError("RANKING_VOLUME_INVALID", symbol)
        previous_volume = volumes[-21:-1]
        baseline = sum(previous_volume) / len(previous_volume) if previous_volume else 0.0
        if baseline <= 0:
            continue
        closes = [float(item["close"]) for item in bars]
        first_open = float(bars[0]["open"])
        latest = bars[-1]
        session_return = closes[-1] / first_open - 1.0
        return_5 = closes[-1] / closes[-6] - 1.0
        return_15 = closes[-1] / closes[-16] - 1.0
        momentum = 0.50 * session_return + 0.25 * return_5 + 0.25 * return_15
        relative_volume = volumes[-1] / baseline
        typical = [(float(item["high"]) + float(item["low"]) + float(item["close"])) / 3 for item in bars]
        total_volume = sum(volumes)
        if total_volume <= 0:
            continue
        vwap = sum(price * volume for price, volume in zip(typical, volumes)) / total_volume
        vwap_distance = closes[-1] / vwap - 1.0
        prior_ranges = [float(item["high"]) - float(item["low"]) for item in bars[-21:-1]]
        mean_range = sum(prior_ranges) / len(prior_ranges) if prior_ranges else 0.0
        current_range = float(latest["high"]) - float(latest["low"])
        range_expansion = current_range / mean_range if mean_range > 0 else 0.0
        local_open = datetime.combine(decision.date(), SESSION_OPEN, tzinfo=IST)
        elapsed = max(1, int((min(stamps[-1] + timedelta(minutes=1), decision) - local_open).total_seconds() // 60))
        quality = min(1.0, len(bars) / elapsed) * (1.0 if total_volume > 0 else 0.0)
        features[symbol] = {
            "momentum": momentum, "session_return": session_return,
            "return_5_bars": return_5, "return_15_bars": return_15,
            "relative_volume": relative_volume, "vwap": vwap,
            "vwap_distance": vwap_distance, "above_vwap": float(closes[-1] > vwap),
            "range_expansion": range_expansion, "data_quality_liquidity": quality,
            "last_price": closes[-1], "observed_bars": float(len(bars)),
        }
    if not features:
        return []
    # The score label is ``vwap_strength`` while the measured input retained in
    # each row is the signed distance from VWAP.  Positive distance ranks above
    # VWAP; negative distance ranks below it.
    score_inputs = {
        "momentum": "momentum",
        "relative_volume": "relative_volume",
        "vwap_strength": "vwap_distance",
        "range_expansion": "range_expansion",
        "data_quality_liquidity": "data_quality_liquidity",
    }
    normalized = {
        component: _rank_percentiles({symbol: values[feature] for symbol, values in features.items()})
        for component, feature in score_inputs.items()
    }
    ranked = []
    for symbol, values in features.items():
        score = sum(SCORE_WEIGHTS[name] * normalized[name][symbol] for name in SCORE_WEIGHTS)
        ranked.append({
            "decision_timestamp": decision.isoformat(), "symbol": symbol,
            "score": score, **values,
            "score_momentum": normalized["momentum"][symbol],
            "score_relative_volume": normalized["relative_volume"][symbol],
            "score_vwap_strength": normalized["vwap_strength"][symbol],
            "score_range_expansion": normalized["range_expansion"][symbol],
            "score_data_quality_liquidity": normalized["data_quality_liquidity"][symbol],
        })
    ranked.sort(key=lambda item: (-item["score"], item["symbol"]))
    for rank, item in enumerate(ranked[:top_n], start=1):
        item["rank"] = rank
    return ranked[:top_n]


def aggregate_complete_1m_to_5m(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Derive only complete 5m bars from five observed consecutive 1m bars."""
    grouped: dict[str, dict[datetime, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        if str(row.get("timeframe", "")).lower() != "1m":
            continue
        stamp = datetime.fromisoformat(str(row["timestamp"]))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise Phase4GYFError("AGGREGATION_TIMEZONE_UNKNOWN", str(row.get("timestamp")))
        symbol = str(row["symbol"]).upper()
        stamp = stamp.astimezone(IST)
        if stamp in grouped[symbol]:
            raise Phase4GYFError("AGGREGATION_DUPLICATE_TIMESTAMP", f"{symbol} {stamp.isoformat()}")
        if str(row.get("status", "")).upper() != "CLOSED":
            raise Phase4GYFError("AGGREGATION_FORMING_BAR_REJECTED", f"{symbol} {stamp.isoformat()}")
        if str(row.get("bar_status", "")).upper() != "IMMUTABLE_CLOSED":
            raise Phase4GYFError("AGGREGATION_NON_IMMUTABLE_BAR_REJECTED", f"{symbol} {stamp.isoformat()}")
        grouped[symbol][stamp] = row
    result = []
    for symbol, bars in grouped.items():
        starts = sorted(bars)
        by_start = set(starts)
        blocks: set[datetime] = set()
        for stamp in starts:
            offset = (stamp.hour * 60 + stamp.minute) - (9 * 60 + 15)
            if offset < 0:
                continue
            block_offset = (offset // 5) * 5
            blocks.add(datetime.combine(stamp.date(), SESSION_OPEN, tzinfo=IST) + timedelta(minutes=block_offset))
        for block in sorted(blocks):
            expected = [block + timedelta(minutes=minute) for minute in range(5)]
            if not all(stamp in by_start for stamp in expected):
                continue
            members = [bars[stamp] for stamp in expected]
            volumes_known = all(str(item.get("volume_kind", "UNKNOWN")).upper() == "BAR" for item in members)
            received_values = [datetime.fromisoformat(str(item["received_timestamp"]))
                               for item in members if item.get("received_timestamp")]
            started_values = [datetime.fromisoformat(str(item["request_started_at"]))
                              for item in members if item.get("request_started_at")]
            receipt = max(received_values).astimezone(IST).isoformat() if received_values else None
            request_started = min(started_values).astimezone(IST).isoformat() if started_values else None
            result.append({
                "timestamp": block.isoformat(), "market_timestamp": block.isoformat(),
                "received_timestamp": receipt, "request_started_at": request_started,
                "response_received_at": receipt,
                "batch_id": members[-1].get("batch_id"),
                "symbol": symbol, "exchange": "NSE", "series": "EQ",
                "timeframe": "5m", "open": float(members[0]["open"]),
                "high": max(float(item["high"]) for item in members),
                "low": min(float(item["low"]) for item in members),
                "close": float(members[-1]["close"]),
                "unadjusted_close": float(members[-1].get("unadjusted_close", members[-1]["close"])),
                "adjusted_close": None,
                "volume": sum(float(item["volume"]) for item in members) if volumes_known else None,
                "volume_kind": "BAR" if volumes_known else "UNKNOWN", "status": "CLOSED",
                "bar_status": "IMMUTABLE_CLOSED",
                "immutable_eligible_at": max(str(item.get("immutable_eligible_at") or "") for item in members),
                "immutable_at": max(str(item.get("immutable_at") or "") for item in members),
                "source": "yahoo_finance_yfinance_derived_from_five_observed_1m_bars",
                "source_timestamp": members[0].get("source_timestamp"),
                "source_timezone": members[0].get("source_timezone"),
            })
    return sorted(result, key=lambda item: (item["timestamp"], item["symbol"]))


def reconciliation_report(forward_rows: Sequence[Mapping[str, Any]],
                          final_rows: Sequence[Mapping[str, Any]], *,
                          requested_symbols: Sequence[str]) -> dict[str, Any]:
    """Compare first-observed forward bars with post-close source bars only."""
    def keyed(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[str, str], Mapping[str, Any]]:
        out: dict[tuple[str, str], Mapping[str, Any]] = {}
        for row in rows:
            key = (str(row["symbol"]).upper(), str(row["timestamp"]))
            if key in out:
                raise Phase4GYFError("RECONCILIATION_DUPLICATE_KEY", repr(key))
            out[key] = row
        return out
    forward = keyed(forward_rows)
    final = keyed(final_rows)
    matched = sorted(forward.keys() & final.keys())
    late = sorted(final.keys() - forward.keys())
    missing = sorted(forward.keys() - final.keys())
    price_diffs = []
    volume_diffs = []
    for key in matched:
        left, right = forward[key], final[key]
        if any(float(left[field]) != float(right[field]) for field in ("open", "high", "low", "close")):
            price_diffs.append({"symbol": key[0], "timestamp": key[1],
                                "forward": {field: left[field] for field in ("open", "high", "low", "close")},
                                "after_close": {field: right[field] for field in ("open", "high", "low", "close")}})
        if float(left["volume"]) != float(right["volume"]):
            volume_diffs.append({"symbol": key[0], "timestamp": key[1],
                                  "forward": left["volume"], "after_close": right["volume"]})
    forward_symbols = {key[0] for key in forward}
    final_symbols = {key[0] for key in final}
    return {
        "source": "Yahoo Finance/yfinance; THIRD_PARTY_RESEARCH_SOURCE",
        "forward_bars_observed": len(forward), "after_close_bars_available": len(final),
        "matched_bars": len(matched), "late_arriving_bars": len(late),
        "bars_available_after_close_not_seen_during_session": len(late),
        "forward_bars_absent_after_close": len(missing),
        "forward_symbol_coverage": sorted(forward_symbols), "after_close_symbol_coverage": sorted(final_symbols),
        "missing_forward_symbols": sorted(set(requested_symbols) - forward_symbols),
        "missing_after_close_symbols": sorted(set(requested_symbols) - final_symbols),
        "price_differences": price_diffs, "price_difference_count": len(price_diffs),
        "volume_differences": volume_diffs, "volume_difference_count": len(volume_diffs),
        "signal_mutation": "PROHIBITED_AND_NOT_PERFORMED",
        "ledger_mutation": "PROHIBITED_AND_NOT_PERFORMED",
    }


def paper_config() -> Phase4CPaperConfig:
    """Fixed Phase 4C defaults requested for this paper-only experiment."""
    return Phase4CPaperConfig(
        enabled=True, execution_mode=ExecutionMode.PAPER, initial_capital=100_000.0,
        risk_per_trade=0.01, max_positions=5, max_daily_loss=0.02,
        max_portfolio_drawdown=0.10, max_symbol_exposure=0.25,
        max_sector_exposure=0.25, max_gross_exposure=1.0,
        realtime_enabled=False, live_trading_enabled=False,
        force_intraday_exit=True,
    )


class Phase4GYFForwardProcessor:
    """Incremental data→Phase 4E→Phase 4B→Phase 4C paper path.

    ``process_snapshot`` must be called only during the live target session.
    Repeated source snapshots are merged first-observed-wins; revised old bars
    are journaled for reconciliation and never reprocessed into a signal.
    """

    def __init__(self, *, project_root: str | Path, output_dir: str | Path,
                 instruments: Mapping[str, YahooInstrument], session_date: date = SESSION_DATE,
                 scanner: IntradayScanner | None = None, engine: PaperExecutionEngine | None = None,
                 require_ranked_candidate: bool = False,
                 revision_seed_dir: str | Path | None = None,
                 bar_settling_seconds: int = BAR_SETTLING_SECONDS) -> None:
        self.root = Path(project_root).resolve()
        self.output = Path(output_dir).resolve()
        self.session_date = session_date
        self.require_ranked_candidate = bool(require_ranked_candidate)
        if int(bar_settling_seconds) < 0:
            raise ValueError("bar_settling_seconds must be non-negative")
        self.bar_settling_seconds = int(bar_settling_seconds)
        self.revision_seed_dir = Path(revision_seed_dir).resolve() if revision_seed_dir else None
        self.instruments = dict(instruments)
        self.symbols = tuple(self.instruments)
        self.output.mkdir(parents=True, exist_ok=True)
        self.ledger_path = self.output / "paper_ledger.json"
        self.scanner = scanner or IntradayScanner(
            self.symbols,
            config=IntradayScannerConfig(enabled=True, universe_size=len(self.symbols), timeframes=("1m", "5m")),
            calendar=default_nse_calendar(),
        )
        if engine is None:
            app = AppConfig.load(self.root / "config.yaml")
            engine = PaperExecutionEngine(paper_config(), cost_settings=app.costs,
                                          calendar=default_nse_calendar(), ledger_path=self.ledger_path)
        self.engine = engine
        self.bar_store = CanonicalBarStore()
        self.reconciler = YahooRevisionReconciler(
            session_date=self.session_date, settling_seconds=self.bar_settling_seconds,
            expected_symbols=self.symbols,
        )
        # Compatibility alias for the rest of Phase 4G; keys are now exactly
        # (canonical symbol, canonical market timestamp).
        self.bar_rows = self.bar_store.records
        self.processed_ids: set[str] = set()
        self.revisions: list[dict[str, Any]] = []
        self.rankings: list[dict[str, Any]] = []
        self.signals: list[dict[str, Any]] = []
        self.signal_audit: list[dict[str, Any]] = []
        self._audit_ids: set[str] = set()
        self.decision_snapshots: list[dict[str, Any]] = []
        self.last_available_timestamp: str | None = None
        self.last_processed_timestamp: str | None = None
        self.latest_observed_at: str | None = None
        self.late_bars_not_used_for_execution = 0
        self.delayed_signals_suppressed = 0
        self.warmed_up = False
        self.warmup_event_ids: set[str] = set()
        self.signal_keys: set[tuple[str, str, str, str]] = set()
        self.skipped_decisions: list[dict[str, str]] = []
        self.performance_seconds = {
            "validation_seconds": 0.0, "deduplication_seconds": 0.0,
            "ranking_seconds": 0.0, "indicator_seconds": 0.0,
            "indicator_strategy_seconds": 0.0, "scanner_total_seconds": 0.0,
            "strategy_seconds": 0.0, "execution_seconds": 0.0,
        }
        self._restore()

    def _record_scanner_timing(self, elapsed: float) -> None:
        """Accumulate scanner components when the production scanner exposes them."""
        indicator = getattr(self.scanner, "last_indicator_duration_seconds", None)
        strategy = getattr(self.scanner, "last_strategy_duration_seconds", None)
        self.performance_seconds["scanner_total_seconds"] += elapsed
        self.performance_seconds["indicator_strategy_seconds"] += elapsed
        if isinstance(indicator, (int, float)) and math.isfinite(float(indicator)):
            self.performance_seconds["indicator_seconds"] += max(0.0, float(indicator))
        if isinstance(strategy, (int, float)) and math.isfinite(float(strategy)):
            self.performance_seconds["strategy_seconds"] += max(0.0, float(strategy))
        elif indicator is None:
            # Test doubles and external scanner adapters without split timings
            # are reported honestly as combined strategy time.
            self.performance_seconds["strategy_seconds"] += elapsed

    def _restore(self) -> None:
        canonical = self.output / "canonical/forward_bars_1m.csv"
        canonical_rows: list[dict[str, Any]] = []
        if canonical.is_file():
            with canonical.open("r", newline="", encoding="utf-8") as stream:
                canonical_rows.extend(dict(row) for row in csv.DictReader(stream))
        if self.engine.events:
            for event in self.engine.events:
                if event.get("event_type") == "MARKET_BAR_PROCESSED":
                    bar_event_id = event.get("payload", {}).get("bar_event_id")
                    if bar_event_id:
                        self.processed_ids.add(str(bar_event_id))
        for path, target in ((self.output / "rankings/candidate_rankings.csv", self.rankings),
                             (self.output / "signals/signals.csv", self.signals)):
            if path.is_file():
                with path.open("r", newline="", encoding="utf-8") as stream:
                    target.extend(dict(row) for row in csv.DictReader(stream))
        audit_path = self.output / "reports/signal_audit.json"
        if audit_path.is_file():
            try:
                audit_items = json.loads(audit_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise Phase4GYFError("SIGNAL_AUDIT_INVALID", str(exc)) from exc
            if not isinstance(audit_items, list) or any(not isinstance(item, dict) for item in audit_items):
                raise Phase4GYFError("SIGNAL_AUDIT_INVALID", str(audit_path))
            self.signal_audit.extend(audit_items)
            self._audit_ids.update(str(item.get("audit_id")) for item in audit_items if item.get("audit_id"))
            self.signal_keys.update(
                (str(item.get("symbol") or "").upper(), str(item.get("strategy") or ""),
                 str(item.get("market_timestamp") or ""), str(item.get("direction") or "LONG").upper())
                for item in audit_items
                if item.get("symbol") and item.get("strategy") and item.get("market_timestamp")
            )
        snapshot_path = self.output / "reports/decision_snapshots.jsonl"
        if snapshot_path.is_file():
            with snapshot_path.open("r", encoding="utf-8") as stream:
                for line in stream:
                    try:
                        self.decision_snapshots.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        raise Phase4GYFError("DECISION_SNAPSHOT_INVALID", str(exc)) from exc
        state = self.output / "run_state.json"
        state_value: dict[str, Any] = {}
        if state.is_file():
            try:
                value = json.loads(state.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise Phase4GYFError("RUN_STATE_INVALID", str(exc)) from exc
            if value.get("session_date") != self.session_date.isoformat():
                raise Phase4GYFError("RUN_STATE_SESSION_MISMATCH")
            self.last_available_timestamp = value.get("last_available_timestamp")
            self.last_processed_timestamp = value.get("last_processed_timestamp")
            self.latest_observed_at = value.get("latest_observed_at")
            self.late_bars_not_used_for_execution = int(value.get("late_bars_not_used_for_execution", 0))
            self.delayed_signals_suppressed = int(value.get("delayed_signals_suppressed", 0))
            self.revisions = list(value.get("bar_revisions", []))
            self.warmed_up = bool(value.get("warmed_up", False))
            self.warmup_event_ids.update(map(str, value.get("warmup_event_ids", [])))
            self.skipped_decisions = list(value.get("skipped_decisions", []))
            if value.get("processed_event_ids"):
                self.processed_ids.update(map(str, value["processed_event_ids"]))
            state_value = value

        # Replay the observation journal when available. A pre-YF.1 directory is
        # migrated from its immutable raw CSV plus the compact revision rows in
        # run_state.json. Explicit seed directories let an isolated controlled
        # refresh compare against the protected existing monitor without
        # copying its ledger or paper state.
        journal = self.output / "reports/observations.jsonl"
        replay_rows: list[dict[str, Any]] = []
        if journal.is_file():
            try:
                with journal.open("r", encoding="utf-8") as stream:
                    for line in stream:
                        if line.strip():
                            record = json.loads(line)
                            if isinstance(record, dict) and record.get("symbol") and record.get("received_timestamp"):
                                replay_rows.append(record)
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise Phase4GYFError("REVISION_JOURNAL_INVALID", str(exc)) from exc
        else:
            replay_rows.extend(canonical_rows)
            for item in state_value.get("bar_revisions", []):
                if not isinstance(item, Mapping):
                    continue
                later = item.get("later_source_revision")
                if isinstance(later, Mapping):
                    replay_rows.append(dict(later))

        if self.revision_seed_dir and self.revision_seed_dir != self.output:
            seed_canonical = self.revision_seed_dir / "canonical/forward_bars_1m.csv"
            if seed_canonical.is_file():
                with seed_canonical.open("r", newline="", encoding="utf-8") as stream:
                    replay_rows.extend(dict(row) for row in csv.DictReader(stream))
            seed_state_path = self.revision_seed_dir / "run_state.json"
            if seed_state_path.is_file():
                try:
                    seed_state = json.loads(seed_state_path.read_text(encoding="utf-8"))
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise Phase4GYFError("REVISION_SEED_STATE_INVALID", str(exc)) from exc
                for item in seed_state.get("bar_revisions", []):
                    if isinstance(item, Mapping) and isinstance(item.get("later_source_revision"), Mapping):
                        replay_rows.append(dict(item["later_source_revision"]))

        if replay_rows:
            latest_receipt: datetime | None = None
            for row in replay_rows:
                try:
                    received = datetime.fromisoformat(str(row["received_timestamp"]))
                    if received.tzinfo is not None and (latest_receipt is None or received > latest_receipt):
                        latest_receipt = received
                except (KeyError, TypeError, ValueError):
                    continue
            replay_decision = latest_receipt or datetime.combine(self.session_date, SESSION_OPEN, tzinfo=IST)
            self.reconciler.replay_observations(replay_rows, decision_timestamp=replay_decision)
        if journal.is_file():
            def line_count(path: Path) -> int:
                if not path.is_file():
                    return 0
                with path.open("r", encoding="utf-8") as stream:
                    return sum(1 for line in stream if line.strip())

            self.reconciler.mark_persisted_counts(
                observations=line_count(journal),
                revisions=line_count(self.output / "reports/revision_records.jsonl"),
                immutable=line_count(self.output / "reports/immutable_bar_audit.jsonl"),
            )
            persisted_readiness = self.output / "reports/readiness_report.json"
            if persisted_readiness.is_file():
                try:
                    persisted_gate = json.loads(persisted_readiness.read_text(encoding="utf-8"))
                    self.reconciler.readiness_reasons.update(
                        str(item) for item in persisted_gate.get("blocking_reasons", [])
                    )
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise Phase4GYFError("READINESS_REPORT_INVALID", str(exc)) from exc
        # Canonical strategy state is reconstructed exclusively from immutable
        # snapshots; old provisional CLOSED rows are never restored directly.
        for row in self.reconciler.immutable_rows():
            try:
                self.bar_store.restore(row)
            except (KeyError, TypeError, ValueError) as exc:
                raise Phase4GYFError("CANONICAL_BAR_STORE_CORRUPTED", str(exc)) from exc

        # Rebuild indicators solely from immutable bars already present.
        # Signals emitted during this warm-up are intentionally discarded.
        if self.bar_rows and self.warmed_up:
            one_minute = list(self.bar_rows.values())
            warm_rows = one_minute + aggregate_complete_1m_to_5m(one_minute)
            for row in sorted(warm_rows, key=lambda item: (self._bar(item).end, item["symbol"], item["timeframe"])):
                self.scanner.handle_bar(self._bar(row))

    @staticmethod
    def _bar(row: Mapping[str, Any]) -> IntradayBar:
        stamp = datetime.fromisoformat(str(row["timestamp"]))
        volume = None if row.get("volume") in (None, "", "nan", "NaN") else float(row["volume"])
        return IntradayBar(
            symbol=str(row["symbol"]), exchange=str(row.get("exchange", "NSE")),
            series=str(row.get("series", "EQ")), timeframe=str(row["timeframe"]),
            timestamp=stamp, open=float(row["open"]), high=float(row["high"]),
            low=float(row["low"]), close=float(row["close"]), volume=volume,
            status=str(row.get("status", "CLOSED")), source=str(row.get("source", YAHOO_SOURCE)),
            volume_kind=str(row.get("volume_kind", "UNKNOWN")),
            bar_status=str(row.get("bar_status", "")),
        )

    def _event_id(self, row: Mapping[str, Any]) -> str:
        # Event id follows market identity. The 5m timeframe distinguishes a
        # derived interval from its 1m source bars; receipt time and payload are
        # deliberately excluded (payload changes are handled as conflicts).
        market = str(row.get("market_timestamp") or row["timestamp"])
        timeframe = str(row.get("timeframe", "1m")).lower()
        identity = [str(row["symbol"]).strip().upper(), market]
        if timeframe != "1m":
            identity.append(timeframe)
        raw = json.dumps(identity, separators=(",", ":"))
        return "4GYF-" + hashlib.sha256(raw.encode()).hexdigest()[:24]

    def _persist_state(self) -> None:
        _atomic_json(self.output / "run_state.json", {
            "schema_version": 1, "session_date": self.session_date.isoformat(),
            "bar_settling_seconds": self.bar_settling_seconds,
            "last_available_timestamp": self.last_available_timestamp,
            "last_processed_timestamp": self.last_processed_timestamp,
            "latest_observed_at": self.latest_observed_at,
            "late_bars_not_used_for_execution": self.late_bars_not_used_for_execution,
            "delayed_signals_suppressed": self.delayed_signals_suppressed,
            "warmed_up": self.warmed_up,
            "processed_event_ids": sorted(self.processed_ids),
            "warmup_event_ids": sorted(self.warmup_event_ids),
            "bar_revisions": self.revisions,
            "skipped_decisions": self.skipped_decisions,
            "performance_seconds": self.performance_seconds,
            "immutable_bar_readiness": self.reconciler.readiness_report(),
            "signals_count": len(self.signals), "rankings_count": len(self.rankings),
        })

    def _persist_views(self) -> None:
        ordered = self.bar_store.ordered_rows()
        canonical_columns = ["timestamp", "market_timestamp", "received_timestamp", "request_started_at",
                             "response_received_at", "batch_id", "symbol", "exchange", "series", "timeframe",
                             "open", "high", "low", "close", "unadjusted_close", "adjusted_close", "volume",
                             "volume_kind", "status", "source", "yahoo_symbol", "isin", "source_timestamp",
                             "source_timezone", "adjustment_status", "bar_status", "observation_hash",
                             "immutable_eligible_at", "immutable_at"]
        _atomic_csv(self.output / "canonical/forward_bars_1m.csv", ordered, canonical_columns)
        _atomic_csv(self.output / "canonical/derived_bars_5m.csv", aggregate_complete_1m_to_5m(ordered))
        _atomic_csv(self.output / "rankings/candidate_rankings.csv", self.rankings)
        _atomic_csv(self.output / "signals/signals.csv", self.signals)
        orders = [asdict(item) for item in self.engine.all_orders]
        fills = [item for item in orders if item.get("status") in {"FILLED", "PARTIALLY_FILLED"}]
        positions = [asdict(item) for item in self.engine.open_positions]
        trades = [asdict(item) for item in self.engine.all_trades]
        risk_events = [asdict(item) for item in self.engine.risk_events]
        _atomic_csv(self.output / "orders/orders.csv", orders)
        _atomic_csv(self.output / "fills/fills.csv", fills)
        _atomic_csv(self.output / "positions/open_positions.csv", positions)
        _atomic_csv(self.output / "risk/risk_events.csv", risk_events)
        _atomic_csv(self.output / "reports/closed_trades.csv", trades)
        _atomic_json(self.output / "reports/signal_audit.json", self.signal_audit)
        _atomic_jsonl(self.output / "reports/signal_audit.jsonl", self.signal_audit)

    def _record_audit(self, item: Mapping[str, Any]) -> None:
        record = dict(item)
        identity = str(record.get("audit_id") or "")
        if not identity or identity in self._audit_ids:
            return
        self._audit_ids.add(identity)
        self.signal_audit.append(record)

    def _write_bar_accounting(self, plan: BarIngestPlan, *, observed_at: datetime,
                              outcome: str, validation_status: str,
                              source_issues: Sequence[Mapping[str, Any]] = (),
                              timings: Mapping[str, Any] | None = None) -> None:
        metrics = dict(plan.accounting)
        refresh_id = hashlib.sha256(
            f"{observed_at.astimezone(IST).isoformat()}|{metrics['rows_seen']}|"
            f"{metrics['unique_market_bars']}|{metrics['new_market_bars']}".encode("utf-8")
        ).hexdigest()[:24]
        summary = {
            "refresh_id": refresh_id,
            "decision_timestamp": observed_at.astimezone(IST).isoformat(),
            "outcome": outcome,
            "validation_status": validation_status,
            **metrics,
            "performance_durations_seconds": dict(timings or {}),
            "source_issues": [dict(item) for item in source_issues],
            "bar_identity": "(symbol, market_timestamp)",
            "receipt_time_is_identity": False,
            "paper_only": True,
            "broker_calls": 0,
            "live_orders": 0,
        }
        _append_jsonl(self.output / "reports/refresh_audit.jsonl", summary)
        for item in plan.accounting.get("per_symbol", []):
            _append_jsonl(self.output / "reports/bar_accounting.jsonl", {
                "refresh_id": refresh_id,
                "decision_timestamp": observed_at.astimezone(IST).isoformat(),
                **dict(item),
            })
        for conflict in plan.conflicts:
            _append_jsonl(self.output / "reports/bar_conflicts.jsonl", {
                "refresh_id": refresh_id,
                "decision_timestamp": observed_at.astimezone(IST).isoformat(),
                **conflict,
            })
        for invalid in plan.invalid_rows:
            _append_jsonl(self.output / "reports/bar_invalid_rows.jsonl", {
                "refresh_id": refresh_id,
                "decision_timestamp": observed_at.astimezone(IST).isoformat(),
                **invalid,
            })

    def process_snapshot(self, rows: Sequence[Mapping[str, Any]], *, now: datetime,
                         available_timestamp: datetime | None = None,
                         data_quality_missing_symbols: Sequence[str] = (),
                         refresh_failures: Mapping[str, Mapping[str, Any]] | None = None,
                         source_issues: Sequence[Mapping[str, Any]] = (),
                         source_invalid_rows: Sequence[Mapping[str, Any]] = (),
                         refresh_timings: Mapping[str, Any] | None = None,
                         minimum_universe_coverage: float = MINIMUM_UNIVERSE_COVERAGE) -> dict[str, Any]:
        """Validate, deduplicate, snapshot, and process only newly observed closed bars."""
        total_started = time_module.perf_counter()
        if now.tzinfo is None or now.utcoffset() is None:
            raise Phase4GYFError("UNKNOWN_TIMEZONE", "observer clock must be timezone aware")
        local_now = now.astimezone(IST)
        if local_now.date() != self.session_date or not (SESSION_OPEN <= local_now.timetz().replace(tzinfo=None) < SESSION_CLOSE):
            raise Phase4GYFError("PHASE4G_YF_FORWARD_WINDOW_CLOSED", local_now.isoformat())
        previous_observed_at = datetime.fromisoformat(self.latest_observed_at) if self.latest_observed_at else None
        dedup_started = time_module.perf_counter()
        self.reconciler.observe_many(rows, decision_timestamp=local_now,
                                     source_issues=source_invalid_rows)
        readiness = self.reconciler.readiness_report(decision_timestamp=local_now)
        self.reconciler.write_artifacts(self.output, decision_timestamp=local_now)
        self.performance_seconds["deduplication_seconds"] += time_module.perf_counter() - dedup_started
        self.latest_observed_at = local_now.isoformat()
        if readiness["status"] == "BLOCKED":
            raise Phase4GYFError(
                "PHASE4G_YF_IMMUTABLE_BAR_READINESS_BLOCKED",
                json.dumps(readiness, sort_keys=True),
            )

        immutable_rows = self.reconciler.immutable_rows()
        snapshot_rows = [row for row in immutable_rows
                         if canonical_bar_key(row) not in self.bar_store.records]
        if not snapshot_rows:
            if not rows and not self.bar_store.records:
                empty_plan = self.bar_store.plan_ingest(
                    [], decision_timestamp=local_now, expected_symbols=self.symbols,
                    warmed_up=self.warmed_up, previous_decision_timestamp=previous_observed_at,
                    refresh_failures=refresh_failures,
                )
                self._write_bar_accounting(
                    empty_plan, observed_at=local_now, outcome="BLOCKED_NO_VALID_ROWS",
                    validation_status="FAIL", source_issues=source_issues,
                    timings={"bar_settling_seconds": self.bar_settling_seconds},
                )
                raise Phase4GYFError("YAHOO_NO_INTRADAY_DATA",
                                     "refresh contained no valid unique market bars")
            self._persist_state()
            return {
                "status": "PHASE4G_YF_WAITING_FOR_IMMUTABLE_BARS",
                "immutable_bar_readiness": readiness,
                "bar_status_counts": readiness["bar_status_counts"],
                "rows_seen": len(rows), "new_immutable_bars": 0,
                "new_market_bars": 0, "duplicate_identical": self.reconciler.last_batch_counts.get("identical_duplicates", 0),
                "duplicate_conflict": self.reconciler.last_batch_counts.get("post_immutability_revisions", 0),
                "signals": 0, "new_signals": 0,
                "orders": len(self.engine.all_orders), "fills": sum(
                    item.status in {"FILLED", "PARTIALLY_FILLED"} for item in self.engine.all_orders
                ),
                "ledger_mutations": len(self.engine.events),
                "reason": ("no newly immutable bars are available; forming and settling bars "
                           "are never sent to the strategy"),
            }

        plan = self.bar_store.plan_ingest(
            snapshot_rows, decision_timestamp=local_now, expected_symbols=self.symbols,
            warmed_up=self.warmed_up, previous_decision_timestamp=previous_observed_at,
            refresh_failures=refresh_failures,
        )
        dedup_elapsed = time_module.perf_counter() - dedup_started
        audit_timings = dict(refresh_timings or {})
        audit_timings["deduplication_duration_seconds"] = dedup_elapsed
        audit_timings["bar_settling_seconds"] = self.bar_settling_seconds

        def write_refresh_audit(outcome: str, validation_status: str) -> None:
            self._write_bar_accounting(
                plan, observed_at=local_now, outcome=outcome,
                validation_status=validation_status, source_issues=source_issues,
                timings=audit_timings,
            )

        if plan.conflicts or plan.invalid_rows:
            blocked_status = "BLOCKED_BAR_CONFLICT" if plan.conflicts else "BLOCKED_INVALID_BAR"
            write_refresh_audit(blocked_status, "FAIL")
            code = "DUPLICATE_MARKET_BAR_CONFLICT" if plan.conflicts else "INVALID_MARKET_BAR"
            raise Phase4GYFError(code, json.dumps({
                "duplicate_conflict": plan.conflicts,
                "invalid_rows": plan.invalid_rows,
                "accounting": {key: value for key, value in plan.accounting.items() if key != "per_symbol"},
            }, default=str, sort_keys=True))
        snapshot_rows = plan.snapshot_rows
        if any(datetime.fromisoformat(str(row["market_timestamp"])).astimezone(IST).date() != self.session_date
               for row in snapshot_rows):
            write_refresh_audit("BLOCKED_WRONG_SESSION", "FAIL")
            raise Phase4GYFError("PHASE4G_YF_WRONG_SESSION_DATA")

        symbols_present = sorted({str(row["symbol"]).upper() for row in snapshot_rows})
        coverage = len(symbols_present) / len(self.symbols)
        if coverage < minimum_universe_coverage:
            write_refresh_audit("BLOCKED_LOW_REFRESH_COVERAGE", "FAIL")
            raise Phase4GYFError("INSUFFICIENT_SYMBOL_COVERAGE",
                                 f"current snapshot has {len(symbols_present)}/{len(self.symbols)} symbols ({coverage:.1%})")

        validation_started = time_module.perf_counter()
        report = validate_intraday_dataset(
            snapshot_rows, expected_symbols=symbols_present, expected_sessions=[self.session_date],
            timeframe="1m", expected_exchange="NSE", expected_series="EQ",
        )
        self.performance_seconds["validation_seconds"] += time_module.perf_counter() - validation_started
        if report.validation_status != "PASS":
            write_refresh_audit("BLOCKED_PHASE4E_VALIDATION", report.validation_status)
            raise Phase4GYFError("PHASE4E_VALIDATION_FAILED", json.dumps(report.to_dict(), default=str))

        try:
            self.bar_store.commit(plan)
        except ValueError as exc:
            write_refresh_audit("BLOCKED_STORE_COMMIT", "FAIL")
            raise Phase4GYFError("BAR_STORE_COMMIT_FAILED", str(exc)) from exc
        write_refresh_audit("ACCEPTED", report.validation_status)
        self.revisions.extend(plan.conflicts)

        if available_timestamp is not None:
            if available_timestamp.tzinfo is None or available_timestamp.utcoffset() is None:
                raise Phase4GYFError("UNKNOWN_TIMEZONE", "last available timestamp is naive")
            available_text = available_timestamp.astimezone(IST).isoformat()
            if self.last_available_timestamp is None or available_text > self.last_available_timestamp:
                self.last_available_timestamp = available_text

        all_1m = self.bar_store.ordered_rows()
        current_decision_snapshot = build_decision_snapshot(
            all_1m, decision=local_now, expected_symbols=self.symbols,
            data_quality_missing_symbols=data_quality_missing_symbols,
            settling_seconds=self.bar_settling_seconds,
        )
        self._store_decision_snapshot(current_decision_snapshot)
        self.latest_observed_at = local_now.isoformat()
        self.late_bars_not_used_for_execution += plan.accounting["late_bars"]

        all_have_interval_volume = all(str(row.get("volume_kind", "UNKNOWN")).upper() == "BAR" for row in all_1m)
        base_result: dict[str, Any] = {
            "requested_symbols": len(self.symbols),
            "symbols_with_data": len(symbols_present),  # current-refresh coverage only
            "symbols_with_valid_session_data": plan.accounting["symbols_with_valid_session_data"],
            "symbols_with_new_bars": plan.accounting["symbols_with_new_bars"],
            "symbols_with_refresh_failure": plan.accounting["symbols_with_refresh_failure"],
            "refresh_failure_symbols": plan.accounting["refresh_failure_symbols"],
            "universe_coverage": coverage,
            "phase4e_validation_status": report.validation_status,
            "rows_validated": report.rows,
            "rows_seen": plan.accounting["rows_seen"],
            "unique_rows_seen": plan.accounting["unique_rows_seen"],
            "unique_market_bars": plan.accounting["unique_market_bars"],
            "unique_market_bars_total": plan.accounting["unique_market_bars_total"],
            "new_market_bars": plan.accounting["new_market_bars"],
            "current_session_new_bars": plan.accounting["current_session_new_bars"],
            "duplicate_identical": plan.accounting["duplicate_identical"],
            "duplicate_conflict": plan.accounting["duplicate_conflict"],
            "duplicate_market_bars": plan.accounting["duplicate_market_bars"],
            "invalid_rows": plan.accounting["invalid_rows"],
            "late_bars": plan.accounting["late_bars"],
            "eligible_bars": plan.accounting["eligible_bars"],
            "warmup_bars": plan.accounting["warmup_bars"],
            "per_symbol": plan.accounting["per_symbol"],
            "decision_snapshot": current_decision_snapshot,
        }
        if not all_have_interval_volume:
            self._persist_views()
            self._persist_state()
            return {
                **base_result,
                "status": "PHASE4G_YF_WAITING_VOLUME_SEMANTICS",
                "signals": 0, "new_signals": 0,
                "orders": len(self.engine.all_orders), "ledger_mutations": len(self.engine.events),
                "performance_seconds": dict(self.performance_seconds),
                "reason": "empirical interval-volume check is incomplete; no paper signal was generated",
                "new_bars_processed": 0,
            }

        if not self.warmed_up:
            warm_rows = all_1m + aggregate_complete_1m_to_5m(all_1m)
            for row in sorted(warm_rows, key=lambda item: (self._bar(item).end, item["symbol"], item["timeframe"])):
                strategy_started = time_module.perf_counter()
                self.scanner.handle_bar(self._bar(row))  # warm-up outputs are intentionally discarded
                elapsed = time_module.perf_counter() - strategy_started
                self._record_scanner_timing(elapsed)
                self.warmup_event_ids.add(self._event_id(row))
            self.warmed_up = True
            if warm_rows:
                self.last_processed_timestamp = max(self._bar(row).end for row in warm_rows).isoformat()
            ranking_started = time_module.perf_counter()
            self._record_due_rankings(all_1m, local_now,
                                      data_quality_missing_symbols=data_quality_missing_symbols)
            self.performance_seconds["ranking_seconds"] += time_module.perf_counter() - ranking_started
            self._persist_views()
            self._persist_state()
            return {
                **base_result,
                "status": "PHASE4G_YF_WARMED_UP_NO_RETRO_SIGNALS",
                "signals": 0, "new_signals": 0, "orders": 0, "ledger_mutations": 0,
                "warmup_bars": len(all_1m), "warmup_derived_5m_bars": len(warm_rows) - len(all_1m),
                "current_session_new_bars": 0, "new_bars_processed": 0,
                "paper_from_timestamp": self.last_processed_timestamp,
                "performance_seconds": dict(self.performance_seconds),
            }

        gap_report = check_missing_bar_fraction(all_1m, session_date=self.session_date)
        if local_now.time() >= time(10, 0) and not gap_report["within_limit"]:
            raise Phase4GYFError("EXCESSIVE_MISSING_BARS", json.dumps(gap_report, sort_keys=True))

        strategy_new_rows = [row for row in plan.current_session_new_rows
                             if bar_eligibility_reason(row, local_now,
                                                       settling_seconds=self.bar_settling_seconds) is None]
        late_receipt_count = sum(bar_eligibility_reason(
            row, local_now, settling_seconds=self.bar_settling_seconds) == "late_receipt"
                                 for row in plan.new_rows)
        current_buckets: set[tuple[str, str]] = set()
        for row in strategy_new_rows:
            stamp = datetime.fromisoformat(row["market_timestamp"]).astimezone(IST)
            offset = (stamp.hour * 60 + stamp.minute) - (9 * 60 + 15)
            bucket = datetime.combine(stamp.date(), SESSION_OPEN, tzinfo=IST) + timedelta(minutes=(offset // 5) * 5)
            current_buckets.add((row["symbol"], bucket.isoformat()))
        all_5m = aggregate_complete_1m_to_5m(all_1m)
        strategy_rows = list(strategy_new_rows)
        strategy_rows.extend(
            row for row in all_5m
            if (row["symbol"], row["market_timestamp"]) in current_buckets
            and bar_eligibility_reason(row, local_now,
                                       settling_seconds=self.bar_settling_seconds) is None
        )
        combined: list[tuple[IntradayBar, Mapping[str, Any]]] = []
        for row in strategy_rows:
            event_id = self._event_id(row)
            if event_id in self.processed_ids or event_id in self.warmup_event_ids:
                continue
            combined.append((self._bar(row), row))
        combined.sort(key=lambda item: (item[0].end, item[0].symbol, item[0].timeframe,
                                       item[1].get("market_timestamp", item[1].get("timestamp", ""))))

        ranking_started = time_module.perf_counter()
        self._record_due_rankings(all_1m, local_now,
                                  data_quality_missing_symbols=data_quality_missing_symbols)
        self.performance_seconds["ranking_seconds"] += time_module.perf_counter() - ranking_started
        latest_signal_bar: dict[tuple[str, str], datetime] = {}
        for bar, _ in combined:
            key = (bar.symbol, bar.timeframe)
            latest_signal_bar[key] = max(latest_signal_bar.get(key, bar.end), bar.end)
        pending_signals: list[tuple[Any, Mapping[str, Any], IntradayBar]] = []
        for bar, row in combined:
            event_id = self._event_id(row)
            if event_id in self.processed_ids or event_id in self.warmup_event_ids:
                continue
            execution_eligible = bar_eligibility_reason(
                row, local_now, settling_seconds=self.bar_settling_seconds) is None
            if execution_eligible:
                execution_started = time_module.perf_counter()
                self.engine.process_bar(bar, event_time=local_now, session_id=self.session_date.isoformat(),
                                        bar_event_id=event_id)
                self.performance_seconds["execution_seconds"] += time_module.perf_counter() - execution_started
            strategy_started = time_module.perf_counter()
            signals = self.scanner.handle_bar(bar)
            elapsed = time_module.perf_counter() - strategy_started
            self._record_scanner_timing(elapsed)
            if execution_eligible and bar.end == latest_signal_bar.get((bar.symbol, bar.timeframe)):
                for signal in signals:
                    pending_signals.append((signal, row, bar))
            else:
                self.delayed_signals_suppressed += len(signals)
                for signal in signals:
                    self._record_audit({
                        "audit_id": f"signal|{signal.signal_id}",
                        "decision_timestamp": local_now.isoformat(), "symbol": signal.symbol,
                        "strategy": signal.strategy, "signal": "REJECTED",
                        "market_timestamp": row.get("market_timestamp", row.get("timestamp")),
                        "signal_bar_timestamp": row.get("market_timestamp", row.get("timestamp")),
                        "received_timestamp": row.get("received_timestamp"),
                        "bar_status": row.get("bar_status"),
                        "timestamp_valid": False, "timestamp_proof": False,
                        "strategy_valid": True, "risk_valid": False,
                        "entry": signal.reference_price, "stop_loss": signal.stop_reference,
                        "target": signal.target_reference,
                        "risk_per_share": signal.reference_price - signal.stop_reference,
                        "risk_reward": ((signal.target_reference - signal.reference_price)
                                        / (signal.reference_price - signal.stop_reference)
                                        if signal.reference_price > signal.stop_reference else None),
                        "quantity": None, "risk_amount": None,
                        "candidate_rank": None, "candidate_score": None,
                        "paper_order_id": None, "paper_order_status": None,
                        "rejection_reason": "DELAYED_SIGNAL_SUPPRESSED",
                        "data_quality": "PASS", "reason_codes": list(signal.reason_codes),
                        "evidence": dict(signal.confidence_metadata),
                        "paper_only": True, "broker_calls": 0, "live_orders": 0,
                    })
            self.processed_ids.add(event_id)

        # Pending orders from previous refreshes have already consumed eligible
        # bars. New orders are submitted only after every bar in this response,
        # so the response that produced a BUY can never fill that order.
        new_signals = 0
        rejected_signals = 0
        for signal, row, _bar in pending_signals:
            decision_text = local_now.isoformat()
            market_text = str(row.get("market_timestamp") or row.get("timestamp") or "")
            received_text = str(row.get("received_timestamp") or "")
            try:
                timestamp_proof = bar_eligibility_reason(
                    row, local_now, settling_seconds=self.bar_settling_seconds) is None
            except (TypeError, ValueError):
                timestamp_proof = False
            signal_key = (str(signal.symbol).upper(), str(signal.strategy), market_text,
                          str(signal.side).upper())
            duplicate_signal = signal_key in self.signal_keys
            if not duplicate_signal:
                self.signal_keys.add(signal_key)
            risk_per_share = float(signal.reference_price) - float(signal.stop_reference)
            reward_per_share = float(signal.target_reference) - float(signal.reference_price)
            risk_reward = reward_per_share / risk_per_share if risk_per_share > 0 else None
            signal_stamp = signal.timestamp
            signal_valid_until = signal.valid_until
            if signal_stamp.tzinfo is None or signal_stamp.utcoffset() is None:
                decision_signal = signal
            else:
                # The strategy bar closes before its immutable observation is
                # available. Start signal age at the actual decision instant,
                # retaining the strategy's configured lifetime. The source
                # bar timestamp remains separate provenance below.
                availability_delay = local_now - signal_stamp
                decision_signal = replace(
                    signal, timestamp=local_now,
                    valid_until=signal_valid_until + availability_delay,
                )
            record = {
                "signal_id": signal.signal_id, "timestamp": local_now.isoformat(),
                "strategy_signal_timestamp": signal.timestamp.isoformat(),
                "generated_at": decision_text, "symbol": signal.symbol,
                "strategy": signal.strategy, "direction": signal.side,
                "entry_reference": signal.reference_price,
                "stop_reference": signal.stop_reference, "target_reference": signal.target_reference,
                "confidence_metadata": json.dumps(dict(signal.confidence_metadata), sort_keys=True),
                "reason_codes": ",".join(signal.reason_codes), "candidate_rank": None,
                "candidate_score": None, "source_dataset": "yahoo_nse_equity_intraday_1m",
                "source": YAHOO_SOURCE, "timeframe": signal.timeframe,
                "market_timestamp": market_text, "received_timestamp": received_text,
                "signal_bar_timestamp": market_text,
                "decision_timestamp": decision_text,
                "bar_status": row.get("bar_status"),
            }
            prior_rank = next((item for item in reversed(self.rankings)
                               if item["symbol"] == signal.symbol
                               and item["decision_timestamp"] <= signal.timestamp.isoformat()), None)
            if prior_rank:
                record["candidate_rank"] = prior_rank["rank"]
                record["candidate_score"] = prior_rank["score"]
            rejection_reason = None
            order = None
            if duplicate_signal:
                rejection_reason = "DUPLICATE_SIGNAL_BAR"
            elif not timestamp_proof:
                rejection_reason = "TIMESTAMP_PROVENANCE_INSUFFICIENT"
            elif risk_per_share <= 0 or reward_per_share <= 0 or risk_reward is None:
                rejection_reason = "INVALID_RISK_REWARD"
            elif self.require_ranked_candidate and prior_rank is None:
                rejection_reason = "NOT_IN_TOP_RANKED_CANDIDATES"
            else:
                execution_started = time_module.perf_counter()
                order = self.engine.submit_signal(
                    SignalEnvelope(decision_signal, source=YAHOO_SOURCE,
                                   source_metadata={"phase": "4G-YF", "dataset_classification": "THIRD_PARTY_RESEARCH_SOURCE",
                                                    "authorized_nse_feed": False, "observed_at": decision_text,
                                                    "market_timestamp": market_text,
                                                    "signal_bar_timestamp": market_text,
                                                    "received_timestamp": received_text,
                                                    "decision_timestamp": decision_text,
                                                    "bar_status": row.get("bar_status"),
                                                    "source_timestamp": row.get("source_timestamp")}),
                    replay_time=local_now,
                    sector=self._sector(signal.symbol),
                )
                self.performance_seconds["execution_seconds"] += time_module.perf_counter() - execution_started
                if str(order.status).upper() != "PENDING":
                    rejection_reason = str(getattr(order, "rejection_reason", None) or order.status)
            signal_state = "BUY" if order is not None and str(order.status).upper() == "PENDING" else "REJECTED"
            audit = {
                "audit_id": f"signal|{signal.signal_id}",
                "decision_timestamp": decision_text, "symbol": signal.symbol,
                "strategy": signal.strategy, "signal": signal_state,
                "market_timestamp": market_text or None, "received_timestamp": received_text or None,
                "signal_bar_timestamp": market_text or None,
                "bar_status": row.get("bar_status"),
                "timestamp_valid": timestamp_proof, "timestamp_proof": timestamp_proof,
                "strategy_valid": True,
                "candidate_ranking_valid": (not self.require_ranked_candidate or prior_rank is not None),
                "risk_valid": bool(order is not None and str(order.status).upper() == "PENDING"),
                "entry": signal.reference_price, "stop_loss": signal.stop_reference,
                "target": signal.target_reference, "risk_per_share": risk_per_share,
                "reward_per_share": reward_per_share, "risk_reward": risk_reward,
                "quantity": getattr(order, "quantity", None),
                "risk_amount": getattr(order, "planned_risk", None),
                "candidate_rank": record["candidate_rank"], "candidate_score": record["candidate_score"],
                "paper_order_id": getattr(order, "order_id", None),
                "paper_order_status": getattr(order, "status", None),
                "rejection_reason": rejection_reason,
                "data_quality": "PASS", "reason_codes": list(signal.reason_codes),
                "evidence": dict(signal.confidence_metadata),
                "paper_only": True, "broker_calls": 0, "live_orders": 0,
            }
            self._record_audit(audit)
            record.update({"signal_state": signal_state, "rejection_reason": rejection_reason,
                           "paper_order_id": getattr(order, "order_id", None),
                           "paper_order_status": getattr(order, "status", None),
                           "decision_timestamp": decision_text})
            if signal_state == "BUY":
                self.signals.append(record)
                new_signals += 1
            else:
                rejected_signals += 1

        if combined:
            self.last_processed_timestamp = max(bar.end for bar, _ in combined).isoformat()
        self._persist_views()
        self._persist_state()
        elapsed_refresh = time_module.perf_counter() - total_started
        return {
            **base_result,
            "status": "PHASE4G_YF_COLLECTING",
            # Legacy output field now has an explicit 1m market-observation meaning.
            "new_bars_processed": plan.accounting["current_session_new_bars"],
            "new_signals": new_signals, "rejected_signals": rejected_signals,
            "signals": len(self.signals), "orders": len(self.engine.all_orders),
            "last_available_timestamp": self.last_available_timestamp,
            "last_processed_timestamp": self.last_processed_timestamp,
            "forming_bars_ignored": report.session_coverage.get("forming_bars_ignored", 0),
            "late_bars_not_used_for_execution": self.late_bars_not_used_for_execution,
            "delayed_signals_suppressed": self.delayed_signals_suppressed,
            "ledger_path": str(self.ledger_path), "broker_calls": 0, "live_orders": 0,
            "missing_bar_fraction": gap_report["missing_fraction"],
            "performance_seconds": {**self.performance_seconds, "total_refresh_seconds": elapsed_refresh},
        }

    def _record_due_rankings(self, all_1m: Sequence[Mapping[str, Any]], now: datetime,
                             *, grace_seconds: int = 60,
                             data_quality_missing_symbols: Sequence[str] = ()) -> None:
        existing = {item["decision_timestamp"] for item in self.rankings}
        skipped = {item["decision_timestamp"] for item in self.skipped_decisions}
        completed_snapshots = {
            item.get("decision_timestamp") for item in self.decision_snapshots
            if item.get("decision_kind") == "SCHEDULED_RANKING"
        }
        for decision_time in DECISION_TIMES:
            stamp = datetime.combine(self.session_date, decision_time, tzinfo=IST)
            decision_text = stamp.isoformat()
            if decision_text in existing or decision_text in skipped or decision_text in completed_snapshots:
                continue
            delay = (now.astimezone(IST) - stamp).total_seconds()
            if delay < 0:
                continue
            snapshot = build_decision_snapshot(
                all_1m, decision=stamp, expected_symbols=self.symbols,
                data_quality_missing_symbols=data_quality_missing_symbols,
                decision_kind="SCHEDULED_RANKING",
                settling_seconds=self.bar_settling_seconds,
            )
            if delay > grace_seconds:
                self.skipped_decisions.append({"decision_timestamp": decision_text,
                                               "reason": "decision window passed before data was available"})
                snapshot["decision_status"] = "SKIPPED_GRACE_WINDOW_PASSED"
                self._store_decision_snapshot(snapshot)
                continue
            decision_rows = [row for row in all_1m
                             if _bar_decision_exclusion(row, stamp,
                                                        settling_seconds=self.bar_settling_seconds) is None]
            ranking_started = time_module.perf_counter()
            ranking = rank_candidates(decision_rows, as_of=stamp, top_n=10,
                                      settling_seconds=self.bar_settling_seconds)
            self.performance_seconds["ranking_seconds"] += time_module.perf_counter() - ranking_started
            snapshot["decision_status"] = "EVALUATED" if ranking else "NO_CANDIDATES"
            snapshot["candidate_count"] = len(ranking)
            self._store_decision_snapshot(snapshot)
            ranked_symbols = {str(item["symbol"]).upper() for item in ranking}
            available_symbols = set(snapshot.get("symbols_available_list", []))
            latest_by_symbol: dict[str, Mapping[str, Any]] = {}
            eligible_counts: dict[str, int] = defaultdict(int)
            for row in decision_rows:
                symbol = str(row.get("symbol", "")).upper()
                eligible_counts[symbol] += 1
                current = latest_by_symbol.get(symbol)
                if current is None or str(row.get("timestamp", "")) > str(current.get("timestamp", "")):
                    latest_by_symbol[symbol] = row
            for symbol in sorted(set(self.symbols)):
                row = latest_by_symbol.get(symbol)
                if symbol in ranked_symbols:
                    state, reason = "CANDIDATE", None
                elif symbol in available_symbols:
                    state = "WATCH"
                    reason = ("INSUFFICIENT_RANKING_HISTORY"
                              if eligible_counts.get(symbol, 0) < MINIMUM_RANKING_BARS
                              else "NOT_IN_TOP_RANKED_CANDIDATES")
                elif symbol in snapshot.get("symbols_missing_due_to_latency_list", []):
                    state, reason = "REJECTED", "LATE_OR_UNAVAILABLE_RECEIPT_TIME"
                else:
                    state, reason = "REJECTED", "NO_ELIGIBLE_CLOSED_BAR_OR_DATA_QUALITY"
                self._record_audit({
                    "audit_id": f"ranking|{decision_text}|{symbol}",
                    "decision_timestamp": decision_text,
                    "symbol": symbol,
                    "strategy": None,
                    "signal": state,
                    "market_timestamp": row.get("market_timestamp", row.get("timestamp")) if row else None,
                    "received_timestamp": row.get("received_timestamp") if row else None,
                    "timestamp_proof": bool(row and _bar_decision_exclusion(
                        row, stamp, settling_seconds=self.bar_settling_seconds) is None),
                    "entry": None, "stop_loss": None, "target": None,
                    "risk_per_share": None, "risk_reward": None, "quantity": None, "risk_amount": None,
                    "candidate_rank": next((item["rank"] for item in ranking if item["symbol"] == symbol), None),
                    "candidate_score": next((item["score"] for item in ranking if item["symbol"] == symbol), None),
                    "rejection_reason": reason,
                    "data_quality": "PASS" if row else "NOT_AVAILABLE",
                    "paper_only": True, "broker_calls": 0, "live_orders": 0,
                })
            for item in ranking:
                item["generated_at"] = now.astimezone(IST).isoformat()
                self.rankings.append(item)

    def _store_decision_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        item = dict(snapshot)
        key = (item.get("decision_timestamp"), item.get("decision_kind"))
        if any((existing.get("decision_timestamp"), existing.get("decision_kind")) == key
               for existing in self.decision_snapshots):
            return
        self.decision_snapshots.append(item)
        history = self.output / "reports/decision_snapshots.jsonl"
        history.parent.mkdir(parents=True, exist_ok=True)
        with history.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(item, sort_keys=True, default=str) + "\n")
            stream.flush()

    def _sector(self, symbol: str) -> str | None:
        path = self.root / "data/paper_trading/phase3d_personal_universe_v2.csv"
        try:
            with path.open("r", newline="", encoding="utf-8-sig") as stream:
                row = next((item for item in csv.DictReader(stream) if item.get("Symbol", "").upper() == symbol), None)
            return str(row.get("Industry") or "UNKNOWN") if row else None
        except OSError:
            return None

    def finish_session(self, *, now: datetime) -> dict[str, Any]:
        """Close paper positions at/after session end; no data fetch or new signals."""
        if now.tzinfo is None or now.astimezone(IST).date() != self.session_date:
            raise Phase4GYFError("RECONCILIATION_TIME_INVALID")
        local = now.astimezone(IST)
        if local.time() < SESSION_CLOSE:
            raise Phase4GYFError("SESSION_NOT_CLOSED")
        if self.engine.events:
            self.engine.close_session(self.session_date, local)
        self._persist_views()
        self._persist_state()
        return {"status": "PHASE4G_YF_FORWARD_SESSION_CLOSED", "open_positions": len(self.engine.open_positions),
                "closed_trades": len(self.engine.all_trades), "ledger_path": str(self.ledger_path),
                "post_close_signal_generation": False, "broker_calls": 0, "live_orders": 0}

    def checkpoint_shutdown(self) -> dict[str, Any]:
        """Persist the current paper state without processing bars or creating orders."""
        self._persist_views()
        self._persist_state()
        return {
            "ledger_path": str(self.ledger_path),
            "ledger_events": len(self.engine.events),
            "orders": len(self.engine.all_orders),
            "open_positions": len(self.engine.open_positions),
            "signals": len(self.signals),
            "audit_records": len(self.signal_audit),
            "no_order_generation": True,
        }

    def execution_summary(self) -> dict[str, Any]:
        orders = self.engine.all_orders
        trades = self.engine.all_trades
        realized_gross = sum(item.gross_pnl for item in trades)
        transaction_costs = sum(item.transaction_costs for item in trades)
        slippage = sum(item.slippage for item in trades)
        costs = transaction_costs + slippage
        components = {"brokerage": 0.0, "stt": 0.0, "exchange": 0.0, "gst": 0.0,
                      "sebi": 0.0, "stamp_duty": 0.0, "dp": 0.0}
        for trade in trades:
            entry = self.engine.cost_model.order_cost(trade.entry_price, trade.quantity, "buy", True)
            exit_ = self.engine.cost_model.order_cost(trade.exit_price, trade.quantity, "sell", True, is_exit=True)
            for field, target in (("brokerage", "brokerage"), ("stt", "stt"), ("exchange", "exchange"),
                                  ("gst", "gst"), ("sebi", "sebi"), ("stamp", "stamp_duty"), ("dp", "dp")):
                components[target] += getattr(entry, field) + getattr(exit_, field)
        return {
            "signals": len(self.signals), "orders": len(orders),
            "fills": sum(item.status == "FILLED" for item in orders),
            "open_positions": len(self.engine.open_positions), "closed_trades": len(trades),
            "gross_pnl_closed_trades": realized_gross, "transaction_costs_closed_trades": transaction_costs,
            "slippage_closed_trades": slippage, "net_pnl_closed_trades": sum(item.net_pnl for item in trades),
            "cost_components_closed_trades": components,
            "cost_total_closed_trades": costs,
            "max_intraday_drawdown": max((point.drawdown for point in self.engine.equity_curve), default=0.0),
            "risk_events": len(self.engine.risk_events),
            "blocked_orders": sum(item.status == "REJECTED" for item in orders),
            "paper_only": True, "broker_calls": 0, "live_orders": 0,
        }


def register_phase4g_dataset(*, project_root: str | Path, output_dir: str | Path,
                             instruments: Mapping[str, YahooInstrument],
                             volume_evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Register and normalize the closed session using existing Phase 4F code.

    An isolated Phase 4F workspace under this experiment preserves the existing
    project registry as a protected Phase 4F artifact. One day remains
    ``NOT_RESEARCH_READY`` by Phase 4F design; registration does not promote it.
    """
    from nse_research.data.intraday_ingestion import (
        Phase4FError, normalize_dataset, register_dataset, validate_dataset,
    )

    root = Path(project_root).resolve()
    output = Path(output_dir).resolve()
    source = output / "canonical/forward_bars_1m.csv"
    if not source.is_file() or source.stat().st_size == 0:
        raise Phase4GYFError("PHASE4G_YF_BLOCKED_DATA_QUALITY", "no first-observed 1m session bars to register")
    rows: list[dict[str, Any]] = []
    with source.open("r", newline="", encoding="utf-8") as stream:
        rows.extend(dict(item) for item in csv.DictReader(stream))
    observed_symbols = sorted({str(row["symbol"]).upper() for row in rows})
    if not rows or any(str(row.get("volume_kind", "UNKNOWN")).upper() != "BAR" for row in rows):
        raise Phase4GYFError("PHASE4G_YF_BLOCKED_DATA_QUALITY", "registered session is empty or interval volume is unverified")
    phase4f_input = output / "validated/phase4f_input.csv"
    columns = ["timestamp", "symbol", "exchange", "series", "timeframe", "open", "high", "low", "close",
               "volume", "volume_kind", "status", "source", "yahoo_symbol", "isin", "source_timestamp", "source_timezone"]
    _atomic_csv(phase4f_input, rows, columns)
    symbol_mapping = {
        instrument.yahoo_symbol: {"symbol": instrument.symbol, "exchange": "NSE", "series": "EQ", "isin": instrument.isin}
        for instrument in instruments.values()
    }
    metadata = {
        "dataset_name": "Phase 4G-YF Sep 28 forward observed session",
        "provider": "yahoo_finance_yfinance",
        "publisher": "Yahoo Finance",
        "source_url": "https://finance.yahoo.com/",
        "revision": "phase4g-yf-2026-09-28-forward-v1",
        "timeframe": "1m", "data_type": "bars",
        "original_timezone": str(rows[0].get("source_timezone") or "UNKNOWN"),
        "timestamp_timezone_documentation": "Timezone-aware timestamp returned by yfinance; source timestamp is retained in source_timestamp column.",
        "timestamp_convention": "BAR_START",
        "timestamp_convention_documentation": "yfinance interval index is preserved as the bar start; rows are eligible only after timestamp plus one minute is no later than the observation clock.",
        "bars_finalized": True,
        "bars_finalized_documentation": "Only closed one-minute bars are included; forming bars were excluded by the live collector.",
        "exchange": "NSE", "series": "EQ",
        "symbol_mapping": symbol_mapping,
        "symbol_mapping_documentation": "Yahoo .NS access symbols map through the repository's frozen phase3d_personal_universe_v2.csv and hash-matched NSE security-history-derived nse_equity_universe.json; the .NS suffix is not treated as identity evidence.",
        "expected_sessions": [SESSION_DATE.isoformat()],
        "expected_symbols": observed_symbols,
        "volume_semantics": "UNKNOWN",
        "volume_semantics_observation": dict(volume_evidence),
        "adjustment_status": "UNKNOWN",
        "adjustment_status_documentation": "The client used auto_adjust=False and did not adjust prices. Upstream Yahoo price adjustment treatment is not independently established.",
        "license_status": "THIRD_PARTY_TERMS_NOT_VERIFIED",
        "license": "YAHOO_FINANCE_THIRD_PARTY_TERMS_NOT_VERIFIED",
        "data_rights": "THIRD_PARTY_PERSONAL_RESEARCH",
        "research_allowed": False,
        "commercial_allowed": False,
        "redistribution_terms": "NOT_VERIFIED",
        "universe_bias": "Existing frozen 486-symbol EQ universe; Yahoo coverage failures are separately reported.",
        "acquisition_timestamp": datetime.now(IST).isoformat(),
        "downloaded_at": datetime.now(IST).isoformat(),
        "license_documentation": "Provider terms were not independently verified; this is not an authorized NSE dataset.",
        "column_map": {field: field for field in ("timestamp", "symbol", "exchange", "series", "timeframe", "open", "high", "low", "close", "volume", "volume_kind", "status", "source")},
    }
    sidecar = phase4f_input.with_suffix(".metadata.json")
    _atomic_json(sidecar, metadata)
    workspace = output / "phase4f_workspace"
    try:
        registration = register_dataset(phase4f_input, metadata_path=sidecar, project_root=workspace)
        dataset_id = registration["dataset"]["dataset_id"]
        validation = validate_dataset(dataset_id, project_root=workspace)
        if validation.get("result", {}).get("phase4e_validation_status") != "PASS":
            raise Phase4GYFError("PHASE4E_VALIDATION_FAILED", json.dumps(validation.get("result", {}), default=str))
        normalized = normalize_dataset(dataset_id, project_root=workspace)
        canonical_source = workspace / normalized["output"]
        canonical_target = output / "canonical/phase4f_canonical_intraday_bars.csv"
        shutil.copyfile(canonical_source, canonical_target)
        readiness = validation.get("result", {}).get("research_readiness", "NOT_RESEARCH_READY")
        return {
            "status": "REGISTERED_AND_PHASE4E_VALIDATED",
            "dataset_id": dataset_id,
            "registry_path": str(workspace / "data/intraday/datasets.json"),
            "phase4e_validation_status": "PASS",
            "research_readiness": readiness,
            "research_ready": False,
            "normalized_path": str(canonical_target),
            "rows": len(rows), "symbols": observed_symbols,
            "notes": ["Isolated Phase 4F registry protects the existing project registry.",
                      "One-session Yahoo data is not promoted to historical research readiness.",
                      "Yahoo source terms and upstream adjustment status remain unverified."],
        }
    except Phase4FError as exc:
        raise Phase4GYFError("PHASE4F_REGISTRATION_FAILED", str(exc)) from exc


def build_top_candidate_summary(rankings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_symbol: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rankings:
        by_symbol[str(row["symbol"]).upper()].append(row)
    result = []
    for symbol, rows in by_symbol.items():
        top_rank = min(int(float(row["rank"])) for row in rows)
        first = min(rows, key=lambda row: str(row["decision_timestamp"]))
        result.append({"symbol": symbol, "best_rank": top_rank,
                       "maximum_rank": top_rank,
                       "worst_rank": max(int(float(row["rank"])) for row in rows),
                       "time_first_ranked": first["decision_timestamp"],
                       "maximum_score": max(float(row["score"]) for row in rows),
                       "ranking_appearances": len(rows)})
    return sorted(result, key=lambda row: (row["best_rank"], -row["maximum_score"], row["symbol"]))


def build_strategy_signal_summary(signals: Sequence[Mapping[str, Any]],
                                  orders: Sequence[Mapping[str, Any]],
                                  trades: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    order_by_signal = {str(item.get("signal_id")): item for item in orders}
    trade_by_signal = {str(item.get("signal_id", "")): item for item in trades}
    output = []
    for signal in signals:
        order = order_by_signal.get(str(signal.get("signal_id")), {})
        trade = trade_by_signal.get(str(signal.get("signal_id")), {})
        output.append({
            "symbol": signal.get("symbol"), "strategy": signal.get("strategy"),
            "signal_time": signal.get("timestamp"), "direction": signal.get("direction"),
            "entry": signal.get("entry_reference"), "stop": signal.get("stop_reference"),
            "target": signal.get("target_reference"), "candidate_rank": signal.get("candidate_rank"),
            "candidate_score": signal.get("candidate_score"),
            "paper_order_status": order.get("status"),
            "paper_rejection_reason": order.get("rejection_reason"),
            "paper_result_net_pnl": trade.get("net_pnl"),
        })
    return output


def check_missing_bar_fraction(rows: Sequence[Mapping[str, Any]], *, session_date: date,
                               maximum_fraction: float = MAX_MISSING_INTERVAL_FRACTION) -> dict[str, Any]:
    """Report expected minute slots through the latest observed close, no filling."""
    by_symbol: dict[str, set[datetime]] = defaultdict(set)
    for row in rows:
        stamp = datetime.fromisoformat(str(row["timestamp"])).astimezone(IST)
        by_symbol[str(row["symbol"]).upper()].add(stamp)
    if not by_symbol:
        return {"expected_slots": 0, "observed_slots": 0, "missing_slots": 0,
                "missing_fraction": 1.0, "within_limit": False}
    latest_minute = max(max(stamps) for stamps in by_symbol.values())
    start = datetime.combine(session_date, SESSION_OPEN, tzinfo=IST)
    expected = max(0, int((latest_minute + timedelta(minutes=1) - start).total_seconds() // 60))
    expected_slots = expected * len(by_symbol)
    observed_slots = sum(len(stamps) for stamps in by_symbol.values())
    missing = max(0, expected_slots - observed_slots)
    fraction = missing / expected_slots if expected_slots else 1.0
    return {"expected_slots": expected_slots, "observed_slots": observed_slots,
            "missing_slots": missing, "missing_fraction": fraction,
            "maximum_fraction": maximum_fraction, "within_limit": fraction <= maximum_fraction,
            "gap_policy": "report only; never fill"}
