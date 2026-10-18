#!/usr/bin/env python3
"""Offline/preflight/live-read-only Upstox V3 validation; never trades."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from http.client import HTTPConnection
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nse_research.config import AppConfig  # noqa: E402
from nse_research.forward.calendar import NseTradingCalendar  # noqa: E402
from nse_research.realtime.engine import (  # noqa: E402
    IntradayCandleBuilder,
    RealtimeFeedMonitor,
    TickRecorder,
    validate_quote,
)
from nse_research.realtime.upstox import (  # noqa: E402
    DEFAULT_REALTIME_UNIVERSE_SIZE,
    MAPPING_COLUMNS,
    PROVIDER_NAME,
    SMOKE_SYMBOLS,
    SUPPORTED_REALTIME_UNIVERSE_SIZES,
    UpstoxConfigurationError,
    UpstoxV3Provider,
    env_true,
    load_verified_mapping,
    mapping_artifact_path,
    parse_epoch_ms,
    read_realtime_universe_csv,
    realtime_universe_artifact_path,
    required_dependencies,
)

RESULTS_PATH = ROOT / "results/phase4a8a_upstox_live_validation.json"
COVERAGE_PATH = ROOT / "results/phase4a8a_live_coverage_100.csv"
FIVE_SYMBOL_PATH = ROOT / "results/phase4a8a_five_symbol_validation.csv"
IST = ZoneInfo("Asia/Kolkata")
COVERAGE_STALE_AFTER_SECONDS = 60.0


def _rooted(path: str | Path) -> Path:
    value = Path(path)
    return value if value.is_absolute() else ROOT / value


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _next_weekday(calendar: NseTradingCalendar, today: date) -> date:
    target = today
    for _ in range(15):
        target += timedelta(days=1)
        if target.weekday() < 5 and target not in calendar.holidays and target not in calendar.special_sessions:
            return target
    raise RuntimeError("no next weekday session candidate in 15 days")


def _current_or_next_session(calendar: NseTradingCalendar, today: date) -> date:
    if today.weekday() < 5 and today not in calendar.holidays and today not in calendar.special_sessions:
        return today
    return _next_weekday(calendar, today)


def _regular_nse_session_open(config: AppConfig, *, now: datetime | None = None) -> bool:
    current = now or datetime.now(ZoneInfo(config.project.timezone))
    local = current.astimezone(ZoneInfo(config.project.timezone))
    calendar = NseTradingCalendar(_rooted(config.forward_paper.trading_calendar_path))
    if local.date().weekday() >= 5 or local.date() in calendar.holidays or local.date() in calendar.special_sessions:
        return False
    return (time.fromisoformat(config.realtime.market_open)
            <= local.timetz().replace(tzinfo=None)
            < time.fromisoformat(config.realtime.market_close))


def _dashboard_http_ready(*, timeout_seconds: float = 0.5) -> bool:
    """Check the existing local dashboard's read-only health route only."""
    connection = HTTPConnection("127.0.0.1", 8765, timeout=timeout_seconds)
    try:
        connection.request("GET", "/health")
        response = connection.getresponse()
        if response.status != 200:
            return False
        payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return False
    finally:
        connection.close()
    return isinstance(payload, dict) and payload.get("dashboard_read_only") is True


def _mapping_summary(path: Path, *, expected_count: int) -> dict:
    if not path.is_file():
        return {"exists": False, "rows": 0, "verified": 0, "statuses": {}, "schema_valid": False, "full_universe_valid": False}
    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            schema_valid = tuple(reader.fieldnames or ()) == MAPPING_COLUMNS
            rows = list(reader)
    except (OSError, csv.Error):
        return {"exists": True, "rows": 0, "verified": 0, "statuses": {}, "schema_valid": False, "full_universe_valid": False}
    statuses: dict[str, int] = {}
    for row in rows:
        status = str(row.get("mapping_status") or "INVALID").upper()
        statuses[status] = statuses.get(status, 0) + 1
    verified = statuses.get("VERIFIED", 0)
    full = schema_valid and len(rows) == expected_count and verified == expected_count
    return {"exists": True, "rows": len(rows), "verified": verified,
            "statuses": statuses, "schema_valid": schema_valid, "full_universe_valid": full}


def _configured_universe_size() -> tuple[int, bool]:
    raw = os.environ.get("REALTIME_UNIVERSE_SIZE", str(DEFAULT_REALTIME_UNIVERSE_SIZE)).strip()
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_REALTIME_UNIVERSE_SIZE, False
    if value not in SUPPORTED_REALTIME_UNIVERSE_SIZES:
        return DEFAULT_REALTIME_UNIVERSE_SIZE, False
    return value, True


def _preflight(config_path: Path, *, now: datetime | None = None) -> dict:
    current = now or datetime.now(IST)
    config = AppConfig.load(config_path)
    calendar_path = _rooted(config.forward_paper.trading_calendar_path)
    calendar = NseTradingCalendar(calendar_path)
    target_session = _current_or_next_session(calendar, current.astimezone(IST).date())
    universe_size, universe_size_valid = _configured_universe_size()
    selection_path = realtime_universe_artifact_path(universe_size, project_root=ROOT)
    mapping_path = mapping_artifact_path(universe_size, project_root=ROOT)
    mapping = _mapping_summary(mapping_path, expected_count=universe_size)
    selection_valid = False
    required_five_present = False
    try:
        selected = read_realtime_universe_csv(selection_path, expected_count=universe_size)
        selected_identities = {row["symbol"].strip().upper(): row["isin"].strip().upper() for row in selected}
        required_five_present = set(SMOKE_SYMBOLS).issubset(selected_identities)
        selection_valid = universe_size_valid and required_five_present
        if mapping["full_universe_valid"]:
            loaded = load_verified_mapping(
                mapping_path, expected_count=universe_size,
                allow_missing_trading_symbol=True,
            )
            mapping["identity_valid"] = (
                set(loaded) == set(selected_identities)
                and all(loaded[symbol]["project_isin"].upper() == isin
                        for symbol, isin in selected_identities.items())
            )
            if not mapping["identity_valid"]:
                mapping["full_universe_valid"] = False
        else:
            mapping["identity_valid"] = False
    except (OSError, ValueError, KeyError):
        selection_valid = False
        required_five_present = False
        mapping["identity_valid"] = False
        mapping["full_universe_valid"] = False
    dependencies = required_dependencies()
    token_present = bool(os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip())
    realtime_enabled = env_true(os.environ.get("REALTIME_ENABLED"))
    provider_name = os.environ.get("REALTIME_PROVIDER", "").strip().lower()
    provider_name_configured = provider_name == PROVIDER_NAME
    mode_is_ltpc = os.environ.get("UPSTOX_MODE", "ltpc").strip().lower() == "ltpc"
    try:
        reconnect_attempts = int(os.environ.get("UPSTOX_MAX_RECONNECT_ATTEMPTS", "5"))
        reconnect_configuration_valid = 1 <= reconnect_attempts <= 5
    except ValueError:
        reconnect_configuration_valid = False
    live_enabled = (env_true(os.environ.get("LIVE_TRADING"))
                    or env_true(os.environ.get("LIVE_TRADING_ENABLED"))
                    or config.live_trading.enabled)
    live_trading_explicitly_false = os.environ.get("LIVE_TRADING", "").strip().lower() == "false"
    broker_provider = os.environ.get("BROKER_PROVIDER", "").strip()
    baseline_path = _rooted(config.forward_paper.baseline_manifest_path)
    dashboard_python = (ROOT / "src/nse_research/dashboard.py").is_file()
    dashboard_js = (ROOT / "forward/dashboard/app.js").is_file()
    dashboard_http_ready = _dashboard_http_ready()
    health_snapshot_path = _rooted(config.realtime.health_snapshot_path)
    provider_health_state = "NOT_STARTED"
    provider_health_safe = True
    provider_feed_idle = True
    if health_snapshot_path.is_file():
        try:
            prior_health = json.loads(health_snapshot_path.read_text(encoding="utf-8"))
            safety_keys = ("live_trading_enabled", "broker_execution_enabled", "signals_generated", "ledger_mutated")
            provider_health_safe = (
                isinstance(prior_health, dict)
                and prior_health.get("provider") == PROVIDER_NAME
                and all(prior_health.get(key) is False for key in safety_keys)
                and isinstance(prior_health.get("feed_status"), str)
            )
            provider_health_state = str(prior_health.get("feed_status") or "INVALID") if isinstance(prior_health, dict) else "INVALID"
            provider_feed_idle = provider_health_state not in {"CONNECTED", "CONNECTING", "RECONNECTING"}
        except (OSError, UnicodeError, json.JSONDecodeError):
            provider_health_safe = False
            provider_feed_idle = False
            provider_health_state = "INVALID"
    prior_run = None
    if RESULTS_PATH.is_file():
        try:
            prior_run = json.loads(RESULTS_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            prior_run = {"status": "UNREADABLE"}
    duplicate_run = bool(
        prior_run is not None
        and (not isinstance(prior_run, dict)
             or (prior_run.get("session_date") == target_session.isoformat()
                 and prior_run.get("status") in {"IN_PROGRESS", "COMPLETED"}))
    )
    checks = {
        "token_present": token_present,
        "realtime_enabled_for_live": realtime_enabled,
        "provider_is_upstox": provider_name_configured,
        "mode_is_ltpc": mode_is_ltpc,
        "reconnect_configuration_valid": reconnect_configuration_valid,
        "live_trading_disabled": not live_enabled and live_trading_explicitly_false,
        "broker_provider_unset": not broker_provider,
        "universe_size_supported": universe_size_valid,
        "universe_selection_valid": selection_valid,
        "five_symbol_universe_gate": required_five_present,
        f"mapping_{universe_size}_verified": mapping["full_universe_valid"] and mapping.get("identity_valid", False),
        "dependencies_available": all(dependencies.values()),
        "calendar_loaded": calendar_path.is_file(),
        "target_weekday_not_holiday": target_session.weekday() < 5 and target_session not in calendar.holidays,
        "dashboard_available": dashboard_python and dashboard_js,
        "dashboard_http_ready": dashboard_http_ready,
        "provider_health_snapshot_safe": provider_health_safe,
        "no_provider_feed_already_running": provider_feed_idle,
        "baseline_manifest_exists": baseline_path.is_file(),
        "no_duplicate_forward_run": not duplicate_run,
    }
    blockers = [name for name, passed in checks.items() if not passed]
    return {
        "status": "PHASE4A8A_PREFLIGHT_READY" if not blockers else "PHASE4A8A_PREFLIGHT_BLOCKED",
        "checked_at": current.astimezone(timezone.utc).isoformat(),
        "target_session_date": target_session.isoformat(),
        "checks": checks,
        "blockers": blockers,
        "mapping": mapping,
        "universe": {"size": universe_size, "selection_path": str(selection_path.relative_to(ROOT)),
                     "mapping_path": str(mapping_path.relative_to(ROOT))},
        "dependencies": dependencies,
        "provider_health_state": provider_health_state,
        "readiness": {
            "UPSTOX_TOKEN_PRESENT": token_present,
            "MAPPING_READY": bool(mapping["full_universe_valid"] and mapping.get("identity_valid", False)),
            "UNIVERSE_SIZE": universe_size,
            "FIVE_SYMBOLS_PRESENT": required_five_present,
            "REALTIME_ENABLED": realtime_enabled,
            "LIVE_TRADING": "false" if live_trading_explicitly_false and not live_enabled else "BLOCKED",
            "DASHBOARD_READY": dashboard_python and dashboard_js and dashboard_http_ready,
        },
        "configuration": {
            "provider": provider_name,
            "realtime_enabled": realtime_enabled,
            "mode": os.environ.get("UPSTOX_MODE", "ltpc").strip().upper(),
            "universe_size": universe_size,
            "live_trading_enabled": live_enabled,
            "live_trading_explicitly_false": live_trading_explicitly_false,
            "broker_provider_configured": bool(broker_provider),
            "recording_enabled": bool(config.realtime.recording),
            "dashboard_port": 8765,
        },
        "network_market_data_request_made": False,
        "credential_value_displayed_or_persisted": False,
        "orders_created": False,
        "signals_generated": False,
        "ledger_mutated": False,
    }


def _write_coverage(
    provider: UpstoxV3Provider,
    mapping: dict[str, dict[str, str]],
    path: Path,
    *,
    active_period: bool,
    connection_status: str | None = None,
    observed_at: datetime | None = None,
) -> dict:
    snapshot = provider.health_snapshot()
    stats = snapshot["symbol_stats"]
    feed_connected = (connection_status or snapshot["connection_status"]) == "CONNECTED"
    observed_utc = (observed_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    fieldnames = [
        "symbol", "instrument_key", "subscription_status", "first_tick_timestamp",
        "last_tick_timestamp", "tick_count", "invalid_count", "stale_count",
        "duplicate_count", "latency_min_ms", "latency_avg_ms", "latency_p95_ms",
        "latency_max_ms", "status",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    output_rows = []
    for symbol, security in mapping.items():
        detail = stats[symbol]
        if symbol not in snapshot["subscribed_symbol_list"] or not feed_connected:
            status = "DISCONNECTED"
        elif detail["tick_count"]:
            try:
                last_received = datetime.fromisoformat(detail["last_received_timestamp"])
                age_seconds = (observed_utc - last_received.astimezone(timezone.utc)).total_seconds()
            except (KeyError, TypeError, ValueError):
                age_seconds = COVERAGE_STALE_AFTER_SECONDS + 1
            status = "STALE" if age_seconds > COVERAGE_STALE_AFTER_SECONDS else "RECEIVING"
        elif detail["invalid_count"]:
            status = "INVALID"
        elif detail["stale_count"]:
            status = "STALE"
        else:
            status = "SUBSCRIBED_NO_TICK_YET"
        output_rows.append({
            "symbol": symbol,
            "instrument_key": security["upstox_instrument_key"],
            "subscription_status": detail["subscription_status"],
            "first_tick_timestamp": detail["first_tick_timestamp"],
            "last_tick_timestamp": detail["last_tick_timestamp"],
            "tick_count": detail["tick_count"],
            "invalid_count": detail["invalid_count"],
            "stale_count": detail["stale_count"],
            "duplicate_count": detail["duplicate_count"],
            "latency_min_ms": detail["latency_min_ms"],
            "latency_avg_ms": detail["latency_avg_ms"],
            "latency_p95_ms": detail["latency_p95_ms"],
            "latency_max_ms": detail["latency_max_ms"],
            "status": status,
        })
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    return snapshot


def _write_five_symbol_validation(provider: UpstoxV3Provider, mapping: dict[str, dict[str, str]]) -> int:
    snapshot = provider.health_snapshot()
    stats = snapshot["symbol_stats"]
    fields = (
        "symbol", "instrument_key", "first_tick", "last_tick", "tick_count", "ltp",
        "ltt", "source_timestamp", "received_timestamp", "latency_ms",
        "normalized_quote_status", "status",
    )
    temporary = FIVE_SYMBOL_PATH.with_name(f".{FIVE_SYMBOL_PATH.name}.{os.getpid()}.tmp")
    FIVE_SYMBOL_PATH.parent.mkdir(parents=True, exist_ok=True)
    passed = 0
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for symbol in SMOKE_SYMBOLS:
            detail = snapshot["symbol_stats"][symbol]
            success = bool(
                detail["snapshot_received"] and detail["live_update_received"]
                and detail["tick_count"] >= 2 and detail["normalized_quote_status"] == "VALID"
            )
            passed += int(success)
            writer.writerow({
                "symbol": symbol,
                "instrument_key": mapping[symbol]["upstox_instrument_key"],
                "first_tick": detail["first_tick_timestamp"],
                "last_tick": detail["last_tick_timestamp"],
                "tick_count": detail["tick_count"],
                "ltp": detail["last_price"],
                "ltt": detail["last_ltt_raw"],
                "source_timestamp": detail["last_tick_timestamp"],
                "received_timestamp": detail["last_received_timestamp"],
                "latency_ms": detail["latency_avg_ms"],
                "normalized_quote_status": detail["normalized_quote_status"],
                "status": "PASS" if success else "FAIL",
            })
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, FIVE_SYMBOL_PATH)
    return passed


def _five_symbol_timestamps_empirically_plausible(
    provider: UpstoxV3Provider, config: AppConfig, *, session_date: date,
) -> bool:
    """Require five observed production timestamps to pass the configured epoch-ms contract."""
    stats = provider.health_snapshot()["symbol_stats"]
    zone = ZoneInfo(config.project.timezone)
    market_open = time.fromisoformat(config.realtime.market_open)
    market_close = time.fromisoformat(config.realtime.market_close)
    for symbol in SMOKE_SYMBOLS:
        detail = stats.get(symbol) or {}
        try:
            raw = str(detail["last_ltt_raw"])
            parsed = parse_epoch_ms(raw)
            source = datetime.fromisoformat(detail["last_tick_timestamp"])
            received = datetime.fromisoformat(detail["last_received_timestamp"])
            if source.tzinfo is None or received.tzinfo is None:
                return False
            if abs((parsed - source.astimezone(timezone.utc)).total_seconds()) > 0.001:
                return False
            local_source = parsed.astimezone(zone)
            local_received = received.astimezone(zone)
            age = (received.astimezone(timezone.utc) - parsed).total_seconds()
            if (local_source.date() != session_date or local_received.date() != session_date
                    or not market_open <= local_source.timetz().replace(tzinfo=None) < market_close
                    or age < -config.realtime.max_future_skew_seconds
                    or age > config.realtime.max_quote_age_seconds):
                return False
        except (KeyError, TypeError, ValueError, OverflowError):
            return False
    return True


async def _live_read_only(
    config: AppConfig,
    mapping: dict[str, dict[str, str]],
    *,
    smoke_timeout_seconds: int,
    coverage_seconds: int,
) -> dict:
    settings = config.realtime.model_copy(update={"enabled": True, "provider": PROVIDER_NAME})
    calendar = NseTradingCalendar(_rooted(config.forward_paper.trading_calendar_path))
    builder = IntradayCandleBuilder(
        settings.candle_intervals,
        calendar=calendar,
        timezone_name=config.project.timezone,
        preopen_start=time.fromisoformat(settings.preopen_start),
        market_open=time.fromisoformat(settings.market_open),
        market_close=time.fromisoformat(settings.market_close),
    )
    recorder = None
    if settings.recording:
        day_folder = datetime.now(IST).date().isoformat()
        recorder = TickRecorder(_rooted(settings.recording_dir) / day_folder)
    provider = UpstoxV3Provider(
        mapping,
        access_token=os.environ["UPSTOX_ACCESS_TOKEN"],
        max_quote_age_seconds=settings.max_quote_age_seconds,
        max_future_skew_seconds=settings.max_future_skew_seconds,
        max_reconnect_attempts=int(os.environ.get("UPSTOX_MAX_RECONNECT_ATTEMPTS", "5")),
        reconnect_base_seconds=settings.retry_backoff_seconds,
        mode=os.environ.get("UPSTOX_MODE", "ltpc").strip().lower(),
        calendar=calendar,
        session_timezone=config.project.timezone,
    )
    monitor = RealtimeFeedMonitor(
        provider,
        requested_symbols=list(mapping),
        settings=settings,
        candle_builder=builder,
        health_snapshot_path=_rooted(settings.health_snapshot_path),
        recorder=recorder,
    )
    session_date = datetime.now(IST).date().isoformat()
    universe_size = len(mapping)
    universe_result_key = f"{universe_size}_symbol_result"
    state = {
        "status": "IN_PROGRESS", "session_date": session_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "provider": PROVIDER_NAME, "mode": "LIVE_READ_ONLY",
        "mode_name": "LTPC", "universe_size": universe_size,
        "smoke_timeout_seconds": smoke_timeout_seconds,
        "coverage_observation_seconds": coverage_seconds,
        "coverage_stale_after_seconds": COVERAGE_STALE_AFTER_SECONDS,
        "mapped_symbols": len(mapping), "subscribed_symbols": 0,
        "five_symbol_gate": "PENDING", universe_result_key: "PENDING",
        "volume_status": "UNAVAILABLE_IN_LTPC; no aggregate volume fabricated",
        "timestamp_semantics": "CONFIGURED_EPOCH_MS_NOT_EMPIRICALLY_VALIDATED",
        "market_data_connection_attempted": True,
        "live_trading_enabled": False, "orders_created": False,
        "signals_generated": False, "ledger_mutated": False,
    }
    _atomic_json(RESULTS_PATH, state)
    stream_task = asyncio.create_task(provider.run(SMOKE_SYMBOLS))
    loop = asyncio.get_running_loop()
    start = loop.time()
    smoke_deadline = start + smoke_timeout_seconds
    smoke_passed_at: float | None = None
    reconnect_requested = False
    reconnect_passed = False
    reconnect_count_before: int | None = None
    ticks_before_reconnect: dict[str, int] = {}
    all_symbol_subscription_started = False
    connection_status_at_observation_end = "DISCONNECTED"
    try:
        while not stream_task.done():
            now = datetime.now(timezone.utc)
            monitor.poll_once()
            stats = provider.health_snapshot()["symbol_stats"]
            candles = {(bar["symbol"], bar["interval"]) for bar in builder.latest_candles()}
            smoke_passed = all(
                stats[symbol]["snapshot_received"]
                and stats[symbol]["live_update_received"]
                and stats[symbol]["tick_count"] >= 2
                and (symbol, "1m") in candles
                and (symbol, "5m") in candles
                for symbol in SMOKE_SYMBOLS
            )
            health = provider.health_snapshot()
            if smoke_passed and not reconnect_requested and not all_symbol_subscription_started:
                state["five_symbol_gate"] = "PASS"
                state["five_symbol_gate_passed_at"] = now.isoformat()
                state["controlled_reconnect"] = "REQUESTED_AFTER_FIVE_SYMBOL_GATE"
                reconnect_count_before = health["reconnect_count"]
                ticks_before_reconnect = {symbol: stats[symbol]["tick_count"] for symbol in SMOKE_SYMBOLS}
                await provider.request_validation_reconnect()
                reconnect_requested = True
                _atomic_json(RESULTS_PATH, state)
            if reconnect_requested and not reconnect_passed and not all_symbol_subscription_started:
                health = provider.health_snapshot()
                reconnect_passed = (
                    reconnect_count_before is not None
                    and health["reconnect_count"] > reconnect_count_before
                    and health["connection_status"] == "CONNECTED"
                    and all(stats[symbol]["tick_count"] > ticks_before_reconnect[symbol]
                            for symbol in SMOKE_SYMBOLS)
                )
                if reconnect_passed:
                    await provider.subscribe(symbol for symbol in mapping if symbol not in SMOKE_SYMBOLS)
                    all_symbol_subscription_started = True
                    smoke_passed_at = loop.time()
                    state["controlled_reconnect"] = "PASS"
                    state["subscribed_symbols"] = len(provider.health_snapshot()["subscribed_symbol_list"])
                    state["all_symbol_subscription_started_at"] = now.isoformat()
                    _atomic_json(RESULTS_PATH, state)
            if not all_symbol_subscription_started and loop.time() >= smoke_deadline:
                if not smoke_passed:
                    state["five_symbol_gate"] = "FAIL"
                    state[universe_result_key] = "NOT_STARTED_FIVE_SYMBOL_GATE_FAILED"
                else:
                    state["controlled_reconnect"] = "FAIL_OR_NO_POST_RECONNECT_TICKS"
                    state[universe_result_key] = "NOT_STARTED_CONTROLLED_RECONNECT_FAILED"
                break
            if all_symbol_subscription_started and smoke_passed_at is not None:
                if loop.time() - smoke_passed_at >= coverage_seconds:
                    break
            await asyncio.sleep(0.5)
        if stream_task.done() and not stream_task.cancelled():
            try:
                stream_task.result()
            except Exception as error:
                state["stream_task_error_type"] = type(error).__name__
        connection_status_at_observation_end = provider.health_snapshot()["connection_status"]
        await provider.stop()
        if not stream_task.done():
            await asyncio.wait_for(stream_task, timeout=3.0)
    except asyncio.TimeoutError:
        state["stream_stop"] = "STOP_TIMEOUT"
    finally:
        if not stream_task.done():
            stream_task.cancel()
            try:
                await stream_task
            except asyncio.CancelledError:
                pass

    monitor.poll_once()
    active_period = provider.health_snapshot().get("market_segment_status") == "NORMAL_OPEN"
    provider_snapshot = _write_coverage(
        provider, mapping, COVERAGE_PATH, active_period=active_period,
        connection_status=connection_status_at_observation_end,
    )
    five_count = _write_five_symbol_validation(provider, mapping)
    candles = builder.latest_candles()
    five_count = min(five_count, sum(
        all(provider_snapshot["symbol_stats"][symbol][key] for key in ("snapshot_received", "live_update_received"))
        and (symbol, "1m") in {(c.symbol, c.interval) for c in candles}
        and (symbol, "5m") in {(c.symbol, c.interval) for c in candles}
        for symbol in SMOKE_SYMBOLS
    ))
    timestamp_empirically_validated = (
        five_count == len(SMOKE_SYMBOLS)
        and _five_symbol_timestamps_empirically_plausible(
            provider, config, session_date=date.fromisoformat(session_date),
        )
    )
    state.update({
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "five_symbol_pass_count": five_count,
        "controlled_reconnect": state.get("controlled_reconnect", "NOT_RUN"),
        "timestamp_semantics": (
            "EMPIRICALLY_VALIDATED" if timestamp_empirically_validated
            else "CONFIGURED_EPOCH_MS_NOT_EMPIRICALLY_VALIDATED"
        ),
        universe_result_key: "OBSERVED" if all_symbol_subscription_started else "NOT_STARTED",
        "requested_symbols": universe_size if all_symbol_subscription_started else len(SMOKE_SYMBOLS),
        "mapped_symbols": provider_snapshot["mapped_symbols"],
        "subscribed_symbols": provider_snapshot["subscribed_symbols"],
        "receiving_symbols": provider_snapshot["receiving_symbols"],
        "quotes": provider_snapshot["quote_count"],
        "invalid": provider_snapshot["invalid_count"],
        "stale": provider_snapshot["stale_count"],
        "duplicates": provider_snapshot["duplicate_count"],
        "volume_anomalies": provider_snapshot["volume_anomaly_count"],
        "reconnects": provider_snapshot["reconnect_count"],
        "latency_sample_count": provider_snapshot["latency_sample_count"],
        "latency_min_ms": provider_snapshot["latency_min_ms"],
        "latency_mean_ms": provider_snapshot["latency_mean_ms"],
        "latency_median_ms": provider_snapshot["latency_median_ms"],
        "latency_p95_ms": provider_snapshot["latency_p95_ms"],
        "latency_p99_ms": provider_snapshot["latency_p99_ms"],
        "latency_max_ms": provider_snapshot["latency_max_ms"],
        "candle_1m_count": sum(bar.interval == "1m" for bar in candles),
        "candle_5m_count": sum(bar.interval == "5m" for bar in candles),
        "market_segment_status": provider_snapshot["market_segment_status"],
        "coverage_file": str(COVERAGE_PATH.relative_to(ROOT)),
        "five_symbol_file": str(FIVE_SYMBOL_PATH.relative_to(ROOT)),
        "validation_report_file": str(RESULTS_PATH.relative_to(ROOT)),
        "status": "COMPLETED",
        "result_status": (
            "PASS" if state.get("five_symbol_gate") == "PASS" and reconnect_passed
            and timestamp_empirically_validated and all_symbol_subscription_started
            and provider_snapshot["subscribed_symbols"] == universe_size
            else "PARTIAL"
        ),
    })
    _atomic_json(RESULTS_PATH, state)
    return state


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upstox V3 read-only market-data validation")
    parser.add_argument("--mode", choices=("OFFLINE", "PREFLIGHT", "LIVE_READ_ONLY"), default="PREFLIGHT")
    parser.add_argument("--config", default="config/phase3d_personal.yaml")
    parser.add_argument("--smoke-timeout-seconds", type=int, default=300)
    parser.add_argument("--coverage-seconds", type=int, default=900)
    args = parser.parse_args(argv)
    if args.smoke_timeout_seconds < 1 or args.coverage_seconds < 1:
        parser.error("live validation durations must be positive")

    if args.mode == "OFFLINE":
        deps = required_dependencies()
        universe_size, size_valid = _configured_universe_size()
        mapping = _mapping_summary(
            mapping_artifact_path(universe_size, project_root=ROOT), expected_count=universe_size,
        )
        print(json.dumps({
            "status": "OFFLINE_READY" if all(deps.values()) and size_valid else "OFFLINE_BLOCKED",
            "dependencies": deps, "mapping": mapping, "universe_size": universe_size,
            "network_market_data_request_made": False,
            "orders_created": False, "signals_generated": False, "ledger_mutated": False,
        }, sort_keys=True))
        return 0 if all(deps.values()) else 2

    if args.mode == "PREFLIGHT":
        try:
            result = _preflight(_rooted(args.config))
        except Exception as error:
            result = {
                "status": "PHASE4A8A_PREFLIGHT_BLOCKED",
                "blockers": [f"configuration_or_preflight_error:{type(error).__name__}"],
                "network_market_data_request_made": False,
                "credential_value_displayed_or_persisted": False,
                "orders_created": False, "signals_generated": False, "ledger_mutated": False,
            }
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "PHASE4A8A_PREFLIGHT_READY" else 2

    # LIVE_READ_ONLY is the only mode allowed to contact the Upstox market-data API.
    live_trading_value = os.environ.get("LIVE_TRADING", "").strip().lower()
    if live_trading_value == "true" or env_true(os.environ.get("LIVE_TRADING_ENABLED")):
        print(json.dumps({"status": "LIVE_TRADING_MUST_REMAIN_DISABLED", "market_data_request_made": False,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 2
    if not os.environ.get("UPSTOX_ACCESS_TOKEN", "").strip():
        print(json.dumps({"status": "UPSTOX_PROVIDER_NOT_CONFIGURED",
                          "token_present": False, "market_data_request_made": False,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 2
    if os.environ.get("REALTIME_PROVIDER", "").strip().lower() != PROVIDER_NAME:
        print(json.dumps({"status": "REALTIME_PROVIDER_NOT_UPSTOX", "market_data_request_made": False,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 2
    if not env_true(os.environ.get("REALTIME_ENABLED")):
        print(json.dumps({"status": "REALTIME_DISABLED", "market_data_request_made": False,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 2
    if live_trading_value != "false":
        print(json.dumps({"status": "LIVE_TRADING_MUST_REMAIN_DISABLED", "market_data_request_made": False,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 2
    universe_size, size_valid = _configured_universe_size()
    if not size_valid:
        print(json.dumps({"status": "REALTIME_UNIVERSE_SIZE_INVALID", "market_data_request_made": False,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 2
    try:
        config = AppConfig.load(_rooted(args.config))
        if config.live_trading.enabled:
            print(json.dumps({"status": "LIVE_TRADING_MUST_REMAIN_DISABLED", "market_data_request_made": False,
                              "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
            return 2
        selected = read_realtime_universe_csv(
            realtime_universe_artifact_path(universe_size, project_root=ROOT),
            expected_count=universe_size,
        )
        mapping_path = mapping_artifact_path(universe_size, project_root=ROOT)
        mapping = load_verified_mapping(
            mapping_path, expected_count=universe_size,
            allow_missing_trading_symbol=True,
        )
        selected_identity = {row["symbol"].strip().upper(): row["isin"].strip().upper() for row in selected}
        if (len(mapping) != universe_size or set(mapping) != set(selected_identity)
                or any(mapping[symbol]["project_isin"].upper() != isin
                       for symbol, isin in selected_identity.items())
                or not set(SMOKE_SYMBOLS).issubset(mapping)):
            raise UpstoxConfigurationError("verified selected-universe mapping or five-symbol gate is incomplete")
        reconnects = int(os.environ.get("UPSTOX_MAX_RECONNECT_ATTEMPTS", "5"))
        if reconnects < 1 or reconnects > 5:
            raise UpstoxConfigurationError("UPSTOX_MAX_RECONNECT_ATTEMPTS must be between one and five")
        if os.environ.get("UPSTOX_MODE", "ltpc").strip().lower() != "ltpc":
            raise UpstoxConfigurationError("UPSTOX_MODE must be ltpc in Phase 4A.8")
        readiness = _preflight(_rooted(args.config))
        if readiness["status"] != "PHASE4A8A_PREFLIGHT_READY":
            raise UpstoxConfigurationError("read-only preflight checks did not all pass")
        if not _regular_nse_session_open(config):
            print(json.dumps({"status": "NSE_SESSION_CLOSED", "market_data_request_made": False,
                              "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
            return 2
    except Exception as error:
        print(json.dumps({"status": "LIVE_READ_ONLY_PREFLIGHT_BLOCKED",
                          "reason_code": type(error).__name__, "market_data_request_made": False,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 3
    try:
        result = asyncio.run(_live_read_only(
            config, mapping,
            smoke_timeout_seconds=args.smoke_timeout_seconds,
            coverage_seconds=args.coverage_seconds,
        ))
    except KeyboardInterrupt:
        print(json.dumps({"status": "LIVE_READ_ONLY_STOPPED", "reason": "keyboard_interrupt",
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 130
    except Exception as error:
        # Never stringify provider/auth/network exceptions because a library may
        # include a request URL or Authorization header in its message.
        print(json.dumps({"status": "LIVE_READ_ONLY_FAILED", "reason_code": type(error).__name__,
                          "orders_created": False, "signals_generated": False, "ledger_mutated": False}, sort_keys=True))
        return 3
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("five_symbol_gate") == "PASS" else 4


if __name__ == "__main__":
    raise SystemExit(main())
