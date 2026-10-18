"""Deterministic Phase 4D paper-session orchestration.

This module consumes normalized closed bars only.  It has no broker or live
provider imports; REALTIME_PAPER and LIVE are explicit fail-closed modes.
"""

from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import math
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from time import perf_counter
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from nse_research.execution.phase4c import (
    IntradayPaperReplay,
    PaperExecutionEngine,
    PaperExecutionError,
    PaperStateCorrupted,
    inspect_paper_ledger,
)
from nse_research.execution.phase4c_models import ExecutionMode, OrderStatus, Phase4CPaperConfig, SignalEnvelope
from nse_research.intraday.model import (
    BarValidationError,
    IntradayBar,
    default_nse_calendar,
    load_frozen_universe,
    validate_intraday_bar,
)
from nse_research.intraday.scanner import IntradayScanner, IntradayScannerConfig


IST = ZoneInfo("Asia/Kolkata")
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_PATH = PROJECT_ROOT / "data/intraday/canonical_intraday_bars.csv"
DEFAULT_LEDGER_PATH = PROJECT_ROOT / "data/paper_trading/phase4c/ledger.json"
CANONICAL_COLUMNS = {
    "symbol", "exchange", "series", "timeframe", "timestamp", "open", "high", "low", "close",
    "volume", "volume_kind", "status", "source",
}
BLOCKED_SOURCE_PARTS = ("SYNTHETIC", "FIXTURE", "MOCK")


class Phase4DError(RuntimeError):
    """Fail-closed Phase 4D status with a stable reason code."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False, allow_nan=False, default=str) + "\n"
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            pass
    except OSError:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _read_checkpoint(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4DError("CHECKPOINT_LEDGER_MISMATCH", f"checkpoint cannot be read: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise Phase4DError("CHECKPOINT_LEDGER_MISMATCH", "checkpoint schema is invalid")
    return value


def _truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    if raw.strip().lower() in {"1", "true", "yes", "on"}:
        return True
    if raw.strip().lower() in {"0", "false", "no", "off"}:
        return False
    raise Phase4DError("CONFIG_INVALID", f"{name} must be a boolean")


def _default_trading_date(calendar, now: datetime | None = None) -> date:
    local = (now or datetime.now(IST)).astimezone(IST).date()
    candidate = local
    while candidate.weekday() >= 5 or candidate in calendar.holidays or candidate in calendar.special_sessions:
        candidate += timedelta(days=1)
    return candidate


def _bar_from_row(row: Mapping[str, str], row_number: int, calendar) -> IntradayBar:
    try:
        raw_stamp = datetime.fromisoformat(str(row.get("timestamp", "")).strip().replace("Z", "+00:00"))
        raw_volume = str(row.get("volume", "")).strip()
        bar = IntradayBar(
            symbol=row.get("symbol", ""), exchange=row.get("exchange", ""), series=row.get("series", ""),
            timeframe=row.get("timeframe", ""), timestamp=raw_stamp,
            open=float(row.get("open", "")), high=float(row.get("high", "")),
            low=float(row.get("low", "")), close=float(row.get("close", "")),
            volume=None if not raw_volume else float(raw_volume),
            volume_kind=row.get("volume_kind", ""), status=row.get("status", ""), source=row.get("source", ""),
        )
    except (ValueError, TypeError, OverflowError) as exc:
        raise Phase4DError("DATA_RECORD_INVALID", f"row {row_number}: malformed timestamp or numeric field ({exc})") from exc
    try:
        return validate_intraday_bar(bar, calendar=calendar, require_closed=True)
    except BarValidationError as exc:
        raise Phase4DError("DATA_RECORD_INVALID", f"row {row_number}: {exc.code}: {exc}") from exc


def _validate_persisted_state(state: Mapping[str, Any]) -> None:
    """Validate paper accounting records beyond JSON and checksum integrity."""
    def finite(value: Any, field_name: str, *, positive: bool = False, nonnegative: bool = False) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"{field_name} is not numeric") from exc
        if not math.isfinite(number) or (positive and number <= 0) or (nonnegative and number < 0):
            raise ValueError(f"{field_name} is outside its valid numeric range")
        return number

    def aware(value: Any, field_name: str) -> None:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{field_name} must be timezone-aware")

    finite(state.get("cash"), "cash", nonnegative=True)
    finite(state.get("peak_equity"), "peak_equity", positive=True)
    finite(state.get("daily_start_equity"), "daily_start_equity", positive=True)
    for name in ("last_event_time", "last_valid_market_time", "last_data_quality_halt"):
        if state.get(name) is not None:
            aware(state[name], name)
    if state.get("session_date") is not None:
        date.fromisoformat(str(state["session_date"]))
    metrics = state.get("portfolio_metrics")
    if not isinstance(metrics, Mapping) or metrics.get("risk_state") not in {
        "ACTIVE", "DAILY_LOSS_HALT", "PORTFOLIO_DRAWDOWN_HALT", "MANUAL_HALT", "DATA_QUALITY_HALT",
    }:
        raise ValueError("risk state is missing or invalid")
    for name in ("positions", "orders", "trades", "risk_events", "equity_curve"):
        if not isinstance(state.get(name), list):
            raise ValueError(f"{name} must be a list")
    for index, position in enumerate(state["positions"]):
        if not isinstance(position, Mapping) or position.get("status") not in {"OPEN", "CLOSED"}:
            raise ValueError(f"position {index} status/schema is invalid")
        finite(position.get("quantity"), f"position {index} quantity", positive=True)
        for key in ("average_entry_price", "current_price", "stop_price", "target_price"):
            finite(position.get(key), f"position {index} {key}", positive=True)
        aware(position.get("opened_at"), f"position {index} opened_at")
    for index, order in enumerate(state["orders"]):
        if not isinstance(order, Mapping) or order.get("status") not in {
            "CREATED", "VALIDATED", "REJECTED", "PENDING", "PARTIALLY_FILLED", "FILLED", "CANCELLED", "EXPIRED",
        }:
            raise ValueError(f"order {index} status/schema is invalid")
        for key in ("timestamp", "signal_timestamp", "signal_valid_until"):
            aware(order.get(key), f"order {index} {key}")
        for key in ("requested_price", "stop_price", "target_price"):
            finite(order.get(key), f"order {index} {key}", nonnegative=True)
        finite(order.get("quantity"), f"order {index} quantity", nonnegative=True)
        if order["status"] in {"PENDING", "PARTIALLY_FILLED", "FILLED"}:
            finite(order.get("quantity"), f"order {index} active quantity", positive=True)
            finite(order.get("requested_price"), f"order {index} active price", positive=True)
        if order.get("filled_at") is not None:
            aware(order["filled_at"], f"order {index} filled_at")
        if order.get("filled_price") is not None:
            finite(order["filled_price"], f"order {index} filled_price", positive=True)
    for index, trade in enumerate(state["trades"]):
        if not isinstance(trade, Mapping):
            raise ValueError(f"trade {index} schema is invalid")
        for key in ("entry_time", "exit_time"):
            aware(trade.get(key), f"trade {index} {key}")
        finite(trade.get("quantity"), f"trade {index} quantity", positive=True)
        for key in ("entry_price", "exit_price"):
            if key in trade:
                finite(trade[key], f"trade {index} {key}", positive=True)
        for key in ("gross_pnl", "net_pnl", "total_cost"):
            if key in trade:
                finite(trade[key], f"trade {index} {key}")
    for collection, timestamp_key in (("risk_events", "timestamp"), ("equity_curve", "timestamp")):
        for index, item in enumerate(state[collection]):
            if not isinstance(item, Mapping):
                raise ValueError(f"{collection} item {index} schema is invalid")
            aware(item.get(timestamp_key), f"{collection} item {index} {timestamp_key}")
    session_state = state.get("session_state", {})
    if not isinstance(session_state, Mapping):
        raise ValueError("session_state must be an object")
    valid_session_statuses = {"NOT_STARTED", "PRECHECK", "RUNNING", "HALTED", "COMPLETED", "BLOCKED", "FAILED"}
    active = session_state.get("active_session")
    if active is not None and (not isinstance(active, Mapping) or active.get("status") not in valid_session_statuses):
        raise ValueError("active session state is invalid")


@dataclass(frozen=True)
class PaperSession:
    session_id: str
    trading_date: date
    exchange: str = "NSE"
    status: str = "PRECHECK"
    started_at: datetime | None = None
    last_event_at: datetime | None = None
    last_bar_at: datetime | None = None
    signal_count: int = 0
    order_count: int = 0
    fill_count: int = 0
    closed_trade_count: int = 0
    ending_equity: float | None = None

    def __post_init__(self) -> None:
        if self.exchange != "NSE":
            raise ValueError("Phase 4D supports the NSE session only")
        if self.status not in {"NOT_STARTED", "PRECHECK", "RUNNING", "HALTED", "COMPLETED", "BLOCKED", "FAILED"}:
            raise ValueError("invalid Phase 4D session status")


@dataclass
class PreflightResult:
    status: str
    reasons: list[str]
    trading_date: date
    session_id: str
    universe_size: int
    universe: list[str]
    data_path: Path
    ledger_path: Path
    data_quality: dict[str, Any] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    session: PaperSession | None = None
    config: Phase4CPaperConfig | None = None
    bars: list[IntradayBar] = field(default_factory=list, repr=False)
    dataset_checksum: str | None = None
    ledger_state: dict[str, Any] | None = field(default=None, repr=False)
    checkpoint: dict[str, Any] | None = field(default=None, repr=False)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status, "reasons": list(self.reasons), "trading_date": self.trading_date.isoformat(),
            "session_id": self.session_id, "universe_size": self.universe_size,
            "session": asdict(self.session) if self.session else None,
            "data_path": str(self.data_path), "ledger_path": str(self.ledger_path),
            "data_quality": self.data_quality, "provenance": self.provenance,
            "dataset_checksum": self.dataset_checksum,
        }


class SessionLock:
    """Unix advisory lock; the OS releases it automatically after process death."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._stream = None

    def __enter__(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._stream = self.path.open("a+")
            fcntl.flock(self._stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._stream.seek(0)
            self._stream.truncate()
            self._stream.write(f"pid={os.getpid()}\n")
            self._stream.flush()
            return self
        except (OSError, BlockingIOError) as exc:
            if self._stream is not None:
                self._stream.close()
                self._stream = None
            raise Phase4DError("SESSION_LOCKED", f"could not acquire {self.path.name}") from exc

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._stream is not None:
            try:
                fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            finally:
                self._stream.close()
                self._stream = None


class Phase4DOrchestrator:
    """Preflight and run one closed-bar replay through existing Phase 4B/4C."""

    def __init__(
        self,
        *,
        project_root: str | Path = PROJECT_ROOT,
        data_path: str | Path | None = None,
        ledger_path: str | Path | None = None,
        universe_size: int | None = None,
        min_coverage: float | None = None,
        calendar=None,
    ) -> None:
        self.root = Path(project_root).resolve()
        configured_path = data_path or os.environ.get("INTRADAY_DATA_PATH") or (self.root / "data/intraday/canonical_intraday_bars.csv")
        self.data_path = Path(configured_path)
        if not self.data_path.is_absolute():
            self.data_path = self.root / self.data_path
        self.ledger_path = Path(ledger_path) if ledger_path else self.root / "data/paper_trading/phase4c/ledger.json"
        if not self.ledger_path.is_absolute():
            self.ledger_path = self.root / self.ledger_path
        self.universe_size = universe_size if universe_size is not None else int(os.environ.get("INTRADAY_UNIVERSE_SIZE", "100"))
        if self.universe_size not in {100, 200, 486}:
            raise Phase4DError("CONFIG_INVALID", "universe size must be 100, 200, or 486")
        self.min_coverage = min_coverage if min_coverage is not None else float(os.environ.get("MIN_INTRADAY_COVERAGE", "0.95"))
        if not math.isfinite(self.min_coverage) or not 0 < self.min_coverage <= 1:
            raise Phase4DError("CONFIG_INVALID", "minimum intraday coverage must be in (0, 1]")
        self.calendar = calendar or default_nse_calendar()

    @staticmethod
    def session_id_for(trading_date: date) -> str:
        return f"PAPER-NSE-{trading_date:%Y%m%d}"

    def checkpoint_path(self, session_id: str) -> Path:
        return self.ledger_path.parent / "checkpoints" / f"{session_id}.json"

    def _runtime_config(self) -> Phase4CPaperConfig:
        config = Phase4CPaperConfig.from_env(explicit_paper_command=True)
        if config.execution_mode is not ExecutionMode.PAPER:
            raise Phase4DError("PAPER_MODE_REQUIRED", "Phase 4D supports PAPER only")
        if config.live_trading_enabled or _truthy("LIVE_TRADING", False) or _truthy("LIVE_TRADING_ENABLED", False):
            raise Phase4DError("LIVE_TRADING_MUST_REMAIN_DISABLED", "LIVE_TRADING and LIVE_TRADING_ENABLED must be false")
        if config.realtime_enabled:
            raise Phase4DError("REALTIME_PROVIDER_NOT_VALIDATED", "realtime paper execution is not validated")
        broker_provider = os.environ.get("BROKER_PROVIDER", "").strip()
        if broker_provider and broker_provider.upper() not in {"NONE", "DISABLED"}:
            raise Phase4DError("BROKER_PROVIDER_NOT_ALLOWED", "broker configuration must remain absent")
        return config

    def _read_dataset(self, day: date, universe: Sequence[str]) -> tuple[list[IntradayBar], dict[str, Any], dict[str, Any], str]:
        if not self.data_path.is_file():
            raise Phase4DError("PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE", f"canonical intraday dataset not found: {self.data_path}")
        checksum = _sha256_file(self.data_path)
        try:
            with self.data_path.open("r", newline="", encoding="utf-8-sig") as stream:
                reader = csv.DictReader(stream)
                names = {str(name).strip().lower() for name in (reader.fieldnames or [])}
                if not CANONICAL_COLUMNS.issubset(names):
                    missing = sorted(CANONICAL_COLUMNS - names)
                    raise Phase4DError("DATA_SCHEMA_INVALID", f"missing canonical fields: {', '.join(missing)}")
                normalized_names = {str(name).strip().lower(): name for name in reader.fieldnames or []}
                bars: list[IntradayBar] = []
                seen: set[tuple[str, str, date, datetime]] = set()
                source_labels: set[str] = set()
                symbols_with_data: set[str] = set()
                timeframe_counts: dict[str, int] = {}
                duplicates = 0
                out_of_order = 0
                invalid = 0
                timestamp_errors = 0
                previous_key = None
                rejected: list[str] = []
                rows_total = 0
                for row_number, raw in enumerate(reader, start=2):
                    rows_total += 1
                    row = {key: raw.get(value, "") for key, value in normalized_names.items()}
                    try:
                        item = _bar_from_row(row, row_number, self.calendar)
                    except Phase4DError as exc:
                        invalid += 1
                        if any(token in str(exc) for token in ("TIMESTAMP", "SESSION", "OUTSIDE_SESSION")):
                            timestamp_errors += 1
                        if len(rejected) < 20:
                            rejected.append(str(exc))
                        continue
                    source_labels.add(item.source)
                    if any(part in item.source.upper() for part in BLOCKED_SOURCE_PARTS):
                        invalid += 1
                        if len(rejected) < 20:
                            rejected.append(f"row {row_number}: synthetic/fixture source is not accepted in REPLAY")
                        continue
                    key = item.identity
                    if key in seen:
                        duplicates += 1
                        continue
                    seen.add(key)
                    event_key = (item.end, item.symbol, item.timeframe, item.timestamp)
                    if previous_key is not None and event_key < previous_key:
                        out_of_order += 1
                    previous_key = event_key
                    if item.trading_date == day and item.symbol in universe:
                        bars.append(item)
                        symbols_with_data.add(item.symbol)
                        timeframe_counts[item.timeframe] = timeframe_counts.get(item.timeframe, 0) + 1
        except Phase4DError:
            raise
        except (OSError, UnicodeError, csv.Error) as exc:
            raise Phase4DError("DATA_FILE_INVALID", str(exc)) from exc
        bars.sort(key=lambda item: (item.trading_date, item.end, item.symbol, item.timeframe, item.timestamp))
        expected = set(universe)
        missing = sorted(expected - symbols_with_data)
        coverage = len(symbols_with_data) / len(expected) if expected else 0.0
        available_timeframes = sorted(timeframe_counts, key=lambda item: (item != "1m", item))
        expected_bars = len(expected) * sum(375 if timeframe == "1m" else 75 for timeframe in available_timeframes)
        bar_gaps = max(0, expected_bars - len(bars))
        quality = {
            "rows_in_file": rows_total, "first_date": min((bar.trading_date for bar in bars), default=None),
            "last_date": max((bar.trading_date for bar in bars), default=None),
            "expected_symbols": len(expected), "symbols_with_data": len(symbols_with_data), "missing_symbols": missing,
            "coverage_percent": coverage, "minimum_coverage": self.min_coverage,
            "available_timeframes": available_timeframes, "bars_received": len(bars),
            "expected_bars": expected_bars, "missing_intervals": bar_gaps,
            "duplicate_intervals": duplicates, "out_of_order_rows": out_of_order,
            "rejected_rows": invalid, "timestamp_errors": timestamp_errors,
            "rejection_examples": rejected,
        }
        session_close = datetime.combine(day, time(15, 30), IST)
        first_event = min((bar.end for bar in bars), default=None)
        last_event = max((bar.end for bar in bars), default=None)
        quality["first_event_time"] = first_event.isoformat() if first_event else None
        quality["last_event_time"] = last_event.isoformat() if last_event else None
        quality["session_close_present"] = last_event == session_close
        quality["first_date"] = quality["first_date"].isoformat() if quality["first_date"] else None
        quality["last_date"] = quality["last_date"].isoformat() if quality["last_date"] else None
        provenance: dict[str, Any] = {
            "source": sorted(source_labels), "dataset": self.data_path.name, "sha256": checksum,
            "retrieved_at": None, "source_version": None, "timezone": "Asia/Kolkata",
            "exchange": "NSE", "series": "EQ",
        }
        sidecar_candidates = [self.data_path.with_suffix(self.data_path.suffix + ".manifest.json"),
                             self.data_path.with_suffix(".manifest.json")]
        for sidecar in sidecar_candidates:
            if sidecar.is_file():
                try:
                    supplied = json.loads(sidecar.read_text(encoding="utf-8"))
                    if isinstance(supplied, dict):
                        for name in ("provider", "retrieved_at", "source_version", "source_identifier", "authorization_reference"):
                            if name in supplied:
                                provenance[name] = supplied[name]
                except (OSError, UnicodeError, json.JSONDecodeError):
                    raise Phase4DError("DATA_PROVENANCE_INVALID", f"manifest is malformed: {sidecar}")
                break
        if not bars and invalid == 0:
            raise Phase4DError("PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE", f"no valid bars for {day.isoformat()} and the frozen universe")
        if invalid:
            quality["validation_status"] = "DATA_RECORD_INVALID"
        elif duplicates:
            quality["validation_status"] = "DATA_DUPLICATE_BARS"
        elif coverage < self.min_coverage:
            quality["validation_status"] = "DATA_COVERAGE_HALT"
        elif last_event != session_close:
            quality["validation_status"] = "DATA_SESSION_INCOMPLETE"
        else:
            quality["validation_status"] = "VALID"
        if self.calendar.classify(day, observed_market_data=True).status != "TRADING_DAY":
            raise Phase4DError("SESSION_NOT_TRADING_DAY", f"NSE calendar rejected {day.isoformat()}")
        quality["symbols_with_data_list"] = sorted(symbols_with_data)
        return bars, quality, provenance, checksum

    def _load_ledger_read_only(self, config: Phase4CPaperConfig, day: date, session_id: str,
                               checksum: str | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
        reasons: list[str] = []
        if not self.ledger_path.exists():
            return None, None, reasons
        inspected = inspect_paper_ledger(self.ledger_path)
        if inspected.get("status") != "OK":
            return None, None, [f"PAPER_LEDGER_INVALID: {inspected.get('reason') or inspected.get('status')}"]
        try:
            # Construction performs restore and accounting-invariant checks but does not mutate.
            engine = PaperExecutionEngine(config, calendar=self.calendar, ledger_path=self.ledger_path)
            _validate_persisted_state(engine._state_payload())
        except (PaperStateCorrupted, PaperExecutionError, ValueError) as exc:
            return None, None, [f"PAPER_LEDGER_INVALID: {exc}"]
        session_state = engine.session_state
        completed = set(session_state.get("completed_session_ids") or [])
        active = session_state.get("active_session")
        last_session = session_state.get("last_session")
        if session_id in completed or (isinstance(active, dict) and active.get("session_id") == session_id
                                       and active.get("status") == "COMPLETED"):
            reasons.append("DUPLICATE_SESSION: this trading date already has a completed Phase 4D session")
        elif isinstance(active, dict) and active.get("status") in {"RUNNING", "FAILED", "HALTED"} and active.get("session_id") != session_id:
            reasons.append(f"ACTIVE_SESSION_EXISTS: {active.get('session_id')}")
        elif isinstance(last_session, dict) and last_session.get("trading_date") and last_session.get("trading_date") >= day.isoformat() and last_session.get("session_id") != session_id:
            reasons.append("SESSION_DATE_OUT_OF_ORDER: replay dates must advance monotonically in the persistent ledger")
        checkpoint = _read_checkpoint(self.checkpoint_path(session_id))
        if checkpoint is not None:
            events_by_id = {event.get("event_id"): event for event in engine.events}
            ledger_event = events_by_id.get(checkpoint.get("last_ledger_event_id"))
            if (checkpoint.get("session_id") != session_id or checkpoint.get("trading_date") != day.isoformat()
                    or (checksum and checkpoint.get("dataset_sha256") != checksum)
                    or not ledger_event or ledger_event.get("event_hash") != checkpoint.get("ledger_event_hash")):
                reasons.append("CHECKPOINT_LEDGER_MISMATCH: checkpoint does not reconcile to the Phase 4C ledger")
            else:
                bar_event = next((event for event in engine.events
                                  if event.get("event_type") == "MARKET_BAR_PROCESSED"
                                  and event.get("payload", {}).get("bar_event_id") == checkpoint.get("last_processed_event_id")), None)
                if bar_event is None or bar_event.get("payload", {}).get("session_id") != session_id:
                    reasons.append("CHECKPOINT_LEDGER_MISMATCH: checkpoint bar event is absent from the ledger")
        elif isinstance(active, dict) and active.get("session_id") == session_id and active.get("status") in {"RUNNING", "FAILED", "HALTED"}:
            # A crash can occur after an event commit but before the first atomic checkpoint.
            # Deterministic bar-event ids let the replay safely reconstruct this boundary.
            pass
        state = {"session_state": session_state, "event_count": len(engine.events),
                 "last_event_hash": inspect_paper_ledger(self.ledger_path).get("last_event_hash"),
                 "events": list(engine.events)}
        return state, checkpoint, reasons

    def preflight(self, *, trading_date: date | None = None) -> PreflightResult:
        day = trading_date or _default_trading_date(self.calendar)
        session_id = self.session_id_for(day)
        reasons: list[str] = []
        universe: list[str] = []
        try:
            universe = load_frozen_universe(self.root, self.universe_size)
        except (OSError, ValueError) as exc:
            reasons.append(f"FROZEN_UNIVERSE_INVALID: {exc}")
        if day in self.calendar.special_sessions:
            reasons.append("SPECIAL_SESSION_HOURS_UNVERIFIED: special-session schedule is not represented")
        elif day.weekday() >= 5 or day in self.calendar.holidays:
            reasons.append("SESSION_NOT_TRADING_DAY: requested date is a weekend or local NSE holiday")
        try:
            config = self._runtime_config()
        except Phase4DError as exc:
            config = None
            reasons.append(str(exc))
        bars: list[IntradayBar] = []
        quality: dict[str, Any] = {
            "expected_symbols": len(universe), "symbols_with_data": 0, "missing_symbols": list(universe),
            "coverage_percent": 0.0, "bars_received": 0, "expected_bars": 0, "missing_intervals": 0,
            "duplicate_intervals": 0, "rejected_rows": 0, "validation_status": "UNAVAILABLE",
        }
        provenance: dict[str, Any] = {"dataset": self.data_path.name, "sha256": None,
                                      "retrieved_at": None, "source_version": None,
                                      "timezone": "Asia/Kolkata", "exchange": "NSE", "series": "EQ"}
        checksum = None
        if universe:
            try:
                bars, quality, provenance, checksum = self._read_dataset(day, universe)
                if quality.get("rejected_rows"):
                    examples = "; ".join(quality.get("rejection_examples", [])[:5])
                    reasons.append(f"DATA_RECORD_INVALID: {quality['rejected_rows']} rejected rows; {examples}")
                if quality.get("duplicate_intervals"):
                    reasons.append(f"DATA_DUPLICATE_BARS: {quality['duplicate_intervals']} duplicate symbol/timeframe intervals")
                if not bars and not quality.get("rejected_rows"):
                    reasons.append(f"PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE: no valid bars for {day.isoformat()} and the frozen universe")
                if quality.get("coverage_percent", 0) < self.min_coverage and quality.get("bars_received", 0):
                    reasons.append(
                        f"DATA_COVERAGE_HALT: {quality.get('symbols_with_data', 0)}/{quality.get('expected_symbols', 0)} symbols "
                        f"({quality.get('coverage_percent', 0):.1%}); minimum is {self.min_coverage:.1%}"
                    )
                if quality.get("bars_received", 0) and not quality.get("session_close_present"):
                    reasons.append("DATA_SESSION_INCOMPLETE: no valid closed bar reaches the 15:30 NSE session close")
            except Phase4DError as exc:
                reasons.append(str(exc))
                if exc.code not in {"PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE"}:
                    quality["validation_status"] = exc.code
        ledger_state = None
        checkpoint = None
        if config is not None:
            ledger_state, checkpoint, ledger_reasons = self._load_ledger_read_only(config, day, session_id, checksum)
            reasons.extend(ledger_reasons)
        status = "PHASE4D_READY" if not reasons else (
            "PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE"
            if any("PHASE4D_BLOCKED_INTRADAY_DATA_UNAVAILABLE" in reason for reason in reasons)
            else "PHASE4D_FORWARD_BLOCKED"
        )
        session = PaperSession(session_id, day, status="PRECHECK" if not reasons else "BLOCKED")
        return PreflightResult(status=status, reasons=reasons, trading_date=day, session_id=session_id,
                               universe_size=self.universe_size, universe=universe, data_path=self.data_path,
                               ledger_path=self.ledger_path, data_quality=quality, provenance=provenance,
                               session=session, config=config, bars=bars, dataset_checksum=checksum,
                               ledger_state=ledger_state, checkpoint=checkpoint)

    def _event_id_for_bar(self, session_id: str, checksum: str, bar: IntradayBar) -> str:
        material = "|".join((session_id, checksum, bar.symbol, bar.timeframe, bar.timestamp.isoformat(),
                             repr((bar.open, bar.high, bar.low, bar.close, bar.volume, bar.volume_kind, bar.source))))
        return "BAR4D-" + hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]

    def _verify_checkpoint(self, result: PreflightResult) -> int:
        checkpoint = result.checkpoint
        if checkpoint is None:
            return -1
        ids = [self._event_id_for_bar(result.session_id, str(result.dataset_checksum), bar) for bar in result.bars]
        try:
            return ids.index(str(checkpoint.get("last_processed_event_id")))
        except ValueError as exc:
            raise Phase4DError("CHECKPOINT_LEDGER_MISMATCH", "checkpoint event is not present in the requested replay data") from exc

    def run_replay(self, *, trading_date: date | None = None) -> dict[str, Any]:
        initial = self.preflight(trading_date=trading_date)
        if initial.status != "PHASE4D_READY":
            return {**initial.as_dict(), "mode": "REPLAY_DATA", "orders": 0, "fills": 0, "trades": 0,
                    "ledger_mutations": 0, "live_orders": 0, "broker_calls": 0}
        # One writer per shared ledger also serializes different trading dates.
        # flock is released by the OS when a crashed process exits.
        lock_path = self.ledger_path.parent / "session.lock"
        with SessionLock(lock_path):
            result = self.preflight(trading_date=initial.trading_date)
            if result.status != "PHASE4D_READY":
                return {**result.as_dict(), "mode": "REPLAY_DATA", "orders": 0, "fills": 0, "trades": 0,
                        "ledger_mutations": 0, "live_orders": 0, "broker_calls": 0}
            if not result.dataset_checksum or _sha256_file(result.data_path) != result.dataset_checksum:
                raise Phase4DError("DATA_CHANGED_AFTER_PREFLIGHT", "dataset checksum changed before execution")
            assert result.config is not None
            engine = PaperExecutionEngine(result.config, calendar=self.calendar, ledger_path=result.ledger_path)
            if not result.ledger_path.exists():
                engine.initialize_ledger()
                engine = PaperExecutionEngine(result.config, calendar=self.calendar, ledger_path=result.ledger_path)
            checkpoint_index = self._verify_checkpoint(result)
            now = datetime.now(IST)
            event_start = result.bars[0].end
            active = engine.session_state.get("active_session")
            started_at = active.get("started_at") if isinstance(active, dict) and active.get("session_id") == result.session_id else now.isoformat()
            session_started_at = datetime.fromisoformat(started_at) if isinstance(started_at, str) else now
            active_state = asdict(PaperSession(
                result.session_id, result.trading_date, status="RUNNING", started_at=session_started_at,
                last_bar_at=event_start, signal_count=0, order_count=0, fill_count=0,
                closed_trade_count=0,
            ))
            active_state.update({"mode": "REPLAY_DATA", "dataset_sha256": result.dataset_checksum,
                                 "data_source": result.provenance.get("source", []),
                                 "universe_size": result.universe_size})
            active_state["trading_date"] = result.trading_date.isoformat()
            active_state["started_at"] = session_started_at.isoformat()
            active_state["last_bar_at"] = event_start.isoformat()
            prior_event_time = (datetime.fromisoformat(engine.events[-1]["timestamp"]).astimezone(IST)
                                if engine.events else event_start)
            engine.set_session_state({**engine.session_state, "active_session": active_state},
                                     at=max(event_start, prior_event_time))
            timeframes = tuple(result.data_quality.get("available_timeframes") or ("1m",))
            scanner = IntradayScanner(result.universe, config=IntradayScannerConfig(
                enabled=True, universe_size=result.universe_size, timeframes=timeframes,
            ), calendar=self.calendar)
            sector_path = self.root / "data/paper_trading/phase3d_personal_universe_v2.csv"
            sectors = self._sector_map(sector_path)
            processed = 0
            signals_seen = 0
            run_started = perf_counter()
            first_time = None
            last_time = None
            try:
                for index, bar in enumerate(result.bars):
                    bar_id = self._event_id_for_bar(result.session_id, str(result.dataset_checksum), bar)
                    event_time = bar.end
                    if first_time is None:
                        first_time = event_time
                    last_time = event_time
                    already_checkpointed = index <= checkpoint_index
                    engine.process_bar(bar, event_time=event_time, session_id=result.session_id, bar_event_id=bar_id)
                    signals = scanner.handle_bar(bar)
                    signals_seen += len(signals)
                    if not already_checkpointed:
                        for signal in signals:
                            engine.submit_signal(
                                SignalEnvelope(signal, source=bar.source,
                                               source_metadata={"session_id": result.session_id,
                                                                "dataset_sha256": result.dataset_checksum,
                                                                "replay_provider": "canonical_intraday_csv",
                                                                "bar_source": bar.source}),
                                replay_time=signal.timestamp,
                                sector=sectors.get(signal.symbol) or sectors.get(f"{signal.symbol}.NS"),
                            )
                        processed += 1
                        event = engine.events[-1] if engine.events else None
                        if event is None:
                            raise Phase4DError("CHECKPOINT_LEDGER_MISMATCH", "ledger has no event after a processed bar")
                        _atomic_json(self.checkpoint_path(result.session_id), {
                            "schema_version": 1, "session_id": result.session_id,
                            "trading_date": result.trading_date.isoformat(),
                            "last_processed_timestamp": event_time.isoformat(),
                            "last_processed_event_id": bar_id,
                            "last_ledger_event_id": event.get("event_id"),
                            "ledger_event_hash": event.get("event_hash"),
                            "dataset_sha256": result.dataset_checksum,
                            "updated_at": datetime.now(IST).isoformat(),
                            "bars_processed": index + 1,
                        })
                session_was_closed = any(event.get("event_type") == "SESSION_SQUARED_OFF"
                                         and str(event.get("timestamp", "")).startswith(result.trading_date.isoformat())
                                         for event in engine.events)
                if (last_time is not None and last_time.astimezone(IST).time() >= time(15, 30)
                        and not session_was_closed):
                    engine.close_session(result.trading_date, last_time.astimezone(IST))
                metrics = engine.portfolio_snapshot(last_time)
                session_orders = [order for order in engine.all_orders
                                  if order.source_metadata.get("session_id") == result.session_id]
                session_trades = [trade for trade in engine.all_trades
                                  if getattr(trade, "source_metadata", {}).get("session_id") == result.session_id]
                completed_ids = list(engine.session_state.get("completed_session_ids") or [])
                if result.session_id not in completed_ids:
                    completed_ids.append(result.session_id)
                completed_state = {"active_session": {**active_state, "status": "COMPLETED",
                                                       "last_event_at": last_time.isoformat() if last_time else None,
                                                       "last_bar_at": last_time.isoformat() if last_time else None,
                                                       "signal_count": len(scanner.signals),
                                                       "order_count": len(session_orders),
                                                       "fill_count": sum(order.status == OrderStatus.FILLED.value for order in session_orders),
                                                       "closed_trade_count": len(session_trades),
                                                       "ending_equity": metrics.get("total_equity")},
                                   "completed_session_ids": completed_ids,
                                   "last_session": {"session_id": result.session_id,
                                                    "trading_date": result.trading_date.isoformat(),
                                                    "status": "COMPLETED", "mode": "REPLAY_DATA",
                                                    "dataset_sha256": result.dataset_checksum}}
                report = self._write_session_report(result, scanner, engine, session_orders, session_trades,
                                                    metrics, first_time, last_time, processed,
                                                    runtime_seconds=perf_counter() - run_started)
                engine.set_session_state(completed_state, at=(last_time or now))
                return {"status": "PHASE4D_SESSION_COMPLETED", "mode": "REPLAY_DATA",
                        "session_id": result.session_id, "trading_date": result.trading_date.isoformat(),
                        "bars_processed": processed, "signals": len(scanner.signals),
                        "orders": len(session_orders),
                        "fills": sum(order.status == OrderStatus.FILLED.value for order in session_orders),
                        "trades": len(session_trades), "ending_equity": metrics.get("total_equity"),
                        "report_dir": str(report), "data_quality": result.data_quality,
                        "live_orders": 0, "broker_calls": 0}
            except Phase4DError:
                raise
            except Exception as exc:
                failed_active = {**active_state, "status": "FAILED", "last_event_at": last_time.isoformat() if last_time else None}
                engine.set_session_state({**engine.session_state, "active_session": failed_active},
                                         at=last_time or event_start)
                raise Phase4DError("PHASE4D_SESSION_FAILED", str(exc)) from exc

    def _sector_map(self, path: Path) -> dict[str, str]:
        mapping: dict[str, str] = {}
        if not path.is_file():
            return mapping
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            names = {str(name).strip().lower(): name for name in (reader.fieldnames or [])}
            symbol_field = names.get("symbol")
            sector_field = names.get("industry") or names.get("sector")
            if not symbol_field or not sector_field:
                return mapping
            for row in reader:
                if str(row.get("series") or "EQ").strip().upper() != "EQ":
                    continue
                symbol = str(row.get(symbol_field) or "").strip().upper()
                sector = str(row.get(sector_field) or "").strip().upper()
                if symbol and sector:
                    mapping[symbol] = sector
                    mapping[symbol.removesuffix(".NS")] = sector
        return mapping

    def _write_session_report(self, result: PreflightResult, scanner: IntradayScanner, engine: PaperExecutionEngine,
                              orders: Sequence[Any], trades: Sequence[Any], metrics: Mapping[str, Any],
                              first_time: datetime | None, last_time: datetime | None, bars_processed: int,
                              *, runtime_seconds: float) -> Path:
        folder = self.root / "results/phase4d" / result.session_id
        folder.mkdir(parents=True, exist_ok=True)
        signals = scanner.signals
        fills = [order for order in orders if order.status == OrderStatus.FILLED.value]
        positions = [asdict(position) for position in engine.positions.values()
                     if position.opened_at.date() == result.trading_date]
        risk_events = [asdict(event) for event in engine.risk_events
                       if event.timestamp.astimezone(IST).date() == result.trading_date]
        report = {
            "session_id": result.session_id, "trading_date": result.trading_date.isoformat(),
            "exchange": "NSE", "status": "COMPLETED", "mode": "REPLAY_DATA",
            "data_source": result.provenance, "universe_size": result.universe_size,
            "bars_processed_this_run": bars_processed, "signals": len(signals), "orders": len(orders),
            "fills": len(fills), "closed_trades": len(trades), "first_event_time": first_time,
            "last_event_time": last_time, "ending_equity": metrics.get("total_equity"),
            "daily_pnl": metrics.get("today_pnl"), "drawdown": metrics.get("drawdown"),
            "risk_state": metrics.get("risk_state"), "risk_halts": metrics.get("halt_reasons"),
            "runtime_seconds": runtime_seconds,
            "bars_per_second": bars_processed / runtime_seconds if runtime_seconds > 0 else None,
            "symbols_processed": result.data_quality.get("symbols_with_data", 0),
            "broker_calls": 0, "live_orders": 0,
        }
        _atomic_json(folder / "session.json", report)
        _write_csv(folder / "signals.csv", [asdict(signal) for signal in signals])
        _write_csv(folder / "orders.csv", [asdict(order) for order in orders])
        _write_csv(folder / "fills.csv", [asdict(order) for order in fills])
        _write_csv(folder / "positions.csv", positions)
        _write_csv(folder / "trades.csv", [asdict(trade) for trade in trades])
        _write_csv(folder / "risk_events.csv", risk_events)
        _write_csv(folder / "equity_curve.csv", [asdict(point) for point in engine.equity_curve
                                                  if point.timestamp.astimezone(IST).date() == result.trading_date])
        quality = {**result.data_quality, "validation_status": "VALID", "processing_status": "COMPLETED"}
        _atomic_json(folder / "data_quality.json", quality)
        return folder


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = sorted({key for row in rows for key in row})
    if not headers:
        headers = {
            "signals.csv": ["signal_id", "timestamp", "valid_until", "symbol", "exchange", "series", "side", "strategy", "timeframe", "reference_price", "stop_reference", "target_reference", "confidence_metadata", "reason_codes", "setup_id"],
            "orders.csv": ["order_id", "signal_id", "timestamp", "symbol", "side", "quantity", "requested_price", "filled_price", "status", "rejection_reason"],
            "fills.csv": ["order_id", "signal_id", "timestamp", "filled_at", "symbol", "side", "quantity", "filled_price", "status"],
            "positions.csv": ["position_id", "symbol", "status", "quantity", "average_entry_price", "current_price", "opened_at"],
            "trades.csv": ["trade_id", "position_id", "symbol", "entry_time", "exit_time", "quantity", "net_pnl", "exit_reason"],
            "risk_events.csv": ["event_id", "timestamp", "code", "state", "detail", "symbol", "signal_id"],
            "equity_curve.csv": ["timestamp", "cash", "equity", "drawdown", "open_positions"],
        }.get(path.name, [])
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", newline="", encoding="utf-8") as stream:
            if headers:
                writer = csv.DictWriter(stream, fieldnames=headers, extrasaction="ignore")
                writer.writeheader()
                for row in rows:
                    writer.writerow({key: json.dumps(value, default=str, ensure_ascii=False) if isinstance(value, (dict, list, tuple)) else value
                                     for key, value in row.items()})
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            pass
    except OSError:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def synthetic_test(ledger_path: str | Path | None = None) -> dict[str, Any]:
    """Run the known software-only Phase 4B → Phase 4C fixture in temp storage."""
    import tempfile as tempfile_module

    if ledger_path is not None:
        target = Path(ledger_path).resolve()
        temp_roots = {Path(tempfile.gettempdir()).resolve(), Path("/private/tmp").resolve()}
        if not any(target == root or root in target.parents for root in temp_roots):
            raise Phase4DError("SYNTHETIC_LEDGER_MUST_BE_TEMPORARY", "synthetic mode accepts temporary paths only")
        target.parent.mkdir(parents=True, exist_ok=True)
        return _run_synthetic(target)
    with tempfile_module.TemporaryDirectory(prefix="phase4d-synthetic-") as temporary:
        return _run_synthetic(Path(temporary) / "ledger.json")


def _run_synthetic(ledger_path: Path) -> dict[str, Any]:
    from datetime import time as dtime

    day = date(2026, 9, 28)
    symbols = ["RELIANCE"] + [f"SYM{index:03}" for index in range(99)]
    scanner = IntradayScanner(symbols, config=IntradayScannerConfig(enabled=True, universe_size=100, timeframes=("1m",)))
    bars: list[IntradayBar] = []
    for offset in range(60):
        stamp = datetime.combine(day, dtime(9, 15), IST) + timedelta(minutes=offset)
        price = 100.0 + offset * 0.5
        o, h, l, c = ((109.59, 113, 107, 109.59) if offset == 20 else (price, price + 1, price - 1, price))
        bars.append(IntradayBar("RELIANCE", "NSE", "EQ", "1m", stamp, o, h, l, c, 100.0,
                                "CLOSED", "SYNTHETIC_TEST_DATA", "BAR"))
    config = Phase4CPaperConfig.from_env(explicit_paper_command=True)
    engine = PaperExecutionEngine(config, ledger_path=ledger_path)
    replay = IntradayPaperReplay(scanner, engine)

    class Provider:
        provider_name = "SYNTHETIC_TEST_FIXTURE"

        def iter_bars(self, _symbols, _timeframes, *, start=None, end=None):
            yield from bars

    result = replay.run(Provider(), speed="max", sectors={"RELIANCE": "SYNTHETIC_TEST_SECTOR"})
    if (result["signals_processed"] != 2 or result["orders"] != 3 or result["fills"] != 2
            or result["trades"] != 1 or result["portfolio"]["live_trading_enabled"]):
        raise Phase4DError("SYNTHETIC_TEST_FAILED", "Phase 4B → Phase 4C fixture did not satisfy its software assertions")
    reloaded = PaperExecutionEngine(config, ledger_path=ledger_path)
    if len(reloaded.all_orders) != 3 or len(reloaded.all_trades) != 1:
        raise Phase4DError("SYNTHETIC_TEST_FAILED", "temporary paper ledger failed restart/reload validation")
    return {"status": "SYNTHETIC_TEST_PASS", "mode": "SYNTHETIC_TEST", "signals": result["signals_processed"],
            "orders": result["orders"], "fills": result["fills"], "trades": result["trades"],
            "temporary_ledger": str(ledger_path), "live_orders": 0, "broker_calls": 0,
            "performance_metrics": "NOT_CALCULATED"}


def preflight_text(result: PreflightResult) -> str:
    q = result.data_quality
    lines = [
        "PHASE 4D PRECHECK", "", "Execution mode: PAPER", "Live trading: DISABLED",
        "Broker: NOT CONFIGURED", f"Trading date: {result.trading_date.isoformat()}",
        f"NSE session: {'YES' if result.trading_date.weekday() < 5 and result.trading_date not in default_nse_calendar().holidays else 'NO'}",
        f"Intraday data: {'AVAILABLE' if q.get('bars_received') else 'UNAVAILABLE'}",
        f"1m: {'AVAILABLE' if '1m' in q.get('available_timeframes', []) else 'UNAVAILABLE'}",
        f"5m: {'AVAILABLE' if '5m' in q.get('available_timeframes', []) else 'UNAVAILABLE'}",
        f"Universe: {result.universe_size}", f"Coverage: {q.get('coverage_percent', 0):.1%}",
        f"Result: {result.status}",
    ]
    lines.extend(f"- {reason}" for reason in result.reasons)
    lines.extend(("", "Orders: 0", "Fills: 0", "Ledger mutations: 0"))
    return "\n".join(lines)
