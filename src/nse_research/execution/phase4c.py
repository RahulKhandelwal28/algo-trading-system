"""Fail-closed paper-only execution, risk, ledger, and replay for Phase 4C.

This module intentionally imports no broker or provider adapter. It consumes
the normalized Phase 4B signal/bar contracts and structural quote fields.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import re
import tempfile
from collections import defaultdict
from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from time import sleep
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from nse_research.backtest.costs import CostModel
from nse_research.config import CostSettings, RiskSettings
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.intraday.indicators import IntradayIndicatorState
from nse_research.intraday.model import (
    IST,
    IntradayBar,
    IntradayBarStore,
    IntradayDataProvider,
    default_nse_calendar,
    validate_intraday_bar,
)
from nse_research.intraday.scanner import IntradayScanner
from nse_research.risk.controls import position_size

from .phase4c_models import (
    EquityPoint,
    ExecutionMode,
    OrderStatus,
    OrderType,
    PaperOrder,
    PaperPosition,
    PaperQuote,
    PaperRiskEvent,
    PaperTrade,
    Phase4CPaperConfig,
    RejectCode,
    RiskState,
    SignalEnvelope,
)


_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")
_DEFAULT_LEDGER_PATH = Path(__file__).resolve().parents[3] / "data" / "paper_trading" / "phase4c" / "ledger.json"
_ALLOWED_TRANSITIONS = {
    OrderStatus.CREATED.value: {OrderStatus.VALIDATED.value, OrderStatus.REJECTED.value,
                                OrderStatus.CANCELLED.value, OrderStatus.EXPIRED.value},
    OrderStatus.VALIDATED.value: {OrderStatus.PENDING.value, OrderStatus.REJECTED.value,
                                  OrderStatus.CANCELLED.value, OrderStatus.EXPIRED.value},
    OrderStatus.PENDING.value: {OrderStatus.PARTIALLY_FILLED.value, OrderStatus.FILLED.value,
                                OrderStatus.REJECTED.value, OrderStatus.CANCELLED.value,
                                OrderStatus.EXPIRED.value},
    OrderStatus.PARTIALLY_FILLED.value: {OrderStatus.PARTIALLY_FILLED.value, OrderStatus.FILLED.value,
                                         OrderStatus.CANCELLED.value, OrderStatus.EXPIRED.value},
    OrderStatus.REJECTED.value: set(),
    OrderStatus.FILLED.value: set(),
    OrderStatus.CANCELLED.value: set(),
    OrderStatus.EXPIRED.value: set(),
}


class PaperExecutionError(RuntimeError):
    pass


class PaperStateCorrupted(PaperExecutionError):
    """Persisted state failed schema, hash-chain, or accounting validation."""


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, (set, tuple)):
        return list(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default,
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _parse_time(value: Any, *, field_name: str = "timestamp") -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} is malformed") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return result.astimezone(IST)


def _finite_positive(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return math.isfinite(number) and number > 0


def _safe_metadata(values: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep ordinary provenance and omit likely credentials/secrets."""
    secret_words = ("secret", "token", "password", "api_key", "authorization")

    def sanitize(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): sanitize(item)
                for key, item in value.items()
                if not any(word in str(key).lower() for word in secret_words)
            }
        if isinstance(value, (list, tuple)):
            return [sanitize(item) for item in value]
        if isinstance(value, set):
            return sorted((sanitize(item) for item in value), key=repr)
        try:
            json.dumps(value, default=_json_default, allow_nan=False)
        except (TypeError, ValueError):
            return str(value)
        return value

    return sanitize(values or {})


def transition_order(order: PaperOrder, next_status: str | OrderStatus, *, rejection_reason: str | None = None) -> PaperOrder:
    """Validate one deterministic state transition without mutating the input."""
    target = next_status.value if isinstance(next_status, OrderStatus) else str(next_status).upper()
    allowed = _ALLOWED_TRANSITIONS.get(order.status, set())
    if target not in allowed:
        raise PaperExecutionError(f"invalid paper-order transition {order.status} -> {target}")
    return replace(order, status=target, rejection_reason=rejection_reason)


def verify_paper_ledger_document(document: Mapping[str, Any]) -> None:
    """Validate the append-only event hash chain and atomic state snapshot."""
    if not isinstance(document, Mapping):
        raise PaperStateCorrupted("Phase 4C ledger root must be a JSON object")
    if document.get("schema_version") != 1 or document.get("schema_id") not in (None, "phase4c_v1"):
        raise PaperStateCorrupted("LEDGER_SCHEMA_UNSUPPORTED: unsupported Phase 4C ledger schema")
    events = document.get("events")
    state = document.get("state")
    if not isinstance(events, list) or not isinstance(state, dict):
        raise PaperStateCorrupted("Phase 4C ledger has invalid event/state containers")
    previous = "0" * 64
    for index, event in enumerate(events):
        if not isinstance(event, dict) or event.get("sequence") != index + 1:
            raise PaperStateCorrupted("Phase 4C ledger event sequence is invalid")
        if event.get("previous_hash") != previous:
            raise PaperStateCorrupted("Phase 4C ledger hash chain is broken")
        material = {key: value for key, value in event.items() if key != "event_hash"}
        actual = _sha256(material)
        if event.get("event_hash") != actual:
            raise PaperStateCorrupted("Phase 4C ledger event checksum does not match")
        previous = actual
    if document.get("last_event_hash") != previous:
        raise PaperStateCorrupted("Phase 4C ledger head checksum does not match")
    if document.get("state_hash") != _sha256(state):
        raise PaperStateCorrupted("Phase 4C ledger state checksum does not match")
    if document.get("schema_id") == "phase4c_v1":
        try:
            projection = {
                "initial_capital": state["config"]["initial_capital"],
                "cash": state["cash"],
                "positions": state["positions"],
                "orders": state["orders"],
                "trades": state["trades"],
                "equity_history": state["equity_curve"],
                "risk_state": state["portfolio_metrics"]["risk_state"],
                "session_state": state["session_state"],
            }
            if any(document.get(key) != value for key, value in projection.items()):
                raise PaperStateCorrupted("Phase 4C ledger projection fields do not match its state snapshot")
            _parse_time(document["created_at"], field_name="created_at")
            _parse_time(document["updated_at"], field_name="updated_at")
        except (KeyError, TypeError, ValueError) as exc:
            raise PaperStateCorrupted(f"Phase 4C v1 ledger metadata is invalid: {exc}") from exc
    document_hash = document.get("document_hash")
    if document_hash is not None:
        material = {key: value for key, value in document.items() if key != "document_hash"}
        if document_hash != _sha256(material):
            raise PaperStateCorrupted("Phase 4C ledger document checksum does not match")
    elif document.get("schema_id") == "phase4c_v1":
        raise PaperStateCorrupted("Phase 4C v1 ledger is missing its document checksum")


def inspect_paper_ledger(path: str | Path) -> dict[str, Any]:
    """Read-only dashboard helper; it never repairs or creates a ledger."""
    target = Path(path)
    if not target.is_file():
        return {"status": "DATA_UNAVAILABLE", "path": str(target)}
    try:
        document = json.loads(target.read_text(encoding="utf-8"))
        verify_paper_ledger_document(document)
    except (OSError, UnicodeError, json.JSONDecodeError, PaperStateCorrupted, TypeError, ValueError) as exc:
        return {"status": "PAPER_STATE_CORRUPTED", "path": str(target), "reason": str(exc)}
    state = document["state"]
    return {"status": "OK", "path": str(target), "state": state,
            "event_count": len(document["events"]), "last_event_hash": document["last_event_hash"]}


class Phase4CRiskEngine:
    """Uses the project's sizing helper and cost model to cap planned risk."""

    def __init__(self, config: Phase4CPaperConfig, cost_model: CostModel | None) -> None:
        self.config = config
        self.cost_model = cost_model
        self.settings = RiskSettings(
            risk_per_trade=config.risk_per_trade,
            max_daily_loss=config.max_daily_loss,
            max_portfolio_drawdown=config.max_portfolio_drawdown,
            max_open_positions=config.max_positions,
            max_single_position_fraction=config.max_symbol_exposure,
            max_gross_exposure_fraction=config.max_gross_exposure,
            max_sector_exposure_fraction=config.max_sector_exposure,
            allow_short_intraday=False,
        )

    def _slipped(self, price: float, direction: int, is_entry: bool) -> float:
        if self.cost_model is None:
            raise ValueError(RejectCode.COST_RATE_UNCONFIGURED.value)
        mode = self.config.slippage_mode
        value = self.config.slippage_value
        if mode == "basis_points" and value is None:
            return self.cost_model.slippage_price(price, direction, is_entry)
        adverse_sign = direction if is_entry else -direction
        if mode == "fixed":
            return price + adverse_sign * float(value or 0.0)
        if mode == "basis_points":
            fraction = (float(value) if value is not None else self.cost_model.settings.slippage_bps) / 10_000.0
        else:
            fraction = (float(value) / 100.0 if value is not None
                        else self.cost_model.settings.slippage_bps / 10_000.0)
        return price * (1.0 + adverse_sign * fraction)

    def _loss_at_stop(self, entry: float, stop: float, quantity: int) -> tuple[float, float]:
        assert self.cost_model is not None
        buy_fill = self._slipped(entry, 1, True)
        stop_fill = self._slipped(stop, 1, False)
        fees = self.cost_model.order_cost(buy_fill, quantity, "buy", True).total
        fees += self.cost_model.order_cost(stop_fill, quantity, "sell", True, is_exit=True).total
        slippage = max(0.0, buy_fill - entry) * quantity + max(0.0, stop - stop_fill) * quantity
        return max(0.0, buy_fill - stop_fill) * quantity + fees, slippage

    def size(
        self,
        *,
        equity: float,
        cash_available: float,
        exposure_available: float,
        entry: float,
        stop: float,
        lot_size: int = 1,
    ) -> tuple[int, float, float, str | None]:
        if self.cost_model is None:
            return 0, equity * self.config.risk_per_trade, 0.0, RejectCode.COST_RATE_UNCONFIGURED.value
        if lot_size < 1 or not _finite_positive(entry) or not _finite_positive(stop) or entry <= stop:
            return 0, equity * self.config.risk_per_trade, 0.0, RejectCode.INVALID_QUANTITY.value
        budget = equity * self.config.risk_per_trade
        sizing = position_size(
            equity=equity,
            entry=entry,
            stop_distance=entry - stop,
            risk=self.settings,
            available_exposure=min(exposure_available, cash_available),
        )
        maximum = sizing.quantity - sizing.quantity % lot_size
        if maximum <= 0:
            return 0, budget, 0.0, RejectCode.INSUFFICIENT_CAPITAL.value

        def affordable(quantity: int) -> bool:
            entry_fill = self._slipped(entry, 1, True)
            entry_fees = self.cost_model.order_cost(entry_fill, quantity, "buy", True).total
            planned_loss, _ = self._loss_at_stop(entry, stop, quantity)
            return (entry_fill * quantity <= exposure_available + 1e-9
                    and entry_fill * quantity + entry_fees <= cash_available + 1e-9
                    and planned_loss <= budget + 1e-9)

        low_lots, high_lots, best = 1, maximum // lot_size, 0
        while low_lots <= high_lots:
            middle_lots = (low_lots + high_lots) // 2
            middle = middle_lots * lot_size
            if affordable(middle):
                best = middle
                low_lots = middle_lots + 1
            else:
                high_lots = middle_lots - 1
        if best <= 0:
            return 0, budget, 0.0, RejectCode.INVALID_QUANTITY.value
        planned, _ = self._loss_at_stop(entry, stop, best)
        return best, budget, planned, None


class PaperExecutionEngine:
    """Chronological paper order and position manager; no broker execution port."""

    def __init__(
        self,
        config: Phase4CPaperConfig | None = None,
        *,
        cost_settings: CostSettings | None = None,
        calendar: NseTradingCalendar | None = None,
        ledger_path: str | Path | None = _DEFAULT_LEDGER_PATH,
    ) -> None:
        self.config = config or Phase4CPaperConfig()
        self.cost_settings = cost_settings or CostSettings()
        self.cost_model = CostModel(self.cost_settings)
        self.risk_engine = Phase4CRiskEngine(self.config, self.cost_model)
        self.calendar = calendar or default_nse_calendar()
        self.bar_store = IntradayBarStore(calendar=self.calendar)
        self.ledger_path = Path(ledger_path) if ledger_path is not None else None
        if self.ledger_path is not None:
            if self.ledger_path.suffix.lower() != ".json" or any(
                token in part.lower() for part in self.ledger_path.parts for token in ("phase3c", "phase3d")
            ):
                raise ValueError("Phase 4C ledger must be a separate .json file outside Phase 3C/3D paths")
        self.orders: dict[str, PaperOrder] = {}
        self.positions: dict[str, PaperPosition] = {}
        self.trades: dict[str, PaperTrade] = {}
        self.risk_events: list[PaperRiskEvent] = []
        self.equity_curve: list[EquityPoint] = []
        self._events: list[dict[str, Any]] = []
        self._signal_fingerprints: dict[str, str] = {}
        self._halts: dict[str, str] = {}
        self._last_data_quality_halt: datetime | None = None
        self._last_valid_market_time: datetime | None = None
        self._last_event_time: datetime | None = None
        self._session_date: date | None = None
        self._session_state: dict[str, Any] = {
            "active_session": None,
            "completed_session_ids": [],
            "last_session": None,
        }
        self._processed_bar_event_ids: set[str] = set()
        self._ledger_created_at = datetime.now(IST).isoformat()
        self._daily_start_equity = self.config.initial_capital
        self._cash = self.config.initial_capital
        self._peak_equity = self.config.initial_capital
        self._market_marks: dict[str, dict[str, Any]] = {}
        self._indicator_states: dict[tuple[str, str], IntradayIndicatorState] = {}
        self._committed_state = self._state_payload()
        self._committed_events: list[dict[str, Any]] = []
        if self.ledger_path is not None and self.ledger_path.is_file():
            self._load_ledger()

    @property
    def events(self) -> tuple[dict[str, Any], ...]:
        return tuple(copy.deepcopy(self._events))

    @property
    def risk_state(self) -> RiskState:
        for value in (RiskState.MANUAL_HALT.value, RiskState.DATA_QUALITY_HALT.value,
                      RiskState.PORTFOLIO_DRAWDOWN_HALT.value, RiskState.DAILY_LOSS_HALT.value):
            if value in self._halts:
                return RiskState(value)
        return RiskState.ACTIVE

    @property
    def open_positions(self) -> list[PaperPosition]:
        return sorted((item for item in self.positions.values() if item.status == "OPEN"),
                      key=lambda item: (item.opened_at, item.symbol, item.position_id))

    @property
    def all_orders(self) -> list[PaperOrder]:
        return sorted(self.orders.values(), key=lambda item: (item.timestamp, item.symbol, item.order_id))

    @property
    def all_trades(self) -> list[PaperTrade]:
        return sorted(self.trades.values(), key=lambda item: (item.exit_time, item.symbol, item.trade_id))

    def _risk_metrics(self) -> dict[str, Any]:
        liquidation_value = 0.0
        unrealized = 0.0
        exposure = 0.0
        reserved = 0.0
        symbol_exposures: dict[str, float] = defaultdict(float)
        sector_exposures: dict[str, float] = defaultdict(float)
        for position in self.open_positions:
            exposure += position.current_price * position.quantity
            symbol_exposures[position.symbol] += position.current_price * position.quantity
            if position.sector:
                sector_exposures[position.sector] += position.current_price * position.quantity
            reserved += position.average_entry_price * position.quantity
            exit_price = self._slipped(position.current_price, entry=False)
            exit_fees = self.cost_model.order_cost(exit_price, position.quantity, "sell", True, is_exit=True).total if self.cost_model else 0.0
            proceeds = exit_price * position.quantity - exit_fees
            liquidation_value += proceeds
            unrealized += proceeds - position.average_entry_price * position.quantity - position.entry_fees
        for order in self.orders.values():
            if order.status == OrderStatus.PENDING.value:
                notional = order.requested_price * order.quantity
                reserved += notional
                exposure += notional
                symbol_exposures[order.symbol] += notional
                if order.sector:
                    sector_exposures[order.sector] += notional
        equity = self._cash + liquidation_value
        peak = max(self._peak_equity, equity)
        drawdown = max(0.0, (peak - equity) / peak) if peak > 0 else 0.0
        realized = sum(trade.net_pnl for trade in self.trades.values())
        costs = sum(trade.total_cost for trade in self.trades.values())
        for position in self.open_positions:
            exit_price = self._slipped(position.current_price, entry=False)
            exit_fees = self.cost_model.order_cost(exit_price, position.quantity, "sell", True, is_exit=True).total if self.cost_model else 0.0
            exit_slippage = max(0.0, position.current_price - exit_price) * position.quantity
            costs += position.entry_fees + position.entry_slippage + exit_fees + exit_slippage
        return {
            "initial_capital": self.config.initial_capital,
            "cash": self._cash,
            "available_cash": self._cash - sum(self._pending_reserve(order) for order in self._active_pending_orders()),
            "reserved_capital": reserved,
            "used_capital": reserved,
            "gross_exposure": exposure,
            "net_exposure": exposure,
            "symbol_exposures": dict(sorted(symbol_exposures.items())),
            "sector_exposures": dict(sorted(sector_exposures.items())),
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "today_pnl": equity - self._daily_start_equity,
            "total_costs": costs,
            "total_equity": equity,
            "peak_equity": peak,
            "drawdown": drawdown,
            "start_of_day_equity": self._daily_start_equity,
            "daily_loss_limit_amount": self._daily_start_equity * self.config.max_daily_loss,
            "max_portfolio_drawdown": self.config.max_portfolio_drawdown,
            "open_positions": len(self.open_positions),
            "pending_orders": sum(order.status == OrderStatus.PENDING.value for order in self.orders.values()),
            "risk_state": self.risk_state.value,
            "halt_reasons": dict(self._halts),
        }

    def portfolio_snapshot(self, timestamp: datetime | None = None) -> dict[str, Any]:
        values = self._risk_metrics()
        values.update({
            "execution_mode": self.config.execution_mode.value,
            "paper_trading_enabled": self.config.enabled,
            "realtime_enabled": self.config.realtime_enabled,
            "live_trading_enabled": self.config.live_trading_enabled,
            "max_positions": self.config.max_positions,
            "positions": [asdict(item) for item in self.open_positions],
            "orders": [asdict(item) for item in self.all_orders],
            "trades": [asdict(item) for item in self.all_trades],
            "risk_events": [asdict(item) for item in self.risk_events],
            "equity_curve": [asdict(item) for item in self.equity_curve],
            "timestamp": timestamp or self._last_event_time,
        })
        return values

    def _slipped(self, price: float, *, entry: bool) -> float:
        return self.risk_engine._slipped(price, 1, entry)

    def _pending_reserve(self, order: PaperOrder) -> float:
        if order.order_type == OrderType.LIMIT.value:
            estimated_fill = order.requested_price
        else:
            estimated_fill = self._slipped(order.requested_price, entry=True)
        fees = (self.cost_model.order_cost(estimated_fill, order.quantity, "buy", True).total
                if self.cost_model is not None else 0.0)
        return estimated_fill * order.quantity + fees

    def _state_payload(self) -> dict[str, Any]:
        return {
            "config": {
                "execution_mode": self.config.execution_mode.value,
                "paper_trading_enabled": self.config.enabled,
                "initial_capital": self.config.initial_capital,
                "risk_per_trade": self.config.risk_per_trade,
                "max_positions": self.config.max_positions,
                "max_daily_loss": self.config.max_daily_loss,
                "max_portfolio_drawdown": self.config.max_portfolio_drawdown,
                "max_symbol_exposure": self.config.max_symbol_exposure,
                "max_sector_exposure": self.config.max_sector_exposure,
                "max_gross_exposure": self.config.max_gross_exposure,
                "force_intraday_exit": self.config.force_intraday_exit,
                "trailing_atr_multiple": self.config.trailing_atr_multiple,
                "signal_max_age_seconds": self.config.signal_max_age_seconds,
                "quote_max_age_seconds": self.config.quote_max_age_seconds,
                "slippage_mode": self.config.slippage_mode,
                "slippage_value": self.config.slippage_value,
                "realtime_enabled": self.config.realtime_enabled,
                "live_trading_enabled": self.config.live_trading_enabled,
                "allow_duplicate_symbol_positions": self.config.allow_duplicate_symbol_positions,
                "cost_settings": self.cost_settings.model_dump(mode="json"),
            },
            "cash": self._cash,
            "peak_equity": self._peak_equity,
            "daily_start_equity": self._daily_start_equity,
            "session_date": self._session_date.isoformat() if self._session_date else None,
            "session_state": copy.deepcopy(self._session_state),
            "last_event_time": self._last_event_time.isoformat() if self._last_event_time else None,
            "last_valid_market_time": self._last_valid_market_time.isoformat() if self._last_valid_market_time else None,
            "last_data_quality_halt": self._last_data_quality_halt.isoformat() if self._last_data_quality_halt else None,
            "halts": dict(self._halts),
            "market_marks": copy.deepcopy(self._market_marks),
            "signal_fingerprints": dict(self._signal_fingerprints),
            "orders": [asdict(item) for item in self.all_orders],
            "positions": [asdict(item) for item in sorted(self.positions.values(), key=lambda x: x.position_id)],
            "trades": [asdict(item) for item in self.all_trades],
            "risk_events": [asdict(item) for item in self.risk_events],
            "equity_curve": [asdict(item) for item in self.equity_curve],
            "portfolio_metrics": self._risk_metrics(),
        }

    def _restore_state(self, state: Mapping[str, Any]) -> None:
        try:
            self._cash = float(state["cash"])
            self._peak_equity = float(state["peak_equity"])
            self._daily_start_equity = float(state["daily_start_equity"])
            self._session_date = date.fromisoformat(state["session_date"]) if state.get("session_date") else None
            session_state = state.get("session_state")
            self._session_state = copy.deepcopy(session_state) if isinstance(session_state, dict) else {
                "active_session": None, "completed_session_ids": [], "last_session": None,
            }
            self._last_event_time = _parse_time(state["last_event_time"]) if state.get("last_event_time") else None
            self._last_valid_market_time = _parse_time(state["last_valid_market_time"]) if state.get("last_valid_market_time") else None
            self._last_data_quality_halt = _parse_time(state["last_data_quality_halt"]) if state.get("last_data_quality_halt") else None
            self._halts = {str(k): str(v) for k, v in state.get("halts", {}).items()}
            self._market_marks = copy.deepcopy(state.get("market_marks", {}))
            self._signal_fingerprints = {str(k): str(v) for k, v in state.get("signal_fingerprints", {}).items()}
            self.orders = {item["order_id"]: PaperOrder(**self._decode_datetimes(item, {"timestamp", "signal_timestamp", "signal_valid_until", "filled_at"}))
                           for item in state.get("orders", [])}
            self.positions = {item["position_id"]: PaperPosition(**self._decode_datetimes(item, {"opened_at"}))
                              for item in state.get("positions", [])}
            self.trades = {item["trade_id"]: PaperTrade(**self._decode_datetimes(item, {"entry_time", "exit_time"}))
                           for item in state.get("trades", [])}
            self.risk_events = [PaperRiskEvent(**self._decode_datetimes(item, {"timestamp"}))
                                for item in state.get("risk_events", [])]
            self.equity_curve = [EquityPoint(**self._decode_datetimes(item, {"timestamp"}))
                                 for item in state.get("equity_curve", [])]
            self._validate_invariants()
        except PaperStateCorrupted:
            raise
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise PaperStateCorrupted(f"Phase 4C state snapshot cannot be restored: {exc}") from exc

    @staticmethod
    def _decode_datetimes(values: Mapping[str, Any], fields: set[str]) -> dict[str, Any]:
        result = dict(values)
        for field_name in fields:
            if result.get(field_name) is not None:
                result[field_name] = _parse_time(result[field_name], field_name=field_name)
        return result

    def _validate_invariants(self) -> None:
        if not math.isfinite(self._cash) or self._cash < -1e-6:
            raise PaperStateCorrupted("cash invariant failed")
        if not math.isfinite(self._peak_equity) or self._peak_equity <= 0:
            raise PaperStateCorrupted("peak-equity invariant failed")
        if any(pos.status == "OPEN" and pos.quantity <= 0 for pos in self.positions.values()):
            raise PaperStateCorrupted("open position quantity invariant failed")
        if sum(pos.status == "OPEN" for pos in self.positions.values()) > self.config.max_positions:
            raise PaperStateCorrupted("open-position limit invariant failed")
        if not self.config.allow_duplicate_symbol_positions:
            symbols = [pos.symbol for pos in self.positions.values() if pos.status == "OPEN"]
            if len(symbols) != len(set(symbols)):
                raise PaperStateCorrupted("duplicate open symbol invariant failed")
        for order in self.orders.values():
            if order.status == OrderStatus.FILLED.value and order.quantity <= 0:
                raise PaperStateCorrupted("filled order quantity invariant failed")
            if order.status == OrderStatus.FILLED.value and order.filled_price is None:
                raise PaperStateCorrupted("filled order price invariant failed")
        if self._active_position_count() > self.config.max_positions:
            raise PaperStateCorrupted("active position/order limit invariant failed")

    def _load_ledger(self) -> None:
        assert self.ledger_path is not None
        try:
            document = json.loads(self.ledger_path.read_text(encoding="utf-8"))
            verify_paper_ledger_document(document)
            if document["state"].get("config") != self._state_payload().get("config"):
                raise PaperStateCorrupted("Phase 4C ledger configuration differs from runtime risk configuration")
            self._restore_state(document["state"])
            self._ledger_created_at = str(document.get("created_at") or self._ledger_created_at)
        except PaperStateCorrupted:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise PaperStateCorrupted(f"Phase 4C ledger cannot be read: {exc}") from exc
        self._events = copy.deepcopy(document["events"])
        self._processed_bar_event_ids = {
            str(event.get("payload", {}).get("bar_event_id"))
            for event in self._events
            if event.get("event_type") == "MARKET_BAR_PROCESSED"
            and event.get("payload", {}).get("bar_event_id")
        }
        self._committed_state = self._state_payload()
        self._committed_events = copy.deepcopy(self._events)

    def _audit_summary(self) -> dict[str, Any]:
        metrics = self._risk_metrics()
        return {key: metrics[key] for key in (
            "cash", "total_equity", "realized_pnl", "unrealized_pnl", "drawdown",
            "open_positions", "pending_orders", "risk_state",
        )}

    def _record_event(
        self,
        event_type: str,
        at: datetime,
        *,
        before: Mapping[str, Any],
        reason: str | None = None,
        order_id: str | None = None,
        position_id: str | None = None,
        trade_id: str | None = None,
        signal_id: str | None = None,
        payload: Mapping[str, Any] | None = None,
    ) -> str:
        timestamp = _parse_time(at)
        previous = self._events[-1]["event_hash"] if self._events else "0" * 64
        sequence = len(self._events) + 1
        core = {
            "sequence": sequence,
            "event_id": f"EVT-{_sha256([sequence, event_type, timestamp, signal_id, order_id, position_id, trade_id])[:24]}",
            "timestamp": timestamp.isoformat(),
            "event_type": event_type,
            "order_id": order_id,
            "position_id": position_id,
            "trade_id": trade_id,
            "signal_id": signal_id,
            "reason": reason,
            "before_state": copy.deepcopy(dict(before)),
            "after_state": self._audit_summary(),
            "payload": _safe_metadata(payload),
            "previous_hash": previous,
        }
        event = {**core, "event_hash": _sha256(core)}
        self._events.append(event)
        try:
            self._persist_ledger()
        except (OSError, TypeError, ValueError):
            self._events = copy.deepcopy(self._committed_events)
            self._restore_state(self._committed_state)
            raise
        if event_type == "MARKET_BAR_PROCESSED" and payload and payload.get("bar_event_id"):
            self._processed_bar_event_ids.add(str(payload["bar_event_id"]))
        return event["event_id"]

    def initialize_ledger(self) -> bool:
        """Atomically create an empty persistent ledger, without replacing one.

        Returns ``True`` only when this call created the file. Existing files
        are left byte-for-byte unchanged and were already validated by the
        constructor before this method can be reached.
        """
        if self.ledger_path is None:
            raise PaperExecutionError("persistent ledger path is not configured")
        if self.ledger_path.exists():
            return False
        self._persist_ledger()
        return True

    @property
    def session_state(self) -> dict[str, Any]:
        return copy.deepcopy(self._session_state)

    def set_session_state(self, session_state: Mapping[str, Any], *, at: datetime) -> None:
        """Persist a sanitized Phase 4D lifecycle snapshot in the Phase 4C chain."""
        clean = _safe_metadata(session_state)
        if not isinstance(clean, dict):
            raise ValueError("session state must be a JSON object")
        if clean == self._session_state:
            return
        before = self._audit_summary()
        previous = self._session_state
        self._session_state = clean
        try:
            self._record_event("SESSION_STATE_UPDATED", at, before=before,
                               reason=str(clean.get("status") or "SESSION_STATE_UPDATED"),
                               payload={"session_state": clean})
        except Exception:
            self._session_state = previous
            raise

    def _persist_ledger(self) -> None:
        current = self._state_payload()
        if self.ledger_path is not None:
            self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
            previous = self._events[-1]["event_hash"] if self._events else "0" * 64
            document = {
                "schema_version": 1,
                "schema_id": "phase4c_v1",
                "created_at": self._ledger_created_at,
                "updated_at": datetime.now(IST).isoformat(),
                "initial_capital": self.config.initial_capital,
                "cash": current["cash"],
                "positions": current["positions"],
                "orders": current["orders"],
                "trades": current["trades"],
                "equity_history": current["equity_curve"],
                "risk_state": current["portfolio_metrics"]["risk_state"],
                "session_state": current["session_state"],
                "events": self._events,
                "last_event_hash": previous,
                "state": current,
                "state_hash": _sha256(current),
            }
            document["document_hash"] = _sha256(document)
            encoded = json.dumps(document, ensure_ascii=False, indent=2, default=_json_default, allow_nan=False) + "\n"
            file_descriptor, temp_name = tempfile.mkstemp(prefix=".phase4c-ledger-", suffix=".tmp", dir=self.ledger_path.parent)
            try:
                with os.fdopen(file_descriptor, "w", encoding="utf-8") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temp_name, self.ledger_path)
                try:
                    directory_fd = os.open(self.ledger_path.parent, os.O_RDONLY)
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                except OSError:
                    pass
            except OSError:
                try:
                    os.unlink(temp_name)
                except OSError:
                    pass
                raise
        self._committed_state = copy.deepcopy(current)
        self._committed_events = copy.deepcopy(self._events)

    def _add_risk_event(self, code: str, detail: str, at: datetime, *, symbol: str | None = None,
                        signal_id: str | None = None) -> str:
        before = self._audit_summary()
        timestamp = _parse_time(at)
        next_index = len(self.risk_events) + 1
        event_id = f"RISK-{_sha256([next_index, timestamp, code, symbol, signal_id])[:20]}"
        event = PaperRiskEvent(event_id, timestamp, code, self.risk_state.value, detail, symbol, signal_id)
        self.risk_events.append(event)
        self._record_event("RISK_EVENT", timestamp, before=before, reason=code, signal_id=signal_id,
                           payload={"detail": detail, "symbol": symbol, "risk_event_id": event_id})
        return event_id

    def _set_halt(self, code: str, detail: str, at: datetime) -> None:
        if code in self._halts:
            return
        before = self._audit_summary()
        self._halts[code] = detail
        if code == RiskState.DATA_QUALITY_HALT.value:
            self._last_data_quality_halt = _parse_time(at)
        self._add_risk_event(code, detail, at)
        self._record_event("RISK_HALT_SET", at, before=before, reason=code, payload={"detail": detail})

    def _update_risk_limits(self, at: datetime) -> None:
        metrics = self._risk_metrics()
        equity = float(metrics["total_equity"])
        if equity > self._peak_equity:
            self._peak_equity = equity
            metrics = self._risk_metrics()
        daily_loss = self._daily_start_equity - float(metrics["total_equity"])
        if daily_loss >= self._daily_start_equity * self.config.max_daily_loss:
            self._set_halt(RiskState.DAILY_LOSS_HALT.value,
                           f"daily loss {daily_loss:.2f} reached configured limit", at)
        drawdown = float(metrics["drawdown"])
        if drawdown >= self.config.max_portfolio_drawdown:
            self._set_halt(RiskState.PORTFOLIO_DRAWDOWN_HALT.value,
                           f"drawdown {drawdown:.6f} reached configured limit", at)

    def _append_equity_point(self, at: datetime) -> None:
        self._update_risk_limits(at)
        metrics = self._risk_metrics()
        equity = float(metrics["total_equity"])
        self._peak_equity = max(self._peak_equity, equity)
        point = EquityPoint(
            timestamp=_parse_time(at), cash=float(metrics["cash"]),
            reserved_capital=float(metrics["reserved_capital"]), realized_pnl=float(metrics["realized_pnl"]),
            unrealized_pnl=float(metrics["unrealized_pnl"]), total_costs=float(metrics["total_costs"]),
            total_equity=equity, peak_equity=self._peak_equity, drawdown=float(metrics["drawdown"]),
            available_cash=float(metrics["available_cash"]), gross_exposure=float(metrics["gross_exposure"]),
            net_exposure=float(metrics["net_exposure"]),
        )
        self.equity_curve.append(point)
        if len(self.equity_curve) > 10_000:
            del self.equity_curve[:-10_000]
        if self.risk_state is not RiskState.ACTIVE:
            self._reject_pending_for_halt(at)

    def _roll_session(self, next_date: date, event_time: datetime) -> None:
        if self._session_date is not None and next_date != self._session_date:
            previous_day = self._session_date
            if self.config.force_intraday_exit:
                close_at = datetime.combine(previous_day, time(15, 30), tzinfo=IST)
                self.close_session(previous_day, close_at, allow_last_available_mark=True)
            before = self._audit_summary()
            self._session_date = next_date
            self._daily_start_equity = float(self._risk_metrics()["total_equity"])
            if RiskState.DAILY_LOSS_HALT.value in self._halts:
                self._halts.pop(RiskState.DAILY_LOSS_HALT.value, None)
                self._add_risk_event("DAILY_LOSS_HALT_CLEARED", "new NSE session; daily loss counter reset", event_time)
            self._record_event("SESSION_STARTED", event_time, before=before,
                               payload={"session_date": next_date.isoformat(), "start_of_day_equity": self._daily_start_equity})
        elif self._session_date is None:
            before = self._audit_summary()
            self._session_date = next_date
            self._daily_start_equity = float(self._risk_metrics()["total_equity"])
            self._record_event("SESSION_STARTED", event_time, before=before,
                               payload={"session_date": next_date.isoformat(), "start_of_day_equity": self._daily_start_equity})

    def _active_pending_orders(self) -> list[PaperOrder]:
        return [order for order in self.orders.values() if order.status == OrderStatus.PENDING.value]

    def _active_position_count(self) -> int:
        return len(self.open_positions) + len(self._active_pending_orders())

    def _reject_pending_for_halt(self, at: datetime) -> None:
        reason = {
            RiskState.DAILY_LOSS_HALT.value: RejectCode.DAILY_LOSS_LIMIT.value,
            RiskState.PORTFOLIO_DRAWDOWN_HALT.value: RejectCode.PORTFOLIO_DRAWDOWN_LIMIT.value,
            RiskState.MANUAL_HALT.value: RejectCode.MANUAL_HALT.value,
            RiskState.DATA_QUALITY_HALT.value: RejectCode.DATA_QUALITY.value,
        }.get(self.risk_state.value, RejectCode.DATA_QUALITY.value)
        for order in self._active_pending_orders():
            before = self._audit_summary()
            self.orders[order.order_id] = transition_order(order, OrderStatus.REJECTED, rejection_reason=reason)
            self._add_risk_event(reason, "pending entry blocked by active risk halt", at,
                                 symbol=order.symbol, signal_id=order.signal_id)
            self._record_event("ORDER_REJECTED", at, before=before, reason=reason, order_id=order.order_id,
                               signal_id=order.signal_id, payload={"detail": "pending order blocked by active halt"})

    def _signal_values(self, signal: Any) -> dict[str, Any]:
        fields = ("signal_id", "timestamp", "valid_until", "symbol", "exchange", "series", "side", "strategy",
                  "timeframe", "reference_price", "stop_reference", "target_reference", "confidence_metadata", "reason_codes")
        return {name: getattr(signal, name, None) for name in fields}

    def _fingerprint(self, values: Mapping[str, Any]) -> str:
        safe = dict(values)
        try:
            return _sha256(safe)
        except (TypeError, ValueError):
            return hashlib.sha256(repr(sorted((str(k), repr(v)) for k, v in safe.items())).encode()).hexdigest()

    def _order_id(self, signal_id: str, fingerprint: str, *, duplicate: bool = False) -> str:
        return f"P4C-{_sha256([signal_id, fingerprint, 'duplicate' if duplicate else 'entry'])[:24]}"

    def _reject_order(
        self, *, values: Mapping[str, Any], fingerprint: str, reason: str, at: datetime, source: str,
        source_metadata: Mapping[str, Any], sector: str | None, order_type: str, limit_price: float | None,
        duplicate: bool = False,
    ) -> PaperOrder:
        signal_id = str(values.get("signal_id") or "")
        order_id = self._order_id(signal_id, fingerprint, duplicate=duplicate)
        if order_id in self.orders:
            return self.orders[order_id]
        timestamp = _parse_time(at)
        try:
            signal_timestamp = _parse_time(values.get("timestamp"), field_name="signal.timestamp")
        except (TypeError, ValueError):
            signal_timestamp = timestamp
        try:
            signal_expiry = _parse_time(values.get("valid_until"), field_name="signal.valid_until")
        except (TypeError, ValueError):
            signal_expiry = timestamp
        order = PaperOrder(
            order_id=order_id, signal_id=signal_id, timestamp=timestamp,
            signal_timestamp=signal_timestamp, signal_valid_until=signal_expiry,
            symbol=str(values.get("symbol") or "UNKNOWN").strip().upper(),
            exchange=str(values.get("exchange") or "UNKNOWN").strip().upper(),
            series=str(values.get("series") or "UNKNOWN").strip().upper(),
            side=("BUY" if str(values.get("side") or "").strip().upper() in {"LONG", "BUY"}
                  else "SELL" if str(values.get("side") or "").strip().upper() in {"SHORT", "SELL"}
                  else "UNKNOWN"),
            order_type=order_type, quantity=0,
            requested_price=float(limit_price if limit_price is not None and _finite_positive(limit_price)
                                  else values.get("reference_price") or 0.0),
            stop_price=float(values.get("stop_reference") or 0.0), target_price=float(values.get("target_reference") or 0.0),
            trailing_stop_config=None, strategy=str(values.get("strategy") or "UNKNOWN"),
            timeframe=str(values.get("timeframe") or "UNKNOWN"), status=OrderStatus.REJECTED.value,
            rejection_reason=reason, source=source, source_metadata=_safe_metadata(source_metadata), sector=sector,
        )
        before = self._audit_summary()
        if signal_id and not duplicate:
            self._signal_fingerprints[signal_id] = fingerprint
        self.orders[order_id] = order
        self._add_risk_event(reason, reason, timestamp, symbol=order.symbol, signal_id=signal_id or None)
        self._record_event("ORDER_REJECTED", timestamp, before=before, reason=reason, order_id=order_id,
                           signal_id=signal_id or None, payload={"source": source, "strategy": order.strategy})
        return order

    def submit_signal(
        self,
        signal: Any | SignalEnvelope,
        *,
        replay_time: datetime,
        source: str | None = None,
        source_metadata: Mapping[str, Any] | None = None,
        sector: str | None = None,
        order_type: str | OrderType = OrderType.MARKET,
        limit_price: float | None = None,
        lot_size: int = 1,
        trailing_atr_multiple: float | None = None,
    ) -> PaperOrder:
        """Validate and queue a paper entry. It can never submit a live order."""
        now = _parse_time(replay_time, field_name="replay_time")
        if isinstance(signal, SignalEnvelope):
            envelope = signal
            signal = envelope.signal
            source = envelope.source
            source_metadata = envelope.source_metadata
        source_text = str(source or "").strip()
        metadata = _safe_metadata(source_metadata)
        sector = str(sector).strip().upper() if sector is not None and str(sector).strip() else None
        values = self._signal_values(signal)
        fingerprint = self._fingerprint(values)
        signal_id = str(values.get("signal_id") or "")
        if signal_id and signal_id in self._signal_fingerprints:
            prior = next((item for item in self.orders.values() if item.signal_id == signal_id), None)
            if self._signal_fingerprints[signal_id] == fingerprint and prior is not None:
                return prior
            return self._reject_order(values=values, fingerprint=fingerprint, reason=RejectCode.DUPLICATE_SIGNAL.value,
                                      at=now, source=source_text or "UNKNOWN", source_metadata=metadata,
                                      sector=sector, order_type=str(order_type.value if isinstance(order_type, OrderType) else order_type).upper(),
                                      limit_price=limit_price, duplicate=True)

        resolved_order_type = order_type.value if isinstance(order_type, OrderType) else str(order_type).upper()
        if resolved_order_type not in {OrderType.MARKET.value, OrderType.LIMIT.value}:
            resolved_order_type = OrderType.MARKET.value
            reason = RejectCode.INVALID_SIGNAL.value
        else:
            reason = None
        if self.config.execution_mode is ExecutionMode.LIVE or self.config.live_trading_enabled:
            reason = RejectCode.LIVE_EXECUTION_NOT_AVAILABLE.value
        elif self.config.realtime_enabled:
            reason = RejectCode.LIVE_EXECUTION_NOT_AVAILABLE.value
        elif not self.config.enabled:
            reason = RejectCode.PAPER_TRADING_DISABLED.value
        elif self.cost_model is None:
            reason = RejectCode.COST_RATE_UNCONFIGURED.value
        elif self.risk_state is not RiskState.ACTIVE:
            reason = {
                RiskState.DAILY_LOSS_HALT: RejectCode.DAILY_LOSS_LIMIT.value,
                RiskState.PORTFOLIO_DRAWDOWN_HALT: RejectCode.PORTFOLIO_DRAWDOWN_LIMIT.value,
                RiskState.MANUAL_HALT: RejectCode.MANUAL_HALT.value,
                RiskState.DATA_QUALITY_HALT: RejectCode.DATA_QUALITY.value,
            }[self.risk_state]

        timestamp = values.get("timestamp")
        valid_until = values.get("valid_until")
        if reason is None:
            if (not signal_id or not source_text or not values.get("symbol") or not values.get("strategy")
                    or str(values.get("exchange") or "").strip().upper() != "NSE"
                    or str(values.get("series") or "").strip().upper() != "EQ"
                    or str(values.get("timeframe") or "").lower() not in {"1m", "5m"}
                    or str(values.get("side") or "").upper() != "LONG"):
                reason = RejectCode.INVALID_SIGNAL.value
        if reason is None:
            try:
                stamp = _parse_time(timestamp, field_name="signal.timestamp")
                expiry = _parse_time(valid_until, field_name="signal.valid_until")
            except (TypeError, ValueError):
                reason = RejectCode.INVALID_SIGNAL.value
            else:
                if now < stamp:
                    reason = RejectCode.FUTURE_TIMESTAMP.value
                elif now - stamp > timedelta(seconds=self.config.signal_max_age_seconds):
                    reason = RejectCode.STALE_SIGNAL.value
                elif now >= expiry:
                    reason = RejectCode.EXPIRED_SIGNAL.value
                elif expiry <= stamp:
                    reason = RejectCode.INVALID_SIGNAL.value
        if reason is None:
            if not all(_finite_positive(values.get(field)) for field in ("reference_price", "stop_reference", "target_reference")):
                reason = RejectCode.INVALID_PRICE.value
            elif not (float(values["stop_reference"]) < float(values["reference_price"]) < float(values["target_reference"])):
                reason = RejectCode.INVALID_STOP.value
        symbol = str(values.get("symbol") or "").strip().upper()
        if reason is None and not _SYMBOL_RE.fullmatch(symbol):
            reason = RejectCode.INVALID_SIGNAL.value
        if reason is None and (lot_size < 1 or int(lot_size) != lot_size):
            reason = RejectCode.INVALID_QUANTITY.value
        if reason is None and resolved_order_type == OrderType.LIMIT.value:
            if not _finite_positive(limit_price):
                reason = RejectCode.INVALID_PRICE.value
            elif not float(values["stop_reference"]) < float(limit_price) < float(values["target_reference"]):
                reason = RejectCode.INVALID_PRICE.value
        if reason is not None:
            order = self._reject_order(values=values, fingerprint=fingerprint, reason=reason, at=now,
                                       source=source_text or "UNKNOWN", source_metadata=metadata, sector=sector,
                                       order_type=resolved_order_type, limit_price=limit_price)
            return order

        assert self.cost_model is not None
        existing_symbol = any(pos.symbol == symbol for pos in self.open_positions) or any(
            order.symbol == symbol for order in self._active_pending_orders()
        )
        if reason is None and existing_symbol and not self.config.allow_duplicate_symbol_positions:
            reason = RejectCode.EXISTING_POSITION.value
        if reason is None and self._active_position_count() >= self.config.max_positions:
            reason = RejectCode.MAX_POSITIONS.value
        if reason is None and not sector:
            reason = RejectCode.SECTOR_EXPOSURE_UNKNOWN.value
        if reason is not None:
            order = self._reject_order(values=values, fingerprint=fingerprint, reason=reason, at=now,
                                       source=source_text, source_metadata=metadata, sector=sector,
                                       order_type=resolved_order_type, limit_price=limit_price)
            return order

        entry_reference = float(limit_price if resolved_order_type == OrderType.LIMIT.value else values["reference_price"])
        metrics = self._risk_metrics()
        equity = float(metrics["total_equity"])
        pending_notional = sum(self._pending_reserve(order) for order in self._active_pending_orders())
        cash_available = max(0.0, float(metrics["cash"]) - pending_notional)
        total_gross = float(metrics["gross_exposure"])
        symbol_exposure = sum(p.current_price * p.quantity for p in self.open_positions if p.symbol == symbol)
        symbol_exposure += sum(o.requested_price * o.quantity for o in self._active_pending_orders() if o.symbol == symbol)
        sector_exposure = sum(p.current_price * p.quantity for p in self.open_positions if p.sector == sector)
        sector_exposure += sum(o.requested_price * o.quantity for o in self._active_pending_orders() if o.sector == sector)
        symbol_cap = max(0.0, equity * self.config.max_symbol_exposure - symbol_exposure)
        sector_cap = max(0.0, equity * self.config.max_sector_exposure - sector_exposure)
        gross_cap = max(0.0, equity * self.config.max_gross_exposure - total_gross)
        if symbol_cap < entry_reference:
            reason = RejectCode.SYMBOL_EXPOSURE_LIMIT.value
        elif sector_cap < entry_reference:
            reason = RejectCode.SECTOR_EXPOSURE_LIMIT.value
        elif min(cash_available, gross_cap) < entry_reference:
            reason = RejectCode.INSUFFICIENT_CAPITAL.value
        if reason is not None:
            order = self._reject_order(values=values, fingerprint=fingerprint, reason=reason, at=now,
                                       source=source_text, source_metadata=metadata, sector=sector,
                                       order_type=resolved_order_type, limit_price=limit_price)
            return order

        exposure_available = min(cash_available, gross_cap, symbol_cap, sector_cap)
        quantity, budget, planned_risk, sizing_error = self.risk_engine.size(
            equity=equity, cash_available=cash_available, exposure_available=exposure_available,
            entry=entry_reference, stop=float(values["stop_reference"]), lot_size=int(lot_size),
        )
        if sizing_error or quantity <= 0:
            order = self._reject_order(values=values, fingerprint=fingerprint,
                                       reason=sizing_error or RejectCode.INVALID_QUANTITY.value, at=now,
                                       source=source_text, source_metadata=metadata, sector=sector,
                                       order_type=resolved_order_type, limit_price=limit_price)
            return order

        trailing = trailing_atr_multiple if trailing_atr_multiple is not None else self.config.trailing_atr_multiple
        order_id = self._order_id(signal_id, fingerprint)
        requested = float(limit_price) if resolved_order_type == OrderType.LIMIT.value else float(values["reference_price"])
        order = PaperOrder(
            order_id=order_id, signal_id=signal_id, timestamp=now,
            signal_timestamp=_parse_time(timestamp), signal_valid_until=_parse_time(valid_until), symbol=symbol,
            exchange="NSE", series="EQ", side="BUY", order_type=resolved_order_type,
            quantity=quantity, requested_price=requested, stop_price=float(values["stop_reference"]),
            target_price=float(values["target_reference"]),
            trailing_stop_config={"atr_multiple": float(trailing)} if trailing > 0 else None,
            strategy=str(values["strategy"]), timeframe=str(values["timeframe"]).lower(),
            status=OrderStatus.PENDING.value, source=source_text, source_metadata=metadata,
            sector=str(sector), risk_budget=budget, planned_risk=planned_risk,
        )
        before = self._audit_summary()
        self.orders[order_id] = order
        self._signal_fingerprints[signal_id] = fingerprint
        self._record_event("ORDER_PENDING", now, before=before, reason="RISK_ACCEPTED", order_id=order_id,
                           signal_id=signal_id,
                           payload={"transitions": ["CREATED", "VALIDATED", "PENDING"], "quantity": quantity,
                                    "requested_price": requested, "stop_price": order.stop_price,
                                    "target_price": order.target_price, "source": source_text,
                                    "source_metadata": metadata, "sector": sector,
                                    "risk_budget": budget, "planned_risk": planned_risk})
        return order

    def _reject_order_internal(self, order: PaperOrder, reason: str, at: datetime) -> None:
        if order.status != OrderStatus.PENDING.value:
            return
        before = self._audit_summary()
        self.orders[order.order_id] = transition_order(order, OrderStatus.REJECTED, rejection_reason=reason)
        self._add_risk_event(reason, "pending entry rejected before fill", at, symbol=order.symbol, signal_id=order.signal_id)
        self._record_event("ORDER_REJECTED", at, before=before, reason=reason, order_id=order.order_id,
                           signal_id=order.signal_id)

    def cancel_order(self, order_id: str, *, at: datetime, reason: str = "CANCELLED_BY_REPLAY") -> PaperOrder:
        order = self.orders[order_id]
        if order.status != OrderStatus.PENDING.value:
            raise PaperExecutionError("only pending paper orders may be cancelled")
        before = self._audit_summary()
        cancelled = transition_order(order, OrderStatus.CANCELLED, rejection_reason=reason)
        self.orders[order_id] = cancelled
        self._record_event("ORDER_CANCELLED", at, before=before, reason=reason, order_id=order_id,
                           signal_id=order.signal_id)
        return cancelled

    def _fill_entry(self, order: PaperOrder, raw_price: float, at: datetime, *, fill_source: str) -> bool:
        if self.risk_state is not RiskState.ACTIVE:
            self._reject_order_internal(order, RejectCode.DATA_QUALITY.value, at)
            return False
        assert self.cost_model is not None
        is_limit = order.order_type == OrderType.LIMIT.value
        actual = float(raw_price) if is_limit else self._slipped(float(raw_price), entry=True)
        if not _finite_positive(actual) or float(raw_price) <= order.stop_price or actual >= order.target_price:
            self._reject_order_internal(order, RejectCode.INVALID_PRICE.value, at)
            return False
        planned_loss, _ = self.risk_engine._loss_at_stop(float(raw_price), order.stop_price, order.quantity)
        if planned_loss > order.risk_budget + 1e-8:
            self._reject_order_internal(order, RejectCode.RISK_BUDGET_EXCEEDED.value, at)
            return False
        entry_fees = self.cost_model.order_cost(actual, order.quantity, "buy", True).total
        cash_required = actual * order.quantity + entry_fees
        other_pending_reserve = sum(
            self._pending_reserve(pending) for pending in self._active_pending_orders()
            if pending.order_id != order.order_id
        )
        if cash_required + other_pending_reserve > self._cash + 1e-8:
            self._reject_order_internal(order, RejectCode.INSUFFICIENT_CAPITAL.value, at)
            return False
        metrics = self._risk_metrics()
        equity = float(metrics["total_equity"])
        actual_notional = actual * order.quantity
        gross_without_order = float(metrics["gross_exposure"]) - order.requested_price * order.quantity
        if gross_without_order + actual_notional > equity * self.config.max_gross_exposure + 1e-8:
            self._reject_order_internal(order, RejectCode.INSUFFICIENT_CAPITAL.value, at)
            return False
        symbol_exposure = sum(position.current_price * position.quantity for position in self.open_positions
                              if position.symbol == order.symbol)
        symbol_exposure += sum(
            pending.requested_price * pending.quantity for pending in self._active_pending_orders()
            if pending.order_id != order.order_id and pending.symbol == order.symbol
        )
        if symbol_exposure + actual_notional > equity * self.config.max_symbol_exposure + 1e-8:
            self._reject_order_internal(order, RejectCode.SYMBOL_EXPOSURE_LIMIT.value, at)
            return False
        sector_exposure = sum(position.current_price * position.quantity for position in self.open_positions
                              if position.sector == order.sector)
        sector_exposure += sum(
            pending.requested_price * pending.quantity for pending in self._active_pending_orders()
            if pending.order_id != order.order_id and pending.sector == order.sector
        )
        if sector_exposure + actual_notional > equity * self.config.max_sector_exposure + 1e-8:
            self._reject_order_internal(order, RejectCode.SECTOR_EXPOSURE_LIMIT.value, at)
            return False
        before = self._audit_summary()
        filled = replace(order, status=OrderStatus.FILLED.value, filled_price=actual,
                         raw_filled_price=float(raw_price), filled_at=_parse_time(at),
                         rejection_reason=None)
        self.orders[order.order_id] = filled
        position_id = f"POS-{_sha256([order.order_id, 'position'])[:24]}"
        entry_slippage = max(0.0, actual - float(raw_price)) * order.quantity
        trailing_multiple = float((order.trailing_stop_config or {}).get("atr_multiple", 0.0))
        position = PaperPosition(
            position_id=position_id, symbol=order.symbol, exchange=order.exchange, series=order.series,
            side="LONG", quantity=order.quantity, average_entry_price=actual, raw_entry_price=float(raw_price),
            current_price=float(raw_price), stop_price=order.stop_price, target_price=order.target_price,
            trailing_stop=order.stop_price if trailing_multiple > 0 else None,
            trailing_atr_multiple=trailing_multiple, peak_price=float(raw_price), opened_at=_parse_time(at),
            strategy=order.strategy, timeframe=order.timeframe, signal_id=order.signal_id,
            entry_order_id=order.order_id, entry_fees=entry_fees, entry_slippage=entry_slippage,
            unrealized_pnl=0.0, realized_pnl=0.0, status="OPEN", source=order.source,
            source_metadata=order.source_metadata, sector=order.sector,
        )
        self.positions[position_id] = position
        self._cash -= cash_required
        self._market_marks[order.symbol] = {"timestamp": _parse_time(at).isoformat(), "price": float(raw_price),
                                           "source": fill_source, "exchange": order.exchange, "series": order.series}
        self._update_position_unrealized(position_id)
        self._record_event("ENTRY_FILLED", at, before=before, reason="PAPER_FILL", order_id=order.order_id,
                           position_id=position_id, signal_id=order.signal_id,
                           payload={"raw_price": raw_price, "fill_price": actual, "quantity": order.quantity,
                                    "entry_fees": entry_fees, "entry_slippage": entry_slippage,
                                    "fill_source": fill_source})
        return True

    def _update_position_unrealized(self, position_id: str) -> None:
        position = self.positions[position_id]
        if position.status != "OPEN" or self.cost_model is None:
            return
        exit_price = self._slipped(position.current_price, entry=False)
        fees = self.cost_model.order_cost(exit_price, position.quantity, "sell", True, is_exit=True).total
        net_unrealized = exit_price * position.quantity - fees - position.average_entry_price * position.quantity - position.entry_fees
        self.positions[position_id] = replace(position, unrealized_pnl=net_unrealized)

    def _market_exit_fill(self, raw_price: float) -> float:
        return self._slipped(raw_price, entry=False)

    def _close_position(
        self,
        position_id: str,
        raw_price: float,
        at: datetime,
        reason: str,
        *,
        limit_fill: bool = False,
        source: str = "",
    ) -> PaperTrade:
        position = self.positions[position_id]
        if position.status != "OPEN":
            raise PaperExecutionError("closed position cannot be modified")
        assert self.cost_model is not None
        actual_exit = float(raw_price) if limit_fill else self._market_exit_fill(float(raw_price))
        exit_fees = self.cost_model.order_cost(actual_exit, position.quantity, "sell", True, is_exit=True).total
        gross = (float(raw_price) - position.raw_entry_price) * position.quantity
        exit_slippage = max(0.0, float(raw_price) - actual_exit) * position.quantity
        transaction_costs = position.entry_fees + exit_fees
        slippage = position.entry_slippage + exit_slippage
        total_cost = transaction_costs + slippage
        net = gross - total_cost
        timestamp = _parse_time(at)
        if self._last_event_time is not None and timestamp < self._last_event_time:
            raise PaperExecutionError("position exit is out of chronological order")
        exit_order_id = f"EXIT-{_sha256([position_id, timestamp, reason])[:24]}"
        if exit_order_id in self.orders:
            existing_trade = next((trade for trade in self.trades.values() if trade.exit_order_id == exit_order_id), None)
            if existing_trade is not None:
                return existing_trade
            raise PaperExecutionError("exit order identity collision")
        before = self._audit_summary()
        exit_order = PaperOrder(
            order_id=exit_order_id, signal_id=position.signal_id, timestamp=timestamp,
            signal_timestamp=position.opened_at, signal_valid_until=timestamp,
            symbol=position.symbol, exchange=position.exchange, series=position.series,
            side="SELL", order_type=OrderType.LIMIT.value if limit_fill else OrderType.MARKET.value,
            quantity=position.quantity, requested_price=float(raw_price), stop_price=position.stop_price,
            target_price=position.target_price, trailing_stop_config=None, strategy=position.strategy,
            timeframe=position.timeframe, status=OrderStatus.FILLED.value, filled_price=actual_exit,
            raw_filled_price=float(raw_price), filled_at=timestamp, source=source or position.source,
            source_metadata=position.source_metadata, sector=position.sector, exit_reason=reason,
        )
        self.orders[exit_order_id] = exit_order
        self._cash += actual_exit * position.quantity - exit_fees
        trade_id = f"TRD-{_sha256([position_id, exit_order_id])[:24]}"
        trade = PaperTrade(
            trade_id=trade_id, position_id=position_id, entry_order_id=position.entry_order_id,
            exit_order_id=exit_order_id, symbol=position.symbol, strategy=position.strategy,
            timeframe=position.timeframe, side="LONG", entry_time=position.opened_at,
            entry_price=position.average_entry_price, raw_entry_price=position.raw_entry_price,
            exit_time=timestamp, exit_price=actual_exit, raw_exit_price=float(raw_price),
            quantity=position.quantity, gross_pnl=gross, transaction_costs=transaction_costs,
            slippage=slippage, total_cost=total_cost, net_pnl=net, exit_reason=reason,
            source=position.source, source_metadata=position.source_metadata,
        )
        self.trades[trade_id] = trade
        self.positions[position_id] = replace(position, current_price=float(raw_price), unrealized_pnl=0.0,
                                              realized_pnl=net, status="CLOSED")
        self._record_event("POSITION_CLOSED", timestamp, before=before, reason=reason, order_id=exit_order_id,
                           position_id=position_id, trade_id=trade_id, signal_id=position.signal_id,
                           payload={"raw_exit_price": raw_price, "exit_fill_price": actual_exit,
                                    "quantity": position.quantity, "gross_pnl": gross,
                                    "transaction_costs": transaction_costs, "slippage": slippage,
                                    "total_cost": total_cost, "net_pnl": net,
                                    "transitions": ["CREATED", "VALIDATED", "PENDING", "FILLED"]})
        return trade

    def _handle_entry_orders_for_bar(self, bar: IntradayBar, at: datetime) -> None:
        for order in list(self._active_pending_orders()):
            if order.symbol != bar.symbol or order.timeframe != bar.timeframe:
                continue
            if bar.timestamp >= min(order.signal_valid_until, order.signal_timestamp + timedelta(seconds=self.config.signal_max_age_seconds)):
                before = self._audit_summary()
                self.orders[order.order_id] = transition_order(order, OrderStatus.EXPIRED, rejection_reason=RejectCode.EXPIRED_SIGNAL.value)
                self._record_event("ORDER_EXPIRED", at, before=before, reason=RejectCode.EXPIRED_SIGNAL.value,
                                   order_id=order.order_id, signal_id=order.signal_id)
                continue
            if bar.timestamp < order.timestamp or at <= order.timestamp:
                continue
            if order.order_type == OrderType.MARKET.value:
                self._fill_entry(order, bar.open, bar.timestamp, fill_source=bar.source)
            elif bar.low <= order.requested_price:
                # Conservative limit policy: fill exactly at the limit, never at
                # a better opening price that OHLC cannot sequence reliably.
                self._fill_entry(order, order.requested_price, bar.timestamp, fill_source=bar.source)

    def _handle_position_exits(self, bar: IntradayBar, at: datetime) -> None:
        for position in list(self.open_positions):
            if position.symbol != bar.symbol or position.timeframe != bar.timeframe:
                continue
            effective_stop = max(position.stop_price, position.trailing_stop or position.stop_price)
            hit_stop = bar.low <= effective_stop
            hit_target = bar.high >= position.target_price
            if hit_stop:
                # If both barriers occur inside this OHLC bar, stop-first is the
                # fixed conservative ordering. A gap below the stop exits at open.
                raw_exit = min(bar.open, effective_stop) if bar.open <= effective_stop else effective_stop
                reason = "TRAILING_STOP" if position.trailing_stop is not None and effective_stop > position.stop_price else "STOP_LOSS"
                self._close_position(position.position_id, raw_exit, at, reason, source=bar.source)
            elif hit_target:
                # Target is modeled as a sell limit at exactly target; no price
                # improvement is inferred from the bar high.
                self._close_position(position.position_id, position.target_price, at, "TARGET",
                                     limit_fill=True, source=bar.source)

    def _update_trailing_stops(self, bar: IntradayBar, atr: float | None, at: datetime) -> None:
        if atr is None or not math.isfinite(atr) or atr <= 0:
            return
        for position in list(self.open_positions):
            if position.symbol != bar.symbol or position.timeframe != bar.timeframe or position.trailing_atr_multiple <= 0:
                continue
            new_peak = max(position.peak_price, bar.high)
            new_stop = max(position.stop_price, new_peak - atr * position.trailing_atr_multiple)
            if position.trailing_stop is None or new_stop > position.trailing_stop:
                before = self._audit_summary()
                self.positions[position.position_id] = replace(position, peak_price=new_peak, trailing_stop=new_stop)
                self._record_event("TRAILING_STOP_RAISED", at, before=before, reason="ATR_TRAIL_RATCHET",
                                   position_id=position.position_id, signal_id=position.signal_id,
                                   payload={"atr": atr, "previous_stop": position.trailing_stop, "new_stop": new_stop,
                                            "peak_price": new_peak})
            elif new_peak > position.peak_price:
                self.positions[position.position_id] = replace(position, peak_price=new_peak)

    def _data_quality_halt(self, code: str, detail: str, *, event_time: datetime | None = None) -> None:
        try:
            timestamp = _parse_time(event_time) if event_time is not None else None
        except (TypeError, ValueError):
            timestamp = None
        timestamp = timestamp or self._last_event_time or datetime.now(IST)
        self._set_halt(RiskState.DATA_QUALITY_HALT.value, f"{code}: {detail}", timestamp)
        self._reject_pending_for_halt(timestamp)

    def process_bar(
        self,
        bar: IntradayBar,
        *,
        event_time: datetime | None = None,
        session_id: str | None = None,
        bar_event_id: str | None = None,
    ) -> list[PaperTrade]:
        """Advance marks, fills, exits and trailing state using one completed bar."""
        if bar_event_id and str(bar_event_id) in self._processed_bar_event_ids:
            return []
        before_trade_ids = set(self.trades)
        try:
            item = validate_intraday_bar(bar, calendar=self.calendar, require_closed=True)
            at = _parse_time(event_time or item.end, field_name="event_time")
            if at < item.end:
                raise ValueError("bar event time precedes its interval close")
            if self._last_event_time is not None and at < self._last_event_time:
                raise ValueError("bar event is out of chronological order")
            prior_ooo = self.bar_store.out_of_order_bars
            accepted = self.bar_store.add(item)
            if not accepted:
                return []
            if self.bar_store.out_of_order_bars != prior_ooo:
                raise ValueError("out-of-order market bar")
        except (ValueError, TypeError, OverflowError) as exc:
            self._data_quality_halt("DATA_QUALITY", str(exc), event_time=event_time)
            return []

        self._roll_session(item.trading_date, item.timestamp)
        before = self._audit_summary()
        self._last_valid_market_time = at
        self._market_marks[item.symbol] = {"timestamp": at.isoformat(), "price": item.close,
                                           "source": item.source, "exchange": item.exchange, "series": item.series}
        for position in list(self.open_positions):
            if position.symbol == item.symbol:
                updated = replace(position, current_price=item.close)
                self.positions[position.position_id] = updated
                self._update_position_unrealized(position.position_id)

        self._update_risk_limits(at)
        if self.risk_state is not RiskState.ACTIVE:
            self._reject_pending_for_halt(at)
        else:
            self._handle_entry_orders_for_bar(item, at)
        self._handle_position_exits(item, at)

        indicator_state = self._indicator_states.setdefault((item.symbol, item.timeframe), IntradayIndicatorState(item.timeframe))
        try:
            snapshot = indicator_state.update(item)
        except ValueError as exc:
            self._data_quality_halt("INDICATOR_STATE", str(exc), event_time=at)
            snapshot = None
        self._update_trailing_stops(item, snapshot.atr14 if snapshot is not None else None, at)
        self._append_equity_point(at)
        self._last_event_time = at
        self._record_event("MARKET_BAR_PROCESSED", at, before=before, reason="CLOSED_BAR",
                           payload={"symbol": item.symbol, "exchange": item.exchange, "series": item.series,
                                    "timeframe": item.timeframe, "bar_start": item.timestamp.isoformat(),
                                    "source": item.source, "volume_kind": item.volume_kind,
                                    "session_id": session_id, "bar_event_id": bar_event_id})
        return [trade for trade in self.all_trades if trade.trade_id not in before_trade_ids]

    def process_quote(self, quote: PaperQuote | Any, *, event_time: datetime) -> list[PaperTrade]:
        """Consume a normalized quote structurally; replay freshness uses event time."""
        before_trades = set(self.trades)
        try:
            at = _parse_time(event_time, field_name="event_time")
            symbol = str(getattr(quote, "symbol", "") or "").strip().upper()
            exchange = str(getattr(quote, "exchange", "") or "").strip().upper()
            series = str(getattr(quote, "series", "") or "").strip().upper()
            source = str(getattr(quote, "source", "") or "").strip()
            timestamp = _parse_time(getattr(quote, "source_timestamp", None) or getattr(quote, "timestamp", None),
                                    field_name="quote.timestamp")
            price = float(getattr(quote, "last_price", None) or getattr(quote, "price", None))
            validity = str(getattr(quote, "validity_status", "VALID") or "").upper()
            if not _SYMBOL_RE.fullmatch(symbol) or exchange != "NSE" or series != "EQ" or not source:
                raise ValueError("quote must explicitly identify a valid symbol, NSE, EQ, and source")
            if validity != "VALID" or not math.isfinite(price) or price <= 0:
                raise ValueError("quote status or price is invalid")
            age = (at - timestamp).total_seconds()
            if age < 0:
                raise ValueError("quote timestamp is in the future")
            if age > self.config.quote_max_age_seconds:
                raise ValueError("quote is stale relative to replay/event time")
            session = self.calendar.intraday_status(timestamp, timezone_name="Asia/Kolkata", observed_market_data=True)
            if session.status != "OPEN":
                raise ValueError(f"quote is outside a confirmed NSE regular session: {session.status}")
            if self._last_event_time is not None and at < self._last_event_time:
                raise ValueError("quote event is out of chronological order")
        except (TypeError, ValueError, OverflowError) as exc:
            self._data_quality_halt("QUOTE_DATA_QUALITY", str(exc), event_time=event_time)
            return []

        self._roll_session(timestamp.date(), at)
        before = self._audit_summary()
        self._last_valid_market_time = at
        self._market_marks[symbol] = {"timestamp": at.isoformat(), "price": price,
                                      "source": source, "exchange": exchange, "series": series}
        for position in list(self.open_positions):
            if position.symbol == symbol:
                self.positions[position.position_id] = replace(position, current_price=price)
                self._update_position_unrealized(position.position_id)
        self._update_risk_limits(at)
        if self.risk_state is not RiskState.ACTIVE:
            self._reject_pending_for_halt(at)
        else:
            for order in list(self._active_pending_orders()):
                if order.symbol == symbol and at > order.timestamp:
                    if at >= min(order.signal_valid_until, order.signal_timestamp + timedelta(seconds=self.config.signal_max_age_seconds)):
                        before_expire = self._audit_summary()
                        self.orders[order.order_id] = transition_order(order, OrderStatus.EXPIRED,
                                                                       rejection_reason=RejectCode.EXPIRED_SIGNAL.value)
                        self._record_event("ORDER_EXPIRED", at, before=before_expire,
                                           reason=RejectCode.EXPIRED_SIGNAL.value, order_id=order.order_id,
                                           signal_id=order.signal_id)
                    elif order.order_type == OrderType.MARKET.value:
                        self._fill_entry(order, price, at, fill_source=source)
                    # Limit order fills are determined by bar lows, not a quote.
        for position in list(self.open_positions):
            if position.symbol != symbol:
                continue
            effective_stop = max(position.stop_price, position.trailing_stop or position.stop_price)
            if price <= effective_stop:
                reason = "TRAILING_STOP" if position.trailing_stop and effective_stop > position.stop_price else "STOP_LOSS"
                self._close_position(position.position_id, price, at, reason, source=source)
            elif price >= position.target_price:
                self._close_position(position.position_id, position.target_price, at, "TARGET",
                                     limit_fill=True, source=source)
        self._append_equity_point(at)
        self._last_event_time = at
        self._record_event("MARKET_QUOTE_PROCESSED", at, before=before, reason="VALID_QUOTE",
                           payload={"symbol": symbol, "price": price, "source": source,
                                    "quote_timestamp": timestamp.isoformat()})
        return [trade for trade in self.all_trades if trade.trade_id not in before_trades]

    def close_session(self, session_date: date, at: datetime, *, allow_last_available_mark: bool = False) -> list[PaperTrade]:
        """Square off using each position's last observed mark, never a future bar."""
        timestamp = _parse_time(at)
        if timestamp.date() != session_date or timestamp.time() < time(15, 30):
            if not allow_last_available_mark:
                raise ValueError("session close must be at or after 15:30 IST on the requested session")
            timestamp = datetime.combine(session_date, time(15, 30), tzinfo=IST)
        before_ids = set(self.trades)
        for order in list(self._active_pending_orders()):
            if order.timestamp.date() == session_date:
                before = self._audit_summary()
                self.orders[order.order_id] = transition_order(order, OrderStatus.CANCELLED,
                                                               rejection_reason="SESSION_CLOSE")
                self._record_event("ORDER_CANCELLED", timestamp, before=before,
                                   reason="SESSION_CLOSE", order_id=order.order_id,
                                   signal_id=order.signal_id)
        if self.config.force_intraday_exit:
            before = self._audit_summary()
            for position in list(self.open_positions):
                mark = self._market_marks.get(position.symbol)
                if mark is None or date.fromisoformat(mark["timestamp"][:10]) != session_date:
                    self._set_halt(RiskState.DATA_QUALITY_HALT.value,
                                   f"no same-session final price for {position.symbol}; position not force-closed", timestamp)
                    continue
                self._close_position(position.position_id, float(mark["price"]), timestamp,
                                     "SESSION_CLOSE", source=str(mark.get("source") or position.source))
            self._append_equity_point(timestamp)
            self._last_event_time = max(self._last_event_time or timestamp, timestamp)
            self._record_event("SESSION_SQUARED_OFF", timestamp, before=before, reason="SESSION_CLOSE")
        return [trade for trade in self.all_trades if trade.trade_id not in before_ids]

    def exit_position(self, position_id: str, *, market_context: PaperQuote | IntradayBar, at: datetime,
                      reason: str = "EXPLICIT_EXIT") -> PaperTrade:
        """Explicit analytical exit using only the supplied current context."""
        position = self.positions[position_id]
        if position.status != "OPEN":
            raise PaperExecutionError("closed position cannot be modified")
        timestamp = _parse_time(at, field_name="exit event timestamp")
        try:
            if self._last_event_time is not None and timestamp < self._last_event_time:
                raise ValueError("exit event is out of chronological order")
            if isinstance(market_context, IntradayBar):
                bar = validate_intraday_bar(market_context, calendar=self.calendar, require_closed=True)
                if bar.symbol != position.symbol or bar.exchange != position.exchange or bar.series != position.series:
                    raise ValueError("exit bar identity does not match the position")
                if timestamp < bar.end or (timestamp - bar.end).total_seconds() > self.config.quote_max_age_seconds:
                    raise ValueError("exit bar is not current relative to replay time")
                price, source = bar.close, bar.source
            else:
                symbol = str(getattr(market_context, "symbol", "") or "").strip().upper()
                exchange = str(getattr(market_context, "exchange", "") or "").strip().upper()
                series = str(getattr(market_context, "series", "") or "").strip().upper()
                source = str(getattr(market_context, "source", "") or "").strip()
                quote_at = _parse_time(
                    getattr(market_context, "source_timestamp", None) or getattr(market_context, "timestamp", None),
                    field_name="exit quote timestamp",
                )
                price = float(getattr(market_context, "last_price", None) or getattr(market_context, "price", None))
                validity = str(getattr(market_context, "validity_status", "VALID") or "").upper()
                age = (timestamp - quote_at).total_seconds()
                if symbol != position.symbol or exchange != position.exchange or series != position.series or not source:
                    raise ValueError("exit quote identity or source is invalid")
                if validity != "VALID" or not _finite_positive(price):
                    raise ValueError("exit quote price or validation status is invalid")
                if age < 0 or age > self.config.quote_max_age_seconds:
                    raise ValueError("exit quote is stale or in the future relative to replay time")
                session = self.calendar.intraday_status(
                    quote_at, timezone_name="Asia/Kolkata", observed_market_data=True,
                )
                if session.status != "OPEN":
                    raise ValueError("exit quote is outside a confirmed NSE regular session")
        except (TypeError, ValueError, OverflowError) as exc:
            self._data_quality_halt("EXIT_CONTEXT", str(exc), event_time=timestamp)
            raise ValueError(str(exc)) from exc
        return self._close_position(position_id, price, timestamp, reason, source=source)

    def set_manual_halt(self, at: datetime, *, reason: str = "manual paper halt") -> None:
        self._set_halt(RiskState.MANUAL_HALT.value, reason, at)
        self._reject_pending_for_halt(at)

    def clear_manual_halt(self, at: datetime, *, confirmed: bool = False) -> None:
        if not confirmed:
            raise ValueError("manual halt clear requires explicit confirmation")
        if RiskState.MANUAL_HALT.value not in self._halts:
            return
        before = self._audit_summary()
        self._halts.pop(RiskState.MANUAL_HALT.value)
        self._record_event("MANUAL_HALT_CLEARED", at, before=before, reason="CONFIRMED_RESUME")

    def clear_portfolio_halt(self, at: datetime, *, confirmed: bool = False) -> None:
        if not confirmed:
            raise ValueError("portfolio halt clear requires explicit confirmation")
        if RiskState.PORTFOLIO_DRAWDOWN_HALT.value not in self._halts:
            return
        drawdown = float(self._risk_metrics()["drawdown"])
        if drawdown >= self.config.max_portfolio_drawdown:
            raise ValueError("portfolio drawdown remains at or above the configured halt limit")
        before = self._audit_summary()
        self._halts.pop(RiskState.PORTFOLIO_DRAWDOWN_HALT.value)
        self._record_event("PORTFOLIO_HALT_CLEARED", at, before=before,
                           reason="CONFIRMED_RESUME_BELOW_LIMIT", payload={"drawdown": drawdown,
                                                                          "peak_equity_preserved": self._peak_equity})

    def clear_data_quality_halt(self, at: datetime, *, confirmed: bool = False) -> None:
        if not confirmed:
            raise ValueError("data-quality halt clear requires explicit confirmation")
        if RiskState.DATA_QUALITY_HALT.value not in self._halts:
            return
        timestamp = _parse_time(at)
        if self._last_valid_market_time is None or self._last_data_quality_halt is None or self._last_valid_market_time <= self._last_data_quality_halt:
            raise ValueError("a valid market event after the data-quality halt is required before resume")
        before = self._audit_summary()
        self._halts.pop(RiskState.DATA_QUALITY_HALT.value)
        self._record_event("DATA_QUALITY_HALT_CLEARED", timestamp, before=before,
                           reason="CONFIRMED_AFTER_VALID_MARKET_EVENT")


class IntradayPaperReplay:
    """Interleave Phase 4B closed-bar signals with Phase 4C paper events."""

    SPEEDS = {"1x": 1.0, "5x": 5.0, "max": None}

    def __init__(self, scanner: IntradayScanner, engine: PaperExecutionEngine,
                 *, sleep_fn: Callable[[float], None] = sleep) -> None:
        self.scanner = scanner
        self.engine = engine
        self.sleep_fn = sleep_fn

    def run(
        self,
        provider: IntradayDataProvider,
        *,
        speed: str = "max",
        sectors: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        if speed not in self.SPEEDS:
            raise ValueError("replay speed must be 1x, 5x, or max")
        factor = self.SPEEDS[speed]
        previous_key = None
        previous_time: datetime | None = None
        bars_processed = 0
        signals_processed = 0
        first_time = None
        last_time = None
        for bar in provider.iter_bars(self.scanner.symbols, self.scanner.config.timeframes):
            if bar.status != "CLOSED":
                continue
            event_time = bar.end
            key = (event_time, bar.symbol, bar.timeframe, bar.timestamp)
            if previous_key is not None and key < previous_key:
                raise ValueError("Phase 4C replay provider must be chronological")
            if factor is not None and previous_time is not None:
                delay = max(0.0, (event_time - previous_time).total_seconds() / factor)
                if delay > 0:
                    self.sleep_fn(delay)
            if first_time is None:
                first_time = event_time
            # Existing orders and positions see this event before the scanner
            # emits signals from the bar that has just completed.
            self.engine.process_bar(bar, event_time=event_time)
            signals = self.scanner.handle_bar(bar)
            for signal in signals:
                sector = (sectors or {}).get(signal.symbol)
                self.engine.submit_signal(
                    SignalEnvelope(signal, source=bar.source,
                                   source_metadata={"replay_provider": provider.provider_name,
                                                    "bar_source": bar.source}),
                    replay_time=signal.timestamp, sector=sector,
                )
                signals_processed += 1
            bars_processed += 1
            previous_key = key
            previous_time = event_time
            last_time = event_time
        if last_time is not None and last_time.astimezone(IST).time() >= time(15, 30):
            self.engine.close_session(last_time.astimezone(IST).date(), last_time)
        return {
            "bars_processed": bars_processed,
            "signals_processed": signals_processed,
            "orders": len(self.engine.all_orders),
            "fills": sum(order.status == OrderStatus.FILLED.value for order in self.engine.all_orders),
            "trades": len(self.engine.all_trades),
            "first_event_time": first_time,
            "last_event_time": last_time,
            "portfolio": self.engine.portfolio_snapshot(last_time),
        }
