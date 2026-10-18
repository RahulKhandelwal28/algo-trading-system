"""Provider-independent models and safe configuration for Phase 4C paper execution."""

from __future__ import annotations

import math
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Mapping


class ExecutionMode(str, Enum):
    PAPER = "PAPER"
    LIVE = "LIVE"  # Reserved value; Phase 4C deliberately has no LIVE adapter.


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(str, Enum):
    CREATED = "CREATED"
    VALIDATED = "VALIDATED"
    REJECTED = "REJECTED"
    PENDING = "PENDING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class RiskState(str, Enum):
    ACTIVE = "ACTIVE"
    DAILY_LOSS_HALT = "DAILY_LOSS_HALT"
    PORTFOLIO_DRAWDOWN_HALT = "PORTFOLIO_DRAWDOWN_HALT"
    MANUAL_HALT = "MANUAL_HALT"
    DATA_QUALITY_HALT = "DATA_QUALITY_HALT"


class RejectCode(str, Enum):
    INVALID_SIGNAL = "INVALID_SIGNAL"
    STALE_SIGNAL = "STALE_SIGNAL"
    EXPIRED_SIGNAL = "EXPIRED_SIGNAL"
    FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
    INVALID_PRICE = "INVALID_PRICE"
    INVALID_STOP = "INVALID_STOP"
    INVALID_QUANTITY = "INVALID_QUANTITY"
    RISK_BUDGET_EXCEEDED = "RISK_BUDGET_EXCEEDED"
    INSUFFICIENT_CAPITAL = "INSUFFICIENT_CAPITAL"
    MAX_POSITIONS = "MAX_POSITIONS"
    EXISTING_POSITION = "EXISTING_POSITION"
    SYMBOL_EXPOSURE_LIMIT = "SYMBOL_EXPOSURE_LIMIT"
    SECTOR_EXPOSURE_LIMIT = "SECTOR_EXPOSURE_LIMIT"
    SECTOR_EXPOSURE_UNKNOWN = "SECTOR_EXPOSURE_UNKNOWN"
    DAILY_LOSS_LIMIT = "DAILY_LOSS_LIMIT"
    PORTFOLIO_DRAWDOWN_LIMIT = "PORTFOLIO_DRAWDOWN_LIMIT"
    DATA_QUALITY = "DATA_QUALITY"
    MANUAL_HALT = "MANUAL_HALT"
    DUPLICATE_SIGNAL = "DUPLICATE_SIGNAL"
    LIVE_EXECUTION_NOT_AVAILABLE = "LIVE_EXECUTION_NOT_AVAILABLE_IN_PHASE4C"
    PAPER_TRADING_DISABLED = "PAPER_TRADING_DISABLED"
    COST_RATE_UNCONFIGURED = "COST_RATE_UNCONFIGURED"


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


@dataclass(frozen=True, slots=True)
class Phase4CPaperConfig:
    """Execution policy. Constructing defaults cannot create an order."""

    enabled: bool = False
    execution_mode: ExecutionMode = ExecutionMode.PAPER
    initial_capital: float = 100_000.0
    risk_per_trade: float = 0.01
    max_positions: int = 5
    max_daily_loss: float = 0.02
    max_portfolio_drawdown: float = 0.10
    max_symbol_exposure: float = 0.25
    max_sector_exposure: float = 0.25
    max_gross_exposure: float = 1.0
    allow_duplicate_symbol_positions: bool = False
    signal_max_age_seconds: float = 60.0
    quote_max_age_seconds: float = 5.0
    force_intraday_exit: bool = True
    trailing_atr_multiple: float = 0.0
    slippage_mode: str = "basis_points"
    slippage_value: float | None = None
    realtime_enabled: bool = False
    live_trading_enabled: bool = False

    def __post_init__(self) -> None:
        mode = self.execution_mode
        if not isinstance(mode, ExecutionMode):
            try:
                mode = ExecutionMode(str(mode).strip().upper())
            except ValueError as exc:
                raise ValueError("EXECUTION_MODE must be PAPER or LIVE") from exc
            object.__setattr__(self, "execution_mode", mode)
        if not math.isfinite(self.initial_capital) or self.initial_capital not in {25_000, 50_000, 100_000}:
            raise ValueError("PAPER_INITIAL_CAPITAL must be 25000, 50000, or 100000")
        if not math.isfinite(self.risk_per_trade) or not 0 < self.risk_per_trade <= 0.01:
            raise ValueError("RISK_PER_TRADE must be greater than 0 and at most 0.01")
        if self.max_positions < 1:
            raise ValueError("MAX_POSITIONS must be at least 1")
        for name in ("max_daily_loss", "max_portfolio_drawdown", "max_symbol_exposure", "max_sector_exposure", "max_gross_exposure"):
            value = getattr(self, name)
            if not math.isfinite(value) or not 0 < value <= 1:
                raise ValueError(f"{name} must be in (0, 1]")
        for name in ("signal_max_age_seconds", "quote_max_age_seconds"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive")
        if not math.isfinite(self.trailing_atr_multiple) or self.trailing_atr_multiple < 0:
            raise ValueError("trailing_atr_multiple must be non-negative")
        if self.slippage_mode not in {"fixed", "basis_points", "percentage"}:
            raise ValueError("slippage_mode must be fixed, basis_points, or percentage")
        if self.slippage_value is not None and (not math.isfinite(self.slippage_value) or self.slippage_value < 0):
            raise ValueError("slippage_value must be finite and non-negative")
        if self.slippage_value is not None and self.slippage_mode == "basis_points" and self.slippage_value > 10_000:
            raise ValueError("basis-point slippage must be at most 10000")
        if self.slippage_value is not None and self.slippage_mode == "percentage" and self.slippage_value > 100:
            raise ValueError("percentage slippage must be at most 100")

    @classmethod
    def from_env(cls, *, explicit_paper_command: bool = False) -> "Phase4CPaperConfig":
        """Read opt-in flags. An explicit paper-test command may enable PAPER only."""
        enabled = True if explicit_paper_command else _env_bool("PAPER_TRADING_ENABLED", False)
        return cls(
            enabled=enabled,
            execution_mode=os.environ.get("EXECUTION_MODE", "PAPER"),
            initial_capital=float(os.environ.get("PAPER_INITIAL_CAPITAL", "100000")),
            risk_per_trade=float(os.environ.get("RISK_PER_TRADE", "0.01")),
            max_positions=int(os.environ.get("MAX_POSITIONS", "5")),
            max_daily_loss=float(os.environ.get("MAX_DAILY_LOSS", "0.02")),
            max_portfolio_drawdown=float(os.environ.get("MAX_PORTFOLIO_DRAWDOWN", "0.10")),
            max_symbol_exposure=float(os.environ.get("MAX_SYMBOL_EXPOSURE", "0.25")),
            max_sector_exposure=float(os.environ.get("MAX_SECTOR_EXPOSURE", "0.25")),
            max_gross_exposure=float(os.environ.get("MAX_GROSS_EXPOSURE", "1.0")),
            allow_duplicate_symbol_positions=_env_bool("ALLOW_DUPLICATE_SYMBOL_POSITIONS", False),
            signal_max_age_seconds=float(os.environ.get("SIGNAL_MAX_AGE_SECONDS", "60")),
            quote_max_age_seconds=float(os.environ.get("PAPER_QUOTE_MAX_AGE_SECONDS", "5")),
            force_intraday_exit=_env_bool("FORCE_INTRADAY_EXIT", True),
            trailing_atr_multiple=float(os.environ.get("PAPER_TRAILING_ATR_MULTIPLE", "0")),
            slippage_mode=os.environ.get("PAPER_SLIPPAGE_MODE", "basis_points").strip().lower(),
            slippage_value=(float(os.environ["PAPER_SLIPPAGE_VALUE"]) if "PAPER_SLIPPAGE_VALUE" in os.environ else None),
            realtime_enabled=_env_bool("REALTIME_ENABLED", False),
            live_trading_enabled=(
                _env_bool("LIVE_TRADING", False) or _env_bool("LIVE_TRADING_ENABLED", False)
            ),
        )


@dataclass(frozen=True, slots=True)
class SignalEnvelope:
    """Attach source provenance without changing the Phase 4B signal contract."""

    signal: Any
    source: str
    source_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PaperQuote:
    """Normalized provider-independent quote accepted by the paper engine."""

    symbol: str
    exchange: str
    series: str
    timestamp: datetime
    last_price: float
    source: str
    validity_status: str = "VALID"


@dataclass(frozen=True, slots=True)
class PaperOrder:
    order_id: str
    signal_id: str
    timestamp: datetime
    signal_timestamp: datetime
    signal_valid_until: datetime
    symbol: str
    exchange: str
    series: str
    side: str
    order_type: str
    quantity: int
    requested_price: float
    stop_price: float
    target_price: float
    trailing_stop_config: Mapping[str, float] | None
    strategy: str
    timeframe: str
    status: str
    rejection_reason: str | None = None
    filled_price: float | None = None
    raw_filled_price: float | None = None
    filled_at: datetime | None = None
    source: str = ""
    source_metadata: Mapping[str, Any] = field(default_factory=dict)
    sector: str | None = None
    risk_budget: float = 0.0
    planned_risk: float = 0.0
    exit_reason: str | None = None


@dataclass(frozen=True, slots=True)
class PaperPosition:
    position_id: str
    symbol: str
    exchange: str
    series: str
    side: str
    quantity: int
    average_entry_price: float
    raw_entry_price: float
    current_price: float
    stop_price: float
    target_price: float
    trailing_stop: float | None
    trailing_atr_multiple: float
    peak_price: float
    opened_at: datetime
    strategy: str
    timeframe: str
    signal_id: str
    entry_order_id: str
    entry_fees: float
    entry_slippage: float
    unrealized_pnl: float
    realized_pnl: float
    status: str
    source: str
    source_metadata: Mapping[str, Any] = field(default_factory=dict)
    sector: str | None = None


@dataclass(frozen=True, slots=True)
class PaperTrade:
    trade_id: str
    position_id: str
    entry_order_id: str
    exit_order_id: str
    symbol: str
    strategy: str
    timeframe: str
    side: str
    entry_time: datetime
    entry_price: float
    raw_entry_price: float
    exit_time: datetime
    exit_price: float
    raw_exit_price: float
    quantity: int
    gross_pnl: float
    transaction_costs: float
    slippage: float
    total_cost: float
    net_pnl: float
    exit_reason: str
    source: str
    source_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PaperRiskEvent:
    event_id: str
    timestamp: datetime
    code: str
    state: str
    detail: str
    symbol: str | None = None
    signal_id: str | None = None


@dataclass(frozen=True, slots=True)
class EquityPoint:
    timestamp: datetime
    cash: float
    reserved_capital: float
    realized_pnl: float
    unrealized_pnl: float
    total_costs: float
    total_equity: float
    peak_equity: float
    drawdown: float
    available_cash: float
    gross_exposure: float
    net_exposure: float


def model_to_dict(model: Any) -> dict[str, Any]:
    """Convert one Phase 4C dataclass to a JSON-compatible mapping."""
    result = asdict(model)
    for key, value in list(result.items()):
        if isinstance(value, Enum):
            result[key] = value.value
    return result
