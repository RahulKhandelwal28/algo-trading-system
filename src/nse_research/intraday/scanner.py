"""Provider-independent incremental scanner and in-memory signal store."""

from __future__ import annotations

import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence
from zoneinfo import ZoneInfo

from nse_research.forward.calendar import NseTradingCalendar

from .indicators import IndicatorSnapshot, IntradayIndicatorState
from .model import IntradayBar, IntradayBarStore, default_nse_calendar, validate_intraday_bar
from .strategies import IntradaySignal, RegimeContext, StrategySettings, candidate_to_signal, evaluate_candidates


_IST = ZoneInfo("Asia/Kolkata")


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
class IntradayScannerConfig:
    enabled: bool = False
    universe_size: int = 100
    timeframes: tuple[str, ...] = ("1m", "5m")
    regime_filter_enabled: bool = False
    signal_ttl_bars: int = 3
    max_history_bars_per_symbol_timeframe: int = 256

    def __post_init__(self) -> None:
        if self.universe_size not in {100, 200, 486}:
            raise ValueError("INTRADAY_UNIVERSE_SIZE must be 100, 200, or 486")
        if not self.timeframes or not set(self.timeframes).issubset({"1m", "5m"}):
            raise ValueError("INTRADAY_TIMEFRAMES must be a non-empty subset of 1m,5m")
        if self.signal_ttl_bars < 1 or self.max_history_bars_per_symbol_timeframe < 50:
            raise ValueError("intraday signal lifetime and indicator history must be positive")

    @classmethod
    def from_env(cls) -> "IntradayScannerConfig":
        size = int(os.environ.get("INTRADAY_UNIVERSE_SIZE", "100"))
        raw_timeframes = os.environ.get("INTRADAY_TIMEFRAMES", "1m,5m")
        timeframes = tuple(dict.fromkeys(part.strip().lower() for part in raw_timeframes.split(",") if part.strip()))
        return cls(
            enabled=_env_bool("INTRADAY_SCANNER_ENABLED", False),
            universe_size=size,
            timeframes=timeframes,
            regime_filter_enabled=_env_bool("REGIME_FILTER_ENABLED", False),
        )


class SignalStore:
    """Memory-only deduplicated signal collection; it has no ledger/order API."""

    def __init__(self) -> None:
        self._signals: dict[str, IntradaySignal] = {}
        self.duplicate_signals = 0

    def add(self, signal: IntradaySignal) -> bool:
        if signal.signal_id in self._signals:
            self.duplicate_signals += 1
            return False
        self._signals[signal.signal_id] = signal
        return True

    def all(self) -> list[IntradaySignal]:
        return sorted(self._signals.values(), key=lambda s: (s.timestamp, s.symbol, s.strategy, s.timeframe, s.signal_id))

    def active(self, now: datetime) -> list[IntradaySignal]:
        return [signal for signal in self.all() if now < signal.valid_until]

    def on_date(self, trading_date) -> list[IntradaySignal]:
        return [signal for signal in self.all() if signal.timestamp.date() == trading_date]


class IntradayScanner:
    """Process one closed bar at a time; signals cannot reach order execution."""

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        config: IntradayScannerConfig | None = None,
        calendar: NseTradingCalendar | None = None,
        strategy_settings: StrategySettings | None = None,
    ) -> None:
        self.config = config or IntradayScannerConfig.from_env()
        self.symbols = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols))
        if len(self.symbols) != self.config.universe_size or len(self.symbols) != len(symbols):
            raise ValueError(f"scanner requires exactly {self.config.universe_size} unique frozen-universe symbols")
        self.calendar = calendar or default_nse_calendar()
        self.store = IntradayBarStore(self.symbols, calendar=self.calendar)
        self.signal_store = SignalStore()
        self.strategy_settings = strategy_settings or StrategySettings(signal_ttl_bars=self.config.signal_ttl_bars)
        self._indicators: dict[tuple[str, str], IntradayIndicatorState] = {}
        self._history: dict[tuple[str, str], deque[IntradayBar]] = defaultdict(
            lambda: deque(maxlen=self.config.max_history_bars_per_symbol_timeframe),
        )
        self._snapshots: dict[tuple[str, str], deque[IndicatorSnapshot]] = defaultdict(
            lambda: deque(maxlen=self.config.max_history_bars_per_symbol_timeframe),
        )
        self._session_dates: dict[tuple[str, str], object] = {}
        self._active_setups: dict[tuple[str, str, str, object], str] = {}
        self.regime_missing_count = 0
        self.repeated_setup_suppressions = 0
        self.disabled_reason = "INTRADAY_SCANNER_DISABLED" if not self.config.enabled else None
        self.disabled_strategies: dict[str, str] = {}
        self.signals_generated = 0
        self.last_indicator_duration_seconds = 0.0
        self.last_strategy_duration_seconds = 0.0

    def handle_bar(self, bar: IntradayBar, *, regime_context: RegimeContext | None = None) -> list[IntradaySignal]:
        self.last_indicator_duration_seconds = 0.0
        self.last_strategy_duration_seconds = 0.0
        if not self.config.enabled:
            self.disabled_reason = "INTRADAY_SCANNER_DISABLED"
            return []
        if bar.symbol.strip().upper() not in self.symbols:
            self.store.bars_received += 1
            self.store.bars_rejected += 1
            return []
        if bar.timeframe.strip().lower() not in self.config.timeframes:
            self.store.bars_received += 1
            self.store.bars_rejected += 1
            return []
        if str(bar.status).upper() != "CLOSED":
            self.store.add(bar)
            return []
        if str(bar.bar_status).upper() != "IMMUTABLE_CLOSED":
            self.store.bars_received += 1
            self.store.bars_rejected += 1
            return []
        try:
            out_of_order_before = self.store.out_of_order_bars
            accepted = self.store.add(bar)
        except (ValueError, TypeError):
            return []
        if self.store.out_of_order_bars != out_of_order_before:
            return []
        if not accepted:
            return []
        # Use the accepted input directly. Looking up/sorting the entire session
        # history for each event would turn streaming replay into O(n²) work.
        item = validate_intraday_bar(bar, calendar=self.calendar, require_closed=True)
        key = (item.symbol, item.timeframe)
        if self._session_dates.get(key) != item.trading_date:
            self._history[key].clear()
            self._snapshots[key].clear()
            self._session_dates[key] = item.trading_date
        state = self._indicators.setdefault(key, IntradayIndicatorState(item.timeframe))
        previous = list(self._history[key])
        previous_snapshots = list(self._snapshots[key])
        indicator_started = time.perf_counter()
        snapshot = state.update(item)
        self.last_indicator_duration_seconds = time.perf_counter() - indicator_started
        self._history[key].append(item)
        self._snapshots[key].append(snapshot)

        if self.config.regime_filter_enabled:
            if (regime_context is None or regime_context.timestamp.tzinfo is None
                    or regime_context.timestamp.utcoffset() is None or regime_context.timestamp > item.end
                    or regime_context.timestamp.astimezone(_IST).date() != item.trading_date or not regime_context.allow_long):
                self.regime_missing_count += 1
                return []
        strategy_started = time.perf_counter()
        candidates = evaluate_candidates(item, snapshot, previous, previous_snapshots, self.strategy_settings)
        if item.volume is None or item.volume_kind != "BAR":
            self.disabled_strategies["volume_breakout"] = "STRATEGY_DISABLED_VOLUME_UNAVAILABLE"
        else:
            self.disabled_strategies.pop("volume_breakout", None)
        present_setups = {(item.symbol, item.timeframe, candidate.strategy, item.trading_date): candidate.setup_id
                          for candidate in candidates}
        for setup_key in list(self._active_setups):
            if setup_key[:2] == key and setup_key[3] == item.trading_date and setup_key not in present_setups:
                self._active_setups.pop(setup_key, None)
        generated: list[IntradaySignal] = []
        for candidate in candidates:
            setup_key = (item.symbol, item.timeframe, candidate.strategy, item.trading_date)
            if self._active_setups.get(setup_key) == candidate.setup_id:
                self.repeated_setup_suppressions += 1
                continue
            signal = candidate_to_signal(candidate, item, snapshot, self.strategy_settings)
            if signal is not None and self.signal_store.add(signal):
                generated.append(signal)
                self._active_setups[setup_key] = candidate.setup_id
        self.signals_generated += len(generated)
        self.last_strategy_duration_seconds = time.perf_counter() - strategy_started
        return generated

    def health(self) -> dict[str, object]:
        values = self.store.health()
        readiness = sum(1 for state in self._indicators.values() if state.bars and self._snapshots[(state.bars[-1].symbol, state.timeframe)] and self._snapshots[(state.bars[-1].symbol, state.timeframe)][-1].indicator_ready)
        all_bars = self.store.bars()
        latest = max((bar.end for bar in all_bars), default=None)
        values.update({
            "bars_1m": sum(1 for bar in all_bars if bar.timeframe == "1m"),
            "bars_5m": sum(1 for bar in all_bars if bar.timeframe == "5m"),
            "indicators_ready": readiness,
            "signals_today": len(self.signal_store.on_date(datetime.now(_IST).date())),
            "duplicate_signals": self.signal_store.duplicate_signals,
            "repeated_setup_suppressions": self.repeated_setup_suppressions,
            "regime_context_missing": self.regime_missing_count,
            "last_bar": latest,
            "disabled_reason": self.disabled_reason,
            "strategy_status": {"volume_breakout": self.disabled_strategies.get("volume_breakout", "ENABLED_OR_NO_BAR_YET")},
        })
        return values

    @property
    def signals(self) -> list[IntradaySignal]:
        return self.signal_store.all()
