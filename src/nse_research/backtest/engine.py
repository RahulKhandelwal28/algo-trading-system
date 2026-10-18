"""Event-driven next-bar backtest with explicit cash, exposure, and exit accounting."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from math import floor
from typing import Any

import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.config import AppConfig
from nse_research.indicators.features import add_features, align_regime
from nse_research.risk.controls import portfolio_drawdown_tripwire, position_size
from nse_research.strategies.base import Candidate
from nse_research.strategies.equity import EquityStrategy

LOGGER = logging.getLogger(__name__)
EXIT_MODES = {"stop_loss", "fixed_r", "partial_trailing", "atr_trailing", "max_holding"}


@dataclass
class Position:
    symbol: str
    direction: int
    quantity: int
    initial_quantity: int
    entry_time: pd.Timestamp
    signal_time: pd.Timestamp
    entry: float
    stop: float
    initial_stop: float
    target: float
    risk_budget: float
    risk_amount: float
    strategy: str
    regime: str
    score: float
    reason: str
    sector: str
    entry_cost: float
    bars_held: int = 0
    realized_gross: float = 0.0
    exit_costs: float = 0.0
    exit_value: float = 0.0
    closed_quantity: int = 0
    exit_cash_flow: float = 0.0
    partial_exits: int = 0
    exit_reasons: list[str] = field(default_factory=list)


@dataclass
class BacktestResult:
    strategy: str
    initial_capital: float
    final_capital: float
    trades: list[dict[str, Any]]
    rejected: list[dict[str, Any]]
    equity_curve: list[dict[str, Any]]


class BacktestEngine:
    def __init__(self, config: AppConfig, cost_model: CostModel) -> None:
        self.config = config
        self.cost_model = cost_model
        self._feature_cache: dict[tuple[str, int], pd.DataFrame] = {}
        self._regime_cache: dict[int, pd.DataFrame] = {}
        self._candidate_cache: dict[tuple[int, str], dict[pd.Timestamp, Candidate]] = {}
        # Keep strategy instances alive so Python cannot recycle id() keys between
        # parameter-sensitivity variants and accidentally reuse another variant's signals.
        self._strategy_refs: dict[int, Any] = {}
        self._drawdown_tripwire = portfolio_drawdown_tripwire(config.risk)

    def run(
        self,
        strategy: EquityStrategy,
        bars_by_symbol: dict[str, pd.DataFrame],
        benchmark_bars: pd.DataFrame | None,
        capital: float,
        start: date,
        end: date,
        sector_by_symbol: dict[str, str] | None = None,
        exit_mode: str = "fixed_r",
        india_vix_bars: pd.DataFrame | None = None,
    ) -> BacktestResult:
        if capital <= 0:
            raise ValueError("capital must be positive")
        if exit_mode not in EXIT_MODES:
            raise ValueError(f"unsupported exit mode: {exit_mode}")
        sectors = sector_by_symbol or {}
        benchmark_key = id(benchmark_bars) if benchmark_bars is not None else 0
        if benchmark_bars is not None and not benchmark_bars.empty:
            if benchmark_key not in self._regime_cache:
                vix_close = india_vix_bars["close"] if india_vix_bars is not None and not india_vix_bars.empty else None
                self._regime_cache[benchmark_key] = add_features(
                    benchmark_bars,
                    atr_period=self.config.strategies.atr_period,
                    rsi_period=self.config.strategies.rsi_period,
                    ema_fast=self.config.regime.ema_fast,
                    ema_slow=self.config.regime.ema_slow,
                    ema_long=self.config.regime.ema_long,
                    adx_period=self.config.regime.adx_period,
                    india_vix=vix_close,
                    india_vix_high_threshold=self.config.regime.india_vix_high_threshold,
                )
            regime_features = self._regime_cache[benchmark_key]
        else:
            regime_features = None

        events: dict[pd.Timestamp, list[tuple[str, pd.Series, Candidate | None]]] = {}
        features_by_symbol: dict[str, pd.DataFrame] = {}
        for symbol, raw_bars in bars_by_symbol.items():
            if raw_bars.empty:
                continue
            raw_dates = pd.Index(raw_bars.index.date)
            if not ((raw_dates >= start) & (raw_dates <= end)).any():
                continue
            feature_key = (symbol, id(raw_bars))
            features = self._feature_cache.get(feature_key)
            if features is None:
                features = add_features(
                    raw_bars,
                    atr_period=self.config.strategies.atr_period,
                    rsi_period=self.config.strategies.rsi_period,
                    ema_fast=self.config.regime.ema_fast,
                    ema_slow=self.config.regime.ema_slow,
                    ema_long=self.config.regime.ema_long,
                    adx_period=self.config.regime.adx_period,
                )
                self._feature_cache[feature_key] = features
            features_by_symbol[symbol] = features
            self._strategy_refs[id(strategy)] = strategy
            signal_key = (id(strategy), symbol)
            symbol_candidates = self._candidate_cache.get(signal_key)
            if symbol_candidates is None:
                regimes = align_regime(features, regime_features)
                symbol_candidates = {
                    candidate.signal_time: candidate for candidate in strategy.evaluate(features, regimes)
                }
                self._candidate_cache[signal_key] = symbol_candidates
            index = features.index
            in_period = (index.date >= start) & (index.date <= end)
            for row_number in pd.RangeIndex(len(index))[in_period]:
                timestamp = pd.Timestamp(index[row_number])
                previous_time = pd.Timestamp(index[row_number - 1]) if row_number > 0 else None
                pending = symbol_candidates.get(previous_time) if previous_time is not None else None
                events.setdefault(timestamp, []).append((symbol, features.iloc[row_number], pending))

        if not events:
            return BacktestResult(strategy.name, capital, capital, [], [], [])

        cash = float(capital)
        peak = float(capital)
        positions: dict[str, Position] = {}
        last_prices: dict[str, float] = {}
        last_times: dict[str, pd.Timestamp] = {}
        trades: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        curve: list[dict[str, Any]] = []
        current_day: date | None = None
        day_open_equity = float(capital)
        portfolio_halted = False
        risk_cfg = self.config.risk
        setting = self.config.strategies

        for timestamp in sorted(events):
            batch = sorted(events[timestamp], key=lambda item: item[0])
            session_date = timestamp.date()

            if current_day is not None and session_date != current_day:
                for symbol in list(positions):
                    position = positions[symbol]
                    if strategy.intraday_only:
                        raw_exit = last_prices.get(symbol, position.entry)
                        trade = self._close(position, last_times.get(symbol, position.entry_time), raw_exit, "session_end")
                        cash += trade["exit_cash_flow"]
                        trades.append(trade)
                        del positions[symbol]
            if current_day != session_date:
                current_day = session_date
                day_open_equity = self._portfolio_value(cash, positions, last_prices)

            # At the open, only the new open prices are known. Process gap exits and
            # size queued signals before looking at this candle's high, low, or close.
            opening_prices = dict(last_prices)
            positioned_before_open = set(positions)
            for symbol, row, _ in batch:
                opening_prices[symbol] = float(row.open)
                last_times[symbol] = timestamp

            for symbol, row, _ in batch:
                position = positions.get(symbol)
                if position is None:
                    continue
                exit_price, reason = self._gap_exit(position, row, exit_mode)
                if exit_price is not None:
                    if reason == "partial_target":
                        quantity = min(position.quantity - 1, max(1, floor(position.initial_quantity * setting.partial_exit_fraction)))
                        if quantity > 0:
                            leg = self._execute_exit_leg(position, exit_price, quantity)
                            cash += leg["cash_flow"]
                            position.partial_exits += 1
                            position.exit_reasons.append("partial_target")
                            position.stop = max(position.stop, position.entry) if position.direction > 0 else min(position.stop, position.entry)
                            continue
                        reason = "target"
                    trade = self._close(position, timestamp, exit_price, reason or "gap_exit")
                    cash += trade["exit_cash_flow"]
                    trades.append(trade)
                    del positions[symbol]

            opening_equity = self._portfolio_value(cash, positions, opening_prices)
            peak = max(peak, opening_equity)
            if self._is_drawdown_breached(opening_equity, peak):
                portfolio_halted = True
                cash = self._liquidate_all(positions, cash, opening_prices, last_times, timestamp, trades, "portfolio_drawdown_kill")

            ranked = [
                (candidate.score, symbol, row, candidate)
                for symbol, row, candidate in batch if candidate is not None
            ]
            ranked.sort(key=lambda item: (-item[0], item[1]))
            for _, symbol, row, candidate in ranked:
                if portfolio_halted:
                    rejected.append(self._rejection(symbol, candidate, timestamp, "portfolio drawdown kill switch is latched"))
                    continue
                if symbol in positioned_before_open:
                    rejected.append(self._rejection(symbol, candidate, timestamp, "signal arrived while a position was open at the prior close"))
                    continue
                opening_equity = self._portfolio_value(cash, positions, opening_prices)
                day_pnl = opening_equity - day_open_equity
                peak = max(peak, opening_equity)
                sector = sectors.get(symbol, "UNKNOWN") or "UNKNOWN"
                reject_reason = self._admission_reason(
                    symbol, candidate, timestamp, opening_equity, peak, day_open_equity,
                    day_pnl, positions, strategy, sector, opening_prices,
                )
                if reject_reason:
                    rejected.append(self._rejection(symbol, candidate, timestamp, reject_reason))
                    continue

                raw_entry = float(row.open)
                entry = self.cost_model.slippage_price(raw_entry, candidate.direction, True)
                raw_stop = entry - candidate.direction * candidate.stop_distance
                risk_per_share = candidate.stop_distance + entry * self.config.costs.slippage_bps / 10000 * 2
                estimated_cost_per_share = self._rough_cost_per_share(entry, raw_stop, candidate.direction, strategy.intraday_only)
                gross_exposure = sum(abs(opening_prices.get(pos_symbol, pos.entry) * pos.quantity) for pos_symbol, pos in positions.items())
                gross_limit = max(0.0, opening_equity * risk_cfg.max_gross_exposure_fraction - gross_exposure)
                sector_exposure = sum(
                    abs(opening_prices.get(pos_symbol, pos.entry) * pos.quantity)
                    for pos_symbol, pos in positions.items() if pos.sector == sector
                )
                sector_limit = max(0.0, opening_equity * risk_cfg.max_sector_exposure_fraction - sector_exposure)
                if candidate.direction > 0:
                    cash_limit = max(0.0, cash) / (1 + self.config.costs.delivery_stamp_buy_rate + self.config.costs.exchange_transaction_rate * 1.01)
                    exposure_limit = min(gross_limit, sector_limit, cash_limit)
                else:
                    exposure_limit = min(gross_limit, sector_limit)
                sizing = position_size(
                    opening_equity,
                    entry,
                    risk_per_share,
                    risk_cfg,
                    exposure_limit,
                    estimated_cost_per_share,
                    fixed_costs=0.0 if strategy.intraday_only else self.config.costs.delivery_dp_charge_per_sell,
                )
                if sizing.quantity < 1:
                    reason = "sector exposure limit reached" if sector_limit <= entry else (sizing.reason or "position size below one share")
                    rejected.append(self._rejection(symbol, candidate, timestamp, reason))
                    continue

                entry_side = "buy" if candidate.direction > 0 else "sell"
                entry_cost = self.cost_model.order_cost(entry, sizing.quantity, entry_side, strategy.intraday_only).total
                # The delivery ledger debits long notional and credits intraday short proceeds.
                cash -= candidate.direction * sizing.quantity * entry + entry_cost
                stop = entry - candidate.direction * candidate.stop_distance
                target_multiple = 1.0 if exit_mode == "partial_trailing" else candidate.reward_risk
                target = entry + candidate.direction * candidate.stop_distance * target_multiple
                position = Position(
                    symbol=symbol,
                    direction=candidate.direction,
                    quantity=sizing.quantity,
                    initial_quantity=sizing.quantity,
                    entry_time=timestamp,
                    signal_time=candidate.signal_time,
                    entry=entry,
                    stop=stop,
                    initial_stop=stop,
                    target=target,
                    risk_budget=sizing.risk_budget,
                    risk_amount=sizing.estimated_risk,
                    strategy=candidate.source_strategy or strategy.name,
                    regime=candidate.regime,
                    score=candidate.score,
                    reason=candidate.reason,
                    sector=sector,
                    entry_cost=entry_cost,
                )
                positions[symbol] = position
                opening_prices[symbol] = raw_entry

            # The candle has now completed: evaluate its range, holding limits, and
            # session close. Stop wins when stop and target both trade in one bar.
            for symbol, row, _ in batch:
                position = positions.get(symbol)
                if position is None:
                    continue
                position.bars_held += 1
                exit_price, reason = self._range_exit(position, row, exit_mode)
                if exit_price is not None and reason == "partial_target":
                    quantity = min(position.quantity - 1, max(1, floor(position.initial_quantity * setting.partial_exit_fraction)))
                    if quantity > 0:
                        leg = self._execute_exit_leg(position, exit_price, quantity)
                        cash += leg["cash_flow"]
                        position.partial_exits += 1
                        position.exit_reasons.append("partial_target")
                        position.stop = max(position.stop, position.entry) if position.direction > 0 else min(position.stop, position.entry)
                        self._update_trailing_stop(position, row, exit_mode, setting.atr_trailing_multiple)
                        exit_price = None
                    else:
                        exit_reason = "target"
                        trade = self._close(position, timestamp, exit_price, exit_reason)
                        cash += trade["exit_cash_flow"]
                        trades.append(trade)
                        del positions[symbol]
                        continue
                if exit_price is not None:
                    trade = self._close(position, timestamp, exit_price, reason or "exit")
                    cash += trade["exit_cash_flow"]
                    trades.append(trade)
                    del positions[symbol]
                    continue
                if (
                    exit_mode != "stop_loss"
                    and not strategy.intraday_only
                    and position.bars_held >= strategy.max_holding_bars
                ):
                    trade = self._close(position, timestamp, float(row.close), "time_exit")
                    cash += trade["exit_cash_flow"]
                    trades.append(trade)
                    del positions[symbol]
                    continue
                if strategy.intraday_only and timestamp.time() >= pd.Timestamp(setting.intraday_squareoff).time():
                    trade = self._close(position, timestamp, float(row.close), "session_end")
                    cash += trade["exit_cash_flow"]
                    trades.append(trade)
                    del positions[symbol]
                    continue
                self._update_trailing_stop(position, row, exit_mode, setting.atr_trailing_multiple)

            close_prices = dict(opening_prices)
            for symbol, row, _ in batch:
                close_prices[symbol] = float(row.close)
                last_prices[symbol] = float(row.close)
            marked_equity = self._portfolio_value(cash, positions, close_prices)
            peak = max(peak, marked_equity)
            if self._is_drawdown_breached(marked_equity, peak):
                portfolio_halted = True
                cash = self._liquidate_all(positions, cash, close_prices, last_times, timestamp, trades, "portfolio_drawdown_kill")
                marked_equity = cash
                peak = max(peak, marked_equity)
            gross_exposure = sum(abs(close_prices.get(pos_symbol, pos.entry) * pos.quantity) for pos_symbol, pos in positions.items())
            curve.append({
                "timestamp": timestamp,
                "equity": marked_equity,
                "cash": cash,
                "unrealized_pnl": marked_equity - cash,
                "gross_exposure": gross_exposure,
                "open_positions": len(positions),
                "drawdown": marked_equity / peak - 1 if peak else 0.0,
            })

        last_time = max(events)
        for symbol, position in list(positions.items()):
            features = features_by_symbol[symbol]
            available = features.loc[(features.index >= position.entry_time) & (features.index.date <= end)]
            if available.empty:
                continue
            last_bar = available.iloc[-1]
            trade = self._close(position, pd.Timestamp(available.index[-1]), float(last_bar.close), "period_end")
            cash += trade["exit_cash_flow"]
            trades.append(trade)
            del positions[symbol]
        if curve:
            curve[-1]["equity"] = cash
            curve[-1]["cash"] = cash
            curve[-1]["unrealized_pnl"] = 0.0
            curve[-1]["gross_exposure"] = 0.0
            curve[-1]["open_positions"] = 0
            curve[-1]["drawdown"] = cash / max(peak, cash) - 1 if max(peak, cash) else 0.0

        trade_total = sum(float(trade["net_pnl"]) for trade in trades)
        if abs(cash - (capital + trade_total)) > max(0.02, len(trades) * 0.0001):
            raise AssertionError("cash ledger does not reconcile with closed trade P&L")
        return BacktestResult(strategy.name, capital, cash, trades, rejected, curve)

    def _admission_reason(
        self,
        symbol: str,
        candidate: Candidate,
        timestamp: pd.Timestamp,
        equity: float,
        peak: float,
        day_open_equity: float,
        day_pnl: float,
        positions: dict[str, Position],
        strategy: EquityStrategy,
        sector: str,
        marks: dict[str, float],
    ) -> str | None:
        if symbol in positions:
            return "position already open in symbol"
        if len(positions) >= self.config.risk.max_open_positions:
            return "maximum open positions reached"
        if candidate.direction < 0 and (not strategy.intraday_only or not self.config.risk.allow_short_intraday):
            return "short cash-equity position is only enabled for intraday"
        if day_pnl <= -day_open_equity * self.config.risk.max_daily_loss:
            return "daily loss limit reached"
        if peak > 0 and equity <= peak * (1 - self._drawdown_tripwire):
            return "portfolio drawdown limit reached"
        if candidate.stop_distance <= 0 or candidate.reward_risk <= 0:
            return "invalid stop distance or risk/reward"
        if self.config.costs.slippage_bps > getattr(self.config.risk, "max_expected_slippage_bps", float("inf")):
            return "expected slippage exceeds configured limit"
        if strategy.intraday_only and timestamp.time() > pd.Timestamp(self.config.strategies.entry_cutoff).time():
            return "entry cutoff passed"
        sector_value = sum(abs(marks.get(pos_symbol, pos.entry) * pos.quantity) for pos_symbol, pos in positions.items() if pos.sector == sector)
        if sector_value >= equity * self.config.risk.max_sector_exposure_fraction:
            return "sector exposure limit reached"
        return None

    def _gap_exit(self, position: Position, row: pd.Series, exit_mode: str) -> tuple[float | None, str | None]:
        opening = float(row.open)
        if (position.direction > 0 and opening <= position.stop) or (position.direction < 0 and opening >= position.stop):
            return opening, "gap_stop"
        target_active = exit_mode == "fixed_r" or (exit_mode == "partial_trailing" and position.partial_exits == 0)
        if target_active and ((position.direction > 0 and opening >= position.target) or (position.direction < 0 and opening <= position.target)):
            return position.target, "partial_target" if exit_mode == "partial_trailing" else "target"
        return None, None

    def _range_exit(self, position: Position, row: pd.Series, exit_mode: str) -> tuple[float | None, str | None]:
        high, low = float(row.high), float(row.low)
        if position.direction > 0:
            stop_hit = low <= position.stop
            target_active = exit_mode == "fixed_r" or (exit_mode == "partial_trailing" and position.partial_exits == 0)
            target_hit = target_active and high >= position.target
        else:
            stop_hit = high >= position.stop
            target_active = exit_mode == "fixed_r" or (exit_mode == "partial_trailing" and position.partial_exits == 0)
            target_hit = target_active and low <= position.target
        if stop_hit:
            return position.stop, "stop"
        if target_hit:
            return position.target, "partial_target" if exit_mode == "partial_trailing" else "target"
        return None, None

    def _update_trailing_stop(self, position: Position, row: pd.Series, exit_mode: str, atr_multiple: float) -> None:
        should_trail = exit_mode == "atr_trailing" or (exit_mode == "partial_trailing" and position.partial_exits > 0)
        atr = row.get("atr")
        if not should_trail or pd.isna(atr) or float(atr) <= 0:
            return
        proposed = float(row.close) - position.direction * float(atr) * atr_multiple
        if position.direction > 0:
            position.stop = max(position.stop, proposed)
        else:
            position.stop = min(position.stop, proposed)

    def _execute_exit_leg(self, position: Position, raw_exit: float, quantity: int) -> dict[str, float]:
        exit_price = self.cost_model.slippage_price(raw_exit, position.direction, False)
        exit_side = "sell" if position.direction > 0 else "buy"
        exit_cost = self.cost_model.order_cost(
            exit_price, quantity, exit_side, self._intraday_strategy(position.strategy), is_exit=True
        ).total
        gross = position.direction * quantity * (exit_price - position.entry)
        cash_flow = position.direction * quantity * exit_price - exit_cost
        position.realized_gross += gross
        position.exit_costs += exit_cost
        position.exit_value += quantity * exit_price
        position.closed_quantity += quantity
        position.exit_cash_flow += cash_flow
        position.quantity -= quantity
        return {"exit_price": exit_price, "gross": gross, "exit_cost": exit_cost, "cash_flow": cash_flow}

    def _close(self, position: Position, timestamp: pd.Timestamp, raw_exit: float, reason: str) -> dict[str, Any]:
        remaining = position.quantity
        prior_cash_flow = position.exit_cash_flow
        if remaining > 0:
            self._execute_exit_leg(position, raw_exit, remaining)
        total_costs = position.entry_cost + position.exit_costs
        gross = position.realized_gross
        net = gross - total_costs
        average_exit = position.exit_value / position.closed_quantity if position.closed_quantity else raw_exit
        return {
            "signal_time": position.signal_time,
            "entry_time": position.entry_time,
            "exit_time": timestamp,
            "symbol": position.symbol,
            "strategy": position.strategy,
            "sector": position.sector,
            "direction": "LONG" if position.direction > 0 else "SHORT",
            "entry": position.entry,
            "stop": position.initial_stop,
            "last_stop": position.stop,
            "target": position.target,
            "quantity": position.initial_quantity,
            "exit": average_exit,
            "gross_pnl": gross,
            "costs": total_costs,
            "net_pnl": net,
            "entry_cost": position.entry_cost,
            "exit_cost": position.exit_costs,
            "turnover": position.entry * position.initial_quantity + position.exit_value,
            "exit_cash_flow": position.exit_cash_flow - prior_cash_flow,
            "exit_cash_flow_total": position.exit_cash_flow,
            "risk_budget": position.risk_budget,
            "risk_amount": position.risk_amount,
            "holding_bars": position.bars_held,
            "holding_period": max(0.0, (timestamp - position.entry_time).total_seconds() / 86400),
            "partial_exits": position.partial_exits,
            "reason_for_signal": position.reason,
            "reason_for_exit": reason,
            "market_regime": position.regime,
            "signal_score": position.score,
        }

    def _portfolio_value(self, cash: float, positions: dict[str, Position], prices: dict[str, float]) -> float:
        value = cash
        for symbol, position in positions.items():
            raw_mark = prices.get(symbol, position.entry)
            liquidation = self.cost_model.slippage_price(raw_mark, position.direction, False)
            exit_side = "sell" if position.direction > 0 else "buy"
            exit_cost = self.cost_model.order_cost(
                liquidation, position.quantity, exit_side,
                self._intraday_strategy(position.strategy), is_exit=True,
            ).total
            value += position.direction * position.quantity * liquidation - exit_cost
        return value

    def _is_drawdown_breached(self, equity: float, peak: float) -> bool:
        return peak > 0 and equity <= peak * (1 - self._drawdown_tripwire)

    def _liquidate_all(
        self,
        positions: dict[str, Position],
        cash: float,
        prices: dict[str, float],
        times: dict[str, pd.Timestamp],
        timestamp: pd.Timestamp,
        trades: list[dict[str, Any]],
        reason: str,
    ) -> float:
        for symbol, position in list(positions.items()):
            trade = self._close(position, times.get(symbol, timestamp), prices.get(symbol, position.entry), reason)
            cash += trade["exit_cash_flow"]
            trades.append(trade)
            del positions[symbol]
        return cash

    @staticmethod
    def _intraday_strategy(name: str) -> bool:
        return name == "vwap_intraday"

    def _rough_cost_per_share(self, entry: float, stop: float, direction: int, intraday: bool) -> float:
        entry_side = "buy" if direction > 0 else "sell"
        exit_side = "sell" if direction > 0 else "buy"
        one_share_cost = self.cost_model.order_cost(entry, 1, entry_side, intraday).total
        # Delivery DP is a fixed per-exit fee and is deducted once by position_size below.
        one_share_cost += self.cost_model.order_cost(stop, 1, exit_side, intraday, is_exit=False).total
        return one_share_cost

    @staticmethod
    def _rejection(symbol: str, candidate: Candidate, timestamp: pd.Timestamp, reason: str) -> dict[str, Any]:
        return {
            "timestamp": timestamp,
            "symbol": symbol,
            "strategy": "",
            "direction": "LONG" if candidate.direction > 0 else "SHORT",
            "score": candidate.score,
            "market_regime": candidate.regime,
            "reason": reason,
        }
