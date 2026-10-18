"""NSE liquidity, regime, signal, and portfolio-size scan."""

from __future__ import annotations

from math import floor
from pathlib import Path
from typing import Any

import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.config import AppConfig
from nse_research.data.universe import apply_liquidity_filter, load_sector_map
from nse_research.indicators.features import add_features, align_regime
from nse_research.risk.controls import position_size
from nse_research.risk.controls import portfolio_drawdown_tripwire
from nse_research.strategies.equity import EquityStrategy


def scan_universe(
    bars_by_symbol: dict[str, pd.DataFrame],
    benchmark_bars: pd.DataFrame | None,
    strategies: list[EquityStrategy],
    config: AppConfig,
    capital: float | None = None,
    existing_positions: list[dict[str, Any]] | None = None,
    realized_day_pnl: float = 0.0,
    india_vix_bars: pd.DataFrame | None = None,
    portfolio_drawdown_pct: float = 0.0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    capital = capital or config.backtest.capital
    vix_close = india_vix_bars["close"] if india_vix_bars is not None and not india_vix_bars.empty else None
    benchmark_features = add_features(
        benchmark_bars, india_vix=vix_close,
        india_vix_high_threshold=config.regime.india_vix_high_threshold,
    ) if benchmark_bars is not None and not benchmark_bars.empty else None
    candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for symbol, bars in bars_by_symbol.items():
        liquidity_bars = bars
        if strategies and strategies[0].intraday_only:
            working = bars.copy()
            working["session"] = [pd.Timestamp(stamp).date() for stamp in working.index]
            liquidity_bars = working.groupby("session").agg(
                open=("open", "first"), high=("high", "max"), low=("low", "min"),
                close=("close", "last"), volume=("volume", "sum"),
            )
        _, liquidity_reason = apply_liquidity_filter(
            liquidity_bars,
            20 if strategies and strategies[0].intraday_only else config.universe.min_history_rows,
            config.universe.min_avg_volume,
            config.universe.min_avg_traded_value,
            config.universe.min_price,
            config.universe.liquidity_lookback,
        )
        if liquidity_reason:
            rejected.append({"symbol": symbol, "strategy": "", "timestamp": bars.index[-1], "reason": liquidity_reason})
            continue
        features = add_features(bars)
        regimes = align_regime(features, benchmark_features)
        for strategy in strategies:
            signals = strategy.evaluate(features, regimes)
            signal = next((item for item in reversed(signals) if item.signal_time == features.index[-1]), None)
            if signal is None:
                rejected.append({"symbol": symbol, "strategy": strategy.name, "timestamp": features.index[-1], "reason": "no completed-bar signal met this strategy's filters"})
                continue
            if config.costs.slippage_bps > config.risk.max_expected_slippage_bps:
                rejected.append({"symbol": symbol, "strategy": strategy.name, "timestamp": features.index[-1], "reason": "expected slippage exceeds configured limit"})
                continue
            estimate_entry = float(features["close"].iloc[-1])
            entry = config_cost_slippage(config, estimate_entry, signal.direction)
            stop = entry - signal.direction * signal.stop_distance
            target = entry + signal.direction * signal.stop_distance * signal.reward_risk
            stop_risk = abs(entry - stop) + entry * config.costs.slippage_bps / 10000 * 2
            side = "buy" if signal.direction > 0 else "sell"
            close_side = "sell" if signal.direction > 0 else "buy"
            costs_per_share = CostModel(config.costs).order_cost(entry, 1, side, strategy.intraday_only).total
            costs_per_share += CostModel(config.costs).order_cost(stop, 1, close_side, strategy.intraday_only, is_exit=False).total
            sizing = position_size(
                capital,
                entry,
                stop_risk,
                config.risk,
                capital * config.risk.max_single_position_fraction,
                costs_per_share,
                fixed_costs=0.0 if strategy.intraday_only else config.costs.delivery_dp_charge_per_sell,
            )
            if sizing.quantity < 1:
                rejected.append({"symbol": symbol, "strategy": strategy.name, "timestamp": signal.signal_time, "reason": sizing.reason or "quantity below one share"})
                continue
            candidates.append({
                "symbol": symbol,
                "strategy": strategy.name,
                "direction": "LONG" if signal.direction > 0 else "SHORT",
                "signal_time": signal.signal_time,
                "entry_estimate": entry,
                "stop_loss": stop,
                "target": target,
                "risk_reward": signal.reward_risk,
                "quantity": sizing.quantity,
                "maximum_loss": sizing.estimated_risk,
                "signal_score": signal.score,
                "market_regime": signal.regime,
                "reason": signal.reason,
            })
    candidates.sort(key=lambda candidate: (-candidate["signal_score"], candidate["symbol"], candidate["strategy"]))
    return _apply_portfolio_limits(
        candidates, rejected, bars_by_symbol, config, capital,
        existing_positions or [], realized_day_pnl, portfolio_drawdown_pct,
    )


def _apply_portfolio_limits(
    candidates: list[dict[str, Any]],
    rejected: list[dict[str, Any]],
    bars_by_symbol: dict[str, pd.DataFrame],
    config: AppConfig,
    capital: float,
    existing_positions: list[dict[str, Any]],
    realized_day_pnl: float,
    portfolio_drawdown_pct: float = 0.0,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sector_file = Path(config.universe.symbols_file)
    if not sector_file.is_absolute():
        sector_file = Path.cwd() / sector_file
    sectors = load_sector_map(sector_file)
    open_symbols = {str(item.get("symbol", "")) for item in existing_positions}
    exposures: dict[str, float] = {}
    total_exposure = 0.0
    for item in existing_positions:
        symbol = str(item.get("symbol", ""))
        sector = str(item.get("sector") or sectors.get(symbol, "UNKNOWN"))
        bars = bars_by_symbol.get(symbol)
        mark = float(bars.close.iloc[-1]) if bars is not None and not bars.empty else float(item.get("entry", 0))
        exposure = abs(mark * int(item.get("quantity", 0)))
        total_exposure += exposure
        exposures[sector] = exposures.get(sector, 0.0) + exposure

    accepted: list[dict[str, Any]] = []
    if portfolio_drawdown_pct >= portfolio_drawdown_tripwire(config.risk):
        for item in candidates:
            rejected.append({"symbol": item["symbol"], "strategy": item["strategy"], "timestamp": item["signal_time"], "reason": "portfolio drawdown limit reached in paper portfolio"})
        return accepted, rejected
    if realized_day_pnl <= -capital * config.risk.max_daily_loss:
        for item in candidates:
            rejected.append({"symbol": item["symbol"], "strategy": item["strategy"], "timestamp": item["signal_time"], "reason": "daily loss limit reached in paper portfolio"})
        return accepted, rejected

    cash = max(0.0, capital - sum(
        abs(float(item.get("entry", 0)) * int(item.get("quantity", 0)))
        for item in existing_positions if item.get("direction", "LONG") == "LONG"
    ))
    for item in candidates:
        symbol = item["symbol"]
        sector = sectors.get(symbol, "UNKNOWN")
        if symbol in open_symbols:
            rejected.append({"symbol": symbol, "strategy": item["strategy"], "timestamp": item["signal_time"], "reason": "position already open in paper portfolio"})
            continue
        if len(existing_positions) + len(accepted) >= config.risk.max_open_positions:
            rejected.append({"symbol": symbol, "strategy": item["strategy"], "timestamp": item["signal_time"], "reason": "maximum open positions reached"})
            continue
        equity = max(0.0, capital + realized_day_pnl)
        gross_remaining = max(0.0, equity * config.risk.max_gross_exposure_fraction - total_exposure)
        sector_remaining = max(0.0, equity * config.risk.max_sector_exposure_fraction - exposures.get(sector, 0.0))
        position_cap = equity * config.risk.max_single_position_fraction
        entry = float(item["entry_estimate"])
        capacity = min(gross_remaining, sector_remaining, position_cap)
        if item["direction"] == "LONG":
            capacity = min(capacity, cash)
        maximum_quantity = floor(capacity / entry) if entry > 0 else 0
        old_quantity = int(item["quantity"])
        quantity = min(old_quantity, maximum_quantity)
        if quantity < 1:
            reason = "sector exposure limit reached" if sector_remaining < entry else "portfolio exposure or cash limit reached"
            rejected.append({"symbol": symbol, "strategy": item["strategy"], "timestamp": item["signal_time"], "reason": reason})
            continue
        item["sector"] = sector
        item["quantity"] = quantity
        item["maximum_loss"] = float(item["maximum_loss"]) * quantity / old_quantity
        value = entry * quantity
        total_exposure += value
        exposures[sector] = exposures.get(sector, 0.0) + value
        if item["direction"] == "LONG":
            cash -= value
        accepted.append(item)
        open_symbols.add(symbol)
    return accepted, rejected


def config_cost_slippage(config: AppConfig, price: float, direction: int) -> float:
    return price * (1 + direction * config.costs.slippage_bps / 10000)
