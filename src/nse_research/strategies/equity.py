"""Transparent equity signal rules for research comparison."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time

import numpy as np
import pandas as pd

from nse_research.strategies.base import Candidate


@dataclass
class EquityStrategy:
    name: str
    reward_risk: float = 1.5
    atr_multiple: float = 2.0
    adx_minimum: float = 20
    volume_multiple: float = 1.3
    max_holding_bars: int = 20
    intraday_only: bool = False
    entry_cutoff: str = "14:45"
    allow_short: bool = False
    min_avg_volume: float = 0.0
    min_avg_traded_value: float = 0.0
    min_price: float = 0.0
    stop_mode: str = "atr"
    score_weights: dict[str, float] | None = None
    breakout_buffer_atr: float = 0.1

    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        if features.empty:
            return []
        close, high, low = features["close"], features["high"], features["low"]
        regime = regimes.reindex(features.index).fillna("UNKNOWN").astype(str)
        valid = features[["atr", "ema20", "ema50", "adx", "rsi", "volume_ratio", "close"]].notna().all(axis=1)
        valid &= features["atr"] > 0
        valid &= close >= self.min_price
        if not self.intraday_only:
            valid &= features["volume_mean20"].notna() & (features["volume_mean20"] >= self.min_avg_volume)
            valid &= features["traded_value_mean20"].notna() & (features["traded_value_mean20"] >= self.min_avg_traded_value)

        trend_up = (close > features["ema20"]) & (features["ema20"] > features["ema50"])
        volume_ok = features["volume_ratio"] >= self.volume_multiple
        strength_ok = features["adx"] >= self.adx_minimum
        long_signal = pd.Series(False, index=features.index)
        short_signal = pd.Series(False, index=features.index)
        reason = ""

        if self.name == "trend_following":
            long_signal = valid & trend_up & strength_ok & (features["ema50"] > features["ema200"]) & regime.isin(["BULL", "STRONG_BULL"])
            reason = "EMA20 > EMA50 > EMA200, ADX and NIFTY bull-regime filters"
        elif self.name == "momentum":
            long_signal = valid & trend_up & volume_ok & (close > features["prior_high20"] + self.breakout_buffer_atr * features["atr"]) & (features["macd"] > features["macd_signal"]) & features["rsi"].between(50, 78) & regime.isin(["BULL", "STRONG_BULL"])
            reason = "trend alignment, 20-bar close breakout, volume expansion, MACD confirmation"
        elif self.name.startswith("breakout_") and self.name != "breakout_previous_day":
            lookback = self.name.removeprefix("breakout_")
            field = f"prior_high{lookback}"
            market_bull = regime.isin(["BULL", "STRONG_BULL"])
            long_signal = valid & features[field].notna() & (close > features[field] + self.breakout_buffer_atr * features["atr"]) & volume_ok & market_bull
            reason = f"{lookback}-bar close breakout, volume confirmation, benchmark regime"
        elif self.name == "breakout_previous_day":
            level = features["prior_high1"]
            long_signal = valid & level.notna() & (close > level + self.breakout_buffer_atr * features["atr"]) & volume_ok & regime.isin(["BULL", "STRONG_BULL"])
            reason = "previous-day high close breakout with volume and NIFTY bull-regime confirmation"
        elif self.name == "pullback":
            long_signal = (
                valid & trend_up & features["prior_high1"].notna() & (close > features["prior_high1"])
                & (low <= features["ema20"] + 0.5 * features["atr"])
                & features["rsi"].between(45, 68) & strength_ok & regime.isin(["BULL", "STRONG_BULL"])
            )
            reason = "bull trend pullback toward EMA20 followed by prior-high reclaim"
        elif self.name == "mean_reversion":
            below_band = close.shift(1) <= features["bb_lower"].shift(1)
            long_signal = valid & (regime == "SIDEWAYS") & features["bb_lower"].notna() & below_band & (close > features["bb_lower"]) & (features["rsi"] < 45)
            reason = "sideways regime, close re-enters below Bollinger band, RSI oversold"
            if self.allow_short:
                above_band = close.shift(1) >= features["bb_upper"].shift(1)
                short_signal = valid & (regime == "SIDEWAYS") & features["bb_upper"].notna() & above_band & (close < features["bb_upper"]) & (features["rsi"] > 55)
        elif self.name.startswith("swing_"):
            long_signal = valid & trend_up & strength_ok & (features["ema50"] > features["ema200"]) & (features["rsi"] >= 48) & (close > features["prior_high1"])
            reason = f"swing trend continuation; maximum hold {self.max_holding_bars} sessions"
        elif self.name == "vwap_intraday":
            clock = features.index.hour * 60 + features.index.minute
            cutoff = time.fromisoformat(self.entry_cutoff)
            before_cutoff = (clock >= 9 * 60 + 45) & (clock <= cutoff.hour * 60 + cutoff.minute)
            long_signal = valid & before_cutoff & features["vwap"].notna() & (close > features["vwap"]) & (close > features["ema20"]) & volume_ok & (
                (features["opening_range_high"].notna() & (close > features["opening_range_high"]))
                | (features["prior_day_high"].notna() & (close > features["prior_day_high"]))
            )
            if self.allow_short:
                short_signal = valid & before_cutoff & features["vwap"].notna() & (close < features["vwap"]) & (close < features["ema20"]) & volume_ok & (
                    (features["opening_range_low"].notna() & (close < features["opening_range_low"]))
                    | (features["prior_day_low"].notna() & (close < features["prior_day_low"]))
                )
            reason = "VWAP and EMA20 alignment with volume, opening-range or prior-day breakout"

        directions = pd.Series(0, index=features.index, dtype="int8")
        directions.loc[long_signal] = 1
        directions.loc[short_signal] = -1
        signal_times = features.index[directions != 0]
        candidates: list[Candidate] = []
        for timestamp in signal_times:
            row = features.loc[timestamp]
            close_price = float(row.close)
            atr = float(row.atr)
            strength = float(row.adx)
            volume_ratio = float(row.volume_ratio)
            direction = int(directions.loc[timestamp])
            stop_distance = self._stop_distance(row, direction)
            if not np.isfinite(stop_distance) or stop_distance <= 0:
                continue
            score = self._score(row, direction, str(regime.loc[timestamp]), stop_distance)
            candidates.append(Candidate(
                signal_time=pd.Timestamp(timestamp),
                direction=direction,
                stop_distance=stop_distance,
                reward_risk=self.reward_risk,
                score=score,
                regime=str(regime.loc[timestamp]),
                reason=reason,
            ))
        return candidates

    def _evaluate_row(self, timestamp: pd.Timestamp, row: pd.Series, previous: pd.Series | None, regime: str) -> Candidate | None:
        required = ["atr", "ema20", "ema50", "adx", "rsi", "volume_ratio", "close"]
        if previous is None or any(pd.isna(row.get(key)) for key in required):
            return None
        if not self.intraday_only:
            if pd.isna(row.get("volume_mean20")) or float(row.volume_mean20) < self.min_avg_volume:
                return None
            if pd.isna(row.get("traded_value_mean20")) or float(row.traded_value_mean20) < self.min_avg_traded_value:
                return None
        close = float(row.close)
        atr = float(row.atr)
        if not np.isfinite(atr) or atr <= 0:
            return None
        trend_up = close > float(row.ema20) > float(row.ema50)
        trend_down = close < float(row.ema20) < float(row.ema50)
        strength = float(row.adx)
        volume_ok = float(row.volume_ratio) >= self.volume_multiple
        prior_close = float(previous.close)
        signal_long = False
        signal_short = False
        reason = ""

        if self.name == "trend_following":
            signal_long = trend_up and strength >= self.adx_minimum and float(row.ema50) > float(row.ema200) if pd.notna(row.get("ema200")) else False
            reason = "EMA20 > EMA50 > EMA200, price above EMA20, ADX trend filter"
        elif self.name == "momentum":
            signal_long = trend_up and volume_ok and close > float(row.prior_high20) and float(row.macd) > float(row.macd_signal) and 50 <= float(row.rsi) <= 78 if pd.notna(row.get("prior_high20")) else False
            reason = "trend alignment, 20-bar close breakout, volume expansion, MACD confirmation"
        elif self.name.startswith("breakout_") and self.name != "breakout_previous_day":
            lookback = self.name.removeprefix("breakout_")
            field = f"prior_high{lookback}"
            market_bull = regime in {"BULL", "STRONG_BULL"}
            level = row.get(field)
            signal_long = pd.notna(level) and close > float(level) and volume_ok and market_bull
            reason = f"{field.replace('prior_', '').replace('_', ' ')} close breakout, volume confirmation, {regime} regime"
        elif self.name == "pullback":
            level = row.get("prior_high1")
            near_ema = float(row.low) <= float(row.ema20) + 0.5 * atr
            signal_long = trend_up and pd.notna(level) and close > float(level) and near_ema and float(row.rsi) >= 45 and float(row.rsi) <= 68 and strength >= self.adx_minimum
            reason = "bull trend pullback toward EMA20 followed by prior-high reclaim"
        elif self.name == "mean_reversion":
            previous_lower = previous.get("bb_lower", np.nan)
            signal_long = regime == "SIDEWAYS" and pd.notna(row.get("bb_lower")) and pd.notna(previous_lower) and prior_close <= float(previous_lower) and close > float(row.bb_lower) and float(row.rsi) < 45
            reason = "sideways regime, close re-enters below Bollinger band, RSI oversold"
            signal_short = self.allow_short and regime == "SIDEWAYS" and pd.notna(row.get("bb_upper")) and prior_close >= float(previous.get("bb_upper", np.nan)) and close < float(row.bb_upper) and float(row.rsi) > 55
        elif self.name.startswith("swing_"):
            signal_long = trend_up and strength >= self.adx_minimum and float(row.ema50) > float(row.ema200) and float(row.rsi) >= 48 and close > float(row.prior_high1)
            reason = f"swing trend continuation; maximum hold {self.max_holding_bars} sessions"
        elif self.name == "vwap_intraday":
            if not self.intraday_only or pd.isna(row.get("vwap")):
                return None
            clock = timestamp.time() if hasattr(timestamp, "time") else time(0, 0)
            cutoff = time.fromisoformat(self.entry_cutoff)
            if clock > cutoff:
                return None
            signal_long = close > float(row.vwap) and close > float(row.ema20) and volume_ok
            signal_short = self.allow_short and close < float(row.vwap) and close < float(row.ema20) and volume_ok
            reason = "close confirms VWAP and EMA20 alignment with volume expansion"

        direction = 1 if signal_long else (-1 if signal_short else 0)
        if direction == 0:
            return None
        risk_distance = max(atr * self.atr_multiple, close * 0.005)
        score = float(np.clip(50 + max(0.0, strength - self.adx_minimum) * 0.8 + min(25.0, max(0.0, float(row.volume_ratio) - 1) * 15), 0, 100))
        return Candidate(
            signal_time=pd.Timestamp(timestamp),
            direction=direction,
            stop_distance=risk_distance,
            reward_risk=self.reward_risk,
            score=score,
            regime=regime,
            reason=reason,
        )

    def _stop_distance(self, row: pd.Series, direction: int) -> float:
        close, atr = float(row.close), float(row.atr)
        if self.stop_mode == "swing_low" and direction > 0 and pd.notna(row.get("prior_low10")):
            distance = close - float(row.prior_low10)
            if distance > 0:
                return distance
        return max(atr * self.atr_multiple, close * 0.005)

    def _score(self, row: pd.Series, direction: int, regime: str, stop_distance: float) -> float:
        close, rsi = float(row.close), float(row.rsi)
        trend = 100.0 if close > float(row.ema20) > float(row.ema50) > float(row.ema200) else 60.0 if close > float(row.ema20) > float(row.ema50) else 35.0
        momentum = float(np.clip(100 - abs(rsi - 60) * 2, 0, 100))
        if (direction > 0 and float(row.macd) <= float(row.macd_signal)) or (direction < 0 and float(row.macd) >= float(row.macd_signal)):
            momentum *= 0.65
        volume = float(np.clip(float(row.volume_ratio) / max(self.volume_multiple, 0.1) * 70, 0, 100))
        regime_score = 100.0 if regime in {"STRONG_BULL", "STRONG_BEAR"} else 75.0 if regime in {"BULL", "BEAR"} else 50.0 if regime == "SIDEWAYS" else 20.0
        traded_value = float(row.get("traded_value_mean20", 0) or 0)
        liquidity = float(np.clip(traded_value / max(self.min_avg_traded_value, 1.0) * 70, 0, 100)) if self.min_avg_traded_value else 75.0
        atr_pct = float(row.get("atr_pct", 0) or 0)
        volatility = float(np.clip(100 - max(0.0, atr_pct - 0.02) * 2000, 0, 100))
        risk_reward = float(np.clip(self.reward_risk / 3 * 100, 0, 100))
        components = {
            "trend": trend, "momentum": momentum, "volume": volume,
            "regime": regime_score, "liquidity": liquidity,
            "volatility": volatility, "risk_reward": risk_reward,
        }
        weights = self.score_weights or {name: 1 / len(components) for name in components}
        active = {name: max(0.0, float(weight)) for name, weight in weights.items() if name in components}
        total_weight = sum(active.values())
        if total_weight <= 0:
            return 0.0
        return float(np.clip(sum(components[name] * weight for name, weight in active.items()) / total_weight, 0, 100))


def build_strategies(
    names: list[str],
    reward_risk: float,
    atr_multiple: float,
    adx_minimum: float,
    volume_multiple: float,
    entry_cutoff: str,
    interval: str,
    allow_short_intraday: bool,
    min_avg_volume: float = 0.0,
    min_avg_traded_value: float = 0.0,
    min_price: float = 0.0,
    stop_mode: str = "atr",
    score_weights: dict[str, float] | None = None,
) -> list[EquityStrategy]:
    intraday = interval not in {"1d", "1wk", "1mo"}
    strategies: list[EquityStrategy] = []
    for name in names:
        if intraday and name != "vwap_intraday":
            continue
        if name == "vwap_intraday" and not intraday:
            continue
        if name == "vwap_intraday":
            strategies.append(EquityStrategy(name, reward_risk, atr_multiple, adx_minimum, volume_multiple, 1, True, entry_cutoff, allow_short_intraday, min_avg_volume, min_avg_traded_value, min_price, stop_mode, score_weights))
        elif name.startswith("swing_"):
            try:
                maximum = int(name.split("_")[2])
            except (IndexError, ValueError):
                maximum = 10
            strategies.append(EquityStrategy(name, reward_risk, atr_multiple, adx_minimum, volume_multiple, maximum, False, entry_cutoff, False, min_avg_volume, min_avg_traded_value, min_price, stop_mode, score_weights))
        else:
            strategies.append(EquityStrategy(name, reward_risk, atr_multiple, adx_minimum, volume_multiple, 20, False, entry_cutoff, False, min_avg_volume, min_avg_traded_value, min_price, stop_mode, score_weights))
    return strategies
