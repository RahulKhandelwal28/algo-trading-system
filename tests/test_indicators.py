import numpy as np
import pandas as pd

from nse_research.indicators.features import add_features
from nse_research.strategies.equity import EquityStrategy


def sample_bars(count: int = 260) -> pd.DataFrame:
    index = pd.bdate_range("2025-01-01", periods=count)
    close = pd.Series(np.linspace(100, 150, count), index=index)
    return pd.DataFrame({
        "open": close - 0.2,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": 700000,
    }, index=index)


def test_indicators_do_not_change_prior_values_when_future_prices_change() -> None:
    original = sample_bars()
    altered = original.copy()
    altered.iloc[220:, altered.columns.get_loc("close")] *= 4
    altered.iloc[220:, altered.columns.get_loc("high")] *= 4
    first = add_features(original)
    second = add_features(altered)
    pd.testing.assert_series_equal(first.loc[:original.index[219], "ema20"], second.loc[:original.index[219], "ema20"])
    pd.testing.assert_series_equal(first.loc[:original.index[219], "atr"], second.loc[:original.index[219], "atr"])


def test_indicator_frame_contains_expected_columns_and_no_future_highs() -> None:
    frame = add_features(sample_bars())
    assert {"ema20", "ema50", "ema200", "sma20", "atr", "rsi", "adx", "vwap", "regime", "macd", "bb_upper", "roc20", "momentum20", "volatility", "prior_high30", "prior_high100"} <= set(frame.columns)
    assert frame["prior_high20"].iloc[100] == frame["high"].iloc[80:100].max()
    assert frame["prior_high30"].iloc[100] == frame["high"].iloc[70:100].max()
    assert frame["prior_high100"].iloc[100] == frame["high"].iloc[:100].max()


def test_indicator_values_cover_ema_rsi_atr_adx_and_bollinger() -> None:
    bars = sample_bars()
    frame = add_features(bars)
    assert frame["ema20"].iloc[-1] == bars.close.ewm(span=20, adjust=False, min_periods=20).mean().iloc[-1]
    assert frame["rsi"].iloc[-1] == 100.0  # monotonic advances have no average loss
    assert frame["atr"].iloc[-1] > 0
    assert 0 <= frame["adx"].iloc[-1] <= 100
    expected_middle = bars.close.rolling(20).mean().iloc[-1]
    expected_deviation = bars.close.rolling(20).std(ddof=0).iloc[-1]
    assert abs(frame["bb_middle"].iloc[-1] - expected_middle) < 1e-10
    assert abs(frame["bb_upper"].iloc[-1] - (expected_middle + 2 * expected_deviation)) < 1e-10
    assert abs(frame["bb_lower"].iloc[-1] - (expected_middle - 2 * expected_deviation)) < 1e-10


def test_market_regime_uses_india_vix_when_available() -> None:
    bars = sample_bars()
    vix = pd.Series(18.0, index=bars.index)
    vix.iloc[-1] = 30.0
    frame = add_features(bars, india_vix=vix)
    assert frame["india_vix"].iloc[-1] == 30.0
    assert frame["regime"].iloc[-1] == "HIGH_VOLATILITY"


def test_breakout_strategy_uses_previous_window_and_emits_close_time_signal() -> None:
    bars = sample_bars()
    previous_window_high = bars["high"].iloc[-21:-1].max()
    bars.iloc[-1, bars.columns.get_loc("close")] = previous_window_high + 5
    bars.iloc[-1, bars.columns.get_loc("open")] = previous_window_high + 4
    bars.iloc[-1, bars.columns.get_loc("high")] = previous_window_high + 6
    bars.iloc[-1, bars.columns.get_loc("low")] = previous_window_high + 3
    bars.iloc[-1, bars.columns.get_loc("volume")] = 3_000_000
    features = add_features(bars)
    regimes = pd.Series("BULL", index=features.index)
    strategy = EquityStrategy("breakout_20", volume_multiple=1.3)
    signals = strategy.evaluate(features, regimes)
    assert signals
    assert signals[-1].signal_time == features.index[-1]
    assert signals[-1].direction == 1


def test_previous_day_breakout_and_swing_low_stop_are_configurable() -> None:
    bars = sample_bars()
    previous_high = float(bars.high.iloc[-2])
    bars.iloc[-1, bars.columns.get_loc("close")] = previous_high + 5
    bars.iloc[-1, bars.columns.get_loc("open")] = previous_high + 4
    bars.iloc[-1, bars.columns.get_loc("high")] = previous_high + 6
    bars.iloc[-1, bars.columns.get_loc("low")] = previous_high + 3
    bars.iloc[-1, bars.columns.get_loc("volume")] = 3_000_000
    features = add_features(bars)
    regimes = pd.Series("BULL", index=features.index)
    strategy = EquityStrategy("breakout_previous_day", volume_multiple=1.3, stop_mode="swing_low")
    signals = strategy.evaluate(features, regimes)
    assert signals
    assert signals[-1].signal_time == features.index[-1]
    assert signals[-1].stop_distance == features.close.iloc[-1] - features.prior_low10.iloc[-1]
