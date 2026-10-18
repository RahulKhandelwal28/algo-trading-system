from __future__ import annotations

import csv
import hashlib
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from nse_research.forward.calendar import NseTradingCalendar
from nse_research.intraday import (
    BarValidationError,
    ConflictingDuplicateBar,
    CsvIntradayDataProvider,
    IndicatorSnapshot,
    IntradayBar,
    IntradayBarStore,
    IntradayIndicatorState,
    IntradayReplay,
    IntradayScanner,
    IntradayScannerConfig,
    ObservationAudit,
    ReplayOrderError,
    StrategySettings,
    calculate_intraday_indicators,
    classify_observations,
    load_frozen_universe,
    intraday_bar_from_realtime_candle,
    validate_intraday_bar,
)
from nse_research.intraday.strategies import (
    breakout_5m,
    candidate_to_signal,
    mean_reversion,
    momentum,
    opening_range_breakout,
    volume_breakout,
    vwap_pullback,
    vwap_trend,
)


IST = ZoneInfo("Asia/Kolkata")
SESSION = date(2026, 9, 28)


def stamp(offset_minutes: int, *, day: date = SESSION, timeframe: str = "1m") -> datetime:
    return datetime.combine(day, datetime.strptime("09:15", "%H:%M").time(), IST) + timedelta(
        minutes=offset_minutes * (5 if timeframe == "5m" else 1)
    )


def bar(
    *, symbol: str = "RELIANCE", timeframe: str = "1m", offset: int = 0,
    day: date = SESSION, price: float = 100.0, open_price: float | None = None,
    high: float | None = None, low: float | None = None, volume: float | None = 10.0,
    volume_kind: str = "BAR", status: str = "CLOSED", source: str = "SYNTHETIC_TEST_DATA",
    bar_status: str = "IMMUTABLE_CLOSED",
) -> IntradayBar:
    op = price if open_price is None else open_price
    return IntradayBar(
        symbol=symbol, exchange="NSE", series="EQ", timeframe=timeframe,
        timestamp=stamp(offset, day=day, timeframe=timeframe), open=op,
        high=max(op, price) + 1 if high is None else high,
        low=min(op, price) - 1 if low is None else low,
        close=price, volume=volume, status=status, source=source, volume_kind=volume_kind,
        bar_status=bar_status,
    )


def snapshot(
    item: IntradayBar, *, vwap: float | None = 100, ema9: float | None = 102,
    ema20: float | None = 101, ema50: float | None = 100, rsi: float | None = 60,
    atr: float | None = 2, prior_volume_avg: float | None = 100,
    opening5: tuple[float | None, float | None] = (105, 99),
    opening15: tuple[float | None, float | None] = (106, 98),
) -> IndicatorSnapshot:
    return IndicatorSnapshot(
        symbol=item.symbol, timeframe=item.timeframe, timestamp=item.end, close=item.close,
        vwap=vwap, vwap_status="AVAILABLE" if vwap is not None else "VWAP_UNAVAILABLE",
        ema9=ema9, ema20=ema20, ema50=ema50, rsi14=rsi, atr14=atr,
        volume_avg_20=100, prior_volume_avg_20=prior_volume_avg,
        opening_range_5m_high=opening5[0], opening_range_5m_low=opening5[1],
        opening_range_15m_high=opening15[0], opening_range_15m_low=opening15[1],
        bars_in_session=50, indicator_ready=True,
    )


def scanner_for(symbols: list[str], *, enabled: bool = True, timeframe: tuple[str, ...] = ("1m", "5m"), **config_kwargs):
    cfg = IntradayScannerConfig(enabled=enabled, universe_size=len(symbols), timeframes=timeframe, **config_kwargs)
    return IntradayScanner(symbols, config=cfg)


def test_repository_audit_detects_daily_intraday_tick_and_invalid_timestamps():
    daily = classify_observations([
        {"symbol": "R", "timestamp": "2026-09-28"},
        {"symbol": "R", "timestamp": "2026-09-29"},
    ])
    assert daily.classification == "DAILY_ONLY"
    assert daily.timestamp_resolution == "DATE_ONLY"
    minute = classify_observations([
        {"symbol": "R", "timestamp": "2026-09-28T09:15:00+05:30"},
        {"symbol": "R", "timestamp": "2026-09-28T09:16:00+05:30"},
        {"symbol": "R", "timestamp": "2026-09-28T09:17:00+05:30"},
    ])
    assert minute.classification == "GENUINE_INTRADAY"
    assert minute.timezone_awareness == "AWARE"
    assert minute.multi_timestamp_symbol_sessions == 1
    ticks = classify_observations([
        {"symbol": "R", "timestamp": "2026-09-28T09:15:01+05:30"},
        {"symbol": "R", "timestamp": "2026-09-28T09:15:02+05:30"},
    ])
    assert ticks.classification == "TICK_DATA"
    assert classify_observations([{"symbol": "R", "timestamp": "not-a-date"}]).classification == "INVALID"


def test_bar_normalizes_aware_utc_to_ist_without_guessing_timezone():
    item = bar()
    utc_item = replace(item, timestamp=datetime(2026, 9, 28, 3, 45, tzinfo=timezone.utc))
    normalized = validate_intraday_bar(utc_item)
    assert normalized.timestamp == datetime(2026, 9, 28, 9, 15, tzinfo=IST)
    with pytest.raises(BarValidationError, match="timezone offset"):
        validate_intraday_bar(replace(item, timestamp=datetime(2026, 9, 28, 9, 15)))


@pytest.mark.parametrize(
    ("changes", "code"),
    [
        ({"exchange": "BSE"}, "BAR_EXCHANGE_INVALID"),
        ({"series": "BE"}, "BAR_SERIES_INVALID"),
        ({"open": 102, "high": 101, "low": 99, "close": 100}, "BAR_OHLC_RELATION_INVALID"),
        ({"open": 0, "high": 101, "low": 99, "close": 100}, "BAR_PRICE_INVALID"),
        ({"volume": -1}, "BAR_VOLUME_INVALID"),
        ({"timeframe": "15m"}, "BAR_TIMEFRAME_INVALID"),
    ],
)
def test_bar_identity_ohlc_volume_and_timeframe_validation(changes, code):
    with pytest.raises(BarValidationError) as exc:
        validate_intraday_bar(replace(bar(), **changes))
    assert exc.value.code == code


def test_bar_session_calendar_rejects_holiday_special_session_and_daily_row():
    calendar = NseTradingCalendar()
    calendar.holidays[SESSION] = "fixture holiday"
    with pytest.raises(BarValidationError, match="CLOSED"):
        validate_intraday_bar(bar(), calendar=calendar)
    calendar = NseTradingCalendar()
    calendar.special_sessions[SESSION] = "fixture special session"
    with pytest.raises(BarValidationError, match="SPECIAL_SESSION"):
        validate_intraday_bar(bar(), calendar=calendar)
    daily = replace(bar(), timestamp=datetime(2026, 9, 28, tzinfo=IST))
    with pytest.raises(BarValidationError, match="09:15"):
        validate_intraday_bar(daily)


def test_duplicate_bars_are_not_overwritten_and_gaps_are_not_forward_filled():
    store = IntradayBarStore(["RELIANCE"])
    first = bar(offset=0)
    second = bar(offset=2, price=102)
    assert store.add(first)
    assert not store.add(first)
    assert store.add(second)
    assert len(store.bars()) == 2
    assert store.health()["duplicate_bars"] == 1
    assert store.health()["missing_intervals"] == 1
    with pytest.raises(ConflictingDuplicateBar):
        store.add(replace(first, close=100.5, high=102))
    assert store.health()["bars_rejected"] == 1


def test_out_of_order_bars_are_counted_and_returned_in_order():
    store = IntradayBarStore(["RELIANCE"])
    later, earlier = bar(offset=2, price=102), bar(offset=0)
    store.add(later)
    store.add(earlier)
    assert store.health()["out_of_order_bars"] == 1
    assert [item.timestamp for item in store.bars()] == [earlier.timestamp, later.timestamp]


def test_forming_bars_are_ignored_and_cannot_be_stored_as_closed():
    store = IntradayBarStore(["RELIANCE"])
    assert not store.add(bar(status="FORMING"))
    assert store.bars() == []
    assert store.health()["forming_bars_ignored"] == 1


def test_existing_realtime_candle_adapts_only_with_explicit_exchange_and_series():
    existing = SimpleNamespace(
        symbol="RELIANCE", interval="1m", start=stamp(0), open=100, high=102, low=99,
        close=101, volume=25, status="CLOSED", source="existing_realtime_engine",
    )
    normalized = intraday_bar_from_realtime_candle(existing, exchange="NSE", series="EQ")
    assert normalized.timestamp == existing.start
    assert normalized.volume_kind == "BAR"
    assert normalized.source == "existing_realtime_engine"
    with pytest.raises(BarValidationError, match="series must explicitly be EQ"):
        validate_intraday_bar(intraday_bar_from_realtime_candle(existing, exchange="NSE", series="BE"))


def test_frozen_universe_loader_supports_100_200_486_without_reordering_files():
    root = Path(__file__).resolve().parents[1]
    for size in (100, 200, 486):
        symbols = load_frozen_universe(root, size)
        assert len(symbols) == size
        assert len(set(symbols)) == size
    assert load_frozen_universe(root, 100)[0] == "RELIANCE"
    with pytest.raises(ValueError, match="100, 200, or 486"):
        load_frozen_universe(root, 99)


def test_csv_provider_requires_normalized_intraday_contract_and_streams_rows(tmp_path):
    path = tmp_path / "bars.csv"
    fields = sorted(CsvIntradayDataProvider.REQUIRED_COLUMNS)
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        item = bar()
        writer.writerow({
            "symbol": item.symbol, "exchange": item.exchange, "series": item.series,
            "timeframe": item.timeframe, "timestamp": item.timestamp.isoformat(),
            "open": item.open, "high": item.high, "low": item.low, "close": item.close,
            "volume": item.volume, "volume_kind": item.volume_kind, "status": item.status, "source": item.source,
        })
    provider = CsvIntradayDataProvider(path)
    rows = list(provider.iter_bars(["RELIANCE"], ["1m"]))
    assert rows == [item]
    with pytest.raises(ValueError, match="start bound must include a timezone"):
        list(provider.iter_bars(["RELIANCE"], ["1m"], start=datetime(2026, 9, 28, 9, 15)))


def test_vwap_is_session_anchored_volume_weighted_and_unavailable_without_bar_volume():
    state = IntradayIndicatorState("1m")
    first = bar(offset=0, open_price=100, high=102, low=99, price=101, volume=10)
    second = bar(offset=1, open_price=101, high=103, low=100, price=102, volume=30)
    one = state.update(first)
    two = state.update(second)
    expected = (((102 + 99 + 101) / 3) * 10 + ((103 + 100 + 102) / 3) * 30) / 40
    assert one.vwap == pytest.approx(100.6666666667)
    assert two.vwap == pytest.approx(expected)
    unknown = IntradayIndicatorState("1m")
    assert unknown.update(bar(volume=None, volume_kind="UNKNOWN")).vwap is None
    assert unknown.update(bar(offset=1, volume=100, volume_kind="BAR")).vwap is None


def test_ema_rsi_atr_and_volume_average_have_explicit_warmups():
    state = IntradayIndicatorState("1m")
    snapshots = []
    for i in range(50):
        snapshots.append(state.update(bar(offset=i, price=100 + i * 0.1, volume=10 + i)))
    assert snapshots[7].ema9 is None
    assert snapshots[8].ema9 is not None
    assert snapshots[18].ema20 is None
    assert snapshots[19].ema20 is not None
    assert snapshots[48].ema50 is None
    assert snapshots[49].ema50 is not None
    assert snapshots[13].atr14 is not None
    assert snapshots[13].rsi14 is None
    assert snapshots[14].rsi14 is not None
    assert snapshots[18].volume_avg_20 is None
    assert snapshots[19].volume_avg_20 == pytest.approx(mean(range(10, 30)))
    assert snapshots[20].prior_volume_avg_20 == pytest.approx(mean(range(10, 30)))
    assert snapshots[49].indicator_ready is True


def test_opening_ranges_use_actual_complete_bars_only():
    state = IntradayIndicatorState("1m")
    snaps = [state.update(bar(offset=i, price=100 + i, high=101 + i, low=99 - i)) for i in range(15)]
    assert snaps[3].opening_range_5m_high is None
    assert snaps[4].opening_range_5m_high == 105
    assert snaps[4].opening_range_5m_low == 95
    assert snaps[13].opening_range_15m_high is None
    assert snaps[14].opening_range_15m_high == 115
    assert snaps[14].opening_range_15m_low == 85
    missing = IntradayIndicatorState("5m")
    missing.update(bar(timeframe="5m", offset=0, price=100))
    final = missing.update(bar(timeframe="5m", offset=2, price=105))
    assert final.opening_range_15m_high is None


def test_indicator_state_resets_between_sessions_and_never_carries_vwap_or_ema():
    state = IntradayIndicatorState("1m")
    for i in range(50):
        state.update(bar(offset=i, price=100 + i))
    next_day = state.update(bar(day=date(2026, 9, 29), offset=0, price=200))
    assert next_day.bars_in_session == 1
    assert next_day.ema9 is None
    assert next_day.ema50 is None
    assert next_day.vwap == pytest.approx(200)


def test_indicator_batch_utility_orders_bars_without_daily_conversion():
    bars = [bar(offset=1), bar(offset=0)]
    result = calculate_intraday_indicators(bars)
    assert [snapshot.timestamp for snapshot in result] == [stamp(0) + timedelta(minutes=1), stamp(1) + timedelta(minutes=1)]


def test_vwap_trend_requires_price_above_vwap_fast_ema_and_positive_momentum():
    previous = [bar(offset=0, price=101)]
    current = bar(offset=1, price=102)
    found = vwap_trend(current, snapshot(current, vwap=100, ema9=102, ema20=101), previous)
    assert found and found.strategy == "vwap_trend"
    assert vwap_trend(current, snapshot(current, vwap=103), previous) is None


def test_vwap_pullback_requires_touch_and_closed_reclaim():
    settings = StrategySettings()
    previous = [bar(offset=0, price=101, low=99)]
    previous_snapshots = [snapshot(previous[0], vwap=100, ema9=101, ema20=100)]
    current = bar(offset=1, price=103, low=101)
    found = vwap_pullback(current, snapshot(current, vwap=100, ema9=103, ema20=101), previous, previous_snapshots, settings)
    assert found and "CLOSED_BAR_RECLAIM" in found.reason_codes


def test_orb_5m_and_15m_use_completed_opening_ranges_and_closed_confirmation():
    settings = StrategySettings()
    previous5 = [bar(timeframe="5m", offset=0, price=104)]
    current5 = bar(timeframe="5m", offset=1, price=106)
    assert opening_range_breakout(current5, snapshot(current5, opening5=(105, 99)), previous5, window_minutes=5)
    assert opening_range_breakout(current5, snapshot(current5, opening5=(107, 99)), previous5, window_minutes=5) is None
    previous15 = [bar(timeframe="5m", offset=i, price=105 + i) for i in range(3)]
    current15 = bar(timeframe="5m", offset=3, price=110)
    found = opening_range_breakout(current15, snapshot(current15, opening15=(109, 98)), previous15, window_minutes=15)
    assert found and found.strategy == "orb_5m_15min"


def test_prior_completed_5m_breakout_does_not_use_current_bar_range_as_threshold():
    settings = StrategySettings(breakout_lookback_5m_bars=3)
    previous = [bar(timeframe="5m", offset=i, price=100 + i) for i in range(3)]
    current = bar(timeframe="5m", offset=3, price=105, high=1000)
    found = breakout_5m(current, snapshot(current), previous, settings)
    assert found and found.confidence_metadata["prior_range_high"] == max(item.high for item in previous)


def test_momentum_uses_closed_bars_and_configurable_rsi_filter():
    previous = [bar(offset=i, price=100 + i) for i in range(3)]
    current = bar(offset=3, price=105)
    settings = StrategySettings(momentum_lookback_bars=3)
    assert momentum(current, snapshot(current, ema9=104, ema20=103), previous, settings)
    filtered = StrategySettings(momentum_lookback_bars=3, momentum_rsi_filter_enabled=True, momentum_rsi_min=70)
    assert momentum(current, snapshot(current, ema9=104, ema20=103, rsi=60), previous, filtered) is None


def test_mean_reversion_requires_prior_oversold_condition_and_bullish_confirmation():
    settings = StrategySettings()
    previous = [bar(offset=0, price=94)]
    previous_snapshots = [snapshot(previous[0], vwap=100, rsi=30, atr=2)]
    current = bar(offset=1, open_price=95, price=97)
    assert mean_reversion(current, snapshot(current, vwap=100, rsi=40, atr=2), previous, previous_snapshots, settings)
    bearish = bar(offset=1, open_price=98, price=97)
    assert mean_reversion(bearish, snapshot(bearish, vwap=100, atr=2), previous, previous_snapshots, settings) is None


def test_volume_breakout_requires_real_bar_volume_and_prior_average():
    settings = StrategySettings(breakout_lookback_5m_bars=3, volume_breakout_multiple=1.5)
    previous = [bar(timeframe="5m", offset=i, price=100 + i, volume=100) for i in range(3)]
    current = bar(timeframe="5m", offset=3, price=105, volume=200)
    found = volume_breakout(current, snapshot(current, prior_volume_avg=100), previous, settings)
    assert found and "BAR_VOLUME_ABOVE_PRIOR_20_BAR_AVERAGE" in found.reason_codes
    assert volume_breakout(replace(current, volume=None, volume_kind="UNKNOWN"), snapshot(current), previous, settings) is None


def test_signal_has_deterministic_expiry_and_analytical_references_only():
    item = bar(offset=4, price=102)
    candidate = vwap_trend(item, snapshot(item), [bar(offset=3, price=101)])
    assert candidate
    signal = candidate_to_signal(candidate, item, snapshot(item, atr=2), StrategySettings(signal_ttl_bars=3))
    assert signal and signal.side == "LONG"
    assert signal.timestamp == item.end
    assert signal.valid_until == item.end + timedelta(minutes=3)
    assert signal.stop_reference < signal.reference_price < signal.target_reference
    assert not hasattr(signal, "quantity")


@pytest.mark.parametrize("size", [100, 200, 486])
def test_scanner_supports_frozen_universe_sizes_and_deterministic_symbol_order(size):
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(size - 1)]
    scanner = scanner_for(symbols, timeframe=("1m",))
    for symbol in reversed(symbols):
        scanner.handle_bar(bar(symbol=symbol))
    assert scanner.health()["symbols_expected"] == size
    assert scanner.health()["symbols_with_data"] == size
    assert [item.symbol for item in scanner.store.bars()][:2] == sorted(symbols)[:2]


def test_scanner_requires_closed_bars_rejects_unknown_symbols_and_deduplicates_setup():
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    scanner = scanner_for(symbols, timeframe=("1m",))
    assert scanner.handle_bar(bar(status="FORMING")) == []
    assert scanner.handle_bar(bar(symbol="UNKNOWN")) == []
    generated = []
    for i in range(60):
        generated.extend(scanner.handle_bar(bar(offset=i, price=100 + i * 0.5, high=101 + i * 0.5, low=99 + i * 0.5)))
    trend = [item for item in generated if item.strategy == "vwap_trend"]
    assert len(trend) == 1
    assert scanner.handle_bar(bar(offset=59, price=129.5, high=130.5, low=128.5)) == []
    health = scanner.health()
    assert health["duplicate_bars"] == 1
    assert health["forming_bars_ignored"] == 1
    assert health["bars_rejected"] == 1
    assert health["repeated_setup_suppressions"] > 0


def test_scanner_rejects_settling_and_conflicting_bars_before_indicator_or_strategy_use():
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    scanner = scanner_for(symbols, timeframe=("1m",))
    assert scanner.handle_bar(bar(bar_status="SETTLING_BAR")) == []
    assert scanner.handle_bar(bar(offset=1, bar_status="CONFLICTING_BAR")) == []
    assert scanner.health()["symbols_with_data"] == 0
    assert scanner.health()["bars_rejected"] == 2


def test_future_bars_and_volume_cannot_change_earlier_signals_or_history():
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    base = scanner_for(symbols, timeframe=("1m",))
    extended = scanner_for(symbols, timeframe=("1m",))
    prefix = [bar(offset=i, price=100 + i * 0.5, high=101 + i * 0.5, low=99 + i * 0.5, volume=100) for i in range(30)]
    for item in prefix:
        base.handle_bar(item)
        extended.handle_bar(item)
    before = tuple(base.signals)
    future = bar(offset=30, price=200, high=210, low=190, volume=10_000)
    extended.handle_bar(future)
    cutoff = prefix[-1].end
    assert tuple(s for s in extended.signals if s.timestamp <= cutoff) == before
    assert all(s.timestamp <= cutoff for s in before)


def test_scanner_session_isolation_clears_prior_day_history():
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    scanner = scanner_for(symbols, timeframe=("1m",))
    for i in range(50):
        scanner.handle_bar(bar(offset=i, price=100 + i))
    next_day = bar(day=date(2026, 9, 29), offset=0, price=200)
    scanner.handle_bar(next_day)
    assert len(scanner._history[("RELIANCE", "1m")]) == 1
    assert scanner._snapshots[("RELIANCE", "1m")][-1].ema9 is None


def test_regime_filter_is_disabled_by_default_and_fails_closed_without_same_session_context():
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    config = IntradayScannerConfig(enabled=True, universe_size=100, timeframes=("1m",), regime_filter_enabled=True)
    scanner = IntradayScanner(symbols, config=config)
    for i in range(30):
        scanner.handle_bar(bar(offset=i, price=100 + i))
    assert scanner.signals == []
    assert scanner.health()["regime_context_missing"] == 30
    assert IntradayScannerConfig().regime_filter_enabled is False


class _ReplayProvider:
    provider_name = "SYNTHETIC_TEST_DATA"

    def __init__(self, rows, on_yield=None):
        self.rows = rows
        self.on_yield = on_yield

    def iter_bars(self, symbols, timeframes, *, start=None, end=None):
        for row in self.rows:
            yield row
            if self.on_yield:
                self.on_yield(row)


def test_replay_streams_in_event_order_and_supports_1x_5x_max():
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    scanner = scanner_for(symbols, timeframe=("1m",))
    rows = [bar(offset=0), bar(offset=1, price=101)]
    sleeps = []
    replay = IntradayReplay(scanner, sleep_fn=sleeps.append)
    result = replay.run(_ReplayProvider(rows), speed="5x")
    assert result.bars_processed == 2
    assert result.first_event_time == rows[0].end
    assert result.last_event_time == rows[1].end
    assert sleeps == [12.0]
    another = IntradayReplay(scanner_for(symbols, timeframe=("1m",)), sleep_fn=sleeps.append)
    another.run(_ReplayProvider(rows), speed="max")
    assert sleeps == [12.0]
    with pytest.raises(ValueError, match="1x, 5x, or max"):
        replay.run(_ReplayProvider(rows), speed="2x")


def test_replay_rejects_out_of_order_event_time_instead_of_reordering_future_input():
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    scanner = scanner_for(symbols, timeframe=("1m",))
    rows = [bar(offset=1, price=101), bar(offset=0, price=100)]
    with pytest.raises(ReplayOrderError, match="chronological event-time order"):
        IntradayReplay(scanner, sleep_fn=lambda _: None).run(_ReplayProvider(rows), speed="max")


def test_scanner_and_signal_path_do_not_mutate_phase3d_ledger_or_enable_live_trading():
    root = Path(__file__).resolve().parents[1]
    ledger = root / "results/phase3d_forward_20260926_225042/paper_ledger.sqlite"
    if not ledger.exists():
        pytest.skip("local Phase 3D ledger is not present")
    before = hashlib.sha256(ledger.read_bytes()).hexdigest()
    symbols = ["RELIANCE"] + [f"SYM{i:03}" for i in range(99)]
    scanner = scanner_for(symbols, timeframe=("1m",))
    for i in range(30):
        scanner.handle_bar(bar(offset=i, price=100 + i))
    assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before
    assert all(signal.side == "LONG" for signal in scanner.signals)
    assert not any(hasattr(signal, key) for signal in scanner.signals for key in ("order_id", "quantity", "fill_price"))
    import yaml
    config = yaml.safe_load((root / "config/phase3d_personal.yaml").read_text())
    assert config["live_trading"]["enabled"] is False
    assert config["realtime"]["enabled"] is True
    assert config["realtime"]["provider"] == "jugaad_data"
