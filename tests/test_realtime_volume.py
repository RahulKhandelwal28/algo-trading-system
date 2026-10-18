from datetime import date, datetime, timedelta, timezone

import pytest

from nse_research.realtime.volume import CumulativeVolumeDeltaTracker


SESSION = date(2026, 9, 28)
START = datetime(2026, 9, 28, 3, 45, tzinfo=timezone.utc)


def observe(tracker, volume, seconds, *, session_date=SESSION, feed_gap=False):
    return tracker.update(
        "RELIANCE",
        volume,
        START + timedelta(seconds=seconds),
        session_date=session_date,
        feed_gap=feed_gap,
    )


def test_first_observation_sets_baseline_and_next_observation_returns_increment():
    tracker = CumulativeVolumeDeltaTracker()

    baseline = observe(tracker, 100, 0)
    next_tick = observe(tracker, 125, 1)

    assert baseline.incremental_volume is None
    assert baseline.quality_events == ("BASELINE_ESTABLISHED",)
    assert next_tick.incremental_volume == 25
    assert next_tick.quality_events == ()


def test_same_cumulative_volume_produces_zero_increment():
    tracker = CumulativeVolumeDeltaTracker()
    observe(tracker, 100, 0)

    result = observe(tracker, 100, 1)

    assert result.incremental_volume == 0


def test_counter_decrease_is_reported_without_negative_volume_and_rebased():
    tracker = CumulativeVolumeDeltaTracker()
    observe(tracker, 100, 0)

    reset = observe(tracker, 15, 1)
    resumed = observe(tracker, 20, 2)

    assert reset.incremental_volume is None
    assert reset.quality_events == ("VOLUME_COUNTER_DECREASED",)
    assert resumed.incremental_volume == 5
    assert resumed.incremental_volume >= 0


def test_feed_gap_does_not_assign_unobserved_volume_to_a_candle():
    tracker = CumulativeVolumeDeltaTracker()
    observe(tracker, 100, 0)

    after_gap = observe(tracker, 150, 30, feed_gap=True)
    next_tick = observe(tracker, 160, 31)

    assert after_gap.incremental_volume is None
    assert after_gap.quality_events == ("FEED_GAP_VOLUME_UNATTRIBUTED",)
    assert next_tick.incremental_volume == 10


def test_new_exchange_session_establishes_a_fresh_baseline():
    tracker = CumulativeVolumeDeltaTracker()
    observe(tracker, 500, 0)
    next_session = date(2026, 9, 29)

    baseline = observe(tracker, 8, 1, session_date=next_session)
    next_tick = observe(tracker, 11, 2, session_date=next_session)

    assert baseline.incremental_volume is None
    assert baseline.quality_events == ("SESSION_RESET_BASELINE",)
    assert next_tick.incremental_volume == 3


def test_out_of_order_tick_does_not_corrupt_the_cumulative_baseline():
    tracker = CumulativeVolumeDeltaTracker()
    observe(tracker, 100, 0)
    observe(tracker, 120, 5)

    late = observe(tracker, 110, 4)
    next_tick = observe(tracker, 125, 6)

    assert late.incremental_volume is None
    assert late.quality_events == ("OUT_OF_ORDER_VOLUME_OBSERVATION_IGNORED",)
    assert next_tick.incremental_volume == 5


@pytest.mark.parametrize(
    "kwargs",
    [
        {"cumulative_volume": -1},
        {"cumulative_volume": 1.5},
        {"cumulative_volume": True},
        {"source_timestamp": datetime(2026, 9, 28, 9, 15)},
        {"session_date": datetime(2026, 9, 28, 9, 15, tzinfo=timezone.utc)},
    ],
)
def test_invalid_volume_metadata_fails_closed(kwargs):
    tracker = CumulativeVolumeDeltaTracker()
    args = {
        "symbol": "RELIANCE",
        "cumulative_volume": 1,
        "source_timestamp": START,
        "session_date": SESSION,
    }
    args.update(kwargs)

    with pytest.raises(ValueError):
        tracker.update(**args)
