from datetime import date

import pandas as pd

import pytest

from nse_research.data.providers import YahooFinanceProvider, _ticker_frame, normalize_bars, validate_and_normalize_bars
from nse_research.data.universe import remove_placeholder_symbols


def test_yfinance_multi_ticker_columns_normalize_to_india_local_bars() -> None:
    index = pd.DatetimeIndex(["2026-09-25 04:15:00"], tz="UTC")
    columns = pd.MultiIndex.from_product([["RELIANCE.NS", "TCS.NS"], ["Open", "High", "Low", "Close", "Volume"]])
    values = [[100, 102, 99, 101, 1000, 200, 202, 199, 201, 2000]]
    frame = pd.DataFrame(values, index=index, columns=columns)
    selected = normalize_bars(_ticker_frame(frame, "RELIANCE.NS", ["RELIANCE.NS", "TCS.NS"]))
    assert list(selected.columns[:5]) == ["open", "high", "low", "close", "volume"]
    assert selected.index[0] == pd.Timestamp("2026-09-25 09:45:00")
    assert selected.iloc[0]["close"] == 101


def test_bad_ohlc_rows_are_removed() -> None:
    frame = pd.DataFrame(
        {"Open": [10, 12], "High": [11, 11], "Low": [9, 10], "Close": [10, 10], "Volume": [100, 100]},
        index=pd.date_range("2026-01-01", periods=2),
    )
    normalized = normalize_bars(frame)
    assert len(normalized) == 1
    assert normalized.iloc[0]["open"] == 10


def test_bar_audit_counts_duplicates_missing_prices_bad_ohlc_and_out_of_session() -> None:
    index = pd.DatetimeIndex([
        "2026-09-25 09:15", "2026-09-25 09:15", "2026-09-25 09:20",
        "2026-09-25 09:25", "2026-09-25 08:55",
    ])
    frame = pd.DataFrame({
        "Open": [100, 101, 0, 100, 100],
        "High": [101, 102, 101, 99, 101],
        "Low": [99, 100, 0, 98, 99],
        "Close": [100, 101, 100, 100, 100],
        "Volume": [10, 11, 12, 13, 14],
    }, index=index)
    clean, audit = validate_and_normalize_bars(frame, "5m")
    assert len(clean) == 1  # duplicate timestamps keep the last valid bar
    assert audit["duplicate_timestamps"] == 1
    assert audit["invalid_price_rows"] == 1
    assert audit["invalid_ohlc_rows"] == 1
    assert audit["out_of_session_rows"] == 1
    assert audit["accepted_rows"] == 1


def test_missing_volume_column_is_rejected_instead_of_fabricated() -> None:
    frame = pd.DataFrame({"Open": [10], "High": [11], "Low": [9], "Close": [10]}, index=pd.date_range("2026-01-01", periods=1))
    with pytest.raises(ValueError, match="volume"):
        normalize_bars(frame)


def test_daily_cache_accepts_history_starting_after_warmup_request(tmp_path) -> None:
    provider = YahooFinanceProvider(tmp_path, auto_adjust=False)
    first_session = pd.Timestamp("2026-01-05")  # New listing starts after requested warm-up
    cached = pd.DataFrame(
        {"open": [10, 11, 12], "high": [11, 12, 13], "low": [9, 10, 11], "close": [10, 11, 12], "volume": [100, 100, 100]},
        index=pd.date_range(first_session, periods=3),
    )
    provider._write_cache("TEST.NS", "1d", cached)

    result = provider.history("TEST.NS", date(2025, 7, 20), date(2026, 1, 7), "1d")

    assert len(result) == 3


def test_universe_marks_placeholder_constituents_as_rejected() -> None:
    tradable, excluded = remove_placeholder_symbols(["RELIANCE.NS", "DUMMYHEG.NS"])
    assert tradable == ["RELIANCE.NS"]
    assert excluded == [{"symbol": "DUMMYHEG.NS", "reason": "placeholder constituent row; not a tradable security"}]
