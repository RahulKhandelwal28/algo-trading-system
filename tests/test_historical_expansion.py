from datetime import date

import pandas as pd

from nse_research.data.baseline import run_fixed_baseline_11
from nse_research.data.expansion import inspect_symbol_history, normalize_symbol, write_data_expansion_artifacts
from nse_research.data.providers import (
    NseOfficialHistoryCsvProvider,
    YahooFinanceProvider,
    validate_and_normalize_bars,
)


def _bars(index: pd.DatetimeIndex, base: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [base] * len(index),
            "high": [base + 2] * len(index),
            "low": [base - 2] * len(index),
            "close": [base + 1] * len(index),
            "volume": [1000] * len(index),
        },
        index=index,
    )


def test_nse_price_volume_export_date_range_and_symbol_series(tmp_path) -> None:
    pd.DataFrame(
        {
            "Symbol": ["ABC", "ABC"],
            "Series": ["EQ", "BE"],
            "Date": ["01-Jan-2020", "01-Jan-2020"],
            "Open Price": ["1,000", "9,000"],
            "High Price": ["1,010", "9,010"],
            "Low Price": ["990", "8,990"],
            "Close Price": ["1,005", "9,005"],
            "Total Traded Quantity": ["2,50,000", "1"],
        }
    ).to_csv(tmp_path / "ABC.csv", index=False)
    provider = NseOfficialHistoryCsvProvider(tmp_path)

    bars = provider.history("abc.ns", date(2020, 1, 1), date(2020, 1, 2))

    assert list(bars.columns) == ["open", "high", "low", "close", "volume"]
    assert len(bars) == 1
    assert bars.iloc[0]["open"] == 1000
    assert bars.iloc[0]["volume"] == 250000


def test_nse_udiff_field_names_are_supported(tmp_path) -> None:
    pd.DataFrame(
        {
            "TckrSymb": ["ABC"],
            "SctySrs": ["EQ"],
            "TradDt": ["2026-09-25"],
            "OpnPric": [100],
            "HghPric": [105],
            "LwPric": [99],
            "ClsPric": [102],
            "TtlTradgVol": [8000],
        }
    ).to_csv(tmp_path / "ABC.NS_1d.csv", index=False)
    provider = NseOfficialHistoryCsvProvider(tmp_path)

    bars = provider.history("ABC.NS", date(2026, 9, 25), date(2026, 9, 25))

    assert len(bars) == 1
    assert bars.iloc[0]["close"] == 102


def test_nse_index_iso_dates_are_not_reinterpreted_as_day_first(tmp_path) -> None:
    pd.DataFrame(
        {
            "Date": ["2025-12-01", "2026-01-02"],
            "Open": [100, 101],
            "High": [102, 103],
            "Low": [99, 100],
            "Close": [101, 102],
        }
    ).to_csv(tmp_path / "index_NSEI.csv", index=False)
    provider = NseOfficialHistoryCsvProvider(tmp_path)

    bars = provider.history("^NSEI", date(2025, 12, 1), date(2025, 12, 1))

    assert len(bars) == 1
    assert bars.index[0].date() == date(2025, 12, 1)


def test_nse_index_export_uses_index_names_and_volume_is_not_applicable(tmp_path) -> None:
    pd.DataFrame(
        {
            "Index Name": ["NIFTY 50", "NIFTY BANK", "INDIA VIX"],
            "Date": ["25-Sep-2026"] * 3,
            "Open": [100, 200, 15],
            "High": [105, 205, 17],
            "Low": [99, 198, 14],
            "Close": [103, 202, 16],
        }
    ).to_csv(tmp_path / "index_NSEI.csv", index=False)
    provider = NseOfficialHistoryCsvProvider(tmp_path)

    bars = provider.history("^NSEI", date(2026, 9, 25), date(2026, 9, 25))

    assert len(bars) == 1
    assert bars.iloc[0]["close"] == 103
    assert bars.iloc[0]["volume"] == 0


def test_symbol_normalization_does_not_guess_company_identity() -> None:
    assert normalize_symbol(" reliance ") == "RELIANCE.NS"
    assert normalize_symbol("reliance.ns") == "RELIANCE.NS"
    assert normalize_symbol("^nsei") == "^NSEI"


def test_incremental_yahoo_cache_fetches_only_missing_edges_and_merges(tmp_path, monkeypatch) -> None:
    provider = YahooFinanceProvider(tmp_path, retries=1, min_request_interval_seconds=0)
    cached = _bars(pd.DatetimeIndex(["2022-01-03", "2022-01-04"]))
    provider._write_cache("ABC.NS", "1d", cached)
    calls = []

    def download(symbol, start, end, interval):
        calls.append((start, end))
        if start == date(2022, 1, 1):
            return _bars(pd.DatetimeIndex(["2022-01-01", "2022-01-02"]))
        return _bars(pd.DatetimeIndex(["2022-01-05", "2022-01-06", "2022-01-07", "2022-01-08"]))

    monkeypatch.setattr(provider, "_download_symbol", download)

    result = provider.history("ABC.NS", date(2022, 1, 1), date(2022, 1, 8))

    assert calls == [(date(2022, 1, 1), date(2022, 1, 2)), (date(2022, 1, 5), date(2022, 1, 8))]
    assert len(result) == 8
    assert result.index.min().date() == date(2022, 1, 1)
    assert result.index.max().date() == date(2022, 1, 8)


def test_valid_cache_reuse_skips_provider_call(tmp_path, monkeypatch) -> None:
    provider = YahooFinanceProvider(tmp_path, retries=1, min_request_interval_seconds=0)
    cached = _bars(pd.date_range("2022-01-01", periods=7, freq="D"))
    provider._write_cache("ABC.NS", "1d", cached)

    def fail(*args, **kwargs):
        raise AssertionError("covered cache should be reused")

    monkeypatch.setattr(provider, "_download_symbol", fail)
    result = provider.history("ABC.NS", date(2022, 1, 1), date(2022, 1, 7))

    assert len(result) == 7


def test_retry_recovers_provider_failure(tmp_path, monkeypatch) -> None:
    provider = YahooFinanceProvider(tmp_path, retries=2, min_request_interval_seconds=0)
    monkeypatch.setattr("nse_research.data.providers.time.sleep", lambda _seconds: None)
    attempts = {"count": 0}

    def flaky(symbol, start, end, interval):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise OSError("temporary provider failure")
        return _bars(pd.DatetimeIndex(["2022-01-03"]))

    monkeypatch.setattr(provider, "_download_symbol", flaky)
    result = provider.history("ABC.NS", date(2022, 1, 3), date(2022, 1, 3))

    assert attempts["count"] == 2
    assert len(result) == 1
    assert "ABC.NS" not in provider.download_failures


def test_failed_provider_is_reported_without_losing_cache(tmp_path, monkeypatch) -> None:
    provider = YahooFinanceProvider(tmp_path, retries=1, min_request_interval_seconds=0)
    cached = _bars(pd.DatetimeIndex(["2022-01-03"]))
    provider._write_cache("ABC.NS", "1d", cached)

    def fail(*args, **kwargs):
        raise OSError("offline")

    monkeypatch.setattr(provider, "_download_symbol", fail)
    result = provider.history("ABC.NS", date(2021, 1, 1), date(2022, 1, 5))

    assert len(result) == 1
    assert "offline" in provider.download_failures["ABC.NS"]


def test_quality_check_counts_duplicate_rows_and_missing_observed_sessions() -> None:
    raw = _bars(pd.DatetimeIndex(["2022-01-03", "2022-01-03", "2022-01-05"]))
    normalized, audit = validate_and_normalize_bars(raw, "1d")
    observed_nifty = pd.DatetimeIndex(["2022-01-03", "2022-01-04", "2022-01-05"])

    quality = inspect_symbol_history("ABC.NS", raw, normalized, audit, observed_nifty)

    assert quality["duplicate_timestamps"] == 1
    assert quality["duplicate_rows"] == 2
    assert quality["missing_observed_nifty_sessions"] == 1
    assert quality["corporate_action_fields_available"] is False
    assert quality["split_event_rows"] is None


def test_manifest_and_quality_outputs_are_generated_data_only(tmp_path) -> None:
    cache = tmp_path / "cache"
    output = tmp_path / "report"
    cache.mkdir()
    dates = pd.date_range("2020-01-01", periods=205, freq="B")
    bars = _bars(dates)
    bars.to_csv(cache / YahooFinanceProvider.cache_filename("AAA.NS", "1d"), index_label="timestamp")
    bars.to_csv(cache / YahooFinanceProvider.cache_filename("^NSEI", "1d"), index_label="timestamp")

    manifest = write_data_expansion_artifacts(
        project_root=tmp_path,
        output_dir=output,
        cache_dir=cache,
        requested_start=date(2020, 1, 1),
        requested_end=dates[-1].date(),
        provider="NSE official CSV test fixture",
        requested_symbols=["AAA", "DUMMYABC"],
        min_history_rows=200,
    )

    assert manifest["minimum_target_met"] is True
    assert manifest["preferred_target_met"] is True
    assert manifest["data_only_validation"]["vix_status"] == "VIX_UNAVAILABLE"
    assert all(row["symbol"] != "DUMMYABC.NS" for row in manifest["symbols_failed"])
    assert (output / "historical_data_manifest.json").exists()
    assert (output / "historical_universe_manifest.csv").exists()
    assert (output / "historical_symbol_mapping.csv").exists()
    assert (output / "historical_data_quality.csv").exists()
    assert (output / "data_quality_report.html").exists()
    assert (output / "baseline_strategy_results.csv").exists()


def test_fixed_baseline_gate_skips_when_data_only_target_fails(tmp_path) -> None:
    result = run_fixed_baseline_11(
        root=tmp_path,
        cache_dir=tmp_path / "cache",
        output_dir=tmp_path / "report",
        manifest={"data_only_validation": {"status": "TARGET_RANGE_MISSING"}},
        requested_start=date(2020, 1, 1),
        requested_end=date(2026, 9, 26),
    )

    assert result["status"] == "SKIPPED"
    assert "coverage" in result["reason"]
    assert not (tmp_path / "report" / "baseline_trades.csv").exists()
