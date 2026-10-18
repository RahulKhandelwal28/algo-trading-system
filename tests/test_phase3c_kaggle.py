from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from nse_research.backtest.phase3c_kaggle import (
    DATASET_NAME,
    DATASET_SLUG,
    SURVIVORSHIP_STATUS,
    UNIVERSE_CLASS,
    audit_kaggle_data,
    build_index_alignment_report,
    build_phase3c_walk_forward_boundaries,
    build_result_manifest,
    load_official_index_csv,
    summarize_extreme_return_trade_impact,
    _trade_extreme_dates,
    _result_status,
)
from nse_research.backtest.validation import monte_carlo_trade_order
from nse_research.indicators.features import add_features
from nse_research.strategies.equity import EquityStrategy


def _normalized_frame(closes: tuple[float, ...] = (100.0, 130.0)) -> pd.DataFrame:
    dates = ["2024-01-02", "2024-01-03"][: len(closes)]
    rows = []
    for day, close in zip(dates, closes):
        rows.append({
            "Symbol": "AAA",
            "Date": day,
            "Open Price": close - 1.0,
            "High Price": close + 1.0,
            "Low Price": close - 2.0,
            "Close Price": close,
            "Total Traded Quantity": 1000,
        })
    return pd.DataFrame(rows)


def test_walk_forward_boundaries_snap_to_sessions_and_do_not_overlap() -> None:
    sessions = pd.bdate_range("2021-06-09", "2026-06-09").date

    folds = build_phase3c_walk_forward_boundaries(sessions)

    assert len(folds) == 5
    for fold in folds:
        parts = [fold[name] for name in ("train", "validation", "test")]
        assert fold["segment_session_separation_pass"] is True
        assert parts[0]["actual_end"] < parts[1]["actual_start"]
        assert parts[1]["actual_end"] < parts[2]["actual_start"]
        assert all(part["equity_sessions"] > 0 for part in parts)
    test_ranges = [(fold["test"]["actual_start"], fold["test"]["actual_end"]) for fold in folds]
    assert all(test_ranges[i][1] < test_ranges[i + 1][0] for i in range(len(test_ranges) - 1))
    assert folds[-1]["test"]["actual_end"] == "2026-06-08"


def test_causal_features_and_strategy_candidates_ignore_future_bars() -> None:
    index = pd.bdate_range("2022-01-03", periods=320)
    rng = np.random.default_rng(41)
    close = 100 * np.exp(np.cumsum(rng.normal(0.0005, 0.008, len(index))))
    bars = pd.DataFrame(
        {
            "open": close * (1 - 0.001),
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": rng.integers(800_000, 1_500_000, len(index)),
        },
        index=index,
    )
    cutoff = index[250]
    prefix = bars.loc[:cutoff]
    future_shock = bars.loc[index > cutoff].copy()
    future_shock.loc[:, ["open", "high", "low", "close"]] *= 4.0
    full = pd.concat([prefix, future_shock])

    prefix_features = add_features(prefix)
    full_features = add_features(full).loc[:cutoff]
    pd.testing.assert_frame_equal(prefix_features, full_features)

    strategy = EquityStrategy("breakout_20", min_avg_volume=0, min_avg_traded_value=0)
    prefix_regime = prefix_features["regime"]
    full_regime = add_features(full)["regime"]
    prefix_candidates = strategy.evaluate(prefix_features, prefix_regime)
    full_candidates = [candidate for candidate in strategy.evaluate(add_features(full), full_regime) if candidate.signal_time <= cutoff]
    assert prefix_candidates == full_candidates


def test_kaggle_data_quality_provenance_and_extreme_close_are_preserved(tmp_path) -> None:
    source = tmp_path / "kaggle.csv"
    _normalized_frame().to_csv(source, index=False)
    official_sessions = {date(2024, 1, 2), date(2024, 1, 3)}

    frame, report, extremes = audit_kaggle_data(source, official_sessions, official_sessions)

    assert report["status"] == "PASS_WITH_KNOWN_LIMITATIONS"
    assert report["provider"] == "kaggle"
    assert report["dataset_name"] == DATASET_NAME
    assert report["dataset"] == DATASET_SLUG
    assert report["symbols"] == 1
    assert report["series_field"].endswith("not fabricated")
    assert report["adjustedness"].startswith("unknown")
    assert report["provenance"].startswith("underlying data source")
    assert report["universe_class"] == UNIVERSE_CLASS
    assert report["survivorship_bias"] == SURVIVORSHIP_STATUS
    assert len(report["symbol_coverage"]) == 1
    assert len(frame) == 2
    assert len(extremes) == 1
    assert extremes[0]["close"] == 130.0
    assert extremes[0]["previous_close"] == 100.0
    assert np.isclose(extremes[0]["close_return_pct"], 30.0)


def test_data_quality_check_reports_duplicate_symbol_date_as_fatal(tmp_path) -> None:
    source = tmp_path / "duplicate.csv"
    duplicate = _normalized_frame((100.0,)).iloc[0]
    pd.DataFrame([duplicate, duplicate]).to_csv(source, index=False)

    _, report, _ = audit_kaggle_data(source, {date(2024, 1, 2)}, {date(2024, 1, 2)})

    assert report["status"] == "DATA_QUALITY_FAILURE"
    assert report["duplicate_symbol_dates"] == 1
    assert any("duplicate symbol/date" in issue for issue in report["fatal_issues"])


def test_index_alignment_uses_exact_dates_and_reports_missing_vix() -> None:
    equity = {date(2024, 1, 2), date(2024, 1, 4)}
    nifty = {date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)}
    vix = {date(2024, 1, 2)}

    report = build_index_alignment_report(equity, nifty, vix)

    assert report["nifty_common_sessions"] == 2
    assert report["nifty_sessions_absent_from_equity"] == ["2024-01-03"]
    assert report["vix_common_sessions"] == 1
    assert report["equity_sessions_missing_from_vix"] == ["2024-01-04"]
    assert report["forward_fill_applied"] is False


def test_official_index_loader_strictly_parses_dates_without_filling(tmp_path) -> None:
    source = tmp_path / "index_INDIAVIX.csv"
    pd.DataFrame(
        {
            "Date": ["2024-01-02", "2024-01-04"],
            "Open": [14.0, 15.0],
            "High": [15.0, 16.0],
            "Low": [13.0, 14.0],
            "Close": [14.5, 15.5],
            "Prev. Close": [14.0, 14.5],
            "Change": [0.5, 1.0],
            "% Change": [3.57, 6.9],
        }
    ).to_csv(source, index=False)

    bars, metadata = load_official_index_csv(source, "India VIX")

    assert bars.index.tolist() == [pd.Timestamp("2024-01-02"), pd.Timestamp("2024-01-04")]
    assert len(bars) == 2
    assert metadata["source_url"].endswith("historical-vix")
    assert metadata["forward_filled"] is False


def test_vix_ohlc_source_anomalies_are_retained_and_disclosed_when_close_is_valid(tmp_path) -> None:
    source = tmp_path / "index_INDIAVIX.csv"
    pd.DataFrame(
        {
            "Date": ["2021-02-12"],
            "Open": [0.0],
            "High": [0.0],
            "Low": [0.0],
            "Close": [23.045],
        }
    ).to_csv(source, index=False)

    bars, metadata = load_official_index_csv(source, "India VIX")

    assert bars.iloc[0]["close"] == 23.045
    assert bars.iloc[0]["open"] == 0.0
    assert metadata["invalid_ohlc_rows"] == 1
    assert metadata["invalid_ohlc_dates"] == ["2021-02-12"]
    assert "Close is validated and used" in metadata["invalid_ohlc_policy"]


def test_monte_carlo_has_required_distribution_schema_for_30_trades() -> None:
    trades = [
        {"net_pnl": 100.0 if number % 2 else -80.0, "risk_amount": 1000.0}
        for number in range(30)
    ]

    summary, samples = monte_carlo_trade_order(
        trades, initial_capital=100_000, risk_fraction=0.01, simulations=5000, seed=17
    )

    assert summary["status"] == "computed_trade_order_permutations"
    assert summary["simulations"] == 5000
    assert set(summary["final_capital_percentiles"]) == {"p5", "p25", "p50", "p75", "p95"}
    assert set(summary["maximum_drawdown_percentiles"]) == {"p5", "p25", "p50", "p75", "p95"}
    assert {"final_capital", "maximum_drawdown_pct"}.issubset(samples.columns)
    assert len(samples) == 5000


def test_result_manifest_captures_data_limits_and_reproduction_fields() -> None:
    manifest = build_result_manifest(
        run_id="phase3c-test",
        run_timestamp="2026-09-26T12:00:00+05:30",
        dataset_path="data/kaggle_nifty500/normalized/nifty500_kaggle.csv",
        dataset_sha256="dataset-hash",
        date_start="2021-06-09",
        date_end="2026-06-09",
        universe_symbols=["TCS.NS", "AAA.NS"],
        capital=100_000,
        risk_config={"risk_per_trade": 0.01},
        cost_config={"slippage_bps": 5},
        config_sha256="config-hash",
        git_commit="abc123",
    )

    assert manifest["provider"] == "kaggle"
    assert manifest["dataset_name"] == DATASET_NAME
    assert manifest["universe_symbols"] == ["AAA.NS", "TCS.NS"]
    assert manifest["dataset_provenance"].startswith("unknown")
    assert manifest["adjustedness"].startswith("unknown")
    assert manifest["series_field"].endswith("not fabricated")
    assert manifest["universe_class"] == UNIVERSE_CLASS
    assert manifest["survivorship_bias"] == SURVIVORSHIP_STATUS
    assert manifest["point_in_time_constituents_available"] is False
    assert manifest["live_trading_enabled"] is False
    assert manifest["config_sha256"] == "config-hash"


def test_extreme_trade_flag_includes_signal_bar_and_reports_observed_pnl() -> None:
    trade = {
        "signal_time": "2024-01-02",
        "entry_time": "2024-01-03",
        "exit_time": "2024-01-05",
        "symbol": "AAA.NS",
    }
    assert _trade_extreme_dates(trade, {"AAA": {date(2024, 1, 2)}}) == [date(2024, 1, 2)]

    rows = [
        {
            "strategy_run": "pullback",
            "window": 5,
            "segment": "test",
            "is_primary_test": True,
            "contains_extreme_return_observation": True,
            "gross_pnl": -100.0,
            "costs": 10.0,
            "net_pnl": -110.0,
        },
        {
            "strategy_run": "pullback",
            "window": 5,
            "segment": "test",
            "is_primary_test": True,
            "contains_extreme_return_observation": False,
            "gross_pnl": 50.0,
            "costs": 10.0,
            "net_pnl": 40.0,
        },
    ]
    summary = summarize_extreme_return_trade_impact(rows)[0]
    assert summary["extreme_affected_trades"] == 1
    assert summary["extreme_affected_gross_pnl"] == -100.0
    assert summary["extreme_affected_transaction_costs"] == 10.0
    assert summary["extreme_affected_net_pnl"] == -110.0
    assert summary["total_net_pnl"] == -70.0
    assert summary["counterfactual_repriced"] is False


def test_run_status_reports_mixed_nonoverlapping_oos_results_as_not_robust() -> None:
    quality = {"status": "PASS_WITH_KNOWN_LIMITATIONS"}
    strategy_rows = [{
        "strategy": "pullback",
        "total_test_trades_across_nonoverlapping_windows": 158,
        "positive_test_windows": 1,
        "test_windows_with_trades": 5,
    }]
    robustness_rows = [
        {"strategy": "pullback", "sensitivity_dimension": "baseline", "net_pnl": -100.0},
        {"strategy": "pullback", "sensitivity_dimension": "atr_multiple", "net_pnl": -80.0},
    ]

    status, reason = _result_status(quality, strategy_rows, robustness_rows)

    assert status == "RESULTS_NOT_ROBUST"
    assert "1/5" in reason
