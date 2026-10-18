"""Phase 3C fixed-rule walk-forward validation on the isolated Kaggle dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from nse_research import __version__
from nse_research.backtest.costs import CostModel
from nse_research.backtest.engine import BacktestEngine, BacktestResult
from nse_research.backtest.phase3_validation import (
    rolling_walk_forward_windows,
    scale_cost_settings,
)
from nse_research.backtest.validation import monte_carlo_trade_order
from nse_research.config import AppConfig
from nse_research.data.kaggle_nifty500 import (
    DATASET_SLUG,
    KaggleNifty500DataError,
    KaggleNifty500Provider,
    NORMALIZED_COLUMNS,
)
from nse_research.data.universe import load_sector_map
from nse_research.reporting.performance import performance_metrics
from nse_research.strategies.composite import CombinedEquityStrategy
from nse_research.strategies.equity import EquityStrategy, build_strategies

DATASET_NAME = "Kaggle NIFTY 500 historical research dataset"
DATASET_RELATIVE_PATH = "data/kaggle_nifty500/normalized/nifty500_kaggle.csv"
NIFTY_RELATIVE_PATH = "data/nse_exports/index_NSEI.csv"
VIX_RELATIVE_PATH = "data/nse_exports/index_INDIAVIX.csv"
UNIVERSE_CLASS = "CURRENT_CONSTITUENT_HISTORICAL_DATA"
SURVIVORSHIP_STATUS = "SURVIVORSHIP_BIAS_PRESENT"
DAILY_STRATEGIES = (
    "trend_following",
    "momentum",
    "breakout_20",
    "breakout_50",
    "breakout_previous_day",
    "pullback",
    "mean_reversion",
    "swing_1_3",
    "swing_3_5",
    "swing_5_10",
    "swing_10_20",
)
INTRADAY_UNAVAILABLE = {"vwap_intraday": "requires intraday bars; Kaggle source is daily OHLCV"}
PHASE2_FOCUS = ("pullback", "swing_10_20")
PORTFOLIO_COMBINATIONS = (
    ("pullback", "swing_10_20"),
    ("trend_following", "pullback"),
    ("trend_following", "breakout_20", "pullback"),
    ("trend_following", "pullback", "swing_10_20"),
)
MONTE_CARLO_SIMULATIONS = 5000
MIN_MONTE_CARLO_TRADES = 30
OHLC_TOLERANCE = 1e-9


class Phase3CKaggleError(RuntimeError):
    """A fatal preflight or execution problem that prevents valid Phase 3C output."""


def jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(item) for item in value]
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if np.isfinite(value) else None
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if value is pd.NA or value is pd.NaT:
        return None
    if isinstance(value, Path):
        return str(value)
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(jsonable(payload), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_csv(path: Path, rows: Iterable[dict[str, Any]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(list(rows))
    if columns is not None and frame.empty:
        frame = pd.DataFrame(columns=columns)
    temporary = path.with_name(f".{path.name}.tmp")
    frame.to_csv(temporary, index=False)
    temporary.replace(path)


def load_official_index_csv(path: str | Path, index_label: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load an already-downloaded official NSE index CSV using strict ISO dates.

    No network request and no missing-session forward fill are performed.
    """
    source_path = Path(path)
    if not source_path.is_file():
        raise Phase3CKaggleError(f"official NSE {index_label} CSV is missing: {source_path}")
    raw = pd.read_csv(source_path, dtype={"Date": "string"}, low_memory=False)
    required = {"Date", "Open", "High", "Low", "Close"}
    if not required.issubset(raw.columns):
        raise Phase3CKaggleError(f"official NSE {index_label} CSV is missing columns: {sorted(required - set(raw.columns))}")
    dates = pd.to_datetime(raw["Date"], format="%Y-%m-%d", errors="coerce")
    if dates.isna().any():
        raise Phase3CKaggleError(f"official NSE {index_label} CSV has invalid ISO dates")
    if dates.duplicated().any():
        raise Phase3CKaggleError(f"official NSE {index_label} CSV has duplicate dates")
    if not dates.is_monotonic_increasing:
        raise Phase3CKaggleError(f"official NSE {index_label} CSV is not chronologically ordered")
    frame = pd.DataFrame(index=pd.DatetimeIndex(dates, name="timestamp"))
    for target, source in (("open", "Open"), ("high", "High"), ("low", "Low"), ("close", "Close")):
        frame[target] = pd.to_numeric(raw[source], errors="coerce").to_numpy()
    volume_source = "Shares Traded" if "Shares Traded" in raw.columns else None
    frame["volume"] = (
        pd.to_numeric(raw[volume_source], errors="coerce").fillna(0).to_numpy()
        if volume_source
        else 0.0
    )
    numbers = frame[["open", "high", "low", "close", "volume"]].to_numpy(dtype="float64")
    if not np.isfinite(numbers).all():
        raise Phase3CKaggleError(f"official NSE {index_label} CSV contains malformed numeric fields")
    if (frame["close"] <= 0).any():
        raise Phase3CKaggleError(f"official NSE {index_label} CSV contains non-positive close values")
    high_floor = frame[["open", "close", "low"]].max(axis=1)
    low_ceiling = frame[["open", "close", "high"]].min(axis=1)
    ohlc_invalid = (
        (frame["high"] + OHLC_TOLERANCE < high_floor)
        | (frame["low"] - OHLC_TOLERANCE > low_ceiling)
        | (frame[["open", "high", "low", "close"]] <= 0).any(axis=1)
    )
    # The existing India VIX regime path reads Close only. Preserve and disclose
    # known source-row OHLC anomalies instead of silently repairing or using
    # those unused fields. NIFTY OHLC is consumed by the benchmark features and
    # remains a fatal gate.
    if ohlc_invalid.any() and index_label != "India VIX":
        raise Phase3CKaggleError(f"official NSE {index_label} CSV violates OHLC bounds")
    metadata = {
        "source": "official NSE index CSV export",
        "source_url": (
            "https://www.nseindia.com/reports-indices-historical-index-data"
            if index_label == "NIFTY 50"
            else "https://www.nseindia.com/reports-indices-historical-vix"
        ),
        "path": str(source_path),
        "sha256": _sha256(source_path),
        "index_label": index_label,
        "rows": int(len(frame)),
        "first_date": frame.index.min().date().isoformat(),
        "last_date": frame.index.max().date().isoformat(),
        "date_column_format": "%Y-%m-%d",
        "duplicate_dates": 0,
        "chronologically_ordered": True,
        "forward_filled": False,
        "feature_inputs_used": ["close"] if index_label == "India VIX" else ["open", "high", "low", "close"],
        "invalid_ohlc_rows": int(ohlc_invalid.sum()),
        "invalid_ohlc_dates": [
            pd.Timestamp(day).date().isoformat()
            for day in frame.index[ohlc_invalid]
        ],
        "invalid_ohlc_policy": (
            "source rows preserved; Open/High/Low are not consumed by the existing VIX regime path; Close is validated and used"
            if index_label == "India VIX"
            else "fatal if any row is invalid"
        ),
    }
    return frame, metadata


def _series_date_sets(bars_by_symbol: dict[str, pd.DataFrame]) -> dict[str, set[date]]:
    return {
        symbol: set(pd.to_datetime(frame.index).date)
        for symbol, frame in bars_by_symbol.items()
    }


def build_index_alignment_report(
    equity_sessions: Iterable[date],
    nifty_sessions: Iterable[date],
    vix_sessions: Iterable[date],
) -> dict[str, Any]:
    equity = set(equity_sessions)
    nifty = set(nifty_sessions)
    vix = set(vix_sessions)
    if not equity:
        raise ValueError("equity session set is empty")
    period_start, period_end = min(equity), max(equity)
    nifty_in_span = {day for day in nifty if period_start <= day <= period_end}
    vix_in_span = {day for day in vix if period_start <= day <= period_end}
    return {
        "equity_session_count": len(equity),
        "equity_first_date": period_start.isoformat(),
        "equity_last_date": period_end.isoformat(),
        "nifty_source": "official NSE index_NSEI.csv",
        "nifty_sessions_in_equity_span": len(nifty_in_span),
        "nifty_common_sessions": len(equity & nifty),
        "equity_sessions_missing_from_nifty": sorted(day.isoformat() for day in equity - nifty),
        "nifty_sessions_absent_from_equity": sorted(day.isoformat() for day in nifty_in_span - equity),
        "vix_source": "official NSE index_INDIAVIX.csv",
        "vix_sessions_in_equity_span": len(vix_in_span),
        "vix_common_sessions": len(equity & vix),
        "equity_sessions_missing_from_vix": sorted(day.isoformat() for day in equity - vix),
        "vix_sessions_absent_from_equity": sorted(day.isoformat() for day in vix_in_span - equity),
        "forward_fill_applied": False,
        "alignment_policy": "exact ISO calendar-date joins; absent observations remain absent",
    }


def audit_kaggle_data(
    path: str | Path,
    nifty_sessions: Iterable[date],
    vix_sessions: Iterable[date],
) -> tuple[pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    """Check the normalized Kaggle file before any strategy evaluation."""
    source = Path(path)
    if not source.is_file():
        raise Phase3CKaggleError(f"normalized Kaggle file is missing: {source}")
    raw = pd.read_csv(source, dtype={"Symbol": "string", "Date": "string"}, low_memory=False)
    fatal: list[str] = []
    if list(raw.columns) != list(NORMALIZED_COLUMNS):
        fatal.append(f"column schema must exactly equal {list(NORMALIZED_COLUMNS)}")
    absent = [column for column in NORMALIZED_COLUMNS if column not in raw.columns]
    if absent:
        report = {"status": "DATA_QUALITY_FAILURE", "fatal_issues": [f"missing columns: {absent}"]}
        raise Phase3CKaggleError(json.dumps(report))
    if "Series" in raw.columns:
        fatal.append("unexpected Series column is present; source schema must not fabricate Series")

    frame = raw.copy()
    frame["Symbol"] = frame["Symbol"].astype("string").str.strip().str.upper()
    if frame["Symbol"].isna().any() or frame["Symbol"].eq("").any():
        fatal.append("null or blank symbols are present")
    dates = pd.to_datetime(frame["Date"], format="%Y-%m-%d", errors="coerce")
    invalid_dates = int(dates.isna().sum())
    if invalid_dates:
        fatal.append(f"{invalid_dates} invalid dates")
    frame["Date"] = dates
    numeric_columns = ["Open Price", "High Price", "Low Price", "Close Price", "Total Traded Quantity"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    numeric = frame[numeric_columns].to_numpy(dtype="float64")
    missing_numeric = int(frame[numeric_columns].isna().any(axis=1).sum())
    nonfinite_numeric = int((~np.isfinite(numeric)).any(axis=1).sum())
    if missing_numeric or nonfinite_numeric:
        fatal.append(f"missing numeric rows={missing_numeric}; non-finite numeric rows={nonfinite_numeric}")

    if frame[["Symbol", "Date"]].duplicated().any():
        duplicate_rows = int(frame[["Symbol", "Date"]].duplicated().sum())
        fatal.append(f"{duplicate_rows} duplicate symbol/date rows")
    ordered_by_symbol = True
    if not invalid_dates and not frame.empty:
        ordering = frame.groupby("Symbol", sort=False)["Date"].apply(lambda values: values.is_monotonic_increasing)
        ordered_by_symbol = bool(ordering.all())
        if not ordered_by_symbol:
            fatal.append(f"{int((~ordering).sum())} symbol histories are not chronologically ordered")

    price_columns = ["Open Price", "High Price", "Low Price", "Close Price"]
    invalid_price_rows = int((frame[price_columns] <= 0).any(axis=1).sum())
    negative_volume_rows = int((frame["Total Traded Quantity"] < 0).sum())
    zero_volume_rows = int((frame["Total Traded Quantity"] == 0).sum())
    if invalid_price_rows:
        fatal.append(f"{invalid_price_rows} rows have non-positive prices")
    if negative_volume_rows:
        fatal.append(f"{negative_volume_rows} rows have negative volume")

    high_floor = frame[["Open Price", "Close Price", "Low Price"]].max(axis=1)
    low_ceiling = frame[["Open Price", "Close Price", "High Price"]].min(axis=1)
    strict_ohlc = (frame["High Price"] < high_floor) | (frame["Low Price"] > low_ceiling)
    tolerant_ohlc = (
        (frame["High Price"] + OHLC_TOLERANCE < high_floor)
        | (frame["Low Price"] - OHLC_TOLERANCE > low_ceiling)
    )
    if tolerant_ohlc.any():
        fatal.append(f"{int(tolerant_ohlc.sum())} OHLC rows violate {OHLC_TOLERANCE} tolerance")

    usable = frame.loc[~frame["Date"].isna()].copy()
    equity_sessions = sorted(set(usable["Date"].dt.date))
    nifty_set, vix_set = set(nifty_sessions), set(vix_sessions)
    outside_nifty = sorted(set(equity_sessions) - nifty_set)
    if outside_nifty:
        fatal.append(f"{len(outside_nifty)} Kaggle dates are absent from the official NSE NIFTY calendar")
    alignment = build_index_alignment_report(equity_sessions, nifty_set, vix_set) if equity_sessions else {}

    symbol_coverage: list[dict[str, Any]] = []
    nse_in_span = {
        day for day in nifty_set
        if equity_sessions and equity_sessions[0] <= day <= equity_sessions[-1]
    }
    if not invalid_dates:
        for symbol, group in usable.groupby("Symbol", sort=True):
            dates_for_symbol = set(group["Date"].dt.date)
            first, last = min(dates_for_symbol), max(dates_for_symbol)
            expected = {day for day in nse_in_span if first <= day <= last}
            symbol_coverage.append({
                "symbol": str(symbol),
                "rows": int(len(group)),
                "first_date": first.isoformat(),
                "last_date": last.isoformat(),
                "missing_nse_sessions_within_symbol_span": len(expected - dates_for_symbol),
            })

    extreme_rows: list[dict[str, Any]] = []
    if not missing_numeric and not invalid_dates:
        ordered = usable.sort_values(["Symbol", "Date"], kind="mergesort")
        ordered["previous_close"] = ordered.groupby("Symbol")["Close Price"].shift(1)
        ordered["close_return"] = ordered["Close Price"] / ordered["previous_close"] - 1.0
        selected = ordered.loc[ordered["close_return"].abs() > 0.20]
        for row in selected.itertuples(index=False):
            extreme_rows.append({
                "symbol": str(row[0]),
                "date": pd.Timestamp(row[1]).date().isoformat(),
                "previous_close": float(row[7]),
                "close": float(row[5]),
                "close_return_pct": float(row[8] * 100),
            })

    if not len(frame):
        fatal.append("normalized Kaggle dataset is empty")
    if fatal:
        status = "DATA_QUALITY_FAILURE"
    else:
        status = "PASS_WITH_KNOWN_LIMITATIONS"
    missing_counts = [row["missing_nse_sessions_within_symbol_span"] for row in symbol_coverage]
    report: dict[str, Any] = {
        "dataset_name": DATASET_NAME,
        "provider": "kaggle",
        "dataset": DATASET_SLUG,
        "dataset_path": str(source),
        "dataset_sha256": _sha256(source),
        "provenance": "underlying data source and collection method are unknown",
        "status": status,
        "fatal_issues": fatal,
        "ohlc_tolerance": OHLC_TOLERANCE,
        "rows": int(len(frame)),
        "symbols": int(frame["Symbol"].nunique()),
        "first_date": frame["Date"].min().date().isoformat() if not invalid_dates and len(frame) else None,
        "last_date": frame["Date"].max().date().isoformat() if not invalid_dates and len(frame) else None,
        "unique_dates": int(frame["Date"].nunique()),
        "invalid_dates": invalid_dates,
        "duplicate_symbol_dates": int(frame[["Symbol", "Date"]].duplicated().sum()),
        "chronologically_ordered_by_symbol": ordered_by_symbol,
        "missing_numeric_rows": missing_numeric,
        "nonfinite_numeric_rows": nonfinite_numeric,
        "nonpositive_price_rows": invalid_price_rows,
        "zero_volume_rows": zero_volume_rows,
        "negative_volume_rows": negative_volume_rows,
        "strict_ohlc_violation_rows": int(strict_ohlc.sum()),
        "ohlc_violations_beyond_tolerance": int(tolerant_ohlc.sum()),
        "series_field": "unavailable in source dataset; not fabricated",
        "adjustedness": "unknown; source does not identify adjusted versus unadjusted prices",
        "universe_class": UNIVERSE_CLASS,
        "survivorship_bias": SURVIVORSHIP_STATUS,
        "symbol_rows_min": int(min((row["rows"] for row in symbol_coverage), default=0)),
        "symbol_rows_max": int(max((row["rows"] for row in symbol_coverage), default=0)),
        "symbol_missing_nse_sessions_total": int(sum(missing_counts)),
        "symbol_missing_nse_sessions_median": float(np.median(missing_counts)) if missing_counts else 0.0,
        "symbol_missing_nse_sessions_max": int(max(missing_counts, default=0)),
        "kaggle_sessions_absent_from_nifty": outside_nifty,
        "index_alignment": alignment,
        "symbol_coverage": symbol_coverage,
        "extreme_close_moves_over_20pct": len(extreme_rows),
        "extreme_close_move_symbols": len({row["symbol"] for row in extreme_rows}),
        "extreme_observations": extreme_rows,
        "no_rows_removed_or_adjusted": True,
    }
    return usable, report, extreme_rows


def build_phase3c_walk_forward_boundaries(
    equity_sessions: Iterable[date],
    train_months: int = 24,
    validation_months: int = 6,
    test_months: int = 6,
    step_months: int = 6,
) -> list[dict[str, Any]]:
    """Build fixed 24/6/6 date windows and snap reporting bounds to Kaggle sessions."""
    sessions = sorted(set(equity_sessions))
    if not sessions:
        raise ValueError("equity_sessions must contain at least one date")
    windows = rolling_walk_forward_windows(
        sessions[0], sessions[-1], train_months, validation_months, test_months, step_months
    )
    output: list[dict[str, Any]] = []
    for number, window in enumerate(windows, start=1):
        actual: dict[str, Any] = {"window": number}
        split_sessions: dict[str, list[date]] = {}
        for split, start_key, end_key in (
            ("train", "train_start", "train_end"),
            ("validation", "validation_start", "validation_end"),
            ("test", "test_start", "test_end"),
        ):
            nominal_start, nominal_end = window[start_key], window[end_key]
            dates = [day for day in sessions if nominal_start <= day <= nominal_end]
            if not dates:
                break
            actual[split] = {
                "nominal_start": nominal_start.isoformat(),
                "nominal_end": nominal_end.isoformat(),
                "actual_start": dates[0].isoformat(),
                "actual_end": dates[-1].isoformat(),
                "equity_sessions": len(dates),
            }
            split_sessions[split] = dates
        if len(split_sessions) != 3:
            continue
        if not (actual["train"]["actual_end"] < actual["validation"]["actual_start"] < actual["validation"]["actual_end"] < actual["test"]["actual_start"]):
            raise ValueError("walk-forward segments overlap or are not chronological")
        sets = [set(split_sessions[name]) for name in ("train", "validation", "test")]
        if any(sets[i] & sets[j] for i in range(3) for j in range(i + 1, 3)):
            raise ValueError("walk-forward segments share one or more sessions")
        actual["segment_session_separation_pass"] = True
        output.append(actual)
    return output


def _metric_summary(
    result: BacktestResult,
    capital: float,
    cost_model: CostModel,
) -> tuple[dict[str, Any], dict[str, float]]:
    trades = sorted(result.trades, key=lambda row: (pd.Timestamp(row["exit_time"]), str(row.get("symbol", ""))))
    base = performance_metrics(trades, result.equity_curve, capital)
    gross_after_slippage = 0.0
    gross_before_slippage = 0.0
    explicit_fees = 0.0
    slippage_cost = 0.0
    turnover = 0.0
    component_totals = {"brokerage": 0.0, "stt": 0.0, "exchange": 0.0, "sebi": 0.0, "gst": 0.0, "stamp": 0.0, "dp": 0.0}
    per_trade_cost_rows = []
    bps = cost_model.settings.slippage_bps / 10000.0
    for trade in trades:
        quantity = int(trade["quantity"])
        direction = 1 if str(trade["direction"]).upper() == "LONG" else -1
        adjusted_entry, adjusted_exit = float(trade["entry"]), float(trade["exit"])
        raw_entry = adjusted_entry / (1.0 + direction * bps)
        raw_exit = adjusted_exit / (1.0 - direction * bps)
        slipped_gross = float(trade["gross_pnl"])
        pre_slippage_gross = direction * quantity * (raw_exit - raw_entry)
        estimated_slippage = pre_slippage_gross - slipped_gross
        gross_after_slippage += slipped_gross
        gross_before_slippage += pre_slippage_gross
        slippage_cost += estimated_slippage
        turnover += float(trade.get("turnover", 0.0) or 0.0)

        entry_side = "buy" if direction > 0 else "sell"
        exit_side = "sell" if direction > 0 else "buy"
        entry_cost = cost_model.order_cost(adjusted_entry, quantity, entry_side, False)
        exit_cost = cost_model.order_cost(adjusted_exit, quantity, exit_side, False, is_exit=True)
        row_costs = {
            field: float(getattr(entry_cost, field) + getattr(exit_cost, field))
            for field in component_totals
        }
        for field, value in row_costs.items():
            component_totals[field] += value
        fees = float(trade.get("costs", 0.0) or 0.0)
        explicit_fees += fees
        per_trade_cost_rows.append({
            "fee_components": row_costs,
            "fee_component_total": sum(row_costs.values()),
            "engine_recorded_fees": fees,
            "slippage_estimate": estimated_slippage,
        })

    net_values = [float(row.get("net_pnl", 0.0) or 0.0) for row in trades]
    # Reverse slippage per trade to report gross price P&L before all modeled costs.
    pre_cost_gross_values = []
    for trade in trades:
        quantity = int(trade["quantity"])
        direction = 1 if str(trade["direction"]).upper() == "LONG" else -1
        adjusted_entry, adjusted_exit = float(trade["entry"]), float(trade["exit"])
        raw_entry = adjusted_entry / (1.0 + direction * bps)
        raw_exit = adjusted_exit / (1.0 - direction * bps)
        pre_cost_gross_values.append(direction * quantity * (raw_exit - raw_entry))
    gross_profit = sum(value for value in pre_cost_gross_values if value > 0)
    gross_loss = abs(sum(value for value in pre_cost_gross_values if value < 0))
    net_wins = [value for value in net_values if value > 0]
    net_losses = [value for value in net_values if value < 0]
    holding_bars = [float(row.get("holding_bars", 0) or 0) for row in trades]
    total_cost = explicit_fees + slippage_cost
    recorded_net = float(sum(net_values))
    accounting_difference = (gross_before_slippage - total_cost) - recorded_net
    if abs(accounting_difference) > max(0.05, len(trades) * 0.001):
        raise AssertionError(f"reported gross/cost/net values fail reconciliation by {accounting_difference}")
    summary = {
        "initial_capital": capital,
        "final_capital": float(result.final_capital),
        "total_trades": int(len(trades)),
        "winning_trades": int(sum(value > 0 for value in net_values)),
        "losing_trades": int(sum(value < 0 for value in net_values)),
        "win_rate_pct": (sum(value > 0 for value in net_values) / len(net_values) * 100) if net_values else None,
        "gross_profit": float(gross_profit),
        "gross_loss": float(gross_loss),
        "gross_pnl": float(gross_before_slippage),
        "gross_pnl_after_slippage_before_fees": float(gross_after_slippage),
        "profit_factor": float(base["profit_factor"]),
        "expectancy_per_trade": float(base["expectancy_per_trade"]),
        "net_pnl": recorded_net,
        "net_return_pct": float(base["net_return_pct"]),
        "maximum_drawdown_pct": float(base["maximum_drawdown_pct"]),
        "sharpe_ratio": float(base["sharpe"]),
        "sortino_ratio": float(base["sortino"]),
        "average_win": float(np.mean(net_wins)) if net_wins else 0.0,
        "average_loss": float(np.mean(net_losses)) if net_losses else 0.0,
        "largest_win": float(max(net_values)) if net_values else 0.0,
        "largest_loss": float(min(net_values)) if net_values else 0.0,
        "longest_winning_streak": int(base["longest_winning_streak"]),
        "longest_losing_streak": int(base["longest_losing_streak"]),
        "average_holding_period_sessions": float(np.mean(holding_bars)) if holding_bars else 0.0,
        "median_holding_period_sessions": float(np.median(holding_bars)) if holding_bars else 0.0,
        "total_transaction_costs": float(total_cost),
        "explicit_fees_taxes_dp": float(explicit_fees),
        "estimated_slippage_cost": float(slippage_cost),
        "turnover": float(turnover),
        "cost_pct_abs_gross_pnl": total_cost / abs(gross_before_slippage) * 100 if gross_before_slippage else None,
        "cost_pct_turnover": total_cost / turnover * 100 if turnover else None,
        "recorded_fee_reconciliation_difference": float(sum(row["fee_component_total"] - row["engine_recorded_fees"] for row in per_trade_cost_rows)),
        "gross_cost_net_reconciliation_difference": float(accounting_difference),
    }
    return summary, component_totals


def _common_artifact_context(
    run_id: str,
    run_timestamp: str,
    dataset_path: Path,
    date_start: date,
    date_end: date,
    universe_symbols: Iterable[str],
    capital: float,
    risk_config: dict[str, Any],
    cost_config: dict[str, Any],
    config_sha256: str,
    git_commit: str | None,
) -> dict[str, Any]:
    symbol_list = sorted(universe_symbols)
    return {
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "application_version": __version__,
        "config_sha256": config_sha256,
        "git_commit": git_commit,
        "dataset_name": DATASET_NAME,
        "provider": "kaggle",
        "dataset": DATASET_SLUG,
        "dataset_path": str(dataset_path),
        "dataset_provenance": "unknown; underlying source and collection method are not established",
        "adjustedness": "unknown; no adjusted-close field or adjustment flag",
        "point_in_time_constituents_available": False,
        "date_range_start": date_start.isoformat(),
        "date_range_end": date_end.isoformat(),
        "universe_class": UNIVERSE_CLASS,
        "universe_symbol_count": len(symbol_list),
        "universe_symbols_sha256": hashlib.sha256("\n".join(symbol_list).encode("utf-8")).hexdigest(),
        "survivorship_bias": SURVIVORSHIP_STATUS,
        "series_field": "unavailable in source dataset; not fabricated",
        "capital": float(capital),
        "risk_config_json": json.dumps(jsonable(risk_config), sort_keys=True),
        "cost_model_json": json.dumps(jsonable(cost_config), sort_keys=True),
        "slippage_bps": float(cost_config.get("slippage_bps", 0)),
        "live_trading_enabled": False,
    }


def build_result_manifest(
    *,
    run_id: str,
    run_timestamp: str,
    dataset_path: str,
    dataset_sha256: str,
    date_start: str,
    date_end: str,
    universe_symbols: Iterable[str],
    capital: float,
    risk_config: dict[str, Any],
    cost_config: dict[str, Any],
    config_sha256: str,
    git_commit: str | None,
) -> dict[str, Any]:
    """Build the reproducibility fields shared by the run's result manifest."""
    symbols = sorted(str(symbol) for symbol in universe_symbols)
    return {
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "status": "RUNNING",
        "phase": "Phase 3C — Kaggle NIFTY 500 Historical Research Validation",
        "dataset_name": DATASET_NAME,
        "provider": "kaggle",
        "dataset": DATASET_SLUG,
        "dataset_path": dataset_path,
        "dataset_sha256": dataset_sha256,
        "dataset_date_range": {"start": date_start, "end": date_end},
        "dataset_provenance": "unknown; underlying source and collection method are not established",
        "adjustedness": "unknown; no adjusted-close field or adjustment flag",
        "series_field": "unavailable in source dataset; not fabricated",
        "universe_class": UNIVERSE_CLASS,
        "universe_symbols": symbols,
        "symbols_used": len(symbols),
        "survivorship_bias": SURVIVORSHIP_STATUS,
        "point_in_time_constituents_available": False,
        "capital": float(capital),
        "risk_configuration": jsonable(risk_config),
        "cost_model": jsonable(cost_config),
        "slippage_bps": float(cost_config.get("slippage_bps", 0)),
        "config_sha256": config_sha256,
        "application_version": __version__,
        "git_commit": git_commit,
        "live_trading_enabled": False,
    }


def _stamp(row: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return {**row, **context}


def _trade_extreme_dates(
    trade: dict[str, Any], extreme_by_symbol: dict[str, set[date]]
) -> list[date]:
    symbol = str(trade.get("symbol", "")).upper().removesuffix(".NS")
    signal = pd.Timestamp(trade.get("signal_time", trade["entry_time"])).date()
    entry = min(signal, pd.Timestamp(trade["entry_time"]).date())
    exit_ = pd.Timestamp(trade["exit_time"]).date()
    return sorted(day for day in extreme_by_symbol.get(symbol, set()) if entry <= day <= exit_)


def summarize_extreme_return_trade_impact(
    trade_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Attribute observed trade P&L to flagged signal/holding intervals.

    This is descriptive attribution only; it does not create a counterfactual
    price path or assume why a large return occurred.
    """
    frame = pd.DataFrame(list(trade_rows))
    if frame.empty:
        return []
    required = {"strategy_run", "window", "segment", "net_pnl", "gross_pnl", "costs", "contains_extreme_return_observation"}
    if not required.issubset(frame.columns):
        raise ValueError(f"trade rows are missing fields required for extreme attribution: {sorted(required - set(frame.columns))}")
    flagged = frame["contains_extreme_return_observation"].astype(str).str.lower().isin({"true", "1"})
    for field in ("net_pnl", "gross_pnl", "costs"):
        frame[field] = pd.to_numeric(frame[field], errors="coerce").fillna(0.0)
    group_fields = ["strategy_run", "window", "segment"]
    if "is_primary_test" in frame:
        group_fields.append("is_primary_test")
    results: list[dict[str, Any]] = []
    for keys, group in frame.groupby(group_fields, dropna=False, sort=True):
        values = keys if isinstance(keys, tuple) else (keys,)
        label = dict(zip(group_fields, values))
        local_flagged = flagged.loc[group.index]
        affected = group.loc[local_flagged]
        total_net = float(group["net_pnl"].sum())
        affected_net = float(affected["net_pnl"].sum())
        row = {
            **{key: group.iloc[0][key] for key in (
                "run_id", "run_timestamp", "application_version", "config_sha256", "git_commit",
                "dataset_name", "provider", "dataset", "dataset_path", "dataset_provenance",
                "adjustedness", "point_in_time_constituents_available", "date_range_start",
                "date_range_end", "universe_class", "universe_symbol_count",
                "universe_symbols_sha256", "survivorship_bias", "series_field", "capital",
                "risk_config_json", "cost_model_json", "slippage_bps", "live_trading_enabled",
            ) if key in group.columns},
            **label,
            "total_trades": int(len(group)),
            "extreme_affected_trades": int(len(affected)),
            "extreme_affected_trade_share_pct": float(len(affected) / len(group) * 100) if len(group) else 0.0,
            "total_gross_pnl": float(group["gross_pnl"].sum()),
            "total_transaction_costs": float(group["costs"].sum()),
            "total_net_pnl": total_net,
            "extreme_affected_gross_pnl": float(affected["gross_pnl"].sum()),
            "extreme_affected_transaction_costs": float(affected["costs"].sum()),
            "extreme_affected_net_pnl": affected_net,
            "extreme_net_pnl_as_pct_of_total_net_pnl": affected_net / total_net * 100 if total_net else None,
            "counterfactual_repriced": False,
        }
        results.append(row)
    return results


def _append_trades(
    destination: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    context: dict[str, Any],
    strategy_run: str,
    window: int,
    segment: str,
    is_primary_test: bool,
    extreme_by_symbol: dict[str, set[date]],
) -> None:
    for trade in trades:
        extreme_days = _trade_extreme_dates(trade, extreme_by_symbol)
        row = {
            **trade,
            "strategy_run": strategy_run,
            "window": window,
            "segment": segment,
            "is_primary_test": is_primary_test,
            "contains_extreme_return_observation": bool(extreme_days),
            "extreme_return_dates": ";".join(day.isoformat() for day in extreme_days),
        }
        destination.append(_stamp(row, context))


def _cost_row(
    summary: dict[str, Any],
    fee_components: dict[str, float],
    strategy_name: str,
    window: int,
    segment: str,
) -> dict[str, Any]:
    return {
        "strategy": strategy_name,
        "window": window,
        "segment": segment,
        "total_trades": summary["total_trades"],
        "gross_pnl_before_costs": summary["gross_pnl"],
        "gross_pnl_after_slippage_before_fees": summary["gross_pnl_after_slippage_before_fees"],
        "brokerage": fee_components["brokerage"],
        "stt": fee_components["stt"],
        "exchange_charges": fee_components["exchange"],
        "sebi_charges": fee_components["sebi"],
        "gst": fee_components["gst"],
        "stamp_duty": fee_components["stamp"],
        "dp_charges": fee_components["dp"],
        "explicit_fees_taxes_dp": summary["explicit_fees_taxes_dp"],
        "estimated_slippage_cost": summary["estimated_slippage_cost"],
        "total_transaction_costs_including_slippage": summary["total_transaction_costs"],
        "cost_pct_abs_gross_pnl": summary["cost_pct_abs_gross_pnl"],
        "cost_pct_turnover": summary["cost_pct_turnover"],
        "turnover": summary["turnover"],
        "net_pnl": summary["net_pnl"],
        "fee_reconciliation_difference": summary["recorded_fee_reconciliation_difference"],
    }


def _result_status(
    quality: dict[str, Any],
    strategy_results: list[dict[str, Any]],
    robust_rows: list[dict[str, Any]],
) -> tuple[str, str]:
    def count(row: dict[str, Any], key: str) -> int:
        value = pd.to_numeric(pd.Series([row.get(key, 0)]), errors="coerce").iloc[0]
        return int(value) if pd.notna(value) else 0

    if quality["status"] == "DATA_QUALITY_FAILURE":
        return "DATA_QUALITY_FAILURE", "Fatal data-quality issues prevent strategy evaluation."
    focused = [
        row for row in strategy_results
        if row["strategy"] in PHASE2_FOCUS
    ]
    enough_oos = any(
        count(row, "total_test_trades_across_nonoverlapping_windows") >= MIN_MONTE_CARLO_TRADES
        for row in focused
    )
    if not enough_oos:
        return "INSUFFICIENT_OUT_OF_SAMPLE_DATA", (
            "Neither pre-existing Phase 2 focus strategy reached the existing 30-closed-trade minimum across non-overlapping OOS test windows."
        )
    robustness_by_focus = {
        name: [row for row in robust_rows if row.get("strategy") == name]
        for name in PHASE2_FOCUS
    }
    for name, rows in robustness_by_focus.items():
        baseline = next((row for row in rows if row.get("sensitivity_dimension") == "baseline"), None)
        if baseline is None:
            continue
        baseline_sign = np.sign(float(baseline.get("net_pnl", 0.0) or 0.0))
        neighbor_signs = [
            np.sign(float(row.get("net_pnl", 0.0) or 0.0))
            for row in rows if row.get("sensitivity_dimension") != "baseline"
        ]
        if any(sign != baseline_sign for sign in neighbor_signs):
            return "RESULTS_NOT_ROBUST", f"Predeclared neighboring parameter/cost cases changed the sign of {name}'s latest-test net P&L."
    mixed_oos_sign = []
    for row in focused:
        windows_with_trades = count(row, "test_windows_with_trades")
        positive_windows = count(row, "positive_test_windows")
        if windows_with_trades >= 2 and 0 < positive_windows < windows_with_trades:
            mixed_oos_sign.append(f"{row['strategy']} {positive_windows}/{windows_with_trades}")
    if mixed_oos_sign:
        return "RESULTS_NOT_ROBUST", (
            "Net P&L changed sign across the non-overlapping OOS test windows for "
            + ", ".join(mixed_oos_sign)
            + "; this status describes outcome instability, not a strategy approval decision."
        )
    return "VALIDATED_WITH_LIMITATIONS", (
        "Data checks and the fixed-rule OOS process completed with the existing minimum trade sample; this status validates the research run only. "
        "It does not approve a strategy, and source provenance, adjustedness, and survivorship remain limitations."
    )


def run_phase3c(project_root: str | Path, result_root: str | Path | None = None) -> Path:
    root = Path(project_root).resolve()
    config_path = root / "config.yaml"
    config = AppConfig.load(config_path)
    if config.live_trading.enabled:
        raise Phase3CKaggleError("live_trading.enabled must be false for Phase 3C")
    if not config.backtest.fill_at_next_bar_open:
        raise Phase3CKaggleError("Phase 3C requires the configured next-bar-open execution policy")

    dataset_path = root / DATASET_RELATIVE_PATH
    nifty_path = root / NIFTY_RELATIVE_PATH
    vix_path = root / VIX_RELATIVE_PATH
    nifty_bars, nifty_meta = load_official_index_csv(nifty_path, "NIFTY 50")
    vix_bars, vix_meta = load_official_index_csv(vix_path, "India VIX")

    # Check the file exactly as stored before asking the provider to load/group it.
    raw_symbols = pd.read_csv(dataset_path, usecols=["Symbol"], dtype={"Symbol": "string"})["Symbol"]
    provider = KaggleNifty500Provider(dataset_path)
    _, quality, extreme_rows = audit_kaggle_data(dataset_path, nifty_bars.index.date, vix_bars.index.date)
    if quality["status"] == "DATA_QUALITY_FAILURE":
        raise Phase3CKaggleError("Kaggle data-quality gate failed: " + "; ".join(quality["fatal_issues"]))
    data_start = date.fromisoformat(quality["first_date"])
    data_end = date.fromisoformat(quality["last_date"])
    bars_by_raw_symbol = provider.histories(data_start, data_end)
    if set(bars_by_raw_symbol) != set(raw_symbols.dropna().str.strip().str.upper().unique()):
        raise Phase3CKaggleError("provider symbol set differs from the validated normalized Kaggle source")
    bars_by_symbol = {f"{symbol}.NS": bars for symbol, bars in bars_by_raw_symbol.items()}
    sectors = load_sector_map(root / config.universe.symbols_file)
    for symbol in bars_by_symbol:
        sectors.setdefault(symbol, "UNKNOWN")

    sessions = sorted({day for bars in bars_by_symbol.values() for day in pd.to_datetime(bars.index).date})
    boundaries = build_phase3c_walk_forward_boundaries(sessions)
    if not boundaries:
        raise Phase3CKaggleError("no complete 24/6/6 walk-forward window fits the Kaggle date range")
    for fold in boundaries:
        fold["index_alignment"] = {
            "nifty_sessions_in_equity_period": sum(
                fold["train"]["actual_start"] <= day.isoformat() <= fold["test"]["actual_end"]
                for day in nifty_bars.index.date
            ),
            "vix_sessions_in_equity_period": sum(
                fold["train"]["actual_start"] <= day.isoformat() <= fold["test"]["actual_end"]
                for day in vix_bars.index.date
            ),
            "forward_fill": False,
        }
    primary_fold = boundaries[-1]
    primary_fold_number = int(primary_fold["window"])
    capital = float(config.backtest.capital)
    risk_cfg = config.risk.model_dump()
    costs_cfg = config.costs.model_dump()
    now = datetime.now(ZoneInfo(config.project.timezone))
    run_id = now.strftime("%Y%m%d_%H%M%S")
    target_root = Path(result_root) if result_root else root / config.project.result_root
    result_dir = target_root / f"phase3c_kaggle_{run_id}"
    suffix = 1
    while result_dir.exists():
        result_dir = target_root / f"phase3c_kaggle_{run_id}_{suffix:02d}"
        suffix += 1
    result_dir.mkdir(parents=True, exist_ok=False)
    config_sha256 = _sha256(config_path)
    git_commit = _git_commit(root)
    context = _common_artifact_context(
        run_id,
        now.isoformat(),
        dataset_path,
        data_start,
        data_end,
        bars_by_symbol,
        capital,
        risk_cfg,
        costs_cfg,
        config_sha256,
        git_commit,
    )

    try:
        strategies = build_strategies(
            [name for name in DAILY_STRATEGIES if name in config.strategies.enabled],
            config.strategies.reward_risk,
            config.strategies.atr_stop_multiple,
            config.strategies.adx_minimum,
            config.strategies.volume_multiple,
            config.strategies.entry_cutoff,
            "1d",
            False,
            config.universe.min_avg_volume,
            config.universe.min_avg_traded_value,
            config.universe.min_price,
            config.strategies.stop_mode,
            config.strategies.signal_score_weights,
        )
        strategy_by_name = {strategy.name: strategy for strategy in strategies}
        missing_strategies = sorted(set(DAILY_STRATEGIES) - set(strategy_by_name))
        unavailable = dict(INTRADAY_UNAVAILABLE)
        unavailable.update({name: "not enabled or unavailable in the current strategy factory" for name in missing_strategies})
        if set(strategy_by_name) != set(DAILY_STRATEGIES) - set(missing_strategies):
            raise Phase3CKaggleError("strategy factory returned an unexpected daily strategy set")

        cost_model = CostModel(config.costs)
        engine = BacktestEngine(config, cost_model)
        segment_rows: dict[str, list[dict[str, Any]]] = {name: [] for name in ("train", "validation", "test")}
        all_trade_rows: list[dict[str, Any]] = []
        cost_rows: list[dict[str, Any]] = []
        strategy_result_rows: list[dict[str, Any]] = []
        primary_test_trades: dict[str, list[dict[str, Any]]] = {}

        extreme_by_symbol: dict[str, set[date]] = {}
        for row in extreme_rows:
            extreme_by_symbol.setdefault(row["symbol"], set()).add(date.fromisoformat(row["date"]))

        for fold in boundaries:
            fold_number = int(fold["window"])
            by_strategy_split: dict[str, dict[str, dict[str, Any]]] = {}
            for segment in ("train", "validation", "test"):
                start = date.fromisoformat(fold[segment]["actual_start"])
                end = date.fromisoformat(fold[segment]["actual_end"])
                for strategy in strategies:
                    result = engine.run(
                        strategy,
                        bars_by_symbol,
                        nifty_bars,
                        capital,
                        start,
                        end,
                        sectors,
                        config.strategies.exit_mode,
                        vix_bars,
                    )
                    metrics, fee_components = _metric_summary(result, capital, cost_model)
                    row = {
                        "strategy": strategy.name,
                        "window": fold_number,
                        "segment": segment,
                        "segment_start": start.isoformat(),
                        "segment_end": end.isoformat(),
                        "is_primary_test": segment == "test" and fold_number == primary_fold_number,
                        "strategy_parameters_json": json.dumps({
                            "reward_risk": strategy.reward_risk,
                            "atr_multiple": strategy.atr_multiple,
                            "adx_minimum": strategy.adx_minimum,
                            "volume_multiple": strategy.volume_multiple,
                            "max_holding_bars": strategy.max_holding_bars,
                            "breakout_buffer_atr": strategy.breakout_buffer_atr,
                            "stop_mode": strategy.stop_mode,
                        }, sort_keys=True),
                        **metrics,
                    }
                    stamped = _stamp(row, context)
                    segment_rows[segment].append(stamped)
                    by_strategy_split.setdefault(strategy.name, {})[segment] = row
                    _append_trades(
                        all_trade_rows,
                        result.trades,
                        context,
                        strategy.name,
                        fold_number,
                        segment,
                        bool(row["is_primary_test"]),
                        extreme_by_symbol,
                    )
                    cost_rows.append(_stamp(
                        _cost_row(metrics, fee_components, strategy.name, fold_number, segment), context
                    ))
                    if segment == "test" and fold_number == primary_fold_number:
                        primary_test_trades[strategy.name] = result.trades
            for strategy in strategies:
                split = by_strategy_split.get(strategy.name, {})
                train, validation, test = (split.get(key, {}) for key in ("train", "validation", "test"))
                result_row = {
                    "strategy": strategy.name,
                    "window": fold_number,
                    "primary_test_window": fold_number == primary_fold_number,
                    "train_trades": train.get("total_trades", 0),
                    "validation_trades": validation.get("total_trades", 0),
                    "test_trades": test.get("total_trades", 0),
                    "train_net_return_pct": train.get("net_return_pct"),
                    "validation_net_return_pct": validation.get("net_return_pct"),
                    "test_net_return_pct": test.get("net_return_pct"),
                    "train_to_validation_return_change_pp": (
                        validation.get("net_return_pct", 0.0) - train.get("net_return_pct", 0.0)
                    ),
                    "validation_to_test_return_change_pp": (
                        test.get("net_return_pct", 0.0) - validation.get("net_return_pct", 0.0)
                    ),
                    "train_to_test_return_change_pp": test.get("net_return_pct", 0.0) - train.get("net_return_pct", 0.0),
                }
                strategy_result_rows.append(_stamp(result_row, context))

        # Existing Phase 3 supports shared-risk union-of-signals portfolios; report their OOS test folds.
        portfolio_rows: list[dict[str, Any]] = []
        for combination in PORTFOLIO_COMBINATIONS:
            if not all(name in strategy_by_name for name in combination):
                continue
            portfolio = CombinedEquityStrategy(" + ".join(combination), tuple(strategy_by_name[name] for name in combination))
            for fold in boundaries:
                fold_number = int(fold["window"])
                start = date.fromisoformat(fold["test"]["actual_start"])
                end = date.fromisoformat(fold["test"]["actual_end"])
                result = engine.run(
                    portfolio,
                    bars_by_symbol,
                    nifty_bars,
                    capital,
                    start,
                    end,
                    sectors,
                    config.strategies.exit_mode,
                    vix_bars,
                )
                metrics, fee_components = _metric_summary(result, capital, cost_model)
                portfolio_rows.append(_stamp({
                    "portfolio": portfolio.name,
                    "member_strategies": ",".join(combination),
                    "window": fold_number,
                    "segment": "test",
                    "segment_start": start.isoformat(),
                    "segment_end": end.isoformat(),
                    "is_primary_test": fold_number == primary_fold_number,
                    **metrics,
                }, context))
                _append_trades(
                    all_trade_rows,
                    result.trades,
                    context,
                    portfolio.name,
                    fold_number,
                    "test_portfolio",
                    fold_number == primary_fold_number,
                    extreme_by_symbol,
                )
                cost_rows.append(_stamp(
                    _cost_row(metrics, fee_components, portfolio.name, fold_number, "test_portfolio"), context
                ))

        # Predeclared neighboring values only; these are sensitivity checks, never selected.
        robustness_rows: list[dict[str, Any]] = []
        primary_start = date.fromisoformat(primary_fold["test"]["actual_start"])
        primary_end = date.fromisoformat(primary_fold["test"]["actual_end"])
        for name in PHASE2_FOCUS:
            baseline = strategy_by_name.get(name)
            if baseline is None:
                continue
            cases: list[tuple[str, Any, dict[str, Any]]] = [
                ("baseline", "unchanged", {}),
                ("atr_multiple", baseline.atr_multiple * 0.9, {"atr_multiple": baseline.atr_multiple * 0.9}),
                ("atr_multiple", baseline.atr_multiple * 1.1, {"atr_multiple": baseline.atr_multiple * 1.1}),
            ]
            hold_delta = 1 if baseline.max_holding_bars <= 5 else 2
            cases.extend([
                ("max_holding_bars", max(1, baseline.max_holding_bars - hold_delta), {"max_holding_bars": max(1, baseline.max_holding_bars - hold_delta)}),
                ("max_holding_bars", baseline.max_holding_bars + hold_delta, {"max_holding_bars": baseline.max_holding_bars + hold_delta}),
                ("breakout_buffer_atr", max(0.0, baseline.breakout_buffer_atr - 0.05), {"breakout_buffer_atr": max(0.0, baseline.breakout_buffer_atr - 0.05)}),
                ("breakout_buffer_atr", baseline.breakout_buffer_atr + 0.05, {"breakout_buffer_atr": baseline.breakout_buffer_atr + 0.05}),
                ("volume_multiple", max(0.1, baseline.volume_multiple - 0.1), {"volume_multiple": max(0.1, baseline.volume_multiple - 0.1)}),
                ("volume_multiple", baseline.volume_multiple + 0.1, {"volume_multiple": baseline.volume_multiple + 0.1}),
                ("cost_multiplier", 0.8, {"cost_multiplier": 0.8}),
                ("cost_multiplier", 1.2, {"cost_multiplier": 1.2}),
                ("slippage_bps", max(0.0, config.costs.slippage_bps - 2.0), {"slippage_bps": max(0.0, config.costs.slippage_bps - 2.0)}),
                ("slippage_bps", config.costs.slippage_bps + 2.0, {"slippage_bps": config.costs.slippage_bps + 2.0}),
            ])
            for dimension, value, changes in cases:
                variant = replace(baseline, **{key: val for key, val in changes.items() if key != "cost_multiplier" and key != "slippage_bps"})
                scenario_config = config
                if "cost_multiplier" in changes:
                    scenario_config = config.model_copy(update={"costs": scale_cost_settings(config.costs, changes["cost_multiplier"])})
                elif "slippage_bps" in changes:
                    scenario_cost = config.costs.model_copy(update={"slippage_bps": changes["slippage_bps"]})
                    scenario_config = config.model_copy(update={"costs": scenario_cost})
                previous_config, previous_cost_model = engine.config, engine.cost_model
                engine.config = scenario_config
                engine.cost_model = CostModel(scenario_config.costs)
                try:
                    result = engine.run(
                        variant,
                        bars_by_symbol,
                        nifty_bars,
                        capital,
                        primary_start,
                        primary_end,
                        sectors,
                        config.strategies.exit_mode,
                        vix_bars,
                    )
                    metrics, _ = _metric_summary(result, capital, engine.cost_model)
                finally:
                    engine.config, engine.cost_model = previous_config, previous_cost_model
                robustness_rows.append(_stamp({
                    "strategy": name,
                    "window": primary_fold_number,
                    "segment": "primary_test",
                    "sensitivity_dimension": dimension,
                    "sensitivity_value": value,
                    "strategy_parameters_json": json.dumps({
                        "atr_multiple": variant.atr_multiple,
                        "max_holding_bars": variant.max_holding_bars,
                        "breakout_buffer_atr": variant.breakout_buffer_atr,
                        "volume_multiple": variant.volume_multiple,
                    }, sort_keys=True),
                    "scenario_cost_model_json": json.dumps(scenario_config.costs.model_dump(), sort_keys=True),
                    "selection_performed": False,
                    **metrics,
                }, context))

        # Monte Carlo is restricted to the latest six-month test trades.
        monte_carlo_rows: list[dict[str, Any]] = []
        monte_carlo_sample_rows: list[dict[str, Any]] = []
        monte_carlo_details: dict[str, Any] = {}
        for strategy in strategies:
            trades = primary_test_trades.get(strategy.name, [])
            summary, samples = monte_carlo_trade_order(
                trades,
                capital,
                config.risk.risk_per_trade,
                simulations=MONTE_CARLO_SIMULATIONS,
                seed=config.project.random_seed,
                max_portfolio_drawdown=config.risk.max_portfolio_drawdown,
            )
            if summary.get("status") != "computed_trade_order_permutations":
                detail = {
                    "status": "INSUFFICIENT_TRADES_FOR_MONTE_CARLO",
                    "trades": len(trades),
                    "minimum_trades": MIN_MONTE_CARLO_TRADES,
                    "simulations_requested": MONTE_CARLO_SIMULATIONS,
                    "simulations_run": 0,
                    "reason": "The existing Monte Carlo framework requires at least 30 closed trades; no full-sample substitute was used.",
                }
            else:
                final_capital_p = summary["final_capital_percentiles"]
                detail = {
                    **summary,
                    "status": "COMPUTED_5000_SIMULATIONS",
                    "simulations_run": MONTE_CARLO_SIMULATIONS,
                    "final_return_percentiles": {
                        key: (value / capital - 1) * 100 for key, value in final_capital_p.items()
                    },
                    "probability_positive_final_pnl_pct": summary["probability_of_ending_profitable_pct"],
                    "probability_of_loss_pct": summary["probability_of_losing_capital_pct"],
                }
                if not samples.empty:
                    sample_output = samples.copy()
                    sample_output.insert(0, "simulation", np.arange(1, len(sample_output) + 1))
                    sample_output.insert(0, "strategy", strategy.name)
                    sample_output.insert(0, "window", primary_fold_number)
                    monte_carlo_sample_rows.extend(
                        _stamp(row, context) for row in sample_output.to_dict("records")
                    )
            monte_carlo_details[strategy.name] = detail
            monte_carlo_rows.append(_stamp({"strategy": strategy.name, "window": primary_fold_number, **detail}, context))

        # Summarize test evidence across the non-overlapping rolling test periods without merging capital paths.
        for row in strategy_result_rows:
            strategy = row["strategy"]
            if row["primary_test_window"]:
                test_runs = [r for r in segment_rows["test"] if r["strategy"] == strategy]
                row["total_test_trades_across_nonoverlapping_windows"] = int(sum(r["total_trades"] for r in test_runs))
                row["positive_test_windows"] = int(sum(float(r["net_pnl"]) > 0 for r in test_runs))
                row["test_windows_with_trades"] = int(sum(int(r["total_trades"]) > 0 for r in test_runs))
                row["test_windows"] = len(test_runs)
        extreme_impact_rows = summarize_extreme_return_trade_impact(all_trade_rows)
        run_status, status_reason = _result_status(quality, strategy_result_rows, robustness_rows)

        alignment = quality["index_alignment"]
        quality.update({
            "run_id": run_id,
            "run_timestamp": now.isoformat(),
            "application_version": __version__,
            "config_sha256": config_sha256,
            "git_commit": git_commit,
            "capital": capital,
            "risk_configuration": risk_cfg,
            "cost_model": costs_cfg,
            "slippage_bps": config.costs.slippage_bps,
            "universe_symbols": sorted(bars_by_symbol),
            "run_status": run_status,
            "official_index_sources": {"nifty_50": nifty_meta, "india_vix": vix_meta},
            "session_alignment": alignment,
            "walk_forward_window_count": len(boundaries),
            "walk_forward_train_validation_test_separation": all(row["segment_session_separation_pass"] for row in boundaries),
            "live_trading_enabled": False,
            "no_broker_access": True,
            "paper_ledger_modified": False,
            "source_prices_adjusted_or_modified": False,
        })
        walk_forward_document = {
            "run_id": run_id,
            "run_timestamp": now.isoformat(),
            "application_version": __version__,
            "config_sha256": config_sha256,
            "git_commit": git_commit,
            "dataset_name": DATASET_NAME,
            "provider": "kaggle",
            "dataset": DATASET_SLUG,
            "dataset_path": DATASET_RELATIVE_PATH,
            "dataset_sha256": quality["dataset_sha256"],
            "dataset_provenance": quality["provenance"],
            "adjustedness": quality["adjustedness"],
            "series_field": quality["series_field"],
            "universe_class": UNIVERSE_CLASS,
            "survivorship_bias": SURVIVORSHIP_STATUS,
            "symbols_used": len(bars_by_symbol),
            "capital": capital,
            "risk_configuration": risk_cfg,
            "cost_model": costs_cfg,
            "slippage_bps": config.costs.slippage_bps,
            "live_trading_enabled": False,
            "date_range": {"start": data_start.isoformat(), "end": data_end.isoformat()},
            "structure": {"train_months": 24, "validation_months": 6, "test_months": 6, "step_months": 6},
            "window_count": len(boundaries),
            "primary_test_window": primary_fold_number,
            "primary_test_period": primary_fold["test"],
            "boundary_policy": "calendar-month half-open windows from the first Kaggle session; each reported boundary is snapped inward to observed Kaggle sessions",
            "segments_reset_capital_and_start_flat": True,
            "signals_at_segment_boundary": "a completed signal from the prior bar may enter at the first segment bar open; no future bar is consulted",
            "windows": boundaries,
            "all_within_fold_segments_disjoint": True,
            "rolling_test_periods_nonoverlapping": True,
        }
        run_manifest = build_result_manifest(
            run_id=run_id,
            run_timestamp=now.isoformat(),
            dataset_path=DATASET_RELATIVE_PATH,
            dataset_sha256=quality["dataset_sha256"],
            date_start=data_start.isoformat(),
            date_end=data_end.isoformat(),
            universe_symbols=bars_by_symbol,
            capital=capital,
            risk_config=risk_cfg,
            cost_config=costs_cfg,
            config_sha256=config_sha256,
            git_commit=git_commit,
        )
        run_manifest.update({
            "strategies_run": list(strategy_by_name),
            "strategies_unavailable": unavailable,
            "strategy_parameters": config.strategies.model_dump(),
            "exit_mode": config.strategies.exit_mode,
            "monte_carlo_simulations": MONTE_CARLO_SIMULATIONS,
            "minimum_monte_carlo_trades": MIN_MONTE_CARLO_TRADES,
            "robustness_focus": list(PHASE2_FOCUS),
            "robustness_is_predeclared_sensitivity_not_optimization": True,
            "nifty_source": nifty_meta,
            "vix_source": vix_meta,
            "live_trading_enabled": False,
            "broker_contacted": False,
            "paper_ledger_modified": False,
            "phase3d_results_modified": False,
            "previous_phase_results_modified": False,
            "risk_reset_policy": "each train/validation/test segment is an independent flat-start, initial-capital run; no open positions cross boundaries",
            "walk_forward_windows": len(boundaries),
            "primary_test_window": primary_fold_number,
            "status_reason": status_reason,
        })

        _write_json(result_dir / "run_manifest.json", run_manifest)
        _write_json(result_dir / "data_manifest.json", {
            "run_id": run_id,
            "run_timestamp": now.isoformat(),
            "application_version": __version__,
            "config_sha256": config_sha256,
            "git_commit": git_commit,
            "dataset_name": DATASET_NAME,
            "provider": "kaggle",
            "dataset": DATASET_SLUG,
            "dataset_path": DATASET_RELATIVE_PATH,
            "dataset_provenance": quality["provenance"],
            "dataset_sha256": quality["dataset_sha256"],
            "dataset_provenance": quality["provenance"],
            "dataset_audit_manifest": "data/kaggle_nifty500/normalized/nifty500_kaggle.manifest.json",
            "first_date": quality["first_date"],
            "last_date": quality["last_date"],
            "rows": quality["rows"],
            "symbols": quality["symbols"],
            "universe": sorted(bars_by_symbol),
            "series_field": quality["series_field"],
            "adjustedness": quality["adjustedness"],
            "universe_class": UNIVERSE_CLASS,
            "survivorship_bias": SURVIVORSHIP_STATUS,
            "point_in_time_constituents_available": False,
            "capital": capital,
            "risk_configuration": risk_cfg,
            "cost_model": costs_cfg,
            "slippage_bps": config.costs.slippage_bps,
            "official_nifty_50": nifty_meta,
            "official_india_vix": vix_meta,
            "session_alignment": alignment,
            "no_forward_fill": True,
            "prices_modified": False,
        })
        _write_json(result_dir / "data_quality_report.json", quality)
        _write_json(result_dir / "walk_forward_boundaries.json", walk_forward_document)
        _write_csv(result_dir / "strategy_results.csv", strategy_result_rows)
        for segment in ("train", "validation", "test"):
            _write_csv(result_dir / f"{segment}_results.csv", segment_rows[segment])
        _write_csv(result_dir / "trade_log.csv", all_trade_rows)
        _write_csv(result_dir / "portfolio_results.csv", portfolio_rows)
        _write_csv(result_dir / "cost_analysis.csv", cost_rows)
        _write_csv(result_dir / "robustness_results.csv", robustness_rows)
        _write_csv(result_dir / "monte_carlo_results.csv", monte_carlo_rows)
        _write_csv(result_dir / "monte_carlo_samples.csv", monte_carlo_sample_rows)
        _write_csv(result_dir / "extreme_return_impact.csv", extreme_impact_rows)
        _write_csv(result_dir / "extreme_return_observations.csv", [
            _stamp({**row, "classification": "unknown; not reconciled to corporate-action records"}, context)
            for row in extreme_rows
        ])

        report = render_phase3c_report(
            run_id=run_id,
            status=run_status,
            status_reason=status_reason,
            quality=quality,
            boundaries=boundaries,
            strategy_rows=strategy_result_rows,
            train_rows=segment_rows["train"],
            validation_rows=segment_rows["validation"],
            test_rows=segment_rows["test"],
            portfolio_rows=portfolio_rows,
            robustness_rows=robustness_rows,
            monte_carlo=monte_carlo_details,
            cost_rows=cost_rows,
            all_trade_rows=all_trade_rows,
            extreme_impact_rows=extreme_impact_rows,
            unavailable=unavailable,
            config=config,
            nifty_meta=nifty_meta,
            vix_meta=vix_meta,
        )
        (result_dir / "PHASE3C_KAGGLE_REPORT.md").write_text(report, encoding="utf-8")
        docs_report = root / "docs" / "PHASE3C_KAGGLE_RESEARCH.md"
        docs_report.write_text(report, encoding="utf-8")

        run_manifest["status"] = run_status
        run_manifest["status_reason"] = status_reason
        run_manifest["completed_at"] = datetime.now(ZoneInfo(config.project.timezone)).isoformat()
        run_manifest["artifacts"] = sorted(path.name for path in result_dir.iterdir())
        _write_json(result_dir / "run_manifest.json", run_manifest)
        print(f"Phase 3C status: {run_status}")
        print(f"Run directory: {result_dir}")
        print(f"Walk-forward windows: {len(boundaries)}; symbols: {len(bars_by_symbol)}; strategies: {len(strategies)}")
        return result_dir
    except Exception as exc:
        failure_manifest = {
            "run_id": run_id,
            "run_timestamp": now.isoformat(),
            "status": "FAILED",
            "error": f"{type(exc).__name__}: {exc}",
            "dataset_name": DATASET_NAME,
            "provider": "kaggle",
            "dataset": DATASET_SLUG,
            "dataset_path": DATASET_RELATIVE_PATH,
            "live_trading_enabled": False,
        }
        _write_json(result_dir / "run_manifest.json", failure_manifest)
        raise


def _git_commit(root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True, timeout=2
        )
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


def _format_money(value: Any) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "—" if pd.isna(number) or not np.isfinite(number) else f"₹{number:,.0f}"


def _format_pct(value: Any, decimals: int = 2) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return "—" if pd.isna(number) or not np.isfinite(number) else f"{number:.{decimals}f}%"


def _table(rows: list[dict[str, Any]], columns: list[tuple[str, str]], empty: str = "No rows.") -> str:
    if not rows:
        return empty
    labels = [label for label, _ in columns]
    values = [[row.get(key) for _, key in columns] for row in rows]
    text = ["| " + " | ".join(labels) + " |", "|" + "|".join(["---"] * len(labels)) + "|"]
    for row in values:
        text.append("| " + " | ".join("—" if value is None else str(value) for value in row) + " |")
    return "\n".join(text)


def render_phase3c_report(
    *,
    run_id: str,
    status: str,
    status_reason: str,
    quality: dict[str, Any],
    boundaries: list[dict[str, Any]],
    strategy_rows: list[dict[str, Any]],
    train_rows: list[dict[str, Any]],
    validation_rows: list[dict[str, Any]],
    test_rows: list[dict[str, Any]],
    portfolio_rows: list[dict[str, Any]],
    robustness_rows: list[dict[str, Any]],
    monte_carlo: dict[str, Any],
    cost_rows: list[dict[str, Any]],
    all_trade_rows: list[dict[str, Any]],
    extreme_impact_rows: list[dict[str, Any]],
    unavailable: dict[str, str],
    config: AppConfig,
    nifty_meta: dict[str, Any],
    vix_meta: dict[str, Any],
) -> str:
    primary_window = max(int(row["window"]) for row in boundaries)
    def primary(rows: list[dict[str, Any]], segment: str | None = None) -> list[dict[str, Any]]:
        return sorted([
            row for row in rows
            if int(row.get("window", 0)) == primary_window
            and (segment is None or row.get("segment") == segment)
        ], key=lambda row: str(row.get("strategy", row.get("portfolio", ""))))

    primary_train = primary(train_rows, "train")
    primary_validation = primary(validation_rows, "validation")
    primary_test = primary(test_rows, "test")
    by_key = {(row["strategy"], int(row["window"])): row for row in strategy_rows}
    degradation = []
    for strategy in DAILY_STRATEGIES:
        row = by_key.get((strategy, primary_window), {})
        degradation.append({
            "strategy": strategy,
            "train_to_validation_return_change_pp": _format_pct(row.get("train_to_validation_return_change_pp"), 2),
            "validation_to_test_return_change_pp": _format_pct(row.get("validation_to_test_return_change_pp"), 2),
            "train_to_test_return_change_pp": _format_pct(row.get("train_to_test_return_change_pp"), 2),
        })
    train_table = _table(primary_train, [
        ("Strategy", "strategy"), ("Trades", "total_trades"), ("Net %", "net_return_pct"),
        ("PF", "profit_factor"), ("Max DD", "maximum_drawdown_pct"), ("Costs", "total_transaction_costs"),
    ])
    validation_table = _table(primary_validation, [
        ("Strategy", "strategy"), ("Trades", "total_trades"), ("Net %", "net_return_pct"),
        ("PF", "profit_factor"), ("Max DD", "maximum_drawdown_pct"), ("Costs", "total_transaction_costs"),
    ])
    test_table = _table(primary_test, [
        ("Strategy", "strategy"), ("Trades", "total_trades"), ("Net %", "net_return_pct"),
        ("PF", "profit_factor"), ("Max DD", "maximum_drawdown_pct"), ("Costs", "total_transaction_costs"),
    ])
    train_table = _format_metric_table(primary_train)
    validation_table = _format_metric_table(primary_validation)
    test_table = _format_metric_table(primary_test)
    degradation_table = _table(degradation, [
        ("Strategy", "strategy"), ("Train→validation Δ pp", "train_to_validation_return_change_pp"),
        ("Validation→test Δ pp", "validation_to_test_return_change_pp"),
        ("Train→test Δ pp", "train_to_test_return_change_pp"),
    ])
    all_test_rows = sorted([row for row in test_rows if row.get("segment") == "test"], key=lambda row: (int(row["window"]), str(row["strategy"])))
    fold_test_table = _table([
        {**row, "net_return_pct": _format_pct(row.get("net_return_pct")), "maximum_drawdown_pct": _format_pct(row.get("maximum_drawdown_pct")), "total_transaction_costs": _format_money(row.get("total_transaction_costs"))}
        for row in all_test_rows
    ], [("Fold", "window"), ("Strategy", "strategy"), ("Trades", "total_trades"), ("Net %", "net_return_pct"), ("PF", "profit_factor"), ("Max DD", "maximum_drawdown_pct"), ("Costs", "total_transaction_costs")])

    primary_costs = [row for row in cost_rows if int(row.get("window", 0)) == primary_window and row.get("segment") == "test"]
    cost_table = _table([
        {
            **row,
            "gross_pnl_before_costs": _format_money(row.get("gross_pnl_before_costs")),
            "explicit_fees_taxes_dp": _format_money(row.get("explicit_fees_taxes_dp")),
            "estimated_slippage_cost": _format_money(row.get("estimated_slippage_cost")),
            "total_transaction_costs_including_slippage": _format_money(row.get("total_transaction_costs_including_slippage")),
            "cost_pct_abs_gross_pnl": _format_pct(row.get("cost_pct_abs_gross_pnl")),
            "cost_pct_turnover": _format_pct(row.get("cost_pct_turnover"), 4),
            "net_pnl": _format_money(row.get("net_pnl")),
        }
        for row in primary_costs
    ], [("Strategy", "strategy"), ("Gross", "gross_pnl_before_costs"), ("Fees/taxes/DP", "explicit_fees_taxes_dp"), ("Slippage est.", "estimated_slippage_cost"), ("Total costs", "total_transaction_costs_including_slippage"), ("Cost/gross", "cost_pct_abs_gross_pnl"), ("Cost/turnover", "cost_pct_turnover"), ("Net", "net_pnl")])

    focus_sensitivity = [row for row in robustness_rows if row.get("strategy") in PHASE2_FOCUS]
    robustness_summary = []
    for name in PHASE2_FOCUS:
        rows = [row for row in focus_sensitivity if row["strategy"] == name]
        by_dim: dict[str, list[float]] = {}
        for row in rows:
            if row["sensitivity_dimension"] == "baseline":
                continue
            by_dim.setdefault(row["sensitivity_dimension"], []).append(float(row["net_return_pct"]))
        baseline_row = next((row for row in rows if row["sensitivity_dimension"] == "baseline"), None)
        for dimension, values in sorted(by_dim.items()):
            robustness_summary.append({
                "strategy": name,
                "dimension": dimension,
                "baseline_net_pct": _format_pct(baseline_row.get("net_return_pct") if baseline_row else None),
                "neighbor_net_pct_range": f"{min(values):.2f}% to {max(values):.2f}%",
                "sign_changes": bool(baseline_row and any(np.sign(float(row["net_pnl"])) != np.sign(float(baseline_row["net_pnl"])) for row in rows if row["sensitivity_dimension"] == dimension)),
            })
    robustness_table = _table(robustness_summary, [
        ("Strategy", "strategy"), ("Predeclared dimension", "dimension"),
        ("Baseline net", "baseline_net_pct"), ("Neighbor net range", "neighbor_net_pct_range"),
        ("Sign changed", "sign_changes"),
    ])

    mc_rows = []
    for strategy in DAILY_STRATEGIES:
        row = monte_carlo.get(strategy, {})
        returns = row.get("final_return_percentiles", {})
        dds = row.get("maximum_drawdown_percentiles", {})
        mc_rows.append({
            "strategy": strategy,
            "status": row.get("status", "INSUFFICIENT_TRADES_FOR_MONTE_CARLO"),
            "trades": row.get("trades", 0),
            "positive_pct": _format_pct(row.get("probability_positive_final_pnl_pct")),
            "loss_pct": _format_pct(row.get("probability_of_loss_pct")),
            "p5_p50_p95_return": " / ".join(_format_pct(returns.get(key)) for key in ("p5", "p50", "p95")),
            "p50_dd": _format_pct(dds.get("p50")),
        })
    mc_table = _table(mc_rows, [
        ("Strategy", "strategy"), ("Status", "status"), ("Trades", "trades"),
        ("P(positive)", "positive_pct"), ("P(loss)", "loss_pct"),
        ("Final return p5/p50/p95", "p5_p50_p95_return"), ("Median max DD", "p50_dd"),
    ])
    mc_distribution_rows = []
    mc_threshold_rows = []
    for strategy in DAILY_STRATEGIES:
        detail = monte_carlo.get(strategy, {})
        if detail.get("status") != "COMPUTED_5000_SIMULATIONS":
            continue
        return_p = detail.get("final_return_percentiles", {}) or {}
        drawdown_p = detail.get("maximum_drawdown_percentiles", {}) or {}
        mc_distribution_rows.append({
            "strategy": strategy,
            "return_distribution": " / ".join(_format_pct(return_p.get(key)) for key in ("p5", "p25", "p50", "p75", "p95")),
            "drawdown_distribution": " / ".join(_format_pct(drawdown_p.get(key)) for key in ("p5", "p25", "p50", "p75", "p95")),
        })
        milestones = detail.get("capital_milestones", {}) or {}
        mc_threshold_rows.append({
            "strategy": strategy,
            **{
                f"{multiple}_probability": (
                    f"{_format_pct(milestones.get(milestone_key, {}).get('probability_reached_pct'))} / "
                    f"{_format_pct(milestones.get(milestone_key, {}).get('probability_final_at_or_above_pct'))}"
                )
                for multiple, milestone_key in (
                    ("1.25", "1.25x"), ("1.5", "1.50x"),
                    ("1.75", "1.75x"), ("2.0", "2.00x"),
                )
            },
        })
    mc_distribution_table = _table(mc_distribution_rows, [
        ("Strategy", "strategy"),
        ("Final return p5 / p25 / p50 / p75 / p95", "return_distribution"),
        ("Maximum drawdown p5 / p25 / p50 / p75 / p95", "drawdown_distribution"),
    ], empty="No latest-test strategy met the existing Monte Carlo trade minimum.")
    mc_threshold_table = _table(mc_threshold_rows, [
        ("Strategy", "strategy"),
        ("1.25× reach / final", "1.25_probability"),
        ("1.5× reach / final", "1.5_probability"),
        ("1.75× reach / final", "1.75_probability"),
        ("2× reach / final", "2.0_probability"),
    ], empty="No Monte Carlo threshold probabilities were estimated.")
    extreme_trade_rows = [row for row in all_trade_rows if row.get("contains_extreme_return_observation")]
    extreme_primary = [row for row in extreme_trade_rows if row.get("is_primary_test")]
    extreme_trade_count = len(extreme_trade_rows)
    extreme_primary_count = len(extreme_primary)
    extreme_trade_word = "trade" if extreme_trade_count == 1 else "trades"
    extreme_trade_verb = "includes" if extreme_trade_count == 1 else "include"
    extreme_primary_verb = "is" if extreme_primary_count == 1 else "are"
    primary_extreme_impact = [
        row for row in extreme_impact_rows
        if str(row.get("is_primary_test", "")).lower() in {"true", "1"}
    ]
    def format_extreme_impact(rows: list[dict[str, Any]], empty: str) -> str:
        return _table([
            {
                **row,
                "extreme_affected_gross_pnl": _format_money(row.get("extreme_affected_gross_pnl")),
                "extreme_affected_transaction_costs": _format_money(row.get("extreme_affected_transaction_costs")),
                "extreme_affected_net_pnl": _format_money(row.get("extreme_affected_net_pnl")),
                "total_net_pnl": _format_money(row.get("total_net_pnl")),
                "extreme_net_pnl_as_pct_of_total_net_pnl": _format_pct(row.get("extreme_net_pnl_as_pct_of_total_net_pnl")),
            }
            for row in rows if int(row.get("extreme_affected_trades", 0)) > 0
        ], [
            ("Strategy/portfolio", "strategy_run"), ("Fold", "window"), ("Segment", "segment"),
            ("Trades", "total_trades"), ("Affected trades", "extreme_affected_trades"),
            ("Affected gross", "extreme_affected_gross_pnl"), ("Affected costs", "extreme_affected_transaction_costs"),
            ("Affected net", "extreme_affected_net_pnl"), ("Total net", "total_net_pnl"),
            ("Affected net / total net", "extreme_net_pnl_as_pct_of_total_net_pnl"),
        ], empty=empty)

    extreme_impact_table = format_extreme_impact(
        extreme_impact_rows,
        "No strategy or portfolio trade overlapped a flagged signal/holding date.",
    )
    primary_extreme_impact_table = format_extreme_impact(
        primary_extreme_impact,
        "No latest-test strategy or portfolio trade overlapped a flagged signal/holding date.",
    )
    primary_portfolios = [row for row in portfolio_rows if row.get("is_primary_test")]
    portfolio_table = _table([
        {**row, "net_return_pct": _format_pct(row.get("net_return_pct")), "maximum_drawdown_pct": _format_pct(row.get("maximum_drawdown_pct")), "total_transaction_costs": _format_money(row.get("total_transaction_costs"))}
        for row in primary_portfolios
    ], [("Portfolio", "portfolio"), ("Trades", "total_trades"), ("Net %", "net_return_pct"), ("PF", "profit_factor"), ("Max DD", "maximum_drawdown_pct"), ("Costs", "total_transaction_costs")])

    boundary_table = _table([
        {
            "window": row["window"],
            "train": f"{row['train']['actual_start']}–{row['train']['actual_end']} ({row['train']['equity_sessions']})",
            "validation": f"{row['validation']['actual_start']}–{row['validation']['actual_end']} ({row['validation']['equity_sessions']})",
            "test": f"{row['test']['actual_start']}–{row['test']['actual_end']} ({row['test']['equity_sessions']})",
        }
        for row in boundaries
    ], [("Fold", "window"), ("Train", "train"), ("Validation", "validation"), ("Test", "test")])

    pullback_phase2 = "Phase 2 pullback was +2.60% over 17 trades (PF 1.49); Phase 3 full-window pullback was +2.22% over 43 trades (PF 1.12)."
    swing_phase2 = "Phase 2 swing_10_20 was +5.35% over 18 trades (PF 2.57); Phase 3 full-window swing_10_20 was −8.64% over 36 trades (PF 0.45)."
    strategy_compare = "Phase 2/Phase 3 figures are descriptive and use different data, dates and sampling than the Kaggle walk-forward results; they are not pooled or overwritten."
    phase_comparison_rows = []
    earlier_comparison = {
        "pullback": {
            "Phase 2": {"total_trades": 17, "win_rate_pct": 41.18, "profit_factor": 1.49, "net_return_pct": 2.60, "maximum_drawdown_pct": -2.63},
            "Phase 3 full window": {"total_trades": 43, "win_rate_pct": 41.86, "profit_factor": 1.12, "net_return_pct": 2.22, "maximum_drawdown_pct": -6.19},
        },
        "swing_10_20": {
            "Phase 2": {"total_trades": 18, "win_rate_pct": 61.11, "profit_factor": 2.57, "net_return_pct": 5.35, "maximum_drawdown_pct": -2.51},
            "Phase 3 full window": {"total_trades": 36, "win_rate_pct": 30.56, "profit_factor": 0.45, "net_return_pct": -8.64, "maximum_drawdown_pct": -9.15},
        },
    }
    for strategy_name in PHASE2_FOCUS:
        for period, values in earlier_comparison[strategy_name].items():
            phase_comparison_rows.append({"strategy": strategy_name, "period": period, **values})
        latest = next((row for row in primary_test if row["strategy"] == strategy_name), None)
        if latest:
            phase_comparison_rows.append({
                "strategy": strategy_name,
                "period": f"Phase 3C latest 6-month test ({latest['segment_start']} to {latest['segment_end']})",
                **latest,
            })
    phase_comparison_table = _table([
        {
            **row,
            "net_return_pct": _format_pct(row.get("net_return_pct")),
            "win_rate_pct": _format_pct(row.get("win_rate_pct")),
            "maximum_drawdown_pct": _format_pct(row.get("maximum_drawdown_pct")),
            "profit_factor": "∞" if row.get("profit_factor") == float("inf") else f"{float(row.get('profit_factor', 0.0)):.2f}",
        }
        for row in phase_comparison_rows
    ], [
        ("Strategy", "strategy"), ("Period/scope", "period"), ("Trades", "total_trades"),
        ("Win %", "win_rate_pct"), ("Profit factor", "profit_factor"),
        ("Net return", "net_return_pct"), ("Max drawdown", "maximum_drawdown_pct"),
    ])
    vix_common = quality["index_alignment"]["vix_common_sessions"]
    nifty_common = quality["index_alignment"]["nifty_common_sessions"]
    vix_ohlc_warning = "; ".join(vix_meta.get("invalid_ohlc_dates", [])) or "none"
    primary_fold_start = next(row["test"]["actual_start"] for row in boundaries if int(row["window"]) == primary_window)
    final_test_end = next(row["test"]["actual_end"] for row in boundaries if int(row["window"]) == primary_window)
    total_trades_all = sum(int(row["total_trades"]) for row in primary_test)
    symbols_used = int(quality["symbols"])

    return f"""# Phase 3C — Kaggle NIFTY 500 Historical Research Validation

Run: `{run_id}`  
Status: **{status}** — {status_reason}

## 1. Executive summary

This fixed-rule study used the **{DATASET_NAME}** as a separate source
(`provider=kaggle`) and official NSE NIFTY 50 and India VIX CSVs only for
market-regime context. It ran {symbols_used} symbols and {len(DAILY_STRATEGIES)}
daily strategies over {len(boundaries)} rolling 24/6/6 windows. The latest
six-month test period is {primary_fold_start} to {final_test_end}; the latest
test produced {total_trades_all} closed trades across the 11 independent
strategy runs. The run is research-only, with `LIVE_TRADING_ENABLED=false`.

No strategy is approved by this experiment. Train, validation and test figures
are kept separate; the test-window table below also retains all five test
windows so deterioration is visible.

## 2. Dataset description

- Name: {DATASET_NAME}
- Provider: `kaggle`
- Dataset slug: `{DATASET_SLUG}`
- Normalized path: `{DATASET_RELATIVE_PATH}`
- Date range: {quality['first_date']} to {quality['last_date']}
- Symbols / rows: {quality['symbols']} / {quality['rows']:,}
- Universe label: `{UNIVERSE_CLASS}`; `{SURVIVORSHIP_STATUS}`
- `Series`: unavailable in source; no `EQ` value was created.
- Adjustedness: unknown. Source OHLC prices were not adjusted during this run.

## 3. Provenance, survivorship and data limitations

The Kaggle dataset has an MIT license as reported by the Kaggle CLI, but its
underlying data source, collection method and version were not available from
the unauthenticated Kaggle metadata response. The dataset must not be described
as official NSE equity data. It has no point-in-time constituent history and
its 500-symbol set overlaps 498 of 501 entries in the current project universe;
the experiment therefore uses the supplied fixed roster and retains
survivorship/current-constituent selection bias.

The source spans approximately five years and ends {quality['last_date']}, not
the current date. It contains no series or adjusted-close field. It has
{quality['symbol_missing_nse_sessions_total']:,} per-symbol missing official
NSE sessions within symbol date spans (median
{quality['symbol_missing_nse_sessions_median']:.0f}, maximum
{quality['symbol_missing_nse_sessions_max']}). No missing bars were synthesized.

## 4. Exact walk-forward boundaries

Each fold resets to ₹{config.backtest.capital:,.0f} and starts flat. Bounds are
calendar-month 24/6/6 partitions snapped inward to observed Kaggle sessions.
The first two years provide the initial train period; the five six-month test
periods are chronological and non-overlapping. The latest period ends at
{final_test_end}; the {quality['last_date']} source observation falls after
that exact six-month boundary and is not included in the final fold.

{boundary_table}

The train/validation/test session sets are disjoint within each fold. Rolling
train and validation samples overlap across folds by design; test periods do
not. Full date/session details are in `walk_forward_boundaries.json`.

## 5. Strategy methodology and lookahead audit

The study used the existing fixed strategy objects, default parameters, exit
mode `{config.strategies.exit_mode}`, event-driven `BacktestEngine`, and
next-bar-open fills. The enabled daily rules were Trend (`trend_following`),
Momentum, Breakout 20-day, Breakout 50-day, Previous-day breakout, Pullback,
Mean reversion, Swing 1–3, Swing 3–5, Swing 5–10, and Swing 10–20. No daily
rule was unavailable. VWAP intraday was unavailable because the Kaggle source
contains daily bars only.

Indicators use chronological EWM/rolling calculations and prior levels shifted
by one bar. Signals are calculated from a completed bar and the engine retrieves
that candidate only when processing the next bar's open; current high/low/close
are evaluated after the entry. Exits use only the bar at their exit timestamp,
with the configured stop-first rule when stop and target both occur in a daily
bar. No parameter is selected from training; robustness values are predeclared
neighbors evaluated only on the latest test window. All 500 source symbols are
retained; the existing rolling liquidity/price signal filters use only data
available at that historical bar, not future history.

## 6. Train results — latest fold

All required metrics (including gross profit/loss, turnover, holding sessions,
Sharpe, Sortino, streaks and cost fields) are in `train_results.csv`.

{train_table}

## 7. Validation results — latest fold

Validation metrics use unchanged strategy rules and the same risk/cost model.
No validation result was used to tune or select a rule.

{validation_table}

## 8. Test results — latest fold

This is the primary six-month evaluation period. Exact values for every
strategy and fold are in `test_results.csv`.

{test_table}

### OOS results across all non-overlapping six-month test folds

{fold_test_table}

### Train-to-validation/test changes — latest fold

Changes are net-return percentage-point differences; a negative value means
the later segment's return was lower.

{degradation_table}

## 9. Costs and turnover — latest test

The canonical cost schedule was not changed: delivery brokerage 0%, delivery
STT 0.10% per side, exchange transaction charge 0.00307%, SEBI charge 0.0001%,
GST 18% on brokerage/exchange/SEBI, buy stamp duty 0.015%, sell DP charge
₹{config.costs.delivery_dp_charge_per_sell:.2f}, and 5 bps adverse slippage
per execution. Costs are recomputed from the same model. Gross-before-costs
reverses estimated slippage; explicit taxes/fees and slippage are reported
separately so net P&L reconciles. `cost_analysis.csv` contains detailed fee
components for every strategy segment and portfolio test.

{cost_table}

## 10. Drawdown and portfolio results

Maximum drawdown is from the engine's daily marked-equity path for each
independent segment, including the 10% configured portfolio drawdown control
and the existing pre-limit tripwire behavior. It is not aggregated across
reset segments. The existing Phase 3 engine's four union-of-signals portfolios
were run on each test fold with the same risk limits:

{portfolio_table}

## 11. Robustness analysis

The existing Phase 3 focus candidates, Pullback and Swing 10–20, were checked
on the latest test window using predeclared neighboring ATR stop multiples
(±10%), holding caps (±1 or ±2 sessions), breakout buffer (±0.05 ATR), volume
threshold (±0.1), total modeled-cost multipliers (0.8×/1.2×), and slippage
(3/7 bps). These are sensitivity observations only; no setting was selected.

{robustness_table}

## 12. Monte Carlo analysis

The existing trade-order permutation method was used, with 5,000 simulations,
the configured 1% risk fraction, 10% drawdown halt, and the project seed. It
requires at least 30 closed trades. Only trades in the latest six-month test
were eligible; full-sample trades were not substituted. Final-return and
maximum-drawdown p5/p25/p50/p75/p95 fields plus existing capital-milestone
probabilities are in `monte_carlo_results.csv` when estimable.

{mc_table}

For strategies with at least 30 latest-test trades, all requested return and
drawdown percentiles are shown as p5 / p25 / p50 / p75 / p95:

{mc_distribution_table}

Existing framework capital-milestone probabilities are shown as probability
of reaching / probability of finishing above each threshold:

{mc_threshold_table}

## 13. Extreme-return analysis

The pre-run dataset scan found {quality['extreme_close_moves_over_20pct']} close
moves over 20% across {quality['extreme_close_move_symbols']} symbols. No
extreme prices were removed, adjusted, winsorized or smoothed. Trades whose
signal or holding interval includes a flagged symbol/date are explicitly marked
in `trade_log.csv`. {extreme_trade_count} closed {extreme_trade_word} across all
recorded segments {extreme_trade_verb} at least one such observation;
{extreme_primary_count} {extreme_primary_verb} in
the latest primary test. The table and `extreme_return_impact.csv` report the
observed gross P&L, costs and net P&L for affected trades alongside the full
run's strategy/portfolio totals. This is trade attribution, not a re-priced
counterfactual; the effect of replacing these source bars cannot be established
without verified corporate-action data. No corporate action is presumed or confirmed.
`extreme_return_observations.csv` contains the dates/returns and labels their
event classification unknown.

### Extreme observations in the latest test

{primary_extreme_impact_table}

### Observed trade attribution across all folds

{extreme_impact_table}

## 14. Official NSE NIFTY/VIX alignment

NIFTY 50 comes from official NSE `index_NSEI.csv`; India VIX comes from official
NSE `index_INDIAVIX.csv`. Both files are date-parsed as ISO `YYYY-MM-DD` and
joined on exact dates. There is no forward fill. Within the equity period,
{nifty_common} of {quality['index_alignment']['equity_session_count']} Kaggle
sessions align with official NIFTY and {vix_common} align with official VIX.
Kaggle dates missing from NIFTY: {len(quality['index_alignment']['equity_sessions_missing_from_nifty'])};
Kaggle dates missing from VIX: {len(quality['index_alignment']['equity_sessions_missing_from_vix'])}.
Official weekend/special sessions absent from the Kaggle equity date union are
listed in `data_quality_report.json`. Official NIFTY is used for regime
classification; official VIX is joined by exact date to the existing regime
feature path. An unavailable VIX observation remains missing.
The local NSE VIX export contains {vix_meta['invalid_ohlc_rows']} rows with
invalid Open/High/Low fields ({vix_ohlc_warning}); the existing regime logic
consumes only Close, which is positive on these rows. Source values were kept
unchanged, and this anomaly is recorded in the data manifest.
Both dates precede the Kaggle dataset's 2021-06-09 first session and fall
outside every Phase 3C fold.

## 15. Comparison with Phase 2 and Phase 3

{pullback_phase2} {swing_phase2} {strategy_compare}

{phase_comparison_table}

The latest Kaggle test rows are primary OOS observations. Pullback's positive
Phase 2/Phase 3 descriptive returns did not persist in the latest Kaggle test;
Swing 10–20's Phase 2 positive and Phase 3 negative behavior is followed by a
negative latest Kaggle test. The separate rolling test folds also change sign
for both focus rules. Win rate, profit factor and drawdown levels therefore
vary across samples. Provider, dates, coverage and universe construction differ,
so these figures are descriptive and are not pooled. Earlier reports/results
were not modified.

## 16. Limitations

- Unknown underlying provider and collection method; no `Series` field.
- Unknown corporate-action adjustment status; 50 extreme close moves remain
  untreated and may affect individual strategy trades.
- Current-constituent historical roster creates survivorship/membership bias.
- Daily bars cannot reconstruct intraday path; the existing stop-first
  ambiguity assumption applies.
- Four NSE weekend index sessions are absent from the Kaggle equity calendar;
  other per-symbol session gaps remain visible.
- Segment simulations reset capital and positions; they are independent
  segment estimates, not a single continuously compounded portfolio path.
- Cost rates are the existing model and may differ from a broker's actual
  charges. Slippage is estimated at the configured 5 bps each way.
- No Phase 3C-specific strategy approval threshold exists; this report makes
  no approval claim.

## 17. Reproducibility

- Run id: `{run_id}`
- Application version: `{__version__}`
- Config file: `config.yaml`; SHA-256 is in `run_manifest.json`
- Data SHA-256: `data_manifest.json`
- Seed: `{config.project.random_seed}`
- Universe: the exact 500 normalized Kaggle symbols; current sector labels are
  used for the existing 25% sector-risk control, with unmatched labels set to
  `UNKNOWN`.
- Capital: ₹{config.backtest.capital:,.0f}; risk: {config.risk.risk_per_trade:.1%}
  per trade, {config.risk.max_daily_loss:.1%} daily loss, 10% portfolio
  drawdown, {config.risk.max_open_positions} positions, 25% maximum position
  and 25% sector exposure.
- Live trading disabled; no broker contacted; paper ledger and earlier result
  folders were not modified.
- Detailed rows: `train_results.csv`, `validation_results.csv`,
  `test_results.csv`, `strategy_results.csv`, `trade_log.csv`,
  `portfolio_results.csv`, `cost_analysis.csv`, `robustness_results.csv`,
  and `monte_carlo_results.csv`.

## 18. Final research conclusion

**{status}.** {status_reason} The experiment completed on a structurally
validated, separate Kaggle source and produced chronological train,
validation and test evidence. The source provenance, price adjustedness and
point-in-time universe are not verified. Results therefore describe only this
fixed current-constituent historical dataset under the existing engine,
configuration and modeled costs; they do not establish an official NSE
history or approve any strategy.
"""


def _format_metric_table(rows: list[dict[str, Any]]) -> str:
    prepared = []
    for row in rows:
        prepared.append({
            **row,
            "net_return_pct": _format_pct(row.get("net_return_pct")),
            "profit_factor": "∞" if row.get("profit_factor") == float("inf") else (f"{float(row.get('profit_factor')):.2f}" if row.get("profit_factor") is not None and np.isfinite(float(row.get("profit_factor"))) else "—"),
            "maximum_drawdown_pct": _format_pct(row.get("maximum_drawdown_pct")),
            "total_transaction_costs": _format_money(row.get("total_transaction_costs")),
        })
    return _table(prepared, [
        ("Strategy", "strategy"), ("Trades", "total_trades"), ("Wins", "winning_trades"),
        ("Losses", "losing_trades"), ("Win %", "win_rate_pct"), ("Gross profit", "gross_profit"),
        ("Gross loss", "gross_loss"), ("PF", "profit_factor"), ("Expectancy", "expectancy_per_trade"),
        ("Net %", "net_return_pct"), ("Max DD", "maximum_drawdown_pct"),
        ("Sharpe", "sharpe_ratio"), ("Sortino", "sortino_ratio"), ("Costs", "total_transaction_costs"),
    ])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run fixed-rule Phase 3C validation on the local Kaggle NIFTY 500 dataset.")
    parser.add_argument("--project-root", default=str(Path(__file__).resolve().parents[3]))
    parser.add_argument("--result-root", default=None)
    args = parser.parse_args(argv)
    result_dir = run_phase3c(args.project_root, args.result_root)
    print(f"Report: {result_dir / 'PHASE3C_KAGGLE_REPORT.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
