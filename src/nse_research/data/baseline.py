"""Run the fixed 11-strategy Phase 3 baseline after official data passes audit."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.backtest.engine import BacktestEngine
from nse_research.backtest.phase3_validation import metrics_for_trades
from nse_research.config import AppConfig
from nse_research.data.expansion import BASELINE_STRATEGIES, normalize_symbol
from nse_research.data.providers import YahooFinanceProvider, validate_and_normalize_bars
from nse_research.data.universe import load_sector_map, remove_placeholder_symbols
from nse_research.reporting.performance import performance_metrics
from nse_research.strategies.equity import build_strategies

CAPITAL = 100_000.0
NSE_EXPORT_PROVIDER = "NSE official historical CSV export"


def _skip_baseline(output_dir: Path, reason: str) -> dict[str, str]:
    for filename in ("baseline_strategy_results.csv", "yearly_results.csv", "monthly_results.csv"):
        path = output_dir / filename
        if path.exists():
            frame = pd.read_csv(path)
            frame["status"] = "NOT_RUN_BASELINE_GATE"
            frame["reason"] = reason
            frame.to_csv(path, index=False)
    payload = {
        "status": "SKIPPED",
        "reason": reason,
        "analysis_scope": "fixed 11-strategy baseline only; no parameter search or strategy optimization",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "baseline_run_manifest.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return {"status": "SKIPPED", "reason": reason}


def run_fixed_baseline_11(
    *,
    root: Path,
    cache_dir: Path,
    output_dir: Path,
    manifest: dict[str, Any],
    requested_start: date,
    requested_end: date,
) -> dict[str, str]:
    """Run fixed strategies only after the data audit and NSE-source gate pass."""
    validation = manifest.get("data_only_validation", {})
    if validation.get("status") != "PASS":
        return _skip_baseline(output_dir, "data-only coverage did not meet the 2021 minimum target")
    source_manifest = manifest.get("source_download_manifest", {})
    if source_manifest.get("provider") != NSE_EXPORT_PROVIDER:
        return _skip_baseline(output_dir, "no verified NSE official CSV import manifest is attached")

    official_symbols = set(source_manifest.get("symbols_with_verified_nse_cache_provenance", []))
    official_equities = {
        symbol for symbol in official_symbols
        if symbol.endswith(".NS") and not symbol.removesuffix(".NS").startswith("DUMMY")
    }
    required_equities = int(validation.get("minimum_target_equities_required", 0))
    if "^NSEI" not in official_symbols or len(official_equities) < required_equities:
        return _skip_baseline(
            output_dir,
            f"official NSE CSV provenance covers {len(official_equities)} equities and NIFTY; {required_equities} equities plus NIFTY are required",
        )

    config = AppConfig.load(root / "config.yaml")
    universe_path = Path(config.universe.symbols_file)
    if not universe_path.is_absolute():
        universe_path = root / universe_path
    universe = pd.read_csv(universe_path)
    raw_symbols = [normalize_symbol(value) for value in universe["Symbol"].dropna() if str(value).strip()]
    equities, _ = remove_placeholder_symbols(raw_symbols)
    symbols = list(dict.fromkeys([*equities, config.universe.benchmark, config.regime.india_vix_symbol]))

    bars: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        cache_file = cache_dir / YahooFinanceProvider.cache_filename(symbol, "1d")
        if not cache_file.exists():
            continue
        try:
            raw = pd.read_csv(cache_file, parse_dates=["timestamp"], index_col="timestamp")
            normalized, _ = validate_and_normalize_bars(raw, "1d")
        except Exception:
            continue
        if not normalized.empty:
            bars[symbol] = normalized

    benchmark = bars.get(config.universe.benchmark)
    if benchmark is None or benchmark.empty:
        return _skip_baseline(output_dir, "NSE NIFTY 50 bars are missing from the audited cache")

    backtest_start = requested_start
    if not manifest.get("preferred_target_met") and backtest_start < date(2021, 1, 1):
        backtest_start = date(2021, 1, 1)
    official_coverage = manifest.get("data_only_validation", {})
    if not official_coverage.get("minimum_target_met") or not official_coverage.get("nifty_end_date_reaches_requested_window"):
        return _skip_baseline(output_dir, "the verified dataset does not cover the full minimum date window")

    eligible = {
        symbol: frame for symbol, frame in bars.items()
        if symbol not in {config.universe.benchmark, config.regime.india_vix_symbol}
        and len(frame) >= config.universe.min_history_rows
    }
    if len(eligible) < required_equities:
        return _skip_baseline(
            output_dir,
            f"only {len(eligible)} equities meet the {config.universe.min_history_rows}-row strategy warm-up; {required_equities} required",
        )

    enabled = set(config.strategies.enabled)
    if any(name not in enabled for name in BASELINE_STRATEGIES):
        missing = sorted(set(BASELINE_STRATEGIES) - enabled)
        return _skip_baseline(output_dir, f"config differs from the fixed 11-strategy baseline: {missing}")
    strategies = build_strategies(
        list(BASELINE_STRATEGIES),
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
    sectors = load_sector_map(universe_path)
    engine = BacktestEngine(config, CostModel(config.costs))
    vix = bars.get(config.regime.india_vix_symbol)
    strategy_rows: list[dict[str, Any]] = []
    yearly_rows: list[dict[str, Any]] = []
    monthly_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []

    for strategy in strategies:
        result = engine.run(
            strategy, eligible, benchmark, CAPITAL, backtest_start, requested_end,
            sectors, config.strategies.exit_mode, vix,
        )
        metrics = performance_metrics(result.trades, result.equity_curve, CAPITAL)
        strategy_rows.append({
            "strategy": strategy.name,
            "status": "BASELINE_ONLY",
            "start": backtest_start.isoformat(),
            "end": requested_end.isoformat(),
            "eligible_equities": len(eligible),
            **metrics,
        })
        trade_rows.extend({**trade, "strategy": strategy.name} for trade in result.trades)
        trades_by_month: dict[str, list[dict[str, Any]]] = {}
        for trade in result.trades:
            month = pd.Timestamp(trade["exit_time"]).strftime("%Y-%m")
            trades_by_month.setdefault(month, []).append(trade)
        months = pd.period_range(backtest_start, requested_end, freq="M").astype(str)
        for month in months:
            metrics_for_period = metrics_for_trades(trades_by_month.get(month, []), CAPITAL)
            monthly_rows.append({"strategy": strategy.name, "month": month, **metrics_for_period})
        for year in range(backtest_start.year, requested_end.year + 1):
            year_trades = [
                trade for trade in result.trades
                if pd.Timestamp(trade["exit_time"]).year == year
            ]
            metrics_for_period = metrics_for_trades(year_trades, CAPITAL)
            yearly_rows.append({
                "strategy": strategy.name,
                "year": f"{year} YTD" if year == requested_end.year else str(year),
                **metrics_for_period,
            })

    output_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(strategy_rows).to_csv(output_dir / "baseline_strategy_results.csv", index=False)
    pd.DataFrame(yearly_rows).to_csv(output_dir / "yearly_results.csv", index=False)
    pd.DataFrame(monthly_rows).to_csv(output_dir / "monthly_results.csv", index=False)
    pd.DataFrame(trade_rows).to_csv(output_dir / "baseline_trades.csv", index=False)
    run_manifest = {
        "status": "COMPLETED_BASELINE_ONLY",
        "provider": manifest["provider"],
        "source_files_by_symbol": source_manifest.get("source_files_by_symbol", {}),
        "period": {"start": backtest_start.isoformat(), "end": requested_end.isoformat()},
        "eligible_equities": len(eligible),
        "strategies": list(BASELINE_STRATEGIES),
        "capital": CAPITAL,
        "configuration": {
            "strategy": config.strategies.model_dump(),
            "risk": config.risk.model_dump(),
            "costs": config.costs.model_dump(),
        },
        "analysis_scope": "unchanged fixed baseline only; no parameter search or strategy optimization",
        "india_vix_available": vix is not None and not vix.empty,
    }
    (output_dir / "baseline_run_manifest.json").write_text(json.dumps(run_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    table = pd.DataFrame(strategy_rows)[["strategy", "trades", "win_rate_pct", "profit_factor", "expectancy_per_trade", "net_return_pct", "maximum_drawdown_pct"]].to_html(index=False, border=0)
    (output_dir / "baseline_report.html").write_text(
        "<!doctype html><html lang='en'><meta charset='utf-8'><title>Phase 3B fixed baseline</title>"
        "<style>body{font:15px system-ui;margin:2rem;color:#182230}table{border-collapse:collapse}td,th{padding:.4rem;border-bottom:1px solid #ddd;text-align:left}</style>"
        f"<h1>Phase 3B fixed baseline</h1><p>Period {backtest_start} to {requested_end}; {len(eligible)} eligible equities. "
        "Fixed rules and costs; no parameter search or optimization.</p>{table}</html>",
        encoding="utf-8",
    )
    return {"status": "COMPLETED_BASELINE_ONLY", "reason": f"ran {len(strategies)} fixed baseline strategies"}
