"""End-to-end backtest orchestration and reproducible artifact writer."""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from nse_research import __version__
from nse_research.backtest.costs import CostModel
from nse_research.backtest.engine import BacktestEngine
from nse_research.backtest.validation import monte_carlo_trade_order, walk_forward_slices
from nse_research.config import AppConfig
from nse_research.data.providers import YahooFinanceProvider, download_many
from nse_research.data.universe import load_constituents, load_sector_map, remove_placeholder_symbols
from nse_research.reporting.performance import performance_metrics
from nse_research.reporting.report import make_comparison, write_charts, write_config_snapshot, write_html_report, write_json
from nse_research.risk.controls import portfolio_drawdown_tripwire
from nse_research.strategies.equity import build_strategies
from nse_research.utils.dates import resolve_backtest_period, warmup_start
from nse_research.utils.logging import configure_logging

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a causal NSE cash-equity backtest")
    parser.add_argument("--config", default="config.yaml", help="YAML config path")
    parser.add_argument("--capital", type=float, help="Run a single capital amount instead of all default scenarios")
    parser.add_argument("--start-date", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Backtest end date (YYYY-MM-DD)")
    parser.add_argument("--interval", help="Override bar interval; daily strategies use 1d, intraday VWAP uses 5m or 15m")
    parser.add_argument("--symbols", help="Optional comma-separated symbols, e.g. RELIANCE.NS,TCS.NS")
    parser.add_argument("--symbols-file", help="Optional CSV with a Symbol column")
    parser.add_argument("--refresh", action="store_true", help="Ignore cached price bars and refresh from the provider")
    return parser


def run_experiment(args: argparse.Namespace) -> tuple[Path, int]:
    project_root = Path(__file__).resolve().parents[2]
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = project_root / config_path
    config = AppConfig.load(config_path)
    if args.interval:
        config.backtest.interval = args.interval
    start_arg = args.start_date or config.backtest.start_date
    end_arg = args.end_date or config.backtest.end_date
    capital_scenarios = [args.capital] if args.capital else list(dict.fromkeys(config.backtest.capital_scenarios))
    if args.capital and args.capital <= 0:
        raise ValueError("capital must be positive")

    run_clock = datetime.now(ZoneInfo(config.project.timezone))
    run_id = run_clock.strftime("%Y%m%d_%H%M%S")
    result_dir = Path(config.project.result_root) / run_id
    if not result_dir.is_absolute():
        result_dir = project_root / result_dir
    chart_dir = result_dir / "charts"
    result_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(result_dir / "run.log")

    config_snapshot = config.model_dump(mode="json")
    config_snapshot["experiment"] = {
        "run_id": run_id,
        "requested_start": start_arg,
        "requested_end": end_arg,
        "capital_scenarios": capital_scenarios,
        "strategy_version": __version__,
        "data_provider": config.data.provider,
        "interval": config.backtest.interval,
    }
    write_config_snapshot(result_dir / "config.yaml", config_snapshot)

    warnings = [
        "Current Nifty 500 constituents are not point-in-time historical membership; survivorship bias remains.",
        "Yahoo Finance OHLCV is a research feed and can contain omissions, adjustments, or delayed data; data is not exchange-certified.",
        "The fixed strategy rules are not tuned on the test slice. Two months and a small trade count cannot establish robustness.",
        f"The portfolio kill tripwire is {portfolio_drawdown_tripwire(config.risk):.1%} against a configured {config.risk.max_portfolio_drawdown:.1%} cap; it reserves up to one per-trade risk unit, halts new entries, and liquidates at observed bar closes. Gaps can still push realized drawdown beyond the cap.",
    ]
    if config.backtest.interval not in {"1d", "1wk", "1mo"}:
        warnings.append("Yahoo intraday retention is rolling and may not cover the complete requested two-calendar-month interval.")

    try:
        symbols = _load_symbols(args, config, project_root)
        symbols, placeholder_failures = remove_placeholder_symbols(symbols)
    except Exception as exc:
        warnings.append(f"Universe unavailable: {exc}")
        _write_empty_run(result_dir, warnings, "NOT AVAILABLE", config_snapshot)
        print(f"NO DATA AVAILABLE: {exc}")
        return result_dir, 2

    today = run_clock.date()
    if end_arg:
        request_end = min(pd.Timestamp(end_arg).date(), today)
    else:
        request_end = today
    requested_start = pd.Timestamp(start_arg).date() if start_arg else (pd.Timestamp(request_end) - pd.DateOffset(months=config.backtest.lookback_months)).date()
    interval = config.backtest.interval
    if interval in {"1d", "1wk", "1mo"}:
        fetch_start = warmup_start(requested_start, config.backtest.warmup_calendar_days)
    else:
        fetch_start = max(requested_start, request_end - timedelta(days=config.data.intraday_max_age_days))
    all_symbols = list(dict.fromkeys([*symbols, config.universe.benchmark, config.regime.india_vix_symbol]))
    provider = YahooFinanceProvider(
        cache_dir=project_root / config.data.cache_dir,
        auto_adjust=config.data.auto_adjust,
        force_refresh=args.refresh,
    )
    bars, failures = download_many(provider, all_symbols, fetch_start, request_end, interval)
    failures.extend(placeholder_failures)
    benchmark_bars = bars.get(config.universe.benchmark)
    observed = benchmark_bars.index if benchmark_bars is not None and not benchmark_bars.empty else _first_index(bars)
    if observed is None:
        reason = "no price bars were downloaded; install requirements and check data-provider/network access"
        warnings.append(reason)
        pd.DataFrame(failures).to_csv(result_dir / "data_failures.csv", index=False)
        write_json(result_dir / "data_manifest.json", _data_manifest(bars, failures, config, None, None))
        _write_empty_run(result_dir, warnings, "NOT AVAILABLE", config_snapshot)
        print(f"NO DATA AVAILABLE: {reason}")
        print(f"Experiment record: {result_dir}")
        return result_dir, 2
    try:
        actual_start, actual_end = resolve_backtest_period(observed, config.backtest.lookback_months, start_arg, end_arg, run_clock)
    except ValueError as exc:
        warnings.append(str(exc))
        _write_empty_run(result_dir, warnings, "NOT AVAILABLE", config_snapshot)
        return result_dir, 2
    first_observed = min(pd.Timestamp(value).date() for value in observed)
    if actual_start < first_observed:
        warnings.append(f"Provider history begins {first_observed}; the requested period starts {actual_start}, so the effective test starts at the available data boundary.")
        actual_start = first_observed
    if actual_start > requested_start:
        warnings.append(f"Actual data starts on {actual_start}; requested period began {requested_start}.")
    if benchmark_bars is None or benchmark_bars.empty:
        warnings.append("NIFTY 50 benchmark bars unavailable; each stock's own technical regime is used where possible.")
    for failure in failures:
        if failure["symbol"] != config.universe.benchmark:
            warnings.append(f"Skipped {failure['symbol']}: {failure['reason']}")
    late_warmup_symbols = [
        symbol for symbol in symbols
        if symbol in bars and bars[symbol].index.min().date() > fetch_start + timedelta(days=4)
    ]
    if late_warmup_symbols:
        warnings.append(
            f"{len(late_warmup_symbols)} symbols have cached history beginning after the requested warm-up date; indicators stay unavailable until enough observations accumulate. See symbol_coverage in data_manifest.json."
        )

    india_vix_bars = bars.get(config.regime.india_vix_symbol)
    data_manifest = _data_manifest(bars, failures, config, actual_start, actual_end, provider.quality_reports)
    data_manifest["india_vix"] = {
        "symbol": config.regime.india_vix_symbol,
        "available": india_vix_bars is not None and not india_vix_bars.empty,
        "high_volatility_threshold": config.regime.india_vix_high_threshold,
    }
    missing_session_total = sum(
        int(item.get("missing_benchmark_sessions_inside_observed_span", 0))
        for item in data_manifest.get("symbol_coverage", {}).values()
    )
    missing_intraday_total = sum(
        int(item.get("missing_intraday_candles_estimate", 0))
        for item in data_manifest.get("symbol_coverage", {}).values()
    )
    if missing_session_total:
        warnings.append(f"Found {missing_session_total} symbol/session omissions inside observed symbol spans versus NIFTY 50 sessions; see data_manifest.json. No bars were filled.")
    if missing_intraday_total:
        warnings.append(f"Estimated {missing_intraday_total} missing intraday candles across observed symbol sessions; see data_manifest.json. Illiquid intervals may naturally have no trade.")
    write_json(result_dir / "data_manifest.json", data_manifest)
    pd.DataFrame(failures).to_csv(result_dir / "data_failures.csv", index=False)

    eligible: dict[str, pd.DataFrame] = {}
    rejected_rows: list[dict[str, str]] = []
    for symbol in symbols:
        frame = bars.get(symbol)
        if frame is None or len(frame) < config.universe.min_history_rows:
            rejected_rows.append({"symbol": symbol, "strategy": "", "reason": "insufficient pre-period/warm-up history"})
            continue
        eligible[symbol] = frame
    sector_file = Path(config.universe.symbols_file)
    if not sector_file.is_absolute():
        sector_file = project_root / sector_file
    sector_by_symbol = load_sector_map(sector_file)

    session_index = benchmark_bars.index if benchmark_bars is not None else observed
    sessions = len({pd.Timestamp(stamp).date() for stamp in session_index if actual_start <= pd.Timestamp(stamp).date() <= actual_end})
    print("BACKTEST PERIOD")
    print(f"Start: {actual_start}")
    print(f"End: {actual_end}")
    print(f"Trading sessions: {sessions}")

    strategies = build_strategies(
        config.strategies.enabled,
        config.strategies.reward_risk,
        config.strategies.atr_stop_multiple,
        config.strategies.adx_minimum,
        config.strategies.volume_multiple,
        config.strategies.entry_cutoff,
        interval,
        config.risk.allow_short_intraday,
        config.universe.min_avg_volume,
        config.universe.min_avg_traded_value,
        config.universe.min_price,
        config.strategies.stop_mode,
        config.strategies.signal_score_weights,
    )
    if "vwap_intraday" in config.strategies.enabled and interval in {"1d", "1wk", "1mo"}:
        warnings.append("VWAP intraday strategy was not run because this experiment uses daily bars. Use a reliable 5m or 15m data source for intraday testing.")
    if india_vix_bars is None or india_vix_bars.empty:
        warnings.append(f"India VIX ({config.regime.india_vix_symbol}) was unavailable; HIGH_VOLATILITY uses realized benchmark volatility only.")
    if not strategies or not eligible:
        warnings.append("No eligible equity strategy or symbol data was available for this interval.")
        _write_empty_run(result_dir, warnings, "PARTIAL", config_snapshot)
        return result_dir, 2

    cost_model = CostModel(config.costs)
    engine = BacktestEngine(config, cost_model)
    all_trades: list[dict[str, object]] = []
    all_rejected: list[dict[str, object]] = list(rejected_rows)
    metric_rows: list[dict[str, object]] = []
    equity_rows: list[dict[str, object]] = []
    daily_rows: list[dict[str, object]] = []
    cost_rows: list[dict[str, object]] = []
    monthly_rows: list[dict[str, object]] = []
    exit_comparison_rows: list[dict[str, object]] = []
    reward_risk_rows: list[dict[str, object]] = []
    walk_results: dict[str, object] = {}
    monte_results: dict[str, object] = {}
    monte_samples: list[pd.DataFrame] = []
    baseline_by_strategy: dict[str, tuple[object, dict[str, object]]] = {}
    reference_capital = 100000.0 if 100000.0 in capital_scenarios else capital_scenarios[-1]

    for strategy in strategies:
        baseline_result = None
        baseline_metrics = None
        for capital in capital_scenarios:
            print(f"Running {strategy.name} | capital ₹{capital:,.0f} | {len(eligible)} symbols", flush=True)
            LOGGER.info("Backtest start strategy=%s capital=%.2f symbols=%d", strategy.name, capital, len(eligible))
            result = engine.run(
                strategy, eligible, benchmark_bars, capital, actual_start, actual_end,
                sector_by_symbol, config.strategies.exit_mode, india_vix_bars,
            )
            LOGGER.info("Backtest end strategy=%s capital=%.2f trades=%d", strategy.name, capital, len(result.trades))
            for trade in result.trades:
                all_trades.append({**trade, "initial_capital": capital})
            for rejected in result.rejected:
                all_rejected.append({**rejected, "strategy": strategy.name})
            curve = [{**row, "strategy": strategy.name, "initial_capital": capital} for row in result.equity_curve]
            if capital == reference_capital:
                equity_rows.extend(curve)
                baseline_result = result
            metrics = performance_metrics(result.trades, result.equity_curve, capital)
            metrics["strategy"] = strategy.name
            metrics["initial_capital"] = capital
            metrics["strategy_version"] = __version__
            metrics["exit_mode"] = config.strategies.exit_mode
            metric_rows.append(metrics)
            if capital == reference_capital:
                baseline_metrics = metrics
                baseline_by_strategy[strategy.name] = (result, metrics)
                walk_results[strategy.name] = walk_forward_slices(result.trades, actual_start, actual_end, capital)
                mc, samples = monte_carlo_trade_order(
                    result.trades, capital, config.risk.risk_per_trade,
                    simulations=5000, seed=config.project.random_seed,
                    max_portfolio_drawdown=portfolio_drawdown_tripwire(config.risk),
                )
                monte_results[strategy.name] = mc
                if not samples.empty:
                    samples["strategy"] = strategy.name
                    samples["initial_capital"] = capital
                    monte_samples.append(samples)
            if result.trades:
                trade_frame = pd.DataFrame(result.trades)
                daily = trade_frame.assign(day=pd.to_datetime(trade_frame["exit_time"]).dt.date).groupby("day", as_index=False)["net_pnl"].sum()
                for row in daily.to_dict("records"):
                    daily_rows.append({**row, "strategy": strategy.name, "initial_capital": capital})
                monthly = trade_frame.assign(month=pd.to_datetime(trade_frame["exit_time"]).dt.to_period("M").astype(str)).groupby("month", as_index=False).agg(
                    trades=("net_pnl", "size"), gross_pnl=("gross_pnl", "sum"), trading_costs=("costs", "sum"), net_pnl=("net_pnl", "sum")
                )
                monthly_map = {row["month"]: row for row in monthly.to_dict("records")}
                costs = trade_frame.groupby("strategy", as_index=False).agg(gross_pnl=("gross_pnl", "sum"), trading_costs=("costs", "sum"), net_pnl=("net_pnl", "sum"), entry_costs=("entry_cost", "sum"), exit_costs=("exit_cost", "sum"))
                for row in costs.to_dict("records"):
                    cost_rows.append({**row, "initial_capital": capital})
            else:
                monthly_map = {}
            for month in pd.period_range(actual_start, actual_end, freq="M").astype(str):
                row = monthly_map.get(month, {"month": month, "trades": 0, "gross_pnl": 0.0, "trading_costs": 0.0, "net_pnl": 0.0})
                monthly_rows.append({**row, "strategy": strategy.name, "initial_capital": capital})

    # Exit behavior is a declared research grid at reference capital. The default
    # mode result is reused from the required all-capital experiment.
    for strategy in strategies:
        base_result, base_metrics = baseline_by_strategy[strategy.name]
        for mode in config.strategies.exit_comparison_modes:
            if mode == config.strategies.exit_mode:
                result, metrics = base_result, base_metrics
            else:
                result = engine.run(
                    strategy, eligible, benchmark_bars, reference_capital, actual_start, actual_end,
                    sector_by_symbol, mode, india_vix_bars,
                )
                metrics = performance_metrics(result.trades, result.equity_curve, reference_capital)
            exit_comparison_rows.append({
                "strategy": strategy.name, "exit_mode": mode, "initial_capital": reference_capital,
                **{key: metrics.get(key) for key in [
                    "trades", "winning_trades", "losing_trades", "win_rate_pct", "average_win",
                    "average_loss", "payoff_ratio", "profit_factor", "expectancy_per_trade",
                    "gross_pnl", "net_pnl", "net_return_pct", "maximum_drawdown_pct",
                    "sharpe", "sortino", "trading_costs", "turnover",
                ]},
            })

    # The trend-following target sweep is a predeclared sensitivity analysis;
    # it does not choose an optimized target from the test period.
    trend = next((item for item in strategies if item.name == "trend_following"), None)
    if trend is not None:
        for reward_risk in config.strategies.reward_risk_choices:
            variant = replace(trend, reward_risk=float(reward_risk))
            if float(reward_risk) == float(config.strategies.reward_risk):
                result, metrics = baseline_by_strategy[trend.name]
            else:
                result = engine.run(
                    variant, eligible, benchmark_bars, reference_capital, actual_start, actual_end,
                    sector_by_symbol, "fixed_r", india_vix_bars,
                )
                metrics = performance_metrics(result.trades, result.equity_curve, reference_capital)
            reward_risk_rows.append({
                "strategy": trend.name, "reward_risk_target": reward_risk,
                **{key: metrics.get(key) for key in [
                    "trades", "win_rate_pct", "average_win", "average_loss", "payoff_ratio",
                    "profit_factor", "expectancy_per_trade", "net_pnl", "net_return_pct",
                    "maximum_drawdown_pct", "trading_costs", "turnover",
                ]},
            })

    metric_frame = pd.DataFrame(metric_rows)
    comparison, best_strategy = make_comparison(metric_rows, reference_capital)
    comparison.to_csv(result_dir / "strategy_comparison.csv", index=False)
    pd.DataFrame(all_trades).to_csv(result_dir / "all_trades.csv", index=False)
    pd.DataFrame(daily_rows).to_csv(result_dir / "daily_pnl.csv", index=False)
    pd.DataFrame(monthly_rows).to_csv(result_dir / "monthly_pnl.csv", index=False)
    pd.DataFrame(equity_rows).to_csv(result_dir / "equity_curve.csv", index=False)
    if equity_rows:
        drawdowns = pd.DataFrame(equity_rows)[["timestamp", "strategy", "initial_capital", "drawdown"]]
    else:
        drawdowns = pd.DataFrame(columns=["timestamp", "strategy", "initial_capital", "drawdown"])
    drawdowns.to_csv(result_dir / "drawdown_curve.csv", index=False)
    metric_frame.to_csv(result_dir / "strategy_metrics.csv", index=False)
    pd.DataFrame(all_rejected).to_csv(result_dir / "rejected_signals.csv", index=False)
    pd.DataFrame(cost_rows).to_csv(result_dir / "transaction_costs.csv", index=False)
    pd.DataFrame(exit_comparison_rows).to_csv(result_dir / "exit_comparison.csv", index=False)
    pd.DataFrame(reward_risk_rows).to_csv(result_dir / "reward_risk_sensitivity.csv", index=False)
    milestones = []
    for row in metric_rows:
        multiple = float(row.get("capital_multiple", 0.0))
        for threshold in [1.25, 1.50, 1.75, 2.00]:
            milestones.append({
                "strategy": row["strategy"], "initial_capital": row["initial_capital"],
                "target_multiple": threshold,
                "target_capital": float(row["initial_capital"]) * threshold,
                "final_capital": row["final_capital"], "observed_multiple": multiple,
                "status": "ACHIEVED" if multiple >= threshold else "NOT ACHIEVED",
            })
    pd.DataFrame(milestones).to_csv(result_dir / "capital_milestones.csv", index=False)
    capital_growth = metric_frame[["strategy", "initial_capital", "final_capital", "net_return_pct"]].rename(columns={"initial_capital": "capital"})
    capital_growth.to_csv(result_dir / "capital_growth.csv", index=False)
    pd.concat(monte_samples, ignore_index=True).to_csv(result_dir / "monte_carlo_samples.csv", index=False) if monte_samples else pd.DataFrame().to_csv(result_dir / "monte_carlo_samples.csv", index=False)
    write_json(result_dir / "walk_forward.json", walk_results)
    write_json(result_dir / "monte_carlo_summary.json", monte_results)
    if all(
        value.get("slices", {}).get("out_of_sample_test", {}).get("metrics", {}).get("trades", 0) < 10
        for value in walk_results.values()
    ):
        warnings.append("Walk-forward slices use fixed, predeclared rules; most two-month slices will have too few trades for statistical conclusions.")
    warnings.append("Cash-equity swing strategies are long-only; short cash positions are limited to intraday VWAP simulation.")

    chart_files = write_charts(chart_dir, equity_rows, metric_rows, capital_growth.to_dict("records"))
    write_html_report(
        result_dir / "report.html",
        {"start": actual_start.isoformat(), "end": actual_end.isoformat(), "sessions": sessions, "symbols": len(eligible), "provider": config.data.provider, "reference_capital": reference_capital},
        comparison,
        metric_rows,
        None,
        monte_results,
        walk_results,
        warnings,
        "AVAILABLE" if eligible else "NOT AVAILABLE",
        details={
            "data": {
                "provider": config.data.provider,
                "interval": interval,
                "symbols_with_price_data": len(bars),
                "eligible_strategy_symbols": len(eligible),
                "estimated_missing_symbol_sessions": missing_session_total,
                "estimated_missing_intraday_candles": missing_intraday_total,
                "failed_symbols": len(failures),
                "India VIX available": bool(india_vix_bars is not None and not india_vix_bars.empty),
            },
            "risk": config.risk.model_dump(),
            "execution_costs": config.costs.model_dump(),
            "strategy_parameters": config.strategies.model_dump(),
            "exit_comparison": exit_comparison_rows,
            "reward_risk_sensitivity": reward_risk_rows,
            "monthly_pnl": monthly_rows,
            "all_scenario_metrics": [
                {key: row.get(key) for key in [
                    "strategy", "initial_capital", "trades", "winning_trades", "losing_trades",
                    "win_rate_pct", "average_win", "average_loss", "payoff_ratio", "profit_factor",
                    "expectancy_per_trade", "gross_pnl", "net_pnl", "net_return_pct",
                    "maximum_drawdown_pct", "sharpe", "sortino", "longest_winning_streak",
                    "longest_losing_streak", "average_holding_days", "trading_costs", "turnover",
                    "capital_multiple", "trade_sample_status",
                ]}
                for row in metric_rows
            ],
        },
    )
    write_json(result_dir / "run_manifest.json", {
        "status": "COMPLETED",
        "run_id": run_id,
        "created_at": run_clock.isoformat(),
        "software_version": __version__,
        "strategy_parameters": config.strategies.model_dump(),
        "risk_settings": config.risk.model_dump(),
        "cost_settings": config.costs.model_dump(),
        "period": {"start": actual_start, "end": actual_end, "sessions": sessions},
        "universe": {"eligible_symbols": len(eligible), "symbols_with_bars": len(bars), "sector_labels": len(set(sector_by_symbol.values()))},
        "backtest": {"trades": len(all_trades), "capital_scenarios": capital_scenarios, "reference_capital": reference_capital},
        "objective_analysis": {
            "strategies_with_observed_win_rate_at_least_90_pct": [
                row["strategy"] for row in metric_rows
                if row["initial_capital"] == reference_capital and row.get("win_rate_90pct_plus")
            ],
            "capital_milestones": "see capital_milestones.csv; NOT ACHIEVED where final capital multiple is below target",
        },
        "strategy_status": {"vwap_intraday": "DATA LIMITED — not run on daily bars" if interval in {"1d", "1wk", "1mo"} else "RUN"},
        "options": "NOT RUN — equity-only phase; no options results were produced",
        "warnings": warnings,
        "charts": chart_files,
    })

    print_summary(reference_capital, metric_rows, None, result_dir)
    return result_dir, 0


def _load_symbols(args: argparse.Namespace, config: AppConfig, root: Path) -> list[str]:
    if args.symbols:
        symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
        return [symbol if symbol.endswith(".NS") or symbol.startswith("^") else f"{symbol}.NS" for symbol in symbols]
    symbols_path = Path(args.symbols_file or config.universe.symbols_file)
    if not symbols_path.is_absolute():
        symbols_path = root / symbols_path
    if args.symbols_file:
        if not symbols_path.exists():
            raise FileNotFoundError(f"symbols file not found: {symbols_path}")
        frame = pd.read_csv(symbols_path)
        column = next((item for item in frame.columns if item.lower() in {"symbol", "ticker"}), None)
        if column is None:
            raise ValueError("symbols file requires a Symbol column")
        return [str(item).strip().upper() + ("" if str(item).strip().upper().endswith(".NS") else ".NS") for item in frame[column].dropna()]
    return load_constituents(config.universe.constituents_url, symbols_path, config.universe.max_symbols)


def _first_index(bars: dict[str, pd.DataFrame]) -> pd.Index | None:
    for frame in bars.values():
        if not frame.empty:
            return frame.index
    return None


def _data_manifest(
    bars: dict[str, pd.DataFrame],
    failures: list[dict[str, str]],
    config: AppConfig,
    start: date | None,
    end: date | None,
    quality_reports: dict[str, dict[str, int | str]] | None = None,
) -> dict[str, object]:
    benchmark = bars.get(config.universe.benchmark)
    expected_sessions = set()
    if benchmark is not None and start is not None and end is not None:
        expected_sessions = {
            pd.Timestamp(stamp).date() for stamp in benchmark.index
            if start <= pd.Timestamp(stamp).date() <= end
        }
    coverage = {}
    for symbol, frame in bars.items():
        dates = {pd.Timestamp(stamp).date() for stamp in frame.index}
        observed_in_period = {day for day in dates if start is None or end is None or start <= day <= end}
        if expected_sessions and observed_in_period:
            first_day, last_day = min(observed_in_period), max(observed_in_period)
            expected_for_symbol = {day for day in expected_sessions if first_day <= day <= last_day}
            missing_sessions = sorted(expected_for_symbol - observed_in_period)
        else:
            missing_sessions = []
        missing_intraday = 0
        intraday_missing_dates: list[str] = []
        if config.backtest.interval.endswith("m") and len(frame):
            minutes = int(config.backtest.interval[:-1])
            timestamps_by_day: dict[date, set[pd.Timestamp]] = {}
            for stamp in frame.index:
                timestamps_by_day.setdefault(pd.Timestamp(stamp).date(), set()).add(pd.Timestamp(stamp))
            for day in sorted(observed_in_period):
                expected = set(pd.date_range(
                    pd.Timestamp(day) + pd.Timedelta(hours=9, minutes=15),
                    pd.Timestamp(day) + pd.Timedelta(hours=15, minutes=30),
                    freq=f"{minutes}min", inclusive="left",
                ))
                observed_times = timestamps_by_day.get(day, set())
                missing_count = len(expected - observed_times)
                missing_intraday += missing_count
                if missing_count:
                    intraday_missing_dates.append(day.isoformat())
        splits = int(pd.to_numeric(frame.get("stock_splits", pd.Series(dtype=float)), errors="coerce").fillna(0).ne(0).sum()) if len(frame) else 0
        dividends = int(pd.to_numeric(frame.get("dividends", pd.Series(dtype=float)), errors="coerce").fillna(0).ne(0).sum()) if len(frame) else 0
        coverage[symbol] = {
            "rows": len(frame),
            "first_bar": frame.index.min().isoformat() if len(frame) else None,
            "last_bar": frame.index.max().isoformat() if len(frame) else None,
            "missing_benchmark_sessions_inside_observed_span": len(missing_sessions),
            "missing_session_dates": [day.isoformat() for day in missing_sessions],
            "missing_intraday_candles_estimate": missing_intraday,
            "sessions_with_intraday_gaps": intraday_missing_dates,
            "split_events_in_cached_bars": splits,
            "dividend_events_in_cached_bars": dividends,
            "normalization_audit": (quality_reports or {}).get(symbol, {}),
        }
    return {
        "provider": config.data.provider,
        "provider_package_version": _package_version("yfinance"),
        "python_version": sys.version.split()[0],
        "auto_adjust": config.data.auto_adjust,
        "interval": config.backtest.interval,
        "backtest_start": start.isoformat() if start else None,
        "backtest_end": end.isoformat() if end else None,
        "symbols_with_data": len(bars),
        "failed_symbols": failures,
        "symbol_coverage": coverage,
        "universe": config.universe.source,
        "universe_membership": "current constituent list; not point-in-time",
        "missing_candles": "per-symbol dates absent inside observed backtest span compared with NIFTY 50 sessions; no rows are synthesized",
        "corporate_actions": "Yahoo action columns counted when present; auto-adjust setting recorded separately; symbol/name changes are not reconstructed point-in-time",
    }


def _package_version(name: str) -> str | None:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _write_empty_run(result_dir: Path, warnings: list[str], data_status: str, config_snapshot: dict[str, object]) -> None:
    write_config_snapshot(result_dir / "config.yaml", config_snapshot)
    write_json(result_dir / "run_manifest.json", {"status": data_status, "data_status": data_status, "warnings": warnings, "options": "NOT RUN — equity-only phase"})
    for filename in [
        "all_trades.csv", "daily_pnl.csv", "monthly_pnl.csv", "equity_curve.csv",
        "drawdown_curve.csv", "strategy_metrics.csv", "strategy_comparison.csv",
        "rejected_signals.csv", "transaction_costs.csv", "capital_growth.csv",
        "exit_comparison.csv", "reward_risk_sensitivity.csv", "capital_milestones.csv",
    ]:
        pd.DataFrame().to_csv(result_dir / filename, index=False)
    write_json(result_dir / "walk_forward.json", {"status": "unavailable", "reason": "no data-backed trades"})
    write_json(result_dir / "monte_carlo_summary.json", {"status": "unavailable", "reason": "no data-backed trades"})
    write_html_report(result_dir / "report.html", {}, pd.DataFrame(), [], None, {}, {}, warnings, data_status)


def print_summary(reference_capital: float, metric_rows: list[dict[str, object]], best: str | None, result_dir: Path) -> None:
    print("\nSTRATEGY COMPARISON (reference capital ₹{:,.0f})".format(reference_capital))
    print("Strategy | Trades | Win % | PF | Expectancy/trade | Return | Max DD | Costs")
    for row in sorted((row for row in metric_rows if row["initial_capital"] == reference_capital), key=lambda item: str(item["strategy"])):
        win = "—" if row["win_rate_pct"] is None else f"{row['win_rate_pct']:.1f}%"
        pf = "∞" if row["profit_factor"] == float("inf") else f"{row['profit_factor']:.2f}"
        print(f"{row['strategy']} | {row['trades']} | {win} | {pf} | ₹{row['expectancy_per_trade']:.2f} | {row['net_return_pct']:.2f}% | {row['maximum_drawdown_pct']:.2f}% | ₹{row['trading_costs']:.2f}")
    print(f"Experiment artifacts: {result_dir}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result_dir, code = run_experiment(args)
        return code
    except Exception as exc:
        logging.exception("Backtest run failed")
        print(f"BACKTEST FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
