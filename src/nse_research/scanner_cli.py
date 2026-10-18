"""Command-line daily scanner and data downloader."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from nse_research.backtest.costs import CostModel
from nse_research.config import AppConfig
from nse_research.data.providers import YahooFinanceProvider, download_many
from nse_research.data.universe import load_constituents
from nse_research.paper.trader import active_paper_positions, paper_portfolio_drawdown, paper_realized_pnl, record_paper_candidates, update_paper_positions
from nse_research.reporting.report import write_json
from nse_research.scanner.scanner import scan_universe
from nse_research.strategies.equity import build_strategies
from nse_research.utils.dates import latest_completed_trading_date
from nse_research.utils.logging import configure_logging

LOGGER = logging.getLogger(__name__)


def parser(mode: str = "scan") -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Scan liquid NSE cash equities")
    result.add_argument("--config", default="config.yaml")
    result.add_argument("--capital", type=float)
    result.add_argument("--interval", default=None, help="1d, 5m, or 15m")
    result.add_argument("--symbols", help="Comma-separated symbols for a focused scan")
    result.add_argument("--symbols-file")
    result.add_argument("--database", help="SQLite path for paper mode")
    result.add_argument("--download-only", action="store_true", help="Fetch/cache bars but do not produce candidates")
    return result


def run(argv: list[str] | None = None, mode: str = "scan") -> int:
    args = parser(mode).parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    config_file = Path(args.config)
    if not config_file.is_absolute():
        config_file = root / config_file
    config = AppConfig.load(config_file)
    universe_file = Path(config.universe.symbols_file)
    if not universe_file.is_absolute():
        config.universe.symbols_file = str(root / universe_file)
    interval = args.interval or "1d"
    capital = args.capital or config.backtest.capital
    today = datetime.now(ZoneInfo(config.project.timezone)).date()
    end = today
    lookback = 59 if interval not in {"1d", "1wk", "1mo"} else config.backtest.warmup_calendar_days
    start = today - timedelta(days=lookback)
    if args.symbols:
        symbols = [value.strip().upper() for value in args.symbols.split(",") if value.strip()]
        symbols = [value if value.endswith(".NS") or value.startswith("^") else f"{value}.NS" for value in symbols]
    else:
        constituents_path = Path(args.symbols_file or config.universe.symbols_file)
        if not constituents_path.is_absolute():
            constituents_path = root / constituents_path
        symbols = load_constituents(config.universe.constituents_url, constituents_path, config.universe.max_symbols)
    all_symbols = list(dict.fromkeys([*symbols, config.universe.benchmark, config.regime.india_vix_symbol]))
    provider = YahooFinanceProvider(root / config.data.cache_dir, config.data.auto_adjust, force_refresh=False)
    bars, failures = download_many(provider, all_symbols, start, end, interval)
    benchmark = bars.get(config.universe.benchmark)
    if not bars:
        print("NO DATA AVAILABLE: check Python dependencies and market-data connectivity.")
        return 2
    reference = benchmark.index if benchmark is not None and not benchmark.empty else next(iter(bars.values())).index
    if interval.endswith("m"):
        step = int(interval[:-1])
        completed_bars = _completed_intraday_bars(bars, step, config.project.timezone)
        bars = completed_bars
        if not bars:
            print("NO DATA AVAILABLE: no completed intraday bars yet.")
            return 2
        latest_stamp = max(frame.index.max() for frame in bars.values())
        completed = pd.Timestamp(latest_stamp).date()
        benchmark = bars.get(config.universe.benchmark)
    else:
        completed = latest_completed_trading_date(reference)
        bars = {symbol: frame.loc[[pd.Timestamp(stamp).date() <= completed for stamp in frame.index]] for symbol, frame in bars.items()}
    bars = {symbol: frame for symbol, frame in bars.items() if not frame.empty}
    scan_id = completed.isoformat()
    output_dir = root / config.project.result_root / "scans" / scan_id
    output_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(output_dir / "scan.log")
    pd.DataFrame(failures).to_csv(output_dir / "data_failures.csv", index=False)
    if args.download_only:
        write_json(output_dir / "download_manifest.json", {"symbols_requested": len(all_symbols), "symbols_downloaded": len(bars), "last_completed_session": scan_id, "interval": interval})
        print(f"Downloaded/cached {len(bars)} symbols through {scan_id}; manifest: {output_dir / 'download_manifest.json'}")
        return 0

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
    equities = {symbol: frame for symbol, frame in bars.items() if symbol not in {config.universe.benchmark, config.regime.india_vix_symbol}}
    india_vix = bars.get(config.regime.india_vix_symbol)
    database = Path(args.database or config.paper.database)
    if not database.is_absolute():
        database = root / database
    existing_positions: list[dict[str, object]] = []
    realized_day_pnl = 0.0
    portfolio_drawdown_pct = 0.0
    exited = 0
    if mode == "paper":
        exited = update_paper_positions(database, equities, config)
        existing_positions = active_paper_positions(database)
        realized_day_pnl = paper_realized_pnl(database, scan_id, equities)
        portfolio_drawdown_pct = paper_portfolio_drawdown(database, capital, equities, config)
    candidates, rejected = scan_universe(
        equities, benchmark, strategies, config, capital,
        existing_positions, realized_day_pnl, india_vix, portfolio_drawdown_pct,
    )
    pd.DataFrame(candidates).to_csv(output_dir / "candidates.csv", index=False)
    pd.DataFrame(rejected).to_csv(output_dir / "rejected_signals.csv", index=False)
    write_json(output_dir / "scan_manifest.json", {
        "date": scan_id,
        "interval": interval,
        "capital": capital,
        "candidate_count": len(candidates),
        "rejected_count": len(rejected),
        "data_provider": config.data.provider,
        "live_orders_sent": False,
        "paper_only": mode == "paper",
        "open_paper_positions_reserved": len(existing_positions),
        "paper_portfolio_drawdown_pct": portfolio_drawdown_pct * 100,
        "risk_limits": config.risk.model_dump(),
    })
    print("TODAY'S TOP CANDIDATES")
    for rank, candidate in enumerate(candidates[:20], 1):
        print(
            f"{rank}. {candidate['symbol']} | {candidate['strategy']} {candidate['direction']} | "
            f"entry ₹{candidate['entry_estimate']:.2f} SL ₹{candidate['stop_loss']:.2f} "
            f"target ₹{candidate['target']:.2f} qty {candidate['quantity']} "
            f"R:R {candidate['risk_reward']:.2f} score {candidate['signal_score']:.1f} "
            f"regime {candidate['market_regime']}"
        )
        LOGGER.info("ACCEPTED %s %s score=%.1f reason=%s", candidate["symbol"], candidate["strategy"], candidate["signal_score"], candidate["reason"])
    for rejection in rejected:
        LOGGER.info("REJECTED %s %s reason=%s", rejection.get("symbol", ""), rejection.get("strategy", ""), rejection.get("reason", ""))
    if not candidates:
        print("NO TRADE: no candidates passed liquidity, regime, signal, and risk filters.")
    print(f"Rejected-symbol details: {output_dir / 'rejected_signals.csv'}")
    if mode == "paper":
        count = record_paper_candidates(database, candidates)
        print(f"Paper-only journaled {count} signals and closed {exited} simulated positions in {database}. No broker orders were sent.")
    return 0


def _completed_intraday_bars(
    bars: dict[str, pd.DataFrame], interval_minutes: int, timezone_name: str
) -> dict[str, pd.DataFrame]:
    now = datetime.now(ZoneInfo(timezone_name))
    delta = pd.Timedelta(minutes=interval_minutes)
    result: dict[str, pd.DataFrame] = {}
    for symbol, frame in bars.items():
        local_index = pd.DatetimeIndex(frame.index)
        if local_index.tz is None:
            local_index = local_index.tz_localize(timezone_name)
        else:
            local_index = local_index.tz_convert(timezone_name)
        keep = (local_index + delta) <= pd.Timestamp(now)
        completed_frame = frame.loc[keep]
        if not completed_frame.empty:
            result[symbol] = completed_frame
    return result


def download_main(argv: list[str] | None = None) -> int:
    arguments = list(argv or [])
    if "--download-only" not in arguments:
        arguments.append("--download-only")
    return run(arguments, "download")
