"""Manual daily forward-paper entry point. It never creates a live broker."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from nse_research.config import AppConfig
from nse_research.forward.data import LocalEodCsvProvider, PersonalPaperEodProvider
from nse_research.forward.engine import load_local_universe, run_daily_paper_workflow
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.forward.preflight import run_preflight


def _enabled(value: str | None, fallback: bool) -> bool:
    if value is None:
        return fallback
    return value.strip().lower() in {"1", "true", "yes", "on"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one forward-only daily NSE paper scan")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--preflight", action="store_true",
                        help="check Phase 3D readiness without creating orders or changing the paper ledger")
    dates = parser.add_mutually_exclusive_group()
    dates.add_argument("--market-date", help="Run the production forward ledger for this completed date")
    dates.add_argument("--date", help="Replay date YYYY-MM-DD; uses a separate replay ledger and output tree")
    parser.add_argument("--provider", choices=("licensed_nse_cm_eod", "local_eod_csv", "personal_local_eod"),
                        help="Explicit local provider; no network fallback")
    parser.add_argument("--licensed-dataset", help="Canonical NSE licensed EOD dataset CSV")
    parser.add_argument("--market-data-dir")
    parser.add_argument("--daily-data-dir", help="Validated normalized dated personal EOD files")
    parser.add_argument("--identity-file", help="Universe with explicit EQ series metadata")
    parser.add_argument("--index-data-dir")
    parser.add_argument("--database")
    parser.add_argument("--capital", type=float, help="Starting capital for a new forward ledger")
    parser.add_argument("--symbols", help="Comma-separated symbols for a focused local scan")
    parser.add_argument("--symbols-file")
    parser.add_argument("--output-root", help="Experiment output root (defaults to configured isolated path)")
    parser.add_argument("--baseline-manifest", help="Frozen configured baseline manifest path")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = root / config_path
    config = AppConfig.load(config_path)

    if args.preflight:
        if args.date and args.database:
            print("PHASE3D_FORWARD_BLOCKED\nReasons:\n- --date replay cannot use --database.", file=sys.stderr)
            return 2
        index_data_dir = Path(args.index_data_dir or config.forward_paper.index_data_dir)
        provider_name = args.provider or config.forward_paper.provider
        licensed_dataset = Path(args.licensed_dataset or config.forward_paper.licensed_dataset_path)
        market_data_dir = Path(args.market_data_dir or config.forward_paper.market_data_dir)
        daily_data_dir = Path(args.daily_data_dir or config.forward_paper.daily_data_dir)
        identity_file = Path(args.identity_file or config.forward_paper.identity_file)
        universe_path = Path(args.symbols_file or config.universe.symbols_file)
        calendar_path = Path(config.forward_paper.trading_calendar_path)
        for name, path in (("index data directory", index_data_dir), ("licensed dataset", licensed_dataset),
                           ("market data directory", market_data_dir), ("daily data directory", daily_data_dir),
                           ("identity file", identity_file),
                           ("universe file", universe_path), ("trading calendar", calendar_path)):
            if not path.is_absolute():
                if name == "index data directory":
                    index_data_dir = root / path
                elif name == "licensed dataset":
                    licensed_dataset = root / path
                elif name == "market data directory":
                    market_data_dir = root / path
                elif name == "daily data directory":
                    daily_data_dir = root / path
                elif name == "identity file":
                    identity_file = root / path
                elif name == "universe file":
                    universe_path = root / path
                else:
                    calendar_path = root / path
        config.universe.symbols_file = str(universe_path)
        try:
            calendar = NseTradingCalendar(calendar_path)
            if args.date:
                market_date = date.fromisoformat(args.date)
                database = root / "data" / "paper_trading" / "replay" / "forward_paper.sqlite"
                output_root = root / "results" / "phase3d_forward" / "replay"
            else:
                market_date = date.fromisoformat(args.market_date) if args.market_date else calendar.latest_completed_candidate(
                    now=datetime.now(ZoneInfo(config.project.timezone)), timezone_name=config.project.timezone
                )
                database = Path(args.database or config.forward_paper.database)
                output_root = Path(args.output_root or config.forward_paper.output_root)
                if not output_root.is_absolute():
                    output_root = root / output_root
                if not database.is_absolute():
                    database = root / database
            if args.symbols:
                symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]
            else:
                symbols = load_local_universe(universe_path, config.universe.max_symbols)
            baseline = Path(args.baseline_manifest or config.forward_paper.baseline_manifest_path)
            baseline = baseline if baseline.is_absolute() else root / baseline
            dataset_path = market_data_dir if provider_name == "personal_local_eod" else licensed_dataset
            result = run_preflight(
                config=config,
                root=root,
                market_date=market_date,
                symbols=symbols,
                dataset_path=dataset_path,
                index_data_dir=index_data_dir,
                database=database,
                baseline_manifest=baseline,
                paper_mode_enabled=os.getenv("PAPER_TRADING_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"},
                live_mode_disabled=os.getenv("LIVE_TRADING_ENABLED", "").strip().lower() in {"0", "false", "no", "off"},
                provider_name=args.provider or config.forward_paper.provider,
                output_root=output_root,
                daily_data_dir=daily_data_dir,
                identity_file=identity_file,
            )
        except Exception as error:
            print(f"PHASE3D_FORWARD_BLOCKED\nReasons:\n- preflight could not complete: {type(error).__name__}: {error}")
            return 2
        print(result.format())
        return 0 if result.status == "PHASE3D_READY" else 2

    live_env = os.getenv("LIVE_TRADING_ENABLED", "").strip().lower()
    if config.live_trading.enabled or live_env not in {"0", "false", "no", "off"}:
        print("LIVE_TRADING_ENABLED must be false. No live execution is implemented.", file=sys.stderr)
        return 2
    if not _enabled(os.getenv("PAPER_TRADING_ENABLED"), config.forward_paper.enabled):
        print("Set PAPER_TRADING_ENABLED=true to run the forward-paper workflow.", file=sys.stderr)
        return 2
    print("MODE: PAPER TRADING")
    print("LIVE TRADING: DISABLED")
    print("BROKER ORDERS: DISABLED")
    config.forward_paper.enabled = True
    provider_name = args.provider or config.forward_paper.provider
    if provider_name not in {"licensed_nse_cm_eod", "local_eod_csv", "personal_local_eod"}:
        print(f"Unsupported forward provider {provider_name!r}; no fallback provider is selected.", file=sys.stderr)
        return 2
    if provider_name != config.forward_paper.provider:
        print("Provider override differs from the frozen Phase 3D configuration; create and review a new baseline before switching.", file=sys.stderr)
        return 2
    capital = args.capital
    if capital is None and os.getenv("PAPER_STARTING_CAPITAL"):
        capital = float(os.environ["PAPER_STARTING_CAPITAL"])
    if capital is None:
        capital = config.forward_paper.starting_capital
    if capital is None or capital <= 0:
        capital = config.backtest.capital

    market_data_dir = Path(args.market_data_dir or config.forward_paper.market_data_dir)
    daily_data_dir = Path(args.daily_data_dir or config.forward_paper.daily_data_dir)
    index_data_dir = Path(args.index_data_dir or config.forward_paper.index_data_dir)
    licensed_dataset = Path(args.licensed_dataset or config.forward_paper.licensed_dataset_path)
    if args.date and args.database:
        print("--date replay cannot use --database; the replay ledger must remain isolated.", file=sys.stderr)
        return 2
    database = Path(args.database or config.forward_paper.database)
    universe_path = Path(args.symbols_file or config.universe.symbols_file)
    baseline_path = Path(args.baseline_manifest or config.forward_paper.baseline_manifest_path)
    market_data_dir = market_data_dir if market_data_dir.is_absolute() else root / market_data_dir
    daily_data_dir = daily_data_dir if daily_data_dir.is_absolute() else root / daily_data_dir
    index_data_dir = index_data_dir if index_data_dir.is_absolute() else root / index_data_dir
    licensed_dataset = licensed_dataset if licensed_dataset.is_absolute() else root / licensed_dataset
    database = database if database.is_absolute() else root / database
    universe_path = universe_path if universe_path.is_absolute() else root / universe_path
    baseline_path = baseline_path if baseline_path.is_absolute() else root / baseline_path
    config.universe.symbols_file = str(universe_path)
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()] if args.symbols else load_local_universe(universe_path, config.universe.max_symbols)
    timezone_name = config.project.timezone
    calendar_path = Path(config.forward_paper.trading_calendar_path)
    calendar_path = calendar_path if calendar_path.is_absolute() else root / calendar_path
    calendar = NseTradingCalendar(calendar_path)
    if args.date:
        market_date = date.fromisoformat(args.date)
        database = root / "data" / "paper_trading" / "replay" / "forward_paper.sqlite"
        output_root = root / "results" / "phase3d_forward" / "replay"
    else:
        market_date = date.fromisoformat(args.market_date) if args.market_date else calendar.latest_completed_candidate(
            now=datetime.now(ZoneInfo(timezone_name)), timezone_name=timezone_name
        )
        output_root = Path(args.output_root or config.forward_paper.output_root)
        output_root = output_root if output_root.is_absolute() else root / output_root
    if provider_name == "personal_local_eod":
        provider = PersonalPaperEodProvider(market_data_dir, index_data_dir, daily_data_dir)
    else:
        provider = LocalEodCsvProvider(
            market_data_dir,
            index_data_dir,
            licensed_dataset_path=licensed_dataset if provider_name == "licensed_nse_cm_eod" else None,
        )
    try:
        report = run_daily_paper_workflow(
            config=config,
            provider=provider,
            market_date=market_date,
            symbols=symbols,
            database=database,
            starting_capital=capital,
            output_root=output_root,
            baseline_manifest=baseline_path,
            trading_calendar=calendar_path,
        )
    except Exception as error:
        print(f"Forward paper workflow failed closed: {error}", file=sys.stderr)
        return 2
    print(f"FORWARD PAPER {market_date.isoformat()} — {report.get('session_status', 'TRADING_DAY')} / {report['data']['system_data_status']}")
    print(f"Signals: {report['signals']['generated']} generated, {report['signals']['accepted']} accepted, {report['signals']['rejected']} rejected")
    print(f"Orders: {report['orders']['created']} created, {report['orders']['filled']} filled, {report['orders']['exited']} exited")
    print(f"Equity: {report['portfolio']['ending_equity']:.2f}; daily P&L: {report['portfolio']['daily_pnl']:.2f}")
    print(f"Report: {report['report_path']}")
    print("LIVE TRADING: DISABLED")
    if report.get("session_status") == "NON_TRADING_DAY":
        return 0
    return 0 if report["data"]["system_data_status"] == "FRESH" else 2


if __name__ == "__main__":
    raise SystemExit(main())
