#!/usr/bin/env python3
"""Run the deterministic Phase 4C software fixture; it is not market data."""

from __future__ import annotations

import argparse
import sys
import tempfile
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nse_research.execution.phase4c import IntradayPaperReplay, PaperExecutionEngine  # noqa: E402
from nse_research.execution.phase4c_models import ExecutionMode, OrderStatus, Phase4CPaperConfig  # noqa: E402
from nse_research.intraday.model import IntradayBar  # noqa: E402
from nse_research.intraday.scanner import IntradayScanner, IntradayScannerConfig  # noqa: E402


IST = ZoneInfo("Asia/Kolkata")
TEST_DATE = date(2026, 9, 28)
BAR_SOURCE = "SYNTHETIC_TEST_DATA"
FIXTURE_LABEL = "SYNTHETIC_EXECUTION_TEST"


def _bar(offset: int, *, open_price: float, high: float, low: float, close: float) -> IntradayBar:
    start = datetime.combine(TEST_DATE, time(9, 15), IST) + timedelta(minutes=offset)
    return IntradayBar(
        symbol="RELIANCE", exchange="NSE", series="EQ", timeframe="1m", timestamp=start,
        open=open_price, high=high, low=low, close=close, volume=100.0,
        status="CLOSED", source=BAR_SOURCE, volume_kind="BAR",
    )


class _FixtureProvider:
    provider_name = FIXTURE_LABEL

    def __init__(self, bars: list[IntradayBar]) -> None:
        self.bars = bars

    def iter_bars(self, symbols, timeframes, *, start=None, end=None):
        yield from self.bars


def run_fixture(ledger_path: Path) -> dict:
    config = Phase4CPaperConfig.from_env(explicit_paper_command=True)
    scanner_symbols = ["RELIANCE"] + [f"SYM{index:03}" for index in range(99)]
    scanner = IntradayScanner(
        scanner_symbols,
        config=IntradayScannerConfig(enabled=True, universe_size=100, timeframes=("1m",)),
    )
    fixture_bars = []
    for offset in range(60):
        if offset == 20:
            # At the next interval open the queued market order fills; this
            # completed bar then touches both barriers, invoking stop-first.
            fixture_bars.append(_bar(offset, open_price=109.59, high=113.0, low=107.0, close=109.59))
        else:
            price = 100.0 + offset * 0.5
            fixture_bars.append(_bar(offset, open_price=price, high=price + 1, low=price - 1, close=price))
    engine = PaperExecutionEngine(config, ledger_path=ledger_path)
    result = IntradayPaperReplay(scanner, engine).run(
        _FixtureProvider(fixture_bars), speed="max",
        sectors={"RELIANCE": "SYNTHETIC_TEST_SECTOR"},
    )
    entry_orders = [order for order in engine.all_orders if order.side == "BUY"]
    entry_fills = [order for order in entry_orders if order.status == OrderStatus.FILLED.value]
    result.update({
        "signals": len(scanner.signals),
        "scanner_signals": tuple(scanner.signals),
        "entry_orders": len(entry_orders),
        "entry_fills": len(entry_fills),
        "exit_orders": sum(order.side == "SELL" for order in engine.all_orders),
        "risk_events": len(engine.risk_events),
        "live_orders": 0,
        "broker_calls": 0,
        "engine": engine,
    })
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger-dir", type=Path,
        help="persist the synthetic test ledger in this directory (default: temporary directory)",
    )
    args = parser.parse_args()
    print("PHASE4C PAPER EXECUTION TEST")
    temp_context = None
    try:
        if args.ledger_dir is None:
            temp_context = tempfile.TemporaryDirectory(prefix="phase4c-synthetic-")
            ledger_dir = Path(temp_context.name)
        else:
            ledger_dir = args.ledger_dir
            ledger_dir.mkdir(parents=True, exist_ok=True)
        outcome = run_fixture(ledger_dir / "ledger.json")
        paper = outcome["engine"].config
        print(f"MODE={paper.execution_mode.value}")
        print(f"LIVE_TRADING={str(paper.live_trading_enabled).lower()}")
        print(f"REALTIME_ENABLED={str(paper.realtime_enabled).lower()}")
        print(f"fixture={FIXTURE_LABEL}")
        print(f"synthetic signals processed: {outcome['signals_processed']}")
        print(f"paper orders: {outcome['orders']}")
        print(f"paper fills: {outcome['fills']}")
        print(f"closed trades: {outcome['trades']}")
        print(f"risk events: {outcome['risk_events']}")
        print(f"LIVE ORDERS: {outcome['live_orders']}")
        print(f"BROKER CALLS: {outcome['broker_calls']}")
        engine = outcome["engine"]
        if (paper.execution_mode is not ExecutionMode.PAPER or paper.live_trading_enabled
                or paper.realtime_enabled or outcome["signals_processed"] != 2
                or {signal.strategy for signal in outcome["scanner_signals"]} != {"vwap_trend", "momentum"}
                or outcome["entry_orders"] != 2 or outcome["entry_fills"] != 1
                or outcome["trades"] != 1 or outcome["live_orders"] != 0 or outcome["broker_calls"] != 0
                or any(order.source != BAR_SOURCE or order.source_metadata.get("replay_provider") != FIXTURE_LABEL
                       for order in engine.all_orders)):
            raise RuntimeError("Phase 4C synthetic safety or expected-flow assertion failed")
        print("SYNTHETIC TEST: PASS (software validation only; no market-performance statistics)")
        return 0
    except Exception as exc:
        print(f"SYNTHETIC TEST: FAIL ({exc})", file=sys.stderr)
        return 1
    finally:
        if temp_context is not None:
            temp_context.cleanup()


if __name__ == "__main__":
    raise SystemExit(main())
