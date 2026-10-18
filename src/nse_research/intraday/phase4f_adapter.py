"""Thin Phase 4B/4D adapters over a Phase 4E-accepted canonical dataset."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator, Sequence

from nse_research.data.intraday_ingestion import (
    PROJECT_ROOT,
    Phase4FError,
    assert_research_ready,
    get_registered_dataset,
    sha256_file,
)
from nse_research.intraday.model import (
    CsvIntradayDataProvider,
    IntradayBar,
    IntradayDataProvider,
)
from nse_research.intraday.scanner import (
    IntradayScanner,
    IntradayScannerConfig,
)


class CanonicalIntradayDatasetProvider:
    """Research-ready dataset provider; it never reads raw or fixture data."""

    provider_name = "phase4f_canonical_dataset"

    def __init__(self, dataset_id: str, *, project_root: str | Path = PROJECT_ROOT) -> None:
        root = Path(project_root).resolve()
        ready = assert_research_ready(dataset_id, project_root=root)
        self.dataset_id = dataset_id
        self.root = root
        self.path = Path(ready["normalized_path"])
        manifest_path = self.path.parent / "manifest.json"
        self.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self._csv = CsvIntradayDataProvider(self.path)
        self._symbols = set(str(symbol) for symbol in self.manifest.get("symbols", []))
        self._symbol_coverage = {
            str(item["symbol"]): item
            for item in self.manifest.get("coverage", {}).get("coverage_by_symbol", [])
        }

    def available_symbols(self) -> list[str]:
        return sorted(str(symbol) for symbol in self.manifest.get("symbols", []))

    def has_symbol(self, symbol: str) -> bool:
        return str(symbol).strip().upper() in self._symbols

    def available_dates(self) -> list[date]:
        return sorted(
            date.fromisoformat(str(item["date"]))
            for item in self.manifest.get("coverage", {}).get("coverage_by_session", [])
            if int(item.get("bars_present", 0)) > 0
        )

    def get_bars(
        self,
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        timeframe: str | None = None,
    ) -> list[IntradayBar]:
        frames = (timeframe,) if timeframe else tuple(self.manifest.get("timeframe", "1m").split(","))
        return list(self.iter_bars((symbol,), frames, start=start, end=end))

    def iter_bars(
        self,
        symbols: Sequence[str],
        timeframes: Sequence[str] = ("1m", "5m"),
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Iterator[IntradayBar]:
        if not self.path.is_file() or self.manifest.get("status") != "COMPLETE":
            raise Phase4FError("NORMALIZED_DATASET_INCOMPLETE", self.dataset_id)
        if sha256_file(self.path) != self.manifest.get("normalized_hash"):
            raise Phase4FError("NORMALIZED_DATASET_INCOMPLETE", "canonical CSV hash differs from completed manifest")
        yield from self._csv.iter_bars(symbols, timeframes, start=start, end=end)

    def coverage(self, symbol: str | None = None, start: date | None = None,
                 end: date | None = None) -> dict[str, Any]:
        if symbol is None:
            coverage = self.manifest.get("coverage", {})
            return dict(coverage)
        item = self._symbol_coverage.get(str(symbol).strip().upper())
        if item is None:
            return {"symbol": str(symbol).strip().upper(), "available": False,
                    "missing_sessions": None, "rows": 0}
        result = dict(item)
        if start is not None and result.get("first_date") and result["first_date"] > start.isoformat():
            result["range_starts_late"] = True
        if end is not None and result.get("last_date") and result["last_date"] < end.isoformat():
            result["range_ends_early"] = True
        return result

    def metadata(self) -> dict[str, Any]:
        return {"dataset_id": self.dataset_id, **self.manifest}


class Phase4BScannerAdapter:
    """Feed chronological canonical bars into the existing Phase 4B scanner."""

    def __init__(self, provider: IntradayDataProvider, universe: Sequence[str], *,
                 timeframes: Sequence[str] = ("1m", "5m"),
                 scanner: IntradayScanner | None = None) -> None:
        # Do not accept subclasses here: a subclass can override __init__ and
        # iter_bars, bypassing the provider's Phase 4E readiness/hash checks.
        if type(provider) is not CanonicalIntradayDatasetProvider:
            raise Phase4FError(
                "PHASE4F_DATA_NOT_RESEARCH_READY",
                "Phase 4B integration requires the Phase 4F canonical provider after readiness validation",
            )
        ready = assert_research_ready(provider.dataset_id, project_root=provider.root)
        if Path(ready["normalized_path"]).resolve() != provider.path.resolve():
            raise Phase4FError(
                "PHASE4F_DATA_NOT_RESEARCH_READY",
                "canonical provider path does not match the currently accepted dataset",
            )
        symbols = tuple(str(item).strip().upper() for item in universe)
        if len(symbols) not in {100, 200, 486} or len(set(symbols)) != len(symbols):
            raise ValueError("Phase 4B adapter requires the existing 100/200/486-symbol frozen universe")
        self.provider = provider
        self.universe = symbols
        self.timeframes = tuple(timeframes)
        self.scanner = scanner or IntradayScanner(
            symbols,
            config=IntradayScannerConfig(enabled=True, universe_size=len(symbols),
                                         timeframes=self.timeframes),
        )

    def run(self, *, start: datetime | None = None, end: datetime | None = None) -> dict[str, Any]:
        bars = 0
        for bar in self.provider.iter_bars(self.universe, self.timeframes, start=start, end=end):
            self.scanner.handle_bar(bar)
            bars += 1
        health = self.scanner.health()
        return {"bars_processed": bars, "signals": self.scanner.signals,
                "health": health, "order_generation": "NOT_AVAILABLE_IN_PHASE4B"}


def _temporary_ledger(path: str | Path) -> Path:
    target = Path(path).expanduser().resolve()
    allowed = {Path(tempfile.gettempdir()).resolve(), Path("/tmp").resolve(), Path("/private/tmp").resolve()}
    if not any(target == base or base in target.parents for base in allowed):
        raise Phase4FError("REPLAY_LEDGER_MUST_BE_TEMPORARY", "Phase 4F replay only accepts a ledger under the system temporary directory")
    return target


class Phase4DReplayAdapter:
    """Invoke the existing Phase 4D paper replay with a temporary ledger only."""

    def __init__(self, dataset_id: str, *, ledger_path: str | Path,
                 project_root: str | Path = PROJECT_ROOT, universe_size: int = 100,
                 minimum_coverage: float | None = None) -> None:
        self.root = Path(project_root).resolve()
        ledger_target = Path(ledger_path).expanduser()
        if not ledger_target.is_absolute():
            ledger_target = self.root / ledger_target
        ledger_target = ledger_target.resolve()
        persistent_ledger = (self.root / "data/paper_trading/phase4c/ledger.json").resolve()
        if ledger_target == persistent_ledger:
            raise Phase4FError(
                "PHASE4D_PERSISTENT_LEDGER_REJECTED_FOR_REPLAY",
                "historical replay requires a temporary/custom ledger",
            )
        self.ledger_path = _temporary_ledger(ledger_target)
        get_registered_dataset(dataset_id, project_root=self.root)
        self.ready = assert_research_ready(dataset_id, project_root=self.root)
        self.provider = CanonicalIntradayDatasetProvider(dataset_id, project_root=self.root)
        self.dataset_id = dataset_id
        self.universe_size = universe_size
        self.minimum_coverage = minimum_coverage

    def run(self, trading_date: date) -> dict[str, Any]:
        if trading_date not in self.provider.available_dates():
            raise Phase4FError(
                "PHASE4F_SESSION_NOT_AVAILABLE",
                f"{trading_date.isoformat()} is not present in the registered dataset's observed session coverage",
            )
        # Import only the paper-only orchestrator. No broker provider or live
        # order path is reachable through this adapter.
        from nse_research.execution.phase4d import Phase4DOrchestrator

        orchestrator = Phase4DOrchestrator(
            project_root=self.root,
            data_path=self.provider.path,
            ledger_path=self.ledger_path,
            universe_size=self.universe_size,
            min_coverage=self.minimum_coverage,
        )
        result = orchestrator.run_replay(trading_date=trading_date)
        result["phase4f_dataset_id"] = self.dataset_id
        result["ledger_path"] = str(self.ledger_path)
        return result
