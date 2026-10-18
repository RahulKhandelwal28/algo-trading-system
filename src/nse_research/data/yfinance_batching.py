"""Bounded, auditable yfinance batch acquisition for Phase 4G-YF.

The installed yfinance release uses a process-wide ``YfData`` singleton for
its session/cookie state. Production batches therefore run in a bounded
process pool, which gives each worker its own singleton state. Each worker
still calls ``yf.download(..., threads=False)`` so it never fans out one
thread per symbol. Workers return frames and metadata only; they never touch a
paper ledger or decision engine.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from multiprocessing import get_context
from typing import Any, Callable, Mapping, Sequence

import pandas as pd

from nse_research.data.yfinance_intraday import (
    IST,
    YahooInstrument,
    YFinanceDataError,
    download_yfinance_history,
)


MAX_BATCH_RETRIES = 2
DEFAULT_BATCH_SIZE = 50
DEFAULT_MAX_CONCURRENT_BATCHES = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 0.25


@dataclass(frozen=True, slots=True)
class BatchDefinition:
    batch_id: int
    symbols: tuple[str, ...]
    yahoo_symbols: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BatchAttempt:
    attempt_number: int
    request_started_at: str
    request_finished_at: str
    response_received_at: str | None
    duration_seconds: float
    status: str
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class BatchFetchResult:
    batch_id: int
    symbols: tuple[str, ...]
    yahoo_symbols: tuple[str, ...]
    status: str
    frame: pd.DataFrame | None
    attempts: tuple[BatchAttempt, ...]
    request_started_at: str
    request_finished_at: str
    response_received_at: str | None
    error_type: str | None = None
    error_message: str | None = None
    provider_warnings: tuple[str, ...] = ()

    @property
    def retry_count(self) -> int:
        return max(0, len(self.attempts) - 1)

    @property
    def duration_seconds(self) -> float:
        return sum(item.duration_seconds for item in self.attempts)

    def to_manifest(self, *, bars_returned: int | None = None) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "batch_symbol_count": len(self.symbols),
            "symbols_requested": list(self.symbols),
            "yahoo_symbols_requested": list(self.yahoo_symbols),
            "request_started_at": self.request_started_at,
            "request_finished_at": self.request_finished_at,
            "response_received_at": self.response_received_at,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "attempts": len(self.attempts),
            "retry_count": self.retry_count,
            "bars_returned": bars_returned,
            "symbols_returned": None,
            "symbols_returned_count": None,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "provider_warnings": list(self.provider_warnings),
            "attempt_records": [
                {
                    "attempt_number": item.attempt_number,
                    "request_started_at": item.request_started_at,
                    "request_finished_at": item.request_finished_at,
                    "response_received_at": item.response_received_at,
                    "duration_seconds": item.duration_seconds,
                    "status": item.status,
                    "error_type": item.error_type,
                    "error_message": item.error_message,
                }
                for item in self.attempts
            ],
        }


def validate_batch_configuration(batch_size: int, max_concurrent_batches: int,
                                 max_retries: int = MAX_BATCH_RETRIES) -> None:
    if isinstance(batch_size, bool) or int(batch_size) <= 0:
        raise YFinanceDataError("BATCH_SIZE_INVALID", "batch size must be a positive integer")
    if isinstance(max_concurrent_batches, bool) or int(max_concurrent_batches) <= 0:
        raise YFinanceDataError("BATCH_CONCURRENCY_INVALID", "maximum concurrent batches must be a positive integer")
    if isinstance(max_retries, bool) or not 0 <= int(max_retries) <= MAX_BATCH_RETRIES:
        raise YFinanceDataError("BATCH_RETRY_LIMIT_INVALID", f"maximum retries must be between 0 and {MAX_BATCH_RETRIES}")


def partition_frozen_universe(instruments: Mapping[str, YahooInstrument], *, batch_size: int = DEFAULT_BATCH_SIZE
                              ) -> list[BatchDefinition]:
    """Slice the existing insertion-ordered frozen mapping without re-sorting."""
    validate_batch_configuration(batch_size, 1, 0)
    items = list(instruments.items())
    symbols = [str(symbol).strip().upper() for symbol, _ in items]
    yahoo = [instrument.yahoo_symbol.strip().upper() for _, instrument in items]
    if not items or len(symbols) != len(set(symbols)) or len(yahoo) != len(set(yahoo)):
        raise YFinanceDataError("FROZEN_UNIVERSE_INVALID", "expected a non-empty unique canonical and Yahoo symbol mapping")
    if any(instrument.symbol.strip().upper() != symbol for (symbol, instrument) in items):
        raise YFinanceDataError("FROZEN_UNIVERSE_ORDER_INVALID", "mapping keys must match canonical instrument symbols")
    return [
        BatchDefinition(
            batch_id=(offset // int(batch_size)) + 1,
            symbols=tuple(symbols[offset:offset + int(batch_size)]),
            yahoo_symbols=tuple(yahoo[offset:offset + int(batch_size)]),
        )
        for offset in range(0, len(items), int(batch_size))
    ]


def _aware_ist(now_fn: Callable[..., datetime]) -> datetime:
    current = now_fn(IST)
    if current.tzinfo is None or current.utcoffset() is None:
        raise YFinanceDataError("UNKNOWN_TIMEZONE", "batch timestamps must be timezone aware")
    return current.astimezone(IST)


def fetch_yfinance_batch(
    batch: BatchDefinition,
    *,
    start: date | datetime | str,
    end: date | datetime | str,
    interval: str = "1m",
    timeout_seconds: float = 15.0,
    max_retries: int = MAX_BATCH_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    downloader: Callable[..., Any] | None = None,
    now_fn: Callable[..., datetime] = datetime.now,
    sleep_fn: Callable[[float], None] = time.sleep,
    monotonic_fn: Callable[[], float] = time.monotonic,
) -> BatchFetchResult:
    """Fetch a batch with at most two bounded retries and response-level times."""
    validate_batch_configuration(1, 1, max_retries)
    if not batch.symbols or len(batch.symbols) != len(batch.yahoo_symbols):
        raise YFinanceDataError("BATCH_DEFINITION_INVALID", f"batch {batch.batch_id}")
    if retry_backoff_seconds < 0:
        raise YFinanceDataError("BATCH_BACKOFF_INVALID", "retry backoff cannot be negative")
    attempts: list[BatchAttempt] = []
    successful_frame: pd.DataFrame | None = None
    successful_receipt: str | None = None
    last_error: Exception | None = None
    provider_warnings: list[str] = []
    provider_logger = logging.getLogger("yfinance")

    class _WarningCollector(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                message = record.getMessage()
            except Exception:
                message = str(record.msg)
            if record.levelno >= logging.WARNING and message not in provider_warnings:
                provider_warnings.append(message)

    warning_handler = _WarningCollector()
    provider_logger.addHandler(warning_handler)
    for attempt_number in range(1, max_retries + 2):
        fallback_monotonic = monotonic_fn()
        attempt_clock: dict[str, Any] = {}

        def mark_request_started() -> None:
            attempt_clock["started"] = _aware_ist(now_fn)
            attempt_clock["monotonic_start"] = monotonic_fn()

        def mark_response_received() -> None:
            attempt_clock["received"] = _aware_ist(now_fn)

        try:
            frame = download_yfinance_history(
                batch.yahoo_symbols,
                start=start,
                end=end,
                interval=interval,
                timeout_seconds=timeout_seconds,
                downloader=downloader,
                request_started_callback=mark_request_started,
                response_received_callback=mark_response_received,
            )
            started = attempt_clock.get("started") or _aware_ist(now_fn)
            response_received = attempt_clock.get("received")
            if response_received is None:
                response_received = _aware_ist(now_fn)
            monotonic_start = attempt_clock.get("monotonic_start", fallback_monotonic)
            successful_frame = frame.copy(deep=True)
            successful_receipt = response_received.isoformat()
            finished = response_received
            attempts.append(BatchAttempt(
                attempt_number=attempt_number,
                request_started_at=started.isoformat(),
                request_finished_at=finished.isoformat(),
                response_received_at=successful_receipt,
                duration_seconds=max(0.0, monotonic_fn() - monotonic_start),
                status="SUCCESS",
            ))
            break
        except Exception as exc:  # batch-level data/provider failures remain explicit
            last_error = exc
            started = attempt_clock.get("started") or _aware_ist(now_fn)
            response_received = attempt_clock.get("received")
            monotonic_start = attempt_clock.get("monotonic_start", fallback_monotonic)
            finished = response_received or _aware_ist(now_fn)
            attempts.append(BatchAttempt(
                attempt_number=attempt_number,
                request_started_at=started.isoformat(),
                request_finished_at=finished.isoformat(),
                response_received_at=response_received.isoformat() if response_received else None,
                duration_seconds=max(0.0, monotonic_fn() - monotonic_start),
                status="FAILED",
                error_type=type(exc).__name__,
                error_message=str(exc),
            ))
            if attempt_number <= max_retries:
                sleep_fn(retry_backoff_seconds * (2 ** (attempt_number - 1)))
    first = attempts[0]
    final = attempts[-1]
    success = successful_frame is not None
    provider_logger.removeHandler(warning_handler)
    return BatchFetchResult(
        batch_id=batch.batch_id,
        symbols=batch.symbols,
        yahoo_symbols=batch.yahoo_symbols,
        status="SUCCESS" if success else "FAILED",
        frame=successful_frame,
        attempts=tuple(attempts),
        request_started_at=first.request_started_at,
        request_finished_at=final.request_finished_at,
        response_received_at=successful_receipt,
        error_type=None if success else (type(last_error).__name__ if last_error else "UnknownError"),
        error_message=None if success else (str(last_error) if last_error else "batch failed without a recorded exception"),
        provider_warnings=tuple(provider_warnings),
    )


def _fetch_batch_job(batch: BatchDefinition, *, start: date | datetime | str, end: date | datetime | str,
                     interval: str, timeout_seconds: float, max_retries: int,
                     retry_backoff_seconds: float) -> BatchFetchResult:
    return fetch_yfinance_batch(
        batch,
        start=start,
        end=end,
        interval=interval,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
    )


def acquire_batches(
    batches: Sequence[BatchDefinition],
    *,
    start: date | datetime | str,
    end: date | datetime | str,
    interval: str = "1m",
    timeout_seconds: float = 15.0,
    max_concurrent_batches: int = DEFAULT_MAX_CONCURRENT_BATCHES,
    max_retries: int = MAX_BATCH_RETRIES,
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
    worker: Callable[..., BatchFetchResult] = _fetch_batch_job,
    executor_type: Any = ProcessPoolExecutor,
) -> list[BatchFetchResult]:
    """Acquire independent batches with a hard executor worker ceiling.

    Results are returned in recorded receipt-time order, then batch-id order;
    they are never used to process strategy, execution, or ledger state here.
    """
    validate_batch_configuration(1, max_concurrent_batches, max_retries)
    if not batches:
        return []
    results: list[BatchFetchResult] = []
    executor_kwargs: dict[str, Any] = {"max_workers": max_concurrent_batches}
    if executor_type is ProcessPoolExecutor:
        # Explicit spawn ensures each worker starts with an independent
        # yfinance YfData singleton/session even on platforms whose default
        # multiprocessing context is fork.
        executor_kwargs["mp_context"] = get_context("spawn")
    with executor_type(**executor_kwargs) as executor:
        futures = {
            executor.submit(
                worker,
                batch,
                start=start,
                end=end,
                interval=interval,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                retry_backoff_seconds=retry_backoff_seconds,
            ): batch
            for batch in batches
        }
        for future in as_completed(futures):
            batch = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                failed_at = datetime.now(IST).isoformat()
                result = BatchFetchResult(
                    batch_id=batch.batch_id,
                    symbols=batch.symbols,
                    yahoo_symbols=batch.yahoo_symbols,
                    status="FAILED",
                    frame=None,
                    attempts=(BatchAttempt(
                        attempt_number=1,
                        request_started_at=failed_at,
                        request_finished_at=failed_at,
                        response_received_at=None,
                        duration_seconds=0.0,
                        status="FAILED",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    ),),
                    request_started_at=failed_at,
                    request_finished_at=failed_at,
                    response_received_at=None,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if result.batch_id != batch.batch_id or result.symbols != batch.symbols:
                raise YFinanceDataError("BATCH_RESULT_IDENTITY_MISMATCH", f"expected batch {batch.batch_id}")
            results.append(result)
    return sorted(results, key=lambda item: (item.response_received_at or item.request_finished_at, item.batch_id))
