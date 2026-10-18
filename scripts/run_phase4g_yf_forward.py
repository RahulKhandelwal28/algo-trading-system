#!/usr/bin/env python3
"""Prepare or run the Phase 4G-YF third-party intraday paper experiment.

No network request occurs in ``preflight``.  ``collect`` is hard-bound to the
explicitly selected, calendar-validated live session, stores only observed
Yahoo/yfinance data, and cannot start outside that session. The default date
preserves the original 2026-09-28 run. ``reconcile`` is data-only and never
invokes the Phase 4B scanner or Phase 4C execution engine.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time as time_module
import statistics
from dataclasses import asdict
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from nse_research.data.yfinance_intraday import (
    IST, NormalizedYahooBars, infer_interval_volume_semantics, load_yahoo_instruments,
    normalize_yfinance_frame,
)
from nse_research.data.yfinance_batching import (
    DEFAULT_BATCH_SIZE, DEFAULT_MAX_CONCURRENT_BATCHES, MAX_BATCH_RETRIES,
    BatchDefinition, BatchFetchResult, acquire_batches, partition_frozen_universe,
    validate_batch_configuration,
)
from nse_research.data.intraday_validator import validate_intraday_dataset
from nse_research.intraday.phase4g_yf import (
    SESSION_CLOSE, SESSION_DATE as DEFAULT_SESSION_DATE, SESSION_OPEN,
    Phase4GYFError, Phase4GYFForwardProcessor, _atomic_json, _atomic_csv,
    build_strategy_signal_summary, build_top_candidate_summary, phase4g_preflight,
    reconciliation_report, register_phase4g_dataset,
)
from nse_research.intraday.yahoo_revision_reconciler import BAR_SETTLING_SECONDS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = PROJECT_ROOT / "results/phase4g_yf_20260928_batched"
MONITOR_OUTPUT_ROOT = PROJECT_ROOT / "results/phase4g_yf_20260928_monitor"
POLL_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 15
_ACTIVE_PROCESSOR: Phase4GYFForwardProcessor | None = None


def refresh_sleep_seconds(cycle_elapsed_seconds: float, refresh_seconds: int) -> float:
    """Keep cycle start cadence at least the requested interval, without overlap."""
    if refresh_seconds < 30:
        raise ValueError("refresh_seconds must be at least 30")
    return max(0.0, float(refresh_seconds) - max(0.0, float(cycle_elapsed_seconds)))


def _read_json(path: Path, default: Mapping[str, Any] | None = None) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return dict(default or {})
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4GYFError("MANIFEST_INVALID", f"{path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Phase4GYFError("MANIFEST_INVALID", str(path))
    return value


def _last_refresh_audit(output: Path) -> dict[str, Any] | None:
    path = output / "reports/refresh_audit.jsonl"
    try:
        lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        value = json.loads(lines[-1]) if lines else None
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _batch_plan(batches: Sequence[BatchDefinition]) -> list[dict[str, Any]]:
    return [{"batch_id": item.batch_id, "batch_symbol_count": len(item.symbols),
             "symbols": list(item.symbols), "yahoo_symbols": list(item.yahoo_symbols)}
            for item in batches]


def _write_batch_manifest(output: Path, *, batches: Sequence[BatchDefinition], batch_size: int,
                          max_concurrent_batches: int,
                          executions: Sequence[Mapping[str, Any]] = (),
                          session_date: date | None = None) -> dict[str, Any]:
    path = output / "batch_manifest.json"
    existing = _read_json(path)
    stored_date = existing.get("session_date")
    effective_date = session_date or (date.fromisoformat(stored_date) if stored_date else DEFAULT_SESSION_DATE)
    history = list(existing.get("executions", []))
    history.extend(dict(item) for item in executions)
    value = {
        "schema_version": 1,
        "data_source": "Yahoo Finance/yfinance",
        "data_classification": "THIRD_PARTY_RESEARCH_SOURCE",
        "authorized_nse_feed": False,
        "session_date": effective_date.isoformat(),
        "frozen_universe_size": sum(len(item.symbols) for item in batches),
        "session_universe_size": sum(len(item.symbols) for item in batches),
        "batch_size": batch_size,
        "max_concurrent_batches": max_concurrent_batches,
        "number_of_batches": len(batches),
        "planned_batches": _batch_plan(batches),
        "execution_status": "NOT_STARTED" if not history else "EXECUTIONS_RECORDED",
        "executions": history,
        "paper_only": True,
        "live_trading_enabled": False,
        "broker": "NONE",
        "broker_calls": 0,
        "live_orders": 0,
    }
    _atomic_json(path, value)
    return value


def _batch_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    receipt_latencies: list[float] = []
    request_count = retry_count = successful = failed = 0
    empty_no_trade_cells_ignored = 0
    for record in records:
        attempts = int(record.get("attempts", 0) or 0)
        request_count += attempts
        retry_count += int(record.get("retry_count", max(0, attempts - 1)) or 0)
        empty_no_trade_cells_ignored += int(record.get("empty_no_trade_cells_ignored", 0) or 0)
        if record.get("status") == "SUCCESS":
            successful += 1
        elif str(record.get("status", "")).startswith("FAILED"):
            failed += 1
        started, received = record.get("request_started_at"), record.get("response_received_at")
        if started and received:
            try:
                receipt_latencies.append((datetime.fromisoformat(received) - datetime.fromisoformat(started)).total_seconds())
            except (TypeError, ValueError):
                pass
    return {
        "total_requests": request_count,
        "successful_batches": successful,
        "failed_batches": failed,
        "retry_count": retry_count,
        "empty_no_trade_cells_ignored": empty_no_trade_cells_ignored,
        "min_receipt_latency_seconds": min(receipt_latencies) if receipt_latencies else None,
        "max_receipt_latency_seconds": max(receipt_latencies) if receipt_latencies else None,
        "avg_receipt_latency_seconds": statistics.fmean(receipt_latencies) if receipt_latencies else None,
        "batch_execution_summary": {
            "batch_executions": len(records), "successful_batches": successful,
            "failed_batches": failed, "retry_count": retry_count,
        },
    }


def _normalize_batch_results(results: Sequence[BatchFetchResult], *, instruments: Mapping[str, Any],
                             output: Path, start: datetime, end: datetime,
                             volume_kind: str = "UNKNOWN",
                             session_date: date = DEFAULT_SESSION_DATE) -> tuple[NormalizedYahooBars, list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    symbols_with_data: set[str] = set()
    failed_symbols: set[str] = set()
    invalid_symbols: set[str] = set()
    duplicate_symbols: set[str] = set()
    issues: list[dict[str, Any]] = []
    source_timezones: set[str] = set()
    firsts: list[str] = []
    lasts: list[str] = []
    forming_rows = duplicate_timestamps = 0
    records: list[dict[str, Any]] = []
    quality_missing: set[str] = set()
    empty_no_trade_cells_ignored = 0
    for result in results:
        if result.status != "SUCCESS" or result.frame is None or result.response_received_at is None:
            failed_symbols.update(result.symbols)
            quality_missing.update(result.symbols)
            records.append({**result.to_manifest(bars_returned=0),
                            "symbols_returned": [], "symbols_returned_count": 0})
            continue
        receipt = datetime.fromisoformat(result.response_received_at).astimezone(IST)
        request_started = datetime.fromisoformat(result.request_started_at).astimezone(IST)
        raw = _write_snapshot(
            result.frame, output=output, observed_at=receipt, interval="1m", start=start, end=end,
            batch_id=result.batch_id,
        )
        try:
            batch_instruments = {symbol: instruments[symbol] for symbol in result.symbols}
            normalized = normalize_yfinance_frame(
                result.frame, batch_instruments, interval="1m", now=receipt,
                request_started_at=request_started, response_received_at=receipt,
                batch_id=result.batch_id, session_date=session_date, volume_kind=volume_kind,
            )
        except Exception as exc:
            quality_missing.update(result.symbols)
            failed_symbols.update(result.symbols)
            records.append({**result.to_manifest(bars_returned=0), "status": "FAILED_DATA_QUALITY",
                            "symbols_returned": [], "symbols_returned_count": 0,
                            "error_type": type(exc).__name__, "error_message": str(exc),
                            "raw_snapshot": raw})
            continue
        rows.extend(normalized.rows)
        symbols_with_data.update(normalized.symbols_with_data)
        failed_symbols.update(normalized.failed_symbols or [])
        invalid_symbols.update(normalized.invalid_symbols or [])
        duplicate_symbols.update(normalized.duplicate_symbols or [])
        issues.extend(normalized.issues or [])
        source_timezones.add(normalized.source_timezone)
        if normalized.raw_index_first:
            firsts.append(normalized.raw_index_first)
        if normalized.raw_index_last:
            lasts.append(normalized.raw_index_last)
        forming_rows += normalized.forming_rows_ignored
        duplicate_timestamps += normalized.raw_duplicate_timestamps
        empty_no_trade_cells_ignored += normalized.empty_no_trade_cells_ignored
        quality_missing.update(normalized.missing_symbols)
        records.append({**result.to_manifest(bars_returned=len(normalized.rows)),
                        "symbols_returned": normalized.symbols_with_data,
                        "symbols_returned_count": len(normalized.symbols_with_data),
                        "source_timezone": normalized.source_timezone,
                        "forming_rows_ignored": normalized.forming_rows_ignored,
                        "empty_no_trade_cells_ignored": normalized.empty_no_trade_cells_ignored,
                        "symbols_with_data": normalized.symbols_with_data,
                        "missing_symbols": normalized.missing_symbols,
                        "invalid_symbols": normalized.invalid_symbols,
                        "duplicate_symbols": normalized.duplicate_symbols,
                        "raw_snapshot": raw})
    expected = list(instruments)
    aggregate = NormalizedYahooBars(
        rows=sorted(rows, key=lambda row: (row["timestamp"], row["symbol"])),
        requested_symbols=expected,
        symbols_with_data=sorted(symbols_with_data),
        missing_symbols=sorted(set(expected) - symbols_with_data),
        forming_rows_ignored=forming_rows,
        source_timezone=",".join(sorted(source_timezones)) if source_timezones else "UNKNOWN",
        interval="1m",
        raw_index_first=min(firsts) if firsts else None,
        raw_index_last=max(lasts) if lasts else None,
        raw_duplicate_timestamps=duplicate_timestamps,
        issues=issues,
        failed_symbols=sorted(failed_symbols),
        invalid_symbols=sorted(invalid_symbols),
        duplicate_symbols=sorted(duplicate_symbols),
        empty_no_trade_cells_ignored=empty_no_trade_cells_ignored,
    )
    return aggregate, records, sorted(quality_missing | invalid_symbols | duplicate_symbols)


def _current_refresh_failures(
    batch_results: Sequence[BatchFetchResult],
    normalized: NormalizedYahooBars,
    *,
    attempt_timestamp: datetime,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Build per-symbol transient availability records; never infer delisting."""
    failures: dict[str, dict[str, Any]] = {}
    issues: list[dict[str, Any]] = []
    normalized_issues: dict[str, list[Mapping[str, Any]]] = {}
    for issue in normalized.issues or []:
        normalized_issues.setdefault(str(issue.get("symbol") or "").upper(), []).append(issue)
    for result in batch_results:
        if result.status != "SUCCESS":
            for symbol in result.symbols:
                failures[symbol] = {
                    "error_type": result.error_type or "BATCH_FETCH_FAILED",
                    "error_message": result.error_message or "Yahoo batch returned no data",
                }
        for warning in result.provider_warnings:
            matching = [canonical for canonical, yahoo_symbol in zip(result.symbols, result.yahoo_symbols)
                        if yahoo_symbol.upper() in warning.upper()]
            issues.append({
                "attempt_timestamp": attempt_timestamp.astimezone(IST).isoformat(),
                "error_type": "YAHOO_PROVIDER_WARNING",
                "error_message": warning,
                "symbols": matching,
                "classification": "DATA_UNAVAILABLE_FOR_THIS_REFRESH" if matching else "PROVIDER_WARNING",
                "delisting_conclusion": "NONE",
            })
            for symbol in matching:
                if symbol in normalized.missing_symbols:
                    failures[symbol] = {"error_type": "YAHOO_PROVIDER_WARNING",
                                        "error_message": warning}
    for symbol in sorted(set(normalized.missing_symbols) | set(normalized.failed_symbols or [])):
        if symbol in failures:
            continue
        details = normalized_issues.get(symbol, [])
        first = details[0] if details else {}
        failures[symbol] = {
            "error_type": first.get("code") or "NO_VALID_BARS_IN_REFRESH",
            "error_message": first.get("detail") or
                             "Yahoo returned no normalized 1m observations in this refresh; delisting was not inferred",
        }
    for symbol, failure in sorted(failures.items()):
        issues.append({
            "symbol": symbol,
            "attempt_timestamp": attempt_timestamp.astimezone(IST).isoformat(),
            "error_type": failure["error_type"],
            "error_message": failure["error_message"],
            "classification": "DATA_UNAVAILABLE_FOR_THIS_REFRESH",
            "universe_membership": "PRESERVED",
            "synthetic_bar_created": False,
            "delisting_conclusion": "NONE",
        })
    return failures, issues


def _format_refresh_diagnostic(result: Mapping[str, Any], universe_size: int, *,
                               include_title: bool = True) -> str:
    """Render refresh accounting without conflating coverage and new bars."""
    lines = [
        *( ["REFRESH DIAGNOSTIC"] if include_title else [] ),
        f"Rows returned                  : {result.get('rows_seen', 0)}",
        f"Unique market bars             : {result.get('unique_market_bars', 0)}",
        f"New market bars                : {result.get('new_market_bars', 0)}",
        f"Duplicate market bars          : {result.get('duplicate_market_bars', 0)}",
        f"Conflicting duplicate bars     : {result.get('duplicate_conflict', 0)}",
        f"Invalid rows                   : {result.get('invalid_rows', 0)}",
        f"Late bars excluded             : {result.get('late_bars', 0)}",
        f"Eligible bars                  : {result.get('eligible_bars', 0)}",
        f"Symbols requested              : {universe_size}",
        f"Symbols with valid session data: {result.get('symbols_with_valid_session_data', 0)}",
        f"Symbols with current data      : {result.get('symbols_with_data', 0)}",
        f"Symbols with new bars          : {result.get('symbols_with_new_bars', 0)}",
        f"Symbols with refresh failure   : {result.get('symbols_with_refresh_failure', 0)}",
        f"Warmup bars                    : {result.get('warmup_bars', 0)}",
        f"Current-session new bars       : {result.get('current_session_new_bars', 0)}",
        "Receipt-time protection        : PASS",
    ]
    failures = [item for item in result.get("per_symbol", []) if item.get("fetch_failure")]
    if failures:
        lines.append("Unavailable this refresh      : " + ", ".join(
            str(item.get("symbol")) for item in failures[:12]
        ))
    if int(result.get("new_signals", 0) or 0) == 0:
        lines.append("PAPER RECOMMENDATION            : NO QUALIFIED BUY SIGNAL")
    return "\n".join(lines)


def phase4g_diagnostic_report(source_dir: Path) -> dict[str, Any]:
    """Read existing Phase 4G artifacts and diagnose legacy bar-count evidence.

    This helper is intentionally read-only and does not start a provider,
    instantiate the paper engine, or mutate a ledger.
    """
    source = source_dir.expanduser().resolve()
    manifest = _read_json(source / "manifest.json")
    state = _read_json(source / "run_state.json")
    latest = dict(manifest.get("latest_refresh_accounting") or {})
    canonical_path = source / "canonical/forward_bars_1m.csv"
    seen: set[tuple[str, str]] = set()
    canonical_rows = 0
    duplicate_keys = 0
    try:
        with canonical_path.open("r", newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                canonical_rows += 1
                key = (str(row.get("symbol") or "").strip().upper(),
                       str(row.get("market_timestamp") or row.get("timestamp") or ""))
                if key in seen:
                    duplicate_keys += 1
                seen.add(key)
    except FileNotFoundError:
        pass
    except (OSError, UnicodeError, csv.Error) as exc:
        return {"status": "DIAGNOSTIC_READ_FAILED", "source_dir": str(source),
                "error_type": type(exc).__name__, "error_message": str(exc),
                "read_only": True}
    audit_rows = 0
    audit_path = source / "reports/refresh_audit.jsonl"
    try:
        with audit_path.open("r", encoding="utf-8") as stream:
            audit_rows = sum(bool(line.strip()) for line in stream)
    except FileNotFoundError:
        pass
    legacy_metric = latest.get("new_bars_processed", manifest.get("new_bars_processed"))
    if audit_rows:
        status = "CANONICAL_REFRESH_AUDIT_AVAILABLE"
    elif canonical_rows:
        status = "LEGACY_COUNT_NOT_PROVABLE_FROM_REFRESH_ARTIFACTS"
    else:
        status = "NO_PRIOR_MONITOR_DATA"
    return {
        "status": status,
        "source_dir": str(source),
        "read_only": True,
        "legacy_reported_new_bars": legacy_metric,
        "legacy_latest_refresh_accounting": latest,
        "canonical_rows": canonical_rows,
        "canonical_unique_market_bars": len(seen),
        "canonical_duplicate_keys": duplicate_keys,
        "refresh_audit_records": audit_rows,
        "warmup_event_ids": len(state.get("warmup_event_ids") or []),
        "processed_event_ids": len(state.get("processed_event_ids") or []),
        "legacy_bar_revision_records": len(state.get("bar_revisions") or []),
        "bar_identity": "(symbol, market_timestamp)",
        "legacy_root_cause_from_source_audit": (
            "The previous new_bars_processed value counted the combined scanner input, "+
            "including derived 5-minute work, rather than first-seen 1-minute market keys."
        ),
    }


def _write_experiment_manifest(output: Path, updates: Mapping[str, Any], *,
                               session_date: date | None = None) -> dict[str, Any]:
    path = output / "manifest.json"
    manifest = _read_json(path, {
        "schema_version": 1, "phase": "4G-YF", "status": "PREPARED_NOT_RUN",
        "session_date": (session_date or DEFAULT_SESSION_DATE).isoformat(),
        "provider": "Yahoo Finance", "access_method": "yfinance",
        "classification": "THIRD_PARTY_RESEARCH_SOURCE", "authorized_nse_feed": False,
        "paper_only": True, "live_trading_enabled": False,
        "broker": "NONE", "broker_calls": 0, "live_orders": 0,
    })
    if session_date is not None:
        manifest["session_date"] = session_date.isoformat()
    manifest.update(dict(updates))
    _atomic_json(path, manifest)
    return manifest


def _write_snapshot(frame: pd.DataFrame, *, output: Path, observed_at: datetime,
                    interval: str, start: datetime, end: datetime,
                    batch_id: int | None = None) -> dict[str, Any]:
    stamp = observed_at.astimezone(IST).strftime("%Y%m%dT%H%M%S%f%z")
    batch_label = f"_batch{batch_id:02d}" if batch_id is not None else ""
    path = output / "raw" / f"yfinance_{interval}{batch_label}_{stamp}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=True, index_label="SourceTimestamp")
    temporary.replace(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    record = {
        "file": str(path.relative_to(output)), "sha256": digest,
        "size": path.stat().st_size, "provider": "Yahoo Finance",
        "access_method": "yfinance", "interval": interval,
        "requested_start": start.isoformat(), "requested_end_exclusive": end.isoformat(),
        "retrieved_at": observed_at.astimezone(IST).isoformat(),
        "batch_id": batch_id,
        "raw_representation": "yfinance-returned DataFrame CSV; no client price adjustment",
    }
    meta_path = output / "raw/snapshot_manifest.jsonl"
    with meta_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")
        stream.flush()
    return record


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    with path.open("r", newline="", encoding="utf-8") as stream:
        return [dict(row) for row in csv.DictReader(stream)]


def _write_acquisition_report(output: Path, requested: Sequence[str], *,
                              normalized: Any | None = None, failure: Mapping[str, Any] | None = None,
                              retrieved_at: datetime | None = None,
                              validation_status: str = "PENDING_PHASE4E",
                              record_snapshot: bool = True,
                              used_for_forward: bool = True) -> dict[str, Any]:
    """Persist explicit per-snapshot symbol coverage, including failures."""
    report_path = output / "reports/acquisition_report.json"
    previous = _read_json(report_path)
    requested_symbols = sorted(set(str(item).upper() for item in requested))
    successful = sorted(set(previous.get("successful_symbols", []))
                        | set(getattr(normalized, "symbols_with_data", []) or []))
    failed = sorted(set(previous.get("failed_symbols", []))
                    | set(getattr(normalized, "failed_symbols", []) or []))
    missing = sorted(set(requested_symbols) - set(successful))
    invalid = sorted(set(previous.get("invalid_symbols", []))
                     | set(getattr(normalized, "invalid_symbols", []) or []))
    duplicate = sorted(set(previous.get("duplicate_symbols", []))
                       | set(getattr(normalized, "duplicate_symbols", []) or []))
    snapshot = {
        "retrieved_at": retrieved_at.astimezone(IST).isoformat() if retrieved_at else None,
        "successful_symbols": sorted(set(getattr(normalized, "symbols_with_data", []) or [])),
        "failed_symbols": sorted(set(getattr(normalized, "failed_symbols", []) or [])),
        "missing_symbols": sorted(set(requested_symbols) - set(getattr(normalized, "symbols_with_data", []) or [])),
        "duplicate_symbols": sorted(set(getattr(normalized, "duplicate_symbols", []) or [])),
        "invalid_symbols": sorted(set(getattr(normalized, "invalid_symbols", []) or [])),
        "failure": dict(failure) if failure else None,
        "validation_status": "FAIL" if failure else validation_status,
        "used_for_forward_signals": used_for_forward,
    }
    report = {
        "provider": "Yahoo Finance/yfinance",
        "classification": "THIRD_PARTY_RESEARCH_SOURCE",
        "authorized_nse_feed": False,
        "retrieved_at": retrieved_at.astimezone(IST).isoformat() if retrieved_at else previous.get("retrieved_at"),
        "requested_symbols": requested_symbols,
        "successful_symbols": successful,
        "failed_symbols": failed,
        "missing_symbols": missing,
        "duplicate_symbols": duplicate,
        "invalid_symbols": invalid,
        "requested_count": len(requested_symbols),
        "successful_count": len(successful),
        "failed_count": len(failed),
        "missing_count": len(missing),
        "duplicate_count": len(duplicate),
        "invalid_count": len(invalid),
        "duplicate_timestamps": int(getattr(normalized, "raw_duplicate_timestamps", 0) or 0),
        "zero_volume_rows": int(previous.get("zero_volume_rows", 0)) + (
            sum(float(row.get("volume", 0) or 0) == 0 for row in (getattr(normalized, "rows", []) or []))
            if record_snapshot else 0),
        "volume_semantics": "UNKNOWN_UNTIL_EMPIRICALLY_CLASSIFIED",
        "validation_status": "FAIL" if failure or invalid or duplicate else validation_status,
        "failure": dict(failure) if failure else None,
        "issues": list(getattr(normalized, "issues", []) or []),
        "snapshot_count": int(previous.get("snapshot_count", 0)) + int(record_snapshot),
        "latest_snapshot": snapshot,
    }
    if (validation_status == "PENDING_PHASE4E" and normalized is not None and not failure
            and not invalid and not duplicate and not previous.get("invalid_symbols")):
        report["validation_status"] = "PENDING_PHASE4E"
    _atomic_json(report_path, report)
    if record_snapshot:
        history = output / "reports/acquisition_snapshots.jsonl"
        history.parent.mkdir(parents=True, exist_ok=True)
        with history.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(snapshot, sort_keys=True, default=str) + "\n")
            stream.flush()
    return report


def _persist_paper_views(processor: Phase4GYFForwardProcessor) -> None:
    output = processor.output
    orders = [asdict(row) for row in processor.engine.all_orders]
    trades = [asdict(row) for row in processor.engine.all_trades]
    signals = list(processor.signals)
    _atomic_csv(output / "reports/top_ranked_candidates.csv", build_top_candidate_summary(processor.rankings))
    _atomic_csv(output / "reports/actual_strategy_candidates.csv",
                build_strategy_signal_summary(signals, orders, trades))
    summary = processor.execution_summary()
    _atomic_json(output / "reports/descriptive_one_day_summary.json", {
        "scope": "one-session descriptive paper results; not strategy validation or profitability evidence",
        **summary,
        "sharpe_ratio": "NOT_CALCULATED_ONE_SESSION",
        "cost_model": "existing project CostModel from config.yaml",
        "cost_details": "completed round trips; open positions remain separately represented",
    })


def _manifest_after_collection(output: Path, processor: Phase4GYFForwardProcessor,
                               instruments: Mapping[str, Any], *,
                               registration: Mapping[str, Any], volume_evidence: Mapping[str, Any],
                               batch_size: int = DEFAULT_BATCH_SIZE,
                               max_concurrent_batches: int = DEFAULT_MAX_CONCURRENT_BATCHES,
                               session_date: date = DEFAULT_SESSION_DATE) -> None:
    rows = list(processor.bar_rows.values())
    symbols = sorted({str(row["symbol"]).upper() for row in rows})
    stamps = sorted(str(row["timestamp"]) for row in rows)
    requested = sorted(instruments)
    report = validate_intraday_dataset(rows, expected_symbols=symbols,
                                       expected_sessions=[session_date], timeframe="1m") if rows else None
    coverage = report.session_coverage if report else {}
    summary = processor.execution_summary()
    _write_experiment_manifest(output, {
        "status": "FORWARD_SESSION_CLOSED_RECONCILIATION_PENDING",
        "data_acquisition": "COMPLETE",
        "provider": "Yahoo Finance", "access_method": "yfinance",
        "data_classification": "THIRD_PARTY_RESEARCH_SOURCE", "authorized_nse_feed": False,
        "source_timezone": report.source_timezones if report else [],
        "canonical_timezone": "Asia/Kolkata", "timeframe": "1m",
        "price_adjustment_status": "UNKNOWN; client used auto_adjust=False and did not alter OHLC",
        "volume_semantics_evidence": dict(volume_evidence),
        "symbols_requested": len(requested), "symbols_observed": len(symbols),
        "frozen_universe_size": len(instruments), "session_universe_size": len(instruments),
        "batch_size": batch_size, "max_concurrent_batches": max_concurrent_batches,
        "number_of_batches": (len(instruments) + batch_size - 1) // batch_size,
        "symbols_missing": sorted(set(requested) - set(symbols)),
        "bars_observed_first_time": len(rows), "first_timestamp": stamps[0] if stamps else None,
        "last_timestamp": stamps[-1] if stamps else None,
        "last_available_timestamp": processor.last_available_timestamp,
        "last_processed_timestamp": processor.last_processed_timestamp,
        "duplicate_timestamps": 0, "revisions_recorded_not_reprocessed": len(processor.revisions),
        "missing_intervals": coverage.get("missing_intervals"),
        "coverage_report": coverage, "phase4e_validation_status": report.validation_status if report else "UNAVAILABLE",
        "phase4f_registration": dict(registration),
        "candidate_rows": len(processor.rankings), "signals": summary["signals"],
        "orders": summary["orders"], "fills": summary["fills"],
        "closed_trades": summary["closed_trades"], "gross_pnl": summary["gross_pnl_closed_trades"],
        "costs": summary["cost_total_closed_trades"], "net_pnl": summary["net_pnl_closed_trades"],
        "max_drawdown": summary["max_intraday_drawdown"],
        "paper_only": True, "live_trading_enabled": False, "broker": "NONE",
        "broker_calls": 0, "live_orders": 0,
        "performance_metrics": {"status": "MEASURED_COLLECTION", **processor.performance_seconds},
        "reconciliation": "NOT_RUN",
    })


def _run_collection_impl(output: Path = OUTPUT_ROOT, *, session_date: date = DEFAULT_SESSION_DATE,
                         now_fn=datetime.now,
                         sleep_fn=time_module.sleep, once: bool = False,
                         batch_size: int = DEFAULT_BATCH_SIZE,
                         max_concurrent_batches: int = DEFAULT_MAX_CONCURRENT_BATCHES,
                         batch_acquirer: Any | None = None,
                         refresh_seconds: int = POLL_SECONDS,
                         monitor_mode: bool = False,
                         revision_seed_dir: Path | None = None,
                         bar_settling_seconds: int = BAR_SETTLING_SECONDS) -> int:
    global _ACTIVE_PROCESSOR
    validate_batch_configuration(batch_size, max_concurrent_batches)
    if refresh_seconds < 30:
        raise ValueError("refresh_seconds must be at least 30")
    _write_experiment_manifest(output, {"session_date": session_date.isoformat()}, session_date=session_date)
    collection_wall_start = time_module.perf_counter()
    now = now_fn(IST)
    preflight = phase4g_preflight(PROJECT_ROOT, now=now, session_date=session_date,
                                 universe_size=486, output_dir=output)
    _atomic_json(output / "reports/preflight.json", preflight)
    if preflight["calendar_check"] != "TRADING_DAY":
        status = "PHASE4G_YF_BLOCKED_SESSION_NOT_VALIDATED"
        detail = {"status": status, "reason": preflight["calendar_reason"],
                  "data_acquisition": "NOT_STARTED", "network_calls": 0,
                  "broker_calls": 0, "live_orders": 0}
        _write_experiment_manifest(output, detail)
        print(json.dumps(detail, indent=2))
        return 2
    if now.astimezone(IST).date() != session_date:
        status = preflight["status"] if preflight["status"].startswith("PHASE4G_YF_BLOCKED") else "PHASE4G_YF_NOT_TARGET_SESSION"
        _write_experiment_manifest(output, {"status": status, "last_preflight": preflight,
                                            "data_acquisition": "NOT_STARTED", "broker_calls": 0, "live_orders": 0})
        print(json.dumps({"status": status, "preflight": preflight}, indent=2, default=str))
        return 2
    if now.astimezone(IST).time() >= SESSION_CLOSE:
        _write_experiment_manifest(output, {"status": "PHASE4G_YF_COLLECTION_WINDOW_PASSED",
                                            "reason": "Use --mode reconcile; post-close bars cannot generate signals."})
        print("PHASE4G_YF_COLLECTION_WINDOW_PASSED")
        return 2
    if preflight["reasons"]:
        status = preflight["status"]
        _write_experiment_manifest(output, {"status": status, "last_preflight": preflight})
        print(json.dumps({"status": status, "reasons": preflight["reasons"]}, indent=2))
        return 2
    instruments = load_yahoo_instruments(PROJECT_ROOT, size=486)
    batches = partition_frozen_universe(instruments, batch_size=batch_size)
    _write_batch_manifest(output, batches=batches, batch_size=batch_size,
                          max_concurrent_batches=max_concurrent_batches,
                          session_date=session_date)
    _write_experiment_manifest(output, {
        "status": "PHASE4G_YF_READY_FOR_FORWARD_COLLECTION",
        "monitor_mode": monitor_mode, "monitor_status": "WARMING_UP" if monitor_mode else None,
        "refresh_interval_seconds": refresh_seconds if monitor_mode else None,
        "data_acquisition": "NOT_STARTED", "frozen_universe_size": len(instruments),
        "session_universe_size": len(instruments), "batch_size": batch_size,
        "max_concurrent_batches": max_concurrent_batches, "number_of_batches": len(batches),
        "batch_execution_summary": {"batch_executions": 0, "successful_batches": 0,
                                    "failed_batches": 0, "retry_count": 0},
        "total_requests": 0, "successful_batches": 0, "failed_batches": 0, "retry_count": 0,
        "request_started_at": None, "response_received_at": None,
        "min_receipt_latency_seconds": None, "max_receipt_latency_seconds": None,
        "avg_receipt_latency_seconds": None, "late_bars_excluded": 0,
        "bars_eligible_by_decision_time": 0, "bars_excluded_by_receipt_time": 0,
        "performance_metrics": {"status": "NOT_MEASURED_UNTIL_COLLECTION",
                                "total_wall_clock_seconds": None, "network_wait_seconds": None,
                                "batch_processing_seconds": None, "validation_seconds": None,
                                "ranking_seconds": None, "strategy_seconds": None,
                                "execution_seconds": None},
        "performance_comparison": "NOT_AVAILABLE",
        "paper_only": True, "live_trading_enabled": False, "broker": "NONE",
        "broker_calls": 0, "live_orders": 0,
    })
    if monitor_mode:
        monitor_title = "CONTROLLED ONE-SHOT PAPER MONITOR" if once else "CONTINUOUS PAPER MONITOR"
        print(
            f"PHASE 4G-YF\n{monitor_title}\n\n"
            f"DATE: {session_date.isoformat()}\nMARKET: NSE\nDATA: YAHOO FINANCE 1m\n"
            f"UNIVERSE: {len(instruments)}\nBATCH SIZE: {batch_size}\n"
            f"CONCURRENT BATCHES: {max_concurrent_batches}\nREFRESH: {refresh_seconds}s\n\n"
            "PAPER TRADING: ENABLED\nLIVE TRADING: DISABLED\nBROKER: NONE\n\n"
            "TIMESTAMP PROTECTION: ENABLED\nLOOKAHEAD PROTECTION: ENABLED\n\n"
            "WARMING_UP...",
            flush=True,
        )
    else:
        print(
            "PHASE 4G-YF\nFIRST REAL FORWARD SESSION\n\n"
            f"DATE: {session_date.isoformat()}\nMODE: PAPER / RESEARCH ONLY\n"
            "DATA: YAHOO FINANCE 1m\n"
            f"UNIVERSE: {len(instruments)}\nBATCH SIZE: {batch_size}\n"
            f"MAX CONCURRENT BATCHES: {max_concurrent_batches}\n"
            "BROKER: NONE\nLIVE TRADING: DISABLED\nPREFLIGHT: PASS\nSTARTING SESSION...",
            flush=True,
        )
    processor = Phase4GYFForwardProcessor(project_root=PROJECT_ROOT, output_dir=output,
                                          instruments=instruments, session_date=session_date,
                                          require_ranked_candidate=monitor_mode,
                                          revision_seed_dir=revision_seed_dir,
                                          bar_settling_seconds=bar_settling_seconds)
    _ACTIVE_PROCESSOR = processor
    volume_evidence: dict[str, Any] = {"classification": "UNKNOWN", "volume_kind": "UNKNOWN"}
    raw_hashes: list[str] = []
    all_batch_records: list[dict[str, Any]] = []
    latest_available: datetime | None = None
    acquirer = batch_acquirer or acquire_batches
    while True:
        cycle_started = time_module.perf_counter()
        observed_at = now_fn(IST).astimezone(IST)
        if observed_at.date() != session_date or observed_at.time() >= SESSION_CLOSE:
            break
        if observed_at.time() < SESSION_OPEN:
            sleep_fn(min(30.0, (datetime.combine(session_date, SESSION_OPEN, tzinfo=IST) - observed_at).total_seconds()))
            continue
        start = latest_available - timedelta(minutes=2) if latest_available else datetime.combine(session_date, SESSION_OPEN, tzinfo=IST)
        if start.date() != session_date:
            start = datetime.combine(session_date, SESSION_OPEN, tzinfo=IST)
        request_end = observed_at
        requested_canonical = list(instruments)
        normalized: NormalizedYahooBars | None = None
        completed_at: datetime | None = None
        batch_records: list[dict[str, Any]] = []
        request_started_perf = time_module.perf_counter()
        try:
            batch_results = acquirer(
                batches, start=start, end=request_end, interval="1m",
                timeout_seconds=REQUEST_TIMEOUT_SECONDS,
                max_concurrent_batches=max_concurrent_batches,
                max_retries=MAX_BATCH_RETRIES,
            )
            request_duration = time_module.perf_counter() - request_started_perf
            completed_at = now_fn(IST).astimezone(IST)
            response_processing_started = time_module.perf_counter()
            normalized, batch_records, quality_missing = _normalize_batch_results(
                batch_results, instruments=instruments, output=output,
                start=start, end=request_end, volume_kind="UNKNOWN", session_date=session_date,
            )
            response_processing_duration = time_module.perf_counter() - response_processing_started
            batch_processing_seconds = response_processing_duration
            refresh_failures, source_issues = _current_refresh_failures(
                batch_results, normalized, attempt_timestamp=completed_at,
            )
            # Infer volume semantics from one normalized source snapshot before
            # passing it to the processor. Reprocessing a cumulative store is
            # not another market refresh and must not inflate duplicate counts.
            volume_evidence = infer_interval_volume_semantics(
                normalized.rows, minimum_pairs=100, minimum_decrease_ratio=0.05,
            )
            if volume_evidence["volume_kind"] == "BAR":
                normalized.rows = [dict(row, volume_kind="BAR") for row in normalized.rows]
            for record in batch_records:
                record["poll_cycle"] = len(all_batch_records) // max(1, len(batches)) + 1
                record["batch_processing_seconds"] = batch_processing_seconds / max(1, len(batch_records))
                if record.get("raw_snapshot", {}).get("sha256"):
                    raw_hashes.append(record["raw_snapshot"]["sha256"])
            all_batch_records.extend(batch_records)
            batch_manifest = _write_batch_manifest(
                output, batches=batches, batch_size=batch_size,
                max_concurrent_batches=max_concurrent_batches, executions=batch_records,
                session_date=session_date,
            )
            summary = _batch_summary(all_batch_records)
            _write_acquisition_report(output, requested_canonical, normalized=normalized,
                                      retrieved_at=completed_at, record_snapshot=False)
            if completed_at.date() != session_date or completed_at.time() >= SESSION_CLOSE:
                _write_acquisition_report(
                    output, requested_canonical, normalized=normalized,
                    retrieved_at=completed_at, validation_status="NOT_USED_AFTER_SESSION_CLOSE",
                    used_for_forward=False,
                )
                _write_experiment_manifest(output, {
                    "status": "SESSION_CLOSE_DURING_REFRESH", "data_acquisition": "SESSION_CLOSED",
                    "batch_manifest": str((output / "batch_manifest.json").relative_to(output)),
                    "batch_execution_summary": _batch_summary(all_batch_records)["batch_execution_summary"],
                    **summary,
                    "performance_metrics": {
                        "status": "MEASURED_COLLECTION",
                        "total_wall_clock_seconds": time_module.perf_counter() - collection_wall_start,
                        "network_wait_seconds": sum(float(item.get("duration_seconds", 0) or 0)
                                                     for item in all_batch_records),
                        "batch_processing_seconds": sum(float(item.get("batch_processing_seconds", 0) or 0)
                                                         for item in all_batch_records),
                        **processor.performance_seconds,
                    },
                    "post_close_signal_generation": False, "broker_calls": 0, "live_orders": 0,
                })
                break
            if normalized.rows:
                latest_available = max(datetime.fromisoformat(row["market_timestamp"]) for row in normalized.rows)
            previous_signal_count = len(processor.signals)
            previous_trade_count = len(processor.engine.all_trades)
            invalid_symbols = set(normalized.invalid_symbols or []) | set(normalized.duplicate_symbols or [])
            source_invalid_rows = [issue for issue in normalized.issues or []
                                   if str(issue.get("symbol") or "").upper() in invalid_symbols]
            result = processor.process_snapshot(
                normalized.rows, now=completed_at, available_timestamp=latest_available,
                data_quality_missing_symbols=quality_missing,
                refresh_failures=refresh_failures,
                source_issues=source_issues,
                source_invalid_rows=source_invalid_rows,
                refresh_timings={
                    "request_duration_seconds": request_duration,
                    "response_processing_duration_seconds": response_processing_duration,
                },
            )
            _write_acquisition_report(
                output, requested_canonical, normalized=normalized,
                retrieved_at=completed_at,
                validation_status=result.get("phase4e_validation_status", "PENDING_PHASE4E"),
            )
            if volume_evidence["volume_kind"] != "BAR" and completed_at.time() >= time(10, 0):
                raise Phase4GYFError("UNUSABLE_VOLUME_SEMANTICS", json.dumps(volume_evidence, sort_keys=True))
            performance = {
                "status": "MEASURED_COLLECTION",
                "total_wall_clock_seconds": time_module.perf_counter() - collection_wall_start,
                "request_duration_seconds": request_duration,
                "network_wait_seconds": sum(float(item.get("duration_seconds", 0) or 0) for item in batch_records),
                "response_processing_duration_seconds": response_processing_duration,
                "batch_processing_seconds": sum(float(item.get("batch_processing_seconds", 0) or 0)
                                                 for item in all_batch_records),
                **processor.performance_seconds,
                "indicator_duration_seconds": processor.performance_seconds.get("indicator_seconds", 0.0),
                "strategy_duration_seconds": processor.performance_seconds.get("strategy_seconds", 0.0),
                "indicator_strategy_duration_seconds": processor.performance_seconds.get("indicator_strategy_seconds", 0.0),
                "paper_execution_duration_seconds": processor.performance_seconds.get("execution_seconds", 0.0),
                "total_refresh_duration_seconds": time_module.perf_counter() - cycle_started,
            }
            decision_snapshots = processor.decision_snapshots
            decision_counters = {
                "late_bars_excluded": sum(int(item.get("late_bars_excluded", 0) or 0) for item in decision_snapshots),
                "bars_eligible_by_decision_time": sum(int(item.get("bars_eligible_by_decision_time", 0) or 0) for item in decision_snapshots),
                "bars_excluded_by_receipt_time": sum(int(item.get("bars_excluded_by_receipt_time", 0) or 0) for item in decision_snapshots),
            }
            successful_receipts = [item.get("response_received_at") for item in all_batch_records if item.get("response_received_at")]
            starts = [item.get("request_started_at") for item in all_batch_records if item.get("request_started_at")]
            _write_experiment_manifest(output, {
                "status": result["status"], "data_acquisition": "IN_PROGRESS",
                "last_available_timestamp": processor.last_available_timestamp,
                "last_processed_timestamp": processor.last_processed_timestamp,
                "frozen_universe_size": len(instruments), "session_universe_size": len(instruments),
                "batch_size": batch_size, "max_concurrent_batches": max_concurrent_batches,
                "number_of_batches": len(batches), "batch_execution_summary": summary["batch_execution_summary"],
                "total_requests": summary["total_requests"], "successful_batches": summary["successful_batches"],
                "failed_batches": summary["failed_batches"], "retry_count": summary["retry_count"],
                "request_started_at": min(starts) if starts else None,
                "response_received_at": max(successful_receipts) if successful_receipts else None,
                "min_receipt_latency_seconds": summary["min_receipt_latency_seconds"],
                "max_receipt_latency_seconds": summary["max_receipt_latency_seconds"],
                "avg_receipt_latency_seconds": summary["avg_receipt_latency_seconds"],
                **decision_counters,
                "symbols_requested": len(instruments), "symbols_observed": result.get("symbols_with_data"),
                "symbols_with_valid_session_data": result.get("symbols_with_valid_session_data"),
                "symbols_with_new_bars": result.get("symbols_with_new_bars"),
                "symbols_with_refresh_failure": result.get("symbols_with_refresh_failure"),
                "bars_observed_first_time": len(processor.bar_rows), "raw_snapshot_hashes": raw_hashes,
                "latest_refresh_accounting": {key: result.get(key) for key in (
                    "rows_seen", "unique_rows_seen", "unique_market_bars", "new_market_bars",
                    "duplicate_market_bars", "duplicate_identical", "duplicate_conflict",
                    "invalid_rows", "late_bars", "eligible_bars", "current_session_new_bars",
                    "warmup_bars", "symbols_with_data", "symbols_with_valid_session_data",
                    "symbols_with_new_bars", "symbols_with_refresh_failure")},
                "volume_semantics_evidence": volume_evidence,
                "candidate_rows": len(processor.rankings), "signals": len(processor.signals),
                "orders": len(processor.engine.all_orders), "performance_metrics": performance,
                "performance_comparison": "NOT_AVAILABLE",
                "paper_only": True, "live_trading_enabled": False, "broker": "NONE",
                "broker_calls": 0, "live_orders": 0,
            })
            if not monitor_mode:
                print(json.dumps(result, indent=2, default=str))
            else:
                print(_format_refresh_diagnostic(result, len(instruments)), flush=True)
            if monitor_mode:
                buys = processor.signals[previous_signal_count:]
                if result["status"] == "PHASE4G_YF_WARMED_UP_NO_RETRO_SIGNALS":
                    monitor_status = "WARMING_UP"
                elif result["status"] == "PHASE4G_YF_WAITING_VOLUME_SEMANTICS":
                    monitor_status = "WARMING_UP"
                else:
                    monitor_status = "PAPER_MONITORING_ACTIVE"
                current_ranking_time = max(
                    (str(item.get("decision_timestamp") or "") for item in processor.rankings),
                    default="",
                )
                current_candidates = sorted(
                    (item for item in processor.rankings
                     if str(item.get("decision_timestamp") or "") == current_ranking_time),
                    key=lambda item: (int(float(item.get("rank") or 999999)),
                                      str(item.get("symbol") or "")),
                )[:5]
                _write_experiment_manifest(output, {
                    "status": result["status"], "monitor_status": monitor_status,
                    "last_refresh_at": completed_at.isoformat(),
                    "refresh_interval_seconds": refresh_seconds,
                    "data_acquisition": "IN_PROGRESS", "monitor_mode": True,
                })
                print(
                    f"PAPER MONITORING {'ACTIVE' if monitor_status == 'PAPER_MONITORING_ACTIVE' else 'WARMING UP'}\n"
                    f"LAST REFRESH: {completed_at.astimezone(IST).strftime('%H:%M:%S IST')}\n"
                    f"PAPER RECOMMENDATIONS: WATCH={len(current_candidates)} "
                    f"BUY={len(processor.signals)} REJECTED={result.get('rejected_signals', 0)}\n"
                    f"PAPER POSITIONS: {len(processor.engine.open_positions)}/{paper_config().max_positions}",
                    flush=True,
                )
                print("MARKET OBSERVATION", flush=True)
                print(_format_refresh_diagnostic(result, len(instruments), include_title=False), flush=True)
                print("PAPER RECOMMENDATION\nTOP PAPER CANDIDATES", flush=True)
                if current_candidates:
                    print("Symbol       Strategy          Score       State", flush=True)
                    for candidate in current_candidates:
                        print(f"{candidate.get('symbol', ''):<12} {'—':<17} "
                              f"{float(candidate.get('score') or 0.0):>7.3f}      WATCH", flush=True)
                else:
                    print("No ranked candidates are available at this decision.", flush=True)
                if not buys:
                    print("NO QUALIFIED BUY SIGNAL", flush=True)
                print("PAPER POSITION", flush=True)
                for position in processor.engine.open_positions:
                    entry = float(position.average_entry_price)
                    stop = float(position.stop_price)
                    target = float(position.target_price)
                    risk = entry - stop
                    rr = (target - entry) / risk if risk > 0 else 0.0
                    print(
                        f"Symbol: {position.symbol} | Strategy: {position.strategy}\n"
                        f"Entry: ₹{entry:.2f} | Current: ₹{position.current_price:.2f}\n"
                        f"Stop Loss: ₹{stop:.2f} | Target: ₹{target:.2f}\n"
                        f"Quantity: {position.quantity} | Risk: ₹{risk * position.quantity:.2f} "
                        f"| R:R: 1:{rr:.2f} | Unrealized P&L: ₹{position.unrealized_pnl:.2f}",
                        flush=True,
                    )
                for trade in processor.engine.all_trades[previous_trade_count:]:
                    title = ("PAPER STOP LOSS" if trade.exit_reason in {"STOP_LOSS", "TRAILING_STOP"}
                             else "PAPER TARGET HIT" if trade.exit_reason == "TARGET"
                             else "PAPER POSITION CLOSED")
                    closed_position = processor.engine.positions.get(trade.position_id)
                    stop_price = getattr(closed_position, "stop_price", None)
                    target_price = getattr(closed_position, "target_price", None)
                    stop_text = f"₹{float(stop_price):.2f}" if stop_price is not None else "—"
                    target_text = f"₹{float(target_price):.2f}" if target_price is not None else "—"
                    print(
                        "============================================================\n"
                        f"{title}\n============================================================\n\n"
                        f"Symbol: {trade.symbol} | Strategy: {trade.strategy}\n"
                        f"Entry: ₹{trade.entry_price:.2f} | Exit: ₹{trade.exit_price:.2f}\n"
                        f"Stop: {stop_text} | Target: {target_text}\n"
                        f"Quantity: {trade.quantity} | Gross P&L: ₹{trade.gross_pnl:.2f}\n"
                        f"Costs: ₹{trade.total_cost:.2f} | Net P&L: ₹{trade.net_pnl:.2f}\n"
                        f"Exit reason: {trade.exit_reason} | STATUS: CLOSED\n"
                        "============================================================",
                        flush=True,
                    )
                for buy in buys:
                    order = next((item for item in processor.engine.all_orders
                                  if item.order_id == buy.get("paper_order_id")), None)
                    entry = float(buy["entry_reference"])
                    stop = float(buy["stop_reference"])
                    target = float(buy["target_reference"])
                    risk = entry - stop
                    rr = (target - entry) / risk if risk > 0 else 0.0
                    print(
                            "==================================================\nPAPER BUY SIGNAL\n"
                        "==================================================\n\n"
                        f"Symbol       : {buy['symbol']}\nStrategy     : {buy['strategy']}\n"
                        f"Signal Time  : {buy['timestamp']}\n\n"
                        f"Entry        : ₹{entry:.2f}\nStop Loss    : ₹{stop:.2f}\n"
                        f"Target       : ₹{target:.2f}\n\nRisk/Share   : ₹{risk:.2f}\n"
                        f"Risk/Reward  : 1:{rr:.2f}\nQuantity     : {getattr(order, 'quantity', 0)}\n"
                        f"Max Risk     : ₹{getattr(order, 'planned_risk', 0.0):.2f}\n\n"
                        f"Capital      : ₹{paper_config().initial_capital:,.0f}\n"
                        f"Open Positions: {len(processor.engine.open_positions)}/5\n\n"
                        f"Market Bar   : {buy.get('market_timestamp')}\nReceived     : {buy.get('received_timestamp')}\n"
                        f"Decision     : {buy.get('generated_at')}\nStatus       : VALID\n\n"
                            "✓ Strategy confirmed\n✓ Data quality passed\n✓ Timestamp protection passed\n"
                            "✓ Candidate ranking passed\n✓ Risk and portfolio capacity passed\n\n"
                            "STATUS: PAPER BUY\nPAPER ORDER CREATED\nNO LIVE ORDER\n==================================================",
                        flush=True,
                    )
        except Exception as exc:
            code = getattr(exc, "code", "PHASE4G_YF_BLOCKED_PROVIDER")
            immutable_readiness = _read_json(output / "reports/readiness_report.json") or {}
            if code == "YAHOO_NO_INTRADAY_DATA" and now_fn(IST).astimezone(IST).time() < time(9, 20):
                sleep_fn(refresh_seconds)
                continue
            status = "PHASE4G_YF_BLOCKED_PROVIDER" if code in {"YAHOO_TIMEOUT", "PHASE4G_YF_BLOCKED_PROVIDER"} else "PHASE4G_YF_BLOCKED_DATA_QUALITY"
            failure = {"status": status, "reason_code": code, "reason": str(exc),
                       "observed_at": now_fn(IST).astimezone(IST).isoformat(),
                       "monitor_status": "BLOCKED" if monitor_mode else None,
                       "immutable_bar_readiness": immutable_readiness.get("status", "NOT_EVALUATED"),
                       "readiness_report": immutable_readiness,
                       "signals": 0, "orders": 0, "fills": 0,
                       "ledger_mutation_during_blocked_refresh": False,
                       "network_source": "Yahoo Finance/yfinance", "paper_only": True,
                       "broker": "NONE", "broker_calls": 0, "live_orders": 0}
            if normalized is not None:
                _write_acquisition_report(output, requested_canonical, normalized=normalized,
                                          failure={"reason_code": code, "reason": str(exc)},
                                          retrieved_at=now_fn(IST).astimezone(IST),
                                          used_for_forward=False)
            _atomic_json(output / "reports/data_quality_failure.json", failure)
            latest_refresh = _last_refresh_audit(output)
            refresh_accounting = None
            if (latest_refresh and completed_at is not None
                    and latest_refresh.get("decision_timestamp") == completed_at.isoformat()):
                refresh_accounting = {key: latest_refresh.get(key) for key in (
                    "rows_seen", "unique_rows_seen", "unique_market_bars", "new_market_bars",
                    "duplicate_market_bars", "duplicate_identical", "duplicate_conflict",
                    "invalid_rows", "late_bars", "eligible_bars", "current_session_new_bars",
                    "warmup_bars", "symbols_with_valid_session_data", "symbols_with_new_bars",
                    "symbols_with_refresh_failure")}
            _write_experiment_manifest(output, {**failure,
                                                "latest_refresh_accounting": refresh_accounting,
                                                "last_refresh_at": completed_at.isoformat() if completed_at else None,
                                                "raw_snapshot_hashes": raw_hashes,
                                                "data_acquisition": "STOPPED_FAIL_CLOSED",
                                                "batch_execution_summary": _batch_summary(all_batch_records).get("batch_execution_summary"),
                                                "performance_comparison": "NOT_AVAILABLE"})
            print(json.dumps(failure, indent=2))
            return 2
        if once:
            return 0
        if monitor_mode:
            elapsed = time_module.perf_counter() - cycle_started
            sleep_fn(refresh_sleep_seconds(elapsed, refresh_seconds))
        else:
            sleep_fn(POLL_SECONDS)
    close_at = now_fn(IST).astimezone(IST)
    if close_at.date() != session_date:
        close_at = datetime.combine(session_date, SESSION_CLOSE, tzinfo=IST)
    elif close_at.time() < SESSION_CLOSE:
        close_at = datetime.combine(session_date, SESSION_CLOSE, tzinfo=IST)
    processor.finish_session(now=close_at)
    try:
        registration = register_phase4g_dataset(project_root=PROJECT_ROOT, output_dir=output,
                                                instruments=instruments, volume_evidence=volume_evidence)
    except Exception as exc:
        code = getattr(exc, "code", "PHASE4F_REGISTRATION_FAILED")
        failure = {"status": "PHASE4G_YF_BLOCKED_DATA_QUALITY", "reason_code": code,
                   "reason": str(exc), "data_acquisition": "SESSION_CLOSED",
                   "broker_calls": 0, "live_orders": 0}
        _atomic_json(output / "reports/data_quality_failure.json", failure)
        _write_experiment_manifest(output, failure)
        print(json.dumps(failure, indent=2))
        return 2
    _persist_paper_views(processor)
    _manifest_after_collection(output, processor, instruments, registration=registration,
                               volume_evidence=volume_evidence, batch_size=batch_size,
                               max_concurrent_batches=max_concurrent_batches,
                               session_date=session_date)
    final_summary = _batch_summary(all_batch_records)
    _write_experiment_manifest(output, {
        **final_summary,
        "frozen_universe_size": len(instruments), "session_universe_size": len(instruments),
        "batch_size": batch_size, "max_concurrent_batches": max_concurrent_batches,
        "number_of_batches": len(batches), "total_wall_clock_seconds": time_module.perf_counter() - collection_wall_start,
        "network_wait_seconds": sum(float(item.get("duration_seconds", 0) or 0) for item in all_batch_records),
        "performance_comparison": "NOT_AVAILABLE",
        "performance_metrics": {
            "status": "MEASURED_COLLECTION",
            "total_wall_clock_seconds": time_module.perf_counter() - collection_wall_start,
            "network_wait_seconds": sum(float(item.get("duration_seconds", 0) or 0) for item in all_batch_records),
            "batch_processing_seconds": sum(float(item.get("batch_processing_seconds", 0) or 0)
                                             for item in all_batch_records),
            **processor.performance_seconds,
        },
        "paper_only": True, "live_trading_enabled": False, "broker": "NONE",
        "broker_calls": 0, "live_orders": 0,
    })
    print(json.dumps({"status": "FORWARD_SESSION_CLOSED_RECONCILIATION_PENDING",
                      "registration": registration, "summary": processor.execution_summary()}, indent=2, default=str))
    if monitor_mode:
        _write_experiment_manifest(output, {"monitor_status": "SESSION_CLOSED",
                                            "post_close_reconciliation": "STARTED",
                                            "monitor_mode": True})
        return run_reconciliation(output, now_fn=now_fn, batch_size=batch_size,
                                  max_concurrent_batches=max_concurrent_batches,
                                  batch_acquirer=batch_acquirer, session_date=session_date)
    return 0


def run_collection(output: Path = OUTPUT_ROOT, *, session_date: date = DEFAULT_SESSION_DATE,
                   now_fn=datetime.now,
                   sleep_fn=time_module.sleep, once: bool = False,
                   batch_size: int = DEFAULT_BATCH_SIZE,
                   max_concurrent_batches: int = DEFAULT_MAX_CONCURRENT_BATCHES,
                   batch_acquirer: Any | None = None,
                   refresh_seconds: int = POLL_SECONDS,
                   monitor_mode: bool = False,
                   revision_seed_dir: Path | None = None,
                   bar_settling_seconds: int = BAR_SETTLING_SECONDS) -> int:
    """Run the legacy collection path or continuous paper monitor.

    KeyboardInterrupt leaves the Phase 4C paper ledger and run-state checkpoint
    intact; it never creates an order while shutting down.
    """
    global _ACTIVE_PROCESSOR
    _ACTIVE_PROCESSOR = None
    try:
        return _run_collection_impl(
            output, session_date=session_date, now_fn=now_fn, sleep_fn=sleep_fn, once=once,
            batch_size=batch_size, max_concurrent_batches=max_concurrent_batches,
            batch_acquirer=batch_acquirer, refresh_seconds=refresh_seconds,
            monitor_mode=monitor_mode, revision_seed_dir=revision_seed_dir,
            bar_settling_seconds=bar_settling_seconds,
        )
    except KeyboardInterrupt:
        processor = _ACTIVE_PROCESSOR
        checkpoint = None
        if processor is not None:
            checkpoint = processor.checkpoint_shutdown()
        now = now_fn(IST).astimezone(IST)
        detail = {
            "status": "PHASE4G_YF_MONITOR_INTERRUPTED" if monitor_mode else "PHASE4G_YF_COLLECTION_INTERRUPTED",
            "monitor_status": "STOPPED_BY_USER" if monitor_mode else None,
            "interrupted_at": now.isoformat(),
            "data_acquisition": "STOPPED_BY_USER",
            "checkpoint": checkpoint,
            "no_new_orders_during_shutdown": True,
            "paper_only": True, "live_trading_enabled": False, "broker": "NONE",
            "broker_calls": 0, "live_orders": 0,
        }
        _write_experiment_manifest(output, detail)
        _atomic_json(output / "reports/shutdown.json", detail)
        print(json.dumps(detail, indent=2, default=str), flush=True)
        return 130
    finally:
        _ACTIVE_PROCESSOR = None


def run_reconciliation(output: Path = OUTPUT_ROOT, *, session_date: date = DEFAULT_SESSION_DATE,
                       now_fn=datetime.now,
                       batch_size: int = DEFAULT_BATCH_SIZE,
                       max_concurrent_batches: int = DEFAULT_MAX_CONCURRENT_BATCHES,
                       batch_acquirer: Any | None = None) -> int:
    validate_batch_configuration(batch_size, max_concurrent_batches)
    now = now_fn(IST).astimezone(IST)
    if now.date() < session_date or (now.date() == session_date and now.time() < SESSION_CLOSE):
        print("PHASE4G_YF_RECONCILIATION_NOT_AVAILABLE_BEFORE_SESSION_CLOSE")
        return 2
    manifest = _read_json(output / "manifest.json")
    if manifest.get("data_acquisition") == "NOT_STARTED" or not (output / "canonical/forward_bars_1m.csv").is_file():
        print("PHASE4G_YF_BLOCKED_DATA_QUALITY: no forward-acquired data to reconcile")
        return 2
    instruments = load_yahoo_instruments(PROJECT_ROOT, size=486)
    batches = partition_frozen_universe(instruments, batch_size=batch_size)
    start = datetime.combine(session_date, SESSION_OPEN, tzinfo=IST)
    end = datetime.combine(session_date + timedelta(days=1), time(0, 0), tzinfo=IST)
    try:
        acquirer = batch_acquirer or acquire_batches
        results = acquirer(batches, start=start, end=end, interval="1m",
                           timeout_seconds=REQUEST_TIMEOUT_SECONDS,
                           max_concurrent_batches=max_concurrent_batches,
                           max_retries=MAX_BATCH_RETRIES)
        normalized, batch_records, quality_missing = _normalize_batch_results(
            results, instruments=instruments, output=output, start=start, end=end,
            volume_kind="UNKNOWN", session_date=session_date,
        )
        _write_batch_manifest(output, batches=batches, batch_size=batch_size,
                              max_concurrent_batches=max_concurrent_batches,
                              executions=[{**item, "reconciliation": True} for item in batch_records],
                              session_date=session_date)
        kind = "BAR" if manifest.get("volume_semantics_evidence", {}).get("volume_kind") == "BAR" else "UNKNOWN"
        if kind == "BAR":
            normalized.rows = [dict(row, volume_kind="BAR") for row in normalized.rows]
        if normalized.invalid_symbols or normalized.duplicate_symbols:
            raise Phase4GYFError("PHASE4E_RECONCILIATION_VALIDATION_FAILED",
                                 json.dumps({"invalid_symbols": normalized.invalid_symbols,
                                             "duplicate_symbols": normalized.duplicate_symbols,
                                             "issues": normalized.issues}, sort_keys=True))
        final_rows = normalized.rows
        final_symbols = sorted({row["symbol"] for row in final_rows})
        quality = validate_intraday_dataset(final_rows, expected_symbols=final_symbols,
                                            expected_sessions=[session_date], timeframe="1m") if final_rows else None
        if quality is None or quality.validation_status != "PASS":
            raise Phase4GYFError("PHASE4E_RECONCILIATION_VALIDATION_FAILED",
                                 json.dumps(quality.to_dict() if quality else {}, default=str))
        _atomic_csv(output / "reconciliation/final_yfinance_1m.csv", final_rows)
        forward_rows = _read_csv(output / "canonical/forward_bars_1m.csv")
        result = reconciliation_report(forward_rows, final_rows, requested_symbols=sorted(instruments))
        result.update({"status": "RECONCILIATION_COMPLETE", "retrieved_at": now.isoformat(),
                       "batch_executions": batch_records, "phase4e_validation_status": quality.validation_status,
                       "final_phase4e_coverage": quality.session_coverage})
        _atomic_json(output / "reconciliation/reconciliation_report.json", result)
        # Report files are written from the already persisted paper ledger.  No
        # scanner or execution object is constructed in this post-close route.
        ledger = inspect_paper_ledger(output / "paper_ledger.json")
        _write_experiment_manifest(output, {"status": "PHASE4G_YF_DATA_COMPLETE_DESCRIPTIVE_ONLY",
                                            "reconciliation": result,
                                            "after_close_symbols": len(final_symbols),
                                            "after_close_bars": len(final_rows),
                                            "paper_ledger_status": ledger.get("status"),
                                            "post_close_signal_generation": False,
                                            "ledger_mutation_during_reconciliation": False,
                                            "batch_size": batch_size,
                                            "max_concurrent_batches": max_concurrent_batches,
                                            "number_of_batches": len(batches),
                                            "broker_calls": 0, "live_orders": 0},
                                   session_date=session_date)
        print(json.dumps({"status": "PHASE4G_YF_DATA_COMPLETE_DESCRIPTIVE_ONLY", "reconciliation": result}, indent=2, default=str))
        return 0
    except Exception as exc:
        code = getattr(exc, "code", "YAHOO_PROVIDER_ERROR")
        failure = {"status": "PHASE4G_YF_BLOCKED_DATA_QUALITY", "reason_code": code,
                   "reason": str(exc), "post_close_signal_generation": False,
                   "ledger_mutation_during_reconciliation": False, "broker_calls": 0, "live_orders": 0}
        _atomic_json(output / "reconciliation/reconciliation_report.json", failure)
        _write_experiment_manifest(output, {"status": failure["status"], "reconciliation": failure},
                                   session_date=session_date)
        print(json.dumps(failure, indent=2))
        return 2


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("diagnostic", "preflight", "collect", "monitor", "reconcile"), default="preflight")
    parser.add_argument("--session-date", type=_parse_session_date, default=DEFAULT_SESSION_DATE,
                        help="explicit NSE session date (YYYY-MM-DD); defaults to the frozen 2026-09-28 experiment")
    parser.add_argument("--once", action="store_true", help="one live collection refresh, only on target session")
    parser.add_argument("--refresh-seconds", type=_refresh_seconds, default=POLL_SECONDS,
                        help="minimum seconds between refresh starts in monitor mode (minimum 30; default 60)")
    parser.add_argument("--batch-size", type=_positive_int, default=DEFAULT_BATCH_SIZE,
                        help=f"symbols per request batch (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--max-concurrent-batches", type=_positive_int,
                        default=DEFAULT_MAX_CONCURRENT_BATCHES,
                        help=f"maximum concurrent isolated batch workers (default: {DEFAULT_MAX_CONCURRENT_BATCHES})")
    parser.add_argument("--output-dir", type=Path, default=None,
                        help="isolated Phase 4G result directory (custom batch settings get their own namespace)")
    parser.add_argument("--summary", action="store_true",
                        help="print a short human-readable preflight summary instead of the full JSON report")
    parser.add_argument("--source-dir", type=Path, default=None,
                        help="read-only artifact directory for --mode diagnostic")
    parser.add_argument("--revision-seed-dir", type=Path, default=None,
                        help="read-only Phase 4G-YF directory whose canonical bars and revision journal seed reconciliation")
    parser.add_argument("--bar-settling-seconds", type=_nonnegative_int, default=BAR_SETTLING_SECONDS,
                        help=f"closed 1m bar settling delay (default {BAR_SETTLING_SECONDS}; strategy settings are unchanged)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    if args.mode == "diagnostic":
        source = args.source_dir or default_monitor_output_root(args.session_date)
        report = phase4g_diagnostic_report(source)
        print(json.dumps(report, indent=2, default=str))
        return 0 if report.get("status") != "DIAGNOSTIC_READ_FAILED" else 2
    output = (args.output_dir.resolve() if args.output_dir else
              (default_monitor_output_root(args.session_date) if args.mode == "monitor" else
               default_output_root(args.batch_size, args.max_concurrent_batches,
                                   session_date=args.session_date)).resolve())
    output.mkdir(parents=True, exist_ok=True)
    validate_batch_configuration(args.batch_size, args.max_concurrent_batches)
    if args.mode == "preflight":
        report = phase4g_preflight(PROJECT_ROOT, now=datetime.now(IST), session_date=args.session_date,
                                   universe_size=486, output_dir=output)
        _atomic_json(output / "reports/preflight.json", report)
        instruments = load_yahoo_instruments(PROJECT_ROOT, size=486)
        batches = partition_frozen_universe(instruments, batch_size=args.batch_size)
        _write_batch_manifest(output, batches=batches, batch_size=args.batch_size,
                              max_concurrent_batches=args.max_concurrent_batches,
                              session_date=args.session_date)
        _write_experiment_manifest(output, {"status": report["status"], "last_preflight": report,
                                                 "session_date": args.session_date.isoformat(),
                                                 "data_acquisition": "NOT_STARTED" if report["status"] == "PHASE4G_YF_PREPARED_NOT_RUN" else "NOT_RUN",
                                                 "frozen_universe_size": len(instruments),
                                                 "session_universe_size": len(instruments),
                                                 "batch_size": args.batch_size,
                                                 "max_concurrent_batches": args.max_concurrent_batches,
                                                 "number_of_batches": len(batches),
                                                 "performance_metrics": {"status": "NOT_MEASURED_UNTIL_COLLECTION",
                                                                         "total_wall_clock_seconds": None,
                                                                         "network_wait_seconds": None,
                                                                         "batch_processing_seconds": None,
                                                                         "validation_seconds": None,
                                                                         "ranking_seconds": None,
                                                                         "strategy_seconds": None,
                                                                         "execution_seconds": None},
                                                 "performance_comparison": "NOT_AVAILABLE",
                                                 "paper_only": True, "live_trading_enabled": False,
                                                 "broker": "NONE", "broker_calls": 0, "live_orders": 0})
        if args.summary:
            _print_preflight_summary(report)
        else:
            print(json.dumps(report, indent=2, default=str))
        return 0 if report["status"] in {"PHASE4G_YF_PREPARED_NOT_RUN", "PHASE4G_YF_WAITING_FOR_SESSION_OPEN",
                                         "PHASE4G_YF_READY_FOR_FORWARD_COLLECTION", "PHASE4G_YF_RECONCILIATION_ONLY"} else 2
    if args.mode == "collect":
        return run_collection(output, once=args.once, batch_size=args.batch_size,
                              session_date=args.session_date,
                              max_concurrent_batches=args.max_concurrent_batches,
                              revision_seed_dir=args.revision_seed_dir,
                              bar_settling_seconds=args.bar_settling_seconds)
    if args.mode == "monitor":
        return run_collection(output, once=args.once, batch_size=args.batch_size,
                              session_date=args.session_date,
                              max_concurrent_batches=args.max_concurrent_batches,
                              refresh_seconds=args.refresh_seconds, monitor_mode=True,
                              revision_seed_dir=args.revision_seed_dir,
                              bar_settling_seconds=args.bar_settling_seconds)
    return run_reconciliation(output, session_date=args.session_date, batch_size=args.batch_size,
                              max_concurrent_batches=args.max_concurrent_batches)


def _print_preflight_summary(report: Mapping[str, Any]) -> None:
    checks = report.get("checks") or {}
    passed = sum(item.get("status") == "PASS" for item in checks.values() if isinstance(item, Mapping))
    total = len(checks)
    reasons = list(report.get("reasons") or [])
    blocked_checks = [name for name, item in checks.items()
                      if isinstance(item, Mapping) and item.get("status") != "PASS"]
    accepted_preflight_statuses = {
        "PHASE4G_YF_PREPARED_NOT_RUN",
        "PHASE4G_YF_WAITING_FOR_SESSION_OPEN",
        "PHASE4G_YF_READY_FOR_FORWARD_COLLECTION",
    }
    preflight_passed = report.get("status") in accepted_preflight_statuses and not reasons and not blocked_checks
    print("PHASE 4G-YF PAPER READINESS")
    print(f"Result          : {'LOCAL PREFLIGHT PASS - MARKET DATA UNTESTED' if preflight_passed else 'LOCAL PREFLIGHT BLOCKED'}")
    print(f"Status          : {report.get('status', 'UNKNOWN')}")
    print(f"Session         : {report.get('session_date', 'UNKNOWN')} ({report.get('calendar_check', 'UNKNOWN')})")
    print(f"Universe        : {report.get('symbols_requested', 0)}/{report.get('symbols_expected', 0)} mapped")
    print(f"Checks          : {passed}/{total} passed")
    print("Yahoo feed      : NOT TESTED")
    print("Data fetch      : NOT STARTED")
    print("Paper ledger    : UNCHANGED")
    print("Broker / live   : NONE / DISABLED")
    for reason in reasons:
        print(f"Reason          : {reason}")
    for check in blocked_checks:
        detail = checks[check].get("detail", "")
        print(f"Failed check    : {check}{': ' + str(detail) if detail else ''}")
    if preflight_passed:
        print("Next            : run the paper monitor; trust no signal until its live-data checks pass")


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if result <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def _parse_session_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("session date must use YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise argparse.ArgumentTypeError("session date must use YYYY-MM-DD")
    return parsed


def _nonnegative_int(value: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if result < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return result


def _refresh_seconds(value: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("must be an integer of at least 30 seconds") from exc
    if result < 30:
        raise argparse.ArgumentTypeError("must be at least 30 seconds to protect NSE/Yahoo access cadence")
    return result


def default_output_root(batch_size: int = DEFAULT_BATCH_SIZE,
                        max_concurrent_batches: int = DEFAULT_MAX_CONCURRENT_BATCHES, *,
                        session_date: date = DEFAULT_SESSION_DATE) -> Path:
    if (batch_size, max_concurrent_batches) == (DEFAULT_BATCH_SIZE, DEFAULT_MAX_CONCURRENT_BATCHES):
        if session_date == date(2026, 9, 28):
            return OUTPUT_ROOT
        return PROJECT_ROOT / f"results/phase4g_yf_{session_date:%Y%m%d}_batched"
    return PROJECT_ROOT / f"results/phase4g_yf_{session_date:%Y%m%d}_batch{batch_size}_c{max_concurrent_batches}"


def default_monitor_output_root(session_date: date = DEFAULT_SESSION_DATE) -> Path:
    if session_date == date(2026, 9, 28):
        return MONITOR_OUTPUT_ROOT
    return PROJECT_ROOT / f"results/phase4g_yf_{session_date:%Y%m%d}_monitor"


if __name__ == "__main__":
    raise SystemExit(main())
