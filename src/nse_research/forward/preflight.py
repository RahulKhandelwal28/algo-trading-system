"""Read-only readiness checks for the Phase 3D daily paper workflow."""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from nse_research.config import AppConfig
from nse_research.data.licensed_nse_cm_eod import (
    CANONICAL_COLUMNS,
    LicensedNseCmEodProvider,
    NseCmEodSchema,
    ParsedNseCmRow,
)
from nse_research.data.providers import NseOfficialHistoryCsvProvider, REQUIRED_COLUMNS
from nse_research.forward.baseline import validate_baseline_manifest
from nse_research.forward.data import PersonalPaperEodProvider, validate_daily_bars
from nse_research.forward.intake import expected_eq_symbols
from nse_research.forward.engine import load_local_universe


_CANONICAL_SCHEMA = NseCmEodSchema(
    date_column="Date",
    symbol_column="Symbol",
    series_column="Series",
    open_column="Open Price",
    high_column="High Price",
    low_column="Low Price",
    close_column="Close Price",
    volume_column="Total Traded Quantity",
)


@dataclass(frozen=True)
class PreflightResult:
    status: str
    market_date: str
    checks: dict[str, dict[str, Any]]
    reasons: tuple[str, ...]
    dataset_summary: dict[str, Any]

    def format(self) -> str:
        lines = [self.status, f"Market date: {self.market_date}", "Checks:"]
        for name, check in self.checks.items():
            state = "PASS" if check["passed"] else "BLOCKED"
            detail = check.get("detail")
            lines.append(f"- {name}: {state}" + (f" — {detail}" if detail else ""))
        if self.reasons:
            lines.append("Reasons:")
            lines.extend(f"- {reason}" for reason in self.reasons)
        return "\n".join(lines)


def _check(checks: dict[str, dict[str, Any]], name: str, passed: bool, detail: str = "") -> None:
    checks[name] = {"passed": bool(passed), "detail": detail}


def _as_base_symbol(value: str) -> str:
    symbol = str(value).strip().upper()
    return symbol.removesuffix(".NS")


def _read_index(
    path: Path,
    symbol: str,
    start: date,
    end: date,
    market_date: date,
    min_history_rows: int,
    max_daily_price_jump_fraction: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not path.is_file():
        return pd.DataFrame(columns=REQUIRED_COLUMNS), {
            "exists": False,
            "path": str(path),
            "valid": False,
            "freshness_status": "DATA_STALE",
            "reason": f"required official NSE file does not exist: {path}",
            "rows": 0,
        }
    parser = NseOfficialHistoryCsvProvider(path.parent)
    try:
        # Read the named official export directly. This stays local and avoids
        # accidentally selecting a similarly named cache file.
        frames = parser._read_export(path, symbol, preserve_invalid=True)
        frame = pd.concat(frames) if frames else pd.DataFrame(columns=REQUIRED_COLUMNS)
        keep = [
            pd.isna(stamp) or start <= pd.Timestamp(stamp).date() <= end
            for stamp in frame.index
        ]
        frame = frame.loc[keep]
        bars, quality = validate_daily_bars(
            frame,
            provider="nse_official_index_csv",
            symbol=symbol,
            market_date=market_date,
            max_daily_price_jump_fraction=max_daily_price_jump_fraction,
        )
        reasons: list[str] = []
        if not quality.valid:
            reasons.append(quality.reason)
        if len(bars) < min_history_rows:
            reasons.append(f"only {len(bars)} valid rows in warm-up window; {min_history_rows} required")
        if not any(pd.Timestamp(stamp).date() == market_date for stamp in bars.index):
            reasons.append(f"no row for required market date {market_date.isoformat()}")
        return bars, {
            "exists": True,
            "path": str(path),
            "valid": not reasons,
            "reason": "; ".join(reasons),
            "freshness_status": quality.freshness_status,
            "rows": len(bars),
            "first_date": min((pd.Timestamp(stamp).date() for stamp in bars.index), default=None).isoformat()
            if len(bars) else None,
            "last_date": max((pd.Timestamp(stamp).date() for stamp in bars.index), default=None).isoformat()
            if len(bars) else None,
            "duplicate_records": quality.duplicate_records,
            "invalid_records": quality.invalid_records,
        }
    except Exception as error:  # malformed local exports are a preflight blocker
        return pd.DataFrame(columns=REQUIRED_COLUMNS), {
            "exists": True,
            "path": str(path),
            "valid": False,
            "freshness_status": "DATA_INVALID",
            "reason": f"{type(error).__name__}: {error}",
            "rows": 0,
        }


def _audit_personal_local_equities(
    *,
    cache_dir: Path,
    daily_data_dir: Path,
    identity_file: Path,
    required_symbols: Iterable[str],
    market_date: date,
    start: date,
    index_dir: Path,
    max_daily_price_jump_fraction: float,
    min_history_rows: int,
) -> dict[str, Any]:
    """Validate cached/local daily bars and explicit current EQ identities."""
    result: dict[str, Any] = {
        "exists": cache_dir.is_dir(), "path": str(cache_dir), "total_rows": 0,
        "eq_rows": 0, "non_eq_rows": 0, "valid_rows": 0, "symbols": [],
        "target_date_symbols": [], "first_date": None, "last_date": None,
        "sessions": 0, "missing_sessions": [], "duplicate_rows": 0,
        "invalid_rows": 0, "invalid_ohlc_rows": 0, "validation_status": "FAIL",
        "errors": [], "history_by_symbol": {}, "identity_series": "NOT_ASSESSABLE",
        "missing_daily_current": False, "source": "Yahoo-derived existing local OHLC cache plus normalized date files",
    }
    if not cache_dir.is_dir():
        result["errors"].append(f"local historical equity cache directory is missing: {cache_dir}")
        return result
    try:
        eq_symbols = expected_eq_symbols(identity_file)
    except Exception as error:
        result["errors"].append(f"cannot validate explicit EQ identity file: {type(error).__name__}: {error}")
        return result
    result["identity_series"] = "PASS"
    provider = PersonalPaperEodProvider(cache_dir, index_dir, daily_data_dir)
    required = list(dict.fromkeys(_as_base_symbol(value) for value in required_symbols))
    frames: dict[str, pd.DataFrame] = {}
    all_dates: set[date] = set()
    first: date | None = None
    last: date | None = None
    present_anywhere: set[str] = set()
    present_on_date: set[str] = set()
    per_symbol_failures: list[str] = []
    for symbol in required:
        if symbol not in eq_symbols:
            result["invalid_rows"] += 1
            per_symbol_failures.append(f"{symbol}: not an explicitly mapped EQ security in {identity_file}")
            continue
        try:
            frame = provider.history(f"{symbol}.NS", start, market_date, "1d")
        except Exception as error:
            per_symbol_failures.append(f"{symbol}: {type(error).__name__}: {error}")
            continue
        if frame is None or frame.empty:
            per_symbol_failures.append(f"{symbol}: no local EOD rows")
            continue
        frames[symbol] = frame
        present_anywhere.add(symbol)
        result["total_rows"] += len(frame)
        result["eq_rows"] += len(frame)
        dates = [pd.Timestamp(stamp).date() for stamp in frame.index if pd.notna(stamp)]
        all_dates.update(dates)
        if dates:
            first = min(first, min(dates)) if first else min(dates)
            last = max(last, max(dates)) if last else max(dates)
        if any(day == market_date for day in dates):
            present_on_date.add(symbol)
        _, quality = validate_daily_bars(
            frame,
            provider="personal_local_eod",
            symbol=f"{symbol}.NS",
            market_date=market_date,
            max_daily_price_jump_fraction=max_daily_price_jump_fraction,
        )
        if quality.duplicate_records:
            result["duplicate_rows"] += quality.duplicate_records
        if quality.invalid_records:
            result["invalid_rows"] += quality.invalid_records
        if not quality.valid:
            per_symbol_failures.append(f"{symbol}: {quality.reason}")
        elif len(frame) < min_history_rows:
            per_symbol_failures.append(f"{symbol}: only {len(frame)} valid warm-up rows; {min_history_rows} required")
        else:
            result["valid_rows"] += len(frame)
        if not any(day == market_date for day in dates):
            per_symbol_failures.append(f"{symbol}: no exact-date equity bar for {market_date.isoformat()}")

    missing_anywhere = sorted(set(required) - present_anywhere)
    missing_on_date = sorted(set(required) - present_on_date)
    excluded_rows = provider.excluded_non_nifty_rows
    result.update({
        "symbols": sorted(present_anywhere), "target_date_symbols": sorted(present_on_date),
        "first_date": first.isoformat() if first else None, "last_date": last.isoformat() if last else None,
        "sessions": len(all_dates), "missing_anywhere": missing_anywhere,
        "missing_on_date": missing_on_date, "per_symbol_failures": per_symbol_failures,
        "history_by_symbol": frames,
        "excluded_non_nifty_session_rows": sum(excluded_rows.values()),
        "excluded_non_nifty_session_rows_by_symbol": excluded_rows,
        "excluded_non_nifty_session_dates": provider.excluded_non_nifty_dates,
        "missing_daily_current": bool(missing_on_date),
        "validation_status": "PASS" if not per_symbol_failures and not missing_anywhere and not missing_on_date else "FAIL",
    })
    if missing_on_date:
        daily_path = daily_data_dir / f"{market_date.isoformat()}.csv"
        result["errors"].append(
            f"missing current-date EQ rows for {len(missing_on_date)} symbols; expected local intake output {daily_path}"
        )
    result["errors"].extend(per_symbol_failures[:20])
    return result


def _audit_equity_dataset(
    path: Path,
    required_symbols: Iterable[str],
    market_date: date,
    start: date,
) -> dict[str, Any]:
    """Validate the canonical file in bounded chunks through the NSE importer."""
    result: dict[str, Any] = {
        "exists": path.is_file(),
        "path": str(path),
        "total_rows": 0,
        "eq_rows": 0,
        "non_eq_rows": 0,
        "valid_rows": 0,
        "symbols": [],
        "target_date_symbols": [],
        "first_date": None,
        "last_date": None,
        "sessions": 0,
        "missing_sessions": [],
        "duplicate_rows": 0,
        "invalid_rows": 0,
        "invalid_ohlc_rows": 0,
        "nonpositive_price_fields": 0,
        "invalid_volume_rows": 0,
        "malformed_dates": 0,
        "malformed_numeric_rows": 0,
        "validation_status": "NOT_RUN",
        "history_by_symbol": {},
        "errors": [],
    }
    if not path.is_file():
        result["validation_status"] = "FAIL"
        result["errors"].append(f"licensed equity dataset does not exist: {path}")
        return result

    importer = LicensedNseCmEodProvider(path.parent, _CANONICAL_SCHEMA, dataset_path=path)
    required_base_symbols = {_as_base_symbol(value) for value in required_symbols}
    seen_keys: set[tuple[str, date]] = set()
    seen_symbols: set[str] = set()
    target_symbols: set[str] = set()
    all_dates: set[date] = set()
    history: dict[str, list[dict[str, Any]]] = {}
    chunk: list[ParsedNseCmRow] = []
    chunk_size = 20_000
    first_date: date | None = None
    last_date: date | None = None
    parse_failed = False

    def consume(records: list[ParsedNseCmRow]) -> None:
        nonlocal first_date, last_date
        if not records:
            return
        batch = importer.normalize(records, date.min, date.max)
        result["total_rows"] += batch.source_row_count
        result["eq_rows"] += batch.eq_rows_seen
        result["non_eq_rows"] += batch.non_eq_rows_excluded
        result["duplicate_rows"] += batch.duplicate_count
        result["invalid_ohlc_rows"] += batch.invalid_ohlc_count
        result["nonpositive_price_fields"] += batch.nonpositive_price_count
        result["invalid_volume_rows"] += batch.negative_volume_count + batch.zero_volume_count
        result["malformed_dates"] += batch.malformed_date_count
        result["malformed_numeric_rows"] += batch.invalid_numeric_count
        result["invalid_rows"] += batch.quality_rejected_count + batch.zero_volume_count
        for rejected in batch.rejected_rows:
            if len(result["errors"]) < 20:
                result["errors"].append(
                    f"line {rejected['line_number']}: {rejected['reason']} ({rejected['detail']})"
                )
        for row in batch.rows:
            day = datetime.strptime(row["Date"], "%d-%b-%Y").date()
            symbol = row["Symbol"]
            key = (symbol, day)
            if key in seen_keys:
                result["duplicate_rows"] += 1
                result["invalid_rows"] += 1
                if len(result["errors"]) < 20:
                    result["errors"].append(f"duplicate symbol/date across audit chunks: {symbol} {day.isoformat()}")
                continue
            seen_keys.add(key)
            seen_symbols.add(symbol)
            all_dates.add(day)
            first_date = day if first_date is None else min(first_date, day)
            last_date = day if last_date is None else max(last_date, day)
            result["valid_rows"] += 1
            if day == market_date:
                target_symbols.add(symbol)
            if symbol in required_base_symbols and start <= day <= market_date:
                history.setdefault(symbol, []).append({
                    "timestamp": datetime.combine(day, datetime.min.time()),
                    "open": float(row["Open Price"]),
                    "high": float(row["High Price"]),
                    "low": float(row["Low Price"]),
                    "close": float(row["Close Price"]),
                    "volume": float(row["Total Traded Quantity"]),
                })

    try:
        with path.open("r", newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if tuple(reader.fieldnames or ()) != CANONICAL_COLUMNS:
                result["errors"].append(
                    f"canonical columns/order mismatch; expected {list(CANONICAL_COLUMNS)}, "
                    f"received {reader.fieldnames!r}"
                )
                result["validation_status"] = "FAIL"
                return result
            for line_number, raw in enumerate(reader, start=2):
                if raw is None or all(not str(value or "").strip() for value in raw.values() if value is not None):
                    continue
                if None in raw:
                    result["total_rows"] += 1
                    result["invalid_rows"] += 1
                    if len(result["errors"]) < 20:
                        result["errors"].append(f"line {line_number}: extra CSV fields")
                    continue
                chunk.append(ParsedNseCmRow(str(path.resolve()), path.name, line_number, raw))
                if len(chunk) >= chunk_size:
                    consume(chunk)
                    chunk = []
            consume(chunk)
    except (OSError, UnicodeError, csv.Error, ValueError) as error:
        parse_failed = True
        result["errors"].append(f"cannot validate canonical dataset: {type(error).__name__}: {error}")

    result["symbols"] = sorted(seen_symbols)
    result["target_date_symbols"] = sorted(target_symbols)
    result["first_date"] = first_date.isoformat() if first_date else None
    result["last_date"] = last_date.isoformat() if last_date else None
    result["sessions"] = len(all_dates)
    result["history_by_symbol"] = history
    result["_all_dates"] = all_dates
    result["_bars_by_symbol"] = history
    if not seen_symbols:
        result["errors"].append("canonical licensed dataset contains no valid EQ rows")
    if result["non_eq_rows"]:
        result["errors"].append(f"{result['non_eq_rows']} row(s) do not carry explicit EQ series")
    if result["invalid_rows"]:
        result["errors"].append(f"{result['invalid_rows']} malformed, duplicate, or invalid row(s)")
    result["validation_status"] = "FAIL" if parse_failed or result["errors"] else "PASS"
    return result


def _inspect_ledger(database: Path, market_date: date, output_root: Path) -> dict[str, Any]:
    report_path = output_root / market_date.isoformat() / "daily_report.json"
    report_exists = report_path.is_file()
    if not database.is_file():
        return {
            "readable": True,
            "duplicate": report_exists,
            "open_symbols": [],
            "detail": f"prior daily report exists: {report_path}" if report_exists else "ledger does not exist",
        }
    try:
        uri = f"{database.resolve().as_uri()}?mode=ro"
        with sqlite3.connect(uri, uri=True, timeout=2) as connection:
            tables = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            duplicate = report_exists
            open_symbols: list[str] = []
            if "daily_journal" in tables:
                duplicate = duplicate or connection.execute(
                    "SELECT 1 FROM daily_journal WHERE trading_date=? LIMIT 1",
                    (market_date.isoformat(),),
                ).fetchone() is not None
            if "positions" in tables:
                open_symbols = [
                    _as_base_symbol(row[0])
                    for row in connection.execute(
                        "SELECT DISTINCT symbol FROM positions WHERE status='OPEN'"
                    )
                ]
        detail = (f"prior daily report exists: {report_path}" if report_exists else
                  "date already exists in daily_journal" if duplicate else "no journal entry or daily report for date")
        return {"readable": True, "duplicate": duplicate, "open_symbols": open_symbols, "detail": detail}
    except (sqlite3.Error, OSError) as error:
        return {"readable": False, "duplicate": True, "open_symbols": [],
                "detail": f"cannot inspect ledger read-only: {type(error).__name__}: {error}"}


def run_preflight(
    *,
    config: AppConfig,
    root: str | Path,
    market_date: date,
    symbols: Iterable[str],
    dataset_path: str | Path,
    index_data_dir: str | Path,
    database: str | Path,
    baseline_manifest: str | Path,
    paper_mode_enabled: bool,
    live_mode_disabled: bool,
    provider_name: str,
    output_root: str | Path,
    daily_data_dir: str | Path | None = None,
    identity_file: str | Path | None = None,
) -> PreflightResult:
    """Check readiness without instantiating the engine or mutating its ledger."""
    project_root = Path(root).resolve()
    dataset = Path(dataset_path)
    dataset = dataset if dataset.is_absolute() else project_root / dataset
    daily_dir = Path(daily_data_dir or config.forward_paper.daily_data_dir)
    daily_dir = daily_dir if daily_dir.is_absolute() else project_root / daily_dir
    identity_path = Path(identity_file or config.forward_paper.identity_file)
    identity_path = identity_path if identity_path.is_absolute() else project_root / identity_path
    index_dir = Path(index_data_dir)
    index_dir = index_dir if index_dir.is_absolute() else project_root / index_dir
    ledger_path = Path(database)
    ledger_path = ledger_path if ledger_path.is_absolute() else project_root / ledger_path
    baseline = Path(baseline_manifest)
    baseline = baseline if baseline.is_absolute() else project_root / baseline
    output = Path(output_root)
    output = output if output.is_absolute() else project_root / output

    configured_symbols = list(dict.fromkeys(_as_base_symbol(value) for value in symbols if str(value).strip()))
    ledger = _inspect_ledger(ledger_path, market_date, output)
    required_symbols = list(dict.fromkeys([*configured_symbols, *ledger["open_symbols"]]))
    warmup_start = market_date - timedelta(days=config.backtest.warmup_calendar_days)
    nifty_path = index_dir / "index_NSEI.csv"
    vix_path = index_dir / "index_INDIAVIX.csv"
    nifty_bars, nifty = _read_index(
        nifty_path, config.universe.benchmark, warmup_start, market_date, market_date,
        config.universe.min_history_rows, config.forward_paper.max_daily_price_jump_fraction,
    )
    vix_bars, vix = _read_index(
        vix_path, config.regime.india_vix_symbol, warmup_start, market_date, market_date,
        config.universe.min_history_rows, config.forward_paper.max_daily_price_jump_fraction,
    )
    personal_local = provider_name == "personal_local_eod"
    if personal_local:
        equity = _audit_personal_local_equities(
            cache_dir=dataset,
            daily_data_dir=daily_dir,
            identity_file=identity_path,
            required_symbols=required_symbols,
            market_date=market_date,
            start=warmup_start,
            index_dir=index_dir,
            max_daily_price_jump_fraction=config.forward_paper.max_daily_price_jump_fraction,
            min_history_rows=config.universe.min_history_rows,
        )
        required_set = {_as_base_symbol(value) for value in required_symbols}
        all_equity_symbols = set(equity["symbols"])
        target_symbols = set(equity["target_date_symbols"])
        missing_anywhere = equity.get("missing_anywhere", sorted(required_set - all_equity_symbols))
        missing_on_date = equity.get("missing_on_date", sorted(required_set - target_symbols))
        per_symbol_failures = list(equity.get("per_symbol_failures", []))
    else:
        equity = _audit_equity_dataset(dataset, required_symbols, market_date, warmup_start)
        required_set = {_as_base_symbol(value) for value in required_symbols}
        all_equity_symbols = set(equity["symbols"])
        target_symbols = set(equity["target_date_symbols"])
        missing_anywhere = sorted(required_set - all_equity_symbols)
        missing_on_date = sorted(required_set - target_symbols)
        per_symbol_failures = []
        history: dict[str, list[dict[str, Any]]] = equity.get("_bars_by_symbol", {})
        for symbol in required_symbols:
            rows = history.get(_as_base_symbol(symbol), [])
            frame = pd.DataFrame(rows)
            if not frame.empty:
                frame = frame.set_index("timestamp")[[*REQUIRED_COLUMNS]]
            _, quality = validate_daily_bars(
                frame,
                provider="licensed_nse_cm_eod",
                symbol=symbol,
                market_date=market_date,
                max_daily_price_jump_fraction=config.forward_paper.max_daily_price_jump_fraction,
            )
            failures = []
            if not quality.valid:
                failures.append(quality.reason)
            if len(frame) < config.universe.min_history_rows:
                failures.append(
                    f"only {len(frame)} valid warm-up rows; {config.universe.min_history_rows} required"
                )
            if failures:
                per_symbol_failures.append(f"{symbol}: {'; '.join(failures)}")

    vix_fallback = bool(
        not vix.get("valid") and vix.get("freshness_status") == "DATA_STALE"
    )
    index_date_ok = bool(nifty.get("valid") and (vix.get("valid") or vix_fallback))
    equity_dates_ok = not missing_on_date and not per_symbol_failures
    required_dates_ok = index_date_ok and equity_dates_ok
    expected_present = sorted(required_set & all_equity_symbols)
    expected_sessions = {
        pd.Timestamp(stamp).date()
        for stamp in nifty_bars.index
        if pd.notna(stamp)
    }
    observed_sessions = set(equity.get("_all_dates", set()))
    actual_first = date.fromisoformat(equity["first_date"]) if equity.get("first_date") else None
    actual_last = date.fromisoformat(equity["last_date"]) if equity.get("last_date") else None
    missing_sessions = sorted(
        day for day in expected_sessions
        if actual_first is not None and actual_last is not None
        and actual_first <= day <= actual_last and day not in observed_sessions
    )

    checks: dict[str, dict[str, Any]] = {}
    experiment_start = (
        date.fromisoformat(config.forward_paper.experiment_start_date)
        if config.forward_paper.experiment_start_date else None
    )
    experiment_end = (
        date.fromisoformat(config.forward_paper.experiment_end_date)
        if config.forward_paper.experiment_end_date else None
    )
    in_experiment_window = (
        (experiment_start is None or market_date >= experiment_start)
        and (experiment_end is None or market_date <= experiment_end)
    )
    _check(
        checks,
        "inside_configured_experiment_window",
        in_experiment_window,
        f"configured {experiment_start.isoformat() if experiment_start else 'unbounded'} through "
        f"{experiment_end.isoformat() if experiment_end else 'unbounded'}; requested {market_date.isoformat()}",
    )
    if personal_local:
        _check(checks, "personal_equity_source_exists", dataset.is_dir(), str(dataset))
        _check(checks, "explicit_eq_identity_file", equity.get("identity_series") == "PASS", str(identity_path))
        _check(
            checks,
            "off_calendar_equity_rows_excluded",
            True,
            f"excluded {equity.get('excluded_non_nifty_session_rows', 0)} equity row(s) outside official NIFTY sessions on "
            f"{equity.get('excluded_non_nifty_session_dates', [])}",
        )
    else:
        _check(checks, "licensed_equity_dataset_exists", dataset.is_file(), str(dataset))
    _check(checks, "official_nifty_file_exists", nifty.get("exists", False), str(nifty_path))
    _check(checks, "official_india_vix_file_exists", vix.get("exists", False) or vix_fallback,
           str(vix_path) + (" (realized-volatility fallback enabled)" if vix_fallback else ""))
    _check(checks, "india_vix_exact_date_or_nifty_fallback", bool(vix.get("valid") or vix_fallback),
           "official India VIX is current" if vix.get("valid") else (
               "India VIX unavailable/stale; use existing causal NIFTY realized_vol20 fallback" if vix_fallback
               else str(vix.get("reason") or "India VIX invalid")
           ))
    _check(
        checks,
        "required_dates_available",
        required_dates_ok,
        f"market date {market_date.isoformat()}, warm-up from {warmup_start.isoformat()}; "
        f"NIFTY={nifty.get('reason') or 'valid'}, India VIX={vix.get('reason') or ('realized_vol20 fallback' if vix_fallback else 'valid')}"
        + (("; local equity cache is missing, so equity dates cannot be checked" if personal_local else
            "; licensed equity dataset is missing, so equity dates cannot be checked")
           if not (dataset.is_dir() if personal_local else dataset.is_file()) else "")
        + (f"; equity gaps: {', '.join(per_symbol_failures[:8])}" if per_symbol_failures else ""),
    )
    _check(
        checks,
        "required_symbols_available",
        (dataset.is_dir() if personal_local else dataset.is_file()) and not missing_anywhere and not missing_on_date,
        f"required={len(required_set)}, present_anywhere={len(expected_present)}, "
        f"present_on_market_date={len(required_set - set(missing_on_date))}; "
        f"missing_anywhere={missing_anywhere[:20]}, missing_on_date={missing_on_date[:20]}",
    )
    equity_source_exists = dataset.is_dir() if personal_local else dataset.is_file()
    data_pass = equity_source_exists and equity["validation_status"] == "PASS" and index_date_ok and not per_symbol_failures
    data_detail = equity["validation_status"]
    if equity.get("errors"):
        data_detail += "; " + "; ".join(equity["errors"][:8])
    if not index_date_ok:
        data_detail += f"; NIFTY: {nifty.get('reason') or 'valid'}, India VIX: {vix.get('reason') or 'valid'}"
    _check(checks, "data_passes_validation", data_pass, data_detail)
    _check(
        checks,
        "paper_mode_enabled",
        bool(paper_mode_enabled and config.paper.enabled),
        f"PAPER_TRADING_ENABLED={str(bool(paper_mode_enabled)).lower()}, config.paper.enabled={str(config.paper.enabled).lower()}",
    )
    _check(
        checks,
        "live_mode_disabled",
        bool(live_mode_disabled and not config.live_trading.enabled),
        f"LIVE_TRADING_ENABLED disabled={str(bool(live_mode_disabled)).lower()}, config.live_trading.enabled={str(config.live_trading.enabled).lower()}",
    )
    broker_unconfigured = (
        provider_name == config.forward_paper.provider
        and provider_name in {"licensed_nse_cm_eod", "local_eod_csv", "personal_local_eod"}
    )
    _check(checks, "broker_provider_not_configured", broker_unconfigured,
           f"configured data provider={config.forward_paper.provider!r}; broker orders are not exposed")
    baseline_ok = baseline.is_file()
    baseline_detail = str(baseline)
    if baseline_ok:
        try:
            validate_baseline_manifest(
                config, root=project_root, path=baseline,
                baseline_name=config.forward_paper.baseline_name,
            )
        except Exception as error:
            baseline_ok = False
            baseline_detail = f"{baseline}: {type(error).__name__}: {error}"
    _check(checks, "baseline_manifest_exists_and_matches", baseline_ok, baseline_detail)
    _check(checks, "no_duplicate_forward_run", ledger["readable"] and not ledger["duplicate"], ledger["detail"])

    reasons = tuple(
        f"{name}: {check['detail'] or 'check failed'}"
        for name, check in checks.items()
        if not check["passed"]
    )
    summary = {
        "first_date": equity.get("first_date"),
        "last_date": equity.get("last_date"),
        "number_of_sessions": equity.get("sessions", 0),
        "number_of_symbols": len(equity.get("symbols", [])),
        "total_rows": equity.get("total_rows", 0),
        "eq_rows": equity.get("eq_rows", 0),
        "non_eq_rows": equity.get("non_eq_rows", 0),
        "missing_sessions": [day.isoformat() for day in missing_sessions],
        "missing_symbols": missing_anywhere,
        "duplicate_rows": equity.get("duplicate_rows", 0),
        "invalid_rows": equity.get("invalid_rows", 0),
        "invalid_ohlc_rows": equity.get("invalid_ohlc_rows", 0),
        "excluded_non_nifty_session_rows": equity.get("excluded_non_nifty_session_rows", 0),
        "excluded_non_nifty_session_rows_by_symbol": equity.get("excluded_non_nifty_session_rows_by_symbol", {}),
        "excluded_non_nifty_session_dates": equity.get("excluded_non_nifty_session_dates", []),
        "validation_status": equity.get("validation_status", "FAIL"),
        "required_symbols": len(required_set),
        "required_symbols_present_on_market_date": len(required_set - set(missing_on_date)),
        "nifty": nifty,
        "india_vix": vix,
        "india_vix_fallback": vix_fallback,
    }
    for key in ("_all_dates", "_bars_by_symbol", "history_by_symbol"):
        equity.pop(key, None)
    missing_current_equity = personal_local and bool(equity.get("missing_daily_current"))
    status = "PHASE3D_READY" if not reasons else (
        "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA" if missing_current_equity else "PHASE3D_FORWARD_BLOCKED"
    )
    return PreflightResult(status, market_date.isoformat(), checks, reasons, summary)
