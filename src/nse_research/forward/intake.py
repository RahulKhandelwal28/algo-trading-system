"""Fail-closed, local-only daily equity EOD intake for personal paper trading."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import tempfile
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping

DAILY_COLUMNS = ("date", "symbol", "series", "open", "high", "low", "close", "volume")
_NUMBER_COLUMNS = ("open", "high", "low", "close", "volume")
_FILE_NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv$")


class DailyEodIntakeError(ValueError):
    """The supplied dated file cannot be safely imported."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _atomic_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=DAILY_COLUMNS, lineterminator="\n", extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
        # A no-clobber link closes the existence-check race between concurrent
        # import attempts for the same market date.
        os.link(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def expected_eq_symbols(path: Path) -> set[str]:
    """Read only explicitly EQ identities from the configured universe file."""
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise DailyEodIntakeError(f"universe file has no header: {path}")
        normalized_names = [str(name).strip().casefold() for name in reader.fieldnames]
        if len(normalized_names) != len(set(normalized_names)):
            raise DailyEodIntakeError("universe file contains duplicate column names")
        columns = {str(name).strip().casefold(): name for name in reader.fieldnames}
        symbol_key = columns.get("symbol") or columns.get("ticker")
        series_key = columns.get("series")
        if not symbol_key or not series_key:
            raise DailyEodIntakeError("universe file must explicitly contain Symbol and Series columns")
        result: set[str] = set()
        for row in reader:
            symbol = str(row.get(symbol_key) or "").strip().upper()
            series = str(row.get(series_key) or "").strip().upper()
            if symbol and not symbol.startswith("DUMMY") and series == "EQ":
                result.add(symbol.removesuffix(".NS"))
    if not result:
        raise DailyEodIntakeError("universe file contains no explicit EQ symbols")
    return result


def _parse_number(raw: str, field: str) -> Decimal:
    source = raw.strip()
    if "," in source and not re.fullmatch(r"[+-]?\d{1,3}(?:,\d{3})+(?:\.\d+)?", source):
        raise ValueError(f"{field} has malformed thousands separators: {raw!r}")
    text = source.replace(",", "")
    if not text:
        raise ValueError(f"{field} is empty")
    try:
        number = Decimal(text)
    except InvalidOperation as error:
        raise ValueError(f"{field} is malformed: {raw!r}") from error
    if not number.is_finite():
        raise ValueError(f"{field} is not finite: {raw!r}")
    if field in {"open", "high", "low", "close"} and number <= 0:
        raise ValueError(f"{field} must be positive")
    if field == "volume" and (number < 0 or number != number.to_integral_value()):
        raise ValueError("volume must be a non-negative integer")
    return number


def import_daily_eod(
    *,
    source_path: Path,
    expected_symbols: Iterable[str],
    normalized_dir: Path,
    manifest_dir: Path,
    source_provider: str | None = None,
    source_identifier: str | None = None,
    adjustment_status: str | None = None,
) -> dict[str, Any]:
    """Validate one ``YYYY-MM-DD.csv`` and publish only a complete EQ session.

    Original input files are read-only. A failed import writes an audit manifest
    but never publishes a normalized date file. Duplicate keys are never
    deduplicated.
    """
    source_path = Path(source_path).resolve()
    match = _FILE_NAME.fullmatch(source_path.name)
    if not match:
        raise DailyEodIntakeError("daily source filename must be exactly YYYY-MM-DD.csv")
    try:
        requested_date = date.fromisoformat(match.group(1))
    except ValueError as error:
        raise DailyEodIntakeError(f"invalid date in daily source filename: {match.group(1)}") from error
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    expected = {str(symbol).strip().upper().removesuffix(".NS") for symbol in expected_symbols if str(symbol).strip()}
    if not expected:
        raise DailyEodIntakeError("expected symbol universe is empty")

    result: dict[str, Any] = {
        "source_filename": source_path.name,
        "source_sha256": _sha256(source_path),
        "source_file_size_bytes": source_path.stat().st_size,
        "source_date": requested_date.isoformat(),
        "source_provider": source_provider,
        "source_identifier": source_identifier,
        "adjustment_status": adjustment_status,
        "total_rows": 0,
        "symbol_count": 0,
        "EQ_row_count": 0,
        "non_EQ_row_count": 0,
        "observed_series_values": [],
        "duplicate_key_count": 0,
        "duplicate_row_count": 0,
        "conflicting_duplicate_count": 0,
        "malformed_date_rows": 0,
        "invalid_rows": 0,
        "missing_symbols": [],
        "unknown_symbols": [],
        "validation_status": "FAIL",
        "normalized_file": None,
        "errors": [],
        "imported_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    normalized_path = Path(normalized_dir) / f"{requested_date.isoformat()}.csv"
    manifest_path = Path(manifest_dir) / f"{requested_date.isoformat()}.json"

    headers: list[str] = []
    parsed_rows: list[dict[str, Any]] = []
    series_counts: Counter[str] = Counter()
    malformed_dates: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    symbols_seen: set[str] = set()
    eq_symbols: set[str] = set()

    try:
        with source_path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            raw_headers = reader.fieldnames or []
            headers = [str(header).strip() for header in raw_headers]
            normalized_headers = [header.casefold() for header in headers]
            if not headers or len(normalized_headers) != len(set(normalized_headers)):
                raise DailyEodIntakeError("source has an empty header or duplicate column names")
            column_by_name = {
                str(header).strip().casefold(): str(header)
                for header in raw_headers
            }
            missing = [name for name in DAILY_COLUMNS if name not in column_by_name]
            if missing:
                raise DailyEodIntakeError(f"daily source is missing required normalized columns: {missing}")
            for line_number, raw in enumerate(reader, start=2):
                if raw is None or all(not str(value or "").strip() for value in raw.values() if value is not None):
                    continue
                result["total_rows"] += 1
                if None in raw:
                    result["errors"].append(f"line {line_number}: row contains extra fields")
                    result["invalid_rows"] += 1
                    continue
                values = {
                    target: str(raw.get(column_by_name[target]) or "").strip()
                    for target in DAILY_COLUMNS
                }
                symbol = values["symbol"].upper().removesuffix(".NS")
                series = values["series"].upper()
                values["symbol"] = symbol
                values["series"] = series
                if symbol:
                    symbols_seen.add(symbol)
                series_counts[series or "(empty)"] += 1
                row_invalid = False
                if not symbol:
                    invalid_rows.append({"line_number": line_number, "field": "symbol", "reason": "symbol is empty"})
                    row_invalid = True
                if not series:
                    invalid_rows.append({"line_number": line_number, "field": "series", "reason": "series is empty; EQ must be explicit"})
                    row_invalid = True
                try:
                    day = date.fromisoformat(values["date"])
                    if day.isoformat() != values["date"]:
                        raise ValueError("date must be strict YYYY-MM-DD")
                except ValueError as error:
                    result["malformed_date_rows"] += 1
                    malformed_dates.append({"line_number": line_number, "date": values["date"], "symbol": symbol, "reason": str(error)})
                    day = None
                if day is not None and symbol:
                    groups[(day.isoformat(), symbol)].append({"line": line_number, "values": dict(values)})
                if series == "EQ":
                    result["EQ_row_count"] += 1
                    if symbol:
                        eq_symbols.add(symbol)
                else:
                    result["non_EQ_row_count"] += 1
                numeric: dict[str, Decimal] = {}
                for field in _NUMBER_COLUMNS:
                    try:
                        numeric[field] = _parse_number(values[field], field)
                    except ValueError as error:
                        invalid_rows.append({"line_number": line_number, "symbol": symbol, "series": series,
                                             "field": field, "raw_value": values[field], "reason": str(error)})
                        row_invalid = True
                if all(field in numeric for field in ("open", "high", "low", "close")):
                    low, high = numeric["low"], numeric["high"]
                    if low > high or not low <= numeric["open"] <= high or not low <= numeric["close"] <= high:
                        invalid_rows.append({"line_number": line_number, "symbol": symbol, "series": series,
                                             "field": "OHLC", "raw_value": ";".join(values[k] for k in ("open", "high", "low", "close")),
                                             "reason": "low <= open/close <= high relationship failed"})
                        row_invalid = True
                if row_invalid:
                    result["invalid_rows"] += 1
                    continue
                if day is not None and series == "EQ" and symbol:
                    parsed_rows.append({
                        "date": day.isoformat(), "symbol": symbol, "series": "EQ",
                        **{field: format(numeric[field], "f") for field in _NUMBER_COLUMNS},
                    })
    except (OSError, UnicodeError, csv.Error, DailyEodIntakeError) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")

    duplicate_keys = duplicate_rows = conflicting = 0
    for key, items in groups.items():
        if len(items) < 2:
            continue
        duplicate_keys += 1
        duplicate_rows += len(items) - 1
        signatures = {json.dumps(item["values"], sort_keys=True) for item in items}
        conflicting += int(len(signatures) > 1)
    unknown = sorted(symbols_seen - expected)
    missing_symbols = sorted(expected - eq_symbols)
    result.update({
        "symbol_count": len(symbols_seen),
        "observed_series_values": sorted(series_counts),
        "duplicate_key_count": duplicate_keys,
        "duplicate_row_count": duplicate_rows,
        "conflicting_duplicate_count": conflicting,
        "missing_symbols": missing_symbols,
        "unknown_symbols": unknown,
        "source_headers": headers,
        "normalized_file": str(normalized_path),
        "malformed_dates": malformed_dates,
        "invalid_record_details": invalid_rows,
    })
    if not result["total_rows"]:
        result["errors"].append("daily source contains no data rows")
    if result["malformed_date_rows"]:
        result["errors"].append(f"{result['malformed_date_rows']} malformed date row(s)")
    if duplicate_keys:
        result["errors"].append(f"{duplicate_keys} duplicate (date, symbol) key(s); no rows were deduplicated")
    if result["invalid_rows"]:
        result["errors"].append(f"{result['invalid_rows']} row(s) have invalid OHLCV")
        result["errors"].extend(
            f"line {item.get('line_number')}: {item.get('field')} - {item.get('reason')}"
            for item in invalid_rows[:8]
        )
    if unknown:
        result["errors"].append(f"unknown symbol identities: {unknown[:20]}")
    if missing_symbols:
        result["errors"].append(f"missing explicit EQ rows for required symbols: {missing_symbols[:20]}")
    for group in groups:
        if group[0] != requested_date.isoformat():
            result["errors"].append(f"source row date {group[0]} does not match filename date {requested_date.isoformat()}")
            break

    result["validation_status"] = "PASS" if not result["errors"] else "FAIL"
    if result["validation_status"] == "PASS":
        if normalized_path.exists():
            old_manifest = None
            if manifest_path.is_file():
                try:
                    old_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    old_manifest = None
            if not old_manifest or old_manifest.get("source_sha256") != result["source_sha256"]:
                result["validation_status"] = "FAIL"
                result["errors"].append("normalized date already exists for a different or unverified source; refusing overwrite")
            else:
                result["normalization_status"] = "ALREADY_IMPORTED_IDENTICAL_SOURCE"
                result["normalized_sha256"] = _sha256(normalized_path)
        else:
            _atomic_csv(normalized_path, parsed_rows)
            result["normalized_sha256"] = _sha256(normalized_path)
            result["normalization_status"] = "WRITTEN"
    _atomic_text(manifest_path, json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate and normalize one local paper-trading EOD file")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--source", type=Path, required=True, help="incoming YYYY-MM-DD.csv; source is never modified")
    parser.add_argument("--universe", type=Path, help="explicit EQ universe CSV")
    parser.add_argument("--source-provider", help="provider label only when actually supplied")
    parser.add_argument("--source-identifier", help="batch/reference only when actually supplied")
    parser.add_argument("--adjustment-status", help="adjustment statement only when explicitly documented")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    incoming = root / "data" / "paper_trading" / "incoming"
    source = args.source.resolve()
    try:
        source.relative_to(incoming.resolve())
    except ValueError:
        print("DATA_VALIDATION_FAILED: source must be inside data/paper_trading/incoming/")
        return 2
    match = _FILE_NAME.fullmatch(source.name)
    if not match:
        print("DATA_VALIDATION_FAILED: daily source filename must be exactly YYYY-MM-DD.csv")
        return 2
    if not source.is_file():
        print(f"FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA: {source}")
        return 2
    universe = args.universe or root / "data" / "paper_trading" / "phase3d_personal_universe.csv"
    universe = universe if universe.is_absolute() else root / universe
    try:
        symbols = expected_eq_symbols(universe)
        result = import_daily_eod(
            source_path=source,
            expected_symbols=symbols,
            normalized_dir=root / "data" / "paper_trading" / "normalized",
            manifest_dir=root / "data" / "paper_trading" / "manifests",
            source_provider=args.source_provider,
            source_identifier=args.source_identifier,
            adjustment_status=args.adjustment_status,
        )
    except Exception as error:
        print(f"DATA_VALIDATION_FAILED: {type(error).__name__}: {error}")
        return 1
    print(f"DAILY_EOD_{result['validation_status']}: date={result['source_date']} rows={result['total_rows']} EQ={result['EQ_row_count']} symbols={result['symbol_count']}")
    for error in result["errors"]:
        print(f"- {error}")
    return 0 if result["validation_status"] == "PASS" else 1


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
