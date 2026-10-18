"""Local, authorization-gated audit for a supplied NSE CM EOD sample.

This module never contacts NSE and is deliberately separate from the Phase 3D
provider. Source headers, date formats, and numeric separators must be entered
explicitly in the source mapping manifest. No aliases or session dates are
inferred.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import tempfile
import zipfile
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

NORMALIZATION_VERSION = "nse-cm-eod-sample-audit-v1"
TARGET_COLUMNS = (
    "date", "symbol", "series", "isin", "name", "open", "high", "low",
    "close", "prev_close", "volume", "turnover", "trades",
)
REQUIRED_FIELDS = ("date", "symbol", "series", "open", "high", "low", "close", "volume")
OPTIONAL_FIELDS = tuple(field for field in TARGET_COLUMNS if field not in REQUIRED_FIELDS)
VALID_GATE_STATES = {"PASS", "PASS_WITH_WARNINGS", "FAIL", "NOT_ASSESSABLE"}
AUDIT_CSV_HEADERS: dict[str, tuple[str, ...]] = {
    "invalid_ohlcv.csv": (
        "source_file", "source_member", "line_number", "date", "symbol", "series",
        "field", "raw_value", "reason",
    ),
    "duplicate_records.csv": (
        "source_file", "source_member", "line_number", "date", "symbol", "series",
        "duplicate_key_count", "duplicate_row_number", "conflicting_duplicate",
    ),
    "malformed_dates.csv": (
        "source_file", "source_member", "line_number", "date_raw", "symbol", "series", "reason",
    ),
    "symbol_isin_anomalies.csv": ("anomaly_type", "symbol", "isin", "related_values", "row_count"),
    "extreme_returns.csv": (
        "symbol", "date", "previous_observed_date", "observed_day_gap", "close",
        "previous_close", "return_pct", "threshold_exceeded",
    ),
    "coverage_comparison.csv": ("date", "source", "present", "classification", "evidence"),
}


class SampleAuditError(ValueError):
    """Invalid sample-audit configuration or source structure."""


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _atomic_csv(path: Path, headers: tuple[str, ...], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=headers, extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _container_format(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".csv.zip"):
        return "csv.zip"
    if name.endswith(".csv.gz"):
        return "csv.gz"
    if name.endswith(".csv"):
        return "csv"
    if path.suffix.lower() in {".dat", ".bin", ".bhav"}:
        raise SampleAuditError("BINARY_FORMAT_REQUIRES_SPECIFICATION")
    raise SampleAuditError(f"unsupported sample format {path.name!r}; CSV, CSV.GZ, or CSV.ZIP is required")


def _csv_members(path: Path) -> Iterator[tuple[str, io.TextIOBase]]:
    kind = _container_format(path)
    if kind == "csv.zip":
        try:
            archive = zipfile.ZipFile(path)
        except (OSError, zipfile.BadZipFile) as error:
            raise SampleAuditError(f"invalid CSV ZIP: {error}") from error
        with archive:
            members = sorted(
                name for name in archive.namelist()
                if name.lower().endswith(".csv") and not name.endswith("/")
            )
            if not members:
                raise SampleAuditError("ZIP archive contains no CSV member")
            for name in members:
                with archive.open(name) as binary:
                    with io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as stream:
                        yield name, stream
        return
    if kind == "csv.gz":
        with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
            yield path.name, stream
        return
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        yield path.name, stream


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SampleAuditError(f"cannot read JSON manifest {path}: {error}") from error
    if not isinstance(value, dict):
        raise SampleAuditError(f"manifest must contain a JSON object: {path}")
    return value


def _gate(value: str) -> str:
    if value not in VALID_GATE_STATES:
        raise ValueError(f"invalid gate state {value!r}")
    return value


def _authorization_status(manifest: Mapping[str, Any]) -> tuple[str, str]:
    status = str(manifest.get("authorization_status") or "NOT_PROVIDED").strip().upper()
    reference = manifest.get("authorization_reference")
    uses = set(manifest.get("permitted_uses") or [])
    required_uses = {
        "quantitative_research", "historical_backtesting", "walk_forward_research",
        "paper_trading_simulation", "internal_analytical_use", "local_dataset_retention",
    }
    fully_scoped = (
        status == "CONFIRMED"
        and isinstance(reference, str) and bool(reference.strip())
        and required_uses.issubset(uses)
        and manifest.get("local_storage_permitted") is True
        and manifest.get("backtesting_permitted") is True
        and manifest.get("paper_trading_simulation_permitted") is True
    )
    if fully_scoped:
        return "PASS", "CONFIRMED"
    if status == "NOT_PROVIDED" or not reference:
        return "FAIL", "NOT_PROVIDED"
    return "FAIL", "PARTIAL_OR_SCOPE_NOT_CONFIRMED"


def _mapping_errors(mapping: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    if mapping.get("target_schema") not in (None, list(TARGET_COLUMNS)):
        errors.append("target_schema does not match the required 13-field normalized schema")
    if mapping.get("required_fields") not in (None, list(REQUIRED_FIELDS)):
        errors.append("required_fields does not match the required schema contract")
    if mapping.get("optional_fields") not in (None, list(OPTIONAL_FIELDS)):
        errors.append("optional_fields does not match the required schema contract")
    columns = mapping.get("source_columns")
    if not isinstance(columns, dict):
        return ["source_columns must explicitly map target fields to exact source headers"]
    for field in REQUIRED_FIELDS:
        if not isinstance(columns.get(field), str) or not columns[field].strip():
            errors.append(f"required source column mapping is missing for {field}")
    absent = set(mapping.get("optional_fields_confirmed_absent") or [])
    if not absent.issubset(set(OPTIONAL_FIELDS)):
        errors.append("optional_fields_confirmed_absent contains a non-optional or unknown target field")
    for field in OPTIONAL_FIELDS:
        mapped = columns.get(field)
        if mapped is None and field not in absent:
            errors.append(f"optional field {field} must be mapped or explicitly confirmed absent")
        elif mapped is not None and (not isinstance(mapped, str) or not mapped.strip()):
            errors.append(f"optional source column mapping for {field} must be a header or null")
        elif mapped is not None and field in absent:
            errors.append(f"optional field {field} cannot be mapped and confirmed absent at the same time")
    mapped_headers = [value for value in columns.values() if isinstance(value, str) and value.strip()]
    if len(mapped_headers) != len(set(mapped_headers)):
        errors.append("each target field must map to a distinct source header")
    formats = mapping.get("date_formats")
    if not isinstance(formats, list) or not formats or not all(isinstance(value, str) and value for value in formats):
        errors.append("date_formats must list the exact supported source formats")
    decimal = mapping.get("decimal_separator")
    thousands = mapping.get("thousands_separator")
    if not isinstance(decimal, str) or len(decimal) != 1:
        errors.append("decimal_separator must be explicitly set to one character")
    if thousands is not None and (not isinstance(thousands, str) or len(thousands) != 1):
        errors.append("thousands_separator must be null or one character")
    if decimal == thousands and thousands is not None:
        errors.append("decimal_separator and thousands_separator must differ")
    if not mapping.get("source_format"):
        errors.append("source_format must be documented explicitly")
    return errors


def _parse_date(value: object, formats: Iterable[str]) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("date is empty")
    for source_format in formats:
        try:
            parsed = datetime.strptime(text, source_format)
            if parsed.strftime(source_format) != text:
                continue
            return parsed.date()
        except ValueError:
            pass
    raise ValueError(f"date does not match declared formats: {text!r}")


def _parse_number(value: object, thousands: str | None, decimal: str) -> Decimal:
    text = str(value or "").strip()
    if not text or text.count(decimal) > 1:
        raise ValueError(f"malformed numeric value {text!r}")
    integer, has_decimal, fraction = text.partition(decimal)
    if thousands and thousands in fraction:
        raise ValueError(f"grouping separator after decimal point in {text!r}")
    sign = ""
    if integer.startswith(("+", "-")):
        sign, integer = integer[0], integer[1:]
    if thousands and thousands in integer:
        groups = integer.split(thousands)
        if not groups[0].isdigit() or not 1 <= len(groups[0]) <= 3 or any(
            not group.isdigit() or len(group) != 3 for group in groups[1:]
        ):
            raise ValueError(f"malformed grouping in {text!r}")
        integer = "".join(groups)
    elif not integer.isdigit():
        raise ValueError(f"malformed numeric value {text!r}")
    if has_decimal and (not fraction or not fraction.isdigit()):
        raise ValueError(f"malformed numeric value {text!r}")
    try:
        value_decimal = Decimal(sign + integer + (f".{fraction}" if has_decimal else ""))
    except InvalidOperation as error:
        raise ValueError(f"malformed numeric value {text!r}") from error
    if not value_decimal.is_finite():
        raise ValueError(f"non-finite numeric value {text!r}")
    return value_decimal


def _numeric_text(value: Decimal) -> str:
    return format(value, "f")


def _read_reference_dates(path: Path, column: str = "Date") -> set[date]:
    if not path.is_file():
        return set()
    result: set[date] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or column not in reader.fieldnames:
            raise SampleAuditError(f"reference file {path} is missing {column!r}")
        for line, row in enumerate(reader, start=2):
            value = str(row.get(column) or "").strip()
            try:
                parsed = date.fromisoformat(value)
            except ValueError as error:
                raise SampleAuditError(f"reference date malformed at {path}:{line}: {value!r}") from error
            if parsed.isoformat() != value:
                raise SampleAuditError(f"reference date is not strict ISO at {path}:{line}: {value!r}")
            result.add(parsed)
    return result


def initial_sample_validation(authorization_manifest: Mapping[str, Any]) -> dict[str, Any]:
    auth_state, auth_detail = _authorization_status(authorization_manifest)
    if auth_state != "PASS":
        acceptance = "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED"
    else:
        acceptance = "NOT_ASSESSABLE"
    return {
        "authorization": auth_state,
        "authorization_status": auth_detail,
        "schema": "NOT_ASSESSABLE",
        "eq_series": "NOT_ASSESSABLE",
        "ohlcv": "NOT_ASSESSABLE",
        "duplicates": "NOT_ASSESSABLE",
        "dates": "NOT_ASSESSABLE",
        "coverage": "NOT_ASSESSABLE",
        "identity": "NOT_ASSESSABLE",
        "overall": "NOT_ASSESSABLE",
        "acceptance_status": acceptance,
        "sample_present": False,
        "source_file": None,
        "errors": ["No sample file was supplied; technical gates were not run."],
    }


def _provenance_payload(
    *,
    source_path: Path,
    authorization: Mapping[str, Any],
    mapping: Mapping[str, Any],
    source_format: str | None,
    source_period: Mapping[str, Any] | None,
    validation_result: str,
    project_version: str | None,
) -> dict[str, Any]:
    auth_is_confirmed = _authorization_status(authorization)[0] == "PASS"
    try:
        container = _container_format(source_path)
    except SampleAuditError:
        container = source_path.suffix.lower() or None
    payload = {
        "source_provider": authorization.get("provider") if auth_is_confirmed else None,
        "product": authorization.get("product") if auth_is_confirmed else None,
        "source_filename": source_path.name,
        "source_sha256": _sha256(source_path),
        "source_file_size_bytes": source_path.stat().st_size,
        "acquisition_date": authorization.get("acquisition_date") or authorization.get("delivery_date"),
        "coverage": dict(source_period) if source_period is not None else None,
        "source_format": source_format,
        "source_container": container,
        "source_version": mapping.get("source_version"),
        "source_identifier": mapping.get("source_identifier") or authorization.get("source_identifier"),
        "source_column_mapping": mapping.get("source_columns"),
        "authorization_reference": authorization.get("authorization_reference"),
        "normalization_version": NORMALIZATION_VERSION,
        "validation_result": validation_result,
        "git_commit": None,
        "software_version": project_version,
    }
    adjustment_status = mapping.get("adjustment_status")
    if adjustment_status is None:
        adjustment_status = authorization.get("adjustment_status")
    if adjustment_status is not None:
        payload["adjustment_status"] = adjustment_status
    return payload


def _write_empty_audit_csvs(audit_dir: Path) -> None:
    for name, headers in AUDIT_CSV_HEADERS.items():
        _atomic_csv(audit_dir / name, headers, [])


def audit_sample(
    *,
    source_path: Path | None,
    mapping_path: Path,
    authorization_path: Path,
    audit_dir: Path,
    normalized_path: Path,
    provenance_path: Path,
    nifty_path: Path,
    vix_path: Path,
    project_version: str | None = None,
) -> dict[str, Any]:
    """Audit one local sample. Normalization is committed only after all gates pass.

    Missing authorization allows a technical audit but never creates a
    normalized sample. No live Phase 3D path calls this function.
    """
    authorization = _load_json(authorization_path)
    mapping = _load_json(mapping_path)
    audit_dir.mkdir(parents=True, exist_ok=True)
    auth_gate, auth_status = _authorization_status(authorization)
    if source_path is None or not source_path.is_file():
        result = initial_sample_validation(authorization)
        result["source_file"] = str(source_path) if source_path else None
        _atomic_json(audit_dir / "sample_validation.json", result)
        _write_empty_audit_csvs(audit_dir)
        return result

    result: dict[str, Any] = {
        "authorization": auth_gate,
        "authorization_status": auth_status,
        "schema": "NOT_ASSESSABLE", "eq_series": "NOT_ASSESSABLE", "ohlcv": "NOT_ASSESSABLE",
        "duplicates": "NOT_ASSESSABLE", "dates": "NOT_ASSESSABLE", "coverage": "NOT_ASSESSABLE",
        "identity": "NOT_ASSESSABLE", "overall": "NOT_ASSESSABLE",
        "acceptance_status": "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED" if auth_gate != "PASS" else "NOT_ASSESSABLE",
        "sample_present": True, "source_file": source_path.name, "errors": [],
    }
    mapping_errors = _mapping_errors(mapping)
    try:
        actual_format = _container_format(source_path)
    except SampleAuditError as error:
        result["errors"].append(str(error))
        result["schema"] = "FAIL"
        result["acceptance_status"] = "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED" if auth_gate != "PASS" else "TECHNICAL_SAMPLE_FAILED"
        result["overall"] = "FAIL"
        _atomic_json(provenance_path, _provenance_payload(
            source_path=source_path, authorization=authorization, mapping=mapping, source_format=None,
            source_period=None, validation_result="FAIL", project_version=project_version,
        ))
        _atomic_json(audit_dir / "sample_validation.json", result)
        _write_empty_audit_csvs(audit_dir)
        return result
    if mapping.get("source_format") != actual_format:
        mapping_errors.append(f"mapped source_format {mapping.get('source_format')!r} does not match file format {actual_format!r}")
    if mapping_errors:
        result["schema"] = "FAIL"
        result["errors"].extend(mapping_errors)
        result["acceptance_status"] = "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED" if auth_gate != "PASS" else "TECHNICAL_SAMPLE_FAILED"
        result["overall"] = "FAIL"
        _atomic_json(provenance_path, _provenance_payload(
            source_path=source_path, authorization=authorization, mapping=mapping, source_format=actual_format,
            source_period=None, validation_result="FAIL", project_version=project_version,
        ))
        _atomic_json(audit_dir / "sample_validation.json", result)
        _write_empty_audit_csvs(audit_dir)
        return result

    source_columns: Mapping[str, str | None] = mapping["source_columns"]
    date_formats = mapping["date_formats"]
    thousands = mapping["thousands_separator"]
    decimal = mapping["decimal_separator"]
    invalid_rows: list[dict[str, Any]] = []
    malformed_dates: list[dict[str, Any]] = []
    row_entries: list[dict[str, Any]] = []
    source_rows = 0
    series_counts: Counter[str] = Counter()
    all_valid_dates: set[date] = set()
    eq_dates: set[date] = set()
    eq_symbols: set[str] = set()
    eq_rows = non_eq_rows = malformed_date_count = 0
    invalid_ohlcv_rows = 0
    seen_series_header = False
    member_rows: list[dict[str, Any]] = []
    # Each parsed entry stores every mapped source value so duplicate conflicts
    # are assessed without modifying or deduplicating source rows.
    duplicate_groups: dict[tuple[date, str, str], list[dict[str, Any]]] = defaultdict(list)
    symbol_isins: dict[str, set[str]] = defaultdict(set)
    isin_symbols: dict[str, set[str]] = defaultdict(set)
    rows_with_isin = rows_without_isin = 0
    good_close_rows: dict[str, list[tuple[date, Decimal]]] = defaultdict(list)

    try:
        for member, stream in _csv_members(source_path):
            reader = csv.DictReader(stream)
            headers = reader.fieldnames or []
            if not headers:
                raise SampleAuditError(f"CSV member {member!r} has no header")
            if len(headers) != len(set(headers)):
                raise SampleAuditError(f"CSV member {member!r} has duplicate headers")
            mapped_headers = {header for header in source_columns.values() if header is not None}
            missing_headers = sorted(mapped_headers - set(headers))
            if missing_headers:
                raise SampleAuditError(f"CSV member {member!r} is missing explicitly mapped headers {missing_headers}")
            if source_columns.get("series") not in headers:
                raise SampleAuditError("explicit source series column is required; EQ is never fabricated")
            seen_series_header = True
            for line_number, raw in enumerate(reader, start=2):
                if raw is None or all(not str(value or "").strip() for value in raw.values() if value is not None):
                    continue
                source_rows += 1
                if None in raw:
                    result["errors"].append(f"{member}:{line_number}: row has extra fields")
                    continue
                values = {field: (str(raw.get(header) or "").strip() if header else "")
                          for field, header in source_columns.items()}
                series = values.get("series", "")
                symbol = values.get("symbol", "")
                series_counts[series or "(empty)"] += 1
                is_eq = series == "EQ"
                if is_eq:
                    eq_rows += 1
                else:
                    non_eq_rows += 1
                trade_date: date | None
                try:
                    trade_date = _parse_date(values.get("date"), date_formats)
                    all_valid_dates.add(trade_date)
                except ValueError as error:
                    trade_date = None
                    malformed_date_count += 1
                    malformed_dates.append({
                        "source_file": source_path.name, "source_member": member, "line_number": line_number,
                        "date_raw": values.get("date", ""), "symbol": symbol, "series": series, "reason": str(error),
                    })
                entry = {
                    "source_file": source_path.name, "source_member": member, "line_number": line_number,
                    "date_obj": trade_date, "values": values, "raw_values": dict(raw),
                }
                if trade_date is not None and symbol and series:
                    duplicate_groups[(trade_date, symbol, series)].append(entry)
                if not is_eq:
                    continue
                if trade_date is not None:
                    eq_dates.add(trade_date)
                if not symbol:
                    invalid_rows.append({
                        "source_file": source_path.name, "source_member": member, "line_number": line_number,
                        "date": trade_date.isoformat() if trade_date else "", "symbol": symbol, "series": series,
                        "field": "symbol", "raw_value": symbol, "reason": "required symbol is empty",
                    })
                    continue
                member_rows.append(entry)
                eq_symbols.add(symbol)
                isin = values.get("isin", "")
                if isin:
                    rows_with_isin += 1
                    symbol_isins[symbol].add(isin)
                    isin_symbols[isin].add(symbol)
                else:
                    rows_without_isin += 1
                numeric: dict[str, Decimal] = {}
                row_invalid = False
                for field in ("open", "high", "low", "close", "volume", "prev_close", "turnover", "trades"):
                    raw_number = values.get(field, "")
                    if not raw_number:
                        if field in REQUIRED_FIELDS:
                            invalid_rows.append({
                                "source_file": source_path.name, "source_member": member, "line_number": line_number,
                                "date": trade_date.isoformat() if trade_date else "", "symbol": symbol, "series": series,
                                "field": field, "raw_value": "", "reason": "required numeric field is empty",
                            })
                            row_invalid = True
                        continue
                    try:
                        numeric[field] = _parse_number(raw_number, thousands, decimal)
                    except ValueError as error:
                        invalid_rows.append({
                            "source_file": source_path.name, "source_member": member, "line_number": line_number,
                            "date": trade_date.isoformat() if trade_date else "", "symbol": symbol, "series": series,
                            "field": field, "raw_value": raw_number, "reason": str(error),
                        })
                        row_invalid = True
                for field in ("open", "high", "low", "close"):
                    if field in numeric and numeric[field] <= 0:
                        invalid_rows.append({
                            "source_file": source_path.name, "source_member": member, "line_number": line_number,
                            "date": trade_date.isoformat() if trade_date else "", "symbol": symbol, "series": series,
                            "field": field, "raw_value": values[field], "reason": "price must be positive",
                        })
                        row_invalid = True
                if "volume" in numeric and numeric["volume"] < 0:
                    invalid_rows.append({
                        "source_file": source_path.name, "source_member": member, "line_number": line_number,
                        "date": trade_date.isoformat() if trade_date else "", "symbol": symbol, "series": series,
                        "field": "volume", "raw_value": values["volume"], "reason": "volume must be non-negative",
                    })
                    row_invalid = True
                for field in ("turnover", "trades"):
                    if field in numeric and numeric[field] < 0:
                        invalid_rows.append({
                            "source_file": source_path.name, "source_member": member, "line_number": line_number,
                            "date": trade_date.isoformat() if trade_date else "", "symbol": symbol, "series": series,
                            "field": field, "raw_value": values[field], "reason": f"{field} must be non-negative",
                        })
                        row_invalid = True
                if all(field in numeric for field in ("open", "high", "low", "close")):
                    low, high = numeric["low"], numeric["high"]
                    if low > high or not (low - Decimal("1e-9") <= numeric["open"] <= high + Decimal("1e-9")) or not (
                        low - Decimal("1e-9") <= numeric["close"] <= high + Decimal("1e-9")
                    ):
                        invalid_rows.append({
                            "source_file": source_path.name, "source_member": member, "line_number": line_number,
                            "date": trade_date.isoformat() if trade_date else "", "symbol": symbol, "series": series,
                            "field": "OHLC", "raw_value": ";".join(values.get(key, "") for key in ("open", "high", "low", "close")),
                            "reason": "low <= open/close <= high relationship failed (tolerance=1e-9)",
                        })
                        row_invalid = True
                if row_invalid:
                    invalid_ohlcv_rows += 1
                    continue
                if trade_date is None:
                    continue
                good_close_rows[symbol].append((trade_date, numeric["close"]))
                normalized = {}
                for field in TARGET_COLUMNS:
                    if field in numeric:
                        normalized[field] = _numeric_text(numeric[field])
                    elif field == "date":
                        normalized[field] = trade_date.isoformat()
                    else:
                        normalized[field] = values.get(field, "")
                row_entries.append(normalized)
    except (OSError, UnicodeError, csv.Error, zipfile.BadZipFile, SampleAuditError) as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
        result["schema"] = "FAIL"
        result["overall"] = "FAIL"
        result["acceptance_status"] = "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED" if auth_gate != "PASS" else "TECHNICAL_SAMPLE_FAILED"
        _atomic_json(provenance_path, _provenance_payload(
            source_path=source_path, authorization=authorization, mapping=mapping, source_format=actual_format,
            source_period=None, validation_result="FAIL", project_version=project_version,
        ))
        _atomic_json(audit_dir / "sample_validation.json", result)
        _write_empty_audit_csvs(audit_dir)
        return result

    result["sample_present"] = True
    result["schema"] = "PASS" if seen_series_header and not result["errors"] else "FAIL"
    result["total_source_rows"] = source_rows
    result["EQ_rows"] = eq_rows
    result["non_EQ_rows"] = non_eq_rows
    result["unique_EQ_symbols"] = len(eq_symbols)
    result["unique_EQ_dates"] = len(eq_dates)
    result["observed_series_values"] = sorted(series_counts)
    result["min_date"] = min(all_valid_dates).isoformat() if all_valid_dates else None
    result["max_date"] = max(all_valid_dates).isoformat() if all_valid_dates else None
    result["unique_dates"] = len(all_valid_dates)
    result["malformed_date_rows"] = malformed_date_count
    result["invalid_OHLCV_rows"] = invalid_ohlcv_rows
    result["rows_with_ISIN"] = rows_with_isin
    result["rows_without_ISIN"] = rows_without_isin
    result["unique_ISINs"] = len({isin for values in symbol_isins.values() for isin in values})
    result["unique_symbols_with_ISIN"] = len(symbol_isins)
    result["symbol_to_multiple_ISINs"] = {symbol: sorted(isins) for symbol, isins in symbol_isins.items() if len(isins) > 1}
    result["ISIN_to_multiple_symbols"] = {isin: sorted(symbols) for isin, symbols in isin_symbols.items() if len(symbols) > 1}
    result["corporate_actions"] = "UNKNOWN — no separate corporate-action source was supplied; raw prices are not adjusted"
    result["source_format"] = actual_format
    result["source_version"] = mapping.get("source_version")
    result["source_period"] = {
        "start": min(all_valid_dates).isoformat() if all_valid_dates else None,
        "end": max(all_valid_dates).isoformat() if all_valid_dates else None,
    }

    duplicate_rows: list[dict[str, Any]] = []
    duplicate_key_count = duplicate_row_count = conflicting_duplicate_count = 0
    for (trade_date, symbol, series), group in duplicate_groups.items():
        if len(group) < 2:
            continue
        duplicate_key_count += 1
        duplicate_row_count += len(group) - 1
        signatures = {json.dumps(row["raw_values"], sort_keys=True, ensure_ascii=False) for row in group}
        conflict = len(signatures) > 1
        if conflict:
            conflicting_duplicate_count += 1
        for duplicate_number, entry in enumerate(group, start=1):
            duplicate_rows.append({
                "source_file": entry["source_file"], "source_member": entry["source_member"],
                "line_number": entry["line_number"], "date": trade_date.isoformat(), "symbol": symbol,
                "series": series, "duplicate_key_count": duplicate_key_count,
                "duplicate_row_number": duplicate_number, "conflicting_duplicate": str(conflict).lower(),
            })
    for row in duplicate_rows:
        row["duplicate_key_count"] = duplicate_key_count
    result["duplicate_key_count"] = duplicate_key_count
    result["duplicate_row_count"] = duplicate_row_count
    result["conflicting_duplicate_count"] = conflicting_duplicate_count

    # Calendar comparison is diagnostic. Missing NIFTY dates are not
    # automatically rejected as no-equity dates or as definitive data gaps.
    coverage_rows: list[dict[str, Any]] = []
    nifty_dates = _read_reference_dates(nifty_path) if nifty_path.is_file() else set()
    vix_dates = _read_reference_dates(vix_path) if vix_path.is_file() else set()
    nifty_missing = sorted(nifty_dates - eq_dates)
    eq_outside_nifty = sorted(eq_dates - nifty_dates) if nifty_dates else []
    for day in sorted(nifty_dates | eq_dates):
        has_eq = day in eq_dates
        if day in nifty_dates and not has_eq:
            classification = "POSSIBLE_EQ_DATA_GAP"
            evidence = "NIFTY reference date absent from sample EQ dates; official CM evidence required to classify"
        elif has_eq and day not in nifty_dates:
            classification = "CM_EQ_SESSION_NOT_IN_NIFTY"
            evidence = "explicit EQ row observed in the CM sample; investigate calendar/index scope"
        else:
            classification = "CM_EQ_AND_NIFTY_DATE_PRESENT"
            evidence = "date observed in both references"
        coverage_rows.append({
            "date": day.isoformat(), "source": "NIFTY", "present": str(has_eq).lower(),
            "classification": classification, "evidence": evidence,
        })
    for day in sorted(vix_dates | eq_dates):
        coverage_rows.append({
            "date": day.isoformat(), "source": "India VIX",
            "present": str(day in eq_dates).lower(),
            "classification": "CM_EQ_AND_VIX_DATE_PRESENT" if day in vix_dates and day in eq_dates else (
                "CM_EQ_SESSION_NOT_IN_VIX" if day in eq_dates else "VIX_DATE_WITHOUT_SAMPLE_EQ"
            ),
            "evidence": "India VIX calendar is a diagnostic reference, not an equity-session authority",
        })
    result["coverage_comparison"] = {
        "NIFTY_reference_dates": len(nifty_dates), "EQ_source_dates": len(eq_dates),
        "intersection": len(nifty_dates & eq_dates),
        "NIFTY_dates_missing_from_EQ": [day.isoformat() for day in nifty_missing],
        "EQ_dates_not_in_NIFTY": [day.isoformat() for day in eq_outside_nifty],
        "India_VIX_reference_dates": len(vix_dates),
        "EQ_dates_intersecting_India_VIX": len(vix_dates & eq_dates),
        "calendar_source": str(nifty_path) if nifty_dates else None,
        "classification_policy": "NIFTY-only dates remain POSSIBLE_EQ_DATA_GAP until authoritative CM evidence is available",
    }

    identity_rows: list[dict[str, Any]] = []
    for symbol, isins in sorted(symbol_isins.items()):
        if len(isins) > 1:
            identity_rows.append({"anomaly_type": "symbol_to_multiple_ISINs", "symbol": symbol, "isin": "",
                                 "related_values": ";".join(sorted(isins)), "row_count": sum(1 for row in member_rows if row["values"].get("symbol") == symbol)})
    for isin, symbols in sorted(isin_symbols.items()):
        if len(symbols) > 1:
            identity_rows.append({"anomaly_type": "ISIN_to_multiple_symbols", "symbol": "", "isin": isin,
                                 "related_values": ";".join(sorted(symbols)), "row_count": sum(1 for row in member_rows if row["values"].get("isin") == isin)})

    extreme_rows: list[dict[str, Any]] = []
    for symbol, series_rows in good_close_rows.items():
        ordered = sorted(series_rows, key=lambda item: item[0])
        for (previous_date, previous_close), (trade_date, close) in zip(ordered, ordered[1:]):
            if previous_close == 0:
                continue
            change = (close / previous_close - Decimal(1)) * Decimal(100)
            absolute = abs(change)
            thresholds = [value for value in (10, 20, 50) if absolute > Decimal(value)]
            if thresholds:
                extreme_rows.append({
                    "symbol": symbol, "date": trade_date.isoformat(),
                    "previous_observed_date": previous_date.isoformat(),
                    "observed_day_gap": (trade_date - previous_date).days,
                    "close": _numeric_text(close), "previous_close": _numeric_text(previous_close),
                    "return_pct": f"{change:.8f}",
                    "threshold_exceeded": ";".join(f"abs_return_gt_{value}pct" for value in thresholds),
                })

    # Write diagnostics before gates so rejected rows are inspectable.
    _atomic_csv(audit_dir / "invalid_ohlcv.csv", AUDIT_CSV_HEADERS["invalid_ohlcv.csv"], invalid_rows)
    _atomic_csv(audit_dir / "duplicate_records.csv", AUDIT_CSV_HEADERS["duplicate_records.csv"], duplicate_rows)
    _atomic_csv(audit_dir / "malformed_dates.csv", AUDIT_CSV_HEADERS["malformed_dates.csv"], malformed_dates)
    _atomic_csv(audit_dir / "symbol_isin_anomalies.csv", AUDIT_CSV_HEADERS["symbol_isin_anomalies.csv"], identity_rows)
    _atomic_csv(audit_dir / "extreme_returns.csv", AUDIT_CSV_HEADERS["extreme_returns.csv"], extreme_rows)
    _atomic_csv(audit_dir / "coverage_comparison.csv", AUDIT_CSV_HEADERS["coverage_comparison.csv"], coverage_rows)

    result["schema"] = "PASS" if not result["errors"] else "FAIL"
    result["eq_series"] = "PASS" if seen_series_header and eq_rows > 0 else "FAIL"
    result["ohlcv"] = "FAIL" if invalid_rows else ("PASS" if eq_rows else "NOT_ASSESSABLE")
    result["duplicates"] = "FAIL" if duplicate_key_count else "PASS"
    result["dates"] = "FAIL" if malformed_date_count else ("PASS" if all_valid_dates else "NOT_ASSESSABLE")
    if not nifty_dates:
        result["coverage"] = "NOT_ASSESSABLE"
    elif nifty_missing or eq_outside_nifty:
        result["coverage"] = "PASS_WITH_WARNINGS"
    else:
        result["coverage"] = "PASS"
    result["identity"] = "PASS_WITH_WARNINGS" if (
        not source_columns.get("isin") or rows_without_isin or identity_rows
    ) else "PASS"
    technical_gates = [result[key] for key in ("schema", "eq_series", "ohlcv", "duplicates", "dates", "coverage")]
    if "FAIL" in technical_gates:
        result["overall"] = "FAIL"
    elif "NOT_ASSESSABLE" in technical_gates:
        result["overall"] = "NOT_ASSESSABLE"
    elif "PASS_WITH_WARNINGS" in technical_gates or result["coverage"] == "PASS_WITH_WARNINGS" or result["identity"] == "PASS_WITH_WARNINGS":
        result["overall"] = "PASS_WITH_WARNINGS"
    else:
        result["overall"] = "PASS"
    result["overall"] = _gate(result["overall"])

    _atomic_json(provenance_path, _provenance_payload(
        source_path=source_path, authorization=authorization, mapping=mapping, source_format=actual_format,
        source_period=result["source_period"], validation_result=result["overall"],
        project_version=project_version,
    ))

    if auth_gate == "PASS" and result["overall"] in {"PASS", "PASS_WITH_WARNINGS"}:
        _atomic_csv(normalized_path, TARGET_COLUMNS, row_entries)
        result["acceptance_status"] = "AUTHORIZED_RESEARCH_READY_SAMPLE"
        result["normalized_sample_path"] = str(normalized_path)
    elif auth_gate != "PASS":
        result["acceptance_status"] = "UNAUTHORIZED_OR_AUTHORIZATION_NOT_PROVIDED"
        result["normalization_written"] = False
    else:
        result["acceptance_status"] = "TECHNICAL_SAMPLE_FAILED"
        result["normalization_written"] = False
    _atomic_json(audit_dir / "sample_validation.json", result)
    return result


def _default_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _project_version(root: Path) -> str | None:
    try:
        import tomllib
        with (root / "pyproject.toml").open("rb") as stream:
            return str(tomllib.load(stream).get("project", {}).get("version"))
    except (OSError, ValueError, TypeError):
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit one local NSE CM EOD sample using explicit manifests")
    parser.add_argument("--root", type=Path, default=_default_project_root())
    parser.add_argument("--source", type=Path, help="one sample file; defaults to the sole supported file in incoming/")
    args = parser.parse_args(argv)
    root = args.root.resolve()
    base = root / "data" / "licensed_nse_cm_eod"
    incoming = base / "incoming"
    source = args.source.resolve() if args.source else None
    if source is not None:
        try:
            source.relative_to(incoming.resolve())
        except ValueError:
            print("BLOCKED: sample source must be inside data/licensed_nse_cm_eod/incoming/")
            return 2
        if not source.is_file():
            print(f"BLOCKED: sample source does not exist: {source}")
            return 2
    if source is None:
        candidates = sorted(
            (path for path in incoming.iterdir() if path.is_file() and path.name.lower().endswith((".csv", ".csv.gz", ".csv.zip"))),
            key=lambda value: value.name.casefold(),
        ) if incoming.is_dir() else []
        if len(candidates) == 1:
            source = candidates[0]
        elif len(candidates) > 1:
            print("BLOCKED: multiple sample files found; audit each delivery with its own explicit mapping")
            return 2
    result = audit_sample(
        source_path=source,
        mapping_path=base / "manifest" / "source_column_mapping.json",
        authorization_path=base / "manifest" / "source_authorization.json",
        audit_dir=base / "audit",
        normalized_path=base / "normalized" / "nse_cm_eod_sample.csv",
        provenance_path=base / "manifest" / "sample_provenance.json",
        nifty_path=root / "data" / "nse_exports" / "index_NSEI.csv",
        vix_path=root / "data" / "nse_exports" / "index_INDIAVIX.csv",
        project_version=_project_version(root),
    )
    print(f"{result['acceptance_status']}: sample_present={result['sample_present']}")
    return 0 if not result["sample_present"] else (0 if result["overall"] != "FAIL" else 1)


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
