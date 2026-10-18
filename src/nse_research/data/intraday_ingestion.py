"""Phase 4F authorized intraday dataset registration and canonicalization.

This module is data-side only. Raw files are copied once and never edited;
Phase 4E validates a streaming, SQLite-backed view before canonical CSV output
is committed. No provider, strategy, or execution code is changed here.
"""

from __future__ import annotations

import csv
import fcntl
import gzip
import hashlib
import io
import json
import math
import os
import re
import shutil
import sqlite3
import tempfile
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from nse_research.data.intraday_validator import (
    IntradayValidationReport,
    ValidationIssue,
    classify_dataset,
    validate_intraday_dataset,
    validate_session_coverage,
    validate_timezone,
)
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.intraday.model import (
    IST,
    TIMEFRAME_MINUTES,
    BarValidationError,
    IntradayBar,
    default_nse_calendar,
    validate_intraday_bar,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = PROJECT_ROOT / "data" / "intraday"
REGISTRY_PATH = DATA_ROOT / "datasets.json"
RESULTS_ROOT = PROJECT_ROOT / "results" / "phase4f"
CANONICAL_COLUMNS = (
    "timestamp", "symbol", "exchange", "series", "timeframe", "open", "high",
    "low", "close", "volume", "volume_kind", "status", "source",
)
BAR_TYPES = {"1m", "5m"}
VOLUME_SEMANTICS = {
    "INCREMENTAL_SHARES", "CUMULATIVE_SHARES", "TRADE_COUNT", "UNKNOWN",
    "BAR_SHARE_VOLUME", "INCREMENTAL_SHARE_VOLUME",
    "CUMULATIVE_SHARE_VOLUME_WITH_VERIFIED_INCREMENT_RULE",
}
VALID_FORMATS = (".csv", ".csv.gz", ".csv.zip", ".parquet")
_SAFE_ID = re.compile(r"[^a-z0-9_-]+")
_REQUIRED_BAR_FIELDS = ("timestamp", "symbol", "open", "high", "low", "close", "volume")


class Phase4FError(RuntimeError):
    """Stable fail-closed error used by Phase 4F commands and adapters."""

    def __init__(self, code: str, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}" if detail else code)


@dataclass(slots=True)
class ValidationResult:
    status: str
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    statistics: dict[str, Any] = field(default_factory=dict)
    phase4e_validation_status: str = "FAIL"
    research_ready: bool = False
    research_readiness: str = "NOT_RESEARCH_READY"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class SessionTimestampResult:
    status: str
    reason: str


def _json_write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False,
                         allow_nan=False, default=str) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _file_manifest(files: Sequence[Path], base: Path | None = None) -> list[dict[str, Any]]:
    entries = []
    for path in sorted(files, key=lambda item: str(item)):
        relative = path.relative_to(base).as_posix() if base is not None else path.name
        entries.append({"relative_path": relative, "size": path.stat().st_size,
                        "sha256": sha256_file(path)})
    return entries


def _dataset_fingerprint(entries: Sequence[Mapping[str, Any]]) -> str:
    encoded = json.dumps(list(entries), sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _format(path: Path) -> str | None:
    lowered = path.name.lower()
    return next((suffix for suffix in VALID_FORMATS if lowered.endswith(suffix)), None)


def _discover_files(path: Path) -> list[Path]:
    if not path.exists():
        raise Phase4FError("DATASET_FILE_MISSING", str(path))
    if path.is_file():
        files = [path] if _format(path) else []
    elif path.is_dir():
        files = [item for item in path.rglob("*") if item.is_file() and _format(item)
                 and not item.name.startswith(".")]
    else:
        files = []
    if not files:
        raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE",
                           "expected CSV, CSV.GZ, CSV.ZIP, or Parquet input")
    return sorted(files, key=lambda item: item.as_posix())


def _csv_headers(stream: Any) -> list[str]:
    reader = csv.reader(stream)
    try:
        row = next(reader)
    except StopIteration as exc:
        raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", "CSV is empty") from exc
    headers = [str(item).strip() for item in row]
    if not headers or any(not item for item in headers) or len({item.casefold() for item in headers}) != len(headers):
        raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", "CSV header is empty or ambiguous")
    return headers


def _csv_member_headers(path: Path) -> list[tuple[str, list[str]]]:
    fmt = _format(path)
    result: list[tuple[str, list[str]]] = []
    try:
        if fmt == ".csv":
            with path.open("r", newline="", encoding="utf-8-sig") as stream:
                result.append((path.name, _csv_headers(stream)))
        elif fmt == ".csv.gz":
            with gzip.open(path, "rt", newline="", encoding="utf-8-sig") as stream:
                result.append((path.name, _csv_headers(stream)))
        elif fmt == ".csv.zip":
            with zipfile.ZipFile(path) as archive:
                members = sorted(name for name in archive.namelist()
                                 if not name.endswith("/") and name.lower().endswith(".csv"))
                if not members:
                    raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", f"ZIP contains no CSV: {path}")
                for name in members:
                    with archive.open(name) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
                        result.append((f"{path.name}:{name}", _csv_headers(stream)))
    except (OSError, UnicodeError, csv.Error, zipfile.BadZipFile) as exc:
        raise Phase4FError("DATASET_UNREADABLE", f"{path}: {exc}") from exc
    return result


def _schema_names(path: Path) -> list[tuple[str, list[str]]]:
    if _format(path) == ".parquet":
        try:
            import pyarrow.parquet as pq
            schema = pq.ParquetFile(path).schema_arrow
            return [(path.name, [field.name for field in schema])]
        except Exception as exc:
            raise Phase4FError("DATASET_UNREADABLE", f"Parquet schema unavailable for {path}: {exc}") from exc
    return _csv_member_headers(path)


def _metadata_path(path: Path, supplied: str | Path | None) -> Path | None:
    if supplied is not None:
        candidate = Path(supplied)
        if not candidate.is_absolute():
            candidate = Path.cwd() / candidate
        if not candidate.is_file():
            raise Phase4FError("DELIVERY_METADATA_MISSING", str(candidate))
        return candidate
    candidates = ([path / "dataset_metadata.json", path / "delivery_metadata.json"] if path.is_dir()
                  else [Path(str(path) + ".metadata.json"), path.with_suffix(".metadata.json")])
    return next((item for item in candidates if item.is_file()), None)


def _load_metadata(path: Path, supplied: str | Path | None) -> tuple[dict[str, Any], Path | None]:
    metadata_file = _metadata_path(path, supplied)
    if metadata_file is None:
        return {}, None
    try:
        value = json.loads(metadata_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4FError("DELIVERY_METADATA_INVALID", str(exc)) from exc
    if not isinstance(value, dict):
        raise Phase4FError("DELIVERY_METADATA_INVALID", "metadata root must be a JSON object")
    return value, metadata_file


def _slug(value: Any, default: str) -> str:
    normalized = _SAFE_ID.sub("_", str(value or "").strip().lower()).strip("_-")
    return normalized or default


def _header_map(headers: Sequence[str], metadata: Mapping[str, Any]) -> dict[str, str]:
    source_names = {str(item).strip().casefold(): str(item).strip() for item in headers}
    requested = metadata.get("column_map", {})
    if requested is None:
        requested = {}
    if not isinstance(requested, Mapping):
        raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", "column_map must be an object")
    result: dict[str, str] = {}
    for field_name in (*_REQUIRED_BAR_FIELDS, "exchange", "series", "timeframe", "volume_kind", "status", "source"):
        raw_name = requested.get(field_name, field_name)
        actual = source_names.get(str(raw_name).strip().casefold())
        if actual:
            result[field_name] = actual
    return result


def _validate_schema(path: Path, files: Sequence[Path], metadata: Mapping[str, Any]) -> dict[str, Any]:
    data_type = str(metadata.get("data_type", "bars")).strip().lower()
    timeframe = str(metadata.get("timeframe", "")).strip().lower()
    if data_type not in {"bars", "tick"}:
        raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", "data_type must be bars or tick")
    if data_type == "bars" and timeframe not in BAR_TYPES:
        raise Phase4FError("TIMEFRAME_REQUIRED", "metadata must declare timeframe as 1m or 5m")
    if data_type == "tick" and timeframe not in {"tick", "ticks"}:
        raise Phase4FError("TIMEFRAME_REQUIRED", "tick input must declare timeframe=tick")
    schemas: list[dict[str, Any]] = []
    for file in files:
        for label, columns in _schema_names(file):
            mapping = _header_map(columns, metadata)
            required = set(_REQUIRED_BAR_FIELDS if data_type == "bars" else ("timestamp", "symbol"))
            if data_type == "tick":
                raw_columns = {item.strip().casefold() for item in columns}
                price_field = str(metadata.get("column_map", {}).get("price", "price")).strip().casefold()
                if price_field not in raw_columns:
                    raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", f"tick schema missing price field: {label}")
            missing = sorted(required - set(mapping))
            if missing:
                raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE",
                                   f"{label} missing identifiable fields: {', '.join(missing)}")
            if data_type == "bars" and "timeframe" not in mapping and not timeframe:
                raise Phase4FError("TIMEFRAME_REQUIRED", f"no timeframe field or metadata in {label}")
            schemas.append({"file": label, "columns": columns, "column_map": mapping})
    return {"data_type": data_type, "timeframe": "tick" if data_type == "tick" else timeframe,
            "original_schema": schemas}


def _registry_read(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "datasets": []}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4FError("DATASET_REGISTRY_INVALID", str(exc)) from exc
    if not isinstance(value, dict) or value.get("schema_version") != 1 or not isinstance(value.get("datasets"), list):
        raise Phase4FError("DATASET_REGISTRY_INVALID", "expected schema_version=1 and datasets array")
    return value


def get_registered_dataset(dataset_id: str, *, project_root: str | Path = PROJECT_ROOT) -> dict[str, Any]:
    """Return a registry record or fail with the replay boundary's stable code."""
    root = Path(project_root).resolve()
    registry = _registry_read(root / "data" / "intraday" / "datasets.json")
    item = next((entry for entry in registry["datasets"]
                 if isinstance(entry, dict) and entry.get("dataset_id") == dataset_id), None)
    if item is None:
        raise Phase4FError("PHASE4F_DATASET_NOT_REGISTERED", str(dataset_id))
    return dict(item)


def _raw_destination(project_root: Path, dataset_id: str) -> Path:
    return project_root / "data" / "intraday" / "raw" / dataset_id


def _copy_raw(files: Sequence[Path], source_root: Path, metadata_file: Path | None,
              destination: Path) -> None:
    if destination.exists():
        expected_names = {(item.relative_to(source_root).as_posix() if source_root.is_dir() else item.name)
                          for item in files}
        existing_files = {item.relative_to(destination).as_posix() for item in destination.rglob("*")
                          if item.is_file() and item.name != "delivery_metadata.json"}
        if existing_files != expected_names:
            raise Phase4FError("DATASET_ID_CONFLICT", f"raw directory already exists with different files: {destination}")
        for item in files:
            relative = item.relative_to(source_root) if source_root.is_dir() else Path(item.name)
            existing = destination / relative
            if not existing.is_file() or sha256_file(existing) != sha256_file(item):
                raise Phase4FError("DATASET_ID_CONFLICT", f"registered raw source differs: {existing}")
        if metadata_file is not None:
            saved = destination / "delivery_metadata.json"
            if saved.is_file() and sha256_file(saved) != sha256_file(metadata_file):
                raise Phase4FError("DATASET_ID_CONFLICT", "delivery metadata differs for this dataset revision")
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    if temporary.exists():
        shutil.rmtree(temporary)
    temporary.mkdir()
    try:
        for item in files:
            relative = item.relative_to(source_root) if source_root.is_dir() else Path(item.name)
            target = temporary / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(item, target)
        if metadata_file is not None:
            shutil.copyfile(metadata_file, temporary / "delivery_metadata.json")
        os.replace(temporary, destination)
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def register_dataset(
    path: str | Path,
    *,
    metadata_path: str | Path | None = None,
    dry_run: bool = False,
    project_root: str | Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Hash, schema-check, and register a dataset without running trading."""
    source = Path(path).expanduser().resolve()
    files = _discover_files(source)
    metadata, metadata_file = _load_metadata(source, metadata_path)
    schema = _validate_schema(source, files, metadata)
    entries = _file_manifest(files, source if source.is_dir() else None)
    fingerprint = _dataset_fingerprint(entries)
    timeframe = schema["timeframe"]
    provider = str(metadata.get("provider") or "UNKNOWN")
    revision_value = str(metadata.get("revision") or "").strip()
    revision = revision_value if revision_value and revision_value.upper() != "UNKNOWN" else "UNKNOWN"
    id_revision = revision if revision != "UNKNOWN" else f"sha256-{fingerprint[:16]}"
    dataset_id = f"nse_eq_{_slug(timeframe, 'unknown')}_{_slug(provider, 'unknown')}_{_slug(id_revision, 'unknown')}"
    supplied_source_url = str(metadata.get("source_url") or "").strip()
    if supplied_source_url.upper() == "UNKNOWN":
        supplied_source_url = ""
    source_location = supplied_source_url or source.as_posix()
    root = Path(project_root).resolve()
    registry_path = root / "data" / "intraday" / "datasets.json"
    data_root = root / "data" / "intraday"
    metadata_hash = sha256_file(metadata_file) if metadata_file else "UNKNOWN"
    record = {
        "dataset_id": dataset_id,
        "name": str(metadata.get("dataset_name") or source.name),
        "provider": provider,
        "source": source_location,
        "source_url": supplied_source_url or "UNKNOWN",
        "source_path": source.as_posix(),
        "revision": revision,
        "timeframe": timeframe,
        "data_type": schema["data_type"],
        "coverage_start": "UNKNOWN",
        "coverage_end": "UNKNOWN",
        "symbols": 0,
        "rows": 0,
        "exchange": str(metadata.get("exchange") or "UNKNOWN"),
        "series": str(metadata.get("series") or "UNKNOWN"),
        "timezone": str(metadata.get("original_timezone") or "UNKNOWN"),
        "canonical_timezone": "Asia/Kolkata",
        "timestamp_convention": str(metadata.get("timestamp_convention") or "UNKNOWN").upper(),
        "volume_semantics": str(metadata.get("volume_semantics") or "UNKNOWN").upper(),
        "adjustment_status": str(metadata.get("adjustment_status") or "UNKNOWN").upper(),
        "license_status": str(metadata.get("license_status") or metadata.get("license") or "UNKNOWN"),
        "data_rights": metadata.get("data_rights", "UNKNOWN"),
        "provenance_status": "SUPPLIED" if metadata_file else "INCOMPLETE",
        "validation_status": "PENDING",
        "research_readiness": "NOT_RESEARCH_READY",
        "research_ready": False,
        "source_hash": fingerprint,
        "file_manifest": entries,
        "source_manifest": str((data_root / "manifests" / f"{dataset_id}.source_manifest.json").relative_to(root)),
        "original_format": sorted({str(_format(item)).lstrip(".").upper() for item in files}),
        "original_schema": schema["original_schema"],
        "delivery_metadata_sha256": metadata_hash,
        "raw_path": str(_raw_destination(root, dataset_id).relative_to(root)),
        "normalized_path": str((root / "data/intraday/normalized" / dataset_id / "canonical_intraday_bars.csv").relative_to(root)),
        "source_metadata": metadata,
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }
    if schema["data_type"] == "tick":
        record["validation_status"] = "TICK_AGGREGATION_NOT_IMPLEMENTED"
        record["source_metadata"]["research_ready"] = False
    if dry_run:
        return {"status": "DRY_RUN", "dataset": record, "would_write": [str(registry_path), record["raw_path"]]}

    data_root.mkdir(parents=True, exist_ok=True)
    (data_root / "raw").mkdir(exist_ok=True)
    (data_root / "normalized").mkdir(exist_ok=True)
    (data_root / "manifests").mkdir(exist_ok=True)
    (data_root / "samples").mkdir(exist_ok=True)
    (data_root / "quarantine").mkdir(exist_ok=True)
    registry = _registry_read(registry_path)
    existing = next((item for item in registry["datasets"] if item.get("dataset_id") == dataset_id), None)
    if existing:
        if existing.get("source_hash") != fingerprint:
            raise Phase4FError("DATASET_ID_CONFLICT", "same deterministic dataset ID has different source bytes")
        if existing.get("delivery_metadata_sha256", "UNKNOWN") != metadata_hash:
            raise Phase4FError("DATASET_ID_CONFLICT", "same deterministic dataset ID has different delivery metadata")
        return {"status": "ALREADY_REGISTERED", "dataset": existing}
    raw_target = _raw_destination(root, dataset_id)
    _copy_raw(files, source, metadata_file, raw_target)
    source_manifest = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "source_hash": fingerprint,
        "file_manifest": entries,
        "source_path": record["source_path"],
        "delivery_metadata_sha256": metadata_hash,
        "provider": record["provider"],
        "source_url": record["source_url"],
        "revision": revision,
        "original_format": record["original_format"],
        "original_schema": record["original_schema"],
        "original_timezone": record["timezone"],
        "license_status": record["license_status"],
        "data_rights": record["data_rights"],
    }
    _json_write_atomic(root / record["source_manifest"], source_manifest)
    registry["datasets"].append(record)
    registry["datasets"].sort(key=lambda item: item["dataset_id"])
    _json_write_atomic(registry_path, registry)
    return {"status": "REGISTERED", "dataset": record}


def _get_dataset(dataset_id: str, project_root: str | Path = PROJECT_ROOT) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    root = Path(project_root).resolve()
    registry = _registry_read(root / "data/intraday/datasets.json")
    item = next((entry for entry in registry["datasets"] if entry.get("dataset_id") == dataset_id), None)
    if item is None:
        raise Phase4FError("DATASET_NOT_REGISTERED", dataset_id)
    metadata = item.get("source_metadata") if isinstance(item.get("source_metadata"), dict) else {}
    raw = root / str(item.get("raw_path", ""))
    if not raw.is_dir():
        raise Phase4FError("DATASET_RAW_UNAVAILABLE", str(raw))
    metadata_hash = item.get("delivery_metadata_sha256", "UNKNOWN")
    metadata_file = raw / "delivery_metadata.json"
    if metadata_hash != "UNKNOWN" and (not metadata_file.is_file() or sha256_file(metadata_file) != metadata_hash):
        raise Phase4FError("DELIVERY_METADATA_HASH_MISMATCH", str(metadata_file))
    return root, item, metadata


def _registered_raw_files(root: Path, record: Mapping[str, Any]) -> list[Path]:
    folder = root / str(record["raw_path"])
    files = [item for item in folder.rglob("*") if item.is_file() and item.name != "delivery_metadata.json"]
    files = sorted(files, key=lambda item: item.relative_to(folder).as_posix())
    if not files:
        raise Phase4FError("DATASET_RAW_UNAVAILABLE", str(folder))
    current = _file_manifest(files, folder)
    if _dataset_fingerprint(current) != record.get("source_hash"):
        raise Phase4FError("SOURCE_HASH_MISMATCH", "registered raw file hash does not match registry")
    return files


def _row_iterator(path: Path) -> Iterator[tuple[str, dict[str, Any]]]:
    """Yield (source member, row) for the supported raw formats."""
    fmt = _format(path)
    try:
        if fmt in {".csv", ".csv.gz"}:
            opener = gzip.open if fmt == ".csv.gz" else open
            with opener(path, "rt", newline="", encoding="utf-8-sig") as stream:
                reader = csv.DictReader(stream)
                if not reader.fieldnames:
                    raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", f"empty CSV header: {path}")
                for row in reader:
                    yield path.name, dict(row)
        elif fmt == ".csv.zip":
            with zipfile.ZipFile(path) as archive:
                for member in sorted(name for name in archive.namelist()
                                     if not name.endswith("/") and name.lower().endswith(".csv")):
                    with archive.open(member) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as stream:
                        reader = csv.DictReader(stream)
                        if not reader.fieldnames:
                            raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", f"empty CSV header: {path}:{member}")
                        for row in reader:
                            yield f"{path.name}:{member}", dict(row)
        elif fmt == ".parquet":
            import pyarrow.parquet as pq
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=10_000, use_threads=False):
                for row in batch.to_pylist():
                    yield path.name, row
        else:
            raise Phase4FError("DATASET_FORMAT_UNSUPPORTED", str(path))
    except Phase4FError:
        raise
    except (OSError, UnicodeError, csv.Error, zipfile.BadZipFile) as exc:
        raise Phase4FError("DATASET_UNREADABLE", f"{path}: {exc}") from exc
    except Exception as exc:
        if fmt == ".parquet":
            raise Phase4FError("DATASET_UNREADABLE", f"{path}: {exc}") from exc
        raise


def _source_field(row: Mapping[str, Any], requested: str) -> Any:
    key = next((name for name in row if str(name).strip().casefold() == requested.strip().casefold()), None)
    return row.get(key) if key is not None else None


def _parse_zone(name: str) -> Any:
    value = str(name or "").strip()
    if value in {"UNKNOWN", ""}:
        return None
    if re.fullmatch(r"[+-]\d{2}:?\d{2}", value):
        compact = value.replace(":", "")
        sign = 1 if compact[0] == "+" else -1
        offset = timedelta(hours=int(compact[1:3]), minutes=int(compact[3:5])) * sign
        return timezone(offset)
    try:
        return ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise Phase4FError("SOURCE_TIMEZONE_INVALID", value) from exc


def _parse_source_timestamp(value: Any, metadata: Mapping[str, Any], timeframe: str) -> tuple[datetime | None, str | None, str | None]:
    raw = str(value or "").strip()
    if not raw:
        return None, "TIMESTAMP_MISSING", None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None, "TIMESTAMP_MALFORMED", raw
    declared_zone_name = str(metadata.get("original_timezone") or "UNKNOWN")
    declared_zone = _parse_zone(declared_zone_name)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        if declared_zone is None or not str(metadata.get("timestamp_timezone_documentation") or "").strip():
            return None, "TIMESTAMP_NAIVE_UNDOCUMENTED", raw
        if isinstance(declared_zone, ZoneInfo):
            first = parsed.replace(tzinfo=declared_zone, fold=0)
            second = parsed.replace(tzinfo=declared_zone, fold=1)
            if first.utcoffset() != second.utcoffset():
                return None, "TIMESTAMP_AMBIGUOUS_LOCAL_TIME", raw
            parsed = first
        else:
            parsed = parsed.replace(tzinfo=declared_zone)
    elif declared_zone is not None:
        expected_offset = parsed.astimezone(declared_zone).utcoffset()
        if parsed.utcoffset() != expected_offset:
            return parsed, "TIMESTAMP_SOURCE_ZONE_MISMATCH", raw
    convention = str(metadata.get("timestamp_convention") or "UNKNOWN").strip().upper()
    if convention not in {"BAR_START", "BAR_END"}:
        return parsed, "TIMESTAMP_CONVENTION_UNKNOWN", raw
    if convention == "BAR_END":
        parsed -= timedelta(minutes=TIMEFRAME_MINUTES[timeframe])
    return parsed, None, raw


def _symbol_identity(row: Mapping[str, Any], mapping: Mapping[str, Any], metadata: Mapping[str, Any],
                     column_map: Mapping[str, str]) -> tuple[str, str, str, str | None]:
    source_symbol = str(_source_field(row, column_map.get("symbol", "symbol")) or "").strip()
    if not source_symbol:
        return "", str(_source_field(row, column_map.get("exchange", "exchange")) or ""), str(_source_field(row, column_map.get("series", "series")) or ""), None
    mapped_key = next((key for key in mapping if str(key).strip().casefold() == source_symbol.casefold()), None)
    mapped = mapping.get(mapped_key) if mapped_key is not None else None
    if mapped is not None:
        if not isinstance(mapped, Mapping):
            return source_symbol.upper(), "", "", "SYMBOL_MAPPING_INVALID"
        if not str(metadata.get("symbol_mapping_documentation") or "").strip():
            return source_symbol.upper(), "", "", "SYMBOL_MAPPING_PROVENANCE_MISSING"
        canonical = str(mapped.get("symbol") or "").strip().upper()
        exchange = str(mapped.get("exchange") or "").strip().upper()
        series = str(mapped.get("series") or "").strip().upper()
        if not canonical or not exchange or not series:
            return canonical or source_symbol.upper(), exchange, series, "SYMBOL_MAPPING_INVALID"
        row_exchange = str(_source_field(row, column_map.get("exchange", "exchange")) or "").strip().upper()
        row_series = str(_source_field(row, column_map.get("series", "series")) or "").strip().upper()
        if (row_exchange and row_exchange != exchange) or (row_series and row_series != series):
            return canonical, exchange, series, "SYMBOL_MAPPING_SOURCE_IDENTITY_MISMATCH"
        return canonical, exchange, series, None
    symbol = source_symbol.upper()
    exchange = str(_source_field(row, column_map.get("exchange", "exchange")) or "").strip().upper()
    series = str(_source_field(row, column_map.get("series", "series")) or "").strip().upper()
    return symbol, exchange, series, None


def _volume_kind(row: Mapping[str, Any], column_map: Mapping[str, str], metadata: Mapping[str, Any]) -> str:
    raw = _source_field(row, column_map.get("volume_kind", "volume_kind"))
    if raw not in (None, ""):
        return str(raw).strip().upper()
    semantics = str(metadata.get("volume_semantics") or "UNKNOWN").strip().upper()
    if semantics in {"INCREMENTAL_SHARES", "BAR_SHARE_VOLUME", "INCREMENTAL_SHARE_VOLUME"}:
        return "BAR"
    if semantics in {"CUMULATIVE_SHARES", "CUMULATIVE_SHARE_VOLUME_WITH_VERIFIED_INCREMENT_RULE"}:
        return "BAR" if metadata.get("cumulative_to_incremental") is True else "CUMULATIVE"
    return "UNKNOWN"


def _canonical_source_row(row: Mapping[str, Any], metadata: Mapping[str, Any], column_map: Mapping[str, str],
                          *, row_number: int) -> dict[str, Any]:
    mapping = metadata.get("symbol_mapping", {})
    if not isinstance(mapping, Mapping):
        mapping = {}
    symbol, exchange, series, identity_error = _symbol_identity(row, mapping, metadata, column_map)
    raw_timeframe = _source_field(row, column_map.get("timeframe", "timeframe"))
    timeframe = str(raw_timeframe if raw_timeframe not in (None, "") else metadata.get("timeframe") or "").strip().lower()
    raw_timestamp = _source_field(row, column_map.get("timestamp", "timestamp"))
    parsed, timestamp_error, raw_text = _parse_source_timestamp(raw_timestamp, metadata, timeframe) if timeframe in BAR_TYPES else (None, "TIMEFRAME_INVALID", str(raw_timestamp or ""))
    timestamp = parsed.isoformat() if parsed is not None else str(raw_timestamp or "")
    status_value = _source_field(row, column_map.get("status", "status"))
    if status_value in (None, ""):
        finalized = metadata.get("bars_finalized") is True and bool(str(metadata.get("bars_finalized_documentation") or "").strip())
        status_value = "CLOSED" if finalized else "UNKNOWN"
    source_value = _source_field(row, column_map.get("source", "source"))
    if source_value in (None, ""):
        source_value = metadata.get("provider") or "UNKNOWN"
    result = {
        "timestamp": timestamp,
        "symbol": symbol,
        "exchange": exchange,
        "series": series,
        "timeframe": timeframe,
        "open": _source_field(row, column_map.get("open", "open")),
        "high": _source_field(row, column_map.get("high", "high")),
        "low": _source_field(row, column_map.get("low", "low")),
        "close": _source_field(row, column_map.get("close", "close")),
        "volume": _source_field(row, column_map.get("volume", "volume")),
        "volume_kind": _volume_kind(row, column_map, metadata),
        "status": str(status_value or "UNKNOWN").strip().upper(),
        "source": str(source_value or "UNKNOWN").strip(),
        "_source_timestamp": raw_text,
        "_timestamp_error": timestamp_error,
        "_identity_error": identity_error,
        "_source_row": row_number,
    }
    return result


def _special_windows(metadata: Mapping[str, Any]) -> dict[date, Sequence[tuple[str, str]]]:
    result: dict[date, Sequence[tuple[str, str]]] = {}
    values = metadata.get("special_session_windows", {})
    if not isinstance(values, Mapping):
        return result
    for key, windows in values.items():
        try:
            day = date.fromisoformat(str(key))
        except ValueError as exc:
            raise Phase4FError("SESSION_CALENDAR_INVALID", f"invalid special-session date {key!r}") from exc
        if not isinstance(windows, Sequence) or isinstance(windows, (str, bytes)):
            raise Phase4FError("SESSION_CALENDAR_INVALID", f"windows for {key} must be a list")
        result[day] = tuple((str(pair[0]), str(pair[1])) for pair in windows if isinstance(pair, Sequence) and len(pair) == 2)
    return result


def validate_session_timestamp(
    timestamp: datetime | str,
    *,
    timeframe: str = "1m",
    calendar: NseTradingCalendar | None = None,
    special_session_windows: Mapping[date | str, Sequence[tuple[str | time, str | time]]] | None = None,
) -> SessionTimestampResult:
    """Classify a timestamp using the project's existing NSE calendar.

    Special-session hours are accepted only when an explicit schedule is
    supplied. Missing or undocumented no-trade minutes are never inferred.
    """
    if timeframe not in BAR_TYPES:
        return SessionTimestampResult("INVALID", "timeframe must be 1m or 5m")
    try:
        local = validate_timezone(timestamp, canonical_timezone="Asia/Kolkata")
    except ValueError as exc:
        return SessionTimestampResult("INVALID", str(exc))
    exchange_calendar = calendar or default_nse_calendar()
    day = local.date()
    if day in exchange_calendar.holidays:
        return SessionTimestampResult("HOLIDAY", exchange_calendar.holidays[day])
    span = timedelta(minutes=TIMEFRAME_MINUTES[timeframe])
    windows: Mapping[Any, Any] = special_session_windows or {}
    raw_windows = windows.get(day, windows.get(day.isoformat()))
    if day in exchange_calendar.special_sessions:
        if not raw_windows:
            return SessionTimestampResult("SPECIAL_SESSION_HOURS_UNVERIFIED", "calendar lists a special session but no documented hours were supplied")
        for start, end in raw_windows:
            open_time = time.fromisoformat(str(start)) if not isinstance(start, time) else start
            close_time = time.fromisoformat(str(end)) if not isinstance(end, time) else end
            bar_end = (local + span).timetz().replace(tzinfo=None)
            if local.timetz().replace(tzinfo=None) >= open_time and bar_end <= close_time:
                return SessionTimestampResult("SPECIAL_SESSION", "timestamp falls in explicitly supplied NSE special-session window")
        return SessionTimestampResult("OUTSIDE_SESSION", "bar falls outside explicit special-session windows")
    if day.weekday() >= 5:
        return SessionTimestampResult("WEEKEND_OR_NONTRADING", "no explicit special-session calendar entry")
    opened = time(9, 15)
    closed = time(15, 30)
    bar_end = (local + span).timetz().replace(tzinfo=None)
    if local.timetz().replace(tzinfo=None) < opened or bar_end > closed:
        return SessionTimestampResult("OUTSIDE_SESSION", "bar must be wholly within 09:15–15:30 Asia/Kolkata")
    decision = exchange_calendar.intraday_status(local, observed_market_data=True)
    if decision.status != "OPEN":
        return SessionTimestampResult(decision.status, decision.reason)
    return SessionTimestampResult("NORMAL_SESSION", "regular NSE session")


class DatasetLock:
    """Non-blocking lock for one dataset's validation/normalization writers."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.stream: Any = None

    def __enter__(self) -> "DatasetLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+")
        try:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.stream.close()
            self.stream = None
            raise Phase4FError("DATASET_LOCKED", self.path.name) from exc
        self.stream.seek(0)
        self.stream.truncate()
        self.stream.write(f"pid={os.getpid()}\n")
        self.stream.flush()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.stream is not None:
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
            self.stream.close()
            self.stream = None


def _canonical_decimal(value: Any) -> str:
    from decimal import Decimal, InvalidOperation

    raw = str(value if value is not None else "").strip()
    try:
        number = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        return raw
    if not number.is_finite():
        return raw
    rendered = format(number, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def _schema_for_row(path: Path, metadata: Mapping[str, Any]) -> dict[str, str]:
    names = _schema_names(path)
    if not names:
        raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", str(path))
    selected = names[0][1]
    mapping = _header_map(selected, metadata)
    return mapping


def _expected_session_dates(metadata: Mapping[str, Any]) -> list[date] | None:
    value = metadata.get("expected_sessions")
    if value is None:
        return None
    if not isinstance(value, list):
        raise Phase4FError("SESSION_CALENDAR_INVALID", "expected_sessions must be an array of ISO dates")
    result: list[date] = []
    for item in value:
        try:
            result.append(date.fromisoformat(str(item)))
        except ValueError as exc:
            raise Phase4FError("SESSION_CALENDAR_INVALID", f"invalid expected session date {item!r}") from exc
    if len(result) != len(set(result)):
        raise Phase4FError("SESSION_CALENDAR_INVALID", "expected_sessions contains duplicate dates")
    return sorted(result)


def _expected_symbols(metadata: Mapping[str, Any]) -> list[str]:
    value = metadata.get("expected_symbols")
    if value is None:
        return []
    if not isinstance(value, list):
        raise Phase4FError("EXPECTED_SYMBOLS_INVALID", "expected_symbols must be an array")
    symbols = [str(item).strip().upper() for item in value]
    if not all(symbols) or len(set(symbols)) != len(symbols):
        raise Phase4FError("EXPECTED_SYMBOLS_INVALID", "expected_symbols must be unique, non-empty symbols")
    return sorted(symbols)


def _open_stage() -> tuple[tempfile.TemporaryDirectory, sqlite3.Connection]:
    temporary = tempfile.TemporaryDirectory(prefix="phase4f-stage-")
    database = Path(temporary.name) / "stage.sqlite3"
    connection = sqlite3.connect(database)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("PRAGMA cache_size=-32768")
    connection.execute(
        """CREATE TABLE bars (
            row_id INTEGER PRIMARY KEY,
            sort_event TEXT,
            sort_start TEXT,
            timestamp TEXT,
            symbol TEXT,
            exchange TEXT,
            series TEXT,
            timeframe TEXT,
            open TEXT,
            high TEXT,
            low TEXT,
            close TEXT,
            volume TEXT,
            volume_kind TEXT,
            status TEXT,
            source TEXT,
            source_timezone TEXT,
            source_error TEXT
        )"""
    )
    connection.execute("CREATE INDEX bars_event_order ON bars(sort_event, symbol, timeframe, sort_start, row_id)")
    connection.execute("CREATE INDEX bars_identity ON bars(symbol, timeframe, sort_start)")
    return temporary, connection


def _build_stage(root: Path, record: Mapping[str, Any], metadata: Mapping[str, Any],
                 *, batch_size: int = 10_000) -> tuple[tempfile.TemporaryDirectory, sqlite3.Connection, list[str]]:
    if record.get("data_type") == "tick":
        raise Phase4FError("TICK_AGGREGATION_NOT_IMPLEMENTED", "ticks are registered as raw-only; no bar aggregation is enabled")
    files = _registered_raw_files(root, record)
    schema = _validate_schema(root / str(record["source_path"]), files, metadata)
    if schema["data_type"] != "bars":
        raise Phase4FError("TICK_AGGREGATION_NOT_IMPLEMENTED", "tick aggregation is intentionally not implemented")
    timeframe = str(record.get("timeframe", "")).lower()
    if timeframe not in BAR_TYPES:
        raise Phase4FError("TIMEFRAME_INVALID", timeframe)
    declared_sessions = _expected_session_dates(metadata)
    declared_session_set = set(declared_sessions) if declared_sessions is not None else None
    temporary, connection = _open_stage()
    source_errors: list[str] = []
    def note_error(message: str) -> None:
        if len(source_errors) < 100:
            source_errors.append(message)
    row_id = 0
    insert_rows: list[tuple[Any, ...]] = []
    column_map = metadata.get("column_map", {})
    if not isinstance(column_map, Mapping):
        temporary.cleanup()
        connection.close()
        raise Phase4FError("DATASET_SCHEMA_UNIDENTIFIABLE", "column_map must be an object")
    special = _special_windows(metadata)
    try:
        for file in files:
            for member, raw in _row_iterator(file):
                row_id += 1
                row = _canonical_source_row(raw, metadata, column_map, row_number=row_id)
                row["open"] = _canonical_decimal(row["open"])
                row["high"] = _canonical_decimal(row["high"])
                row["low"] = _canonical_decimal(row["low"])
                row["close"] = _canonical_decimal(row["close"])
                row["volume"] = _canonical_decimal(row["volume"])
                row_source_errors = [error for error in (row.get("_timestamp_error"), row.get("_identity_error")) if error]
                for error in row_source_errors:
                    note_error(f"row {row_id}: {error}")
                source_error = ";".join(row_source_errors)
                parsed_timestamp: datetime | None = None
                sort_start = sort_event = source_timezone = None
                try:
                    parsed_timestamp = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
                    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
                        parsed_timestamp = None
                    else:
                        source_timezone = str(parsed_timestamp.tzinfo.key if isinstance(parsed_timestamp.tzinfo, ZoneInfo)
                                             else parsed_timestamp.tzname() or parsed_timestamp.utcoffset())
                        local = parsed_timestamp.astimezone(IST)
                        sort_start = local.isoformat()
                        sort_event = (local + timedelta(minutes=TIMEFRAME_MINUTES[timeframe])).isoformat()
                except (TypeError, ValueError, OverflowError):
                    parsed_timestamp = None
                if parsed_timestamp is not None:
                    local_date = parsed_timestamp.astimezone(IST).date()
                    session = validate_session_timestamp(parsed_timestamp, timeframe=timeframe,
                                                         special_session_windows=special, calendar=default_nse_calendar())
                    if session.status not in {"NORMAL_SESSION", "SPECIAL_SESSION"}:
                        session_error = f"SESSION_{session.status}"
                        note_error(f"row {row_id}: {session_error}: {session.reason}")
                        source_error = ";".join(item for item in (source_error, session_error) if item)
                    if declared_session_set is not None and local_date not in declared_session_set:
                        session_error = "SESSION_UNEXPECTED_DATE"
                        note_error(f"row {row_id}: {session_error}: {local_date.isoformat()} is absent from expected_sessions")
                        source_error = ";".join(item for item in (source_error, session_error) if item)
                insert_rows.append((sort_event, sort_start, row["timestamp"], row["symbol"], row["exchange"],
                                    row["series"], row["timeframe"], row["open"], row["high"], row["low"],
                                    row["close"], row["volume"], row["volume_kind"], row["status"], row["source"],
                                    source_timezone, source_error))
                if len(insert_rows) >= batch_size:
                    connection.executemany(
                        "INSERT INTO bars(sort_event,sort_start,timestamp,symbol,exchange,series,timeframe,open,high,low,close,volume,volume_kind,status,source,source_timezone,source_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        insert_rows,
                    )
                    connection.commit()
                    insert_rows.clear()
        if insert_rows:
            connection.executemany(
                "INSERT INTO bars(sort_event,sort_start,timestamp,symbol,exchange,series,timeframe,open,high,low,close,volume,volume_kind,status,source,source_timezone,source_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                insert_rows,
            )
            connection.commit()
        _convert_declared_cumulative_volume(connection, metadata, source_errors)
        return temporary, connection, source_errors
    except Exception:
        connection.close()
        temporary.cleanup()
        raise


def _convert_declared_cumulative_volume(connection: sqlite3.Connection, metadata: Mapping[str, Any],
                                        source_errors: list[str]) -> None:
    semantics = str(metadata.get("volume_semantics") or "UNKNOWN").strip().upper()
    if metadata.get("cumulative_to_incremental") is not True:
        return
    if semantics not in {"CUMULATIVE_SHARES", "CUMULATIVE_SHARE_VOLUME_WITH_VERIFIED_INCREMENT_RULE"}:
        if len(source_errors) < 100:
            source_errors.append("cumulative conversion requested without declared CUMULATIVE_SHARES semantics")
        connection.execute("UPDATE bars SET source_error=COALESCE(source_error || ';','') || 'CUMULATIVE_SEMANTICS_NOT_DECLARED'")
        return
    if not str(metadata.get("cumulative_increment_rule_documentation") or "").strip():
        if len(source_errors) < 100:
            source_errors.append("cumulative conversion requested without verified increment-rule documentation")
        connection.execute("UPDATE bars SET source_error=COALESCE(source_error || ';','') || 'CUMULATIVE_RULE_UNDOCUMENTED'")
        return
    from decimal import Decimal, InvalidOperation

    prior: dict[tuple[str, str], Decimal] = {}
    rows = connection.execute("SELECT row_id,symbol,sort_start,volume FROM bars ORDER BY symbol,sort_start,row_id")
    updates: list[tuple[str, int]] = []
    for row_id, symbol, stamp, raw_volume in rows:
        if not stamp:
            continue
        try:
            day = str(stamp)[:10]
            current = Decimal(str(raw_volume))
        except (InvalidOperation, TypeError, ValueError):
            continue
        key = (str(symbol), day)
        previous = prior.get(key)
        if previous is not None and current < previous:
            message = f"row {row_id}: cumulative volume reset/decreased within session"
            if len(source_errors) < 100:
                source_errors.append(message)
            connection.execute("UPDATE bars SET source_error=COALESCE(source_error || ';','') || 'CUMULATIVE_VOLUME_RESET' WHERE row_id=?", (row_id,))
            continue
        delta = current if previous is None else current - previous
        prior[key] = current
        updates.append((_canonical_decimal(delta), row_id))
        if len(updates) >= 10_000:
            connection.executemany("UPDATE bars SET volume=?,volume_kind='BAR' WHERE row_id=?", updates)
            connection.commit()
            updates.clear()
    if updates:
        connection.executemany("UPDATE bars SET volume=?,volume_kind='BAR' WHERE row_id=?", updates)
        connection.commit()


def _stage_records(connection: sqlite3.Connection, batch_size: int) -> Iterator[list[dict[str, Any]]]:
    cursor = connection.execute(
        "SELECT row_id,timestamp,symbol,exchange,series,timeframe,open,high,low,close,volume,volume_kind,status,source,source_timezone,source_error FROM bars ORDER BY sort_event,symbol,timeframe,sort_start,row_id"
    )
    while True:
        values = cursor.fetchmany(batch_size)
        if not values:
            break
        batch = []
        for row in values:
            row_id, timestamp, symbol, exchange, series, timeframe, open_, high, low, close, volume, volume_kind, status, source, source_timezone, source_error = row
            batch.append({
                "timestamp": timestamp, "symbol": symbol, "exchange": exchange, "series": series,
                "timeframe": timeframe, "open": open_, "high": high, "low": low, "close": close,
                "volume": volume, "volume_kind": volume_kind, "status": status, "source": source,
                "_row_id": row_id, "_source_timezone": source_timezone, "_source_error": source_error,
            })
        yield batch


def _merge_phase4e_reports(reports: Sequence[IntradayValidationReport], connection: sqlite3.Connection,
                          expected_symbols: Sequence[str], expected_sessions: Sequence[date] | None,
                          metadata: Mapping[str, Any]) -> IntradayValidationReport:
    merged = IntradayValidationReport()
    issue_samples: list[ValidationIssue] = []
    for report in reports:
        merged.rows += report.rows
        merged.valid_rows += report.valid_rows
        merged.invalid_rows += report.invalid_rows
        merged.out_of_order_rows += report.out_of_order_rows
        for code, count in report.issue_counts.items():
            merged.issue_counts[code] = merged.issue_counts.get(code, 0) + count
        for item in report.issues:
            row_number = item.row
            issue_samples.append(ValidationIssue(item.code, item.message, row=row_number,
                                                 symbol=item.symbol, timestamp=item.timestamp))
    row_count = int(connection.execute("SELECT COUNT(*) FROM bars").fetchone()[0])
    duplicate_rows = int(connection.execute(
        "SELECT COALESCE(SUM(n-1),0) FROM (SELECT COUNT(*) AS n FROM bars WHERE sort_start IS NOT NULL GROUP BY symbol,timeframe,sort_start HAVING COUNT(*)>1)"
    ).fetchone()[0])
    merged.rows = row_count
    merged.duplicate_rows = duplicate_rows
    if duplicate_rows:
        merged.issue_counts["DUPLICATE_BAR"] = duplicate_rows
    else:
        merged.issue_counts.pop("DUPLICATE_BAR", None)
    # Phase 4E checks duplicates inside each bounded batch; SQLite supplies the
    # global exact count, including duplicates that cross a batch boundary.
    batch_duplicate_counts = sum(report.duplicate_rows for report in reports)
    cross_batch_duplicates = max(0, duplicate_rows - batch_duplicate_counts)
    merged.invalid_rows += cross_batch_duplicates
    merged.valid_rows = max(0, merged.valid_rows - cross_batch_duplicates)
    merged.symbols = [row[0] for row in connection.execute("SELECT DISTINCT symbol FROM bars WHERE symbol!='' ORDER BY symbol")]
    merged.missing_symbols = sorted(set(expected_symbols) - set(merged.symbols))
    if merged.missing_symbols:
        merged.issue_counts["MISSING_SYMBOLS"] = len(merged.missing_symbols)
        issue_samples.append(ValidationIssue("MISSING_SYMBOLS", "expected symbols absent: " + ", ".join(merged.missing_symbols)))
    merged.exchanges = [row[0] for row in connection.execute("SELECT DISTINCT COALESCE(NULLIF(exchange,''),'MISSING') FROM bars ORDER BY 1")]
    merged.series_values = [row[0] for row in connection.execute("SELECT DISTINCT COALESCE(NULLIF(series,''),'MISSING') FROM bars ORDER BY 1")]
    merged.timeframes = [row[0] for row in connection.execute("SELECT DISTINCT COALESCE(NULLIF(timeframe,''),'MISSING') FROM bars ORDER BY 1")]
    merged.source_timezones = [row[0] for row in connection.execute("SELECT DISTINCT source_timezone FROM bars WHERE source_timezone IS NOT NULL ORDER BY 1")]
    timestamp_values = connection.execute("SELECT MIN(sort_start),MAX(sort_start),COUNT(DISTINCT substr(sort_start,1,10)) FROM bars WHERE sort_start IS NOT NULL").fetchone()
    first, last, sessions = timestamp_values
    merged.first_timestamp = first
    merged.last_timestamp = last
    merged.first_date = first[:10] if first else None
    merged.last_date = last[:10] if last else None
    merged.sessions = int(sessions or 0)
    if duplicate_rows:
        merged.invalid_rows += 0
    if expected_sessions is not None:
        observed_dates = {row[0] for row in connection.execute("SELECT DISTINCT substr(sort_start,1,10) FROM bars WHERE sort_start IS NOT NULL")}
        missing = sorted(day.isoformat() for day in expected_sessions if day.isoformat() not in observed_dates)
        merged.session_coverage = {"expected_sessions": len(expected_sessions), "observed_sessions": len(observed_dates),
                                   "missing_sessions": missing, "missing_session_count": len(missing),
                                   "gap_policy": "reported only; no bars or sessions synthesized"}
        if missing:
            merged.issue_counts["MISSING_SESSIONS"] = len(missing)
            issue_samples.append(ValidationIssue("MISSING_SESSIONS", "one or more explicitly expected sessions have no rows"))
    else:
        merged.session_coverage = {"status": "PARTIAL_CALENDAR", "expected_sessions": None,
                                   "reason": "no source-verified expected session calendar supplied",
                                   "gap_policy": "observed-session gaps reported; absent dates are not labeled holidays or gaps"}
    source_error_rows = int(connection.execute(
        "SELECT COUNT(*) FROM bars WHERE source_error IS NOT NULL AND source_error!=''"
    ).fetchone()[0])
    for row_id, error, symbol, timestamp in connection.execute(
        "SELECT row_id,source_error,symbol,timestamp FROM bars WHERE source_error IS NOT NULL AND source_error!='' ORDER BY row_id"
    ):
        for code in sorted(set(part.partition(": ")[0] for part in str(error).split(";") if part)):
            merged.issue_counts[code] = merged.issue_counts.get(code, 0) + 1
            if len(issue_samples) < 100:
                issue_samples.append(ValidationIssue(code, str(error), row=int(row_id), symbol=symbol, timestamp=timestamp))
    merged.invalid_rows = max(merged.invalid_rows, source_error_rows)
    merged.issues = issue_samples[:100]
    # Duplicate counts are represented once in the aggregate issue count.
    if duplicate_rows:
        merged.issue_counts["DUPLICATE_BAR"] = duplicate_rows
    return merged


def _coverage_report(connection: sqlite3.Connection, timeframe: str, metadata: Mapping[str, Any],
                     report: IntradayValidationReport) -> dict[str, Any]:
    from decimal import Decimal, InvalidOperation

    interval = TIMEFRAME_MINUTES[timeframe]
    expected_sessions = _expected_session_dates(metadata)
    expected_symbols = _expected_symbols(metadata)
    special = _special_windows(metadata)
    calendar = default_nse_calendar()
    # Keep only the bars for one symbol/session in memory. This stays bounded
    # by the regular session size even when the raw source has many years.
    counts_by_symbol: dict[str, list[int]] = {}
    symbol_dates: dict[str, list[str]] = {}
    duplicate_by_symbol = {
        str(symbol): int(count)
        for symbol, count in connection.execute(
            "SELECT symbol,COALESCE(SUM(n-1),0) FROM (SELECT symbol,COUNT(*) n FROM bars WHERE sort_start IS NOT NULL GROUP BY symbol,timeframe,sort_start HAVING COUNT(*)>1) GROUP BY symbol"
        )
    }
    coverage_by_symbol: list[dict[str, Any]] = []
    coverage_by_session: dict[str, dict[str, Any]] = {}
    observed_dates: set[str] = set()
    total_missing_intervals = 0
    total_expected_intervals = 0
    explicit_no_trade = set()
    for item in metadata.get("no_trade_symbol_sessions", []) if isinstance(metadata.get("no_trade_symbol_sessions", []), list) else []:
        if isinstance(item, Mapping) and item.get("symbol") and item.get("date"):
            explicit_no_trade.add((str(item["symbol"]).strip().upper(), str(item["date"])))
    def expected_slot_count(day: date) -> int:
        windows = special.get(day, (("09:15", "15:30"),))
        count = 0
        for start_text, end_text in windows:
            start_minutes = time.fromisoformat(str(start_text)).hour * 60 + time.fromisoformat(str(start_text)).minute
            end_minutes = time.fromisoformat(str(end_text)).hour * 60 + time.fromisoformat(str(end_text)).minute
            count += max(0, (end_minutes - start_minutes) // interval)
        return count

    coverage_by_symbol_index: dict[str, dict[str, Any]] = {}
    current_key: tuple[str, str] | None = None
    stamps: set[datetime] = set()
    row_count = 0
    zero_volume = 0
    previous_volume: Decimal | None = None
    previous_volume_timestamp: str | None = None
    volume_comparison_count = 0
    maximum_volume_multiple = Decimal(1)
    maximum_volume_multiple_detail: dict[str, str] | None = None

    def flush_group() -> None:
        nonlocal row_count, zero_volume, total_missing_intervals, total_expected_intervals
        if current_key is None:
            return
        symbol, day_text = current_key
        day = date.fromisoformat(day_text)
        observed_dates.add(day_text)
        expected_count = expected_slot_count(day)
        windows = special.get(day, (("09:15", "15:30"),))
        present_count = 0
        for stamp in stamps:
            minute_of_day = stamp.hour * 60 + stamp.minute
            for start_text, end_text in windows:
                start = time.fromisoformat(str(start_text))
                end = time.fromisoformat(str(end_text))
                start_minute = start.hour * 60 + start.minute
                end_minute = end.hour * 60 + end.minute
                if (minute_of_day >= start_minute and minute_of_day + interval <= end_minute
                        and (minute_of_day - start_minute) % interval == 0):
                    present_count += 1
                    break
        missing = max(0, expected_count - present_count)
        total_missing_intervals += missing
        total_expected_intervals += expected_count
        counts_by_symbol.setdefault(symbol, []).append(row_count)
        symbol_dates.setdefault(symbol, []).append(day_text)
        session_kind = "HOLIDAY" if day in calendar.holidays else "SPECIAL_SESSION" if day in special else "NORMAL_SESSION"
        if day_text not in coverage_by_session:
            coverage_by_session[day_text] = {"date": day_text, "session_classification": session_kind,
                                             "symbols_expected": None, "symbols_present": 0,
                                             "coverage_percent": None, "bars_expected": None,
                                             "bars_present": 0, "missing_intervals": 0,
                                             "complete_symbols": 0, "partial_symbols": 0,
                                             "symbol_specific_no_trade": 0}
        session = coverage_by_session[day_text]
        session["symbols_present"] += 1
        session["bars_present"] += row_count
        session["missing_intervals"] += missing
        session["zero_volume_bars"] = session.get("zero_volume_bars", 0) + zero_volume
        if missing:
            session["partial_symbols"] += 1
        else:
            session["complete_symbols"] += 1
        row_count = 0
        zero_volume = 0
        stamps.clear()

    for symbol_value, stamp_value, volume_value in connection.execute(
        "SELECT symbol,sort_start,volume FROM bars WHERE sort_start IS NOT NULL ORDER BY symbol,substr(sort_start,1,10),sort_start,row_id"
    ):
        stamp_text = str(stamp_value)
        key = (str(symbol_value), stamp_text[:10])
        if current_key is not None and key != current_key:
            flush_group()
            previous_volume = None
            previous_volume_timestamp = None
        current_key = key
        try:
            stamps.add(datetime.fromisoformat(stamp_text))
        except ValueError:
            pass
        row_count += 1
        try:
            current_volume = Decimal(str(volume_value))
            if current_volume == 0:
                zero_volume += 1
            if previous_volume is not None and previous_volume > 0 and current_volume > 0:
                volume_comparison_count += 1
                ratio = max(current_volume / previous_volume, previous_volume / current_volume)
                if ratio > maximum_volume_multiple:
                    maximum_volume_multiple = ratio
                    maximum_volume_multiple_detail = {
                        "symbol": str(symbol_value),
                        "previous_timestamp": previous_volume_timestamp or "UNKNOWN",
                        "timestamp": stamp_text,
                    }
            previous_volume = current_volume
            previous_volume_timestamp = stamp_text
        except (InvalidOperation, TypeError, ValueError, OverflowError):
            previous_volume = None
            previous_volume_timestamp = None
    flush_group()

    for symbol in sorted(set(report.symbols) | set(expected_symbols)):
        counts = counts_by_symbol.get(symbol, [])
        symbol_day_list = symbol_dates.get(symbol, [])
        missing_session_dates = ([day.isoformat() for day in expected_sessions
                                  if day.isoformat() not in symbol_day_list
                                  and (symbol, day.isoformat()) not in explicit_no_trade]
                                 if expected_sessions is not None else None)
        coverage_by_symbol.append({
            "symbol": symbol,
            "first_date": min(symbol_day_list) if symbol_day_list else None,
            "last_date": max(symbol_day_list) if symbol_day_list else None,
            "sessions": len(set(symbol_day_list)),
            "rows": sum(counts),
            "median_bars_per_session": median(counts) if counts else 0,
            "min_bars_per_session": min(counts) if counts else 0,
            "max_bars_per_session": max(counts) if counts else 0,
            "missing_sessions": missing_session_dates,
            "duplicate_bars": duplicate_by_symbol.get(symbol, 0),
        })

    all_symbols = report.symbols
    selected_expected_symbols = expected_symbols or all_symbols
    observed_days = [date.fromisoformat(item) for item in sorted(observed_dates)]
    dates = sorted(set(expected_sessions or []) | set(observed_days))

    for day in dates:
        day_text = day.isoformat()
        if day in calendar.holidays:
            session_classification = "HOLIDAY"
        elif day in special:
            session_classification = "SPECIAL_SESSION"
        elif day.weekday() >= 5:
            session_classification = "WEEKEND_OR_NONTRADING"
        else:
            session_classification = "NORMAL_SESSION_EXPECTED" if expected_sessions else "UNKNOWN_CALENDAR_WEEKDAY"
        coverage_by_session.setdefault(day_text, {
            "date": day_text, "session_classification": session_classification,
            "symbols_expected": None, "symbols_present": 0, "coverage_percent": None,
            "bars_expected": None, "bars_present": 0, "missing_intervals": 0,
            "complete_symbols": 0, "partial_symbols": 0, "symbol_specific_no_trade": 0,
        })

    # Reconcile the complete expected symbol/session matrix without loading all
    # pairs into RAM. The SQL cursor is sorted by session, so only one session's
    # observed symbol set is held at a time. Explicit no-trade declarations are
    # excluded from expected bar slots; undeclared absences remain gaps/unknowns.
    if selected_expected_symbols:
        pair_cursor = iter(connection.execute(
            "SELECT symbol,substr(sort_start,1,10) FROM bars "
            "WHERE sort_start IS NOT NULL GROUP BY symbol,substr(sort_start,1,10) "
            "ORDER BY substr(sort_start,1,10),symbol"
        ))
        next_pair = next(pair_cursor, None)
        expected_symbols_set = set(selected_expected_symbols)
        for day in dates:
            day_text = day.isoformat()
            present_today: set[str] = set()
            while next_pair is not None and str(next_pair[1]) < day_text:
                next_pair = next(pair_cursor, None)
            while next_pair is not None and str(next_pair[1]) == day_text:
                present_today.add(str(next_pair[0]))
                next_pair = next(pair_cursor, None)

            declared_no_trade = {
                symbol for symbol in expected_symbols_set
                if (symbol, day_text) in explicit_no_trade and symbol not in present_today
            }
            missing_symbols_today = sorted(expected_symbols_set - present_today - declared_no_trade)
            slots = expected_slot_count(day)
            missing_slots = len(missing_symbols_today) * slots
            expected_slots = (len(expected_symbols_set) - len(declared_no_trade)) * slots
            session = coverage_by_session[day_text]
            session["symbols_expected"] = len(expected_symbols_set)
            session["symbol_specific_no_trade"] = len(declared_no_trade)
            session["missing_intervals"] = int(session.get("missing_intervals", 0)) + missing_slots
            session["bars_expected"] = expected_slots
            # A no-trade declaration means no bar was expected for that symbol;
            # the ratio therefore uses only symbols expected to produce bars.
            expected_bar_symbols = len(expected_symbols_set) - len(declared_no_trade)
            session["coverage_percent"] = (
                session["symbols_present"] / expected_bar_symbols if expected_bar_symbols else 1.0
            )
            session["missing_symbol_sessions"] = [f"{symbol}:{day_text}" for symbol in missing_symbols_today]
            session["unclassified_missing_symbol_sessions"] = len(missing_symbols_today)
            session["partial_symbols"] = int(session.get("partial_symbols", 0)) + len(missing_symbols_today)
            if expected_sessions is not None and day_text not in observed_dates:
                session["classification_note"] = "DATASET_GAP: source-declared expected session has no observations"
            elif missing_symbols_today:
                session["classification_note"] = (
                    "missing symbols are not declared as no-trade; source evidence is needed to distinguish a data gap"
                )
        total_missing_intervals = sum(int(item.get("missing_intervals", 0)) for item in coverage_by_session.values())
        total_expected_intervals = sum(int(item.get("bars_expected") or 0) for item in coverage_by_session.values())
    expected_date_text = {day.isoformat() for day in expected_sessions or []}
    global_missing = sorted(expected_date_text - observed_dates)
    for day_text in global_missing:
        if day_text not in coverage_by_session:
            missing_day = date.fromisoformat(day_text)
            coverage_by_session[day_text] = {
                "date": day_text,
                "session_classification": "HOLIDAY" if missing_day in calendar.holidays else
                                          "SPECIAL_SESSION" if missing_day in special else
                                          "NORMAL_SESSION_EXPECTED",
                "symbols_expected": len(selected_expected_symbols),
                "symbols_present": 0,
                "coverage_percent": 0.0,
                "bars_expected": len(selected_expected_symbols) * expected_slot_count(missing_day),
                "bars_present": 0,
                "missing_intervals": 0,
                "complete_symbols": 0,
                "partial_symbols": 0,
                "symbol_specific_no_trade": sum((symbol, day_text) in explicit_no_trade for symbol in selected_expected_symbols),
                "missing_symbol_sessions": sorted(f"{symbol}:{day_text}" for symbol in selected_expected_symbols
                                                   if (symbol, day_text) not in explicit_no_trade),
                "classification_note": "DATASET_GAP: source-declared expected session has no observations",
            }
    return {
        "timeframe": timeframe,
        "timezone": "Asia/Kolkata",
        "coverage_start": report.first_date,
        "coverage_end": report.last_date,
        "trading_sessions": len(observed_dates),
        "symbols": len(all_symbols),
        "rows": report.rows,
        "bars_per_session": {
            "minimum": min((count for counts in counts_by_symbol.values() for count in counts), default=0),
            "median": median([count for counts in counts_by_symbol.values() for count in counts]) if counts_by_symbol else 0,
            "maximum": max((count for counts in counts_by_symbol.values() for count in counts), default=0),
        },
        "complete_sessions": sum(
            1 for row in coverage_by_session.values()
            if row.get("partial_symbols", 0) == 0
            and ((row.get("symbols_expected") is not None
                  and row.get("symbols_present", 0) + row.get("symbol_specific_no_trade", 0) >= row.get("symbols_expected", 0))
                 or (row.get("symbols_expected") is None and row.get("symbols_present", 0) > 0))
        ),
        "partial_sessions": sum(1 for row in coverage_by_session.values() if row.get("partial_symbols", 0) > 0),
        "missing_sessions": global_missing,
        "missing_session_count": len(global_missing),
        "missing_interval_count": total_missing_intervals,
        "expected_interval_count": total_expected_intervals,
        "zero_volume_bars": int(connection.execute("SELECT COUNT(*) FROM bars WHERE CAST(volume AS REAL)=0").fetchone()[0]),
        "negative_volume_bars": int(connection.execute("SELECT COUNT(*) FROM bars WHERE CAST(volume AS REAL)<0").fetchone()[0]),
        "volume_discontinuity_diagnostic": {
            "comparison_count": volume_comparison_count,
            "maximum_multiple_between_observed_rows": _canonical_decimal(maximum_volume_multiple),
            "maximum_change_location": maximum_volume_multiple_detail,
            "threshold": "NOT_CONFIGURED",
            "interpretation": "diagnostic only; no threshold, filtering, or volume alteration is applied",
        },
        "coverage_by_symbol": coverage_by_symbol,
        "coverage_by_session": [coverage_by_session[key] for key in sorted(coverage_by_session)],
        "symbol_specific_no_trade_sessions": sorted(f"{symbol}:{day}" for symbol, day in explicit_no_trade),
        "gap_classification": "explicit no-trade sessions are separated; undeclared missing intervals remain GAP_OR_NO_TRADE_UNKNOWN",
        "calendar_source": "existing NseTradingCalendar; source-verified expected_sessions are required to classify absent sessions as dataset gaps",
        "gap_policy": "report only; never forward-fill, interpolate, or synthesize rows",
    }


def _phase4e_provenance(record: Mapping[str, Any], metadata: Mapping[str, Any],
                        report: IntradayValidationReport) -> dict[str, Any]:
    def fact(key: str, *aliases: str) -> Any:
        for candidate in (key, *aliases):
            value = metadata.get(candidate)
            if value not in (None, "") and str(value).strip().upper() != "UNKNOWN":
                return value
        return None

    volume = str(metadata.get("volume_semantics") or "UNKNOWN").strip().upper()
    if volume == "INCREMENTAL_SHARES":
        volume = "INCREMENTAL_SHARE_VOLUME"
    elif volume == "BAR_SHARE_VOLUME":
        volume = "BAR_SHARE_VOLUME"
    elif volume in {"CUMULATIVE_SHARES", "CUMULATIVE_SHARE_VOLUME_WITH_VERIFIED_INCREMENT_RULE"}:
        volume = ("CUMULATIVE_SHARE_VOLUME_WITH_VERIFIED_INCREMENT_RULE"
                  if metadata.get("cumulative_to_incremental") is True
                  and str(metadata.get("cumulative_increment_rule_documentation") or "").strip()
                  else "CUMULATIVE_SHARES")
    return {
        "dataset": str(record.get("name") or "UNKNOWN"),
        "source_url": fact("source_url") or record.get("source_path"),
        "publisher": fact("publisher", "provider"),
        "revision": fact("revision"),
        "downloaded_at": fact("acquisition_timestamp", "downloaded_at"),
        "sha256": record.get("source_hash", "UNKNOWN"),
        "coverage_start": report.first_date or "UNKNOWN",
        "coverage_end": report.last_date or "UNKNOWN",
        "symbols": len(report.symbols),
        "rows": report.rows,
        "timeframe": record.get("timeframe", "UNKNOWN"),
        "timezone": "Asia/Kolkata",
        "exchange": "NSE",
        "series": "EQ",
        "volume_semantics": volume,
        "license": fact("license"),
        "original_provider": fact("provider"),
        "redistribution_terms": fact("redistribution_terms"),
        "research_allowed": metadata.get("research_allowed") is True,
        "commercial_allowed": metadata.get("commercial_allowed") is True,
        "universe_bias": fact("universe_bias"),
        "preferred_coverage_start": metadata.get("preferred_coverage_start", "2021-01-01"),
        "preferred_coverage_end": metadata.get("preferred_coverage_end", "2026-12-31"),
    }


def _validation_report(root: Path, record: Mapping[str, Any], metadata: Mapping[str, Any],
                       *, batch_size: int = 10_000) -> tuple[ValidationResult, dict[str, Any], IntradayValidationReport, dict[str, Any]]:
    if record.get("data_type") == "tick":
        result = ValidationResult("FAIL", errors=[{"code": "TICK_AGGREGATION_NOT_IMPLEMENTED",
                                                    "message": "raw tick data is registered but cannot be converted into bars in this phase"}],
                                 warnings=[], statistics={"data_type": "tick", "rows": None},
                                 phase4e_validation_status="NOT_APPLICABLE")
        empty = IntradayValidationReport()
        return result, {"coverage_by_symbol": [], "coverage_by_session": []}, empty, {}
    temporary, connection, source_errors = _build_stage(root, record, metadata, batch_size=batch_size)
    try:
        phase_reports: list[IntradayValidationReport] = []
        expected_sessions = _expected_session_dates(metadata)
        expected_symbols = _expected_symbols(metadata)
        for batch in _stage_records(connection, batch_size):
            phase_reports.append(validate_intraday_dataset(
                [{key: value for key, value in row.items() if not key.startswith("_")} for row in batch],
                timeframe=str(record["timeframe"]), issue_sample_limit=100,
            ))
        merged = _merge_phase4e_reports(phase_reports, connection, expected_symbols, expected_sessions, metadata)
        coverage = _coverage_report(connection, str(record["timeframe"]), metadata, merged)
        provenance = _phase4e_provenance(record, metadata, merged)
        readiness = classify_dataset(merged, provenance, required_symbols=expected_symbols)
        if not expected_sessions:
            # The project calendar is intentionally shared with the existing
            # system and is not a complete historical NSE calendar. Require
            # delivery-supplied sessions before promoting multi-year data.
            readiness = "NOT_RESEARCH_READY"
        adjustment_status = str(record.get("adjustment_status") or "UNKNOWN").strip().upper()
        adjustment_documentation = str(metadata.get("adjustment_status_documentation") or "").strip()
        if adjustment_status not in {"RAW", "ADJUSTED"} or not adjustment_documentation:
            readiness = "NOT_RESEARCH_READY"
        external_errors = []
        for error in source_errors[:100]:
            if any(token in error for token in ("UNKNOWN", "UNDOCUMENTED", "MISMATCH", "INVALID", "RESET", "HOLIDAY", "WEEKEND", "OUTSIDE_SESSION", "SPECIAL_SESSION")):
                message = re.sub(r"^row\s+\d+:\s*", "", error)
                code, _, detail = message.partition(": ")
                external_errors.append({"code": code, "message": detail or message})
        for row in merged.issues:
            external_errors.append({"code": row.code, "message": row.message, "row": row.row,
                                   "symbol": row.symbol, "timestamp": row.timestamp})
        if merged.validation_status == "PASS" and not source_errors:
            phase4e_status = "PASS"
        else:
            phase4e_status = "FAIL"
        warnings = []
        if readiness == "NOT_RESEARCH_READY":
            warnings.append("Phase 4E research-readiness gates are not satisfied; replay remains blocked")
        if coverage.get("missing_interval_count"):
            warnings.append(f"{coverage['missing_interval_count']} intraday intervals are absent; no bars were filled")
        if metadata.get("volume_semantics", "UNKNOWN") == "UNKNOWN":
            warnings.append("volume semantics are UNKNOWN; volume-dependent research is not ready")
        if not expected_sessions:
            warnings.append("no source-verified expected-session calendar supplied; research readiness is blocked and absent dates are not labeled as holidays or gaps")
        if adjustment_status not in {"RAW", "ADJUSTED"} or not adjustment_documentation:
            warnings.append("price adjustment basis is UNKNOWN or undocumented; research readiness is blocked")
        validation_status = "FAIL" if phase4e_status != "PASS" else "PASS_WITH_WARNINGS" if warnings else "PASS"
        result = ValidationResult(validation_status, errors=external_errors[:100], warnings=warnings,
                                  statistics={"phase4e": merged.to_dict(), "coverage": coverage,
                                              "provenance": provenance,
                                              "source_errors": int(connection.execute(
                                                  "SELECT COUNT(*) FROM bars WHERE source_error IS NOT NULL AND source_error!=''"
                                              ).fetchone()[0])},
                                  phase4e_validation_status=phase4e_status,
                                  research_ready=readiness in {"RESEARCH_READY", "RESEARCH_READY_WITH_LIMITATIONS"},
                                  research_readiness=readiness)
        return result, coverage, merged, provenance
    finally:
        connection.close()
        temporary.cleanup()


def _write_csv(path: Path, headers: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(headers), extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def validate_dataset(dataset_id: str, *, project_root: str | Path = PROJECT_ROOT,
                     batch_size: int = 10_000) -> dict[str, Any]:
    root, record, metadata = _get_dataset(dataset_id, project_root)
    result, coverage, report, provenance = _validation_report(root, record, metadata, batch_size=batch_size)
    result_dir = root / "results/phase4f" / dataset_id
    result_dir.mkdir(parents=True, exist_ok=True)
    report_document = result.to_dict()
    report_document["dataset_id"] = dataset_id
    _json_write_atomic(result_dir / "validation_report.json", report_document)
    _json_write_atomic(result_dir / "coverage_report.json", coverage)
    _write_csv(result_dir / "coverage_by_symbol.csv",
               ("symbol", "first_date", "last_date", "sessions", "rows", "median_bars_per_session",
                "min_bars_per_session", "max_bars_per_session", "missing_sessions", "duplicate_bars"),
               ({**item, "missing_sessions": json.dumps(item["missing_sessions"])}
                for item in coverage.get("coverage_by_symbol", [])))
    _write_csv(result_dir / "coverage_by_session.csv",
               ("date", "session_classification", "symbols_expected", "symbols_present", "coverage_percent",
                "bars_expected", "bars_present", "missing_intervals", "complete_symbols", "partial_symbols",
                "symbol_specific_no_trade", "zero_volume_bars", "missing_symbol_sessions",
                "unclassified_missing_symbol_sessions", "classification_note"),
               ({**item, "missing_symbol_sessions": json.dumps(item.get("missing_symbol_sessions", []))}
                for item in coverage.get("coverage_by_session", [])))
    registry_path = root / "data/intraday/datasets.json"
    registry = _registry_read(registry_path)
    current = next(item for item in registry["datasets"] if item["dataset_id"] == dataset_id)
    current.update({
        "rows": report.rows,
        "symbols": len(report.symbols),
        "coverage_start": report.first_date or "UNKNOWN",
        "coverage_end": report.last_date or "UNKNOWN",
        "timezone": ",".join(report.source_timezones) if report.source_timezones else "UNKNOWN",
        "exchange": ",".join(report.exchanges) if report.exchanges else "UNKNOWN",
        "series": ",".join(report.series_values) if report.series_values else "UNKNOWN",
        "validation_status": result.phase4e_validation_status,
        "validation_result": result.status,
        "research_readiness": result.research_readiness,
        "research_ready": result.research_ready,
        "validation_report": str((result_dir / "validation_report.json").relative_to(root)),
        "validation_report_sha256": sha256_file(result_dir / "validation_report.json"),
        "coverage_report": str((result_dir / "coverage_report.json").relative_to(root)),
        "phase4e_provenance": provenance,
    })
    _json_write_atomic(registry_path, registry)
    cert_path = result_dir / ("ACCEPTED_DATASET.json" if result.phase4e_validation_status == "PASS" else "REJECTED_DATASET.json")
    other_cert = result_dir / ("REJECTED_DATASET.json" if cert_path.name.startswith("ACCEPTED") else "ACCEPTED_DATASET.json")
    other_cert.unlink(missing_ok=True)
    certificate = {
        "dataset_id": dataset_id,
        "validation_status": result.phase4e_validation_status,
        "validation_result": result.status,
        "research_ready": result.research_ready,
        "research_readiness": result.research_readiness,
        "source_hash": record.get("source_hash"),
        "coverage": {key: coverage.get(key) for key in ("coverage_start", "coverage_end", "trading_sessions", "symbols", "rows", "complete_sessions", "partial_sessions", "missing_sessions", "missing_interval_count")},
        "symbols": report.symbols,
        "timeframe": record.get("timeframe"),
        "timezone": "Asia/Kolkata",
        "exchange": report.exchanges,
        "series": report.series_values,
        "volume_semantics": record.get("volume_semantics", "UNKNOWN"),
        "license_status": record.get("license_status", "UNKNOWN"),
        "accepted_at": datetime.now(timezone.utc).isoformat() if result.phase4e_validation_status == "PASS" else None,
        "failure_reasons": result.errors if result.phase4e_validation_status != "PASS" else [],
    }
    _json_write_atomic(cert_path, certificate)
    if result.phase4e_validation_status != "PASS":
        quarantine = root / "data/intraday/quarantine" / f"{dataset_id}.json"
        _json_write_atomic(quarantine, {"dataset_id": dataset_id, "original_filename": record.get("name"),
                                       "source_hash": record.get("source_hash"), "reason": "PHASE4F_DATA_REJECTED",
                                       "validation_report": str((result_dir / "validation_report.json").relative_to(root)),
                                       "failure_reasons": result.errors})
    return {"dataset_id": dataset_id, "result": result.to_dict(), "certificate": str(cert_path.relative_to(root))}


def _write_canonical_output(connection: sqlite3.Connection, path: Path, *, batch_size: int = 10_000) -> tuple[int, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    rows_written = 0
    try:
        with temporary.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(CANONICAL_COLUMNS), extrasaction="ignore")
            writer.writeheader()
            for batch in _stage_records(connection, batch_size):
                for row in batch:
                    timestamp = validate_timezone(row["timestamp"], canonical_timezone="Asia/Kolkata")
                    item = IntradayBar(
                        symbol=str(row["symbol"]), exchange=str(row["exchange"]), series=str(row["series"]),
                        timeframe=str(row["timeframe"]), timestamp=timestamp,
                        open=float(row["open"]), high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
                        volume=float(row["volume"]), volume_kind=str(row["volume_kind"]), status=str(row["status"]), source=str(row["source"]),
                    )
                    normalized = validate_intraday_bar(item, require_closed=True)
                    writer.writerow({
                        "timestamp": normalized.timestamp.isoformat(), "symbol": normalized.symbol,
                        "exchange": normalized.exchange, "series": normalized.series,
                        "timeframe": normalized.timeframe, "open": _canonical_decimal(row["open"]),
                        "high": _canonical_decimal(row["high"]), "low": _canonical_decimal(row["low"]),
                        "close": _canonical_decimal(row["close"]), "volume": _canonical_decimal(row["volume"]),
                        "volume_kind": normalized.volume_kind, "status": normalized.status, "source": normalized.source,
                    })
                    rows_written += 1
            stream.flush()
            os.fsync(stream.fileno())
        checksum = sha256_file(temporary)
        os.replace(temporary, path)
        return rows_written, checksum
    except (BarValidationError, ValueError, TypeError, OSError) as exc:
        temporary.unlink(missing_ok=True)
        code = exc.code if isinstance(exc, BarValidationError) else "NORMALIZATION_FAILED"
        raise Phase4FError(code, str(exc)) from exc
    finally:
        temporary.unlink(missing_ok=True)


def normalize_dataset(dataset_id: str, *, project_root: str | Path = PROJECT_ROOT,
                      dry_run: bool = False, batch_size: int = 10_000) -> dict[str, Any]:
    root, record, metadata = _get_dataset(dataset_id, project_root)
    if record.get("data_type") == "tick":
        raise Phase4FError("TICK_AGGREGATION_NOT_IMPLEMENTED", "raw tick data remains raw-only")
    report_path = root / str(record.get("validation_report", "")) if record.get("validation_report") else None
    if record.get("validation_status") != "PASS" or report_path is None or not report_path.is_file():
        raise Phase4FError("PHASE4F_DATA_REJECTED", "run validation and obtain Phase 4E PASS before normalization")
    try:
        prior_report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4FError("VALIDATION_REPORT_INVALID", str(exc)) from exc
    if prior_report.get("phase4e_validation_status") != "PASS":
        raise Phase4FError("PHASE4F_DATA_REJECTED", "latest Phase 4E validation did not PASS")
    current_files = _registered_raw_files(root, record)
    current_fingerprint = _dataset_fingerprint(_file_manifest(current_files, root / str(record["raw_path"])))
    if current_fingerprint != record.get("source_hash"):
        raise Phase4FError("SOURCE_HASH_MISMATCH", "raw dataset changed after registration/validation")
    normalized_dir = root / "data/intraday/normalized" / dataset_id
    normalized_file = normalized_dir / "canonical_intraday_bars.csv"
    manifest_path = normalized_dir / "manifest.json"
    lock_path = normalized_dir / ".dataset.lock"
    if dry_run:
        return {"status": "DRY_RUN", "dataset_id": dataset_id, "output": str(normalized_file),
                "source_hash": current_fingerprint, "would_write": [str(normalized_file), str(manifest_path)]}
    with DatasetLock(lock_path):
        if manifest_path.is_file():
            try:
                previous = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                previous = {}
            if previous.get("status") == "COMPLETE" and previous.get("source_hash") == current_fingerprint:
                if (normalized_file.is_file() and sha256_file(normalized_file) == previous.get("normalized_hash")
                        and record.get("normalized_manifest_sha256") == sha256_file(manifest_path)
                        and normalized_file.stat().st_size > 0):
                    return {"status": "ALREADY_COMPLETE", "dataset_id": dataset_id,
                            "output": str(normalized_file.relative_to(root)), "manifest": previous}
                raise Phase4FError("NORMALIZED_DATASET_INCOMPLETE", "manifest says COMPLETE but output hash does not match")
        normalized_dir.mkdir(parents=True, exist_ok=True)
        _json_write_atomic(manifest_path, {"dataset_id": dataset_id, "status": "IN_PROGRESS",
                                           "source_hash": current_fingerprint,
                                           "started_at": datetime.now(timezone.utc).isoformat()})
        temporary, connection, source_errors = _build_stage(root, record, metadata, batch_size=batch_size)
        try:
            reports = [validate_intraday_dataset(
                [{key: value for key, value in row.items() if not key.startswith("_")} for row in batch],
                timeframe=str(record["timeframe"]), issue_sample_limit=100,
            ) for batch in _stage_records(connection, batch_size)]
            merged = _merge_phase4e_reports(reports, connection, _expected_symbols(metadata),
                                            _expected_session_dates(metadata), metadata)
            if merged.validation_status != "PASS" or source_errors:
                _json_write_atomic(manifest_path, {"dataset_id": dataset_id, "status": "INCOMPLETE",
                                                   "source_hash": current_fingerprint,
                                                   "reason": "PHASE4E revalidation failed before canonical write"})
                raise Phase4FError("PHASE4F_DATA_REJECTED", "source failed revalidation; canonical output was not committed")
            rows_written, normalized_hash = _write_canonical_output(connection, normalized_file, batch_size=batch_size)
            coverage = _coverage_report(connection, str(record["timeframe"]), metadata, merged)
            manifest = {
                "schema_version": 1,
                "dataset_id": dataset_id,
                "status": "COMPLETE",
                "source_hash": current_fingerprint,
                "source_file_manifest": record.get("file_manifest", []),
                "normalized_file": normalized_file.name,
                "normalized_hash": normalized_hash,
                "row_count": rows_written,
                "symbol_count": len(merged.symbols),
                "symbols": merged.symbols,
                "coverage": coverage,
                "timeframe": record.get("timeframe"),
                "timezone": "Asia/Kolkata",
                "original_timezone": ",".join(merged.source_timezones) if merged.source_timezones else "UNKNOWN",
                "exchange": merged.exchanges,
                "series": merged.series_values,
                "volume_semantics": record.get("volume_semantics", "UNKNOWN"),
                "timestamp_convention_source": record.get("timestamp_convention", "UNKNOWN"),
                "timestamp_convention_canonical": "BAR_START",
                "adjustment_status": record.get("adjustment_status", "UNKNOWN"),
                "validation_status": "PASS",
                "research_ready": prior_report.get("research_ready") is True,
                "normalization_order": "bar_end, symbol, timeframe, bar_start",
            }
            manifest["complete_at"] = datetime.now(timezone.utc).isoformat()
            _json_write_atomic(manifest_path, manifest)
            registry_path = root / "data/intraday/datasets.json"
            registry = _registry_read(registry_path)
            current = next(item for item in registry["datasets"] if item["dataset_id"] == dataset_id)
            current.update({"normalized_hash": normalized_hash, "normalized_status": "COMPLETE",
                            "normalized_manifest": str(manifest_path.relative_to(root)),
                            "normalized_manifest_sha256": sha256_file(manifest_path)})
            _json_write_atomic(registry_path, registry)
            return {"status": "COMPLETE", "dataset_id": dataset_id,
                    "output": str(normalized_file.relative_to(root)), "manifest": manifest}
        finally:
            connection.close()
            temporary.cleanup()


def assert_research_ready(dataset_id: str | None = None, *, project_root: str | Path = PROJECT_ROOT) -> dict[str, Any]:
    root = Path(project_root).resolve()
    registry_path = root / "data/intraday/datasets.json"
    registry = _registry_read(registry_path)
    candidates = registry["datasets"]
    if dataset_id:
        candidates = [item for item in candidates if item.get("dataset_id") == dataset_id]
        if not candidates:
            raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", f"dataset is not registered: {dataset_id}")
    else:
        candidates = [item for item in candidates if item.get("research_ready") is True]
    if not candidates:
        raise Phase4FError("PHASE4F_NO_RESEARCH_READY_DATA", "no registered dataset passed the Phase 4E research-readiness gate")
    item = candidates[0]
    if item.get("research_ready") is not True or item.get("validation_status") != "PASS":
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", str(item.get("research_readiness", item.get("validation_status"))))
    validation_report_path = root / str(item.get("validation_report") or "")
    if not validation_report_path.is_file():
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", "Phase 4E validation report is missing")
    if item.get("validation_report_sha256") != sha256_file(validation_report_path):
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", "Phase 4E validation report hash differs from the registry")
    try:
        validation_report = json.loads(validation_report_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", f"Phase 4E validation report is invalid: {exc}") from exc
    if (validation_report.get("phase4e_validation_status") != "PASS"
            or validation_report.get("research_ready") is not True
            or validation_report.get("research_readiness") != item.get("research_readiness")):
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", "Phase 4E report does not authorize research readiness")
    certificate = root / "results/phase4f" / str(item["dataset_id"]) / "ACCEPTED_DATASET.json"
    normalized = root / "data/intraday/normalized" / str(item["dataset_id"])
    manifest_path = normalized / "manifest.json"
    normalized_file = normalized / "canonical_intraday_bars.csv"
    if not certificate.is_file() or not manifest_path.is_file() or not normalized_file.is_file():
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", "accepted certificate or complete normalized dataset is missing")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4FError("NORMALIZED_DATASET_INCOMPLETE", str(exc)) from exc
    if (manifest.get("status") != "COMPLETE"
            or manifest.get("validation_status") != "PASS"
            or manifest.get("research_ready") is not True
            or manifest.get("source_hash") != item.get("source_hash")
            or item.get("normalized_manifest_sha256") != sha256_file(manifest_path)):
        raise Phase4FError("NORMALIZED_DATASET_INCOMPLETE", "completion marker, readiness state, or source fingerprint is invalid")
    try:
        accepted = json.loads(certificate.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", f"accepted certificate is invalid: {exc}") from exc
    if (accepted.get("dataset_id") != item.get("dataset_id")
            or accepted.get("validation_status") != "PASS"
            or accepted.get("research_ready") is not True
            or accepted.get("research_readiness") != item.get("research_readiness")
            or accepted.get("source_hash") != item.get("source_hash")):
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", "accepted certificate does not match the research-ready registry")
    metadata_hash = item.get("delivery_metadata_sha256", "UNKNOWN")
    metadata_file = root / str(item.get("raw_path", "")) / "delivery_metadata.json"
    if metadata_hash != "UNKNOWN" and (
        not metadata_file.is_file() or sha256_file(metadata_file) != metadata_hash
    ):
        raise Phase4FError("PHASE4F_DATA_NOT_RESEARCH_READY", "delivery metadata hash differs from the registered source")
    manifest_coverage = manifest.get("coverage", {})
    accepted_coverage = accepted.get("coverage", {})
    for key in ("coverage_start", "coverage_end", "trading_sessions", "symbols", "rows",
                "complete_sessions", "partial_sessions", "missing_sessions", "missing_interval_count"):
        if accepted_coverage.get(key) != manifest_coverage.get(key):
            raise Phase4FError("NORMALIZED_DATASET_INCOMPLETE", f"accepted certificate coverage differs at {key}")
    actual_hash = sha256_file(normalized_file)
    if actual_hash != manifest.get("normalized_hash") or actual_hash != item.get("normalized_hash"):
        raise Phase4FError("NORMALIZED_DATASET_INCOMPLETE", "canonical output hash does not match the manifest")
    _registered_raw_files(root, item)
    return {"status": "PHASE4F_READY", "dataset_id": item["dataset_id"],
            "normalized_path": str(normalized_file), "source_hash": item["source_hash"],
            "normalized_hash": actual_hash, "research_readiness": item.get("research_readiness"),
            "rows": manifest.get("row_count"), "symbols": manifest.get("symbol_count"),
            "timeframe": item.get("timeframe"), "coverage": manifest.get("coverage")}
