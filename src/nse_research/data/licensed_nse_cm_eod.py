"""Local-file import and validation for licensed NSE CM EOD deliveries.

This module deliberately has no network client. The licensed product's actual
file layout must be supplied through :class:`NseCmEodSchema` after NSE provides
an authorized sample and technical specification.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import re
import statistics
import tempfile
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence

import pandas as pd

from nse_research.data.providers import (
    HistoricalDataProvider,
    REQUIRED_COLUMNS,
    validate_and_normalize_bars,
)

CANONICAL_COLUMNS = (
    "Symbol",
    "Series",
    "Date",
    "Open Price",
    "High Price",
    "Low Price",
    "Close Price",
    "Total Traded Quantity",
)
DEFAULT_DATASET_NAME = "nse_cm_eod_dataset.csv"
DEFAULT_MANIFEST_NAME = "nse_cm_eod_import_manifest.json"
DEFAULT_REPORT_NAME = "nse_cm_eod_validation_report.json"
_SUPPORTED_SUFFIXES = (".csv", ".csv.gz", ".csv.zip")
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_DEFAULT_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%d-%b-%Y",
    "%d-%B-%Y",
    "%Y%m%d",
)


class LicensedNseCmEodError(ValueError):
    """Base error for local licensed-file import failures."""


class NseCmEodSchemaError(LicensedNseCmEodError):
    """A delivered file does not match the caller-supplied schema mapping."""


class NseCmEodFormatError(LicensedNseCmEodError):
    """A delivered file is not one of the locally supported CSV containers."""


class NseCmEodAuthorizationError(LicensedNseCmEodError):
    """Import was requested without an explicit authorization reference."""


@dataclass(frozen=True)
class NseCmEodSchema:
    """Exact source header names mapped to the project's canonical fields.

    There is intentionally no built-in header alias table. Construct this only
    from the licensed delivery's sample and accompanying technical spec.
    """

    date_column: str
    symbol_column: str
    series_column: str
    open_column: str
    high_column: str
    low_column: str
    close_column: str
    volume_column: str

    def __post_init__(self) -> None:
        names = [str(value).strip() for value in asdict(self).values()]
        if any(not value for value in names):
            raise ValueError("all source column names must be explicit and non-empty")
        if len({value.casefold() for value in names}) != len(names):
            raise ValueError("source column mapping must use eight distinct columns")

    def source_columns(self) -> tuple[str, ...]:
        return tuple(str(value) for value in asdict(self).values())


@dataclass(frozen=True)
class SymbolHistoryEvent:
    """User-supplied security-history metadata; no identity inference is made."""

    event_type: str
    symbol: str
    effective_date: date | None = None
    related_symbol: str | None = None
    status: str = "unknown"
    source_reference: str | None = None
    note: str = ""

    _ALLOWED_TYPES = frozenset({
        "ticker_change", "rename", "merger", "delisting", "suspension", "corporate_action"
    })

    def __post_init__(self) -> None:
        if self.event_type not in self._ALLOWED_TYPES:
            raise ValueError(f"unsupported symbol-history event type: {self.event_type!r}")
        if not self.symbol.strip():
            raise ValueError("symbol-history event requires a symbol")
        if self.status not in {"known", "unknown"}:
            raise ValueError("symbol-history event status must be 'known' or 'unknown'")
        if self.status == "known" and not (self.source_reference or "").strip():
            raise ValueError("known symbol-history events require a source reference")
        if self.status == "unknown" and self.related_symbol:
            raise ValueError("unknown identity events cannot assert a related-symbol mapping")

    def to_dict(self) -> dict[str, object]:
        result = asdict(self)
        result["effective_date"] = self.effective_date.isoformat() if self.effective_date else None
        return result


@dataclass(frozen=True)
class ParsedNseCmRow:
    source_file: str
    source_member: str
    line_number: int
    values: Mapping[str, str | None]


@dataclass
class NormalizationResult:
    rows: list[dict[str, str]]
    rejected_rows: list[dict[str, object]]
    source_row_count: int
    eq_rows_seen: int
    non_eq_rows_excluded: int
    out_of_range_rows: int
    duplicate_count: int
    invalid_ohlc_count: int
    nonpositive_price_count: int
    negative_volume_count: int
    malformed_date_count: int
    invalid_numeric_count: int
    zero_volume_count: int

    @property
    def quality_rejected_count(self) -> int:
        return sum(
            1 for row in self.rejected_rows
            if row["reason"] not in {"non_eq_series", "outside_requested_range"}
        )


def _parse_date(value: object, formats: Sequence[str]) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("date is empty")
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unsupported date value {text!r}")


def _display_date(value: date) -> str:
    return f"{value.day:02d}-{_MONTHS[value.month - 1]}-{value.year:04d}"


def _normalize_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or not _SYMBOL_RE.fullmatch(symbol) or symbol in {".", ".."}:
        raise ValueError(f"invalid symbol {value!r}")
    return symbol


def _parse_number(
    value: object,
    thousands_separator: str | None,
    decimal_separator: str,
) -> tuple[str, Decimal]:
    text = str(value or "").strip()
    if not text:
        raise ValueError("numeric value is empty")
    if text.count(decimal_separator) > 1:
        raise ValueError(f"malformed numeric value {text!r}")
    integer_text, has_decimal, fraction_text = text.partition(decimal_separator)
    sign = ""
    if integer_text.startswith(("+", "-")):
        sign, integer_text = integer_text[0], integer_text[1:]
    if not integer_text:
        raise ValueError(f"malformed numeric value {text!r}")
    if thousands_separator and thousands_separator in fraction_text:
        raise ValueError(f"group separator appears after decimal separator in {text!r}")
    if thousands_separator and thousands_separator in integer_text:
        groups = integer_text.split(thousands_separator)
        if (
            not groups[0].isdigit()
            or not 1 <= len(groups[0]) <= 3
            or any(not group.isdigit() or len(group) != 3 for group in groups[1:])
        ):
            raise ValueError(f"malformed digit grouping in {text!r}")
        integer_text = "".join(groups)
    elif not integer_text.isdigit():
        raise ValueError(f"malformed numeric value {text!r}")
    if has_decimal and fraction_text and not fraction_text.isdigit():
        raise ValueError(f"malformed numeric value {text!r}")
    if has_decimal and not fraction_text:
        raise ValueError(f"malformed numeric value {text!r}")
    candidate = sign + integer_text + (f".{fraction_text}" if has_decimal else "")
    try:
        numeric = Decimal(candidate)
    except InvalidOperation as error:
        raise ValueError(f"malformed numeric value {text!r}") from error
    if not numeric.is_finite():
        raise ValueError(f"non-finite numeric value {text!r}")
    return candidate, numeric


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _csv_members(path: Path) -> Iterator[tuple[str, io.TextIOBase]]:
    """Yield text streams from local CSV, CSV.GZ, or CSV.ZIP files."""
    lowered = path.name.lower()
    if lowered.endswith(".csv.zip"):
        try:
            archive = zipfile.ZipFile(path)
        except (OSError, zipfile.BadZipFile) as error:
            raise NseCmEodFormatError(f"invalid ZIP archive {path}: {error}") from error
        with archive:
            names = [name for name in archive.namelist() if name.lower().endswith(".csv") and not name.endswith("/")]
            if not names:
                raise NseCmEodFormatError(f"ZIP archive contains no CSV member: {path}")
            for name in sorted(names):
                try:
                    with archive.open(name) as binary:
                        with io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as text:
                            yield name, text
                except (UnicodeDecodeError, OSError, zipfile.BadZipFile) as error:
                    raise NseCmEodFormatError(f"cannot read CSV member {name!r} in {path}: {error}") from error
        return
    if lowered.endswith(".csv.gz"):
        try:
            with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as text:
                yield path.name, text
        except (OSError, EOFError, UnicodeDecodeError) as error:
            raise NseCmEodFormatError(f"cannot read gzip CSV {path}: {error}") from error
        return
    if path.suffix.lower() != ".csv":
        raise NseCmEodFormatError(
            f"unsupported local file {path.name!r}; a licensed sample/technical spec is required for other formats"
        )
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as text:
            yield path.name, text
    except (OSError, UnicodeDecodeError) as error:
        raise NseCmEodFormatError(f"cannot read CSV {path}: {error}") from error


def _read_csv_rows(path: Path, schema: NseCmEodSchema) -> Iterator[ParsedNseCmRow]:
    found_member = False
    for member, handle in _csv_members(path):
        found_member = True
        reader = csv.DictReader(handle)
        headers = reader.fieldnames
        if not headers:
            raise NseCmEodSchemaError(f"CSV member {member!r} has no header row")
        if len(headers) != len(set(headers)):
            raise NseCmEodSchemaError(f"CSV member {member!r} has duplicate header names")
        missing = [name for name in schema.source_columns() if name not in headers]
        if missing:
            raise NseCmEodSchemaError(
                f"CSV member {member!r} is missing explicitly mapped source columns {missing}; "
                f"received {headers!r}"
            )
        for line_number, row in enumerate(reader, start=2):
            if row is None or all(not str(value or "").strip() for value in row.values() if value is not None):
                continue
            if None in row:
                raise NseCmEodSchemaError(f"CSV member {member!r} row {line_number} has extra fields")
            yield ParsedNseCmRow(str(path.resolve()), member, line_number, row)
    if not found_member:
        raise NseCmEodFormatError(f"no CSV content found in {path}")


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _atomic_csv(path: Path, rows: Sequence[Mapping[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CANONICAL_COLUMNS, extrasaction="raise", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class LicensedNseCmEodProvider(HistoricalDataProvider):
    """Local licensed-file importer plus the common daily-bars provider API.

    No NSE request is made by this class. A caller must provide exact source
    headers from an authorized NSE sample and pass a license reference before
    an import can be committed.
    """

    def __init__(
        self,
        source_dir: str | Path,
        schema: NseCmEodSchema,
        *,
        dataset_path: str | Path | None = None,
        manifest_path: str | Path | None = None,
        report_path: str | Path | None = None,
        date_formats: Sequence[str] = _DEFAULT_DATE_FORMATS,
        thousands_separator: str | None = ",",
        decimal_separator: str = ".",
        exchange_sessions: Iterable[date] | None = None,
        symbol_events: Iterable[SymbolHistoryEvent] = (),
        abnormal_jump_threshold: float = 0.20,
        low_coverage_ratio: float = 0.50,
    ) -> None:
        if not date_formats:
            raise ValueError("at least one explicit date format is required")
        if len(decimal_separator) != 1 or (thousands_separator and len(thousands_separator) != 1):
            raise ValueError("numeric separators must be one character")
        if thousands_separator and thousands_separator == decimal_separator:
            raise ValueError("thousands and decimal separators must differ")
        if abnormal_jump_threshold <= 0:
            raise ValueError("abnormal_jump_threshold must be positive")
        if not 0 < low_coverage_ratio <= 1:
            raise ValueError("low_coverage_ratio must be in (0, 1]")
        self.source_dir = Path(source_dir)
        self.schema = schema
        self.dataset_path = Path(dataset_path) if dataset_path else self.source_dir / DEFAULT_DATASET_NAME
        self.manifest_path = Path(manifest_path) if manifest_path else self.source_dir / DEFAULT_MANIFEST_NAME
        self.report_path = Path(report_path) if report_path else self.source_dir / DEFAULT_REPORT_NAME
        self.date_formats = tuple(date_formats)
        self.thousands_separator = thousands_separator
        self.decimal_separator = decimal_separator
        self.exchange_sessions = tuple(sorted(set(exchange_sessions or ()))) if exchange_sessions is not None else None
        self.symbol_events = tuple(symbol_events)
        self.abnormal_jump_threshold = abnormal_jump_threshold
        self.low_coverage_ratio = low_coverage_ratio
        self._history_dataset_signature: tuple[int, int] | None = None
        self._history_dataset_cache: pd.DataFrame | None = None

    def discover(self) -> list[Path]:
        """Discover supported local deliveries deterministically; never access NSE."""
        if not self.source_dir.is_dir():
            return []
        files = [
            path for path in self.source_dir.rglob("*")
            if path.is_file()
            and any(path.name.lower().endswith(suffix) for suffix in _SUPPORTED_SUFFIXES)
            and path.resolve() not in {self.dataset_path.resolve(), self.manifest_path.resolve(), self.report_path.resolve()}
        ]
        return sorted(files, key=lambda value: value.as_posix().casefold())

    def validate_file(self, path: str | Path) -> dict[str, object]:
        """Check a local container, mapped headers, row structure, and checksum."""
        file_path = Path(path)
        if not file_path.is_file():
            raise FileNotFoundError(file_path)
        row_count = 0
        members: list[dict[str, object]] = []
        member_seen: set[str] = set()
        for row in _read_csv_rows(file_path, self.schema):
            if row.source_member not in member_seen:
                member_seen.add(row.source_member)
                members.append({"name": row.source_member, "row_count": 0})
            members[-1]["row_count"] = int(members[-1]["row_count"]) + 1
            row_count += 1
        if not member_seen:
            # Header-only CSV is allowed as a structurally valid file. Validate
            # headers by running a dedicated empty read below.
            for _ in _read_csv_rows(file_path, self.schema):
                pass
        return {
            "path": str(file_path.resolve()),
            "size_bytes": file_path.stat().st_size,
            "sha256": _sha256(file_path),
            "row_count": row_count,
            "members": members,
        }

    def parse_file(self, path: str | Path) -> list[ParsedNseCmRow]:
        """Parse a local CSV delivery without changing source values."""
        return list(_read_csv_rows(Path(path), self.schema))

    def normalize(self, records: Iterable[ParsedNseCmRow], start: date, end: date) -> NormalizationResult:
        """Filter EQ rows, normalize canonical formatting, and audit every exclusion."""
        if start > end:
            raise ValueError("start date must be on or before end date")
        output: list[dict[str, str]] = []
        rejected: list[dict[str, object]] = []
        keys: set[tuple[str, date]] = set()
        source_count = eq_seen = non_eq = out_of_range = duplicate = invalid_ohlc = 0
        nonpositive = negative_volume = malformed_dates = invalid_numeric = zero_volume = 0

        for record in records:
            source_count += 1
            values = record.values
            series = str(values.get(self.schema.series_column) or "").strip().upper()
            if series != "EQ":
                non_eq += 1
                rejected.append(self._rejection(record, "non_eq_series", f"series={series or '(empty)'}"))
                continue
            eq_seen += 1
            try:
                trade_date = _parse_date(values.get(self.schema.date_column), self.date_formats)
            except ValueError as error:
                malformed_dates += 1
                rejected.append(self._rejection(record, "malformed_date", str(error)))
                continue
            if not start <= trade_date <= end:
                out_of_range += 1
                rejected.append(self._rejection(record, "outside_requested_range", trade_date.isoformat()))
                continue
            try:
                symbol = _normalize_symbol(values.get(self.schema.symbol_column))
            except ValueError as error:
                rejected.append(self._rejection(record, "invalid_symbol", str(error)))
                continue

            number_sources = (
                (self.schema.open_column, "Open Price"),
                (self.schema.high_column, "High Price"),
                (self.schema.low_column, "Low Price"),
                (self.schema.close_column, "Close Price"),
                (self.schema.volume_column, "Total Traded Quantity"),
            )
            text_numbers: dict[str, str] = {}
            decimals: dict[str, Decimal] = {}
            numeric_error: str | None = None
            for source_name, output_name in number_sources:
                try:
                    text_numbers[output_name], decimals[output_name] = _parse_number(
                        values.get(source_name), self.thousands_separator, self.decimal_separator
                    )
                except ValueError as error:
                    numeric_error = f"{output_name}: {error}"
                    break
            if numeric_error:
                invalid_numeric += 1
                rejected.append(self._rejection(record, "malformed_numeric", numeric_error))
                continue

            quality_reasons: list[str] = []
            price_names = ("Open Price", "High Price", "Low Price", "Close Price")
            bad_prices = [name for name in price_names if decimals[name] <= 0]
            if bad_prices:
                nonpositive += len(bad_prices)
                quality_reasons.append("nonpositive_prices:" + ",".join(bad_prices))
            volume = decimals["Total Traded Quantity"]
            if volume < 0:
                negative_volume += 1
                quality_reasons.append("negative_volume")
            elif volume == 0:
                zero_volume += 1
            high = decimals["High Price"]
            low = decimals["Low Price"]
            open_price = decimals["Open Price"]
            close_price = decimals["Close Price"]
            if high < low or not (low <= open_price <= high) or not (low <= close_price <= high):
                invalid_ohlc += 1
                quality_reasons.append("invalid_ohlc")
            if quality_reasons:
                rejected.append(self._rejection(record, ";".join(quality_reasons), "OHLCV integrity check failed"))
                continue

            key = (symbol, trade_date)
            if key in keys:
                duplicate += 1
                rejected.append(self._rejection(record, "duplicate_symbol_date", f"{symbol} {trade_date.isoformat()}"))
                continue
            keys.add(key)
            output.append({
                "Symbol": symbol,
                "Series": "EQ",
                "Date": _display_date(trade_date),
                **text_numbers,
            })

        output.sort(key=lambda row: (row["Symbol"], _parse_date(row["Date"], self.date_formats)))
        return NormalizationResult(
            rows=output,
            rejected_rows=rejected,
            source_row_count=source_count,
            eq_rows_seen=eq_seen,
            non_eq_rows_excluded=non_eq,
            out_of_range_rows=out_of_range,
            duplicate_count=duplicate,
            invalid_ohlc_count=invalid_ohlc,
            nonpositive_price_count=nonpositive,
            negative_volume_count=negative_volume,
            malformed_date_count=malformed_dates,
            invalid_numeric_count=invalid_numeric,
            zero_volume_count=zero_volume,
        )

    @staticmethod
    def _rejection(record: ParsedNseCmRow, reason: str, detail: str) -> dict[str, object]:
        return {
            "source_file": record.source_file,
            "source_member": record.source_member,
            "line_number": record.line_number,
            "reason": reason,
            "detail": detail,
        }

    def validate_dataset(
        self,
        rows: Sequence[Mapping[str, str]],
        start: date,
        end: date,
    ) -> dict[str, object]:
        """Audit normalized EQ rows and report coverage, events, and jump flags."""
        errors: list[str] = []
        seen: set[tuple[str, date]] = set()
        by_symbol: dict[str, list[tuple[date, Mapping[str, str]]]] = {}
        normalized_rows: list[dict[str, str]] = []
        invalid_ohlc_count = nonpositive_count = negative_volume_count = malformed_count = duplicate_count = 0
        for index, row in enumerate(rows, start=1):
            missing = [column for column in CANONICAL_COLUMNS if column not in row]
            if missing:
                errors.append(f"row {index} missing columns {missing}")
                continue
            if str(row.get("Series", "")).strip().upper() != "EQ":
                errors.append(f"row {index} is not EQ")
                continue
            try:
                trade_date = _parse_date(row.get("Date"), self.date_formats)
            except ValueError as error:
                malformed_count += 1
                errors.append(f"row {index} has malformed date: {error}")
                continue
            try:
                symbol = _normalize_symbol(row.get("Symbol"))
            except ValueError as error:
                errors.append(f"row {index} has invalid symbol: {error}")
                continue
            key = (symbol, trade_date)
            if key in seen:
                duplicate_count += 1
                errors.append(f"row {index} duplicates {symbol} on {trade_date.isoformat()}")
                continue
            seen.add(key)
            if not start <= trade_date <= end:
                errors.append(f"row {index} is outside the requested range")
                continue
            try:
                fields = {
                    "Open Price": _parse_number(row.get("Open Price"), self.thousands_separator, self.decimal_separator)[1],
                    "High Price": _parse_number(row.get("High Price"), self.thousands_separator, self.decimal_separator)[1],
                    "Low Price": _parse_number(row.get("Low Price"), self.thousands_separator, self.decimal_separator)[1],
                    "Close Price": _parse_number(row.get("Close Price"), self.thousands_separator, self.decimal_separator)[1],
                    "Total Traded Quantity": _parse_number(row.get("Total Traded Quantity"), self.thousands_separator, self.decimal_separator)[1],
                }
            except ValueError as error:
                malformed_count += 1
                errors.append(f"row {index} has malformed numeric value: {error}")
                continue
            prices = [fields[name] for name in ("Open Price", "High Price", "Low Price", "Close Price")]
            nonpositive_count += sum(value <= 0 for value in prices)
            volume = fields["Total Traded Quantity"]
            if any(value <= 0 for value in prices):
                errors.append(f"row {index} has a nonpositive price")
            if volume < 0:
                negative_volume_count += 1
                errors.append(f"row {index} has negative volume")
            high, low, open_price, close_price = fields["High Price"], fields["Low Price"], fields["Open Price"], fields["Close Price"]
            if high < low or not (low <= open_price <= high) or not (low <= close_price <= high):
                invalid_ohlc_count += 1
                errors.append(f"row {index} has invalid OHLC")
            canonical = {column: str(row[column]).strip() for column in CANONICAL_COLUMNS}
            canonical["Symbol"] = symbol
            canonical["Series"] = "EQ"
            canonical["Date"] = _display_date(trade_date)
            normalized_rows.append(canonical)
            by_symbol.setdefault(symbol, []).append((trade_date, canonical))

        for values in by_symbol.values():
            values.sort(key=lambda item: item[0])
        actual_dates = [day for values in by_symbol.values() for day, _ in values]
        unique_symbols = sorted(by_symbol)
        symbol_history = []
        for symbol, values in sorted(by_symbol.items()):
            first, last = values[0][0], values[-1][0]
            symbol_history.append({
                "symbol": symbol,
                "first_date": first.isoformat(),
                "last_date": last.isoformat(),
                "rows": len(values),
                "starts_after_2020": first > date(2020, 1, 1),
                "last_before_requested_end": last < end,
                "disappearance_interpretation": "coverage ends before requested end; delisting/rename is unknown without sourced metadata",
            })

        jumps: list[dict[str, object]] = []
        for symbol, values in sorted(by_symbol.items()):
            previous_date: date | None = None
            previous_close: Decimal | None = None
            for trade_date, row in values:
                close = _parse_number(row["Close Price"], self.thousands_separator, self.decimal_separator)[1]
                if previous_close is not None and previous_close != 0:
                    pct = close / previous_close - Decimal(1)
                    if abs(float(pct)) > self.abnormal_jump_threshold:
                        jumps.append({
                            "symbol": symbol,
                            "from_date": previous_date.isoformat() if previous_date else None,
                            "to_date": trade_date.isoformat(),
                            "return_pct": float(pct * Decimal(100)),
                            "classification": "review_required; no corporate-action inference or adjustment applied",
                        })
                previous_date, previous_close = trade_date, close

        sessions = self._session_statistics(by_symbol, start, end)
        coverage_counts: dict[str, int] = {}
        for trade_date in sorted(set(actual_dates)):
            coverage_counts[trade_date.isoformat()] = len({
                symbol for symbol, values in by_symbol.items() if any(day == trade_date for day, _ in values)
            })
        expected_days = [day.isoformat() for day in self.exchange_sessions or () if start <= day <= end]
        coverage_basis = "supplied_exchange_sessions" if self.exchange_sessions is not None else "dates_present_in_dataset_only"
        all_coverage_days = expected_days if self.exchange_sessions is not None else sorted(coverage_counts)
        if self.exchange_sessions is not None:
            coverage_counts = {
                day.isoformat(): len({symbol for symbol, values in by_symbol.items() if any(item_day == day for item_day, _ in values)})
                for day in (value for value in self.exchange_sessions if start <= value <= end)
            }
        counts = list(coverage_counts.values())
        median_coverage = float(statistics.median(counts)) if counts else 0.0
        low_cutoff = median_coverage * self.low_coverage_ratio
        low_coverage_sessions = [
            {"date": day, "symbol_count": coverage_counts[day], "median_symbol_count": median_coverage,
             "threshold_symbol_count_exclusive": low_cutoff}
            for day in all_coverage_days if coverage_counts.get(day, 0) < low_cutoff
        ]

        event_dicts = [event.to_dict() for event in self.symbol_events]
        known_event_symbols = {str(event.symbol).strip().upper() for event in self.symbol_events if event.status == "known"}
        symbols_without_event_metadata = [symbol for symbol in unique_symbols if symbol not in known_event_symbols]
        longest = sorted(symbol_history, key=lambda item: (-int(item["rows"]), str(item["symbol"])))[:20]
        return {
            "validation_status": "FAIL" if errors else ("PASS_WITH_WARNINGS" if jumps or low_coverage_sessions else "PASS"),
            "validation_errors": errors,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "actual_first_date": min(actual_dates).isoformat() if actual_dates else None,
            "actual_last_date": max(actual_dates).isoformat() if actual_dates else None,
            "unique_symbol_count": len(unique_symbols),
            "symbols": unique_symbols,
            "row_count": len(normalized_rows),
            "eq_row_count": len(normalized_rows),
            "duplicate_count": duplicate_count,
            "invalid_ohlc_count": invalid_ohlc_count,
            "nonpositive_price_field_count": nonpositive_count,
            "negative_volume_row_count": negative_volume_count,
            "malformed_row_value_count": malformed_count,
            "longest_histories": longest,
            "symbols_starting_after_2020": [item for item in symbol_history if item["starts_after_2020"]],
            "symbols_disappearing_before_end": [item for item in symbol_history if item["last_before_requested_end"]],
            "per_symbol_history": symbol_history,
            "low_coverage_sessions": low_coverage_sessions,
            "low_coverage_basis": coverage_basis,
            "abnormal_price_jumps": jumps,
            "abnormal_jump_threshold_pct": self.abnormal_jump_threshold * 100,
            "missing_session_statistics": sessions,
            "symbol_history_metadata": {
                "events": event_dicts,
                "symbols_without_verified_event_metadata": symbols_without_event_metadata,
                "identity_mappings_applied": [],
                "note": "No ticker continuity, rename, merger, delisting, suspension, or corporate-action relationship is inferred from price rows.",
            },
        }

    def _session_statistics(
        self,
        by_symbol: Mapping[str, list[tuple[date, Mapping[str, str]]]],
        start: date,
        end: date,
    ) -> dict[str, object]:
        if self.exchange_sessions is None:
            return {
                "calendar_provided": False,
                "calendar_source": None,
                "expected_session_count_in_request": None,
                "dataset_missing_sessions": None,
                "per_symbol_missing_sessions_within_observed_span": None,
                "note": "Not computed; provide an authorized exchange-session calendar. No sessions were synthesized.",
            }
        sessions = [day for day in self.exchange_sessions if start <= day <= end]
        observed_dates = {day for values in by_symbol.values() for day, _ in values}
        per_symbol: dict[str, dict[str, object]] = {}
        missing_by_symbol = 0
        for symbol, values in sorted(by_symbol.items()):
            observed = {day for day, _ in values}
            first, last = min(observed), max(observed)
            expected_in_span = [day for day in sessions if first <= day <= last]
            missing = sorted(set(expected_in_span) - observed)
            missing_by_symbol += len(missing)
            per_symbol[symbol] = {
                "expected_sessions_within_observed_span": len(expected_in_span),
                "observed_sessions": len(observed & set(expected_in_span)),
                "missing_session_count": len(missing),
                "missing_dates_sample": [day.isoformat() for day in missing[:50]],
            }
        missing_entire_dataset = sorted(set(sessions) - observed_dates)
        return {
            "calendar_provided": True,
            "calendar_source": "caller-supplied local session dates",
            "expected_session_count_in_request": len(sessions),
            "dataset_missing_sessions": len(missing_entire_dataset),
            "dataset_missing_session_dates_sample": [day.isoformat() for day in missing_entire_dataset[:50]],
            "per_symbol_missing_sessions_within_observed_span": missing_by_symbol,
            "per_symbol": per_symbol,
            "note": "Calendar dates are diagnostic only; no rows were synthesized or filled.",
        }

    def import_to_dataset(
        self,
        start: date,
        end: date,
        *,
        authorization_reference: str,
        output_path: str | Path | None = None,
    ) -> dict[str, object]:
        """Import only clean local files; write a dataset only after a passing audit."""
        if start > end:
            raise ValueError("start date must be on or before end date")
        if not authorization_reference.strip():
            raise NseCmEodAuthorizationError(
                "an NSE authorization/license reference is required before importing data"
            )
        target = Path(output_path) if output_path else self.dataset_path
        files = [path for path in self.discover() if path.resolve() != target.resolve()]
        file_reports: list[dict[str, object]] = []
        file_errors: list[str] = []
        parsed: list[ParsedNseCmRow] = []
        for path in files:
            try:
                file_reports.append(self.validate_file(path))
                parsed.extend(self.parse_file(path))
            except (OSError, UnicodeError, EOFError, LicensedNseCmEodError, csv.Error) as error:
                file_errors.append(f"{path}: {error}")
                file_reports.append({"path": str(path.resolve()), "error": str(error), "sha256": _sha256(path)})

        batch = self.normalize(parsed, start, end)
        report = self.validate_dataset(batch.rows, start, end)
        quality_errors = [
            str(row["reason"]) for row in batch.rejected_rows
            if row["reason"] not in {"non_eq_series", "outside_requested_range"}
        ]
        if file_errors or quality_errors or not batch.rows:
            report["validation_status"] = "FAIL"
        report["normalization_counts"] = {
            "source_row_count": batch.source_row_count,
            "eq_rows_seen": batch.eq_rows_seen,
            "accepted_eq_rows": len(batch.rows),
            "rejected_row_count": len(batch.rejected_rows),
            "quality_rejected_row_count": batch.quality_rejected_count,
            "non_eq_rows_excluded": batch.non_eq_rows_excluded,
            "out_of_range_rows": batch.out_of_range_rows,
            "duplicate_count": batch.duplicate_count,
            "invalid_ohlc_count": batch.invalid_ohlc_count,
            "nonpositive_price_field_count": batch.nonpositive_price_count,
            "negative_volume_row_count": batch.negative_volume_count,
            "malformed_date_count": batch.malformed_date_count,
            "invalid_numeric_row_count": batch.invalid_numeric_count,
            "zero_volume_row_count": batch.zero_volume_count,
        }
        report["rejected_rows_sample"] = batch.rejected_rows[:100]
        report["file_errors"] = file_errors
        report["files"] = file_reports
        report["authorization_reference"] = authorization_reference
        report["imported_at_utc"] = datetime.now(timezone.utc).isoformat()
        report["source_access"] = "local files only; no NSE network access"
        report["provider"] = "LicensedNseCmEodProvider"
        report["source_product"] = "NSE Capital Market EOD / Historical Data (licensed delivery)"

        if report["validation_status"] != "FAIL":
            _atomic_csv(target, batch.rows)
            report["dataset_path"] = str(target.resolve())
            report["dataset_sha256"] = _sha256(target)
        else:
            report["dataset_path"] = None
            report["dataset_sha256"] = None

        manifest: dict[str, object] = {
            "provider": "LicensedNseCmEodProvider",
            "source_product": "NSE Capital Market EOD / Historical Data (licensed delivery)",
            "source_access": "local files only; no NSE network access",
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "actual_first_date": report.get("actual_first_date"),
            "actual_last_date": report.get("actual_last_date"),
            "file_count": len(file_reports),
            "row_count": len(batch.rows),
            "source_row_count": batch.source_row_count,
            "unique_symbol_count": report.get("unique_symbol_count", 0),
            "symbols": report.get("symbols", []),
            "eq_row_count": len(batch.rows),
            "eq_rows_seen": batch.eq_rows_seen,
            "rejected_row_count": len(batch.rejected_rows),
            "duplicate_count": batch.duplicate_count,
            "invalid_ohlc_count": batch.invalid_ohlc_count,
            "normalization_counts": report["normalization_counts"],
            "file_errors": file_errors,
            "missing_session_statistics": report.get("missing_session_statistics"),
            "source_file_hashes": [
                {"path": item.get("path"), "sha256": item.get("sha256")}
                for item in file_reports
            ],
            "dataset_sha256": report.get("dataset_sha256"),
            "imported_at_utc": report["imported_at_utc"],
            "validation_status": report["validation_status"],
            "authorization_reference": authorization_reference,
            "dataset_path": report.get("dataset_path"),
        }
        _atomic_text(self.report_path, json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
        _atomic_text(self.manifest_path, json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
        return manifest

    def history(self, symbol: str, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
        """Return the imported licensed data through the engine's common bars API."""
        if start > end:
            raise ValueError("start date must be on or before end date")
        if interval != "1d":
            raise ValueError("licensed NSE CM EOD provides daily bars only")
        if not self.dataset_path.is_file():
            return pd.DataFrame(columns=REQUIRED_COLUMNS)
        stat = self.dataset_path.stat()
        signature = (stat.st_size, stat.st_mtime_ns)
        if self._history_dataset_cache is None or signature != self._history_dataset_signature:
            self._history_dataset_cache = pd.read_csv(self.dataset_path, dtype=str, keep_default_na=False)
            self._history_dataset_signature = signature
        raw = self._history_dataset_cache
        if tuple(raw.columns) != CANONICAL_COLUMNS:
            raise NseCmEodSchemaError(f"normalized dataset has unexpected columns: {list(raw.columns)!r}")
        requested = symbol.strip().upper().removesuffix(".NS")
        raw = raw.loc[(raw["Symbol"].str.strip().str.upper() == requested) & (raw["Series"].str.upper() == "EQ")]
        if raw.empty:
            return pd.DataFrame(columns=REQUIRED_COLUMNS)
        dates = pd.to_datetime(raw["Date"], errors="coerce", dayfirst=True, format="mixed")
        if dates.isna().any():
            raise NseCmEodSchemaError("normalized dataset contains malformed dates")
        frame = pd.DataFrame(index=pd.DatetimeIndex(dates))
        mapping = {
            "open": "Open Price", "high": "High Price", "low": "Low Price",
            "close": "Close Price", "volume": "Total Traded Quantity",
        }
        for target_name, source_name in mapping.items():
            frame[target_name] = pd.to_numeric(raw[source_name].str.replace(",", "", regex=False), errors="coerce").to_numpy()
        bars, audit = validate_and_normalize_bars(frame, "1d")
        if any(
            audit.get(counter, 0)
            for counter in (
                "duplicate_timestamps", "invalid_price_rows", "invalid_ohlc_rows", "missing_ohlcv_rows"
            )
        ):
            raise NseCmEodSchemaError("normalized licensed dataset failed OHLCV validation")
        return bars.loc[(bars.index.date >= start) & (bars.index.date <= end)]
