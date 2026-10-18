"""Conservative downloader for NSE's official security-wise history CSV.

The official page's current CSV endpoint is followed as served by NSE. Only EQ
rows are written. Responses containing only other series are rejected so a
backend/source mismatch cannot be mistaken for equity history.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import socket
import tempfile
import time
import urllib.error
import urllib.parse
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Mapping

from nse_research.data.nse_index_download import (
    NSE_BASE_URL,
    USER_AGENT,
    HttpResponse,
    HttpSession,
    NseAccessBlocked,
    NseDownloadError,
    NseMalformedResponse,
    UrllibNseSession,
    _atomic_write_text,
    _excerpt,
    _is_timeout,
    _looks_like_challenge,
    _sha256_file,
    _utc_now,
)

EQUITY_SOURCE_URL = f"{NSE_BASE_URL}/historical/price-and-volume-data-per-security"
EQUITY_ENDPOINT = "/api/historicalOR/priceAndVolumeDataPerSecurity"
MAX_CHUNK_DAYS = 365
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_INTERVAL_SECONDS = 1.25
OUTPUT_COLUMNS = (
    "Symbol",
    "Series",
    "Date",
    "Open Price",
    "High Price",
    "Low Price",
    "Close Price",
    "Total Traded Quantity",
)
USER_AGENT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
}
_MONTHS = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)
_SYMBOL_RE = re.compile(r"^[A-Z0-9&._-]+$")


class NseEquitySeriesMismatch(NseMalformedResponse):
    """NSE returned rows, but none belonged to the requested EQ series."""


def _header_key(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lower())


_SOURCE_ALIASES: dict[str, set[str]] = {
    "symbol": {"symbol", "tradingsymbol"},
    "series": {"series", "sctysrs"},
    "date": {"date", "tradedate", "tradingdate", "timestamp"},
    "open": {"openprice", "open", "openingprice"},
    "high": {"highprice", "high"},
    "low": {"lowprice", "low"},
    "close": {"closeprice", "close", "closingprice"},
    "volume": {"totaltradedquantity", "totaltradedqty", "quantity", "volume"},
}


def _resolve_source_columns(fieldnames: list[str] | None) -> dict[str, str]:
    if not fieldnames:
        raise NseMalformedResponse("NSE CSV response has no header row")
    normalized = {_header_key(name): name for name in fieldnames if name is not None}
    result: dict[str, str] = {}
    for target, aliases in _SOURCE_ALIASES.items():
        for alias in aliases:
            if alias in normalized:
                result[target] = normalized[alias]
                break
    missing = sorted(set(_SOURCE_ALIASES) - set(result))
    if missing:
        raise NseMalformedResponse(
            "NSE CSV response is missing required fields "
            f"{missing}; received columns {fieldnames!r}"
        )
    return result


def _normalize_date(raw: object) -> tuple[str, str]:
    if raw is None or isinstance(raw, bool):
        raise ValueError("date is empty")
    value = str(raw).strip()
    parsed: date | None = None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value.upper(), fmt).date()
            break
        except ValueError:
            continue
    if parsed is None:
        raise ValueError(f"unsupported NSE date format {value!r}")
    # Use a fixed English month map; strftime's abbreviated month is locale-dependent.
    display = f"{parsed.day:02d}-{_MONTHS[parsed.month - 1]}-{parsed.year:04d}"
    return parsed.isoformat(), display


def _normalize_numeric_cell(raw: object, field: str) -> tuple[str, Decimal | None, str | None]:
    if raw is None:
        return "", None, f"malformed_numeric:{field}"
    original = str(raw).strip()
    if not original:
        return "", None, f"malformed_numeric:{field}"
    candidate = original.replace(",", "").strip()
    try:
        number = Decimal(candidate)
    except InvalidOperation:
        return original, None, f"malformed_numeric:{field}"
    if not number.is_finite():
        return original, None, f"malformed_numeric:{field}"
    # Remove only NSE's grouping separators. Keep scale and precision as supplied.
    return candidate, number, None


def _is_empty_nse_json_data(body: bytes) -> bool:
    try:
        payload = json.loads(body.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and payload.get("data") == []


def _is_nse_no_data_message(text: str) -> bool:
    lowered = " ".join(text.lower().split())
    return any(
        phrase in lowered
        for phrase in ("no data available", "no data found", "no records found", "no records available")
    )


def _quality_issues(row: Mapping[str, str]) -> list[str]:
    issues: list[str] = []
    numeric: dict[str, Decimal] = {}
    labels = {
        "Open Price": "Open",
        "High Price": "High",
        "Low Price": "Low",
        "Close Price": "Close",
        "Total Traded Quantity": "Volume",
    }
    for field, label in labels.items():
        try:
            numeric[label] = Decimal(row[field])
        except (InvalidOperation, KeyError):
            issues.append(f"malformed_numeric:{label}")
    for field in ("Open", "High", "Low", "Close"):
        if field in numeric and numeric[field] <= 0:
            issues.append(f"nonpositive_price:{field}")
    if "Volume" in numeric and numeric["Volume"] < 0:
        issues.append("negative_volume")
    if all(field in numeric for field in ("Open", "High", "Low", "Close")):
        high, low = numeric["High"], numeric["Low"]
        if high < low or not (low <= numeric["Open"] <= high) or not (low <= numeric["Close"] <= high):
            issues.append("invalid_ohlc")
    if "Volume" in numeric and numeric["Volume"] == 0:
        issues.append("zero_volume")
    return issues


def _parse_source_csv(
    body: bytes,
    *,
    requested_symbol: str,
    start: date,
    end: date,
) -> tuple[list[dict[str, str]], int, int, list[dict[str, object]]]:
    try:
        text = body.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise NseMalformedResponse(f"NSE CSV is not UTF-8: {error}", response_excerpt=_excerpt(body)) from error
    if not text.strip():
        raise NseMalformedResponse("NSE returned an empty body instead of a CSV header or explicit no-data response")
    if _is_empty_nse_json_data(body) or _is_nse_no_data_message(text):
        return [], 0, 0, []
    if text.lstrip().startswith(("{", "[", "<")):
        raise NseMalformedResponse(
            "NSE returned JSON/HTML instead of the official CSV download",
            response_excerpt=_excerpt(body),
        )
    reader = csv.DictReader(io.StringIO(text, newline=""))
    columns = _resolve_source_columns(reader.fieldnames)
    rows_by_key: dict[tuple[str, str], dict[str, str]] = {}
    duplicate_count = 0
    non_eq_count = 0
    warning_rows: list[dict[str, object]] = []
    source_rows = 0
    for line_number, source in enumerate(reader, start=2):
        if source is None or all(not str(value or "").strip() for value in source.values()):
            continue
        source_rows += 1
        symbol = str(source.get(columns["symbol"], "")).strip().upper()
        if symbol != requested_symbol:
            raise NseMalformedResponse(
                f"NSE response row {line_number} names {symbol!r}; requested {requested_symbol!r}"
            )
        series = str(source.get(columns["series"], "")).strip().upper()
        if not series:
            raise NseMalformedResponse(f"NSE response row {line_number} has no series")
        try:
            iso_day, display_day = _normalize_date(source.get(columns["date"]))
        except ValueError as error:
            raise NseMalformedResponse(f"NSE response row {line_number}: {error}") from error
        parsed_day = date.fromisoformat(iso_day)
        if not start <= parsed_day <= end:
            raise NseMalformedResponse(
                f"NSE row date {iso_day} is outside requested chunk {start.isoformat()}..{end.isoformat()}"
            )
        if series != "EQ":
            non_eq_count += 1
            continue
        normalized: dict[str, str] = {
            "Symbol": requested_symbol,
            "Series": "EQ",
            "Date": display_day,
        }
        quality: list[str] = []
        for field, target in (
            ("open", "Open Price"),
            ("high", "High Price"),
            ("low", "Low Price"),
            ("close", "Close Price"),
            ("volume", "Total Traded Quantity"),
        ):
            text_value, _, issue = _normalize_numeric_cell(source.get(columns[field]), target)
            normalized[target] = text_value
            if issue:
                quality.append(issue)
        quality.extend(_quality_issues(normalized))
        if quality:
            warning_rows.append({"date": iso_day, "issues": sorted(set(quality))})
        key = (iso_day, "EQ")
        if key in rows_by_key:
            duplicate_count += 1
            if rows_by_key[key] != normalized:
                warning_rows.append({"date": iso_day, "issues": ["conflicting_duplicate"]})
            # Keep the first official row and expose the duplicate in the audit.
            continue
        rows_by_key[key] = normalized
    if source_rows and not rows_by_key and non_eq_count:
        series_seen = sorted(
            {
                str(row.get(columns["series"], "")).strip().upper()
                for row in csv.DictReader(io.StringIO(text, newline=""))
                if row and row.get(columns["series"])
            }
        )
        raise NseEquitySeriesMismatch(
            f"NSE returned {source_rows} row(s) for {requested_symbol}, but no EQ rows; "
            f"returned series: {series_seen}"
        )
    return [rows_by_key[key] for key in sorted(rows_by_key)], duplicate_count, non_eq_count, warning_rows


def _date_chunks(start: date, end: date, chunk_days: int = MAX_CHUNK_DAYS) -> list[tuple[date, date]]:
    if chunk_days < 1 or chunk_days > MAX_CHUNK_DAYS:
        raise ValueError(f"chunk_days must be between 1 and {MAX_CHUNK_DAYS}")
    result: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=chunk_days - 1))
        result.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return result


def _safe_symbol(value: object) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol or not _SYMBOL_RE.fullmatch(symbol) or symbol in {".", ".."}:
        raise ValueError(f"invalid NSE symbol {value!r}")
    return symbol


def _read_universe(path: Path) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    if not path.is_file():
        raise FileNotFoundError(f"NSE symbol universe not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"Symbol", "Series"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(f"universe CSV must contain columns {sorted(required)}")
        selected: list[dict[str, str]] = []
        excluded: list[dict[str, str]] = []
        seen: set[str] = set()
        for row in reader:
            raw_symbol = str(row.get("Symbol", "")).strip()
            if not raw_symbol:
                excluded.append({"symbol": "", "reason": "empty symbol"})
                continue
            try:
                symbol = _safe_symbol(raw_symbol)
            except ValueError as error:
                excluded.append({"symbol": raw_symbol, "reason": str(error)})
                continue
            series = str(row.get("Series", "")).strip().upper()
            reason = None
            if symbol.startswith("DUMMY") or symbol.startswith("TEST"):
                reason = "dummy/test symbol"
            elif series != "EQ":
                reason = f"universe series is {series or '(empty)'}, requested series is EQ"
            if reason:
                excluded.append({"symbol": symbol, "reason": reason})
                continue
            if symbol in seen:
                excluded.append({"symbol": symbol, "reason": "duplicate universe row"})
                continue
            seen.add(symbol)
            selected.append(
                {
                    "symbol": symbol,
                    "series": "EQ",
                    "company_name": str(row.get("Company Name", "")).strip(),
                    "isin": str(row.get("ISIN Code", "")).strip(),
                }
            )
    return selected, excluded


def _read_output_csv(path: Path, expected_symbol: str) -> tuple[dict[str, dict[str, str]], int]:
    if not path.exists():
        return {}, 0
    rows: dict[str, dict[str, str]] = {}
    duplicates = 0
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != OUTPUT_COLUMNS:
            raise ValueError(f"existing file has unexpected columns: {path}")
        for line_number, row in enumerate(reader, start=2):
            if str(row.get("Symbol", "")).strip().upper() != expected_symbol:
                raise ValueError(f"wrong symbol in {path}:{line_number}")
            if str(row.get("Series", "")).strip().upper() != "EQ":
                raise ValueError(f"non-EQ data in {path}:{line_number}")
            iso_day, display_day = _normalize_date(row.get("Date"))
            normalized = {column: str(row.get(column, "")).strip() for column in OUTPUT_COLUMNS}
            normalized["Symbol"] = expected_symbol
            normalized["Series"] = "EQ"
            normalized["Date"] = display_day
            if iso_day in rows:
                duplicates += 1
            rows[iso_day] = normalized
    return rows, duplicates


class NseEquityDownloader:
    """Download, validate, resume, and audit official NSE EQ security history."""

    def __init__(
        self,
        universe_path: str | Path,
        output_dir: str | Path,
        *,
        manifest_path: str | Path | None = None,
        universe_json_path: str | Path | None = None,
        report_path: str | Path | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        retries: int = 2,
        min_request_interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
        chunk_days: int = MAX_CHUNK_DAYS,
        backoff_seconds: float = 1.0,
        session: HttpSession | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout <= 0 or retries < 0 or min_request_interval_seconds < 0 or backoff_seconds < 0:
            raise ValueError("timeout must be positive; retries, interval, and backoff must be nonnegative")
        if not 1 <= chunk_days <= MAX_CHUNK_DAYS:
            raise ValueError(f"chunk_days must be between 1 and {MAX_CHUNK_DAYS}")
        self.universe_path = Path(universe_path)
        self.output_dir = Path(output_dir)
        self.manifest_path = Path(manifest_path) if manifest_path else self.output_dir / "nse_equity_download_manifest.json"
        self.universe_json_path = Path(universe_json_path) if universe_json_path else self.output_dir / "nse_equity_universe.json"
        self.report_path = Path(report_path) if report_path else self.output_dir.parent.parent / "docs" / "NSE_EQUITY_DATA_REPORT.md"
        self.timeout = timeout
        self.retries = retries
        self.min_request_interval_seconds = min_request_interval_seconds
        self.chunk_days = chunk_days
        self.backoff_seconds = backoff_seconds
        self.session = session or UrllibNseSession()
        self.sleep = sleep
        self.monotonic = monotonic
        self._last_request_at: float | None = None

    def download(
        self,
        start: date,
        end: date,
        *,
        symbols: tuple[str, ...] | None = None,
        source_preflight: dict[str, object] | None = None,
    ) -> dict[str, object]:
        if start > end:
            raise ValueError("start date must be on or before end date")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        universe, excluded = _read_universe(self.universe_path)
        selected = universe
        if symbols is not None:
            wanted = {_safe_symbol(item) for item in symbols}
            known = {row["symbol"] for row in universe}
            unknown = wanted - known
            if unknown:
                raise ValueError(f"requested symbol(s) are not EQ members of the project universe: {sorted(unknown)}")
            selected = [row for row in universe if row["symbol"] in wanted]

        manifest = self._read_manifest()
        manifest.update(
            {
                "schema_version": 1,
                "source": "National Stock Exchange of India (official website)",
                "source_url": EQUITY_SOURCE_URL,
                "endpoint": EQUITY_ENDPOINT,
                "request_contract": {
                    "query_parameters": ["symbol", "from", "to", "csv=true"],
                    "series_parameter": None,
                    "series_filter": "EQ, applied locally to the official CSV response",
                    "request_date_format": "DD-MM-YYYY",
                    "response_format": "CSV from NSE page's official CSV action",
                    "maximum_inclusive_chunk_days": self.chunk_days,
                },
                "requested_start": start.isoformat(),
                "requested_end": end.isoformat(),
                "downloaded_at": _utc_now(),
                "request_policy": {
                    "timeout_seconds": self.timeout,
                    "max_retries_after_initial_attempt": self.retries,
                    "min_interval_seconds": self.min_request_interval_seconds,
                    "backoff_seconds_base": self.backoff_seconds,
                    "chunk_days": self.chunk_days,
                },
                "source_preflight": source_preflight if source_preflight is not None else manifest.get("source_preflight"),
            }
        )
        prior_symbols = manifest.get("symbols", {})
        if not isinstance(prior_symbols, dict):
            prior_symbols = {}
        symbol_records: dict[str, dict[str, object]] = {}
        all_chunks = _date_chunks(start, end, self.chunk_days)
        for member in selected:
            symbol = member["symbol"]
            old = prior_symbols.get(symbol, {})
            if not isinstance(old, dict):
                old = {}
            symbol_records[symbol] = self._new_symbol_record(
                symbol, start, end, all_chunks, old, self.output_dir / f"{symbol}.csv"
            )
        manifest["symbols"] = symbol_records
        manifest["requested_symbols"] = [row["symbol"] for row in selected]
        manifest["project_universe_rows"] = len(universe) + len(excluded)
        manifest["excluded_universe_symbols"] = excluded
        self._write_universe_json(universe, excluded, symbol_records, start, end)
        self._write_manifest(manifest)

        pending = any(
            not self._chunk_is_done(record, chunk_start, chunk_end)
            for record in symbol_records.values()
            for chunk_start, chunk_end in all_chunks
        )
        if pending:
            try:
                page_response, page_retries = self._request(EQUITY_SOURCE_URL, expected="html")
                manifest["page_session"] = {
                    "url": EQUITY_SOURCE_URL,
                    "http_status": page_response.status_code,
                    "content_type": page_response.header("content-type"),
                    "retry_count": page_retries,
                    "status": "SUCCESS",
                }
            except NseDownloadError as error:
                manifest["page_session"] = self._error_record(error)
                for record in symbol_records.values():
                    record["status"] = "FAILED"
                    record["validation_status"] = "FAIL"
                    record.setdefault("warnings", []).append(f"page_session:{error.request_status}")
                self._finalize(manifest, symbol_records, selected, start, end)
                return manifest

        halt_reason: str | None = None
        for member in selected:
            symbol = member["symbol"]
            record = symbol_records[symbol]
            if halt_reason:
                record["status"] = "NOT_ATTEMPTED"
                record["warnings"].append(f"run_stopped:{halt_reason}")
                continue
            output_path = self.output_dir / f"{symbol}.csv"
            try:
                rows_by_iso, old_duplicates = _read_output_csv(output_path, symbol)
            except (OSError, ValueError) as error:
                record["status"] = "FAILED"
                record["request_status"] = "LOCAL_FILE_INVALID"
                record["validation_status"] = "FAIL"
                record["local_file_error"] = f"{type(error).__name__}: {error}"
                self._write_manifest(manifest)
                self._write_universe_json(universe, excluded, symbol_records, start, end)
                continue
            record["duplicate_rows"] = int(record.get("duplicate_rows", 0)) + old_duplicates
            for chunk_start, chunk_end in all_chunks:
                if self._chunk_is_done(record, chunk_start, chunk_end):
                    continue
                request_url = self._request_url(symbol, chunk_start, chunk_end)
                response: HttpResponse | None = None
                try:
                    response, retries = self._request(request_url, expected="csv")
                    rows, duplicates, non_eq_rows, warnings = _parse_source_csv(
                        response.body,
                        requested_symbol=symbol,
                        start=chunk_start,
                        end=chunk_end,
                    )
                except NseDownloadError as error:
                    if response is not None:
                        error.http_status = error.http_status or response.status_code
                        error.content_type = error.content_type or response.header("content-type")
                        error.url = error.url or request_url
                        record["http_status"] = response.status_code
                        record["content_type"] = response.header("content-type")
                    record["http_failures"].append(self._error_record(error, chunk_start, chunk_end))
                    record["retry_count"] = int(record.get("retry_count", 0)) + error.retries
                    self._upsert_chunk(
                        record,
                        {
                            "start": chunk_start.isoformat(),
                            "end": chunk_end.isoformat(),
                            "status": "FAILED",
                            "url": request_url,
                            "http_status": error.http_status,
                            "content_type": error.content_type,
                            "retry_count": error.retries,
                            "error": str(error),
                            "completed_at": _utc_now(),
                        },
                    )
                    record["warnings"].append(f"chunk_error:{chunk_start.isoformat()}:{error.request_status}")
                    if isinstance(error, NseAccessBlocked) or error.request_status == "RATE_LIMITED":
                        halt_reason = error.request_status
                        break
                    # A response/schema mismatch is not repaired by asking for
                    # further chunks for this same security.
                    if isinstance(error, NseMalformedResponse):
                        break
                    continue

                record["retry_count"] = int(record.get("retry_count", 0)) + retries
                record["http_status"] = response.status_code
                record["content_type"] = response.header("content-type")
                record["duplicate_rows"] = int(record.get("duplicate_rows", 0)) + duplicates
                record["non_eq_rows_excluded"] = int(record.get("non_eq_rows_excluded", 0)) + non_eq_rows
                record["warnings"].extend(
                    f"{item['date']}:{','.join(item['issues'])}" for item in warnings
                )
                candidate_rows = dict(rows_by_iso)
                for row in rows:
                    iso_day, _ = _normalize_date(row["Date"])
                    candidate_rows[iso_day] = row
                if rows:
                    try:
                        self._atomic_write_symbol(output_path, candidate_rows)
                    except (OSError, ValueError) as error:
                        record.setdefault("file_failures", []).append(
                            {
                                "chunk_start": chunk_start.isoformat(),
                                "chunk_end": chunk_end.isoformat(),
                                "error": f"{type(error).__name__}: {error}",
                            }
                        )
                        self._upsert_chunk(
                            record,
                            {
                                "start": chunk_start.isoformat(),
                                "end": chunk_end.isoformat(),
                                "status": "FAILED",
                                "url": request_url,
                                "http_status": response.status_code,
                                "content_type": response.header("content-type"),
                                "retry_count": retries,
                                "error": f"local file write failed: {error}",
                                "completed_at": _utc_now(),
                            },
                        )
                        record["warnings"].append(f"file_write_error:{chunk_start.isoformat()}")
                        self._write_manifest(manifest)
                        continue
                    rows_by_iso = candidate_rows
                    record["sha256"] = _sha256_file(output_path)
                status = "SUCCESS" if rows else "NO_DATA"
                self._upsert_chunk(
                    record,
                    {
                        "start": chunk_start.isoformat(),
                        "end": chunk_end.isoformat(),
                        "status": status,
                        "url": request_url,
                        "rows_received": len(rows),
                        "non_eq_rows_excluded": non_eq_rows,
                        "http_status": response.status_code,
                        "content_type": response.header("content-type"),
                        "retry_count": retries,
                        "completed_at": _utc_now(),
                    },
                )
                record["downloaded_at"] = _utc_now()
                self._finalize_symbol_record(record, output_path, start, end)
                self._write_manifest(manifest)
                self._write_universe_json(universe, excluded, symbol_records, start, end)

            self._finalize_symbol_record(record, output_path, start, end)
            self._write_manifest(manifest)
            self._write_universe_json(universe, excluded, symbol_records, start, end)

        self._finalize(manifest, symbol_records, selected, start, end)
        return manifest

    def _request_url(self, symbol: str, start: date, end: date) -> str:
        query = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "from": start.strftime("%d-%m-%Y"),
                "to": end.strftime("%d-%m-%Y"),
                "csv": "true",
            }
        )
        return f"{NSE_BASE_URL}{EQUITY_ENDPOINT}?{query}"

    def _request(self, url: str, *, expected: str) -> tuple[HttpResponse, int]:
        headers = {
            **USER_AGENT_HEADERS,
            "Referer": EQUITY_SOURCE_URL,
            "Origin": NSE_BASE_URL,
            "Accept": "text/csv,application/octet-stream,*/*" if expected == "csv" else "text/html,application/xhtml+xml",
        }
        last_response: HttpResponse | None = None
        for attempt in range(self.retries + 1):
            self._respect_rate_limit()
            try:
                response = self.session.get(url, headers=headers, timeout=self.timeout)
                last_response = response
            except (TimeoutError, socket.timeout, OSError, urllib.error.URLError) as error:
                if attempt < self.retries:
                    self.sleep(self.backoff_seconds * (2**attempt))
                    continue
                status = "TIMEOUT" if _is_timeout(error) else "NETWORK_ERROR"
                raise NseDownloadError(
                    f"NSE {status.lower().replace('_', ' ')} after {attempt + 1} attempt(s): {error}",
                    request_status=status,
                    retries=attempt,
                    url=url,
                ) from error
            content_type = (response.header("content-type") or "").lower()
            body_text = response.body[:20000].decode("utf-8", "replace")
            if response.status_code in {401, 403} or _looks_like_challenge(body_text):
                raise NseAccessBlocked(
                    f"NSE access-control response (HTTP {response.status_code}); no workaround was attempted",
                    request_status="BLOCKED",
                    http_status=response.status_code,
                    content_type=content_type or None,
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )
            if response.status_code in {408, 429, 500, 502, 503, 504}:
                if attempt < self.retries:
                    self.sleep(self.backoff_seconds * (2**attempt))
                    continue
                status = "RATE_LIMITED" if response.status_code == 429 else "HTTP_ERROR"
                raise NseDownloadError(
                    f"NSE returned HTTP {response.status_code} after {attempt + 1} attempt(s)",
                    request_status=status,
                    http_status=response.status_code,
                    content_type=content_type or None,
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )
            if response.status_code != 200:
                raise NseDownloadError(
                    f"NSE returned HTTP {response.status_code}",
                    request_status="HTTP_ERROR",
                    http_status=response.status_code,
                    content_type=content_type or None,
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )
            if expected == "html" and "html" not in content_type:
                raise NseMalformedResponse(
                    "NSE security-history page returned an unexpected content type",
                    http_status=response.status_code,
                    content_type=content_type or None,
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )
            if expected == "csv" and "csv" not in content_type:
                if "json" in content_type and _is_empty_nse_json_data(response.body):
                    return response, attempt
                raise NseMalformedResponse(
                    f"NSE security-history endpoint returned {content_type or '(missing content type)'}, expected CSV",
                    http_status=response.status_code,
                    content_type=content_type or None,
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )
            return response, attempt
        raise NseDownloadError(
            f"NSE request failed: {last_response}",
            request_status="FAILED",
            http_status=last_response.status_code if last_response else None,
            url=url,
        )

    def _respect_rate_limit(self) -> None:
        now = self.monotonic()
        if self._last_request_at is not None:
            delay = self.min_request_interval_seconds - (now - self._last_request_at)
            if delay > 0:
                self.sleep(delay)
        self._last_request_at = self.monotonic()

    def _new_symbol_record(
        self,
        symbol: str,
        start: date,
        end: date,
        chunks: list[tuple[date, date]],
        old: dict[str, object],
        path: Path,
    ) -> dict[str, object]:
        old_checksum = old.get("sha256")
        current_checksum = _sha256_file(path) if path.is_file() else None
        old_chunks = old.get("chunk_results", [])
        if not isinstance(old_chunks, list) or old_checksum != current_checksum:
            old_chunks = []
        expected_ranges = {(left.isoformat(), right.isoformat()) for left, right in chunks}
        reusable = [
            entry for entry in old_chunks
            if isinstance(entry, dict)
            and (entry.get("start"), entry.get("end")) in expected_ranges
            and entry.get("status") in {"SUCCESS", "NO_DATA"}
        ]
        return {
            "symbol": symbol,
            "series": "EQ",
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "source_endpoint": EQUITY_ENDPOINT,
            "output_file": str(path),
            "actual_first_date": None,
            "actual_last_date": None,
            "row_count": 0,
            "status": "PENDING",
            "request_status": "PENDING",
            "validation_status": "PENDING",
            "http_status": old.get("http_status"),
            "content_type": old.get("content_type"),
            "retry_count": 0,
            "duplicate_rows": int(old.get("duplicate_rows", 0)) if reusable else 0,
            "duplicate_symbol_date_series": 0,
            "non_eq_rows_excluded": 0,
            "http_failures": [],
            "warnings": [],
            "data_quality_warnings": [],
            "chunks_requested": len(chunks),
            "chunks_successful": sum(entry.get("status") in {"SUCCESS", "NO_DATA"} for entry in reusable),
            "chunks_failed": 0,
            "chunk_results": reusable,
            "sha256": current_checksum,
            "downloaded_at": old.get("downloaded_at"),
        }

    @staticmethod
    def _chunk_is_done(record: dict[str, object], start: date, end: date) -> bool:
        chunks = record.get("chunk_results", [])
        if not isinstance(chunks, list):
            return False
        return any(
            isinstance(chunk, dict)
            and chunk.get("start") == start.isoformat()
            and chunk.get("end") == end.isoformat()
            and chunk.get("status") in {"SUCCESS", "NO_DATA"}
            for chunk in chunks
        )

    @staticmethod
    def _upsert_chunk(record: dict[str, object], entry: dict[str, object]) -> None:
        chunks = record.setdefault("chunk_results", [])
        assert isinstance(chunks, list)
        for index, current in enumerate(chunks):
            if isinstance(current, dict) and (current.get("start"), current.get("end")) == (
                entry["start"], entry["end"]
            ):
                chunks[index] = entry
                break
        else:
            chunks.append(entry)
        record["chunks_successful"] = sum(
            isinstance(chunk, dict) and chunk.get("status") in {"SUCCESS", "NO_DATA"} for chunk in chunks
        )
        record["chunks_failed"] = sum(
            isinstance(chunk, dict) and chunk.get("status") == "FAILED" for chunk in chunks
        )

    def _atomic_write_symbol(self, path: Path, rows_by_iso: Mapping[str, Mapping[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS, extrasaction="raise", lineterminator="\n")
                writer.writeheader()
                for iso_day in sorted(rows_by_iso):
                    writer.writerow({column: rows_by_iso[iso_day][column] for column in OUTPUT_COLUMNS})
                handle.flush()
                os.fsync(handle.fileno())
            # Validate the complete temporary export before the atomic replace.
            expected_symbol = next(iter(rows_by_iso.values()))["Symbol"] if rows_by_iso else path.stem
            parsed, duplicates = _read_output_csv(temporary, expected_symbol)
            if duplicates or len(parsed) != len(rows_by_iso):
                raise ValueError(f"temporary output validation failed for {path}")
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()

    def _finalize_symbol_record(
        self,
        record: dict[str, object],
        path: Path,
        requested_start: date,
        requested_end: date,
    ) -> None:
        try:
            rows, duplicates = _read_output_csv(path, str(record["symbol"]))
        except (OSError, ValueError) as error:
            record["actual_first_date"] = None
            record["actual_last_date"] = None
            record["row_count"] = 0
            record["status"] = "FAILED"
            record["request_status"] = "LOCAL_FILE_INVALID"
            record["validation_status"] = "FAIL"
            record["local_file_error"] = f"{type(error).__name__}: {error}"
            record["validation"] = {
                "rows_in_requested_range": 0,
                "duplicate_dates_in_output": 0,
                "invalid_ohlc_rows": 0,
                "nonpositive_price_fields": 0,
                "negative_volume_rows": 0,
                "zero_volume_rows": 0,
                "malformed_numeric_fields": 0,
                "suspicious_discontinuities": 0,
            }
            return
        dated = sorted(
            (iso_day, row) for iso_day, row in rows.items()
            if requested_start <= date.fromisoformat(iso_day) <= requested_end
        )
        record["actual_first_date"] = dated[0][0] if dated else None
        record["actual_last_date"] = dated[-1][0] if dated else None
        record["row_count"] = len(dated)
        record["duplicate_symbol_date_series"] = duplicates
        record["sha256"] = _sha256_file(path) if path.is_file() else None
        chunks = record.get("chunk_results", [])
        failed = sum(isinstance(item, dict) and item.get("status") == "FAILED" for item in chunks)
        complete = sum(isinstance(item, dict) and item.get("status") in {"SUCCESS", "NO_DATA"} for item in chunks)
        record["chunks_successful"] = complete
        record["chunks_failed"] = failed
        if record.get("status") == "NOT_ATTEMPTED":
            pass
        elif record.get("status") == "FAILED" and not chunks:
            pass
        elif failed and dated:
            record["status"] = "PARTIAL"
        elif failed:
            record["status"] = "FAILED"
        elif complete >= int(record["chunks_requested"]) and dated:
            record["status"] = "SUCCESS"
        elif complete >= int(record["chunks_requested"]):
            record["status"] = "UNAVAILABLE"
        else:
            record["status"] = "PENDING"
        if record["status"] == "NOT_ATTEMPTED":
            record["request_status"] = record.get("request_status", "NOT_ATTEMPTED")
        elif record["status"] in {"SUCCESS", "PARTIAL"}:
            record["request_status"] = record["status"]
        elif record["status"] == "UNAVAILABLE":
            record["request_status"] = "NO_DATA"
        elif record["status"] == "FAILED" and record.get("http_failures"):
            last_failure = record["http_failures"][-1]
            record["request_status"] = last_failure.get("status", "FAILED") if isinstance(last_failure, dict) else "FAILED"
        elif record["status"] == "FAILED":
            record["request_status"] = "FAILED"
        warnings: list[str] = []
        data_quality: list[dict[str, object]] = []
        prior_close: Decimal | None = None
        prior_day: str | None = None
        for iso_day, row in dated:
            issues = _quality_issues(row)
            if issues:
                data_quality.append({"date": iso_day, "issues": sorted(set(issues))})
            try:
                current_close = Decimal(row["Close Price"])
                current_open = Decimal(row["Open Price"])
            except (InvalidOperation, KeyError):
                current_close = current_open = None
            if prior_close is not None and current_close is not None and prior_close > 0:
                move = abs(current_close / prior_close - Decimal(1))
                opening_gap = abs(current_open / prior_close - Decimal(1)) if current_open is not None else Decimal(0)
                if move >= Decimal("0.40") or opening_gap >= Decimal("0.40"):
                    pct = (current_close / prior_close - Decimal(1)) * Decimal(100)
                    data_quality.append(
                        {
                            "date": iso_day,
                            "issues": ["suspicious_discontinuity_over_40pct"],
                            "previous_date": prior_day,
                            "previous_close": str(prior_close),
                            "close": str(current_close),
                            "close_change_pct": str(pct),
                            "interpretation": "review only; possible corporate action or market event; value retained",
                        }
                    )
            if current_close is not None:
                prior_close, prior_day = current_close, iso_day
        record["data_quality_warnings"] = data_quality[:500]
        record["validation"] = {
            "rows_in_requested_range": len(dated),
            "duplicate_dates_in_output": duplicates,
            "invalid_ohlc_rows": sum("invalid_ohlc" in item.get("issues", []) for item in data_quality),
            "nonpositive_price_fields": sum(
                issue.startswith("nonpositive_price:")
                for item in data_quality for issue in item.get("issues", [])
            ),
            "negative_volume_rows": sum("negative_volume" in item.get("issues", []) for item in data_quality),
            "zero_volume_rows": sum("zero_volume" in item.get("issues", []) for item in data_quality),
            "malformed_numeric_fields": sum(
                issue.startswith("malformed_numeric:")
                for item in data_quality for issue in item.get("issues", [])
            ),
            "suspicious_discontinuities": sum(
                "suspicious_discontinuity_over_40pct" in item.get("issues", []) for item in data_quality
            ),
        }
        if record["status"] == "NOT_ATTEMPTED":
            record["validation_status"] = "NOT_RUN"
        elif record["status"] in {"FAILED", "PARTIAL", "UNAVAILABLE", "PENDING"}:
            record["validation_status"] = "FAIL" if record["status"] in {"FAILED", "PENDING"} else "WARN"
        elif data_quality or int(record.get("non_eq_rows_excluded", 0)):
            record["validation_status"] = "WARN_DATA_QUALITY"
        else:
            record["validation_status"] = "PASS"
        if record["status"] != "NOT_ATTEMPTED":
            record["downloaded_at"] = _utc_now()
        else:
            record["downloaded_at"] = None

    def _finalize(
        self,
        manifest: dict[str, object],
        records: dict[str, dict[str, object]],
        selected: list[dict[str, str]],
        start: date,
        end: date,
    ) -> None:
        for record in records.values():
            self._finalize_symbol_record(record, Path(str(record["output_file"])), start, end)
        counts = {status: sum(record.get("status") == status for record in records.values()) for status in (
            "SUCCESS", "PARTIAL", "UNAVAILABLE", "FAILED", "PENDING", "NOT_ATTEMPTED"
        )}
        manifest["aggregate"] = {
            "requested_symbols": len(selected),
            "successful_symbols": counts["SUCCESS"],
            "partial_symbols": counts["PARTIAL"],
            "unavailable_symbols": counts["UNAVAILABLE"],
            "failed_symbols": counts["FAILED"],
            "pending_symbols": counts["PENDING"],
            "not_attempted_symbols": counts["NOT_ATTEMPTED"],
            "total_rows": sum(int(record.get("row_count", 0)) for record in records.values()),
            "duplicate_rows": sum(int(record.get("duplicate_rows", 0)) for record in records.values()),
            "invalid_ohlc_rows": sum(int(record.get("validation", {}).get("invalid_ohlc_rows", 0)) for record in records.values()),
            "nonpositive_price_fields": sum(int(record.get("validation", {}).get("nonpositive_price_fields", 0)) for record in records.values()),
            "negative_volume_rows": sum(int(record.get("validation", {}).get("negative_volume_rows", 0)) for record in records.values()),
            "zero_volume_rows": sum(int(record.get("validation", {}).get("zero_volume_rows", 0)) for record in records.values()),
            "malformed_numeric_fields": sum(int(record.get("validation", {}).get("malformed_numeric_fields", 0)) for record in records.values()),
            "suspicious_discontinuities": sum(int(record.get("validation", {}).get("suspicious_discontinuities", 0)) for record in records.values()),
            "non_eq_rows_excluded": sum(int(record.get("non_eq_rows_excluded", 0)) for record in records.values()),
            "http_failures": sum(len(record.get("http_failures", [])) for record in records.values()),
            "retry_count": sum(int(record.get("retry_count", 0)) for record in records.values()),
        }
        manifest["downloaded_at"] = _utc_now()
        self._write_manifest(manifest)
        self._write_universe_json(
            *_read_universe(self.universe_path), records, start, end
        )
        self._write_report(manifest, records, start, end)

    def _write_universe_json(
        self,
        universe: list[dict[str, str]],
        excluded: list[dict[str, str]],
        records: dict[str, dict[str, object]],
        start: date,
        end: date,
    ) -> None:
        symbols = []
        for member in universe:
            record = records.get(member["symbol"], {})
            symbols.append(
                {
                    "symbol": member["symbol"],
                    "requested_series": "EQ",
                    "company_name": member.get("company_name", ""),
                    "isin": member.get("isin", ""),
                    "source": "project data/universe.csv; retrieved only from official NSE security-history page",
                    "download_status": record.get("status", "NOT_REQUESTED"),
                }
            )
        value = {
            "generated_at": _utc_now(),
            "source_universe": str(self.universe_path),
            "source_universe_sha256": _sha256_file(self.universe_path),
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "requested_series": "EQ",
            "symbols_requested": len(records),
            "symbols_in_project_universe": len(universe) + len(excluded),
            "eligible_eq_symbols": len(universe),
            "symbols": symbols,
            "excluded_symbols": excluded,
        }
        _atomic_write_text(
            self.universe_json_path,
            json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )

    def _write_report(
        self,
        manifest: dict[str, object],
        records: dict[str, dict[str, object]],
        start: date,
        end: date,
    ) -> None:
        aggregate = manifest.get("aggregate", {})
        assert isinstance(aggregate, dict)
        covered = [record for record in records.values() if int(record.get("row_count", 0)) > 0]
        spans = [
            (date.fromisoformat(str(record["actual_last_date"])) - date.fromisoformat(str(record["actual_first_date"]))).days + 1
            for record in covered
        ]
        distribution = {
            "0 rows": sum(int(record.get("row_count", 0)) == 0 for record in records.values()),
            "under 1 year": sum(0 < span < 365 for span in spans),
            "1 to under 3 years": sum(365 <= span < 365 * 3 for span in spans),
            "3 to under 5 years": sum(365 * 3 <= span < 365 * 5 for span in spans),
            "5 years or more": sum(span >= 365 * 5 for span in spans),
        }
        actual_firsts = [str(record["actual_first_date"]) for record in covered if record.get("actual_first_date")]
        actual_lasts = [str(record["actual_last_date"]) for record in covered if record.get("actual_last_date")]
        multi_year_symbols = sum(
            record.get("status") == "SUCCESS"
            and record.get("actual_first_date")
            and record.get("actual_last_date")
            and (date.fromisoformat(str(record["actual_last_date"])) - date.fromisoformat(str(record["actual_first_date"]))).days >= 730
            for record in records.values()
        )
        overall_coverage = (
            f"{min(actual_firsts)} through {max(actual_lasts)}"
            if actual_firsts and actual_lasts
            else "no validated equity rows"
        )
        output_files = sorted(self.output_dir.glob("*.csv"))
        output_files = [path for path in output_files if not path.name.lower().startswith("index_")]
        excluded = manifest.get("excluded_universe_symbols", [])
        excluded_text = "; ".join(
            f"{item.get('symbol')}: {item.get('reason')}" for item in excluded if isinstance(item, dict)
        ) if isinstance(excluded, list) else ""
        index_coverage: dict[str, str] = {}
        for key, filename in (("NIFTY", "index_NSEI.csv"), ("VIX", "index_INDIAVIX.csv")):
            path = self.output_dir / filename
            if path.is_file():
                try:
                    with path.open("r", encoding="utf-8-sig", newline="") as handle:
                        reader = csv.DictReader(handle)
                        dates = [str(row.get("Date", "")).strip() for row in reader if row.get("Date")]
                    index_coverage[key] = f"{min(dates)}..{max(dates)}; {len(dates)} rows" if dates else "present; no rows"
                except (OSError, csv.Error):
                    index_coverage[key] = "present; could not audit"
            else:
                index_coverage[key] = "not present"
        page_session = manifest.get("page_session", {})
        preflight = manifest.get("source_preflight", {})
        if not isinstance(preflight, dict):
            preflight = {}
        findings = preflight.get("findings", [])
        if not isinstance(findings, list):
            findings = []
        finding_lines = [
            f"- {item.get('summary', item)}" if isinstance(item, dict) else f"- {item}"
            for item in findings
        ]
        if not finding_lines:
            finding_lines = ["- No separate source-preflight evidence is stored in this manifest."]
        held = (
            preflight.get("status") == "BLOCKED_SOURCE_INTEGRITY"
            and aggregate.get("successful_symbols", 0) == 0
            and aggregate.get("total_rows", 0) == 0
        )
        acquisition_note = (
            "Bulk acquisition and historical-cache import were held because the source preflight failed integrity checks."
            if held
            else "This script only downloads and audits raw NSE exports; the separate historical-cache importer is not invoked automatically."
        )
        required_majority = max(1, (aggregate.get("requested_symbols", 0) + 1) // 2)
        index_inputs_ready = all(
            value not in {"not present", "present; no rows", "present; could not audit"}
            for value in index_coverage.values()
        )
        system_gate = (
            "PASS"
            if multi_year_symbols >= required_majority and index_inputs_ready and not held
            else "BLOCKED"
        )
        gate_reason = (
            "majority multi-year coverage and NIFTY/VIX inputs are present"
            if system_gate == "PASS"
            else "requires majority-universe multi-year EQ history plus both NIFTY and VIX inputs; source-integrity hold is active"
            if held
            else "requires majority-universe multi-year EQ history plus both NIFTY and VIX inputs"
        )
        challenge_note = preflight.get("access_control", "Not recorded")
        report = [
            "# NSE Equity Data Acquisition Report",
            "",
            f"Generated: {_utc_now()}",
            "",
            "## Scope and source",
            "",
            f"- Official page: {EQUITY_SOURCE_URL}",
            f"- Official endpoint: `{EQUITY_ENDPOINT}`",
            "- Range: " + f"{start.isoformat()} through {end.isoformat()} (inclusive).",
            "- Series requested: `EQ`; the current endpoint has no series query parameter, so non-EQ rows are excluded locally.",
            "- Price basis: NSE values are retained without corporate-action adjustment, gap filling, or synthetic rows.",
            "- Official CSV source fields normalized: Symbol, Series, Date, Open Price, High Price, Low Price, Close Price, and Total Traded Quantity. Comma grouping is removed from numeric values; decimal scale is retained.",
            "- Current project universe has survivorship bias; this run does not provide point-in-time constituents.",
            "",
            "## Acquisition summary",
            "",
            f"- Symbols requested: {aggregate.get('requested_symbols', 0)}",
            f"- Existing universe: {manifest.get('project_universe_rows', 'unknown')} rows; eligible EQ symbols: {aggregate.get('requested_symbols', 0)}.",
            f"- Project-universe exclusions: {excluded_text or 'none'}",
            f"- Successful: {aggregate.get('successful_symbols', 0)}",
            f"- Partial: {aggregate.get('partial_symbols', 0)}",
            f"- Unavailable: {aggregate.get('unavailable_symbols', 0)}",
            f"- Failed: {aggregate.get('failed_symbols', 0)}",
            f"- Not attempted: {aggregate.get('not_attempted_symbols', 0)}",
            f"- Rows: {aggregate.get('total_rows', 0)}",
            f"- Per-symbol coverage distribution: {json.dumps(distribution, sort_keys=True)}",
            f"- Symbols spanning at least two years between first and last rows: {multi_year_symbols} (majority target: {required_majority}).",
            f"- Overall equity data coverage: {overall_coverage}.",
            f"- CSV files present: {len(output_files)}",
            "",
            "## Data-quality audit",
            "",
            f"- Duplicate rows encountered: {aggregate.get('duplicate_rows', 0)}",
            f"- Invalid OHLC rows: {aggregate.get('invalid_ohlc_rows', 0)}",
            f"- Nonpositive price fields: {aggregate.get('nonpositive_price_fields', 0)}",
            f"- Negative volume rows: {aggregate.get('negative_volume_rows', 0)}",
            f"- Zero-volume rows: {aggregate.get('zero_volume_rows', 0)}",
            f"- Malformed numeric fields: {aggregate.get('malformed_numeric_fields', 0)}",
            f"- Suspicious discontinuities (40% close/open gap heuristic): {aggregate.get('suspicious_discontinuities', 0)}",
            f"- Non-EQ rows excluded: {aggregate.get('non_eq_rows_excluded', 0)}",
            "- Discontinuities are warnings only and are retained; they are not automatically classified as corporate actions.",
            "- Missing weekdays are not classified as trading-date gaps because no official exchange-session calendar is joined here.",
            "",
            "## NSE endpoint preflight and limitations",
            "",
            "The page's served JavaScript calls the endpoint with `symbol`, `from`, and `to`; its CSV action appends `csv=true`. "
            "It passes the selected autocomplete `symbol` value, and the current page enforces a maximum range of 365 days. "
            "No `series` request parameter is sent. Recorded source-preflight findings:",
            "",
            *finding_lines,
            "",
            f"{acquisition_note} No alternate endpoint or access-control workaround was attempted.",
            "",
            "NSE's autocomplete currently supplies symbols from its available security catalogue. The endpoint accepts a symbol string, not an ISIN or former-symbol mapping; this inspection therefore cannot establish recovery of delisted, renamed, or merged issues that are absent from the current catalogue. Historical coverage of old symbols must be audited separately when NSE offers an authorized lookup. The project universe itself contains only current names and cannot remove survivorship bias.",
            "",
            "## Index prerequisites and Phase 3B gate",
            "",
            "NIFTY 50 and India VIX files/manifests are maintained by the separate index downloader. They are not changed by this equity downloader.",
            f"- NIFTY: {index_coverage['NIFTY']}",
            f"- India VIX: {index_coverage['VIX']}",
            f"- Equity data gate: **{system_gate}** ({gate_reason}).",
            f"- Historical cache: {'not imported because the integrity preflight held bulk acquisition' if held else 'import is a separate explicit data-only step and is not run by this downloader'}.",
            "",
            "## Request/session status",
            "",
            f"- NSE page session: {page_session.get('status', 'NOT_REQUESTED') if isinstance(page_session, dict) else 'UNKNOWN'}",
            f"- CAPTCHA or explicit access block: {str(challenge_note).rstrip('.')}.",
            "",
            "See `data/nse_exports/nse_equity_download_manifest.json` for per-symbol chunk status and provenance.",
        ]
        _atomic_write_text(self.report_path, "\n".join(report) + "\n")

    def _read_manifest(self) -> dict[str, object]:
        if not self.manifest_path.exists():
            return {}
        try:
            value = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"cannot read existing manifest {self.manifest_path}: {error}") from error
        if not isinstance(value, dict):
            raise ValueError("equity manifest root must be a JSON object")
        return value

    def _write_manifest(self, manifest: dict[str, object]) -> None:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_text(
            self.manifest_path,
            json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        )

    @staticmethod
    def _error_record(error: NseDownloadError, start: date | None = None, end: date | None = None) -> dict[str, object]:
        result: dict[str, object] = {
            "status": error.request_status,
            "error": str(error),
            "http_status": error.http_status,
            "content_type": error.content_type,
            "retry_count": error.retries,
            "url": error.url,
            "response_excerpt": error.response_excerpt,
        }
        if start is not None and end is not None:
            result.update({"chunk_start": start.isoformat(), "chunk_end": end.isoformat()})
        return result


def download_nse_equities_main(argv: list[str] | None = None) -> int:
    project_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description="Download official NSE security-wise EQ history CSVs")
    parser.add_argument("--start", default="2020-01-01", help="Inclusive ISO date; default 2020-01-01")
    parser.add_argument("--end", default=(date.today() - timedelta(days=1)).isoformat(), help="Inclusive ISO date; default yesterday")
    parser.add_argument("--symbols-file", default="data/universe.csv")
    parser.add_argument("--symbols", help="Comma-separated project-universe EQ symbols (optional subset)")
    parser.add_argument("--output-dir", default="data/nse_exports")
    parser.add_argument("--manifest", help="Defaults to nse_equity_download_manifest.json in output directory")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--chunk-days", type=int, default=MAX_CHUNK_DAYS)
    args = parser.parse_args(argv)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if start > end:
        parser.error("--start must be on or before --end")
    universe_path = Path(args.symbols_file)
    output_dir = Path(args.output_dir)
    manifest_path = Path(args.manifest) if args.manifest else None
    for path_name, path_value in (("symbols file", universe_path), ("output directory", output_dir)):
        if not path_value.is_absolute():
            path_value = project_root / path_value
        if path_name == "symbols file":
            universe_path = path_value
        else:
            output_dir = path_value
    if manifest_path is not None and not manifest_path.is_absolute():
        manifest_path = project_root / manifest_path
    requested_symbols = tuple(args.symbols.split(",")) if args.symbols else None
    downloader = NseEquityDownloader(
        universe_path,
        output_dir,
        manifest_path=manifest_path,
        timeout=args.timeout,
        retries=args.retries,
        min_request_interval_seconds=args.interval_seconds,
        chunk_days=args.chunk_days,
    )
    try:
        manifest = downloader.download(start, end, symbols=requested_symbols)
    except (OSError, ValueError, NseDownloadError) as error:
        print(f"NSE equity download failed: {error}")
        print(f"Manifest: {downloader.manifest_path}")
        return 2
    aggregate = manifest.get("aggregate", {})
    assert isinstance(aggregate, dict)
    print(
        f"NSE EQ symbols: requested={aggregate.get('requested_symbols', 0)} "
        f"successful={aggregate.get('successful_symbols', 0)} partial={aggregate.get('partial_symbols', 0)} "
        f"unavailable={aggregate.get('unavailable_symbols', 0)} failed={aggregate.get('failed_symbols', 0)}"
    )
    print(f"Manifest: {downloader.manifest_path}")
    print(f"Report: {downloader.report_path}")
    if aggregate.get("failed_symbols", 0) or aggregate.get("partial_symbols", 0):
        return 1
    return 0
