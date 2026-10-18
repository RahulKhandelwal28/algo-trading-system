"""Rate-limited downloads of historical index data from NSE's official site.

NSE's historical pages currently call the JSON endpoints below and build CSV
files in the browser. This module follows that official request flow, validates
the JSON payload, and writes the requested normalized CSVs locally.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import http.cookiejar
import json
import os
import socket
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Mapping, Protocol

NSE_BASE_URL = "https://www.nseindia.com"
MAX_CHUNK_DAYS = 365
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
)


@dataclass(frozen=True)
class IndexDefinition:
    key: str
    index_name: str
    source_url: str
    endpoint: str
    output_filename: str
    output_columns: tuple[str, ...]
    source_fields: tuple[str, ...]
    price_fields: tuple[str, ...]


INDEX_DEFINITIONS: dict[str, IndexDefinition] = {
    "nifty50": IndexDefinition(
        key="nifty50",
        index_name="NIFTY 50",
        source_url=f"{NSE_BASE_URL}/reports-indices-historical-index-data",
        endpoint="/api/historicalOR/indicesHistory",
        output_filename="index_NSEI.csv",
        output_columns=("Date", "Open", "High", "Low", "Close", "Shares Traded", "Turnover"),
        source_fields=(
            "EOD_TIMESTAMP",
            "EOD_OPEN_INDEX_VAL",
            "EOD_HIGH_INDEX_VAL",
            "EOD_LOW_INDEX_VAL",
            "EOD_CLOSE_INDEX_VAL",
            "HIT_TRADED_QTY",
            "HIT_TURN_OVER",
        ),
        price_fields=("Open", "High", "Low", "Close"),
    ),
    "indiavix": IndexDefinition(
        key="indiavix",
        index_name="INDIA VIX",
        source_url=f"{NSE_BASE_URL}/reports-indices-historical-vix",
        endpoint="/api/historicalOR/vixhistory",
        output_filename="index_INDIAVIX.csv",
        output_columns=(
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Prev. Close",
            "Change",
            "% Change",
        ),
        source_fields=(
            "EOD_TIMESTAMP",
            "EOD_OPEN_INDEX_VAL",
            "EOD_HIGH_INDEX_VAL",
            "EOD_LOW_INDEX_VAL",
            "EOD_CLOSE_INDEX_VAL",
            "EOD_PREV_CLOSE",
            "VIX_PTS_CHG",
            "VIX_PERC_CHG",
        ),
        price_fields=("Open", "High", "Low", "Close", "Prev. Close"),
    ),
}

_FIELD_TO_OUTPUT = {
    "EOD_TIMESTAMP": "Date",
    "EOD_OPEN_INDEX_VAL": "Open",
    "EOD_HIGH_INDEX_VAL": "High",
    "EOD_LOW_INDEX_VAL": "Low",
    "EOD_CLOSE_INDEX_VAL": "Close",
    "HIT_TRADED_QTY": "Shares Traded",
    "HIT_TURN_OVER": "Turnover",
    "EOD_PREV_CLOSE": "Prev. Close",
    "VIX_PTS_CHG": "Change",
    "VIX_PERC_CHG": "% Change",
}


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes

    def header(self, name: str) -> str | None:
        wanted = name.lower()
        return next((str(value) for key, value in self.headers.items() if key.lower() == wanted), None)


class HttpSession(Protocol):
    def get(self, url: str, *, headers: Mapping[str, str], timeout: float) -> HttpResponse: ...


class UrllibNseSession:
    """Cookie-aware HTTPS session using only the Python standard library."""

    def __init__(self) -> None:
        cookies = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            _OfficialNseRedirectHandler(),
            urllib.request.HTTPCookieProcessor(cookies),
        )

    def get(self, url: str, *, headers: Mapping[str, str], timeout: float) -> HttpResponse:
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with self._opener.open(request, timeout=timeout) as response:
                return HttpResponse(response.status, dict(response.headers.items()), response.read())
        except urllib.error.HTTPError as error:
            return HttpResponse(error.code, dict(error.headers.items()), error.read())


class NseDownloadError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        request_status: str = "FAILED",
        http_status: int | None = None,
        content_type: str | None = None,
        retries: int = 0,
        url: str | None = None,
        response_excerpt: str | None = None,
    ) -> None:
        super().__init__(message)
        self.request_status = request_status
        self.http_status = http_status
        self.content_type = content_type
        self.retries = retries
        self.url = url
        self.response_excerpt = response_excerpt


class NseAccessBlocked(NseDownloadError):
    """NSE returned an access-control or challenge response; do not work around it."""


class _OfficialNseRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow HTTPS redirects only when they remain on NSE's official host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        parsed = urllib.parse.urlparse(newurl)
        if parsed.scheme != "https" or parsed.hostname not in {"www.nseindia.com", "nseindia.com"}:
            raise urllib.error.URLError("refusing redirect outside NSE's official HTTPS host")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class NseMalformedResponse(NseDownloadError):
    pass


class NseNoDataResponse(NseDownloadError):
    def __init__(self, message: str, **kwargs: object) -> None:
        super().__init__(message, request_status="NO_DATA", **kwargs)


class NseIndexDownloader:
    """Download, validate, resume, and atomically write official NSE index CSVs."""

    def __init__(
        self,
        output_dir: str | Path,
        *,
        manifest_path: str | Path | None = None,
        timeout: float = 10.0,
        retries: int = 2,
        min_request_interval_seconds: float = 1.25,
        chunk_days: int = MAX_CHUNK_DAYS,
        backoff_seconds: float = 1.0,
        session: HttpSession | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if retries < 0:
            raise ValueError("retries cannot be negative")
        if min_request_interval_seconds < 0:
            raise ValueError("minimum request interval cannot be negative")
        if not 1 <= chunk_days <= MAX_CHUNK_DAYS:
            raise ValueError(f"chunk_days must be between 1 and {MAX_CHUNK_DAYS}")
        if backoff_seconds < 0:
            raise ValueError("backoff_seconds cannot be negative")
        self.output_dir = Path(output_dir)
        self.manifest_path = Path(manifest_path) if manifest_path else self.output_dir / "nse_index_download_manifest.json"
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
        indexes: tuple[str, ...] = ("nifty50", "indiavix"),
    ) -> dict[str, object]:
        if start > end:
            raise ValueError("start date must be on or before end date")
        unknown = set(indexes) - INDEX_DEFINITIONS.keys()
        if unknown:
            raise ValueError(f"unknown index key(s): {sorted(unknown)}")
        if len(set(indexes)) != len(indexes):
            raise ValueError("each index may be requested only once per run")

        self.output_dir.mkdir(parents=True, exist_ok=True)
        manifest = self._read_manifest()
        manifest.update(
            {
                "schema_version": 1,
                "source": "National Stock Exchange of India (official website)",
                "source_base_url": NSE_BASE_URL,
                "downloaded_at": _utc_now(),
                "request_policy": {
                    "timeout_seconds": self.timeout,
                    "max_retries_after_initial_attempt": self.retries,
                    "min_interval_seconds": self.min_request_interval_seconds,
                    "maximum_chunk_days_inclusive": self.chunk_days,
                    "backoff_seconds_base": self.backoff_seconds,
                },
            }
        )
        index_records = manifest.setdefault("indexes", {})
        assert isinstance(index_records, dict)

        for key in indexes:
            definition = INDEX_DEFINITIONS[key]
            old_record = index_records.get(key, {})
            if not isinstance(old_record, dict):
                old_record = {}
            record = self._new_index_record(definition, start, end, old_record)
            index_records[key] = record
            self._write_manifest(manifest)
            try:
                self._download_one(definition, start, end, record, manifest)
            except NseDownloadError as error:
                record["request_status"] = "BLOCKED" if isinstance(error, NseAccessBlocked) else error.request_status
                record["validation_status"] = "FAIL"
                record["error"] = str(error)
                record["last_http_status"] = error.http_status
                record["last_content_type"] = error.content_type
                record["retry_count"] = int(record.get("retry_count", 0)) + error.retries
                record["failed_url"] = error.url
                record["response_excerpt"] = error.response_excerpt
                self._finalize_record(definition, record, start, end)
                manifest["downloaded_at"] = _utc_now()
                self._write_manifest(manifest)
                raise
            except Exception as error:
                record["request_status"] = "FAILED"
                record["validation_status"] = "FAIL"
                record["error"] = f"{type(error).__name__}: {error}"
                self._finalize_record(definition, record, start, end)
                manifest["downloaded_at"] = _utc_now()
                self._write_manifest(manifest)
                raise

        self._cross_validate(index_records, indexes, start, end)
        manifest["downloaded_at"] = _utc_now()
        self._write_manifest(manifest)
        return manifest

    def _new_index_record(
        self,
        definition: IndexDefinition,
        start: date,
        end: date,
        old_record: dict[str, object],
    ) -> dict[str, object]:
        previous_chunks = old_record.get("completed_chunks", [])
        if not isinstance(previous_chunks, list):
            previous_chunks = []
        output_path = self.output_dir / definition.output_filename
        checksum = _sha256_file(output_path) if output_path.is_file() else None
        prior_checksum = old_record.get("checksum_sha256")
        if not checksum or checksum != prior_checksum:
            # A file edited or removed outside the downloader invalidates its
            # saved coverage ledger. Refetching is idempotent and safe.
            previous_chunks = []
        latest_completed_chunk = next(
            (
                chunk
                for chunk in reversed(previous_chunks)
                if isinstance(chunk, dict) and chunk.get("status") == "SUCCESS"
            ),
            {},
        )
        return {
            "index": definition.index_name,
            "source_url": definition.source_url,
            "endpoint": definition.endpoint,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "output_file": str(output_path),
            "actual_first_date": None,
            "actual_last_date": None,
            "rows": 0,
            "downloaded_at": _utc_now(),
            "request_status": "PENDING",
            "http_status": old_record.get("http_status") or latest_completed_chunk.get("http_status"),
            "content_type": old_record.get("content_type") or latest_completed_chunk.get("content_type"),
            "retry_count": 0,
            "validation_status": "PENDING",
            "duplicates_encountered": 0,
            "data_quality_anomalies": [],
            "missing_weekday_dates": [],
            "missing_trading_dates_vs_other_downloaded_index": [],
            "sha256": None,
            "checksum_sha256": None,
            "completed_chunks": previous_chunks,
            "requests": [],
        }

    def _download_one(
        self,
        definition: IndexDefinition,
        start: date,
        end: date,
        record: dict[str, object],
        manifest: dict[str, object],
    ) -> None:
        output_path = self.output_dir / definition.output_filename
        rows_by_date, existing_duplicates = self._read_existing_csv(output_path, definition)
        record["duplicates_encountered"] = int(record.get("duplicates_encountered", 0)) + existing_duplicates

        chunks = record["completed_chunks"]
        assert isinstance(chunks, list)
        completed_ranges = [
            (date.fromisoformat(str(chunk["start"])), date.fromisoformat(str(chunk["end"])))
            for chunk in chunks
            if isinstance(chunk, dict) and chunk.get("status") == "SUCCESS"
        ]
        missing_ranges = _subtract_ranges(start, end, completed_ranges)
        if missing_ranges:
            self._bootstrap_page(definition, record)
        else:
            record["request_status"] = "SUCCESS_CACHED"

        for missing_start, missing_end in missing_ranges:
            chunk_start = missing_start
            while chunk_start <= missing_end:
                chunk_end = min(missing_end, chunk_start + timedelta(days=self.chunk_days - 1))
                try:
                    payload, response, used_retries, request_url = self._fetch_payload(definition, chunk_start, chunk_end)
                except NseDownloadError as error:
                    request_log = record.setdefault("requests", [])
                    assert isinstance(request_log, list)
                    request_log.append(
                        {
                            "kind": "data",
                            "url": error.url,
                            "status": error.request_status,
                            "http_status": error.http_status,
                            "content_type": error.content_type,
                            "retry_count": error.retries,
                            "chunk_start": chunk_start.isoformat(),
                            "chunk_end": chunk_end.isoformat(),
                            "error": str(error),
                        }
                    )
                    record["http_status"] = error.http_status
                    record["content_type"] = error.content_type
                    raise

                record["http_status"] = response.status_code
                record["content_type"] = response.header("content-type")
                request_log = record.setdefault("requests", [])
                assert isinstance(request_log, list)
                request_entry: dict[str, object] = {
                    "kind": "data",
                    "url": request_url,
                    "status": "RESPONSE_RECEIVED",
                    "http_status": response.status_code,
                    "content_type": response.header("content-type"),
                    "retry_count": used_retries,
                    "chunk_start": chunk_start.isoformat(),
                    "chunk_end": chunk_end.isoformat(),
                }
                request_log.append(request_entry)
                try:
                    normalized_rows, payload_duplicates = _normalize_payload(
                        payload, definition, chunk_start, chunk_end
                    )
                except NseDownloadError as error:
                    error.http_status = error.http_status or response.status_code
                    error.content_type = error.content_type or response.header("content-type")
                    error.retries = used_retries
                    error.url = error.url or request_url
                    request_entry["status"] = (
                        "MALFORMED" if isinstance(error, NseMalformedResponse) else error.request_status
                    )
                    request_entry["error"] = str(error)
                    raise
                except ValueError as error:
                    malformed = NseMalformedResponse(
                        f"NSE returned malformed {definition.index_name} row: {error}",
                        http_status=response.status_code,
                        content_type=response.header("content-type"),
                        retries=used_retries,
                        url=request_url,
                    )
                    request_entry["status"] = "MALFORMED"
                    request_entry["error"] = str(malformed)
                    raise malformed from error

                record["retry_count"] = int(record.get("retry_count", 0)) + used_retries
                request_entry["status"] = "SUCCESS"
                request_entry["rows"] = len(normalized_rows)
                record["duplicates_encountered"] = int(record.get("duplicates_encountered", 0)) + payload_duplicates
                for row in normalized_rows:
                    trading_date = row["Date"]
                    if trading_date in rows_by_date:
                        record["duplicates_encountered"] = int(record.get("duplicates_encountered", 0)) + 1
                    rows_by_date[trading_date] = row

                if not normalized_rows:
                    # Empty chunks could mean an NSE no-data response or an
                    # upstream schema change. Do not call them covered.
                    raise NseNoDataResponse(
                        f"NSE returned no {definition.index_name} rows for {chunk_start}..{chunk_end}",
                        http_status=response.status_code,
                        content_type=response.header("content-type"),
                        retries=used_retries,
                        url=request_url,
                    )

                self._atomic_write_csv(output_path, definition.output_columns, rows_by_date)
                completed_chunk = {
                    "start": chunk_start.isoformat(),
                    "end": chunk_end.isoformat(),
                    "rows_received": len(normalized_rows),
                    "status": "SUCCESS",
                    "http_status": response.status_code,
                    "content_type": response.header("content-type"),
                    "retry_count": used_retries,
                    "completed_at": _utc_now(),
                }
                _upsert_chunk(chunks, completed_chunk)
                record["request_status"] = "SUCCESS"
                record["downloaded_at"] = _utc_now()
                self._finalize_record(definition, record, start, end)
                self._write_manifest(manifest)
                chunk_start = chunk_end + timedelta(days=1)

        if not missing_ranges:
            record["request_status"] = "SUCCESS_CACHED"
        elif record.get("request_status") != "SUCCESS":
            record["request_status"] = "SUCCESS"
        self._finalize_record(definition, record, start, end)
        self._write_manifest(manifest)

    def _bootstrap_page(self, definition: IndexDefinition, record: dict[str, object]) -> None:
        response, retries = self._request(
            definition.source_url,
            referer=definition.source_url,
            expected="html",
        )
        request_log = record.setdefault("requests", [])
        assert isinstance(request_log, list)
        request_log.append(
            {
                "kind": "page_session",
                "url": definition.source_url,
                "status": "SUCCESS",
                "http_status": response.status_code,
                "content_type": response.header("content-type"),
                "retry_count": retries,
            }
        )
        record["http_status"] = response.status_code
        record["content_type"] = response.header("content-type")
        record["retry_count"] = int(record.get("retry_count", 0)) + retries

    def _fetch_payload(
        self,
        definition: IndexDefinition,
        start: date,
        end: date,
    ) -> tuple[object, HttpResponse, int, str]:
        params = {
            "from": start.strftime("%d-%m-%Y"),
            "to": end.strftime("%d-%m-%Y"),
            # NSE's own CSV button appends csv=true; the live backend returns
            # JSON, which the page maps through its official CSV field list.
            "csv": "true",
        }
        if definition.key == "nifty50":
            params["indexType"] = definition.index_name.upper()
        query = urllib.parse.urlencode(params)
        request_url = f"{NSE_BASE_URL}{definition.endpoint}?{query}"
        response, retries = self._request(
            request_url,
            referer=definition.source_url,
            expected="json",
        )
        try:
            payload = json.loads(response.body.decode("utf-8-sig"), parse_float=Decimal, parse_int=Decimal)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise NseMalformedResponse(
                f"NSE returned malformed JSON for {definition.index_name}: {error}",
                http_status=response.status_code,
                content_type=response.header("content-type"),
                retries=retries,
                url=request_url,
                response_excerpt=_excerpt(response.body),
            ) from error
        return payload, response, retries, request_url

    def _request(
        self,
        url: str,
        *,
        referer: str,
        expected: str,
    ) -> tuple[HttpResponse, int]:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/plain, */*" if expected == "json" else "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
            "Origin": NSE_BASE_URL,
        }
        last_error: Exception | None = None
        last_response: HttpResponse | None = None
        for attempt in range(self.retries + 1):
            self._respect_rate_limit()
            try:
                response = self.session.get(url, headers=headers, timeout=self.timeout)
                last_response = response
            except (TimeoutError, socket.timeout, OSError, urllib.error.URLError) as error:
                last_error = error
                timed_out = _is_timeout(error)
                if attempt < self.retries:
                    self.sleep(self.backoff_seconds * (2**attempt))
                    continue
                status = "TIMEOUT" if timed_out else "NETWORK_ERROR"
                raise NseDownloadError(
                    f"NSE request {status.lower().replace('_', ' ')} after {attempt + 1} attempt(s): {error}",
                    request_status=status,
                    retries=attempt,
                    url=url,
                ) from error

            body_text = response.body[:20000].decode("utf-8", "replace")
            if response.status_code in {401, 403} or _looks_like_challenge(body_text):
                raise NseAccessBlocked(
                    f"NSE access-control response (HTTP {response.status_code}) for {url}; no workaround was attempted",
                    request_status="BLOCKED",
                    http_status=response.status_code,
                    content_type=response.header("content-type"),
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )

            if response.status_code in {408, 429, 500, 502, 503, 504}:
                if attempt < self.retries:
                    self.sleep(self.backoff_seconds * (2**attempt))
                    continue
                request_status = "RATE_LIMITED" if response.status_code == 429 else "HTTP_ERROR"
                raise NseDownloadError(
                    f"NSE returned HTTP {response.status_code} for {url} after {attempt + 1} attempt(s)",
                    request_status=request_status,
                    http_status=response.status_code,
                    content_type=response.header("content-type"),
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )

            if response.status_code != 200:
                raise NseDownloadError(
                    f"NSE returned HTTP {response.status_code} for {url}",
                    request_status="HTTP_ERROR",
                    http_status=response.status_code,
                    content_type=response.header("content-type"),
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )

            content_type = (response.header("content-type") or "").lower()
            if expected == "html" and "html" not in content_type:
                raise NseMalformedResponse(
                    f"NSE historical page returned unexpected Content-Type {content_type or '(missing)'}",
                    http_status=response.status_code,
                    content_type=content_type or None,
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )
            if expected == "json" and "json" not in content_type:
                raise NseMalformedResponse(
                    f"NSE historical API returned unexpected Content-Type {content_type or '(missing)'}; expected JSON",
                    http_status=response.status_code,
                    content_type=content_type or None,
                    retries=attempt,
                    url=url,
                    response_excerpt=_excerpt(response.body),
                )
            return response, attempt

        # The loop always returns or raises. Keep this as a defensive guard.
        raise NseDownloadError(
            f"NSE request failed: {last_error or last_response}",
            request_status="FAILED",
            http_status=last_response.status_code if last_response else None,
            content_type=last_response.header("content-type") if last_response else None,
            retries=self.retries,
            url=url,
        )

    def _respect_rate_limit(self) -> None:
        now = self.monotonic()
        if self._last_request_at is not None:
            wait = self.min_request_interval_seconds - (now - self._last_request_at)
            if wait > 0:
                self.sleep(wait)
        self._last_request_at = self.monotonic()

    def _read_existing_csv(
        self,
        path: Path,
        definition: IndexDefinition,
    ) -> tuple[dict[str, dict[str, str]], int]:
        if not path.exists():
            return {}, 0
        rows: dict[str, dict[str, str]] = {}
        duplicates = 0
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != definition.output_columns:
                raise ValueError(f"existing {path} has an unexpected CSV header")
            for line_number, row in enumerate(reader, start=2):
                try:
                    normalized = _validate_output_row(row, definition)
                except (ValueError, KeyError) as error:
                    raise ValueError(f"invalid existing CSV row {path}:{line_number}: {error}") from error
                day = normalized["Date"]
                if day in rows:
                    duplicates += 1
                rows[day] = normalized
        return rows, duplicates

    def _atomic_write_csv(
        self,
        path: Path,
        columns: tuple[str, ...],
        rows_by_date: Mapping[str, Mapping[str, str]],
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="raise", lineterminator="\n")
                writer.writeheader()
                for day in sorted(rows_by_date):
                    writer.writerow({column: rows_by_date[day][column] for column in columns})
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if temporary.exists():
                temporary.unlink()

    def _read_manifest(self) -> dict[str, object]:
        if not self.manifest_path.exists():
            return {}
        try:
            value = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"cannot read existing manifest {self.manifest_path}: {error}") from error
        if not isinstance(value, dict):
            raise ValueError(f"manifest root must be a JSON object: {self.manifest_path}")
        return value

    def _write_manifest(self, manifest: dict[str, object]) -> None:
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        _atomic_write_text(self.manifest_path, payload)

    def _finalize_record(
        self,
        definition: IndexDefinition,
        record: dict[str, object],
        start: date,
        end: date,
    ) -> None:
        path = self.output_dir / definition.output_filename
        rows: dict[str, dict[str, str]] = {}
        duplicates = int(record.get("duplicates_encountered", 0))
        if path.exists():
            rows, file_duplicates = self._read_existing_csv(path, definition)
            # Rewriting removes any duplicate dates found in a previous file.
            if file_duplicates:
                self._atomic_write_csv(path, definition.output_columns, rows)
        requested_rows = [day for day in rows if start.isoformat() <= day <= end.isoformat()]
        requested_rows.sort()
        record["actual_first_date"] = requested_rows[0] if requested_rows else None
        record["actual_last_date"] = requested_rows[-1] if requested_rows else None
        record["rows"] = len(requested_rows)
        record["duplicates_encountered"] = duplicates
        observed = set(requested_rows)
        weekday_dates = [
            (start + timedelta(days=offset)).isoformat()
            for offset in range((end - start).days + 1)
            if (start + timedelta(days=offset)).weekday() < 5
        ]
        record["missing_weekday_dates"] = [day for day in weekday_dates if day not in observed]
        record["missing_weekday_dates_note"] = (
            "Candidate weekday gaps include NSE market holidays; this is not an authoritative exchange-session calendar."
        )
        record["validation"] = {
            "requested_start_date_present": start.isoformat() in observed,
            "requested_end_date_present": end.isoformat() in observed,
            "rows_in_requested_range": len(requested_rows),
            "first_and_last_session_match_not_independently_verified": True,
            "duplicate_dates_in_output": 0,
            "invalid_ohlc_rows": 0,
            "nonpositive_price_rows": 0,
            "malformed_numeric_fields": 0,
            "weekday_gaps_candidate_count": len(record["missing_weekday_dates"]),
            "weekday_gaps_are_unclassified_market_holidays": True,
        }
        anomalies: list[dict[str, object]] = []
        invalid_ohlc_rows = 0
        nonpositive_price_rows = 0
        malformed_numeric_fields = 0
        for day in requested_rows:
            _, issues = _inspect_output_row(rows[day], definition)
            if not issues:
                continue
            anomalies.append({"date": day, "issues": issues})
            invalid_ohlc_rows += int("invalid_ohlc" in issues)
            nonpositive_price_rows += int(any(issue.startswith("nonpositive_price:") for issue in issues))
            malformed_numeric_fields += sum(issue.startswith("malformed_numeric:") for issue in issues)
        record["data_quality_anomalies"] = anomalies[:100]
        record["validation"]["invalid_ohlc_rows"] = invalid_ohlc_rows
        record["validation"]["nonpositive_price_rows"] = nonpositive_price_rows
        record["validation"]["malformed_numeric_fields"] = malformed_numeric_fields
        record["validation"]["data_quality_issue_rows"] = len(anomalies)
        record["validation"]["data_quality_anomaly_samples"] = min(100, len(anomalies))
        checksum = _sha256_file(path) if path.exists() else None
        record["sha256"] = checksum
        record["checksum_sha256"] = checksum
        if checksum:
            record["validation"]["duplicate_dates_in_output"] = 0
            if record.get("validation_status") != "FAIL":
                record["validation_status"] = "WARN_DATA_QUALITY" if anomalies else "PASS_WITH_CALENDAR_CAVEAT"
        elif record.get("request_status") in {"SUCCESS", "SUCCESS_CACHED"}:
            record["validation_status"] = "FAIL"
        record["downloaded_at"] = _utc_now()

    def _cross_validate(
        self,
        index_records: dict[str, object],
        indexes: tuple[str, ...],
        start: date,
        end: date,
    ) -> None:
        if not {"nifty50", "indiavix"}.issubset(indexes):
            return
        nifty_record = index_records.get("nifty50")
        vix_record = index_records.get("indiavix")
        if not isinstance(nifty_record, dict) or not isinstance(vix_record, dict):
            return
        nifty_dates = self._dates_for(INDEX_DEFINITIONS["nifty50"], start, end)
        vix_dates = self._dates_for(INDEX_DEFINITIONS["indiavix"], start, end)
        nifty_record["missing_trading_dates_vs_other_downloaded_index"] = sorted(vix_dates - nifty_dates)
        vix_record["missing_trading_dates_vs_other_downloaded_index"] = sorted(nifty_dates - vix_dates)
        for record in (nifty_record, vix_record):
            validation = record.get("validation")
            if isinstance(validation, dict):
                validation["dates_missing_vs_other_downloaded_index"] = len(
                    record["missing_trading_dates_vs_other_downloaded_index"]
                )

    def _dates_for(self, definition: IndexDefinition, start: date, end: date) -> set[str]:
        path = self.output_dir / definition.output_filename
        rows, _ = self._read_existing_csv(path, definition)
        return {day for day in rows if start.isoformat() <= day <= end.isoformat()}


def _normalize_payload(
    payload: object,
    definition: IndexDefinition,
    start: date,
    end: date,
) -> tuple[list[dict[str, str]], int]:
    if not isinstance(payload, dict):
        raise NseMalformedResponse(f"NSE returned unexpected response root for {definition.index_name}")
    data = payload.get("data")
    if not isinstance(data, list):
        error_text = payload.get("message") or payload.get("error") or payload.get("status")
        raise NseMalformedResponse(
            f"NSE response has no data array for {definition.index_name}"
            + (f": {error_text}" if error_text else "")
        )
    if not data:
        message = payload.get("message") or payload.get("error") or "empty data array"
        raise NseNoDataResponse(f"NSE returned no {definition.index_name} data: {message}")

    rows_by_date: dict[str, dict[str, str]] = {}
    duplicates = 0
    for index, source_row in enumerate(data):
        if not isinstance(source_row, dict):
            raise NseMalformedResponse(f"NSE {definition.index_name} row {index} is not a JSON object")
        missing = [field for field in definition.source_fields if field not in source_row]
        if missing:
            raise NseMalformedResponse(
                f"NSE {definition.index_name} row {index} is missing required fields: {missing}"
            )
        actual_index_name = str(source_row.get("EOD_INDEX_NAME", "")).strip().upper()
        if actual_index_name != definition.index_name:
            raise NseMalformedResponse(
                f"unexpected index name {actual_index_name!r}; expected {definition.index_name!r}"
            )
        output_row: dict[str, str] = {}
        try:
            output_row["Date"] = _normalize_date(source_row["EOD_TIMESTAMP"])
        except ValueError as error:
            raise NseMalformedResponse(f"NSE {definition.index_name} row {index} has an invalid date: {error}") from error
        parsed_date = date.fromisoformat(output_row["Date"])
        if not start <= parsed_date <= end:
            raise NseMalformedResponse(
                f"NSE row date {output_row['Date']} is outside requested chunk {start}..{end}"
            )
        for source_field in definition.source_fields[1:]:
            output_field = _FIELD_TO_OUTPUT[source_field]
            try:
                output_row[output_field] = _normalize_number(source_row[source_field], output_field)
            except ValueError:
                # Keep NSE's original null or malformed cell visible in the
                # CSV and flag it in the manifest. Never substitute a price.
                output_row[output_field] = _retained_cell(source_row[source_field])
        try:
            _validate_output_row(output_row, definition)
        except ValueError as error:
            raise NseMalformedResponse(f"NSE {definition.index_name} row {index} has invalid values: {error}") from error
        if output_row["Date"] in rows_by_date:
            duplicates += 1
        rows_by_date[output_row["Date"]] = output_row
    return [rows_by_date[day] for day in sorted(rows_by_date)], duplicates


def _normalize_date(raw: object) -> str:
    if raw is None or isinstance(raw, bool):
        raise ValueError("date is empty or not a string")
    value = str(raw).strip()
    for format_string in ("%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.upper(), format_string).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"unsupported NSE date format: {value!r}")


def _normalize_number(raw: object, field: str) -> str:
    if raw is None or isinstance(raw, bool):
        raise ValueError(f"{field} is null or non-numeric")
    text = str(raw).strip().replace(",", "")
    if not text:
        raise ValueError(f"{field} is empty")
    try:
        number = Decimal(text)
    except InvalidOperation as error:
        raise ValueError(f"{field} is not numeric: {raw!r}") from error
    if not number.is_finite():
        raise ValueError(f"{field} is not finite: {raw!r}")
    # Keep the Decimal's full value and avoid rounding prices or turnover.
    return format(number, "f")


def _validate_output_row(row: Mapping[str, object], definition: IndexDefinition) -> dict[str, str]:
    normalized, _ = _inspect_output_row(row, definition)
    return normalized


def _inspect_output_row(
    row: Mapping[str, object], definition: IndexDefinition
) -> tuple[dict[str, str], list[str]]:
    normalized: dict[str, str] = {}
    if set(row) != set(definition.output_columns):
        missing = set(definition.output_columns) - set(row)
        extra = set(row) - set(definition.output_columns)
        raise ValueError(f"unexpected CSV columns (missing={sorted(missing)}, extra={sorted(extra)})")
    normalized["Date"] = _normalize_date(row["Date"])
    numbers: dict[str, Decimal] = {}
    issues: list[str] = []
    for field in definition.output_columns[1:]:
        try:
            normalized[field] = _normalize_number(row[field], field)
            numbers[field] = Decimal(normalized[field])
        except ValueError:
            normalized[field] = _retained_cell(row[field])
            issues.append(f"malformed_numeric:{field}")
    for field in definition.price_fields:
        if field in numbers and numbers[field] <= 0:
            issues.append(f"nonpositive_price:{field}")
    ohlc_fields = ("Open", "High", "Low", "Close")
    if all(field in numbers for field in ohlc_fields) and (
        numbers["Low"] > numbers["High"]
        or numbers["Low"] > min(numbers["Open"], numbers["Close"])
        or numbers["High"] < max(numbers["Open"], numbers["Close"])
    ):
        issues.append("invalid_ohlc")
    for field in definition.output_columns[1:]:
        if field in numbers and field not in {"Change", "% Change"} and numbers[field] < 0:
            issues.append(f"negative_numeric:{field}")
    return normalized, issues


def _retained_cell(raw: object) -> str:
    if raw is None:
        return ""
    if isinstance(raw, bool):
        return str(raw).lower()
    if isinstance(raw, (dict, list)):
        return json.dumps(raw, ensure_ascii=False, sort_keys=True)
    return str(raw).strip()


def _subtract_ranges(start: date, end: date, covered: list[tuple[date, date]]) -> list[tuple[date, date]]:
    cursor = start
    gaps: list[tuple[date, date]] = []
    for covered_start, covered_end in sorted(covered):
        if covered_end < cursor or covered_start > end:
            continue
        if covered_start > cursor:
            gaps.append((cursor, min(end, covered_start - timedelta(days=1))))
        cursor = max(cursor, covered_end + timedelta(days=1))
        if cursor > end:
            break
    if cursor <= end:
        gaps.append((cursor, end))
    return [(gap_start, gap_end) for gap_start, gap_end in gaps if gap_start <= gap_end]


def _upsert_chunk(chunks: list[object], new_chunk: dict[str, object]) -> None:
    for index, chunk in enumerate(chunks):
        if isinstance(chunk, dict) and (chunk.get("start"), chunk.get("end")) == (
            new_chunk["start"],
            new_chunk["end"],
        ):
            chunks[index] = new_chunk
            return
    chunks.append(new_chunk)


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _excerpt(body: bytes, limit: int = 500) -> str:
    return " ".join(body[:limit].decode("utf-8", "replace").split())[:limit]


def _looks_like_challenge(body: str) -> bool:
    lowered = body.lower()
    return any(
        indicator in lowered
        for indicator in (
            "captcha",
            "verify you are human",
            "access denied",
            "unusual traffic",
            "automated access",
            "request blocked",
        )
    )


def _is_timeout(error: BaseException) -> bool:
    if isinstance(error, (TimeoutError, socket.timeout)):
        return True
    if isinstance(error, urllib.error.URLError):
        return _is_timeout(error.reason) if isinstance(error.reason, BaseException) else False
    return False


def _date_chunks(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=chunk_days - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def download_nse_indices_main(argv: list[str] | None = None) -> int:
    project_root = Path(__file__).resolve().parents[3]
    parser = argparse.ArgumentParser(description="Download historical indices from NSE's official endpoints")
    parser.add_argument("--start", default="2021-01-01", help="Inclusive ISO date; default 2021-01-01")
    parser.add_argument("--end", default=date.today().isoformat(), help="Inclusive ISO date; default today")
    parser.add_argument(
        "--index",
        choices=("both", "nifty50", "indiavix"),
        default="both",
        help="Which official NSE index series to download",
    )
    parser.add_argument("--output-dir", default="data/nse_exports")
    parser.add_argument("--manifest", help="Defaults to <output-dir>/nse_index_download_manifest.json")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--retries", type=int, default=2, help="Retries after the initial request")
    parser.add_argument("--interval-seconds", type=float, default=1.25)
    parser.add_argument("--chunk-days", type=int, default=MAX_CHUNK_DAYS)
    args = parser.parse_args(argv)

    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    if start > end:
        parser.error("--start must be on or before --end")
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = project_root / output_dir
    manifest_path = Path(args.manifest) if args.manifest else None
    if manifest_path is not None and not manifest_path.is_absolute():
        manifest_path = project_root / manifest_path
    indexes = ("nifty50", "indiavix") if args.index == "both" else (args.index,)
    downloader = NseIndexDownloader(
        output_dir,
        manifest_path=manifest_path,
        timeout=args.timeout,
        retries=args.retries,
        min_request_interval_seconds=args.interval_seconds,
        chunk_days=args.chunk_days,
    )
    try:
        manifest = downloader.download(start, end, indexes)
    except NseDownloadError as error:
        print(f"NSE download {error.request_status}: {error}")
        if error.http_status is not None:
            print(f"HTTP status: {error.http_status}; content type: {error.content_type or '(missing)'}")
        if error.response_excerpt:
            print(f"Response excerpt: {error.response_excerpt}")
        print(f"Manifest: {downloader.manifest_path}")
        return 2
    except (OSError, ValueError) as error:
        print(f"NSE index download failed: {error}")
        print(f"Manifest: {downloader.manifest_path}")
        return 2

    index_records = manifest.get("indexes", {})
    assert isinstance(index_records, dict)
    for key in indexes:
        record = index_records[key]
        assert isinstance(record, dict)
        print(
            f"{record['index']}: {record['actual_first_date']}..{record['actual_last_date']}; "
            f"{record['rows']} rows; {record['validation_status']} ({record['output_file']})"
        )
    print(f"Manifest: {downloader.manifest_path}")
    return 0
