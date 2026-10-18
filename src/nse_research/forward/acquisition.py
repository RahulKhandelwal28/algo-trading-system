"""Conservative daily EOD acquisition into the validated Phase 3D intake.

The HTTP adapter is intentionally provider-neutral. It accepts only a batch CSV
or JSON response that identifies ``Series`` on every security record. Yahoo
Finance's chart response does not carry the NSE series field and is therefore
rejected by the schema gate instead of inheriting ``EQ`` from the frozen
universe.
"""

from __future__ import annotations

import csv
from email.utils import parsedate_to_datetime
import hashlib
import io
import json
import math
import os
import re
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

import requests

from nse_research.forward.calendar import NseTradingCalendar
from nse_research.forward.intake import DAILY_COLUMNS, import_daily_eod


class EodAcquisitionError(RuntimeError):
    """Acquisition transport failure with a machine-readable status."""

    def __init__(self, status: str, message: str, *, retry_count: int = 0) -> None:
        super().__init__(message)
        self.status = status
        self.retry_count = retry_count


class DataSourceNotSuitable(ValueError):
    """The response cannot establish the required NSE EQ identity/schema."""


@dataclass(frozen=True)
class EodHttpResponse:
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    url: str
    retry_count: int = 0


class DailyEodSource(Protocol):
    source_name: str
    url_template: str

    def fetch(self, market_date: date, symbols: Iterable[str]) -> EodHttpResponse: ...


_TRANSIENT_HTTP_STATUSES = {408, 425, 429, 500, 502, 503, 504}
_MAX_AUTOMATIC_RETRIES = 5
_MAX_TIMEOUT_SECONDS = 60.0
_MAX_BACKOFF_SECONDS = 30.0
_MAX_RETRY_AFTER_SECONDS = 60.0
_SECRET_QUERY_KEYS = {"api_key", "apikey", "token", "access_token", "authorization", "secret", "crumb"}
_ALIASES: dict[str, set[str]] = {
    "date": {"date", "tradedate", "timestamp", "marketdate"},
    "symbol": {"symbol", "ticker", "scrip", "security", "tradingsymbol"},
    "series": {"series", "slbmhseries", "securityseries"},
    "open": {"open", "openprice"},
    "high": {"high", "highprice"},
    "low": {"low", "lowprice"},
    "close": {"close", "closeprice"},
    "volume": {"volume", "totaltradedquantity", "tradedquantity", "sharestraded", "noofshares"},
}


def _header_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


class HttpDailyEodSource:
    """One conservative HTTPS batch request per date; no authentication or bypass."""

    def __init__(
        self,
        source_name: str,
        url_template: str,
        *,
        session: Any | None = None,
        timeout_seconds: float = 15.0,
        retries: int = 2,
        backoff_seconds: float = 1.0,
        minimum_interval_seconds: float = 1.0,
        rate_limit_state_dir: Path | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        wall_time: Callable[[], float] = time.time,
    ) -> None:
        if not source_name.strip():
            raise ValueError("source_name must be provided")
        timing_values = (timeout_seconds, backoff_seconds, minimum_interval_seconds)
        if any(not math.isfinite(value) for value in timing_values):
            raise ValueError("timeout and delay values must be finite")
        if timeout_seconds <= 0 or retries < 0 or backoff_seconds < 0 or minimum_interval_seconds < 0:
            raise ValueError("timeout must be positive; retries and delays must be nonnegative")
        if timeout_seconds > _MAX_TIMEOUT_SECONDS:
            raise ValueError(f"timeout_seconds must not exceed {_MAX_TIMEOUT_SECONDS:g}")
        if retries > _MAX_AUTOMATIC_RETRIES:
            raise ValueError(f"retries must not exceed {_MAX_AUTOMATIC_RETRIES}")
        if backoff_seconds > _MAX_BACKOFF_SECONDS:
            raise ValueError(f"backoff_seconds must not exceed {_MAX_BACKOFF_SECONDS:g}")
        parsed = urlsplit(url_template)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("source URL must be an absolute HTTPS URL")
        if parsed.username or parsed.password or parsed.fragment:
            raise ValueError("source URL must not contain credentials or a fragment")
        query_keys = {part.split("=", 1)[0].casefold() for part in parsed.query.split("&") if part}
        if query_keys & _SECRET_QUERY_KEYS or any(
            fragment in key for key in query_keys for fragment in ("token", "apikey", "secret", "authori", "password", "crumb")
        ):
            raise ValueError("credentials or access tokens in source URLs are not supported")
        if "{date}" not in url_template:
            raise ValueError("source URL template must include a {date} placeholder")
        self.source_name = source_name.strip()
        self.url_template = url_template
        self.session = session or requests.Session()
        self.timeout_seconds = float(timeout_seconds)
        self.retries = int(retries)
        self.backoff_seconds = float(backoff_seconds)
        self.minimum_interval_seconds = float(minimum_interval_seconds)
        self.rate_limit_state_dir = Path(rate_limit_state_dir).resolve() if rate_limit_state_dir else None
        self.sleep = sleep
        self.monotonic = monotonic
        self.wall_time = wall_time
        self._last_request_at: float | None = None
        host_hash = hashlib.sha256(parsed.netloc.casefold().encode("utf-8")).hexdigest()[:16]
        self._rate_limit_state_path = (
            self.rate_limit_state_dir / f"{host_hash}.json" if self.rate_limit_state_dir else None
        )

    def _url(self, market_date: date, symbols: Iterable[str]) -> str:
        joined = ",".join(quote(str(item).strip(), safe="") for item in sorted(set(symbols)))
        return self.url_template.replace("{date}", quote(market_date.isoformat(), safe="")).replace("{symbols}", joined)

    def _respect_rate_limit(self) -> None:
        delay = 0.0
        if self._last_request_at is not None:
            delay = max(delay, self.minimum_interval_seconds - (self.monotonic() - self._last_request_at))
        if self._rate_limit_state_path and self._rate_limit_state_path.is_file():
            try:
                prior = float(json.loads(self._rate_limit_state_path.read_text(encoding="utf-8"))["last_request_epoch"])
                delay = max(delay, self.minimum_interval_seconds - (self.wall_time() - prior))
            except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
                # Invalid limiter state fails closed rather than permitting a burst.
                delay = max(delay, self.minimum_interval_seconds)
        if delay > 0:
            self.sleep(delay)

    def _mark_request_time(self) -> None:
        self._last_request_at = self.monotonic()
        if self._rate_limit_state_path:
            _atomic_text(
                self._rate_limit_state_path,
                json.dumps({"last_request_epoch": self.wall_time()}, sort_keys=True) + "\n",
            )

    @staticmethod
    def _retry_after(headers: Mapping[str, str], fallback: float) -> float:
        raw = next((value for key, value in headers.items() if str(key).casefold() == "retry-after"), None)
        if raw is None:
            return fallback
        try:
            delay = float(raw)
        except (TypeError, ValueError):
            try:
                retry_time = parsedate_to_datetime(str(raw))
                if retry_time.tzinfo is None:
                    retry_time = retry_time.replace(tzinfo=timezone.utc)
                delay = (retry_time.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()
            except (TypeError, ValueError, OverflowError):
                return fallback
        if math.isnan(delay):
            return fallback
        return max(0.0, delay)

    def fetch(self, market_date: date, symbols: Iterable[str]) -> EodHttpResponse:
        url = self._url(market_date, symbols)
        headers = {
            "Accept": "text/csv, application/json;q=0.8",
            "User-Agent": "Phase3D-Paper-EOD-Adapter/1.0 (personal research; no order execution)",
        }
        for attempt in range(self.retries + 1):
            self._respect_rate_limit()
            self._mark_request_time()
            try:
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=self.timeout_seconds,
                    allow_redirects=False,
                )
            except requests.Timeout as error:
                if attempt < self.retries:
                    self.sleep(min(self.backoff_seconds * (2**attempt), _MAX_BACKOFF_SECONDS))
                    continue
                raise EodAcquisitionError("TIMEOUT", f"source request timed out after {attempt + 1} attempt(s)", retry_count=attempt) from error
            except requests.RequestException as error:
                if attempt < self.retries:
                    self.sleep(min(self.backoff_seconds * (2**attempt), _MAX_BACKOFF_SECONDS))
                    continue
                raise EodAcquisitionError("NETWORK_ERROR", f"source request failed: {type(error).__name__}", retry_count=attempt) from error
            status = int(response.status_code)
            if status in _TRANSIENT_HTTP_STATUSES and attempt < self.retries:
                fallback = min(self.backoff_seconds * (2**attempt), _MAX_BACKOFF_SECONDS)
                delay = self._retry_after(response.headers, fallback) if status == 429 else fallback
                # If the server asks us to wait longer than the adapter's
                # bounded automatic wait, return its 429 response and stop.
                # Retrying earlier would violate Retry-After.
                if delay <= _MAX_RETRY_AFTER_SECONDS:
                    self.sleep(delay)
                    continue
            body = getattr(response, "content", None)
            if body is None:
                body = str(getattr(response, "text", "")).encode("utf-8")
            elif isinstance(body, str):
                body = body.encode("utf-8")
            return EodHttpResponse(
                status_code=status,
                headers=dict(response.headers),
                content=bytes(body),
                url=str(getattr(response, "url", url) or url),
                retry_count=attempt,
            )
        raise AssertionError("bounded retry loop ended without a response or error")


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _atomic_raw(raw_dir: Path, day: date, content: bytes, extension: str) -> Path:
    raw_dir.mkdir(parents=True, exist_ok=True)
    digest = _sha256_bytes(content)
    extension = re.sub(r"[^a-z0-9]", "", extension.casefold()) or "bin"
    base = raw_dir / f"{day.isoformat()}.{extension}"
    path = base
    if path.exists() and _sha256_file(path) != digest:
        path = raw_dir / f"{day.isoformat()}.{digest[:12]}.{extension}"
    if path.exists():
        if _sha256_file(path) != digest:
            raise OSError(f"refusing to replace immutable raw acquisition file: {path}")
        return path
    fd, temporary = tempfile.mkstemp(prefix=f".{day.isoformat()}.", suffix=".tmp", dir=raw_dir)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if _sha256_file(path) != digest:
                path = raw_dir / f"{day.isoformat()}.{digest[:12]}.{extension}"
                if not path.exists():
                    os.link(temporary, path)
                elif _sha256_file(path) != digest:
                    raise OSError(f"refusing to replace immutable raw acquisition file: {path}")
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return path


def _content_extension(content_type: str, content: bytes) -> str:
    kind = content_type.casefold().split(";", 1)[0].strip()
    prefix = content.lstrip()[:16].lower()
    if "html" in kind or prefix.startswith((b"<!doctype html", b"<html")):
        return "html"
    if "json" in kind or prefix.startswith((b"{", b"[")):
        return "json"
    if "csv" in kind or "text/plain" in kind:
        return "csv"
    if b"\n" in content[:4096] and b"," in content[:4096]:
        sample = content[:4096].decode("utf-8", errors="ignore").casefold()
        if "date" in sample and ("symbol" in sample or "ticker" in sample):
            return "csv"
    return "bin"


def _is_access_block(content: bytes) -> bool:
    sample = content[:10000].decode("utf-8", errors="ignore").casefold()
    return any(term in sample for term in ("captcha", "access denied", "request blocked", "robot check", "forbidden"))


def _parse_date(raw: Any) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if re.match(r"^\d{4}-\d{2}-\d{2}[T ]", value):
        try:
            parsed_timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed_timestamp.tzinfo is not None:
                return parsed_timestamp.astimezone(ZoneInfo("Asia/Kolkata")).date().isoformat()
            return parsed_timestamp.date().isoformat()
        except ValueError:
            pass
    candidate = value
    try:
        parsed = date.fromisoformat(candidate)
        return parsed.isoformat() if parsed.isoformat() == candidate else value
    except ValueError:
        pass
    for pattern in ("%d-%m-%Y", "%d-%b-%Y", "%d/%m/%Y", "%d-%B-%Y"):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            continue
    return value


def _extract_records(content: bytes, content_type: str) -> tuple[list[dict[str, Any]], list[str]]:
    extension = _content_extension(content_type, content)
    if extension == "html":
        raise DataSourceNotSuitable("source returned HTML instead of structured EOD data")
    if extension == "json":
        try:
            payload = json.loads(content.decode("utf-8-sig"))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise DataSourceNotSuitable(f"source returned malformed JSON: {error}") from error
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            if isinstance(payload.get("chart"), dict):
                raise DataSourceNotSuitable(
                    "Yahoo-style chart data does not include an explicit NSE Series field"
                )
            rows = None
            for key in ("data", "rows", "records", "results"):
                if isinstance(payload.get(key), list):
                    rows = payload[key]
                    break
            if rows is None:
                raise DataSourceNotSuitable("JSON response has no row collection")
        else:
            raise DataSourceNotSuitable("JSON response must contain a row collection")
        if not rows or not all(isinstance(item, dict) for item in rows):
            raise DataSourceNotSuitable("JSON response rows must be nonempty objects")
        records = [dict(item) for item in rows]
        headers = list(records[0])
    elif extension == "csv":
        try:
            reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""))
            headers = list(reader.fieldnames or [])
            records = [dict(row) for row in reader if row is not None]
        except (UnicodeError, csv.Error) as error:
            raise DataSourceNotSuitable(f"source returned malformed CSV: {error}") from error
        if not headers:
            raise DataSourceNotSuitable("CSV response has no header")
    else:
        raise DataSourceNotSuitable(f"unsupported response content type {content_type or '(missing)'}")

    normalized_header_keys = [_header_key(name) for name in headers]
    if len(normalized_header_keys) != len(set(normalized_header_keys)):
        raise DataSourceNotSuitable("source response contains duplicate/ambiguous columns")
    if any(None in row for row in records):
        raise DataSourceNotSuitable("CSV response contains rows with extra fields")

    lookup = {_header_key(name): name for name in headers}
    selected: dict[str, str] = {}
    for field, aliases in _ALIASES.items():
        source_key = next((lookup[alias] for alias in aliases if alias in lookup), None)
        if source_key:
            selected[field] = source_key
    if "series" not in selected:
        raise DataSourceNotSuitable("source does not provide an explicit NSE Series field; EQ cannot be inferred")
    missing = [field for field in DAILY_COLUMNS if field not in selected]
    if missing:
        raise DataSourceNotSuitable(f"source schema cannot provide required fields: {missing}")
    normalized: list[dict[str, str]] = []
    for record in records:
        values = {field: str(record.get(selected[field]) or "").strip() for field in DAILY_COLUMNS}
        values["date"] = _parse_date(values["date"])
        values["symbol"] = values["symbol"].upper().removesuffix(".NS")
        values["series"] = values["series"].upper()
        normalized.append(values)
    return normalized, headers


def _raw_row_count(content: bytes, content_type: str) -> int | None:
    """Count tabular/object rows when the response shape makes that count factual."""
    extension = _content_extension(content_type, content)
    try:
        if extension == "csv":
            reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""))
            return sum(1 for row in reader if row is not None and any(str(value or "").strip() for value in row.values()))
        if extension == "json":
            payload = json.loads(content.decode("utf-8-sig"))
            if isinstance(payload, list):
                return len(payload)
            if isinstance(payload, dict):
                chart = payload.get("chart")
                if isinstance(chart, dict):
                    results = chart.get("result")
                    if isinstance(results, list):
                        counts = [len(item["timestamp"]) for item in results
                                  if isinstance(item, dict) and isinstance(item.get("timestamp"), list)]
                        return sum(counts) if counts else None
                for key in ("data", "rows", "records", "results"):
                    if isinstance(payload.get(key), list):
                        return len(payload[key])
    except (UnicodeError, csv.Error, json.JSONDecodeError):
        return None
    return None


def _official_index_dates(path: Path) -> tuple[set[date], date | None]:
    dates: set[date] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames:
            raise ValueError(f"official NIFTY calendar has no header: {path}")
        columns = {_header_key(name): name for name in reader.fieldnames}
        date_key = columns.get("date")
        if not date_key:
            raise ValueError(f"official NIFTY calendar has no Date column: {path}")
        for row in reader:
            text = str(row.get(date_key) or "").strip()
            if text:
                dates.add(date.fromisoformat(text))
    return dates, max(dates) if dates else None


def _read_jsonl(path: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    if not path.is_file():
        return records
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
            key = str(item["date"])
            date.fromisoformat(key)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid daily acquisition manifest line {line_number}: {error}") from error
        records[key] = item
    return records


def _write_jsonl(path: Path, records: Mapping[str, Mapping[str, Any]]) -> None:
    content = "".join(
        json.dumps(records[key], sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n"
        for key in sorted(records)
    )
    _atomic_text(path, content)


class DailyEodAcquisitionAdapter:
    """Acquire one dated batch, retain raw bytes, validate, and publish normalized rows."""

    def __init__(
        self,
        *,
        source: DailyEodSource,
        official_nifty_path: Path,
        trading_calendar_path: Path,
        raw_dir: Path,
        normalized_dir: Path,
        intake_manifest_dir: Path,
        daily_manifest_path: Path,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.source = source
        self.official_nifty_path = Path(official_nifty_path).resolve()
        self.trading_calendar_path = Path(trading_calendar_path).resolve()
        self.raw_dir = Path(raw_dir).resolve()
        self.normalized_dir = Path(normalized_dir).resolve()
        self.intake_manifest_dir = Path(intake_manifest_dir).resolve()
        self.daily_manifest_path = Path(daily_manifest_path).resolve()
        self.now = now

    def _cached_success(self, day: date, expected_symbols: set[str]) -> dict[str, Any] | None:
        records = _read_jsonl(self.daily_manifest_path)
        current = records.get(day.isoformat())
        if not current or current.get("validation_status") != "PASS":
            return None
        raw_path = Path(current.get("raw_file") or "")
        normalized_path = Path(current.get("normalized_file") or "")
        if not raw_path.is_absolute():
            raw_path = self.daily_manifest_path.parent.parent.parent / raw_path
        if not normalized_path.is_absolute():
            normalized_path = self.daily_manifest_path.parent.parent.parent / normalized_path
        try:
            intact = (
                raw_path.is_file()
                and _sha256_file(raw_path) == current.get("sha256")
                and normalized_path.is_file()
                and _sha256_file(normalized_path) == current.get("normalized_sha256")
            )
        except OSError:
            intact = False
        if not intact:
            return None
        if set(current.get("expected_symbol_list", [])) != expected_symbols:
            current = dict(current)
            current["validation_status"] = "FROZEN_UNIVERSE_MISMATCH"
            current["errors"] = ["cached daily response was validated against a different frozen symbol set"]
            return current
        if current.get("adapter_sha256") != _sha256_file(Path(__file__).resolve()):
            current = dict(current)
            current["validation_status"] = "ACQUISITION_ADAPTER_VERSION_MISMATCH"
            current["errors"] = ["cached response was validated by a different adapter version"]
            return current
        if current.get("source") != self.source.source_name or current.get("source_url_template") != self.source.url_template:
            current = dict(current)
            current["validation_status"] = "ALREADY_ACQUIRED_FROM_DIFFERENT_SOURCE"
            current["errors"] = ["a passing date is cached from a different configured source; refusing a silent source switch"]
            return current
        result = dict(current)
        result["cache_hit"] = True
        return result

    def _record(self, record: dict[str, Any]) -> dict[str, Any]:
        records = _read_jsonl(self.daily_manifest_path)
        key = str(record["date"])
        previous = records.get(key)
        history = list(previous.get("attempt_history", [])) if previous else []
        if previous:
            history.append({
                field: previous.get(field)
                for field in ("acquired_at", "raw_file", "sha256", "response_status", "retry_count", "validation_status", "errors")
            })
        if history:
            record["attempt_history"] = history
        records[key] = record
        _write_jsonl(self.daily_manifest_path, records)
        return record

    def acquire(self, market_date: date, expected_symbols: Iterable[str]) -> dict[str, Any]:
        expected = sorted({str(item).strip().upper().removesuffix(".NS") for item in expected_symbols if str(item).strip()})
        if not expected:
            raise ValueError("expected symbol universe is empty")
        source_url = self.source.url_template
        project_root = Path(__file__).resolve().parents[3]
        integration_script = project_root / "scripts" / "run_phase3d_personal_daily.py"
        base: dict[str, Any] = {
            "date": market_date.isoformat(),
            "source": self.source.source_name,
            "adapter_file": str(Path(__file__).resolve()),
            "adapter_sha256": _sha256_file(Path(__file__).resolve()),
            "integration_script": str(integration_script),
            "integration_script_sha256": _sha256_file(integration_script) if integration_script.is_file() else None,
            "official_nifty_file": str(self.official_nifty_path),
            "official_nifty_sha256": _sha256_file(self.official_nifty_path) if self.official_nifty_path.is_file() else None,
            "nse_calendar_file": str(self.trading_calendar_path),
            "nse_calendar_sha256": _sha256_file(self.trading_calendar_path) if self.trading_calendar_path.is_file() else None,
            "source_url": None,
            "source_url_template": source_url,
            "acquired_at": None,
            "source_date": None,
            "raw_file": None,
            "file_size_bytes": None,
            "sha256": None,
            "response_status": None,
            "content_type": None,
            "raw_rows": None,
            "eq_rows": 0,
            "non_eq_rows": 0,
            "symbols": 0,
            "expected_symbols": len(expected),
            "expected_symbol_list": expected,
            "received_symbols": 0,
            "missing_symbols": expected,
            "unexpected_symbols": [],
            "duplicate_count": 0,
            "duplicate_key_count": 0,
            "conflicting_duplicate_count": 0,
            "series_values": [],
            "retry_count": 0,
            "normalized_file": None,
            "normalized_sha256": None,
            "validation_status": "FAIL",
            "errors": [],
        }
        cached = self._cached_success(market_date, set(expected))
        if cached:
            return cached
        if not self.official_nifty_path.is_file():
            base["validation_status"] = "BLOCKED_MISSING_OFFICIAL_NIFTY_CALENDAR"
            base["errors"] = [f"official NIFTY date calendar is missing: {self.official_nifty_path}"]
            return self._record(base)
        try:
            official_dates, latest_official_date = _official_index_dates(self.official_nifty_path)
        except (OSError, ValueError) as error:
            base["validation_status"] = "BLOCKED_INVALID_OFFICIAL_NIFTY_CALENDAR"
            base["errors"] = [str(error)]
            return self._record(base)
        if market_date not in official_dates:
            try:
                decision = NseTradingCalendar(self.trading_calendar_path).classify(
                    market_date, observed_market_data=False
                )
            except (OSError, ValueError) as error:
                base["validation_status"] = "BLOCKED_INVALID_NSE_TRADING_CALENDAR"
                base["errors"] = [str(error)]
                return self._record(base)
            if decision.status == "NON_TRADING_DAY":
                base["validation_status"] = "NON_TRADING_DAY"
                base["errors"] = [decision.reason]
                return self._record(base)
            if latest_official_date is None or market_date > latest_official_date:
                status = f"WAITING_FOR_{market_date.isoformat().replace('-', '_')}_EOD_DATA"
                error_text = (
                    f"official NIFTY file has no observation for {market_date.isoformat()} "
                    f"(latest: {latest_official_date}); NSE calendar: {decision.reason}"
                )
            else:
                status = "NOT_IN_OFFICIAL_NIFTY_CALENDAR"
                error_text = f"{market_date.isoformat()} is absent from the official NIFTY date calendar"
            base["validation_status"] = status
            base["errors"] = [error_text]
            return self._record(base)

        try:
            response = self.source.fetch(market_date, expected)
        except EodAcquisitionError as error:
            base["validation_status"] = error.status
            base["retry_count"] = error.retry_count
            base["errors"] = [str(error)]
            return self._record(base)
        acquired_at = self.now().astimezone(timezone.utc).isoformat()
        content_type = next((str(value) for key, value in response.headers.items() if str(key).casefold() == "content-type"), "")
        extension = _content_extension(content_type, response.content)
        try:
            raw_path = _atomic_raw(self.raw_dir, market_date, response.content, extension)
        except OSError as error:
            base.update({"acquired_at": acquired_at, "response_status": response.status_code,
                         "content_type": content_type, "retry_count": response.retry_count,
                         "validation_status": "RAW_RETENTION_FAILED", "errors": [str(error)]})
            return self._record(base)
        base.update({
            "acquired_at": acquired_at,
            "source_url": response.url,
            "raw_file": str(raw_path),
            "file_size_bytes": raw_path.stat().st_size,
            "sha256": _sha256_file(raw_path),
            "response_status": response.status_code,
            "content_type": content_type,
            "retry_count": response.retry_count,
        })
        if response.status_code in {401, 403, 429} or _is_access_block(response.content):
            base["validation_status"] = "SOURCE_BLOCKED_ACCESS_CONTROL"
            base["errors"] = [f"source returned HTTP {response.status_code}; access controls were respected"]
            return self._record(base)
        if response.status_code != 200:
            base["validation_status"] = "HTTP_ERROR"
            base["errors"] = [f"source returned HTTP {response.status_code}"]
            return self._record(base)
        try:
            rows, source_headers = _extract_records(response.content, content_type)
        except DataSourceNotSuitable as error:
            base["raw_rows"] = _raw_row_count(response.content, content_type)
            base["validation_status"] = "DATA_SOURCE_NOT_SUITABLE"
            base["errors"] = [str(error)]
            return self._record(base)
        base["raw_rows"] = len(rows)
        base["source_headers"] = source_headers
        raw_symbols = {str(item.get("symbol") or "").strip().upper().removesuffix(".NS") for item in rows}
        raw_symbols.discard("")
        eq_symbols = {str(item.get("symbol") or "").strip().upper().removesuffix(".NS")
                      for item in rows if str(item.get("series") or "").strip().upper() == "EQ"}
        eq_symbols.discard("")
        series_values = sorted({str(item.get("series") or "").strip().upper() or "(empty)" for item in rows})
        expected_set = set(expected)
        unknown_symbols = sorted(raw_symbols - expected_set)
        missing_symbols = sorted(expected_set - eq_symbols)
        duplicate_groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
        for item in rows:
            key = (item.get("date", ""), item.get("symbol", ""), item.get("series", ""))
            duplicate_groups.setdefault(key, []).append(item)
        duplicate_keys = [group for group in duplicate_groups.values() if len(group) > 1]
        duplicate_count = sum(len(group) - 1 for group in duplicate_keys)
        conflicting_duplicates = sum(
            len({json.dumps(row, sort_keys=True) for row in group}) > 1 for group in duplicate_keys
        )
        eq_row_count = sum(str(item.get("series") or "").strip().upper() == "EQ" for item in rows)
        base.update({
            "eq_rows": eq_row_count,
            "non_eq_rows": len(rows) - eq_row_count,
            "symbols": len(raw_symbols),
            # Report all distinct explicit EQ symbols returned by the source;
            # keep expected-universe coverage separately in missing/unexpected.
            "received_symbols": len(eq_symbols),
            "missing_symbols": missing_symbols,
            "unexpected_symbols": unknown_symbols,
            "series_values": series_values,
            "duplicate_count": duplicate_count,
            "duplicate_key_count": len(duplicate_keys),
            "conflicting_duplicate_count": conflicting_duplicates,
        })
        date_values = {str(item.get("date") or "").strip() for item in rows}
        date_values.discard("")
        valid_dates: set[str] = set()
        for value in date_values:
            try:
                parsed = date.fromisoformat(value)
                if parsed.isoformat() == value:
                    valid_dates.add(value)
            except ValueError:
                continue
        if len(date_values) == 1:
            base["source_date"] = next(iter(date_values))
        if not rows:
            base["validation_status"] = "DATA_VALIDATION_FAILED"
            base["errors"] = ["source response contains no EOD rows"]
            return self._record(base)
        if valid_dates and valid_dates != {market_date.isoformat()}:
            base["validation_status"] = "SOURCE_DATE_MISMATCH"
            base["errors"] = [f"requested {market_date.isoformat()}, source returned date(s) {sorted(valid_dates)}"]
            return self._record(base)
        if duplicate_keys:
            base["validation_status"] = (
                "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA" if missing_symbols else "DATA_VALIDATION_FAILED"
            )
            base["errors"] = [f"{len(duplicate_keys)} duplicate (date, symbol, series) key(s); no rows were deduplicated"]
            if missing_symbols:
                base["errors"].append(f"missing explicit EQ rows for required symbols: {missing_symbols[:20]}")
            return self._record(base)
        if unknown_symbols:
            base["validation_status"] = (
                "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA" if missing_symbols else "DATA_VALIDATION_FAILED"
            )
            base["errors"] = [f"unexpected symbols: {unknown_symbols[:20]}"]
            if missing_symbols:
                base["errors"].append(f"missing explicit EQ rows for required symbols: {missing_symbols[:20]}")
            return self._record(base)

        normalized_path = self.normalized_dir / f"{market_date.isoformat()}.csv"
        if normalized_path.exists():
            base["validation_status"] = "NORMALIZED_DATE_ALREADY_EXISTS_UNVERIFIED"
            base["errors"] = [
                f"normalized date already exists but has no matching passing acquisition cache: {normalized_path}"
            ]
            return self._record(base)

        try:
            staging_root = self.raw_dir / ".normalization_staging"
            staging_root.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix=f"{market_date.isoformat()}.", dir=staging_root) as temporary_dir:
                source_path = Path(temporary_dir) / f"{market_date.isoformat()}.csv"
                with source_path.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=DAILY_COLUMNS, lineterminator="\n")
                    writer.writeheader()
                    writer.writerows(item for item in rows if item["series"] == "EQ")
                    stream.flush()
                    os.fsync(stream.fileno())
                imported = import_daily_eod(
                    source_path=source_path,
                    expected_symbols=expected,
                    normalized_dir=self.normalized_dir,
                    manifest_dir=self.intake_manifest_dir,
                    source_provider=self.source.source_name,
                )
        except Exception as error:
            base["validation_status"] = "DATA_VALIDATION_FAILED"
            base["errors"] = [f"normalization failed: {type(error).__name__}: {error}"]
            return self._record(base)
        received_count = int(imported.get("symbol_count", 0))
        base.update({
            "eq_rows": eq_row_count,
            "symbols": len(raw_symbols),
            "received_symbols": received_count,
            "missing_symbols": imported.get("missing_symbols", []),
            "unexpected_symbols": unknown_symbols,
            "intake_manifest": str(self.intake_manifest_dir / f"{market_date.isoformat()}.json"),
        })
        if imported.get("validation_status") == "PASS":
            base.update({
                "validation_status": "PASS",
                "normalized_file": str(normalized_path),
                "normalized_sha256": imported.get("normalized_sha256"),
                "missing_symbols": [],
                "errors": [],
            })
        elif imported.get("missing_symbols"):
            base["validation_status"] = "FORWARD_RUN_BLOCKED_MISSING_EQUITY_DATA"
            base["errors"] = list(imported.get("errors", []))
        else:
            base["validation_status"] = "DATA_VALIDATION_FAILED"
            base["errors"] = list(imported.get("errors", []))
        return self._record(base)
