"""Read-only Upstox Market Data Feed V3 adapter and instrument-map helpers.

This module is intentionally limited to data acquisition. It does not import
strategy, risk, paper-ledger, or order-execution code. The live socket is only
started by an explicit caller after its own mode and safety gates pass.
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import json
import math
import os
import statistics
import threading
import time
import uuid
from collections import Counter, deque
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from urllib.parse import urlparse

import requests
from google.protobuf.message import DecodeError

from nse_research.config import RealtimeSettings
from nse_research.forward.calendar import NseTradingCalendar
from nse_research.realtime.proto import MarketDataFeedV3_pb2 as upstox_pb
from nse_research.realtime.providers import NormalizedQuote, QuoteValidity, RealtimeDataProvider
from nse_research.realtime.volume import CumulativeVolumeDeltaTracker


PROVIDER_NAME = "upstox"
SOURCE_NAME = "upstox_v3"
AUTH_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
SEARCH_URL = "https://api.upstox.com/v2/instruments/search"
BOD_NSE_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
EXPECTED_SYMBOL_COUNT = 486
SUPPORTED_REALTIME_UNIVERSE_SIZES = (100, 200, 486)
DEFAULT_REALTIME_UNIVERSE_SIZE = 100
SMOKE_SYMBOLS = ("RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN")
REALTIME_UNIVERSE_COLUMNS = ("rank", "symbol", "isin", "exchange", "series", "selection_method")
SELECTION_METHOD = "REQUIRED_SMOKE_FIRST_THEN_FROZEN_FILE_ORDER"
MAPPING_COLUMNS = (
    "project_symbol", "project_isin", "project_exchange", "project_series",
    "upstox_instrument_key", "upstox_exchange", "upstox_segment",
    "upstox_instrument_type", "upstox_isin", "upstox_trading_symbol",
    "upstox_exchange_token", "mapping_method", "mapping_status", "mapping_reason",
)
MAPPING_PATH = Path(__file__).resolve().parents[3] / "results/phase4a8_upstox_instrument_mapping.csv"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
_TRUE = {"1", "true", "yes", "on"}


class UpstoxConfigurationError(RuntimeError):
    """Sanitized configuration/API error that cannot include an access token."""


def env_true(value: str | None) -> bool:
    return str(value or "").strip().lower() in _TRUE


def parse_epoch_ms(raw: Any) -> datetime:
    """Parse the specified millisecond epoch contract without unit guessing.

    The public example is a 13-digit millisecond value; the docs label LTT as a
    timestamp but do not spell out the unit in prose. We enforce the requested
    epoch_ms contract strictly, with no seconds/microseconds fallback.
    """
    if isinstance(raw, bool):
        raise ValueError("timestamp must be integer epoch milliseconds")
    if isinstance(raw, int):
        value = raw
    elif isinstance(raw, str) and raw.isdigit():
        value = int(raw)
    else:
        raise ValueError("timestamp must be integer epoch milliseconds")
    if not 1_000_000_000_000 <= value < 10_000_000_000_000:
        raise ValueError("timestamp violates the 13-digit epoch_ms contract")
    try:
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError("timestamp is outside the supported epoch_ms range") from error


def read_project_universe(path: str | Path) -> list[dict[str, str]]:
    """Read the frozen Phase 3D universe without inferring its series."""
    source = Path(path)
    with source.open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        required = {"Symbol", "Series", "ISIN Code"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError("frozen universe must contain Symbol, Series, and ISIN Code")
        rows: list[dict[str, str]] = []
        for row in reader:
            rows.append({
                "symbol": str(row.get("Symbol") or "").strip().upper(),
                "isin": str(row.get("ISIN Code") or "").strip().upper(),
                "exchange": "NSE",
                "series": str(row.get("Series") or "").strip().upper(),
            })
    if any(not row["symbol"] or not row["isin"] for row in rows):
        raise ValueError("frozen universe contains a missing symbol or ISIN")
    return rows


def mapping_artifact_path(size: int, *, project_root: str | Path = PROJECT_ROOT) -> Path:
    if size not in SUPPORTED_REALTIME_UNIVERSE_SIZES:
        raise ValueError(f"REALTIME_UNIVERSE_SIZE must be one of {SUPPORTED_REALTIME_UNIVERSE_SIZES}")
    root = Path(project_root)
    return root / f"results/phase4a8a_upstox_instrument_mapping_{size}.csv"


def realtime_universe_artifact_path(size: int, *, project_root: str | Path = PROJECT_ROOT) -> Path:
    if size not in SUPPORTED_REALTIME_UNIVERSE_SIZES:
        raise ValueError(f"REALTIME_UNIVERSE_SIZE must be one of {SUPPORTED_REALTIME_UNIVERSE_SIZES}")
    return Path(project_root) / f"results/phase4a8a_upstox_live_universe_{size}.csv"


def select_realtime_universe(
    universe: Sequence[Mapping[str, str]], size: int = DEFAULT_REALTIME_UNIVERSE_SIZE,
) -> list[dict[str, str]]:
    """Select a stable realtime subset without prices or random sampling.

    The current project roster has no NIFTY 100 membership field or separate
    membership file. The five required smoke symbols are ranked first in the
    brief's fixed order; all remaining rows retain the frozen roster file
    order. Only explicit NSE/EQ rows with unique symbols and ISINs qualify.
    """
    if size not in SUPPORTED_REALTIME_UNIVERSE_SIZES:
        raise ValueError(f"REALTIME_UNIVERSE_SIZE must be one of {SUPPORTED_REALTIME_UNIVERSE_SIZES}")
    valid = [
        {
            "symbol": str(row.get("symbol") or "").strip().upper(),
            "isin": str(row.get("isin") or "").strip().upper(),
            "exchange": str(row.get("exchange") or "").strip().upper(),
            "series": str(row.get("series") or "").strip().upper(),
        }
        for row in universe
        if str(row.get("exchange") or "").strip().upper() == "NSE"
        and str(row.get("series") or "").strip().upper() == "EQ"
        and str(row.get("symbol") or "").strip()
        and str(row.get("isin") or "").strip()
    ]
    symbols = [row["symbol"] for row in valid]
    isins = [row["isin"] for row in valid]
    if len(set(symbols)) != len(symbols) or len(set(isins)) != len(isins):
        raise ValueError("frozen universe contains duplicate symbols or ISINs")
    if len(valid) < size:
        raise ValueError(f"frozen universe has only {len(valid)} valid NSE EQ rows; {size} are required")
    by_symbol = {row["symbol"]: row for row in valid}
    missing_smoke = [symbol for symbol in SMOKE_SYMBOLS if symbol not in by_symbol]
    if missing_smoke:
        raise ValueError(f"frozen universe is missing required smoke symbols: {','.join(missing_smoke)}")
    ordered = [by_symbol[symbol] for symbol in SMOKE_SYMBOLS]
    ordered.extend(row for row in valid if row["symbol"] not in set(SMOKE_SYMBOLS))
    selected = ordered[:size]
    if not set(SMOKE_SYMBOLS).issubset({row["symbol"] for row in selected}):
        raise ValueError("selected realtime universe does not include all five smoke symbols")
    return selected


def write_realtime_universe_csv(rows: Sequence[Mapping[str, str]], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=REALTIME_UNIVERSE_COLUMNS)
        writer.writeheader()
        for rank, row in enumerate(rows, start=1):
            writer.writerow({
                "rank": rank,
                "symbol": row.get("symbol", ""),
                "isin": row.get("isin", ""),
                "exchange": row.get("exchange", ""),
                "series": row.get("series", ""),
                "selection_method": row.get("selection_method", SELECTION_METHOD),
            })
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    return target


def read_realtime_universe_csv(path: str | Path, *, expected_count: int) -> list[dict[str, str]]:
    with Path(path).open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != REALTIME_UNIVERSE_COLUMNS:
            raise ValueError("realtime universe CSV schema is invalid")
        rows = list(reader)
    if len(rows) != expected_count:
        raise ValueError(f"realtime universe requires {expected_count} rows; found {len(rows)}")
    symbols: set[str] = set()
    isins: set[str] = set()
    for rank, row in enumerate(rows, start=1):
        symbol = row["symbol"].strip().upper()
        isin = row["isin"].strip().upper()
        if (row["rank"].strip() != str(rank) or not symbol or not isin
                or row["exchange"].strip().upper() != "NSE"
                or row["series"].strip().upper() != "EQ"
                or row["selection_method"].strip() != SELECTION_METHOD
                or symbol in symbols or isin in isins):
            raise ValueError("realtime universe contains invalid identity, rank, or duplicates")
        symbols.add(symbol)
        isins.add(isin)
    if not set(SMOKE_SYMBOLS).issubset(symbols):
        raise ValueError("realtime universe is missing one or more required smoke symbols")
    return rows


def read_instrument_master(path: str | Path) -> list[dict[str, Any]]:
    """Read an official Upstox BOD JSON or JSON.GZ document from disk."""
    source = Path(path)
    opener = gzip.open if source.suffix.lower() == ".gz" else open
    with opener(source, "rt", encoding="utf-8") as stream:
        payload = json.load(stream)
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get("data"), list):
        records = payload["data"]
    else:
        raise ValueError("instrument master must be a JSON list or contain a data list")
    if any(not isinstance(row, dict) for row in records):
        raise ValueError("instrument master contains a non-object row")
    return records


def download_official_bod(
    path: str | Path,
    *,
    session: Any = requests,
    timeout_seconds: float = 15.0,
) -> list[dict[str, Any]]:
    """Fetch and validate the official Upstox NSE BOD JSON.GZ file once."""
    if timeout_seconds <= 0:
        raise ValueError("BOD download timeout must be positive")
    try:
        response = session.get(BOD_NSE_URL, timeout=timeout_seconds)
    except Exception as error:
        raise UpstoxConfigurationError(f"official BOD download failed: {type(error).__name__}") from None
    if int(getattr(response, "status_code", 0)) != 200:
        raise UpstoxConfigurationError(f"official BOD download returned HTTP {getattr(response, 'status_code', 'unknown')}")
    body = getattr(response, "content", b"")
    if not isinstance(body, bytes) or body[:2] != b"\x1f\x8b":
        raise UpstoxConfigurationError("official BOD response was not gzip data")
    try:
        decoded = gzip.decompress(body)
        payload = json.loads(decoded.decode("utf-8"))
    except Exception:
        raise UpstoxConfigurationError("official BOD response was malformed gzip JSON") from None
    if not isinstance(payload, list) or not payload or any(not isinstance(item, dict) for item in payload):
        raise UpstoxConfigurationError("official BOD response had an unexpected instrument structure")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with temporary.open("wb") as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    return payload


def _blank_mapping(project: Mapping[str, str], method: str, status: str, reason: str) -> dict[str, str]:
    row = {
        "project_symbol": project.get("symbol", ""),
        "project_isin": project.get("isin", ""),
        "project_exchange": project.get("exchange", "NSE"),
        "project_series": project.get("series", ""),
        "upstox_instrument_key": "", "upstox_exchange": "", "upstox_segment": "",
        "upstox_instrument_type": "", "upstox_isin": "", "upstox_trading_symbol": "",
        "upstox_exchange_token": "", "mapping_method": method,
        "mapping_status": status, "mapping_reason": reason,
    }
    return row


def build_mapping_rows(
    universe: Sequence[Mapping[str, str]],
    instruments: Sequence[Mapping[str, Any]],
    *,
    method: str,
    require_trading_symbol: bool = True,
) -> list[dict[str, str]]:
    """Match by ISIN and require explicit NSE/NSE_EQ/EQ identity.

    The historical BOD path keeps its original strict symbol requirement. The
    ISIN Search fallback may omit ``trading_symbol``; if it supplies one, it
    must match the project symbol exactly.
    """
    if method not in {"UPSTOX_BOD_JSON", "INSTRUMENT_SEARCH_ISIN"}:
        raise ValueError("unsupported mapping method")
    by_isin: dict[str, list[Mapping[str, Any]]] = {}
    for item in instruments:
        isin = str(item.get("isin") or "").strip().upper()
        if isin:
            by_isin.setdefault(isin, []).append(item)

    input_symbol_counts = Counter(row.get("symbol", "") for row in universe)
    input_isin_counts = Counter(row.get("isin", "") for row in universe)
    results: list[dict[str, str]] = []
    for project in universe:
        symbol = project.get("symbol", "").strip().upper()
        isin = project.get("isin", "").strip().upper()
        candidates = by_isin.get(isin, [])
        strict = [candidate for candidate in candidates if (
            str(candidate.get("exchange") or "").strip().upper() == "NSE"
            and str(candidate.get("segment") or "").strip().upper() == "NSE_EQ"
            and str(candidate.get("instrument_type") or "").strip().upper() == "EQ"
            and (not str(candidate.get("series") or "").strip()
                 or str(candidate.get("series") or "").strip().upper() == "EQ")
            and str(candidate.get("isin") or "").strip().upper() == isin
        )]
        if input_symbol_counts[symbol] > 1 or input_isin_counts[isin] > 1:
            results.append(_blank_mapping(project, method, "DUPLICATE", "duplicate symbol or ISIN in project universe"))
            continue
        if not candidates:
            if method == "INSTRUMENT_SEARCH_ISIN" and instruments:
                results.append(_blank_mapping(
                    project, method, "INVALID", "Search returned candidates but none matched the exact project ISIN",
                ))
                continue
            results.append(_blank_mapping(project, method, "MISSING", "no Upstox instrument matched the project ISIN"))
            continue
        if len(strict) > 1:
            results.append(_blank_mapping(project, method, "DUPLICATE", "multiple exact NSE_EQ/EQ records matched the project ISIN"))
            continue
        if not strict:
            results.append(_blank_mapping(project, method, "INVALID", "ISIN matched records but none explicitly identified NSE/NSE_EQ/EQ"))
            continue
        candidate = strict[0]
        up_symbol = str(candidate.get("trading_symbol") or "").strip().upper()
        key = str(candidate.get("instrument_key") or "").strip()
        symbol_invalid = (require_trading_symbol and not up_symbol) or (up_symbol and up_symbol != symbol)
        key_value = key.partition("|")[2].strip() if "|" in key else ""
        if symbol_invalid or not key.startswith("NSE_EQ|") or not key_value:
            results.append(_blank_mapping(project, method, "INVALID", "trading_symbol or instrument_key does not exactly verify the project NSE EQ identity"))
            continue
        reason = "exact ISIN, NSE, NSE_EQ, and EQ match"
        if up_symbol:
            reason += "; trading_symbol matches project symbol"
        else:
            reason += "; Upstox response omitted trading_symbol"
        row = _blank_mapping(project, method, "VERIFIED", reason)
        row.update({
            "upstox_instrument_key": key,
            "upstox_exchange": str(candidate.get("exchange") or "").strip().upper(),
            "upstox_segment": str(candidate.get("segment") or "").strip().upper(),
            "upstox_instrument_type": str(candidate.get("instrument_type") or "").strip().upper(),
            "upstox_isin": str(candidate.get("isin") or "").strip().upper(),
            "upstox_trading_symbol": up_symbol,
            "upstox_exchange_token": str(candidate.get("exchange_token") or "").strip(),
        })
        results.append(row)

    keys: dict[str, list[int]] = {}
    for index, row in enumerate(results):
        if row["mapping_status"] == "VERIFIED":
            keys.setdefault(row["upstox_instrument_key"], []).append(index)
    for key, indices in keys.items():
        if len(indices) > 1:
            for index in indices:
                results[index]["mapping_status"] = "DUPLICATE"
                results[index]["mapping_reason"] = f"instrument key is assigned to multiple project rows: {key}"
    return results


def unverified_mapping_rows(
    universe: Sequence[Mapping[str, str]], reason: str, *, method: str = "UPSTOX_BOD_JSON",
) -> list[dict[str, str]]:
    return [_blank_mapping(item, method, "UNVERIFIED", reason) for item in universe]


def write_mapping_csv(rows: Sequence[Mapping[str, Any]], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=MAPPING_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in MAPPING_COLUMNS})
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    return target


def load_verified_mapping(
    path: str | Path,
    *,
    expected_count: int = EXPECTED_SYMBOL_COUNT,
    allow_missing_trading_symbol: bool = False,
) -> dict[str, dict[str, str]]:
    """Load only a complete, internally unique, explicitly verified map."""
    with Path(path).open("r", newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != MAPPING_COLUMNS:
            raise ValueError("Upstox mapping CSV schema does not match Phase 4A.8")
        rows = list(reader)
    if len(rows) != expected_count:
        raise ValueError(f"Upstox mapping requires {expected_count} rows; found {len(rows)}")
    symbols: dict[str, dict[str, str]] = {}
    keys: set[str] = set()
    for row in rows:
        symbol = row["project_symbol"].strip().upper()
        if row["mapping_status"] != "VERIFIED":
            raise ValueError(f"Upstox mapping is not complete: {symbol} is {row['mapping_status']}")
        if (row["project_exchange"].upper() != "NSE" or row["project_series"].upper() != "EQ"
                or row["upstox_exchange"].upper() != "NSE" or row["upstox_segment"].upper() != "NSE_EQ"
                or row["upstox_instrument_type"].upper() != "EQ"
                or row["project_isin"].upper() != row["upstox_isin"].upper()
                or (not allow_missing_trading_symbol and not row["upstox_trading_symbol"].strip())
                or (row["upstox_trading_symbol"].strip()
                    and symbol != row["upstox_trading_symbol"].upper())
                or not row["upstox_instrument_key"].startswith("NSE_EQ|")
                or not row["upstox_instrument_key"].partition("|")[2].strip()):
            raise ValueError(f"Upstox mapping identity validation failed for {symbol}")
        key = row["upstox_instrument_key"]
        if not symbol or symbol in symbols or key in keys:
            raise ValueError("Upstox mapping contains duplicate project symbols or instrument keys")
        symbols[symbol] = row
        keys.add(key)
    return symbols


def _retry_after_seconds(response: Any, *, maximum: float) -> float | None:
    headers = getattr(response, "headers", {}) or {}
    raw = headers.get("Retry-After") if hasattr(headers, "get") else None
    try:
        value = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return min(value, maximum)


def search_instrument_by_isin(
    isin: str,
    token: str,
    *,
    session: Any = requests,
    timeout_seconds: float = 10.0,
    max_retries: int = 3,
    retry_base_seconds: float = 0.25,
    retry_max_seconds: float = 8.0,
    sleeper: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Issue a bounded, read-only ISIN search; retry only transient failures."""
    if not token:
        raise UpstoxConfigurationError("UPSTOX_ACCESS_TOKEN is not configured")
    if timeout_seconds <= 0 or retry_base_seconds < 0 or retry_max_seconds < retry_base_seconds:
        raise ValueError("instrument search timing configuration is invalid")
    if max_retries < 0 or max_retries > 5:
        raise ValueError("instrument search retries must be between zero and five")
    params = {"query": isin, "exchanges": "NSE", "segments": "EQ", "page_number": 1, "records": 30}
    transient_request_errors = (
        requests.Timeout, requests.ConnectionError, TimeoutError, ConnectionError,
    )
    for attempt in range(max_retries + 1):
        retry_after: float | None = None
        try:
            response = session.get(
                SEARCH_URL, params=params,
                headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                timeout=timeout_seconds,
            )
        except transient_request_errors as error:
            if attempt >= max_retries:
                raise UpstoxConfigurationError(
                    f"instrument search exhausted bounded retries: {type(error).__name__}"
                ) from None
            delay = min(retry_base_seconds * (2 ** attempt), retry_max_seconds)
            sleeper(delay)
            continue
        except Exception as error:
            raise UpstoxConfigurationError(
                f"instrument search request failed: {type(error).__name__}"
            ) from None

        status = int(getattr(response, "status_code", 0))
        if status != 200:
            retryable = status in {408, 425, 429} or 500 <= status <= 599
            retry_after = _retry_after_seconds(response, maximum=retry_max_seconds) if status == 429 else None
            if retryable and attempt < max_retries:
                delay = retry_after if retry_after is not None else min(retry_base_seconds * (2 ** attempt), retry_max_seconds)
                sleeper(delay)
                continue
            suffix = " after bounded retries" if retryable else ""
            raise UpstoxConfigurationError(f"instrument search returned HTTP {status}{suffix}")
        try:
            payload = response.json()
        except Exception:
            raise UpstoxConfigurationError("instrument search returned malformed JSON") from None
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, list):
            if any(not isinstance(item, dict) for item in data):
                raise UpstoxConfigurationError("instrument search returned a malformed data row")
            return data
        if isinstance(data, dict) and isinstance(data.get("instruments"), list):
            if any(not isinstance(item, dict) for item in data["instruments"]):
                raise UpstoxConfigurationError("instrument search returned a malformed instrument row")
            return data["instruments"]
        raise UpstoxConfigurationError("instrument search returned an unexpected response structure")
    raise UpstoxConfigurationError("instrument search exhausted bounded retries")


def search_universe_with_rate_limit(
    universe: Sequence[Mapping[str, str]],
    token: str,
    *,
    session: Any = requests,
    interval_seconds: float = 0.25,
    timeout_seconds: float = 10.0,
    max_retries: int = 3,
    sleeper: Callable[[float], None] = time.sleep,
) -> list[dict[str, str]]:
    """Controlled serial fallback; preserve partial results and stop on API errors."""
    if interval_seconds < 0 or timeout_seconds <= 0 or max_retries < 0 or max_retries > 5:
        raise ValueError("search timing configuration is invalid")
    results: list[dict[str, str]] = []
    for index, security in enumerate(universe):
        if index:
            sleeper(interval_seconds)
        try:
            records = search_instrument_by_isin(
                security["isin"], token, session=session, timeout_seconds=timeout_seconds,
                max_retries=max_retries, retry_base_seconds=max(interval_seconds, 0.25),
                sleeper=sleeper,
            )
        except UpstoxConfigurationError as error:
            reason = f"ISIN Search stopped safely: {error}"
            results.extend(unverified_mapping_rows(
                universe[index:], reason, method="INSTRUMENT_SEARCH_ISIN",
            ))
            break
        results.extend(build_mapping_rows(
            [security], records, method="INSTRUMENT_SEARCH_ISIN", require_trading_symbol=False,
        ))

    key_indices: dict[str, list[int]] = {}
    for row_index, row in enumerate(results):
        if row["mapping_status"] == "VERIFIED":
            key_indices.setdefault(row["upstox_instrument_key"], []).append(row_index)
    for key, row_indices in key_indices.items():
        if len(row_indices) > 1:
            for row_index in row_indices:
                results[row_index]["mapping_status"] = "DUPLICATE"
                results[row_index]["mapping_reason"] = "instrument key is assigned to multiple project rows"
    return results


def _p95(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return float(ordered[index])


class UpstoxV3Provider(RealtimeDataProvider):
    """Bounded-reconnect Upstox LTPC stream implementing the existing provider."""

    provider_name = PROVIDER_NAME

    def __init__(
        self,
        mapping: Mapping[str, Mapping[str, str]],
        *,
        access_token: str,
        max_quote_age_seconds: float = 5.0,
        max_future_skew_seconds: float = 1.0,
        max_reconnect_attempts: int = 5,
        reconnect_base_seconds: float = 0.5,
        reconnect_max_seconds: float = 8.0,
        mode: str = "ltpc",
        authorize_get: Callable[..., Any] | None = None,
        websocket_connect: Callable[..., Any] | None = None,
        utcnow: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleeper: Callable[[float], Any] = asyncio.sleep,
        calendar: NseTradingCalendar | None = None,
        session_timezone: str = "Asia/Kolkata",
        max_pending_quotes: int = 50000,
    ) -> None:
        if not access_token:
            raise UpstoxConfigurationError("UPSTOX_ACCESS_TOKEN is not configured")
        if mode != "ltpc":
            raise UpstoxConfigurationError("Phase 4A.8 supports only LTPC subscription mode")
        if max_quote_age_seconds <= 0 or max_future_skew_seconds < 0:
            raise ValueError("quote freshness limits are invalid")
        if max_reconnect_attempts < 0 or max_reconnect_attempts > 5:
            raise ValueError("max reconnect attempts must be between zero and five")
        if reconnect_base_seconds <= 0 or reconnect_max_seconds < reconnect_base_seconds:
            raise ValueError("reconnect backoff limits are invalid")
        if max_pending_quotes < 1:
            raise ValueError("max_pending_quotes must be positive")
        self.mapping = {str(symbol).upper(): dict(row) for symbol, row in mapping.items()}
        self.key_to_symbol = {
            row["upstox_instrument_key"]: symbol for symbol, row in self.mapping.items()
        }
        self._access_token = access_token
        self.max_quote_age_seconds = float(max_quote_age_seconds)
        self.max_future_skew_seconds = float(max_future_skew_seconds)
        self.max_reconnect_attempts = max_reconnect_attempts
        self.reconnect_base_seconds = reconnect_base_seconds
        self.reconnect_max_seconds = reconnect_max_seconds
        self.mode = mode
        self._authorize_get = authorize_get or requests.get
        self._websocket_connect = websocket_connect
        self._utcnow = utcnow
        self._async_sleeper = sleeper
        self.calendar = calendar or NseTradingCalendar()
        self.session_timezone = session_timezone
        self._max_pending_quotes = max_pending_quotes
        self._pending: deque[NormalizedQuote] = deque()
        self._lock = threading.RLock()
        self._ws: Any = None
        self._stop_event: asyncio.Event | None = None
        self._subscription_lock: asyncio.Lock | None = None
        self._subscribed: set[str] = set()
        self._requested: set[str] = set()
        self._seen_tick_ids: set[str] = set()
        self._seen_order: deque[str] = deque()
        self._volume = CumulativeVolumeDeltaTracker()
        self._gap_pending: set[str] = set()
        self._connected = False
        self._connection_status = "DISCONNECTED"
        self._connection_error: str | None = None
        self._reconnect_count = 0
        self._attempt_count = 0
        self._invalid_count = 0
        self._stale_count = 0
        self._duplicate_count = 0
        self._volume_anomaly_count = 0
        self._volume_anomaly_events: deque[dict[str, str]] = deque(maxlen=100)
        self._queue_drop_count = 0
        self._message_count = 0
        self._current_ts_raw: int | None = None
        self._current_ts_utc: datetime | None = None
        self._market_status: dict[str, str] = {}
        self._market_status_updated_at: datetime | None = None
        self._per_symbol: dict[str, dict[str, Any]] = {
            symbol: {
                "message_count": 0, "tick_count": 0, "invalid_count": 0,
                "stale_count": 0, "duplicate_count": 0,
                "first_tick_timestamp": None, "last_tick_timestamp": None,
                "last_ltt_raw": None, "last_received_timestamp": None,
                "last_price": None, "normalized_quote_status": None,
                "snapshot_received": False, "live_update_received": False,
                "latencies_ms": [], "subscription_status": "NOT_SUBSCRIBED",
            }
            for symbol in self.mapping
        }

    @staticmethod
    def reconnect_delay(failed_attempt: int, base_seconds: float = 0.5, maximum_seconds: float = 8.0) -> float:
        """Exponential reconnect delay for attempts after the first connection."""
        if failed_attempt < 1:
            raise ValueError("failed_attempt starts at one")
        return min(base_seconds * (2 ** (failed_attempt - 1)), maximum_seconds)

    def get_quote(self, symbol: str, exchange: str = "NSE") -> NormalizedQuote:
        for quote in reversed(self.get_quotes([symbol])):
            if quote.symbol == symbol.strip().upper():
                return quote
        now = self._utcnow()
        return NormalizedQuote(
            symbol=symbol.strip().upper(), exchange=exchange, last_price=None,
            change=None, change_percent=None, volume=None, market_cap=None,
            timestamp=None, source=SOURCE_NAME, received_at=now,
            data_age_seconds=None, validity_status=QuoteValidity.NO_DATA,
            reason="no unconsumed Upstox quote is available",
        )

    def get_quotes(self, symbols: list[str]) -> list[NormalizedQuote]:
        requested = {symbol.strip().upper() for symbol in symbols}
        with self._lock:
            selected: list[NormalizedQuote] = []
            retained: deque[NormalizedQuote] = deque()
            while self._pending:
                quote = self._pending.popleft()
                if quote.symbol in requested:
                    selected.append(quote)
                else:
                    retained.append(quote)
            self._pending = retained
            return selected

    def health(self) -> dict[str, Any]:
        return self.health_snapshot()

    def health_snapshot(self) -> dict[str, Any]:
        with self._lock:
            latencies = [value for row in self._per_symbol.values() for value in row["latencies_ms"]]
            ordered = sorted(latencies)
            return {
                "provider": PROVIDER_NAME,
                "mode": self.mode.upper(),
                "universe_size": len(self.mapping),
                "connection_status": self._connection_status,
                "requested_symbols": len(self._requested),
                "requested_symbol_list": sorted(self._requested),
                "mapped_symbols": len(self.mapping),
                "subscribed_symbols": len(self._subscribed),
                "subscribed_symbol_list": sorted(self._subscribed),
                "receiving_symbols": sum(1 for row in self._per_symbol.values() if row["tick_count"]),
                "quote_count": sum(row["tick_count"] for row in self._per_symbol.values()),
                "invalid_count": self._invalid_count,
                "stale_count": self._stale_count,
                "duplicate_count": self._duplicate_count,
                "last_tick_timestamp": max(
                    (row["last_tick_timestamp"] for row in self._per_symbol.values()
                     if row["last_tick_timestamp"]),
                    default=None,
                ),
                "timestamp_semantics": "CONFIGURED_EPOCH_MS_NOT_EMPIRICALLY_VALIDATED",
                "volume_anomaly_count": self._volume_anomaly_count,
                "volume_anomaly_events": list(self._volume_anomaly_events),
                "queue_drop_count": self._queue_drop_count,
                "reconnect_count": self._reconnect_count,
                "connection_attempts": self._attempt_count,
                "market_segment": "NSE_EQ",
                "market_segment_status": self._market_status.get("NSE_EQ"),
                "market_status_by_segment": dict(self._market_status),
                "market_current_timestamp_raw": self._current_ts_raw,
                "market_current_timestamp_utc": _iso(self._current_ts_utc),
                "market_status_updated_at": _iso(self._market_status_updated_at),
                "last_provider_error_type": self._connection_error,
                "latency_sample_count": len(latencies),
                "latency_min_ms": min(latencies) if latencies else None,
                "latency_mean_ms": statistics.fmean(latencies) if latencies else None,
                "latency_median_ms": statistics.median(latencies) if latencies else None,
                "latency_p50_ms": _percentile(ordered, 0.50),
                "latency_p95_ms": _percentile(ordered, 0.95),
                "latency_p99_ms": _percentile(ordered, 0.99),
                "latency_max_ms": max(latencies) if latencies else None,
                "symbol_stats": {
                    symbol: {
                        key: value for key, value in row.items() if key != "latencies_ms"
                    } | _symbol_latency_stats(row["latencies_ms"])
                    for symbol, row in self._per_symbol.items()
                },
            }

    async def subscribe(self, symbols: Iterable[str]) -> None:
        """Subscribe only mapped NSE_EQ keys and only once per socket session."""
        normalized = {str(symbol).strip().upper() for symbol in symbols}
        if not normalized or not normalized.issubset(self.mapping):
            raise UpstoxConfigurationError("subscription contains an unmapped project symbol")
        assert self._subscription_lock is not None
        async with self._subscription_lock:
            self._requested.update(normalized)
            new_symbols = normalized - self._subscribed
            if not new_symbols:
                return
            self._subscribed.update(new_symbols)
            with self._lock:
                for symbol in new_symbols:
                    self._per_symbol[symbol]["subscription_status"] = "REQUESTED"
            if self._ws is not None:
                await self._send_subscription(new_symbols)

    async def stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        ws = self._ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    async def request_validation_reconnect(self) -> None:
        """Close an active data socket once for the controlled read-only test.

        The run loop performs its existing bounded reconnect and resubscription.
        Callers must invoke this only after validated live data is stable.
        """
        if not self._connected or self._ws is None:
            raise UpstoxConfigurationError("controlled reconnect requires an active market-data socket")
        await self._ws.close()

    async def run(self, symbols: Iterable[str]) -> None:
        """Connect, authenticate, subscribe and reconnect a bounded number of times."""
        if self._stop_event is not None:
            raise RuntimeError("provider run loop is already active")
        self._stop_event = asyncio.Event()
        self._subscription_lock = asyncio.Lock()
        await self.subscribe(symbols)
        reconnects = 0
        for attempt in range(self.max_reconnect_attempts + 1):
            if self._stop_event.is_set():
                break
            self._attempt_count += 1
            if attempt:
                reconnects += 1
                self._reconnect_count += 1
                self._gap_pending.update(self._requested)
                self._connection_status = "RECONNECTING"
            try:
                redirect_uri = self._authorized_url()
                connector = self._websocket_connect
                if connector is None:
                    import websockets
                    connector = websockets.connect
                async with connector(
                    redirect_uri,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    open_timeout=10,
                    max_size=2 * 1024 * 1024,
                ) as websocket:
                    self._ws = websocket
                    self._connected = True
                    self._connection_status = "CONNECTED"
                    self._connection_error = None
                    await self._send_subscription(self._subscribed)
                    while not self._stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        self.process_message(raw, received_at=self._utcnow())
                    return
            except asyncio.CancelledError:
                self._connection_status = "DISCONNECTED"
                raise
            except Exception as error:
                self._connection_error = type(error).__name__
                self._connection_status = "DISCONNECTED"
                if self._stop_event.is_set() or attempt >= self.max_reconnect_attempts:
                    break
                delay = self.reconnect_delay(attempt + 1, self.reconnect_base_seconds, self.reconnect_max_seconds)
                await self._async_sleeper(delay)
            finally:
                self._ws = None
                self._connected = False
        self._connection_status = "DISCONNECTED"

    def _authorized_url(self) -> str:
        try:
            response = self._authorize_get(
                AUTH_URL,
                headers={"Accept": "application/json", "Authorization": f"Bearer {self._access_token}"},
                timeout=10,
            )
        except Exception as error:
            raise UpstoxConfigurationError(f"market-data authorization failed: {type(error).__name__}") from None
        if int(getattr(response, "status_code", 0)) != 200:
            raise UpstoxConfigurationError(f"market-data authorization returned HTTP {getattr(response, 'status_code', 'unknown')}")
        try:
            payload = response.json()
        except Exception:
            raise UpstoxConfigurationError("market-data authorization returned malformed JSON") from None
        uri = payload.get("data", {}).get("authorized_redirect_uri") if isinstance(payload, dict) else None
        parsed = urlparse(str(uri or ""))
        if parsed.scheme != "wss" or not parsed.hostname or not parsed.hostname.endswith(".upstox.com"):
            raise UpstoxConfigurationError("market-data authorization returned an unexpected WebSocket URL")
        return str(uri)

    async def _send_subscription(self, symbols: Iterable[str]) -> None:
        keys = sorted({self.mapping[symbol]["upstox_instrument_key"] for symbol in symbols})
        if not keys or self._ws is None:
            return
        body = {
            "guid": str(uuid.uuid4()),
            "method": "sub",
            "data": {"mode": "ltpc", "instrumentKeys": keys},
        }
        await self._ws.send(json.dumps(body, separators=(",", ":")).encode("utf-8"))

    def process_message(self, raw: bytes | bytearray | memoryview, *, received_at: datetime | None = None) -> int:
        """Decode one upstream Protobuf frame and enqueue only valid EQ quotes."""
        now = received_at or self._utcnow()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("local receipt timestamp must be timezone-aware")
        if not isinstance(raw, (bytes, bytearray, memoryview)) or not raw:
            self._invalid_count += 1
            return 0
        response = upstox_pb.FeedResponse()
        try:
            response.ParseFromString(bytes(raw))
        except DecodeError:
            self._invalid_count += 1
            return 0
        if response.type not in {upstox_pb.initial_feed, upstox_pb.live_feed, upstox_pb.market_info}:
            self._invalid_count += 1
            return 0
        if response.type == upstox_pb.market_info and not response.HasField("marketInfo"):
            self._invalid_count += 1
            return 0
        if response.type != upstox_pb.market_info and not response.feeds:
            self._invalid_count += 1
            return 0
        self._message_count += 1
        if response.currentTs:
            self._current_ts_raw = int(response.currentTs)
            try:
                self._current_ts_utc = parse_epoch_ms(response.currentTs)
            except ValueError:
                self._current_ts_utc = None
        if response.HasField("marketInfo"):
            self._market_status = {
                str(segment): upstox_pb.MarketStatus.Name(status)
                for segment, status in response.marketInfo.segmentStatus.items()
            }
            self._market_status_updated_at = now.astimezone(timezone.utc)
        accepted = 0
        for instrument_key, feed in response.feeds.items():
            symbol = self.key_to_symbol.get(instrument_key)
            if symbol is None:
                self._invalid_count += 1
                continue
            stats = self._per_symbol[symbol]
            stats["message_count"] += 1
            ltpc, cumulative = _ltpc_and_vtt(feed)
            if ltpc is None:
                self._invalid_count += 1
                stats["invalid_count"] += 1
                continue
            try:
                source_timestamp = parse_epoch_ms(int(ltpc.ltt))
                price = float(ltpc.ltp)
                last_trade_quantity = int(ltpc.ltq)
                previous_close = float(ltpc.cp)
            except (TypeError, ValueError, OverflowError):
                self._invalid_count += 1
                stats["invalid_count"] += 1
                continue
            if (not math.isfinite(price) or price <= 0 or last_trade_quantity < 0
                    or not math.isfinite(previous_close) or previous_close <= 0):
                self._invalid_count += 1
                stats["invalid_count"] += 1
                continue
            now_utc = now.astimezone(timezone.utc)
            source_utc = source_timestamp.astimezone(timezone.utc)
            source_age = (now_utc - source_utc).total_seconds()
            if source_age < -self.max_future_skew_seconds:
                self._invalid_count += 1
                stats["invalid_count"] += 1
                continue
            if source_age > self.max_quote_age_seconds:
                self._stale_count += 1
                stats["stale_count"] += 1
                continue
            tick_signature = f"{instrument_key}|{int(ltpc.ltt)}|{price:.12g}|{last_trade_quantity}"
            if tick_signature in self._seen_tick_ids:
                self._duplicate_count += 1
                stats["duplicate_count"] += 1
                continue
            self._seen_tick_ids.add(tick_signature)
            self._seen_order.append(tick_signature)
            while len(self._seen_order) > 100000:
                self._seen_tick_ids.discard(self._seen_order.popleft())

            volume_value: int | None = None
            volume_kind: str | None = None
            if cumulative is not None:
                local_source_date = source_timestamp.astimezone(__import__("zoneinfo").ZoneInfo(self.session_timezone)).date()
                self.calendar.classify(local_source_date, observed_market_data=True)
                update = self._volume.update(
                    symbol, int(cumulative), source_timestamp, session_date=local_source_date,
                    feed_gap=symbol in self._gap_pending,
                )
                self._gap_pending.discard(symbol)
                if any("DECREASED" in event for event in update.quality_events):
                    self._volume_anomaly_count += 1
                    self._volume_anomaly_events.append({
                        "symbol": symbol,
                        "timestamp": _iso(source_timestamp) or "",
                        "code": "VOLUME_RESET_OR_ANOMALY",
                    })
                if update.incremental_volume is not None:
                    volume_value = update.incremental_volume
                    volume_kind = "INCREMENTAL"
            identity = f"upstox-v3:{tick_signature}"
            quote = NormalizedQuote(
                symbol=symbol,
                exchange="NSE",
                last_price=price,
                change=price - previous_close,
                change_percent=(price - previous_close) / previous_close * 100.0,
                volume=volume_value,
                market_cap=None,
                timestamp=source_timestamp,
                source=SOURCE_NAME,
                received_at=now_utc,
                data_age_seconds=max(0.0, source_age),
                validity_status=QuoteValidity.VALID,
                series="EQ",
                tick_id=identity,
                volume_kind=volume_kind,
                instrument_key=instrument_key,
                source_timestamp_raw=str(int(ltpc.ltt)),
                source_timestamp_unit="epoch_ms",
                source_timestamp_origin="UPSTOX_LTT_CONFIGURED_EPOCH_MS_NOT_EMPIRICALLY_VALIDATED",
                local_received_timestamp_utc=now_utc,
                last_trade_quantity=last_trade_quantity,
                feed_current_timestamp_raw=str(int(response.currentTs)) if response.currentTs else None,
                feed_current_timestamp_utc=self._current_ts_utc,
                feed_message_type=upstox_pb.Type.Name(response.type),
                latency_ms=source_age * 1000.0,
            )
            stats["tick_count"] += 1
            if response.type == upstox_pb.initial_feed:
                stats["snapshot_received"] = True
            elif response.type == upstox_pb.live_feed:
                stats["live_update_received"] = True
            stats["first_tick_timestamp"] = stats["first_tick_timestamp"] or _iso(source_timestamp)
            stats["last_tick_timestamp"] = _iso(source_timestamp)
            stats["last_ltt_raw"] = str(int(ltpc.ltt))
            stats["last_received_timestamp"] = _iso(now_utc)
            stats["last_price"] = price
            stats["normalized_quote_status"] = QuoteValidity.VALID
            stats["latencies_ms"].append(quote.latency_ms or 0.0)
            if len(stats["latencies_ms"]) > 10000:
                del stats["latencies_ms"][: len(stats["latencies_ms"]) - 10000]
            stats["subscription_status"] = "RECEIVING"
            with self._lock:
                if len(self._pending) >= self._max_pending_quotes:
                    self._pending.popleft()
                    self._queue_drop_count += 1
                self._pending.append(quote)
            accepted += 1
        return accepted


def _ltpc_and_vtt(feed: Any) -> tuple[Any | None, int | None]:
    selected = feed.WhichOneof("FeedUnion")
    if selected == "ltpc":
        return feed.ltpc, None
    if selected == "fullFeed":
        which = feed.fullFeed.WhichOneof("FullFeedUnion")
        if which == "marketFF":
            full = feed.fullFeed.marketFF
            return full.ltpc, int(full.vtt) if full.HasField("ltpc") else None
        if which == "indexFF":
            return feed.fullFeed.indexFF.ltpc, None
    if selected == "firstLevelWithGreeks":
        full = feed.firstLevelWithGreeks
        return full.ltpc, int(full.vtt) if full.HasField("ltpc") else None
    return None, None


def _symbol_latency_stats(values: Sequence[float]) -> dict[str, float | None]:
    return {
        "latency_min_ms": min(values) if values else None,
        "latency_avg_ms": statistics.fmean(values) if values else None,
        "latency_median_ms": statistics.median(values) if values else None,
        "latency_p95_ms": _percentile(sorted(values), 0.95),
        "latency_p99_ms": _percentile(sorted(values), 0.99),
        "latency_max_ms": max(values) if values else None,
    }


def _percentile(ordered: Sequence[float], percentile: float) -> float | None:
    if not ordered:
        return None
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return float(ordered[index])


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).isoformat() if value is not None else None


def create_upstox_provider(settings: RealtimeSettings, *, mapping_path: str | Path = MAPPING_PATH) -> UpstoxV3Provider:
    """Create the provider only with a complete verified map and env token."""
    token = os.environ.get("UPSTOX_ACCESS_TOKEN", "")
    if not token:
        raise UpstoxConfigurationError("UPSTOX_ACCESS_TOKEN is not configured")
    mapping = load_verified_mapping(mapping_path)
    try:
        reconnect_attempts = int(os.environ.get("UPSTOX_MAX_RECONNECT_ATTEMPTS", "5"))
    except ValueError as error:
        raise UpstoxConfigurationError("UPSTOX_MAX_RECONNECT_ATTEMPTS must be an integer") from error
    mode = os.environ.get("UPSTOX_MODE", "ltpc").strip().lower()
    return UpstoxV3Provider(
        mapping,
        access_token=token,
        max_quote_age_seconds=settings.max_quote_age_seconds,
        max_future_skew_seconds=settings.max_future_skew_seconds,
        max_reconnect_attempts=reconnect_attempts,
        reconnect_base_seconds=settings.retry_backoff_seconds,
        mode=mode,
    )


def required_dependencies() -> dict[str, bool]:
    """Report import availability without importing or inspecting credentials."""
    try:
        import websockets  # noqa: F401
        websockets_ok = True
    except ImportError:
        websockets_ok = False
    try:
        import google.protobuf  # noqa: F401
        protobuf_ok = True
    except ImportError:
        protobuf_ok = False
    return {"requests": True, "websockets": websockets_ok, "protobuf": protobuf_ok}
