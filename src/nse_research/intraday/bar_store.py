"""Canonical first-observed store for Phase 4G-YF one-minute market bars.

Market identity is ``(SYMBOL, MARKET_TIMESTAMP)``.  Receipt time is retained as
provenance and is never part of identity.  Planning is side-effect free so a
conflicting/invalid refresh cannot partially update the store or reach the
strategy, risk, or paper-ledger path.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from nse_research.intraday.yahoo_revision_reconciler import BAR_SETTLING_SECONDS
from typing import Any, Mapping, Sequence

from nse_research.data.yfinance_intraday import IST


BarKey = tuple[str, str]
OHLCV_FIELDS = ("open", "high", "low", "close", "volume")
PRICE_FIELDS = ("open", "high", "low", "close")


def _aware(value: Any, name: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} is malformed") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.astimezone(IST)


def canonical_bar_key(row: Mapping[str, Any]) -> BarKey:
    """Return the sole canonical one-minute bar identity."""
    symbol = str(row.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("symbol is required")
    market = _aware(row.get("market_timestamp") or row.get("timestamp"), "market_timestamp")
    timestamp = row.get("timestamp")
    if timestamp not in (None, "") and _aware(timestamp, "timestamp") != market:
        raise ValueError("timestamp and market_timestamp identify different market bars")
    return symbol, market.isoformat()


def bar_eligibility_reason(row: Mapping[str, Any], decision_timestamp: datetime, *,
                           settling_seconds: int = BAR_SETTLING_SECONDS) -> str | None:
    """Central closed-bar and receipt-time rule used by all Phase 4G decisions."""
    decision = _aware(decision_timestamp, "decision_timestamp")
    market = _aware(row.get("market_timestamp") or row.get("timestamp"), "market_timestamp")
    received = _aware(row.get("received_timestamp"), "received_timestamp")
    timeframe = str(row.get("timeframe") or "1m").strip().lower()
    try:
        minutes = int(timeframe[:-1])
    except (TypeError, ValueError) as exc:
        raise ValueError("timeframe is invalid") from exc
    if minutes < 1 or not timeframe.endswith("m"):
        raise ValueError("timeframe is invalid")
    if received > decision:
        return "late_receipt"
    if market >= decision or market + timedelta(minutes=minutes) > decision:
        return "market_bar_not_closed_before_decision"
    if market + timedelta(minutes=minutes, seconds=settling_seconds) > decision:
        return "bar_settling_period_not_elapsed"
    if str(row.get("bar_status", "")).upper() != "IMMUTABLE_CLOSED":
        return "bar_not_immutable"
    if str(row.get("status", "")).upper() != "CLOSED":
        return "bar_not_closed_status"
    return None


def _normalise_row(row: Mapping[str, Any]) -> dict[str, Any]:
    key = canonical_bar_key(row)
    timeframe = str(row.get("timeframe") or "1m").strip().lower()
    if timeframe != "1m":
        raise ValueError("canonical source store accepts only 1m observations")
    received = _aware(row.get("received_timestamp"), "received_timestamp")
    if str(row.get("status") or "").upper() != "CLOSED":
        raise ValueError("bar status must be CLOSED")
    if str(row.get("bar_status") or "").upper() != "IMMUTABLE_CLOSED":
        raise ValueError("canonical strategy store accepts only immutable closed bars")
    source = str(row.get("source") or "").strip()
    if not source:
        raise ValueError("source provenance is required")
    result = dict(row)
    result["symbol"] = key[0]
    result["timestamp"] = key[1]
    result["market_timestamp"] = key[1]
    result["received_timestamp"] = received.isoformat()
    result["timeframe"] = "1m"
    market = datetime.fromisoformat(key[1])
    if market + timedelta(minutes=1) > received:
        raise ValueError("bar was received before its one-minute interval closed")
    for field in OHLCV_FIELDS:
        value = float(row[field])
        if not math.isfinite(value):
            raise ValueError(f"{field} must be finite")
        result[field] = value
    if any(result[field] <= 0 for field in PRICE_FIELDS):
        raise ValueError("OHLC prices must be positive")
    if result["volume"] < 0:
        raise ValueError("volume must be non-negative")
    if (result["high"] < max(result["open"], result["close"], result["low"])
            or result["low"] > min(result["open"], result["close"], result["high"])):
        raise ValueError("OHLC relationships are invalid")
    return result


def _payload_equal(first: Mapping[str, Any], second: Mapping[str, Any]) -> bool:
    # Treat sub-microprice floating point serialization noise as identical;
    # volume changes are exact because even a zero/non-zero change is material.
    return all(
        math.isclose(float(first[field]), float(second[field]), rel_tol=1e-12, abs_tol=1e-8)
        if field in PRICE_FIELDS else float(first[field]) == float(second[field])
        for field in OHLCV_FIELDS
    )


def _conflict_record(first: Mapping[str, Any], second: Mapping[str, Any]) -> dict[str, Any]:
    record: dict[str, Any] = {
        "status": "DUPLICATE_CONFLICT",
        "symbol": str(first["symbol"]).upper(),
        "market_timestamp": str(first["market_timestamp"]),
        "first_received_timestamp": first.get("received_timestamp"),
        "second_received_timestamp": second.get("received_timestamp"),
        "first_source": first.get("source"),
        "second_source": second.get("source"),
    }
    for field in OHLCV_FIELDS:
        record[f"old_{field}"] = first.get(field)
        record[f"new_{field}"] = second.get(field)
    return record


@dataclass(slots=True)
class BarIngestPlan:
    accounting: dict[str, Any]
    snapshot_rows: list[dict[str, Any]]
    new_rows: list[dict[str, Any]]
    current_session_new_rows: list[dict[str, Any]]
    late_rows: list[dict[str, Any]]
    conflicts: list[dict[str, Any]]
    invalid_rows: list[dict[str, Any]]
    metadata_updates: dict[BarKey, dict[str, Any]]
    _staged: dict[BarKey, dict[str, Any]]


class CanonicalBarStore:
    """Session-scoped first-observed bar store with conflict-safe planning."""

    def __init__(self) -> None:
        self.records: dict[BarKey, dict[str, Any]] = {}

    def restore(self, row: Mapping[str, Any]) -> None:
        """Load one persisted canonical row, rejecting duplicate/corrupt keys."""
        try:
            canonical = _normalise_row(row)
            key = canonical_bar_key(canonical)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"persisted bar is invalid: {exc}") from exc
        if key in self.records:
            raise ValueError(f"persisted canonical bar key is duplicated: {key!r}")
        self.records[key] = canonical

    def high_watermarks(self) -> dict[str, datetime]:
        result: dict[str, datetime] = {}
        for (symbol, market_text) in self.records:
            market = datetime.fromisoformat(market_text)
            if symbol not in result or market > result[symbol]:
                result[symbol] = market
        return result

    def plan_ingest(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        decision_timestamp: datetime,
        expected_symbols: Sequence[str],
        warmed_up: bool,
        previous_decision_timestamp: datetime | None = None,
        refresh_failures: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> BarIngestPlan:
        """Classify a refresh without mutating stored bars."""
        decision = _aware(decision_timestamp, "decision_timestamp")
        previous_decision = (_aware(previous_decision_timestamp, "previous_decision_timestamp")
                             if previous_decision_timestamp is not None else None)
        requested = sorted({str(symbol).strip().upper() for symbol in expected_symbols})
        failures = {str(symbol).strip().upper(): dict(value)
                    for symbol, value in (refresh_failures or {}).items()}
        per_symbol: dict[str, dict[str, Any]] = {
            symbol: {"symbol": symbol, "rows_seen": 0, "new_bars": 0, "duplicates": 0,
                     "last_market_timestamp": None, "last_received_timestamp": None,
                     "fetch_failure": symbol in failures,
                     "fetch_status": ("DATA_UNAVAILABLE_FOR_THIS_REFRESH" if symbol in failures else "NO_ROWS_THIS_REFRESH"),
                     "error_type": failures.get(symbol, {}).get("error_type"),
                     "error_message": failures.get(symbol, {}).get("error_message"),
                     "attempt_timestamp": decision.isoformat() if symbol in failures else None}
            for symbol in requested
        }
        invalid_rows: list[dict[str, Any]] = []
        normalized: list[dict[str, Any]] = []
        raw_counts: dict[str, int] = {}
        for index, raw in enumerate(rows):
            raw_symbol = str(raw.get("symbol") or "<UNKNOWN>").strip().upper()
            raw_counts[raw_symbol] = raw_counts.get(raw_symbol, 0) + 1
            if raw_symbol in per_symbol:
                per_symbol[raw_symbol]["rows_seen"] += 1
            try:
                normalized.append(_normalise_row(raw))
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                invalid_rows.append({"row_index": index, "symbol": raw_symbol,
                                     "error_type": type(exc).__name__, "error_message": str(exc)})
        normalized.sort(key=lambda item: (
            item["symbol"], item["market_timestamp"], item["received_timestamp"],
            tuple(float(item[field]) for field in OHLCV_FIELDS), str(item.get("source") or ""),
        ))
        # ``snapshot_first`` is distinct from ``staged``: it represents every
        # distinct key returned by this refresh, including bars already in the
        # session store.  This lets accounting report both unique rows seen in
        # a response and genuinely new first observations.
        snapshot_first: dict[BarKey, dict[str, Any]] = {}
        staged: dict[BarKey, dict[str, Any]] = {}
        metadata_updates: dict[BarKey, dict[str, Any]] = {}
        conflicts: list[dict[str, Any]] = []
        duplicate_identical = 0
        for row in normalized:
            key = canonical_bar_key(row)
            response_first = snapshot_first.get(key)
            stored_first = self.records.get(key)
            if response_first is None:
                snapshot_first[key] = row
            first = stored_first or response_first
            if first is not None:
                item = per_symbol.setdefault(key[0], {
                    "symbol": key[0], "rows_seen": raw_counts.get(key[0], 0), "new_bars": 0,
                    "duplicates": 0, "last_market_timestamp": None,
                    "last_received_timestamp": None, "fetch_failure": key[0] in failures,
                    "fetch_status": "DATA_UNAVAILABLE_FOR_THIS_REFRESH" if key[0] in failures else "OK",
                    "error_type": failures.get(key[0], {}).get("error_type"),
                    "error_message": failures.get(key[0], {}).get("error_message"),
                    "attempt_timestamp": decision.isoformat() if key[0] in failures else None,
                })
                item["duplicates"] += 1
                if _payload_equal(first, row):
                    duplicate_identical += 1
                    if (str(first.get("volume_kind", "UNKNOWN")).upper() == "UNKNOWN"
                            and str(row.get("volume_kind", "UNKNOWN")).upper() == "BAR"):
                        metadata_updates[key] = {"volume_kind": "BAR"}
                else:
                    conflicts.append(_conflict_record(first, row))
                continue
            staged[key] = row

        prior_high = self.high_watermarks()
        new_rows = [staged[key] for key in sorted(staged)]
        late_rows: list[dict[str, Any]] = []
        current_new: list[dict[str, Any]] = []
        for row in new_rows:
            symbol = row["symbol"]
            market = datetime.fromisoformat(row["market_timestamp"])
            prior = prior_high.get(symbol)
            bar_end = market + timedelta(minutes=1)
            if ((prior is not None and market < prior)
                    or (previous_decision is not None and bar_end <= previous_decision)):
                late_rows.append(row)
            elif warmed_up and prior is not None and market > prior:
                current_new.append(row)
            per_symbol.setdefault(symbol, {
                "symbol": symbol, "rows_seen": raw_counts.get(symbol, 0), "new_bars": 0,
                "duplicates": 0, "last_market_timestamp": None,
                "last_received_timestamp": None, "fetch_failure": symbol in failures,
                "fetch_status": "DATA_UNAVAILABLE_FOR_THIS_REFRESH" if symbol in failures else "OK",
                "error_type": failures.get(symbol, {}).get("error_type"),
                "error_message": failures.get(symbol, {}).get("error_message"),
                "attempt_timestamp": decision.isoformat() if symbol in failures else None,
            })["new_bars"] += 1

        unique_snapshot_rows = [snapshot_first[key] for key in sorted(snapshot_first)]
        # Eligibility for an already-known market bar is based on its retained
        # first receipt, never a later duplicate's receipt timestamp.
        eligibility_rows = [self.records.get(key) or snapshot_first[key]
                            for key in sorted(snapshot_first)]
        eligible_bars = sum(bar_eligibility_reason(row, decision) is None for row in eligibility_rows)
        for symbol, entry in per_symbol.items():
            observed = [row for row in unique_snapshot_rows if row["symbol"] == symbol]
            if observed:
                entry["last_market_timestamp"] = max(row["market_timestamp"] for row in observed)
                entry["last_received_timestamp"] = max(row["received_timestamp"] for row in observed)
                if not entry["fetch_failure"]:
                    entry["fetch_status"] = "OK"
        committed_symbols = {key[0] for key in self.records} | {key[0] for key in staged}
        current_symbols = {row["symbol"] for row in current_new}
        metrics = {
            "rows_seen": len(rows),
            "unique_market_bars": len(snapshot_first),
            "unique_rows_seen": len(snapshot_first),
            "new_market_bars": len(new_rows),
            "duplicate_identical": duplicate_identical,
            "duplicate_conflict": len(conflicts),
            "duplicate_market_bars": duplicate_identical + len(conflicts),
            "invalid_rows": len(invalid_rows),
            "late_bars": len(late_rows),
            "eligible_bars": eligible_bars,
            "unique_market_bars_total": len(self.records) + len(staged),
            "current_session_new_bars": len(current_new),
            "warmup_bars": 0 if warmed_up else len(unique_snapshot_rows),
            "universe_symbols": len(requested),
            "symbols_with_valid_session_data": len(committed_symbols),
            "symbols_with_new_bars": len(current_symbols),
            "symbols_with_refresh_failure": sum(bool(per_symbol.get(symbol, {}).get("fetch_failure"))
                                                  for symbol in requested),
            "refresh_failure_symbols": sorted(symbol for symbol in requested
                                               if per_symbol.get(symbol, {}).get("fetch_failure")),
            "per_symbol": [per_symbol[symbol] for symbol in sorted(per_symbol)],
        }
        return BarIngestPlan(
            accounting=metrics,
            snapshot_rows=unique_snapshot_rows,
            new_rows=new_rows,
            current_session_new_rows=current_new,
            late_rows=late_rows,
            conflicts=conflicts,
            invalid_rows=invalid_rows,
            metadata_updates=metadata_updates,
            _staged=staged,
        )

    def commit(self, plan: BarIngestPlan) -> None:
        """Commit a previously validated and conflict-free refresh plan."""
        if plan.conflicts or plan.invalid_rows:
            raise ValueError("cannot commit an invalid or conflicting bar-store plan")
        for key, row in plan._staged.items():
            if key in self.records:
                raise ValueError(f"bar store changed after planning: {key!r}")
            self.records[key] = dict(row)
        for key, updates in plan.metadata_updates.items():
            if key in self.records:
                self.records[key].update(updates)

    def ordered_rows(self) -> list[dict[str, Any]]:
        return [self.records[key] for key in sorted(self.records, key=lambda item: (item[1], item[0]))]
