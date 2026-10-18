"""Deterministic session-scoped Yahoo observation reconciliation.

The reconciler is the provider boundary for Phase 4G-YF. Market-bar identity is
always ``(symbol, market_timestamp)``; receipt time is provenance only. A valid
observation becomes eligible to freeze after the one-minute interval and the
configured settling period. A different payload received after that freeze is
retained as a conflict and never replaces the immutable snapshot.

Strategies do not depend on Yahoo fields: this module emits canonical rows
marked ``bar_status=IMMUTABLE_CLOSED`` and the processor gates all inputs on
that marker.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
BAR_SETTLING_SECONDS = int(os.environ.get("PHASE4G_YF_BAR_SETTLING_SECONDS", "5"))
OHLCV_FIELDS = ("open", "high", "low", "close", "volume")
PRICE_FIELDS = ("open", "high", "low", "close")
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)


def _aware(value: Any, field_name: str) -> datetime:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
            str(value).strip().replace("Z", "+00:00")
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is malformed") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone")
    return parsed.astimezone(IST)


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(IST).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return repr(value)
    return value


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(_json_safe(value), sort_keys=True, indent=2,
                                    allow_nan=False, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _atomic_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(_json_safe(row), sort_keys=True, allow_nan=False, default=str) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _append_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("a", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(_json_safe(row), sort_keys=True, allow_nan=False, default=str) + "\n")
            count += 1
        stream.flush()
        os.fsync(stream.fileno())
    return count


def canonical_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    """Return canonical ``(SYMBOL, market_timestamp)`` identity."""
    symbol = str(row.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("symbol is required")
    stamp = _aware(row.get("market_timestamp") or row.get("timestamp"), "market_timestamp")
    return symbol, stamp.isoformat()


def observation_hash(row: Mapping[str, Any]) -> str:
    """Hash the market payload, excluding receipt-time provenance."""
    symbol, market_timestamp = canonical_identity(row)
    payload: dict[str, Any] = {"symbol": symbol, "market_timestamp": market_timestamp,
                               "source": str(row.get("source") or "")}
    for name in OHLCV_FIELDS:
        value = float(row[name])
        if not math.isfinite(value):
            raise ValueError(f"{name} must be finite")
        payload[name] = value
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class BarObservation:
    symbol: str
    market_timestamp: str
    received_timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    observation_hash: str
    bar_state: str
    row: Mapping[str, Any] = field(repr=False)

    def to_dict(self) -> dict[str, Any]:
        result = dict(self.row)
        result.update({
            "symbol": self.symbol, "market_timestamp": self.market_timestamp,
            "received_timestamp": self.received_timestamp, "open": self.open,
            "high": self.high, "low": self.low, "close": self.close,
            "volume": self.volume, "source": self.source,
            "observation_hash": self.observation_hash, "bar_state": self.bar_state,
        })
        return _json_safe(result)


@dataclass(slots=True)
class _Timeline:
    symbol: str
    market_timestamp: str
    versions: dict[str, dict[str, Any]] = field(default_factory=dict)
    latest: dict[str, Any] | None = None
    immutable_bar: dict[str, Any] | None = None
    immutable_at: str | None = None
    current_status: str = "FORMING_BAR"
    conflicting: bool = False


class YahooRevisionReconciler:
    """Keep every observation and expose only immutable, validated bars."""

    def __init__(self, *, session_date: date, settling_seconds: int = BAR_SETTLING_SECONDS,
                 expected_symbols: Sequence[str] | None = None) -> None:
        if settling_seconds < 0:
            raise ValueError("settling_seconds must be non-negative")
        self.session_date = session_date
        self.settling_seconds = int(settling_seconds)
        self.expected_symbols = set(str(item).strip().upper() for item in (expected_symbols or ()))
        self.timelines: dict[tuple[str, str], _Timeline] = {}
        self.observations: list[dict[str, Any]] = []
        self.revision_records: list[dict[str, Any]] = []
        self.immutable_bar_audit: list[dict[str, Any]] = []
        self.invalid_observations: list[dict[str, Any]] = []
        self.readiness_reasons: set[str] = set()
        self.latest_decision_timestamp: str | None = None
        self.last_batch_counts: dict[str, int] = {}
        self._persisted_counts = {"observations": 0, "revisions": 0, "immutable": 0}

    def _cutoff(self, market_timestamp: str) -> datetime:
        return datetime.fromisoformat(market_timestamp) + timedelta(minutes=1, seconds=self.settling_seconds)

    def _validate(self, raw: Mapping[str, Any]) -> dict[str, Any]:
        row = dict(raw)
        identity = canonical_identity(row)
        symbol, market_text = identity
        market = datetime.fromisoformat(market_text)
        received = _aware(row.get("received_timestamp"), "received_timestamp")
        if market.date() != self.session_date:
            raise ValueError("market timestamp is outside the configured session date")
        if market.second or market.microsecond:
            raise ValueError("market timestamp is not minute-aligned")
        if not (SESSION_OPEN <= market.timetz().replace(tzinfo=None) < SESSION_CLOSE):
            raise ValueError("market timestamp is outside NSE regular session")
        if received < market:
            raise ValueError("received timestamp precedes market timestamp")
        if str(row.get("timeframe") or "1m").strip().lower() != "1m":
            raise ValueError("only native 1m bars can enter the immutable-bar gate")
        if self.expected_symbols and symbol not in self.expected_symbols:
            raise ValueError("symbol is outside the frozen expected universe")
        if str(row.get("status") or "CLOSED").strip().upper() != "CLOSED":
            raise ValueError("source row status is not CLOSED")
        source = str(row.get("source") or "").strip()
        if not source:
            raise ValueError("source provenance is required")
        values: dict[str, float] = {}
        for name in OHLCV_FIELDS:
            try:
                value = float(row[name])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"{name} is missing or malformed") from exc
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
            values[name] = value
        if any(values[name] <= 0 for name in PRICE_FIELDS):
            raise ValueError("OHLC values must be positive")
        if values["volume"] < 0:
            raise ValueError("volume must be non-negative")
        if (values["high"] < max(values["open"], values["close"], values["low"])
                or values["low"] > min(values["open"], values["close"], values["high"])):
            raise ValueError("OHLC relationships are invalid")
        row.update(values)
        row.update({"symbol": symbol, "timestamp": market_text, "market_timestamp": market_text,
                    "received_timestamp": received.isoformat(), "timeframe": "1m",
                    "source": source, "status": "CLOSED"})
        return row

    @staticmethod
    def _state_at(market_text: str, received: datetime, settling_seconds: int) -> str:
        market = datetime.fromisoformat(market_text)
        if received < market + timedelta(minutes=1):
            return "FORMING_BAR"
        if received < market + timedelta(minutes=1, seconds=settling_seconds):
            return "SETTLING_BAR"
        return "IMMUTABLE_CLOSED_BAR"

    def _record_invalid(self, raw: Mapping[str, Any], exc: Exception) -> None:
        record = {"raw_observation": _json_safe(dict(raw)), "error_type": type(exc).__name__,
                  "error": str(exc), "classification": "INVALID_OBSERVATION"}
        self.invalid_observations.append(record)
        self.observations.append(record)
        self.readiness_reasons.add("INVALID_OR_UNTRUSTED_OBSERVATION")
        reason = str(exc).casefold()
        if "timezone" in reason:
            self.readiness_reasons.add("TIMEZONE_FAILURE")
        elif "precedes market" in reason or "later than the decision" in reason or "not earlier than the decision" in reason:
            self.readiness_reasons.add("FUTURE_DATA_VIOLATION")
        elif "timestamp" in reason or "session" in reason or "minute-aligned" in reason:
            self.readiness_reasons.add("TIMESTAMP_INTEGRITY_FAILURE")
        elif "ohlc" in reason or "price" in reason or "volume" in reason:
            self.readiness_reasons.add("INVALID_OHLCV_DATA")

    def _advance_key(self, timeline: _Timeline, now: datetime) -> None:
        if timeline.immutable_bar is not None:
            if not timeline.conflicting:
                timeline.current_status = "IMMUTABLE_CLOSED_BAR"
            return
        if timeline.latest is None:
            return
        market = datetime.fromisoformat(timeline.market_timestamp)
        cutoff = self._cutoff(timeline.market_timestamp)
        received = datetime.fromisoformat(str(timeline.latest["received_timestamp"]))
        if now < market + timedelta(minutes=1):
            timeline.current_status = "FORMING_BAR"
            return
        if now < cutoff:
            timeline.current_status = "SETTLING_BAR"
            return
        if received > now:
            # This cannot normally occur because event receipt is replayed in
            # order, but keep the boundary explicit for malformed clocks.
            self.readiness_reasons.add("FUTURE_DATA_VIOLATION")
            timeline.current_status = "SETTLING_BAR"
            return
        frozen = dict(timeline.latest)
        frozen["bar_status"] = "IMMUTABLE_CLOSED"
        frozen["immutable_eligible_at"] = cutoff.isoformat()
        frozen["immutable_at"] = now.isoformat()
        timeline.immutable_bar = frozen
        timeline.immutable_at = now.isoformat()
        timeline.current_status = "IMMUTABLE_CLOSED_BAR"
        self.immutable_bar_audit.append({
            "symbol": timeline.symbol, "market_timestamp": timeline.market_timestamp,
            "received_timestamp": received.isoformat(), "immutable_eligible_at": cutoff.isoformat(),
            "immutable_at": now.isoformat(), "observation_hash": frozen["observation_hash"],
            "bar_status": "IMMUTABLE_CLOSED", "ohlcv": {name: frozen[name] for name in OHLCV_FIELDS},
        })

    def advance(self, decision_timestamp: datetime) -> list[dict[str, Any]]:
        decision = _aware(decision_timestamp, "decision_timestamp")
        self.latest_decision_timestamp = decision.isoformat()
        for key in sorted(self.timelines, key=lambda item: (item[1], item[0])):
            self._advance_key(self.timelines[key], decision)
        return self.immutable_rows()

    def _observe_validated(self, row: dict[str, Any]) -> None:
        key = canonical_identity(row)
        timeline = self.timelines.setdefault(key, _Timeline(*key))
        received = datetime.fromisoformat(row["received_timestamp"])
        state_at_receipt = self._state_at(key[1], received, self.settling_seconds)
        digest = observation_hash(row)
        row["observation_hash"] = digest
        old_latest = dict(timeline.latest) if timeline.latest else None
        old_hash = str(old_latest.get("observation_hash")) if old_latest else None
        record = {**row, "first_seen": timeline.versions.get(digest, {}).get("first_seen", received.isoformat()),
                  "last_seen": received.isoformat(), "bar_state": state_at_receipt,
                  "observation_status": "OBSERVATION"}

        if timeline.immutable_bar is not None:
            immutable_hash = str(timeline.immutable_bar["observation_hash"])
            if digest == immutable_hash:
                record["observation_status"] = "IDENTICAL_DUPLICATE"
            else:
                record["observation_status"] = "POST_IMMUTABILITY_REVISION"
                timeline.conflicting = True
                timeline.current_status = "CONFLICTING_BAR"
                conflict = self._make_revision_record(
                    timeline.immutable_bar, row, previous_state="IMMUTABLE_CLOSED_BAR",
                    new_state="IMMUTABLE_CLOSED_BAR", classification="POST_IMMUTABILITY_REVISION",
                )
                self.revision_records.append(conflict)
                self.readiness_reasons.add("POST_IMMUTABILITY_REVISION")
        elif old_latest is None:
            timeline.latest = dict(row)
            timeline.current_status = state_at_receipt
        elif digest == old_hash:
            record["observation_status"] = "IDENTICAL_DUPLICATE"
        else:
            previous_state = timeline.current_status
            if received < datetime.fromisoformat(key[1]) + timedelta(minutes=1):
                classification = "PRE_CLOSE_REVISION"
            elif received < self._cutoff(key[1]):
                classification = "PRE_IMMUTABILITY_REVISION"
            else:
                classification = "PRE_IMMUTABILITY_REVISION"
            record["observation_status"] = classification
            conflict = self._make_revision_record(old_latest, row, previous_state=previous_state,
                                                  new_state=state_at_receipt, classification=classification)
            self.revision_records.append(conflict)
            timeline.latest = dict(row)
            timeline.current_status = state_at_receipt

        version = timeline.versions.setdefault(digest, {
            "observation_hash": digest, "first_seen": received.isoformat(),
            "last_seen": received.isoformat(), "count": 0,
        })
        version["count"] += 1
        version["last_seen"] = received.isoformat()
        self.observations.append(record)

    @staticmethod
    def _make_revision_record(previous: Mapping[str, Any], new: Mapping[str, Any], *,
                              previous_state: str, new_state: str,
                              classification: str) -> dict[str, Any]:
        ohlc_changed = any(float(previous[name]) != float(new[name]) for name in PRICE_FIELDS)
        volume_changed = float(previous["volume"]) != float(new["volume"])
        conflict_type = "OHLC_AND_VOLUME" if ohlc_changed and volume_changed else (
            "OHLC" if ohlc_changed else "VOLUME" if volume_changed else "IDENTICAL"
        )
        result: dict[str, Any] = {
            "symbol": str(new.get("symbol") or previous.get("symbol") or "").upper(),
            "market_timestamp": str(new.get("market_timestamp") or previous.get("market_timestamp")),
            "previous_received_timestamp": previous.get("received_timestamp"),
            "new_received_timestamp": new.get("received_timestamp"),
            "previous_observation_hash": previous.get("observation_hash"),
            "new_observation_hash": new.get("observation_hash"),
            "bar_state_at_previous_observation": previous_state,
            "bar_state_at_new_observation": new_state,
            "classification": classification,
            "conflict_type": conflict_type,
            "previous_observation": _json_safe(dict(previous)),
            "new_observation": _json_safe(dict(new)),
        }
        for name in OHLCV_FIELDS:
            result[f"previous_{name}"] = previous.get(name)
            result[f"new_{name}"] = new.get(name)
        result["ohlc_changed"] = ohlc_changed
        result["volume_changed"] = volume_changed
        result["zero_volume_to_positive"] = float(previous["volume"]) == 0 and float(new["volume"]) > 0
        return result

    def observe_many(self, rows: Sequence[Mapping[str, Any]], *, decision_timestamp: datetime,
                     source_issues: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
        """Replay a batch in receipt order, independent of response completion order."""
        decision = _aware(decision_timestamp, "decision_timestamp")
        before_obs = len(self.observations)
        before_revs = len(self.revision_records)
        before_invalid = len(self.invalid_observations)
        valid: list[dict[str, Any]] = []
        for raw in rows:
            try:
                row = self._validate(raw)
                received = datetime.fromisoformat(row["received_timestamp"])
                market = datetime.fromisoformat(row["market_timestamp"])
                if received > decision:
                    raise ValueError("received timestamp is later than the decision timestamp")
                if market >= decision:
                    raise ValueError("market timestamp is not earlier than the decision timestamp")
                valid.append(row)
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                self._record_invalid(raw, exc)
        # Receipt timestamp is the chronological ordering key. Canonical key
        # and payload digest make simultaneous inputs deterministic.
        prepared: list[tuple[str, tuple[str, str], str, dict[str, Any]]] = []
        for row in valid:
            try:
                key = canonical_identity(row)
                prepared.append((row["received_timestamp"], key, observation_hash(row), row))
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                self._record_invalid(row, exc)
        prepared.sort(key=lambda item: (item[0], item[1][1], item[1][0], item[2]))

        index = 0
        while index < len(prepared):
            receipt_text = prepared[index][0]
            receipt = datetime.fromisoformat(receipt_text)
            # A cutoff equal to this receipt must see every observation in this
            # same receipt-time group before freezing the candidate.
            self.advance(receipt - timedelta(microseconds=1))
            end = index
            while end < len(prepared) and prepared[end][0] == receipt_text:
                end += 1
            group = prepared[index:end]
            by_key: dict[tuple[str, str], set[str]] = {}
            for _, key, digest, _row in group:
                by_key.setdefault(key, set()).add(digest)
            for key, hashes in by_key.items():
                if len(hashes) > 1 and self.timelines.get(key, _Timeline(*key)).immutable_bar is None:
                    self.readiness_reasons.add("UNRESOLVED_SAME_RECEIPT_CONFLICT")
                    self.revision_records.append({
                        "symbol": key[0], "market_timestamp": key[1],
                        "previous_received_timestamp": receipt_text,
                        "new_received_timestamp": receipt_text,
                        "bar_state_at_previous_observation": self._state_at(
                            key[1], receipt, self.settling_seconds),
                        "bar_state_at_new_observation": self._state_at(
                            key[1], receipt, self.settling_seconds),
                        "classification": "UNKNOWN", "conflict_type": "SIMULTANEOUS_DIFFERING_PAYLOADS",
                        "observation_hashes": sorted(hashes),
                    })
            for _, _key, _digest, row in group:
                self._observe_validated(row)
            self.advance(receipt)
            index = end

        for issue in source_issues:
            item = {"classification": "SOURCE_VALIDATION_FAILURE", **_json_safe(dict(issue))}
            self.invalid_observations.append(item)
            self.observations.append(item)
            self.readiness_reasons.add("INVALID_OR_UNTRUSTED_OBSERVATION")
        self.advance(decision)
        counts = Counter(item.get("observation_status", item.get("classification", "UNKNOWN"))
                         for item in self.observations[before_obs:])
        self.last_batch_counts = {
            "observations_received": len(rows),
            "observations_recorded": len(self.observations) - before_obs,
            "pre_close_revisions": counts.get("PRE_CLOSE_REVISION", 0),
            "pre_immutability_revisions": counts.get("PRE_IMMUTABILITY_REVISION", 0),
            "post_immutability_revisions": counts.get("POST_IMMUTABILITY_REVISION", 0),
            "identical_duplicates": counts.get("IDENTICAL_DUPLICATE", 0),
            "invalid_observations": len(self.invalid_observations) - before_invalid,
            "revision_records": len(self.revision_records) - before_revs,
        }
        return dict(self.last_batch_counts)

    def immutable_rows(self) -> list[dict[str, Any]]:
        return [dict(timeline.immutable_bar) for key, timeline in sorted(
            self.timelines.items(), key=lambda item: (item[0][1], item[0][0])
        ) if timeline.immutable_bar is not None]

    def mutable_status_counts(self, decision_timestamp: datetime | None = None) -> dict[str, int]:
        if decision_timestamp is not None:
            self.advance(decision_timestamp)
        counts = Counter(timeline.current_status for timeline in self.timelines.values())
        counts["CONFLICTING_BAR"] = sum(item.conflicting for item in self.timelines.values())
        counts["IMMUTABLE_CLOSED_BAR"] = sum(item.immutable_bar is not None and not item.conflicting
                                              for item in self.timelines.values())
        return {name: int(counts.get(name, 0)) for name in (
            "FORMING_BAR", "SETTLING_BAR", "IMMUTABLE_CLOSED_BAR", "CONFLICTING_BAR")}

    def readiness_report(self, *, decision_timestamp: datetime | None = None) -> dict[str, Any]:
        if decision_timestamp is not None:
            self.advance(decision_timestamp)
        blockers = sorted(self.readiness_reasons)
        conflicting_count = sum(timeline.conflicting for timeline in self.timelines.values())
        status = "BLOCKED" if blockers else "READY"
        if not blockers and not self.immutable_rows():
            status = "WAITING"
        rejected_count = len(self.invalid_observations) + conflicting_count
        return {
            "gate": "PHASE4G_YF_IMMUTABLE_BAR_READINESS",
            "status": status,
            "decision_timestamp": self.latest_decision_timestamp,
            "settling_seconds": self.settling_seconds,
            "bar_identity": "(symbol, market_timestamp)",
            "immutable_bars": sum(item.immutable_bar is not None for item in self.timelines.values()),
            "rejected_bars": rejected_count,
            "unresolved_revision_conflicts": conflicting_count,
            "invalid_observations": len(self.invalid_observations),
            "blocking_reasons": blockers,
            "bar_status_counts": self.mutable_status_counts(),
            "strategy_input_contract": "IMMUTABLE_CLOSED only",
            "signals_allowed": status == "READY",
            "orders_allowed": status == "READY",
            "fills_allowed": status == "READY",
            "ledger_mutations_allowed": status == "READY",
        }

    def summary(self, *, decision_timestamp: datetime | None = None) -> dict[str, Any]:
        readiness = self.readiness_report(decision_timestamp=decision_timestamp)
        revisions = self.revision_records
        observations = self.observations
        classes = Counter(str(item.get("classification") or item.get("observation_status") or "UNKNOWN")
                          for item in revisions)
        return {
            **readiness,
            "observations_recorded": len(observations),
            "invalid_rows": len(self.invalid_observations),
            "unique_market_bars": len(self.timelines),
            "unique_observation_versions": sum(len(item.versions) for item in self.timelines.values()),
            "revision_records": len(revisions),
            "pre_close_revisions": sum(item.get("classification") == "PRE_CLOSE_REVISION" for item in revisions),
            "pre_immutability_revisions": sum(item.get("classification") == "PRE_IMMUTABILITY_REVISION" for item in revisions),
            "post_immutability_revisions": sum(item.get("classification") == "POST_IMMUTABILITY_REVISION" for item in revisions),
            "unknown_conflicts": sum(item.get("classification") == "UNKNOWN" for item in revisions),
            "ohlc_revisions": sum(bool(item.get("ohlc_changed")) for item in revisions),
            "volume_revisions": sum(bool(item.get("volume_changed")) for item in revisions),
            "zero_volume_to_positive_revisions": sum(bool(item.get("zero_volume_to_positive")) for item in revisions),
            "revision_class_counts": dict(sorted(classes.items())),
            "last_refresh_counts": dict(self.last_batch_counts),
        }

    def mark_persisted_counts(self, *, observations: int = 0, revisions: int = 0,
                              immutable: int = 0) -> None:
        """Set append cursors after loading an existing journal during restore."""
        self._persisted_counts = {"observations": max(0, int(observations)),
                                  "revisions": max(0, int(revisions)),
                                  "immutable": max(0, int(immutable))}

    def write_artifacts(self, output_dir: str | Path, *, decision_timestamp: datetime | None = None) -> dict[str, Any]:
        root = Path(output_dir)
        summary = self.summary(decision_timestamp=decision_timestamp)
        self._persisted_counts["observations"] += _append_jsonl(
            root / "reports/observations.jsonl", self.observations[self._persisted_counts["observations"]:]
        )
        self._persisted_counts["revisions"] += _append_jsonl(
            root / "reports/revision_records.jsonl", self.revision_records[self._persisted_counts["revisions"]:]
        )
        self._persisted_counts["immutable"] += _append_jsonl(
            root / "reports/immutable_bar_audit.jsonl", self.immutable_bar_audit[self._persisted_counts["immutable"]:]
        )
        report = self.readiness_report(decision_timestamp=decision_timestamp)
        _atomic_json(root / "reports/readiness_report.json", report)
        _atomic_json(root / "reports/revision_summary.json", summary)
        return summary

    def replay_observations(self, rows: Sequence[Mapping[str, Any]], *, decision_timestamp: datetime) -> dict[str, Any]:
        return self.observe_many(rows, decision_timestamp=decision_timestamp)
