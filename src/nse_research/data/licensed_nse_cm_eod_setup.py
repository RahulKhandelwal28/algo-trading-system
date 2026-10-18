"""Post-import coverage and source-fact provenance for licensed NSE CM EOD."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from nse_research.data.licensed_nse_cm_eod import (
    CANONICAL_COLUMNS,
    LicensedNseCmEodProvider,
    NseCmEodSchema,
)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _base_symbol(value: object) -> str:
    return str(value or "").strip().upper().removesuffix(".NS")


def write_coverage_and_provenance(
    *,
    provider: LicensedNseCmEodProvider,
    import_manifest: Mapping[str, Any],
    expected_symbols: Iterable[str],
    source_provider: str | None = None,
    delivery_date: str | None = None,
    source_identifier: str | None = None,
    adjustment_status: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Write audit JSON from the importer's results and explicitly supplied facts.

    Missing source metadata remains absent. ``adjustment_status`` should only
    be supplied when it is stated in the authorized delivery documentation.
    """
    report_path = Path(provider.report_path)
    report: dict[str, Any] = {}
    if report_path.is_file():
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            report = {}
    counts = report.get("normalization_counts", {})
    if not isinstance(counts, dict):
        counts = {}
    symbols = sorted({_base_symbol(item) for item in import_manifest.get("symbols", []) if _base_symbol(item)})
    expected = sorted({_base_symbol(item) for item in expected_symbols if _base_symbol(item)})
    missing_symbols = sorted(set(expected) - set(symbols))
    session_stats = report.get("missing_session_statistics", {})
    if not isinstance(session_stats, dict):
        session_stats = {}
    expected_session_count = session_stats.get("expected_session_count_in_request")
    missing_sessions: int | None = session_stats.get("dataset_missing_sessions")
    first_date = import_manifest.get("actual_first_date")
    last_date = import_manifest.get("actual_last_date")
    row_count = int(import_manifest.get("row_count", 0) or 0)
    distinct_sessions = 0
    observed_sessions: set[date] = set()
    if provider.dataset_path.is_file():
        try:
            # The normalized output is the importer's canonical EQ-only file.
            frame = pd.read_csv(provider.dataset_path, usecols=["Date"], dtype=str, keep_default_na=False)
            dates = pd.to_datetime(frame["Date"], errors="coerce", dayfirst=True, format="mixed").dropna()
            distinct_sessions = int(dates.dt.date.nunique())
            observed_sessions = set(dates.dt.date)
        except Exception:
            distinct_sessions = 0
    if expected_session_count is None:
        session_count = distinct_sessions
    else:
        session_count = int(expected_session_count)
    missing_session_dates: list[str] | None = None
    if provider.exchange_sessions is not None:
        requested_start = date.fromisoformat(str(import_manifest.get("requested_start")))
        requested_end = date.fromisoformat(str(import_manifest.get("requested_end")))
        expected_dates = sorted({
            day for day in provider.exchange_sessions if requested_start <= day <= requested_end
        })
        missing_session_dates = [day.isoformat() for day in expected_dates if day not in observed_sessions]
        missing_sessions = len(missing_session_dates)

    import_status = str(import_manifest.get("validation_status", "FAIL"))
    coverage_status = import_status
    if import_status != "FAIL" and (missing_symbols or (missing_sessions not in (None, 0))):
        coverage_status = "FAIL"
    elif import_status == "PASS_WITH_WARNINGS":
        coverage_status = "PASS_WITH_WARNINGS"
    elif import_status == "PASS":
        coverage_status = "PASS"
    coverage: dict[str, Any] = {
        "first_date": first_date,
        "last_date": last_date,
        "number_of_sessions": session_count,
        "number_of_symbols": len(symbols),
        "total_rows": int(counts.get("source_row_count", import_manifest.get("source_row_count", row_count)) or 0),
        "eq_rows": int(counts.get("eq_rows_seen", import_manifest.get("eq_rows_seen", row_count)) or 0),
        "non_eq_rows": int(counts.get("non_eq_rows_excluded", 0) or 0),
        "missing_sessions": missing_sessions,
        "missing_session_dates": missing_session_dates,
        "missing_symbols": missing_symbols,
        "duplicate_rows": int(counts.get("duplicate_count", import_manifest.get("duplicate_count", 0)) or 0),
        "invalid_rows": int(counts.get("quality_rejected_row_count", 0) or 0),
        "validation_status": coverage_status,
    }
    coverage_path = Path(provider.dataset_path).parent / "coverage_report.json"
    _atomic_json(coverage_path, coverage)

    file_names = sorted({
        Path(str(item.get("path", ""))).name
        for item in import_manifest.get("source_file_hashes", [])
        if isinstance(item, dict) and item.get("path")
    })
    provenance: dict[str, Any] = {
        "coverage": coverage,
        "schema": {
            "canonical_columns": list(CANONICAL_COLUMNS),
            "source_column_mapping": {
                "date": provider.schema.date_column,
                "symbol": provider.schema.symbol_column,
                "series": provider.schema.series_column,
                "open": provider.schema.open_column,
                "high": provider.schema.high_column,
                "low": provider.schema.low_column,
                "close": provider.schema.close_column,
                "volume": provider.schema.volume_column,
            },
        },
    }
    if len(file_names) == 1:
        provenance["file_name"] = file_names[0]
    elif file_names:
        provenance["file_names"] = file_names
    supplied: dict[str, Any] = {
        "provider": source_provider,
        "delivery_date": delivery_date,
        "source_identifier": source_identifier,
        "adjustment_status": adjustment_status,
        "authorization_reference": import_manifest.get("authorization_reference"),
    }
    provenance.update({key: value for key, value in supplied.items() if value not in (None, "")})
    provenance_path = Path(provider.dataset_path).parent / "provenance.json"
    _atomic_json(provenance_path, provenance)
    return coverage, provenance


def import_authorized_nse_cm_eod(
    *,
    provider: LicensedNseCmEodProvider,
    start: date,
    end: date,
    authorization_reference: str,
    expected_symbols: Iterable[str],
    source_provider: str | None = None,
    delivery_date: str | None = None,
    source_identifier: str | None = None,
    adjustment_status: str | None = None,
) -> dict[str, Any]:
    """Run the existing licensed importer, then write its coverage/provenance."""
    manifest = provider.import_to_dataset(start, end, authorization_reference=authorization_reference)
    coverage, provenance = write_coverage_and_provenance(
        provider=provider,
        import_manifest=manifest,
        expected_symbols=expected_symbols,
        source_provider=source_provider,
        delivery_date=delivery_date,
        source_identifier=source_identifier,
        adjustment_status=adjustment_status,
    )
    return {"import": manifest, "coverage": coverage, "provenance": provenance}
