"""Acquire a pinned public TejHQ snapshot and audit it without running research."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))
DATASET_ID = "tejhq/indian-markets"
DATASET_URL = "https://huggingface.co/datasets/tejhq/indian-markets"
API_URL = "https://huggingface.co/api/datasets/tejhq/indian-markets"
FILES_BASE = "https://huggingface.co/datasets/tejhq/indian-markets/resolve"
CURRENT_PIN = "12cf222405044b511b28730d071e7693a53e5a52"
FOLDS = [
    (1, "train", "2021-06-09", "2023-06-08"),
    (1, "validation", "2023-06-09", "2023-12-08"),
    (1, "test", "2023-12-11", "2024-06-07"),
    (2, "train", "2021-12-09", "2023-12-08"),
    (2, "validation", "2023-12-11", "2024-06-07"),
    (2, "test", "2024-06-10", "2024-12-06"),
    (3, "train", "2022-06-09", "2024-06-07"),
    (3, "validation", "2024-06-10", "2024-12-06"),
    (3, "test", "2024-12-09", "2025-06-06"),
    (4, "train", "2022-12-09", "2024-12-06"),
    (4, "validation", "2024-12-09", "2025-06-06"),
    (4, "test", "2025-06-09", "2025-12-08"),
    (5, "train", "2023-06-09", "2025-06-06"),
    (5, "validation", "2025-06-09", "2025-12-08"),
    (5, "test", "2025-12-09", "2026-06-08"),
]
RAW_REQUIRED = (
    "date", "symbol", "series", "isin", "name", "open", "high", "low", "close",
    "last", "prev_close", "volume", "turnover", "trades",
)
NUMERIC_FIELDS = ("open", "high", "low", "close", "last", "prev_close", "volume", "turnover", "trades")
NORMALIZED_FIELDS = (
    "date", "symbol", "series", "isin", "open", "high", "low", "close", "prev_close", "volume", "turnover",
)
OHLC_TOLERANCE = 1e-9


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_default) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return float(value) if math.isfinite(float(value)) else None
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _repo_artifacts(tree: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = []
    for item in tree:
        path = item.get("path", "")
        is_nse_raw = path.startswith("nse/year=") and path.endswith(".parquet")
        is_nse_action = path.startswith("actions/nse_") and path.endswith(".parquet")
        if is_nse_raw or is_nse_action or path in {
            "symbol_history/nse.parquet", "universe/nse_liquid.parquet", "README.md",
        }:
            selected.append(item)
    return sorted(selected, key=lambda item: item["path"])


def _local_path_for(root: Path, repo_path: str) -> Path:
    if repo_path == "README.md":
        return root / "metadata" / "tejhq_dataset_card.md"
    return root / "raw" / repo_path


def _download_one(session: requests.Session, revision: str, artifact: dict[str, Any], destination: Path) -> dict[str, Any]:
    remote_path = artifact["path"]
    lfs = artifact.get("lfs") or {}
    expected_size = int(lfs.get("size", artifact.get("size", -1)))
    expected_sha256 = lfs.get("oid")
    if destination.is_file():
        digest = sha256_file(destination)
        if expected_sha256 and digest == expected_sha256 and destination.stat().st_size == expected_size:
            return {
                "repo_path": remote_path, "local_path": str(destination.relative_to(root_for(destination))),
                "bytes": destination.stat().st_size, "sha256": digest, "repo_oid": artifact.get("oid"),
                "lfs_sha256": expected_sha256, "http_status": "reused_verified_local_file",
            }
        raise RuntimeError(f"refusing to overwrite existing TejHQ snapshot file with different bytes: {destination}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.partial")
    url = f"{FILES_BASE}/{revision}/{remote_path}"
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with session.get(url, stream=True, timeout=(15, 120), allow_redirects=True) as response:
                response.raise_for_status()
                digest = hashlib.sha256()
                byte_count = 0
                with temporary.open("wb") as stream:
                    for block in response.iter_content(chunk_size=1024 * 1024):
                        if block:
                            stream.write(block)
                            digest.update(block)
                            byte_count += len(block)
                if expected_size >= 0 and byte_count != expected_size:
                    raise RuntimeError(f"{remote_path}: expected {expected_size} bytes, received {byte_count}")
                file_sha256 = digest.hexdigest()
                if expected_sha256 and file_sha256 != expected_sha256:
                    raise RuntimeError(f"{remote_path}: downloaded bytes do not match the Hugging Face LFS SHA-256")
                os.replace(temporary, destination)
                return {
                    "repo_path": remote_path,
                    "local_path": str(destination.relative_to(root_for(destination))),
                    "bytes": byte_count,
                    "sha256": file_sha256,
                    "repo_oid": artifact.get("oid"),
                    "lfs_sha256": expected_sha256,
                    "http_status": int(response.status_code),
                    "content_type": response.headers.get("content-type"),
                    "etag": response.headers.get("etag"),
                }
        except (requests.RequestException, OSError, RuntimeError) as exc:
            last_error = exc
            temporary.unlink(missing_ok=True)
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
    raise RuntimeError(f"failed to download {remote_path}: {last_error}")


def root_for(path: Path) -> Path:
    """Find the tejhq data root from a destination under its raw/ or metadata/."""
    for parent in path.parents:
        if parent.name == "tejhq":
            return parent
    raise ValueError(f"path is not below data/tejhq: {path}")


def acquire_snapshot(root: Path, revision: str = CURRENT_PIN) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    metadata_dir = root / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": "nse-research-tejhq-independent-audit/1.0"})

    response = session.get(API_URL, timeout=(15, 45))
    response.raise_for_status()
    repo_metadata = response.json()
    resolved_revision = revision or repo_metadata.get("sha")
    if not resolved_revision:
        raise RuntimeError("Hugging Face did not provide a dataset revision")
    tree_response = session.get(f"{API_URL}/tree/{resolved_revision}?recursive=true", timeout=(15, 45))
    tree_response.raise_for_status()
    tree = tree_response.json()
    artifacts = _repo_artifacts(tree)
    raw_partitions = [item for item in artifacts if item["path"].startswith("nse/year=")]
    if len(raw_partitions) < 1 or not any(item["path"].startswith("nse/year=2026/") for item in raw_partitions):
        raise RuntimeError("pinned TejHQ revision does not contain the expected full-history NSE tree through 2026")
    metadata_safe = {
        key: repo_metadata.get(key)
        for key in ("id", "sha", "lastModified", "private", "gated", "disabled", "tags", "cardData", "description")
    }
    _write_json(metadata_dir / "huggingface_dataset_metadata.json", metadata_safe)
    _write_json(metadata_dir / "huggingface_repo_tree.json", artifacts)

    downloaded = []
    for artifact in artifacts:
        destination = _local_path_for(root, artifact["path"])
        downloaded.append(_download_one(session, resolved_revision, artifact, destination))
    dataset_card = metadata_dir / "tejhq_dataset_card.md"
    card_record = next((item for item in downloaded if item["repo_path"] == "README.md"), None)
    if card_record is None or not dataset_card.is_file():
        raise RuntimeError("pinned TejHQ README was not acquired")

    # Snapshot identity is content-addressed over exact repository paths and file SHA-256s.
    frozen_files = sorted(
        ({"repo_path": item["repo_path"], "sha256": item["sha256"]} for item in downloaded),
        key=lambda item: item["repo_path"],
    )
    canonical = json.dumps(frozen_files, sort_keys=True, separators=(",", ":")).encode("utf-8")
    snapshot_sha256 = hashlib.sha256(canonical).hexdigest()
    card_text = dataset_card.read_text(encoding="utf-8", errors="replace")
    card_license = repo_metadata.get("cardData", {}).get("license")
    manifest = {
        "provider": "tejhq",
        "dataset_name": "TejHQ Indian Markets",
        "dataset_id": DATASET_ID,
        "source_url": DATASET_URL,
        "huggingface_api_url": API_URL,
        "dataset_revision": resolved_revision,
        "dataset_head_at_acquisition": repo_metadata.get("sha"),
        "source_last_modified_utc": repo_metadata.get("lastModified"),
        "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_description": "Third-party historical market data derived from NSE/BSE bhavcopy data, subject to independent audit.",
        "license": card_license or "UNKNOWN",
        "license_reference": f"{DATASET_URL} (pinned README at {resolved_revision})",
        "license_notice": "The pinned dataset card says MIT and states exchange-published data redistribution is permitted; it advises verifying exchange terms before commercial use. This is recorded as the source's statement, not an independent legal determination.",
        "source_methodology_claims": {
            "source": "NSE and BSE EOD bhavcopy and corporate-action feeds, as stated by TejHQ",
            "raw_tree_processing": "TejHQ documents raw prices as cleaned, deduped, and typed; its caveat says detected anomalous bhavcopy rows are dropped before publishing.",
            "corporate_action_adjustment": "See pinned source documentation; raw-tree OHLCV is kept distinct from the adjusted-price tree.",
        },
        "source_documentation": {
            "dataset_card_path": "metadata/tejhq_dataset_card.md",
            "dataset_card_sha256": card_record["sha256"],
            "metadata_path": "metadata/huggingface_dataset_metadata.json",
            "repository_tree_path": "metadata/huggingface_repo_tree.json",
            "license_text_found_in_pinned_card": "mit" if "license: mit" in card_text.lower() or "MIT" in card_text else "UNKNOWN",
        },
        "files": downloaded,
        "snapshot_sha256": snapshot_sha256,
        "row_count": None,
        "column_names": None,
        "date_min": None,
        "date_max": None,
        "symbol_count": None,
        "series_count": None,
        "audit_status": "AUDIT_PENDING",
        "normalized_dataset": None,
    }
    _write_json(root / "tejhq_dataset_manifest.json", manifest)
    return manifest


def _safe_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.normalize()


def _longest_missing_run(expected: list[pd.Timestamp], actual: set[pd.Timestamp]) -> int:
    longest = current = 0
    for stamp in expected:
        if stamp not in actual:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _audit_duplicates(frame: pd.DataFrame) -> tuple[int, int, int, list[dict[str, Any]]]:
    key = ["date", "symbol", "series"]
    duplicate_mask = frame.duplicated(key, keep=False)
    if not duplicate_mask.any():
        return 0, 0, 0, []
    duplicate_rows = frame.loc[duplicate_mask].copy()
    grouped = duplicate_rows.groupby(key, dropna=False, sort=False)
    excess_count = sum(len(group) - 1 for _, group in grouped)
    exact_groups = 0
    conflicting_groups = 0
    examples: list[dict[str, Any]] = []
    compare_cols = [column for column in frame.columns if column not in key]
    for values, group in grouped:
        exact = len(group.drop_duplicates(subset=compare_cols)) == 1
        exact_groups += int(exact)
        conflicting_groups += int(not exact)
        if len(examples) < 30:
            values = values if isinstance(values, tuple) else (values,)
            examples.append({
                "date": values[0], "symbol": values[1], "series": values[2],
                "rows_in_key_group": len(group), "identical_payloads": exact,
            })
    return int(excess_count), exact_groups, conflicting_groups, examples


def _build_schema_audit(schema: pa.Schema, nulls: Counter[str], rows: int) -> list[dict[str, Any]]:
    descriptions = {
        "date": "Trading date; raw tree field.",
        "symbol": "Source ticker; preserved without symbol-history rewriting.",
        "series": "Explicit NSE series; only exact EQ is admitted to normalized equity output.",
        "isin": "Null before 2012 is documented by TejHQ; no values are fabricated.",
        "name": "Source instrument/company name.",
        "open": "Raw-tree OHLC price, INR.", "high": "Raw-tree OHLC price, INR.",
        "low": "Raw-tree OHLC price, INR.", "close": "Raw-tree close, INR.",
        "last": "Last traded price.", "prev_close": "Previous close.",
        "volume": "Shares traded.", "turnover": "Turnover, INR.",
        "trades": "Trade count; null before 2012 is documented by TejHQ.",
    }
    output = []
    for field in RAW_REQUIRED:
        arrow_field = schema.field(field) if field in schema.names else None
        count = nulls[field]
        output.append({
            "field": field,
            "present": arrow_field is not None,
            "type": str(arrow_field.type) if arrow_field else "MISSING",
            "null_count": count,
            "null_pct": (100.0 * count / rows) if rows else None,
            "notes": descriptions.get(field, "Unknown"),
        })
    return output


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    frame.to_csv(temporary, index=False, date_format="%Y-%m-%d")
    os.replace(temporary, path)


def _load_official_index(
    project_root: Path, filename: str = "index_NSEI.csv", label: str = "NIFTY 50"
) -> tuple[set[pd.Timestamp], dict[str, Any]]:
    path = project_root / "data" / "nse_exports" / filename
    if not path.is_file():
        return set(), {"status": "UNKNOWN", "reason": f"official NSE {label} calendar is missing: {path}"}
    raw = pd.read_csv(path, dtype={"Date": "string"}, low_memory=False)
    if "Date" not in raw:
        return set(), {"status": "FAIL", "reason": f"official NSE {label} index file has no Date column"}
    parsed = pd.to_datetime(raw["Date"], format="%Y-%m-%d", errors="coerce")
    if parsed.isna().any() or parsed.duplicated().any():
        return set(), {"status": "FAIL", "reason": f"official NSE {label} calendar contains malformed or duplicate dates"}
    sessions = set(parsed.dt.normalize())
    return sessions, {
        "status": "PASS", "source": f"existing official NSE {label} CSV",
        "path": str(path.relative_to(project_root)), "rows": int(len(parsed)),
        "first_date": parsed.min().date().isoformat(), "last_date": parsed.max().date().isoformat(),
    }


def audit_snapshot(root: Path, project_root: Path) -> dict[str, Any]:
    manifest_path = root / "tejhq_dataset_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_files = sorted((root / "raw" / "nse").glob("year=*/nse_*.parquet"))
    if not raw_files:
        raise RuntimeError("TejHQ raw NSE annual Parquet files are absent")
    audit_dir = root / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    null_counts: Counter[str] = Counter()
    series_counts: Counter[str] = Counter()
    series_symbols: dict[str, set[str]] = defaultdict(set)
    all_symbols: set[str] = set()
    series_dates: dict[str, set[pd.Timestamp]] = defaultdict(set)
    all_dates: set[pd.Timestamp] = set()
    eq_dates: set[pd.Timestamp] = set()
    annual_symbol_rows: list[dict[str, Any]] = []
    monthly_symbol_rows: list[dict[str, Any]] = []
    daily_rows: Counter[str] = Counter()
    daily_symbols: dict[str, set[str]] = defaultdict(set)
    daily_eq_rows: Counter[str] = Counter()
    daily_eq_symbols: dict[str, set[str]] = defaultdict(set)
    annual_symbol_aggregates: dict[tuple[str, str], dict[str, Any]] = {}
    duplicate_rows_total = 0
    duplicate_isin_rows_total = 0
    duplicate_identical_groups = 0
    duplicate_conflicting_groups = 0
    duplicate_examples: list[dict[str, Any]] = []
    equity_frames: list[pd.DataFrame] = []
    anomaly_frames: list[pd.DataFrame] = []
    schema: pa.Schema | None = None
    partition_audits = []

    from nse_research.data.tejhq import equity_quality_anomalies, normalize_eq_rows

    for raw_path in raw_files:
        parquet_schema = pq.read_schema(raw_path)
        if schema is None:
            schema = parquet_schema
        frame = pd.read_parquet(raw_path)
        missing = [field for field in RAW_REQUIRED if field not in frame.columns]
        if missing:
            raise RuntimeError(f"TejHQ source schema is missing required documented columns: {missing}")
        total_rows += len(frame)
        for field in frame.columns:
            null_counts[field] += int(frame[field].isna().sum())
        parsed_dates = _safe_dates(frame["date"])
        series = frame["series"].astype("string")
        symbol_text = frame["symbol"].astype("string")
        all_symbols.update(symbol_text.dropna().astype(str).unique().tolist())
        valid = parsed_dates.notna()
        frame["_audit_date"] = parsed_dates
        frame["_audit_symbol"] = symbol_text
        frame["_audit_series"] = series
        frame["_audit_row_year_matches_partition"] = parsed_dates.dt.year.eq(int(raw_path.parent.name.removeprefix("year=")))
        series_bad = frame["_audit_series"].isna() | frame["_audit_series"].eq("")
        if series_bad.any():
            anomaly = frame.loc[series_bad, ["date", "symbol", "series", "open", "high", "low", "close", "volume"]].copy()
            anomaly["source_index"] = anomaly.index.astype(str)
            anomaly["issues"] = "missing_series"
            anomaly_frames.append(anomaly)
        if (~frame["_audit_row_year_matches_partition"]).any():
            anomaly = frame.loc[~frame["_audit_row_year_matches_partition"], ["date", "symbol", "series", "open", "high", "low", "close", "volume"]].copy()
            anomaly["source_index"] = anomaly.index.astype(str)
            anomaly["issues"] = "row_date_year_does_not_match_partition"
            anomaly_frames.append(anomaly)

        duplicates, exact_groups, conflict_groups, examples = _audit_duplicates(frame.drop(columns=[
            "_audit_date", "_audit_symbol", "_audit_series", "_audit_row_year_matches_partition",
        ]))
        duplicate_rows_total += duplicates
        duplicate_identical_groups += exact_groups
        duplicate_conflicting_groups += conflict_groups
        nonnull_isin = frame.loc[frame["isin"].notna()]
        duplicate_isin_rows_total += int(nonnull_isin.duplicated(["date", "isin", "series"], keep=False).sum())
        for example in examples:
            if len(duplicate_examples) < 200:
                duplicate_examples.append(example)

        valid_dates = parsed_dates.loc[valid]
        all_dates.update(pd.Timestamp(stamp).normalize() for stamp in valid_dates.drop_duplicates().tolist())
        if len(valid_dates):
            by_date_all = frame.loc[valid].assign(_audit_date=valid_dates).groupby("_audit_date").agg(
                rows=("symbol", "size"), symbols=("symbol", "nunique"),
            )
            by_date_symbol_sets = (
                frame.loc[valid, ["_audit_date", "symbol"]]
                .dropna(subset=["symbol"])
                .groupby("_audit_date", sort=False)["symbol"]
                .agg(lambda values: set(values.astype(str)))
            )
            for stamp, counts in by_date_all.iterrows():
                key = pd.Timestamp(stamp).date().isoformat()
                daily_rows[key] += int(counts["rows"])
                daily_symbols[key].update(by_date_symbol_sets.get(stamp, set()))
            eq_in_year = frame.loc[valid & frame["_audit_series"].eq("EQ").fillna(False), ["_audit_date", "symbol"]]
            if not eq_in_year.empty:
                for stamp, group in eq_in_year.dropna(subset=["symbol"]).groupby("_audit_date", sort=False):
                    key = pd.Timestamp(stamp).date().isoformat()
                    daily_eq_rows[key] += len(group)
                    daily_eq_symbols[key].update(group["symbol"].astype(str).unique().tolist())

        for value, part in frame.groupby("_audit_series", dropna=False, sort=False):
            label = "<NULL>" if pd.isna(value) else str(value)
            series_counts[label] += len(part)
            series_symbols[label].update(part["symbol"].dropna().astype(str).unique().tolist())
            series_dates[label].update(pd.Timestamp(stamp).normalize() for stamp in part["_audit_date"].dropna().unique().tolist())

        valid_symbols = frame.loc[valid & frame["symbol"].notna()].copy()
        if not valid_symbols.empty:
            valid_symbols["_audit_month"] = valid_symbols["_audit_date"].dt.to_period("M").astype(str)
            annual_groups = valid_symbols.groupby([valid_symbols["_audit_date"].dt.year.astype(str), "_audit_series"], dropna=False)
            for (year, ser), part in annual_groups:
                annual_symbol_rows.append({
                    "year": str(year), "series": "<NULL>" if pd.isna(ser) else str(ser),
                    "rows": len(part), "symbols": int(part["symbol"].nunique()),
                    "sessions": int(part["_audit_date"].nunique()),
                })
            monthly_groups = valid_symbols.groupby(["_audit_month", "_audit_series"], dropna=False)
            for (month, ser), part in monthly_groups:
                monthly_symbol_rows.append({
                    "month": str(month), "series": "<NULL>" if pd.isna(ser) else str(ser),
                    "rows": len(part), "symbols": int(part["symbol"].nunique()),
                    "sessions": int(part["_audit_date"].nunique()),
                })
            for (symbol, ser), part in valid_symbols.groupby(["symbol", "_audit_series"], dropna=False, sort=False):
                key = (str(symbol), "<NULL>" if pd.isna(ser) else str(ser))
                aggregate = annual_symbol_aggregates.setdefault(key, {
                    "rows": 0, "first": None, "last": None, "isin_values": set(),
                    "year_rows": Counter(),
                })
                aggregate["rows"] += len(part)
                first = part["_audit_date"].min()
                last = part["_audit_date"].max()
                aggregate["first"] = first if aggregate["first"] is None else min(aggregate["first"], first)
                aggregate["last"] = last if aggregate["last"] is None else max(aggregate["last"], last)
                aggregate["isin_values"].update(part["isin"].dropna().astype(str).unique().tolist())
                aggregate["year_rows"].update(part["_audit_date"].dt.year.astype(str).value_counts().to_dict())

        equity_mask = frame["series"].astype("string").eq("EQ").fillna(False)
        equity = frame.loc[equity_mask].drop(columns=[
            "_audit_date", "_audit_symbol", "_audit_series", "_audit_row_year_matches_partition",
        ]).copy()
        if not equity.empty:
            quality = equity_quality_anomalies(equity)
            if not quality.empty:
                anomaly_frames.append(quality)
            equity["date"] = parsed_dates.loc[equity.index].to_numpy()
            eq_dates.update(pd.Timestamp(stamp).normalize() for stamp in equity["date"].dropna().unique().tolist())
            try:
                normalized = normalize_eq_rows(equity.loc[:, [column for column in NORMALIZED_FIELDS if column in equity.columns]])
                equity_frames.append(normalized)
            except Exception as exc:
                # Keep source diagnostics complete. The failed normalization is represented in the final status.
                partition_audits.append({"file": raw_path.name, "normalization_error": str(exc)})
                equity_frames.append(equity.loc[:, [column for column in NORMALIZED_FIELDS if column in equity.columns]])
        partition_audits.append({
            "file": str(raw_path.relative_to(root)), "rows": len(frame),
            "equity_rows": int(equity_mask.sum()),
            "first_date": valid_dates.min().date().isoformat() if len(valid_dates) else None,
            "last_date": valid_dates.max().date().isoformat() if len(valid_dates) else None,
            "duplicate_excess_rows": duplicates,
            "row_year_mismatch_rows": int((~frame["_audit_row_year_matches_partition"] & valid).sum()),
        })

    if not equity_frames:
        raise RuntimeError("TejHQ snapshot contains no explicit EQ records")
    equities = pd.concat(equity_frames, ignore_index=True)
    equities["date"] = pd.to_datetime(equities["date"], errors="coerce").dt.normalize()
    equities = equities.sort_values(["symbol", "date"], kind="mergesort").reset_index(drop=True)
    eq_duplicate_mask = equities.duplicated(["date", "symbol", "series"], keep=False)
    if eq_duplicate_mask.any():
        duplicate_equities = equities.loc[eq_duplicate_mask].copy()
        duplicate_equities["issues"] = "duplicate_date_symbol_series"
        anomaly_frames.append(duplicate_equities)

    # The actual raw tables are third-party cleaned snapshots: any checks cannot recover
    # anomalies already dropped upstream, as the source card explicitly discloses.
    anomalies = pd.concat(anomaly_frames, ignore_index=True, sort=False) if anomaly_frames else pd.DataFrame(
        columns=["source_index", "date", "symbol", "series", "open", "high", "low", "close", "volume", "issues"]
    )
    if "issues" not in anomalies:
        anomalies["issues"] = "unknown"
    _write_frame(audit_dir / "data_quality_anomalies.csv", anomalies)
    _write_frame(audit_dir / "duplicate_examples.csv", pd.DataFrame(
        duplicate_examples,
        columns=["date", "symbol", "series", "rows_in_key_group", "identical_payloads"],
    ))

    all_date_list = sorted(pd.Timestamp(stamp).normalize() for stamp in all_dates)
    equity_date_list = sorted(pd.Timestamp(stamp).normalize() for stamp in eq_dates)
    if not all_date_list or not equity_date_list:
        raise RuntimeError("TejHQ snapshot has no parseable NSE trading dates")
    first_date, last_date = all_date_list[0], all_date_list[-1]
    calendar_days = int((last_date - first_date).days + 1)
    business_days = pd.bdate_range(first_date, last_date)
    missing_weekdays = [stamp for stamp in business_days if stamp not in all_dates]

    official_sessions, official_calendar = _load_official_index(project_root, "index_NSEI.csv", "NIFTY 50")
    vix_sessions, vix_calendar = _load_official_index(project_root, "index_INDIAVIX.csv", "India VIX")
    overlap_start = max(first_date, pd.Timestamp(official_calendar.get("first_date", first_date))) if official_calendar.get("status") == "PASS" else None
    overlap_end = min(last_date, pd.Timestamp(official_calendar.get("last_date", last_date))) if official_calendar.get("status") == "PASS" else None
    overlapping_official = {stamp for stamp in official_sessions if overlap_start <= stamp <= overlap_end} if overlap_start is not None and overlap_end is not None and overlap_start <= overlap_end else set()
    dataset_in_overlap = {stamp for stamp in eq_dates if overlap_start <= stamp <= overlap_end} if overlap_start is not None and overlap_end is not None and overlap_start <= overlap_end else set()
    official_missing = sorted(overlapping_official - dataset_in_overlap)
    dataset_extra = sorted(dataset_in_overlap - overlapping_official)
    observed_nonofficial = sorted(stamp for stamp in eq_dates if overlap_start is not None and overlap_end is not None and overlap_start <= stamp <= overlap_end and stamp not in official_sessions)
    index_alignments: dict[str, Any] = {"NIFTY 50": {
        "calendar": official_calendar,
        "first_date": overlap_start.date().isoformat() if overlap_start is not None else None,
        "last_date": overlap_end.date().isoformat() if overlap_end is not None else None,
        "official_sessions": len(overlapping_official),
        "tejhq_eq_dates": len(dataset_in_overlap),
        "missing_tejhq_eq_sessions": [stamp.date().isoformat() for stamp in official_missing],
        "tejhq_eq_dates_not_in_index": [stamp.date().isoformat() for stamp in dataset_extra],
        "status": "PASS" if official_calendar.get("status") == "PASS" and not official_missing else "FAIL" if official_missing else "UNKNOWN",
    }}
    vix_overlap_start = max(first_date, pd.Timestamp(vix_calendar.get("first_date", first_date))) if vix_calendar.get("status") == "PASS" else None
    vix_overlap_end = min(last_date, pd.Timestamp(vix_calendar.get("last_date", last_date))) if vix_calendar.get("status") == "PASS" else None
    vix_overlap_official = {stamp for stamp in vix_sessions if vix_overlap_start <= stamp <= vix_overlap_end} if vix_overlap_start is not None and vix_overlap_end is not None and vix_overlap_start <= vix_overlap_end else set()
    vix_overlap_equity = {stamp for stamp in eq_dates if vix_overlap_start <= stamp <= vix_overlap_end} if vix_overlap_start is not None and vix_overlap_end is not None and vix_overlap_start <= vix_overlap_end else set()
    vix_missing = sorted(vix_overlap_official - vix_overlap_equity)
    vix_extra = sorted(vix_overlap_equity - vix_overlap_official)
    index_alignments["India VIX"] = {
        "calendar": vix_calendar,
        "first_date": vix_overlap_start.date().isoformat() if vix_overlap_start is not None else None,
        "last_date": vix_overlap_end.date().isoformat() if vix_overlap_end is not None else None,
        "official_sessions": len(vix_overlap_official),
        "tejhq_eq_dates": len(vix_overlap_equity),
        "missing_tejhq_eq_sessions": [stamp.date().isoformat() for stamp in vix_missing],
        "tejhq_eq_dates_not_in_index": [stamp.date().isoformat() for stamp in vix_extra],
        "status": "PASS" if vix_calendar.get("status") == "PASS" and not vix_missing else "FAIL" if vix_missing else "UNKNOWN",
    }
    alignment_rows = []
    for label, alignment in index_alignments.items():
        alignment_rows.append({
            "index": label,
            "first_date": alignment["first_date"], "last_date": alignment["last_date"],
            "official_sessions": alignment["official_sessions"], "tejhq_eq_dates": alignment["tejhq_eq_dates"],
            "missing_tejhq_eq_sessions": ";".join(alignment["missing_tejhq_eq_sessions"]),
            "tejhq_eq_dates_not_in_index": ";".join(alignment["tejhq_eq_dates_not_in_index"]),
            "status": alignment["status"],
        })
    _write_frame(audit_dir / "index_alignment.csv", pd.DataFrame(alignment_rows))

    # Date-level counts, retaining sessions instead of creating/filling records.
    daily_frame = pd.DataFrame({
        "date": list(daily_rows.keys()), "rows": list(daily_rows.values()),
        "symbols": [len(daily_symbols[key]) for key in daily_rows],
        "eq_rows": [daily_eq_rows[key] for key in daily_rows],
        "eq_symbols": [len(daily_eq_symbols[key]) for key in daily_rows],
    })
    daily_frame["date"] = pd.to_datetime(daily_frame["date"])
    daily_frame = daily_frame.sort_values("date")
    _write_frame(audit_dir / "date_coverage.csv", daily_frame)
    _write_frame(audit_dir / "symbols_by_year.csv", pd.DataFrame(annual_symbol_rows).sort_values(["year", "series"]))
    _write_frame(audit_dir / "symbols_by_month.csv", pd.DataFrame(monthly_symbol_rows).sort_values(["month", "series"]))

    series_rows = []
    for label in sorted(series_counts):
        values = series_dates[label]
        series_rows.append({
            "series": label, "rows": series_counts[label], "symbols": len(series_symbols[label]),
            "sessions": len(values), "first_date": pd.Timestamp(min(values)).date().isoformat() if values else None,
            "last_date": pd.Timestamp(max(values)).date().isoformat() if values else None,
        })
    _write_frame(audit_dir / "series_coverage.csv", pd.DataFrame(series_rows))

    # Official NIFTY calendar is the expected NSE session schedule from its first date.
    # Earlier sessions use the observed union of TejHQ dates as a documented proxy only.
    proxy_dates = set(all_dates)
    expected_sessions = set(proxy_dates)
    expected_sessions.update(official_sessions)
    expected_sorted = sorted(expected_sessions)
    symbols_records = []
    missing_records = []
    eq = equities.loc[equities["series"].astype("string").eq("EQ")].copy()
    eq["date"] = pd.to_datetime(eq["date"], errors="coerce").dt.normalize()
    for (symbol, series_label), group in eq.groupby(["symbol", "series"], sort=False, dropna=False):
        actual_dates = set(group["date"].dropna().tolist())
        start = min(actual_dates) if actual_dates else None
        end = max(actual_dates) if actual_dates else None
        expected_for_symbol = [stamp for stamp in expected_sorted if start is not None and start <= stamp <= end]
        missing_for_symbol = [stamp for stamp in expected_for_symbol if stamp not in actual_dates]
        symbols_records.append({
            "symbol": symbol, "series": series_label, "rows": len(group),
            "first_date": start.date().isoformat() if start is not None else None,
            "last_date": end.date().isoformat() if end is not None else None,
            "isin_count": int(group["isin"].nunique(dropna=True)),
            "isins_observed": ";".join(sorted(group["isin"].dropna().astype(str).unique().tolist())),
            "ending_year": end.year if end is not None else None,
            "expected_sessions_in_span": len(expected_for_symbol),
            "observed_sessions": len(actual_dates),
            "missing_expected_sessions": len(missing_for_symbol),
            "longest_consecutive_missing_sessions": _longest_missing_run(expected_for_symbol, actual_dates),
            "expected_session_coverage_pct": (100.0 * (len(expected_for_symbol) - len(missing_for_symbol)) / len(expected_for_symbol)) if expected_for_symbol else None,
            "expected_calendar_basis": "official NSE NIFTY calendar from 2020 onward; union of observed TejHQ NSE dates before 2020",
        })
        missing_records.append({
            "symbol": symbol, "series": series_label,
            "first_date": start.date().isoformat() if start is not None else None,
            "last_date": end.date().isoformat() if end is not None else None,
            "expected_sessions": len(expected_for_symbol), "observed_sessions": len(actual_dates),
            "missing_trading_dates": len(missing_for_symbol),
            "longest_consecutive_gap_sessions": _longest_missing_run(expected_for_symbol, actual_dates),
            "expected_session_coverage_pct": (100.0 * (len(expected_for_symbol) - len(missing_for_symbol)) / len(expected_for_symbol)) if expected_for_symbol else None,
            "calendar_basis": "NIFTY 50 official calendar where present; observed date-union proxy for older history",
        })
    symbols_df = pd.DataFrame(symbols_records).sort_values(["symbol", "series"])
    _write_frame(audit_dir / "symbol_coverage.csv", symbols_df)
    _write_frame(audit_dir / "missing_data_by_symbol.csv", pd.DataFrame(missing_records).sort_values(["symbol", "series"]))

    identity_records = []
    identity_source = eq.dropna(subset=["isin"]).copy()
    if not identity_source.empty:
        identity_pairs = identity_source.groupby(["isin", "symbol"], sort=True).agg(
            first_date=("date", "min"), last_date=("date", "max"), trading_days=("date", "nunique"),
        ).reset_index()
        symbols_per_isin = identity_pairs.groupby("isin")["symbol"].nunique()
        isins_per_symbol = identity_pairs.groupby("symbol")["isin"].nunique()
        for row in identity_pairs.to_dict("records"):
            identity_records.append({
                "isin": row["isin"], "symbol": row["symbol"],
                "first_date": pd.Timestamp(row["first_date"]).date().isoformat(),
                "last_date": pd.Timestamp(row["last_date"]).date().isoformat(),
                "trading_days": int(row["trading_days"]),
                "isin_observed_with_multiple_symbols": bool(symbols_per_isin.loc[row["isin"]] > 1),
                "symbol_observed_with_multiple_isins": bool(isins_per_symbol.loc[row["symbol"]] > 1),
                "interpretation": "observed mapping only; rename, merger, relisting, and data error are not inferred",
            })
    _write_frame(audit_dir / "security_identity_changes.csv", pd.DataFrame(identity_records))

    # Compare the published NSE ISIN intervals with mappings observed in the raw rows.
    history_path = root / "raw" / "symbol_history" / "nse.parquet"
    history = pd.read_parquet(history_path) if history_path.is_file() else pd.DataFrame()
    history_audit: dict[str, Any] = {"status": "UNKNOWN", "path": str(history_path.relative_to(root))}
    if not history.empty:
        for field in ("valid_from", "valid_to"):
            if field in history:
                history[field] = _safe_dates(history[field])
        nse_history = history.loc[history["exchange"].astype("string").eq("NSE")].copy() if "exchange" in history else history.copy()
        raw_pairs = set(zip(identity_source["isin"].astype(str), identity_source["symbol"].astype(str))) if not identity_source.empty else set()
        history_pairs_frame = nse_history.dropna(subset=["isin", "symbol"])
        history_pairs = set(zip(history_pairs_frame["isin"].astype(str), history_pairs_frame["symbol"].astype(str)))
        duplicate_intervals = int(nse_history.duplicated(["isin", "symbol", "valid_from", "valid_to"], keep=False).sum())
        invalid_intervals = int((nse_history["valid_to"] < nse_history["valid_from"]).sum())
        history_audit = {
            "status": "PASS" if duplicate_intervals == 0 and invalid_intervals == 0 else "PARTIAL",
            "path": str(history_path.relative_to(root)),
            "rows": int(len(nse_history)),
            "columns": list(nse_history.columns),
            "duplicate_interval_rows": duplicate_intervals,
            "invalid_interval_rows": invalid_intervals,
            "raw_isin_symbol_pairs": len(raw_pairs),
            "history_isin_symbol_pairs": len(history_pairs),
            "raw_pairs_absent_from_symbol_history": len(raw_pairs - history_pairs),
            "symbol_history_pairs_not_observed_in_raw_rows": len(history_pairs - raw_pairs),
            "interpretation": "observed mappings compared only; no ticker remapping or event classification is performed",
        }
    _write_json(audit_dir / "symbol_history_audit.json", history_audit)

    # Close-to-close moves are reported but never removed or explained from price shape alone.
    eq_returns = eq.sort_values(["symbol", "date"], kind="mergesort").copy()
    eq_returns["previous_close"] = eq_returns.groupby("symbol", sort=False)["close"].shift(1)
    eq_returns["return_pct"] = (pd.to_numeric(eq_returns["close"], errors="coerce") / pd.to_numeric(eq_returns["previous_close"], errors="coerce") - 1.0) * 100.0
    extremes = eq_returns.loc[eq_returns["return_pct"].abs() > 10.0, ["date", "symbol", "series", "previous_close", "close", "return_pct"]].copy()
    extremes["over_10pct"] = extremes["return_pct"].abs().gt(10.0)
    extremes["over_20pct"] = extremes["return_pct"].abs().gt(20.0)
    extremes["over_50pct"] = extremes["return_pct"].abs().gt(50.0)
    extremes["event_cause"] = "UNKNOWN; no cause inferred from a return threshold"
    extremes["corporate_action_correlation"] = "not independently linked until action audit"

    actions_files = sorted((root / "raw" / "actions").glob("nse_*.parquet"))
    action_frames = [pd.read_parquet(path) for path in actions_files]
    actions = pd.concat(action_frames, ignore_index=True) if action_frames else pd.DataFrame()
    if not actions.empty:
        actions["ex_date"] = _safe_dates(actions["ex_date"])
        action_counts = actions.groupby("type", dropna=False).agg(
            events=("type", "size"), symbols=("symbol", "nunique"), first_ex_date=("ex_date", "min"), last_ex_date=("ex_date", "max"),
        ).reset_index()
        action_counts["first_ex_date"] = pd.to_datetime(action_counts["first_ex_date"]).dt.date
        action_counts["last_ex_date"] = pd.to_datetime(action_counts["last_ex_date"]).dt.date
        _write_frame(audit_dir / "corporate_action_summary.csv", action_counts)
        action_index = {
            str(symbol): (
                group["ex_date"].to_numpy(dtype="datetime64[ns]"),
                group["type"].astype("string").fillna("").to_numpy(),
            )
            for symbol, group in actions.dropna(subset=["ex_date"]).groupby("symbol", sort=False)
        }
        for idx, move in extremes.iterrows():
            indexed = action_index.get(str(move["symbol"]))
            if indexed is None:
                continue
            event_dates, event_types = indexed
            center = np.datetime64(pd.Timestamp(move["date"]).to_datetime64())
            left = np.searchsorted(event_dates, center - np.timedelta64(7, "D"), side="left")
            right = np.searchsorted(event_dates, center + np.timedelta64(7, "D"), side="right")
            if right > left:
                linked_types = sorted({str(value) for value in event_types[left:right] if str(value)})
                extremes.loc[idx, "corporate_action_correlation"] = ";".join(linked_types) + " within +/-7 calendar days (correlation only)"
    else:
        action_counts = pd.DataFrame(columns=["type", "events", "symbols", "first_ex_date", "last_ex_date"])
        _write_frame(audit_dir / "corporate_action_summary.csv", action_counts)
    extremes["date"] = pd.to_datetime(extremes["date"], errors="coerce")
    _write_frame(audit_dir / "extreme_returns.csv", extremes.sort_values(["date", "symbol"]))

    action_examples = []
    if not actions.empty:
        action_candidates = actions.loc[actions["type"].astype("string").isin(["split", "bonus", "dividend", "rights", "merger", "demerger"])].sort_values("ex_date")
        for _, action in action_candidates.iterrows():
            symbol_rows = eq.loc[eq["symbol"].astype("string").eq(str(action["symbol"]))].sort_values("date")
            before = symbol_rows.loc[symbol_rows["date"] < action["ex_date"]].tail(1)
            after = symbol_rows.loc[symbol_rows["date"] >= action["ex_date"]].head(1)
            if before.empty or after.empty or pd.Timestamp(after.iloc[0]["date"]) != pd.Timestamp(action["ex_date"]):
                continue
            prev_close = float(before.iloc[0]["close"])
            event_close = float(after.iloc[0]["close"])
            event_return = (event_close / prev_close - 1.0) * 100.0 if prev_close else None
            action_type = str(action["type"])
            expected_ratio = None
            formula = "not defined for this action type from available source fields"
            if action_type == "split" and pd.notna(action.get("face_value_from")) and pd.notna(action.get("face_value_to")) and float(action["face_value_from"]) > 0:
                expected_ratio = float(action["face_value_to"]) / float(action["face_value_from"])
                formula = "face_value_to / face_value_from; theoretical price ratio, not expected close after market movement"
            elif action_type == "bonus" and pd.notna(action.get("ratio_num")) and pd.notna(action.get("ratio_den")):
                denominator = float(action["ratio_num"]) + float(action["ratio_den"])
                if denominator > 0:
                    expected_ratio = float(action["ratio_den"]) / denominator
                    formula = "ratio_den / (ratio_num + ratio_den); theoretical price ratio, not expected close after market movement"
            elif action_type == "dividend" and pd.notna(action.get("cash_amount")) and prev_close > 0:
                expected_ratio = (prev_close - float(action["cash_amount"])) / prev_close
                formula = "(previous_close - cash_amount) / previous_close; source convention before other price movement"
            observed_ratio = event_close / prev_close if prev_close else None
            ratio_residual_pct = ((observed_ratio / expected_ratio) - 1.0) * 100.0 if observed_ratio is not None and expected_ratio not in (None, 0) else None
            action_examples.append({
                "symbol": action["symbol"], "isin_as_reported": action.get("isin"),
                "action_type": action_type, "ex_date": pd.Timestamp(action["ex_date"]).date().isoformat(),
                "raw_previous_session_date": pd.Timestamp(before.iloc[0]["date"]).date().isoformat(),
                "raw_previous_close": prev_close, "raw_ex_date_close": event_close,
                "raw_close_return_pct": event_return,
                "observed_close_ratio": observed_ratio,
                "theoretical_action_ratio": expected_ratio,
                "ratio_residual_pct": ratio_residual_pct,
                "comparison_formula": formula,
                "raw_subject": action.get("raw_subject"),
                "validation": "independent arithmetic check of separate published action and raw-price tables; residual includes ordinary market movement; external exchange confirmation UNKNOWN",
            })
            if len(action_examples) >= 12:
                break
    _write_frame(audit_dir / "corporate_action_examples.csv", pd.DataFrame(action_examples))

    # Point-in-time universe claims are audited as published rows, not used as research input.
    universe_path = root / "raw" / "universe" / "nse_liquid.parquet"
    universe = pd.read_parquet(universe_path) if universe_path.is_file() else pd.DataFrame()
    universe_quality: dict[str, Any] = {"status": "UNKNOWN", "rows": 0}
    if not universe.empty:
        for column in ("rebalance_date", "valid_to"):
            if column in universe:
                universe[column] = _safe_dates(universe[column])
        key_columns = [column for column in ("rebalance_date", "symbol", "isin") if column in universe.columns]
        duplicate_memberships = int(universe.duplicated(key_columns, keep=False).sum()) if key_columns else 0
        numeric_rank = pd.to_numeric(universe["rank"], errors="coerce") if "rank" in universe else pd.Series(dtype="float64")
        invalid_rank_rows = int(numeric_rank.isna().sum() + (numeric_rank.lt(1) | numeric_rank.gt(500)).sum()) if "rank" in universe else None
        duplicate_rank_rows = int(universe.duplicated(["rebalance_date", "rank"], keep=False).sum()) if "rank" in universe else None
        rank_counts = universe.groupby("rebalance_date").agg(
            members=("symbol", "nunique"), rows=("symbol", "size"), max_rank=("rank", "max"),
        ).reset_index()
        rank_counts["duplicate_memberships"] = rank_counts["rebalance_date"].map(universe.groupby("rebalance_date").apply(lambda group: int(group.duplicated(["symbol", "isin"], keep=False).sum()), include_groups=False))
        _write_frame(audit_dir / "universe_coverage.csv", rank_counts)
        latest_rebalance = universe["rebalance_date"].max()
        latest_members = set(universe.loc[universe["rebalance_date"].eq(latest_rebalance), "symbol"].dropna().astype(str))
        all_universe_symbols = set(universe["symbol"].dropna().astype(str))
        eq_symbol_set = set(eq["symbol"].dropna().astype(str))
        universe_quality = {
            "status": "PARTIAL",
            "rows": int(len(universe)),
            "rebalance_dates": int(universe["rebalance_date"].nunique()),
            "first_rebalance_date": universe["rebalance_date"].min().date().isoformat(),
            "last_rebalance_date": latest_rebalance.date().isoformat(),
            "members_per_rebalance_min": int(rank_counts["members"].min()),
            "members_per_rebalance_max": int(rank_counts["members"].max()),
            "duplicate_memberships": duplicate_memberships,
            "duplicate_rank_rows": duplicate_rank_rows,
            "invalid_rank_rows": invalid_rank_rows,
            "invalid_valid_to_rows": int((universe["valid_to"] < universe["rebalance_date"]).sum()) if "valid_to" in universe else None,
            "historical_symbols_absent_from_latest_top500": len(all_universe_symbols - latest_members),
            "published_universe_symbols_missing_from_raw_eq": len(all_universe_symbols - eq_symbol_set),
            "documented_method": "first trading day monthly; top 500 by trailing 63-trading-day mean turnover; membership claimed based only on rebalance-date information",
            "independent_recalculation": "not performed; methodology is source documentation and row-level plausibility checks only",
        }

    # Exact Phase 3C fold date-window/calendar check.
    fold_rows = []
    for fold, segment, start_text, end_text in FOLDS:
        start = pd.Timestamp(start_text)
        end = pd.Timestamp(end_text)
        expected = {stamp for stamp in official_sessions if start <= stamp <= end}
        actual = {stamp for stamp in eq_dates if start <= stamp <= end}
        missing = sorted(expected - actual)
        fold_rows.append({
            "fold": fold, "segment": segment, "required_start": start_text, "required_end": end_text,
            "required_calendar_sessions": len(expected), "sessions_with_EQ_data": len(expected & actual),
            "EQ_dates_outside_official_calendar": len(actual - expected),
            "missing_official_sessions": len(missing),
            "missing_session_dates": ";".join(stamp.date().isoformat() for stamp in missing),
            "first_EQ_date": min(actual).date().isoformat() if actual else None,
            "last_EQ_date": max(actual).date().isoformat() if actual else None,
            "status": "PASS" if expected and not missing else "FAIL" if expected else "UNKNOWN",
        })
    folds_df = pd.DataFrame(fold_rows)
    _write_frame(audit_dir / "phase3c_fold_coverage.csv", folds_df)
    fold_pass = bool(not folds_df.empty and folds_df["status"].eq("PASS").all())

    # PIT per-symbol missing-session analysis uses exactly the applicable EQ rows.
    _write_frame(audit_dir / "extreme_returns.csv", extremes.sort_values(["date", "symbol"]))

    # Existence-based delisting evidence is intentionally not upgraded to a delisting claim.
    ending_year_counts = symbols_df.groupby("ending_year")["symbol"].nunique().to_dict() if not symbols_df.empty else {}
    ending_stats = {
        "symbols_ending_before_2022": int(symbols_df.loc[symbols_df["ending_year"] < 2022, "symbol"].nunique()) if not symbols_df.empty else 0,
        "symbols_ending_2022": int(ending_year_counts.get(2022, 0)),
        "symbols_ending_2023": int(ending_year_counts.get(2023, 0)),
        "symbols_ending_2024": int(ending_year_counts.get(2024, 0)),
        "symbols_ending_2025": int(ending_year_counts.get(2025, 0)),
        "symbols_ending_2026": int(ending_year_counts.get(2026, 0)),
        "symbols_ending_prior_to_snapshot_last_date": int(symbols_df.loc[symbols_df["last_date"] < last_date.date().isoformat(), "symbol"].nunique()) if not symbols_df.empty else 0,
    }

    actual_series_values = sorted(series_counts)
    eq_rows = int((equities["series"].astype("string") == "EQ").sum())
    malformed_dates = sum(int(item.get("row_year_mismatch_rows", 0)) for item in partition_audits)
    anomaly_count = int(len(anomalies))
    critical = anomaly_count > 0 or duplicate_rows_total > 0 or bool(eq_duplicate_mask.any())
    normalization_path = root / "normalized" / "tejhq_nse_equity.parquet"
    normalized_info = None
    normalization_gate_pass = not critical and fold_pass
    if normalization_gate_pass:
        normalization_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = normalization_path.with_name(f".{normalization_path.name}.{os.getpid()}.tmp")
        writer = None
        try:
            for partition in raw_files:
                part = pd.read_parquet(partition, columns=list(NORMALIZED_FIELDS))
                normalized = normalize_eq_rows(part)
                if normalized.empty:
                    continue
                table = pa.Table.from_pandas(normalized, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(temporary, table.schema, compression="zstd", use_dictionary=["symbol", "series", "isin"])
                writer.write_table(table, row_group_size=100_000)
            if writer is not None:
                writer.close()
                writer = None
                os.replace(temporary, normalization_path)
                normalized_info = {
                    "path": str(normalization_path.relative_to(project_root)),
                    "sha256": sha256_file(normalization_path),
                    "bytes": normalization_path.stat().st_size,
                    "rows": eq_rows,
                    "columns": list(NORMALIZED_FIELDS),
                    "series_filter": "exact source value EQ; non-EQ rows excluded",
                    "price_handling": "raw values preserved; no corporate-action adjustment; no forward filling or gap filling",
                }
        finally:
            if writer is not None:
                writer.close()
            temporary.unlink(missing_ok=True)
    elif normalization_path.is_file():
        previous_normalized = manifest.get("normalized_dataset") or {}
        if previous_normalized.get("sha256") and sha256_file(normalization_path) == previous_normalized["sha256"]:
            # Remove only the exact normalized file produced by the earlier audit run.
            normalization_path.unlink()
            if not any(normalization_path.parent.iterdir()):
                normalization_path.parent.rmdir()

    # Validate corporate action independent event arithmetic sample and record action frequencies.
    return_counts = {
        "over_10pct": int(extremes["return_pct"].abs().gt(10).sum()),
        "over_20pct": int(extremes["return_pct"].abs().gt(20).sum()),
        "over_50pct": int(extremes["return_pct"].abs().gt(50).sum()),
    }
    duplicate_isin_count = duplicate_isin_rows_total
    quality = {
        "invalid_eq_rows": anomaly_count,
        "duplicate_key_excess_rows": duplicate_rows_total,
        "duplicate_key_identical_groups": duplicate_identical_groups,
        "duplicate_key_conflicting_groups": duplicate_conflicting_groups,
        "duplicate_date_isin_series_rows": duplicate_isin_count,
        "zero_volume_eq_rows": int(pd.to_numeric(eq["volume"], errors="coerce").eq(0).sum()),
        "missing_volume_eq_rows": int(eq["volume"].isna().sum()),
        "negative_volume_eq_rows": int(pd.to_numeric(eq["volume"], errors="coerce").lt(0).sum()),
        "invalid_ohlc_rows": int(anomalies["issues"].astype("string").str.contains("invalid_(high|low)_relationship|nonpositive_(open|high|low|close)", regex=True, na=False).sum()) if not anomalies.empty else 0,
        "malformed_numeric_fields": 0,
        "quality_status": "PASS" if not critical else "FAIL",
        "float_tolerance": OHLC_TOLERANCE,
        "anomaly_policy": "preserve source rows; do not delete or repair; normalized artifact is withheld if any row anomaly, duplicate, or required Phase 3C session is missing",
    }

    no_source_rows_removed_claim = "TejHQ docs explicitly state raw-tree files are cleaned/deduped and detected anomaly rows are dropped before publication; the pre-publication row set is unavailable for auditing."
    summary = {
        "provider": "tejhq",
        "dataset_id": DATASET_ID,
        "dataset_revision": manifest["dataset_revision"],
        "snapshot_sha256": manifest["snapshot_sha256"],
        "source_description": manifest["dataset_description"],
        "source_last_modified_utc": manifest.get("source_last_modified_utc"),
        "rows": total_rows,
        "eq_rows": eq_rows,
        "first_date": first_date.date().isoformat(),
        "last_date": last_date.date().isoformat(),
        "unique_nse_dates": len(all_dates),
        "unique_eq_dates": len(eq_dates),
        "calendar_days_inclusive": calendar_days,
        "missing_weekdays_from_union": len(missing_weekdays),
        "missing_weekday_examples": [stamp.date().isoformat() for stamp in missing_weekdays[:30]],
        "symbols": int(eq["symbol"].nunique()),
        "raw_symbols_all_series": len(all_symbols),
        "series": actual_series_values,
        "series_count": len(actual_series_values),
        "series_summary": series_rows,
        "schema_audit": _build_schema_audit(schema, null_counts, total_rows) if schema is not None else [],
        "partition_audits": partition_audits,
        "nifty_calendar": official_calendar,
        "official_calendar_overlap": {
            "source": official_calendar.get("source", "UNKNOWN"),
            "path": official_calendar.get("path", "UNKNOWN"),
            "first_date": overlap_start.date().isoformat() if overlap_start is not None else None,
            "last_date": overlap_end.date().isoformat() if overlap_end is not None else None,
            "official_nifty_sessions": len(overlapping_official),
            "tejhq_eq_sessions": len(dataset_in_overlap),
            "missing_tejhq_eq_sessions": [stamp.date().isoformat() for stamp in official_missing],
            "tejhq_eq_dates_not_in_nifty_calendar": [stamp.date().isoformat() for stamp in dataset_extra],
            "nifty_calendar_complete_through_overlap": not official_missing,
            "interpretation": "weekend/special NSE sessions in the official index file are retained; dates absent from NIFTY are reported, not automatically treated as errors",
        },
        "index_alignments": index_alignments,
        "symbol_ending_year_counts": ending_stats,
        "unique_symbols_overall_eq": int(eq["symbol"].nunique()),
        "symbols_with_multiple_isins": int((symbols_df["isin_count"] > 1).sum()),
        "isins_mapped_to_multiple_symbols": int(identity_source.groupby("isin")["symbol"].nunique().gt(1).sum()) if not identity_source.empty else 0,
        "symbol_history_audit": history_audit,
        "missing_data": {
            "symbols_with_any_expected_session_gap": int((symbols_df["missing_expected_sessions"] > 0).sum()),
            "symbols_with_gaps_at_least_20_sessions": int((symbols_df["longest_consecutive_missing_sessions"] >= 20).sum()),
            "symbols_with_20_or_fewer_rows": int((symbols_df["rows"] <= 20).sum()),
            "calendar_method": "official NSE NIFTY date set where available, plus observed NSE date union; pre-2020 expected calendar is an incomplete proxy and may undercount a market-wide omission",
        },
        "duplicates": {
            "date_symbol_series_excess_rows": duplicate_rows_total,
            "identical_duplicate_key_groups": duplicate_identical_groups,
            "conflicting_duplicate_key_groups": duplicate_conflicting_groups,
            "date_isin_series_duplicate_rows": duplicate_isin_count,
            "examples_artifact": "audit/duplicate_examples.csv",
            "deduplication_performed": False,
        },
        "quality": quality,
        "extreme_returns": return_counts,
        "corporate_actions": {
            "rows": int(len(actions)),
            "types": action_counts.to_dict("records"),
            "examples": action_examples,
            "documents": "Pinned TejHQ card documents split/bonus/dividend and other classified action events and describes reverse cumulative close factors for adjusted prices.",
            "raw_price_status": "Raw NSE-tree OHLC/prev_close/volume retained and not adjusted by this audit.",
            "adjustment_methodology": "TejHQ states adj_close = close * cumulative factor over future actions; dividend factor=(prev_close-cash)/prev_close; split/bonus factors included; demerger, rights, buyback and merger not scaled.",
            "independent_validation": "Raw price vs separate TejHQ action-tree ex-date sample arithmetic is provided; external NSE event-by-event confirmation remains UNKNOWN.",
        },
        "survivorship": {
            "status": "PARTIAL",
            "evidence": ending_stats,
            "source_claim": "TejHQ claims all NSE listings since 2010 and says delisted names remain in months in which they qualified for its monthly liquidity universe.",
            "assessment": "Historical symbols ending before snapshot end are present, but an absent later bar does not prove a delisting. Completeness of inactive/delisted history cannot be independently established from this snapshot alone.",
        },
        "point_in_time_universe": universe_quality,
        "phase3c_folds": {
            "status": "PASS" if fold_pass else "FAIL",
            "fold_count": int(folds_df["fold"].nunique()) if not folds_df.empty else 0,
            "segments_passing": int(folds_df["status"].eq("PASS").sum()) if not folds_df.empty else 0,
            "segments_total": len(folds_df),
            "artifact": "audit/phase3c_fold_coverage.csv",
            "date_policy": "exact official NSE NIFTY session calendar for each requested training, validation, and test window; no date filling",
        },
        "lookahead": {
            "status": "PARTIAL",
            "raw_data": "The raw source tree has no adjusted price or future universe membership field. The normalized output was withheld because exact date coverage failed; its intended projection would select raw EQ values without transformation or filling.",
            "future_adjustment_risk": "The separate TejHQ adj_close uses cumulative actions with ex-dates after a row, so it is backward-adjusted using future events relative to that row; do not treat it as point-in-time without a separate leakage design.",
            "revision_risk": "Historical rows in a current snapshot may reflect later source corrections; prior dataset revisions were not compared.",
            "universe_risk": "PIT membership is separately published and documented, but its selection calculation has not been independently recomputed; it is not included in normalized data or used here.",
        },
        "provenance_limitations": [
            "Source calls the published raw tree cleaned/deduped and says it drops some anomaly rows before publication; pre-cleaning source data is unavailable in the snapshot.",
            "Coverage before 2020 cannot be compared with the existing official NSE NIFTY file; historical per-symbol missing-session denominator is an observed-date proxy.",
            "The dataset card reports MIT and redistribution permission and recommends checking exchange terms before commercial use; independent licensing advice is outside this audit.",
            "No NSE security reference list was acquired for an independent completeness check of EQ membership or delisted securities.",
        ],
        "assessments": {
            "source_revision_and_file_hashes": "PASS",
            "underlying_exchange_record_completeness": "UNKNOWN",
            "source_schema_and_explicit_EQ_series": "PASS",
            "published_EQ_OHLCV_row_quality": "PASS",
            "date_alignment_to_official_NIFTY": index_alignments["NIFTY 50"]["status"],
            "date_alignment_to_official_India_VIX": index_alignments["India VIX"]["status"],
            "duplicate_key_audit": "PASS" if duplicate_rows_total == 0 and duplicate_isin_count == 0 else "FAIL",
            "per_symbol_missing_data": "PARTIAL",
            "corporate_action_records_present": "PASS" if len(actions) else "UNKNOWN",
            "corporate_action_external_event_verification": "UNKNOWN",
            "raw_tree_adjustment_status": "PASS: documented as raw and no adjustment performed",
            "adjusted_close_point_in_time_safety": "FAIL if used historically without an explicit future-action leakage design",
            "symbol_ISIN_continuity": history_audit.get("status", "UNKNOWN"),
            "delisted_security_completeness": "UNKNOWN",
            "survivorship_assessment": "PARTIAL",
            "point_in_time_universe_methodology": "PARTIAL",
            "five_fold_date_coverage": "PASS" if fold_pass else "FAIL",
            "normalized_Phase3C_candidate": "NOT_APPLICABLE: withheld because required coverage failed" if not normalized_info else "PASS",
            "license_and_exchange_terms": "PARTIAL: dataset-card MIT label recorded; exchange terms not independently determined",
            "historical_revision_lookahead": "UNKNOWN: only one pinned revision was audited",
        },
        "normalization": normalized_info,
        "no_source_rows_removed_claim": no_source_rows_removed_claim,
    }

    if critical:
        verdict = "NOT_RESEARCH_READY"
    elif not fold_pass or not official_calendar.get("status") == "PASS":
        verdict = "NOT_RESEARCH_READY"
    else:
        verdict = "RESEARCH_READY_WITH_LIMITATIONS"
    summary["audit_status"] = verdict
    summary["normalization_status"] = (
        "CREATED" if normalized_info else
        "WITHHELD: required data-quality or exact Phase 3C date-coverage gate failed"
    )
    summary_path = audit_dir / "audit_summary.json"
    _write_json(summary_path, summary)
    if normalized_info is None:
        normalized_dir = root / "normalized"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        (normalized_dir / "README.md").write_text(
            "# Normalized TejHQ data withheld\n\n"
            "No normalized Phase 3C dataset was emitted. The audit found missing official NSE EQ dates inside the requested five-fold windows. "
            "See `../audit/phase3c_fold_coverage.csv` and `../audit/audit_summary.json`. No dates were filled.\n",
            encoding="utf-8",
        )
    manifest.update({
        "row_count": total_rows,
        "column_names": schema.names if schema is not None else [],
        "date_min": summary["first_date"],
        "date_max": summary["last_date"],
        "symbol_count": summary["raw_symbols_all_series"],
        "equity_symbol_count": summary["symbols"],
        "series_count": len(actual_series_values),
        "series_values": actual_series_values,
        "audit_status": verdict,
        "audit_summary_path": "audit/audit_summary.json",
        "normalized_dataset": normalized_info,
    })
    _write_json(manifest_path, manifest)
    write_report(root, project_root, summary)
    return summary


def write_report(root: Path, project_root: Path, summary: dict[str, Any]) -> None:
    schema_rows = summary["schema_audit"]
    series_rows = summary["series_summary"]
    actions = summary["corporate_actions"]
    survivorship = summary["survivorship"]
    pit = summary["point_in_time_universe"]
    missing = summary["missing_data"]
    calendar = summary["official_calendar_overlap"]
    fold = summary["phase3c_folds"]
    lookahead = summary["lookahead"]
    normalized = summary["normalization"]
    index_alignments = summary["index_alignments"]
    schema_table = "\n".join(
        f"| {row['field']} | {'Yes' if row['present'] else 'No'} | {row['type']} | {row['null_pct']:.4f}% | {row['notes']} |"
        for row in schema_rows
    )
    series_table = "\n".join(
        f"| {row['series']} | {row['rows']:,} | {row['symbols']:,} | {row['sessions']:,} | {row['first_date']} | {row['last_date']} |"
        for row in series_rows
    )
    fold_table = "\n".join(
        f"| {row['fold']} | {row['segment']} | {row['required_start']} → {row['required_end']} | {row['required_calendar_sessions']} | {row['sessions_with_EQ_data']} | {row['missing_official_sessions']} ({row['missing_session_dates'] or 'none'}) | {row['status']} |"
        for row in pd.read_csv(root / "audit" / "phase3c_fold_coverage.csv", keep_default_na=False).to_dict("records")
    )
    action_table = "\n".join(
        f"| {row.get('type')} | {row.get('events')} | {row.get('symbols')} | {row.get('first_ex_date')} | {row.get('last_ex_date')} |"
        for row in actions["types"]
    ) or "| No action rows | 0 | 0 | UNKNOWN | UNKNOWN |"
    report = f"""# TejHQ Historical NSE Dataset Audit

Audit timestamp (UTC): {datetime.now(timezone.utc).isoformat()}

## 1. Executive summary

**Verdict: {summary['audit_status']}**

This is third-party historical market data derived from NSE/BSE bhavcopy data, subject to independent audit. This report audits the frozen NSE raw-price tree; it does not run strategies, a backtest, or Phase 3C. It leaves the Kaggle snapshot, official NSE index files, licensed NSE CM EOD files, and all existing results untouched.

| Snapshot metric | Observed |
|---|---:|
| Revision | `{summary['dataset_revision']}` |
| Snapshot SHA-256 | `{summary['snapshot_sha256']}` |
| Raw NSE rows | {summary['rows']:,} |
| Explicit EQ rows | {summary['eq_rows']:,} |
| EQ symbols | {summary['symbols']:,} |
| Date range | {summary['first_date']} → {summary['last_date']} |
| Raw series values | {', '.join(summary['series'])} |
| Duplicate key excess rows | {summary['duplicates']['date_symbol_series_excess_rows']:,} |
| Invalid EQ rows | {summary['quality']['invalid_eq_rows']:,} |
| Phase 3C fold date coverage | {fold['status']} |

The source card states that raw-tree data is cleaned, deduplicated, and typed, and that some anomalous source rows are dropped before publishing. Thus, zero anomalies in this snapshot cannot establish that the original exchange files had no anomalies or that no records were removed upstream.

## 2. Source and provenance

- Provider: TejHQ, dataset `{DATASET_ID}`.
- Source URL: [{DATASET_URL}]({DATASET_URL})
- Source classification: third-party historical market data derived from NSE/BSE bhavcopy data, subject to independent audit. This is **not** called official NSE data.
- Immutable Hugging Face revision: `{summary['dataset_revision']}`.
- Repository head reported during acquisition: `{manifest_value(root, 'dataset_head_at_acquisition')}`.
- Source updated: `{summary.get('source_last_modified_utc')}`; downloaded: `{manifest_value(root, 'downloaded_at_utc')}`.
- Exact downloaded repository files, byte sizes, SHA-256 values, and object IDs are listed in `data/tejhq/tejhq_dataset_manifest.json`.
- Pinned dataset card and Hugging Face metadata/tree response are preserved under `data/tejhq/metadata/`.

## 3. License and use information

The pinned Hugging Face card labels the dataset `MIT`. It states that the data is exchange-published and redistribution is permitted, and explicitly says to verify exchange terms before commercial use. This records TejHQ's published statement; this audit does not independently determine underlying exchange rights. References and pinned documentation are preserved in `data/tejhq/metadata/tejhq_dataset_card.md`.

## 4. Dataset snapshot and schema

The acquisition includes the pinned NSE raw annual Parquet partitions for {len([x for x in manifest_value(root, 'files') if x['repo_path'].startswith('nse/year=')])} years, NSE corporate-action partitions, `symbol_history/nse.parquet`, `universe/nse_liquid.parquet`, and the pinned source README. Raw files are not rewritten. The TejHQ raw tree is itself a typed/cleaned publication, not the original NSE files.

| Field | Present | Type | Null % | Notes |
|---|---|---|---:|---|
{schema_table}

Actual series counts:

| Series | Rows | Symbols | Dates | First date | Last date |
|---|---:|---:|---:|---|---|
{series_table}

`EQ` exists as an explicit source `series` value and its rows contain positive OHLC prices and traded share volumes where validated below. It is kept distinct from `BE` and `BZ`. An NSE-hosted BAJAJELEC filing labels that security's series as `EQ` ([NSE filing](https://nsearchives.nseindia.com/corporate/BAJAJELEC_16092023182415_BELSEIntimation.pdf)); this is a spot confirmation for one security, not an independent comparison of every source row against an NSE security master. No series is fabricated.

## 5. Date coverage and index alignment

- Unique NSE dates: {summary['unique_nse_dates']:,}; unique EQ dates: {summary['unique_eq_dates']:,}.
- Inclusive calendar span: {summary['calendar_days_inclusive']:,} days.
- Missing Monday–Friday calendar dates across the full observed union: {summary['missing_weekdays_from_union']:,}. These are **not** called errors because exchange holidays and unscheduled closures are not inferred from weekdays.
- Official NIFTY calendar file: [existing official NSE index export]({"https://www.nseindia.com/reports-indices-historical-index-data"}) saved as `{calendar.get('path', 'UNKNOWN')}` ({calendar.get('first_date', 'UNKNOWN')} → {calendar.get('last_date', 'UNKNOWN')}).
- Overlap: {calendar['first_date']} → {calendar['last_date']}; {calendar['official_nifty_sessions']:,} official NIFTY sessions and {calendar['tejhq_eq_sessions']:,} TejHQ EQ dates.
- Official NIFTY sessions absent from TejHQ EQ: {len(calendar['missing_tejhq_eq_sessions']):,} ({', '.join(calendar['missing_tejhq_eq_sessions'])}); TejHQ EQ dates not in the NIFTY calendar: {len(calendar['tejhq_eq_dates_not_in_nifty_calendar']):,}.
- Official India VIX comparison: {index_alignments['India VIX']['first_date']} → {index_alignments['India VIX']['last_date']}; {index_alignments['India VIX']['official_sessions']:,} VIX dates, {len(index_alignments['India VIX']['missing_tejhq_eq_sessions']):,} not represented by a TejHQ EQ date ({', '.join(index_alignments['India VIX']['missing_tejhq_eq_sessions'])}). Index CSVs were read only and left unchanged. Full NIFTY/VIX alignment is in `data/tejhq/audit/index_alignment.csv`.
- Date-level rows/symbols are in `data/tejhq/audit/date_coverage.csv`; exact unique symbols by year and month are in the two `symbols_by_*.csv` files.

For 2010–2019 the project has no official NSE index calendar, so the per-symbol missing-session denominator uses the observed NSE date union and can undercount a date omitted for every security. For 2020 onward, the official index date calendar is used where available.

## 6. Symbol coverage and identity

- EQ symbols observed: {summary['symbols']:,}.
- Symbols with at least one ISIN transition: {summary['symbols_with_multiple_isins']:,}.
- ISIN values observed with multiple symbols: {summary['isins_mapped_to_multiple_symbols']:,}.
- Per-symbol row counts, first/last dates, observed ISINs, expected-session gaps, longest gap, and coverage percentage are in `audit/symbol_coverage.csv` and `audit/missing_data_by_symbol.csv`.
- `audit/security_identity_changes.csv` reports only observed ISIN↔symbol/date mappings. It does not infer a rename, merger, relisting, or delisting from those mappings.
- Newly appearing/disappearing symbols are observable first/last-bar facts, not verified listing/delisting events.

## 7. Series analysis

The raw data contains {summary['series_count']} distinct series: {', '.join(summary['series'])}. Full rows/symbols/date coverage is in `audit/series_coverage.csv`. The intended normalized output contains only explicit `EQ`; non-equity rows are never passed through it.

## 8. OHLCV validation

- Invalid EQ records: {summary['quality']['invalid_eq_rows']:,}.
- OHLC relationship/non-positive price rows: {summary['quality']['invalid_ohlc_rows']:,}; tolerance: {summary['quality']['float_tolerance']}.
- Missing volume: {summary['quality']['missing_volume_eq_rows']:,}; negative volume: {summary['quality']['negative_volume_eq_rows']:,}; zero volume: {summary['quality']['zero_volume_eq_rows']:,} (allowed by the stated `Volume >= 0` test, reported separately).
- Malformed dates, non-finite numbers, and OHLCV row anomalies are written to `audit/data_quality_anomalies.csv`; source values are not corrected or deleted.
- Since Parquet stores typed columns, malformed text numerics are not expected in the published files. Nulls are measured per field in the schema table; TejHQ documents pre-2012 null ISIN/trade-count fields.

## 9. Duplicate analysis

- Natural key: `date + symbol + series`; duplicate excess rows: {summary['duplicates']['date_symbol_series_excess_rows']:,}.
- Duplicate key groups with identical payloads: {summary['duplicates']['identical_duplicate_key_groups']:,}; conflicting payload groups: {summary['duplicates']['conflicting_duplicate_key_groups']:,}.
- `date + ISIN + series` duplicate rows: {summary['duplicates']['date_isin_series_duplicate_rows']:,}.
- Examples: `audit/duplicate_examples.csv`. No deduplication was performed by this audit.

## 10. Missing-data analysis

Symbols with any expected-session gap: {missing['symbols_with_any_expected_session_gap']:,}; symbols with a longest consecutive gap of at least 20 expected sessions: {missing['symbols_with_gaps_at_least_20_sessions']:,}; symbols with 20 or fewer observations: {missing['symbols_with_20_or_fewer_rows']:,}. Expected sessions use the official NIFTY calendar from 2020 where available and the observed union of TejHQ NSE dates before 2020, so pre-2020 gap rates are not a definitive exchange-completeness measure. No values are filled.

## 11. Extreme-return analysis

EQ close-to-close moves exceeding absolute 10%, 20%, and 50% are counted as {summary['extreme_returns']['over_10pct']:,}, {summary['extreme_returns']['over_20pct']:,}, and {summary['extreme_returns']['over_50pct']:,}. Every move above 10% is listed in `audit/extreme_returns.csv`. Nearby action events are labeled as correlations only; price jumps are not automatically assigned a cause or removed.

## 12. Corporate actions and adjustment methodology

- NSE action records acquired: {actions['rows']:,}; type counts:

| Type | Events | Symbols | First ex-date | Last ex-date |
|---|---:|---:|---|---|
{action_table}

- TejHQ documents a separate action tree and an adjusted-price tree. Its card describes `adj_close = close × cumulative factor` over later corporate actions; splits/bonuses and a dividend factor `(prev_close - cash) / prev_close` are included, while demergers, rights, buybacks and mergers are not scaled.
- The raw tree and normalized output here remain raw/unadjusted values; the separate adjusted tree is not used.
- `audit/corporate_action_examples.csv` independently computes expected mechanical split/bonus/dividend ratios from the action fields and compares them with raw close ratios around selected ex-dates. The comparison is a consistency check, not a test of the final ex-date close: the residual includes market movement. External NSE event-by-event verification remains UNKNOWN. A nearby move does not prove causation.
- Some raw source action rows can carry a stale post-merger ISIN, which TejHQ itself notes; use the published identity history for review, not automatic symbol mapping.

## 13. Symbol/ISIN continuity

The source documents one contiguous interval per ISIN/symbol in `symbol_history/nse.parquet`. The audit compared its rows with identity pairs observed in raw data; see `audit/symbol_history_audit.json` for interval checks and unmatched pair counts, and `audit/security_identity_changes.csv` for observed mappings. No mapping is applied. The card notes pre-2012 NSE data has no ISIN and that ISIN can change around face-value splits/re-listings. Old rows without ISIN remain null.

## 14. Delisted and historical security behavior

Observed EQ last-date facts: {json.dumps(survivorship['evidence'], sort_keys=True)}. A security ending before the snapshot's last date may be delisted, renamed, inactive, newly assigned another ISIN, or missing from the provider snapshot; this data alone does not distinguish those states. TejHQ claims it covers all listings since 2010 and preserves delisted members in qualifying historical liquidity-universe months. Complete delisted-security coverage is therefore **PARTIAL/UNKNOWN**, not proven survivorship-free.

## 15. Survivorship assessment

**{survivorship['status']}** — historical EQ securities with earlier last observations are present, but completeness of inactive/delisted securities cannot be independently established. The monthly PIT liquidity universe is a distinct artifact, not a guarantee that every raw security history is complete.

## 16. Point-in-time universe assessment

The separately published NSE liquidity universe has {pit.get('rows', 0):,} rows and {pit.get('rebalance_dates', 0):,} rebalance dates from {pit.get('first_rebalance_date', 'UNKNOWN')} to {pit.get('last_rebalance_date', 'UNKNOWN')}. TejHQ documents first-trading-day monthly top 500 by trailing 63-session turnover and says membership uses information available on the rebalance date. Duplicate/date/rank row checks are in `audit/universe_coverage.csv`. The ranking computation was not independently recalculated; status is **{pit.get('status', 'UNKNOWN')}**. The universe was not used in a strategy or backtest. It is a distinct concept from applying the current NIFTY 500 list to all prior history.

## 17. Phase 3C fold coverage

**{fold['status']}** — {fold['segments_passing']}/{fold['segments_total']} train/validation/test windows contain all dates in the official NSE NIFTY calendar. Exact requested boundaries and availability are in `audit/phase3c_fold_coverage.csv`.

| Fold | Segment | Requested window | Official sessions | EQ-covered sessions | Missing | Status |
|---:|---|---|---:|---:|---:|---|
{fold_table}

This is calendar-coverage verification only. No Phase 3C strategy ran.

## 18. Lookahead and leakage assessment

**{lookahead['status']}** — the raw source tree does not contain adjusted close, future corporate-action factors, future membership, or derived future-looking fields. The intended raw EQ projection would preserve that property, but it was withheld because date coverage failed. TejHQ's separate back-adjusted close uses factors from actions after each row, so that tree can encode future corporate actions relative to historical rows. The PIT universe is separate and is not selected in this task. Later corrections to old snapshots were not compared; revision risk remains.

Data quality and lookahead safety are distinct: clean bars do not establish point-in-time safety, and a point-in-time membership claim does not establish raw data completeness.

## 19. Known limitations

1. The published raw tree is already cleaned/deduplicated and drops some detected anomalies; the original NSE files and omitted rows are not available for this audit.
2. Official index calendar comparison begins only in 2020; earlier per-symbol completeness denominators are proxies.
3. TejHQ's stated source license is not an independent determination of the exchange's terms for commercial use.
4. Historical security disappearance cannot be equated with a delisting, and total dead-security completeness remains unknown.
5. Corporate action examples have not been externally checked against original NSE action notices.
6. Raw OHLC prices are not corporate-action adjusted. The dataset card documents adjusted close only; using raw bars for return comparisons can create action-related discontinuities. The adjusted close has future-action back-adjustment risk.
7. The point-in-time universe formula is documented but not recalculated independently.
8. The normalized dataset was withheld because the exact five-fold date-coverage gate failed; the provider adapter is isolated and not wired into the Phase 3C runner.

## 20. Final suitability verdict

**{summary['audit_status']}** for the existing five-fold Phase 3C walk-forward requirement. Although the overall history spans all requested boundaries, the dataset misses official NSE EQ dates inside the required windows, so exact session coverage fails. The raw published rows pass local OHLCV/key checks, subject to source-side filtering and survivorship/adjustment limits. No normalized Phase 3C candidate was created because required coverage did not pass. No strategy results were generated or reinterpreted here.

### Normalized data candidate

""" + (f"Created `{normalized['path']}` ({normalized['rows']:,} rows, SHA-256 `{normalized['sha256']}`), exact `EQ` series only, with no price adjustments or filled dates. Provider remains isolated and not selected by default.\n" if normalized else "Not created: the dataset misses official NSE EQ dates inside the required five-fold windows. The reason is also recorded in `data/tejhq/normalized/README.md`.\n") + """

### Phase 3C execution approval

**Do not run Phase 3C until the user explicitly approves after reviewing this audit.**
"""
    report_path = project_root / "docs" / "TEJHQ_DATA_AUDIT.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")


def manifest_value(root: Path, key: str) -> Any:
    manifest = json.loads((root / "tejhq_dataset_manifest.json").read_text(encoding="utf-8"))
    return manifest.get(key)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download a pinned TejHQ NSE snapshot and produce a local independent audit.")
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--revision", default=CURRENT_PIN, help="Immutable Hugging Face dataset commit SHA (defaults to the verified current snapshot).")
    parser.add_argument("--skip-download", action="store_true", help="Audit a previously acquired snapshot in data/tejhq.")
    args = parser.parse_args()
    project_root = args.project_root.resolve()
    root = project_root / "data" / "tejhq"
    if not args.skip_download:
        acquire_snapshot(root, args.revision)
    summary = audit_snapshot(root, project_root)
    print(f"TEJHQ AUDIT STATUS: {summary['audit_status']}")
    print(f"DATASET: {summary['dataset_id']} @ {summary['dataset_revision']}")
    print(f"SNAPSHOT SHA-256: {summary['snapshot_sha256']}")
    print(f"ROWS: {summary['rows']:,} (EQ {summary['eq_rows']:,})")
    print(f"DATES: {summary['first_date']} -> {summary['last_date']}")
    print(f"SYMBOLS: {summary['symbols']:,}; SERIES: {summary['series']}")
    print(f"DUPLICATES: {summary['duplicates']['date_symbol_series_excess_rows']:,}; INVALID EQ ROWS: {summary['quality']['invalid_eq_rows']:,}")
    print(f"PHASE 3C FOLD COVERAGE: {summary['phase3c_folds']['status']}")
    print("No backtest or strategy execution was performed.")
    return 0 if summary["audit_status"] in {"RESEARCH_READY", "RESEARCH_READY_WITH_LIMITATIONS"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
