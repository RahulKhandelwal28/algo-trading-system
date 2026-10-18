import hashlib
import json

import pandas as pd
import pytest

from nse_research.data.tejhq import (
    DATASET_ID,
    TejHQDataError,
    TejHQProvider,
    equity_quality_anomalies,
    normalize_eq_rows,
)


def _raw_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-02", "2024-01-03"],
            "symbol": ["TCS", "BEONLY", "TCS"],
            "series": ["EQ", "BE", "EQ"],
            "isin": ["INE467B01029", "INE000B01000", "INE467B01029"],
            "name": ["TCS", "Test BE", "TCS"],
            "open": [100.0, 10.0, 102.0],
            "high": [104.0, 11.0, 106.0],
            "low": [99.0, 9.0, 101.0],
            "close": [103.0, 10.5, 105.0],
            "last": [103.0, 10.5, 105.0],
            "prev_close": [99.0, 10.0, 103.0],
            "volume": [1000, 50, 1100],
            "turnover": [102000.0, 500.0, 114000.0],
            "trades": [100, 5, 110],
        }
    )


def _manifest_for(path, normalized: pd.DataFrame) -> tuple[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_parquet(path, index=False)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest_path = path.parents[1] / "tejhq_dataset_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "dataset_id": DATASET_ID,
                "dataset_revision": "0123456789abcdef0123456789abcdef01234567",
                "snapshot_sha256": "a" * 64,
                "normalized_dataset": {"path": path.name, "sha256": digest},
            }
        ),
        encoding="utf-8",
    )
    return str(manifest_path), digest


def test_normalizer_keeps_only_explicit_eq_and_preserves_raw_values() -> None:
    normalized = normalize_eq_rows(_raw_frame())

    assert list(normalized.columns) == [
        "date", "symbol", "series", "isin", "open", "high", "low", "close",
        "prev_close", "volume", "turnover",
    ]
    assert len(normalized) == 2
    assert set(normalized["series"]) == {"EQ"}
    assert normalized["symbol"].tolist() == ["TCS", "TCS"]
    assert normalized["close"].tolist() == [103.0, 105.0]
    assert normalized["volume"].tolist() == [1000, 1100]


def test_quality_audit_flags_bad_ohlc_and_does_not_repair_source_row() -> None:
    source = _raw_frame()
    source.loc[0, "high"] = 102.0

    anomalies = equity_quality_anomalies(source)

    assert len(anomalies) == 1
    assert "invalid_high_relationship" in anomalies.iloc[0]["issues"]
    assert source.loc[0, "high"] == 102.0
    with pytest.raises(TejHQDataError, match="anomalous EQ rows"):
        normalize_eq_rows(source)


def test_normalizer_requires_series_and_rejects_duplicate_natural_key() -> None:
    source = _raw_frame().drop(columns=["series"])
    with pytest.raises(TejHQDataError, match="missing columns.*series"):
        normalize_eq_rows(source)

    source = _raw_frame().iloc[[0, 0]].copy()
    with pytest.raises(TejHQDataError, match=r"duplicate \(date, symbol, series\)"):
        normalize_eq_rows(source)


def test_provider_reads_local_daily_history_and_exposes_snapshot_metadata(tmp_path) -> None:
    normalized_path = tmp_path / "data" / "tejhq" / "normalized" / "tejhq_nse_equity.parquet"
    normalized = normalize_eq_rows(_raw_frame())
    manifest_path, digest = _manifest_for(normalized_path, normalized)

    provider = TejHQProvider(normalized_path, manifest_path)
    history = provider.history("tcs.ns", pd.Timestamp("2024-01-03").date(), pd.Timestamp("2024-01-03").date())

    assert provider.provider == "tejhq"
    assert provider.metadata == {
        "provider": "tejhq",
        "dataset_id": DATASET_ID,
        "dataset_revision": "0123456789abcdef0123456789abcdef01234567",
        "snapshot_sha256": "a" * 64,
    }
    assert history.index.tolist() == [pd.Timestamp("2024-01-03")]
    assert history.iloc[0]["close"] == 105.0
    assert hashlib.sha256(normalized_path.read_bytes()).hexdigest() == digest


def test_provider_rejects_normalized_file_with_wrong_manifest_hash(tmp_path) -> None:
    normalized_path = tmp_path / "data" / "tejhq" / "normalized" / "tejhq_nse_equity.parquet"
    manifest_path, _ = _manifest_for(normalized_path, normalize_eq_rows(_raw_frame()))
    normalized_path.write_bytes(normalized_path.read_bytes() + b"changed")

    with pytest.raises(TejHQDataError, match="SHA-256"):
        TejHQProvider(normalized_path, manifest_path)
