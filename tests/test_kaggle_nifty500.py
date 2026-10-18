import json

import pandas as pd
import pytest

from nse_research.data.kaggle_nifty500 import (
    DATASET_SLUG,
    KaggleNifty500DataError,
    KaggleNifty500Provider,
    normalize_kaggle_nifty500,
)


def _source_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2021-01-05", "2021-01-04", "2021-01-04"],
            "Ticker": ["TCS", "RELIANCE", "TCS"],
            "Open": [102.0, 100.0, 100.0],
            "High": [104.0, 103.0, 103.0],
            "Low": [101.0, 99.0, 99.0],
            "Close": [103.0, 102.0, 102.0],
            "Volume": [1200, 1000, 1100],
            "Notes": [None, None, "annotation"],
        }
    )


def test_normalization_maps_source_and_records_provenance_without_series(tmp_path) -> None:
    source = tmp_path / "nifty500_5yr_data.csv"
    output = tmp_path / "normalized" / "nifty500_kaggle.csv"
    _source_frame().to_csv(source, index=False)

    result = normalize_kaggle_nifty500(source, output)

    normalized = pd.read_csv(output)
    manifest = json.loads(result.manifest_path.read_text())
    assert list(normalized.columns) == [
        "Symbol",
        "Date",
        "Open Price",
        "High Price",
        "Low Price",
        "Close Price",
        "Total Traded Quantity",
    ]
    assert "Series" not in normalized.columns
    assert normalized[["Symbol", "Date"]].values.tolist() == [
        ["RELIANCE", "2021-01-04"],
        ["TCS", "2021-01-04"],
        ["TCS", "2021-01-05"],
    ]
    assert result.rows == 3
    assert result.symbols == 2
    assert manifest["provider"] == "kaggle"
    assert manifest["dataset"] == DATASET_SLUG
    assert manifest["series_field"] == "unavailable in source dataset; not fabricated"
    assert manifest["price_adjustment"] == "none performed; source adjustedness is unverified"


def test_provider_reads_separate_normalized_file_and_filters_inclusive_dates(tmp_path) -> None:
    source = tmp_path / "source.csv"
    normalized = tmp_path / "normalized.csv"
    _source_frame().to_csv(source, index=False)
    normalize_kaggle_nifty500(source, normalized)

    provider = KaggleNifty500Provider(normalized)
    bars = provider.history("tcs.ns", pd.Timestamp("2021-01-05").date(), pd.Timestamp("2021-01-05").date())

    assert provider.provider == "kaggle"
    assert provider.dataset == DATASET_SLUG
    assert provider.series_available is False
    assert list(bars.columns) == ["open", "high", "low", "close", "volume"]
    assert bars.index.tolist() == [pd.Timestamp("2021-01-05")]
    assert bars.iloc[0].to_dict() == {
        "open": 102.0,
        "high": 104.0,
        "low": 101.0,
        "close": 103.0,
        "volume": 1200,
    }


def test_provider_batch_histories_keeps_kaggle_provenance_and_separate_symbols(tmp_path) -> None:
    source = tmp_path / "source.csv"
    normalized = tmp_path / "normalized.csv"
    _source_frame().to_csv(source, index=False)
    normalize_kaggle_nifty500(source, normalized)

    provider = KaggleNifty500Provider(normalized)
    histories = provider.histories(
        pd.Timestamp("2021-01-04").date(), pd.Timestamp("2021-01-05").date()
    )

    assert provider.provider == "kaggle"
    assert set(histories) == {"TCS", "RELIANCE"}
    assert histories["TCS"].index.tolist() == [pd.Timestamp("2021-01-04"), pd.Timestamp("2021-01-05")]
    assert histories["RELIANCE"].index.tolist() == [pd.Timestamp("2021-01-04")]
    assert all(list(frame.columns) == ["open", "high", "low", "close", "volume"] for frame in histories.values())


def test_provider_rejects_non_daily_intervals_and_invalid_date_range(tmp_path) -> None:
    source = tmp_path / "source.csv"
    normalized = tmp_path / "normalized.csv"
    _source_frame().to_csv(source, index=False)
    normalize_kaggle_nifty500(source, normalized)
    provider = KaggleNifty500Provider(normalized)

    with pytest.raises(ValueError, match="daily bars only"):
        provider.history("TCS", pd.Timestamp("2021-01-04").date(), pd.Timestamp("2021-01-05").date(), "5m")
    with pytest.raises(ValueError, match="start date"):
        provider.history("TCS", pd.Timestamp("2021-01-05").date(), pd.Timestamp("2021-01-04").date())


def test_normalizer_rejects_duplicate_symbol_date_without_replacing_output(tmp_path) -> None:
    source = tmp_path / "source.csv"
    output = tmp_path / "normalized.csv"
    frame = _source_frame()
    frame.loc[2, "Ticker"] = "RELIANCE"
    frame.loc[2, "Date"] = "2021-01-04"
    frame.loc[2, ["Open", "High", "Low", "Close", "Volume"]] = [100, 103, 99, 102, 1100]
    frame.to_csv(source, index=False)
    output.write_text("previous complete file\n")

    with pytest.raises(KaggleNifty500DataError, match=r"duplicate \(Symbol, Date\)"):
        normalize_kaggle_nifty500(source, output)

    assert output.read_text() == "previous complete file\n"


def test_normalizer_rejects_material_ohlc_violation(tmp_path) -> None:
    source = tmp_path / "source.csv"
    output = tmp_path / "normalized.csv"
    frame = _source_frame()
    frame.loc[0, "High"] = 101.0
    frame.loc[0, "Close"] = 103.0
    frame.to_csv(source, index=False)

    with pytest.raises(KaggleNifty500DataError, match="violate OHLC bounds"):
        normalize_kaggle_nifty500(source, output)

    assert not output.exists()


def test_normalizer_accepts_only_float_precision_ohlc_difference(tmp_path) -> None:
    source = tmp_path / "source.csv"
    output = tmp_path / "normalized.csv"
    frame = _source_frame()
    frame.loc[0, "High"] = 103.0 - 5e-10
    frame.loc[0, "Close"] = 103.0
    frame.to_csv(source, index=False)

    normalize_kaggle_nifty500(source, output)

    normalized = pd.read_csv(output)
    row = normalized.loc[
        normalized["Symbol"].eq("TCS") & normalized["Date"].eq("2021-01-05")
    ].iloc[0]
    assert row["High Price"] < row["Close Price"]
