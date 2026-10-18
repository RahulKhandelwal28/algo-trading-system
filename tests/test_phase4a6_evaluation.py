from __future__ import annotations

import csv
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from nse_research.config import AppConfig, RealtimeSettings
from nse_research.realtime.engine import (
    FeedConfigurationError,
    configured_provider,
    load_eq_universe,
    validate_quote,
)
from nse_research.realtime.providers import NormalizedQuote, QuoteValidity


ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "docs/PHASE4A6_REALTIME_PROVIDER_EVALUATION.md"
UNIVERSE = ROOT / "data/paper_trading/phase3d_personal_universe_v2.csv"
ALLOWED_MATRIX_STATUSES = {"PASS", "PARTIAL", "FAIL", "UNKNOWN"}
ALLOWED_CLASSIFICATIONS = {
    "IMPLEMENTATION_READY",
    "UAT_REQUIRED",
    "DOCUMENTATION_BLOCKED",
    "UNSUITABLE",
}


def test_provider_matrix_has_all_required_columns_and_valid_status_cells():
    report = REPORT.read_text(encoding="utf-8")
    matrix = report.split("## Quote contract compatibility matrix", 1)[1].split("\n## ", 1)[0]
    header = next(line for line in matrix.splitlines() if line.startswith("| Requirement"))
    assert all(name in header for name in ("Choice FinX", "Upstox V3", "DhanHQ V2", "Angel One SmartAPI", "FYERS API V3"))

    rows = [
        line
        for line in matrix.splitlines()
        if line.startswith("|") and not line.startswith("| Requirement") and not line.startswith("|---")
    ]
    assert len(rows) >= 20
    for row in rows:
        cells = [cell.strip() for cell in row.strip("|").split("|")]
        assert len(cells) == 6
        for provider_cell in cells[1:]:
            status = provider_cell.split(" — ", 1)[0]
            assert status in ALLOWED_MATRIX_STATUSES | ALLOWED_CLASSIFICATIONS

    assert "| Overall classification | DOCUMENTATION_BLOCKED | UAT_REQUIRED | DOCUMENTATION_BLOCKED | UAT_REQUIRED | DOCUMENTATION_BLOCKED |" in matrix
    assert "no provider has been tested" in report.lower()


def test_frozen_universe_contains_486_explicit_eq_rows():
    with UNIVERSE.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 486
    assert all(row["Series"].strip().upper() == "EQ" for row in rows)
    symbols = [row["Symbol"].strip().upper() for row in rows]
    assert len(set(symbols)) == 486
    assert load_eq_universe(UNIVERSE) == symbols


def test_personal_profile_selects_jugaad_and_shortlisted_adapters_are_not_registered():
    from nse_research.config import RealtimeSettings

    config = AppConfig.load(ROOT / "config/phase3d_personal.yaml")
    assert RealtimeSettings().enabled is False
    assert config.realtime.enabled is True
    assert config.realtime.provider == "jugaad_data"
    assert config.live_trading.enabled is False

    for provider in ("upstox_v3", "angelone_smartapi"):
        with pytest.raises(FeedConfigurationError) as error:
            configured_provider(RealtimeSettings(enabled=True, provider=provider))
        assert error.value.code == QuoteValidity.DATA_SOURCE_NOT_SUITABLE


def test_existing_normalized_quote_contract_accepts_explicit_source_and_receipt_times():
    source_time = datetime(2026, 9, 27, 4, 0, tzinfo=timezone.utc)
    received_at = source_time + timedelta(milliseconds=250)
    quote = NormalizedQuote(
        symbol="RELIANCE",
        exchange="NSE",
        last_price=100.0,
        change=None,
        change_percent=None,
        volume=5.0,
        market_cap=None,
        timestamp=source_time,
        source="evaluation_contract_fixture",
        received_at=received_at,
        data_age_seconds=0.25,
        validity_status=QuoteValidity.VALID,
        series="EQ",
        tick_id="sequence-1",
        volume_kind="INCREMENTAL",
    )
    result = validate_quote(
        quote,
        "RELIANCE",
        now=received_at,
        max_quote_age_seconds=5,
    )
    assert result.accepted is True
    assert quote.source_timestamp == source_time
    assert quote.received_at == received_at

    missing_series = NormalizedQuote(
        **{**quote.__dict__, "series": None}
    )
    rejected = validate_quote(
        missing_series,
        "RELIANCE",
        now=received_at,
        max_quote_age_seconds=5,
    )
    assert rejected.accepted is False
    assert rejected.code == QuoteValidity.QUOTE_SERIES_INVALID


def test_evaluation_report_contains_no_token_or_secret_values():
    report = REPORT.read_text(encoding="utf-8")
    assert re.search(r"eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.", report) is None
    assert re.search(
        r"(?im)^\s*(?:access_token|refresh_token|feed_token|api_secret)\s*=\s*['\"][^'\"]{12,}",
        report,
    ) is None
    assert "No credentials were read, requested, stored, or written." in report
