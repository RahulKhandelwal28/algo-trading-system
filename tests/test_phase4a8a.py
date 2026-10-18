from __future__ import annotations

import csv
import importlib.util
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import requests
from datetime import timezone

from nse_research.realtime.upstox import (
    MAPPING_COLUMNS,
    REALTIME_UNIVERSE_COLUMNS,
    SMOKE_SYMBOLS,
    UpstoxConfigurationError,
    UpstoxV3Provider,
    build_mapping_rows,
    load_verified_mapping,
    read_realtime_universe_csv,
    search_instrument_by_isin,
    search_universe_with_rate_limit,
    select_realtime_universe,
    write_mapping_csv,
    write_realtime_universe_csv,
)


ROOT = Path(__file__).resolve().parents[1]
ISIN_BASE = 1_000_000_000_000


def _universe(count: int = 110) -> list[dict[str, str]]:
    rows = [
        {"symbol": f"ZZ{index:03d}", "isin": f"INE{ISIN_BASE + index:012d}", "exchange": "NSE", "series": "EQ"}
        for index in range(count)
    ]
    rows[0]["symbol"] = "RELIANCE"
    rows[1]["symbol"] = "TCS"
    rows[2]["symbol"] = "INFY"
    rows[3]["symbol"] = "HDFCBANK"
    rows[4]["symbol"] = "SBIN"
    return rows


def _instrument(row: dict[str, str], **overrides) -> dict[str, str]:
    result = {
        "instrument_key": f"NSE_EQ|{row['isin']}",
        "exchange": "NSE",
        "segment": "NSE_EQ",
        "instrument_type": "EQ",
        "isin": row["isin"],
        "trading_symbol": row["symbol"],
        "exchange_token": "1",
    }
    result.update(overrides)
    return result


def _launcher():
    source = ROOT / "scripts/test_upstox_realtime.py"
    spec = importlib.util.spec_from_file_location("phase4a8a_live_launcher_test", source)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_realtime_universe_is_100_deterministic_and_includes_five_symbols(tmp_path: Path):
    source = _universe()
    selected_a = select_realtime_universe(source, 100)
    selected_b = select_realtime_universe(source, 100)

    assert selected_a == selected_b
    assert [row["symbol"] for row in selected_a[:5]] == list(SMOKE_SYMBOLS)
    assert len(selected_a) == 100
    assert all(row["series"] == "EQ" and row["exchange"] == "NSE" for row in selected_a)

    path = write_realtime_universe_csv(selected_a, tmp_path / "universe.csv")
    loaded = read_realtime_universe_csv(path, expected_count=100)
    with path.open(newline="", encoding="utf-8") as stream:
        assert tuple(csv.DictReader(stream).fieldnames or ()) == REALTIME_UNIVERSE_COLUMNS
    assert len(loaded) == 100
    assert {row["symbol"] for row in loaded}.issuperset(SMOKE_SYMBOLS)


def test_realtime_selection_fails_closed_on_small_or_missing_smoke_universe():
    with pytest.raises(ValueError, match="only"):
        select_realtime_universe(_universe(99), 100)
    rows = _universe()
    rows = [row for row in rows if row["symbol"] != "SBIN"]
    with pytest.raises(ValueError, match="missing required smoke"):
        select_realtime_universe(rows, 100)


@pytest.mark.parametrize("size", [100, 200, 486])
def test_supported_realtime_universe_sizes_are_exact(size):
    selected = select_realtime_universe(_universe(486), size)
    assert len(selected) == size
    assert set(SMOKE_SYMBOLS).issubset({row["symbol"] for row in selected})


@pytest.mark.parametrize(
    ("override", "expected"),
    [
        ({"exchange": "BSE", "segment": "BSE_EQ"}, "INVALID"),
        ({"segment": "NSE_FO"}, "INVALID"),
        ({"instrument_type": "BE"}, "INVALID"),
        ({"series": "BE"}, "INVALID"),
        ({"series": "BZ"}, "INVALID"),
        ({"series": "SME"}, "INVALID"),
        ({"trading_symbol": "WRONG"}, "INVALID"),
    ],
)
def test_isin_search_mapping_rejects_wrong_identity(override, expected):
    project = _universe(100)[0]
    mapped = build_mapping_rows(
        [project], [_instrument(project, **override)],
        method="INSTRUMENT_SEARCH_ISIN", require_trading_symbol=False,
    )
    assert mapped[0]["mapping_status"] == expected


def test_search_mapping_allows_omitted_symbol_but_not_wrong_symbol(tmp_path: Path):
    project = _universe(100)[0]
    candidate = _instrument(project)
    candidate.pop("trading_symbol")
    mapped = build_mapping_rows(
        [project], [candidate], method="INSTRUMENT_SEARCH_ISIN", require_trading_symbol=False,
    )
    assert mapped[0]["mapping_status"] == "VERIFIED"
    assert mapped[0]["upstox_trading_symbol"] == ""

    path = write_mapping_csv(mapped, tmp_path / "mapping.csv")
    assert load_verified_mapping(path, expected_count=1, allow_missing_trading_symbol=True)
    with pytest.raises(ValueError, match="identity"):
        load_verified_mapping(path, expected_count=1)


def test_search_candidate_with_wrong_isin_is_invalid_not_missing():
    project = _universe(100)[0]
    wrong_isin = dict(project, isin="INE9999999999")
    result = build_mapping_rows(
        [project], [_instrument(wrong_isin)], method="INSTRUMENT_SEARCH_ISIN",
        require_trading_symbol=False,
    )
    assert result[0]["mapping_status"] == "INVALID"
    assert "exact project ISIN" in result[0]["mapping_reason"]


def test_search_mapping_marks_duplicate_candidates_and_keys():
    project = _universe(100)[0]
    candidate = _instrument(project)
    duplicate_candidates = build_mapping_rows(
        [project], [candidate, dict(candidate)], method="INSTRUMENT_SEARCH_ISIN",
        require_trading_symbol=False,
    )
    assert duplicate_candidates[0]["mapping_status"] == "DUPLICATE"

    second = dict(project, symbol="OTHER", isin="INE9999999999")
    second_candidate = _instrument(second, instrument_key=candidate["instrument_key"])
    collision = search_universe_with_rate_limit(
        [project, second], "token", interval_seconds=0,
        session=type("Session", (), {"get": lambda self, *a, **k: type(
            "Response", (), {"status_code": 200, "json": lambda self: {"data": [
                candidate if k["params"]["query"] == project["isin"] else second_candidate
            ]}}
        )()})(),
        sleeper=lambda _seconds: None,
    )
    assert [row["mapping_status"] for row in collision] == ["DUPLICATE", "DUPLICATE"]


def test_instrument_search_retries_429_with_bounded_backoff_and_redacts_token():
    secret = "never-log-this-token"
    sleeps: list[float] = []

    class Response:
        status_code = 200

        def json(self):
            return {"data": []}

    class Session:
        calls = 0

        def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return type("Limited", (), {"status_code": 429, "headers": {"Retry-After": "0.5"}})()
            return Response()

    session = Session()
    result = search_instrument_by_isin(
        "INE0000000000", secret, session=session, sleeper=sleeps.append,
    )
    assert result == []
    assert session.calls == 2
    assert sleeps == [0.5]
    assert secret not in repr(result)


def test_search_does_not_retry_permanent_http_errors():
    sleeps: list[float] = []
    calls = []

    class Session:
        def get(self, *_args, **_kwargs):
            calls.append(1)
            return type("Response", (), {"status_code": 401})()

    with pytest.raises(UpstoxConfigurationError, match="HTTP 401"):
        search_instrument_by_isin(
            "INE0000000000", "token", session=Session(), sleeper=sleeps.append,
        )
    assert calls == [1]
    assert sleeps == []


def test_search_retries_timeouts_and_stops_after_configured_bound():
    calls = []
    sleeps = []

    class Session:
        def get(self, *_args, **_kwargs):
            calls.append(1)
            raise requests.Timeout("secret should not surface")

    with pytest.raises(UpstoxConfigurationError, match="bounded retries") as error:
        search_instrument_by_isin(
            "INE0000000000", "hidden", session=Session(), max_retries=2,
            retry_base_seconds=0.25, sleeper=sleeps.append,
        )
    assert len(calls) == 3
    assert sleeps == [0.25, 0.5]
    assert "hidden" not in str(error.value)


def test_universe_search_preserves_completed_rows_and_marks_unqueried_unverified():
    rows = _universe(100)[:2]
    sleeps = []

    class Session:
        calls = 0

        def get(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 2:
                raise requests.ConnectionError("private details")
            first = rows[0]
            return type("Response", (), {"status_code": 200, "json": lambda self: {
                "data": [_instrument(first)]
            }})()

    mapped = search_universe_with_rate_limit(
        rows, "token", session=Session(), interval_seconds=0.25,
        max_retries=0, sleeper=sleeps.append,
    )
    assert [row["mapping_status"] for row in mapped] == ["VERIFIED", "UNVERIFIED"]
    assert sleeps == [0.25]
    assert "private details" not in mapped[1]["mapping_reason"]


@pytest.mark.parametrize(
    ("variables", "expected"),
    [
        ({"REALTIME_PROVIDER": "upstox", "REALTIME_ENABLED": "true"}, "UPSTOX_PROVIDER_NOT_CONFIGURED"),
        ({"REALTIME_PROVIDER": "upstox", "REALTIME_ENABLED": "false", "LIVE_TRADING": "false", "UPSTOX_ACCESS_TOKEN": "test-secret"}, "REALTIME_DISABLED"),
        ({"REALTIME_PROVIDER": "upstox", "REALTIME_ENABLED": "true", "LIVE_TRADING": "true", "UPSTOX_ACCESS_TOKEN": "test-secret"}, "LIVE_TRADING_MUST_REMAIN_DISABLED"),
        ({"REALTIME_PROVIDER": "upstox", "REALTIME_ENABLED": "true", "UPSTOX_ACCESS_TOKEN": "test-secret"}, "LIVE_TRADING_MUST_REMAIN_DISABLED"),
    ],
)
def test_live_launcher_exact_fail_closed_statuses_and_secret_redaction(monkeypatch, capsys, variables, expected):
    launcher = _launcher()
    for name in ("REALTIME_PROVIDER", "REALTIME_ENABLED", "LIVE_TRADING", "UPSTOX_ACCESS_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    for name, value in variables.items():
        monkeypatch.setenv(name, value)
    assert launcher.main(["--mode", "LIVE_READ_ONLY"]) == 2
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["status"] == expected
    assert payload["market_data_request_made"] is False
    assert payload["orders_created"] is False and payload["signals_generated"] is False
    assert payload["ledger_mutated"] is False
    assert "test-secret" not in output


def test_launcher_rejects_unsupported_realtime_universe_size(monkeypatch, capsys):
    launcher = _launcher()
    monkeypatch.setenv("REALTIME_PROVIDER", "upstox")
    monkeypatch.setenv("REALTIME_ENABLED", "true")
    monkeypatch.setenv("LIVE_TRADING", "false")
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", "test-secret")
    monkeypatch.setenv("REALTIME_UNIVERSE_SIZE", "150")
    assert launcher.main(["--mode", "LIVE_READ_ONLY"]) == 2
    output = capsys.readouterr().out
    assert json.loads(output)["status"] == "REALTIME_UNIVERSE_SIZE_INVALID"
    assert "test-secret" not in output


def test_dashboard_readiness_checks_only_local_read_only_health(monkeypatch):
    launcher = _launcher()
    observed = {}

    class Response:
        status = 200

        def read(self):
            return b'{"dashboard_read_only": true}'

    class FakeConnection:
        def __init__(self, host, port, *, timeout):
            observed.update({"host": host, "port": port, "timeout": timeout})

        def request(self, method, path):
            observed.update({"method": method, "path": path})

        def getresponse(self):
            return Response()

        def close(self):
            observed["closed"] = True

    monkeypatch.setattr(launcher, "HTTPConnection", FakeConnection)
    assert launcher._dashboard_http_ready()
    assert observed == {
        "host": "127.0.0.1", "port": 8765, "timeout": 0.5,
        "method": "GET", "path": "/health", "closed": True,
    }


def test_coverage_distinguishes_no_tick_receiving_stale_invalid_and_disconnected(tmp_path: Path):
    launcher = _launcher()

    universe = _universe(100)[:5]
    mapping_rows = build_mapping_rows(
        universe, [_instrument(row) for row in universe], method="UPSTOX_BOD_JSON",
    )
    mapping = {row["project_symbol"]: row for row in mapping_rows}
    stream = UpstoxV3Provider(mapping, access_token="offline-test-token")
    stream._subscribed.update({"RELIANCE", "TCS", "INFY", "HDFCBANK"})
    stream._per_symbol["RELIANCE"]["subscription_status"] = "REQUESTED"
    stream._per_symbol["TCS"].update({
        "tick_count": 1, "last_tick_timestamp": "2026-09-28T09:59:50+00:00",
        "last_received_timestamp": "2026-09-28T09:59:50+00:00",
    })
    stream._per_symbol["INFY"]["invalid_count"] = 1
    stream._per_symbol["HDFCBANK"].update({
        "tick_count": 1, "last_tick_timestamp": "2026-09-28T09:58:00+00:00",
        "last_received_timestamp": "2026-09-28T09:58:00+00:00",
    })

    path = tmp_path / "coverage.csv"
    launcher._write_coverage(
        stream, mapping, path, active_period=True,
        connection_status="CONNECTED",
        observed_at=datetime(2026, 9, 28, 10, 0, tzinfo=ZoneInfo("UTC")),
    )

    with path.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        rows = {row["symbol"]: row for row in reader}
        assert reader.fieldnames == [
            "symbol", "instrument_key", "subscription_status", "first_tick_timestamp",
            "last_tick_timestamp", "tick_count", "invalid_count", "stale_count",
            "duplicate_count", "latency_min_ms", "latency_avg_ms", "latency_p95_ms",
            "latency_max_ms", "status",
        ]
    assert rows["RELIANCE"]["status"] == "SUBSCRIBED_NO_TICK_YET"
    assert rows["TCS"]["status"] == "RECEIVING"
    assert rows["INFY"]["status"] == "INVALID"
    assert rows["HDFCBANK"]["status"] == "STALE"
    assert rows["SBIN"]["status"] == "DISCONNECTED"


def test_five_symbol_report_is_a_separate_offline_artifact(tmp_path: Path, monkeypatch):
    launcher = _launcher()

    universe = _universe(100)[:5]
    mapping_rows = build_mapping_rows(
        universe, [_instrument(row) for row in universe], method="UPSTOX_BOD_JSON",
    )
    mapping = {row["project_symbol"]: row for row in mapping_rows}
    stream = UpstoxV3Provider(mapping, access_token="offline-test-token")
    for symbol in SMOKE_SYMBOLS:
        stream._per_symbol[symbol].update({
            "snapshot_received": True, "live_update_received": True,
            "tick_count": 2, "last_price": 100.0, "last_ltt_raw": "1790565300000",
            "last_tick_timestamp": "2026-09-28T03:15:00+00:00",
            "last_received_timestamp": "2026-09-28T03:15:00.100000+00:00",
            "normalized_quote_status": "VALID",
        })
    output = tmp_path / "five.csv"
    monkeypatch.setattr(launcher, "FIVE_SYMBOL_PATH", output)

    assert launcher._write_five_symbol_validation(stream, mapping) == 5
    with output.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        rows = list(reader)
    assert len(rows) == 5
    assert [row["symbol"] for row in rows] == list(SMOKE_SYMBOLS)
    assert {row["status"] for row in rows} == {"PASS"}


def test_live_session_gate_allows_only_regular_weekday_hours(monkeypatch):
    launcher = _launcher()
    from nse_research.config import AppConfig

    config = AppConfig.load(ROOT / "config/phase3d_personal.yaml")
    zone = ZoneInfo("Asia/Kolkata")
    assert launcher._regular_nse_session_open(config, now=datetime(2026, 9, 28, 9, 15, tzinfo=zone))
    assert not launcher._regular_nse_session_open(config, now=datetime(2026, 9, 28, 15, 30, tzinfo=zone))
    assert not launcher._regular_nse_session_open(config, now=datetime(2026, 9, 27, 10, 0, tzinfo=zone))


def test_production_timestamp_gate_requires_five_plausible_local_session_samples():
    launcher = _launcher()
    from nse_research.config import AppConfig

    config = AppConfig.load(ROOT / "config/phase3d_personal.yaml")
    universe = _universe(100)[:5]
    mapping_rows = build_mapping_rows(
        universe, [_instrument(row) for row in universe], method="UPSTOX_BOD_JSON",
    )
    mapping = {row["project_symbol"]: row for row in mapping_rows}
    stream = UpstoxV3Provider(mapping, access_token="offline-test-token")
    zone = ZoneInfo(config.project.timezone)
    source_local = datetime(2026, 9, 28, 9, 15, 1, tzinfo=zone)
    received_local = datetime(2026, 9, 28, 9, 15, 1, 100000, tzinfo=zone)
    source_utc = source_local.astimezone(timezone.utc)
    received_utc = received_local.astimezone(timezone.utc)
    raw = str(int(source_utc.timestamp() * 1000))
    for symbol in SMOKE_SYMBOLS:
        stream._per_symbol[symbol].update({
            "last_ltt_raw": raw,
            "last_tick_timestamp": source_utc.isoformat(),
            "last_received_timestamp": received_utc.isoformat(),
        })

    assert launcher._five_symbol_timestamps_empirically_plausible(
        stream, config, session_date=datetime(2026, 9, 28).date(),
    )
    assert not launcher._five_symbol_timestamps_empirically_plausible(
        stream, config, session_date=datetime(2026, 9, 29).date(),
    )


def test_controlled_reconnect_requests_only_a_socket_close():
    stream = UpstoxV3Provider(
        {"RELIANCE": build_mapping_rows(
            [_universe(100)[0]], [_instrument(_universe(100)[0])], method="UPSTOX_BOD_JSON",
        )[0]},
        access_token="offline-test-token",
    )
    events = []

    class Socket:
        async def close(self):
            events.append("socket_closed")

    stream._connected = True
    stream._ws = Socket()
    import asyncio
    asyncio.run(stream.request_validation_reconnect())
    assert events == ["socket_closed"]


def test_required_artifact_schemas_remain_exact():
    assert tuple(MAPPING_COLUMNS) == (
        "project_symbol", "project_isin", "project_exchange", "project_series",
        "upstox_instrument_key", "upstox_exchange", "upstox_segment",
        "upstox_instrument_type", "upstox_isin", "upstox_trading_symbol",
        "upstox_exchange_token", "mapping_method", "mapping_status", "mapping_reason",
    )
    assert tuple(REALTIME_UNIVERSE_COLUMNS) == (
        "rank", "symbol", "isin", "exchange", "series", "selection_method",
    )
