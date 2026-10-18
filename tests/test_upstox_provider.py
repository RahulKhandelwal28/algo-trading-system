from __future__ import annotations

import asyncio
import csv
import gzip
import hashlib
import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from nse_research.forward.calendar import NseTradingCalendar
from nse_research.config import RealtimeSettings
from nse_research.realtime.proto import MarketDataFeedV3_pb2 as pb
from nse_research.realtime.engine import RealtimeFeedMonitor, TickRecorder
from nse_research.realtime.upstox import (
    MAPPING_COLUMNS,
    SMOKE_SYMBOLS,
    UpstoxConfigurationError,
    UpstoxV3Provider,
    build_mapping_rows,
    download_official_bod,
    load_verified_mapping,
    parse_epoch_ms,
    read_instrument_master,
    read_project_universe,
    search_instrument_by_isin,
    search_universe_with_rate_limit,
    unverified_mapping_rows,
    write_mapping_csv,
)
from nse_research.realtime.engine import IntradayCandleBuilder, validate_quote


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 28, 4, 0, 1, 200000, tzinfo=timezone.utc)
ISIN = "INE002A01018"
KEY = f"NSE_EQ|{ISIN}"


def universe_row(symbol: str = "RELIANCE", isin: str = ISIN) -> dict[str, str]:
    return {"symbol": symbol, "isin": isin, "exchange": "NSE", "series": "EQ"}


def instrument(symbol: str = "RELIANCE", isin: str = ISIN, **overrides) -> dict:
    result = {
        "instrument_key": f"NSE_EQ|{isin}", "exchange": "NSE", "segment": "NSE_EQ",
        "instrument_type": "EQ", "isin": isin, "trading_symbol": symbol,
        "exchange_token": "2885", "tick_size": 0.05, "lot_size": 1,
    }
    result.update(overrides)
    return result


def verified_mapping(symbol: str = "RELIANCE") -> dict[str, dict[str, str]]:
    rows = build_mapping_rows([universe_row(symbol)], [instrument(symbol)], method="UPSTOX_BOD_JSON")
    assert rows[0]["mapping_status"] == "VERIFIED"
    return {symbol: rows[0]}


def provider(*, symbol: str = "RELIANCE", now: datetime = NOW, **kwargs) -> UpstoxV3Provider:
    return UpstoxV3Provider(
        verified_mapping(symbol), access_token="test-token-never-print",
        utcnow=lambda: now, **kwargs,
    )


def feed_message(*, symbol_key: str = KEY, ltt: int | None = None, price: float = 100.0,
                 ltq: int = 5, cp: float | None = None, message_type: int = pb.initial_feed,
                 current_ts: int | None = None) -> bytes:
    response = pb.FeedResponse(type=message_type)
    response.currentTs = current_ts if current_ts is not None else int(NOW.timestamp() * 1000)
    response.feeds[symbol_key].ltpc.ltp = price
    response.feeds[symbol_key].ltpc.ltt = ltt if ltt is not None else int((NOW - timedelta(seconds=1)).timestamp() * 1000)
    response.feeds[symbol_key].ltpc.ltq = ltq
    response.feeds[symbol_key].ltpc.cp = price - 1 if cp is None else cp
    return response.SerializeToString()


def test_mapping_uses_exact_isin_and_requires_explicit_nse_eq_identity():
    rows = build_mapping_rows([universe_row()], [instrument()], method="UPSTOX_BOD_JSON")
    assert rows[0]["mapping_status"] == "VERIFIED"
    assert rows[0]["upstox_instrument_key"] == KEY
    assert rows[0]["upstox_isin"] == ISIN


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [({"exchange": "BSE", "segment": "BSE_EQ"}, "INVALID"),
     ({"segment": "NSE_FO", "instrument_type": "FUT"}, "INVALID"),
     ({"instrument_type": "BE"}, "INVALID"),
     ({"trading_symbol": "WRONG"}, "INVALID")],
)
def test_mapping_rejects_wrong_exchange_segment_series_or_symbol(overrides, expected):
    rows = build_mapping_rows([universe_row()], [instrument(**overrides)], method="UPSTOX_BOD_JSON")
    assert rows[0]["mapping_status"] == expected
    assert not rows[0]["upstox_instrument_key"]


def test_missing_duplicate_unverified_and_schema_validation(tmp_path: Path):
    missing = build_mapping_rows([universe_row()], [], method="UPSTOX_BOD_JSON")
    duplicate = build_mapping_rows([universe_row()], [instrument(), instrument()], method="UPSTOX_BOD_JSON")
    unverified = unverified_mapping_rows([universe_row()], "BOD unavailable")
    assert missing[0]["mapping_status"] == "MISSING"
    assert duplicate[0]["mapping_status"] == "DUPLICATE"
    assert unverified[0]["mapping_status"] == "UNVERIFIED"
    assert tuple(unverified[0]) == MAPPING_COLUMNS

    path = tmp_path / "mapping.csv"
    valid = build_mapping_rows([universe_row()], [instrument()], method="UPSTOX_BOD_JSON")
    write_mapping_csv(valid, path)
    loaded = load_verified_mapping(path, expected_count=1)
    assert loaded["RELIANCE"]["upstox_isin"] == ISIN
    path.write_text("project_symbol,project_isin\nRELIANCE,INE\n", encoding="utf-8")
    with pytest.raises(ValueError, match="schema"):
        load_verified_mapping(path, expected_count=1)


def test_frozen_universe_reader_preserves_explicit_eq_and_isin():
    path = ROOT / "data/paper_trading/phase3d_personal_universe_v2.csv"
    rows = read_project_universe(path)
    assert len(rows) == 486
    assert all(row["series"] == "EQ" and row["exchange"] == "NSE" for row in rows)
    assert len({row["isin"] for row in rows}) == 486
    assert set(SMOKE_SYMBOLS).issubset({row["symbol"] for row in rows})


def test_bod_json_reader_and_atomic_download_validation(tmp_path: Path):
    body = gzip.compress(json.dumps([instrument()]).encode())
    path = tmp_path / "NSE.json.gz"
    path.write_bytes(body)
    assert read_instrument_master(path)[0]["isin"] == ISIN

    class Response:
        status_code = 200
        content = body

    class Session:
        calls = 0

        def get(self, url, **kwargs):
            self.calls += 1
            assert "assets.upstox.com" in url
            return Response()

    session = Session()
    downloaded = download_official_bod(path, session=session)
    assert session.calls == 1
    assert downloaded[0]["isin"] == ISIN
    assert gzip.decompress(path.read_bytes()) == json.dumps([instrument()]).encode()


def test_instrument_search_isin_uses_read_only_exact_search_and_redacts_token():
    secret = "secret-test-token"

    class Response:
        status_code = 200

        def json(self):
            return {"data": [instrument()]}

    class Session:
        def get(self, url, *, params, headers, timeout):
            assert url.endswith("/v2/instruments/search")
            assert params == {"query": ISIN, "exchanges": "NSE", "segments": "EQ", "page_number": 1, "records": 30}
            assert headers["Authorization"] == f"Bearer {secret}"
            return Response()

    assert search_instrument_by_isin(ISIN, secret, session=Session())[0]["isin"] == ISIN

    class FailingSession:
        def get(self, *args, **kwargs):
            raise RuntimeError(secret)

    with pytest.raises(UpstoxConfigurationError) as error:
        search_instrument_by_isin(ISIN, secret, session=FailingSession())
    assert secret not in str(error.value)


def test_instrument_search_fallback_is_serial_rate_limited_and_verifies_each_isin():
    securities = [universe_row("RELIANCE"), universe_row("TCS", "INE467B01029")]
    records = [instrument("RELIANCE"), instrument("TCS", "INE467B01029")]

    class Response:
        status_code = 200

        def __init__(self, row):
            self.row = row

        def json(self):
            return {"data": [self.row]}

    class Session:
        def __init__(self):
            self.index = 0
            self.params = []

        def get(self, url, *, params, headers, timeout):
            self.params.append(params)
            result = Response(records[self.index])
            self.index += 1
            return result

    session = Session()
    sleeps = []
    mapped = search_universe_with_rate_limit(
        securities, "secret", session=session, interval_seconds=0.75,
        sleeper=sleeps.append,
    )
    assert [row["mapping_status"] for row in mapped] == ["VERIFIED", "VERIFIED"]
    assert len(session.params) == 2
    assert sleeps == [0.75]


def test_epoch_millisecond_timestamp_is_strict_and_timezone_aware():
    raw = int(NOW.timestamp() * 1000)
    parsed = parse_epoch_ms(str(raw))
    assert parsed.tzinfo is timezone.utc
    assert parsed == NOW.replace(microsecond=200000)
    for wrong in (NOW.timestamp(), str(int(NOW.timestamp())), "not-a-time", True, "10000000000000"):
        with pytest.raises(ValueError):
            parse_epoch_ms(wrong)


def test_official_protobuf_ltpc_decodes_into_normalized_quote_with_raw_times():
    stream = provider()
    payload = feed_message(price=123.45, ltq=20)
    assert stream.process_message(payload, received_at=NOW) == 1
    quote = stream.get_quotes(["RELIANCE"])[0]
    assert quote.exchange == "NSE" and quote.series == "EQ"
    assert quote.instrument_key == KEY
    assert quote.last_price == 123.45
    assert quote.change == 1.0
    assert quote.change_percent == pytest.approx(100 / 122.45)
    assert quote.last_trade_quantity == 20
    assert quote.source_timestamp_unit == "epoch_ms"
    assert quote.source_timestamp_origin == "UPSTOX_LTT_CONFIGURED_EPOCH_MS_NOT_EMPIRICALLY_VALIDATED"
    assert quote.source_timestamp_raw == str(int((NOW - timedelta(seconds=1)).timestamp() * 1000))
    assert quote.local_received_timestamp_utc == NOW
    assert quote.feed_current_timestamp_utc == NOW
    assert quote.volume is None and quote.volume_kind is None


def test_protobuf_market_status_and_malformed_frames_are_handled_fail_closed():
    stream = provider()
    status = pb.FeedResponse(type=pb.market_info, currentTs=int(NOW.timestamp() * 1000))
    status.marketInfo.segmentStatus["NSE_EQ"] = pb.NORMAL_OPEN
    assert stream.process_message(status.SerializeToString(), received_at=NOW) == 0
    assert stream.health_snapshot()["market_segment_status"] == "NORMAL_OPEN"
    assert stream.process_message(b"", received_at=NOW) == 0
    assert stream.process_message(b"\x12", received_at=NOW) == 0
    empty_structure = pb.FeedResponse(type=pb.initial_feed).SerializeToString()
    assert stream.process_message(empty_structure, received_at=NOW) == 0
    assert stream.health_snapshot()["invalid_count"] >= 3


def test_future_and_stale_timestamps_are_rejected_before_quote_creation():
    stream = provider(max_quote_age_seconds=5, max_future_skew_seconds=1)
    too_future = int((NOW + timedelta(seconds=2)).timestamp() * 1000)
    stale = int((NOW - timedelta(seconds=6)).timestamp() * 1000)
    assert stream.process_message(feed_message(ltt=too_future), received_at=NOW) == 0
    assert stream.process_message(feed_message(ltt=stale), received_at=NOW) == 0
    assert stream.get_quotes(["RELIANCE"]) == []
    assert stream.health_snapshot()["invalid_count"] == 1
    assert stream.health_snapshot()["stale_count"] == 1


def test_ltpc_previous_close_is_validated_and_normalized():
    stream = provider()
    assert stream.process_message(feed_message(price=101.0, cp=100.0), received_at=NOW) == 1
    quote = stream.get_quotes(["RELIANCE"])[0]
    assert quote.change == 1.0
    assert quote.change_percent == 1.0

    invalid = provider()
    assert invalid.process_message(feed_message(cp=0.0), received_at=NOW) == 0
    assert invalid.health_snapshot()["invalid_count"] == 1


def test_duplicate_tick_signature_is_rejected_and_counted():
    stream = provider()
    message = feed_message(message_type=pb.live_feed)
    assert stream.process_message(message, received_at=NOW) == 1
    assert stream.process_message(message, received_at=NOW) == 0
    assert len(stream.get_quotes(["RELIANCE"])) == 1
    assert stream.health_snapshot()["duplicate_count"] == 1


def _full_message(cumulative: int, seconds: int) -> bytes:
    response = pb.FeedResponse(type=pb.live_feed)
    response.currentTs = int((NOW + timedelta(seconds=seconds)).timestamp() * 1000)
    full = response.feeds[KEY].fullFeed.marketFF
    full.ltpc.ltp = 100 + seconds
    full.ltpc.ltt = int((NOW + timedelta(seconds=seconds)).timestamp() * 1000)
    full.ltpc.ltq = 5
    full.ltpc.cp = 99 + seconds
    full.vtt = cumulative
    return response.SerializeToString()


def test_cumulative_volume_baseline_delta_and_counter_reset_never_go_negative():
    stream = provider(max_quote_age_seconds=30)
    assert stream.process_message(_full_message(100, 0), received_at=NOW) == 1
    first = stream.get_quotes(["RELIANCE"])[0]
    assert first.volume is None
    assert stream.process_message(_full_message(125, 1), received_at=NOW + timedelta(seconds=1)) == 1
    second = stream.get_quotes(["RELIANCE"])[0]
    assert second.volume == 25 and second.volume_kind == "INCREMENTAL"
    assert stream.process_message(_full_message(7, 2), received_at=NOW + timedelta(seconds=2)) == 1
    reset = stream.get_quotes(["RELIANCE"])[0]
    assert reset.volume is None
    assert stream.health_snapshot()["volume_anomaly_count"] == 1
    assert stream.health_snapshot()["volume_anomaly_events"][-1]["code"] == "VOLUME_RESET_OR_ANOMALY"
    assert stream.process_message(_full_message(12, 3), received_at=NOW + timedelta(seconds=3)) == 1
    resumed = stream.get_quotes(["RELIANCE"])[0]
    assert resumed.volume == 5
    assert resumed.volume >= 0


def test_decoded_quote_runs_through_existing_session_anchored_candles():
    stream = provider()
    local = datetime(2026, 9, 28, 9, 15, 1, tzinfo=ZoneInfo("Asia/Kolkata"))
    source_ms = int(local.astimezone(timezone.utc).timestamp() * 1000)
    assert stream.process_message(feed_message(ltt=source_ms), received_at=local.astimezone(timezone.utc)) == 1
    quote = stream.get_quotes(["RELIANCE"])[0]
    validation = validate_quote(quote, "RELIANCE", now=local.astimezone(timezone.utc), max_quote_age_seconds=5)
    assert validation.accepted
    builder = IntradayCandleBuilder(calendar=NseTradingCalendar())
    builder.add_quote(quote, validation, now=local)
    candles = {(bar.interval, bar.start.timetz().replace(tzinfo=None)) for bar in builder.latest_candles()}
    assert ("1m", datetime.strptime("09:15", "%H:%M").time()) in candles
    assert ("5m", datetime.strptime("09:15", "%H:%M").time()) in candles


def test_raw_timestamp_and_feed_provenance_survive_monitor_and_optional_recorder(tmp_path: Path):
    stream = provider()
    payload = feed_message(price=102.5, ltq=3)
    stream.process_message(payload, received_at=NOW)
    settings = RealtimeSettings(enabled=True, provider="upstox", max_quote_age_seconds=5)
    builder = IntradayCandleBuilder(calendar=NseTradingCalendar())
    monitor = RealtimeFeedMonitor(
        stream, requested_symbols=["RELIANCE"], settings=settings,
        candle_builder=builder, clock=lambda: NOW,
    )
    health = monitor.poll_once()
    quote_row = health["quotes"][0]
    assert quote_row["source_timestamp_raw"] == str(int((NOW - timedelta(seconds=1)).timestamp() * 1000))
    assert quote_row["source_timestamp_unit"] == "epoch_ms"
    assert quote_row["source_timestamp_origin"] == "UPSTOX_LTT_CONFIGURED_EPOCH_MS_NOT_EMPIRICALLY_VALIDATED"
    assert quote_row["instrument_key"] == KEY
    assert quote_row["last_trade_quantity"] == 3

    recording_stream = provider()
    recording_stream.process_message(payload, received_at=NOW)
    record_quote = recording_stream.get_quotes(["RELIANCE"])[0]
    record_validation = validate_quote(record_quote, "RELIANCE", now=NOW, max_quote_age_seconds=5)
    recorded = TickRecorder(tmp_path).record(record_quote, record_validation)
    body = recorded.read_text(encoding="utf-8")
    assert "source_timestamp_raw" in body
    assert "test-token-never-print" not in body


def test_bounded_reconnect_policy_and_resubscription_preserve_state():
    stream = provider(max_reconnect_attempts=1)
    response = SimpleNamespace(status_code=200, json=lambda: {
        "data": {"authorized_redirect_uri": "wss://feed.upstox.com/v3/test?code=one-time"}
    })

    class FakeSocket:
        def __init__(self, attempt):
            self.attempt = attempt
            self.sent = []

        async def __aenter__(self):
            if self.attempt == 2:
                stream._stop_event.set()
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def send(self, body):
            self.sent.append(body)

        async def recv(self):
            if self.attempt == 1:
                raise OSError("network break")
            raise asyncio.TimeoutError()

        async def close(self):
            pass

    sockets = []

    def connect(url, **kwargs):
        assert url.startswith("wss://feed.upstox.com/")
        socket = FakeSocket(len(sockets) + 1)
        sockets.append(socket)
        return socket

    async def no_wait(_seconds):
        return None

    stream._authorize_get = lambda *args, **kwargs: response
    stream._websocket_connect = connect
    stream._async_sleeper = no_wait
    asyncio.run(stream.run(["RELIANCE"]))

    assert stream.health_snapshot()["reconnect_count"] == 1
    assert stream.health_snapshot()["connection_attempts"] == 2
    assert len(sockets) == 2
    requests = [json.loads(item.sent[0].decode()) for item in sockets]
    assert all(request["method"] == "sub" and request["data"]["mode"] == "ltpc" for request in requests)
    assert requests[0]["data"]["instrumentKeys"] == [KEY]
    assert requests[1]["data"]["instrumentKeys"] == [KEY]
    assert stream._gap_pending == {"RELIANCE"}


def test_live_launcher_fails_without_token_or_realtime_flag_and_redacts_token(monkeypatch, capsys):
    launcher_path = ROOT / "scripts/test_upstox_realtime.py"
    spec = importlib.util.spec_from_file_location("upstox_live_launcher", launcher_path)
    launcher = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(launcher)
    monkeypatch.delenv("UPSTOX_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("REALTIME_PROVIDER", "upstox")
    monkeypatch.setenv("REALTIME_ENABLED", "true")
    monkeypatch.setenv("LIVE_TRADING", "false")
    assert launcher.main(["--mode", "LIVE_READ_ONLY"]) == 2
    missing = json.loads(capsys.readouterr().out)
    assert missing["status"] == "UPSTOX_PROVIDER_NOT_CONFIGURED"
    assert missing["market_data_request_made"] is False

    secret = "never-print-this-token"
    monkeypatch.setenv("UPSTOX_ACCESS_TOKEN", secret)
    monkeypatch.setenv("REALTIME_ENABLED", "false")
    monkeypatch.setenv("LIVE_TRADING", "false")
    assert launcher.main(["--mode", "LIVE_READ_ONLY"]) == 2
    disabled = capsys.readouterr().out
    assert "REALTIME_DISABLED" in disabled
    assert secret not in disabled


def test_preflight_is_read_only_and_does_not_touch_phase3_paper_ledger(monkeypatch, capsys):
    launcher_path = ROOT / "scripts/test_upstox_realtime.py"
    spec = importlib.util.spec_from_file_location("upstox_preflight_launcher", launcher_path)
    launcher = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(launcher)
    monkeypatch.setattr(launcher, "_dashboard_http_ready", lambda: False)
    ledger = ROOT / "results/phase3d_forward_20260926_222615/paper_ledger.sqlite"
    before = hashlib.sha256(ledger.read_bytes()).hexdigest() if ledger.is_file() else None
    health = ROOT / "data/realtime/feed_health.json"
    prior_health = health.read_bytes() if health.is_file() else None
    assert launcher.main(["--mode", "PREFLIGHT"]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "PHASE4A8A_PREFLIGHT_BLOCKED"
    assert result["network_market_data_request_made"] is False
    assert result["orders_created"] is False and result["ledger_mutated"] is False
    if before is not None:
        assert hashlib.sha256(ledger.read_bytes()).hexdigest() == before
    if prior_health is None:
        assert not health.exists()
    else:
        assert health.read_bytes() == prior_health
