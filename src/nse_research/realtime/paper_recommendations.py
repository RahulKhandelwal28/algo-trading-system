"""Paper-only signal generation from validated Jugaad LTP snapshot candles.

The upstream source is polled last-traded-price snapshots, not a trade-by-trade
websocket or exchange-native OHLC feed. Its sampled candles are therefore
labelled as approximate and never include inferred volume.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from nse_research.forward.calendar import NseTradingCalendar
from nse_research.intraday import (
    IntradayScanner,
    IntradayScannerConfig,
    StrategySettings,
    intraday_bar_from_realtime_candle,
)


class JugaadPaperRecommendationSession:
    """Pass immutable closed sample-bars through the existing long-only scanner.

    The scanner is initialized with the full frozen universe to preserve its
    existing universe-size invariant. Only ``requested_symbols`` are allowed
    to enter this session from the capped Jugaad quote monitor.
    """

    def __init__(
        self,
        *,
        universe_symbols: Iterable[str],
        requested_symbols: Iterable[str],
        timeframes: Iterable[str],
        calendar: NseTradingCalendar,
        strategy_settings: StrategySettings | None = None,
        source: str = "jugaad_data_nse_live",
        paper_capital: float | None = None,
        risk_per_trade_fraction: float = 0.01,
        max_single_position_fraction: float = 0.25,
    ) -> None:
        universe = tuple(str(item).strip().upper() for item in universe_symbols)
        requested = tuple(str(item).strip().upper() for item in requested_symbols)
        frames = tuple(dict.fromkeys(str(item).strip().lower() for item in timeframes))
        if len(universe) not in {100, 200, 486} or len(set(universe)) != len(universe):
            raise ValueError("paper scanner requires a unique frozen universe of 100, 200, or 486 symbols")
        if not requested or len(set(requested)) != len(requested):
            raise ValueError("requested paper symbols must be non-empty and unique")
        if len(requested) > 5 or not set(requested).issubset(universe):
            raise ValueError("paper feed symbols must be a subset of the frozen EQ universe, capped at five")
        if not frames or not set(frames).issubset({"1m", "5m"}):
            raise ValueError("paper scanner accepts only 1m and 5m sampled candles")
        if not source.strip():
            raise ValueError("paper recommendation source is required")
        if paper_capital is not None and (not math.isfinite(paper_capital) or paper_capital <= 0):
            raise ValueError("paper capital must be positive and finite")
        if not math.isfinite(risk_per_trade_fraction) or not 0 < risk_per_trade_fraction <= 0.01:
            raise ValueError("risk per trade must be in (0, 0.01]")
        if not math.isfinite(max_single_position_fraction) or not 0 < max_single_position_fraction <= 1:
            raise ValueError("max position fraction must be in (0, 1]")

        self.requested_symbols = frozenset(requested)
        self.source = source.strip()
        self.paper_capital = paper_capital
        self.risk_per_trade_fraction = risk_per_trade_fraction
        self.max_single_position_fraction = max_single_position_fraction
        self.scanner = IntradayScanner(
            universe,
            config=IntradayScannerConfig(
                enabled=True,
                universe_size=len(universe),
                timeframes=frames,
                regime_filter_enabled=False,
                signal_ttl_bars=3,
                max_history_bars_per_symbol_timeframe=256,
            ),
            calendar=calendar,
            strategy_settings=strategy_settings or StrategySettings(),
        )
        self._seen_candles: set[tuple[str, str, datetime]] = set()
        self._signal_candles: dict[str, dict[str, object]] = {}
        self._quality = {
            "closed_candles_seen": 0,
            "candles_rejected_low_sample_count": 0,
            "candles_processed": 0,
        }

    def ingest_closed_candles(self, candles: Iterable[object]) -> list[str]:
        """Process each immutable closed candle once and return new signal IDs."""
        generated: list[str] = []
        ordered = sorted(
            candles,
            key=lambda item: (
                getattr(item, "start", datetime.min.replace(tzinfo=timezone.utc)),
                str(getattr(item, "symbol", "")),
                str(getattr(item, "interval", "")),
            ),
        )
        for candle in ordered:
            symbol = str(getattr(candle, "symbol", "") or "").strip().upper()
            timeframe = str(getattr(candle, "interval", "") or "").strip().lower()
            start = getattr(candle, "start", None)
            if symbol not in self.requested_symbols or str(getattr(candle, "status", "")).upper() != "CLOSED":
                continue
            if not isinstance(start, datetime) or start.tzinfo is None or start.utcoffset() is None:
                continue
            identity = (symbol, timeframe, start)
            if identity in self._seen_candles:
                continue
            self._seen_candles.add(identity)
            self._quality["closed_candles_seen"] += 1

            # Snapshot-derived OHLC is useful for a bounded paper experiment,
            # but one sample cannot define even approximate intrabar bounds.
            tick_count = int(getattr(candle, "tick_count", 0) or 0)
            if tick_count < 2:
                self._quality["candles_rejected_low_sample_count"] += 1
                continue
            bar = intraday_bar_from_realtime_candle(candle, exchange="NSE", series="EQ")
            signals = self.scanner.handle_bar(bar)
            self._quality["candles_processed"] += 1
            for signal in signals:
                self._signal_candles[signal.signal_id] = {
                    "bar_start": start.isoformat(),
                    "bar_end": signal.timestamp.isoformat(),
                    "last_source_timestamp": _timestamp_iso(getattr(candle, "last_source_timestamp", None)),
                    "open": float(getattr(candle, "open")),
                    "high": float(getattr(candle, "high")),
                    "low": float(getattr(candle, "low")),
                    "close": float(getattr(candle, "close")),
                    "sample_count": tick_count,
                    "construction": "APPROXIMATE_LTP_SNAPSHOT_OHLC",
                    "volume_available": bar.volume is not None and bar.volume_kind == "BAR",
                }
                generated.append(signal.signal_id)
        return generated

    def snapshot(
        self,
        *,
        now: datetime,
        fresh_symbols: Iterable[str],
        latest_quotes: Mapping[str, Mapping[str, object]] | None = None,
    ) -> dict[str, object]:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("snapshot time must be timezone-aware")
        fresh = {str(item).strip().upper() for item in fresh_symbols}
        current_quotes = {str(key).strip().upper(): value for key, value in (latest_quotes or {}).items()}
        recommendations = []
        for signal in self.scanner.signals:
            if now >= signal.valid_until:
                status = "EXPIRED"
            elif signal.symbol not in fresh:
                status = "WATCH_ONLY_CURRENT_QUOTE_NOT_FRESH"
            else:
                status = "ACTIVE_PAPER_CANDIDATE"
            risk_per_share = signal.reference_price - signal.stop_reference
            reward_per_share = signal.target_reference - signal.reference_price
            risk_budget = self.paper_capital * self.risk_per_trade_fraction if self.paper_capital is not None else None
            position_budget = self.paper_capital * self.max_single_position_fraction if self.paper_capital is not None else None
            paper_quantity = None
            if (risk_budget is not None and position_budget is not None and risk_per_share > 0
                    and signal.reference_price > 0):
                by_risk = math.floor(risk_budget / risk_per_share)
                by_capital = math.floor(position_budget / signal.reference_price)
                paper_quantity = max(0, min(by_risk, by_capital))
            current_quote = current_quotes.get(signal.symbol, {})
            current_ltp = current_quote.get("last_price") if signal.symbol in fresh else None
            try:
                current_ltp_value = float(current_ltp) if current_ltp is not None else None
            except (TypeError, ValueError, OverflowError):
                current_ltp_value = None
            recommendations.append({
                "signal_id": signal.signal_id,
                "status": status,
                "market_timestamp": signal.timestamp.isoformat(),
                "valid_until": signal.valid_until.isoformat(),
                "symbol": signal.symbol,
                "exchange": signal.exchange,
                "series": signal.series,
                "side": signal.side,
                "strategy": signal.strategy,
                "timeframe": signal.timeframe,
                "entry_reference_price": signal.reference_price,
                "entry_reference_type": "LAST_CLOSED_BAR_CLOSE",
                "current_ltp": current_ltp_value,
                "current_ltp_source_timestamp": current_quote.get("source_timestamp") if current_ltp_value is not None else None,
                "current_ltp_vs_entry_percent": (
                    (current_ltp_value / signal.reference_price - 1.0) * 100.0
                    if current_ltp_value is not None and signal.reference_price > 0 else None
                ),
                "stop_loss_reference": signal.stop_reference,
                "target_reference": signal.target_reference,
                "risk_per_share": risk_per_share,
                "reward_per_share": reward_per_share,
                "reward_risk_ratio": reward_per_share / risk_per_share if risk_per_share > 0 else None,
                "reason_codes": list(signal.reason_codes),
                "strategy_metadata": dict(signal.confidence_metadata),
                "metadata_is_probability": False,
                "paper_only": True,
                "quantity": None,
                "paper_quantity_advisory": paper_quantity,
                "paper_risk_budget": risk_budget,
                "paper_position_budget": position_budget,
                "paper_max_loss_at_stop_before_costs": (
                    paper_quantity * risk_per_share if paper_quantity is not None and paper_quantity > 0 else None
                ),
                "paper_sizing_note": (
                    "Advisory only; based on configured paper capital, risk fraction, and position cap. "
                    "Excludes fees, slippage, gaps, and execution risk. No order is created."
                    if self.paper_capital is not None else "No capital amount supplied; quantity is not estimated."
                ),
                "broker_order": None,
                "bar_quality": self._signal_candles.get(signal.signal_id, {}),
            })
        active_count = sum(item["status"] == "ACTIVE_PAPER_CANDIDATE" for item in recommendations)
        return {
            "status": "PAPER_CANDIDATES_PRESENT" if active_count else "NO_ACTIVE_PAPER_CANDIDATE",
            "generated_at": now.isoformat(),
            "source": self.source,
            "recommendation_kind": "DETERMINISTIC_RULE_BASED_NOT_AI_MODEL_OUTPUT",
            "ai_review": "NOT_CONFIGURED; signal levels are produced by deterministic scanner rules",
            "paper_capital": self.paper_capital,
            "bar_construction": "APPROXIMATE_LTP_SNAPSHOT_OHLC",
            "volume_semantics": "UNAVAILABLE_FOR_CUMULATIVE_QUOTE_SNAPSHOTS",
            "requested_symbols": sorted(self.requested_symbols),
            "fresh_symbols": sorted(fresh & self.requested_symbols),
            "quality": dict(self._quality),
            "scanner_health": self.scanner.health(),
            "recommendation_count": len(recommendations),
            "active_candidate_count": active_count,
            "paper_only": True,
            "live_trading_enabled": False,
            "broker_execution_enabled": False,
            "orders_created": False,
            "ledger_mutated": False,
            "recommendations": recommendations,
        }


def write_recommendation_snapshot(path: str | Path, payload: dict[str, object]) -> Path:
    """Atomically replace the paper recommendation snapshot, never a ledger."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=destination.parent,
            prefix=f".{destination.name}.", suffix=".tmp", delete=False,
        ) as stream:
            temporary_name = stream.name
            json.dump(payload, stream, sort_keys=True, indent=2, allow_nan=False, default=_json_default)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return destination


def _json_default(value: object) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__} in paper recommendation snapshot")


def _timestamp_iso(value: object) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None
