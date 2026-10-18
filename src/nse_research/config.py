"""Typed application configuration loaded from YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ProjectSettings(StrictModel):
    name: str = "nse-equity-research"
    timezone: str = "Asia/Kolkata"
    random_seed: int = 20260925
    result_root: str = "results"


class BacktestSettings(StrictModel):
    capital: float = Field(gt=0, default=100000)
    capital_scenarios: list[float] = Field(default_factory=lambda: [25000, 50000, 100000])
    lookback_months: int = Field(ge=1, default=2)
    start_date: str | None = None
    end_date: str | None = None
    warmup_calendar_days: int = Field(ge=250, default=370)
    interval: str = "1d"
    fill_at_next_bar_open: bool = True
    ambiguous_stop_target: str = "stop_first"

    @field_validator("capital_scenarios")
    @classmethod
    def scenarios_positive(cls, value: list[float]) -> list[float]:
        if not value or any(amount <= 0 for amount in value):
            raise ValueError("capital_scenarios must contain positive values")
        return value


class UniverseSettings(StrictModel):
    source: str = "nse_nifty500"
    constituents_url: str
    symbols_file: str
    min_history_rows: int = 220
    min_avg_volume: float = 500000
    min_avg_traded_value: float = 50000000
    min_price: float = 50
    liquidity_lookback: int = 20
    max_symbols: int | None = None
    benchmark: str = "^NSEI"


class RegimeSettings(StrictModel):
    ema_fast: int = 20
    ema_slow: int = 50
    ema_long: int = 200
    adx_period: int = 14
    adx_trend_threshold: float = 20
    india_vix_symbol: str = "^INDIAVIX"
    india_vix_high_threshold: float = 25.0


class StrategySettings(StrictModel):
    enabled: list[str]
    reward_risk: float = Field(gt=0, default=1.5)
    atr_period: int = 14
    atr_stop_multiple: float = 2.0
    adx_minimum: float = 20
    volume_multiple: float = 1.3
    rsi_period: int = 14
    entry_cutoff: str = "14:45"
    intraday_squareoff: str = "15:25"
    exit_mode: str = "fixed_r"
    exit_comparison_modes: list[str] = Field(default_factory=lambda: [
        "stop_loss", "fixed_r", "partial_trailing", "atr_trailing", "max_holding"
    ])
    partial_exit_fraction: float = Field(gt=0, lt=1, default=0.5)
    atr_trailing_multiple: float = Field(gt=0, default=2.0)
    stop_mode: str = "atr"
    reward_risk_choices: list[float] = Field(default_factory=lambda: [1, 1.5, 2, 2.5, 3])
    signal_score_weights: dict[str, float] = Field(default_factory=lambda: {
        "trend": 0.25, "momentum": 0.20, "volume": 0.15,
        "regime": 0.15, "liquidity": 0.10, "volatility": 0.10, "risk_reward": 0.05,
    })

    @field_validator("exit_mode")
    @classmethod
    def valid_exit_mode(cls, value: str) -> str:
        if value not in {"stop_loss", "fixed_r", "partial_trailing", "atr_trailing", "max_holding"}:
            raise ValueError("unsupported exit_mode")
        return value

    @field_validator("exit_comparison_modes")
    @classmethod
    def valid_exit_modes(cls, value: list[str]) -> list[str]:
        allowed = {"stop_loss", "fixed_r", "partial_trailing", "atr_trailing", "max_holding"}
        if not value or any(mode not in allowed for mode in value):
            raise ValueError("exit_comparison_modes contains an unsupported mode")
        return value

    @field_validator("stop_mode")
    @classmethod
    def valid_stop_mode(cls, value: str) -> str:
        if value not in {"atr", "swing_low"}:
            raise ValueError("stop_mode must be 'atr' or 'swing_low'")
        return value

    @field_validator("reward_risk_choices")
    @classmethod
    def valid_reward_risk_choices(cls, value: list[float]) -> list[float]:
        if not value or any(item <= 0 for item in value):
            raise ValueError("reward_risk_choices must contain positive values")
        return value


class RiskSettings(StrictModel):
    risk_per_trade: float = Field(gt=0, le=0.01, default=0.01)
    max_daily_loss: float = Field(gt=0, le=1, default=0.02)
    max_portfolio_drawdown: float = Field(gt=0, le=1, default=0.10)
    max_open_positions: int = Field(ge=1, default=5)
    max_single_position_fraction: float = Field(gt=0, le=1, default=0.25)
    max_gross_exposure_fraction: float = Field(gt=0, le=1, default=1.0)
    max_sector_exposure_fraction: float = Field(gt=0, le=1, default=0.25)
    max_expected_slippage_bps: float = Field(ge=0, default=10.0)
    allow_short_intraday: bool = True


class CostSettings(StrictModel):
    exchange: str = "NSE"
    intraday_brokerage_rate: float = 0.0003
    brokerage_cap_per_order: float = 20
    delivery_brokerage_rate: float = 0
    delivery_stt_rate: float = 0.001
    intraday_sell_stt_rate: float = 0.00025
    exchange_transaction_rate: float = 0.0000307
    sebi_rate: float = 0.000001
    gst_rate: float = 0.18
    delivery_stamp_buy_rate: float = 0.00015
    intraday_stamp_buy_rate: float = 0.00003
    delivery_dp_charge_per_sell: float = 15.34
    slippage_bps: float = Field(ge=0, default=5)


class PaperSettings(StrictModel):
    enabled: bool = True
    database: str = "data/paper_trades.sqlite"


class ForwardPaperSettings(StrictModel):
    enabled: bool = False
    starting_capital: float | None = Field(default=None, gt=0)
    provider: str = "licensed_nse_cm_eod"
    licensed_dataset_path: str = "data/licensed_nse_cm_eod/nse_cm_eod_dataset.csv"
    market_data_dir: str = "forward/market_data"
    daily_data_dir: str = "data/paper_trading/normalized"
    identity_file: str = "data/universe.csv"
    index_data_dir: str = "data/nse_exports"
    database: str = "data/paper_trading/forward_paper.sqlite"
    trading_calendar_path: str = "data/paper_trading/nse_cash_holidays_2026.csv"
    baseline_manifest_path: str = "data/paper_trading/PHASE3C_BASELINE.json"
    baseline_name: str = "PHASE3C_BASELINE"
    output_root: str = "results/phase3d_forward"
    experiment_manifest_path: str | None = None
    experiment_start_date: str | None = None
    experiment_end_date: str | None = None
    target_sessions: int | None = Field(default=None, ge=1)
    max_daily_price_jump_fraction: float = Field(gt=0, default=0.40)


class LiveSettings(StrictModel):
    enabled: bool = False
    kill_switch_file: str = "HALT_LIVE_TRADING"


class DataSettings(StrictModel):
    cache_dir: str = "data/cache"
    raw_dir: str = "data/raw"
    processed_dir: str = "data/processed"
    provider: str = "yfinance"
    auto_adjust: bool = True
    intraday_max_age_days: int = 59


class RealtimeSettings(StrictModel):
    """Explicitly opt-in, data-only intraday feed settings."""

    enabled: bool = False
    provider: str = ""
    max_quote_age_seconds: float = Field(gt=0, default=5.0)
    max_future_skew_seconds: float = Field(ge=0, default=1.0)
    candle_intervals: list[str] = Field(default_factory=lambda: ["1m", "5m"])
    preopen_start: str = "09:00"
    market_open: str = "09:15"
    market_close: str = "15:30"
    symbols: list[str] = Field(default_factory=list)
    recording: bool = False
    recording_dir: str = "data/realtime/recordings"
    health_snapshot_path: str = "data/realtime/feed_health.json"
    max_retries: int = Field(ge=0, le=5, default=2)
    retry_backoff_seconds: float = Field(gt=0, le=30, default=0.5)

    @field_validator("candle_intervals")
    @classmethod
    def supported_candle_intervals(cls, value: list[str]) -> list[str]:
        if not value or len(set(value)) != len(value):
            raise ValueError("candle_intervals must contain unique intervals")
        for interval in value:
            if not isinstance(interval, str) or not interval.endswith("m"):
                raise ValueError("candle intervals must be minute intervals such as 1m or 5m")
            try:
                minutes = int(interval[:-1])
            except ValueError as error:
                raise ValueError("candle intervals must be minute intervals such as 1m or 5m") from error
            if minutes < 1 or minutes > 60:
                raise ValueError("candle intervals must be between 1m and 60m")
        return value

    @field_validator("preopen_start", "market_open", "market_close")
    @classmethod
    def valid_market_time(cls, value: str) -> str:
        from datetime import time

        try:
            time.fromisoformat(value)
        except (TypeError, ValueError) as error:
            raise ValueError("market session times must use HH:MM format") from error
        if len(value) != 5:
            raise ValueError("market session times must use HH:MM format")
        return value

    @model_validator(mode="after")
    def validate_session_order(self) -> "RealtimeSettings":
        from datetime import time

        preopen = time.fromisoformat(self.preopen_start)
        opening = time.fromisoformat(self.market_open)
        closing = time.fromisoformat(self.market_close)
        if not preopen < opening < closing:
            raise ValueError("realtime session times must satisfy pre-open < open < close")
        return self

    @field_validator("symbols")
    @classmethod
    def normalize_symbols(cls, value: list[str]) -> list[str]:
        symbols = [symbol.strip().upper() for symbol in value]
        if any(not symbol for symbol in symbols) or len(set(symbols)) != len(symbols):
            raise ValueError("realtime symbols must be non-empty and unique")
        return symbols


class AppConfig(StrictModel):
    project: ProjectSettings
    backtest: BacktestSettings
    universe: UniverseSettings
    regime: RegimeSettings
    strategies: StrategySettings
    risk: RiskSettings
    costs: CostSettings
    paper: PaperSettings
    forward_paper: ForwardPaperSettings = Field(default_factory=ForwardPaperSettings)
    live_trading: LiveSettings
    data: DataSettings
    realtime: RealtimeSettings = Field(default_factory=RealtimeSettings)

    @classmethod
    def load(cls, path: str | Path) -> "AppConfig":
        with Path(path).open("r", encoding="utf-8") as stream:
            payload: Any = yaml.safe_load(stream)
        if not isinstance(payload, dict):
            raise ValueError("Configuration must be a YAML mapping")
        return cls.model_validate(payload)
