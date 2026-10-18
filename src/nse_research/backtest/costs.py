"""Indian cash-equity costs, applied to each execution leg."""

from __future__ import annotations

from dataclasses import dataclass

from nse_research.config import CostSettings


@dataclass(frozen=True)
class CostBreakdown:
    brokerage: float = 0.0
    stt: float = 0.0
    exchange: float = 0.0
    sebi: float = 0.0
    gst: float = 0.0
    stamp: float = 0.0
    dp: float = 0.0

    @property
    def total(self) -> float:
        return self.brokerage + self.stt + self.exchange + self.sebi + self.gst + self.stamp + self.dp


class CostModel:
    """Configurable approximation to NSE equity taxes and a retail broker's fees."""

    def __init__(self, settings: CostSettings) -> None:
        self.settings = settings

    def order_cost(self, price: float, quantity: int, side: str, intraday: bool, is_exit: bool = False) -> CostBreakdown:
        if price <= 0 or quantity <= 0 or side not in {"buy", "sell"}:
            raise ValueError("positive price/quantity and side buy or sell are required")
        value = price * quantity
        cfg = self.settings
        brokerage_rate = cfg.intraday_brokerage_rate if intraday else cfg.delivery_brokerage_rate
        brokerage = min(value * brokerage_rate, cfg.brokerage_cap_per_order) if brokerage_rate else 0.0
        stt_rate = cfg.intraday_sell_stt_rate if intraday and side == "sell" else (0.0 if intraday else cfg.delivery_stt_rate)
        stt = value * stt_rate
        exchange = value * cfg.exchange_transaction_rate
        sebi = value * cfg.sebi_rate
        gst = (brokerage + exchange + sebi) * cfg.gst_rate
        stamp_rate = cfg.intraday_stamp_buy_rate if intraday else cfg.delivery_stamp_buy_rate
        stamp = value * stamp_rate if side == "buy" else 0.0
        dp = cfg.delivery_dp_charge_per_sell if (not intraday and side == "sell" and is_exit) else 0.0
        return CostBreakdown(brokerage, stt, exchange, sebi, gst, stamp, dp)

    def round_trip(
        self,
        entry: float,
        exit: float,
        quantity: int,
        direction: int,
        intraday: bool,
    ) -> tuple[float, CostBreakdown, CostBreakdown]:
        entry_side = "buy" if direction > 0 else "sell"
        exit_side = "sell" if direction > 0 else "buy"
        entry_cost = self.order_cost(entry, quantity, entry_side, intraday, is_exit=False)
        exit_cost = self.order_cost(exit, quantity, exit_side, intraday, is_exit=True)
        return entry_cost.total + exit_cost.total, entry_cost, exit_cost

    def slippage_price(self, price: float, direction: int, is_entry: bool) -> float:
        adverse_sign = direction if is_entry else -direction
        return price * (1 + adverse_sign * self.settings.slippage_bps / 10000)
