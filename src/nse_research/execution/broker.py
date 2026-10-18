"""Execution ports for the paper workflow and an explicitly disabled live guard."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from nse_research.config import AppConfig
from nse_research.risk.controls import kill_switch_active


@dataclass(frozen=True)
class SimulatedFill:
    symbol: str
    direction: str
    quantity: int
    price: float
    timestamp: datetime
    order_id: str
    simulated: bool = True


class ExecutionBroker(ABC):
    @abstractmethod
    def place(self, order: dict[str, Any]) -> SimulatedFill:
        """Return an execution result. The paper implementation never sends an order."""


class PaperBroker(ExecutionBroker):
    def place(self, order: dict[str, Any]) -> SimulatedFill:
        from uuid import uuid4

        return SimulatedFill(
            symbol=str(order["symbol"]),
            direction=str(order["direction"]),
            quantity=int(order["quantity"]),
            price=float(order["entry_estimate"]),
            timestamp=datetime.now(),
            order_id=f"PAPER-{uuid4().hex[:12]}",
        )


class LiveExecutionGuard(ExecutionBroker):
    """A fail-closed placeholder; this release cannot transmit live orders."""

    def __init__(self, config: AppConfig, project_root: str = ".") -> None:
        self.config = config
        self.project_root = project_root

    def place(self, order: dict[str, Any]) -> SimulatedFill:
        switch = f"{self.project_root.rstrip('/')}/{self.config.live_trading.kill_switch_file}"
        if not self.config.live_trading.enabled:
            raise RuntimeError("Live order rejected: live_trading.enabled is false")
        if kill_switch_active(switch):
            raise RuntimeError("Live order rejected: emergency kill switch is active")
        raise RuntimeError("Live order rejected: live execution adapter is intentionally not implemented")
