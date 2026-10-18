"""Common signal model and strategy interface."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class Candidate:
    signal_time: pd.Timestamp
    direction: int
    stop_distance: float
    reward_risk: float
    score: float
    regime: str
    reason: str
    source_strategy: str | None = None


class Strategy(Protocol):
    name: str
    intraday_only: bool
    max_holding_bars: int

    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        """Emit close-of-bar signals; execution must occur at a later bar."""
