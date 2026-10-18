"""Union-of-signals wrapper for risk-limited research portfolios."""

from __future__ import annotations

from dataclasses import dataclass, replace

import pandas as pd

from nse_research.strategies.base import Candidate
from nse_research.strategies.equity import EquityStrategy


@dataclass
class CombinedEquityStrategy:
    """Combine signals while letting the backtest engine enforce one portfolio.

    When multiple member strategies signal the same symbol on the same bar, the
    highest score wins; ties use the member name for deterministic behavior.
    """

    name: str
    members: tuple[EquityStrategy, ...]
    intraday_only: bool = False

    @property
    def max_holding_bars(self) -> int:
        return max((member.max_holding_bars for member in self.members), default=1)

    def evaluate(self, features: pd.DataFrame, regimes: pd.Series) -> list[Candidate]:
        by_time: dict[pd.Timestamp, list[Candidate]] = {}
        for member in self.members:
            for candidate in member.evaluate(features, regimes):
                by_time.setdefault(candidate.signal_time, []).append(
                    replace(
                        candidate,
                        source_strategy=member.name,
                        reason=f"source={member.name}; {candidate.reason}",
                    )
                )
        return [
            sorted(candidates, key=lambda candidate: (-candidate.score, candidate.source_strategy or ""))[0]
            for _, candidates in sorted(by_time.items())
        ]
