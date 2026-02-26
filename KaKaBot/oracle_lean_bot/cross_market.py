"""
cross_market.py – Cross-market dampener for systemic risk control.

>=5 markets with |Z|>=2.0: reduce sizes 20%
>=8 markets with |Z|>=2.5: reduce sizes 35%
"""
from __future__ import annotations

import logging
from typing import Dict

log = logging.getLogger("lean_bot")


class CrossMarketDampener:
    def __init__(self, threshold_5_reduction: float = 0.20, threshold_8_reduction: float = 0.35):
        self._t5 = threshold_5_reduction
        self._t8 = threshold_8_reduction
        self._z_scores: Dict[str, float] = {}

    def update(self, market_id: str, z_score: float) -> None:
        self._z_scores[market_id] = z_score

    def remove(self, market_id: str) -> None:
        self._z_scores.pop(market_id, None)

    def get_dampener(self) -> float:
        c2 = sum(1 for z in self._z_scores.values() if abs(z) >= 2.0)
        c25 = sum(1 for z in self._z_scores.values() if abs(z) >= 2.5)
        if c25 >= 8:
            log.warning(f"[DAMPENER] {c25} markets |Z|>=2.5 — reducing {self._t8:.0%}")
            return 1.0 - self._t8
        if c2 >= 5:
            log.info(f"[DAMPENER] {c2} markets |Z|>=2.0 — reducing {self._t5:.0%}")
            return 1.0 - self._t5
        return 1.0

    @property
    def active_markets(self) -> int:
        return len(self._z_scores)
