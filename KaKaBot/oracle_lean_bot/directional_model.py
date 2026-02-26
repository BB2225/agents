"""
directional_model.py – Z-Score Directional Model.

Signal chain:
  1. distance = btc_price - strike_price
  2. volatility = rolling_std(120s log returns)
  3. z_score = distance / (vol * sqrt(time_remaining) + eps)
  4. prob_up = norm.cdf(z_score)
  5. edge = prob_up - poly_yes
  6. surplus_cap = f(|Z|)
"""
from __future__ import annotations

import logging
import math
import time
from collections import deque
from typing import Optional, Tuple

from scipy.stats import norm

log = logging.getLogger("lean_bot")

EPSILON = 1e-8


class RollingVolatilityTracker:
    """Rolling volatility of BTC log returns over a configurable window."""

    def __init__(self, window_sec: int = 120, max_samples: int = 500):
        self.window_sec = window_sec
        self._prices: deque[Tuple[float, float]] = deque(maxlen=max_samples)

    def update(self, price: float, ts: Optional[float] = None) -> None:
        ts = ts or time.time()
        self._prices.append((ts, price))
        cutoff = ts - self.window_sec * 1.5
        while self._prices and self._prices[0][0] < cutoff:
            self._prices.popleft()

    def get_volatility(self) -> float:
        """Rolling std of 1-second log returns. Returns 0 if < 10 samples."""
        now = time.time()
        cutoff = now - self.window_sec
        pts = [(t, p) for t, p in self._prices if t >= cutoff]
        if len(pts) < 10:
            return 0.0
        log_returns = []
        for i in range(1, len(pts)):
            if pts[i - 1][1] > 0 and pts[i][1] > 0:
                log_returns.append(math.log(pts[i][1] / pts[i - 1][1]))
        if len(log_returns) < 5:
            return 0.0
        mean = sum(log_returns) / len(log_returns)
        var = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
        return math.sqrt(var)

    @property
    def sample_count(self) -> int:
        return len(self._prices)

    @property
    def latest_price(self) -> Optional[float]:
        return self._prices[-1][1] if self._prices else None


class DirectionalModel:
    """Z-Score directional model for Polymarket 15-min BTC binary markets."""

    def __init__(
        self,
        edge_threshold: float = 0.03,
        z_lean_entry: float = 1.15,
        z_lean_moderate: float = 1.75,
        z_lean_strong: float = 2.4,
        z_lean_aggressive: float = 3.2,
        surplus_cap_z1: float = 0.15,
        surplus_cap_z2: float = 0.25,
        surplus_cap_z3: float = 0.40,
    ):
        self.edge_threshold = edge_threshold
        self.z_lean_entry = z_lean_entry
        self.z_lean_moderate = z_lean_moderate
        self.z_lean_strong = z_lean_strong
        self.z_lean_aggressive = z_lean_aggressive
        self.surplus_cap_z1 = surplus_cap_z1
        self.surplus_cap_z2 = surplus_cap_z2
        self.surplus_cap_z3 = surplus_cap_z3

    def compute_z_score(self, btc_price: float, strike: float, vol: float, time_sec: float) -> float:
        """z = (btc - strike) / (vol * sqrt(time) + eps)"""
        distance = btc_price - strike
        return distance / (vol * math.sqrt(max(0, time_sec)) + EPSILON)

    def compute_implied_probability(self, z: float) -> Tuple[float, float]:
        prob_up = norm.cdf(z)
        return prob_up, 1.0 - prob_up

    def compute_edge(self, z: float, poly_yes: float, poly_no: float) -> Tuple[float, float, str]:
        """Returns (edge, side_price, direction). direction = "UP"/"DOWN"/"NONE"."""
        prob_up, prob_down = self.compute_implied_probability(z)
        edge_up = prob_up - poly_yes
        edge_down = prob_down - poly_no
        if edge_up > edge_down and edge_up > self.edge_threshold:
            return edge_up, poly_yes, "UP"
        elif edge_down > edge_up and edge_down > self.edge_threshold:
            return edge_down, poly_no, "DOWN"
        return max(edge_up, edge_down), 0.0, "NONE"

    def calculate_surplus_cap(self, z: float) -> float:
        """Map |Z| -> surplus cap: <1=0%, 1-2=15%, 2-3=25%, >3=40%."""
        abs_z = abs(z)
        if abs_z < 1.0:
            return 0.0
        elif abs_z < 2.0:
            return self.surplus_cap_z1
        elif abs_z < 3.0:
            return self.surplus_cap_z2
        return self.surplus_cap_z3

    def get_size_multiplier(self, z: float) -> float:
        """Map |Z| -> size multiplier (15-market calibration)."""
        abs_z = abs(z)
        if abs_z < self.z_lean_entry:
            return 1.0
        elif abs_z < self.z_lean_moderate:
            return 1.3
        elif abs_z < self.z_lean_strong:
            return 1.8
        elif abs_z < self.z_lean_aggressive:
            return 2.5
        return 3.5

    def lean_direction(self, z: float) -> str:
        if z > 0:
            return "UP"
        elif z < 0:
            return "DOWN"
        return "NEUTRAL"
