"""
inventory.py – Inventory tracking, dynamic order sizing, flip threshold.

Per-market:
  - YES/NO share counts and cost basis
  - Min lot: 5 shares / $1 notional
  - Dynamic sizing with surplus gap
  - Time-scaled flip risk threshold
"""
from __future__ import annotations

import math
from dataclasses import dataclass

MIN_SHARES = 5
MIN_NOTIONAL = 1.0


@dataclass
class InventoryState:
    yes_qty: float = 0.0
    no_qty: float = 0.0
    yes_cost: float = 0.0
    no_cost: float = 0.0
    pairs_merged: int = 0
    total_realized_pnl: float = 0.0

    @property
    def net_inventory(self) -> float:
        return self.yes_qty - self.no_qty

    @property
    def total_inventory(self) -> float:
        return self.yes_qty + self.no_qty

    @property
    def surplus_pct(self) -> float:
        return abs(self.net_inventory) / max(self.total_inventory, 1)

    @property
    def matched_pairs(self) -> float:
        return min(self.yes_qty, self.no_qty)

    @property
    def avg_yes(self) -> float:
        return self.yes_cost / self.yes_qty if self.yes_qty > 0 else 0.0

    @property
    def avg_no(self) -> float:
        return self.no_cost / self.no_qty if self.no_qty > 0 else 0.0

    @property
    def pair_cost(self) -> float:
        return self.avg_yes + self.avg_no

    @property
    def unrealized_pnl(self) -> float:
        if self.matched_pairs <= 0:
            return 0.0
        return (1.0 - self.pair_cost) * self.matched_pairs

    def record_fill(self, side: str, price: float, size: float) -> None:
        if side in ("UP", "YES"):
            self.yes_qty += size
            self.yes_cost += price * size
        else:
            self.no_qty += size
            self.no_cost += price * size

    def record_merge(self, pairs: float) -> float:
        actual = min(pairs, self.matched_pairs)
        if actual <= 0:
            return 0.0
        realized = (1.0 - self.pair_cost) * actual
        self.total_realized_pnl += realized
        self.pairs_merged += int(actual)
        if self.yes_qty > 0:
            r = self.yes_cost / self.yes_qty
            self.yes_cost = max(0.0, self.yes_cost - r * actual)
            self.yes_qty = max(0.0, self.yes_qty - actual)
        if self.no_qty > 0:
            r = self.no_cost / self.no_qty
            self.no_cost = max(0.0, self.no_cost - r * actual)
            self.no_qty = max(0.0, self.no_qty - actual)
        return realized


def calculate_min_lot(price: float) -> int:
    """Min lot satisfying 5 shares AND $1 notional."""
    if price <= 0:
        return MIN_SHARES
    return max(MIN_SHARES, math.ceil(MIN_NOTIONAL / price))


def calculate_dynamic_order_size(
    price: float, surplus_gap: int, max_trade_cap: int, size_multiplier: float = 1.0,
) -> int:
    min_lot = calculate_min_lot(price)
    if surplus_gap <= 0:
        return 0
    base_size = int(min_lot * 1.5 * size_multiplier)
    return min(max(min_lot, base_size), surplus_gap, max_trade_cap)


def calculate_imbalance_tolerance(price: float) -> float:
    return 1.5 * calculate_min_lot(price)


def calculate_surplus_target(total_inv: float, surplus_cap: float, net_inv: float) -> int:
    target = int(total_inv * surplus_cap)
    return max(0, target - int(abs(net_inv)))


def calculate_flip_threshold(btc_price: float, time_remaining: float) -> float:
    """Time-scaled flip threshold. base = 0.03% of BTC, min $10."""
    base = 0.0003 * btc_price
    return max(10.0, base * math.sqrt(max(0, time_remaining) / 60.0))


def is_flip_risk(btc_price: float, strike: float, time_remaining: float) -> bool:
    return abs(btc_price - strike) <= calculate_flip_threshold(btc_price, time_remaining)
