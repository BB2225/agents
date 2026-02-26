"""
market_context.py – Per-market context combining all components.

Each active market has: state machine, inventory, orderbook, signals, CSV logger.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from .state_machine import StateMachine, BotState
from .directional_model import DirectionalModel
from .inventory import (
    InventoryState, calculate_flip_threshold, is_flip_risk,
)
from .csv_logger import CSVLogger

log = logging.getLogger("lean_bot")
STATE_DIR = "state_data"


@dataclass
class OpenOrder:
    order_id: str
    token_id: str
    side: str       # "YES" or "NO"
    price: float
    size: float
    placed_at: float


@dataclass
class MarketContext:
    """Complete per-market state."""

    condition_id: str
    token_yes: str
    token_no: str
    start_ts: int
    end_ts: int
    tick_size: float = 0.01
    strike_price: float = 0.0

    state_machine: StateMachine = field(default=None)  # type: ignore
    inventory: InventoryState = field(default_factory=InventoryState)
    csv_logger: CSVLogger = field(default=None)  # type: ignore

    # Live orderbook
    best_bid_yes: float = 0.0
    best_ask_yes: float = 1.0
    best_bid_no: float = 0.0
    best_ask_no: float = 1.0
    clob_updated_yes: float = 0.0
    clob_updated_no: float = 0.0

    # Computed signals
    z_score: float = 0.0
    prob_up: float = 0.5
    prob_down: float = 0.5
    edge_magnitude: float = 0.0
    edge_direction: str = "NONE"
    surplus_cap: float = 0.0
    size_multiplier: float = 1.0
    flip_threshold: float = 0.0

    # Order tracking
    open_orders: Dict[str, OpenOrder] = field(default_factory=dict)
    _seen_fill_ids: Set[str] = field(default_factory=set, repr=False)
    _all_placed_order_ids: Set[str] = field(default_factory=set, repr=False)

    def __post_init__(self):
        if self.state_machine is None:
            self.state_machine = StateMachine(self.condition_id)
        if self.csv_logger is None:
            self.csv_logger = CSVLogger(self.condition_id)

    @property
    def time_remaining(self) -> float:
        return max(0, self.end_ts - time.time())

    @property
    def mid_yes(self) -> float:
        if 0 < self.best_bid_yes < self.best_ask_yes < 1:
            return (self.best_bid_yes + self.best_ask_yes) / 2
        return 0.5

    @property
    def mid_no(self) -> float:
        if 0 < self.best_bid_no < self.best_ask_no < 1:
            return (self.best_bid_no + self.best_ask_no) / 2
        return 0.5

    @property
    def poly_yes(self) -> float:
        return self.best_bid_yes if self.best_bid_yes > 0 else self.mid_yes

    @property
    def poly_no(self) -> float:
        return self.best_bid_no if self.best_bid_no > 0 else self.mid_no

    @property
    def state(self) -> BotState:
        return self.state_machine.state

    @property
    def is_active(self) -> bool:
        return self.time_remaining > 0 and self.state != BotState.DORMANT

    def update_orderbook(self, token_id: str, bids: list, asks: list) -> None:
        best_bid = float(bids[0]["price"]) if bids else 0.0
        best_ask = float(asks[0]["price"]) if asks else 1.0
        now = time.time()
        if token_id == self.token_yes:
            self.best_bid_yes, self.best_ask_yes, self.clob_updated_yes = best_bid, best_ask, now
        elif token_id == self.token_no:
            self.best_bid_no, self.best_ask_no, self.clob_updated_no = best_bid, best_ask, now

    def record_fill(self, fill_id: str, token_id: str, price: float, size: float) -> bool:
        if fill_id in self._seen_fill_ids:
            return False
        self._seen_fill_ids.add(fill_id)
        side = "YES" if token_id == self.token_yes else "NO"
        self.inventory.record_fill(side, price, size)
        self.open_orders.pop(fill_id, None)
        log.info(
            f"[FILL:{self.condition_id[:8]}] {side} {size}@{price:.4f} "
            f"inv={self.inventory.yes_qty:.0f}Y/{self.inventory.no_qty:.0f}N"
        )
        return True

    def compute_signals(self, model: DirectionalModel, btc_price: float, vol: float) -> None:
        tr = self.time_remaining
        self.z_score = model.compute_z_score(btc_price, self.strike_price, vol, tr)
        self.prob_up, self.prob_down = model.compute_implied_probability(self.z_score)
        self.edge_magnitude, _, self.edge_direction = model.compute_edge(self.z_score, self.poly_yes, self.poly_no)
        self.surplus_cap = model.calculate_surplus_cap(self.z_score)
        self.size_multiplier = model.get_size_multiplier(self.z_score)
        self.flip_threshold = calculate_flip_threshold(btc_price, tr)

    def evaluate_state(
        self, btc_price: float, edge_threshold: float,
        final_window_sec: int, emergency_sec: int, profit_exit: float,
    ) -> BotState:
        flip = is_flip_risk(btc_price, self.strike_price, self.time_remaining)
        net_profit = self.inventory.total_realized_pnl + self.inventory.unrealized_pnl
        return self.state_machine.evaluate(
            z_score=self.z_score, prob_edge=self.edge_magnitude,
            edge_threshold=edge_threshold, flip_risk=flip,
            time_remaining=self.time_remaining,
            final_window_sec=final_window_sec, emergency_sec=emergency_sec,
            net_profit=net_profit, profit_exit=profit_exit,
        )

    def log_csv_tick(self, btc_price: float) -> None:
        self.csv_logger.log_tick(
            market_id=self.condition_id, state=self.state.value,
            oracle_price=btc_price, strike=self.strike_price,
            z_score=self.z_score, prob_up=self.prob_up,
            poly_yes=self.poly_yes, poly_no=self.poly_no,
            flip_threshold=self.flip_threshold,
            yes_price=self.best_bid_yes, no_price=self.best_bid_no,
            net_inventory=self.inventory.net_inventory,
            surplus_pct=self.inventory.surplus_pct,
            matched_pairs=self.inventory.matched_pairs,
            realized_pnl=self.inventory.total_realized_pnl,
            unrealized_pnl=self.inventory.unrealized_pnl,
            time_remaining=self.time_remaining,
            surplus_cap=self.surplus_cap, size_multiplier=self.size_multiplier,
            edge_direction=self.edge_direction, edge_magnitude=self.edge_magnitude,
        )

    def log_summary(self) -> None:
        inv = self.inventory
        log.info(
            f"[MKT:{self.condition_id[:8]}] state={self.state.value} z={self.z_score:+.2f} "
            f"edge={self.edge_direction}:{self.edge_magnitude:.3f} "
            f"inv={inv.yes_qty:.0f}Y/{inv.no_qty:.0f}N surplus={inv.surplus_pct:.1%} "
            f"cap={self.surplus_cap:.0%} pairs={inv.matched_pairs:.0f} "
            f"pnl=${inv.total_realized_pnl:.2f}+${inv.unrealized_pnl:.2f} T-{self.time_remaining:.0f}s"
        )

    def save(self) -> None:
        os.makedirs(STATE_DIR, exist_ok=True)
        safe_id = self.condition_id.replace("0x", "")[:16]
        path = os.path.join(STATE_DIR, f"lean_{safe_id}.json")
        data = {
            "condition_id": self.condition_id, "token_yes": self.token_yes,
            "token_no": self.token_no, "start_ts": self.start_ts, "end_ts": self.end_ts,
            "strike_price": self.strike_price,
            "yes_qty": self.inventory.yes_qty, "no_qty": self.inventory.no_qty,
            "yes_cost": self.inventory.yes_cost, "no_cost": self.inventory.no_cost,
            "pairs_merged": self.inventory.pairs_merged,
            "total_realized_pnl": self.inventory.total_realized_pnl,
            "seen_fill_ids": list(self._seen_fill_ids),
        }
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)

    @classmethod
    def load_or_create(
        cls, condition_id: str, token_yes: str, token_no: str,
        start_ts: int, end_ts: int, tick_size: float = 0.01, strike_price: float = 0.0,
    ) -> "MarketContext":
        ctx = cls(
            condition_id=condition_id, token_yes=token_yes, token_no=token_no,
            start_ts=start_ts, end_ts=end_ts, tick_size=tick_size, strike_price=strike_price,
        )
        safe_id = condition_id.replace("0x", "")[:16]
        path = os.path.join(STATE_DIR, f"lean_{safe_id}.json")
        if os.path.exists(path):
            try:
                with open(path) as f:
                    d = json.load(f)
                if d.get("condition_id") == condition_id:
                    ctx.strike_price = d.get("strike_price", strike_price)
                    ctx.inventory.yes_qty = d.get("yes_qty", 0.0)
                    ctx.inventory.no_qty = d.get("no_qty", 0.0)
                    ctx.inventory.yes_cost = d.get("yes_cost", 0.0)
                    ctx.inventory.no_cost = d.get("no_cost", 0.0)
                    ctx.inventory.pairs_merged = d.get("pairs_merged", 0)
                    ctx.inventory.total_realized_pnl = d.get("total_realized_pnl", 0.0)
                    ctx._seen_fill_ids = set(d.get("seen_fill_ids", []))
            except Exception as e:
                log.warning(f"[CTX] Could not load state: {e}")
        return ctx
