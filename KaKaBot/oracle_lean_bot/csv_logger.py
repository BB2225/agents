"""
csv_logger.py – Per-tick CSV logging for the Hybrid Lean Bot.

22-column output: Timestamp | State | Oracle_Price | Strike | Z_Score | ...
"""
from __future__ import annotations

import csv
import logging
import os
import time
from typing import Optional

log = logging.getLogger("lean_bot")

CSV_DIR = "logs"
CSV_HEADERS = [
    "Timestamp", "Market_ID", "State", "Oracle_Price", "Strike",
    "Z_Score", "Prob_Up", "Poly_Yes", "Poly_No", "Flip_Threshold",
    "YES_Price", "NO_Price", "Net_Inventory", "Surplus_Pct",
    "Matched_Pairs", "Realized_PnL", "Unrealized_PnL", "Time_Remaining",
    "Surplus_Cap", "Size_Multiplier", "Edge_Direction", "Edge_Magnitude",
]


class CSVLogger:
    def __init__(self, market_id: str, session_id: Optional[str] = None):
        os.makedirs(CSV_DIR, exist_ok=True)
        safe_id = market_id.replace("0x", "")[:16]
        ts = session_id or str(int(time.time()))
        self._path = os.path.join(CSV_DIR, f"lean_{safe_id}_{ts}.csv")
        self._initialized = False

    def _ensure_header(self) -> None:
        if self._initialized:
            return
        if not os.path.exists(self._path):
            with open(self._path, "w", newline="") as f:
                csv.writer(f).writerow(CSV_HEADERS)
        self._initialized = True

    def log_tick(
        self, market_id: str, state: str, oracle_price: float, strike: float,
        z_score: float, prob_up: float, poly_yes: float, poly_no: float,
        flip_threshold: float, yes_price: float, no_price: float,
        net_inventory: float, surplus_pct: float, matched_pairs: float,
        realized_pnl: float, unrealized_pnl: float, time_remaining: float,
        surplus_cap: float = 0.0, size_multiplier: float = 1.0,
        edge_direction: str = "NONE", edge_magnitude: float = 0.0,
    ) -> None:
        self._ensure_header()
        try:
            with open(self._path, "a", newline="") as f:
                csv.writer(f).writerow([
                    f"{time.time():.3f}", market_id[:12], state,
                    f"{oracle_price:.2f}", f"{strike:.2f}", f"{z_score:.4f}",
                    f"{prob_up:.4f}", f"{poly_yes:.4f}", f"{poly_no:.4f}",
                    f"{flip_threshold:.2f}", f"{yes_price:.4f}", f"{no_price:.4f}",
                    f"{net_inventory:.1f}", f"{surplus_pct:.4f}", f"{matched_pairs:.0f}",
                    f"{realized_pnl:.4f}", f"{unrealized_pnl:.4f}", f"{time_remaining:.0f}",
                    f"{surplus_cap:.4f}", f"{size_multiplier:.2f}",
                    edge_direction, f"{edge_magnitude:.4f}",
                ])
        except Exception as e:
            log.error(f"[CSV] Write failed: {e}")

    @property
    def path(self) -> str:
        return self._path
