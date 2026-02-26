"""
state_machine.py – 6-state machine for the Hybrid Lean Bot.

States: NEUTRAL_MM, MOMENTUM_LEAN, CONTROLLED_LEAN_FINAL,
        HIGH_FLIP_RISK, EMERGENCY_FLATTEN, DORMANT

All transitions are logged with timestamp and reason.
"""
from __future__ import annotations

import logging
import time
from enum import Enum
from typing import Optional

log = logging.getLogger("lean_bot")


class BotState(Enum):
    NEUTRAL_MM = "NEUTRAL_MM"
    MOMENTUM_LEAN = "MOMENTUM_LEAN"
    CONTROLLED_LEAN_FINAL = "CONTROLLED_LEAN_FINAL"
    HIGH_FLIP_RISK = "HIGH_FLIP_RISK"
    EMERGENCY_FLATTEN = "EMERGENCY_FLATTEN"
    DORMANT = "DORMANT"


class StateMachine:
    """Per-market state machine with transition logging."""

    def __init__(self, market_id: str):
        self.market_id = market_id[:12]
        self._state = BotState.NEUTRAL_MM
        self._prev_state: Optional[BotState] = None
        self._state_entered_at: float = time.time()
        self._transition_log: list[tuple[float, str, str, str]] = []
        log.info(f"[SM:{self.market_id}] Initialized in {self._state.value}")

    @property
    def state(self) -> BotState:
        return self._state

    @property
    def time_in_state(self) -> float:
        return time.time() - self._state_entered_at

    def transition(self, new_state: BotState, reason: str = "") -> None:
        if new_state == self._state:
            return
        old = self._state
        self._prev_state = old
        self._state = new_state
        self._state_entered_at = time.time()
        self._transition_log.append((time.time(), old.value, new_state.value, reason))
        log.warning(f"[SM:{self.market_id}] {old.value} -> {new_state.value} | {reason}")

    def evaluate(
        self,
        z_score: float, prob_edge: float, edge_threshold: float,
        flip_risk: bool, time_remaining: float,
        final_window_sec: int, emergency_sec: int,
        net_profit: float, profit_exit: float,
    ) -> BotState:
        """
        Evaluate and transition. Priority (highest first):
          1. DORMANT — profit exit / market ended
          2. EMERGENCY_FLATTEN — flip risk + final minute
          3. CONTROLLED_LEAN_FINAL — final 45s
          4. HIGH_FLIP_RISK — price near strike
          5. MOMENTUM_LEAN — Z-score with edge
          6. NEUTRAL_MM — default
        """
        if net_profit >= profit_exit:
            self.transition(BotState.DORMANT, f"profit_exit: ${net_profit:.2f}>=${profit_exit:.2f}")
            return self._state
        if time_remaining <= 0:
            self.transition(BotState.DORMANT, "market_ended")
            return self._state
        if flip_risk and time_remaining <= emergency_sec:
            self.transition(BotState.EMERGENCY_FLATTEN, f"flip_risk+T-{time_remaining:.0f}s<={emergency_sec}s")
            return self._state
        if self._state in (BotState.EMERGENCY_FLATTEN, BotState.DORMANT):
            return self._state
        if time_remaining <= final_window_sec:
            self.transition(BotState.CONTROLLED_LEAN_FINAL, f"final_window:T-{time_remaining:.0f}s")
            return self._state
        if flip_risk:
            self.transition(BotState.HIGH_FLIP_RISK, "flip_risk")
            return self._state

        # Directional lean
        if abs(z_score) >= 1.0 and prob_edge > edge_threshold:
            if self._state != BotState.MOMENTUM_LEAN:
                self.transition(BotState.MOMENTUM_LEAN, f"z={z_score:.2f} edge={prob_edge:.3f}")
            return self._state

        # Default neutral
        if self._state != BotState.NEUTRAL_MM:
            self.transition(BotState.NEUTRAL_MM, f"z={z_score:.2f} edge={prob_edge:.3f}")
        return self._state

    def reset(self) -> None:
        self._prev_state = self._state
        self._state = BotState.NEUTRAL_MM
        self._state_entered_at = time.time()
        self._transition_log.clear()
        log.info(f"[SM:{self.market_id}] Reset to NEUTRAL_MM")

    @property
    def transitions(self) -> list[tuple[float, str, str, str]]:
        return list(self._transition_log)
