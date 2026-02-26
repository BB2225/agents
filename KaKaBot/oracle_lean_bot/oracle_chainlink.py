"""
oracle_chainlink.py – Chainlink BTC/USD price oracle for Polygon.
Background daemon thread polls aggregator V3 at ~1s.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from typing import Optional, Tuple

log = logging.getLogger("lean_bot")

CHAINLINK_BTC_USD_POLYGON = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
CHAINLINK_DECIMALS = 8
STALE_SEC = 10.0

AGGREGATOR_V3_ABI = [
    {
        "inputs": [], "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view", "type": "function",
    },
    {
        "inputs": [], "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view", "type": "function",
    },
]


class ChainlinkOracle:
    """High-frequency BTC/USD oracle via Chainlink on Polygon."""

    def __init__(self, rpc_url: str, aggregator_address: str = CHAINLINK_BTC_USD_POLYGON, poll_interval: float = 1.0):
        self._rpc_url = rpc_url
        self._aggregator_address = aggregator_address
        self._poll_interval = poll_interval
        self._price: Optional[float] = None
        self._last_update: float = 0.0
        self._round_id: int = 0
        self._history: deque[Tuple[float, float]] = deque(maxlen=1000)
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def get_price(self) -> Optional[float]:
        with self._lock:
            if time.time() - self._last_update > STALE_SEC:
                return None
            return self._price

    def get_price_and_time(self) -> Tuple[Optional[float], float]:
        with self._lock:
            age = time.time() - self._last_update if self._last_update > 0 else float("inf")
            return (None, age) if age > STALE_SEC else (self._price, age)

    def get_history(self, window_sec: float = 120.0) -> list[Tuple[float, float]]:
        cutoff = time.time() - window_sec
        with self._lock:
            return [(ts, p) for ts, p in self._history if ts >= cutoff]

    def is_alive(self) -> bool:
        with self._lock:
            return self._price is not None and time.time() - self._last_update < STALE_SEC

    @property
    def round_id(self) -> int:
        with self._lock:
            return self._round_id

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        log.info(f"[ORACLE] Chainlink BTC/USD started (poll={self._poll_interval}s)")

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        log.info("[ORACLE] Chainlink feed stopped")

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._poll_loop())
        except Exception as e:
            log.error(f"[ORACLE] Event loop crashed: {e}")
        finally:
            loop.close()

    async def _poll_loop(self) -> None:
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider(self._rpc_url))
        contract = w3.eth.contract(
            address=Web3.to_checksum_address(self._aggregator_address), abi=AGGREGATOR_V3_ABI,
        )
        backoff = self._poll_interval
        failures = 0

        while self._running:
            try:
                round_id, answer, _, _, _ = contract.functions.latestRoundData().call()
                price = answer / (10 ** CHAINLINK_DECIMALS)
                now = time.time()
                with self._lock:
                    self._price = price
                    self._last_update = now
                    self._round_id = round_id
                    self._history.append((now, price))
                    while self._history and self._history[0][0] < now - 300:
                        self._history.popleft()
                failures = 0
                backoff = self._poll_interval
            except Exception as e:
                failures += 1
                if failures <= 3:
                    log.warning(f"[ORACLE] Poll failed ({failures}): {e}")
                elif failures % 10 == 0:
                    log.error(f"[ORACLE] Poll failing ({failures}): {e}")
                backoff = min(self._poll_interval * (2 ** min(failures, 5)), 30)
            await asyncio.sleep(backoff)
