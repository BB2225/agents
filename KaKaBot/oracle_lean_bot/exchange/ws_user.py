"""
exchange/ws_user.py – Authenticated WebSocket user channel.

Protocol:
  1. Connect to wss://ws-subscriptions-clob.polymarket.com/ws/user
  2. Send combined auth+subscription message
  3. Send "PING" every 10s
  4. Receive trade and order events
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("lean_bot")

WS_USER_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
PING_INTERVAL_SEC = 10


class UserWSClient:
    def __init__(
        self,
        api_key: str, api_secret: str, api_passphrase: str,
        condition_ids: List[str],
        on_fill: Optional[Callable[[dict], None]] = None,
        on_order_update: Optional[Callable[[dict], None]] = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.condition_ids = condition_ids
        self.on_fill = on_fill
        self.on_order_update = on_order_update
        self._running = False
        self._ws = None
        self.connected = False
        self._consec_failures = 0

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                async with websockets.connect(
                    WS_USER_URL, ping_interval=None, open_timeout=10,
                ) as ws:
                    self._ws = ws
                    await ws.send(json.dumps({
                        "type": "user",
                        "markets": self.condition_ids,
                        "auth": {
                            "apiKey": self.api_key,
                            "secret": self.api_secret,
                            "passphrase": self.api_passphrase,
                        },
                    }))
                    log.info(f"[WS_USER] Connected + subscribed to {len(self.condition_ids)} markets")
                    self.connected = True
                    self._consec_failures = 0

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    try:
                        async for raw in ws:
                            if not self._running:
                                break
                            try:
                                self._handle(json.loads(raw))
                            except Exception as e:
                                log.debug(f"[WS_USER] parse error: {e}")
                    finally:
                        ping_task.cancel()

            except (ConnectionClosed, Exception) as e:
                self.connected = False
                if not self._running:
                    break
                self._consec_failures += 1
                backoff = min(3 * (2 ** (self._consec_failures - 1)), 60)
                log.warning(f"[WS_USER] Disconnected ({e}). Retry in {backoff}s...")
                await asyncio.sleep(backoff)

        self.connected = False
        log.info("[WS_USER] Stopped")

    async def _ping_loop(self, ws) -> None:
        try:
            while True:
                await asyncio.sleep(PING_INTERVAL_SEC)
                await ws.send("PING")
        except Exception:
            pass

    def _handle(self, msg: dict) -> None:
        if isinstance(msg, list):
            for m in msg:
                self._handle(m)
            return
        event_type = msg.get("event_type") or msg.get("type", "")
        if event_type == "trade":
            if self.on_fill and msg.get("status") in ("MATCHED", "MINED", "CONFIRMED"):
                self.on_fill(msg)
        elif event_type == "order":
            if self.on_order_update:
                self.on_order_update(msg)

    async def resubscribe(self, condition_ids: List[str]) -> None:
        self.condition_ids = condition_ids
        log.info(f"[WS_USER] Resubscribing to {len(condition_ids)} markets")
        if self._ws:
            await self._ws.close()

    def stop(self) -> None:
        self._running = False
        if self._ws:
            asyncio.ensure_future(self._ws.close())
