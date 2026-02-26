"""
exchange/ws_market.py – WebSocket market channel client.
Receives orderbook snapshots and price_change updates for token IDs.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Callable, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

log = logging.getLogger("lean_bot")

WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class MarketWSClient:
    def __init__(self, token_ids: List[str], on_book: Callable[[str, list, list], None]):
        self.token_ids = token_ids
        self.on_book = on_book
        self._running = False
        self._ws = None

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                async with websockets.connect(
                    WS_MARKET_URL, ping_interval=20, ping_timeout=30,
                ) as ws:
                    self._ws = ws
                    log.info(f"[WS_MARKET] Connected. Subscribing to {len(self.token_ids)} tokens")
                    await ws.send(json.dumps({"type": "market", "assets_ids": self.token_ids}))

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            self._handle(json.loads(raw))
                        except Exception as e:
                            log.debug(f"[WS_MARKET] parse error: {e}")

            except ConnectionClosed as e:
                log.warning(f"[WS_MARKET] Connection closed: {e}. Reconnecting in 2s...")
                await asyncio.sleep(2)
            except Exception as e:
                log.error(f"[WS_MARKET] Error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)
        log.info("[WS_MARKET] Stopped")

    def _handle(self, msg: dict) -> None:
        if isinstance(msg, list):
            for m in msg:
                self._handle(m)
            return

        event_type = msg.get("event_type")

        if event_type == "book":
            asset_id = msg.get("asset_id", "")
            bids = sorted(msg.get("bids", []), key=lambda x: float(x["price"]), reverse=True)
            asks = sorted(msg.get("asks", []), key=lambda x: float(x["price"]))
            self.on_book(asset_id, bids, asks)

        elif event_type == "price_change":
            for pc in msg.get("price_changes", []):
                asset_id = pc.get("asset_id", "")
                best_bid = float(pc.get("best_bid", 0))
                best_ask = float(pc.get("best_ask", 1))
                bids = [{"price": str(best_bid), "size": "0"}] if best_bid > 0 else []
                asks = [{"price": str(best_ask), "size": "0"}] if best_ask < 1 else []
                self.on_book(asset_id, bids, asks)

    async def resubscribe(self, token_ids: List[str]) -> None:
        self.token_ids = token_ids
        log.info(f"[WS_MARKET] Resubscribing to {len(token_ids)} new tokens")
        if self._ws:
            await self._ws.close()

    def stop(self) -> None:
        self._running = False
        if self._ws:
            asyncio.ensure_future(self._ws.close())
