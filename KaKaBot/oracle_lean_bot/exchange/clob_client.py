"""
exchange/clob_client.py – CLOB REST wrapper using py-clob-client.

Handles both proxy-wallet (signature_type=1, funder) and EOA (default) accounts.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Dict, List, Optional

# Ensure py-clob-client is importable from sibling repo
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
for _dep in ("py-clob-client",):
    _dep_path = os.path.join(_REPO_ROOT, _dep)
    if os.path.isdir(_dep_path) and _dep_path not in sys.path:
        sys.path.insert(0, _dep_path)

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    AssetType,
    BalanceAllowanceParams,
    OrderArgs,
    OrderType,
    OpenOrderParams,
)
from py_clob_client.order_builder.constants import BUY

log = logging.getLogger("lean_bot")


class CLOBWrapper:
    """Polymarket CLOB REST API wrapper."""

    CTF_DECIMALS = 6

    def __init__(
        self,
        host: str,
        private_key: str,
        chain_id: int,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        funder: Optional[str] = None,
    ):
        if funder:
            self._client = ClobClient(
                host, key=private_key, chain_id=chain_id,
                signature_type=1, funder=funder,
            )
            self.maker_address = funder.lower()
            log.info(f"[CLOB] Initialized with proxy funder={funder[:12]}...")
        else:
            self._client = ClobClient(host, key=private_key, chain_id=chain_id)
            self.maker_address = None
            log.info("[CLOB] Initialized as EOA (no funder)")

        creds = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
        self._client.set_api_creds(creds)
        log.info("[CLOB] API credentials set")

    def get_tick_size(self, token_id: str) -> float:
        try:
            return float(self._client.get_tick_size(token_id))
        except Exception as e:
            log.warning(f"get_tick_size failed for {token_id[:12]}: {e}")
            return 0.01

    def get_token_balance(self, token_id: str) -> float:
        """Fetch wallet balance in human-readable shares."""
        try:
            resp = self._client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token_id)
            )
            return float(resp.get("balance", 0)) / (10 ** self.CTF_DECIMALS)
        except Exception as e:
            log.error(f"[BALANCE_ERROR] {token_id[-12:]}: {e}")
            return 0.0

    def get_balance_allowance_usdc(self) -> float:
        """Fetch USDC.e wallet balance in human-readable units."""
        try:
            resp = self._client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            return float(resp.get("balance", 0)) / (10 ** self.CTF_DECIMALS)
        except Exception as e:
            log.error(f"[BALANCE_ERROR] USDC: {e}")
            return 0.0

    def place_maker_bid(
        self, token_id: str, price: float, size: float,
        fee_rate_bps: int = 0, dry_run: bool = True,
    ) -> Optional[str]:
        """Place a GTC BUY limit order with post_only=True. Returns order_id or None."""
        price = round(price, 4)
        size = round(size, 2)
        log.info(f"[ORDER] {'DRY ' if dry_run else ''}BUY {size}@{price} token={token_id[:12]}")

        if dry_run:
            return f"dry_{token_id[:8]}_{price}"

        try:
            order_args = OrderArgs(price=price, size=size, side=BUY, token_id=token_id)
            signed = self._client.create_order(order_args)
            resp = self._client.post_order(signed, OrderType.GTC, post_only=True)

            if resp.get("success"):
                order_id = (
                    resp.get("orderId", "")
                    or resp.get("orderID", "")
                    or resp.get("order_id", "")
                )

                # Polymarket sometimes returns empty orderId even on success
                if not order_id:
                    try:
                        active = self._client.get_orders(OpenOrderParams())
                        if isinstance(active, list):
                            for o in active:
                                if (str(o.get("asset_id", "")) == token_id
                                        and float(o.get("price", 0)) == price):
                                    order_id = o.get("id", "") or o.get("order_id", "")
                                    if order_id:
                                        break
                    except Exception:
                        pass

                if not order_id:
                    import uuid
                    order_id = f"unknown_{uuid.uuid4().hex[:12]}"
                    log.warning(f"[PLACED] Could not get orderId — using synthetic {order_id}")

                log.info(f"[PLACED] order_id={order_id[:16]} {size}@{price} token={token_id[:12]}")
                return order_id
            else:
                log.warning(f"[PLACE_FAIL] {resp.get('errorMsg', 'unknown')} | {size}@{price}")
                return None
        except Exception as e:
            log.error(f"[PLACE_EXCEPTION] {e}")
            if "balance" in str(e).lower() or "allowance" in str(e).lower():
                raise
            return None

    def place_taker_fok(
        self, token_id: str, price: float, size: float, dry_run: bool = True,
    ) -> Optional[str]:
        """Place a Fill-Or-Kill taker order for emergency rebalancing."""
        price = min(round(price, 4), 0.99)
        size = round(size, 2)
        log.warning(f"[TAKER] {'DRY ' if dry_run else ''}FOK BUY {size}@{price} token={token_id[:12]}")

        if dry_run:
            return f"dry_fok_{token_id[:8]}_{price}"

        try:
            order_args = OrderArgs(price=price, size=size, side=BUY, token_id=token_id)
            signed = self._client.create_order(order_args)
            resp = self._client.post_order(signed, OrderType.FOK)
            if resp.get("success"):
                order_id = resp.get("orderId", "") or f"fok_{token_id[:8]}"
                log.warning(f"[TAKER] FOK placed: {order_id[:16]}")
                return order_id
            else:
                log.warning(f"[TAKER] FOK failed: {resp.get('errorMsg', 'unknown')}")
                return None
        except Exception as e:
            log.error(f"[TAKER_EXCEPTION] {e}")
            return None

    def cancel_order(self, order_id: str, dry_run: bool = True) -> bool:
        if dry_run:
            return True
        try:
            resp = self._client.cancel(order_id)
            return bool(resp.get("canceled"))
        except Exception as e:
            log.error(f"[CANCEL_EXCEPTION] {order_id[:16]}: {e}")
            return False

    def cancel_all(self, dry_run: bool = True) -> None:
        log.warning("[CANCEL_ALL] Cancelling all open orders")
        if dry_run:
            return
        try:
            self._client.cancel_all()
        except Exception as e:
            log.error(f"[CANCEL_ALL_EXCEPTION] {e}")

    def get_active_orders(self, market: str = None) -> List[Dict]:
        try:
            params = OpenOrderParams(market=market) if market else OpenOrderParams()
            resp = self._client.get_orders(params)
            return resp if isinstance(resp, list) else []
        except Exception as e:
            log.error(f"[GET_ORDERS_EXCEPTION] {e}")
            return []
