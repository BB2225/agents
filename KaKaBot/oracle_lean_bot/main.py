"""
main.py – Async main loop for the Oracle-Lead Hybrid Lean Bot.

Architecture (5 concurrent loops):
  oracle_loop:    Poll Chainlink BTC/USD ~1s -> feed volatility tracker
  quote_loop:     Every 0.25s per market -> compute signals, place/cancel orders
  merge_loop:     Every 15s -> merge matched pairs when conditions met
  discovery_loop: Every 30s -> discover new markets, clean up expired
  monitor_loop:   Every 5s -> aggregate P&L, dampener, per-market summaries

Manages up to 15 simultaneous BTC 15-minute binary markets.
Each market has its own MarketContext with independent state machine.

Key constraints:
  - 250ms taker delay -> design for maker, not sniper
  - HTTP 425 during restart -> pause, retry, resume
  - Min 5 shares / $1 notional per order
  - Post-only GTC for spread capture, FOK only for emergency flatten
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, List, Optional

# Ensure sibling SDK repos are importable
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_PROJECT_ROOT)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)
for _dep in ("py-clob-client", "py-builder-relayer-client", "py-builder-signing-sdk"):
    _dep_path = os.path.join(_REPO_ROOT, _dep)
    if os.path.isdir(_dep_path) and _dep_path not in sys.path:
        sys.path.insert(0, _dep_path)

import aiohttp

from oracle_lean_bot.config import LeanBotConfig
from oracle_lean_bot.state_machine import BotState
from oracle_lean_bot.directional_model import DirectionalModel, RollingVolatilityTracker
from oracle_lean_bot.inventory import (
    calculate_min_lot, calculate_dynamic_order_size,
    calculate_imbalance_tolerance, calculate_surplus_target, is_flip_risk,
)
from oracle_lean_bot.oracle_chainlink import ChainlinkOracle
from oracle_lean_bot.market_context import MarketContext, OpenOrder
from oracle_lean_bot.cross_market import CrossMarketDampener
from oracle_lean_bot.http_retry import retry_on_425, is_in_restart
from oracle_lean_bot.exchange.clob_client import CLOBWrapper
from oracle_lean_bot.exchange.ws_market import MarketWSClient
from oracle_lean_bot.exchange.ws_user import UserWSClient
from oracle_lean_bot.exchange.merger import Merger
from oracle_lean_bot.exchange.gamma import fetch_btc_15m_market

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("lean_bot")


class LeanBot:
    """
    Oracle-Lead Hybrid Lean Bot.

    Two regimes:
      1. Delta-Neutral Maker Mode — harvest spread, 1:1 inventory
      2. Directional Lean Mode — overweight statistically favored side

    Primary signal: Chainlink BTC/USD on Polygon.
    """

    def __init__(self, config: LeanBotConfig):
        self.cfg = config
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Core components
        self.oracle = ChainlinkOracle(
            rpc_url=config.polygon_rpc_url,
            aggregator_address=config.chainlink_btc_usd_address,
            poll_interval=config.chainlink_poll_interval_sec,
        )
        self.vol_tracker = RollingVolatilityTracker(window_sec=config.volatility_window_sec)
        self.model = DirectionalModel(
            edge_threshold=config.edge_threshold,
            z_lean_entry=config.z_lean_entry,
            z_lean_moderate=config.z_lean_moderate,
            z_lean_strong=config.z_lean_strong,
            z_lean_aggressive=config.z_lean_aggressive,
            surplus_cap_z1=config.surplus_cap_z1,
            surplus_cap_z2=config.surplus_cap_z2,
            surplus_cap_z3=config.surplus_cap_z3,
        )
        self.dampener = CrossMarketDampener(
            threshold_5_reduction=config.dampener_threshold_5,
            threshold_8_reduction=config.dampener_threshold_8,
        )

        # CLOB client
        self.clob = CLOBWrapper(
            host=config.clob_host, private_key=config.private_key,
            chain_id=config.chain_id, api_key=config.clob_api_key,
            api_secret=config.clob_api_secret, api_passphrase=config.clob_api_passphrase,
            funder=config.polymarket_funder or None,
        )

        # Merger
        self.merger = Merger(
            private_key=config.private_key, polygon_rpc_url=config.polygon_rpc_url,
            dry_run=not config.is_live_trading,
            funder=config.polymarket_funder or None,
            builder_key=config.poly_builder_api_key,
            builder_secret=config.poly_builder_secret,
            builder_passphrase=config.poly_builder_passphrase,
        )

        # Active markets
        self.markets: Dict[str, MarketContext] = {}
        self._ws_market: Optional[MarketWSClient] = None
        self._ws_user: Optional[UserWSClient] = None
        self._ws_tasks: list[asyncio.Task] = []
        self._total_realized_pnl = 0.0
        self._session_start = time.time()

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        self.cfg.validate_live_guard()
        self._running = True

        # Start Chainlink oracle
        self.oracle.start()
        log.info("[BOOT] Chainlink oracle started")

        # Wait for first price
        for _ in range(30):
            if self.oracle.is_alive():
                break
            await asyncio.sleep(1)

        if not self.oracle.is_alive():
            log.error("[BOOT] Chainlink oracle not responding — aborting")
            return

        log.info(f"[BOOT] First BTC price: ${self.oracle.get_price():,.2f}")

        # Preflight merger
        if self.cfg.polymarket_funder:
            self.merger.preflight()
        else:
            self.merger.preflight_safe()

        # Cancel stale orders
        if self.cfg.is_live_trading:
            self.clob.cancel_all(dry_run=False)

        # Start loops
        async with aiohttp.ClientSession() as session:
            self._session = session
            await self._discover_markets(session)

            if not self.markets:
                log.error("[BOOT] No active markets found — aborting")
                return

            await self._start_websockets()

            tasks = [
                asyncio.create_task(self._oracle_loop(), name="oracle"),
                asyncio.create_task(self._quote_loop(), name="quote"),
                asyncio.create_task(self._merge_loop(), name="merge"),
                asyncio.create_task(self._discovery_loop(session), name="discovery"),
                asyncio.create_task(self._monitor_loop(), name="monitor"),
            ]
            tasks.extend(self._ws_tasks)

            try:
                await self._shutdown_event.wait()
            except asyncio.CancelledError:
                pass
            finally:
                self._running = False
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self._cleanup()

    def shutdown(self) -> None:
        log.warning("[SHUTDOWN] Initiating graceful shutdown...")
        self._running = False
        self._shutdown_event.set()

    def _cleanup(self) -> None:
        self.oracle.stop()
        for ctx in self.markets.values():
            ctx.save()
        log.info("[SHUTDOWN] Cleanup complete")

    # ── Market Discovery ──────────────────────────────────────────────────

    async def _discover_markets(self, session: aiohttp.ClientSession) -> None:
        now = time.time()
        btc_price = self.oracle.get_price() or 100000.0
        discovered = 0

        for i in range(self.cfg.max_markets + 5):
            if discovered >= self.cfg.max_markets:
                break
            start_ts = int(now // 900) * 900 + (i * 900)
            end_ts = start_ts + 900
            if end_ts <= now:
                continue

            market = await fetch_btc_15m_market(session, offset_intervals=i)
            if not market:
                continue
            cid = market["condition_id"]
            if cid in self.markets:
                continue

            ctx = MarketContext.load_or_create(
                condition_id=cid, token_yes=market["token_up"],
                token_no=market["token_down"],
                start_ts=start_ts, end_ts=end_ts,
                tick_size=market.get("tick_size", 0.01),
                strike_price=btc_price,
            )
            self.markets[cid] = ctx
            discovered += 1
            log.info(
                f"[DISCOVER] {cid[:12]} | strike=${btc_price:,.2f} | T-{ctx.time_remaining:.0f}s"
            )

        log.info(f"[DISCOVER] {len(self.markets)} active markets")

    # ── WebSockets ────────────────────────────────────────────────────────

    async def _start_websockets(self) -> None:
        all_tokens = []
        all_conditions = []
        for ctx in self.markets.values():
            all_tokens.extend([ctx.token_yes, ctx.token_no])
            all_conditions.append(ctx.condition_id)

        self._ws_market = MarketWSClient(token_ids=all_tokens, on_book=self._on_book_update)
        self._ws_user = UserWSClient(
            api_key=self.cfg.clob_api_key, api_secret=self.cfg.clob_api_secret,
            api_passphrase=self.cfg.clob_api_passphrase,
            condition_ids=all_conditions, on_fill=self._on_fill,
        )
        self._ws_tasks = [
            asyncio.create_task(self._ws_market.run(), name="ws_market"),
            asyncio.create_task(self._ws_user.run(), name="ws_user"),
        ]
        log.info(f"[WS] Started ({len(all_tokens)} tokens, {len(all_conditions)} conditions)")

    def _on_book_update(self, asset_id: str, bids: list, asks: list) -> None:
        for ctx in self.markets.values():
            if asset_id in (ctx.token_yes, ctx.token_no):
                ctx.update_orderbook(asset_id, bids, asks)
                return

    def _on_fill(self, msg: dict) -> None:
        asset_id = str(msg.get("asset_id", ""))
        fill_id = msg.get("id", "") or msg.get("trade_id", "")
        price = float(msg.get("price", 0))
        size = float(msg.get("size", 0))
        if not fill_id or price <= 0 or size <= 0:
            return
        for ctx in self.markets.values():
            if asset_id in (ctx.token_yes, ctx.token_no):
                ctx.record_fill(fill_id, asset_id, price, size)
                ctx.save()
                return

    # ── Oracle Loop ───────────────────────────────────────────────────────

    async def _oracle_loop(self) -> None:
        while self._running:
            try:
                price = self.oracle.get_price()
                if price is not None:
                    self.vol_tracker.update(price)
            except Exception as e:
                log.error(f"[ORACLE_LOOP] {e}")
            await asyncio.sleep(self.cfg.chainlink_poll_interval_sec)

    # ── Quote Loop (Core Trading) ─────────────────────────────────────────

    async def _quote_loop(self) -> None:
        while self._running:
            try:
                if is_in_restart():
                    await asyncio.sleep(1.0)
                    continue

                btc_price = self.oracle.get_price()
                if btc_price is None:
                    await asyncio.sleep(self.cfg.refresh_interval_sec)
                    continue

                vol = self.vol_tracker.get_volatility()
                damp = self.dampener.get_dampener()

                for cid, ctx in list(self.markets.items()):
                    if not ctx.is_active:
                        continue
                    try:
                        ctx.compute_signals(self.model, btc_price, vol)
                        self.dampener.update(cid, ctx.z_score)
                        ctx.evaluate_state(
                            btc_price, self.cfg.edge_threshold,
                            self.cfg.final_window_sec, self.cfg.emergency_flatten_sec,
                            self.cfg.profit_exit_usdc,
                        )
                        await self._execute_state_actions(ctx, btc_price, damp)
                        ctx.log_csv_tick(btc_price)
                    except Exception as e:
                        log.error(f"[QUOTE:{cid[:8]}] {e}", exc_info=True)

            except Exception as e:
                log.error(f"[QUOTE_LOOP] {e}", exc_info=True)

            await asyncio.sleep(self.cfg.refresh_interval_sec)

    async def _execute_state_actions(self, ctx: MarketContext, btc_price: float, damp: float) -> None:
        state = ctx.state
        dry = not self.cfg.is_live_trading

        if state == BotState.DORMANT:
            return
        if state == BotState.EMERGENCY_FLATTEN:
            await self._emergency_flatten(ctx, dry)
            return
        if state == BotState.CONTROLLED_LEAN_FINAL:
            await self._controlled_lean_final(ctx, btc_price, damp, dry)
            return
        if state == BotState.HIGH_FLIP_RISK:
            await self._high_flip_risk(ctx, btc_price, dry)
            return
        if state == BotState.MOMENTUM_LEAN:
            await self._momentum_lean(ctx, btc_price, damp, dry)
            return
        # NEUTRAL_MM
        await self._neutral_mm(ctx, btc_price, damp, dry)

    # ── State Actions ─────────────────────────────────────────────────────

    async def _neutral_mm(self, ctx: MarketContext, btc_price: float, damp: float, dry: bool) -> None:
        """Delta-neutral: symmetric quoting, 1:1 inventory, spread harvest."""
        inv = ctx.inventory
        tick = ctx.tick_size
        await self._cancel_stale_orders(ctx, dry)

        cushion = self.cfg.cushion
        target = 1.0 - cushion

        bid_yes = self._compute_bid(ctx.best_bid_yes, ctx.best_ask_yes, ctx.mid_yes, tick)
        bid_no = self._compute_bid(ctx.best_bid_no, ctx.best_ask_no, ctx.mid_no, tick)
        if bid_yes is None or bid_no is None:
            return

        # Pair invariant: bid_yes + bid_no <= target
        total = bid_yes + bid_no
        if total > target:
            excess = total - target
            if bid_yes >= bid_no:
                bid_yes = round(bid_yes - excess, 4)
            else:
                bid_no = round(bid_no - excess, 4)
        bid_yes = max(tick, round(int(bid_yes / tick) * tick, 4))
        bid_no = max(tick, round(int(bid_no / tick) * tick, 4))

        # Skip heavy side near soft cap
        skip_yes = inv.net_inventory >= self.cfg.neutral_soft_imbalance
        skip_no = inv.net_inventory <= -self.cfg.neutral_soft_imbalance

        min_lot = calculate_min_lot(max(bid_yes, bid_no, 0.01))
        size = max(min_lot, int(min_lot * 1.5 * damp))

        if not skip_yes:
            await self._place_order(ctx, "YES", ctx.token_yes, bid_yes, size, dry)
        if not skip_no:
            await self._place_order(ctx, "NO", ctx.token_no, bid_no, size, dry)

    async def _momentum_lean(self, ctx: MarketContext, btc_price: float, damp: float, dry: bool) -> None:
        """Directional lean: overweight favored side within surplus cap."""
        inv = ctx.inventory
        tick = ctx.tick_size
        await self._cancel_stale_orders(ctx, dry)

        direction = self.model.lean_direction(ctx.z_score)
        surplus_gap = calculate_surplus_target(inv.total_inventory, ctx.surplus_cap, inv.net_inventory)

        bid_yes = self._compute_bid(ctx.best_bid_yes, ctx.best_ask_yes, ctx.mid_yes, tick)
        bid_no = self._compute_bid(ctx.best_bid_no, ctx.best_ask_no, ctx.mid_no, tick)
        if bid_yes is None or bid_no is None:
            return

        # Pair invariant
        target = 1.0 - self.cfg.cushion
        total = bid_yes + bid_no
        if total > target:
            excess = total - target
            if bid_yes >= bid_no:
                bid_yes = round(bid_yes - excess, 4)
            else:
                bid_no = round(bid_no - excess, 4)
        bid_yes = max(tick, round(int(bid_yes / tick) * tick, 4))
        bid_no = max(tick, round(int(bid_no / tick) * tick, 4))

        min_lot = calculate_min_lot(max(bid_yes, bid_no, 0.01))
        base_size = max(min_lot, int(min_lot * 1.5))
        imbal_tol = calculate_imbalance_tolerance(max(bid_yes, bid_no, 0.01))

        # Favored side: dynamic sizing
        favored_size = calculate_dynamic_order_size(
            price=bid_yes if direction == "UP" else bid_no,
            surplus_gap=surplus_gap, max_trade_cap=self.cfg.max_trade_cap,
            size_multiplier=ctx.size_multiplier * damp,
        )
        unfavored_size = int(base_size * damp)

        if direction == "UP":
            skip_yes = surplus_gap <= 0 and inv.net_inventory > imbal_tol
            if not skip_yes:
                await self._place_order(ctx, "YES", ctx.token_yes, bid_yes, favored_size or base_size, dry)
            await self._place_order(ctx, "NO", ctx.token_no, bid_no, unfavored_size, dry)
        else:
            skip_no = surplus_gap <= 0 and inv.net_inventory < -imbal_tol
            await self._place_order(ctx, "YES", ctx.token_yes, bid_yes, unfavored_size, dry)
            if not skip_no:
                await self._place_order(ctx, "NO", ctx.token_no, bid_no, favored_size or base_size, dry)

    async def _controlled_lean_final(self, ctx: MarketContext, btc_price: float, damp: float, dry: bool) -> None:
        """
        T <= 45s: ride winner, cancel losing side. Do NOT force parity.
        If z >= 2.0 and no flip risk: allow imbalance +15%.
        """
        direction = self.model.lean_direction(ctx.z_score)
        abs_z = abs(ctx.z_score)
        flip = is_flip_risk(btc_price, ctx.strike_price, ctx.time_remaining)

        # Cancel losing side
        losing_token = ctx.token_no if direction == "UP" else ctx.token_yes
        for oid, order in list(ctx.open_orders.items()):
            if order.token_id == losing_token:
                self._cancel_order_sync(oid, dry)
                ctx.open_orders.pop(oid, None)

        # Strong signal + no flip: let imbalance ride
        if abs_z >= 2.0 and not flip:
            surplus_cap = min(ctx.surplus_cap * 1.15, 0.50)
            surplus_gap = calculate_surplus_target(ctx.inventory.total_inventory, surplus_cap, ctx.inventory.net_inventory)
            if surplus_gap > 0:
                tick = ctx.tick_size
                if direction == "UP":
                    bid = self._compute_bid(ctx.best_bid_yes, ctx.best_ask_yes, ctx.mid_yes, tick)
                    if bid:
                        await self._place_order(ctx, "YES", ctx.token_yes, bid, calculate_min_lot(bid), dry)
                else:
                    bid = self._compute_bid(ctx.best_bid_no, ctx.best_ask_no, ctx.mid_no, tick)
                    if bid:
                        await self._place_order(ctx, "NO", ctx.token_no, bid, calculate_min_lot(bid), dry)

    async def _high_flip_risk(self, ctx: MarketContext, btc_price: float, dry: bool) -> None:
        """Price near strike: cancel all, only rebalance if extreme imbalance."""
        await self._cancel_stale_orders(ctx, dry, cancel_all=True)

        inv = ctx.inventory
        if abs(inv.net_inventory) > self.cfg.neutral_hard_imbalance:
            tick = ctx.tick_size
            if inv.net_inventory > 0:
                bid = self._compute_bid(ctx.best_bid_no, ctx.best_ask_no, ctx.mid_no, tick)
                if bid:
                    await self._place_order(ctx, "NO", ctx.token_no, bid, calculate_min_lot(bid), dry)
            else:
                bid = self._compute_bid(ctx.best_bid_yes, ctx.best_ask_yes, ctx.mid_yes, tick)
                if bid:
                    await self._place_order(ctx, "YES", ctx.token_yes, bid, calculate_min_lot(bid), dry)

    async def _emergency_flatten(self, ctx: MarketContext, dry: bool) -> None:
        """Cancel all, FOK to parity, merge, DORMANT."""
        inv = ctx.inventory
        log.warning(f"[EMERGENCY:{ctx.condition_id[:8]}] Flattening inv={inv.yes_qty:.0f}Y/{inv.no_qty:.0f}N")

        # Cancel all
        self.clob.cancel_all(dry_run=dry)
        ctx.open_orders.clear()

        # FOK to parity
        imbalance = inv.net_inventory
        if abs(imbalance) >= calculate_min_lot(0.5):
            if imbalance > 0:
                price = min(ctx.best_ask_no + ctx.tick_size, 0.99)
                self.clob.place_taker_fok(ctx.token_no, price, abs(imbalance), dry_run=dry)
            else:
                price = min(ctx.best_ask_yes + ctx.tick_size, 0.99)
                self.clob.place_taker_fok(ctx.token_yes, price, abs(imbalance), dry_run=dry)

        # Merge remaining pairs
        pairs = inv.matched_pairs
        if pairs > 0:
            success = self.merger.merge_positions(ctx.condition_id, pairs)
            if success:
                realized = inv.record_merge(pairs)
                self._total_realized_pnl += realized
                log.info(f"[EMERGENCY] Merged {pairs:.0f} pairs, realized ${realized:.4f}")

        ctx.state_machine.transition(BotState.DORMANT, "emergency_flatten_complete")
        ctx.save()

    # ── Order Helpers ─────────────────────────────────────────────────────

    def _compute_bid(self, best_bid: float, best_ask: float, mid: float, tick: float) -> Optional[float]:
        has_clob = 0 < best_bid < 1 and 0 < best_ask < 1
        if has_clob and best_bid < best_ask:
            bid = round(best_bid, 4)
        else:
            bid = round(mid - tick, 4)
            if has_clob and bid >= best_ask:
                bid = round(best_ask - tick, 4)
        return bid if bid >= tick else None

    @retry_on_425(max_retries=5, base_delay=1.0)
    def _place_order_inner(self, token_id, price, size, dry) -> Optional[str]:
        return self.clob.place_maker_bid(token_id=token_id, price=price, size=size, dry_run=dry)

    async def _place_order(self, ctx, side, token_id, price, size, dry) -> None:
        if size <= 0 or price <= 0:
            return
        # Don't duplicate at same price+token
        for order in ctx.open_orders.values():
            if order.token_id == token_id and abs(order.price - price) < ctx.tick_size:
                return
        try:
            oid = await asyncio.get_event_loop().run_in_executor(
                None, self._place_order_inner, token_id, price, size, dry,
            )
            if oid:
                ctx.open_orders[oid] = OpenOrder(oid, token_id, side, price, size, time.time())
                ctx._all_placed_order_ids.add(oid)
        except Exception as e:
            log.error(f"[ORDER:{ctx.condition_id[:8]}] Place failed: {e}")

    @retry_on_425(max_retries=3, base_delay=0.5)
    def _cancel_order_sync(self, order_id, dry) -> bool:
        return self.clob.cancel_order(order_id, dry_run=dry)

    async def _cancel_stale_orders(self, ctx, dry, cancel_all=False) -> None:
        now = time.time()
        to_cancel = [
            oid for oid, order in ctx.open_orders.items()
            if cancel_all or (now - order.placed_at > 5.0)
        ]
        for oid in to_cancel:
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._cancel_order_sync, oid, dry)
            except Exception:
                pass
            ctx.open_orders.pop(oid, None)

    # ── Merge Loop ────────────────────────────────────────────────────────

    async def _merge_loop(self) -> None:
        """Merge every 75 matched pairs when cost is favorable."""
        while self._running:
            await asyncio.sleep(self.cfg.merge_check_interval_sec)
            if is_in_restart():
                continue

            for cid, ctx in list(self.markets.items()):
                if ctx.state in (BotState.EMERGENCY_FLATTEN, BotState.DORMANT):
                    continue
                inv = ctx.inventory
                pairs = inv.matched_pairs
                if pairs < self.cfg.merge_pair_threshold:
                    continue

                # Time-decaying cost ceiling
                secs_left = ctx.time_remaining
                t_frac = max(0.0, min(1.0, 1.0 - secs_left / 900.0))
                ceiling = self.cfg.max_pair_cost_to_merge + 0.013 * t_frac
                if inv.pair_cost > ceiling:
                    continue
                if secs_left < 60:  # endgame protection
                    continue

                try:
                    success = await asyncio.get_event_loop().run_in_executor(
                        None, self.merger.merge_positions, ctx.condition_id, pairs,
                    )
                    if success:
                        realized = inv.record_merge(pairs)
                        self._total_realized_pnl += realized
                        ctx.save()
                        log.info(f"[MERGE:{cid[:8]}] {pairs:.0f} pairs, realized ${realized:.4f}")
                except Exception as e:
                    log.error(f"[MERGE:{cid[:8]}] Failed: {e}")

    # ── Discovery Loop ────────────────────────────────────────────────────

    async def _discovery_loop(self, session: aiohttp.ClientSession) -> None:
        while self._running:
            await asyncio.sleep(30)
            try:
                # Clean up expired markets
                now = time.time()
                expired = [cid for cid, ctx in self.markets.items() if ctx.end_ts < now - 60]
                for cid in expired:
                    ctx = self.markets.pop(cid)
                    self.dampener.remove(cid)
                    # Final merge attempt
                    pairs = ctx.inventory.matched_pairs
                    if pairs > 0:
                        try:
                            success = await asyncio.get_event_loop().run_in_executor(
                                None, self.merger.merge_positions, ctx.condition_id, pairs,
                            )
                            if success:
                                self._total_realized_pnl += ctx.inventory.record_merge(pairs)
                        except Exception:
                            pass
                    ctx.save()
                    log.info(f"[CLEANUP:{cid[:8]}] Removed expired, pnl=${ctx.inventory.total_realized_pnl:.4f}")

                # Discover new markets
                if len(self.markets) < self.cfg.max_markets:
                    await self._discover_markets(session)
                    if self._ws_market:
                        tokens = []
                        for ctx in self.markets.values():
                            tokens.extend([ctx.token_yes, ctx.token_no])
                        await self._ws_market.resubscribe(tokens)
                    if self._ws_user:
                        await self._ws_user.resubscribe(list(self.markets.keys()))
            except Exception as e:
                log.error(f"[DISCOVERY_LOOP] {e}", exc_info=True)

    # ── Monitor Loop ──────────────────────────────────────────────────────

    async def _monitor_loop(self) -> None:
        while self._running:
            await asyncio.sleep(5)
            try:
                total_unreal = sum(ctx.inventory.unrealized_pnl for ctx in self.markets.values())
                total_net = self._total_realized_pnl + total_unreal
                active = sum(1 for ctx in self.markets.values() if ctx.is_active)

                log.info(
                    f"[MONITOR] Markets:{active}/{len(self.markets)} "
                    f"Realized:${self._total_realized_pnl:.4f} "
                    f"Unrealized:${total_unreal:.4f} Net:${total_net:.4f} "
                    f"Dampener:{self.dampener.get_dampener():.2f} "
                    f"Uptime:{time.time() - self._session_start:.0f}s"
                )

                for ctx in self.markets.values():
                    if ctx.is_active:
                        ctx.log_summary()

                # Aggregate profit exit
                if total_net >= self.cfg.profit_exit_usdc:
                    log.warning(f"[PROFIT_EXIT] Net ${total_net:.2f} >= ${self.cfg.profit_exit_usdc:.2f}")
                    self.shutdown()
            except Exception as e:
                log.error(f"[MONITOR] {e}")


# ── Entry Point ───────────────────────────────────────────────────────────

def main() -> None:
    try:
        config = LeanBotConfig()  # type: ignore[call-arg]
    except Exception as e:
        print(f"Configuration error: {e}")
        print("Copy .env.template to .env and fill in your values.")
        sys.exit(1)

    bot = LeanBot(config)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, bot.shutdown)

    try:
        loop.run_until_complete(bot.run())
    except KeyboardInterrupt:
        bot.shutdown()
        loop.run_until_complete(asyncio.sleep(1))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
