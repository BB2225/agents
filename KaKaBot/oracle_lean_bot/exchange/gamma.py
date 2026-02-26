"""
exchange/gamma.py – Market discovery via Gamma API.
Finds active BTC 15-minute markets by computing slugs from current time.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

import aiohttp

log = logging.getLogger("lean_bot")

GAMMA_HOST = "https://gamma-api.polymarket.com"


def _current_15m_start_ts(now: float) -> int:
    return int(now // 900) * 900


async def fetch_btc_15m_market(
    session: aiohttp.ClientSession, offset_intervals: int = 0,
) -> Optional[dict]:
    """
    Find an active BTC 15-minute market.
    offset_intervals: 0 = current, 1 = next, etc.
    """
    now = time.time()
    start_ts = _current_15m_start_ts(now) + (offset_intervals * 900)
    end_ts = start_ts + 900
    slug = f"btc-updown-15m-{start_ts}"

    try:
        url = f"{GAMMA_HOST}/events"
        async with session.get(
            url, params={"slug": slug}, timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status != 200:
                return None
            events = await resp.json()
            if not events:
                return None

            event = events[0]
            for market in event.get("markets", []):
                if market.get("closed"):
                    continue
                condition_id = market.get("conditionId")
                if not condition_id:
                    continue

                raw_tokens = market.get("clobTokenIds") or "[]"
                tokens = json.loads(raw_tokens) if isinstance(raw_tokens, str) else list(raw_tokens)
                if len(tokens) < 2:
                    continue

                raw_outcomes = market.get("outcomes") or '["Up","Down"]'
                outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else list(raw_outcomes)

                token_up = token_down = None
                for idx, outcome in enumerate(outcomes):
                    o = str(outcome).lower()
                    if o in ("up", "yes") and idx < len(tokens):
                        token_up = str(tokens[idx])
                    elif o in ("down", "no") and idx < len(tokens):
                        token_down = str(tokens[idx])

                return {
                    "condition_id": condition_id,
                    "token_up": token_up or str(tokens[0]),
                    "token_down": token_down or str(tokens[1]),
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "tick_size": float(market.get("orderPriceMinTickSize") or 0.01),
                    "slug": slug,
                }
    except Exception as e:
        log.debug(f"[GAMMA] Failed to fetch {slug}: {e}")
    return None


async def discover_multiple_markets(
    session: aiohttp.ClientSession, max_markets: int = 15,
) -> list[dict]:
    """Discover up to max_markets active/upcoming BTC 15-min markets."""
    markets = []
    seen_cids = set()
    now = time.time()

    for i in range(max_markets + 5):
        if len(markets) >= max_markets:
            break
        start_ts = _current_15m_start_ts(now) + (i * 900)
        if start_ts + 900 <= now:
            continue
        market = await fetch_btc_15m_market(session, offset_intervals=i)
        if market and market["condition_id"] not in seen_cids:
            seen_cids.add(market["condition_id"])
            markets.append(market)
    return markets
