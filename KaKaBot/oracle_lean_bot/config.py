"""
config.py – Configuration for the Oracle-Lead Hybrid Lean Bot.
"""
from __future__ import annotations

import os

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ENV_FILE = os.path.join(_PROJECT_ROOT, ".env")


class LeanBotConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE, env_file_encoding="utf-8", extra="ignore",
    )

    # ── Web3 ──────────────────────────────────────────────────────────────
    polygon_rpc_url: str = Field(default="https://polygon-bor-rpc.publicnode.com")
    private_key: str = Field(..., description="EOA private key (0x-prefixed)")

    # ── CLOB API ──────────────────────────────────────────────────────────
    clob_api_key: str = Field(...)
    clob_api_secret: str = Field(...)
    clob_api_passphrase: str = Field(...)

    # ── Builder / Relayer (gasless merges) ────────────────────────────────
    poly_builder_api_key: str = Field(default="")
    poly_builder_secret: str = Field(default="")
    poly_builder_passphrase: str = Field(default="")

    # ── Proxy wallet ──────────────────────────────────────────────────────
    polymarket_funder: str = Field(default="")

    # ── Safety Guards ─────────────────────────────────────────────────────
    dry_run: bool = True
    live: bool = False

    # ── Capital ───────────────────────────────────────────────────────────
    total_capital_usdc: float = Field(default=300.0)
    profit_target_usdc: float = Field(default=15.0)
    profit_exit_usdc: float = Field(default=25.0)
    max_markets: int = Field(default=15, ge=1, le=15)

    # ── Z-Score Directional Model ─────────────────────────────────────────
    edge_threshold: float = Field(default=0.03)
    z_lean_entry: float = Field(default=1.15)
    z_lean_moderate: float = Field(default=1.75)
    z_lean_strong: float = Field(default=2.4)
    z_lean_aggressive: float = Field(default=3.2)
    volatility_window_sec: int = Field(default=120)

    # ── Surplus Caps ──────────────────────────────────────────────────────
    surplus_cap_z1: float = Field(default=0.15)
    surplus_cap_z2: float = Field(default=0.25)
    surplus_cap_z3: float = Field(default=0.40)

    # ── Neutral Mode ──────────────────────────────────────────────────────
    neutral_soft_imbalance: int = Field(default=20)
    neutral_hard_imbalance: int = Field(default=35)

    # ── Spread / Quoting ──────────────────────────────────────────────────
    cushion: float = Field(default=0.02, ge=0.005, le=0.10)
    refresh_interval_sec: float = Field(default=0.25)

    # ── Merge ─────────────────────────────────────────────────────────────
    merge_pair_threshold: int = Field(default=75)
    max_pair_cost_to_merge: float = Field(default=0.985)
    merge_check_interval_sec: int = Field(default=15)

    # ── Flip Risk ─────────────────────────────────────────────────────────
    flip_base_fraction: float = Field(default=0.0003)
    final_window_sec: int = Field(default=45)
    emergency_flatten_sec: int = Field(default=60)

    # ── Cross-Market Dampener ─────────────────────────────────────────────
    dampener_threshold_5: float = Field(default=0.20)
    dampener_threshold_8: float = Field(default=0.35)

    # ── Max Trade Cap ─────────────────────────────────────────────────────
    max_trade_cap: int = Field(default=50)

    # ── Polymarket Constants ──────────────────────────────────────────────
    clob_host: str = "https://clob.polymarket.com"
    gamma_host: str = "https://gamma-api.polymarket.com"
    chain_id: int = 137

    # ── Chainlink ─────────────────────────────────────────────────────────
    chainlink_btc_usd_address: str = Field(default="0xc907E116054Ad103354f2D350FD2514433D57F6f")
    chainlink_poll_interval_sec: float = Field(default=1.0)

    @field_validator("private_key")
    @classmethod
    def validate_private_key(cls, v: str) -> str:
        if not v.startswith("0x") or len(v) != 66:
            raise ValueError("PRIVATE_KEY must be 0x-prefixed 32-byte hex")
        return v

    @property
    def is_live_trading(self) -> bool:
        return self.live and not self.dry_run

    @property
    def capital_per_market(self) -> float:
        return self.total_capital_usdc / self.max_markets

    def validate_live_guard(self) -> None:
        if self.live and self.dry_run:
            print("WARNING: LIVE=true but DRY_RUN=true — running in DRY_RUN mode.")
        if not self.live and not self.dry_run:
            print("WARNING: DRY_RUN=false but LIVE=false — running in DRY_RUN mode.")
        mode = "LIVE TRADING" if self.is_live_trading else "DRY RUN"
        print(f"Mode: {mode}")
        if self.is_live_trading:
            print("WARNING: Real orders will be placed. Ctrl+C to abort within 5s.")
            import time
            time.sleep(5)
