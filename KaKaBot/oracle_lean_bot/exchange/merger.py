"""
exchange/merger.py – CTF mergePositions for the Oracle Lean Bot.

Two merge paths:
  1. Proxy wallet (funder set): Direct on-chain via ProxyWalletFactory.
  2. EOA wallet (no funder): Gasless via Polymarket relayer + Gnosis Safe.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from typing import Optional

log = logging.getLogger("lean_bot")

# Ensure SDK deps are importable from sibling repos
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
for _dep in ("py-builder-relayer-client", "py-builder-signing-sdk"):
    _dep_path = os.path.join(_REPO_ROOT, _dep)
    if os.path.isdir(_dep_path) and _dep_path not in sys.path:
        sys.path.insert(0, _dep_path)

from web3 import Web3

try:
    from web3.middleware import ExtraDataToPOAMiddleware
except ImportError:
    from web3.middleware import geth_poa_middleware as ExtraDataToPOAMiddleware

CTF_ADDRESS = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
USDC_E = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
RELAYER_URL = "https://relayer-v2.polymarket.com"
CHAIN_ID = 137
PROXY_WALLET_FACTORY = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
CALL_TYPE_CALL = 1

CTF_ABI = [
    {
        "name": "mergePositions", "type": "function",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [],
    },
]

PROXY_FACTORY_ABI = [
    {
        "name": "proxy", "type": "function",
        "inputs": [
            {
                "name": "calls", "type": "tuple[]",
                "components": [
                    {"name": "typeCode", "type": "uint8"},
                    {"name": "to", "type": "address"},
                    {"name": "value", "type": "uint256"},
                    {"name": "data", "type": "bytes"},
                ],
            }
        ],
        "outputs": [{"name": "returnValues", "type": "bytes[]"}],
        "stateMutability": "payable",
    }
]


def _encode_merge(condition_id: str, amount: int) -> str:
    w3 = Web3()
    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_ADDRESS), abi=CTF_ABI)
    cid_bytes = bytes.fromhex(condition_id.replace("0x", "").zfill(64))
    return ctf.encode_abi(
        abi_element_identifier="mergePositions",
        args=[Web3.to_checksum_address(USDC_E), b"\x00" * 32, cid_bytes, [1, 2], amount],
    )


def _build_relayer_client(private_key, builder_key, builder_secret, builder_passphrase, funder=None):
    from py_builder_relayer_client.client import RelayClient
    from py_builder_signing_sdk.config import BuilderConfig
    from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(key=builder_key, secret=builder_secret, passphrase=builder_passphrase)
    )
    try:
        return RelayClient(RELAYER_URL, CHAIN_ID, private_key, builder_config, funder=funder)
    except TypeError as exc:
        if "funder" not in str(exc):
            raise
        client = RelayClient(RELAYER_URL, CHAIN_ID, private_key, builder_config)
        client.funder = funder
        if funder:
            client.signer.address = lambda: funder
        return client


class Merger:
    """Merge matched outcome token pairs back into USDC."""

    def __init__(
        self, private_key: str, polygon_rpc_url: str, dry_run: bool = True,
        funder: str = None, builder_key: str = "", builder_secret: str = "",
        builder_passphrase: str = "",
    ):
        self.private_key = private_key
        self.polygon_rpc_url = polygon_rpc_url
        self.dry_run = dry_run
        self.funder = funder
        self.builder_key = builder_key
        self.builder_secret = builder_secret
        self.builder_passphrase = builder_passphrase
        self._safe_deployed = False
        self._w3: Optional[Web3] = None
        self._eoa_address: Optional[str] = None

        if self.funder:
            self._w3 = Web3(Web3.HTTPProvider(polygon_rpc_url))
            self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            acct = self._w3.eth.account.from_key(private_key)
            self._eoa_address = acct.address
            log.info(f"[MERGER] Initialized (dry_run={dry_run}, proxy wallet) EOA={self._eoa_address}")
        else:
            log.info(f"[MERGER] Initialized (dry_run={dry_run}, EOA, gasless relayer)")

    def preflight(self) -> bool:
        if self.dry_run:
            return True
        try:
            if not self._w3:
                return False
            chain_id = self._w3.eth.chain_id
            if chain_id != CHAIN_ID:
                log.error(f"[MERGER] Wrong chain: expected {CHAIN_ID}, got {chain_id}")
                return False
            bal = self._w3.eth.get_balance(self._eoa_address)
            pol = self._w3.from_wei(bal, "ether")
            log.info(f"[MERGER] Web3 OK, POL={pol:.4f}")
            if pol < 0.1:
                log.warning(f"[MERGER] Low POL balance ({pol:.4f})")
            return True
        except Exception as e:
            log.error(f"[MERGER] Preflight failed: {e}")
            return False

    def preflight_safe(self) -> bool:
        if self.dry_run:
            return True
        try:
            client = self._gasless_client()
            safe_addr = client.get_expected_safe()
            deployed = client.get_deployed(safe_addr)
            if deployed:
                self._safe_deployed = True
                return True
            log.warning(f"[MERGER] Safe NOT deployed — deploying...")
            resp = client.deploy()
            result = resp.wait()
            if result:
                self._safe_deployed = True
                return True
            return False
        except Exception as e:
            log.error(f"[MERGER] Safe preflight failed: {e}")
            return False

    def _gasless_client(self):
        return _build_relayer_client(
            self.private_key, self.builder_key, self.builder_secret,
            self.builder_passphrase, funder=self.funder,
        )

    def merge_positions(self, condition_id: str, shares: float, max_retries: int = 3) -> bool:
        if shares <= 0:
            return False
        amount = int(shares * 1_000_000)
        if amount <= 0:
            return False
        log.info(f"[MERGER] {'DRY ' if self.dry_run else ''}merge cid={condition_id[:12]} shares={shares:.4f}")
        if self.dry_run:
            return True
        if self.funder:
            return self._merge_direct(condition_id, amount, shares, max_retries)
        return self._merge_gasless(condition_id, amount, shares, max_retries)

    def _merge_direct(self, condition_id, amount, shares, max_retries):
        calldata = _encode_merge(condition_id, amount)
        calldata_bytes = bytes.fromhex(calldata.replace("0x", ""))
        factory = self._w3.eth.contract(
            address=Web3.to_checksum_address(PROXY_WALLET_FACTORY), abi=PROXY_FACTORY_ABI,
        )
        proxy_call = (CALL_TYPE_CALL, Web3.to_checksum_address(CTF_ADDRESS), 0, calldata_bytes)

        for attempt in range(1, max_retries + 1):
            try:
                nonce = self._w3.eth.get_transaction_count(self._eoa_address)
                tx = factory.functions.proxy([proxy_call]).build_transaction({
                    "chainId": CHAIN_ID, "gas": 300_000,
                    "from": self._eoa_address, "nonce": nonce,
                })
                signed = self._w3.eth.account.sign_transaction(tx, private_key=self.private_key)
                tx_hash = self._w3.eth.send_raw_transaction(signed.raw_transaction)
                receipt = self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
                if receipt.get("status") == 1:
                    log.info(f"[MERGER] Direct merge confirmed shares={shares:.4f}")
                    return True
                log.error(f"[MERGER] Direct merge REVERTED (attempt {attempt})")
            except Exception as e:
                log.error(f"[MERGER] Direct merge attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
        return False

    def _merge_gasless(self, condition_id, amount, shares, max_retries):
        from py_builder_relayer_client.models import SafeTransaction, OperationType
        if not self._safe_deployed and not self.preflight_safe():
            return False
        calldata = _encode_merge(condition_id, amount)
        tx = SafeTransaction(to=CTF_ADDRESS, operation=OperationType.Call, data=calldata, value="0")

        for attempt in range(1, max_retries + 1):
            try:
                client = self._gasless_client()
                response = client.execute([tx], f"Merge {shares:.4f}")
                result = response.wait()
                if result:
                    log.info(f"[MERGER] Gasless merge confirmed")
                    return True
            except Exception as e:
                log.error(f"[MERGER] Gasless merge attempt {attempt}/{max_retries} failed: {e}")
                if "not deployed" in str(e).lower():
                    self._safe_deployed = False
                    if not self.preflight_safe():
                        return False
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
        return False
