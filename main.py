#!/usr/bin/env python3
"""
Async Etherscan BFS Crawler (v9 – optimized & production ready)

Improvements:
- Worker-based concurrent BFS (true parallel crawling)
- Encapsulated shared state (no uncontrolled globals)
- Deterministic merging without O(n²)
- Better rate-limit handling
- Stronger typing & validation
- Clean shutdown support
"""

from __future__ import annotations

import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Union

from web3 import Web3

# =============================================================================
# Configuration
# =============================================================================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "")
    base_url: str = "https://api.etherscan.io/api"
    start_address: str = os.getenv("START_ADDRESS", "")
    depth: int = int(os.getenv("CRAWL_DEPTH", 2))

    max_retries: int = 5
    concurrent_requests: int = 8
    request_timeout: int = 15
    page_size: int = 10_000

    output_file: str = "transactions.json"
    resume: bool = True

    rate_limit_sleep: float = 1.5
    max_backoff: float = 12.0
    workers: int = 6


CFG = Config()
random.seed(42)

# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("etherscan-crawler")

# =============================================================================
# Runtime State
# =============================================================================

@dataclass
class CrawlState:
    semaphore: asyncio.Semaphore
    seen_hashes: Set[str] = field(default_factory=set)
    visited: Set[str] = field(default_factory=set)
    tx_cache: Dict[str, List[Dict]] = field(default_factory=dict)


# =============================================================================
# Utilities
# =============================================================================

def checksum(addr: str) -> Optional[str]:
    try:
        return Web3.to_checksum_address(addr)
    except Exception:
        return None


def wei_to_eth(value: Union[str, int]) -> float:
    try:
        return int(value) / 1e18
    except Exception:
        return 0.0


def build_url(module: str, action: str, **params: Union[str, int]) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{CFG.base_url}?module={module}&action={action}&{query}&apikey={CFG.api_key}"


async def backoff(attempt: int) -> None:
    delay = min((2 ** attempt) + random.random(), CFG.max_backoff)
    await asyncio.sleep(delay)


# =============================================================================
# HTTP
# =============================================================================

async def fetch_json(
    session: aiohttp.ClientSession,
    state: CrawlState,
    url: str,
) -> Optional[Dict]:

    for attempt in range(1, CFG.max_retries + 1):
        try:
            async with state.semaphore:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=CFG.request_timeout),
                ) as r:

                    if r.status == 429:
                        logger.warning("HTTP 429 → rate limited")
                        await asyncio.sleep(CFG.rate_limit_sleep)
                        continue

                    if r.status != 200:
                        logger.warning("HTTP %s → retrying", r.status)
                        await backoff(attempt)
                        continue

                    return await r.json(content_type=None)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning("Attempt %s failed → %s", attempt, e)
            await backoff(attempt)

    logger.error("Failed after retries → %s", url)
    return None


async def fetch_transactions(
    session: aiohttp.ClientSession,
    state: CrawlState,
    address: str,
) -> List[Dict]:

    if address in state.tx_cache:
        return state.tx_cache[address]

    all_txs: List[Dict] = []
    page = 1

    while True:
        url = build_url(
            "account",
            "txlist",
            address=address,
            startblock=0,
            endblock=99999999,
            page=page,
            offset=CFG.page_size,
            sort="asc",
        )

        data = await fetch_json(session, state, url)
        if not data:
            break

        status = data.get("status")
        message = str(data.get("message", "")).lower()

        if status == "0":
            if "rate limit" in message:
                await asyncio.sleep(CFG.rate_limit_sleep)
                continue
            if "no transactions" in message:
                break
            logger.warning("Etherscan error → %s", message)
            break

        batch = data.get("result", [])
        if not batch:
            break

        all_txs.extend(batch)

        if len(batch) < CFG.page_size:
            break

        page += 1

    state.tx_cache[address] = all_txs
    return all_txs


# =============================================================================
# Worker-Based BFS
# =============================================================================

async def worker(
    name: int,
    session: aiohttp.ClientSession,
    state: CrawlState,
    queue: asyncio.Queue[Tuple[str, int]],
    results: Dict[str, Dict],
):

    while True:
        try:
            address, depth = await queue.get()
        except asyncio.CancelledError:
            break

        try:
            if depth <= 0 or address in state.visited:
                continue

            state.visited.add(address)
            logger.info("[W%s] Crawling %s | depth=%s", name, address, depth)

            txs = await fetch_transactions(session, state, address)

            for tx in txs:
                tx_hash = tx.get("hash")
                if not tx_hash or tx_hash in state.seen_hashes:
                    continue

                from_addr = checksum(tx.get("from", ""))
                to_addr = checksum(tx.get("to", ""))

                if not from_addr or not to_addr:
                    continue

                state.seen_hashes.add(tx_hash)

                results[tx_hash] = {
                    "hash": tx_hash,
                    "from": from_addr,
                    "to": to_addr,
                    "value_eth": wei_to_eth(tx.get("value", 0)),
                    "timestamp": int(tx.get("timeStamp", 0)),
                }

                if depth > 1:
                    if from_addr not in state.visited:
                        await queue.put((from_addr, depth - 1))
                    if to_addr not in state.visited:
                        await queue.put((to_addr, depth - 1))

        finally:
            queue.task_done()


async def crawl(
    session: aiohttp.ClientSession,
    start_address: str,
    max_depth: int,
    state: CrawlState,
) -> Dict[str, Dict]:

    queue: asyncio.Queue[Tuple[str, int]] = asyncio.Queue()
    await queue.put((start_address, max_depth))

    results: Dict[str, Dict] = {}

    workers = [
        asyncio.create_task(worker(i, session, state, queue, results))
        for i in range(CFG.workers)
    ]

    await queue.join()

    for w in workers:
        w.cancel()

    await asyncio.gather(*workers, return_exceptions=True)

    logger.info("Crawl finished | addresses=%s", len(state.visited))
    return results


# =============================================================================
# Persistence
# =============================================================================

async def load_existing(path: str, state: CrawlState) -> Dict[str, Dict]:
    if not CFG.resume or not os.path.exists(path):
        return {}

    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())

        existing = {}
        for tx in data:
            if "hash" in tx:
                existing[tx["hash"]] = tx
                state.seen_hashes.add(tx["hash"])

        logger.info("Resumed %s transactions", len(existing))
        return existing

    except Exception as e:
        logger.warning("Resume failed → %s", e)
        return {}


async def save(path: str, data: Dict[str, Dict]) -> None:
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(list(data.values()), indent=2))

    logger.info("Saved %s transactions → %s", len(data), path)


# =============================================================================
# Main
# =============================================================================

async def main() -> None:
    if not CFG.api_key or not CFG.start_address:
        logger.error("ETHERSCAN_API_KEY and START_ADDRESS must be set")
        return

    start = checksum(CFG.start_address)
    if not start:
        logger.error("Invalid START_ADDRESS")
        return

    state = CrawlState(
        semaphore=asyncio.Semaphore(CFG.concurrent_requests)
    )

    connector = aiohttp.TCPConnector(limit=CFG.concurrent_requests)
    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/9.0",
    }

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:

        existing = await load_existing(CFG.output_file, state)

        new_data = await crawl(session, start, CFG.depth, state)

        existing.update(new_data)

        await save(CFG.output_file, existing)

    logger.info("Done | TXs total: %s", len(existing))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
