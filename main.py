#!/usr/bin/env python3
"""
Async Etherscan BFS Crawler (v11 – hardened)

Upgrades:
- Real periodic batched saving
- Graceful SIGINT/SIGTERM shutdown
- Better token bucket limiter
- Better retry handling for Etherscan soft failures
- Bounded tx cache
- Safer queue/task lifecycle
"""

from __future__ import annotations

import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
import random
import signal
import time
from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set

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

    concurrent_requests: int = 6
    workers: int = 6

    rate_limit_per_sec: float = 4.5

    max_retries: int = 5
    request_timeout: int = 15

    page_size: int = 10000

    output_file: str = "transactions.json"

    save_every: int = 5000
    save_interval_sec: int = 15

    resume: bool = True

    tx_cache_max_addresses: int = 300


CFG = Config()
random.seed(42)


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("crawler")


# =============================================================================
# Models
# =============================================================================

@dataclass
class Transaction:
    hash: str
    from_addr: str
    to_addr: str
    value_eth: float
    timestamp: int


@dataclass
class CrawlState:
    semaphore: asyncio.Semaphore

    seen_hashes: Set[str] = field(default_factory=set)
    visited: Set[str] = field(default_factory=set)
    enqueued: Set[str] = field(default_factory=set)

    tx_cache: OrderedDict[str, List[dict]] = field(default_factory=OrderedDict)

    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    flush_event: asyncio.Event = field(default_factory=asyncio.Event)

    total_requests: int = 0
    total_txs: int = 0
    last_saved_count: int = 0

    save_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


# =============================================================================
# Rate Limiter
# =============================================================================

class RateLimiter:
    def __init__(self, rate: float):
        self.rate = rate
        self.capacity = max(1.0, rate)
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now

                self.tokens = min(
                    self.capacity,
                    self.tokens + elapsed * self.rate,
                )

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                wait_time = (1 - self.tokens) / self.rate

            await asyncio.sleep(wait_time)


rate_limiter = RateLimiter(CFG.rate_limit_per_sec)


# =============================================================================
# Utils
# =============================================================================

def checksum(addr: str) -> Optional[str]:
    try:
        return Web3.to_checksum_address(addr)
    except Exception:
        return None


def wei_to_eth(value: str) -> float:
    try:
        return int(value) / 1e18
    except Exception:
        return 0.0


def build_url(**params) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{CFG.base_url}?{query}&apikey={CFG.api_key}"


def bounded_cache_put(state: CrawlState, address: str, txs: List[dict]) -> None:
    state.tx_cache[address] = txs
    state.tx_cache.move_to_end(address)

    while len(state.tx_cache) > CFG.tx_cache_max_addresses:
        state.tx_cache.popitem(last=False)


# =============================================================================
# HTTP
# =============================================================================

async def fetch_json(
    session: aiohttp.ClientSession,
    state: CrawlState,
    url: str,
) -> Optional[dict]:

    for attempt in range(CFG.max_retries):
        if state.stop_event.is_set():
            return None

        try:
            await rate_limiter.acquire()

            async with state.semaphore:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=CFG.request_timeout),
                ) as r:

                    state.total_requests += 1

                    if r.status == 429:
                        await asyncio.sleep(1.5 + random.random())
                        continue

                    if r.status >= 500:
                        await asyncio.sleep(min(8, 2 ** attempt))
                        continue

                    if r.status != 200:
                        return None

                    data = await r.json(content_type=None)

                    result = data.get("result")
                    message = str(data.get("message", "")).lower()

                    if isinstance(result, str) and "rate limit" in result.lower():
                        await asyncio.sleep(1.5 + random.random())
                        continue

                    if "rate limit" in message:
                        await asyncio.sleep(1.5 + random.random())
                        continue

                    return data

        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(min(8, 2 ** attempt))

    return None


async def fetch_transactions(
    session: aiohttp.ClientSession,
    state: CrawlState,
    address: str,
) -> List[dict]:

    if address in state.tx_cache:
        state.tx_cache.move_to_end(address)
        return state.tx_cache[address]

    result: List[dict] = []
    page = 1

    while not state.stop_event.is_set():
        url = build_url(
            module="account",
            action="txlist",
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

        if data.get("status") == "0":
            msg = str(data.get("message", "")).lower()

            if "no transactions" in msg:
                break

            result_field = str(data.get("result", "")).lower()
            if "rate limit" in result_field:
                await asyncio.sleep(1.5)
                continue

            break

        txs = data.get("result", [])
        if not txs:
            break

        result.extend(txs)

        if len(txs) < CFG.page_size:
            break

        page += 1

    bounded_cache_put(state, address, result)
    return result


# =============================================================================
# Persistence
# =============================================================================

async def load_existing(path: str, state: CrawlState) -> Dict[str, Transaction]:
    if not CFG.resume or not os.path.exists(path):
        return {}

    try:
        async with aiofiles.open(path, "r") as f:
            raw = await f.read()

        if not raw.strip():
            return {}

        data = json.loads(raw)

        out: Dict[str, Transaction] = {}
        for tx in data:
            h = tx["hash"]
            out[h] = Transaction(**tx)
            state.seen_hashes.add(h)

        logger.info("Loaded %s existing TXs", len(out))
        return out

    except Exception as e:
        logger.warning("Could not load existing file: %s", e)
        return {}


async def save(path: str, data: Dict[str, Transaction], state: CrawlState):
    async with state.save_lock:
        tmp_path = f"{path}.tmp"

        async with aiofiles.open(tmp_path, "w") as f:
            await f.write(
                json.dumps(
                    [asdict(v) for v in data.values()],
                    indent=2,
                )
            )

        os.replace(tmp_path, path)
        state.last_saved_count = len(data)


async def periodic_saver(
    state: CrawlState,
    results: Dict[str, Transaction],
):
    while not state.stop_event.is_set():
        try:
            await asyncio.wait_for(
                state.flush_event.wait(),
                timeout=CFG.save_interval_sec,
            )
        except asyncio.TimeoutError:
            pass

        state.flush_event.clear()

        if len(results) != state.last_saved_count:
            await save(CFG.output_file, results, state)
            logger.info("Saved %s TXs", len(results))


# =============================================================================
# Worker
# =============================================================================

async def worker(
    name: int,
    session: aiohttp.ClientSession,
    state: CrawlState,
    queue: asyncio.Queue,
    results: Dict[str, Transaction],
):
    while True:
        if state.stop_event.is_set() and queue.empty():
            return

        try:
            address, depth = await asyncio.wait_for(queue.get(), timeout=1)
        except asyncio.TimeoutError:
            continue

        try:
            if depth <= 0:
                continue

            if address in state.visited:
                continue

            state.visited.add(address)

            txs = await fetch_transactions(session, state, address)

            for tx in txs:
                if state.stop_event.is_set():
                    break

                h = tx.get("hash")
                if not h or h in state.seen_hashes:
                    continue

                fa = checksum(tx.get("from", ""))
                ta = checksum(tx.get("to", ""))

                if not fa or not ta:
                    continue

                state.seen_hashes.add(h)

                results[h] = Transaction(
                    hash=h,
                    from_addr=fa,
                    to_addr=ta,
                    value_eth=wei_to_eth(tx.get("value", "0")),
                    timestamp=int(tx.get("timeStamp", 0)),
                )

                state.total_txs += 1

                if state.total_txs % CFG.save_every == 0:
                    state.flush_event.set()

                if depth > 1:
                    for nxt in (fa, ta):
                        if nxt not in state.enqueued and nxt not in state.visited:
                            state.enqueued.add(nxt)
                            await queue.put((nxt, depth - 1))

        finally:
            queue.task_done()


# =============================================================================
# Crawl
# =============================================================================

async def crawl(
    session: aiohttp.ClientSession,
    state: CrawlState,
    results: Dict[str, Transaction],
):
    queue: asyncio.Queue = asyncio.Queue()

    start = checksum(CFG.start_address)
    if not start:
        raise ValueError("Invalid START_ADDRESS")

    await queue.put((start, CFG.depth))
    state.enqueued.add(start)

    workers = [
        asyncio.create_task(
            worker(i, session, state, queue, results)
        )
        for i in range(CFG.workers)
    ]

    saver_task = asyncio.create_task(periodic_saver(state, results))

    async def progress():
        while not state.stop_event.is_set():
            await asyncio.sleep(5)
            logger.info(
                "Progress | TXs=%s | visited=%s | requests=%s | queue=%s",
                state.total_txs,
                len(state.visited),
                state.total_requests,
                queue.qsize(),
            )

    progress_task = asyncio.create_task(progress())

    try:
        while not state.stop_event.is_set():
            if queue.empty():
                active = any(not w.done() for w in workers)
                if not active:
                    break

            await asyncio.sleep(0.25)

            if queue.empty():
                await asyncio.sleep(0.5)
                if queue.empty():
                    break

        await queue.join()

    finally:
        state.stop_event.set()

        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

        progress_task.cancel()
        saver_task.cancel()

        await asyncio.gather(
            progress_task,
            saver_task,
            return_exceptions=True,
        )

        await save(CFG.output_file, results, state)


# =============================================================================
# Main
# =============================================================================

async def main():
    if not CFG.api_key:
        logger.error("Missing ETHERSCAN_API_KEY")
        return

    if not CFG.start_address:
        logger.error("Missing START_ADDRESS")
        return

    state = CrawlState(
        semaphore=asyncio.Semaphore(CFG.concurrent_requests)
    )

    loop = asyncio.get_running_loop()

    def handle_signal():
        logger.warning("Shutdown signal received")
        state.stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    connector = aiohttp.TCPConnector(
        limit=CFG.concurrent_requests,
        ttl_dns_cache=300,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        results = await load_existing(CFG.output_file, state)

        await crawl(session, state, results)

        await save(CFG.output_file, results, state)

    logger.info(
        "Done | total TXs=%s | visited=%s | requests=%s",
        len(results),
        len(state.visited),
        state.total_requests,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted")
