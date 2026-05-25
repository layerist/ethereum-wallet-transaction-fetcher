#!/usr/bin/env python3
"""
Ultra Async Etherscan BFS Crawler (v12)

Improvements:
- NDJSON persistence (append-only, scalable)
- Faster resume
- Better token bucket limiter
- Adaptive retry/backoff
- Robust Etherscan soft-failure handling
- Proper graceful shutdown
- Better queue lifecycle
- Throughput + ETA metrics
- Lower memory usage
- Connection pooling tuning
- Safer cancellation
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
from collections import OrderedDict, deque
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, List, Optional, Set

from web3 import Web3


# =============================================================================
# CONFIG
# =============================================================================

@dataclass(frozen=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "")
    base_url: str = "https://api.etherscan.io/api"

    start_address: str = os.getenv("START_ADDRESS", "")
    depth: int = int(os.getenv("CRAWL_DEPTH", 2))

    workers: int = 12
    concurrent_requests: int = 12

    rate_limit_per_sec: float = 4.9
    burst_size: int = 6

    request_timeout: int = 20
    max_retries: int = 8

    page_size: int = 10000

    output_file: str = "transactions.ndjson"

    save_every: int = 2000
    flush_interval_sec: int = 10

    tx_cache_max_addresses: int = 500

    resume: bool = True
    skip_self_transfers: bool = False


CFG = Config()

random.seed(42)


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("crawler")


# =============================================================================
# MODELS
# =============================================================================

@dataclass(slots=True)
class Transaction:
    hash: str
    from_addr: str
    to_addr: str
    value_eth: float
    timestamp: int


@dataclass
class CrawlState:
    semaphore: asyncio.Semaphore

    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    flush_event: asyncio.Event = field(default_factory=asyncio.Event)

    save_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    seen_hashes: Set[str] = field(default_factory=set)
    visited: Set[str] = field(default_factory=set)
    enqueued: Set[str] = field(default_factory=set)

    tx_cache: OrderedDict[str, List[dict]] = field(default_factory=OrderedDict)

    total_requests: int = 0
    total_txs: int = 0

    last_save_time: float = field(default_factory=time.time)
    start_time: float = field(default_factory=time.time)


# =============================================================================
# RATE LIMITER
# =============================================================================

class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity

        self.tokens = capacity
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

                sleep_for = (1 - self.tokens) / self.rate

            await asyncio.sleep(sleep_for)


rate_limiter = TokenBucket(
    CFG.rate_limit_per_sec,
    CFG.burst_size,
)


# =============================================================================
# UTILS
# =============================================================================

def checksum(address: str) -> Optional[str]:
    try:
        return Web3.to_checksum_address(address)
    except Exception:
        return None


def wei_to_eth(value: str) -> float:
    try:
        return int(value) / 1e18
    except Exception:
        return 0.0


def build_url(**params) -> str:
    query = "&".join(
        f"{k}={v}"
        for k, v in params.items()
    )

    return (
        f"{CFG.base_url}"
        f"?{query}"
        f"&apikey={CFG.api_key}"
    )


def bounded_cache_put(
    state: CrawlState,
    address: str,
    txs: List[dict],
):
    cache = state.tx_cache

    cache[address] = txs
    cache.move_to_end(address)

    while len(cache) > CFG.tx_cache_max_addresses:
        cache.popitem(last=False)


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
                async with session.get(url) as r:

                    state.total_requests += 1

                    if r.status == 429:
                        delay = min(
                            20,
                            2 ** attempt
                        ) + random.random()

                        await asyncio.sleep(delay)
                        continue

                    if r.status >= 500:
                        await asyncio.sleep(
                            min(10, 2 ** attempt)
                        )
                        continue

                    if r.status != 200:
                        return None

                    data = await r.json(
                        content_type=None
                    )

                    result = str(
                        data.get("result", "")
                    ).lower()

                    message = str(
                        data.get("message", "")
                    ).lower()

                    if (
                        "rate limit" in result
                        or "max rate limit" in message
                    ):
                        await asyncio.sleep(
                            1.5 + random.random()
                        )
                        continue

                    return data

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ):
            sleep = (
                min(10, 2 ** attempt)
                + random.random()
            )

            await asyncio.sleep(sleep)

    return None


async def fetch_transactions(
    session: aiohttp.ClientSession,
    state: CrawlState,
    address: str,
) -> List[dict]:

    cache = state.tx_cache

    if address in cache:
        cache.move_to_end(address)
        return cache[address]

    txs_result = []
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

        data = await fetch_json(
            session,
            state,
            url,
        )

        if not data:
            break

        result = data.get("result", [])

        if not isinstance(result, list):
            break

        if not result:
            break

        txs_result.extend(result)

        if len(result) < CFG.page_size:
            break

        page += 1

    bounded_cache_put(
        state,
        address,
        txs_result,
    )

    return txs_result


# =============================================================================
# SAVE / LOAD
# =============================================================================

async def load_existing(
    path: str,
    state: CrawlState,
) -> Set[str]:

    seen = set()

    if (
        not CFG.resume
        or not Path(path).exists()
    ):
        return seen

    logger.info("Loading existing TX hashes...")

    try:
        async with aiofiles.open(
            path,
            "r"
        ) as f:

            async for line in f:
                line = line.strip()

                if not line:
                    continue

                try:
                    tx = json.loads(line)
                    h = tx["hash"]

                    seen.add(h)

                except Exception:
                    continue

    except Exception as e:
        logger.warning(
            "Resume failed: %s",
            e,
        )

    logger.info(
        "Loaded %s hashes",
        len(seen),
    )

    state.seen_hashes = seen

    return seen


async def append_transactions(
    path: str,
    txs: List[Transaction],
    state: CrawlState,
):
    if not txs:
        return

    async with state.save_lock:
        async with aiofiles.open(
            path,
            "a"
        ) as f:

            for tx in txs:
                await f.write(
                    json.dumps(
                        asdict(tx)
                    ) + "\n"
                )


async def periodic_saver(
    state: CrawlState,
    buffer: deque[Transaction],
):

    while not state.stop_event.is_set():

        try:
            await asyncio.wait_for(
                state.flush_event.wait(),
                timeout=CFG.flush_interval_sec,
            )
        except asyncio.TimeoutError:
            pass

        state.flush_event.clear()

        if buffer:
            batch = list(buffer)
            buffer.clear()

            await append_transactions(
                CFG.output_file,
                batch,
                state,
            )

            logger.info(
                "Saved %s TXs",
                len(batch),
            )


# =============================================================================
# WORKER
# =============================================================================

async def worker(
    wid: int,
    session: aiohttp.ClientSession,
    state: CrawlState,
    queue: asyncio.Queue,
    write_buffer: deque[Transaction],
):

    while True:

        if (
            state.stop_event.is_set()
            and queue.empty()
        ):
            return

        try:
            address, depth = (
                await asyncio.wait_for(
                    queue.get(),
                    timeout=1,
                )
            )
        except asyncio.TimeoutError:
            continue

        try:
            if (
                depth <= 0
                or address in state.visited
            ):
                continue

            state.visited.add(address)

            txs = await fetch_transactions(
                session,
                state,
                address,
            )

            for tx in txs:

                h = tx.get("hash")

                if (
                    not h
                    or h in state.seen_hashes
                ):
                    continue

                fa = checksum(
                    tx.get("from", "")
                )

                ta = checksum(
                    tx.get("to", "")
                )

                if not fa or not ta:
                    continue

                if (
                    CFG.skip_self_transfers
                    and fa == ta
                ):
                    continue

                state.seen_hashes.add(h)

                tr = Transaction(
                    hash=h,
                    from_addr=fa,
                    to_addr=ta,
                    value_eth=wei_to_eth(
                        tx.get("value", "0")
                    ),
                    timestamp=int(
                        tx.get(
                            "timeStamp",
                            0
                        )
                    ),
                )

                write_buffer.append(tr)

                state.total_txs += 1

                if (
                    state.total_txs
                    % CFG.save_every
                    == 0
                ):
                    state.flush_event.set()

                if depth > 1:
                    for nxt in (fa, ta):
                        if (
                            nxt
                            not in state.visited
                            and nxt
                            not in state.enqueued
                        ):
                            state.enqueued.add(
                                nxt
                            )

                            await queue.put(
                                (
                                    nxt,
                                    depth - 1,
                                )
                            )

        finally:
            queue.task_done()


# =============================================================================
# MAIN CRAWL
# =============================================================================

async def crawl(state: CrawlState):

    queue = asyncio.Queue()

    start = checksum(
        CFG.start_address
    )

    if not start:
        raise ValueError(
            "Invalid START_ADDRESS"
        )

    await queue.put(
        (
            start,
            CFG.depth,
        )
    )

    state.enqueued.add(start)

    write_buffer = deque()

    connector = aiohttp.TCPConnector(
        limit=CFG.concurrent_requests,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        keepalive_timeout=120,
    )

    timeout = aiohttp.ClientTimeout(
        total=CFG.request_timeout
    )

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
    ) as session:

        workers = [
            asyncio.create_task(
                worker(
                    i,
                    session,
                    state,
                    queue,
                    write_buffer,
                )
            )
            for i in range(
                CFG.workers
            )
        ]

        saver_task = asyncio.create_task(
            periodic_saver(
                state,
                write_buffer,
            )
        )

        try:
            while True:

                await asyncio.sleep(5)

                elapsed = (
                    time.time()
                    - state.start_time
                )

                speed = (
                    state.total_txs
                    / elapsed
                    if elapsed
                    else 0
                )

                logger.info(
                    "TX=%s | visited=%s | queue=%s | req=%s | speed=%.2f tx/s",
                    state.total_txs,
                    len(state.visited),
                    queue.qsize(),
                    state.total_requests,
                    speed,
                )

                if (
                    queue.empty()
                    and queue._unfinished_tasks
                    == 0
                ):
                    break

        finally:

            await queue.join()

            state.stop_event.set()

            for w in workers:
                w.cancel()

            await asyncio.gather(
                *workers,
                return_exceptions=True,
            )

            state.flush_event.set()

            await saver_task


# =============================================================================
# ENTRY
# =============================================================================

async def main():

    if not CFG.api_key:
        raise RuntimeError(
            "Missing ETHERSCAN_API_KEY"
        )

    if not CFG.start_address:
        raise RuntimeError(
            "Missing START_ADDRESS"
        )

    state = CrawlState(
        semaphore=asyncio.Semaphore(
            CFG.concurrent_requests
        )
    )

    loop = asyncio.get_running_loop()

    def shutdown():
        logger.warning(
            "Shutdown signal received"
        )
        state.stop_event.set()

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        try:
            loop.add_signal_handler(
                sig,
                shutdown,
            )
        except NotImplementedError:
            pass

    await load_existing(
        CFG.output_file,
        state,
    )

    await crawl(state)

    logger.info(
        "Done | TX=%s | visited=%s | requests=%s",
        state.total_txs,
        len(state.visited),
        state.total_requests,
    )


if __name__ == "__main__":
    asyncio.run(main())
