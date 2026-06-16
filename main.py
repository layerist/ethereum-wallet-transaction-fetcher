#!/usr/bin/env python3
"""
Ultra Async Etherscan BFS Crawler (v13)

What this version improves over v12:
- Final flush is guaranteed on normal finish and Ctrl+C/SIGTERM.
- No private asyncio.Queue internals are used.
- Safer URL building via urlencode.
- More runtime configuration through environment variables.
- Atomic-ish NDJSON writes through one writer task and periodic flushes.
- Better retry/backoff and Etherscan soft-error handling.
- Optional startblock/endblock, chainid, internal transactions.
- Safer JSON serialization and ETH value formatting.
- Output directory is created automatically.
- Cleaner shutdown without losing buffered transactions.

Required env:
  ETHERSCAN_API_KEY=...
  START_ADDRESS=0x...

Useful optional env:
  CRAWL_DEPTH=2
  CRAWL_WORKERS=12
  CRAWL_CONCURRENT_REQUESTS=12
  ETHERSCAN_RATE_LIMIT_PER_SEC=4.8
  ETHERSCAN_BURST_SIZE=5
  ETHERSCAN_CHAIN_ID=1
  ETHERSCAN_STARTBLOCK=0
  ETHERSCAN_ENDBLOCK=99999999
  OUTPUT_FILE=transactions.ndjson
  RESUME=true
  SKIP_SELF_TRANSFERS=false
  INCLUDE_INTERNAL_TXS=false
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
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Deque, Optional
from urllib.parse import urlencode

from web3 import Web3


# =============================================================================
# CONFIG HELPERS
# =============================================================================

def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


# =============================================================================
# CONFIG
# =============================================================================

@dataclass(frozen=True, slots=True)
class Config:
    api_key: str = os.getenv("ETHERSCAN_API_KEY", "").strip()
    base_url: str = os.getenv("ETHERSCAN_BASE_URL", "https://api.etherscan.io/api").strip()
    chain_id: str = os.getenv("ETHERSCAN_CHAIN_ID", "").strip()

    start_address: str = os.getenv("START_ADDRESS", "").strip()
    depth: int = env_int("CRAWL_DEPTH", 2)

    workers: int = env_int("CRAWL_WORKERS", 12)
    concurrent_requests: int = env_int("CRAWL_CONCURRENT_REQUESTS", 12)

    rate_limit_per_sec: float = env_float("ETHERSCAN_RATE_LIMIT_PER_SEC", 4.8)
    burst_size: int = env_int("ETHERSCAN_BURST_SIZE", 5)

    request_timeout: int = env_int("ETHERSCAN_REQUEST_TIMEOUT", 25)
    max_retries: int = env_int("ETHERSCAN_MAX_RETRIES", 8)

    startblock: int = env_int("ETHERSCAN_STARTBLOCK", 0)
    endblock: int = env_int("ETHERSCAN_ENDBLOCK", 99999999)
    page_size: int = env_int("ETHERSCAN_PAGE_SIZE", 10000)

    output_file: str = os.getenv("OUTPUT_FILE", "transactions.ndjson").strip()

    save_every: int = env_int("SAVE_EVERY", 2000)
    flush_interval_sec: int = env_int("FLUSH_INTERVAL_SEC", 10)

    tx_cache_max_addresses: int = env_int("TX_CACHE_MAX_ADDRESSES", 500)

    resume: bool = env_bool("RESUME", True)
    skip_self_transfers: bool = env_bool("SKIP_SELF_TRANSFERS", False)
    include_internal_txs: bool = env_bool("INCLUDE_INTERNAL_TXS", False)

    log_every_sec: int = env_int("LOG_EVERY_SEC", 5)


CFG = Config()
random.seed()


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
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
    value_wei: str
    value_eth: str
    timestamp: int
    block_number: int
    kind: str = "normal"


@dataclass
class CrawlState:
    semaphore: asyncio.Semaphore

    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    flush_event: asyncio.Event = field(default_factory=asyncio.Event)
    queue_drained_event: asyncio.Event = field(default_factory=asyncio.Event)

    seen_hashes: set[str] = field(default_factory=set)
    visited: set[str] = field(default_factory=set)
    enqueued: set[str] = field(default_factory=set)

    tx_cache: OrderedDict[str, list[dict[str, Any]]] = field(default_factory=OrderedDict)

    total_requests: int = 0
    total_http_errors: int = 0
    total_soft_errors: int = 0
    total_txs_written: int = 0
    total_txs_seen_this_run: int = 0
    total_addresses_failed: int = 0

    start_time: float = field(default_factory=time.time)


# =============================================================================
# RATE LIMITER
# =============================================================================

class TokenBucket:
    def __init__(self, rate: float, capacity: int):
        if rate <= 0:
            raise ValueError("rate must be positive")
        if capacity <= 0:
            raise ValueError("capacity must be positive")

        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated
                self.updated = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                sleep_for = max(0.0, (1 - self.tokens) / self.rate)

            await asyncio.sleep(sleep_for)


rate_limiter = TokenBucket(CFG.rate_limit_per_sec, CFG.burst_size)


# =============================================================================
# UTILS
# =============================================================================

def checksum(address: str | None) -> Optional[str]:
    if not address:
        return None
    try:
        return Web3.to_checksum_address(address)
    except Exception:
        return None


def wei_to_eth_str(value: str | int | None) -> str:
    try:
        wei = Decimal(str(value or "0"))
        return format(wei / Decimal("1000000000000000000"), "f")
    except (InvalidOperation, ValueError):
        return "0"


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def build_url(**params: Any) -> str:
    clean_params = {k: v for k, v in params.items() if v is not None and v != ""}

    if CFG.chain_id:
        clean_params.setdefault("chainid", CFG.chain_id)

    clean_params["apikey"] = CFG.api_key
    return f"{CFG.base_url}?{urlencode(clean_params)}"


def bounded_cache_put(state: CrawlState, address: str, txs: list[dict[str, Any]]) -> None:
    cache = state.tx_cache
    cache[address] = txs
    cache.move_to_end(address)

    while len(cache) > CFG.tx_cache_max_addresses:
        cache.popitem(last=False)


def is_rate_limit_soft_error(data: dict[str, Any]) -> bool:
    result = str(data.get("result", "")).lower()
    message = str(data.get("message", "")).lower()
    return (
        "rate limit" in result
        or "max rate limit" in result
        or "rate limit" in message
        or "max rate limit" in message
        or "too many requests" in result
        or "too many requests" in message
    )


def is_empty_etherscan_response(data: dict[str, Any]) -> bool:
    # Etherscan often returns: {"status":"0", "message":"No transactions found", "result":[]}
    result = data.get("result")
    message = str(data.get("message", "")).lower()
    return result == [] or "no transactions found" in message


# =============================================================================
# HTTP
# =============================================================================

async def fetch_json(
    session: aiohttp.ClientSession,
    state: CrawlState,
    url: str,
) -> Optional[dict[str, Any]]:
    for attempt in range(CFG.max_retries):
        if state.stop_event.is_set():
            return None

        try:
            await rate_limiter.acquire()

            async with state.semaphore:
                async with session.get(url) as r:
                    state.total_requests += 1

                    if r.status == 429:
                        delay = min(30.0, 1.5 * (2 ** attempt)) + random.random()
                        logger.debug("HTTP 429, retry in %.2fs", delay)
                        await asyncio.sleep(delay)
                        continue

                    if r.status in {403, 418}:
                        state.total_http_errors += 1
                        body = await r.text()
                        logger.warning("HTTP %s: %s", r.status, body[:300])
                        await asyncio.sleep(min(30.0, 2 ** attempt) + random.random())
                        continue

                    if r.status >= 500:
                        state.total_http_errors += 1
                        await asyncio.sleep(min(20.0, 2 ** attempt) + random.random())
                        continue

                    if r.status != 200:
                        state.total_http_errors += 1
                        body = await r.text()
                        logger.warning("Unexpected HTTP %s: %s", r.status, body[:300])
                        return None

                    try:
                        data = await r.json(content_type=None)
                    except Exception:
                        body = await r.text()
                        logger.warning("Invalid JSON response: %s", body[:300])
                        return None

                    if not isinstance(data, dict):
                        return None

                    if is_rate_limit_soft_error(data):
                        state.total_soft_errors += 1
                        delay = min(20.0, 1.2 * (2 ** attempt)) + random.random()
                        logger.debug("Etherscan soft rate limit, retry in %.2fs", delay)
                        await asyncio.sleep(delay)
                        continue

                    return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            delay = min(20.0, 2 ** attempt) + random.random()
            logger.debug("Request failed: %r, retry in %.2fs", e, delay)
            await asyncio.sleep(delay)

    return None


async def fetch_tx_action(
    session: aiohttp.ClientSession,
    state: CrawlState,
    address: str,
    action: str,
) -> list[dict[str, Any]]:
    txs_result: list[dict[str, Any]] = []
    page = 1

    while not state.stop_event.is_set():
        url = build_url(
            module="account",
            action=action,
            address=address,
            startblock=CFG.startblock,
            endblock=CFG.endblock,
            page=page,
            offset=CFG.page_size,
            sort="asc",
        )

        data = await fetch_json(session, state, url)
        if not data:
            break

        if is_empty_etherscan_response(data):
            break

        result = data.get("result", [])
        if not isinstance(result, list):
            # Examples: "Max rate limit reached", "Invalid API Key".
            state.total_soft_errors += 1
            logger.debug("Non-list result for %s/%s: %r", address, action, result)
            break

        if not result:
            break

        txs_result.extend(result)

        if len(result) < CFG.page_size:
            break

        page += 1

    return txs_result


async def fetch_transactions(
    session: aiohttp.ClientSession,
    state: CrawlState,
    address: str,
) -> list[dict[str, Any]]:
    cache = state.tx_cache
    if address in cache:
        cache.move_to_end(address)
        return cache[address]

    normal_txs = await fetch_tx_action(session, state, address, "txlist")

    if CFG.include_internal_txs:
        internal_txs = await fetch_tx_action(session, state, address, "txlistinternal")
        for tx in internal_txs:
            tx.setdefault("_kind", "internal")
        txs_result = normal_txs + internal_txs
    else:
        txs_result = normal_txs

    bounded_cache_put(state, address, txs_result)
    return txs_result


# =============================================================================
# SAVE / LOAD
# =============================================================================

async def load_existing(path: str, state: CrawlState) -> set[str]:
    seen: set[str] = set()

    if not CFG.resume or not Path(path).exists():
        return seen

    logger.info("Loading existing TX hashes from %s...", path)

    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    tx = json.loads(line)
                    h = tx.get("hash")
                    if h:
                        seen.add(h)
                except Exception:
                    continue

    except Exception as e:
        logger.warning("Resume failed: %s", e)

    logger.info("Loaded %s existing hashes", len(seen))
    state.seen_hashes = seen
    return seen


async def append_transactions(path: str, txs: list[Transaction]) -> None:
    if not txs:
        return

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [json.dumps(asdict(tx), ensure_ascii=False, separators=(",", ":")) + "\n" for tx in txs]

    async with aiofiles.open(output_path, "a", encoding="utf-8") as f:
        await f.writelines(lines)


async def flush_buffer(
    state: CrawlState,
    buffer: Deque[Transaction],
) -> int:
    if not buffer:
        return 0

    batch = list(buffer)
    buffer.clear()
    await append_transactions(CFG.output_file, batch)
    state.total_txs_written += len(batch)
    return len(batch)


async def periodic_saver(
    state: CrawlState,
    buffer: Deque[Transaction],
) -> None:
    try:
        while True:
            if state.stop_event.is_set() and not buffer:
                return

            try:
                await asyncio.wait_for(
                    state.flush_event.wait(),
                    timeout=CFG.flush_interval_sec,
                )
            except asyncio.TimeoutError:
                pass

            state.flush_event.clear()
            saved = await flush_buffer(state, buffer)
            if saved:
                logger.info("Saved %s TXs", saved)

    except asyncio.CancelledError:
        saved = await flush_buffer(state, buffer)
        if saved:
            logger.info("Saved %s TXs before saver cancellation", saved)
        raise


# =============================================================================
# WORKER
# =============================================================================

async def worker(
    wid: int,
    session: aiohttp.ClientSession,
    state: CrawlState,
    queue: asyncio.Queue[tuple[str, int]],
    write_buffer: Deque[Transaction],
) -> None:
    while not state.stop_event.is_set():
        try:
            address, depth = await asyncio.wait_for(queue.get(), timeout=1)
        except asyncio.TimeoutError:
            if state.queue_drained_event.is_set():
                return
            continue

        try:
            if depth <= 0 or address in state.visited:
                continue

            state.visited.add(address)
            txs = await fetch_transactions(session, state, address)

            if not txs and state.stop_event.is_set():
                state.total_addresses_failed += 1
                continue

            for tx in txs:
                h = tx.get("hash")
                if not h or h in state.seen_hashes:
                    continue

                fa = checksum(tx.get("from", ""))
                ta = checksum(tx.get("to", ""))

                if not fa or not ta:
                    continue

                if CFG.skip_self_transfers and fa == ta:
                    continue

                state.seen_hashes.add(h)
                state.total_txs_seen_this_run += 1

                value_wei = str(tx.get("value", "0") or "0")
                tr = Transaction(
                    hash=h,
                    from_addr=fa,
                    to_addr=ta,
                    value_wei=value_wei,
                    value_eth=wei_to_eth_str(value_wei),
                    timestamp=safe_int(tx.get("timeStamp", 0)),
                    block_number=safe_int(tx.get("blockNumber", 0)),
                    kind=str(tx.get("_kind", "normal")),
                )

                write_buffer.append(tr)

                if len(write_buffer) >= CFG.save_every:
                    state.flush_event.set()

                if depth > 1:
                    for nxt in (fa, ta):
                        if nxt not in state.visited and nxt not in state.enqueued:
                            state.enqueued.add(nxt)
                            await queue.put((nxt, depth - 1))

        except asyncio.CancelledError:
            raise
        except Exception:
            state.total_addresses_failed += 1
            logger.exception("Worker %s failed on address=%s depth=%s", wid, address, depth)
        finally:
            queue.task_done()


# =============================================================================
# MONITOR
# =============================================================================

async def monitor(state: CrawlState, queue: asyncio.Queue[tuple[str, int]]) -> None:
    while not state.stop_event.is_set() and not state.queue_drained_event.is_set():
        await asyncio.sleep(CFG.log_every_sec)

        elapsed = max(0.001, time.time() - state.start_time)
        speed = state.total_txs_seen_this_run / elapsed

        logger.info(
            "seen_run=%s | written=%s | visited=%s | enqueued=%s | queue=%s | "
            "req=%s | http_err=%s | soft_err=%s | speed=%.2f tx/s",
            state.total_txs_seen_this_run,
            state.total_txs_written,
            len(state.visited),
            len(state.enqueued),
            queue.qsize(),
            state.total_requests,
            state.total_http_errors,
            state.total_soft_errors,
            speed,
        )


def drain_queue(queue: asyncio.Queue[tuple[str, int]]) -> int:
    drained = 0
    while True:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            return drained
        else:
            drained += 1
            queue.task_done()


# =============================================================================
# MAIN CRAWL
# =============================================================================

async def crawl(state: CrawlState) -> None:
    queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()

    start = checksum(CFG.start_address)
    if not start:
        raise ValueError("Invalid START_ADDRESS")

    await queue.put((start, CFG.depth))
    state.enqueued.add(start)

    write_buffer: Deque[Transaction] = deque()

    connector = aiohttp.TCPConnector(
        limit=CFG.concurrent_requests,
        limit_per_host=CFG.concurrent_requests,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        keepalive_timeout=120,
    )

    timeout = aiohttp.ClientTimeout(total=CFG.request_timeout)

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-bfs-crawler/13.0",
    }

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=headers,
    ) as session:
        workers = [
            asyncio.create_task(worker(i, session, state, queue, write_buffer))
            for i in range(CFG.workers)
        ]
        saver_task = asyncio.create_task(periodic_saver(state, write_buffer))
        monitor_task = asyncio.create_task(monitor(state, queue))

        try:
            join_task = asyncio.create_task(queue.join())
            stop_task = asyncio.create_task(state.stop_event.wait())

            done, pending = await asyncio.wait(
                {join_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_task in done and not join_task.done():
                dropped = drain_queue(queue)
                if dropped:
                    logger.warning("Dropped %s queued addresses during shutdown", dropped)
                await join_task

            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            state.queue_drained_event.set()
        finally:
            state.stop_event.set()
            state.queue_drained_event.set()
            state.flush_event.set()

            await asyncio.gather(*workers, return_exceptions=True)

            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)

            await saver_task


# =============================================================================
# ENTRY
# =============================================================================

async def main() -> None:
    if not CFG.api_key:
        raise RuntimeError("Missing ETHERSCAN_API_KEY")

    if not CFG.start_address:
        raise RuntimeError("Missing START_ADDRESS")

    if CFG.depth < 1:
        raise RuntimeError("CRAWL_DEPTH must be >= 1")

    state = CrawlState(semaphore=asyncio.Semaphore(CFG.concurrent_requests))

    loop = asyncio.get_running_loop()

    def shutdown() -> None:
        logger.warning("Shutdown signal received")
        state.stop_event.set()
        state.flush_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except (NotImplementedError, RuntimeError):
            pass

    await load_existing(CFG.output_file, state)

    try:
        await crawl(state)
    finally:
        elapsed = max(0.001, time.time() - state.start_time)
        logger.info(
            "Done | seen_run=%s | written=%s | visited=%s | enqueued=%s | "
            "requests=%s | http_err=%s | soft_err=%s | failed_addr=%s | elapsed=%.1fs",
            state.total_txs_seen_this_run,
            state.total_txs_written,
            len(state.visited),
            len(state.enqueued),
            state.total_requests,
            state.total_http_errors,
            state.total_soft_errors,
            state.total_addresses_failed,
            elapsed,
        )


if __name__ == "__main__":
    asyncio.run(main())
