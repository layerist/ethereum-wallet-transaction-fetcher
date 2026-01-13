import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Union
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
    max_retries: int = 4
    concurrent_requests: int = 8
    request_timeout: int = 12
    output_file: str = "transactions.json"
    resume: bool = True
    rate_limit_sleep: float = 1.6


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
# Globals (controlled)
# =============================================================================

semaphore = asyncio.Semaphore(CFG.concurrent_requests)
tx_cache: Dict[str, List[Dict]] = {}
seen_hashes: Set[str] = set()

# =============================================================================
# Utilities
# =============================================================================

def checksum(addr: str) -> Optional[str]:
    try:
        return Web3.toChecksumAddress(addr)
    except Exception:
        return None


def wei_to_eth(value: Union[str, int]) -> float:
    try:
        return int(value) / 1e18
    except Exception:
        return 0.0


def build_url(module: str, action: str, **params: Union[str, int]) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return (
        f"{CFG.base_url}"
        f"?module={module}&action={action}&{query}&apikey={CFG.api_key}"
    )


async def backoff(attempt: int) -> None:
    delay = min((2 ** attempt) + random.uniform(0, 0.5), 10)
    await asyncio.sleep(delay)

# =============================================================================
# HTTP
# =============================================================================

async def fetch_json(
    session: aiohttp.ClientSession,
    url: str,
) -> Optional[Dict]:
    for attempt in range(1, CFG.max_retries + 1):
        try:
            async with semaphore:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=CFG.request_timeout),
                ) as r:
                    if r.status != 200:
                        logger.warning(f"HTTP {r.status} → {url}")
                        await backoff(attempt)
                        continue

                    return await r.json(content_type=None)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Attempt {attempt} failed → {e}")
            await backoff(attempt)

    logger.error(f"Failed after retries → {url}")
    return None


async def fetch_transactions(
    session: aiohttp.ClientSession,
    address: str,
) -> List[Dict]:
    if address in tx_cache:
        return tx_cache[address]

    url = build_url(
        "account",
        "txlist",
        address=address,
        startblock=0,
        endblock=99999999,
        sort="asc",
    )

    data = await fetch_json(session, url)
    if not data:
        tx_cache[address] = []
        return []

    status = data.get("status")
    message = str(data.get("message", "")).lower()

    if status == "0":
        if "rate limit" in message:
            logger.warning("Etherscan rate limit hit → cooling down")
            await asyncio.sleep(CFG.rate_limit_sleep)
            return []
        if "no transactions" in message:
            tx_cache[address] = []
            return []

    result = data.get("result", [])
    tx_cache[address] = result
    return result

# =============================================================================
# Crawler (queue-based, bounded)
# =============================================================================

async def crawl(
    session: aiohttp.ClientSession,
    start_address: str,
    max_depth: int,
) -> List[Dict]:
    visited: Set[str] = set()
    results: List[Dict] = []

    queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
    await queue.put((start_address, max_depth))

    while not queue.empty():
        address, depth = await queue.get()

        if depth <= 0 or address in visited:
            continue

        visited.add(address)
        logger.info(f"Crawling {address} | depth={depth}")

        txs = await fetch_transactions(session, address)
        for tx in txs:
            tx_hash = tx.get("hash")
            if not tx_hash or tx_hash in seen_hashes:
                continue

            from_addr = checksum(tx.get("from", ""))
            to_addr = checksum(tx.get("to", ""))

            if not from_addr or not to_addr:
                continue

            seen_hashes.add(tx_hash)

            results.append({
                "hash": tx_hash,
                "from": from_addr,
                "to": to_addr,
                "value_eth": wei_to_eth(tx.get("value", 0)),
                "timestamp": int(tx.get("timeStamp", 0)),
            })

            if depth > 1:
                if from_addr not in visited:
                    await queue.put((from_addr, depth - 1))
                if to_addr not in visited:
                    await queue.put((to_addr, depth - 1))

    logger.info(f"Crawl finished | addresses={len(visited)}")
    return results

# =============================================================================
# Persistence
# =============================================================================

async def load_existing(path: str) -> List[Dict]:
    if not CFG.resume or not os.path.exists(path):
        return []

    try:
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())

        for tx in data:
            if "hash" in tx:
                seen_hashes.add(tx["hash"])

        logger.info(f"Resumed {len(data)} transactions")
        return data

    except Exception as e:
        logger.warning(f"Resume failed → {e}")
        return []


async def save(path: str, data: List[Dict]) -> None:
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, indent=2))
    logger.info(f"Saved {len(data)} transactions → {path}")

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

    existing = await load_existing(CFG.output_file)

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/7.0",
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        new_data = await crawl(session, start, CFG.depth)

    merged = {tx["hash"]: tx for tx in existing}
    for tx in new_data:
        merged[tx["hash"]] = tx

    await save(CFG.output_file, list(merged.values()))
    logger.info(f"Done | TXs total: {len(merged)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
