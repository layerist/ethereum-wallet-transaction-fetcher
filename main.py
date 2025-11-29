import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
import random
from typing import List, Set, Dict, Optional, Union, Tuple
from web3 import Web3

# =============================================================================
# Configuration
# =============================================================================

API_KEY: str = os.getenv("ETHERSCAN_API_KEY", "Your_Etherscan_API_Key_Here")
BASE_URL: str = "https://api.etherscan.io/api"
START_ADDRESS: str = os.getenv("START_ADDRESS", "Your_Ethereum_Wallet_Address_Here")

DEPTH: int = int(os.getenv("CRAWL_DEPTH", 2))
MAX_RETRIES: int = 3
CONCURRENT_REQUESTS: int = 10
REQUEST_TIMEOUT: int = 10

OUTPUT_FILE: str = "transactions.json"
RESUME: bool = True

# Optional: set deterministic randomness for reproducible backoff timings
random.seed(42)

# =============================================================================
# Logging Setup
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("etherscan-crawler")

# =============================================================================
# Global State
# =============================================================================

semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
transaction_cache: Dict[str, List[Dict]] = {}
seen_transactions: Set[str] = set()


# =============================================================================
# Utility Functions
# =============================================================================

def wei_to_eth(wei: Union[str, int]) -> float:
    """Convert Wei → Ether."""
    try:
        return int(wei) / 1e18
    except Exception:
        return 0.0


def etherscan_url(module: str, action: str, **params) -> str:
    """Build Etherscan API URL."""
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_URL}?module={module}&action={action}&{query}&apikey={API_KEY}"


async def exponential_backoff(attempt: int) -> None:
    """Sleep using exponential backoff + jitter."""
    delay = (2 ** attempt) + random.uniform(0, 0.5)
    await asyncio.sleep(delay)


# =============================================================================
# Network Requests
# =============================================================================

async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
    """Fetch URL with retries, backoff, and JSON parsing."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f"[Attempt {attempt}] HTTP {resp.status} for {url}"
                        )
                        await exponential_backoff(attempt)
                        continue

                    data = await resp.json(content_type=None)
                    return data

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"[Attempt {attempt}] Request failed: {e}")
            await exponential_backoff(attempt)

    logger.error(f"Failed after {MAX_RETRIES} retries → {url}")
    return None


async def fetch_transactions(
    session: aiohttp.ClientSession, address: str
) -> List[Dict]:
    """
    Fetch all normal transactions for a given address.
    Caches results to avoid duplicate API calls.
    """
    try:
        checksum_address = Web3.toChecksumAddress(address)
    except Exception:
        logger.debug(f"Invalid Ethereum address: {address}")
        return []

    if checksum_address in transaction_cache:
        return transaction_cache[checksum_address]

    url = etherscan_url(
        "account", "txlist",
        address=checksum_address,
        startblock=0,
        endblock=99999999,
        sort="asc"
    )

    logger.debug(f"Fetching txs for {checksum_address}")
    resp = await fetch_json(session, url)

    if not resp:
        transaction_cache[checksum_address] = []
        return []

    # Etherscan error-handling
    status = resp.get("status")
    message = resp.get("message")

    if status == "0" and "rate limit" in message.lower():
        logger.warning("Rate limit reached. Slowing down...")
        await asyncio.sleep(1.5)
        return await fetch_transactions(session, address)

    if status == "0" and "No transactions" in message:
        transaction_cache[checksum_address] = []
        return []

    result = resp.get("result", [])
    transaction_cache[checksum_address] = result
    return result


# =============================================================================
# Core Crawler Logic
# =============================================================================

async def process_address(
    session: aiohttp.ClientSession,
    address: str,
    visited: Set[str],
    depth: int,
) -> List[Dict]:
    """Recursively process an Ethereum address and related addresses."""
    try:
        checksum_address = Web3.toChecksumAddress(address)
    except Exception:
        return []

    if depth <= 0 or checksum_address in visited:
        return []

    visited.add(checksum_address)
    logger.info(f"Exploring {checksum_address} | depth={depth}")

    txs = await fetch_transactions(session, checksum_address)
    if not txs:
        return []

    results: List[Dict] = []
    next_addresses: Set[str] = set()

    for tx in txs:
        tx_hash = tx.get("hash")
        if not tx_hash or tx_hash in seen_transactions:
            continue

        seen_transactions.add(tx_hash)

        from_addr = tx.get("from")
        to_addr = tx.get("to")

        if not from_addr or not to_addr:
            continue

        results.append({
            "hash": tx_hash,
            "from": from_addr,
            "to": to_addr,
            "value": wei_to_eth(tx.get("value", 0)),
            "timestamp": tx.get("timeStamp")
        })

        if depth > 1:
            if from_addr not in visited:
                next_addresses.add(from_addr)
            if to_addr not in visited:
                next_addresses.add(to_addr)

    # Recurse
    if next_addresses and depth > 1:
        subtasks = [
            process_address(session, addr, visited, depth - 1)
            for addr in next_addresses
        ]
        nested_results = await asyncio.gather(*subtasks, return_exceptions=True)

        for r in nested_results:
            if isinstance(r, list):
                results.extend(r)

    return results


# =============================================================================
# File IO
# =============================================================================

async def save_to_file(data: List[Dict], filename: str) -> None:
    """Save transaction data to JSON file."""
    try:
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        logger.info(f"Saved {len(data)} transactions → {filename}")
    except Exception as e:
        logger.error(f"File write error: {e}")


async def load_existing_results(filename: str) -> List[Dict]:
    """Load previous crawl results."""
    if not RESUME or not os.path.exists(filename):
        return []

    try:
        async with aiofiles.open(filename, "r", encoding="utf-8") as f:
            content = await f.read()
            data = json.loads(content)

        for tx in data:
            seen_transactions.add(tx.get("hash", ""))

        logger.info(f"Resumed with {len(data)} existing transactions.")
        return data
    except Exception as e:
        logger.warning(f"Resume failed: {e}")
        return []


# =============================================================================
# Main
# =============================================================================

async def main() -> None:
    """Entry point."""
    if not API_KEY or not START_ADDRESS:
        logger.error("ETHERSCAN_API_KEY and START_ADDRESS must be set.")
        return

    logger.info(f"Start crawling from {START_ADDRESS} (depth={DEPTH})")

    visited: Set[str] = set()
    existing_data = await load_existing_results(OUTPUT_FILE)
    all_data: List[Dict] = list(existing_data)

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/5.0"
    }

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            new_data = await process_address(session, START_ADDRESS, visited, DEPTH)

            # Deduplicate final output
            combined = {tx["hash"]: tx for tx in all_data}
            for tx in new_data:
                combined[tx["hash"]] = tx

            final_list = list(combined.values())
            await save_to_file(final_list, OUTPUT_FILE)

        logger.info(
            f"Crawl completed. "
            f"Addresses explored: {len(visited)} | "
            f"Unique TXs: {len(final_list)}"
        )

    except asyncio.CancelledError:
        logger.warning("Cancelled by user.")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")


if __name__ == "__main__":
    asyncio.run(main())
