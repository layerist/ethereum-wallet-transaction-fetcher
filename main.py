import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
import random
from typing import List, Set, Dict, Optional, Union
from web3 import Web3

# === Configuration ===
API_KEY: str = os.getenv("ETHERSCAN_API_KEY", "Your_Etherscan_API_Key_Here")
BASE_URL: str = "https://api.etherscan.io/api"
START_ADDRESS: str = os.getenv("START_ADDRESS", "Your_Ethereum_Wallet_Address_Here")
DEPTH: int = int(os.getenv("CRAWL_DEPTH", 2))
MAX_RETRIES: int = 3
CONCURRENT_REQUESTS: int = 10
OUTPUT_FILE: str = "transactions.json"
REQUEST_TIMEOUT: int = 10
RESUME: bool = True  # if True, will reload previous results from OUTPUT_FILE

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("etherscan-crawler")

# === Global state ===
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
transaction_cache: Dict[str, List[Dict]] = {}
seen_transactions: Set[str] = set()


def wei_to_eth(wei: Union[str, int]) -> float:
    """Convert Wei to Ether safely."""
    try:
        return int(wei) / 1e18
    except (ValueError, TypeError):
        return 0.0


def etherscan_url(module: str, action: str, **params) -> str:
    """Build a properly formatted Etherscan API URL."""
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_URL}?module={module}&action={action}&{query}&apikey={API_KEY}"


async def fetch_with_retries(session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
    """Fetch JSON data from a URL with retries, exponential backoff, and jitter."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    if resp.status != 200:
                        logger.warning(f"[{attempt}/{MAX_RETRIES}] HTTP {resp.status} for {url}")
                        continue

                    try:
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict) and "result" in data:
                            return data
                    except Exception as e:
                        logger.warning(f"Invalid JSON response: {e}")
                        continue

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug(f"Attempt {attempt} failed: {e}")

        # Exponential backoff with jitter
        delay = (2 ** attempt) + random.uniform(0, 0.5)
        await asyncio.sleep(delay)

    logger.error(f"All retries failed for URL: {url}")
    return None


async def fetch_transactions(session: aiohttp.ClientSession, address: str) -> List[Dict]:
    """Fetch all normal transactions for a given Ethereum address (with caching)."""
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

    logger.debug(f"Fetching transactions for {checksum_address}")
    data = await fetch_with_retries(session, url)
    txs = data.get("result", []) if data else []
    transaction_cache[checksum_address] = txs
    return txs


async def process_address(
    session: aiohttp.ClientSession,
    address: str,
    visited: Set[str],
    depth: int
) -> List[Dict]:
    """Recursively process an Ethereum address and its related transactions."""
    try:
        checksum_address = Web3.toChecksumAddress(address)
    except Exception:
        logger.debug(f"Invalid address skipped: {address}")
        return []

    if checksum_address in visited or depth <= 0:
        return []

    visited.add(checksum_address)
    logger.info(f"Exploring address: {checksum_address} | Depth: {depth}")

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

        from_addr, to_addr = tx.get("from"), tx.get("to")
        if not from_addr or not to_addr:
            continue

        results.append({
            "hash": tx_hash,
            "from": from_addr,
            "to": to_addr,
            "value": wei_to_eth(tx.get("value", "0")),
            "timestamp": tx.get("timeStamp")
        })

        if depth > 1:
            if from_addr not in visited:
                next_addresses.add(from_addr)
            if to_addr not in visited:
                next_addresses.add(to_addr)

    if next_addresses and depth > 1:
        subtasks = [
            asyncio.create_task(process_address(session, addr, visited, depth - 1))
            for addr in next_addresses
        ]
        try:
            results_lists = await asyncio.gather(*subtasks, return_exceptions=True)
            for r in results_lists:
                if isinstance(r, list):
                    results.extend(r)
        except asyncio.CancelledError:
            for t in subtasks:
                t.cancel()
            raise

    return results


async def save_to_file(data: List[Dict], filename: str) -> None:
    """Save transaction data to a JSON file asynchronously."""
    try:
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            json_data = json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True)
            await f.write(json_data)
        logger.info(f"Saved {len(data)} transactions → {filename}")
    except Exception as e:
        logger.error(f"Failed to write file '{filename}': {e}")


async def load_existing_results(filename: str) -> List[Dict]:
    """Load existing results from a JSON file if RESUME mode is enabled."""
    if not RESUME or not os.path.exists(filename):
        return []

    try:
        async with aiofiles.open(filename, "r", encoding="utf-8") as f:
            data = json.loads(await f.read())
            for tx in data:
                seen_transactions.add(tx.get("hash"))
            logger.info(f"Resumed from {len(data)} existing transactions.")
            return data
    except Exception as e:
        logger.warning(f"Could not load previous results: {e}")
        return []


async def main() -> None:
    """Main entry point for the async Ethereum crawler."""
    if not API_KEY or not START_ADDRESS:
        logger.error("Missing ETHERSCAN_API_KEY or START_ADDRESS environment variable.")
        return

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/4.0"
    }

    visited: Set[str] = set()
    logger.info(f"Starting Ethereum crawl from: {START_ADDRESS} (depth={DEPTH})")

    existing_data = await load_existing_results(OUTPUT_FILE)
    all_data = existing_data.copy()

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            new_data = await process_address(session, START_ADDRESS, visited, DEPTH)
            all_data.extend(new_data)
            await save_to_file(all_data, OUTPUT_FILE)

        logger.info(
            f"Crawl finished. Addresses processed: {len(visited)} | "
            f"Unique TXs: {len(seen_transactions)} | Output: {OUTPUT_FILE}"
        )

    except asyncio.CancelledError:
        logger.warning("Crawl cancelled by user.")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    finally:
        logger.info("Exiting crawler.")


if __name__ == "__main__":
    asyncio.run(main())
