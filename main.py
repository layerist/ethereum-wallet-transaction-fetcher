import asyncio
import aiohttp
import aiofiles
import json
import logging
import os
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

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("etherscan-crawler")

# Global state
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
transaction_cache: Dict[str, List[Dict]] = {}
seen_transactions: Set[str] = set()


def wei_to_eth(wei: Union[str, int]) -> float:
    """Convert Wei to Ether safely."""
    try:
        return int(wei) / 1e18
    except Exception:
        logger.debug(f"Invalid wei value: {wei}")
        return 0.0


def etherscan_url(module: str, action: str, **params) -> str:
    """Build a properly formatted Etherscan API URL."""
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_URL}?module={module}&action={action}&{query}&apikey={API_KEY}"


async def fetch_with_retries(session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
    """Fetch JSON data from a URL with retries and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    if resp.status != 200:
                        logger.warning(f"[{attempt}/{MAX_RETRIES}] HTTP {resp.status} for {url}")
                        continue

                    try:
                        data = await resp.json(content_type=None)
                    except Exception as e:
                        logger.warning(f"Invalid JSON response: {e}")
                        continue

                    if isinstance(data, dict) and data.get("status") in ("1", "0"):
                        return data
                    logger.warning(f"Unexpected Etherscan response: {data}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug(f"Attempt {attempt} failed: {e}")

        await asyncio.sleep(2 ** attempt)

    logger.error(f"All retries failed for URL: {url}")
    return None


async def fetch_transactions(session: aiohttp.ClientSession, address: str) -> List[Dict]:
    """Fetch transactions for a specific Ethereum address (cached)."""
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


async def process_transactions(
    session: aiohttp.ClientSession,
    transactions: List[Dict],
    visited: Set[str],
    depth: int
) -> List[Dict]:
    """Process transactions and recursively explore related addresses."""
    results: List[Dict] = []
    next_addresses: Set[str] = set()

    for tx in transactions:
        tx_hash = tx.get("hash")
        if not tx_hash or tx_hash in seen_transactions:
            continue
        seen_transactions.add(tx_hash)

        from_addr, to_addr = tx.get("from"), tx.get("to")
        if not from_addr or not to_addr:
            continue

        results.append({
            "from": from_addr,
            "to": to_addr,
            "value": wei_to_eth(tx.get("value", "0")),
            "hash": tx_hash,
            "timestamp": tx.get("timeStamp")
        })

        if depth > 0:
            if from_addr not in visited:
                next_addresses.add(from_addr)
            if to_addr not in visited:
                next_addresses.add(to_addr)

    if depth > 0 and next_addresses:
        async with asyncio.TaskGroup() as tg:
            tasks = [
                tg.create_task(process_address(session, addr, visited, depth))
                for addr in next_addresses
            ]

        for t in tasks:
            if not t.exception():
                results.extend(t.result())

    return results


async def process_address(
    session: aiohttp.ClientSession,
    address: str,
    visited: Set[str],
    depth: int
) -> List[Dict]:
    """Process an address and its transaction network recursively."""
    try:
        checksum_address = Web3.toChecksumAddress(address)
    except Exception:
        logger.debug(f"Invalid address skipped: {address}")
        return []

    if depth <= 0 or checksum_address in visited:
        return []

    visited.add(checksum_address)
    logger.info(f"Exploring address: {checksum_address} | Depth: {depth}")

    txs = await fetch_transactions(session, checksum_address)
    if not txs:
        return []

    return await process_transactions(session, txs, visited, depth - 1)


async def save_to_file(data: List[Dict], filename: str) -> None:
    """Save results to a JSON file asynchronously."""
    try:
        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            json_data = json.dumps(data, indent=2, ensure_ascii=False)
            await f.write(json_data)
        logger.info(f"Saved {len(data)} transactions → {filename}")
    except Exception as e:
        logger.error(f"Failed to write file '{filename}': {e}")


async def main() -> None:
    """Main entry point for the crawler."""
    if not API_KEY or not START_ADDRESS:
        logger.error("Missing ETHERSCAN_API_KEY or START_ADDRESS environment variable.")
        return

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/3.0"
    }

    visited: Set[str] = set()
    logger.info(f"Starting Ethereum crawl from: {START_ADDRESS} (depth={DEPTH})")

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            initial_txs = await fetch_transactions(session, START_ADDRESS)
            if not initial_txs:
                logger.warning("No initial transactions found.")
                return

            all_data = await process_transactions(session, initial_txs, visited, DEPTH)
            await save_to_file(all_data, OUTPUT_FILE)

            logger.info(f"Crawl finished. Addresses processed: {len(visited)} | Unique TXs: {len(seen_transactions)}")

    except asyncio.CancelledError:
        logger.warning("Crawl cancelled.")
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    finally:
        logger.info("Exiting crawler.")


if __name__ == "__main__":
    asyncio.run(main())
