import asyncio
import aiohttp
import aiofiles
import json
import logging
from aiohttp import ClientSession
from typing import List, Set, Dict, Optional, Union
from web3 import Web3

# === Configuration ===
API_KEY = "Your_Etherscan_API_Key_Here"
BASE_URL = "https://api.etherscan.io/api"
START_ADDRESS = "Your_Ethereum_Wallet_Address_Here"
DEPTH = 2
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 10
OUTPUT_FILE = "transactions.json"

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("etherscan-crawler")

semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


def wei_to_eth(wei: Union[str, int]) -> float:
    """Convert Wei to Ether."""
    try:
        return int(wei) / 1e18
    except (ValueError, TypeError):
        logger.error(f"Invalid wei value: {wei}")
        return 0.0


def etherscan_url(module: str, action: str, **params) -> str:
    """Build a properly formatted Etherscan API URL."""
    params_str = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{BASE_URL}?module={module}&action={action}&{params_str}&apikey={API_KEY}"


async def fetch_with_retries(session: ClientSession, url: str) -> Optional[Dict]:
    """Fetch JSON data from a URL with retry and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Etherscan returns status "0" for empty results too
                        if data.get("status") in ("0", "1"):
                            return data
                        logger.warning(f"Etherscan API returned unexpected data: {data}")
                    else:
                        logger.warning(f"HTTP {resp.status} on attempt {attempt} for {url}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
        await asyncio.sleep(2 ** attempt)
    logger.error(f"All retries failed for {url}")
    return None


async def fetch_transactions(session: ClientSession, address: str) -> List[Dict]:
    """Fetch transactions for a specific Ethereum address."""
    checksum_address = Web3.toChecksumAddress(address)
    url = etherscan_url(
        "account", "txlist",
        address=checksum_address,
        startblock=0,
        endblock=99999999,
        sort="asc"
    )
    logger.debug(f"Fetching transactions for {checksum_address}")
    data = await fetch_with_retries(session, url)
    return data.get("result", []) if data else []


async def process_address(
    session: ClientSession,
    address: str,
    visited: Set[str],
    depth: int
) -> List[Dict]:
    """Process an address and recursively process related addresses."""
    checksum_address = Web3.toChecksumAddress(address)
    if depth <= 0 or checksum_address in visited:
        return []

    visited.add(checksum_address)
    logger.info(f"Processing address: {checksum_address} | Depth: {depth}")

    transactions = await fetch_transactions(session, checksum_address)
    return await process_transactions(session, transactions, visited, depth - 1)


async def process_transactions(
    session: ClientSession,
    transactions: List[Dict],
    visited: Set[str],
    depth: int
) -> List[Dict]:
    """Process a list of transactions and find new addresses to explore."""
    results: List[Dict] = []
    next_addresses: Set[str] = set()
    seen_hashes: Set[str] = set()

    for tx in transactions:
        tx_hash = tx.get("hash")
        if not tx_hash or tx_hash in seen_hashes:
            continue
        seen_hashes.add(tx_hash)

        from_addr = tx.get("from")
        to_addr = tx.get("to")
        if not from_addr or not to_addr:
            continue

        results.append({
            "from": from_addr,
            "to": to_addr,
            "value": wei_to_eth(tx.get("value", "0")),
            "hash": tx_hash,
            "timestamp": tx.get("timeStamp")
        })

        if from_addr not in visited:
            next_addresses.add(from_addr)
        if to_addr not in visited:
            next_addresses.add(to_addr)

    # Concurrency limited by semaphore
    tasks = [process_address(session, addr, visited, depth) for addr in next_addresses]
    for coro in asyncio.as_completed(tasks):
        try:
            results.extend(await coro)
        except Exception as e:
            logger.error(f"Error in recursive processing: {e}")

    return results


async def save_to_file(data: List[Dict], filename: str) -> None:
    """Save data to a JSON file."""
    try:
        async with aiofiles.open(filename, "w") as f:
            await f.write(json.dumps(data, indent=4))
        logger.info(f"Saved {len(data)} transactions to '{filename}'")
    except Exception as e:
        logger.error(f"Failed to save file '{filename}': {e}")


async def main() -> None:
    """Main entry point for the crawler."""
    if not API_KEY or not START_ADDRESS:
        logger.error("Missing API key or start address.")
        return

    logger.info(f"Starting crawl from: {START_ADDRESS}")
    visited: Set[str] = set()

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/1.1"
    }

    async with ClientSession(headers=headers) as session:
        initial_txs = await fetch_transactions(session, START_ADDRESS)
        if not initial_txs:
            logger.error("No transactions found. Aborting.")
            return

        all_data = await process_transactions(session, initial_txs, visited, DEPTH)
        await save_to_file(all_data, OUTPUT_FILE)

        logger.info(f"Crawl complete. Unique addresses processed: {len(visited)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
