import asyncio
import aiohttp
import aiofiles
import json
import logging
from aiohttp import ClientSession
from typing import List, Set, Dict, Optional, Union

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
logger = logging.getLogger(__name__)
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


def wei_to_eth(wei: Union[str, int]) -> float:
    """Convert Wei to Ether."""
    try:
        return int(wei) / 1e18
    except Exception:
        logger.error(f"Invalid wei value: {wei}")
        return 0.0


async def fetch_with_retries(session: ClientSession, url: str) -> Optional[Dict]:
    """Fetch JSON data from a URL with retry and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("status") == "1":
                            return data
                        logger.warning(f"Etherscan API error: {data.get('message')}")
                    else:
                        logger.warning(f"HTTP {response.status} on attempt {attempt}: {url}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
        await asyncio.sleep(2 ** attempt)
    logger.error(f"All {MAX_RETRIES} retries failed: {url}")
    return None


async def fetch_transactions(session: ClientSession, address: str) -> List[Dict]:
    """Fetch transactions for a specific Ethereum address."""
    url = (
        f"{BASE_URL}?module=account&action=txlist"
        f"&address={address}&startblock=0&endblock=99999999"
        f"&sort=asc&apikey={API_KEY}"
    )
    logger.debug(f"Fetching transactions for {address}")
    data = await fetch_with_retries(session, url)
    return data.get("result", []) if data else []


async def process_address(
    session: ClientSession,
    address: str,
    visited: Set[str],
    depth: int
) -> List[Dict]:
    """Process an address and recursively process related addresses."""
    if depth <= 0 or address in visited:
        return []

    logger.info(f"Processing address: {address} | Remaining depth: {depth}")
    visited.add(address)

    transactions = await fetch_transactions(session, address)
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

    for tx in transactions:
        from_addr = tx.get("from")
        to_addr = tx.get("to")

        if not from_addr or not to_addr:
            continue

        tx_data = {
            "from": from_addr,
            "to": to_addr,
            "value": wei_to_eth(tx.get("value", "0")),
            "hash": tx.get("hash"),
            "timestamp": tx.get("timeStamp")
        }

        results.append(tx_data)

        if from_addr not in visited:
            next_addresses.add(from_addr)
        if to_addr not in visited:
            next_addresses.add(to_addr)

    tasks = [process_address(session, addr, visited, depth) for addr in next_addresses]
    nested_results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in nested_results:
        if isinstance(res, list):
            results.extend(res)
        else:
            logger.error(f"Recursive error: {res}")

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
        logger.error("Missing API key or start address. Please configure the script.")
        return

    logger.info(f"Starting crawl from address: {START_ADDRESS}")
    visited: Set[str] = set()

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/1.0"
    }

    async with ClientSession(headers=headers) as session:
        transactions = await fetch_transactions(session, START_ADDRESS)

        if not transactions:
            logger.error("No transactions found. Aborting.")
            return

        all_data = await process_transactions(session, transactions, visited, DEPTH)
        await save_to_file(all_data, OUTPUT_FILE)

        logger.info(f"Completed crawl. Unique addresses processed: {len(visited)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
