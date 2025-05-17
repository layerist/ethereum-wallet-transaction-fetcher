import asyncio
import aiohttp
import aiofiles
import json
import logging
from aiohttp import ClientSession
from typing import List, Set, Tuple, Dict, Optional

# === Configuration ===
API_KEY = 'Your_Etherscan_API_Key_Here'
BASE_URL = 'https://api.etherscan.io/api'
START_ADDRESS = 'Your_Ethereum_Wallet_Address_Here'
DEPTH = 2
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 10
OUTPUT_FILE = 'transactions.json'

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Semaphore to limit concurrent API requests
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


def wei_to_eth(value: str) -> float:
    """Convert Wei to Ether."""
    try:
        return int(value) / 10**18
    except (ValueError, TypeError):
        logger.error(f"Invalid Wei value: {value}")
        return 0.0


async def fetch_with_retries(session: ClientSession, url: str) -> Optional[Dict]:
    """Fetch JSON data with retry and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore, session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "1":
                        return data
                    logger.warning(f"API error: {data.get('message')} (status {data.get('status')})")
                else:
                    logger.warning(f"HTTP {response.status} on attempt {attempt}: {url}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request failed (attempt {attempt}) for {url}: {e}")
        await asyncio.sleep(2 ** attempt)

    logger.error(f"Failed to fetch after {MAX_RETRIES} attempts: {url}")
    return None


async def fetch_transactions(session: ClientSession, address: str) -> List[Dict]:
    """Fetch transactions for the specified Ethereum address."""
    url = (
        f"{BASE_URL}?module=account&action=txlist"
        f"&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}"
    )
    logger.debug(f"Fetching transactions for address: {address}")
    data = await fetch_with_retries(session, url)
    return data.get("result", []) if data else []


async def process_address(
    session: ClientSession,
    address: str,
    processed: Set[str],
    depth: int
) -> List[Tuple[str, str, float]]:
    """Recursively process an address and return transaction links."""
    if depth <= 0 or address in processed:
        return []

    logger.info(f"Processing address: {address} | Depth remaining: {depth}")
    processed.add(address)
    transactions = await fetch_transactions(session, address)
    return await process_transactions(session, transactions, processed, depth - 1)


async def process_transactions(
    session: ClientSession,
    transactions: List[Dict],
    processed: Set[str],
    depth: int
) -> List[Tuple[str, str, float]]:
    """Extract relationships from transactions and process new addresses."""
    links: List[Tuple[str, str, float]] = []
    new_addresses: Set[str] = set()

    for tx in transactions:
        from_addr = tx.get("from")
        to_addr = tx.get("to")
        value = wei_to_eth(tx.get("value", "0"))

        if from_addr and to_addr:
            links.append((from_addr, to_addr, value))
            if from_addr not in processed:
                new_addresses.add(from_addr)
            if to_addr not in processed:
                new_addresses.add(to_addr)

    tasks = [process_address(session, addr, processed, depth) for addr in new_addresses]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, list):
            links.extend(result)
        else:
            logger.error(f"Exception during recursive address processing: {result}")

    return links


async def save_results_to_file(data: List[Tuple[str, str, float]], file_path: str) -> None:
    """Save transaction link data to a JSON file."""
    try:
        async with aiofiles.open(file_path, "w") as f:
            await f.write(json.dumps(data, indent=4))
        logger.info(f"Saved {len(data)} transactions to {file_path}")
    except IOError as e:
        logger.error(f"Failed to save file {file_path}: {e}")


async def main():
    """Program entry point."""
    async with ClientSession() as session:
        logger.info(f"Starting analysis from address: {START_ADDRESS}")
        processed: Set[str] = set()

        initial_transactions = await fetch_transactions(session, START_ADDRESS)
        if not initial_transactions:
            logger.error("No transactions found for the starting address.")
            return

        all_links = await process_transactions(session, initial_transactions, processed, DEPTH)
        await save_results_to_file(all_links, OUTPUT_FILE)

        logger.info(f"Traversal complete. Processed {len(processed)} unique addresses.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
