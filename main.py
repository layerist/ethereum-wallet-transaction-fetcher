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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Limit concurrent requests
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


def wei_to_eth(value: str) -> float:
    """Convert Wei to ETH."""
    try:
        return int(value) / 10**18
    except (ValueError, TypeError):
        logger.error(f"Invalid Wei value: {value}")
        return 0.0


async def fetch_with_retries(session: ClientSession, url: str) -> Optional[Dict]:
    """Perform a GET request with retries and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore, session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "1":
                        return data
                    logger.warning(f"API responded with status {data.get('status')}: {data.get('message')}")
                else:
                    logger.warning(f"Attempt {attempt}: HTTP {response.status} for URL {url}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Attempt {attempt}: Exception for URL {url} - {e}")
        await asyncio.sleep(2 ** attempt)
    
    logger.error(f"Failed to fetch URL after {MAX_RETRIES} attempts: {url}")
    return None


async def fetch_transactions(session: ClientSession, address: str) -> List[Dict]:
    """Fetch the transaction list for a given address."""
    url = (
        f"{BASE_URL}?module=account&action=txlist"
        f"&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}"
    )
    logger.info(f"Fetching transactions for address: {address}")
    data = await fetch_with_retries(session, url)
    return data.get("result", []) if data else []


async def process_address(
    session: ClientSession,
    address: str,
    processed: Set[str],
    depth: int
) -> List[Tuple[str, str, float]]:
    """Recursively process an address and its transactions."""
    if address in processed or depth <= 0:
        return []

    logger.info(f"Processing address: {address} at depth: {depth}")
    processed.add(address)

    transactions = await fetch_transactions(session, address)
    return await process_transactions(session, transactions, processed, depth - 1)


async def process_transactions(
    session: ClientSession,
    transactions: List[Dict],
    processed: Set[str],
    depth: int
) -> List[Tuple[str, str, float]]:
    """Extract transaction data and recursively process involved addresses."""
    links: List[Tuple[str, str, float]] = []
    tasks: List[asyncio.Task] = []
    new_addresses: Set[str] = set()

    for tx in transactions:
        from_addr = tx.get("from")
        to_addr = tx.get("to")
        value = wei_to_eth(tx.get("value", "0"))

        if from_addr and to_addr:
            links.append((from_addr, to_addr, value))
            for addr in [from_addr, to_addr]:
                if addr not in processed and addr not in new_addresses:
                    new_addresses.add(addr)

    for addr in new_addresses:
        tasks.append(process_address(session, addr, processed, depth))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, list):
            links.extend(result)
        elif isinstance(result, Exception):
            logger.error(f"Error while processing address: {result}")

    return links


async def save_results_to_file(data: List[Tuple[str, str, float]], file_path: str) -> None:
    """Write data to a JSON file asynchronously."""
    try:
        async with aiofiles.open(file_path, "w") as f:
            await f.write(json.dumps(data, indent=4))
        logger.info(f"Saved results to: {file_path}")
    except IOError as e:
        logger.error(f"Failed to save file {file_path}: {e}")


async def main():
    """Entry point for transaction tracing."""
    async with ClientSession() as session:
        logger.info(f"Starting analysis from: {START_ADDRESS}")
        processed_addresses: Set[str] = set()

        initial_transactions = await fetch_transactions(session, START_ADDRESS)
        if not initial_transactions:
            logger.error("No transactions found.")
            return

        results = await process_transactions(session, initial_transactions, processed_addresses, DEPTH)
        await save_results_to_file(results, OUTPUT_FILE)

        for from_addr, to_addr, value in results:
            logger.info(f"{from_addr} -> {to_addr}: {value:.4f} ETH")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
