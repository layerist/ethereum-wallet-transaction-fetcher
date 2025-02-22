import asyncio
import aiohttp
import json
import logging
from aiohttp import ClientSession
from typing import List, Set, Tuple, Optional, Dict

# Configuration
API_KEY = 'Your_Etherscan_API_Key_Here'
BASE_URL = 'https://api.etherscan.io/api'
START_ADDRESS = 'Your_Ethereum_Wallet_Address_Here'
DEPTH = 2
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 10
OUTPUT_FILE = 'transactions.json'

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Semaphore to limit concurrent requests
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


def wei_to_eth(value: str) -> float:
    """Convert Wei to ETH, handling errors gracefully."""
    try:
        return int(value) / 10**18
    except (ValueError, TypeError):
        logger.error(f"Invalid Wei value: {value}")
        return 0.0


async def fetch_with_retries(session: ClientSession, url: str, max_retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Fetch data with retries and exponential backoff."""
    for attempt in range(max_retries):
        try:
            async with semaphore, session.get(url, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "1":
                        return data
                    logger.warning(f"API responded with status {data.get('status')}: {data.get('message')}")
                else:
                    logger.warning(f"Attempt {attempt + 1}: HTTP {response.status} - {url}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Attempt {attempt + 1}: Error fetching {url} - {e}")

        await asyncio.sleep(2 ** attempt)  # Exponential backoff

    logger.error(f"Failed to fetch data from {url} after {max_retries} attempts.")
    return None


async def fetch_transactions(session: ClientSession, address: str) -> List[Dict]:
    """Fetch Ethereum transactions for a given address."""
    url = f"{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}"
    logger.info(f"Fetching transactions for {address}")

    data = await fetch_with_retries(session, url)
    return data.get("result", []) if data else []


async def process_transactions(
    session: ClientSession, transactions: List[Dict], processed_addresses: Set[str], depth: int
) -> List[Tuple[str, str, float]]:
    """Process transactions and explore new addresses up to the given depth."""
    if depth == 0:
        return []

    links = []
    new_tasks = []

    for tx in transactions:
        from_addr, to_addr, value = tx.get("from"), tx.get("to"), wei_to_eth(tx.get("value", "0"))
        if from_addr and to_addr:
            links.append((from_addr, to_addr, value))

            # Avoid reprocessing known addresses
            if depth > 0:
                if from_addr not in processed_addresses:
                    new_tasks.append(process_address(session, from_addr, processed_addresses, depth - 1))
                if to_addr not in processed_addresses:
                    new_tasks.append(process_address(session, to_addr, processed_addresses, depth - 1))

    # Process new addresses concurrently
    results = await asyncio.gather(*new_tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, list):
            links.extend(result)
        elif isinstance(result, Exception):
            logger.error(f"Error in recursive processing: {result}")

    return links


async def process_address(session: ClientSession, address: str, processed_addresses: Set[str], depth: int) -> List[Tuple[str, str, float]]:
    """Recursively fetch and process transactions up to the given depth."""
    if address in processed_addresses:
        return []
    
    processed_addresses.add(address)
    logger.info(f"Processing {address} at depth {depth}")

    transactions = await fetch_transactions(session, address)
    return await process_transactions(session, transactions, processed_addresses, depth)


async def save_results_to_file(data: List[Tuple[str, str, float]], file_path: str) -> None:
    """Save the transaction data asynchronously to a JSON file."""
    try:
        async with aiofiles.open(file_path, "w") as f:
            await f.write(json.dumps(data, indent=4))
        logger.info(f"Results saved to {file_path}")
    except IOError as e:
        logger.error(f"Error saving to {file_path}: {e}")


async def main():
    """Main function to start Ethereum transaction processing."""
    async with ClientSession() as session:
        logger.info(f"Starting transaction tracking from {START_ADDRESS}")
        processed_addresses = set()

        transactions = await fetch_transactions(session, START_ADDRESS)
        if transactions:
            links = await process_transactions(session, transactions, processed_addresses, DEPTH)
            await save_results_to_file(links, OUTPUT_FILE)

            for from_addr, to_addr, val in links:
                logger.info(f"{from_addr} -> {to_addr}: {val:.4f} ETH")
        else:
            logger.error(f"No transactions found for {START_ADDRESS}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user.")
