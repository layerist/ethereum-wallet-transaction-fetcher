import asyncio
import aiohttp
import aiofiles
import json
import logging
from aiohttp import ClientSession
from typing import List, Set, Dict, Optional

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


def wei_to_eth(wei: str) -> float:
    """Converts a string Wei value to Ether."""
    try:
        return int(wei) / 1e18
    except (ValueError, TypeError):
        logger.error(f"Invalid wei value: {wei}")
        return 0.0


async def fetch_with_retries(session: ClientSession, url: str) -> Optional[Dict]:
    """
    Executes a GET request with retry logic and exponential backoff.
    Returns the JSON-decoded data on success, or None on failure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("status") == "1":
                            return data
                        logger.warning(f"Etherscan API returned error: {data.get('message')}")
                    else:
                        logger.warning(f"HTTP {response.status} on attempt {attempt} for {url}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request failed (attempt {attempt}): {e}")
        await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to fetch data after {MAX_RETRIES} attempts: {url}")
    return None


async def fetch_transactions(session: ClientSession, address: str) -> List[Dict]:
    """
    Fetches all transactions for a given Ethereum address.
    """
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
    processed: Set[str],
    depth: int
) -> List[Dict]:
    """
    Recursively processes an address, fetching its transactions
    and discovering linked addresses up to a specified depth.
    """
    if depth <= 0 or address in processed:
        return []

    logger.info(f"Processing address: {address} | Depth: {depth}")
    processed.add(address)

    transactions = await fetch_transactions(session, address)
    return await process_transactions(session, transactions, processed, depth - 1)


async def process_transactions(
    session: ClientSession,
    transactions: List[Dict],
    processed: Set[str],
    depth: int
) -> List[Dict]:
    """
    Processes a list of transactions and recursively explores
    related addresses.
    """
    edges: List[Dict] = []
    next_addresses: Set[str] = set()

    for tx in transactions:
        from_addr = tx.get("from")
        to_addr = tx.get("to")
        value_eth = wei_to_eth(tx.get("value", "0"))

        if from_addr and to_addr:
            edges.append({
                "from": from_addr,
                "to": to_addr,
                "value": value_eth,
                "hash": tx.get("hash"),
                "timestamp": tx.get("timeStamp"),
            })
            if from_addr not in processed:
                next_addresses.add(from_addr)
            if to_addr not in processed:
                next_addresses.add(to_addr)

    tasks = [
        process_address(session, addr, processed, depth)
        for addr in next_addresses
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, list):
            edges.extend(result)
        else:
            logger.error(f"Error during recursive processing: {result}")

    return edges


async def save_to_file(data: List[Dict], filename: str) -> None:
    """
    Saves the given data to a JSON file.
    """
    try:
        async with aiofiles.open(filename, "w") as f:
            await f.write(json.dumps(data, indent=4))
        logger.info(f"Saved {len(data)} records to {filename}")
    except Exception as e:
        logger.error(f"Error saving to file: {e}")


async def main() -> None:
    """
    Entry point for the async crawler.
    """
    logger.info(f"Starting Ethereum transaction crawl from: {START_ADDRESS}")
    processed: Set[str] = set()

    headers = {
        "Accept": "application/json",
        "User-Agent": "etherscan-crawler/1.0"
    }

    async with ClientSession(headers=headers) as session:
        initial_transactions = await fetch_transactions(session, START_ADDRESS)
        if not initial_transactions:
            logger.error("No transactions found for the starting address.")
            return

        all_links = await process_transactions(session, initial_transactions, processed, DEPTH)
        await save_to_file(all_links, OUTPUT_FILE)

        logger.info(f"Completed. Processed {len(processed)} unique addresses.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
