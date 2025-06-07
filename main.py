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

semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


def wei_to_eth(wei: str) -> float:
    """Convert Wei to Ether."""
    try:
        return int(wei) / 1e18
    except (ValueError, TypeError):
        logger.error(f"Invalid wei value: {wei}")
        return 0.0


async def fetch_with_retries(session: ClientSession, url: str) -> Optional[Dict]:
    """Perform a GET request with retries and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("status") == "1":
                            return data
                        logger.warning(f"Etherscan API error: {data.get('message')}")
                    else:
                        logger.warning(f"HTTP {response.status} on attempt {attempt}: {url}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Request error (attempt {attempt}): {e}")
        await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to fetch after {MAX_RETRIES} attempts: {url}")
    return None


async def fetch_transactions(session: ClientSession, address: str) -> List[Dict]:
    """Fetch Ethereum transactions for a given address."""
    url = (
        f"{BASE_URL}?module=account&action=txlist"
        f"&address={address}&startblock=0&endblock=99999999"
        f"&sort=asc&apikey={API_KEY}"
    )
    logger.debug(f"Fetching transactions for: {address}")
    data = await fetch_with_retries(session, url)
    return data.get("result", []) if data else []


async def process_address(
    session: ClientSession,
    address: str,
    processed: Set[str],
    depth: int
) -> List[Dict[str, object]]:
    """Process an address and return its transaction edges."""
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
) -> List[Dict[str, object]]:
    """Process and link addresses found in transactions."""
    edges: List[Dict[str, object]] = []
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
                "timestamp": tx.get("timeStamp")
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
            logger.error(f"Error in recursive call: {result}")

    return edges


async def save_to_file(data: List[Dict[str, object]], filename: str) -> None:
    """Save results to a JSON file."""
    try:
        async with aiofiles.open(filename, "w") as f:
            await f.write(json.dumps(data, indent=4))
        logger.info(f"Saved {len(data)} records to '{filename}'")
    except Exception as e:
        logger.error(f"File save error: {e}")


async def main() -> None:
    """Main execution coroutine."""
    logger.info(f"Starting crawl from: {START_ADDRESS}")
    processed: Set[str] = set()

    async with ClientSession() as session:
        initial_tx = await fetch_transactions(session, START_ADDRESS)
        if not initial_tx:
            logger.error("No transactions found for the starting address.")
            return

        all_links = await process_transactions(session, initial_tx, processed, DEPTH)
        await save_to_file(all_links, OUTPUT_FILE)

        logger.info(f"Completed. Processed {len(processed)} addresses.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted.")
