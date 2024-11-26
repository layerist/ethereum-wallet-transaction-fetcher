import asyncio
import aiohttp
import json
import logging
from aiohttp import ClientSession
from typing import List, Set, Tuple, Optional, Dict

# Configuration
API_KEY = 'Your Etherscan API Key Here'
BASE_URL = 'https://api.etherscan.io/api'
START_ADDRESS = 'Your Ethereum Wallet Address Here'
DEPTH = 2
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 10
OUTPUT_FILE = 'transactions.json'

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Semaphore for managing concurrent requests
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

def wei_to_eth(value: str) -> float:
    """Convert Wei to ETH."""
    return int(value) / 10**18

async def fetch_transactions(session: ClientSession, address: str) -> Optional[List[Dict]]:
    """Fetch Ethereum transactions for a specific address."""
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}'
    logger.info(f'Fetching transactions for address: {address}')
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore, session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Unexpected response code {response.status} for {address}")
                    continue
                data = await response.json()
                if data.get('status') == '1':
                    return data.get('result', [])
                elif data.get('message') == 'No transactions found':
                    logger.info(f'No transactions found for address: {address}')
                    return []
                else:
                    logger.error(f'API error for {address}: {data}')
                    return None
        except (aiohttp.ClientError, aiohttp.ClientResponseError) as e:
            logger.warning(f'Attempt {attempt}/{MAX_RETRIES} failed for {address}: {e}')
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            logger.error(f'Unexpected error while fetching transactions for {address}: {e}')
            break
    
    logger.error(f'Failed to fetch transactions for {address} after {MAX_RETRIES} retries.')
    return None

async def process_address(
    session: ClientSession, address: str, processed_addresses: Set[str], depth: int
) -> List[Tuple[str, str, float]]:
    """Recursively process transactions of an address up to the specified depth."""
    if depth == 0 or address in processed_addresses:
        return []
    
    processed_addresses.add(address)
    logger.info(f'Processing address {address} at depth {depth}')
    
    transactions = await fetch_transactions(session, address)
    if transactions is None:
        return []
    
    return await process_transactions(session, transactions, processed_addresses, depth - 1)

async def process_transactions(
    session: ClientSession, transactions: List[Dict], processed_addresses: Set[str], depth: int
) -> List[Tuple[str, str, float]]:
    """Process a list of transactions, exploring related addresses recursively."""
    links = []
    tasks = []

    for tx in transactions:
        from_address = tx.get('from')
        to_address = tx.get('to')
        value = wei_to_eth(tx.get('value', '0'))

        links.append((from_address, to_address, value))

        # Schedule recursive exploration for new addresses
        if depth > 0:
            if from_address and from_address not in processed_addresses:
                tasks.append(process_address(session, from_address, processed_addresses, depth))
            if to_address and to_address not in processed_addresses:
                tasks.append(process_address(session, to_address, processed_addresses, depth))
    
    # Handle recursive results
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, list):
            links.extend(result)
        elif isinstance(result, Exception):
            logger.error(f'Error during recursive transaction processing: {result}')

    return links

async def save_results_to_file(data: List[Tuple[str, str, float]], file_path: str) -> None:
    """Save the transaction data to a JSON file."""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f'Results saved to {file_path}')
    except IOError as e:
        logger.error(f'Failed to save results to {file_path}: {e}')

async def main():
    """Main entry point for processing Ethereum transactions from a start address."""
    async with ClientSession() as session:
        logger.info(f'Starting transaction processing from {START_ADDRESS}')
        processed_addresses = set()

        transactions = await fetch_transactions(session, START_ADDRESS)
        if transactions is not None:
            links = await process_transactions(session, transactions, processed_addresses, DEPTH)
            await save_results_to_file(links, OUTPUT_FILE)

            for from_addr, to_addr, val in links:
                logger.info(f'{from_addr} -> {to_addr}: {val:.4f} ETH')
        else:
            logger.error(f'No transactions found for {START_ADDRESS}')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Script interrupted by user.')
