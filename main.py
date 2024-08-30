import asyncio
import aiohttp
import json
import logging
import time
from aiohttp import ClientSession, ClientConnectorError, ClientResponseError
from typing import List, Set, Tuple

# Configuration
API_KEY = 'Your Etherscan API Key Here'
BASE_URL = 'https://api.etherscan.io/api'
START_ADDRESS = 'Your Ethereum Wallet Address Here'
DEPTH = 2  # How deep to go in the transaction tree
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 10

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Semaphore to limit the number of concurrent requests
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

async def fetch_transactions(session: ClientSession, address: str) -> List[dict]:
    """Fetch transactions for a given Ethereum address using the Etherscan API."""
    logger.info(f'Fetching transactions for address: {address}')
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}'

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore, session.get(url) as response:
                response.raise_for_status()  # Raise exception for HTTP error codes
                data = await response.json()
                if data['status'] == '1':
                    return data['result']
                else:
                    logger.error(f'Error fetching transactions for address: {address}, status: {data["status"]}, message: {data["message"]}')
                    return []
        except (ClientConnectorError, aiohttp.ClientError, ClientResponseError) as e:
            logger.warning(f'Retry {attempt}/{MAX_RETRIES} for address {address} due to network error: {e}')
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            logger.error(f'Unexpected error occurred for address {address}: {e}')
            return []

    logger.error(f'Failed to fetch transactions for address {address} after {MAX_RETRIES} retries.')
    return []

async def process_transactions(session: ClientSession, transactions: List[dict], processed_addresses: Set[str], depth: int) -> List[Tuple[str, str, float]]:
    """Process transactions recursively to fetch more transactions up to the specified depth."""
    if depth == 0:
        return []

    links = []

    async def process_address(address: str) -> List[Tuple[str, str, float]]:
        if address not in processed_addresses:
            logger.info(f'Processing address: {address}')
            processed_addresses.add(address)
            new_transactions = await fetch_transactions(session, address)
            return await process_transactions(session, new_transactions, processed_addresses, depth - 1)
        return []

    tasks = []
    for tx in transactions:
        from_address = tx['from']
        to_address = tx['to']
        value = int(tx['value']) / 10 ** 18  # Convert from Wei to ETH
        links.append((from_address, to_address, value))

        tasks.append(process_address(from_address))
        tasks.append(process_address(to_address))

    new_links = await asyncio.gather(*tasks, return_exceptions=True)  # Handle exceptions in gathering
    for result in new_links:
        if isinstance(result, list):
            links.extend(result)
        else:
            logger.error(f'Error occurred during processing transactions: {result}')

    return links

async def main():
    """Main entry point for the script to start processing Ethereum transactions."""
    async with ClientSession() as session:
        logger.info(f'Starting to fetch transactions from {START_ADDRESS}')
        transactions = await fetch_transactions(session, START_ADDRESS)
        processed_addresses = {START_ADDRESS}
        links = await process_transactions(session, transactions, processed_addresses, DEPTH)

        # Write to a JSON file
        with open('transactions.json', 'w') as f:
            json.dump(links, f, indent=4)

        # Log the transactions
        for link in links:
            logger.info(f'{link[0]} -> {link[1]}: {link[2]:.4f} ETH')

if __name__ == '__main__':
    asyncio.run(main())
