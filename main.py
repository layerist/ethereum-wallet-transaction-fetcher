import asyncio
import aiohttp
import json
import logging
import time
from aiohttp import ClientSession, ClientConnectorError, ClientResponseError
from typing import List, Set, Tuple, Optional

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
    """
    Fetch transactions for a given Ethereum address using the Etherscan API.
    
    Args:
        session (ClientSession): The aiohttp session.
        address (str): The Ethereum address to fetch transactions for.
    
    Returns:
        List[dict]: A list of transaction dictionaries, or an empty list if an error occurs.
    """
    logger.info(f'Fetching transactions for address: {address}')
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}'

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore, session.get(url) as response:
                response.raise_for_status()  # Raise exception for HTTP errors
                data = await response.json()
                if data['status'] == '1':
                    return data['result']
                else:
                    logger.error(f'Error fetching transactions for address {address}, status: {data["status"]}, message: {data["message"]}')
                    return []
        except (ClientConnectorError, aiohttp.ClientError, ClientResponseError) as e:
            logger.warning(f'Retry {attempt}/{MAX_RETRIES} for address {address} due to network error: {e}')
            await asyncio.sleep(2 ** attempt)  # Exponential backoff for retries
        except Exception as e:
            logger.error(f'Unexpected error for address {address}: {e}')
            return []

    logger.error(f'Failed to fetch transactions for address {address} after {MAX_RETRIES} retries.')
    return []

async def process_address(session: ClientSession, address: str, processed_addresses: Set[str], depth: int) -> List[Tuple[str, str, float]]:
    """
    Process an address by fetching its transactions and recursively processing the transactions to the specified depth.
    
    Args:
        session (ClientSession): The aiohttp session.
        address (str): The Ethereum address to process.
        processed_addresses (Set[str]): A set of already processed addresses to avoid redundancy.
        depth (int): The current depth in the recursive processing.

    Returns:
        List[Tuple[str, str, float]]: A list of tuples representing the transactions (from_address, to_address, value).
    """
    if depth == 0 or address in processed_addresses:
        return []

    logger.info(f'Processing address: {address} at depth {depth}')
    processed_addresses.add(address)

    transactions = await fetch_transactions(session, address)
    if not transactions:
        return []

    return await process_transactions(session, transactions, processed_addresses, depth - 1)

async def process_transactions(session: ClientSession, transactions: List[dict], processed_addresses: Set[str], depth: int) -> List[Tuple[str, str, float]]:
    """
    Process a list of transactions, recursively processing the addresses involved up to the specified depth.
    
    Args:
        session (ClientSession): The aiohttp session.
        transactions (List[dict]): A list of transaction dictionaries.
        processed_addresses (Set[str]): A set of already processed addresses.
        depth (int): The remaining depth for recursive processing.

    Returns:
        List[Tuple[str, str, float]]: A list of tuples representing the transaction links (from_address, to_address, value).
    """
    links = []

    tasks = []
    for tx in transactions:
        from_address = tx['from']
        to_address = tx['to']
        value = int(tx['value']) / 10 ** 18  # Convert from Wei to ETH
        links.append((from_address, to_address, value))

        tasks.append(process_address(session, from_address, processed_addresses, depth))
        tasks.append(process_address(session, to_address, processed_addresses, depth))

    # Gather all the recursive tasks
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, list):
            links.extend(result)
        elif isinstance(result, Exception):
            logger.error(f'Error during transaction processing: {result}')

    return links

async def main():
    """Main entry point to start processing Ethereum transactions."""
    async with ClientSession() as session:
        logger.info(f'Starting to fetch transactions from {START_ADDRESS}')
        transactions = await fetch_transactions(session, START_ADDRESS)
        processed_addresses = {START_ADDRESS}
        links = await process_transactions(session, transactions, processed_addresses, DEPTH)

        # Write the processed transactions to a JSON file
        with open('transactions.json', 'w') as f:
            json.dump(links, f, indent=4)

        # Log the transactions
        for from_address, to_address, value in links:
            logger.info(f'{from_address} -> {to_address}: {value:.4f} ETH')

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Script stopped by user.')
        sys.exit(0)
