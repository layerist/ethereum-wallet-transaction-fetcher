import asyncio
import aiohttp
import json
import logging
from aiohttp import ClientSession, ClientConnectorError, ClientResponseError
from typing import List, Set, Tuple, Optional

# Configuration
API_KEY = 'Your Etherscan API Key Here'
BASE_URL = 'https://api.etherscan.io/api'
START_ADDRESS = 'Your Ethereum Wallet Address Here'
DEPTH = 2  # Depth of transaction tree exploration
MAX_RETRIES = 3
CONCURRENT_REQUESTS = 10

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Semaphore to manage concurrent requests
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def fetch_transactions(session: ClientSession, address: str) -> Optional[List[dict]]:
    """
    Fetch Ethereum transactions for the given address using the Etherscan API.
    Retries on failure up to a maximum defined in MAX_RETRIES.

    Args:
        session (ClientSession): The aiohttp session.
        address (str): The Ethereum address to fetch transactions for.

    Returns:
        Optional[List[dict]]: List of transaction dictionaries, or None on failure.
    """
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}'
    logger.info(f'Fetching transactions for address: {address}')

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore, session.get(url) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get('status') == '1':
                    return data.get('result', [])
                else:
                    logger.error(f'Failed to fetch transactions for {address}, API response: {data}')
                    return None
        except (ClientConnectorError, ClientResponseError, aiohttp.ClientError) as e:
            logger.warning(f'Attempt {attempt}/{MAX_RETRIES}: Network error for {address}: {e}')
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            logger.error(f'Unexpected error for {address}: {e}')
            break

    logger.error(f'Failed to fetch transactions for {address} after {MAX_RETRIES} retries.')
    return None


async def process_address(session: ClientSession, address: str, processed_addresses: Set[str], depth: int) -> List[Tuple[str, str, float]]:
    """
    Recursively process transactions of a given Ethereum address up to the specified depth.

    Args:
        session (ClientSession): The aiohttp session.
        address (str): The Ethereum address to process.
        processed_addresses (Set[str]): Set of processed addresses to avoid redundancy.
        depth (int): Current depth in the transaction exploration.

    Returns:
        List[Tuple[str, str, float]]: A list of tuples representing the transactions (from_address, to_address, value).
    """
    if depth == 0 or address in processed_addresses:
        return []

    logger.info(f'Processing address {address} at depth {depth}')
    processed_addresses.add(address)

    transactions = await fetch_transactions(session, address)
    if transactions is None:
        return []

    return await process_transactions(session, transactions, processed_addresses, depth - 1)


async def process_transactions(session: ClientSession, transactions: List[dict], processed_addresses: Set[str], depth: int) -> List[Tuple[str, str, float]]:
    """
    Process a list of Ethereum transactions, recursively exploring related addresses.

    Args:
        session (ClientSession): The aiohttp session.
        transactions (List[dict]): List of transaction dictionaries.
        processed_addresses (Set[str]): Set of processed addresses.
        depth (int): Remaining depth for exploration.

    Returns:
        List[Tuple[str, str, float]]: List of transaction links (from_address, to_address, value).
    """
    links = []
    tasks = []

    for tx in transactions:
        from_address = tx['from']
        to_address = tx['to']
        value = int(tx['value']) / 10 ** 18  # Convert Wei to ETH
        links.append((from_address, to_address, value))

        # Recursively explore addresses
        if depth > 0:
            if from_address not in processed_addresses:
                tasks.append(process_address(session, from_address, processed_addresses, depth))
            if to_address not in processed_addresses:
                tasks.append(process_address(session, to_address, processed_addresses, depth))

    # Gather results from recursive tasks
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, list):
            links.extend(result)
        elif isinstance(result, Exception):
            logger.error(f'Error during transaction processing: {result}')

    return links


async def main():
    """Main entry point to process Ethereum transactions from a start address."""
    async with ClientSession() as session:
        logger.info(f'Starting transaction processing from {START_ADDRESS}')
        processed_addresses = {START_ADDRESS}

        # Fetch initial transactions and process them recursively
        transactions = await fetch_transactions(session, START_ADDRESS)
        if transactions:
            links = await process_transactions(session, transactions, processed_addresses, DEPTH)

            # Write the result to a JSON file
            with open('transactions.json', 'w') as f:
                json.dump(links, f, indent=4)

            # Log each transaction link
            for from_address, to_address, value in links:
                logger.info(f'{from_address} -> {to_address}: {value:.4f} ETH')
        else:
            logger.error(f'No transactions found for {START_ADDRESS}')


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info('Script interrupted by user.')
