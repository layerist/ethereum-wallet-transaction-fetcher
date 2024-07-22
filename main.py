import asyncio
import aiohttp
import json
import time

API_KEY = 'Your Etherscan API Key Here'
BASE_URL = 'https://api.etherscan.io/api'
START_ADDRESS = 'Your Ethereum Wallet Address Here'
DEPTH = 2  # How deep to go in the transaction tree

async def fetch_transactions(session, address):
    print(f'Fetching transactions for address: {address}')
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}'
    async with session.get(url) as response:
        data = await response.json()
        if data['status'] == '1':
            return data['result']
        else:
            print(f'Error fetching transactions for address: {address}, status: {data["status"]}, message: {data["message"]}')
            return []

async def process_transactions(session, transactions, processed_addresses, depth):
    if depth == 0:
        return []

    links = []

    for tx in transactions:
        from_address = tx['from']
        to_address = tx['to']
        value = int(tx['value']) / 10**18  # Convert from Wei to ETH

        if from_address not in processed_addresses:
            print(f'Processing from_address: {from_address}')
            processed_addresses.add(from_address)
            new_transactions = await fetch_transactions(session, from_address)
            links.append((from_address, to_address, value))
            links.extend(await process_transactions(session, new_transactions, processed_addresses, depth - 1))

        if to_address not in processed_addresses:
            print(f'Processing to_address: {to_address}')
            processed_addresses.add(to_address)
            new_transactions = await fetch_transactions(session, to_address)
            links.append((from_address, to_address, value))
            links.extend(await process_transactions(session, new_transactions, processed_addresses, depth - 1))

        await asyncio.sleep(0.2)  # To prevent hitting the API rate limit

    return links

async def main():
    async with aiohttp.ClientSession() as session:
        print(f'Starting to fetch transactions from {START_ADDRESS}')
        transactions = await fetch_transactions(session, START_ADDRESS)
        processed_addresses = set([START_ADDRESS])
        links = await process_transactions(session, transactions, processed_addresses, DEPTH)

        with open('transactions.json', 'w') as f:
            json.dump(links, f, indent=4)

        for link in links:
            print(f'{link[0]} -> {link[1]}: {link[2]} ETH')

if __name__ == '__main__':
    asyncio.run(main())
