import requests
import json
import time

API_KEY = 'Your Etherscan API Key Here'
BASE_URL = 'https://api.etherscan.io/api'
START_ADDRESS = 'Your Ethereum Wallet Address Here'
DEPTH = 2  # How deep to go in the transaction tree

def get_transactions(address):
    print(f'Fetching transactions for address: {address}')
    url = f'{BASE_URL}?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={API_KEY}'
    response = requests.get(url)
    data = response.json()
    if data['status'] == '1':
        return data['result']
    else:
        return []

def process_transactions(transactions, processed_addresses, depth):
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
            new_transactions = get_transactions(from_address)
            links.append((from_address, to_address, value))
            links.extend(process_transactions(new_transactions, processed_addresses, depth - 1))

        if to_address not in processed_addresses:
            print(f'Processing to_address: {to_address}')
            processed_addresses.add(to_address)
            new_transactions = get_transactions(to_address)
            links.append((from_address, to_address, value))
            links.extend(process_transactions(new_transactions, processed_addresses, depth - 1))

        time.sleep(0.2)  # To prevent hitting the API rate limit

    return links

def main():
    print(f'Starting to fetch transactions from {START_ADDRESS}')
    transactions = get_transactions(START_ADDRESS)
    processed_addresses = set([START_ADDRESS])
    links = process_transactions(transactions, processed_addresses, DEPTH)

    with open('transactions.json', 'w') as f:
        json.dump(links, f, indent=4)

    for link in links:
        print(f'{link[0]} -> {link[1]}: {link[2]} ETH')

if __name__ == '__main__':
    main()
