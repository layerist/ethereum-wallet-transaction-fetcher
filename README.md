# Ethereum Wallet Transaction Fetcher

This Python script fetches all transaction relationships of an Ethereum wallet from the Etherscan API and recursively fetches transactions for the connected wallets. It outputs the relationships and transaction amounts in ETH to the console and saves the data in a `transactions.json` file.

## Features

- Fetches transactions for a given Ethereum wallet.
- Recursively fetches transactions for wallets interacted with, up to a specified depth.
- Outputs transaction relationships and amounts in ETH to the console.
- Saves the transaction data in a JSON file.

## Requirements

- Python 3.x
- `requests` library

## Installation

1. Clone this repository:
    ```bash
    git clone https://github.com/layerist/ethereum-wallet-transaction-fetcher.git
    cd ethereum-wallet-transaction-fetcher
    ```

2. Install the required Python libraries:
    ```bash
    pip install requests
    ```

## Usage

1. Obtain an API key from [Etherscan](https://etherscan.io/apis).

2. Edit the `main.py` script:
    - Replace `'Your Etherscan API Key Here'` with your actual Etherscan API key.
    - Replace `'Your Ethereum Wallet Address Here'` with the Ethereum address you want to start with.
    - Adjust the `DEPTH` variable if you want to go deeper or shallower in the transaction tree.

3. Run the script:
    ```bash
    python main.py
    ```

## Example

If the script is configured with an API key and a starting Ethereum address, running the script will output transaction relationships and save them to a `transactions.json` file. The console output will look like this:

```
Starting to fetch transactions from 0xYourStartingAddress
Fetching transactions for address: 0xYourStartingAddress
Processing from_address: 0xAnotherAddress
Fetching transactions for address: 0xAnotherAddress
Processing to_address: 0xYetAnotherAddress
Fetching transactions for address: 0xYetAnotherAddress
0xYourStartingAddress -> 0xAnotherAddress: 0.123 ETH
0xAnotherAddress -> 0xYetAnotherAddress: 0.456 ETH
```

## Configuration

- `API_KEY`: Your Etherscan API key.
- `START_ADDRESS`: The Ethereum address to start fetching transactions from.
- `DEPTH`: How deep to go in the transaction tree (default is 2).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any changes.

## Contact

For any questions or issues, please open an issue on this repository.

```

### Additional Notes:

- Make sure to keep your API key secure and do not expose it publicly.
- Adjust the sleep interval (`time.sleep(0.2)`) as needed to avoid hitting the API rate limit.
- Ensure you have the necessary permissions to use the Etherscan API and comply with their usage policies.
```
