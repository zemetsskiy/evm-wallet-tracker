import asyncio
import json
from decimal import Decimal

import requests
from web3 import Web3
from web3.exceptions import BlockNotFound
from hexbytes import HexBytes

from log import logger as logging
from config import SERVER_URL


class TransactionProcessor:
    def __init__(self, web3: Web3, network: str):
        self.web3 = web3
        self.network = network

    async def get_block_with_retry(self, block_number, retries=5, delay=1):
        attempt = 0
        while attempt < retries:
            try:
                block = self.web3.eth.get_block(block_number, full_transactions=True)
                return block
            except BlockNotFound:
                attempt += 1
                logging.warning(f"{self.network} - Block {block_number} not found. Retry {attempt}/{retries} after delay.")
                await asyncio.sleep(delay * attempt)
        raise Exception("Max retries reached, block not found")

    async def get_token_decimals(self, token_address):
        token_contract = self.web3.eth.contract(address=token_address, abi=[
            {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "type": "function"
            }
        ])
        try:
            return token_contract.functions.decimals().call()
        except Exception as e:
            logging.error(f"{self.network} - Unable to get decimals for {token_address}: {e}")
            return 18

    def decimal_to_serializable(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError

    def send_data_to_server(self, data):
        serializable_data = json.loads(json.dumps(data, default=self.decimal_to_serializable))

        try:
            response = requests.post(SERVER_URL, json=serializable_data)
            response.raise_for_status()
            logging.info(f"Data successfully sent to server: {serializable_data}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error sending data to server: {e}")

    async def process_erc20_transaction(self, tx, wallets):
        input_data = HexBytes(tx['input'])

        if input_data[:4] != HexBytes('a9059cbb'):
            return

        to_address_hex = input_data[16:36].hex()
        to_address = Web3.to_checksum_address(f"0x{to_address_hex[-40:]}")

        if to_address in wallets:
            amount_hex = input_data[36:].hex()
            amount = int(amount_hex, 16)

            token_address = tx['to']
            decimals = await self.get_token_decimals(token_address)
            formatted_amount = amount / 10 ** decimals

            logging.info(f"{self.network} - ERC20 Transfer: {formatted_amount} tokens to {to_address} from contract {token_address}")

            data = {
                "to": to_address,
                "contractAddress": token_address,
                "amount": formatted_amount,
                "network": self.network
            }
            self.send_data_to_server(data)

    async def process_block(self, block_number, wallets):
        block = await self.get_block_with_retry(block_number)
        transactions = block['transactions']
        for tx in transactions:
            if tx['to'] in wallets:
                amount = self.web3.from_wei(tx['value'], 'ether')
                logging.info(f"{self.network} - Native Transfer: {amount} ETH to {tx['to']} from {tx['from']}")
                data = {
                    "to": tx['to'],
                    "contractAddress": None,
                    "amount": amount,
                    "network": self.network
                }
                self.send_data_to_server(data)
            if tx['input']:
                await self.process_erc20_transaction(tx, wallets)