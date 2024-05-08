import asyncio

import requests
from web3 import Web3
from web3.exceptions import BlockNotFound
from web3.middleware import geth_poa_middleware
from transaction_processor import TransactionProcessor
from concurrent.futures import ThreadPoolExecutor


from config import RPC_NODES, SERVER_URL
from log import logger as logging


async def fetch_wallets():
    response = requests.get(f'{SERVER_URL}/wallets')
    if response.status_code == 200:
        return response.json()
    else:
        logging.error('Could not fetch wallets')
        return []


async def process_block_async(processor, block_number, wallets, network):
    try:
        await processor.process_block(block_number, wallets)
        logging.info(f"{network} - Block {block_number} processed")
    except BlockNotFound:
        logging.error(f"{network} - Block {block_number} not found and skipped.")


async def monitor_network_async(network, rpc_url):
    web3 = Web3(Web3.HTTPProvider(rpc_url))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)

    processor = TransactionProcessor(web3, network)

    latest_block = web3.eth.get_block_number()

    while True:
        current_block = web3.eth.get_block_number()
        WALLETS = await fetch_wallets()
        tasks = []

        while latest_block <= current_block:
            tasks.append(asyncio.create_task(process_block_async(processor, latest_block, WALLETS, network)))
            latest_block += 1

        if tasks:
            await asyncio.gather(*tasks)


async def run_monitor_tasks():
    with ThreadPoolExecutor(max_workers=len(RPC_NODES)) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(executor, asyncio.run, monitor_network_async(network, url))
            for network, url in RPC_NODES.items()
        ]
        await asyncio.gather(*futures)


def main():
    asyncio.run(run_monitor_tasks())


if __name__ == '__main__':
    asyncio.run(main())