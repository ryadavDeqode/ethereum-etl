from concurrent.futures import ThreadPoolExecutor
import time
import logging
import json
from blockchainetl.streaming.streamer import Streamer
from ethereumetl.enumeration.entity_type import EntityType
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.streaming.enrich import enrich_transactions,\
    enrich_alfred_follow_unfollow_logs
from ethereumetl.streaming.eth_streamer_adapter import EthStreamerAdapter, sort_by
from ethereumetl.thread_local_proxy import ThreadLocalProxy
# from core.utils.db_utils import set_last_synced_block
import asyncio
import random
from web3 import Web3

entities = ["block", "transaction", "log", "receipt",
            "token_transfer", 'alfred_follow_unfollow_logs',
            'gmx_execute_limit_orders_logs',
            'gmx_execute_market_orders_logs','ALL']
BLOCK, TRANSACTION, LOG, RECEIPT, TOKEN_TRANSFER,\
ALFRED_FOLLOW_UNFOLLOW_LOGS,GMX_EXECUTE_LIMIT_ORDERS_LOGS\
,GMX_EXECUTE_MARKET_ORDERS_LOGS, ALL = entities

class ConsoleItemExporter:
    ''' Item Exporter class '''

    def open(self):
        ''' open exporter '''
        pass

    def export_items(self, items):
        ''' item exporter '''
        for item in items:
            yield item

    def export_item(self, item):
        ''' item exporter '''
        yield json.dumps(item)

    def close(self):
        ''' close exporter '''
        pass


class EthStreamerAdapterPrivate(EthStreamerAdapter):
    ''' eth stream adapter '''

    def export_all(self, start_block, end_block):
        ''' export all '''
        # Export blocks and transactions
        blocks, transactions = [], []
        if self._should_export(EntityType.BLOCK) or \
            self._should_export(EntityType.TRANSACTION):
            blocks, transactions = self._export_blocks_and_transactions(
                start_block, end_block)

        # Export receipts and logs
        receipts, logs = [], []
        if self._should_export(EntityType.RECEIPT) or \
            self._should_export(EntityType.LOG):
            receipts, logs = self._export_receipts_and_logs(transactions)
        # Export filtered logs
        alfred_follow_log = []
        if self._should_export(EntityType.ALFRED_FOLLOW_UNFOLLOW_LOGS):
            alfred_follow_log = self._extract_alfred_follow_logs(logs)
        enriched_blocks = blocks \
            if EntityType.BLOCK in self.entity_types else []
        enriched_transactions = enrich_transactions(transactions, receipts) \
            if EntityType.TRANSACTION in self.entity_types else []
        enriched_alfred_follow_logs = enrich_alfred_follow_unfollow_logs\
            (blocks, alfred_follow_log) \
            if EntityType.ALFRED_FOLLOW_UNFOLLOW_LOGS in self.entity_types else []

        logging.info('Exporting with %s', type(self.item_exporter).__name__)

        all_items = \
            sort_by(enriched_blocks, 'number') + \
            sort_by(enriched_transactions, ('block_number', 'transaction_index')) + \
            sort_by(enriched_alfred_follow_logs, ('block_number','log_index'))

        self.calculate_item_ids(all_items)
        self.calculate_item_timestamps(all_items)
        return self.item_exporter.export_items(all_items)


class ETHStreamer:
    ''' eth streamer '''

    def __init__(self, provider_uri, entity_types, batch_size=200,
                 max_workers=10, last_synced_block=None,
                 end_block=None, stream_identifier=None,chain_id=42161):
        ''' initialise streamer config '''
        self.end_block = end_block
        self.provider_uri = provider_uri
        self.batch_size = 15
        self.max_workers = max_workers
        self.entity_types = entity_types
        self.stream_identifier = stream_identifier
        self.chain_id = chain_id
        self.last_synced_block = last_synced_block or 0

        self.streamer_adapter = EthStreamerAdapterPrivate(
            batch_web3_provider=ThreadLocalProxy(lambda:
                get_provider_from_uri(self.provider_uri, batch=True)),
            item_exporter=ConsoleItemExporter(),
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            entity_types=self.entity_types
            # [EntityType.BLOCK]  # , LOG, TOKEN_TRANSFER, TRACE, CONTRACT, TOKEN]
        )

        self.streamer = Streamer(
            blockchain_streamer_adapter=self.streamer_adapter,
            lag=1,
            period_seconds=10,
            block_batch_size=self.batch_size,
            pid_file=None
        )
            

    async def get_provider(self):
        rpc_providers = [
            "https://arb-mainnet-public.unifra.io", # passed
            "https://rpc.arb1.arbitrum.gateway.fm", # passed
            "https://arb1.croswap.com/rpc", # passed
            "https://arb1.arbitrum.io/rpc", # passed
            "https://arbitrum.blockpi.network/v1/rpc/public", # passed
            "https://rpc.ankr.com/arbitrum", # passed
            "https://1rpc.io/arb", # failed, too many errors, can be kept
            "https://endpoints.omniatech.io/v1/arbitrum/one/public", # passed
            "https://arbitrum-one.publicnode.com",  # passed
        ]

        # Pick a random provider
        provider = random.choice(rpc_providers)
        print("Attempting provider:", provider)
        try:
            web3_prov = Web3(Web3.HTTPProvider(provider))
            get_block = web3_prov.eth.blockNumber
            if not get_block:
                raise Exception(f"{provider} failed to connect")
            response = provider
        except Exception as e:
            print(e)
            response = await self.get_provider()
        return response

    def stream(self):
        """stream"""
        while True and (
            self.end_block is None or self.last_synced_block < self.end_block
        ):
            with ThreadPoolExecutor(1) as pool:
                self.provider_uri = pool.submit(
                    lambda: asyncio.run(
                        self.get_provider()
                    )
                ).result()
                print("provider", self.provider_uri)
            last_block = self.last_synced_block
            self.last_synced_block = last_block or self.last_synced_block or 0
            currentBlock = self.last_synced_block + 1
            current_block = self.streamer_adapter.get_current_block_number()
            target_block = self.streamer._calculate_target_block(current_block,
                                                                 self.last_synced_block)
            print("next batch target:-", target_block)
            if self.last_synced_block + 1 >= target_block:
                print(
                    f'Nothing to sync. Sleeping for\
                        {self.streamer.period_seconds} seconds...')
                time.sleep(self.streamer.period_seconds)
                continue
            
            items = self.streamer_adapter.export_all(
                self.last_synced_block + 1, target_block)
            self.last_synced_block = target_block
            batched_data = []
            for item in items:
                if item['block_number'] != currentBlock:
                    yield batched_data
                    currentBlock = item['block_number']
                    batched_data = [item]
                else:
                    batched_data.append(item)
            yield batched_data

# streamer = ETHStreamer("https://rpc.arb1.arbitrum.gateway.fm",\
# "alfred_follow_unfollow_logs", last_synced_block\
#     =119011114, end_block=119011114 + 4000,\
#     stream_identifier='stream_id',chain_id=42161)
# st = time.time()
# print(st)
# for item in streamer.stream():
#     print(item)

# print(time.time() - st)


# rpc_providers = [
#             "https://arb-mainnet-public.unifra.io", # passed 573 sec
#             "https://arbitrum-one.publicnode.com",  # 
#             "https://endpoints.omniatech.io/v1/arbitrum/one/public",
#             "https://1rpc.io/arb",
#             "https://arbitrum-one.public.blastapi.io",
#             "https://arbitrum.meowrpc.com",
#             "https://rpc.ankr.com/arbitrum",
#             "https://arbitrum.blockpi.network/v1/rpc/public",
#             "https://arb1.arbitrum.io/rpc",
#             "https://rpc.arb1.arbitrum.gateway.fm",
#             "https://api.zan.top/node/v1/arb/one/public",
#             "https://arb1.croswap.com/rpc"
#         ][::-1]


# rpc_providers = [
#             "https://arb-mainnet-public.unifra.io", # passed
#             "https://rpc.arb1.arbitrum.gateway.fm", # passed
#             "https://arb1.croswap.com/rpc" # passed
#             "https://arb1.arbitrum.io/rpc", # passed
#             "https://arbitrum.blockpi.network/v1/rpc/public", # passed
#             "https://rpc.ankr.com/arbitrum", # passed
#             "https://1rpc.io/arb", # failed, too many errors, can be kept
#             "https://endpoints.omniatech.io/v1/arbitrum/one/public", # passed
#             "https://arbitrum-one.publicnode.com",  # passed
#         ][::-1]

# status = []

# for i in range(0, len(rpc_providers)):
#     try:
#         status.append({
#             "status": "failed",
#             "time_taken": 0
#         })
#         print("Running RPC provider", rpc_providers[i])
#         streamer = ETHStreamer(rpc_providers[i],\
#         "alfred_follow_unfollow_logs", last_synced_block\
#             =119011114, end_block=119011114 + 4000,\
#             stream_identifier='stream_id',chain_id=42161)
#         st = time.time()
#         print(st)
#         for item in streamer.stream():
#             print(item)

#         print(time.time() - st)

#         status[i]["status"] =   "success"
#         status[i]["time_taken"] = time.time() - st
#         print('**************************************')
#         print(status)
#         print('**************************************')
#     except Exception as e:
#         print("exception occured for rpc ")
#         status[i]["status"] =   "failed"
#         status[i]["time_taken"] = 0
#         print('**************************************')
#         print(status)
#         print('**************************************')
#         continue

# print(status)