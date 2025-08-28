from ast import ExceptHandler
from decimal import Decimal, InvalidOperation
from math import log
import time
import pytz
import os 
import json
import csv
import asyncio
import logging 
import collections
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import threading
from concurrent.futures import ThreadPoolExecutor
import math
import ccxt
import ccxt.pro

from utils import file_helper, number_helper

LOG_LEVEL = logging.INFO
LOG_FILE = "naive_fundingrate_recorder.log"
DATA_DIR = "live_fr"
INTERVAL_FUNDINGRATE_SECONDS = 60
INTERVAL_MIDS_SECONDS = 30
INTERVAL_BUFFER_INFO = 300
HEADERS = ["datetime", "base_currency", "symbol", "funding_rate", "ask", "bid", "mid",
    "mark", "oi", "volume_24h", "underlying_price", "premium", "oracle", "dayNtlVlm", "funding_rate_adj", "fr_freq"]

# --- Logger Setup ---
def setup_logging():
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

from pprint import pprint
from dotenv import load_dotenv
load_dotenv()

import quantpylib
from quantpylib.gateway.master import Gateway
from quantpylib.utilities.cringbuffer_docs import RingBuffer
from quantpylib.wrappers.paradex import endpoints as paradex_endpoints

def get_ccxt_keys():
    return {
        "woofipro": {
            "key":os.getenv("WOOFIPRO_KEY"),
            "secret":os.getenv("WOOFIPRO_SECRET"),
            "account_id": os.getenv("WOOFIPRO_ACCOUNT_ID")
            }
        }

def get_gateway_keys():
    config_keys = {
        # "binance":{
        #     "key":os.getenv("BIN_KEY"),
        #     "secret":os.getenv("BIN_SECRET")
        # },
        "hyperliquid":{
            "key":os.getenv("HPL_KEY"),
            "secret":os.getenv("HPL_SECRET"),
            "mode": "live"
        },
        "paradex":{
               "key": os.getenv("PAREDEX_L2"),
               "l2_secret": os.getenv("PARADEX_PRIVATE_KEY")
        },
    }
    return config_keys

async def wait_for_next_minute(delay=0):
    now = datetime.now()
    seconds_to_wait = (60 - delay) - (now.second + (now.microsecond / 1_000_000))
    if seconds_to_wait < 0: seconds_to_wait += 60
    logger.info(f"Waiting for {seconds_to_wait:.4f} seconds...")
    await asyncio.sleep(seconds_to_wait)
    logger.info(f"Woke up at: {datetime.now().isoformat()}")

class NaiveFundingRateRecorder():
    def __init__(self, gateway, exchanges, ccxt_keys, batch_ticks_size=20000, batch_mids_size=20000):
        self.gateway = gateway
        self.exchanges = exchanges
        self.mappings_collector = {}
        self.ticks = asyncio.Queue()
        self.mids = asyncio.Queue()
        self.batch_tick_size = batch_ticks_size
        self.batch_mids_size = batch_mids_size
        self.executor = ThreadPoolExecutor() #global thread pool executor to manage the blocking I/O operations.
        self.is_recording = False
        self.ccxt_keys = ccxt_keys
        self.ccxt_exchange_jobs = {}
        self.ccxt_exchange_tasks = {}
        self.symbols_info = {} # exchange => symbols => symbol information according what the exchange allows

        self.gateway_ccxt = {}

        for exchange in exchanges:
            if exchange == "hyperliquid" : self.mappings_collector[exchange] = [self.collect_ticks_hyperliquid, self.collect_mids_hyperliquid, self.collect_hyperliquid_fr] # #
            #if exchange == "hyperliquid" : self.mappings_collector[exchange] = [self.collect_hyperliquid_fr]
            if exchange == "paradex" : self.mappings_collector[exchange] = [self.collect_ticks_paradex, self.collect_mids_paradex, self.collect_paradex_fr]
            #if exchange == "paradex" : self.mappings_collector[exchange] = [self.collect_paradex_fr]

            #if exchange == "woox" : self.mappings_collector[exchange] = [self.collect_woox_fr]
            if exchange == "woofipro": 
                self.gateway_ccxt[exchange] = ccxt.pro.woofipro({'apiKey': self.ccxt_keys[exchange]['key'], 'secret': self.ccxt_keys[exchange]['secret'], 'enableRateLimit': True, })
                self.mappings_collector[exchange] = [self.start_sequential_ccxt_jobs] # essentially in order to run properly each jobs without websocket conflict
                self.ccxt_exchange_jobs[exchange] = [self.collect_mids_woofipro, self.collect_ticks_woofipro, self.collect_woofipro_fr]
                #self.ccxt_exchange_jobs[exchange] = [self.collect_woofipro_fr]
            #if exchange == "woox" : self.mappings_collector[exchange] = [self.collect_ticks_paradex, self.collect_mids_paradex, self.collect_paradex_fr]
            #if exchange == "paradex" : self.mappings_collector[exchange] = [self.collect_ticks_paradex]
            file_helper.ensure_data_directory(exchange)
            file_helper.ensure_data_directory(exchange, "tick")
            file_helper.ensure_data_directory(exchange, "mid")
            file_helper.ensure_data_directory(exchange, "fr")

            self.symbols_info[exchange] = {}

    async def start_sequential_ccxt_jobs(self, exch):
        try:
            logger.info(f"start_sequential_ccxt_jobs: running starting jobs for {exch}")
            self.ccxt_exchange_tasks[exch] = []
        
            for job in self.ccxt_exchange_jobs[exch]:
                logger.info(f"start_sequential_ccxt_jobs: starting {job.__name__} for {exch}")
            
                # Start the task
                task = asyncio.create_task(job(exch))
                self.ccxt_exchange_tasks[exch].append(task)
            
                # Give the task time to initialize (establish connections, etc.)
                await asyncio.sleep(5)
        
            # Wait for all tasks to complete
            await asyncio.gather(*self.ccxt_exchange_tasks[exch])
        
        except Exception as e: 
            logger.error(f"start_sequential_ccxt_jobs: {exch} -> {e}")

    async def recording(self):
        self.is_recording = True
        consumer_ticks_task = asyncio.create_task(self._consume_tick_data())
        consumer_mids_task = asyncio.create_task(self._consume_mids_data())

        logger.info("recording: Run collector(s) per exchange")
        #await asyncio.gather(*[self.mappings_collector[exc](exc) for exc in self.exchanges])
        await asyncio.gather(*[job(exc) for exc in self.exchanges for job in self.mappings_collector[exc]])

        logger.info("recording: Collect ended, let's shudown buffering system...")
        # once collect is done, send a shutdown signal to the consumer
        self.ticks.put(None)
        self.is_recording = False
        self.mids.put(None)

        logger.info("recording: Waiting for consumer data to finish...")
        await asyncio.gather(*[consumer_ticks_task, consumer_mids_task])

        logger.info("recording: Waiting for ccxt market to close...")
        for exch_name, exchange in self.gateway_ccxt.items(): exchange.close()

    """
    Consumes tick data from the queue, batches it, and writes to a Parquet file.
    """
    async def _consume_tick_data(self):
        last_call_time = time.monotonic()
        exchange_buffer = {}
        force_flush = False

        # Define the PyArrow schema
        schema = pa.schema([
            pa.field('machine_ts', pa.timestamp('us', tz='utc')),
            pa.field('exchange_ts', pa.timestamp('us', tz='utc')),
            pa.field('symbol', pa.string()),
            pa.field('px', pa.decimal128(16, 8)), # Precision and scale for decimal type
            pa.field('size', pa.decimal128(16, 8)),
            pa.field('side', pa.int8())
        ])

        logger.info("_consume_tick_data: Tick consumer is ready and waiting for ticks...")
        while True:
            tick_data = await self.ticks.get()
            if tick_data is None:
                logger.info("_consume_tick_data: Tick consumer received shutdown signal.") # The 'None' value is our shutdown signal
                force_flush = True
        
            exch = tick_data[0]
            if exch not in exchange_buffer: exchange_buffer[exch] = []

            ticks_buffer = exchange_buffer[exch]
            ticks_buffer.append(tick_data[1])

            call_time = time.monotonic()
            if (call_time - last_call_time) > INTERVAL_BUFFER_INFO: 
                logger.info(f"_consume_ticks_data: Buffer sizing[{exch}] {len(ticks_buffer)}/{self.batch_tick_size} mid(s) {(len(ticks_buffer)*100.0)/self.batch_tick_size}%")
                last_call_time = call_time

            if not force_flush and len(ticks_buffer) < self.batch_tick_size: continue # Check if the buffer is ready to be written

            logger.info(f"_consume_tick_data: Buffer full, writing a batch of {len(ticks_buffer)} ticks exch:{exch}...")
            file_path = file_helper.get_parquet_filename(exch, "tick")
            data_to_write = ticks_buffer.copy()

            def create_partition(data):
                df = pd.DataFrame(data)
                df['machine_ts'] = pd.to_datetime(df['machine_ts'])
                df['partition_hour'] = df['machine_ts'].dt.hour
                return pa.Table.from_pandas(df)

            asyncio.get_event_loop().run_in_executor(self.executor, file_helper.append_to_parquetfile, file_path, exch, data_to_write, schema, ['symbol', 'partition_hour'], create_partition, logger)
            ticks_buffer.clear()

            if force_flush: break

        self.ticks.task_done() # Notify the main loop that the consumer is done

    async def _store_ticks_hyperliquid(self, msg): 
        await self.ticks.put(("hyperliquid", {"machine_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc), "exchange_ts": datetime.fromtimestamp(msg["time"]/1000), 
                                              "symbol": msg["coin"], "px": Decimal(msg["px"]), "size": Decimal(msg["sz"]), "side": 1 if msg["side"] == "B" else -1}))

    async def collect_ticks_hyperliquid(self, exch):
        while True:
            try:
                logger.info(f"collect_ticks_hyperliquid: Run ticks collector: {exch}...")
                exchange = self.gateway.exc_clients[exch]
                perps_data = await exchange.perpetuals_contexts()
                universe_meta = perps_data[0]["universe"]

                for meta in universe_meta:
                    await exchange.trades_subscribe(ticker=meta['name'], handler=self._store_ticks_hyperliquid, standardize_schema=False)
                
                '''
                await exchange.trades_subscribe(ticker="ETH", handler=self.store_ticks_hyperliquid, standardize_schema=False)
                await exchange.trades_subscribe(ticker="BTC", handler=self.store_ticks_hyperliquid, standardize_schema=False)
                await exchange.trades_subscribe(ticker="HYPE", handler=self.store_ticks_hyperliquid, standardize_schema=False)
                '''

            except Exception as e: logger.error(f"collect_ticks_hyperliquid: Error during data parsing and processing [{exch}]: {e}", exc_info=True)
            finally: 
                #logger.info(f"Waiting for {INTERVAL_SECONDS} seconds before next data fetch [{exch}]...")
                await asyncio.sleep(1e10)

    async def _store_mids_hyperliquid(self, msg):
            for symbol,mid in msg['mids'].items():
                if '@' in symbol: continue
                await self.mids.put(("hyperliquid", {"machine_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc), "symbol": symbol, "mid": Decimal(mid)}))
    
    ''' Collect mid from hyperliquid plateform '''
    async def collect_mids_hyperliquid(self, exch):
        exchange = self.gateway.exc_clients[exch]
        await wait_for_next_minute(2)
        # waiting the next starting minute
        while True:
            now = datetime.fromtimestamp(time.time(), tz=timezone.utc)
            if now.second > 58 and now.second <= 59: break
            await asyncio.sleep(1)

        while self.is_recording:
            try:
                logger.info(f"collect_mids_hyperliquid: Requesting mids: {exch}...")
                now = datetime.fromtimestamp(time.time(), tz=timezone.utc)
                perps_data = await exchange.perpetuals_contexts()
                universe_meta, universe_ctx = perps_data[0]["universe"],perps_data[1]

                for meta, context in zip(universe_meta, universe_ctx):
                    if not number_helper.is_valid_decimal(context['midPx']) : continue
                    await self.mids.put(("hyperliquid", {"machine_ts": now, "symbol": meta['name'], "mid": Decimal(context['midPx'])}))

            except Exception as e: logger.error(f"collect_mids_hyperliquid: error during data parsing and processing [{exch}]: {e}", exc_info=True)

            finally:
                # Calculate the next scheduled timestamp.
                now_ts = time.time()
                next_ts = (math.ceil(time.time() / (INTERVAL_MIDS_SECONDS)) * (INTERVAL_MIDS_SECONDS)) - 2
                sleep_duration = next_ts - now_ts
                sleep_duration = sleep_duration if sleep_duration > 0 else INTERVAL_MIDS_SECONDS - 1
                logger.info(f"collect_mids_hyperliquid: Waiting for {sleep_duration} seconds before next data fetch...")
                await asyncio.sleep(sleep_duration)

    async def _store_ticks_paradex(self, msg): 
        await self.ticks.put(("paradex", {"machine_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc), "exchange_ts": datetime.fromtimestamp(msg["created_at"]/1000), 
                                                "symbol": msg["market"], "px": Decimal(msg["price"]), "size": Decimal(msg["size"]), "side": 1 if msg["side"] == "BUY" else -1}))

    async def collect_ticks_paradex(self, exch):
        while True:
            try:
                logger.info(f"collect_ticks_paradex: Run ticks collector: {exch}...")
                exchange = self.gateway.exc_clients[exch]
                
                #retrieve symbols
                market_summaries = await exchange.get_markets_summary()
                market_summaries = market_summaries['results']

                for market_summary in market_summaries:
                    if 'PERP' not in market_summary['symbol']: continue
                    await exchange.trades_subscribe(ticker=market_summary['symbol'], handler=self._store_ticks_paradex, standardize_schema=False)

            except Exception as e: logger.error(f"collect_ticks_paradex: Error during data parsing and processing [{exch}]: {e}", exc_info=True)
            finally: 
                #logger.info(f"Waiting for {INTERVAL_SECONDS} seconds before next data fetch [{exch}]...")
                await asyncio.sleep(1e10)

    ''' Collect mid from paradex plateform '''
    async def collect_mids_paradex(self, exch):
        exchange = self.gateway.exc_clients[exch]
        await wait_for_next_minute(2)
        # waiting the next starting minute
        while True:
            now = datetime.fromtimestamp(time.time(), tz=timezone.utc)
            if now.second > 58 and now.second <= 59: break
            await asyncio.sleep(1)

        while self.is_recording:
            try:
                logger.info(f"collect_mids_paradex: Requesting mids: {exch}...")
                now = datetime.fromtimestamp(time.time(), tz=timezone.utc)

                all_mids = await exchange.get_all_mids()

                for symbol, mid in all_mids.items():
                    if 'PERP' not in symbol: continue
                    #if not is_valid_decimal(mid): continue

                    await self.mids.put(("paradex", {"machine_ts": now, "symbol": symbol, "mid": mid}))

            except Exception as e: logger.error(f"collect_mids_paradex: error during data parsing and processing [{exch}]: {e}", exc_info=True)
            finally:
                # Calculate the next scheduled timestamp.
                now_ts = time.time()
                next_ts = (math.ceil(time.time() / (INTERVAL_MIDS_SECONDS)) * (INTERVAL_MIDS_SECONDS)) - 2
                sleep_duration = next_ts - now_ts
                sleep_duration = sleep_duration if sleep_duration > 0 else INTERVAL_MIDS_SECONDS - 1
                logger.info(f"collect_mids_paradex: Waiting for {sleep_duration} seconds before next data fetch...")
                await asyncio.sleep(sleep_duration)

    """
    Consumes mid data from the queue, batches it, and writes to a Parquet file.
    """
    async def _consume_mids_data(self):
        exchange_buffer = {}
        force_flush = False
        last_call_time = time.monotonic()

        # Define the PyArrow schema
        schema = pa.schema([
            pa.field('machine_ts', pa.timestamp('us', tz='utc')),
            pa.field('symbol', pa.string()),
            pa.field('mid', pa.decimal128(16, 8)), # Precision and scale for decimal type
        ])

        logger.info("_consume_mids_data: Mid consumer is ready and waiting for mid(s)...")
        while True:
            mids_data = await self.mids.get()
            if mids_data is None:
                logger.info("_consume_mids_data: Mid consumer received shutdown signal. Flush buffer before quitting") # The 'None' value is our shutdown signal
                force_flush = True
        
            call_time = time.monotonic()

            exch = mids_data[0]
            if exch not in exchange_buffer: exchange_buffer[exch] = []

            mids_buffer = exchange_buffer[exch]
            mids_buffer.append(mids_data[1])

            call_time = time.monotonic()
            if (call_time - last_call_time) > INTERVAL_BUFFER_INFO: 
                logger.info(f"_consume_mids_data: Buffer sizing[{exch}] {len(mids_buffer)}/{self.batch_mids_size} mid(s) {(len(mids_buffer)*100.0)/self.batch_mids_size}%")
                last_call_time = call_time

            if not force_flush and len(mids_buffer) < self.batch_mids_size: continue # Check if the buffer is ready to be written

            logger.info(f"_consume_mids_data: Buffer full, writing a batch of {len(mids_buffer)} mid(s) exch:{exch}...")
            file_path = file_helper.get_parquet_filename(exch, "mid")

            data_to_write = mids_buffer.copy()

            def create_partition(data):
                df = pd.DataFrame(data)
                df['machine_ts'] = pd.to_datetime(df['machine_ts'])
                df['partition_hour'] = df['machine_ts'].dt.hour
                return pa.Table.from_pandas(df)

            asyncio.get_event_loop().run_in_executor(self.executor, file_helper.append_to_parquetfile, file_path, exch, data_to_write, schema, ['symbol', 'partition_hour'], create_partition, logger)
            mids_buffer.clear()

            if force_flush: break

        self.mids.task_done() # Notify the main loop that the consumer is done

    ''' Collect 1h funding rate (wout %) '''
    async def collect_hyperliquid_fr(self, exch):
        start_time = 0
        await wait_for_next_minute()

        while self.is_recording:
            try:
                start_time = time.monotonic() 
                logger.info(f"collect_hyperliquid_fr: Attempting to fetch new data from: {exch}...")

                exchange = self.gateway.exc_clients[exch]
                perps_data = await exchange.perpetuals_contexts()
                universe_meta, universe_ctx = perps_data[0]["universe"],perps_data[1]

                # datetime, base_currency, symbol, funding_rate, ask, bid, mid, mark, oi, volume_24h, underlying_price, premium, oracle, dayNtlVlm
                results = []
                current_timestamp = time.time()
                now = datetime.fromtimestamp(current_timestamp, tz=timezone.utc)

                for meta, context in zip(universe_meta, universe_ctx):
                    results.append({"datetime": now,  "base_currency": meta['name'], "symbol": meta['name'], "funding_rate": context['funding'], "ask": 0, "bid": 0, "mid": context['midPx'], "mark": context['markPx'], 
                                    "oi": context['openInterest'], "volume_24h": 0, "underlying_price":0, "premium":context['premium'], "oracle": context['oraclePx'], "dayNtlVlm": context['dayNtlVlm'],
                                    "funding_rate_adj": context['funding'], "fr_freq": 1})

                asyncio.get_event_loop().run_in_executor(self.executor, file_helper.write_to_csv, exch, results, HEADERS, "fr", logger)

            except Exception as e: logger.error(f"Error during data parsing and processing [{exch}]: {e}", exc_info=True)
            finally: 
                sleep_duration = max(0, INTERVAL_FUNDINGRATE_SECONDS - (time.monotonic() - start_time))
                logger.info(f"collect_hyperliquid_fr: Waiting for {sleep_duration} seconds before next data fetch...")
                await asyncio.sleep(sleep_duration)

    ''' Collect 1h funding rate (wout %) '''
    async def collect_paradex_fr(self, exch):
        start_time = 0
        await wait_for_next_minute()

        while self.is_recording:
            try:
                start_time = time.monotonic()
                logger.info(f"collect_paradex_fr: Attempting to fetch new data from: {exch}...")

                exchange = self.gateway.exc_clients[exch]
                exchange_symbol_infos = self.symbols_info[exch]

                markets = await exchange.get_markets()
                markets = markets['results']
                for market in markets:
                    if not market["asset_kind"] == "PERP": continue
                    exchange_symbol_infos[market["base_currency"]] = market

                market_summaries = await exchange.get_markets_summary()
                market_summaries = market_summaries['results']

                # datetime, base_currency, symbol, funding_rate, ask, bid, mid, mark, oi, volume_24h, underlying_price, premium, oracle, dayNtlVlm
                results = []
                current_timestamp = time.time()
                now = datetime.fromtimestamp(current_timestamp, tz=timezone.utc)

                for market_summary in market_summaries:
                    if 'PERP' not in market_summary['symbol']: continue

                    symbol = market_summary['symbol']
                    ticker = symbol.replace('-USD-PERP', '')

                    if ticker not in exchange_symbol_infos:
                        logger.error(f"collect_paradex_fr: {ticker} does not find into exchange_symbol_infos", exc_info=True)
                        continue

                    fr_freq = exchange_symbol_infos[ticker]["funding_period_hours"]
                    ask_valid, bid_valid, fr_valid = number_helper.is_valid_decimal(market_summary["ask"]), number_helper.is_valid_decimal(market_summary["bid"]), number_helper.is_valid_decimal(market_summary["funding_rate"])
                    mid = (Decimal(market_summary["ask"]) + Decimal(market_summary["bid"])) / 2 if ask_valid and bid_valid else Decimal(market_summary["ask"]) if ask_valid else Decimal(market_summary["bid"])
                    rate = Decimal(market_summary['funding_rate']) if fr_valid else 0
                    rate_adj = rate / fr_freq                 

                    results.append({"datetime": now,  "base_currency": ticker, "symbol": symbol, "funding_rate": rate, "ask": Decimal(market_summary["ask"]) if ask_valid else market_summary["ask"], 
                                    "bid": Decimal(market_summary["bid"]) if bid_valid else market_summary["bid"], "mid": mid, "mark": market_summary['mark_price'], 
                                    "oi": market_summary['open_interest'], "volume_24h": market_summary['volume_24h'], "underlying_price": market_summary['underlying_price'], "premium": 0, "oracle": 0, 
                                    "dayNtlVlm": market_summary['volume_24h'], "funding_rate_adj": rate_adj, "fr_freq": fr_freq})
                    

                asyncio.get_event_loop().run_in_executor(self.executor, file_helper.write_to_csv, exch, results, HEADERS, "fr", logger)
                
                
            except Exception as e: logger.error(f"collect_paradex_fr: Error during data parsing and processing: {e}", exc_info=True)
            finally: 
                sleep_duration = max(0, INTERVAL_FUNDINGRATE_SECONDS - (time.monotonic() - start_time))
                logger.info(f"collect_paradex_fr: Waiting for {sleep_duration} seconds before next data fetch...")
                await asyncio.sleep(sleep_duration)

        return

    async def collect_woofipro_fr(self, exch):
        start_time = 0
        await wait_for_next_minute()

        while self.is_recording:
            try:
                start_time = time.monotonic()
                logger.info(f"collect_woofipro_fr: Attempting to fetch new data from: {exch}...")

                exchange = self.gateway_ccxt[exch]
                exchange_symbol_infos = self.symbols_info[exch]

                results = await exchange.v1PublicGetPublicInfo()
                available_symbols = results['data']['rows']
                for available_symbol in available_symbols:
                    if "PERP" not in available_symbol["symbol"]: continue
                    exchange_symbol_infos[available_symbol["symbol"].replace("PERP_","").replace("_USDC","")] = available_symbol

                results = await exchange.v1PublicGetPublicFutures()
                futures_info = results['data']['rows']

                # datetime, base_currency, symbol, funding_rate, ask, bid, mid, mark, oi, volume_24h, underlying_price, premium, oracle, dayNtlVlm
                results = []
                current_timestamp = time.time()
                now = datetime.fromtimestamp(current_timestamp, tz=timezone.utc)

                for future_info in futures_info:

                    symbol, ticker = future_info['symbol'], future_info['symbol'].replace("PERP_","").replace("_USDC","")

                    if ticker not in exchange_symbol_infos:
                        logger.error(f"collect_woofipro_fr: {ticker} does not find into exchange_symbol_infos", exc_info=True)
                        continue

                    fr_freq = exchange_symbol_infos[ticker]["funding_period"]
                    rate = future_info['est_funding_rate']
                    rate_adj = rate / fr_freq

                    results.append({"datetime": now,  "base_currency": ticker, "symbol": symbol, "funding_rate": rate, "ask": 0, "bid": 0, "mid": future_info['24h_close'], "mark": future_info['mark_price'], 
                                    "oi": 0 if future_info['open_interest'] == "None" else future_info['open_interest'], "volume_24h": future_info['24h_volume'], "underlying_price": 0, "premium": 0, 
                                    "oracle": future_info['index_price'], "dayNtlVlm": future_info['24h_amount'], "funding_rate_adj": rate_adj, "fr_freq": fr_freq})
                    

                asyncio.get_event_loop().run_in_executor(self.executor, file_helper.write_to_csv, exch, results, HEADERS, "fr", logger)
                
                
            except Exception as e: logger.error(f"collect_woofipro_fr: Error during data parsing and processing: {e}", exc_info=True)
            finally: 
                sleep_duration = max(0, INTERVAL_FUNDINGRATE_SECONDS - (time.monotonic() - start_time))
                logger.info(f"collect_woofipro_fr: Waiting for {sleep_duration} seconds before next data fetch...")
                await asyncio.sleep(sleep_duration)

        return

    async def _store_ticks_woofipro(self, exchange_name, exchange, symbol): 
        while self.is_recording:
            try:
                trades = await exchange.watchTrades(symbol)
                for trade in trades:
                    await self.ticks.put(("woofipro", {"machine_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc), "exchange_ts": datetime.fromtimestamp(trade["timestamp"]/1000), 
                                                "symbol": symbol, "px": Decimal(trade["price"]), "size": Decimal(trade["amount"]), "side": 1 if trade["side"] == "buy" else -1}))

            except Exception as e: 
                logger.error(f"_store_ticks_woofipro: Error during data parsing and processing [{exchange_name}]: {e}", exc_info=True)
                break

    async def collect_ticks_woofipro(self, exch):
        try:
            logger.info(f"collect_ticks_woofipro: Run ticks collector: {exch}...")
            exchange = self.gateway_ccxt[exch]
            
            # Retrieve symbols
            markets = await exchange.fetch_markets()
            markets = [x for x in markets if 'PERP' in x['id']]
        
            # Create tasks for each symbol individually (this is key!)
            tasks = [
                asyncio.create_task(self._store_ticks_woofipro(exch, exchange, symbol['id'])) 
                for symbol in markets
            ]
        
            # Wait for all tasks or until recording stops
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                # Cancel all tasks when stopping
                for task in tasks:
                    task.cancel()
                raise
            
        except Exception as e: logger.error(f"collect_ticks_woofipro: Error: {e}", exc_info=True)

    ''' Collect mid from woofipro plateform '''
    async def collect_mids_woofipro(self, exch):
        exchange = self.gateway_ccxt[exch]
        try:
            # await wait_for_next_minute(2)
            # # waiting the next starting minute
            # while True:
            #     now = datetime.fromtimestamp(time.time(), tz=timezone.utc)
            #     if now.second > 58 and now.second <= 59: break
            #     await asyncio.sleep(1)

            markets = await exchange.fetch_markets()
            markets = [x['id'] for x in markets if 'PERP' in x['id']]                  

            while self.is_recording:
                try:
                    logger.info(f"collect_mids_woofipro: Requesting mids: {exch}...")
                    now = datetime.fromtimestamp(time.time(), tz=timezone.utc)

                    #bbos = await exchange.watch_bids_asks(markets)
                    # Add timeout to prevent hanging
                    bbos = await asyncio.wait_for(
                        exchange.watch_bids_asks(markets), 
                        timeout=15.0  # 60 second timeout
                    )

                    for key, bbo in bbos.items():
                        symbol = bbo["info"]["symbol"]
                        mid = (bbo["ask"] + bbo["bid"]) / 2
                        await self.mids.put((exch, {"machine_ts": now, "symbol": symbol, "mid": mid}))
                        # ask_valid, bid_valid = is_valid_decimal(bbo["ask"]), is_valid_decimal(bbo["bid"])
                        # mid = (Decimal(bbo["ask"]) + Decimal(bbo["bid"])) / 2 if ask_valid and bid_valid else Decimal(bbo["ask"]) if not bid_valid else Decimal(bbo["bid"])
                        # await self.mids.put((exch, {"machine_ts": now, "symbol": symbol, "mid": mid}))

            
                except asyncio.TimeoutError:
                    logger.warning(f"collect_mids_woofipro: Timeout on watch_bids_asks [{exch}]")
                    # Don't break, just continue to next iteration
                    continue        
                except Exception as e: logger.error(f"collect_mids_woofipro: error during data parsing and processing [{exch}]: {e}", exc_info=True)
                finally:
                    # Calculate the next scheduled timestamp.
                    now_ts = time.time()
                    next_ts = (math.ceil(time.time() / (INTERVAL_MIDS_SECONDS)) * (INTERVAL_MIDS_SECONDS)) - 2
                    sleep_duration = next_ts - now_ts
                    sleep_duration = sleep_duration if sleep_duration > 0 else INTERVAL_MIDS_SECONDS - 1
                    logger.info(f"collect_mids_woofipro: Waiting for {sleep_duration} seconds before next data fetch...")
                    await asyncio.sleep(sleep_duration)
        
        except Exception as e: logger.error(f"collect_mids_woofipro: Fatal error: {e}", exc_info=True)

async def main():
    
    try:

        # await test("hyperliquid", "mid", "2025-08-27", "BTC", 6)
        # await test("hyperliquid", "tick", "2025-08-27", "BTC", 6)
        # await test("paradex", "tick", "2025-08-27", "BTC-USD-PERP", 4)
        # await test("paradex", "mid", "2025-08-27", "BTC-USD-PERP", 6)
        # await test("woofipro", "tick", "2025-08-27", "PERP_BTC_USDC", 4)
        # await test("woofipro", "mid", "2025-08-27", "PERP_BTC_USDC", 6)

        gateway = None
        if len(get_gateway_keys()) > 0:
            gateway = Gateway(config_keys=get_gateway_keys()) 
            await gateway.init_clients()

        #recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["hyperliquid","paradex"], batch_mids_size=4000)
        #recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["hyperliquid"], batch_mids_size=4000)
        #recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["paradex"], batch_ticks_size=500, batch_mids_size=500)
        #recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["woofipro"], ccxt_keys=get_ccxt_keys(),batch_ticks_size=100, batch_mids_size=100)
        recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["hyperliquid","paradex","woofipro"], ccxt_keys=get_ccxt_keys(), batch_ticks_size=10000, batch_mids_size=4000)
        await recorder.recording()  
    except Exception as e:
        logger.critical(f"main: An unhandled critical error occurred: {e}", exc_info=True)
    finally:
        await gateway.cleanup_clients()

async def test(exch, data_type, date_str, symbol, hour):
    filters = [('symbol', '==', symbol),('partition_hour', '==', hour)]
    file_name = f"{exch}_data/{data_type}/{exch}_{data_type}_{date_str}.parquet"

    try:

        filtered_df = pd.read_parquet(file_name, engine='pyarrow', filters=filters)
        print(filtered_df.head())
        print(filtered_df.tail())
        filtered_df.to_csv(f"{exch}_{data_type}_{date_str}.csv", sep=";")
        logger.info("It is working")
    except Exception as e: 
        print(e)
        logger.error(e)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Data collection stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(f"An unhandled critical error occurred: {e}", exc_info=True)