from ast import ExceptHandler
from decimal import Decimal
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
from datetime import datetime, timezone
import pyarrow as pa
import pyarrow.parquet as pq
import threading
from concurrent.futures import ThreadPoolExecutor

LOG_LEVEL = logging.INFO
LOG_FILE = "naive_fundingrate_recorder.log"
DATA_DIR = "live_fr"
INTERVAL_FUNDINGRATE_SECONDS = 60
INTERVAL_MIDS_SECONDS = 30
HEADERS = ["datetime", "base_currency", "symbol", "funding_rate", "ask", "bid", "mid",
    "mark", "oi", "volume_24h", "underlying_price", "premium", "oracle", "dayNtlVlm"]

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



def get_key():
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
        #"paradex":{
        #        "key": os.getenv("PAREDEX_L2"),
        #        "l2_secret": os.getenv("PARADEX_PRIVATE_KEY")
        #}
    }
    return config_keys

def ensure_data_directory(exch, suffix=None):
    """Ensures the data directory exists."""
    path = f"{exch}_data" if suffix is None else f"{exch}_data/{suffix}"
    if not os.path.exists(path):
        os.makedirs(path)
        logger.info(f"Created data directory: {path}")

def get_csv_filename(exch, suffix):
        """Generates a CSV filename based on the current date (YYYY-MM-DD)."""
        # Get the current date and format it as YYYY-MM-DD
        today_str = datetime.now().strftime("%Y-%m-%d")
        # Construct the full path for the CSV file
        return os.path.join(f"{exch}_data/{suffix}", f"{exch}_{today_str}.csv")

def get_parquet_filename(exch, suffix):
    today_str = datetime.now().strftime("%Y-%m-%d")
    #return os.path.join(f"{exch}_data", f"{exch}_{suffix}_{today_str}.parquet")
    return os.path.join(f"{exch}_data/{suffix}", f"{exch}_{suffix}_{today_str}.parquet")

def write_to_csv(exch, data, headers, suffix):
        """
        Writes a dictionary of data to a CSV file.
        It expects 'data' to be a dictionary where keys are column headers.
        You might need to adjust this function based on the actual structure
        of the data returned by the Loris API.
        """
        filename = get_csv_filename(exch, suffix)
        file_exists = os.path.exists(filename)
    
        if not data:
            logger.warning("No data to write to CSV.")
            return
    
        try:
            with open(filename, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
            
                if not file_exists:
                    writer.writeheader()  # Write header only if the file is new
                    logger.info(f"Created new CSV file: {filename} with headers: {headers}")
            
                for data_raw in data: writer.writerow(data_raw)
                logger.info(f"Data successfully written to {filename}.")
        except IOError as e:
            logger.error(f"Error writing to CSV file {filename}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while writing data to CSV: {e}")

def append_to_parquetfile(file_path, exch, data, schema, partition_cols, dataFormaterFct):
    #sucess = False
    try:
        if not data: return

        df = dataFormaterFct(data)
        pq.write_to_dataset(
            df,
            root_path=file_path,
            partition_cols=partition_cols
            )
        logger.info(f"[{exch}]: Batch written successfully.")
        #sucess = True
    except Exception as e: logger.error(f"write_to_parquetfile: an unexpected error occurred while writing data to parquet file: {e}")
    #finally: return sucess

async def wait_for_next_minute(delay=0):
    now = datetime.now()
    seconds_to_wait = (60 - delay) - (now.second + (now.microsecond / 1_000_000))
    if seconds_to_wait < 0: seconds_to_wait += 60
    logger.info(f"Waiting for {seconds_to_wait:.4f} seconds...")
    await asyncio.sleep(seconds_to_wait)
    logger.info(f"Woke up at: {datetime.now().isoformat()}")

class NaiveFundingRateRecorder():
    def __init__(self, gateway, exchanges, batch_ticks_size=20000, batch_mids_size=20000):
        self.gateway = gateway
        self.exchanges = exchanges
        self.mappings_collector = {}
        self.ticks = asyncio.Queue()
        self.mids = asyncio.Queue()
        self.batch_tick_size = batch_ticks_size
        self.batch_mids_size = batch_mids_size
        self.executor = ThreadPoolExecutor() #global thread pool executor to manage the blocking I/O operations.
        self.is_recording = False

        for exchange in exchanges:
            if exchange == "hyperliquid" : 
                self.mappings_collector[exchange] = [self.collect_ticks_hyperliquid, self.collect_mids_hyperliquid, self.collect_hyperliquid_fr] # #
            if exchange == "paradex" : self.mappings_collector[exchange] = self.collect_paradex_fr
            ensure_data_directory(exchange)
            ensure_data_directory(exchange, "tick")
            ensure_data_directory(exchange, "mid")
            ensure_data_directory(exchange, "fr")

    async def recording(self):
        self.is_recording = True
        consumer_ticks_task = asyncio.create_task(self._consume_tick_data())
        consumer_mids_task = asyncio.create_task(self._consume_mids_data())

        logger.info("Run collector(s) per exchange")
        #await asyncio.gather(*[self.mappings_collector[exc](exc) for exc in self.exchanges])
        await asyncio.gather(*[job(exc) for exc in self.exchanges for job in self.mappings_collector[exc]])

        logger.info("Collect ended, let's shudown buffering system...")
        # once collect is done, send a shutdown signal to the consumer
        self.ticks.put(None)
        self.is_recording = False
        self.mids.put(None)

        logger.info("Waiting for consumer data to finish...")
        await asyncio.gather(*[consumer_ticks_task, consumer_mids_task])

    """
    Consumes tick data from the queue, batches it, and writes to a Parquet file.
    """
    async def _consume_tick_data(self):
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

        logger.info("Tick consumer is ready and waiting for ticks...")
        while True:
            tick_data = await self.ticks.get()
            if tick_data is None:
                logger.info("Tick consumer received shutdown signal.") # The 'None' value is our shutdown signal
                force_flush = True
        
            exch = tick_data[0]
            if exch not in exchange_buffer: exchange_buffer[exch] = []

            ticks_buffer = exchange_buffer[exch]
            ticks_buffer.append(tick_data[1])

            if not force_flush and len(ticks_buffer) < self.batch_tick_size: continue # Check if the buffer is ready to be written

            logger.info(f"Buffer full, writing a batch of {len(ticks_buffer)} ticks exch:{exch}...")
            file_path = get_parquet_filename(exch, "tick")
            data_to_write = ticks_buffer.copy()

            def create_partition(data):
                df = pd.DataFrame(data)
                df['machine_ts'] = pd.to_datetime(df['machine_ts'])
                df['partition_hour'] = df['machine_ts'].dt.hour
                return pa.Table.from_pandas(df)

            asyncio.get_event_loop().run_in_executor(self.executor, append_to_parquetfile, file_path, exch, data_to_write, schema, ['symbol', 'partition_hour'], create_partition)
            ticks_buffer.clear()

            if force_flush: break

        self.ticks.task_done() # Notify the main loop that the consumer is done

    async def _store_ticks_hyperliquid(self, msg): 
        await self.ticks.put(("hyperliquid", {"machine_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc), "exchange_ts": datetime.fromtimestamp(msg["time"]/1000), 
                                              "symbol": msg["coin"], "px": Decimal(msg["px"]), "size": Decimal(msg["sz"]), "side": 1 if msg["side"] == "B" else -1}))

    async def collect_ticks_hyperliquid(self, exch):
        while True:
            try:
                logger.info(f"Run ticks collector: {exch}...")
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

            except Exception as e: logger.error(f"Error during data parsing and processing [{exch}]: {e}", exc_info=True)
            finally: 
                logger.info(f"end collect_tick_hyperliquid...")
                #logger.info(f"Waiting for {INTERVAL_SECONDS} seconds before next data fetch [{exch}]...")
                await asyncio.sleep(1e10)

    async def _store_mids_hyperliquid(self, msg):
            for symbol,mid in msg['mids'].items():
                if '@' in symbol: continue
                await self.mids.put(("hyperliquid", {"machine_ts": datetime.fromtimestamp(time.time(), tz=timezone.utc), "symbol": symbol, "mid": Decimal(mid)}))
            
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
                logger.info(f"Requesting mids: {exch}...")
                now = datetime.fromtimestamp(time.time(), tz=timezone.utc)
                perps_data = await exchange.perpetuals_contexts()
                universe_meta, universe_ctx = perps_data[0]["universe"],perps_data[1]

                for meta, context in zip(universe_meta, universe_ctx):
                    if context['midPx'] == None: continue
                    await self.mids.put(("hyperliquid", {"machine_ts": now, "symbol": meta['name'], "mid": Decimal(context['midPx'])}))

            except Exception as e: logger.error(f"collect_mids_hyperliquid: error during data parsing and processing [{exch}]: {e}", exc_info=True)

            await asyncio.sleep(INTERVAL_MIDS_SECONDS)

    """
    Consumes mid data from the queue, batches it, and writes to a Parquet file.
    """
    async def _consume_mids_data(self):
        exchange_buffer = {}
        force_flush = False

        # Define the PyArrow schema
        schema = pa.schema([
            pa.field('machine_ts', pa.timestamp('us', tz='utc')),
            pa.field('symbol', pa.string()),
            pa.field('mid', pa.decimal128(16, 8)), # Precision and scale for decimal type
        ])

        logger.info("Mid consumer is ready and waiting for mid(s)...")
        while True:
            mids_data = await self.mids.get()
            if mids_data is None:
                logger.info("Mid consumer received shutdown signal. FLush buffer before quitting") # The 'None' value is our shutdown signal
                force_flush = True
        
            exch = mids_data[0]
            if exch not in exchange_buffer: exchange_buffer[exch] = []

            mids_buffer = exchange_buffer[exch]
            mids_buffer.append(mids_data[1])

            if not force_flush and len(mids_buffer) < self.batch_mids_size: continue # Check if the buffer is ready to be written

            logger.info(f"Buffer full, writing a batch of {len(mids_buffer)} mid(s) exch:{exch}...")
            file_path = get_parquet_filename(exch, "mid")

            data_to_write = mids_buffer.copy()

            def create_partition(data):
                df = pd.DataFrame(data)
                df['machine_ts'] = pd.to_datetime(df['machine_ts'])
                df['partition_hour'] = df['machine_ts'].dt.hour
                return pa.Table.from_pandas(df)

            asyncio.get_event_loop().run_in_executor(self.executor, append_to_parquetfile, file_path, exch, data_to_write, schema, ['symbol', 'partition_hour'], create_partition)
            mids_buffer.clear()

            if force_flush: break

        self.mids.task_done() # Notify the main loop that the consumer is done

    async def collect_hyperliquid_fr(self, exch):
        await wait_for_next_minute()
        while self.is_recording:
            try:
                logger.info(f"Attempting to fetch new data from: {exch}...")

                exchange = self.gateway.exc_clients[exch]
                perps_data = await exchange.perpetuals_contexts()
                universe_meta, universe_ctx = perps_data[0]["universe"],perps_data[1]

                # datetime, base_currency, symbol, funding_rate, ask, bid, mid, mark, oi, volume_24h, underlying_price, premium, oracle, dayNtlVlm
                results = []
                current_timestamp = time.time()
                now = datetime.fromtimestamp(current_timestamp, tz=timezone.utc)

                for meta, context in zip(universe_meta, universe_ctx):
                    results.append({"datetime": now,  "base_currency": meta['name'], "symbol": meta['name'], "funding_rate": context['funding'], "ask": 0, "bid": 0, "mid": context['midPx'], "mark": context['markPx'], 
                                    "oi": context['openInterest'], "volume_24h": 0, "underlying_price":0, "premium":context['premium'], "oracle": context['oraclePx'], "dayNtlVlm": context['dayNtlVlm']})

                asyncio.get_event_loop().run_in_executor(self.executor, write_to_csv, exch, results, HEADERS, "fr")

            except Exception as e: logger.error(f"Error during data parsing and processing [{exch}]: {e}", exc_info=True)
            finally: 
                logger.info(f"Waiting for {INTERVAL_FUNDINGRATE_SECONDS} seconds before next data fetch [{exch}]...")
                await asyncio.sleep(INTERVAL_FUNDINGRATE_SECONDS)

    async def collect_paradex_fr(self, exch):
        await wait_for_next_minute()
        while self.is_recording:
            try:
                logger.info(f"Attempting to fetch new data from: {exch}...")

                exchange = self.gateway.exc_clients[exch]
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

                    ask, bid = Decimal(market_summary['ask']), Decimal(market_summary['bid'])
                    mid = ask - bid

                    results.append({"datetime": now,  "base_currency": ticker, "symbol": symbol, "funding_rate": market_summary['funding_rate'], "ask": ask, "bid": bid, "mid": mid, "mark": market_summary['mark_price'], 
                                    "oi": market_summary['open_interest'], "volume_24h": market_summary['volume_24h'], "underlying_price": market_summary['underlying_price'], "premium": 0, "oracle": 0, "dayNtlVlm": market_summary['volume_24h']})
                    

                asyncio.get_event_loop().run_in_executor(self.executor, write_to_csv, exch, results, HEADERS, "fr")

            except Exception as e: logger.error(f"Error during data parsing and processing: {e}", exc_info=True)
            finally: 
                logger.info(f"Waiting for {INTERVAL_FUNDINGRATE_SECONDS} seconds before next data fetch...")
                await asyncio.sleep(INTERVAL_FUNDINGRATE_SECONDS)

        return

    

async def main():
    
    try:
        gateway = Gateway(config_keys=get_key()) 
        await gateway.init_clients()

        #recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["hyperliquid","paradex"])
        recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["hyperliquid"], batch_mids_size=4000)
        #recorder = NaiveFundingRateRecorder(gateway=gateway, exchanges=["paradex"])
        await recorder.recording()  
    except Exception as e:
        logger.critical(f"An unhandled critical error occurred: {e}", exc_info=True)
    finally:
        await gateway.cleanup_clients()

async def test():
    filters = [('symbol', '==', "BTC"),('partition_hour', '==', 12)]
    file_name = f"hyperliquid_data/tick/hyperliquid_tick_2025-08-14.parquet"

    try:

        filtered_df = pd.read_parquet(file_name, engine='pyarrow', filters=filters)
        print(filtered_df.head())
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