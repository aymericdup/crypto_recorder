from ast import ExceptHandler
import pytz
import sys
import os 
import json
import asyncio
import logging 
import collections
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from enum import Enum

from utils import file_helper

LOG_LEVEL = logging.INFO
LOG_FILE = "funding_rate_poller.log"

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

class FR_MODE(Enum):
    UPDATE = 1
    FILL = 2

def get_key():
    config_keys = {
        # "binance":{
        #     "key":os.getenv("BIN_KEY"),
        #     "secret":os.getenv("BIN_SECRET")
        # },
        "hyperliquid":{
            "key":os.getenv("HPL_KEY"),
            "secret":os.getenv("HPL_SECRET")
        },
        # "paradex":{
        #         "key": os.getenv("PAREDEX_L2"),
        #         "l2_secret": os.getenv("PARADEX_PRIVATE_KEY")
        # }
    }
    return config_keys


class FundingRatePoller():
    def __init__(self, gateway, exchanges):
        self.gateway = gateway
        self.exchanges = exchanges
        self.base_mappings = {exc:{} for exc in exchanges} #binance : BTC : BTCUSDT // hyperliquid : BTC : BTC

    async def hyperliquid_funding_rates(self, exch, start, end, tickers, mode: FR_MODE):

        if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)

        file_helper.ensure_data_directory(exch)
        file_helper.ensure_data_directory(exch, "frs")

        exchange = self.gateway.exc_clients[exch]
        perps_data = await exchange.perpetuals_contexts()
        universe_meta, universe_ctx = perps_data[0]["universe"],perps_data[1]

        def create_partition(df):
                df['date'] = df["datetime"].dt.date
                return pa.Table.from_pandas(df)

        schema = pa.schema([
            pa.field('datetime', pa.timestamp('us', tz='utc')),
            pa.field('fr', pa.decimal128(16, 8)),
            pa.field('premium', pa.decimal128(16, 8)), # Precision and scale for decimal type
        ])

        for meta, context in zip(universe_meta, universe_ctx):

            try:
                ticker = meta['name']

                if len(tickers) > 1 and ticker not in tickers: continue

                file_path = f"{exch}_data/frs/{ticker}.parquet"
                if not os.path.exists(file_path): logger.error(f"hyperliquid_funding_rates: {file_path} does not exist")

                existing_meta  = pd.read_parquet(file_path, engine='pyarrow', columns=["datetime"])
                last_rows = existing_meta.tail(10)
                last_timestamp = last_rows["datetime"].max()

                unix_start, unix_end = int((start.timestamp() if mode == FR_MODE.FILL else (last_timestamp + timedelta(seconds=1)).timestamp()) * 1000), int(end.timestamp() * 1000)

                logging.info(f'hyperliquid_funding_rates: try to retrieve historical funding rate hyperliquid/{ticker}')

                results = []
                attempt = 3;
                while unix_start < unix_end:
                    res = await exchange.perpetuals_funding_historical(
                        ticker=ticker,
                        start=unix_start,
                        end=unix_end,   
                    )
                    if len(res) == 0: 
                        if attempt > 0:
                           attempt = attempt-1
                           continue
                        else: break
                    if res[-1]['time'] == unix_start:
                        break 
                    else:
                        unix_start = res[-1]['time']
                    results.extend(res)
            
                if len(results) < 1: 
                    logging.info(f'hyperliquid_funding_rates: none historical funding rate retrieved for hyperliquid/{ticker}')
                    continue

                df = pd.DataFrame(results)
                df["datetime"] = pd.to_datetime(df["time"],utc=True,unit='ms')
                df = df.set_index("datetime", drop=True).drop(columns=['coin','time'])
                df = df[~df.index.duplicated(keep='first')].rename(columns={'fundingRate':'fr'})
                df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
                df.reset_index(inplace=True)
                #df[start:end].to_csv(f"hl_fr/{ticker}.csv", sep=";")

                file_helper.append_to_parquetfile(file_path, exch, df, schema, ['date'], create_partition, logger)
                #df[start:end].to_parquet(file_path)

                logging.info(f'hyperliquid_funding_rates: historical funding rate retrieved for hyperliquid/{ticker}')
            except Exception as e: logging.error(f'hyperliquid_funding_rates: {ticker}: {e}')

        return

    async def paradex_funding_rates(self, exch, start, end, tickers, mode: FR_MODE):

        if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)
        exchange = self.gateway.exc_clients["paradex"]

        contracts = await exchange.get_markets()
        contracts = contracts['results']

        for contract in contracts:
            if contract['asset_kind'] != 'PERP': continue

            symbol = contract['symbol']
            ticker = contract['base_currency']
            if len(tickers) > 1 and ticker not in tickers: continue

            unix_start, unix_end = int(start.timestamp() * 1000), int(end.timestamp() * 1000)

            logging.info(f'try to retrieve historical funding rate paradex/{ticker}')
            results = []
            attempt = 3;
            next_cursor = None
	
            while unix_start < unix_end:
                parameters_req={"market":symbol, "start_at": unix_start, "end_at": unix_end, "page_size": 5000}
                #parameters_req={"market":symbol}
                if next_cursor is not None: parameters_req["cursor"]=next_cursor
                res = await exchange.http_client.request(
                    **dict(paradex_endpoints['get_funding_data']),
                    params=parameters_req
                )

                if len(res)==0:
                    if attempt > 0:
                        attempt = attempt-1
                        continue
                    else: break
	
                res_data = res["results"]
                next_cursor = res["next"]
                results.extend(res_data)

                if next_cursor is None: break
                
	
            next_cursor = None

            if len(results) < 1: 
                logging.info(f'none historical funding rate retrieved for paradex/{ticker}')
                continue

            df = pd.DataFrame(results)
            df["datetime"] = pd.to_datetime(df["created_at"],utc=True,unit='ms')
            df = df.set_index("datetime", drop=True).drop(columns=['market','created_at', 'funding_index'])
            df = df[~df.index.duplicated(keep='first')].rename(columns={'funding_rate':'fr', 'funding_premium': 'premium'})
            df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
            #df = df.apply(pd.to_numeric)
            df[start:end].to_csv(f"paradex_fr/{ticker}.csv", sep=";")
            logging.info(f'historical funding rate retrieved for paradex/{ticker}')

        return

    async def get_funding_rates(self, exchange, start, end, tickers=[], mode= FR_MODE.FILL):
        
        match exchange:
            case "hyperliquid": await self.hyperliquid_funding_rates(exchange, start, end, tickers, mode)
            case "paradex": await self.paradex_funding_rates(exchange, start, end, tickers, mode)
            case _: logging.info('none funding rates method found for %s',exchange)
        return

async def test(exch, data_type, symbol, date):
    filters = [('date', '==', date)] if date is not None else None
    file_path = f"{exch}_data/{data_type}/{symbol}.parquet"

    try:

        filtered_df = pd.read_parquet(file_path, engine='pyarrow', filters=filters)
        print(filtered_df.head())
        print(filtered_df.tail())
        filtered_df.to_csv(f"{exch}_{data_type}_{symbol}.csv", sep=";")
        logger.info("It is working")
    except Exception as e: 
        print(e)
        logger.error(e)

async def main():

    await test("hyperliquid", "frs", "BTC", None)

    gateway = Gateway(config_keys=get_key()) 
    await gateway.init_clients()

    tickers = []
    # with open('missing assets.txt','r') as f:
    #     tickers = [line.strip() for line in f.readlines() if line != '']

    fr_poller = FundingRatePoller(gateway=gateway, exchanges=["hyperliquid"])
    #fr_poller = FundingRatePoller(gateway=gateway, exchanges=["hyperliquid","paradex"])

    await fr_poller.get_funding_rates("hyperliquid", start=datetime(year=2023, month=1, day=1), end=datetime.now(pytz.utc), tickers=tickers,mode= FR_MODE.UPDATE)
    #await fr_poller.get_funding_rates("hyperliquid", start=datetime(year=2025, month=8, day=1), end=datetime.now())

    #await fr_poller.get_funding_rates("paradex", start=datetime(year=2023, month=9, day=1), end=datetime.now(), tickers=tickers)
    #await fr_poller.get_funding_rates("paradex", start=datetime(year=2023, month=9, day=1), end=datetime.now())

    # market_data = MarketData(
    #     gateway=gateway,
    #     exchanges=["hyperliquid","paradex"],
    #     preference_quote='USDC'
    # )

    # coin = "HYPE-USD-PERP"
    #fr_histo = await gateway.exc_clients["hyperliquid"].get_funding_rates(ticker="LAUNCHCOIN", start=datetime(year=2023, month=9, day=1), end=datetime.now())

    # fr_histo = await gateway.exc_clients["paradex"].get_funding_data(coin)
    # fr_histo.to_csv(f"paradex-{coin}-fr.csv", sep=";")
    #asyncio.create_task(market_data.serve_exchanges())

    await asyncio.sleep(1e9)

    await gateway.cleanup_clients()

if __name__ == "__main__":
    asyncio.run(main())