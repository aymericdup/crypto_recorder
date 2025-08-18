from ast import ExceptHandler
import pytz
import os 
import json
import asyncio
import logging 
import collections
import numpy as np
import pandas as pd
from datetime import datetime


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
        "paradex":{
                "key": os.getenv("PAREDEX_L2"),
                "l2_secret": os.getenv("PARADEX_PRIVATE_KEY")
        }
    }
    return config_keys


class FundingRatePoller():
    def __init__(self, gateway, exchanges):
        self.gateway = gateway
        self.exchanges = exchanges
        self.base_mappings = {exc:{} for exc in exchanges} #binance : BTC : BTCUSDT // hyperliquid : BTC : BTC

    async def hyperliquid_funding_rates(self, start, end, tickers):

        if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)
        exchange = self.gateway.exc_clients["hyperliquid"]
        perps_data = await exchange.perpetuals_contexts()
        universe_meta, universe_ctx = perps_data[0]["universe"],perps_data[1]

        for meta, context in zip(universe_meta, universe_ctx):
            ticker = meta['name']

            if len(tickers) > 1 and ticker not in tickers: continue

            unix_start, unix_end = int(start.timestamp() * 1000), int(end.timestamp() * 1000)

            logging.info(f'try to retrieve historical funding rate hyperliquid/{ticker}')

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
                logging.info(f'none historical funding rate retrieved for hyperliquid/{ticker}')
                continue

            df = pd.DataFrame(results)
            df["datetime"] = pd.to_datetime(df["time"],utc=True,unit='ms')
            df = df.set_index("datetime", drop=True).drop(columns=['coin','time'])
            df = df[~df.index.duplicated(keep='first')].rename(columns={'fundingRate':'fr'})
            df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
            #df = df.apply(pd.to_numeric)
            df[start:end].to_csv(f"hl_fr/{ticker}.csv", sep=";")
            logging.info(f'historical funding rate retrieved for hyperliquid/{ticker}')
        return

    async def paradex_funding_rates(self, start, end, tickers):

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

    async def get_funding_rates(self, exchange, start, end, tickers=[]):
        
        match exchange:
            case "hyperliquid": await self.hyperliquid_funding_rates(start, end, tickers)
            case "paradex": await self.paradex_funding_rates(start, end, tickers)
            case _: logging.info('none funding rates method found for %s',exchange)
        return

async def main():
    gateway = Gateway(config_keys=get_key()) 
    await gateway.init_clients()

    tickers = []
    with open('missing assets.txt','r') as f:
        tickers = [line.strip() for line in f.readlines() if line != '']

    fr_poller = FundingRatePoller(gateway=gateway, exchanges=["hyperliquid","paradex"])
    #await fr_poller.get_funding_rates("hyperliquid", start=datetime(year=2023, month=9, day=1), end=datetime.now(), tickers=tickers)

    #await fr_poller.get_funding_rates("paradex", start=datetime(year=2023, month=9, day=1), end=datetime.now(), tickers=tickers)
    await fr_poller.get_funding_rates("paradex", start=datetime(year=2023, month=9, day=1), end=datetime.now())

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