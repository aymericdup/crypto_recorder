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
from datetime import datetime, timezone, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from enum import Enum
import ccxt
import ccxt.pro
import numpy as np
import httpx

import lighter
from lighter.models.fundings import Fundings
from lighter.rest import ApiException

from dydx import dYdX, dYdXConfig 

from utils import file_helper, number_helper, gateway_helper

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

class HistoricalDataPoller():
    def __init__(self, gateway, ccxt_keys, exchanges):
        self.gateway = gateway
        self.exchanges = exchanges
        self.ccxt_keys = ccxt_keys
        self.base_mappings = {exc:{} for exc in exchanges} #binance : BTC : BTCUSDT // hyperliquid : BTC : BTC

    async def apex_funding_rates(self, exch, start, end, tickers, mode: FR_MODE):
        try:
            if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
            if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)

            file_helper.ensure_data_directory(exch)
            file_helper.ensure_data_directory(exch, "frs")

            exchange = ccxt.pro.apex({'apiKey': self.ccxt_keys[exch]['key'], 'secret': self.ccxt_keys[exch]['secret'], 'enableRateLimit': True, })
            exchange.load_markets()

            markets = await exchange.fetch_markets()
            #markets = [x for x in markets if 'PERP' in x['id']]

            def create_partition(df):
                df['date'] = df["datetime"].dt.date
                return pa.Table.from_pandas(df)

            schema = pa.schema([
                pa.field('datetime', pa.timestamp('us', tz='utc')),
                pa.field('fr', pa.decimal128(16, 8)),
                pa.field('premium', pa.decimal128(16, 8)), # Precision and scale for decimal type
            ])

            nb_iter=0
            times=[]
            base_url = "https://omni.apex.exchange/api/v3/history-funding"

            for market in markets:
                start_time = datetime.now()

                ticker = market['id']
                file_path = f"{exch}_data/frs/{ticker.replace("-USDT", "")}.parquet"
                unix_end = int(end.timestamp() * 1000)
                last_timestamp = int((end + timedelta(seconds=1)).timestamp()) * 1000

                if mode == FR_MODE.UPDATE:
                    if not os.path.exists(file_path): 
                        logger.error(f"apex_funding_rates: {file_path} does not exist")
                        continue

                    existing_meta  = pd.read_parquet(file_path, engine='pyarrow', columns=["datetime"])
                    last_rows = existing_meta.tail(10)
                    last_timestamp = last_rows["datetime"].max()
                    logger.info(f"apex_funding_rates: {ticker} last update found: {last_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                    last_timestamp = int((last_timestamp + timedelta(seconds=1)).timestamp()) * 1000   

                logging.info(f'apex_funding_rates: try to retrieve historical funding rate {exch}/{ticker}')

                results = []
                attempt = 3
                with httpx.Client(timeout=30.0) as client:
                    #while unix_start < unix_end:
                    while attempt > 0:
                        params = { "symbol": market['id'], "endTimeExclusive": unix_end}
                        response = client.get(base_url, params=params)
                        response.raise_for_status()
                        data = response.json()
                        frs = data["data"]["historyFunds"]
                        #frs = await exchange.fetchFundingRateHistory(symbol=market['id'], since=unix_start, params = {"paginate": True})
                        #frs = await exchange.fetchFundingRateHistory(market['info']['crossSymbolName'], unix_start)
                        #frs = await exchange.fetchFundingRateHistory(market['id'], params={"endTime": unix_end})

                        if frs is None or len(frs) == 0: 
                            if attempt > 0:
                               attempt = attempt-1
                               continue
                            else: break
                        #if frs[0]['fundingTimestamp'] == unix_start: break
                        #else: unix_start = frs[0]['timestamp'] + 1

                        unix_end = frs[-1]['fundingTimestamp']
                        #unix_start = frs[-1]['fundingTimestamp']

                        for fr_raw in frs:
                            if mode == FR_MODE.FILL or (mode == FR_MODE.UPDATE and last_timestamp < fr_raw['fundingTimestamp']):
                                results.append({"datetime": fr_raw['fundingTimestamp'], "fr": fr_raw['rate']})
                                continue
                            attempt = -1
                            break
                        #results.extend(frs)
            
                    if len(results) < 1: 
                        logging.info(f'apex_funding_rates: none historical funding rate retrieved for {exch}/{ticker}')
                        continue

                df = pd.DataFrame(results)
                df['premium'] = 0
                df["datetime"] = pd.to_datetime(df["datetime"],utc=True,unit='ms')
                df = df.set_index("datetime", drop=True)
                
                #df = df.set_index("datetime", drop=True).drop(columns=['coin','time'])
                #df = df[~df.index.duplicated(keep='first')].rename(columns={'fundingRate':'fr'})
                df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
                df.reset_index(inplace=True)
                #df[start:end].to_csv(f"hl_fr/{ticker}.csv", sep=";")

                file_helper.append_to_parquetfile(file_path, exch, df, schema, ['date'], create_partition, logger)


                nb_iter = nb_iter+1
                end_time = datetime.now()
                seconds_to_complete = (end_time - start_time).total_seconds()
                times.append(seconds_to_complete)
                iteration = round((nb_iter/len(markets))*100,2)
                iterations_remaining = len(markets) - nb_iter
                average_time_to_complete = np.mean(times)
                estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
                time_remaining = estimated_completion_time - datetime.now()
                logging.info(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

            logging.info(f'apex_funding_rates: END of work')

        except Exception as e: logging.error(f'apex_funding_rates: {e}')

    async def woofipro_funding_rates(self, exch, start, end, tickers, mode: FR_MODE):
        try:
            if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
            if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)

            file_helper.ensure_data_directory(exch)
            file_helper.ensure_data_directory(exch, "frs")

            exchange = ccxt.pro.woofipro({'apiKey': self.ccxt_keys[exch]['key'], 'secret': self.ccxt_keys[exch]['secret'], 'enableRateLimit': True, })
            exchange.load_markets()

            markets = await exchange.fetch_markets()
            markets = [x for x in markets if 'PERP' in x['id']]

            def create_partition(df):
                df['date'] = df["datetime"].dt.date
                return pa.Table.from_pandas(df)

            schema = pa.schema([
                pa.field('datetime', pa.timestamp('us', tz='utc')),
                pa.field('fr', pa.decimal128(16, 8)),
                pa.field('premium', pa.decimal128(16, 8)), # Precision and scale for decimal type
            ])

            nb_iter=0
            times=[]

            for market in markets:
                start_time = datetime.now()

                ticker = market['id']
                file_path = f"{exch}_data/frs/{ticker.replace("PERP_", "").replace("_USDC", "")}.parquet"
                unix_start, unix_end = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
                if mode == FR_MODE.UPDATE:
                    if not os.path.exists(file_path): 
                        logger.error(f"woofipro_funding_rates: {file_path} does not exist")
                        continue

                    existing_meta  = pd.read_parquet(file_path, engine='pyarrow', columns=["datetime"])
                    last_rows = existing_meta.tail(10)
                    last_timestamp = last_rows["datetime"].max()
                    logger.info(f"woofipro_funding_rates: {ticker} last update found: {last_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                    unix_start = int((last_timestamp + timedelta(seconds=1)).timestamp()) * 1000               

                logging.info(f'woofipro_funding_rates: try to retrieve historical funding rate {exch}/{ticker}')

                results = []
                attempt = 3;
                while unix_start < unix_end:
                    #frs = await exchange.fetchFundingRateHistory(symbol=market['id'], since=unix_start, params = {"paginate": True})
                    frs = await exchange.fetchFundingRateHistory(market['id'], unix_start, params = {"paginate": True})

                    if len(frs) == 0: 
                        if attempt > 0:
                           attempt = attempt-1
                           continue
                        else: break
                    if frs[0]['timestamp'] == unix_start: break
                    else: unix_start = frs[0]['timestamp'] + 1

                    for fr_raw in frs:
                        results.append({"datetime": fr_raw['timestamp'], "fr": fr_raw['fundingRate']})
                    #results.extend(frs)
            
                if len(results) < 1: 
                    logging.info(f'woofipro_funding_rates: none historical funding rate retrieved for {exch}/{ticker}')
                    continue

                df = pd.DataFrame(results)
                df['premium'] = 0
                df["datetime"] = pd.to_datetime(df["datetime"],utc=True,unit='ms')
                df = df.set_index("datetime", drop=True)
                
                #df = df.set_index("datetime", drop=True).drop(columns=['coin','time'])
                #df = df[~df.index.duplicated(keep='first')].rename(columns={'fundingRate':'fr'})
                df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
                df.reset_index(inplace=True)
                #df[start:end].to_csv(f"hl_fr/{ticker}.csv", sep=";")

                file_helper.append_to_parquetfile(file_path, exch, df, schema, ['date'], create_partition, logger)


                nb_iter = nb_iter+1
                end_time = datetime.now()
                seconds_to_complete = (end_time - start_time).total_seconds()
                times.append(seconds_to_complete)
                iteration = round((nb_iter/len(markets))*100,2)
                iterations_remaining = len(markets) - nb_iter
                average_time_to_complete = np.mean(times)
                estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
                time_remaining = estimated_completion_time - datetime.now()
                logging.info(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

            logging.info(f'woofipro_funding_rates: END of work')

        except Exception as e: logging.error(f'woofipro_funding_rates: {e}')

    async def lighter_funding_rates(self, exchange, start, end, tickers, mode: FR_MODE):
        if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)

        file_helper.ensure_data_directory(exchange)
        file_helper.ensure_data_directory(exchange, "frs")

        def create_partition(df):
                df['date'] = df["datetime"].dt.date
                return pa.Table.from_pandas(df)

        schema = pa.schema([
            pa.field('datetime', pa.timestamp('us', tz='utc')),
            pa.field('fr', pa.decimal128(16, 8)),
            pa.field('premium', pa.decimal128(16, 8)), # Precision and scale for decimal type
        ])

        max_limit_request_per_min = 60

        nb_iter=0
        times=[]
        resolution = '1h'
        count_back = 1000
        configuration = lighter.Configuration(host = "https://mainnet.zklighter.elliot.ai")
        
        try:
            async with lighter.ApiClient(configuration) as api_client:
                fr_instance = lighter.FundingApi(api_client)
                fr_response = await fr_instance.funding_rates()
                markets = [x for x in fr_response.funding_rates if x.exchange == 'lighter']
                
                request_times = []
                request_times.append(datetime.now())

                cs_api_instance = lighter.CandlestickApi(api_client)

                for market in markets:
                    start_time = datetime.now()
                    ticker = market.symbol
                    file_path = f"{exchange}_data/frs/{ticker}.parquet"

                    last_update_ts, unix_start, unix_end = 0, int(start.timestamp()), int(end.timestamp())
                    if mode == FR_MODE.UPDATE:
                        if os.path.exists(file_path):
                            existing_meta  = pd.read_parquet(file_path, engine='pyarrow', columns=["datetime"])
                            last_rows = existing_meta.tail(10)
                            last_timestamp = last_rows["datetime"].max()
                            logger.info(f"lighter_funding_rates: {ticker} last update found: {last_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                            last_update_ts = int((last_timestamp + timedelta(seconds=1)).timestamp())
                        else: 
                            last_update_ts = unix_start

                    #unix_start, unix_end = int((start.timestamp() if mode == FR_MODE.FILL else (last_timestamp + timedelta(seconds=1)).timestamp()) * 1000), int(end.timestamp() * 1000)
                    
                    logging.info(f'lighter_funding_rates: try to retrieve historical funding rate lighter/{ticker}')

                    results = []
                    attempt = 3
                    while unix_start < unix_end:
                        
                        request_elps_time = (request_times[-1] - request_times[0]).total_seconds()                        
                        if len(request_times)  == max_limit_request_per_min - 1 and request_elps_time < max_limit_request_per_min:
                            sleeping_time = max_limit_request_per_min - int(request_elps_time)
                            logging.info(f'lighter_funding_rates: [{ticker}] max request limit will be soon reached: {len(request_times)} reqs in {request_elps_time} second(s), sleep {sleeping_time} seconds.')                            
                            await asyncio.sleep(sleeping_time)
                            request_times.clear()

                        res = await cs_api_instance.fundings(market.market_id, resolution, unix_start, unix_end, count_back)
                        
                        request_times.append(datetime.now())

                        if len(res.fundings) == 0: 
                            if attempt > 0:
                                attempt = attempt-1
                                continue
                            else: break

                        unix_end = res.fundings[0].timestamp
                        unix_start = int((datetime.fromtimestamp(unix_end) - timedelta(days=30)).timestamp())

                        for fr_raw in res.fundings: 
                            if mode == FR_MODE.FILL or (mode == FR_MODE.UPDATE and last_update_ts <= fr_raw.timestamp):
                                results.append({"datetime": fr_raw.timestamp * 1000, "fr": fr_raw.rate})
                
                    if len(results) < 1: 
                        logging.info(f'lighter_funding_rates: none historical funding rate retrieved for hyperliquid/{ticker}')
                        continue

                    df = pd.DataFrame(results)
                    df['premium'] = 0
                    df["datetime"] = pd.to_datetime(df["datetime"],utc=True,unit='ms')
                    df = df.set_index("datetime", drop=True)
                    df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
                    df['fr'] = df['fr']/100 # remove percentage
                    df.reset_index(inplace=True)

                    file_helper.append_to_parquetfile(file_path, exchange, df, schema, ['date'], create_partition, logger)

                    nb_iter = nb_iter+1
                    end_time = datetime.now()
                    seconds_to_complete = (end_time - start_time).total_seconds()
                    times.append(seconds_to_complete)
                    iteration = round((nb_iter/len(markets))*100,2)
                    iterations_remaining = len(markets) - nb_iter
                    average_time_to_complete = np.mean(times)
                    estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
                    time_remaining = estimated_completion_time - datetime.now()
                    logging.info(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")

        except Exception as e: logging.error(f'lighter_funding_rates: {e}')

        logging.info(f'lighter_funding_rates: END of work')

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

                unix_start, unix_end = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
                if mode == FR_MODE.UPDATE:
                    if not os.path.exists(file_path): 
                        logger.error(f"hyperliquid_funding_rates: {file_path} does not exist")
                        continue

                    existing_meta  = pd.read_parquet(file_path, engine='pyarrow', columns=["datetime"])
                    last_rows = existing_meta.tail(10)
                    last_timestamp = last_rows["datetime"].max()
                    logger.info(f"hyperliquid_funding_rates: {ticker} last update found: {last_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                    unix_start = int((last_timestamp + timedelta(seconds=1)).timestamp()) * 1000

                #unix_start, unix_end = int((start.timestamp() if mode == FR_MODE.FILL else (last_timestamp + timedelta(seconds=1)).timestamp()) * 1000), int(end.timestamp() * 1000)
                

                logging.info(f'hyperliquid_funding_rates: try to retrieve historical funding rate hyperliquid/{ticker}')

                results = []
                attempt = 3
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

        logging.info(f'hyperliquid_funding_rates: END of work')
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
    
    async def dydx_funding_rates(self, exchange, start, end, tickers, mode: FR_MODE):
        if start.tzinfo is None: start = start.replace(tzinfo=pytz.utc)
        if end.tzinfo is None: end = end.replace(tzinfo=pytz.utc)

        file_helper.ensure_data_directory(exchange)
        file_helper.ensure_data_directory(exchange, "frs")

        def create_partition(df):
                df['date'] = df["datetime"].dt.date
                return pa.Table.from_pandas(df)

        schema = pa.schema([
            pa.field('datetime', pa.timestamp('us', tz='utc')),
            pa.field('fr', pa.decimal128(16, 8)),
            pa.field('premium', pa.decimal128(16, 8)), # Precision and scale for decimal type
        ])

        client = dYdX(testnet=False)
        max_limit_request_per_min = 60

        nb_iter=0
        times=[]
        
        try:

            logger.info(f"dydx_funding_rates: fetching perpetual markets..")
            markets = client.get_perpetual_markets()
            request_times = []
            request_times.append(datetime.now())

            for ticker in markets.keys():

                if ',' in ticker: continue

                start_time = datetime.now()
                file_path = f"{exchange}_data/frs/{ticker.replace("-USD", "")}.parquet"

                starting_bound = start
                if mode == FR_MODE.UPDATE:
                    if not os.path.exists(file_path): 
                        logger.error(f"dydx_funding_rates: {file_path} does not exist")
                        continue

                    existing_meta  = pd.read_parquet(file_path, engine='pyarrow', columns=["datetime"])
                    last_rows = existing_meta.tail(10)
                    starting_bound = last_rows["datetime"].max()
                    logger.info(f"dydx_funding_rates: {ticker} last update found: {starting_bound.strftime('%Y-%m-%d %H:%M:%S')}")
                
                logging.info(f'dydx_funding_rates: try to retrieve historical funding rate dYdX/{ticker}')

                results = []
                attempt = 3
                before = end
                while True:
                    
                    request_elps_time = (request_times[-1] - request_times[0]).total_seconds()                        
                    if len(request_times)  == max_limit_request_per_min - 1 and request_elps_time < max_limit_request_per_min:
                        sleeping_time = max_limit_request_per_min - int(request_elps_time)
                        logging.info(f'dydx_funding_rates: [{ticker}] max request limit will be soon reached: {len(request_times)} reqs in {request_elps_time} second(s), sleep {sleeping_time} seconds.')                            
                        await asyncio.sleep(sleeping_time)
                        request_times.clear()

                    res = client.get_historical_funding_rates(ticker, effective_before_or_at=before, limit=1000)
                    
                    request_times.append(datetime.now())

                    if len(res) == 0: 
                        if attempt > 0:
                            attempt = attempt-1
                            continue
                        else: break

                    response_earliest_ts, response_latest_ts = datetime.fromisoformat(res[-1].effectiveAt), datetime.fromisoformat(res[0].effectiveAt)
                    if response_earliest_ts >= before or response_latest_ts <= starting_bound: break
                    before = response_earliest_ts

                    for fr_raw in res: 
                        raw_ts = datetime.fromisoformat(fr_raw.effectiveAt)
                        if mode == FR_MODE.FILL or (mode == FR_MODE.UPDATE and starting_bound < raw_ts):
                            results.append({"datetime": raw_ts, "fr": fr_raw.rate})
                            
            
                if len(results) < 1: 
                    logging.info(f'dydx_funding_rates: none historical funding rate retrieved for dYdX/{ticker}')
                    continue

                df = pd.DataFrame(results)
                df['premium'] = 0
                df["datetime"] = pd.to_datetime(df["datetime"],utc=True,unit='ms')
                df = df.set_index("datetime", drop=True)
                df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
                df['fr'] = df['fr']/100 # remove percentage
                df.reset_index(inplace=True)

                file_helper.append_to_parquetfile(file_path, exchange, df, schema, ['date'], create_partition, logger)

                nb_iter = nb_iter+1
                end_time = datetime.now()
                seconds_to_complete = (end_time - start_time).total_seconds()
                times.append(seconds_to_complete)
                iteration = round((nb_iter/len(markets))*100,2)
                iterations_remaining = len(markets) - nb_iter
                average_time_to_complete = np.mean(times)
                estimated_completion_time = (datetime.now() + timedelta(seconds = int(average_time_to_complete*iterations_remaining)))
                time_remaining = estimated_completion_time - datetime.now()
                logging.info(f"{iteration}% complete, {time_remaining} left, ETA: {estimated_completion_time}")                  

        except Exception as e: logging.error(f'dydx_funding_rates: {e}')

        logging.info(f'dydx_funding_rates: END of work')
    
    async def get_historical_data(self, exchange_jobs, start, end, tickers=[], mode= FR_MODE.FILL):
        
        for exchange, jobs in exchange_jobs.items():
            for job in jobs:
                await job(self, exchange, start, end, tickers, mode)
        '''
        match exchange:
            case "hyperliquid": await self.hyperliquid_funding_rates(exchange, start, end, tickers, mode)
            case "paradex": await self.paradex_funding_rates(exchange, start, end, tickers, mode)
            case "woofipro": await self.woofipro_funding_rates(exchange, start, end, tickers, mode)
            case "apex": await self.apex_funding_rates(exchange, start, end, tickers, mode)
            case "lighter": await self.lighter_funding_rates(exchange, start, end, tickers, mode)
            case "dydx": await self.dydx_funding_rates(exchange, start, end, tickers, mode)
            case _: logging.info('none funding rates method found for %s',exchange)
        '''
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
    # await test("hyperliquid", "frs", "BTC", "2025-09-04")
    # await test("apex", "frs", "BTC-USDT", "2025-09-04")
    #await test("woofipro", "frs", "PERP_BTC_USDC", "2025-09-04")
    # await test("hyperliquid", "frs", "BIO", None)
    # await test("woofipro", "frs", "PERP_BIO_USDC", None)

    gateway = None
    if len(gateway_helper.get_gateway_keys()) > 0:
        gateway = Gateway(config_keys=gateway_helper.get_gateway_keys())
        await gateway.init_clients()

    tickers = []
    # with open('missing assets.txt','r') as f:
    #     tickers = [line.strip() for line in f.readlines() if line != '']

    fr_poller = HistoricalDataPoller(gateway=gateway, exchanges=['hyperliquid'], ccxt_keys=gateway_helper.get_ccxt_keys())

    # dYdX
    #await fr_poller.get_funding_rates("dydx", start=datetime(year=2023, month=1, day=1,tzinfo=timezone.utc), end=datetime.now(tz=timezone.utc), tickers=tickers,mode= FR_MODE.FILL)
    #await fr_poller.get_funding_rates("dydx", start=datetime(year=2023, month=1, day=1,tzinfo=timezone.utc), end=datetime.now(tz=timezone.utc), tickers=tickers,mode= FR_MODE.UPDATE)

    #apex
    #await fr_poller.get_funding_rates("apex", start=datetime(year=2023, month=1, day=1), end=datetime.now(pytz.utc), tickers=tickers,mode= FR_MODE.UPDATE)
    #await fr_poller.get_funding_rates("apex", start=datetime(year=2023, month=1, day=1), end=datetime.now(pytz.utc), tickers=tickers,mode= FR_MODE.FILL)

    #woofipro
    #await fr_poller.get_funding_rates("woofipro", start=datetime(year=2023, month=1, day=1), end=datetime.now(pytz.utc), tickers=tickers,mode= FR_MODE.FILL)
    #await fr_poller.get_funding_rates("woofipro", start=datetime(year=2023, month=1, day=1), end=datetime.now(pytz.utc), tickers=tickers,mode= FR_MODE.UPDATE)

    #hyperliquid
    #await fr_poller.get_funding_rates("hyperliquid", start=datetime(year=2023, month=1, day=1), end=datetime.now(pytz.utc), tickers=tickers,mode= FR_MODE.UPDATE)
    #await fr_poller.get_funding_rates("hyperliquid", start=datetime(year=2025, month=8, day=1), end=datetime.now())

    #lighter
    #await fr_poller.get_funding_rates("lighter", start=datetime(year=2025, month=8, day=1), end=datetime.now(), tickers=tickers,mode= FR_MODE.FILL)
    await fr_poller.get_historical_data(exchange_jobs={"lighter": [HistoricalDataPoller.lighter_funding_rates]}, start=datetime(year=2025, month=8, day=1), end=datetime.now(), tickers=tickers,mode= FR_MODE.UPDATE)

    #paradex
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