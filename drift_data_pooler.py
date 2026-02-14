import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import pytz
import os
import pyarrow
import time

URL = 'https://data.api.drift.trade/'

def get_data(data_type:str, symbol:str, date:datetime, format:str='csv'):
    url = f'{URL}market/{symbol}/{data_type}/{date.year}/{date.month:02d}/{date.day:02d}?format={format}'
    #params = {'format': format}
    #response = requests.get(url, params=params)

    df = pd.DataFrame()
    try: 
        df = pd.read_csv(url)
    except requests.exceptions.RequestException as e: print(f"Error fetching data for {date.strftime('%Y-%m-%d')}: {e}")
    except pd.errors.EmptyDataError: print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: get_data: No data available for {date.strftime('%Y-%m-%d')}")
    finally: return df

def get_bidAskPrice_freq(symbol:str, date:datetime, step:timedelta):
    start = int(date.timestamp())
    end = int((date + step - timedelta(seconds=1)).timestamp())
    return get_bidAskPrice(symbol, start, end)

def get_bidAskPrice(symbol:str, start:int, end:int):
    url = f'{URL}amm/bidAskPrice'
    params = {'marketName': symbol, 'start': start, 'end': end}

    data = pd.DataFrame()
    try:
        response = requests.get(url, params=params)
        data = pd.DataFrame(response.json()['data'], columns=['ts', 'bid', 'ask'])
        #data.rename(columns={'0': 'ts', '1': 'bid', '2': 'ask'}, inplace=True)
    except Exception as e: print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: get_bidAskPrice: Error fetching data for {date.strftime('%Y-%m-%d')}: {e}")
    finally: return data

def get_rate_history(symbol:str, type:str='borrow'):
    url = f'{URL}stats/{symbol}/rateHistory/{type}'
    data = pd.DataFrame()
    try:
        response = requests.get(url)
        data = pd.DataFrame(response.json()['rates'], columns=['ts', 'rate'])
    except Exception as e: print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: get_rate_history: Error fetching data for {symbol}: {e}")
    finally: return data

def get_historical_market_data(symbols:[str], date_range: pd.DatetimeIndex, date_step: timedelta, date_type:str, repo_data:str, sleep_time: int) :

    for symbol in symbols:

        path = os.path.join(repo_data, symbol)
        if not os.path.exists(path): 
            os.makedirs(path)

        for date in date_range:
            print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: [{symbol}]Try to request {date_type} for {date.strftime('%Y-%m-%d')}')
            df =  get_bidAskPrice_freq(symbol, date, date_step) if date_type == 'quotes' else get_data(date_type, symbol, date)

            if df.empty: continue

            df.to_parquet(os.path.join(path, f'{date.strftime('%Y%m%d')}.parquet'), engine='pyarrow')
            
            time.sleep(sleep_time)

script_dir = os.path.dirname(os.path.abspath(__file__))
SYMBOLS = ['SOL-PERP', 'BTC-PERP', 'ETH-PERP']
SLEEP = 2
DATA_TYPE = 'quotes'#'fundingRates'#'trades'
repo = os.path.join(script_dir, 'drift_data', DATA_TYPE)
start, end = '2025-01-01', '2025-11-27'
DATE_RANGE = pd.date_range(start, end , freq='d')
STEP = timedelta(days=1)

# get_historical_market_data(SYMBOLS, DATE_RANGE, STEP, DATA_TYPE, repo, SLEEP)

symbol = 'USDC'
data = get_rate_history(symbol)
data.to_csv(f'{symbol}-rate.csv', sep=";", index=False)




    
