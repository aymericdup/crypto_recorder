import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Dict
from os import path, listdir

class CoinCodexAPI:
    """
    Free CoinCodex API - No API key required!
    Includes historical market cap data.
    """

    def __init__(self):
        self.base_url = "https://coincodex.com/api/coincodex"
        self.limit_platform = 599
    
    def get_all_coins(self) -> pd.DataFrame:
        """
        Get list of all coins from the frontpage endpoint.
        Uses 1 day history to get current data.
        """
        url = f"{self.base_url}/get_firstpage_history/1/1/10000"
        
        print("Fetching all coins from CoinCodex...")
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        
        # Extract coin symbols
        coins = list(data.keys())
        print(f"Found {len(coins)} coins")
        
        return coins

    def get_coin_history(self, 
                        symbol: str,
                        start_date: str = None,
                        end_date: str = None,
                        samples: int = 1000,
                        sleep_time: float = 0.5) -> pd.DataFrame:
        """
        Get historical data for a single coin.
        
        Args:
            symbol: CoinCodex symbol (e.g., 'BTC', 'ETH')
            start_date: 'YYYY-MM-DD' (default: 5 years ago)
            end_date: 'YYYY-MM-DD' (default: today)
            samples: number of data points (max ~1000 works well)
        
        Returns:
            DataFrame with columns: timestamp, price, volume, market_cap
        """
        # Default dates
        one_day = timedelta(1)
        yesterday = datetime.now() - one_day
        if end_date is None: end_date = yesterday.strftime('%Y-%m-%d')
        if start_date is None: start_date = (yesterday - timedelta(days=365*5)).strftime('%Y-%m-%d')

        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = end_dt - timedelta(self.limit_platform)
        end_bound = datetime.strptime(start_date, '%Y-%m-%d')
        earliest_dt = start_dt - timedelta(365*15)

        data = []

        while start_dt >= end_bound :
            
            start_str, end_str = start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d')
            url = f"{self.base_url}/get_coin_history/{symbol}/{start_str}/{end_str}/{self.limit_platform}"
        
            print(f"Fetching {symbol} from {start_str} to {end_str}...")
            resp = requests.get(url)
            resp.raise_for_status()
            resp_data = resp.json()
        
            # Extract data - response is nested
            if symbol not in resp_data:
                print(f"Warning: {symbol} not found in response")
                break
        
            coin_data = resp_data[symbol]
            # Parse into DataFrame
            # Each entry: [timestamp, price, volume, marketcap]
            # NOTE: API docs say index 2 is volume, but you mentioned it's market cap
            # Let's check both interpretations
        
            df = pd.DataFrame(coin_data, columns=['timestamp', 'price', 'volume', 'marketcap'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            data.append(df)
            
            earliest_dt = df['timestamp'][0]
            if earliest_dt > start_dt: break

            end_dt = earliest_dt - one_day
            start_dt = max(end_dt - timedelta(self.limit_platform), end_bound)
            time.sleep(sleep_time)

            #df = df.set_index('timestamp')
            #df.rename(columns={'value': 'volume_or_mcap'}, inplace=True)
        
        return pd.concat(data)
    
    def get_multiple_coins_history(self,
                                   symbols: List[str],
                                   start_date: str = None,
                                   end_date: str = None,
                                   samples: int = 1000,
                                   sleep_time: float = 0.5) -> Dict[str, pd.DataFrame]:
        """
        Get historical data for multiple coins.
        Includes rate limiting to be polite.
        
        Args:
            symbols: List of coin symbols
            sleep_time: seconds to wait between requests
        
        Returns:
            Dict mapping symbol -> DataFrame
        """
        results = {}
        
        for i, symbol in enumerate(symbols):
            try:
                df = self.get_coin_history(symbol, start_date, end_date, samples, sleep_time)
                if not df.empty: results[symbol] = df
                
                # Rate limiting
                if i < len(symbols) - 1: time.sleep(sleep_time)
                    
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")
                continue
        
        return results
    
def get_coins_from_binance_path(path : str) -> list[str] :
        ''' return the list of coin that a repo of binance data contains '''
        return [file_name.split('_')[0].replace('USDT', '') for file_name in listdir(path)] 
    

def main():

    START_DATE = '2010-01-01'
    END_DATE = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    SAMPLE = 5000
    PATH = PATH = path.join('D:/data/coindex_data', f'marketcap-{datetime.now().strftime('%Y-%m-%d %H%M%S')}.csv')

    api = CoinCodexAPI()

    coins = get_coins_from_binance_path('D:/data/binance_data/candles/1h')
    #coins = ['BTC', 'ASTER']
    #coins = api.get_all_coins()
    if not coins or len(coins) < 1:
        print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: None coin found ! Error !')
        return
    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {len(coins)} coin(s) found!')

    data = api.get_multiple_coins_history(coins, START_DATE, END_DATE, SAMPLE)
    if not data or len(data) < 1:
        print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: None data found ! Error !')
        return
    
    n = len(data)

    if n != len(coins):
        print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: System has found data for only {n} coin(s)/{len(coins)}')

    dfs = []
    for symbol, symbol_df in data.items():
        symbol_df['symbol'] = symbol
        dfs.append(symbol_df)

    print(f'{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: record data into a csv at: {PATH}')
    pd.concat(dfs).to_csv(PATH, sep=";", index=False)

main()
    


    

