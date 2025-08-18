import time
import json
import requests
import pandas as pd
import pyarrow.parquet as pq
from websocket import create_connection, WebSocketConnectionClosedException
import threading
import os
import datetime
import httpx
import logging # Import the logging module
from os import getenv
from dotenv import load_dotenv
import ccxt

# --- Configuration Section ---
# IMPORTANT: These API URLs and parsing logic are PLACEHOLDERS.
# You MUST replace them with the actual API documentation details for Paradex, Hyperliquid, and Lighter.
# Also, verify the 'tick_channel_prefix' and 'tick_subscription_template' based on how each exchange's
# WebSocket API expects subscriptions for multiple symbols.
EXCHANGE_APIS = {

    "paradex": {
        "hasREST": "False"
        "hasWB": "False",
        "websocket_url": "wss://ws.api.testnet.paradex.trade/v1", # Example: Find actual WS URL
        "rest_url": "https://api.prod.paradex.trade/v1/markets", # Placeholder! Check actual endpoint for all markets/perps
        "headers" : { 'Accept': 'application/json' },
        "rest_funding_url": "https://api.paradex.trade/v1/funding-rates", # Example: Find actual REST URL
        # How to subscribe to trades for a specific symbol. E.g., 'trades.BTC-USDC'
        "tick_channel_prefix": "trades.",
        # Template for WebSocket subscription message. 'params' will be dynamically populated.
        # This assumes you send one message with a list of all channels. If individual messages
        # are needed per symbol, this logic will need adjustment.
        "tick_subscription_template": {
            "method": "subscribe",
            "params": [] # This list will be filled with 'trades.SYMBOL' dynamically
        },
        # Example for parsing markets API response. Customize this!
        "perps_parser": lambda data: [m['symbol'] for m in data if m.get('asset_kind') == 'PERPETUAL'],
        # Example for parsing funding rates. Customize this!
        "funding_parser": lambda data, symbol: next(
            (float(item['fundingRate']) for item in data if item.get('symbol') == symbol), None
        )
    },

    # "hyperliquid": {
    #     "hasWB": "True",
    #     "websocket_url": "wss://api.hyperliquid.xyz/ws", # Example: Find actual WS URL
    #     "rest_url": "https://api.hyperliquid.xyz/info", # Placeholder! Check actual endpoint
    #     "payload_perps_universe": {"type": "metaAndAssetCtxs"},
    #     #"payload_perps_universe": {"type": "meta"},
    #     "rest_funding_url": "https://api.hyperliquid.trade/funding", # Example: Find actual REST URL
    #     "tick_channel_prefix": "", # Hyperliquid might just send 'ETH-USD' as symbol in trade data
    #     "tick_subscription_template": {
    #         "method": "subscribe",
    #         "subscription": [{"type": "trades", "coin": s} for s in []] # This needs to be dynamically built
    #     },
    #     "perps_parser": lambda data: [c['name'] for c in data.get('universe', []) if not c.get('isDelisted')],
    #     "funding_parser": lambda data, funding_data: [{ "coin": x[1]['name'], "funding": float(funding_data[x[0]].get('funding')), "OI": float(funding_data[x[0]].get('openInterest')), "oracle": float(funding_data[x[0]].get('oraclePx')), "premium": float(funding_data[x[0]].get('premium'))} for x in data if not x[1].get('isDelisted')],
    # },
 
    # "lighter": {
    #     "hasWB": "True",
    #     "websocket_url": "wss://api.lighter.xyz/ws", # Example: Find actual WS URL
    #     "rest_url": "https://api.lighter.xyz/markets", # Placeholder! Check actual endpoint
    #     "rest_funding_url": "https://api.lighter.xyz/funding", # Example: Find actual REST URL
    #     "tick_channel_prefix": "", # Lighter might just send 'SOL-USDT' as symbol in trade data
    #     "tick_subscription_template": {
    #         "event": "subscribe",
    #         "channel": "market_trades",
    #         "symbols": [] # This list will be filled with actual symbols dynamically
    #     },
    #     # Example for parsing markets. Customize this!
    #     "market_parser": lambda data: [m['pair'] for m in data if m.get('type') == 'PERPETUAL'],
    #     # Example for parsing funding rates. Customize this!
    #     "funding_parser": lambda data, symbol: next(
    #         (float(item['rate']) for item in data.get('fundingRates', []) if item.get('pair') == symbol), None
    #     )
    # }

}

# DATA_DIRECTORY = 'collected_crypto_data'
# LOG_FILE_NAME = 'crypto_collector.log' # Name of the log file
# FLUSH_INTERVAL_SECONDS = 60  # Write buffered data to Parquet every 5 minutes (300 seconds)
# FUNDING_RATE_FETCH_INTERVAL_SECONDS = 3600  # Fetch funding rates every hour (3600 seconds)
# MARKET_FETCH_INTERVAL_SECONDS = 18000 # Fetch available markets every 5 hours, in case new perps are listed

load_dotenv()
DATA_DIRECTORY = getenv("DATA_DIRECTORY")
LOG_FILE_NAME = getenv("LOG_FILE_NAME")
FLUSH_INTERVAL_SECONDS = float(getenv("FLUSH_INTERVAL_SECONDS"))
FUNDING_RATE_FETCH_INTERVAL_SECONDS = float(getenv("FUNDING_RATE_FETCH_INTERVAL_SECONDS"))
MARKET_FETCH_INTERVAL_SECONDS = float(getenv("MARKET_FETCH_INTERVAL_SECONDS"))
HISTORICAL_BATCH_SIZE = 1000 # Number of trades to fetch per historical request
HISTORICAL_COLLECTION_INTERVAL_SECONDS = 60 # How often to attempt historical collection per symbol

# Ensure the data directory exists
os.makedirs(DATA_DIRECTORY, exist_ok=True)


# --- Exchange Data Collector Class ---
class ExchangeCollector:
    """
    Collects tick data via WebSocket and funding rates via REST API for a specific exchange,
    for all available perpetuals, and saves them periodically to Parquet files.
    """

    def __init__(self, exchange_name, api_config):

        self.exchange_name = exchange_name
        self.api_config = api_config
        self.ws = None
        self.running = True  # Flag to control the collection loops
        self.logger = logging.getLogger(f'collector.{exchange_name}') # Get a specific logger for this collector

        self.available_perps = [] # List to store all perpetual symbols (e.g., ['BTC-USDC', 'ETH-USDC'])

        self.tick_buffer = []  # Buffer for tick data
        self.funding_buffer = []  # Buffer for funding rate data
        self.tick_buffer_lock = threading.Lock()  # Lock for thread-safe access to tick_buffer
        self.funding_buffer_lock = threading.Lock()  # Lock for thread-safe access to funding_buffer

    def _fetch_available_perps(self):
        """
        Fetches the list of all available perpetual futures symbols from the exchange's REST API.
        This method MUST be customized for each exchange's market API response structure.
        """
        self.logger.info(f"INFO: Fetching available perpetuals for {self.exchange_name} from {self.api_config['rest_url']}...")
        try:

            #response = requests.get(self.api_config['rest_url'])
            #response.raise_for_status()
            #data = response.json()

            response = httpx.post(self.api_config['rest_url'], 
                                  json=self.api_config['payload_perps_universe'] if "payload_perps_universe" in self.api_config else None, 
                                  headers= self.api_config['headers'] if 'headers' in self.api_config else None)
            response.raise_for_status()
            data = response.json()

            # --- CUSTOM MARKET PARSING LOGIC FOR EACH EXCHANGE GOES HERE ---
            # Use the lambda function defined in EXCHANGE_APIS for parsing
            parsed_symbols = self.api_config['perps_parser'](data[0])
            # --- END CUSTOM MARKET PARSING ---

            perps_candidates_subscription = []

            if parsed_symbols:

                set_parsed_symbols = set(parsed_symbols)
                perps_candidates_subscription = list(set_parsed_symbols ^ set(self.available_perps) if self.available_perps else set_parsed_symbols)
                self.available_perps = list(set_parsed_symbols) # Use set to remove duplicates
                self.logger.info(f"INFO: Found {len(self.available_perps)} perpetuals for {self.exchange_name}.")

            else:
                self.logger.warning(f"WARNING: No perpetual symbols found for {self.exchange_name} or parsing failed.")
                return False

            if not perps_candidates_subscription:
                self.logger.info(f"INFO: No new perpetual found for {self.exchange_name} or parsing failed.")
                return True

            # Dynamically build subscription messages for all available perpetuals
            # This logic needs to be adapted based on the exchange's specific WebSocket API.
            # Some APIs might accept a single message for multiple symbols, others require one per symbol.

            if self.exchange_name == "paradex":
                # Paradex example: "params": ["trades.BTC-USDC", "trades.ETH-USDC"]
                channels = [self.api_config['tick_channel_prefix'] + s for s in perps_candidates_subscription]
                subscribe_msg = self.api_config['tick_subscription_template'].copy()
                subscribe_msg['params'] = channels
                self.ws.send(json.dumps(subscribe_msg))
            elif self.exchange_name == "hyperliquid":
                # Hyperliquid example: send a list of objects like {"type": "trades", "coin": "ETH-USD"}
                subscribe_msgs = [{"type": "trades", "coin": s} for s in perps_candidates_subscription]
                # Hyperliquid might require sending individual subscribe messages for each symbol
                # Or it might have a batch subscribe. Assuming individual for now:
                for msg in subscribe_msgs:
                    template = self.api_config['tick_subscription_template'].copy() # Need to copy for structure
                    template['subscription'] = msg # Wrap the specific message in a list for params
                    self.ws.send(json.dumps(template))
                    # print(f"DEBUG: Hyperliquid WS subscription sent for {msg['coin']}") # Uncomment for debugging
            elif self.exchange_name == "lighter":
                # Lighter example: "symbols": ["SOL-USDT", "LINK-USDT"]
                subscribe_msg = self.api_config['tick_subscription_template'].copy()
                subscribe_msg['symbols'] = perps_candidates_subscription
                self.ws.send(json.dumps(subscribe_msg))

            self.logger.info(f"INFO: Subscribed to tick data for all {len(perps_candidates_subscription)} perpetuals on {self.exchange_name}")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"ERROR: HTTP request failed for {self.exchange_name} markets: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"ERROR: Failed to decode JSON for {self.exchange_name} markets: {e}")
        except Exception as e:
            self.logger.error(f"ERROR: An unexpected error occurred while fetching {self.exchange_name} markets: {e}")
        return False

    def _connect_websocket(self):
        self.logger.info(f"INFO: Connecting to {self.exchange_name} WebSocket at {self.api_config['websocket_url']}...")
        try:
            self.ws = create_connection(self.api_config['websocket_url'])
            return True
        except Exception as e:
            self.logger.error(f"ERROR: Failed to connect to {self.exchange_name} WebSocket: {e}")
            self.ws = None
            return False

    # def _connect_websocket(self):
    #     """Establishes a WebSocket connection and subscribes to all available perpetuals."""
    #     if not self.available_perps:
    #         print(f"WARNING: No perpetual symbols loaded for {self.exchange_name}. Cannot connect WebSocket.")
    #         return False

    #     print(f"INFO: Connecting to {self.exchange_name} WebSocket at {self.api_config['websocket_url']}...")
    #     try:
    #         self.ws = create_connection(self.api_config['websocket_url'])

    #         # Dynamically build subscription messages for all available perpetuals
    #         # This logic needs to be adapted based on the exchange's specific WebSocket API.
    #         # Some APIs might accept a single message for multiple symbols, others require one per symbol.

    #         if self.exchange_name == "paradex":
    #             # Paradex example: "params": ["trades.BTC-USDC", "trades.ETH-USDC"]
    #             channels = [self.api_config['tick_channel_prefix'] + s for s in self.available_perps]
    #             subscribe_msg = self.api_config['tick_subscription_template'].copy()
    #             subscribe_msg['params'] = channels
    #             self.ws.send(json.dumps(subscribe_msg))
    #         elif self.exchange_name == "hyperliquid":
    #             # Hyperliquid example: send a list of objects like {"type": "trades", "coin": "ETH-USD"}
    #             subscribe_msgs = [{"type": "trades", "coin": s} for s in self.available_perps]
    #             # Hyperliquid might require sending individual subscribe messages for each symbol
    #             # Or it might have a batch subscribe. Assuming individual for now:
    #             for msg in subscribe_msgs:
    #                 template = self.api_config['tick_subscription_template'].copy() # Need to copy for structure
    #                 template['params'] = [msg] # Wrap the specific message in a list for params
    #                 self.ws.send(json.dumps(template))
    #                 # print(f"DEBUG: Hyperliquid WS subscription sent for {msg['coin']}") # Uncomment for debugging
    #         elif self.exchange_name == "lighter":
    #             # Lighter example: "symbols": ["SOL-USDT", "LINK-USDT"]
    #             subscribe_msg = self.api_config['tick_subscription_template'].copy()
    #             subscribe_msg['symbols'] = self.available_perps
    #             self.ws.send(json.dumps(subscribe_msg))

    #         print(f"INFO: Subscribed to tick data for all {len(self.available_perps)} perpetuals on {self.exchange_name}")
    #         return True
    #     except Exception as e:
    #         print(f"ERROR: Failed to connect or subscribe to {self.exchange_name} WebSocket: {e}")
    #         self.ws = None
    #         return False

    def _parse_tick_data(self, message):
        """
        Parses raw WebSocket message into structured tick data.
        Returns a dictionary {timestamp, symbol, price, volume} or None if parsing fails.
        """
        try:
            data = json.loads(message)
            # --- CUSTOM PARSING LOGIC FOR EACH EXCHANGE GOES HERE ---
            # This should be similar to previous version, but ensure it handles all symbols
            # and extracts the correct symbol from the incoming message.

            if self.exchange_name == "paradex":
                if data.get('type') == 'trade' and 'data' in data:
                    trade_data = data['data']
                    timestamp = trade_data.get('timestamp') / 1000 if trade_data.get('timestamp') else time.time()
                    symbol = trade_data.get('symbol')
                    # Ensure symbol is one we are tracking
                    if symbol and symbol in self.available_perps:
                        return {
                            'timestamp': timestamp,
                            'exchange': self.exchange_name,
                            'symbol': symbol,
                            'price': float(trade_data.get('price')),
                            'volume': float(trade_data.get('size'))
                        }
            elif self.exchange_name == "hyperliquid":
                if data.get('channel') == 'trades' and data.get('data'):
                    for trade in data['data']: # Hyperliquid might send multiple trades
                        timestamp = trade.get('time') # Get the raw timestamp
                        # *** FIX: Assume Hyperliquid 'time' is in milliseconds and convert to seconds ***
                        if timestamp is not None:
                            # Check if it's a large number, typical for millisecond Unix timestamps
                            if timestamp > 1_000_000_000_000: # Example: 1678886400000 ms
                                timestamp /= 1000.0 # Convert milliseconds to seconds
                        else:
                            timestamp = time.time() # Fallback if no timestamp provided
                        symbol = trade.get('coin')
                        if symbol and symbol in self.available_perps:
                            return {
                                'timestamp': timestamp,
                                'exchange': self.exchange_name,
                                'symbol': symbol,
                                'price': float(trade.get('px')),
                                'volume': float(trade.get('sz')),
                                'side': trade.get('side') # B = Bid = Buy, A = Ask = Short. Side is aggressing side for trades.
                            }
            elif self.exchange_name == "lighter":
                if data.get('event') == 'tick' and 'payload' in data:
                    tick_data = data['payload']
                    timestamp = tick_data.get('ts') if tick_data.get('ts') else time.time()
                    symbol = tick_data.get('pair')
                    if symbol and symbol in self.available_perps:
                        return {
                            'timestamp': timestamp,
                            'exchange': self.exchange_name,
                            'symbol': symbol,
                            'price': float(tick_data.get('lastPrice')),
                            'volume': float(tick_data.get('volume'))
                        }
            # --- END CUSTOM PARSING ---
            return None
        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
            # print(f"WARNING: Error parsing {self.exchange_name} tick message: {e}. Message part: {message[:100]}...")
            return None

    def collect_ticks(self):
        """Continuously collects tick data from the WebSocket and buffers it."""
        while self.running:
            # Ensure perpetuals are loaded and WS connected
            if not self.available_perps:
                self.logger.warning(f"WARNING: {self.exchange_name} perpetual symbols not loaded. Retrying in 5s...")
                time.sleep(5)
                continue

            if not self.ws or not self.ws.connected:
                if not self._connect_websocket():
                    time.sleep(5)
                    continue
            try:
                message = self.ws.recv()
                tick_data = self._parse_tick_data(message)
                if tick_data:
                    with self.tick_buffer_lock:
                        self.tick_buffer.append(tick_data)
            except WebSocketConnectionClosedException:
                self.logger.warning(f"WARNING: {self.exchange_name} WebSocket connection closed. Attempting to reconnect...")
                self.ws = None
            except Exception as e:
                self.logger.error(f"ERROR: General error during {self.exchange_name} WebSocket collection: {e}")
                time.sleep(1)

    def fetch_funding_rates(self):
        """Periodically fetches funding rates via REST API for all available perpetuals."""
        while self.running:
            if not self.available_perps:
                self.logger.warning(f"WARNING: {self.exchange_name} perpetual symbols not loaded for funding rates. Retrying in 5s...")
                time.sleep(5)
                continue

            try:
                current_timestamp = time.time()
                # Most exchanges provide all funding rates in one go, but check their API docs.
                # If they require individual calls per symbol, you'll need to loop over self.available_perps
                # and make separate requests (with appropriate delays).
                
                # Assuming a single call for all funding rates or iterating over symbols
                # if the API requires symbol in URL or payload.
                # For simplicity, we make a single request to the generic funding endpoint first.
                #all_funding_data = None
                #response = requests.get(self.api_config['rest_funding_url'])
                #response.raise_for_status()
                #all_funding_data = response.json()

                response = httpx.post(self.api_config['rest_url'], json=self.api_config['payload_perps_universe'])
                response.raise_for_status()
                data = response.json()

                funding_rows = self.api_config['funding_parser'](list(enumerate(data[0].get('universe'))), data[1])
                for funding_row in funding_rows:
                    if funding_row is not None:
                        fr_record = {
                            'timestamp': current_timestamp,
                            'exchange': self.exchange_name,
                            'symbol': funding_row['coin'],
                            'funding_rate': funding_row["funding"],
                            'OI': funding_row["OI"],
                            'premium': funding_row["premium"],
                            'oracle': funding_row["oracle"],
                        }
                        with self.funding_buffer_lock:
                            self.funding_buffer.append(fr_record)
                            # print(f"[{self.exchange_name}] Funding Rate: {symbol} = {funding_rate:.6f}") # Verbose logging

                # for symbol in self.available_perps:
                #     funding_info = self.api_config['funding_parser'](enumerate(data[0].get('universe')), data[1])
                    
                #     if funding_info is not None:
                #         fr_record = {
                #             'timestamp': current_timestamp,
                #             'exchange': self.exchange_name,
                #             'symbol': symbol,
                #             'funding_rate': funding_info["funding"],
                #             'OI': funding_info["OI"],
                #             'premium': funding_info["premium"],
                #             'oracle': funding_info["oracle"],
                #         }
                #         with self.funding_buffer_lock:
                #             self.funding_buffer.append(fr_record)
                #             # print(f"[{self.exchange_name}] Funding Rate: {symbol} = {funding_rate:.6f}") # Verbose logging
            except requests.exceptions.RequestException as e:
                self.logger.error(f"ERROR: HTTP request failed for {self.exchange_name} funding rates: {e}")
            except json.JSONDecodeError as e:
                self.logger.error(f"ERROR: Failed to decode JSON for {self.exchange_name} funding rates: {e}")
            except Exception as e:
                self.logger.error(f"ERROR: An unexpected error occurred while fetching {self.exchange_name} funding rates: {e}")

            time.sleep(FUNDING_RATE_FETCH_INTERVAL_SECONDS)

    def _flush_data_to_parquet(self, is_final=False):
        """
        Periodically writes buffered data to Parquet files.
        This method runs in a separate thread.
        """
        while self.running:
            time.sleep(FLUSH_INTERVAL_SECONDS)
            now_utc = datetime.datetime.now(datetime.timezone.utc) # Get current UTC datetime
            timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S") # For filename unique identifier
            if is_final: timestamp_str += "_final"

            # Flush Tick Data
            ticks_to_write = []
            with self.tick_buffer_lock:
                if self.tick_buffer:
                    ticks_to_write = self.tick_buffer
                    self.tick_buffer = []
            
            if ticks_to_write:
                try:
                    df_ticks = pd.DataFrame(ticks_to_write)
                    df_ticks['datetime'] = pd.to_datetime(df_ticks['timestamp'], unit='s', utc=True)
                    df_ticks = df_ticks[['datetime', 'timestamp', 'exchange', 'symbol', 'price', 'volume']]
                    
                    file_path = os.path.join(DATA_DIRECTORY, f"ticks_{self.exchange_name}_{timestamp_str}_UTC.parquet")
                    df_ticks.to_parquet(file_path, index=False)
                    self.logger.info(f"INFO: Flushed {len(ticks_to_write)} ticks from {self.exchange_name} to {file_path}")
                except Exception as e:
                    self.logger.error(f"ERROR: Failed to write ticks from {self.exchange_name} to Parquet: {e}")

            # Flush Funding Rate Data
            funding_rates_to_write = []
            with self.funding_buffer_lock:
                if self.funding_buffer:
                    funding_rates_to_write = self.funding_buffer
                    self.funding_buffer = []
            
            if funding_rates_to_write:
                try:
                    df_funding = pd.DataFrame(funding_rates_to_write)
                    df_funding['datetime'] = pd.to_datetime(df_funding['timestamp'], unit='s', utc=True)
                    df_funding = df_funding[['datetime', 'timestamp', 'exchange', 'symbol', 'funding_rate']]
                    
                    file_path = os.path.join(DATA_DIRECTORY, f"funding_{self.exchange_name}_{timestamp_str}_UTC.parquet")
                    df_funding.to_parquet(file_path, index=False)
                    self.logger.info(f"INFO: Flushed {len(funding_rates_to_write)} funding rates from {self.exchange_name} to {file_path}")
                except Exception as e:
                    self.logger.error(f"ERROR: Failed to write funding rates from {self.exchange_name} to Parquet: {e}")

    def _market_fetch_loop(self):
        """Periodically fetches available perpetuals in case new ones are listed."""
        while self.running:
            self._fetch_available_perps()
            time.sleep(MARKET_FETCH_INTERVAL_SECONDS)


    def stop(self):
        """Stops the data collection loops and closes WebSocket connection."""
        self.running = False
        if self.ws:
            self.ws.close()
            self.logger.info(f"INFO: {self.exchange_name} WebSocket closed.")
        self._final_flush()


# --- New Historical Collector Class ---
class HistoricalCollector:
    """
    Collects historical tick data via REST API for a specific exchange and symbol,
    deduplicates, and adds to the main collector's buffer for flushing.
    """
    def __init__(self, exchange, exchange_name, api_config, main_collector_tick_buffer, main_collector_tick_lock):
        self.exchange_name = exchange_name
        self.api_config = api_config
        self.main_collector_tick_buffer = main_collector_tick_buffer
        self.main_collector_tick_lock = main_collector_tick_lock
        self.logger = logging.getLogger(f'historical_collector.{exchange_name}')
        self.running = True

        # Persistent set for deduplication of historical trades
        # In a real system, this would be backed by a database or a persistent file for long-term state
        self.historical_seen_trade_ids = set()
        # You might also want to store the latest timestamp fetched for each symbol
        self.last_fetched_timestamp = {} # {'SYMBOL': last_ts_in_seconds}
        self.exchange = exchange


    def _fetch_trades_from_api(self, symbol: str, limit: int, before_timestamp: float = None):
        """
        Makes the actual REST API call to fetch historical trades.
        This method needs to be highly customized per exchange.
        Returns a list of raw trade dictionaries.
        """
        try:
            data = self.exchange.fetch_trades(symbol, limit)

        # url = self.api_config['rest_historical_trades_url']
        # params = {}
        # payload = {}
        # is_post_request = False

        # if self.exchange_name == "paradex":
        #     params = {'market': symbol, 'limit': limit}
        #     if before_timestamp:
        #         params['before'] = int(before_timestamp * 1000) # Paradex 'before' is in milliseconds
        #     request_func = requests.get
        # elif self.exchange_name == "hyperliquid":
        #     # Hyperliquid uses POST /info for historical trades with a specific payload
        #     is_post_request = True
        #     request_func = requests.post
        #     url = self.api_config['rest_historical_trades_url'] # Often the same as info URL
        #     payload_builder = self.api_config.get('historical_trades_post_payload')
        #     if payload_builder:
        #         payload = payload_builder(symbol, limit, before_timestamp if before_timestamp else time.time())
        #     else:
        #         self.logger.error("Hyperliquid historical_trades_post_payload builder not configured!")
        #         return []
        # elif self.exchange_name == "lighter":
        #     params = {'pair': symbol, 'limit': limit}
        #     if before_timestamp:
        #         params['before'] = int(before_timestamp) # Lighter 'before' might be in seconds
        #     request_func = requests.get
        
        # try:
        #     headers = {'Accept': 'application/json', 'Content-Type': 'application/json' if is_post_request else ''}
        #     # For APIs that require authentication for historical data (like Paradex likely does)
        #     # You would need to pass JWT here if it's a persistent client or re-authenticate.
        #     # This example assumes the historical endpoint is public or managed by client outside this function.
        #     # If authentication is needed: headers['Authorization'] = f'Bearer {your_jwt_token}'

        #     if is_post_request:
        #         response = request_func(url, headers=headers, json=payload, timeout=30)
        #     else:
        #         response = request_func(url, headers=headers, params=params, timeout=30)
            
        #     response.raise_for_status()
        #     raw_data = response.json()

        #     # For Hyperliquid, the actual trades list might be nested
        #     if self.exchange_name == "hyperliquid" and isinstance(raw_data, dict) and raw_data.get('trades'):
        #         return raw_data['trades']
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error fetching historical trades for {self.exchange_name} {symbol}: {e}", exc_info=True)
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to decode JSON for {self.exchange_name} historical trades: {e}", exc_info=True)
            return []
        except Exception as e:
            self.logger.error(f"An unexpected error occurred fetching historical trades for {self.exchange_name} {symbol}: {e}", exc_info=True)
            return []

    def _process_historical_batch(self, symbol: str, raw_trades: list):
        """
        Parses a batch of raw historical trades and deduplicates them.
        Adds new, unique trades to the main collector's buffer.
        Returns the timestamp of the oldest trade in the batch (for pagination).
        """
        if not raw_trades:
            return None

        parsed_trades = self.api_config['historical_trade_parser'](raw_trades)
        
        new_trades_count = 0
        oldest_timestamp = float('inf') # To track the timestamp for the next 'before' query

        for trade in parsed_trades:
            unique_key = (self.exchange_name, trade['symbol'], trade['trade_id'])
            if unique_key not in self.historical_seen_trade_ids:
                self.historical_seen_trade_ids.add(unique_key)
                with self.main_collector_tick_lock:
                    self.main_collector_tick_buffer.append(trade)
                new_trades_count += 1
            
            if trade['timestamp'] < oldest_timestamp:
                oldest_timestamp = trade['timestamp']

        self.logger.info(f"Processed {len(parsed_trades)} historical trades for {self.exchange_name} {symbol}. Added {new_trades_count} new trades.")
        # Return oldest timestamp - a small delta to ensure we don't fetch the exact same last trade
        return oldest_timestamp - 0.001

    def collect_historical_data_for_symbol(self, symbol: str, start_time_utc: float = None):
        """
        Continuously collects historical data for a single symbol.
        Paginates backwards from current time or a specified start_time_utc.
        """
        current_end_time = start_time_utc if start_time_utc is not None else time.time()
        # Initialize last_fetched_timestamp for this symbol if not already present
        if symbol not in self.last_fetched_timestamp:
            self.last_fetched_timestamp[symbol] = current_end_time

        self.logger.info(f"Starting historical collection for {self.exchange_name} {symbol} from {datetime.datetime.fromtimestamp(current_end_time, tz=datetime.timezone.utc).isoformat()} (UTC)")

        while self.running:
            # We'll fetch 'before' the last processed timestamp for this symbol
            # This ensures we are always moving backwards in time
            fetch_before_ts = self.last_fetched_timestamp[symbol]

            raw_trades = self._fetch_trades_from_api(symbol, HISTORICAL_BATCH_SIZE, fetch_before_ts)
            
            if not raw_trades:
                self.logger.info(f"No more historical trades found for {self.exchange_name} {symbol} before {datetime.datetime.fromtimestamp(fetch_before_ts, tz=datetime.timezone.utc).isoformat()} (UTC) or API limit reached. Pausing.")
                # We could break here if we assume no more data, or sleep longer and retry later
                break # Exit loop for this symbol if no data is returned
            
            oldest_ts_in_batch = self._process_historical_batch(symbol, raw_trades)
            
            if oldest_ts_in_batch is None or oldest_ts_in_batch >= fetch_before_ts:
                # This condition means we either processed no new data, or the oldest data
                # in the batch is not older than what we asked for, indicating we've hit
                # the end of new historical data for this period or are stuck.
                self.logger.info(f"Reached end of new historical data for {self.exchange_name} {symbol}. Oldest in batch: {oldest_ts_in_batch}. Pausing.")
                break
            
            # Update the last fetched timestamp to move to older data in next iteration
            self.last_fetched_timestamp[symbol] = oldest_ts_in_batch
            
            # Introduce a small delay between requests to avoid hitting rate limits
            time.sleep(HISTORICAL_COLLECTION_INTERVAL_SECONDS / HISTORICAL_BATCH_SIZE) # Try to distribute 1 second per trade if batch is large, for 60s/min. Or just fixed small sleep.
            time.sleep(0.5) # Example fixed small sleep

        self.logger.info(f"Historical collection for {self.exchange_name} {symbol} finished or paused.")

# --- Main Application Execution ---
def main():
    """Main function to initialize and run the data collection application."""
    # --- Logger Setup ---
    # Create logger
    logger = logging.getLogger() # Get the root logger
    logger.setLevel(logging.INFO) # Set the minimum level for messages to be processed

    # Create console handler and set level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO) # Console will show INFO and above

    # Create file handler and set level
    file_handler = logging.FileHandler(LOG_FILE_NAME)
    file_handler.setLevel(logging.INFO) # File will also show INFO and above

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to handlers
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    # --- End Logger Setup ---


    collectors = []
    realtime_threads = []
    historical_threads = []

    # Initialize a collector for each configured exchange
    for exchange_name, api_config in EXCHANGE_APIS.items():
        collector = ExchangeCollector(exchange_name, api_config)
        collectors.append(collector)

        has_websocket = api_config["hasWB"]

        if has_websocket and not collector._connect_websocket():
            logger.critical(f"CRITICAL: Failed to connect to {exchange_name}. Skipping collector.")
            continue # Skip this collector if initial market fetch fails

        # First, fetch available perpetuals synchronously before starting other threads
        # This ensures the lists of symbols are populated before WS connections are attempted.
        # If this fails, the collector will not start its main loops.
        if not collector._fetch_available_perps():
            logger.critical(f"CRITICAL: Failed to fetch initial perpetuals for {exchange_name}. Skipping collector.")
            continue # Skip this collector if initial market fetch fails

        # Start a thread to periodically refresh the list of available perpetuals
        market_fetch_thread = threading.Thread(target=collector._market_fetch_loop, name=f"{exchange_name}-MarketFetch")
        market_fetch_thread.daemon = True
        realtime_threads.append(market_fetch_thread)
        market_fetch_thread.start()

        # Create and start a separate thread for tick data collection
        if has_websocket:
            tick_thread = threading.Thread(target=collector.collect_ticks, name=f"{exchange_name}-Ticks")
            tick_thread.daemon = True
            realtime_threads.append(tick_thread)
            tick_thread.start()

        # Create and start a separate thread for funding rate collection
        funding_thread = threading.Thread(target=collector.fetch_funding_rates, name=f"{exchange_name}-FundingRates")
        funding_thread.daemon = True
        realtime_threads.append(funding_thread)
        funding_thread.start()

        # Create and start a separate thread for periodically flushing data to Parquet
        flush_thread = threading.Thread(target=collector._flush_data_to_parquet, name=f"{exchange_name}-Flush")
        flush_thread.daemon = True
        realtime_threads.append(flush_thread)
        flush_thread.start()

        if exchange_name  == "paradex": continue


        ETH_L1 = getenv("ETH_L1")
        PAREDEX_L2 = getenv("PAREDEX_L2")
        PARADEX_PRIVATE_KEY = getenv("PARADEX_PRIVATE_KEY")
        exchange_ccxt = ccxt.paradex({ 'privateKey': PARADEX_PRIVATE_KEY,'walletAddress': PAREDEX_L2,})

        # --- Historical data collection threads ---
        # For each available perpetual, start a historical collector thread
        # This will backfill data from now going backwards.
        # You might want to fine-tune which symbols to backfill or manage this more externally.
        historical_collector = HistoricalCollector(exchange_ccxt, exchange_name, api_config, collector.tick_buffer, collector.tick_buffer_lock)
        for symbol in collector.available_perps:
            hist_thread = threading.Thread(
                target=historical_collector.collect_historical_data_for_symbol,
                args=(symbol, None), # None means start from current time
                name=f"{exchange_name}-Hist-{symbol}"
            )
            hist_thread.daemon = True
            historical_threads.append(hist_thread)
            hist_thread.start()


    logger.info("\n------------------------------------------------------")
    logger.info(f"Data collection started. Data will be saved to '{DATA_DIRECTORY}' directory, partitioned hourly (UTC).")
    logger.info(f"Log messages will be saved to '{LOG_FILE_NAME}'.")
    logger.info(f"Real-time Tick data will be flushed every {FLUSH_INTERVAL_SECONDS} seconds.")
    logger.info(f"Funding rates will be fetched approximately every {FUNDING_RATE_FETCH_INTERVAL_SECONDS} seconds.")
    logger.info(f"Available markets will be refreshed every {MARKET_FETCH_INTERVAL_SECONDS} seconds.")
    logger.info(f"Historical data collection interval per symbol: {HISTORICAL_COLLECTION_INTERVAL_SECONDS} seconds.")
    logger.info("Press Ctrl+C to stop the application.")
    logger.info("------------------------------------------------------\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nCtrl+C detected. Stopping data collection...")
        # Signal all collectors to stop
        for collector in collectors:
            collector.running = False # Stop real-time collector threads
        # Signal all historical collectors to stop
        # Assuming historical_collector instances are unique per exchange
        # (or manage a list of them if multiple per exchange)
        for collector_instance in {hc for hc_thread in historical_threads for hc in collectors if isinstance(hc, ExchangeCollector) and f'historical_collector.{hc.exchange_name}' in logging.Logger.manager.loggerDict}:
            collector_instance.running = False # This assumes a property 'running' also for HistoricalCollector
        
        # This is a bit tricky with how threads are managed. A cleaner way would be:
        # for hc_instance in unique_historical_collector_instances:
        #     hc_instance.stop()

        # Let's ensure HistoricalCollector also has a stop method and its own running flag.
        # For simplicity, I'll add a 'stop' method to HistoricalCollector
        # and ensure its 'running' flag is set to False here.

        # The current design uses `historical_threads` directly.
        # We need to explicitly stop the `HistoricalCollector` instances.
        # A simple way for this example: iterate through `collectors` and if they are ExchangeCollector,
        # then the associated HistoricalCollector (if created) should also be stopped.
        
        # This assumes a more direct reference. For this example, let's keep it simple:
        # The daemon threads will exit when the main program exits, but signaling them
        # to stop is good practice for clean shutdown and final flushes.
        
        # A better pattern for `HistoricalCollector` is to be a sub-component of `ExchangeCollector`
        # or manage them in a dictionary. For now, rely on `daemon=True` and `running` flags.
        
        # We need to signal the `running` flag for all `HistoricalCollector` instances.
        # Let's store references to the historical collector instances directly.
        historical_collector_instances = []
        for exchange_name, api_config in EXCHANGE_APIS.items(): # Re-create instances to signal stop
             # Only create if the main collector was successfully started
            if any(c.exchange_name == exchange_name for c in collectors):
                historical_collector_instances.append(
                    HistoricalCollector(exchange_name, api_config, None, None) # Don't need buffers here, just the instance to call stop()
                )
        for hc_instance in historical_collector_instances:
            hc_instance.running = False


        # Wait for all threads to terminate
        all_threads = realtime_threads + historical_threads
        for thread in all_threads:
            if thread.is_alive():
                logger.info(f"Waiting for thread {thread.name} to terminate...")
                thread.join(timeout=10) # Give it up to 10 seconds to finish
        logger.info("All collectors stopped. Remaining data flushed.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        pass

if __name__ == "__main__":
    main()
