import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from collections import deque
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RateLimiter:
    """
    Thread-safe rate limiter for Binance Futures API requests.
    Official Binance Futures limits:
    - 2400 request weight per minute per IP (default)
    - 300 orders per 10 seconds  
    - Weight varies by endpoint (klines = 1-2, exchangeInfo = 1)
    - Higher limits available for VIP users based on trading volume
    """
    
    def __init__(self, weight_per_minute: int = 2400, burst_limit: int = 10):
        self.weight_per_minute = weight_per_minute
        self.burst_limit = burst_limit
        self.request_times = deque()  # Store (timestamp, weight) tuples
        self.burst_count = 0
        self.lock = threading.Lock()
        self.last_429_time = None
        self.backoff_until = None
        self.current_weight_used = 0
            
    def wait_if_needed(self, request_weight: int = 1):
        """Wait if rate limit would be exceeded. Uses weight-based limiting."""
        with self.lock:
            now = time.time()
            
            # Handle 429 backoff
            if self.backoff_until and now < self.backoff_until:
                wait_time = self.backoff_until - now
                logger.warning(f"Rate limit backoff: waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
                self.backoff_until = None
            
            # Remove old requests (older than 1 minute) and recalculate weight
            minute_ago = now - 60
            current_weight = 0
            while self.request_times and self.request_times[0][0] < minute_ago:
                self.request_times.popleft()
            
            # Calculate current weight usage
            current_weight = sum(weight for timestamp, weight in self.request_times)
            
            # Check if adding this request would exceed weight limit
            if current_weight + request_weight > self.weight_per_minute:
                # Find the oldest request that we need to wait for
                target_weight = current_weight + request_weight - self.weight_per_minute
                remaining_weight = 0
                oldest_time = now
                
                for timestamp, weight in self.request_times:
                    remaining_weight += weight
                    oldest_time = timestamp
                    if remaining_weight >= target_weight:
                        break
                
                wait_time = max(0, 60 - (now - oldest_time) + 0.1)  # Small buffer
                if wait_time > 0:
                    logger.debug(f"Weight limit: waiting {wait_time:.2f} seconds (current: {current_weight}, requesting: {request_weight}, limit: {self.weight_per_minute})")
                    time.sleep(wait_time)
                    
                    # Clean up old requests after waiting
                    minute_ago = time.time() - 60
                    while self.request_times and self.request_times[0][0] < minute_ago:
                        self.request_times.popleft()
            
            # Burst protection - prevent too many requests in quick succession
            if self.burst_count >= self.burst_limit:
                time.sleep(0.5)  # Small delay for burst protection
                self.burst_count = 0
            
            # Record this request with its weight
            self.request_times.append((time.time(), request_weight))
            self.burst_count += 1
            self.current_weight_used = sum(weight for timestamp, weight in self.request_times)
    
    def handle_429(self, retry_after: int = None):
        """Handle 429 Too Many Requests response."""
        with self.lock:
            self.last_429_time = time.time()
            backoff_time = retry_after if retry_after else 60  # Default 60 seconds
            self.backoff_until = time.time() + backoff_time
            logger.warning(f"Received 429 error, backing off for {backoff_time} seconds")
    
    def get_current_weight_usage(self) -> int:
        """Get current weight usage for monitoring."""
        with self.lock:
            # Clean old requests
            minute_ago = time.time() - 60
            while self.request_times and self.request_times[0][0] < minute_ago:
                self.request_times.popleft()
            
            return sum(weight for timestamp, weight in self.request_times)

class BinanceHistoricalDataFetcher:
    """
    Fetches historical kline data from Binance Futures API with survivorship bias correction.
    Includes delisted symbols to ensure comprehensive historical analysis.
    """
    
    # Define endpoint weights based on Binance documentation
    ENDPOINT_WEIGHTS = {
        '/fapi/v1/exchangeInfo': 1,
        '/fapi/v1/klines': 1,  # Weight 1 for <= 100 limit, 2 for 101-500, 5 for 501-1000, 10 for >1000
        '/fapi/v1/time': 1,
    }
    
    def __init__(self, base_url: str = "https://fapi.binance.com", 
                 weight_per_minute: int = 2400):
        self.base_url = base_url
        self.rate_limiter = RateLimiter(weight_per_minute=weight_per_minute)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; QuantBot/1.0)'
        })
        
    def get_request_weight(self, endpoint: str, params: dict = None) -> int:
        """Calculate the weight of a request based on endpoint and parameters."""
        if endpoint == '/fapi/v1/klines' and params:
            limit = params.get('limit', 500)
            if limit <= 100:
                return 1
            elif limit <= 500:
                return 2
            elif limit <= 1000:
                return 5
            else:
                return 10
        
        return self.ENDPOINT_WEIGHTS.get(endpoint, 1)
        
    def _make_request(self, url: str, params: dict = None, max_retries: int = 3) -> requests.Response:
        """Make a rate-limited request with retry logic."""
        # Calculate endpoint weight
        endpoint = url.replace(self.base_url, '')
        request_weight = self.get_request_weight(endpoint, params)
        
        for attempt in range(max_retries + 1):
            try:
                # Wait if rate limit would be exceeded
                self.rate_limiter.wait_if_needed(request_weight)
                
                response = self.session.get(url, params=params, timeout=10)
                
                # Log current weight usage periodically
                if hasattr(self, '_log_counter'):
                    self._log_counter += 1
                else:
                    self._log_counter = 1
                    
                if self._log_counter % 100 == 0:
                    current_weight = self.rate_limiter.get_current_weight_usage()
                    logger.debug(f"Current weight usage: {current_weight}/{self.rate_limiter.weight_per_minute}")
                
                # Parse rate limit headers from response
                if 'X-MBX-USED-WEIGHT-1M' in response.headers:
                    used_weight = int(response.headers['X-MBX-USED-WEIGHT-1M'])
                    if used_weight > self.rate_limiter.weight_per_minute * 0.9:  # 90% threshold
                        logger.warning(f"High weight usage detected: {used_weight}/{self.rate_limiter.weight_per_minute}")
                
                # Handle different response codes
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    # Rate limit exceeded
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.rate_limiter.handle_429(retry_after)
                    if attempt < max_retries:
                        logger.warning(f"Rate limited, retrying in {retry_after}s (attempt {attempt + 1})")
                        time.sleep(retry_after)
                        continue
                    else:
                        raise requests.RequestException(f"Rate limit exceeded after {max_retries} retries")
                elif response.status_code == 418:
                    # IP banned
                    logger.error("IP has been banned by Binance. Need to wait or change IP.")
                    raise requests.RequestException("IP banned by Binance")
                elif response.status_code in [500, 502, 503, 504]:
                    # Server errors - retry with exponential backoff
                    if attempt < max_retries:
                        wait_time = (2 ** attempt) * 1  # Exponential backoff
                        logger.warning(f"Server error {response.status_code}, retrying in {wait_time}s")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise requests.RequestException(f"Server error {response.status_code} after {max_retries} retries")
                else:
                    response.raise_for_status()
                    
            except requests.RequestException as e:
                if attempt == max_retries:
                    raise
                wait_time = (2 ** attempt) * 1
                logger.warning(f"Request failed: {e}, retrying in {wait_time}s")
                time.sleep(wait_time)
        
        raise requests.RequestException("Max retries exceeded")
        
    def get_exchange_info(self) -> Dict:
        """Fetch current exchange information including active symbols."""
        url = f"{self.base_url}/fapi/v1/exchangeInfo"
        try:
            response = self._make_request(url)
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching exchange info: {e}")
            raise
    
    def get_all_symbols(self, include_inactive: bool = True) -> List[Dict]:
        """
        Get all symbols including potentially delisted ones.
        
        Args:
            include_inactive: Include symbols that might be inactive/delisted
        
        Returns:
            List of symbol information dictionaries
        """
        exchange_info = self.get_exchange_info()
        symbols = []
        
        for symbol_info in exchange_info['symbols']:
            # Filter for USDT perpetual futures (most liquid)
            if (symbol_info['quoteAsset'] == 'USDT' and 
                symbol_info['contractType'] == 'PERPETUAL'):
                
                symbol_data = {
                    'symbol': symbol_info['symbol'],
                    'baseAsset': symbol_info['baseAsset'],
                    'quoteAsset': symbol_info['quoteAsset'],
                    'status': symbol_info['status'],
                    'contractType': symbol_info['contractType'],
                    'onboardDate': symbol_info.get('onboardDate', 0),
                    'deliveryDate': symbol_info.get('deliveryDate', 0)
                }
                
                # Include all symbols if include_inactive is True, otherwise only TRADING status
                if include_inactive or symbol_info['status'] == 'TRADING':
                    symbols.append(symbol_data)
        
        logger.info(f"Found {len(symbols)} symbols")
        return symbols
    
    def get_earliest_valid_timestamp(self, symbol: str) -> int:
        """
        Find the earliest available timestamp for a symbol by testing different dates.
        """
        # Start from a reasonable date (Binance Futures launched in 2019)
        test_dates = [
            datetime(2019, 9, 1),  # Binance Futures launch
            datetime(2020, 1, 1),
            datetime(2020, 6, 1),
            datetime(2021, 1, 1),
            datetime(2021, 6, 1),
            datetime(2022, 1, 1),
            datetime(2023, 1, 1),
            datetime(2024, 1, 1)
        ]
        
        for test_date in test_dates:
            timestamp = int(test_date.timestamp() * 1000)
            if self._test_kline_availability(symbol, timestamp):
                return timestamp
        
        # If no early date works, return current time minus 1 year
        return int((datetime.now() - timedelta(days=365)).timestamp() * 1000)
    
    def get_funding_rates(self, symbol: str, 
                         start_time: Optional[int] = None,
                         end_time: Optional[int] = None,
                         limit: int = 1000) -> List[Dict]:
        """
        Fetch funding rate history for a symbol.
        
        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds  
            limit: Number of records to fetch (max 1000, default 200 if no time range)
        
        Returns:
            List of funding rate records
        """
        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {
            'symbol': symbol,
            'limit': min(limit, 1000)
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        try:
            response = self._make_request(url, params=params)
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching funding rates for {symbol}: {e}")
            return []
    
    def get_all_historical_funding_rates(self, symbol: str,
                                        start_date: Optional[datetime] = None,
                                        end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Fetch all historical funding rate data for a symbol with pagination.
        
        Args:
            symbol: Trading pair symbol
            start_date: Start date (if None, will get recent data)
            end_date: End date (if None, uses current time)
        
        Returns:
            DataFrame with funding rate data
        """
        if end_date is None:
            end_date = datetime.now()
        
        if start_date is None:
            # Default to last 6 months if no start date
            start_date = end_date - timedelta(days=180)
        
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        
        all_funding_rates = []
        current_timestamp = start_timestamp
        batch_count = 0
        
        logger.info(f"Fetching funding rate history for {symbol} from {start_date.date()} to {end_date.date()}")
        
        while current_timestamp < end_timestamp:
            funding_rates = self.get_funding_rates(
                symbol=symbol,
                start_time=current_timestamp,
                end_time=end_timestamp,
                limit=1000
            )
            
            if not funding_rates:
                logger.warning(f"No more funding rate data available for {symbol}")
                break
            
            all_funding_rates.extend(funding_rates)
            batch_count += 1
            
            # Update timestamp for next batch (funding happens every 8 hours)
            # Add small buffer to avoid duplicate records
            last_funding_time = funding_rates[-1]['fundingTime']
            current_timestamp = last_funding_time + 1
            
            # Log progress every 5 batches
            if batch_count % 5 == 0:
                logger.debug(f"Fetched {batch_count} funding rate batches ({len(all_funding_rates):,} records) for {symbol}")
        
        if not all_funding_rates:
            logger.warning(f"No funding rate data found for {symbol}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_funding_rates)
        
        # Convert data types
        df['fundingRate'] = pd.to_numeric(df['fundingRate'], errors='coerce')
        df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
        
        # Add mark price if available
        if 'markPrice' in df.columns:
            df['markPrice'] = pd.to_numeric(df['markPrice'], errors='coerce')
        
        df['symbol'] = symbol
        
        # Remove duplicates and sort
        df = df.drop_duplicates(subset=['fundingTime']).sort_values('fundingTime').reset_index(drop=True)
        
        # Calculate additional metrics
        df['fundingRate_pct'] = df['fundingRate'] * 100  # Convert to percentage
        df['annualized_rate'] = df['fundingRate'] * 365 * 3 * 100  # Approximate annualized rate (3 times daily)
        
        logger.info(f"Successfully fetched {len(df):,} funding rate records for {symbol}")
        return df
    
    def _test_kline_availability(self, symbol: str, start_time: int) -> bool:
        """Test if kline data is available for a symbol at a specific time."""
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': '1d',
            'startTime': start_time,
            'limit': 1
        }
        
        try:
            response = self._make_request(url, params=params)
            data = response.json()
            return len(data) > 0
        except:
            return False
    
    def get_klines(self, symbol: str, interval: str = '1d', 
                   start_time: Optional[int] = None, 
                   end_time: Optional[int] = None,
                   limit: int = 1000) -> List[List]:
        """
        Fetch kline/candlestick data for a symbol.
        
        Args:
            symbol: Trading pair symbol
            interval: Kline interval (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M)
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds
            limit: Number of klines to fetch (max 1500)
        
        Returns:
            List of kline data arrays
        """
        url = f"{self.base_url}/fapi/v1/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': min(limit, 1500)
        }
        
        if start_time:
            params['startTime'] = start_time
        if end_time:
            params['endTime'] = end_time
        
        try:
            response = self._make_request(url, params=params)
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching klines for {symbol}: {e}")
            return []
    
    def get_all_historical_klines(self, symbol: str, interval: str = '1d',
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Fetch all historical kline data for a symbol, handling pagination.
        
        Args:
            symbol: Trading pair symbol
            interval: Kline interval
            start_date: Start date (if None, will find earliest available)
            end_date: End date (if None, uses current time)
        
        Returns:
            DataFrame with OHLCV data
        """
        if end_date is None:
            end_date = datetime.now()
        
        if start_date is None:
            start_timestamp = self.get_earliest_valid_timestamp(symbol)
        else:
            start_timestamp = int(start_date.timestamp() * 1000)
        
        end_timestamp = int(end_date.timestamp() * 1000)
        
        all_klines = []
        current_timestamp = start_timestamp
        batch_count = 0
        
        logger.info(f"Fetching historical data for {symbol} from {datetime.fromtimestamp(start_timestamp/1000)} to {end_date}")
        
        while current_timestamp < end_timestamp:
            klines = self.get_klines(
                symbol=symbol,
                interval=interval,
                start_time=current_timestamp,
                end_time=end_timestamp,
                limit=1500
            )
            
            if not klines:
                logger.warning(f"No more data available for {symbol}")
                break
            
            all_klines.extend(klines)
            batch_count += 1
            
            # Update timestamp for next batch
            current_timestamp = klines[-1][6] + 1  # Close time + 1ms
            
            # Log progress every 10 batches to avoid spam
            if batch_count % 10 == 0:
                logger.debug(f"Fetched {batch_count} batches ({len(all_klines):,} records) for {symbol}")
                
                # Memory management: if we have too many records, consider chunking
                if len(all_klines) > 100000:  # 100k records threshold
                    logger.debug(f"Large dataset detected for {symbol}, consider using chunked processing")
        
        if not all_klines:
            logger.warning(f"No kline data found for {symbol}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(all_klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # Convert data types
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume',
                          'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
        
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        df['symbol'] = symbol
        df['interval'] = interval  # Add interval for filename generation
        
        # Remove duplicates and sort
        df = df.drop_duplicates(subset=['open_time']).sort_values('open_time').reset_index(drop=True)
        
        logger.info(f"Successfully fetched {len(df):,} records for {symbol}")
        return df
    
    def save_symbol_data(self, df: pd.DataFrame, symbol: str, output_dir: str, 
                        output_format: str = 'parquet', data_type: str = 'klines') -> str:
        """
        Save individual symbol data to dedicated file.
        
        Args:
            df: DataFrame with symbol data
            symbol: Symbol name (e.g., 'BTCUSDT')
            output_dir: Directory to save files
            output_format: Format ('parquet', 'csv', 'feather')
            data_type: Type of data ('klines', 'funding_rates')
        
        Returns:
            Path to saved file
        """
        if df.empty:
            logger.warning(f"No data to save for {symbol}")
            return None
        
        # Generate filename with metadata based on data type
        if data_type == 'funding_rates':
            start_date = df['fundingTime'].min().strftime('%Y%m%d')
            end_date = df['fundingTime'].max().strftime('%Y%m%d')
            base_filename = f"{symbol}_funding_{start_date}_{end_date}"
            output_dir = os.path.join( output_dir, 'fr')
        else:  # klines
            start_date = df['open_time'].min().strftime('%Y%m%d')
            end_date = df['open_time'].max().strftime('%Y%m%d')
            interval = df.get('interval', ['unknown'])[0] if 'interval' in df.columns else 'unknown'
            output_dir = os.path.join(output_dir, 'candles', interval)
            base_filename = f"{symbol}_{interval}_{start_date}_{end_date}"

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        if output_format == 'parquet':
            filename = f"{base_filename}.parquet"
            filepath = os.path.join(output_dir, filename)
            df.to_parquet(filepath, index=False, compression='snappy')
        elif output_format == 'feather':
            filename = f"{base_filename}.feather"
            filepath = os.path.join(output_dir, filename)
            df.to_feather(filepath)
        elif output_format == 'csv':
            filename = f"{base_filename}.csv"
            filepath = os.path.join(output_dir, filename)
            df.to_csv(filepath, index=False)
        else:
            raise ValueError(f"Unsupported format: {output_format}")
        
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        logger.info(f"Saved {symbol} {data_type}: {len(df):,} records to {filename} ({file_size_mb:.2f} MB)")
        
        return filepath
    
    def create_survivorship_bias_free_dataset(self, 
                                             interval: str = '1d',
                                             start_date: Optional[datetime] = None,
                                             end_date: Optional[datetime] = None,
                                             max_workers: int = 3,
                                             output_dir: str = "./binance_data",
                                             output_format: str = 'parquet',  # 'parquet', 'csv', 'feather'
                                             save_summary: bool = True) -> Dict:
        """
        Create a comprehensive historical dataset without survivorship bias.
        Each symbol is saved to a separate file to avoid memory issues.
        
        Args:
            interval: Kline interval
            start_date: Start date for data collection
            end_date: End date for data collection
            max_workers: Number of parallel workers
            output_dir: Directory to save individual symbol files
            output_format: Output format ('parquet', 'csv', 'feather')
            save_summary: Whether to save a summary CSV with metadata
        
        Returns:
            Dictionary with processing statistics
        """
        symbols = self.get_all_symbols(include_inactive=True)
        logger.info(f"Processing {len(symbols)} symbols, saving to: {output_dir}")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Statistics tracking
        stats = {
            'total_symbols': len(symbols),
            'successful_symbols': 0,
            'failed_symbols': [],
            'saved_files': [],
            'total_records': 0,
            'total_size_mb': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        def process_symbol(symbol_info):
            """Process a single symbol and save to file."""
            symbol = symbol_info['symbol']
            try:
                # Fetch data for this symbol
                df = self.get_all_historical_klines(
                    symbol=symbol,
                    interval=interval,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if not df.empty:
                    # Add metadata
                    df['base_asset'] = symbol_info['baseAsset']
                    df['status'] = symbol_info['status']
                    df['onboard_date'] = pd.to_datetime(symbol_info['onboardDate'], unit='ms') if symbol_info['onboardDate'] else None
                    
                    # Save to individual file
                    filepath = self.save_symbol_data(
                        df=df,
                        symbol=symbol,
                        output_dir=output_dir,
                        output_format=output_format
                    )
                    
                    if filepath:
                        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                        return {
                            'symbol': symbol,
                            'status': 'success',
                            'records': len(df),
                            'filepath': filepath,
                            'size_mb': file_size_mb,
                            'start_date': df['open_time'].min(),
                            'end_date': df['open_time'].max()
                        }
                    else:
                        return {'symbol': symbol, 'status': 'save_failed'}
                else:
                    logger.warning(f"No data retrieved for {symbol}")
                    return {'symbol': symbol, 'status': 'no_data'}
                    
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
                return {'symbol': symbol, 'status': 'error', 'error': str(e)}
        
        # Process symbols with threading
        processed_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(process_symbol, symbol_info): symbol_info['symbol'] 
                              for symbol_info in symbols}
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    processed_results.append(result)
                    
                    if result['status'] == 'success':
                        stats['successful_symbols'] += 1
                        stats['saved_files'].append(result['filepath'])
                        stats['total_records'] += result['records']
                        stats['total_size_mb'] += result['size_mb']
                        logger.info(f"✓ {symbol}: {result['records']:,} records saved ({stats['successful_symbols']}/{len(symbols)})")
                    else:
                        stats['failed_symbols'].append({'symbol': symbol, 'reason': result['status']})
                        logger.warning(f"✗ {symbol}: {result['status']}")
                        
                except Exception as e:
                    stats['failed_symbols'].append({'symbol': symbol, 'reason': f'exception: {e}'})
                    logger.error(f"✗ {symbol}: Exception - {e}")
        
        stats['end_time'] = datetime.now()
        stats['duration_minutes'] = (stats['end_time'] - stats['start_time']).total_seconds() / 60
        
        # Save summary metadata
        if save_summary:
            summary_df = pd.DataFrame(processed_results)
            summary_path = os.path.join(output_dir, f"summary_{interval}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            summary_df.to_csv(summary_path, index=False)
            logger.info(f"Summary saved to: {summary_path}")
        
        # Log final statistics
        logger.info(f"🎯 Processing Complete!")
        logger.info(f"   ✓ Successful: {stats['successful_symbols']}/{stats['total_symbols']} symbols")
        logger.info(f"   📊 Total records: {stats['total_records']:,}")
        logger.info(f"   💾 Total size: {stats['total_size_mb']:.2f} MB")
        logger.info(f"   ⏱️  Duration: {stats['duration_minutes']:.1f} minutes")
        logger.info(f"   📁 Files saved to: {output_dir}")
        
        if stats['failed_symbols']:
            logger.warning(f"   ❌ Failed symbols: {len(stats['failed_symbols'])}")
            for failed in stats['failed_symbols'][:5]:  # Show first 5 failures
                logger.warning(f"      - {failed['symbol']}: {failed['reason']}")
            if len(stats['failed_symbols']) > 5:
                logger.warning(f"      ... and {len(stats['failed_symbols']) - 5} more")
        
        return stats

    def create_funding_rates_dataset(self,
                                    start_date: Optional[datetime] = None,
                                    end_date: Optional[datetime] = None,
                                    max_workers: int = 3,
                                    output_dir: str = "./binance_funding_data",
                                    output_format: str = 'parquet',
                                    save_summary: bool = True) -> Dict:
        """
        Create a comprehensive funding rates dataset for all perpetual symbols.
        
        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            max_workers: Number of parallel workers
            output_dir: Directory to save individual symbol files
            output_format: Output format ('parquet', 'csv', 'feather')
            save_summary: Whether to save a summary CSV with metadata
        
        Returns:
            Dictionary with processing statistics
        """
        # Get only perpetual symbols (funding rates only apply to perpetuals)
        all_symbols = self.get_all_symbols(include_inactive=True)
        perpetual_symbols = [s for s in all_symbols if s.get('contractType') == 'PERPETUAL']
        
        logger.info(f"Processing funding rates for {len(perpetual_symbols)} perpetual symbols, saving to: {output_dir}")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Statistics tracking
        stats = {
            'total_symbols': len(perpetual_symbols),
            'successful_symbols': 0,
            'failed_symbols': [],
            'saved_files': [],
            'total_records': 0,
            'total_size_mb': 0,
            'start_time': datetime.now(),
            'end_time': None
        }
        
        def process_symbol_funding(symbol_info):
            """Process funding rates for a single symbol."""
            symbol = symbol_info['symbol']
            try:
                # Fetch funding rate data for this symbol
                df = self.get_all_historical_funding_rates(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date
                )
                
                if not df.empty:
                    # Add metadata
                    df['base_asset'] = symbol_info['baseAsset']
                    df['status'] = symbol_info['status']
                    df['onboard_date'] = pd.to_datetime(symbol_info['onboardDate'], unit='ms') if symbol_info['onboardDate'] else None
                    
                    # Save to individual file
                    filepath = self.save_symbol_data(
                        df=df,
                        symbol=symbol,
                        output_dir=output_dir,
                        output_format=output_format,
                        data_type='funding_rates'
                    )
                    
                    if filepath:
                        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
                        return {
                            'symbol': symbol,
                            'status': 'success',
                            'records': len(df),
                            'filepath': filepath,
                            'size_mb': file_size_mb,
                            'start_date': df['fundingTime'].min(),
                            'end_date': df['fundingTime'].max(),
                            'avg_funding_rate': df['fundingRate'].mean(),
                            'min_funding_rate': df['fundingRate'].min(),
                            'max_funding_rate': df['fundingRate'].max()
                        }
                    else:
                        return {'symbol': symbol, 'status': 'save_failed'}
                else:
                    logger.warning(f"No funding rate data retrieved for {symbol}")
                    return {'symbol': symbol, 'status': 'no_data'}
                    
            except Exception as e:
                logger.error(f"Failed to process funding rates for {symbol}: {e}")
                return {'symbol': symbol, 'status': 'error', 'error': str(e)}
        
        # Process symbols with threading
        processed_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {executor.submit(process_symbol_funding, symbol_info): symbol_info['symbol'] 
                              for symbol_info in perpetual_symbols}
            
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    processed_results.append(result)
                    
                    if result['status'] == 'success':
                        stats['successful_symbols'] += 1
                        stats['saved_files'].append(result['filepath'])
                        stats['total_records'] += result['records']
                        stats['total_size_mb'] += result['size_mb']
                        avg_rate = result['avg_funding_rate'] * 100  # Convert to percentage
                        logger.info(f"✓ {symbol}: {result['records']:,} funding records, avg rate: {avg_rate:.4f}% ({stats['successful_symbols']}/{len(perpetual_symbols)})")
                    else:
                        stats['failed_symbols'].append({'symbol': symbol, 'reason': result['status']})
                        logger.warning(f"✗ {symbol}: {result['status']}")
                        
                except Exception as e:
                    stats['failed_symbols'].append({'symbol': symbol, 'reason': f'exception: {e}'})
                    logger.error(f"✗ {symbol}: Exception - {e}")
        
        stats['end_time'] = datetime.now()
        stats['duration_minutes'] = (stats['end_time'] - stats['start_time']).total_seconds() / 60
        
        # Save summary metadata
        if save_summary:
            summary_df = pd.DataFrame(processed_results)
            summary_path = os.path.join(output_dir, f"funding_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            summary_df.to_csv(summary_path, index=False)
            logger.info(f"Funding rates summary saved to: {summary_path}")
        
        # Log final statistics
        logger.info(f"🎯 Funding Rates Processing Complete!")
        logger.info(f"   ✓ Successful: {stats['successful_symbols']}/{stats['total_symbols']} symbols")
        logger.info(f"   📊 Total records: {stats['total_records']:,}")
        logger.info(f"   💾 Total size: {stats['total_size_mb']:.2f} MB")
        logger.info(f"   ⏱️  Duration: {stats['duration_minutes']:.1f} minutes")
        logger.info(f"   📁 Files saved to: {output_dir}")
        
        if stats['failed_symbols']:
            logger.warning(f"   ❌ Failed symbols: {len(stats['failed_symbols'])}")
            for failed in stats['failed_symbols'][:5]:
                logger.warning(f"      - {failed['symbol']}: {failed['reason']}")
            if len(stats['failed_symbols']) > 5:
                logger.warning(f"      ... and {len(stats['failed_symbols']) - 5} more")
        
        return stats

    def load_symbol_data(self, symbol: str, data_dir: str, 
                        file_pattern: str = None, data_type: str = 'klines') -> pd.DataFrame:
        """
        Load individual symbol data from file.
        
        Args:
            symbol: Symbol to load (e.g., 'BTCUSDT')
            data_dir: Directory containing symbol files
            file_pattern: Custom file pattern (if None, will search for symbol files)
            data_type: Type of data ('klines', 'funding_rates')
        
        Returns:
            DataFrame with symbol data
        """
        if file_pattern is None:
            # Find files matching the symbol and data type
            if data_type == 'funding_rates':
                possible_files = [f for f in os.listdir(data_dir) if f.startswith(f"{symbol}_funding")]
            else:
                possible_files = [f for f in os.listdir(data_dir) if f.startswith(symbol) and 'funding' not in f]
        else:
            possible_files = [f for f in os.listdir(data_dir) if file_pattern.format(symbol=symbol) in f]
        
        if not possible_files:
            logger.warning(f"No {data_type} files found for {symbol} in {data_dir}")
            return pd.DataFrame()
        
        # Take the first matching file (could be enhanced to select best match)
        filepath = os.path.join(data_dir, possible_files[0])
        
        try:
            if filepath.endswith('.parquet'):
                df = pd.read_parquet(filepath)
            elif filepath.endswith('.feather'):
                df = pd.read_feather(filepath)
            elif filepath.endswith('.csv'):
                if data_type == 'funding_rates':
                    df = pd.read_csv(filepath, parse_dates=['fundingTime'])
                else:
                    df = pd.read_csv(filepath, parse_dates=['open_time', 'close_time'])
            else:
                raise ValueError(f"Unsupported file format: {filepath}")
            
            logger.info(f"Loaded {len(df):,} {data_type} records for {symbol} from {possible_files[0]}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading {symbol} {data_type} from {filepath}: {e}")
            return pd.DataFrame()

# Example usage and utility functions
def main():
    """Example usage of the BinanceHistoricalDataFetcher"""
    # Configuration
    DATA_FOLDER = "./binance_data"           # 📁 Configure your data folder here
    FUNDING_FOLDER = "./binance_data"     # 📁 Funding rates folder
    INTERVAL = "1h"                         # Klines interval
    START_DATE = datetime(2019, 1, 1)       # Adjust as needed
    END_DATE = datetime.now()
    
    # Use official Binance Futures rate limits
    fetcher = BinanceHistoricalDataFetcher(weight_per_minute=2400)
    
    # Example 1: Get current exchange info
    logger.info("Fetching exchange information...")
    symbols = fetcher.get_all_symbols()
    perpetual_symbols = [s for s in symbols if s.get('contractType') == 'PERPETUAL']
    logger.info(f"Found {len(symbols)} total symbols ({len(perpetual_symbols)} perpetuals)")
    
    # Example 2: Fetch funding rates for a single symbol
    '''
    logger.info("Fetching funding rate sample for BTCUSDT...")
    btc_funding = fetcher.get_all_historical_funding_rates(
        symbol='BTCUSDT',
        start_date=START_DATE,
        end_date=END_DATE
    )
    if not btc_funding.empty:
        logger.info(f"BTCUSDT funding: {len(btc_funding):,} records")
        avg_rate = btc_funding['fundingRate_pct'].mean()
        logger.info(f"Average funding rate: {avg_rate:.4f}% per 8 hours")
        print("\nSample funding data:")
        print(btc_funding[['fundingTime', 'fundingRate_pct', 'annualized_rate']].head())
        
        # Save single symbol funding data
        fetcher.save_symbol_data(
            df=btc_funding,
            symbol='BTCUSDT', 
            output_dir=FUNDING_FOLDER,
            output_format='parquet',
            data_type='funding_rates'
        )
    '''

    # Example 3: Fetch klines for comparison
    '''
    logger.info("Fetching klines sample for BTCUSDT...")
    btc_klines = fetcher.get_all_historical_klines(
        symbol='BTCUSDT',
        interval=INTERVAL,
        start_date=START_DATE,
        end_date=END_DATE
    )
    if not btc_klines.empty:
        logger.info(f"BTCUSDT klines: {len(btc_klines):,} candles")
        print("\nSample klines data:")
        print(btc_klines[['open_time', 'open', 'high', 'low', 'close', 'volume']].head())
    '''

    # Example 4: Monitor current weight usage
    '''
    current_weight = fetcher.rate_limiter.get_current_weight_usage()
    logger.info(f"Current weight usage: {current_weight}/2400")
    '''

    # Example 5: Configuration summary
    logger.info(f"""
    Ready to download historical data!
    
    Configuration:
    Klines folder: {DATA_FOLDER}
    Funding folder: {FUNDING_FOLDER}
    Interval: {INTERVAL}
    Date range: {START_DATE.date()} to {END_DATE.date()}
    Perpetual symbols: {len(perpetual_symbols)}
    
    Tip: Funding rates are much smaller datasets than klines!
    """)
    
    # Example 6: Download all funding rates (uncomment to run)
    
    logger.info("Downloading all funding rates...")
    funding_stats = fetcher.create_funding_rates_dataset(
        start_date=START_DATE,
        end_date=END_DATE,
        max_workers=3,
        output_dir=FUNDING_FOLDER,
        output_format='parquet'
    )
    logger.info(f"Funding rates download complete! Check {FUNDING_FOLDER}")
    
    
    # Example 7: Download all klines (uncomment to run)
    
    logger.info("Downloading all klines...")
    klines_stats = fetcher.create_survivorship_bias_free_dataset(
        interval=INTERVAL,
        start_date=START_DATE,
        end_date=END_DATE,
        max_workers=3,
        output_dir=DATA_FOLDER,
        output_format='parquet'
    )
    logger.info(f"Klines download complete! Check {DATA_FOLDER}")
    
    
    # Example 8: Load data back from files
    """
    logger.info("Loading saved data...")
    btc_funding_loaded = fetcher.load_symbol_data('BTCUSDT', FUNDING_FOLDER, data_type='funding_rates')
    btc_klines_loaded = fetcher.load_symbol_data('BTCUSDT', DATA_FOLDER, data_type='klines')
    
    if not btc_funding_loaded.empty:
        logger.info(f"Loaded funding: {len(btc_funding_loaded):,} records")
    if not btc_klines_loaded.empty:
        logger.info(f"Loaded klines: {len(btc_klines_loaded):,} records")
    """

if __name__ == "__main__":
    main()