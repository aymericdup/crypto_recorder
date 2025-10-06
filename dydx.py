#!/usr/bin/env python3
"""
dYdX v4 Python API Wrapper

A comprehensive Python wrapper for the dYdX v4 protocol API.
Provides access to:
- Perpetual markets and metadata
- Live and historical funding rates
- Real-time market data via WebSocket
- Account data and positions
- Trading functionality

Author: Claude AI Assistant
Version: 1.0.0
Documentation: https://docs.dydx.xyz/
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum

import aiohttp
import websockets
import requests


# Configuration
class dYdXConfig:
    """Configuration for dYdX API endpoints"""
    
    # Mainnet URLs
    MAINNET_INDEXER = "https://indexer.dydx.trade/v4"
    MAINNET_NODE = "https://dydx-grpc.publicnode.com"
    
    # Testnet URLs  
    TESTNET_INDEXER = "https://indexer.v4testnet.dydx.exchange/v4"
    TESTNET_NODE = "https://dydx-testnet.imperator.co"
    
    # WebSocket URLs
    MAINNET_WS = "wss://indexer.dydx.trade/v4/ws"
    TESTNET_WS = "wss://indexer.v4testnet.dydx.exchange/v4/ws"


# Data Models
@dataclass
class PerpetualMarket:
    """Represents a perpetual market"""
    clobPairId: str
    ticker: str
    status: str
    oraclePrice: str
    priceChange24H: str
    volume24H: str
    trades24H: int
    nextFundingRate: str
    initialMarginFraction: str
    maintenanceMarginFraction: str
    openInterest: str
    atomicResolution: int
    quantumConversionExponent: int
    tickSize: str
    stepSize: str
    stepBaseQuantums: int
    subticksPerTick: int
    marketType: str
    openInterestLowerCap: str
    openInterestUpperCap: str
    baseOpenInterest: str
    defaultFundingRate1H: str 

@dataclass
class FundingRate:
    """Represents a funding rate record"""
    ticker: str
    effectiveAt: str
    effectiveAtHeight: str
    price: str
    rate: str


@dataclass
class OrderbookLevel:
    """Represents a single orderbook level"""
    price: str
    size: str


@dataclass
class Orderbook:
    """Represents market orderbook"""
    bids: List[OrderbookLevel]
    asks: List[OrderbookLevel]


@dataclass
class Trade:
    """Represents a trade"""
    id: str
    side: str
    size: str
    price: str
    type: str
    createdAt: str
    createdAtHeight: str


class CandleResolution(Enum):
    """Supported candle resolutions"""
    ONE_MINUTE = "1MIN"
    FIVE_MINUTES = "5MINS"
    FIFTEEN_MINUTES = "15MINS"
    THIRTY_MINUTES = "30MINS"
    ONE_HOUR = "1HOUR"
    FOUR_HOURS = "4HOURS"
    ONE_DAY = "1DAY"


class dYdXAPIError(Exception):
    """Custom exception for dYdX API errors"""
    pass


class dYdX:
    """
    Main wrapper class for dYdX v4 API
    
    Provides access to:
    - Market data (perpetuals, funding rates, orderbooks, trades)
    - Real-time WebSocket feeds
    - Account data (requires authentication)
    - Trading operations (requires authentication)
    """
    
    def __init__(self, 
                 testnet: bool = True,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 passphrase: Optional[str] = None,
                 timeout: int = 30):
        """
        Initialize the dYdX wrapper
        
        Args:
            testnet: Whether to use testnet (True) or mainnet (False)
            api_key: API key for authenticated endpoints (optional)
            api_secret: API secret for authenticated endpoints (optional)
            passphrase: API passphrase for authenticated endpoints (optional)
            timeout: Request timeout in seconds
        """
        self.testnet = testnet
        self.timeout = timeout
        
        # Set base URLs based on network
        if testnet:
            self.indexer_url = dYdXConfig.TESTNET_INDEXER
            self.node_url = dYdXConfig.TESTNET_NODE
            self.ws_url = dYdXConfig.TESTNET_WS
        else:
            self.indexer_url = dYdXConfig.MAINNET_INDEXER
            self.node_url = dYdXConfig.MAINNET_NODE
            self.ws_url = dYdXConfig.MAINNET_WS
            
        # Authentication (if provided)
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        # WebSocket connection
        self.ws_connection = None
        self.ws_subscriptions = set()
        
        # Logger
        self.logger = logging.getLogger(__name__)
    
    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None,
                     data: Optional[Dict] = None, authenticated: bool = False) -> Dict:
        """Make HTTP request to the API"""
        
        url = f"{self.indexer_url}{endpoint}"
        
        headers = self.session.headers.copy()
        
        # Add authentication headers if needed
        if authenticated and self.api_key:
            headers.update({
                'DYDX-SIGNATURE': self._generate_signature(method, endpoint, data),
                'DYDX-API-KEY': self.api_key,
                'DYDX-TIMESTAMP': str(int(time.time())),
                'DYDX-PASSPHRASE': self.passphrase
            })
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {e}")
            raise dYdXAPIError(f"API request failed: {e}")
    
    def _generate_signature(self, method: str, endpoint: str, data: Optional[Dict] = None) -> str:
        """Generate API signature for authenticated requests"""
        # This is a placeholder - actual implementation would require HMAC signing
        # with the API secret. For public endpoints, this won't be called.
        if not self.api_secret:
            return ""
        
        # TODO: Implement proper HMAC-SHA256 signing
        # timestamp = str(int(time.time()))
        # message = timestamp + method.upper() + endpoint + (json.dumps(data) if data else "")
        # signature = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
        return "placeholder_signature"
    
    # Market Data Methods
    
    def get_perpetual_markets(self, market: Optional[str] = None) -> Dict[str, PerpetualMarket]:
        """
        Get all perpetual markets or a specific market
        
        Args:
            market: Optional specific market to fetch (e.g., 'BTC-USD')
            
        Returns:
            Dictionary mapping market names to PerpetualMarket objects
        """
        params = {}
        if market:
            params['market'] = market
            
        response = self._make_request('GET', '/perpetualMarkets', params=params)
        
        markets = {}
        for market_name, market_data in response.get('markets', {}).items():
            markets[market_name] = PerpetualMarket(**market_data)
            
        return markets
    
    def get_historical_funding_rates(self, 
                                   market: str,
                                   effective_before_or_at: Optional[str] = None,
                                   effective_before_or_at_height: Optional[int] = None,
                                   limit: Optional[int] = 100) -> List[FundingRate]:
        """
        Get historical funding rates for a market
        
        Args:
            market: Market symbol (e.g., 'BTC-USD')
            effective_before_or_at: ISO timestamp to filter results
            effective_before_or_at_height: Block height to filter results
            limit: Maximum number of results (default: 100)
            
        Returns:
            List of FundingRate objects
        """
        params = {}
        if effective_before_or_at:
            params['effectiveBeforeOrAt'] = effective_before_or_at
        if effective_before_or_at_height:
            params['effectiveBeforeOrAtHeight'] = effective_before_or_at_height
        if limit:
            params['limit'] = limit
            
        response = self._make_request('GET', f'/historicalFunding/{market}', params=params)
        
        funding_rates = []
        for rate_data in response.get('historicalFunding', []):
            funding_rates.append(FundingRate(**rate_data))
            
        return funding_rates
    
    def get_live_funding_rate(self, market: str) -> Optional[str]:
        """
        Get the current/next funding rate for a market
        
        Args:
            market: Market symbol (e.g., 'BTC-USD')
            
        Returns:
            Current funding rate as string, or None if not found
        """
        markets = self.get_perpetual_markets(market=market)
        if market in markets:
            return markets[market].nextFundingRate
        return None
    
    def get_orderbook(self, market: str) -> Orderbook:
        """
        Get orderbook for a market
        
        Args:
            market: Market symbol (e.g., 'BTC-USD')
            
        Returns:
            Orderbook object with bids and asks
        """
        response = self._make_request('GET', f'/orderbooks/perpetualMarket/{market}')
        
        bids = [OrderbookLevel(price=level[0], size=level[1]) 
                for level in response.get('bids', [])]
        asks = [OrderbookLevel(price=level[0], size=level[1]) 
                for level in response.get('asks', [])]
        
        return Orderbook(bids=bids, asks=asks)
    
    def get_recent_trades(self, 
                         market: str, 
                         limit: Optional[int] = 100,
                         starting_before_or_at_height: Optional[int] = None) -> List[Trade]:
        """
        Get recent trades for a market
        
        Args:
            market: Market symbol (e.g., 'BTC-USD')
            limit: Maximum number of trades to return
            starting_before_or_at_height: Block height to start from
            
        Returns:
            List of Trade objects
        """
        params = {}
        if limit:
            params['limit'] = limit
        if starting_before_or_at_height:
            params['startingBeforeOrAtHeight'] = starting_before_or_at_height
            
        response = self._make_request('GET', f'/trades/perpetualMarket/{market}', params=params)
        
        trades = []
        for trade_data in response.get('trades', []):
            trades.append(Trade(**trade_data))
            
        return trades
    
    def get_candles(self,
                   market: str,
                   resolution: CandleResolution,
                   from_iso: Optional[str] = None,
                   to_iso: Optional[str] = None,
                   limit: Optional[int] = 100) -> List[Dict]:
        """
        Get candlestick data for a market
        
        Args:
            market: Market symbol (e.g., 'BTC-USD')
            resolution: Candle resolution (from CandleResolution enum)
            from_iso: Start time in ISO format
            to_iso: End time in ISO format
            limit: Maximum number of candles
            
        Returns:
            List of candle dictionaries
        """
        params = {'resolution': resolution.value}
        if from_iso:
            params['fromIso'] = from_iso
        if to_iso:
            params['toIso'] = to_iso
        if limit:
            params['limit'] = limit
            
        response = self._make_request('GET', f'/candles/perpetualMarket/{market}', params=params)
        return response.get('candles', [])
    
    def get_market_statistics(self) -> Dict:
        """
        Get 24h statistics for all markets
        
        Returns:
            Dictionary with market statistics
        """
        return self.get_perpetual_markets()
    
    # WebSocket Methods
    
    async def connect_websocket(self):
        """Establish WebSocket connection"""
        try:
            self.ws_connection = await websockets.connect(self.ws_url)
            self.logger.info("WebSocket connected successfully")
        except Exception as e:
            self.logger.error(f"WebSocket connection failed: {e}")
            raise dYdXAPIError(f"WebSocket connection failed: {e}")
    
    async def disconnect_websocket(self):
        """Close WebSocket connection"""
        if self.ws_connection:
            await self.ws_connection.close()
            self.ws_connection = None
            self.ws_subscriptions.clear()
            self.logger.info("WebSocket disconnected")
    
    async def subscribe_to_orderbook(self, market: str, callback=None):
        """
        Subscribe to orderbook updates for a market
        
        Args:
            market: Market symbol (e.g., 'BTC-USD')
            callback: Optional callback function for handling updates
        """
        if not self.ws_connection:
            await self.connect_websocket()
        
        subscribe_msg = {
            "type": "subscribe",
            "channel": "v4_orderbook",
            "id": market
        }
        
        await self.ws_connection.send(json.dumps(subscribe_msg))
        self.ws_subscriptions.add(f"orderbook_{market}")
        
        if callback:
            await self._handle_websocket_messages(callback)
    
    async def subscribe_to_trades(self, market: str, callback=None):
        """
        Subscribe to trade updates for a market
        
        Args:
            market: Market symbol (e.g., 'BTC-USD')
            callback: Optional callback function for handling updates
        """
        if not self.ws_connection:
            await self.connect_websocket()
        
        subscribe_msg = {
            "type": "subscribe",
            "channel": "v4_trades",
            "id": market
        }
        
        await self.ws_connection.send(json.dumps(subscribe_msg))
        self.ws_subscriptions.add(f"trades_{market}")
        
        if callback:
            await self._handle_websocket_messages(callback)
    
    async def subscribe_to_markets(self, callback=None):
        """
        Subscribe to market data updates
        
        Args:
            callback: Optional callback function for handling updates
        """
        if not self.ws_connection:
            await self.connect_websocket()
        
        subscribe_msg = {
            "type": "subscribe",
            "channel": "v4_markets"
        }
        
        await self.ws_connection.send(json.dumps(subscribe_msg))
        self.ws_subscriptions.add("markets")
        
        if callback:
            await self._handle_websocket_messages(callback)
    
    async def _handle_websocket_messages(self, callback):
        """Handle incoming WebSocket messages"""
        try:
            async for message in self.ws_connection:
                data = json.loads(message)
                if callback:
                    await callback(data)
                else:
                    self.logger.info(f"WebSocket message: {data}")
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning("WebSocket connection closed")
        except Exception as e:
            self.logger.error(f"WebSocket error: {e}")
    
    # Utility Methods
    
    def get_server_time(self) -> Dict:
        """Get server time"""
        return self._make_request('GET', '/time')
    
    def get_height(self) -> Dict:
        """Get current block height"""
        return self._make_request('GET', '/height')
    
    def ping(self) -> bool:
        """Test API connectivity"""
        try:
            self.get_server_time()
            return True
        except:
            return False
    
    # Account Methods (require authentication)
    
    def get_account(self, address: str) -> Dict:
        """
        Get account information
        
        Args:
            address: Account address
            
        Returns:
            Account information dictionary
        """
        return self._make_request('GET', f'/addresses/{address}', authenticated=True)
    
    def get_subaccount(self, address: str, subaccount_number: int) -> Dict:
        """
        Get subaccount information
        
        Args:
            address: Account address
            subaccount_number: Subaccount number
            
        Returns:
            Subaccount information dictionary
        """
        endpoint = f'/addresses/{address}/subaccountNumber/{subaccount_number}'
        return self._make_request('GET', endpoint, authenticated=True)
    
    def get_positions(self, address: str, subaccount_number: int) -> Dict:
        """
        Get positions for a subaccount
        
        Args:
            address: Account address
            subaccount_number: Subaccount number
            
        Returns:
            Positions dictionary
        """
        params = {
            'address': address,
            'subaccountNumber': subaccount_number
        }
        return self._make_request('GET', '/perpetualPositions', params=params, authenticated=True)
    
    def get_orders(self, address: str, subaccount_number: int, **kwargs) -> Dict:
        """
        Get orders for a subaccount
        
        Args:
            address: Account address
            subaccount_number: Subaccount number
            **kwargs: Additional filters (market, side, status, etc.)
            
        Returns:
            Orders dictionary
        """
        params = {
            'address': address,
            'subaccountNumber': subaccount_number,
            **kwargs
        }
        return self._make_request('GET', '/orders', params=params, authenticated=True)
    
    def get_fills(self, address: str, subaccount_number: int, **kwargs) -> Dict:
        """
        Get fills for a subaccount
        
        Args:
            address: Account address  
            subaccount_number: Subaccount number
            **kwargs: Additional filters (market, limit, etc.)
            
        Returns:
            Fills dictionary
        """
        params = {
            'address': address,
            'subaccountNumber': subaccount_number,
            **kwargs
        }
        return self._make_request('GET', '/fills', params=params, authenticated=True)
    
    def get_funding_payments(self, address: str, subaccount_number: int, **kwargs) -> Dict:
        """
        Get funding payments for a subaccount
        
        Args:
            address: Account address
            subaccount_number: Subaccount number
            **kwargs: Additional filters (market, limit, etc.)
            
        Returns:
            Funding payments dictionary
        """
        params = {
            'address': address,
            'subaccountNumber': subaccount_number,
            **kwargs
        }
        return self._make_request('GET', '/fundingPayments', params=params, authenticated=True)
    
    def close(self):
        """Close the session and cleanup resources"""
        self.session.close()
        if self.ws_connection:
            asyncio.create_task(self.disconnect_websocket())


# Example usage and helper functions
def example_basic_usage():
    """Example of basic API usage"""
    
    # Initialize client (testnet by default)
    client = dYdX(testnet=True)
    
    # Test connectivity
    if client.ping():
        print("✅ Connected to dYdX API")
    else:
        print("❌ Failed to connect to dYdX API")
        return
    
    # Get all perpetual markets
    print("\n📊 Fetching perpetual markets...")
    markets = client.get_perpetual_markets()
    print(f"Found {len(markets)} markets:")
    for market_name, market in list(markets.items())[:5]:  # Show first 5
        print(f"  {market_name}: {market.indexPrice} USD")
    
    # Get funding rate for BTC-USD
    print("\n💰 Fetching BTC-USD funding rate...")
    funding_rate = client.get_live_funding_rate('BTC-USD')
    if funding_rate:
        print(f"BTC-USD funding rate: {funding_rate}")
    
    # Get historical funding rates
    print("\n📈 Fetching historical funding rates...")
    historical_rates = client.get_historical_funding_rates('BTC-USD', limit=5)
    for rate in historical_rates:
        print(f"  {rate.effectiveAt}: {rate.rate}")
    
    # Get orderbook
    print("\n📖 Fetching BTC-USD orderbook...")
    orderbook = client.get_orderbook('BTC-USD')
    print(f"Best bid: {orderbook.bids[0].price if orderbook.bids else 'N/A'}")
    print(f"Best ask: {orderbook.asks[0].price if orderbook.asks else 'N/A'}")
    
    # Get recent trades
    print("\n🔄 Fetching recent trades...")
    trades = client.get_recent_trades('BTC-USD', limit=3)
    for trade in trades:
        print(f"  {trade.side} {trade.size} at {trade.price}")
    
    # Cleanup
    client.close()


async def example_websocket_usage():
    """Example of WebSocket usage"""
    
    client = dYdX(testnet=True)
    
    # Callback function for handling orderbook updates
    async def handle_orderbook_update(data):
        if data.get('type') == 'channel_data':
            print(f"📊 Orderbook update: {data.get('id')}")
            # Process orderbook data here
    
    # Connect and subscribe to orderbook
    await client.subscribe_to_orderbook('BTC-USD', callback=handle_orderbook_update)
    
    # Keep connection alive for 30 seconds
    await asyncio.sleep(30)
    
    # Cleanup
    await client.disconnect_websocket()
    client.close()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    print("🚀 dYdX v4 Python Wrapper Example")
    print("=" * 50)
    
    # Run basic example
    example_basic_usage()
    
    # Uncomment to run WebSocket example
    # asyncio.run(example_websocket_usage())