from ast import ExceptHandler
import os 
import json
import asyncio
import logging 
import collections
import numpy as np
import pandas as pd
from datetime import datetime

logging.getLogger().setLevel(logging.INFO)
from pprint import pprint
from dotenv import load_dotenv
load_dotenv()

import quantpylib
from quantpylib.gateway.master import Gateway
from quantpylib.utilities.cringbuffer_docs import RingBuffer

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

class MarketData():
    
    def __init__(self,gateway,exchanges,preference_quote='USDT'):
        self.gateway = gateway
        self.exchanges = exchanges
        self.preference_quote = preference_quote

        self.universe = {}
        self.base_mappings = {exc:{} for exc in exchanges} #binance : BTC : BTCUSDT // hyperliquid : BTC : BTC
        self.l2_ticker_streams = {exc:{} for exc in exchanges} #binance : BTCUSDT : lob
        self.balances = {exc:None for exc in exchanges}

    def get_balance(self,exchange):
        return self.balances[exchange]
    
    def get_base_mappings(self,exchange):
        return self.base_mappings[exchange]

    def get_l2_stream(self,exchange,ticker):
        return self.l2_ticker_streams[exchange][ticker]

    async def serve_exchanges(self):
        return await asyncio.gather(*[
            self.serve(exc) for exc in self.exchanges
        ])

    async def serve(self,exchange):
        return await asyncio.gather(*[
            self.serve_base_mappings(self.gateway,exchange),
            #self.serve_l2_stream(self.gateway,exchange),
            #self.serve_balance(self.gateway,exchange)
        ])

    async def serve_base_mappings(self,gateway,exchange):
        while True:
            try:

                # perps_info = await gateway.exchange.contract_specifications(exc=exchange)
                # pd.DataFrame(perps_info).to_csv(f"{exchange} - perps-info.csv", sep=";")

                # if exchange == 'hyperliquid':
                #     fr_histo = await gateway.exc_clients[exchange].get_funding_rates(ticker="BTC", start=datetime(year=2023, month=1, day=1), end=datetime.now())

                perps_data = await gateway.exchange.get_funding_info(exc=exchange)
                # exchange_symbols = [*perps_data]
                # exchange_symbols = [symbol.replace("-USD-PERP", "") for symbol in exchange_symbols if "-USD-PERP" in symbol] if exchange == "paradex" else exchange_symbols
                # pd.DataFrame(perps_data).to_csv(f"{exchange} - perps-data.csv", sep=";")
                mappings = collections.defaultdict(list) #BTC : BTCUSDT,BTCUSDC,BTC
                with open('assets.txt','r') as f:
                    assets = [line.strip() for line in f.readlines() if line != '']
                
                # if exchange == 'binance':
                #     with open('binance_fr.json','r') as f:
                #         fr_data = json.load(f)

                for k,v in perps_data.items():
                    if v['baseAsset'] in assets and v['quoteAsset'] in ['USDC','USDT']:
                        mappings[v['baseAsset']].append(v)
                
                remove = set()
                for k,v in mappings.items():
                    if len(v) > 1 and self.preference_quote is not None:
                        mappings[k] = [x for x in v if x['quoteAsset'] == self.preference_quote]
                    if len(mappings[k]) == 0:
                        remove.add(k)
                for k in remove:
                    mappings.pop(k)
                
                if exchange == 'binance':
                    for k,v in mappings.items():
                        for contract in v:
                            assert contract['symbol'] in fr_data
                            contract['frint'] = np.float64(fr_data[contract['symbol']])
            
                mappings = dict(sorted(mappings.items()))
                self.base_mappings[exchange] = mappings
                self.universe[exchange] = mappings
                logging.info('updated base mappings for %s',exchange)
                await asyncio.sleep(60)
            except:
                logging.exception('error updating base mappings for %s',exchange)
                await asyncio.sleep(15)

    async def serve_l2_stream(self,gateway,exchange):
        universe = self.universe
        while not universe or exchange not in universe:
            await asyncio.sleep(5)
        
        with open('assets.txt','r') as f:
            assets = [line.strip() for line in f.readlines() if line != '']
        base_mappings = universe[exchange]
        tickers = []
        for asset in assets:
            if asset in base_mappings:
                tickers.extend([ticker['symbol'] for ticker in base_mappings[asset]])

        if exchange == 'binance': tickers.append('USDCUSDT')

        lobs = await asyncio.gather(*[
            gateway.executor.l2_book_mirror(
                ticker=ticker,
                speed_ms=500,
                depth=20,
                buffer_size=10,
                as_dict=False,
                exc=exchange
            ) for ticker in tickers
        ])

        for ticker,lob in zip(tickers,lobs):
            self.l2_ticker_streams[exchange][ticker] = lob
        logging.info('updated l2 stream for %s %s',exchange,tickers)
        await asyncio.sleep(1e9)


    async def serve_balance(self,gateway,exchange,poll_interval=100):
        while True:
            try:
                bal = await gateway.account.account_balance(exc=exchange)
                self.balances[exchange] = bal
                logging.info('updated balance for %s',exchange)
                await asyncio.sleep(poll_interval)
            except:
                await asyncio.sleep(15)




async def main():
    gateway = Gateway(config_keys=get_key()) 
    await gateway.init_clients()
    market_data = MarketData(
        gateway=gateway,
        exchanges=["hyperliquid","paradex"],
        preference_quote='USDC'
    )

    coin = "HYPE-USD-PERP"
    #fr_histo = await gateway.exc_clients["hyperliquid"].get_funding_rates(ticker="LAUNCHCOIN", start=datetime(year=2023, month=9, day=1), end=datetime.now())

    # fr_histo = await gateway.exc_clients["paradex"].get_funding_data(coin)
    # fr_histo.to_csv(f"paradex-{coin}-fr.csv", sep=";")
    #asyncio.create_task(market_data.serve_exchanges())

    await asyncio.sleep(1e9)

    await gateway.cleanup_clients()

if __name__ == "__main__":
    asyncio.run(main())





'''
github ssh: https://www.youtube.com/watch?v=J_yt1IzXPes

local computer (laptop/desktop) #ssh connnection using qt410.pem to connect to ec2
ec2 instance #ssh key and authorize your ec2 instance to connect to github

1. version tracking (using Git)
2. using Github repo to store code securely
3. set up ec2 instance
4. connecting ec2 instance to github repo

git checkout 10621ea0a742ac8a0cc9c60d36b8600f7636d146 (quantpylib repo)
'''