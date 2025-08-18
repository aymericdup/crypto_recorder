import ccxt
import httpx
import asyncio
import websockets
import json
from datetime import datetime

async def main():
    
    paradex = ccxt.paradex({
    'privateKey': '0x7fb90886d3c6509f6b238ca67661aad64ff780f64310b5a3dbbc01e1b24b74',
    'walletAddress': '0x1852e8e92336651fb5e2f49e95ba3e854f143a689d77c0705f12ded6268d5ed',
    })

    data = paradex.fetchMarkets()
    
    filter_data = [x for x in data if x['info']['asset_kind'] == 'PERP']
    paradex_get_return = data[0]['info']

    trades = paradex.fetch_trades("BTC-USD-PERP", limit=5000)

    return

if __name__ == "__main__":
    #fetch_hyperliquid_metadata()
    asyncio.run(main())