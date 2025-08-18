import httpx
import asyncio
import websockets
import json
from datetime import datetime

# Async-safe queues
tick_queue = asyncio.Queue()
funding_queue = asyncio.Queue()
paradex_tick_queue = asyncio.Queue()

# retrieve perp metadata
def fetch_hyperliquid_metadata():
    url = "https://api.hyperliquid.xyz/info"
    payload = {"type": "meta"}

    try:
        response = httpx.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

        universe = data.get("universe", [])
        margins = data.get("marginTables", {})

        print("Available instruments:", [u["name"] for u in universe])
        print("Margin info example:", margins.get("BTC", {}))

        return universe, margins
    except Exception as e:
        print("Error fetching metadata:", e)
        return [], {}

# File-based storage for simplicity
async def save_tick_worker(symbol):
    while True:
        tick = await tick_queue.get()
        with open(f"ticks_{symbol}.log", "a") as f:
            timestamp = datetime.utcnow().isoformat()
            f.write(f"{timestamp} {json.dumps(tick)}\n")
        tick_queue.task_done()


async def save_funding_worker(symbol):
    while True:
        funding = await funding_queue.get()
        with open(f"funding_{symbol}.log", "a") as f:
            timestamp = datetime.utcnow().isoformat()
            f.write(f"{timestamp} {json.dumps(funding)}\n")
        funding_queue.task_done()


async def save_paradex_tick_worker(symbol):
    while True:
        tick = await paradex_tick_queue.get()
        with open(f"ticks_paradex_{symbol}.log", "a") as f:
            timestamp = datetime.utcnow().isoformat()
            f.write(f"{timestamp} {json.dumps(tick)}\n")
        paradex_tick_queue.task_done()


async def listen_to_hyperliquid(symbol="BTC"):
    uri = "wss://api.hyperliquid.xyz/ws"
    async with websockets.connect(uri) as ws:
        # Send individual subscribe messages
        await ws.send(json.dumps({ "method": "subscribe", "subscription": { "type": "trades", "coin": symbol } }))
        
        #await ws.send(json.dumps({"method": "subscribe", "topic": "fundingRates", "symbol": symbol}))

         
        #await ws.send(json.dumps({ "method": "subscribe", "subscription": { "type": "allMids" } }))
        #await ws.send(json.dumps({ "method": "subscribe", "subscription": { "type": "activeAssetCtx", "coin": symbol } }))

        print(f"Subscribed to Hyperliquid trades and funding for {symbol}")

        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)
                topic = data.get("topic")

                if topic == "trades":
                    await tick_queue.put(data)
                elif topic == "fundingRates":
                    await funding_queue.put(data)
                else:
                    print("Unhandled message (Hyperliquid):", data)
            except Exception as e:
                print("Hyperliquid WebSocket error:", e)
                break


async def listen_to_paradex(symbol="BTC-USDC"):
    uri = "wss://api.paradex.trade/v1"  # Update this if needed from official docs
    async with websockets.connect(uri) as ws:
        subscribe_msg = {
            "type": "subscribe",
            "channels": [
                {"name": "trades", "symbols": [symbol]}
            ]
        }
        await ws.send(json.dumps(subscribe_msg))
        print(f"Subscribed to Paradex trades for {symbol}")

        while True:
            try:
                message = await ws.recv()
                data = json.loads(message)
                if data.get("type") == "trade":
                    await paradex_tick_queue.put(data)
                else:
                    print("Unhandled message (Paradex):", data)
            except Exception as e:
                print("Paradex WebSocket error:", e)
                break


async def main():
    await asyncio.gather(
        listen_to_hyperliquid("BTC"),
        #save_tick_worker("BTC"),
        #save_funding_worker("BTC"),
        #listen_to_paradex("BTC-USDC"),
        #save_paradex_tick_worker("BTC-USDC")
    )


if __name__ == "__main__":
    #fetch_hyperliquid_metadata()
    asyncio.run(main())