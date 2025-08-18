import pytz
import time 
import math
import logging
logging.getLogger().setLevel(logging.INFO)

import asyncio 
import collections
import numpy as np 

from pprint import pprint
from decimal import Decimal
from datetime import datetime, timedelta

import quantpylib.standards.markets as markets
from data import MarketData, get_key
from quantpylib.gateway.master import Gateway

'''
def get_balance(self,exchange):
    return self.balances[exchange]
    
def get_base_mappings(self,exchange):
    return self.base_mappings[exchange]

def get_l2_stream(self,exchange,ticker):
    return self.l2_ticker_streams[exchange][ticker]
'''
gateway = None 
market_data = None 
exchanges = ["binance","hyperliquid"]
trade_fees = {
    'binance':[0.0002,0.0005],
    'hyperliquid':[0.0001,0.00035]
}
maker_register = {
    'binance' : {},
    'hyperliquid' : {}
}
taker_balance = {
    'binance' : {},
    'hyperliquid' : {}
}

ceil_to_n = lambda x,n: round((math.ceil(x*(10**n)))/(10**n),n)
round_hr = lambda dt : (dt + timedelta(minutes=30)).replace(second=0, microsecond=0, minute=0)

def usdc_usdt():
    l2 = market_data.get_l2_stream('binance','USDCUSDT')
    return l2.get_mid()

async def trade(timeout,**kwargs):
    try:
        await asyncio.wait_for(_trade(**kwargs),timeout=timeout)
    except asyncio.TimeoutError:
        pass 

async def _trade(gateway, exchanges, asset, equities, positions, ev_thresholds, per_asset):
    contracts_on_asset = {
        exchange : (len(market_data.get_base_mappings(exchange)[asset]) if asset in market_data.get_base_mappings(exchange) else 0)
        for exchange in exchanges
    }
    positions = {exc : pos for exc,pos in zip(exchanges,positions)}
    evs = {}
    iter = 0 
    nominal_exchange = {}
    skip = set()
    submitted_volumes = {}

    while True:
        contracts = []

        for exchange in exchanges:
            mapping = market_data.get_base_mappings(exchange) #exchange > asset > contracts
            mapped = mapping.get(asset,[])
            nominal_position = 0
            for contract in mapped:
                if contract['symbol'] in positions[exchange]:
                    nominal_position += positions[exchange][contract['symbol']][markets.VALUE]
                lob = market_data.get_l2_stream(exchange,contract['symbol'])
                l2_last = {'ts':lob.timestamp,'b':lob.bids,'a':lob.asks}
                fx = 1 if contract['quoteAsset'] == 'USDT' else usdc_usdt()
                l2_ts = l2_last['ts']
                bidX = l2_last['b'][0][0] * fx 
                askX = l2_last['a'][0][0] * fx
                fr_ts = contract['timestamp']
                fr = contract['fr']
                frint = contract['frint']
                nx_ts = contract['next_funding']
                mark = contract['markPrice']
                markX = mark * fx
                _interval_ms = frint * 60 * 60 * 1000 
                accrual = mark * fr * (_interval_ms - max(0,nx_ts - fr_ts)) / _interval_ms
                accrualX = accrual * fx
                contracts.append({
                    "symbol":contract['symbol'],
                    "l2_ts":l2_ts,
                    "bidX":bidX,
                    "askX":askX,
                    "markX":markX,
                    "accrualX":accrualX,
                    "exchange":exchange,
                    "lob":lob,
                    "quantityPrecision":contract['quantityPrecision'],
                    "min_notional":contract['min_notional'],
                })        
            nominal_exchange[exchange] = float(nominal_position)
        
        ticker_stats = {}
        for i in range(len(contracts)):
            for j in range(i+1,len(contracts)):
                icontract = contracts[i]
                jcontract = contracts[j]
                if icontract['exchange'] == jcontract['exchange']:
                    continue #skip lemm 
                ij_ticker = (icontract['symbol'],icontract['exchange'],jcontract['symbol'],jcontract['exchange'])
                ji_ticker = (jcontract['symbol'],jcontract['exchange'],icontract['symbol'],icontract['exchange'])
                if ij_ticker in skip or ji_ticker in skip:
                    continue
                l2_delay = int(time.time() * 1000) - min(icontract['l2_ts'],jcontract['l2_ts'])
                if l2_delay > 4000:
                    skip.add(ij_ticker)
                    skip.add(ji_ticker)
                    continue 
                ij_basis = jcontract['bidX'] - icontract['askX']
                ij_basis_adj = ij_basis + (icontract['accrualX'] - jcontract['accrualX'])
                ji_basis = icontract['bidX'] - jcontract['askX']
                ji_basis_adj = ji_basis + (jcontract['accrualX'] - icontract['accrualX'])
                avg_mark = 0.5 * (icontract['markX'] + jcontract['markX'])
                fees = np.max([
                    trade_fees[icontract['exchange']][0] + trade_fees[jcontract['exchange']][1],
                    trade_fees[jcontract['exchange']][0] + trade_fees[icontract['exchange']][1]
                ])
                inertia = fees * avg_mark
                ij_ev = (ij_basis_adj - inertia) / avg_mark
                ji_ev = (ji_basis_adj - inertia) / avg_mark

                evs[ij_ticker] = 0.9*evs[ij_ticker] + 0.1*ij_ev if ij_ticker in evs else ij_ev
                evs[ji_ticker] = 0.9*evs[ji_ticker] + 0.1*ji_ev if ji_ticker in evs else ji_ev
                ticker_stats[ij_ticker] = [icontract,jcontract,avg_mark]
                ticker_stats[ji_ticker] = [jcontract,icontract,avg_mark]
        
        iter += 1
        if iter % 15 != 0:
            await asyncio.sleep(2)
            continue

        cap_dollars = {}
        #assume nominal_held = -500
        #cap = 1000
        #500, 1500
        for exchange,nominal_held in nominal_exchange.items():
            cap_dollars[exchange] = [per_asset[exchange] - nominal_held,per_asset[exchange] + nominal_held]
            cap_dollars[exchange] = np.maximum(cap_dollars[exchange],0)

        orders, registers = [],[]
        for pair,ev in evs.items():
            if pair in skip:
                continue
            pair_threshold = np.mean([
                ev_thresholds[pair[1]][0],
                ev_thresholds[pair[3]][1]
            ])
            if ev < pair_threshold:
                continue
            cross_contracts = contracts_on_asset[pair[1]] * contracts_on_asset[pair[3]]
            tradable_notional = min(cap_dollars[pair[1]][0],cap_dollars[pair[3]][1]) / cross_contracts
            pair_stats = ticker_stats[pair]

            buy,sell = pair_stats[0],pair_stats[1]
            assert buy['exchange'] == pair[1] and sell['exchange'] == pair[3]
            cross_precision = min(int(buy['quantityPrecision']),int(sell['quantityPrecision']))
            min_notional = max(float(buy['min_notional']),float(sell['min_notional'])) + 10.0
            min_volume = round(min_notional / pair_stats[2],cross_precision)
            max_volume = round(tradable_notional / pair_stats[2],cross_precision)
            max_volume = max_volume if pair not in submitted_volumes else max_volume - submitted_volumes[pair]
            if min_volume > max_volume : continue

            #pair l/s pair[1],pair[3]
            mt_basis = sell['bidX'] - buy['bidX']
            tm_basis = sell['askX'] - buy['askX']
            mt_fees = trade_fees[buy['exchange']][0] + trade_fees[sell['exchange']][1]
            tm_fees = trade_fees[buy['exchange']][1] + trade_fees[sell['exchange']][0]
            mt_adj_basis = mt_basis - mt_fees * pair_stats[2]
            tm_adj_basis = tm_basis - tm_fees * pair_stats[2]
            side = 'mt' if mt_adj_basis > tm_adj_basis else 'tm'
            logging.info(f'pair : {pair}, side : {side}, min_volume : {min_volume}, max_volume : {max_volume} :: ev : {ev}')

            if side == 'mt':
                taker_book = sell['lob'].get_bids_buffer()
                maker_book = buy['lob'].get_bids_buffer()
                #take the minimum of the volumes observed at top of book in the last 5 samples 
                #call this the minimal impact volume 
                taker_miv = np.min(taker_book[-5:,0,1])
                if min_volume > taker_miv : continue 
                order_config = {
                    "ticker":pair[0],
                    "amount":min(taker_miv,max_volume),
                    "exc":pair[1],
                    "price":maker_book[-1,1,0],
                    "tif":markets.TIME_IN_FORCE_ALO,
                    "price_match":markets.PRICE_MATCH_QUEUE_1,
                    "reduce_only":False
                }
                orders.append(order_config)
                registers.append({
                    "pair_ticker":pair,
                    "maker":pair[1],
                    "maker_ticker":pair[0],
                    "taker":pair[3],
                    "taker_ticker":pair[2],
                    "taker_precision":int(sell['quantityPrecision']),
                    "taker_min": ceil_to_n(float(sell['min_notional'])/pair_stats[2],int(sell['quantityPrecision'])),
                })

            if side == 'tm':
                taker_book = buy['lob'].get_asks_buffer()
                maker_book = sell['lob'].get_asks_buffer()
                taker_miv = np.min(taker_book[-5:,0,1])
                if min_volume > taker_miv : continue
                order_config = {
                    "ticker":pair[2],
                    "amount":min(taker_miv,max_volume) * -1,
                    "exc":pair[3],
                    "price":maker_book[-1,1,0],
                    "tif":markets.TIME_IN_FORCE_ALO,
                    "price_match":markets.PRICE_MATCH_QUEUE_1,
                    "reduce_only":False
                }
                orders.append(order_config)
                registers.append({
                    "pair_ticker":pair,
                    "maker":pair[3],
                    "maker_ticker":pair[2],
                    "taker":pair[1],
                    "taker_ticker":pair[0],
                    "taker_precision":int(buy['quantityPrecision']),
                    "taker_min": ceil_to_n(float(buy['min_notional'])/pair_stats[2],int(buy['quantityPrecision'])),
                })
        if orders:
            pprint(orders)
            oids = await asyncio.gather(*[
                oid_limit(gateway.executor.limit_order, order_config) for order_config in orders
            ])
            for oid, order, register in zip(oids, orders, registers):
                if oid is not None:
                    pair = register['pair_ticker']
                    maker_register[register['maker']][oid] = register
                    if pair not in submitted_volumes:
                        submitted_volumes[pair] = abs(order['amount'])
                    else:
                        submitted_volumes[pair] += abs(order['amount'])

async def oid_limit(afunc,kwargs):
    try:
        res = await afunc(**kwargs)
        if kwargs['exc'] == 'binance':
            return res['orderId']
        if kwargs['exc'] == 'hyperliquid':
            return res['response']['data']['statuses'][0]['resting']['oid']
        raise ValueError('invalid exchange oid')
    except:
        return None


def binance_fill_handler(gateway):
    async def handler(msg):
        assert msg['e'] == 'ORDER_TRADE_UPDATE'
        order = msg['o']
        ordstatus = order['X']
        ordtype = order['o']
        if ordstatus not in ['FILLED','PARTIALLY_FILLED']:
            return
        if ordtype != 'LIMIT':
            return
        ticker = order['s']
        oid = order['i']
        side = order['S']
        fillqty = Decimal(order['l'])
        fillamt = fillqty * (Decimal('1') if side == 'BUY' else Decimal('-1'))
        await hedge(oid=oid,exchange='binance',fillamt=fillamt,gateway=gateway)
    return handler

def hyperliquid_fill_handler(gateway):
    async def handler(msg):
        if 'isSnapshot' in msg and msg['isSnapshot']:
            return
        fills = msg['fills']
        for fill in fills:
            ticker = fill['coin']
            oid = fill['oid']
            side = fill['side']
            fillqty = Decimal(fill['sz'])
            fillamt = fillqty * (Decimal('1') if side == 'B' else Decimal('-1'))
            await hedge(oid=oid,exchange='hyperliquid',fillamt=fillamt,gateway=gateway)
    return handler

async def hedge(oid,exchange,fillamt,gateway):
    global taker_balance
    try:
        if oid not in maker_register[exchange]:
            return
        register = maker_register[exchange][oid]
        taker = register['taker']
        taker_ticker = register['taker_ticker']
        held_balance = taker_balance[taker][taker_ticker] if taker_ticker in taker_balance[taker] else Decimal('0')
        hedge_amount = fillamt * Decimal('-1') + held_balance
        if hedge_amount == 0: 
            taker_balance[taker][taker_ticker] = Decimal('0')
            return 
        taker_precision = register['taker_precision']
        taker_min = register['taker_min']
        if round(hedge_amount,taker_precision) != hedge_amount or abs(hedge_amount) < taker_min:
            taker_balance[ticker][taker_ticker] = hedge_amount 
            logging.warning('order size has been buffered: hedge amount : %s',hedge_amount)
        else:
            await gateway.executor.market_order(
                ticker=taker_ticker,
                amount=hedge_amount,
                exc=taker
            )
            taker_balance[taker][taker_ticker] = Decimal('0')
    except:
        logging.exception(f'error in hedging, exchange {exchange}, {oid}:{fillamt}')
        pass

async def hedge_excess(gateway,exchanges,assets):
    global maker_register 
    global taker_balance
    maker_register = {exc : {} for exc in exchanges}
    taker_balance = {exc : {} for exc in exchanges}
    positions = await asyncio.gather(*[gateway.positions.positions_get(exc=exchange) for exchange in exchanges])
    asset_positions = collections.defaultdict(list)

    for position,exchange in zip(positions,exchanges):
        mapping = market_data.get_base_mappings(exchange)
        for asset in assets:
            for contract in mapping[asset]:
                if contract['symbol'] in position:
                    asset_positions[asset].append(position[contract['symbol']])
    
    asset_net = {}
    for asset,positions in asset_positions.items():
        asset_net[asset] = np.sum([pos[markets.AMOUNT] for pos in positions])
    hedge_orders = []

    for asset,net in asset_net.items():
        if net != 0:
            asset_mappings = market_data.get_base_mappings('binance')[asset]
            hedge_contract = None 
            for contract in asset_mappings:
                if contract['quoteAsset'] in ['USDC','USDT']:
                    hedge_contract = contract 
                    break
            assert hedge_contract is not None
            excess_hedge = round(net * -1, int(hedge_contract['quantityPrecision']))
            if excess_hedge == 0:
                continue
            if float(abs(excess_hedge)) * hedge_contract['markPrice'] < hedge_contract['min_notional']:
                continue
            hedge_order = {
                'ticker':hedge_contract['symbol'],
                'amount':excess_hedge,
                'tif':markets.TIME_IN_FORCE_ALO,
                'price_match':markets.PRICE_MATCH_QUEUE_1,
                'reduce_only':False,
                'exc':'binance'
            }
            hedge_orders.append(hedge_order)
    if hedge_orders:
        await asyncio.gather(*[
            gateway.executor.limit_order(**order) for order in hedge_orders
        ])

async def main():
    global gateway, market_data 
    calibrate_for = 2
    leverage = 5

    gateway =  Gateway(config_keys=get_key()) 
    await gateway.init_clients()
    market_data = MarketData(
        gateway=gateway,
        exchanges=exchanges,
        preference_quote='USDC'
    )
    asyncio.create_task(market_data.serve_exchanges())
    await asyncio.sleep(15)

    maker_handlers = {
        'binance' : binance_fill_handler(gateway),
        'hyperliquid' : hyperliquid_fill_handler(gateway)
    }

    assert all(exchange in maker_handlers for exchange in exchanges)
    await asyncio.gather(*[
        gateway.account.account_fill_subscribe(exc=exc,handler=maker_handlers[exc]) for exc in exchanges
    ])
    

    while True:
        now = datetime.now(pytz.utc)
        nearest_hr = round_hr(now)
        min_dist = (now.timestamp() - nearest_hr.timestamp()) / 60
        min_dist = abs(min_dist)

        if min_dist < calibrate_for:
            await asyncio.sleep(60)
            continue

        logging.info('running iteration at time: %s', now) 
        with open('assets.txt','r') as f:
            assets = [line.strip() for line in f.readlines() if line != '']
            
        try:
            equities = np.array([market_data.get_balance(exchange)[markets.ACCOUNT_EQUITY] for exchange in exchanges])
            positions = await asyncio.gather(*[
                gateway.positions.positions_get(exc=exchange) for exchange in exchanges
            ])
        except:
            logging.exception('error retrieving portfolio data')
            await asyncio.sleep(15)

        try:
            nominals = np.array(
                [float(np.sum([v[markets.VALUE] for v in position.values()])) for position in positions]
            )
            betas = nominals / equities
            thresholds = [0.00015 + 0.0005 * np.tanh(beta / 3 * np.array([1,-1])) for beta in betas]
            ev_thresholds = {exc : thresh for exc,thresh in zip(exchanges,thresholds)}
            per_asset = {ex : leverage * eq / len(market_data.get_base_mappings(ex)) for ex,eq in zip(exchanges,equities)}
            logging.info(f'ev thresholds : {ev_thresholds}, per asset : {per_asset}')

            await asyncio.gather(*[
                trade(
                    timeout=60*calibrate_for,
                    gateway=gateway,
                    exchanges=exchanges,
                    asset=asset,
                    equities=equities,
                    positions=positions,
                    ev_thresholds=ev_thresholds,
                    per_asset=per_asset
                ) for asset in assets
            ])
        except Exception as e:
            logging.exception('unhandled error')
            raise 
        finally:
            while True:
                try:
                    await asyncio.gather(*[
                        gateway.executor.cancel_open_orders(exc=exchange) for exchange in exchanges
                    ])
                    await hedge_excess(gateway=gateway,exchanges=exchanges,assets=assets) 
                    break
                except:
                    logging.exception('error cancelling remaining, unfilled orders...trying again')
                    await asyncio.sleep(30)


    await gateway.cleanup_clients()
    pass 

if __name__ == '__main__':
    asyncio.run(main())