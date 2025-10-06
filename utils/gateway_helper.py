import os

def get_ccxt_keys():
    return {
        "woofipro": {
            "key":os.getenv("WOOFIPRO_KEY"),
            "secret":os.getenv("WOOFIPRO_SECRET"),
            "account_id": os.getenv("WOOFIPRO_ACCOUNT_ID")
            },
        "apex": {
            "key":os.getenv("APEX_KEY"),
            "secret":os.getenv("APEX_SECRET"),
            "account_id": os.getenv("APEX_ACCOUNT_ID")
            }
        }

def get_gateway_keys():
    config_keys = {
        # "binance":{
        #     "key":os.getenv("BIN_KEY"),
        #     "secret":os.getenv("BIN_SECRET")
        # },
        "hyperliquid":{
            "key":os.getenv("HPL_KEY"),
            "secret":os.getenv("HPL_SECRET"),
            "mode": "live"
        },
        # "paradex":{
        #        "key": os.getenv("PAREDEX_L2"),
        #        "l2_secret": os.getenv("PARADEX_PRIVATE_KEY")
        # },
    }
    return config_keys