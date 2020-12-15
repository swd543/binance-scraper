#!/usr/bin/env python3
# %%

import hashlib
from sqlite3.dbapi2 import OperationalError
from typing import Any
from urllib.parse import urlencode
import tempfile
import gzip
from numpy.lib import utils
import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sqlite3
import numpy as np

host = "http://api.binance.com"
GET_TICKER_PRICE = f"{host}/api/v3/ticker/price"
GET_AVG_PRICES = f"{host}/api/v3/avgPrice"
GET_KLINES = f"{host}/api/v3/klines"
GET_EXCHANGE_INFO = f"{host}/api/v3/exchangeInfo"

columns = ['open_time','open','high','low','close','volume','close_time','quote_asset_volume','number_of_trades','tbbav','tbqav','ignore']
dtypes = ['datetime64[ms]', float, float, float, float, float, 'datetime64[ms]', float, int, float, float, str]

def request(method:requests.request, url, params=None, cache=False):
    print(params)
    if cache:
        to_hash = f"{url}{urlencode(params) if params!=None else ''}"
        hash = hashlib.md5(to_hash.encode()).hexdigest()[:32]
        try:
            with gzip.open(f"{tempfile.gettempdir()}/{hash}.json.gz","r") as f:
                return json.load(f)
        except FileNotFoundError as e:
            print('requesting from server...')
            r=method(url, params)
            if r.status_code==200:
                with gzip.open(f"{tempfile.gettempdir()}/{hash}.json.gz","w") as f:
                    f.write(r.text.encode())
                return r.json()
            else:
                raise ValueError(f'Server responded -> {r} {r.reason} {r.text}')
    else:
        print('requesting from server...')
        return method(url, params).json()

def search_all_symbols(currency='USD', search=None):
    r = request(requests.get, GET_TICKER_PRICE, cache=True)
    symbols = [s for s in r if currency in s['symbol'] and search==None or currency in s['symbol'] and search in s['symbol']]
    return(symbols)

def convert_to_appropriate_datatypes(d):
    for c,t in zip(columns, dtypes):
        d = d.astype({c:t})
    d = d.set_index(['open_time'], drop=False)
    return d

def get_historical(symbol, begin=None, end=None, interval='1d', store=True)->pd.DataFrame:
    params = {'symbol':symbol, 'interval':interval, 'limit':1000}
    if begin is not None:
        params['startTime'] = begin
    if end is not None:
        params['endTime'] = end
    r = request(requests.get, GET_KLINES, params=params, cache=True)
    data = convert_to_appropriate_datatypes(pd.DataFrame(r,columns=columns))
    return data

def get_from_table(symbol, limit=None):
    with sqlite3.connect('data/prices.sqlite3') as connex:
        data = pd.read_sql_query(f"SELECT * from {symbol} order by open_time {'' if limit is None else f' LIMIT {limit}'}", connex)
        data = convert_to_appropriate_datatypes(data)
        return data

def get_time_delta(s):
    return s['close_time'].diff().value_counts().index[0]

def save_to_table(symbol, data: pd.DataFrame):
    with sqlite3.connect('data/prices.sqlite3') as connex:
        try:
            schema = f"""
                CREATE TABLE "{symbol}" (
                "open_time" TIMESTAMP,
                "open" REAL,
                "high" REAL,
                "low" REAL,
                "close" REAL,
                "volume" REAL,
                "close_time" TIMESTAMP,
                "quote_asset_volume" REAL,
                "number_of_trades" INTEGER,
                "tbbav" REAL,
                "tbqav" REAL,
                "ignore" TEXT,
                PRIMARY KEY ("open_time","close_time")
                );
                """
            connex.cursor().execute(schema)
            connex.cursor().execute(f'CREATE INDEX "ix_{symbol}_open_time"ON "{symbol}" ("open_time");')
            connex.cursor().execute(f'CREATE INDEX "ix_{symbol}_close_time"ON "{symbol}" ("close_time");')
        except OperationalError as e:
            print(e)

        def insert(data, connex):
            data = data.copy(deep=False)
            columns = data.columns
            query = f"INSERT OR IGNORE INTO {symbol}({','.join(columns)}) values ({','.join(['?' for _ in columns])})"
            data['open_time']=data['open_time'].astype('str')
            data['close_time']=data['close_time'].astype('str')
            connex.cursor().executemany(query,data.to_records(False))
            connex.commit()

        previous = get_from_table(symbol, 20)

        if previous.empty:
            insert(data, connex)
            return

        previous_time_delta = get_time_delta(previous)
        current_time_delta = get_time_delta(data)
        xdelta = (previous['open_time'].head(1)[0]-data['open_time'].tail(1)[0])%previous_time_delta
        # If time period matches with previous and if difference between datasets is a multiple of the difference between open_times of consecutive rows
        if previous_time_delta == current_time_delta and xdelta.days==0 or previous.empty:
            insert(data, connex)
        else:
            raise ValueError(f'Unmatched time period with previous data ({previous_time_delta} vs {current_time_delta}), cannot save!')

if __name__ == "__main__":
    sqlite3.register_adapter(np.int64, lambda val: int(val))
    sqlite3.register_adapter(np.int32, lambda val: int(val))
    # print(search_all_symbols(search='BTC'))time
    # print(request(requests.get, GET_KLINES, params={'symbol':'BTCEUR', 'interval':'1d'}))
    # print(get_historical_data('BTCEUR'))

    # data = get_historical_data('BTCEUR')
    # print(len(data))
    # data['open'].plot()
    # data['close'].plot()
    # plt.legend()
    # plt.show()
    # data['number_of_trades'].plot()
    # plt.show()

    symbol = "BTCEUR"
    historical = get_historical(symbol)
    print(historical['open_time'].head(1))
    save_to_table(symbol, historical)
    while True:
        start = historical['open_time'][0]
        delta = get_time_delta(historical)
        prestart = start - delta*999
        new = get_historical(symbol, begin=int(prestart.timestamp()*1000), end=int(start.timestamp()*1000))
        print('\n',new)
        save_to_table(symbol, new)