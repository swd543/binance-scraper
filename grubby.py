#!/usr/bin/env python3

import requests
from urllib.parse import urlencode
import pandas as pd
import sqlite3
import numpy as np

host = "http://api.binance.com"
GET_TICKER_PRICE = f"{host}/api/v3/ticker/price"
GET_AVG_PRICES = f"{host}/api/v3/avgPrice"
GET_KLINES = f"{host}/api/v3/klines"
GET_EXCHANGE_INFO = f"{host}/api/v3/exchangeInfo"
GET_SERVER_TIME = f'{host}/api/v3/time'

columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
           'quote_asset_volume', 'number_of_trades', 'tbbav', 'tbqav', 'ignore']
dtypes = ['datetime64[ms]', float, float, float, float,
          float, 'datetime64[ms]', float, float, float, float, str]


def request(method: requests.request, url, params=None, cache=False):
    return method(url, params).json()


def convert_to_appropriate_datatypes(d):
    for c, t in zip(columns, dtypes):
        d = d.astype({c: t})
    d = d.set_index(['open_time'], drop=False)
    return d


def get_server_time():
    return request(requests.get, GET_SERVER_TIME)['serverTime']


def get_historical(symbol, begin=None, end=None, interval='6h') -> pd.DataFrame:
    params = {'symbol': symbol, 'interval': interval, 'limit': 1000}
    if begin is not None:
        params['startTime'] = begin
    if end is not None:
        params['endTime'] = end
    print(params)
    r = request(requests.get, GET_KLINES, params=params, cache=True)
    data = convert_to_appropriate_datatypes(pd.DataFrame(r, columns=columns))
    return data


def get_from_table(symbol, limit=None):
    with sqlite3.connect('data/prices.sqlite3') as connex:
        data = pd.read_sql_query(
            f"SELECT * from {symbol} order by open_time {'' if limit is None else f' LIMIT {limit}'}", connex)
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
            connex.cursor().execute(
                f'CREATE INDEX "ix_{symbol}_open_time"ON "{symbol}" ("open_time");')
            connex.cursor().execute(
                f'CREATE INDEX "ix_{symbol}_close_time"ON "{symbol}" ("close_time");')
        except sqlite3.dbapi2.OperationalError as e:
            print(e)

        def insert(data, connex):
            data = data.copy(deep=False)
            columns = data.columns
            query = f"INSERT OR IGNORE INTO {symbol}({','.join(columns)}) values ({','.join(['?' for _ in columns])})"
            data['open_time'] = data['open_time'].astype('str')
            data['close_time'] = data['close_time'].astype('str')
            connex.cursor().executemany(query, data.to_records(False))
            connex.commit()

        previous = get_from_table(symbol, 20)

        if previous.empty:
            insert(data, connex)
            return

        previous_time_delta = get_time_delta(previous)
        current_time_delta = get_time_delta(data)
        xdelta = (previous['open_time'].head(1)[0] -
                  data['open_time'].tail(1)[0]) % previous_time_delta
        # If time period matches with previous and if difference between datasets is a multiple of the difference between open_times of consecutive rows
        if previous_time_delta == current_time_delta and xdelta.days == 0 or previous.empty:
            insert(data, connex)
        else:
            raise ValueError(
                f'Unmatched time period with previous data ({previous_time_delta} vs {current_time_delta}), cannot save!')


def datetime64_to_epoch(dt64):
    return int((dt64 - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 'ms'))


def data_exists(symbol, start, end):
    with sqlite3.connect('data/prices.sqlite3') as connex:
        try:
            return pd.read_sql_query(f"select count(*) from {symbol} where open_time>datetime({start/1000},'unixepoch') and open_time<datetime({end/1000},'unixepoch') order by open_time", connex)['count(*)'][0]
        except pd.io.sql.DatabaseError:
            return 0


def populate(symbol):
    end = get_server_time()+1000
    # 4 hours
    interval = int(3.6e+6 * 4 * 1000)

    while True:
        if data_exists(symbol, int(end-interval), end) == 1000:
            end = end - interval
            continue
        d = get_historical(symbol, begin=int(
            end-interval), end=end, interval='4h')
        if d.empty:
            break
        save_to_table(symbol, d)
        previous_min_time = d['open_time'].head(1)
        end = datetime64_to_epoch(previous_min_time) - interval


if __name__ == "__main__":
    coins = [s['symbol'] for s in request(requests.get, GET_TICKER_PRICE)]
    for c in coins:
        populate(c)
