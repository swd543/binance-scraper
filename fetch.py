#!/usr/bin/env python3
import asyncio
import sqlite3
from typing import Dict, Tuple

import aiohttp
import pandas as pd

host = "http://api.binance.com"
GET_TICKER_PRICE = f"{host}/api/v3/ticker/price"
GET_AVG_PRICES = f"{host}/api/v3/avgPrice"
GET_KLINES = f"{host}/api/v3/klines"
GET_EXCHANGE_INFO = f"{host}/api/v3/exchangeInfo"
GET_SERVER_TIME = f'{host}/api/v3/time'


types = {'open_time': ('datetime64[ms]', 'TIMESTAMP'),
         'open': ('float', 'REAL'),
         'high': ('float', 'REAL'),
         'low': ('float', 'REAL'),
         'close': ('float', 'REAL'),
         'volume': ('float', 'REAL'),
         'close_time': ('datetime64[ms]', 'TIMESTAMP'),
         'quote_asset_volume': ('float', 'REAL'),
         'number_of_trades': ('float', 'INTEGER'),
         'tbbav': ('float', 'REAL'),
         'tbqav': ('float', 'REAL'),
         'ignore': ('str', 'TEXT')}


class DatabaseHandler(object):
    """Some Information about DataChunk"""

    def __init__(self, database='prices.sqlite3'):
        super(DatabaseHandler, self).__init__()
        self.database = database

    def create_table(self, symbol):
        with sqlite3.connect(self.database) as connex:
            schema = f"""
                    CREATE TABLE IF NOT EXISTS "{symbol}" (
                    {','.join([f'{c} {t}' for c, (_, t) in types.items()])},
                    PRIMARY KEY ("open_time","close_time")
                    );
                    """
            connex.cursor().execute(schema)
            connex.commit()

    def get_latest_datapoints_from_table(self, symbol, limit=10):
        with sqlite3.connect(self.database) as connex:
            return pd.read_sql_query(
                f"SELECT * from \"{symbol}\" order by open_time desc {'' if limit is None else f' LIMIT {limit}'}", connex, parse_dates=['open_time', 'close_time'])

    def get_latest_close_time(self, symbol):
        return DatabaseHandler.datetime64_to_epoch(self.get_latest_datapoints_from_table(symbol, 1)['close_time'][0])

    def datetime64_to_epoch(dt64):
        return int(dt64.value//10e5)

    def push_datapoints_to_table(self, symbol, data: pd.DataFrame):
        with sqlite3.connect(self.database) as connex:
            data = data.copy(deep=False)
            query = f"INSERT OR IGNORE INTO \"{symbol}\"({','.join(types.keys())}) values ({','.join(['?' for _ in types.keys()])})"
            data['open_time'] = data['open_time'].astype('str')
            data['close_time'] = data['close_time'].astype('str')
            connex.cursor().executemany(query, data.to_records(False))
            connex.commit()


class BinanceFetcher(object):
    """Some Information about BinanceFetcher"""

    def __init__(self):
        super(BinanceFetcher, self).__init__()
        self.server_time = None

    async def get_historical(self, session, symbol, begin=None, end=None, interval='6h'):
        params = {'symbol': symbol, 'interval': interval, 'limit': 1000}
        if begin is not None:
            params['startTime'] = begin
        if end is not None:
            params['endTime'] = end
        print(params, end='-->')
        async with await session.get(GET_KLINES, params=params) as response:
            historical = await response.json()
            historical = convert_to_appropriate_datatypes(
                pd.DataFrame(historical, columns=types.keys()))
            print(len(historical))
            return historical

    async def get_server_time(self, session):
        async with await session.get(GET_SERVER_TIME) as response:
            time = await response.json()
            try:
                servertime = time['serverTime']
                self.server_time = servertime
                return servertime
            except KeyError as e:
                return self.server_time


def convert_to_appropriate_datatypes(d: Dict[str, Tuple]):
    for c, (t, _) in types.items():
        d = d.astype({c: t})
    d = d.set_index(['open_time'], drop=False)
    return d


async def query_and_put_in_db(fetcher: BinanceFetcher, dbhandler: DatabaseHandler, session, coin):
    servertime = await fetcher.get_server_time(session)
    databasetime = 0
    try:
        databasetime = dbhandler.get_latest_close_time(coin)
    except pd.io.sql.DatabaseError as e:
        print(f"Perhaps {coin} does not exist in db yet.", e)
        dbhandler.create_table(coin)
    except IndexError as e:
        print(f"Perhaps no data in {coin}.", e)
    finally:
        while(databasetime < servertime):
            data = await fetcher.get_historical(session, coin, begin=databasetime, end=servertime)
            try:
                databasetime = DatabaseHandler.datetime64_to_epoch(
                    data.tail(1)['close_time'][0])
            except IndexError as e:
                print(
                    f"Server has no data for {coin} anymore after {databasetime}.", e)
                break
            dbhandler.push_datapoints_to_table(coin, data)


async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get(GET_TICKER_PRICE) as _r:
            coins = sorted([s['symbol'].strip() for s in await _r.json()])
            print(f'{len(coins)} coins found, querying all!')
            fetcher = BinanceFetcher()
            dbhandler = DatabaseHandler('data/prices.sqlite3')
            await asyncio.gather(*[query_and_put_in_db(fetcher, dbhandler, session, coin) for coin in coins])

if __name__ == "__main__":
    asyncio.run(main())
else:
    print('This is not meant to be run as a lib.')
