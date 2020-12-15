#!/usr/bin/env python3

import requests
import pandas as pd

# # r=requests.get('https://api.coindesk.com/v1/bpi/currentprice.json')
# r = requests.get('https://api.coindesk.com/v1/bpi/historical/close.json', params={'start':'2013-09-01', 'end':'2014-09-05'})
# data = pd.read_json(r.json()['bpi'])
# print(data)

r = requests.get('https://api.binance.com/api/v3/exchangeInfo', headers={'X-MBX-APIKEY':'x9jk5d0z9JyPJSvC7RGyB77XbxeDqu7OkddP6bnbhYWUmBivvibZxSzF1wECS6st'})
print(r.text)