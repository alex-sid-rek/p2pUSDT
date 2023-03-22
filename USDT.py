import asyncio
import aiohttp
import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
import ccxt
from itertools import product
import csv
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
from gspread_dataframe import set_with_dataframe
import ccxt


def current_time():
    return datetime.now().strftime("%H:%M:%S")

def spot():
    binance = ccxt.binance()
    # symbols = ['BTC/USDT', 'BTC/BUSD', 'BTC/RUB', 'BNB/USDT', 'BNB/BUSD', 'BNB/BTC', 'BNB/ETH', 'BNB/RUB', 'ETH/USDT', 'ETH/BTC', 'ETH/RUB', 'ETH/BUSD','BUSD/USDT','BUSD/RUB','BUSD/RUB', 'USDT/RUB']
    symbols = ['USDT/RUB']

    data = []
    for symbol in symbols:
        ticker = binance.fetch_ticker(symbol)
        # data.append({'symbol': symbol, 'last_price': ticker['last']})
        data.append({current_time(): symbol, 'SPOT': ticker['last']})
        print(f'{symbol} цена: {ticker["last"]}')

    df = pd.DataFrame(data)
    df.to_csv('spot.csv', index=False, mode='w', header=True, sep=',', decimal='.', lineterminator='\n')

# file_name = "p2p.csv"

url = r'https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search'
transAmount = ["", "10000", "50000", "100000"]
# asset_types = ["USDT", "BTC", "BUSD", "BNB", "ETH", "RUB"]
asset_types = ["USDT"]
pay_types = ["TinkoffNew", "RosBankNew", "QIWI"]

JSON_KEY_FILE = 'Credentials-p2p-arbi.json'
SHEET_NAME = 'p2p arbi table'
SHEET_TAB_NAME = 'p2p'
p2p = 'buyp2p.csv'


async def get_data_from_api(url, payload):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as response:
            if response.status != 200:
                response.raise_for_status()
            await asyncio.sleep(1) # add a delay of 4 second between requests
            return await response.json()

async def collect_data(asset_types, pay_types, url, trans_amount, trade_type):
    print(current_time() + " Начало сбора цен с p2p binance с лимитами : " + trans_amount)
    all_data = []
    for i, asset_type in enumerate(asset_types):
        for j, pay_type in enumerate(pay_types):
            payload = {
                "page": 1,
                "rows": 1,
                "asset": asset_type,
                "fiat": "RUB",
                "tradeType": trade_type,
                "payTypes": [pay_type],
                "transAmount": trans_amount
            }
            data = await get_data_from_api(url, payload)
            for k in data['data']:
                all_data.append({
                    'order': i*len(pay_types) + j,
                    'pay_type': pay_type,
                    'asset_type': asset_type,
                    'price': k['adv']['price']
                })
    all_data = sorted(all_data, key=lambda x: x['order'])
    for i in all_data:
        i.pop('order')
    df = pd.DataFrame(all_data)
    result = df.pivot(index='pay_type', columns='asset_type', values='price')
    result = result.reindex(index=pay_types, columns=asset_types)
    print(current_time() + " Конец сбора цен с p2p binance с лимитами : " + trans_amount)
    return result

async def buy_csv():
    buy_futures = [asyncio.create_task(collect_data(asset_types, pay_types, url, transAmount[i], "BUY")) for i in range(len(transAmount))]
    dfs_buy = await asyncio.gather(*buy_futures)
    for i in range(len(dfs_buy)):
        with open(f'buy{transAmount[i]}p2p.csv', 'w') as f:
            # f.write(f"{current_time(),}\n")  # добавляем строку с текущим временем
            f.write(f"{current_time()},BUY Limit {transAmount[i]}\n")
            dfs_buy[i].to_csv(f, mode='w', header=True, sep=',', float_format='%.2f', decimal=',', lineterminator='\n')

async def sell_csv():
    sell_futures = [asyncio.create_task(collect_data(asset_types, pay_types, url, transAmount[i], "SELL")) for i in range(len(transAmount))]
    dfs_sell = await asyncio.gather(*sell_futures)
    for i in range(len(dfs_sell)):
        with open(f'sell{transAmount[i]}p2p.csv', 'w') as f:
            # f.write(f"{current_time(),}\n")  # добавляем строку с текущим временем
            f.write(f"{current_time()},SELL Limit {transAmount[i]}\n")
            dfs_sell[i].to_csv(f, mode='w', header=True, sep=',', float_format='%.2f', decimal=',', lineterminator='\n')


def import_Google_Sheets():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
    gc = gspread.authorize(credentials)
    # worksheet = gc.open(SHEET_NAME).worksheet(SHEET_TAB_NAME)
    worksheet = gc.open(SHEET_NAME).worksheet("USDT")

    with open("buyp2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'A5:B9' + str(9 + len(data))
        worksheet.update(range, data)

    with open("buy10000p2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'A11:B15' + str(9 + len(data))
        worksheet.update(range, data)

    with open("buy50000p2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'A17:B21' + str(9 + len(data))
        worksheet.update(range, data)

    with open("buy100000p2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'A23:C27' + str(9 + len(data))
        worksheet.update(range, data)
    
    with open("sellp2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'E5:F9' + str(9 + len(data))
        worksheet.update(range, data)

    with open("sell10000p2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'E11:F15' + str(9 + len(data))
        worksheet.update(range, data)

    with open("sell50000p2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'E17:F21' + str(9 + len(data))
        worksheet.update(range, data)

    with open("sell100000p2p.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'E23:F27' + str(9 + len(data))
        worksheet.update(range, data)

    with open("spot.csv", 'r') as file_obj:
        reader = csv.reader(file_obj)
        data = list(reader)
        range = 'I5:J7' + str(9 + len(data))
        worksheet.update(range, data)

    print(current_time()+" Импортированно!")

def gen_run():
    spot()
    asyncio.run(buy_csv())
    asyncio.run(sell_csv())
    import_Google_Sheets()

# gen_run()

while True:
    try:
        gen_run()
    except ValueError:
        print("Произошла ошибка значения, повторная попытка через 10 секунд...:" + current_time())
        time.sleep(10)
        continue
    except TimeoutError:
        print("Произошла ошибка тайм-аута, повторная попытка через 10 секунд..." + current_time())
        time.sleep(10)
        continue
    except ProtocolError:
        print('Произошла ошибка протокола, повторная попытка через 10 секунд...:' + current_time())
        time.sleep(10)
        continue
    except Exception:
        print("Произошла другая ошибка, повторная попытка через 10 секунд...:" + current_time())
        time.sleep(10)
        continue