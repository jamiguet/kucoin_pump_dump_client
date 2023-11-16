import json
import math
import time
import ccxt
import sys
from functools import reduce
from colorama import Fore, Style
import pandas as pd
from datetime import date
import os
from dotenv import load_dotenv

# TODO
#  Integrate into streamlit


def provision_kucoin_spot_connection(verbose=False):
    exchange = ccxt.kucoin({
        'adjustForTimeDifference': True,
        "apiKey": os.getenv("API_KEY"),
        "secret": os.getenv("API_SECRET"),
        'password': os.getenv("PASSWORD"),
    })
    exchange.verbose = verbose
    return exchange


def persist_tickers(t_list, num):
    with open(f'{coin}_{date.today().strftime("%m_%d_%Y")}_ticks_{num:03}.json', 'w') as ticks_file:
        ticks_file.write(json.dumps(t_list))
    num += 1
    return num


class Position:

    def __init__(self, exchange, base_currency, auto_close=True, max_dr_down=98):
        self.exchange = exchange
        self.auto_close = auto_close
        self.base_currency = base_currency
        self.symbol = None
        self.coin = None
        self.is_open = False
        self.order_list = list()
        self.opening_price = math.inf
        self.closing_price = math.inf
        self.fees = 0
        self.size = 0
        self.cost = 0
        self.max_pnl = -math.inf
        self.pnl = -math.inf
        self.last_valuation = 0
        self.last_ticker = None
        self.price_delta = 0
        self.max_dr_down = max_dr_down

    def open(self, balance, coin):
        """Opens the position at market rate
        of the given size in base currency
        and in the given coin / base_currency pair"""

        self.coin = coin
        self.cost = balance
        self.symbol = f'{self.coin}-{self.base_currency}'
        ticker = self.exchange.fetch_ticker(symbol=self.symbol)
        last_bid = ticker['bid']
        open_order = api.create_market_buy_order(self.symbol, amount=self.cost * (1 / last_bid))
        self.is_open = True
        open_order = api.fetch_order(open_order['id'], self.symbol)
        self.order_list.append(open_order)
        self.opening_price = float(open_order['price'])
        self.size = float(open_order['filled'])
        self.fees += reduce(lambda acc, val: acc + float(val['cost']), open_order['fees'], 0.0)

        print(f"Market order @ {self.opening_price} got {self.size} {self.coin} ")
        return ticker

    def evaluate(self, ticker):
        """Evaluates the current position against the provided ticker"""

        self.last_ticker = ticker

        self.last_valuation = float(self.size * self.last_ticker['ask'] - self.fees)
        self.pnl = float(self.last_valuation - self.cost)
        if self.pnl > self.max_pnl:
            self.max_pnl = self.pnl

        dr_down = (self.pnl / self.max_pnl * 100)
        if self.auto_close and dr_down < self.max_dr_down:
            print(self)
            self.close()

    def close(self):
        """Closes the position at market rate"""
        if self.is_open:
            closing_order = self.exchange.create_market_sell_order(symbol=self.symbol, amount=self.size)
            self.is_open = False
            closing_order = api.fetch_order(closing_order['id'], symbol=self.symbol)
            self.order_list.append(closing_order)
            self.closing_price = float(closing_order['price'])
            self.fees += reduce(lambda acc, val: acc + float(val['cost']), closing_order['fees'], 0.0)
            closing_valuation = self.size * self.closing_price
            self.pnl = closing_valuation - self.cost - self.fees
            self.price_delta = self.closing_price - self.opening_price
        print(f"Closed Position @ {self.closing_price} "
              f"Price Delta {Fore.GREEN if self.price_delta > 0 else Fore.RED} {self.price_delta:.5f} {Style.RESET_ALL}"
              f"Pnl{Fore.GREEN if self.pnl > 0 else Fore.RED} {self.pnl:.5f} {Style.RESET_ALL}{base_currency}")

    def __str__(self):
        return f"Position: {self.size} {self.coin} / {self.cost} {self.base_currency}; UPnL:{Fore.GREEN if self.pnl > 0 else Fore.RED} {self.pnl:.5f} {self.base_currency} {Style.RESET_ALL} {self.last_valuation / self.cost :.2%} MAX UPnL:{Fore.GREEN if self.max_pnl > 0 else Fore.RED} {self.max_pnl:.5f} {Style.RESET_ALL}  DrD% {self.pnl / self.max_pnl :.2%} @ {self.last_ticker['datetime']}"


def fetch_balance(exchange):
    accounts = exchange.fetch_accounts(params={'type': 'trade', 'currency': base_currency})
    return float(accounts[0]['info']['available'])


if __name__ == '__main__':
    # Run configuration
    load_dotenv()
    base_currency = os.getenv('BASE_CURRENCY')

    print('Start')
    print('python', sys.version)
    print('CCXT Version:', ccxt.__version__)
    api = provision_kucoin_spot_connection()

    # Fetch account balance
    balance = fetch_balance(api)
    print(f"Available trading balance: {balance} {base_currency}")

    auto_close = bool(os.getenv("AUTO_CLOSE"))
    if auto_close:
        max_dr_down = int(os.getenv("CLOSING_DRAW_DOWN"))
        position = Position(api, base_currency, auto_close=auto_close, max_dr_down=max_dr_down)
    else:
        position = Position(api, base_currency, auto_close=auto_close)

    ticker_list = list()
    ticker_file_count = 0
    # Sit and wait for coin [prompt]
    coin = input("Pumped Coin: ")

    ticker_list.append(position.open(balance, coin))

    pump_df = pd.DataFrame(
        columns=['coin', 'pos_quote', 'pos_base', 'last_ask', 'last_price', 'u_pnl'])

    try:
        while True:
            ticker = api.fetch_ticker(symbol=position.symbol)

            position.evaluate(ticker)
            print(position)

            ticker_list.append(ticker)
            if len(ticker_list) > 1000:
                ticker_file_count = persist_tickers(ticker_list, ticker_file_count)
                ticker_list = list()

            pump_df.loc[len(pump_df)] = [coin, position.size, position.last_valuation, position.last_ticker['ask'],
                                         position.last_ticker['last'], position.pnl]

            time.sleep(0.7)
    except KeyboardInterrupt:
        if position.is_open:
            close = input("Close position: [Y/n] : ") or "Y"
            if close == "Y":
                position.close()
            else:
                print(f"Keeping position open. Close it on the web: https://www.kucoin.com/trade/{symbol}")

    print("End")
    pump_df.to_csv(f'{coin}_{date.today().strftime("%m_%d_%Y")}.csv', index=False)
    with open(f'{coin}_{date.today().strftime("%m_%d_%Y")}_orders.json', 'w') as order_file:
        order_file.write(json.dumps(position.order_list))
    ticker_file_count = persist_tickers(ticker_list, ticker_file_count)
