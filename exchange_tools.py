import json
import math
import os
from datetime import date, datetime
from functools import reduce

import ccxt
import pandas as pd
import pytz
from ccxt import BadSymbol
from colorama import Fore, Style


# TODO integrate multi-exchange support across all features of the module


class OrderBook:
    PRICE = 0
    QUANTITY = 1

    def __init__(self, symbol, exchange):
        self.symbol = symbol
        self.api = exchange
        self.data = None
        self.min_price = None
        self.max_price = None
        self.min_volume = None
        self.max_volume = None
        self.min_factor = None
        self.max_factor = None
        self.last_price = None

    def sort_side_by(self, side='asks', field=PRICE):
        result = list()
        if side == 'asks':
            result = sorted(self.data[side], key=lambda it: it[field])
        else:
            result = sorted(self.data[side], key=lambda it: it[field], reverse=True)
        return result

    def fetch_data(self, force=False):

        if self.data is None or force:
            self.data = self.api.fetch_order_book(self.symbol)
            sorted_asks = self.sort_side_by('asks', self.PRICE)

            self.min_price = sorted_asks[0][self.PRICE]
            self.max_price = sorted_asks[-1][self.PRICE]

            self.min_volume = sorted_asks[0][self.QUANTITY]
            self.max_volume = sorted_asks[-1][self.QUANTITY]

            self.fetch_price()
            self.min_factor = self.min_price / self.last_price
            self.max_factor = self.max_price / self.last_price

    def fetch_price(self, _force=False):
        if self.last_price is None or _force:
            self.last_price = self.api.fetch_ticker(symbol=self.symbol)['last']

    def volume_at_factor(self, _factor):
        self.fetch_data()
        self.fetch_price()
        thr = self.last_price * _factor
        filtered = list(filter(lambda it: it[self.PRICE] > thr, self.data))
        return reduce(lambda a, v: a + v, map(lambda it: it[self.QUANTITY], filtered), 0)

    def to_df(self, _side=None):
        self.fetch_data()
        self.fetch_price()
        result = pd.DataFrame(
            columns=['timestamp', 'symbol', 'side', 'price', 'factor', 'volume', 'base_volume', 'csum_base_volume'])

        for side in ('bids', 'asks'):
            if _side is None or side == _side:
                c_volume = 0
                for item in self.sort_side_by(side, self.PRICE):
                    _factor = item[self.PRICE] / self.last_price
                    base_price = item[self.QUANTITY] * self.last_price
                    c_volume += base_price
                    result.loc[len(result)] = [
                        datetime.now().astimezone(tz=pytz.timezone('Europe/Zurich')).strftime('%Y-%m-%dT%H:%M:%S%Z'),
                        self.symbol,
                        side,
                        item[self.PRICE],
                        _factor, item[self.QUANTITY],
                        base_price,
                        c_volume]
            else:
                continue

        return result

    def rank_peaks_base_volume(self, side=None):
        _book_df = self.to_df(side)
        _book_df['volume_ranking'] = (_book_df['base_volume'] - _book_df['base_volume'].mean()) / _book_df[
            'base_volume'].std()
        result = _book_df.sort_values(by=['volume_ranking'], ascending=False).iloc[0:5]
        result['symbol'] = self.symbol
        return result

    def pump_volume_factor(self, _pump_volume):
        self.fetch_data()
        self.fetch_price()
        _full_asks = self.to_df('asks')
        result = _full_asks.iloc[_full_asks[_full_asks['csum_base_volume'] <= _pump_volume].index.max()]['factor']

        return result

    def pump_position_price(self, _pump_volume):
        self.fetch_data()
        self.fetch_price()
        _full_bids = self.to_df('bids')
        result = _full_bids.iloc[_full_bids[_full_bids['csum_base_volume'] <= _pump_volume].index.max()]['price']
        return result


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
        open_order = self.exchange.create_market_buy_order(self.symbol, amount=self.cost * (1 / last_bid))
        self.is_open = True
        open_order = self.exchange.fetch_order(open_order['id'], self.symbol)
        self.order_list.append(open_order)
        self.opening_price = float(open_order['price'])
        self.size = float(open_order['filled'])
        self.fees += reduce(lambda acc, val: acc + float(val['cost']), open_order['fees'], 0.0)

        print(f"Market order @ {self.opening_price} got {self.size} {self.coin} T: [{open_order['datetime']}]")
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
            closing_order = self.exchange.fetch_order(closing_order['id'], symbol=self.symbol)
            self.order_list.append(closing_order)
            self.closing_price = float(closing_order['price'])
            self.fees += reduce(lambda acc, val: acc + float(val['cost']), closing_order['fees'], 0.0)
            closing_valuation = self.size * self.closing_price
            self.pnl = closing_valuation - self.cost - self.fees
            self.price_delta = self.closing_price - self.opening_price
        print(f"Closed Position @ {self.closing_price} "
              f"Price Delta {Fore.GREEN if self.price_delta > 0 else Fore.RED} {self.price_delta:.5f} {Style.RESET_ALL}"
              f"Pnl{Fore.GREEN if self.pnl > 0 else Fore.RED} {self.pnl:.5f} {Style.RESET_ALL}{self.base_currency}")

    def persist_tickers(self, t_list, num):
        with open(f'{self.coin}_{date.today().strftime("%m_%d_%Y")}_ticks_{num:03}.json', 'w') as ticks_file:
            ticks_file.write(json.dumps(t_list))
        num += 1
        return num

    def __str__(self):
        return f"Position: {self.size} {self.coin} / {self.cost} {self.base_currency}; UPnL:{Fore.GREEN if self.pnl > 0 else Fore.RED} {self.pnl:.5f} {self.base_currency} {Style.RESET_ALL} {self.last_valuation / self.cost :.2%} MAX UPnL:{Fore.GREEN if self.max_pnl > 0 else Fore.RED} {self.max_pnl:.5f} {Style.RESET_ALL}  DrD% {self.pnl / self.max_pnl :.2%} @ {self.last_ticker['datetime']}"


class ExchangeConnector:

    supported_exchanges = ['kucoin', 'kucoin_f', 'binance_f', 'bitstamp']
    separator = {'kucoin': '-', 'kucoin_f': '-', 'binance_f': '/', 'bitstamp': '/'}

    def __init__(self, name, base_currency):
        if name not in self.supported_exchanges:
            raise ValueError(f"Unsupported exchange {name}")

        self.name = name
        self.base_currency = base_currency
        self.exchange = None

    def make_symbol(self, term_coin):
        return f"{term_coin}{self.separator[self.name]}{self.base_currency}"

    def connect(self):
        if self.name == 'kucoin':
            return self.provision_kucoin_spot_connection()
        elif self.name == 'kucoin_f':
            return self.provision_kucoin_futures_connection()
        elif self.name == 'binance_f':
            return self.provision_binance_futures_connection()
        elif self.name == 'bitstamp':
            return self.provision_bitstamp_spot_connection()
        else:
            raise ValueError(f"Unsupported exchange {self.name}. Must be one of {self.supported_exchanges}")

    def provision_kucoin_spot_connection(self, verbose=False):
        self.exchange = ccxt.kucoin({
            'adjustForTimeDifference': True,
            "apiKey": os.getenv("KUCOIN_API_KEY"),
            "secret": os.getenv("KUCOIN_API_SECRET"),
            'password': os.getenv("PASSWORD"),
        })
        self.exchange.verbose = verbose
        return self.exchange

    def provision_kucoin_futures_connection(self, verbose=False):
        self.exchange = ccxt.kucoinfutures({
            'adjustForTimeDifference': True,
            "apiKey": os.getenv("KUCOIN_API_KEY"),
            "secret": os.getenv("KUCOIN_API_SECRET"),
            'password': os.getenv("PASSWORD"),
        })
        self.exchange.verbose = verbose
        return self.exchange

    def provision_binance_futures_connection(self, verbose=False):
        self.exchange = ccxt.binance({
            'apiKey': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_API_SECRET'),
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
            },
        })
        self.exchange.verbose = verbose
        self.exchange.load_markets()
        return self.exchange

    def provision_bitstamp_spot_connection(self, verbose=False):
        self.exchange = ccxt.bitstamp({
            'apiKey': os.getenv('BITSTAMP_API_KEY'),
            'secret': os.getenv('BITSTAMP_API_SECRET'),
        })
        self.exchange.verbose = verbose
        self.exchange.load_markets()
        return self.exchange

    def fetch_coins(self):
        coins = self.exchange.public_get_symbols()
        coins = list(map(lambda it: it['symbol'],
                         filter(lambda it: it['quoteCurrency'] == self.base_currency, coins['data'])))
        return coins

    def fetch_balance(self):
        accounts = self.exchange.fetch_accounts(params={'type': 'trade', 'currency': self.base_currency})
        return float(accounts[0]['info']['available'])

    def fetch_history(self, coin, time, days):
        since = int(time.values[0].astype('datetime64[s]').astype('int') - days * 3600 * 24) * 1000
        data = self.exchange.fetch_ohlcv(f'{coin}-{self.base_currency}',
                                         timeframe='1d',
                                         since=since - 24 * 3600 * 1000,
                                         # one day earlier so that dont have the pump in the stats
                                         limit=days)
        data_pd = pd.DataFrame(data, columns=['unix', 'open', 'high', 'low', 'close', 'volume'])
        data_pd['date'] = pd.to_datetime(data_pd['unix'].apply(lambda it: it / 1000),
                                         unit='s')
        data_pd['v_hour'] = data_pd['volume'] / 24
        data_pd['v_minute'] = data_pd['volume'] / 24 / 60
        return data_pd

    def fetch_candlesticks(self, coin, time, minutes_before=10, minutes_after=10):
        """Method fetching the candlesticks around the time of the pump"""
        try:
            since = int(time.values[0].astype('datetime64[s]').astype('int') - minutes_before * 60) * 1000
            data = self.exchange.fetch_ohlcv(f'{coin}-{os.getenv("BASE_CURRENCY")}',
                                             since=since,
                                             limit=minutes_before + minutes_after + 1)
            data_pd = pd.DataFrame(data, columns=['unix', 'open', 'high', 'low', 'close', 'volume'])
            data_pd['date'] = pd.to_datetime(data_pd['unix'].apply(lambda it: it / 1000),
                                             unit='s')  # add a human readable date
            return data_pd
        except BadSymbol:
            print(f"No Symbol {coin}-{self.base_currency}, Coin de-listed?")
            return None

    @staticmethod
    def to_utc(time, local=pytz.timezone("Europe/Paris")):
        local_dt = local.localize(time, is_dst=None)
        return local_dt.astimezone(pytz.utc)
