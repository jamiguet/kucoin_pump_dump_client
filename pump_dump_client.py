import json
import os
import sys
import time
from datetime import date

import ccxt
import pandas as pd
from dotenv import load_dotenv

from exchange_tools import Position, ExchangeConnector

if __name__ == '__main__':
    # Run configuration
    load_dotenv()
    base_currency = os.getenv('BASE_CURRENCY')

    print('Start')
    print('python', sys.version)
    print('CCXT Version:', ccxt.__version__)

    exchange = ExchangeConnector('kucoin', base_currency)
    api = exchange.connect()

    # Fetch account balance
    balance = exchange.fetch_balance()
    print(f"Available trading balance: {balance:.3f} {base_currency}")

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

    print(f"Check the action @ https://www.kucoin.com/trade/{position.symbol}")

    pump_df = pd.DataFrame(
        columns=['coin', 'pos_quote', 'pos_base', 'last_ask', 'last_price', 'u_pnl', 'unix'])

    try:
        while True:
            ticker = api.fetch_ticker(symbol=position.symbol)

            position.evaluate(ticker)
            print(position)

            ticker_list.append(ticker)
            if len(ticker_list) > 1000:
                ticker_file_count = position.persist_tickers(ticker_list, ticker_file_count)
                ticker_list = list()

            pump_df.loc[len(pump_df)] = [coin, position.size, position.last_valuation, position.last_ticker['ask'],
                                         position.last_ticker['last'], position.pnl, position.last_ticker['time']]

            time.sleep(0.7)
    except KeyboardInterrupt:
        if position.is_open:
            close = input("Close position: [Y/n] : ") or "Y"
            if close == "Y":
                position.close()
            else:
                print(f"Keeping position open. Close it on the web: https://www.kucoin.com/trade/{position.symbol}")

    print("End")
    pump_df.to_csv(f'{coin}_{date.today().strftime("%m_%d_%Y")}.csv', index=False)
    with open(f'{coin}_{date.today().strftime("%m_%d_%Y")}_orders.json', 'w') as order_file:
        order_file.write(json.dumps(position.order_list))
    ticker_file_count = position.persist_tickers(ticker_list, ticker_file_count)
