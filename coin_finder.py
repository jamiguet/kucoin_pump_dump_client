import os
from datetime import date, datetime

import pandas as pd
from dotenv import load_dotenv

from exchange_tools import OrderBook, ExchangeConnector

if __name__ == '__main__':

    load_dotenv()
    exchange = ExchangeConnector('kucoin', os.getenv('BASE_CURRENCY'))
    api = exchange.connect()
    available_coins = exchange.fetch_coins()
    print(f"Processing {len(available_coins)} coins")

    all_coins = pd.DataFrame()
    for idx, coin in enumerate(available_coins):
        order_book = OrderBook(coin, api)
        current_coin = order_book.rank_peaks_base_volume('asks')
        all_coins = pd.concat([all_coins, current_coin])
        if idx % 100 == 0:
            iteration = datetime.now()
            print(f"Just processed {coin} {idx / len(available_coins):.2%}")

    print("Done")
    all_coins.to_csv(f'all_coins_top_asks_ranked_{date.today().strftime("%Y-%m-%d")}.csv', index_label='idx')
