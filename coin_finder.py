import os

from dotenv import load_dotenv

from exchange_tools import OrderBook, ExchangeConnector
from sqlalchemy import URL, create_engine

import threading


class Obfetcher(threading.Thread):

    def __init__(self, thr_name, db, symbol, api):
        threading.Thread.__init__(self)
        self.name = thr_name
        self.db = db
        self.symbol = symbol
        self.api = api

    def run(self):
        print(f"{self.name} started on {self.symbol}")
        current_book = OrderBook(self.symbol, self.api)
        symbol_data = current_book.rank_peaks_base_volume('asks')
        symbol_data.to_sql('orders', self.db, schema='ranked_orders', index=False, if_exists='append')
        symbol_data.to_csv('sample.csv', index=False)
        print(f"{self.name} processed {self.symbol}")


if __name__ == '__main__':

    num_thr = int(input("Number of threads [1]: ") or 1)
    load_dotenv()
    connection_string = URL.create(
        'postgresql',
        username=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD'),
        host='ep-late-field-82972218-pooler.eu-central-1.aws.neon.tech',
        database='orderbook_sampler'
    )
    engine = create_engine(connection_string)

    exchange = ExchangeConnector('kucoin', os.getenv('BASE_CURRENCY'))
    api = exchange.connect()
    available_coins = exchange.fetch_coins()
    print(f"Processing {len(available_coins)} coins")

    idx = 0
    while idx < len(available_coins):
        threads = list()
        for thr_num in range(num_thr):
            if idx == len(available_coins):
                break
            coin = available_coins[idx]
            threads.append(Obfetcher(f"fetcher-{idx}", engine, coin, api))
            idx += 1
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    print("Done")
