import os
import time
from datetime import datetime

from dotenv import load_dotenv

from exchange_tools import OrderBook, ExchangeConnector


def init_script():
    load_dotenv()
    base_currency = os.getenv('BASE_CURRENCY')
    _exchange = ExchangeConnector('kucoin', base_currency)

    return base_currency, _exchange


if __name__ == '__main__':
    base_coin, exchange = init_script()

    sampled_coin = input("Coin to sample: ")
    symbol = exchange.make_symbol(sampled_coin)
    symbol_dir = f"{symbol.replace('/', '_')}_{exchange.name}"
    os.makedirs(symbol_dir, exist_ok=True)
    seq = 0
    try:
        while True:
            order_book = OrderBook(symbol, exchange.connect())
            order_book_df = order_book.to_df('asks')
            order_book_df['sequence'] = seq
            order_book_df.to_csv(
                os.path.join(symbol_dir,
                             f"ob_{seq:0>3}_{symbol_dir}-{datetime.now().strftime('%Y-%m-%d')}.csv"),
                index=False)
            print(f"Fetched sample #{seq}")
            seq += 1
            time.sleep(2)
    except KeyboardInterrupt as ki:
        print(f"Done fetching {seq} samples")
