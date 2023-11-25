import datetime
import time
from functools import reduce
import streamlit as st
import os
from dotenv import load_dotenv
import pandas as pd
import ccxt
from ccxt.kucoin import BadSymbol
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import pytz


@st.cache_resource
def provision_kucoin_spot_connection(verbose=False):
    exchange = ccxt.kucoin({
        'adjustForTimeDifference': True,
        "apiKey": os.getenv("API_KEY") or st.secrets.API_KEY,
        "secret": os.getenv("API_SECRET") or st.secrets.API_SECRET,
        'password': os.getenv("PASSWORD") or st.secrets.PASSWORD,
    })
    exchange.verbose = verbose
    return exchange

@st.cache_data
def fetch_candlesticks(_api, coin, time, minutes_before=10, minutes_after=10):
    """Method fetching the candlesticks around the time of the pump"""
    try:
        since = int(time.values[0].astype('datetime64[s]').astype('int') - minutes_before * 60) * 1000
        data = _api.fetch_ohlcv(f'{coin}-{os.getenv("BASE_CURRENCY") or st.secrets.BASE_CURRENCY}',
                                since=since,
                                limit=minutes_before + minutes_after + 1)
        data_pd = pd.DataFrame(data, columns=['unix', 'open', 'high', 'low', 'close', 'volume'])
        data_pd['date'] = pd.to_datetime(data_pd['unix'].apply(lambda it: it / 1000),
                                         unit='s')  # add a human readable date
        return data_pd
    except BadSymbol:
        st.error(f"No Symbol {coin}-{st.session_state.base_coin}, Coin de-listed?")
        return None


@st.cache_data
def fetch_history(_api, coin, time, days):
    since = int(time.values[0].astype('datetime64[s]').astype('int') - days * 3600 * 24) * 1000
    data = _api.fetch_ohlcv(f'{coin}-{st.session_state.base_coin}',
                            timeframe='1d',
                            since=since - 24 * 3600 * 1000,  # one day earlier so that dont have the pump in the stats
                            limit=days)
    data_pd = pd.DataFrame(data, columns=['unix', 'open', 'high', 'low', 'close', 'volume'])
    data_pd['date'] = pd.to_datetime(data_pd['unix'].apply(lambda it: it / 1000),
                                     unit='s')
    data_pd['v_hour'] = data_pd['volume'] / 24
    data_pd['v_minute'] = data_pd['volume'] / 24 / 60
    return data_pd


def to_utc(time, local=pytz.timezone("Europe/Paris")):
    local_dt = local.localize(time, is_dst=None)
    return local_dt.astimezone(pytz.utc)


class OrderBook:
    PRICE = 0
    QUANTITY = 1
    data = None
    symbol = None
    api = None
    last_price = None
    min_price = None
    max_price = None
    min_factor = None
    max_factor = None
    min_volume = None
    max_volume = None

    def __init__(self, symbol, exchange):
        self.symbol = symbol
        self.api = exchange

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
        result = pd.DataFrame(columns=['side', 'price', 'factor', 'volume', 'base_volume', 'csum_base_volume'])

        for side in ('bids', 'asks'):
            if _side is None or side == _side:
                c_volume = 0
                for item in self.sort_side_by(side, self.PRICE):
                    _factor = item[self.PRICE] / self.last_price
                    base_price = item[self.QUANTITY] * self.last_price
                    c_volume += base_price
                    result.loc[len(result)] = [side, item[self.PRICE],
                                               _factor, item[self.QUANTITY],
                                               base_price,
                                               c_volume]
            else:
                continue

        return result

    def volume_distribution_by_factor(self, side=None):
        _book_df = self.to_df(side)
        bins = [0.1, 0.2, 0.5, 1, 3, 5, 10, 100, 1000]
        result = pd.DataFrame(columns=['up_to_factor', 'sum_base_volume'])

        for cbin in bins:
            value = _book_df[_book_df['factor'] < cbin]['base_volume'].sum()
            result.loc[len(result)] = [cbin, value]

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
        result = _full_bids.iloc[_full_bids[_full_bids['csum_base_volume'] <= pump_volume].index.max()]['price']
        return result


load_dotenv()
st.session_state.base_coin = (os.getenv("BASE_CURRENCY") or st.secrets.BASE_COIN)

st.title("Order book analysis")

kucoin = provision_kucoin_spot_connection()

coins = kucoin.public_get_symbols()
coins = list(map(lambda it: it['symbol'],
                 filter(lambda it: it['quoteCurrency'] == st.session_state.base_coin, coins['data'])))
params = st.experimental_get_query_params()
default_idx = 0
if 'symbol' in params:
    default_idx = coins.index(params['symbol'][0])

symbol = st.sidebar.selectbox(label=f'Select symbol: ({len(coins)})', options=coins, index=default_idx)
factor = st.sidebar.slider(label="Pump max factor:", value=5, min_value=2, max_value=30)
pump_volume = int(st.sidebar.text_input(f'Pump volume {st.session_state.base_coin}: ', value=150000))
show_bid = st.sidebar.checkbox('Show bid', value=False)
show_ask = st.sidebar.checkbox('Show ask', value=False)

st.write(f'Coin Market: <a href="https://www.kucoin.com/trade/{symbol}">{symbol}</a>', unsafe_allow_html=True)

st.sidebar.markdown('---')
sac = st.sidebar.checkbox("Show all coins", value=False)

ticker = kucoin.fetch_ticker(symbol=symbol)

current_df = pd.DataFrame(columns=['bid', 'ask', 'last', 'mid', 'timestamp'])
current_df.loc[len(current_df)] = [ticker['bid'], ticker['ask'], ticker['last'], (ticker['bid'] + ticker['ask']) / 2,
                                   ticker['datetime']]

st.text('Current price')
st.dataframe(current_df, use_container_width=True)

st.text(f"Orderbook sliced by factor")

# TODO graph bid and ask cumulative volume ordered by price and display threshold in it.
#  Find way of estimating the volume required to reach a given pump threshold
#  The volume in a Spot market is the sum size of all the orders that are below that price.
#  Find a Stale market and test it.

st.session_state.order_book = OrderBook(symbol, kucoin)

if show_bid:
    # bids_df = st.session_state.order_book.volume_distribution_by_factor('bids')
    # st.text(f"Bid ({len(bids_df)})")
    # TODO bin the volume according to the factor and make volume graph
    full_bids = st.session_state.order_book.to_df('bids')
    fig = px.line(data_frame=full_bids,
                  x='price',
                  y=full_bids.base_volume.cumsum(),
                  log_y=True, log_x=True,
                  title=f"Bid volume by price n={len(full_bids)}",
                  labels={'y': 'Cumulated base volume'}
                  )
    fig.add_vline(x=st.session_state.order_book.pump_position_price(pump_volume), line_color='red')
    st.plotly_chart(fig)
    st.text(
        f"Price after full purchase: {full_bids.iloc[full_bids[full_bids['csum_base_volume'] <= pump_volume].index.max()]['price']}")
    # st.dataframe(bids_df, use_container_width=True)

if show_ask:

    full_asks = st.session_state.order_book.to_df('asks')
    full_asks = full_asks[full_asks.price < factor * st.session_state.order_book.last_price]
    fig = px.line(data_frame=full_asks,
                  x='price',
                  y=full_asks.base_volume.cumsum(),
                  log_y=False, log_x=True,
                  title=f"Ask volume by price n={len(full_asks)}",
                  labels={'y': 'Cumulated base volume'})
    palette = ['green', 'yellow', 'orange', 'red', 'blue']
    for mult in range(1, factor):
        fig.add_vline(x=st.session_state.order_book.last_price * mult, line_color=palette[(mult - 1) % 5])
    st.plotly_chart(fig)

    fig2 = make_subplots(specs=[[{"secondary_y": True}]])
    fig2.add_trace(go.Scatter(x=full_asks.index, y=full_asks.factor, name='factor'), secondary_y=True)
    fig2.add_trace(go.Scatter(x=full_asks.index, y=full_asks.base_volume.cumsum(), name='base volume'),
                   secondary_y=False)
    fig2.add_vline(x=full_asks[full_asks.base_volume.cumsum() < pump_volume].index.max(), line_color='red')
    fig2.update_yaxes(
        title_text=f"Base volume {st.session_state.base_coin}",
        secondary_y=False)
    fig2.update_yaxes(
        title_text="Factor",
        secondary_y=True)
    fig2.update_xaxes(title_text='index')
    st.plotly_chart(fig2)

    fig4 = px.line(data_frame=full_asks, x='factor', y='csum_base_volume', title='Base volume by factor')
    fig4.add_hline(y=pump_volume, line_color='red')
    st.plotly_chart(fig4)

    st.text(f"Total volume at factor({factor}): {full_asks.base_volume.sum():,.2f} {st.session_state.base_coin}")
    st.text(f"Total volume at factor({factor}): {full_asks.volume.sum():,.2f} {symbol.split('-')[0]}")
    st.text(f"Factor at pump volume: {st.session_state.order_book.pump_volume_factor(pump_volume):.2f}")

if sac:
    st.text(f"Order book summary for all coins:")
    summary = pd.DataFrame(columns=['coin', 'end_price', 'pump_volume_factor', 'pump_volume', 'last_price'])
    bar_text = "Fetching order books"
    pro_bar = st.progress(0, text=bar_text)
    for idx, coi in enumerate(coins):
        _order_book = OrderBook(symbol, kucoin)
        summary.loc[len(summary)] = [coi, _order_book.pump_position_price(pump_volume),
                                     _order_book.pump_volume_factor(pump_volume),
                                     pump_volume,
                                     _order_book.last_price]
        pro_bar.progress(idx / len(coins), f":blue[In progress {idx / len(coins):.2%}]")
        time.sleep(1)

    pro_bar.progress(1, ":green[Done]")
    summary.to_csv(f"all_coins_{datetime.date.today().strftime('%Y-%m-%d')}", index=False)
    st.dataframe(summary, use_container_width=True)
