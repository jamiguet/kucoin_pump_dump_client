import os

import ccxt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from plotly.subplots import make_subplots

from exchange_tools import OrderBook


@st.cache_resource
def provision_kucoin_spot_connection(verbose=False):
    exchange = ccxt.kucoin({
        'adjustForTimeDifference': True,
        "apiKey": os.getenv("KUCOIN_API_KEY") or st.secrets.KUCOIN_API_KEY,
        "secret": os.getenv("KUCOIN_API_SECRET") or st.secrets.API_SECRET,
        'password': os.getenv("PASSWORD") or st.secrets.PASSWORD,
    })
    exchange.verbose = verbose
    return exchange


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

    fig4 = px.line(data_frame=full_asks, x='factor', y='csum_base_volume', title='Base volume by factor',
                   hover_name='price')
    fig4.add_hline(y=pump_volume, line_color='red')
    st.plotly_chart(fig4)

    fig5 = px.line(data_frame=full_asks, x='factor', y='base_volume', title='Order base volume by factor')
    fig5.add_hline(y=full_asks['base_volume'].mean(), line_color='red')
    fig5.add_hline(y=full_asks['base_volume'].mean() + 3 * full_asks['base_volume'].std(), line_color='yellow')
    st.plotly_chart(fig5)

    st.text(f"Total volume at factor({factor}): {full_asks.base_volume.sum():,.2f} {st.session_state.base_coin}")
    st.text(f"Total volume at factor({factor}): {full_asks.volume.sum():,.2f} {symbol.split('-')[0]}")
    st.text(f"Factor at pump volume: {st.session_state.order_book.pump_volume_factor(pump_volume):.2f}")

    st.text('Highest volume orders and price factor')
    full_asks['volume_ranking'] = (full_asks['base_volume'] - full_asks['base_volume'].mean()) / full_asks[
        'base_volume'].std()
    st.dataframe(full_asks.sort_values(by=['volume_ranking'], ascending=False).iloc[0:5][
                     ['volume_ranking', 'factor', 'price', 'base_volume']].set_index('volume_ranking'),
                 use_container_width=True)
