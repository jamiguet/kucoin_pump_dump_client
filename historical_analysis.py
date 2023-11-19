import streamlit as st
import os
from dotenv import load_dotenv
import pandas as pd
import ccxt
from ccxt.kucoin import BadSymbol
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import numpy as np
import pytz


@st.cache_resource
def provision_kucoin_spot_connection(verbose=False):
    exchange = ccxt.kucoin({
        'adjustForTimeDifference': True,
        "apiKey": os.getenv("API_KEY"),
        "secret": os.getenv("API_SECRET"),
        'password': os.getenv("PASSWORD"),
    })
    exchange.verbose = verbose
    return exchange


def load_historical_file(file_name):
    return pd.read_csv(file_name)


@st.cache_data
def fetch_candlesticks(_api, coin, time, length=30):
    """Method fetching the candlesticks around the time of the pump"""

    try:
        data = _api.fetch_ohlcv(f'{coin}-USDT',
                                since=int(time.values[0].astype('datetime64[s]').astype('int')) * 1000,
                                limit=length)
        data_pd = pd.DataFrame(data, columns=['unix', 'open', 'high', 'low', 'close', 'volume'])
        data_pd['date'] = pd.to_datetime(data_pd['unix'].apply(lambda it: it / 1000),
                                         unit='s')  # add a human readable date
        return data_pd
    except BadSymbol:
        st.error(f"No Symbol {coin}-USDT, Coin de-listed?")
        return None


def to_utc(time, local=pytz.timezone("Europe/Paris")):
    local_dt = local.localize(time, is_dst=None)
    return local_dt.astimezone(pytz.utc)


load_dotenv()

st.title("Historical Pump and Dump event analysis")

kucoin = provision_kucoin_spot_connection()
pumps_df = load_historical_file('pumps.csv')
pumps_df['Date'] = (pd.to_datetime(pumps_df['Date']
                                   .apply(lambda it: it.replace('[', '').replace(']', '')))
                    .apply(lambda it: it.replace(hour=17, minute=50))
                    .apply(to_utc))

# st.dataframe(pumps_df, hide_index=True)

pumped_coins = pumps_df['Coin'].to_list()

current_coin = st.sidebar.selectbox("Event of interest", options=pumped_coins)
pump_time = pumps_df[pumps_df['Coin'] == current_coin]['Date']

pump_data = fetch_candlesticks(kucoin, current_coin, pump_time)

if pump_data is not None:
    st.text(f"Pump on {current_coin} @ {np.datetime_as_string(pump_time.values[0])[:16]} UTC")
    # Create subplots and mention plot grid size
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03, subplot_titles=('OHLC', 'Volume'),
                        row_width=[0.2, 0.7])

    # Plot OHLC on 1st row
    fig.add_trace(go.Candlestick(go.Candlestick(x=pump_data['date'],
                                                open=pump_data['open'],
                                                high=pump_data['high'],
                                                low=pump_data['low'],
                                                close=pump_data['close']),
                                 name="OHLC"), row=1, col=1)

    # Bar trace for volumes on 2nd row without legend
    fig.add_trace(go.Bar(x=pump_data['date'], y=pump_data['volume'], showlegend=False), row=2, col=1)

    # Do not show OHLC's rangeslider plot
    fig.update(layout_xaxis_rangeslider_visible=False)

    st.dataframe(pump_data, use_container_width=True)

    st.plotly_chart(fig)

    st.text("Pump metrics")
    pump_agg = pump_data.agg({
        "open": ['min', 'max', 'median'],
        "close": ['min', 'max', 'median'],
        "high": ['min', 'max', 'median'],
        "low": ['min', 'max', 'median'],
        "volume": ['min', 'max', 'median'],
    })

    st.dataframe(pump_agg)
