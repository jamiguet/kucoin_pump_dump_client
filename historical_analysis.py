import streamlit as st
import os
from dotenv import load_dotenv
import pandas as pd
import ccxt
from ccxt.kucoin import BadSymbol
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
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


def load_historical_file(file_name):
    return pd.read_csv(file_name)


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


load_dotenv()

st.title("Historical Pump and Dump event analysis")

kucoin = provision_kucoin_spot_connection()
pumps_df = load_historical_file('pumps.csv')
pumps_df['Date'] = (pd.to_datetime(pumps_df['Date']
                                   .apply(lambda it: it.replace('[', '').replace(']', '')))
                    .apply(lambda it: it.replace(hour=18, minute=00))
                    .apply(to_utc))

# st.dataframe(pumps_df, hide_index=True)

pumped_coins = pumps_df['Coin'].to_list()

current_coin = st.sidebar.selectbox("Event of interest", options=pumped_coins)
pump_time = pumps_df[pumps_df['Coin'] == current_coin]['Date']
st.sidebar.markdown("---")
pre_minutes = st.sidebar.slider("Minutes before pump: ", min_value=0, max_value=60, value=5)
post_minutes = st.sidebar.slider("Minutes after pump: ", min_value=0, max_value=60, value=30)
st.sidebar.markdown("---")
hist_days = st.sidebar.slider("Days before pump:", min_value=1, max_value=365, value=60)
st.sidebar.markdown("---")
st.session_state.base_coin = os.getenv("BASE_CURRENCY") or st.secrets.BASE_CURRENCY
pumped_amount = int(st.sidebar.text_input(f"Pumped amount ({st.session_state.base_coin}):", value=500))

pump_data = fetch_candlesticks(kucoin, current_coin, pump_time, pre_minutes, post_minutes)

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
    st.text(f"Price delta: {pump_agg.loc['max']['high'] - pump_agg.loc['min']['low']}")
    st.text(
        f"Max PNL: {pumped_amount / pump_agg.loc['min']['low'] * pump_agg.loc['max']['high'] - pumped_amount :.3f} "
        f" {st.session_state.base_coin}")

    st.text("Coin metrics")
    hist_data = fetch_history(kucoin, current_coin, pump_time, hist_days)
    hist_agg = hist_data.agg({
        "open": ['min', 'max', 'median'],
        "close": ['min', 'max', 'median'],
        "high": ['min', 'max', 'median'],
        "low": ['min', 'max', 'median'],
        "volume": ['min', 'max', 'median', 'sum'],
        "v_hour": ['min', 'max', 'median'],
        "v_minute": ['min', 'max', 'median'],

    })
    st.dataframe(hist_agg)

    st.text(f"Volume in previous {hist_days} days")
    vol_evol = make_subplots(rows=2, cols=1, shared_xaxes=True,
                             vertical_spacing=0.03, subplot_titles=('Trading volume', 'minute'))
    vol_evol.add_trace(go.Bar(x=hist_data['date'], y=hist_data['v_hour'], name='Hour'), row=1, col=1, )

    vol_evol.add_trace(go.Bar(x=hist_data['date'], y=hist_data['v_minute'], name='Minute'), row=2, col=1)
    st.plotly_chart(vol_evol)
