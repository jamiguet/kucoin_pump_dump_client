import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from exchange_tools import OrderBook, ExchangeConnector

st.title("Coin forecast")


@st.cache_data
def display_order_book(symbol, _api, pump_volume):

    st.write(f'Coin Market: <a href="https://www.kucoin.com/trade/{symbol}">{symbol}</a>', unsafe_allow_html=True)
    order_book = OrderBook(symbol, _api)
    full_asks = order_book.to_df('asks')
    full_asks = full_asks[full_asks.price < int(factor[1]) * order_book.last_price]

    fig2 = make_subplots(specs=[[{"secondary_y": True}]])
    fig2.add_trace(go.Scatter(x=full_asks.index, y=full_asks.factor, name='factor'), secondary_y=True)
    fig2.add_trace(go.Scatter(x=full_asks.index, y=full_asks.csum_base_volume, name='base volume'),
                   secondary_y=False)
    fig2.add_vline(x=full_asks[full_asks.base_volume.cumsum() < pump_volume[1]].index.max(), line_color='red')
    fig2.add_vline(x=full_asks[full_asks.base_volume.cumsum() < pump_volume[0]].index.max(), line_color='green')
    fig2.update_yaxes(
        title_text=f"Base volume {symbol}",
        secondary_y=False)
    fig2.update_yaxes(
        title_text="Factor",
        secondary_y=True)
    fig2.update_xaxes(title_text='index')
    st.plotly_chart(fig2)

    st.markdown("---")


connector = ExchangeConnector('kucoin', st.secrets.BASE_CURRENCY)
conn = st.connection("postgresql", type="sql")

available_days = conn.query("""select date(timestamp) as "collection day"
from ranked_orders.orders
group by date(timestamp);""")['collection day'].tolist()

current_day = st.sidebar.selectbox(label='Select collection day', options=available_days)

factor = st.sidebar.slider(label='Factor range: ', min_value=1, max_value=10, value=(2, 5))
volume = st.sidebar.slider(label='Volume range: ', min_value=10000, max_value=600000, value=(100000, 200000))
# Perform query.
df = conn.query(f"""
select symbol                as "Symbol",
       max(csum_base_volume) as "Cumulated Base Volume",
       max(volume_ranking)   as "Volume Ranking",
       max(factor)           as "Factor",
       max(price)            as "Price"
from ranked_orders.orders
where factor between {factor[0]} and {factor[1]}
and csum_base_volume between {volume[0]} and  {volume[1]}
and date(timestamp) = date('{current_day}')
group by symbol;
""", ttl=0)

st.text(f"On {current_day} there where {len(df)} markets of interest:")
st.dataframe(df.set_index('Symbol'), use_container_width=True)

if st.sidebar.checkbox("Show graphs "):
    for coin in df['Symbol'].tolist():
        display_order_book(coin, connector.connect(), volume)
