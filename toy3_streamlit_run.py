import numpy as np
import pandas as pd
import streamlit as st
import datetime
import toy3_streamlit_nodes
from hamilton import driver, base, telemetry
from hamilton.execution import executors
from hamilton.function_modifiers import value, source
import json
import plotly_express as px
import plotly.graph_objects as go

telemetry.disable_telemetry()

st.set_page_config(page_title="Hamilton + Streamlit")
st.title('Streamlit + Hamilton callbacks')

col1, col2, col3, col4 = st.columns(4)

with col1:
    start_date = st.date_input("**Start Date**", datetime.date(2023, 10, 1))  # Monday

with col2:
    end_date = st.date_input("**End Date**", datetime.date(2023, 12, 31))  # Sunday

with col3:
    ticker = st.selectbox("**Ticker**", ['A', 'B', 'C'], index=0)

with col4:
    resample = st.selectbox("**Re-sample**", ['D', 'W', 'M'], index=1)

selectable_aggs = {
    # column_name = ('source_column', 'pandas allowed stat func as string or lambda')
    'openp': ('price', 'first'),
    'highp': ('price', 'max'),
    'lowp': ('price', 'min'),
    'closep': ('price', 'last'),
    'volume': ('volume', 'sum'),
    'adv': ('volume', lambda x: np.mean(x)),  # or use ('volume', 'mean') as usual
    'mdv': ('volume', 'median'),
}

st.write("\tSelected 1 field for line chart")
st.write("\tSelected ['openp', 'highp', 'lowp', 'closep'] in any order for OHLC chart")

selected_aggs = st.multiselect("Select fields", options=selectable_aggs.keys(), default=['closep'])
st.write("**Selected fields**", selected_aggs)

inputs = {
    'start_date': start_date.strftime("%Y-%m-%d"),
    'end_date': end_date.strftime("%Y-%m-%d"),
    'ticker': ticker,
    'resample': resample,
    'aggs': {key: selectable_aggs[key] for key in selected_aggs},
}

config = {}

dr = (
    driver.Builder()
    .with_modules(toy3_streamlit_nodes)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_config(config)
    .build()
)

output_columns = [
    # 'ticker_df',  # raw table
    'ticker_aggs'
]

data = dr.execute(output_columns, inputs=inputs)['ticker_aggs']
print('\n**************************\nOutput:')
print(data)

if len(selected_aggs) == 1:
    # Plotly line chart
    fig = px.line(data, x='date', y=selected_aggs[0])
    fig.update_layout(title='Line Chart Example',
                      xaxis_title='',
                      yaxis_title=selected_aggs[0],
                      xaxis_rangeslider_visible=False)
    st.plotly_chart(fig)

elif set(selected_aggs) == set(['openp', 'highp', 'lowp', 'closep']):
    # Plotly OHLC chart
    fig = go.Figure(data=[go.Candlestick(x=data['date'],
                                         open=data['openp'],
                                         high=data['highp'],
                                         low=data['lowp'],
                                         close=data['closep'])])

    # Update layout for better visualization
    fig.update_layout(title='OHLC Chart Example',
                      xaxis_title='',
                      yaxis_title='Price',
                      xaxis_rangeslider_visible=False)

    # Display the chart using st.plotly_chart
    st.plotly_chart(fig)

    # re-order before st.table() call
    data = data[['date', 'from', 'to', 'openp', 'highp', 'lowp', 'closep']]
else:
    # no chart, only print data as table
    pass

st.table(data)

