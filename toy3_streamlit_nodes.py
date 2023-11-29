import random
import typing as ta
from datetime import datetime, timedelta

import pandas as pd
from hamilton.function_modifiers import extract_columns


def stock_data(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    random.seed(42)
    dates = pd.date_range('2023-10-01', '2023-12-31')
    ticker_list = ['A', 'B', 'C']
    data = []
    for date in dates:
        for ticker in ticker_list:
            price = round(random.uniform(50, 200), 2)
            volume = random.randint(10000, 50000)
            data.append([date, ticker, price, volume])

    df = pd.DataFrame(data, columns=['date', 'ticker', 'price', 'volume']).set_index(['date'])
    if start_date is not None:
        df = df[pd.Timestamp(start_date):].copy()

    if end_date is not None:
        df = df[:pd.Timestamp(end_date)].copy()

    return df


def ticker_df(stock_data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    return stock_data[stock_data.ticker == ticker]


def _reshape_agg(aggs: dict):
    """Wrapper to cast into consistent format that's passed to pd.Dataframe.agg(...)"""
    out = {}
    for key, item in aggs.items():
        if isinstance(item, list):
            for agg_name in item:
                out[f'{key}_{agg_name}'] = (key, agg_name)
        elif isinstance(item, tuple) and len(item) == 2:
            out[key] = item
        else:
            raise ValueError("'aggs is not properly defined")

    return out


def ticker_aggs(ticker_df: pd.DataFrame, aggs: dict = None, resample: str = None) -> pd.DataFrame:
    if aggs is None:
        aggs = {'volume': ['sum']}

    aggs_r = {'TICKER': ('ticker', 'max'), 'FROM': ('date', 'min'), 'TO': ('date', 'max'), **_reshape_agg(aggs)}

    if resample is not None:
        tmp = ticker_df.copy()
        tmp['date'] = ticker_df.index  # make date a column that's accessible by .agg() to retrieve from/to date
        df = tmp.resample(resample).agg(**aggs_r).rename(columns=str.lower).reset_index()
        ordered_cols = ['ticker', 'date', 'from', 'to'] + [col for col in df.columns if col not in ['date', 'from', 'to', 'ticker']]
    else:
        df = ticker_df.reset_index().groupby('ticker').agg(**aggs_r).rename(columns=str.lower).reset_index(drop=True)
        df.insert(3, 'num_days', len(ticker_df))
        ordered_cols = ['ticker', 'from', 'to'] + [col for col in df.columns if col not in ['from', 'to', 'ticker']]

    return df[ordered_cols]
