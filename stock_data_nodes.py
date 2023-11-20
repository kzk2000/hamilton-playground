import random
import typing as ta
from datetime import datetime, timedelta

import pandas as pd
from hamilton.function_modifiers import extract_columns
from hamilton.htypes import Parallelizable, Collect



def stock_data(start_date: str = None, end_date: str = None, tickers: list = None) -> pd.DataFrame:
    random.seed(42)
    dates = pd.date_range('2023-10-30', '2023-12-31')
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

    if tickers is not None:
        df = df[df.ticker.isin(tickers)].copy()

    return df


def ticker_df(stock_data: pd.DataFrame) -> Parallelizable[pd.DataFrame]:
    for ticker, df in stock_data.groupby('ticker'):
        yield df


def ticker_list(stock_data: pd.DataFrame) -> list:
    return sorted(stock_data['ticker'].unique().tolist())


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
        tmp['date'] = ticker_df.index  # make date a column to be accesible by .agg()
        df = tmp.resample(resample).agg(**aggs_r).rename(columns=str.lower).reset_index()
        ordered_cols = ['ticker', 'date', 'from', 'to'] + [col for col in df.columns if col not in ['date', 'from', 'to', 'ticker']]
    else:
        df = ticker_df.reset_index().groupby('ticker').agg(**aggs_r).rename(columns=str.lower).reset_index(drop=True)
        df.insert(3, 'num_days', len(ticker_df))
        ordered_cols = ['ticker', 'from', 'to'] + [col for col in df.columns if col not in ['from', 'to', 'ticker']]

    return df[ordered_cols]



def final_stats(ticker_aggs: Collect[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(ticker_aggs, ignore_index=True)
    return df


def final_stats2(ticker_aggs: Collect[pd.DataFrame]) -> pd.DataFrame:
    df = pd.concat(ticker_aggs, ignore_index=True)
    return df



def ticker_adv_5d(ticker_df: pd.DataFrame) -> pd.DataFrame:
    return ticker_df.set_index(['ticker'], append=True)['volume'].rolling(5).mean().reset_index()

def adv_5d(ticker_adv_5d: Collect[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(ticker_adv_5d, axis=0)

# FIXME: make a generic Collect with @resolve for each requested field
def adv_5d_with_resolve(ticker_adv_5d: Collect[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(ticker_adv_5d, axis=0)


