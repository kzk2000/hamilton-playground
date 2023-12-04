import pandas as pd


def foobar(bar: pd.Series, foo: pd.Series) -> pd.DataFrame:
    df = pd.concat([bar, foo], axis=1)
    df.columns = ['x1', 'x2']
    return df
