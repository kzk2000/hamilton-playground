import pandas as pd
import numpy as np


def bar() -> pd.Series:
    np.random.seed(42)
    return pd.Series(np.random.randn(10), index=pd.date_range('2023-01-01', periods=10))


