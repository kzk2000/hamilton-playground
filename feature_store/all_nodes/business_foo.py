import pandas as pd
import numpy as np


def foo() -> pd.Series:
    np.random.seed(42)
    return pd.Series(np.random.rand(10), pd.date_range('2023-01-01', periods=10))

