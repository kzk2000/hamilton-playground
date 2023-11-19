import my_hamilton_nodes as mh
import numpy as np
import pandas as pd

import my_hamilton_nodes
from hamilton import driver, base, telemetry
from hamilton.execution import executors
from hamilton.function_modifiers import value, source
import json

telemetry.disable_telemetry()

# Ultimate objective
# 1. easy way to define multi metrics from single source table -- done
# 2. custom naming of final metric names or default names -- done
# 3. Parallelizable


inputs = {  # load from actuals or wherever -- this is our initial data we use as input.
    # Note: these do not have to be all series, they could be scalar inputs.
    'start_date': '2023-11-01',
    'end_date': '2023-11-10',
    'resample': 'W-SUN',
    #'aggs': {'price': ['mean', 'last'], 'volume': ['sum']},  # default column naming
    'aggs': {
        'openp': ('price', 'first'),
        'highp': ('price', 'max'),
        'lowp': ('price', 'min'),
        'closep': ('price', 'last'),
        'volume': ('volume', 'sum'),
        'volume_mean': ('volume', lambda x: np.mean(x)),
        #'volume_median': ('volume', lambda x: np.median(x)),
    },
    #'tickers': ['B'],
}


config = {}

dr = (
    driver.Builder()
    .with_modules(my_hamilton_nodes)
    .enable_dynamic_execution(allow_experimental_mode=True)
    #.with_local_executor(executors.SynchronousLocalTaskExecutor())
    .with_config(config)
    .build()
)

output_columns = [
    #'stock_data',
    'final_stats'
]


out = dr.execute(output_columns, inputs=inputs)
print('\n**************************\nOutput:')
# print(out)
print(out['final_stats'])


run_it = False
if run_it:
    ticker_df = my_hamilton_nodes.stock_data().query("ticker=='A'")

    cache_file = '/home/user/hamilton_test.dot'
    dr.visualize_execution(output_columns, cache_file, {'format': 'png'})
