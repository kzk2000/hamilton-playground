import my_hamilton_nodes as mh
import numpy as np
import pandas as pd

import my_hamilton_nodes
from hamilton import driver, base, telemetry
from hamilton.execution import executors
from hamilton.function_modifiers import value, source
import json

telemetry.disable_telemetry()

# Ultimate objectives
# 1. easy way to define multi metrics from single source table -- done
# 2. custom naming of final metric names or default names -- done
# 3. Parallelizable -- done
# 4. Pending
#       * Rolling stats
#       * Weighted stats


inputs = {  # load from actuals or wherever -- this is our initial data we use as input.
    # Note: these do not have to be all series, they could be scalar inputs.
    'start_date': '2023-11-01',
    'end_date': '2023-11-10',
    'resample': 'W',
    # 'aggs': {'price': ['mean', 'last'], 'volume': ['sum']},  # default column naming
    'aggs': {
        # colunm_name = ('source_column', 'pandas allowed stat func as string or lambda')
        'openp': ('price', 'first'),
        'highp': ('price', 'max'),
        'lowp': ('price', 'min'),
        'closep': ('price', 'last'),
        'volume': ('volume', 'sum'),
        'adv': ('volume', lambda x: np.mean(x)),  # of course, ('volume', 'mean') as usual works too
        'mdv': ('volume', 'median'),
    },

    # filter for specific tickers
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
    'final_stats',
    'final_stats2',
    #'ticker_df',
    #'ticker_list'
    #'adv_5d'
]


out = dr.execute(output_columns, inputs=inputs)
print('\n**************************\nOutput:')
print(out)
#print(out['final_stats'])


run_it = False
if run_it:
    ticker_df = my_hamilton_nodes.stock_data().query("ticker=='A'")

    cache_file = '/home/user/hamilton_test.dot'
    dr.visualize_execution(output_columns, cache_file, {'format': 'png'})
