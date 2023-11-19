import my_hamilton_nodes as mh
import pandas as pd

import toy_nodes
from hamilton import driver, base, telemetry
from hamilton.execution import executors

telemetry.disable_telemetry()

config = {}

dr = (
    driver.Builder()
    .with_modules(toy_nodes)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_local_executor(executors.SynchronousLocalTaskExecutor())
    .with_config(config)
    .build()
)

output_columns = [
    'total_words',
    'total_words2',
    # having both 'total_words' and 'total_word2' enabled leads to this error
    #   File "/home/user/Programs/anaconda3/envs/py310/lib/python3.10/site-packages/hamilton/execution/state.py", line 113, in read
    #     raise KeyError(f"Key {formatted_key} not found in cache")
    # KeyError: 'Key total_words2 not found in cache'
]


out = dr.execute(output_columns)
print(out)
