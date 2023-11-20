import my_hamilton_nodes as mh
import pandas as pd

import toy2_nodes
from hamilton import driver, base, telemetry
from hamilton.execution import executors

telemetry.disable_telemetry()

config = {}

dr = (
    driver.Builder()
    .with_modules(toy2_nodes)
    .enable_dynamic_execution(allow_experimental_mode=True)
    .with_local_executor(executors.SynchronousLocalTaskExecutor())
    .with_config(config)
    .build()
)

dr.list_available_variables()

output_columns = [
    #'data_agg1',   # works fine
    #'data_agg2',   # works fine

    'output_agg1',  # throws

    # 'output_agg2',  # throws
]


out = dr.execute(output_columns)
print(out)

if False:
    # print for testing
    list(out['data_agg1'])
    list(out['data_agg2'])