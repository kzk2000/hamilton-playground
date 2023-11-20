"""
For this to work, you need to add this fix: https://github.com/DAGWorks-Inc/hamilton/pull/545/files#diff-36163122fda0a94ce7ce6d134ce42cb88f35682c8027ef1d8cded4ae6a3dc238R269
"""
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
    'data_agg1',
    'data_agg2',
    'output_agg1',
    'output_agg2',
]

out = dr.execute(output_columns)
print(out)

if False:
    # print for testing
    list(out['data_agg1'])
    list(out['data_agg2'])

    cache_file = '/home/user/hamilton_test.dot'
    dr.visualize_execution(output_columns, cache_file, {'format': 'png'})
