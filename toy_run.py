
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

dr.list_available_variables()

output_columns = [
    'urls',
    'total_words',
]

cache_file = '/home/user/hamilton_test.dot'
dr.visualize_execution(output_columns, cache_file, {'format': 'png'})




out = dr.execute(output_columns)
print(out)


if False:
    pass
    # Variable(name='counts', type=<class 'int'>, tags={'module': 'toy_nodes'}, is_external_input=False, originating_functions=(<function counts at 0x7fe7303bd1b0>,)),
    # Variable(name='total_words', type=<class 'int'>, tags={'module': 'toy_nodes'}, is_external_input=False, originating_functions=(<function total_words at 0x7fe7303bcdc0>,)),
    # Variable(name='urls', type=hamilton.htypes.Parallelizable[str], tags={'module': 'toy_nodes'}, is_external_input=False, originating_functions=(<function urls_node at 0x7fe7303bcca0>,))