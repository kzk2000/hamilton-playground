from hamilton.htypes import Parallelizable, Collect
from hamilton.function_modifiers import parameterize, value, source
from typing import List


@parameterize(
    data_agg1=dict(input_values=value(['url10_a', 'url10_b'])),
    data_agg2=dict(input_values=value(['url20_a', 'url20_b'])),
)
def data_node(input_values: List) -> Parallelizable[str]:
    for url_ in input_values:
        yield url_


def agg_1(data_agg1: str) -> int:
    print('**********\nagg_1')
    print(type(data_agg1))  # comes in as generator?
    print(list(data_agg1))
    return len(data_agg1.split("_"))


def agg_2(data_agg2: str) -> int:
    print('**********\nagg_2')
    print(type(data_agg2))  # comes in as generator?
    print(list(data_agg2))
    return 20
    #return len(data_agg2)


@parameterize(
    output_agg1=dict(upstream_source=source('agg_1')),
    output_agg2=dict(upstream_source=source('agg_2')),
)
def output_node(upstream_source: Collect[int]) -> int:
    return upstream_source




