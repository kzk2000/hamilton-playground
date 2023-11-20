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
    """Number of words"""
    return len(data_agg1.split("_"))


def agg_2(data_agg2: str) -> int:
    """Total length"""
    return len(data_agg2)


@parameterize(
    output_agg1=dict(upstream_source=source('agg_1')),
    output_agg2=dict(upstream_source=source('agg_2')),
)
def output_node(upstream_source: Collect[int]) -> int:
    return sum(upstream_source)




