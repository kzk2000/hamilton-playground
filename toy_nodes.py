from hamilton.htypes import Parallelizable, Collect
from hamilton.function_modifiers import parameterize, value, source

# @parameterize(
#     urls=dict(aa=value(1))
# )
# def urls_node(aa: int = 10) -> Parallelizable[str]:
#     print(f'aa={aa}')
#     for url_ in ['url_a', 'url_b']:
#         yield url_

def urls() -> Parallelizable[str]:

    for url_ in ['url_a', 'url_b']:
        yield url_

def counts(urls: str) -> int:
    print(urls)
    return len(urls.split("_"))


def total_words(counts: Collect[int]) -> int:
    return sum(counts)


