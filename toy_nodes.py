from hamilton.htypes import Parallelizable, Collect


def urls() -> Parallelizable[str]:
    for url_ in ['url_a', 'url_b']:
        yield url_


def counts(urls: str) -> int:
    return len(urls.split("_"))


def total_words(counts: Collect[int]) -> int:
    return sum(counts)


def total_words2(counts: Collect[int]) -> int:
    return sum(counts)
