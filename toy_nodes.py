from hamilton.htypes import Parallelizable, Collect


def url() -> Parallelizable[str]:
    for url_ in ['url_a', 'url_b']:
        yield url_


def counts(url: str) -> int:
    return len(url_loaded.split("_"))


def total_words(counts: Collect[int]) -> int:
    return sum(counts)


def total_words2(counts: Collect[int]) -> int:
    return sum(counts)
