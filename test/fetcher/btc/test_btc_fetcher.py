from app.fetcher.btc.btc_fetcher import BitcoinFetcher
from pytest import fixture


@fixture
def fetcher():
    return BitcoinFetcher()

def test_fetch_block(fetcher: BitcoinFetcher):
    ret = fetcher.fetch_block()
    # print(ret.json())
    