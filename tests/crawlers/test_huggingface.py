from oslm_analyst.crawlers.huggingface import HfCrawler, HfInfo
from pytest import fixture


@fixture
def hf_crawler():
    return HfCrawler(max_retry=1)


def test_hf_crawl_fetch_id(hf_crawler: HfCrawler):
    models = hf_crawler.fetch('deepseek-ai', 'DeepSeek-V3.2-Speciale', 'model')
    models = list(models)
    assert len(models) == 1
    assert isinstance(models[0], HfInfo)
    print(models[0])


def test_hf_crawl_fetch_repo(hf_crawler: HfCrawler):
    for model in hf_crawler.fetch('deepseek-ai', None, 'model'):
        assert isinstance(model, HfInfo)
