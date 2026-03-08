from oslm_analyst.crawlers.modelscope import MsCrawler, MsInfo
from pytest import fixture
from loguru import logger


@fixture
def ms_crawler():
    return MsCrawler(max_retry=2)


def test_ms_crawl_fetch_id(ms_crawler: MsCrawler):
    models = ms_crawler.fetch('deepseek-ai', 'DeepSeek-V3.2-Speciale', 'model')
    models = list(models)
    assert len(models) == 1
    assert isinstance(models[0], MsInfo)
    logger.info('model info:\n' + models[0].format())


def test_ms_crawl_fetch_repo(ms_crawler: MsCrawler):
    infos = ms_crawler.fetch('deepseek-ai', None, 'model')
    count = 0
    for model in infos:
        assert isinstance(model, MsInfo)
        count += 1
        logger.info('model info:\n' + model.format())
    total_count = ms_crawler.fetch_num_of('deepseek-ai', 'model')
    assert isinstance(total_count, int), f'type of total_count is {type(total_count)}'
    assert count == total_count
