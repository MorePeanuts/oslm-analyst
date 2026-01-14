import pytest
from oslm_analyst.crawlers.open_data_lab import OpenDataLabCrawler
from oslm_analyst.crawlers.crawl_utils import init_single_driver


@pytest.fixture(scope='class')
def driver(request):
    web_driver = init_single_driver()
    request.cls.driver = web_driver
    yield
    web_driver.quit()


@pytest.mark.parametrize(
    'link',
    [
        'https://opendatalab.com/?createdBy=12199&pageNo=0&pageSize=12&sort=downloadCount',
        'https://opendatalab.com/?createdBy=11828&pageNo=0&pageSize=12&sort=downloadCount',
        'https://opendatalab.com/?createdBy=12157&pageNo=0&pageSize=12&sort=downloadCount',
        'https://opendatalab.com/?createdBy=12589&pageNo=0&pageSize=12&sort=downloadCount',
        'https://opendatalab.com/?createdBy=1678533&pageNo=0&pageSize=12&sort=downloadCount',
    ],
)
@pytest.mark.usefixtures('driver')
class TestOpenDataLabPage:
    def test_crawl(self, link):
        page = OpenDataLabCrawler(self.driver)  # type: ignore
        res = page.scrape(link)
        assert isinstance(res, list)
        assert len(res) == len(set(res))
