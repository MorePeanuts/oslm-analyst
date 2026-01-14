from oslm_analyst.crawlers.baai_data import BAAIDataCrawler, BAAIDataInfo


def test_baai_data_page():
    page = BAAIDataCrawler()
    res = page.scrape()
    assert isinstance(res, list)
    assert len(res) > 0
    print(len(res))
    assert isinstance(res[0], BAAIDataInfo)
