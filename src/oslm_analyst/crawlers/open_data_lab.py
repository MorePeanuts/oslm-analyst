import re
from dataclasses import dataclass, field
from loguru import logger
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from .crawl_utils import str2int


@dataclass
class OpenDataLabInfo:
    org: str = field(init=False, default='ShanghaiAILab')
    repo: str = field(init=False)
    dataset_name: str = field(init=False)
    total_downloads: int | None = field(init=False, default=None)
    likes: int | None = field(init=False, default=None)
    date_crawl: str = field()
    link: str = field()
    metadata: dict | None = field(default=None)

    def __post_init__(self):
        self.dataset_name = self.link.rstrip('/').split('/')[-1]
        self.repo = self.link.rstrip('/').split('/')[-2]
        if self.metadata is not None:
            self.total_downloads = str2int(self.metadata.get('downloads', 0))
            self.likes = str2int(self.metadata.get('likes', 0))

    def __eq__(self, other) -> bool:
        if not isinstance(other, OpenDataLabInfo):
            return False

        return self.link == other.link

    def __hash__(self) -> int:
        return hash(self.link)


class OpenDataLabCrawler:
    _main_parts = [
        (By.XPATH, '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div'),
    ]
    _total_count = (
        By.CSS_SELECTOR,
        r'#root > div > div > main > div > div.layout.pt-8.flex.flex-col.bg-clip-content > div.flex-1.pt-\[57px\].mt-0 > div > div > div.flex.mb-4.items-center > div > div.text-base.mr-auto.flex.items-center.ml-4 > div',
    )
    _page_elements = (
        By.XPATH,
        '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div/div',
    )
    _page_first_element = (
        By.XPATH,
        '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/div/div[1]/a',
    )
    _page_navigation = (
        By.XPATH,
        '//*[@id="root"]/div/div/main/div/div[2]/div[3]/div/div/div[2]/div[2]/ul',
    )
    max_count_per_page = 12

    def __init__(self, driver: WebDriver):
        self.driver = driver

    def scrape(self, link: str) -> list[OpenDataLabInfo]:
        date_crawl = str(datetime.today().date())
        self.driver.get(link)
        res = []

        # Waiting until main part of the page appears
        try:
            for part in self._main_parts:
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(part))
        except Exception as e:
            logger.debug(f'Error when waiting main part appearence. Exception {e} is throwed.')
            raise e

        # Scrape information page by page
        try:
            total_count = self._get_total_count()
            if total_count > self.max_count_per_page:
                max_page_number = self._get_max_page_number()
            else:
                max_page_number = 1

            for page in range(1, max_page_number + 1):
                elem_divs = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_all_elements_located(self._page_elements)
                )
                first_elem = None

                # Scrape infomation on current page
                for div in elem_divs:
                    sublink = div.find_element(By.XPATH, './a').get_attribute('href')
                    downloads = div.find_element(By.XPATH, './a/div[2]/div[2]/span[last()]').text
                    likes = div.find_element(By.XPATH, './a/div[1]/div[2]/div[1]/div/span').text
                    first_elem = div if first_elem is None else first_elem
                    assert isinstance(sublink, str)
                    res.append(
                        OpenDataLabInfo(
                            date_crawl=date_crawl,
                            link=sublink,
                            metadata={'downloads': downloads, 'likes': likes},
                        )
                    )

                # Go to next page
                if page != max_page_number:
                    self._next_page(first_elem)
        except Exception as e:
            logger.debug(f'Error when scrape information: {e}')
            raise e

        return res

    def _get_max_page_number(self) -> int:
        try:
            navigation = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._page_navigation)
            )
            max_page_number = navigation.find_element(By.XPATH, './li[last()-2]').get_attribute(
                'title'
            )
            return str2int(max_page_number)
        except Exception:
            return 1

    def _get_total_count(self) -> int:
        try:
            total_count_text = (
                WebDriverWait(self.driver, 5)
                .until(EC.presence_of_element_located(self._total_count))
                .text
            )
            if m := re.search(r'([\d,]+)\s*数据集', total_count_text):
                total_count = m.group(1)
                return str2int(total_count)
            else:
                return 0
        except Exception:
            raise

    def _next_page(self, last_first_elem: WebElement | None):
        if last_first_elem is None:
            return
        try:
            navigation = WebDriverWait(self.driver, 5).until(
                EC.presence_of_element_located(self._page_navigation)
            )
            next_button = navigation.find_element(By.XPATH, './li[last()-1]')
            next_button.click()
            # BUG: Next page wait strategy is not an atomic operation
            WebDriverWait(self.driver, 5).until(EC.staleness_of(last_first_elem))
        except Exception:
            raise
