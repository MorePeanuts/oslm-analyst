from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver import ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager


def init_single_driver() -> WebDriver:
    options = ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver


def str2int(s: str | None) -> int:
    """
    Examples:
    -----
    >>> str2int('295,137')
    295137
    >>> str2int('1.7k')
    1700
    >>> str2int('3.1m')
    3100000
    >>> str2int('38k')
    38000
    >>> str2int('')
    0
    >>> str2int(None)
    0
    >>> str2int('-')
    0
    >>> str2int(1234)
    1234
    """
    if isinstance(s, int):
        return s
    if s is None or s == '' or s == '-':
        return 0
    if ',' in s:
        s = s.replace(',', '')
    try:
        if 'k' in s or 'K' in s:
            s = s.replace('k', '').replace('K', '')
            return int(float(s) * 1_000)
        elif 'm' in s or 'M' in s:
            s = s.replace('m', '').replace('M', '')
            return int(float(s) * 1_000_000)
        elif 'b' in s or 'B' in s:
            s = s.replace('b', '').replace('B', '')
            return int(float(s) * 1_000_000_000)
        else:
            return int(s)
    except ValueError as e:
        raise Exception('str2int error') from e
