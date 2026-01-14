import requests
from loguru import logger
from datetime import datetime
from dataclasses import dataclass, field


@dataclass
class BAAIDataInfo:
    org: str = field(init=False, default='BAAI')
    repo: str = field(init=False, default='BAAI')
    dataset_name: str = field()
    total_downloads: int | None = field()
    likes: int | None = field()
    date_crawl: str = field()
    link: str = field()


class BAAIDataCrawler:
    def __init__(self):
        self._init_headers()
        self._init_cookies()

    def scrape(self) -> list[BAAIDataInfo] | str:
        date_crawl = str(datetime.today().date())
        data = {
            'limit': 1000,
            'offset': 0,
            'datasetName': '',
            'startTime': None,
            'endTime': None,
            'orderBy': '5',
        }
        try:
            infos = self._send_post_request(data)
            res = []
            for link, info in infos.items():
                res.append(
                    BAAIDataInfo(
                        dataset_name=info['uriName'],
                        total_downloads=info['downloadNumb'],
                        likes=info['subscribedNumb'],
                        date_crawl=date_crawl,
                        link=link,
                    )
                )
            return res
        except Exception as e:
            logger.debug(f'Error when scrape information from BAAIData: {e}')
            raise e

    def _init_headers(self):
        self.headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Authorization': 'Bearer',
            'Origin': 'https://data.baai.ac.cn',
            'Referer': 'https://data.baai.ac.cn/dataset',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        }

    def _init_cookies(self):
        self.cookies = {
            'remember_web_59ba36addc2b2f9401580f014c7f58ea4e30989d': 'eyJpdiI6InpNbWpGclRRZGZuZEdkMzZYL09YSVE9PSIsInZhbHVlIjoiMGtVTzh2SWJ0L3hHb1NLeEl6SU9FUC9VbUk5YzVUWWpjeTdsOHpoenB2aG5Vb29wcm9zUlBaV3FudUFwc2VobC8zaU5SSEZhNmZSNCtwUWdJcG5KeFJJUXVUVy92Nnk0MmhiSXVkS1NIVzY1WHo3bSsvcHlyYlp3VXlsdXpYbEdwYVloeDIxUzUzMi9YeDBLUGMxLzdnPT0iLCJtYWMiOiJkZjhmYzkzZmZkZmFkYjAwMDVjMjNlYTdjMDNiMWZmNGJiYTBlNmRlZmEzNzUyMzA0MmRkNzA2MDE1ZmExMDdjIn0%3D',
            'guidance-profile-2024': 'yes',
            'hubapi_session': 'eyJpdiI6IjU4VjRWQjE4Sm5lZ0F2Um9qTkUraHc9PSIsInZhbHVlIjoiallaZzVjNzE3Snd1bzlWWDF0bDZvUTdiVG5nQldPWUpDNjcreWMwT29qTHpIYkY4NUJWMmxjdFJidkl0NEZhNkxHRC9LSDRPbDRhV0JwL0J5aFJ4MmF3U3NTZ2x4cGc3VWUyNnJEeVZlQjhwczBCb0VHaHkreTg0VGhHYWZKZjEiLCJtYWMiOiI3NTA1NzZiMDRiMjcwZmQ0NTkyM2QxN2VlZTZlYTg5MzkxNWUwOTMwNjBkMTkxMTcwNDNiMGUyZDQwMzU2ZTE1In0%3D',
            '_tea_utm_cache_1229': 'undefined',
        }

    def _send_post_request(self, data):
        try:
            response = requests.post(
                'https://data.baai.ac.cn/api/datahub/search/v1/getAllDataset',
                headers=self.headers,
                cookies=self.cookies,
                json=data,
            )
            response.raise_for_status()  # Raise an exception for bad status codes
            response_json = response.json()

            self.total_targets = response_json.get('data', {}).get('total', 0)
            data_list = response_json.get('data', {}).get('list', [])

            # Construct dataset URLs
            all_info = {}
            for item in data_list:
                URL = 'https://data.baai.ac.cn/datadetail/' + item['uriName']
                all_info[URL] = item
            return all_info
        except Exception:
            raise
