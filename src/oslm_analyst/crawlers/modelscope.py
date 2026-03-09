import json
import traceback
from oslm_analyst.utils import today
from collections.abc import Iterator
from typing import Literal
from dataclasses import dataclass, field, asdict
from modelscope.hub.api import HubApi
from modelscope.hub.info import ModelInfo, DatasetInfo
from loguru import logger


@dataclass
class MsInfo:
    repo: str
    name: str
    category: Literal['model', 'dataset']
    date_crawl: str
    downloads: int | None = field(default=None)
    likes: int | None = field(default=None)
    link: str | None = field(default=None)
    readme: str | None = field(default=None)
    error: str | None = field(default=None)
    modality: Literal['language'] | None = field(default=None)
    lifecycle: Literal['pre-training'] | None = field(default=None)
    valid: bool | None = field(default=None)

    def format(self, readme: bool = False) -> str:
        if self.error is not None:
            return json.dumps(
                {
                    'repo': self.repo,
                    'name': self.name,
                    'category': self.category,
                    'date_crawl': self.date_crawl,
                    'error': self.error,
                },
                ensure_ascii=False,
                indent=2,
            )
        else:
            obj = asdict(self)
            obj.pop('error')
            if not readme:
                obj.pop('readme')
            return json.dumps(obj, ensure_ascii=False, indent=2)

    def __repr__(self):
        return self.format()

    def to_dict(self, type: Literal['error', 'config', 'output']) -> dict:
        obj = asdict(self)
        obj.pop('category')
        if self.category == 'model':
            obj.pop('lifecycle')
        match type:
            case 'error':
                return {
                    'repo': self.repo,
                    'name': self.name,
                    'category': self.category,
                    'date_crawl': self.date_crawl,
                    'error': self.error,
                }
            case 'config':
                obj.pop('date_crawl')
                obj.pop('downloads')
                obj.pop('likes')
                obj.pop('error')
            case 'output':
                obj.pop('readme')
                obj.pop('error')
        return obj


class MsCrawler:
    def __init__(self, endpoint='https://modelscope.cn', max_retry=5):
        self.endpoint = endpoint.rstrip('/')
        self.api = HubApi(self.endpoint, max_retries=max_retry)
        self.models_count = {}
        self.datasets_count = {}

    def fetch(
        self,
        repo: str,
        name: str | None,
        category: Literal['model', 'dataset'] = 'model',
    ) -> Iterator[MsInfo]:
        date_crawl = today()

        match category:
            case 'model':
                base_link = self.endpoint + '/models'
            case 'dataset':
                base_link = self.endpoint + '/datasets'

        if name is not None:
            # crawl repo/name single data.
            identifier = repo + '/' + name
            try:
                info = self._fetch_from_identifier(identifier, category)
                link = base_link + '/' + identifier
                yield MsInfo(
                    repo,
                    name,
                    category,
                    date_crawl,
                    info.downloads,
                    info.likes,
                    link,
                    info.readme_content,
                )
            except Exception:
                error = traceback.format_exc()
                yield MsInfo(repo, name, category, date_crawl, error=error)
        else:
            # Crawl all category type data from the current repo.
            pair = self._fetch_from_repo(repo, category)
            for info, error in pair:
                if info is None:
                    # TODO:Extract the model/dataset name from the error message
                    yield MsInfo(repo, 'unknown', category, date_crawl, error=error)
                else:
                    link = f'{base_link}/{repo}/{info.name}'
                    assert isinstance(info.name, str)
                    yield MsInfo(
                        repo,
                        info.name,
                        category,
                        date_crawl,
                        info.downloads,
                        info.likes,
                        link,
                        info.readme_content,
                    )

    def _fetch_from_repo(
        self, repo, category: Literal['model', 'dataset']
    ) -> Iterator[tuple[ModelInfo | DatasetInfo | None, str | None]]:
        page_size = 20
        page_number = 1
        match category:
            case 'model':
                func = self.api.list_models
                infos = func(repo, page_number, page_size)
                key = 'Models'
                self.models_count[repo] = infos['TotalCount']
                total_count = infos['TotalCount']
                Info = ModelInfo
            case 'dataset':
                func = self.api.list_datasets
                infos = func(repo, page_number=page_number, page_size=page_size)
                key = 'datasets'
                self.datasets_count[repo] = infos['total_count']
                total_count = infos['total_count']
                Info = DatasetInfo

        total_page = total_count // page_size
        if total_count % page_size != 0:
            total_page += 1

        for page_number in range(1, total_page + 1):
            try:
                infos = func(repo, page_number=page_number, page_size=page_size)
                for info in infos[key]:
                    res = Info(author=repo, **info)
                    # BUG: If the repository requires authorization, information cannot be obtained through the `repo_info` method.
                    res.readme_content = self._fetch_readme_content(
                        f'{res.author}/{res.name}', category
                    )
                    yield res, None
            except Exception:
                logger.exception(
                    f'Exception when crawl {repo} ({category}) at page {page_number} (page_size={page_size})'
                )
                error = traceback.format_exc()
                yield None, error

    def _fetch_from_identifier(
        self, identifier, category: Literal['model', 'dataset']
    ) -> ModelInfo | DatasetInfo:
        info = self.api.repo_info(identifier, repo_type=category)
        return info

    def fetch_num_of(self, repo, category: Literal['models', 'datasets']) -> int | None:
        if category == 'models':
            num = self.models_count.get(repo)
            if num is None:
                infos = self.api.list_models(repo)
                num = infos['TotalCount']
            return num
        else:
            num = self.datasets_count.get(repo)
            if num is None:
                infos = self.api.list_datasets(repo)
                num = infos['total_count']
            return num

    def _fetch_readme_content(self, identifier, category: Literal['model', 'dataset']) -> str:
        info = self.api.repo_info(identifier, repo_type=category)
        if isinstance(info.readme_content, str):
            return info.readme_content
        else:
            return ''
