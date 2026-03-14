import json
import traceback
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import Literal

from loguru import logger
from modelscope.hub.api import HubApi
from modelscope.hub.info import DatasetInfo, ModelInfo
from requests.exceptions import HTTPError
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
)

from oslm_analyst.data_utils import MsInfo
from oslm_analyst.utils import today


def _is_rate_limit_error(exception):
    return isinstance(exception, HTTPError) and exception.response.status_code == 429


def ms_wait_logit(retry_state):
    exc = retry_state.outcome.exception()

    if isinstance(exc, HTTPError):
        retry_after = exc.response.headers.get('Retry-After')
        if retry_after:
            return float(retry_after)

    return 60.0


class MsCrawler:
    def __init__(self, endpoint='https://modelscope.cn', max_retry=5):
        self.endpoint = endpoint.rstrip('/')
        self.api = HubApi(self.endpoint, max_retries=max_retry)
        self.retrier = Retrying(
            reraise=False,
            retry=retry_if_exception(_is_rate_limit_error),
            wait=ms_wait_logit,
            stop=stop_after_attempt(max_retry),
        )
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
                    )

    def _fetch_from_repo(
        self, repo, category: Literal['model', 'dataset']
    ) -> Iterator[tuple[ModelInfo | DatasetInfo | None, str | None]]:
        # WARNING: Changing it to other numbers may cause a bug, which could be an issue with the modelscope library itself.
        page_size = 10
        page_number = 1
        match category:
            case 'model':
                func = partial(self.retrier, self.api.list_models)
                infos = func(repo, page_number, page_size)
                key = 'Models'
                self.models_count[repo] = infos['TotalCount']
                total_count = infos['TotalCount']
                Info = ModelInfo
            case 'dataset':
                func = partial(self.retrier, self.api.list_datasets)
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
                    # BUG: The infos[key] here may not have retrieved the correct list due to network issues, leading to a TypeError (NoneType) BUG.
                    res = Info(author=repo, **info)
                    # WARNING: In modelscope, the id information in DatasetInfo and ModelInfo is inconsistent. Here is a temporary solution, which may become invalid at any time due to interface changes.
                    if category == 'dataset':
                        assert isinstance(res.id, str)
                        res.name = res.id.split('/')[-1]
                    yield res, None
            except Exception:
                # TODO: The error retry mechanism needs improvement; currently, if an exception occurs, the code cannot identify on which page the exception was encountered.
                logger.exception(
                    f'Exception when crawl {repo} ({category}) at page {page_number} (page_size={page_size})'
                )
                error = traceback.format_exc()
                yield None, error

    def _fetch_from_identifier(
        self, identifier, category: Literal['model', 'dataset']
    ) -> ModelInfo | DatasetInfo:
        try:
            return self.retrier(self.api.repo_info, identifier, repo_type=category)
        except RetryError:
            logger.exception(
                f'Max retry exceeded when fetch {category} information from {identifier}.'
            )
            raise
        except Exception:
            logger.exception(f'Exception when fetch {category} information from {identifier}.')
            raise

    def fetch_num_of(self, repo, category: Literal['models', 'datasets']) -> int | None:
        try:
            if category == 'models':
                num = self.models_count.get(repo)
                if num is None:
                    infos = self.retrier(self.api.list_models, repo)
                    num = infos['TotalCount']
                return num
            else:
                num = self.datasets_count.get(repo)
                if num is None:
                    infos = self.retrier(self.api.list_datasets, repo)
                    num = infos['total_count']
                return num
        except RetryError:
            logger.exception(f'Exception when fetch num of {category} of {repo}')
            raise

    def fetch_readme_content(self, identifier, category: Literal['model', 'dataset']) -> str:
        try:
            info = self.retrier(self.api.repo_info, identifier, repo_type=category)
            if isinstance(info.readme_content, str):
                return info.readme_content
            else:
                return ''
        except RetryError:
            logger.exception(f'Max retry exceeded when fetch {category} readme of {identifier}.')
            raise
        except Exception:
            logger.debug(f'No readme field found in {identifier}.')
            return ''
