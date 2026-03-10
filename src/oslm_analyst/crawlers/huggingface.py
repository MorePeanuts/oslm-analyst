import re
from huggingface_hub.errors import HfHubHTTPError
import json
import traceback
from loguru import logger
from typing import Literal
from collections.abc import Iterator
from dataclasses import dataclass, field, asdict
from huggingface_hub import HfApi, ModelCard, DatasetCard
from huggingface_hub.hf_api import ModelInfo, DatasetInfo
from tenacity import (
    Retrying,
    RetryError,
    stop_after_attempt,
    retry_if_exception,
)
from .crawl_utils import str2int
from ..utils import today


@dataclass
class HfInfo:
    repo: str
    name: str
    category: Literal['model', 'dataset']
    date_crawl: str
    downloads_last_month: int | None = field(default=None)
    likes: int | None = field(default=None)
    discussions: int | None = field(default=None)
    discussion_msg: int | None = field(default=None)
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
                obj.pop('downloads_last_month')
                obj.pop('likes')
                obj.pop('discussions')
                obj.pop('discussion_msg')
                obj.pop('error')
            case 'output':
                obj.pop('readme')
                obj.pop('error')
        return obj


def _is_rate_limit_error(exception):
    return isinstance(exception, HfHubHTTPError) and exception.response.status_code == 429


def hf_wait_logic(retry_state):
    exc = retry_state.outcome.exception()

    if isinstance(exc, HfHubHTTPError):
        retry_after = exc.response.headers.get('Retry-After')
        if retry_after:
            return float(retry_after)

        # "Retry after 55 seconds (0/500 requests remaining...)"
        server_msg = str(exc)
        match = re.search(r'Retry after (\d+) seconds', server_msg)
        if match:
            return float(match.group(1))

    return 60.0


class HfCrawler:
    def __init__(
        self, token: str | bool | None = None, endpoint='https://huggingface.co', max_retry=5
    ):
        self.endpoint = endpoint.rstrip('/')
        self.api = HfApi(endpoint=self.endpoint, token=token)
        # reraise=False: raise RetryError when max retry exceeded
        self.retrier = Retrying(
            reraise=False,
            retry=retry_if_exception(_is_rate_limit_error),
            wait=hf_wait_logic,
            stop=stop_after_attempt(max_retry),
        )

    def fetch(
        self,
        repo: str,
        name: str | None,
        category: Literal['model', 'dataset'] = 'model',
    ) -> Iterator[HfInfo]:
        date_crawl = today()

        match category:
            case 'model':
                base_link = self.endpoint
            case 'dataset':
                base_link = self.endpoint + '/datasets'

        if name is not None:
            # Crawl repo/name single data.
            identifier = repo + '/' + name
            try:
                info = self._fetch_from_identifier(identifier, category)
                disc, msg = self._fetch_discussions_count(identifier)
                readme = self._fetch_readme_content(identifier, category)
                link = base_link + '/' + identifier
                yield HfInfo(
                    repo,
                    name,
                    category,
                    date_crawl,
                    info.downloads,
                    info.likes,
                    disc,
                    msg,
                    link,
                    readme,
                )
            except Exception:
                error = traceback.format_exc()
                yield HfInfo(repo, name, category, date_crawl, error=error)
        else:
            # Crawl all category type data from the current repo.
            pair = self._fetch_from_repo(repo, category)
            for info, error in pair:
                if info is None:
                    # TODO:Extract the model/dataset name from the error message
                    yield HfInfo(repo, '', category, date_crawl, error=error)
                else:
                    identifier = info.id
                    try:
                        disc, msg = self._fetch_discussions_count(identifier)
                        readme = self._fetch_readme_content(identifier, category)
                        link = base_link + '/' + identifier
                        yield HfInfo(
                            repo,
                            identifier.split('/')[-1],
                            category,
                            date_crawl,
                            info.downloads,
                            info.likes,
                            disc,
                            msg,
                            link,
                            readme,
                        )
                    except Exception:
                        error = traceback.format_exc()
                        yield HfInfo(
                            repo,
                            identifier.split('/')[-1],
                            category,
                            date_crawl,
                            error=error,
                        )

    def _fetch_from_identifier(
        self, identifier, category: Literal['model', 'dataset']
    ) -> ModelInfo | DatasetInfo:
        try:
            match category:
                case 'model':
                    return self.retrier(self.api.model_info, identifier)
                case 'dataset':
                    return self.retrier(self.api.dataset_info, identifier)
        except RetryError:
            logger.exception(
                f'Max retry exceeded when fetch {category} information from {identifier}.'
            )
            raise
        except Exception:
            logger.exception(f'Exception when fetch {category} information from {identifier}.')
            raise

    def _fetch_from_repo(
        self, repo, category: Literal['model', 'dataset']
    ) -> Iterator[tuple[ModelInfo | DatasetInfo | None, str | None]]:
        match category:
            case 'model':
                infos = self.api.list_models(author=repo, full=True)
            case 'dataset':
                infos = self.api.list_datasets(author=repo, full=True)

        while True:
            try:
                info = self.retrier(next, infos)
                yield info, None
            except StopIteration:
                break
            except RetryError:
                # WARNING: After the current info exceeds the retry limit, this repo will not
                # proceed with subsequent fetches.
                logger.exception(
                    f'Max retry exceeded when fetch from repo {repo}, stopping iteration'
                )
                error = traceback.format_exc()
                yield None, error
                break

    def _fetch_discussions_count(self, identifier) -> tuple[int, int]:
        discussions = self.api.get_repo_discussions(identifier)
        total_count = 0
        total_msg = 0

        while True:
            try:
                discussion = self.retrier(next, discussions)
                total_count += 1
                discussion_details = self.retrier(
                    self.api.get_discussion_details, identifier, discussion.num
                )
                total_msg += len(discussion_details.events)
            except StopIteration:
                return total_count, total_msg
            except RetryError:
                logger.exception(
                    f'Max retry exceeded when fetch discussions from {identifier}, stopping iteration'
                )
                return total_count, total_msg

    def _fetch_readme_content(self, identifier, category: Literal['model', 'dataset']) -> str:
        try:
            match category:
                case 'model':
                    readme = self.retrier(ModelCard.load, identifier)
                case 'dataset':
                    readme = self.retrier(DatasetCard.load, identifier)
            return str(readme)
        except RetryError:
            logger.exception(f'Max retry exceeded when fetch readme content from {identifier}')
            return ''
        except Exception:
            logger.exception(f'Exception when fetch readme content from {identifier}')
            return ''

    def fetch_num_of(self, repo, category: Literal['models', 'datasets']):
        try:
            try:
                info = self.retrier(self.api.get_organization_overview, repo)
                match category:
                    case 'models':
                        return info.num_models
                    case 'datasets':
                        return info.num_datasets
            except RetryError:
                logger.exception(f'Max retry exceeded when fetch num of {category} of {repo}')
                raise
        except HfHubHTTPError:
            try:
                info = self.retrier(self.api.get_user_overview, repo)
                match category:
                    case 'models':
                        return info.num_models
                    case 'datasets':
                        return info.num_datasets
            except RetryError:
                logger.exception(f'Max retry exceeded when fetch num of {category} of {repo}')
            except Exception:
                return 0
