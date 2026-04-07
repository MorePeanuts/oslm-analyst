import json
import re
import traceback
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from typing import Literal

import httpx
from huggingface_hub import DatasetCard, HfApi, ModelCard
from huggingface_hub.errors import HfHubHTTPError
from huggingface_hub.hf_api import DatasetInfo, ModelInfo
from loguru import logger
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
)

from oslm_analyst.data_utils import HfInfo

from ..utils import today
from .crawl_utils import str2int


def _is_retryable_error(exception):
    # Retry on rate limit errors (429)
    if isinstance(exception, HfHubHTTPError) and exception.response.status_code == 429:
        return True
    # Retry on network connection errors
    if isinstance(exception, (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout)):
        return True
    # Retry on connection reset errors (often wrapped in other exceptions)
    exc_str = str(exception)
    if 'Connection reset by peer' in exc_str or 'ECONNRESET' in exc_str:
        return True
    return False


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

    # For network errors, use a shorter wait time with some jitter
    if isinstance(exc, (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout)):
        return 30.0

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
            retry=retry_if_exception(_is_retryable_error),
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
                disc, msg = self._fetch_discussions_count(identifier, category)
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
                        disc, msg = self._fetch_discussions_count(identifier, category)
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

    def _fetch_discussions_count(
        self, identifier, category: Literal['model', 'dataset']
    ) -> tuple[int, int]:
        total_count = 0
        total_msg = 0
        try:
            discussions = self.retrier(
                self.api.get_repo_discussions, identifier, repo_type=category
            )
        except RetryError:
            logger.exception(
                f'Max retry exceeded when fetch discussions from {identifier}, stopping iteration'
            )
            return total_count, total_msg
        except HfHubHTTPError as e:
            if e.response.status_code == 403 and 'Discussions are disabled for this repo' in str(e):
                # Discussions are disabled for this repo, this is expected, not an error
                return total_count, total_msg
            logger.exception(f'Exception when fetch discussions from {identifier}')
            return total_count, total_msg
        except Exception:
            logger.exception(f'Exception when fetch discussions from {identifier}')
            return total_count, total_msg

        while True:
            try:
                discussion = self.retrier(next, discussions)
                total_count += 1
                try:
                    discussion_details = self.retrier(
                        self.api.get_discussion_details,
                        identifier,
                        discussion.num,
                        repo_type=category,
                    )
                    total_msg += len(discussion_details.events)
                except Exception:
                    logger.exception(f'Exception when fetch discussion_details from {identifier}')
                    continue
            except StopIteration:
                return total_count, total_msg
            except RetryError:
                logger.exception(
                    f'Max retry exceeded when fetch discussions from {identifier}, stopping iteration'
                )
                return total_count, total_msg
            except HfHubHTTPError as e:
                if (
                    e.response.status_code == 403
                    and 'Discussions are disabled for this repo' in str(e)
                ):
                    # Discussions are disabled for this repo, this is expected, not an error
                    return total_count, total_msg
                logger.exception(f'Exception when fetch discussion from {identifier}')
                return total_count, total_msg
            except Exception:
                logger.exception(f'Exception when fetch discussion from {identifier}')
                return total_count, total_msg

    def fetch_readme_content(self, identifier, category: Literal['model', 'dataset']) -> str:
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
            logger.debug(f'No readme file found in {identifier}.')
            return ''

    def fetch_num_of(self, repo, category: Literal['models', 'datasets']):
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
                logger.exception(f'user/organization {repo} doesnot exists.')
                raise
