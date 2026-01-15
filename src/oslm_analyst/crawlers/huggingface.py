import traceback
from loguru import logger
from typing import Literal
from collections.abc import Iterator
from dataclasses import dataclass, field
from huggingface_hub import HfApi, ModelCard
from huggingface_hub.hf_api import ModelInfo, DatasetInfo
from tenacity import (
    Retrying,
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_not_exception_type,
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
    error: str | None = field(default=None)


class HfCrawler:
    def __init__(self, token: str | bool = False, endpoint='https://huggingface.co', max_retry=5):
        self.endpoint = endpoint.rstrip('/')
        self.api = HfApi(endpoint=self.endpoint, token=token)
        self.retrier = Retrying(
            reraise=True,
            retry=retry_if_not_exception_type(ValueError),
            wait=wait_exponential(multiplier=2, min=30, max=360),
            stop=stop_after_attempt(max_retry),
        )

    def fetch(
        self,
        repo: str,
        name: str | None,
        category: Literal['model', 'dataset'] = 'model',
    ) -> list[HfInfo]:
        date_crawl = today()
        match category:
            case 'model':
                base_link = self.endpoint
            case 'dataset':
                base_link = self.endpoint + '/datasets'
        if name is not None:
            identifier = repo + '/' + name
            try:
                info = self._fetch_from_identifier(identifier, category)
                disc, msg = self._fetch_discussions_count(identifier)
                link = base_link + '/' + identifier
                return [
                    HfInfo(
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
                ]
            except Exception:
                error = traceback.format_exc()
                return [HfInfo(repo, name, category, date_crawl, error=error)]
        else:
            pair = self._fetch_from_repo(repo, category)
            res = []
            for info, error in pair:
                if info is None:
                    # BUG:Extract the model/dataset name from the error message
                    res.append(HfInfo(repo, '', category, date_crawl, error=error))
                else:
                    identifier = info.id
                    try:
                        disc, msg = self._fetch_discussions_count(identifier)
                        link = base_link + '/' + identifier
                        res.append(
                            HfInfo(
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
                        )
                    except Exception:
                        error = traceback.format_exc()
                        res.append(
                            HfInfo(
                                repo,
                                identifier.split('/')[-1],
                                category,
                                date_crawl,
                                error=error,
                            )
                        )
            return res

    def _fetch_from_identifier(
        self, identifier, category: Literal['model', 'dataset']
    ) -> ModelInfo | DatasetInfo:
        try:
            match category:
                case 'model':
                    return self.retrier(self.api.model_info, identifier)
                case 'dataset':
                    return self.retrier(self.api.dataset_info, identifier)
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
            except Exception:
                logger.exception(f'Exception when fetch from repo {repo}')
                error = traceback.format_exc()
                yield None, error

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
            except Exception:
                logger.exception(f'Exception when fetch discussions from {identifier}')
                raise
