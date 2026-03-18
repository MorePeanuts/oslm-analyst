"""
Migrate data from model_modality.json and dataset_modality.json
to model_info.jsonl and dataset_info.jsonl.
"""

import json
import re
import sys
from pathlib import Path
from loguru import logger
import jsonlines
from typing import Literal

from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
)
from requests.exceptions import HTTPError

# Add src to path to import crawlers
sys.path.insert(0, str(Path(__file__).parents[2] / 'src'))

from huggingface_hub import HfApi
from huggingface_hub.errors import HfHubHTTPError, RepositoryNotFoundError
from modelscope.hub.api import HubApi


def _is_hf_rate_limit_error(exception):
    """Check if exception is a HuggingFace rate limit error."""
    return isinstance(exception, HfHubHTTPError) and exception.response.status_code == 429


def _is_ms_rate_limit_error(exception):
    """Check if exception is a ModelScope rate limit error."""
    return isinstance(exception, HTTPError) and exception.response.status_code == 429


def hf_wait_logic(retry_state):
    """HuggingFace wait logic for retries."""
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


def ms_wait_logic(retry_state):
    """ModelScope wait logic for retries."""
    exc = retry_state.outcome.exception()

    if isinstance(exc, HTTPError):
        retry_after = exc.response.headers.get('Retry-After')
        if retry_after:
            return float(retry_after)

    return 60.0


class PlatformChecker:
    """Check if a model/dataset exists on HuggingFace or ModelScope."""

    def __init__(self, max_retry: int = 5):
        self.hf_api = HfApi()
        self.ms_api = HubApi()
        self.max_retry = max_retry

        # HuggingFace retrier
        self.hf_retrier = Retrying(
            reraise=True,
            retry=retry_if_exception(_is_hf_rate_limit_error),
            wait=hf_wait_logic,
            stop=stop_after_attempt(max_retry),
        )

        # ModelScope retrier
        self.ms_retrier = Retrying(
            reraise=True,
            retry=retry_if_exception(_is_ms_rate_limit_error),
            wait=ms_wait_logic,
            stop=stop_after_attempt(max_retry),
        )

    def check_hf_model(self, repo_id: str) -> bool:
        """Check if model exists on HuggingFace."""
        try:
            self.hf_retrier(self.hf_api.model_info, repo_id)
            return True
        except RepositoryNotFoundError:
            return False
        except HfHubHTTPError as e:
            if e.response.status_code == 404:
                return False
            logger.warning(f'HF API error checking model {repo_id}: {e}')
            return False
        except RetryError:
            logger.error(f'Failed to check HF model {repo_id} after retries')
            return False
        except Exception:
            return False

    def check_hf_dataset(self, repo_id: str) -> bool:
        """Check if dataset exists on HuggingFace."""
        try:
            self.hf_retrier(self.hf_api.dataset_info, repo_id)
            return True
        except RepositoryNotFoundError:
            return False
        except HfHubHTTPError as e:
            if e.response.status_code == 404:
                return False
            logger.warning(f'HF API error checking dataset {repo_id}: {e}')
            return False
        except RetryError:
            logger.error(f'Failed to check HF dataset {repo_id} after retries')
            return False
        except Exception:
            return False

    def check_ms_model(self, repo_id: str) -> bool:
        """Check if model exists on ModelScope."""
        try:
            self.ms_retrier(self.ms_api.model_info, repo_id)
            return True
        except Exception as e:
            # Check if it's a not found error
            err_str = str(e).lower()
            if 'not found' in err_str or '404' in err_str or 'not exist' in err_str:
                return False
            logger.debug(f'MS API error checking model {repo_id}: {e}')
            return False

    def check_ms_dataset(self, repo_id: str) -> bool:
        """Check if dataset exists on ModelScope."""
        try:
            self.ms_retrier(self.ms_api.dataset_info, repo_id)
            return True
        except Exception as e:
            # Check if it's a not found error
            err_str = str(e).lower()
            if 'not found' in err_str or '404' in err_str or 'not exist' in err_str:
                return False
            logger.debug(f'MS API error checking dataset {repo_id}: {e}')
            return False

    def get_model_platform(self, repo_id: str) -> Literal['huggingface', 'modelscope', None]:
        """Get platform for a model."""
        if self.check_hf_model(repo_id):
            return 'huggingface'
        if self.check_ms_model(repo_id):
            return 'modelscope'
        return None

    def get_dataset_platform(self, repo_id: str) -> Literal['huggingface', 'modelscope', None]:
        """Get platform for a dataset."""
        if self.check_hf_dataset(repo_id):
            return 'huggingface'
        if self.check_ms_dataset(repo_id):
            return 'modelscope'
        return None


def build_link(platform: str | None, full_id: str, category: Literal['model', 'dataset']) -> str:
    """Build link based on platform."""
    if platform == 'huggingface':
        if category == 'model':
            return f'https://huggingface.co/{full_id}'
        else:
            return f'https://huggingface.co/datasets/{full_id}'
    elif platform == 'modelscope':
        if category == 'model':
            return f'https://modelscope.cn/models/{full_id}'
        else:
            return f'https://modelscope.cn/datasets/{full_id}'
    # Default to HuggingFace if platform unknown
    if category == 'model':
        return f'https://huggingface.co/{full_id}'
    else:
        return f'https://huggingface.co/datasets/{full_id}'


def migrate_model_info(
    src_path: Path,
    dst_path: Path,
    checker: PlatformChecker | None = None,
    skip_platform_check: bool = False,
):
    """Migrate model modality data to model_info.jsonl."""
    if not src_path.exists():
        logger.warning(f'Source file not found: {src_path}')
        return

    with open(src_path, 'r', encoding='utf-8') as f:
        model_data = json.load(f)

    logger.info(f'Loaded {len(model_data)} entries from {src_path}')

    # Read existing data if destination exists
    existing = {}
    if dst_path.exists():
        with jsonlines.open(dst_path, 'r') as reader:
            for line in reader:
                key = f'{line["repo"]}/{line["name"]}'
                existing[key] = line

    logger.info(f'Found {len(existing)} existing entries in {dst_path}')

    # Migrate data
    updated_count = 0
    skipped_count = 0
    null_filled_count = 0
    result = []

    for idx, (full_id, info) in enumerate(model_data.items()):
        if '/' not in full_id:
            logger.warning(f'Skipping invalid ID: {full_id}')
            continue

        repo, name = full_id.split('/', 1)
        modality = info.get('modality')
        is_large_model = info.get('is_large_model', True)

        # Check if already exists first to avoid unnecessary API calls
        if full_id in existing:
            existing_entry = existing[full_id]

            # Case 1: Existing entry has complete data and matches - skip API call
            if (
                existing_entry.get('modality') is not None
                and existing_entry.get('valid') is not None
                and existing_entry.get('modality') == modality
                and existing_entry.get('valid') == is_large_model
            ):
                result.append(existing_entry)
                skipped_count += 1
                continue

            # Case 2: Existing entry has null values or data changed - still need to build entry
            # But can keep existing link if available
            link = existing_entry.get('link')
            if link is None:
                # Only build link if not available
                if skip_platform_check or checker is None:
                    platform = None
                else:
                    platform = checker.get_model_platform(full_id)
                    if platform:
                        logger.debug(f'[{idx + 1}/{len(model_data)}] Model {full_id} found on {platform}')
                    else:
                        logger.warning(
                            f'[{idx + 1}/{len(model_data)}] Model {full_id} not found on any platform'
                        )
                link = build_link(platform, full_id, 'model')

            # Create entry
            entry = {
                'repo': repo,
                'name': name,
                'modality': modality,
                'valid': is_large_model,
                'link': link,
            }

            # Check if we're filling null values
            filled_fields = []
            if existing_entry.get('modality') is None and modality is not None:
                filled_fields.append('modality')
            if existing_entry.get('valid') is None and is_large_model is not None:
                filled_fields.append('valid')
            if filled_fields:
                null_filled_count += 1
                logger.info(f'Filling null fields for model {full_id}: {", ".join(filled_fields)}')

            result.append(entry)
            updated_count += 1
            continue

        # Case 3: New entry - need full processing
        if skip_platform_check or checker is None:
            platform = None
        else:
            platform = checker.get_model_platform(full_id)
            if platform:
                logger.debug(f'[{idx + 1}/{len(model_data)}] Model {full_id} found on {platform}')
            else:
                logger.warning(
                    f'[{idx + 1}/{len(model_data)}] Model {full_id} not found on any platform'
                )

        link = build_link(platform, full_id, 'model')

        # Create new entry
        entry = {
            'repo': repo,
            'name': name,
            'modality': modality,
            'valid': is_large_model,
            'link': link,
        }

        result.append(entry)
        updated_count += 1

    # Keep existing entries not in source file
    for full_id, entry in existing.items():
        if full_id not in model_data:
            result.append(entry)

    # Write back
    with jsonlines.open(dst_path, 'w') as writer:
        writer.write_all(result)

    logger.info(
        f'Model info migration complete: '
        f'updated={updated_count}, skipped={skipped_count}, '
        f'null_filled={null_filled_count}, total={len(result)}'
    )


def migrate_dataset_info(
    src_path: Path,
    dst_path: Path,
    checker: PlatformChecker | None = None,
    skip_platform_check: bool = False,
):
    """Migrate dataset modality data to dataset_info.jsonl."""
    if not src_path.exists():
        logger.warning(f'Source file not found: {src_path}')
        return

    with open(src_path, 'r', encoding='utf-8') as f:
        dataset_data = json.load(f)

    logger.info(f'Loaded {len(dataset_data)} entries from {src_path}')

    # Read existing data if destination exists
    existing = {}
    if dst_path.exists():
        with jsonlines.open(dst_path, 'r') as reader:
            for line in reader:
                key = f'{line["repo"]}/{line["name"]}'
                existing[key] = line

    logger.info(f'Found {len(existing)} existing entries in {dst_path}')

    # Migrate data
    updated_count = 0
    skipped_count = 0
    null_filled_count = 0
    result = []

    for idx, (full_id, info) in enumerate(dataset_data.items()):
        if '/' not in full_id:
            logger.warning(f'Skipping invalid ID: {full_id}')
            continue

        repo, name = full_id.split('/', 1)
        modality = info.get('modality')
        lifecycle = info.get('lifecycle')
        is_valid = info.get('is_valid', True)

        # Check if already exists first to avoid unnecessary API calls
        if full_id in existing:
            existing_entry = existing[full_id]

            # Case 1: Existing entry has complete data and matches - skip API call
            if (
                existing_entry.get('modality') is not None
                and existing_entry.get('lifecycle') is not None
                and existing_entry.get('valid') is not None
                and existing_entry.get('modality') == modality
                and existing_entry.get('lifecycle') == lifecycle
                and existing_entry.get('valid') == is_valid
            ):
                result.append(existing_entry)
                skipped_count += 1
                continue

            # Case 2: Existing entry has null values or data changed - still need to build entry
            # But can keep existing link if available
            link = existing_entry.get('link')
            if link is None:
                # Only build link if not available
                if skip_platform_check or checker is None:
                    platform = None
                else:
                    platform = checker.get_dataset_platform(full_id)
                    if platform:
                        logger.debug(
                            f'[{idx + 1}/{len(dataset_data)}] Dataset {full_id} found on {platform}'
                        )
                    else:
                        logger.warning(
                            f'[{idx + 1}/{len(dataset_data)}] Dataset {full_id} not found on any platform'
                        )
                link = build_link(platform, full_id, 'dataset')

            # Create entry
            entry = {
                'repo': repo,
                'name': name,
                'modality': modality,
                'lifecycle': lifecycle,
                'valid': is_valid,
                'link': link,
            }

            # Count if we filled any null values
            filled_fields = []
            if existing_entry.get('modality') is None and modality is not None:
                filled_fields.append('modality')
            if existing_entry.get('lifecycle') is None and lifecycle is not None:
                filled_fields.append('lifecycle')
            if existing_entry.get('valid') is None and is_valid is not None:
                filled_fields.append('valid')
            if filled_fields:
                null_filled_count += 1
                logger.info(
                    f'Filling null fields for dataset {full_id}: {", ".join(filled_fields)}'
                )

            result.append(entry)
            updated_count += 1
            continue

        # Case 3: New entry - need full processing
        if skip_platform_check or checker is None:
            platform = None
        else:
            platform = checker.get_dataset_platform(full_id)
            if platform:
                logger.debug(
                    f'[{idx + 1}/{len(dataset_data)}] Dataset {full_id} found on {platform}'
                )
            else:
                logger.warning(
                    f'[{idx + 1}/{len(dataset_data)}] Dataset {full_id} not found on any platform'
                )

        link = build_link(platform, full_id, 'dataset')

        # Create new entry
        entry = {
            'repo': repo,
            'name': name,
            'modality': modality,
            'lifecycle': lifecycle,
            'valid': is_valid,
            'link': link,
        }

        result.append(entry)
        updated_count += 1

    # Keep existing entries not in source file
    for full_id, entry in existing.items():
        if full_id not in dataset_data:
            result.append(entry)

    # Write back
    with jsonlines.open(dst_path, 'w') as writer:
        writer.write_all(result)

    logger.info(
        f'Dataset info migration complete: '
        f'updated={updated_count}, skipped={skipped_count}, '
        f'null_filled={null_filled_count}, total={len(result)}'
    )


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Migrate modality info from JSON to JSONL format.')
    parser.add_argument(
        '--skip-platform-check',
        action='store_true',
        help='Skip checking if models/datasets exist on HF/MS (faster)',
    )
    parser.add_argument(
        '--max-retry',
        type=int,
        default=5,
        help='Maximum number of retries for rate-limited API requests',
    )
    args = parser.parse_args()

    config_dir = Path(__file__).parents[2] / 'config'

    model_modality_json = config_dir / 'model_modality.json'
    dataset_modality_json = config_dir / 'dataset_modality.json'
    model_info_jsonl = config_dir / 'model_info.jsonl'
    dataset_info_jsonl = config_dir / 'dataset_info.jsonl'

    checker = None
    if not args.skip_platform_check:
        checker = PlatformChecker(max_retry=args.max_retry)

    logger.info('Starting modality info migration...')
    migrate_model_info(
        model_modality_json,
        model_info_jsonl,
        checker=checker,
        skip_platform_check=args.skip_platform_check,
    )
    migrate_dataset_info(
        dataset_modality_json,
        dataset_info_jsonl,
        checker=checker,
        skip_platform_check=args.skip_platform_check,
    )
    logger.info('Migration done!')


if __name__ == '__main__':
    main()
