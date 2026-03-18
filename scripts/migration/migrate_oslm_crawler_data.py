"""
Migrate data from oslm-crawler project to OSLM-Analyst project.

This script migrates processed-models-info.jsonl and processed-datasets-info.jsonl
from oslm-crawler/data/{date}/{platform}/ to OSLM-Analyst/output/{platform}_{date}/.
"""

import json
import sys
from pathlib import Path
from loguru import logger
import jsonlines


def migrate_hf_model_data(old_data: dict) -> dict:
    """Migrate HuggingFace model data from old format to new format."""
    new_data = {
        'repo': old_data['repo'],
        'name': old_data['model_name'],
        'category': 'model',
        'date_crawl': old_data['date_crawl'],
        'downloads_last_month': old_data.get('downloads_last_month'),
        'likes': old_data.get('likes'),
        'discussions': old_data.get('community'),
        'discussion_msg': None,
        'link': old_data.get('link'),
        'modality': old_data.get('modality'),
        'valid': True,
    }
    return new_data


def migrate_hf_dataset_data(old_data: dict) -> dict:
    """Migrate HuggingFace dataset data from old format to new format."""
    new_data = {
        'repo': old_data['repo'],
        'name': old_data['dataset_name'],
        'category': 'dataset',
        'date_crawl': old_data['date_crawl'],
        'downloads_last_month': old_data.get('downloads_last_month'),
        'likes': old_data.get('likes'),
        'discussions': old_data.get('community'),
        'discussion_msg': None,
        'link': old_data.get('link'),
        'modality': old_data.get('modality'),
        'lifecycle': old_data.get('lifecycle'),
        'valid': True,
    }
    return new_data


def migrate_ms_model_data(old_data: dict) -> dict:
    """Migrate ModelScope model data from old format to new format."""
    new_data = {
        'repo': old_data['repo'],
        'name': old_data['model_name'],
        'category': 'model',
        'date_crawl': old_data['date_crawl'],
        'downloads': old_data.get('total_downloads'),
        'likes': old_data.get('likes'),
        'link': old_data.get('link'),
        'modality': old_data.get('modality'),
        'valid': True,
    }
    return new_data


def migrate_ms_dataset_data(old_data: dict) -> dict:
    """Migrate ModelScope dataset data from old format to new format."""
    new_data = {
        'repo': old_data['repo'],
        'name': old_data['dataset_name'],
        'category': 'dataset',
        'date_crawl': old_data['date_crawl'],
        'downloads': old_data.get('total_downloads'),
        'likes': old_data.get('likes'),
        'link': old_data.get('link'),
        'modality': old_data.get('modality'),
        'lifecycle': old_data.get('lifecycle'),
        'valid': True,
    }
    return new_data


def migrate_baai_dataset_data(old_data: dict) -> dict:
    """Migrate BAAIData dataset data from old format to new format."""
    new_data = {
        'repo': 'BAAI',
        'name': old_data['dataset_name'],
        'downloads': old_data.get('total_downloads'),
        'likes': old_data.get('likes'),
        'date_crawl': old_data['date_crawl'],
        'link': old_data.get('link'),
        'profile': '',
        'category': 'dataset',
        'modality': old_data.get('modality'),
        'lifecycle': old_data.get('lifecycle'),
        'valid': True,
    }
    return new_data


def migrate_file(
    src_path: Path,
    dst_path: Path,
    migrate_func,
):
    """Migrate a single file using the specified migration function."""
    if not src_path.exists():
        logger.warning(f'Source file not found: {src_path}')
        return 0

    migrated_count = 0
    result = []

    with jsonlines.open(src_path, 'r') as reader:
        for line in reader:
            try:
                new_data = migrate_func(line)
                result.append(new_data)
                migrated_count += 1
            except Exception as e:
                logger.warning(f'Failed to migrate line: {line}, error: {e}')

    if result:
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        with jsonlines.open(dst_path, 'w') as writer:
            writer.write_all(result)
        logger.info(f'Migrated {migrated_count} entries to {dst_path}')

    return migrated_count


def migrate_date_directory(date_dir: Path, output_root: Path):
    """Migrate all platform data from a single date directory."""
    date_str = date_dir.name
    total_migrated = 0

    # HuggingFace
    hf_dir = date_dir / 'HuggingFace'
    if hf_dir.exists():
        hf_output_dir = output_root / f'huggingface_{date_str}'

        # Models
        hf_models_src = hf_dir / 'processed-models-info.jsonl'
        hf_models_dst = hf_output_dir / 'raw_model_data.jsonl'
        total_migrated += migrate_file(hf_models_src, hf_models_dst, migrate_hf_model_data)

        # Datasets
        hf_datasets_src = hf_dir / 'processed-datasets-info.jsonl'
        hf_datasets_dst = hf_output_dir / 'raw_dataset_data.jsonl'
        total_migrated += migrate_file(hf_datasets_src, hf_datasets_dst, migrate_hf_dataset_data)

    # ModelScope
    ms_dir = date_dir / 'ModelScope'
    if ms_dir.exists():
        ms_output_dir = output_root / f'modelscope_{date_str}'

        # Models
        ms_models_src = ms_dir / 'processed-models-info.jsonl'
        ms_models_dst = ms_output_dir / 'raw_model_data.jsonl'
        total_migrated += migrate_file(ms_models_src, ms_models_dst, migrate_ms_model_data)

        # Datasets
        ms_datasets_src = ms_dir / 'processed-datasets-info.jsonl'
        ms_datasets_dst = ms_output_dir / 'raw_dataset_data.jsonl'
        total_migrated += migrate_file(ms_datasets_src, ms_datasets_dst, migrate_ms_dataset_data)

    # BAAIData
    baai_dir = date_dir / 'BAAIData'
    if baai_dir.exists():
        baai_output_dir = output_root / f'baai-datahub_{date_str}'

        # Datasets only
        baai_datasets_src = baai_dir / 'processed-datasets-info.jsonl'
        baai_datasets_dst = baai_output_dir / 'raw_dataset_data.jsonl'
        total_migrated += migrate_file(
            baai_datasets_src, baai_datasets_dst, migrate_baai_dataset_data
        )

    return total_migrated


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Migrate data from oslm-crawler to OSLM-Analyst format.'
    )
    parser.add_argument(
        '--crawler-data-dir',
        type=Path,
        default=Path(__file__).parents[2] / 'oslm-crawler' / 'data',
        help='Path to oslm-crawler/data directory',
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path(__file__).parents[2] / 'output',
        help='Path to OSLM-Analyst/output directory',
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Specific date to migrate (YYYY-MM-DD), if not specified, migrate all dates',
    )
    args = parser.parse_args()

    crawler_data_dir = args.crawler_data_dir
    output_dir = args.output_dir

    if not crawler_data_dir.exists():
        logger.error(f'Crawler data directory not found: {crawler_data_dir}')
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    total_migrated = 0

    if args.date:
        # Migrate single date
        date_dir = crawler_data_dir / args.date
        if date_dir.exists() and date_dir.is_dir():
            logger.info(f'Migrating data for date: {args.date}')
            total_migrated += migrate_date_directory(date_dir, output_dir)
        else:
            logger.error(f'Date directory not found: {date_dir}')
            sys.exit(1)
    else:
        # Migrate all dates
        for date_dir in sorted(crawler_data_dir.iterdir()):
            if date_dir.is_dir() and date_dir.name.match('????-??-??'):
                logger.info(f'Migrating data for date: {date_dir.name}')
                total_migrated += migrate_date_directory(date_dir, output_dir)

    logger.info(f'Migration complete! Total entries migrated: {total_migrated}')


if __name__ == '__main__':
    main()
