"""
Migrate descendants and dataset_usage fields from old oslm-crawler data.

This script extracts:
- `descendants` field from model data
- `dataset_usage` field from dataset data (stored as `descendants` in output)

and saves them to config/model_descendants.jsonl and config/dataset_descendants.jsonl.
Each entry contains repo, name, and descendants fields.
"""

import json
import sys
import re
from pathlib import Path
from loguru import logger
import jsonlines


def extract_model_descendants(old_data: dict) -> dict | None:
    """Extract descendants data from model entry."""
    if 'descendants' not in old_data:
        return None
    return {
        'repo': old_data['repo'],
        'name': old_data['model_name'],
        'descendants': old_data['descendants'],
    }


def extract_dataset_descendants(old_data: dict) -> dict | None:
    """Extract dataset_usage data from dataset entry (stored as descendants)."""
    if 'dataset_usage' not in old_data:
        return None
    return {
        'repo': old_data['repo'],
        'name': old_data['dataset_name'],
        'descendants': old_data['dataset_usage'],
    }


def process_file(
    src_path: Path,
    extract_func,
    existing_data: dict[tuple[str, str], dict],
):
    """Process a single file and extract data, merging into existing_data."""
    if not src_path.exists():
        logger.warning(f'Source file not found: {src_path}')
        return 0

    extracted_count = 0

    with jsonlines.open(src_path, 'r') as reader:
        for line in reader:
            try:
                new_data = extract_func(line)
                if new_data:
                    key = (new_data['repo'], new_data['name'])
                    # Always overwrite with newer data (processed in chronological order)
                    existing_data[key] = new_data
                    extracted_count += 1
            except Exception as e:
                logger.warning(f'Failed to process line: {line}, error: {e}')

    return extracted_count


def process_date_directory(date_dir: Path, model_data: dict, dataset_data: dict):
    """Process all platform data from a single date directory."""
    date_str = date_dir.name
    total_extracted = 0

    # HuggingFace
    hf_dir = date_dir / 'HuggingFace'
    if hf_dir.exists():
        # Models
        hf_models_src = hf_dir / 'processed-models-info.jsonl'
        total_extracted += process_file(hf_models_src, extract_model_descendants, model_data)

        # Datasets
        hf_datasets_src = hf_dir / 'processed-datasets-info.jsonl'
        total_extracted += process_file(hf_datasets_src, extract_dataset_descendants, dataset_data)

    # ModelScope
    ms_dir = date_dir / 'ModelScope'
    if ms_dir.exists():
        # Models
        ms_models_src = ms_dir / 'processed-models-info.jsonl'
        total_extracted += process_file(ms_models_src, extract_model_descendants, model_data)

        # Datasets
        ms_datasets_src = ms_dir / 'processed-datasets-info.jsonl'
        total_extracted += process_file(ms_datasets_src, extract_dataset_descendants, dataset_data)

    # BAAIData (datasets only)
    baai_dir = date_dir / 'BAAIData'
    if baai_dir.exists():
        baai_datasets_src = baai_dir / 'processed-datasets-info.jsonl'
        total_extracted += process_file(baai_datasets_src, extract_dataset_descendants, dataset_data)

    # OpenDataLab (datasets only)
    odl_dir = date_dir / 'OpenDataLab'
    if odl_dir.exists():
        odl_datasets_src = odl_dir / 'processed-datasets-info.jsonl'
        total_extracted += process_file(odl_datasets_src, extract_dataset_descendants, dataset_data)

    return total_extracted


def write_output(data: dict[tuple[str, str], dict], output_path: Path):
    """Write extracted data to JSONLines file."""
    if not data:
        logger.warning(f'No data to write to {output_path}')
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonlines.open(output_path, 'w') as writer:
        writer.write_all(data.values())
    logger.info(f'Wrote {len(data)} entries to {output_path}')


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description='Migrate descendants/dataset_usage data from oslm-crawler to config files.'
    )
    parser.add_argument(
        'crawler_data_dir',
        type=Path,
        help='Path to oslm-crawler/data directory',
    )
    parser.add_argument(
        '--config-dir',
        type=Path,
        default=Path(__file__).parents[2] / 'config',
        help='Path to OSLM-Analyst/config directory',
    )
    args = parser.parse_args()

    crawler_data_dir = args.crawler_data_dir
    config_dir = args.config_dir

    if not crawler_data_dir.exists():
        logger.error(f'Crawler data directory not found: {crawler_data_dir}')
        sys.exit(1)

    config_dir.mkdir(parents=True, exist_ok=True)

    # Use dictionaries to store data, keyed by (repo, name) to avoid duplicates
    model_data: dict[tuple[str, str], dict] = {}
    dataset_data: dict[tuple[str, str], dict] = {}

    total_extracted = 0

    # Process all dates in chronological order (older dates first, newer dates last)
    # so that newer entries overwrite older ones
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    date_dirs = sorted(
        [d for d in crawler_data_dir.iterdir() if d.is_dir() and date_pattern.match(d.name)],
        reverse=False  # oldest first, newest last
    )
    for date_dir in date_dirs:
        logger.info(f'Processing data for date: {date_dir.name}')
        total_extracted += process_date_directory(date_dir, model_data, dataset_data)

    # Write output files
    model_output_path = config_dir / 'model_descendants.jsonl'
    dataset_output_path = config_dir / 'dataset_descendants.jsonl'

    write_output(model_data, model_output_path)
    write_output(dataset_data, dataset_output_path)

    logger.info(f'Migration complete! Total entries processed: {total_extracted}')


if __name__ == '__main__':
    main()
