"""
Fix date_crawl fields in osir-lmts_* directories.

This script:
1. Finds all huggingface_* directories with full dates (YYYY-MM-DD)
2. Matches them to osir-lmts_* directories with year-month (YYYY-MM)
3. Fixes date_crawl fields in:
   - acc_model_data.jsonl, acc_dataset_data.jsonl: fixes empty string dates
   - model_data.jsonl, dataset_data.jsonl: completes YYYY-MM to YYYY-MM-DD
Uses tempfile for safe atomic writes.
"""

import argparse
import re
import tempfile
from datetime import datetime
from pathlib import Path

import jsonlines


def extract_date_from_dir_name(dir_name: str, pattern: str) -> str | None:
    """
    Extract date from directory name using regex pattern.

    Args:
        dir_name: Directory name to parse
        pattern: Regex pattern with date capture group

    Returns:
        Extracted date string or None if not found
    """
    match = re.search(pattern, dir_name)
    return match.group(1) if match else None


def find_huggingface_directories(root_path: Path) -> dict[str, Path]:
    """
    Find all huggingface_YYYY-MM-DD directories and map YYYY-MM to full path.

    Args:
        root_path: Root directory to search

    Returns:
        Dictionary mapping year-month (YYYY-MM) to full directory path
    """
    huggingface_dirs: dict[str, Path] = {}
    pattern = r'huggingface_(\d{4}-\d{2}-\d{2})'

    for item in root_path.iterdir():
        if item.is_dir() and item.name.startswith('huggingface_'):
            full_date = extract_date_from_dir_name(item.name, pattern)
            if full_date:
                year_month = full_date[:7]  # Extract YYYY-MM from YYYY-MM-DD
                huggingface_dirs[year_month] = item

    return huggingface_dirs


def find_osir_lmts_directories(root_path: Path) -> list[Path]:
    """
    Find all osir-lmts_YYYY-MM directories.

    Args:
        root_path: Root directory to search

    Returns:
        List of osir-lmts directory paths
    """
    osir_dirs: list[Path] = []
    pattern = r'osir-lmts_(\d{4}-\d{2})'

    for item in root_path.iterdir():
        if item.is_dir() and item.name.startswith('osir-lmts_'):
            year_month = extract_date_from_dir_name(item.name, pattern)
            if year_month:
                osir_dirs.append(item)

    return sorted(osir_dirs)


def fix_date_in_file(file_path: Path, correct_date: str) -> tuple[int, int]:
    """
    Fix date_crawl field in a JSONL file using tempfile for safe writes.

    Args:
        file_path: Path to JSONL file
        correct_date: Full date in YYYY-MM-DD format

    Returns:
        Tuple of (total_lines, fixed_lines)
    """
    if not file_path.exists():
        return (0, 0)

    total_lines = 0
    fixed_lines = 0

    # Create a temporary file in the same directory for atomic write
    with tempfile.NamedTemporaryFile(
        'w',
        dir=file_path.parent,
        suffix='.jsonl',
        delete=False,
        encoding='utf-8',
    ) as tf:
        temp_path = Path(tf.name)

        with (
            jsonlines.open(file_path, 'r') as reader,
            jsonlines.Writer(tf) as writer,
        ):
            for line in reader:
                total_lines += 1
                original_date = line.get('date_crawl', '')

                # Fix the date if it's empty, just YYYY-MM, or incorrect
                if not original_date or len(original_date) == 7 or original_date != correct_date:
                    line['date_crawl'] = correct_date
                    fixed_lines += 1

                writer.write(line)

    # Atomically replace the original file
    temp_path.replace(file_path)

    return (total_lines, fixed_lines)


def process_osir_directory(osir_dir: Path, correct_date: str) -> None:
    """
    Process all four JSONL files in an osir-lmts directory.

    Args:
        osir_dir: osir-lmts directory path
        correct_date: Full date in YYYY-MM-DD format
    """
    files_to_process = [
        'acc_model_data.jsonl',
        'acc_dataset_data.jsonl',
        'model_data.jsonl',
        'dataset_data.jsonl',
    ]

    print(f'Processing {osir_dir.name} with date {correct_date}:')

    for filename in files_to_process:
        file_path = osir_dir / filename
        if file_path.exists():
            total, fixed = fix_date_in_file(file_path, correct_date)
            print(f'  {filename}: {fixed}/{total} lines fixed')
        else:
            print(f'  {filename}: [FILE NOT FOUND]')


def main():
    parser = argparse.ArgumentParser(
        description='Fix date_crawl fields in osir-lmts_* directories using '
        'dates from corresponding huggingface_* directories.'
    )
    parser.add_argument(
        'output_dir',
        type=Path,
        default=Path('output'),
        nargs='?',
        help='Path to output directory (default: ./output)',
    )

    args = parser.parse_args()
    output_dir = args.output_dir

    if not output_dir.exists() or not output_dir.is_dir():
        print(f'Error: Directory not found: {output_dir}')
        return

    # Find all relevant directories
    huggingface_dirs = find_huggingface_directories(output_dir)
    osir_dirs = find_osir_lmts_directories(output_dir)

    if not huggingface_dirs:
        print('No huggingface_* directories with full dates found.')
        return

    if not osir_dirs:
        print('No osir-lmts_* directories found.')
        return

    print(f'Found {len(huggingface_dirs)} huggingface date sources')
    print(f'Found {len(osir_dirs)} osir-lmts directories to process\n')

    # Process each osir-lmts directory
    pattern = r'osir-lmts_(\d{4}-\d{2})'
    processed_count = 0

    for osir_dir in osir_dirs:
        year_month = extract_date_from_dir_name(osir_dir.name, pattern)

        if year_month and year_month in huggingface_dirs:
            # Get the full date from the huggingface directory name
            hf_dir = huggingface_dirs[year_month]
            hf_pattern = r'huggingface_(\d{4}-\d{2}-\d{2})'
            full_date = extract_date_from_dir_name(hf_dir.name, hf_pattern)

            if full_date:
                process_osir_directory(osir_dir, full_date)
                processed_count += 1
                print()
            else:
                print(f'Could not extract date from {hf_dir.name}')
        else:
            print(f'Skipping {osir_dir.name}: no matching huggingface directory found\n')

    print(f'Done. Processed {processed_count}/{len(osir_dirs)} osir-lmts directories.')


if __name__ == '__main__':
    main()

