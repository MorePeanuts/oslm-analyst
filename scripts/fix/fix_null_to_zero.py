"""
Fix null values that should be 0 in model_data.jsonl and dataset_data.jsonl.

Specifically:
- likes: null -> 0
- discussions: null -> 0
- descendants: null -> 0

downloads_last_month should remain as null when there is no data.

Uses tempfile for safe atomic writes.

If a directory is provided, recursively finds all folders starting with
"osir-lmts_" and processes the model_data.jsonl and dataset_data.jsonl within them.
"""

import argparse
import tempfile
from pathlib import Path

import jsonlines


def fix_null_values(file_path: Path) -> tuple[int, int]:
    """
    Fix null values that should be 0 in the given JSONL file.

    Args:
        file_path: Path to the JSONL file

    Returns:
        Tuple of (total_count, fixed_count)
    """
    if not file_path.exists():
        raise FileNotFoundError(f'File not found: {file_path}')

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
            total_count = 0
            fixed_count = 0

            for line in reader:
                total_count += 1
                modified = False

                # Fix likes: null -> 0
                if line.get('likes') is None:
                    line['likes'] = 0
                    modified = True

                # Fix discussions: null -> 0
                if line.get('discussions') is None:
                    line['discussions'] = 0
                    modified = True

                # Fix descendants: null -> 0
                if line.get('descendants') is None:
                    line['descendants'] = 0
                    modified = True

                if modified:
                    fixed_count += 1

                writer.write(line)

    # Atomically replace the original file
    temp_path.replace(file_path)

    return total_count, fixed_count


def find_data_files(root_path: Path) -> list[Path]:
    """
    Find all model_data.jsonl and dataset_data.jsonl files in osir-lmts_ prefixed directories.

    Args:
        root_path: Root directory to search from

    Returns:
        List of JSONL file paths
    """
    data_files = []

    # If path is a file, return it directly
    if root_path.is_file():
        return [root_path]

    # If path is a directory, search recursively
    for item in root_path.rglob('*'):
        # Check if item is a file named model_data.jsonl or dataset_data.jsonl
        # and its parent directory starts with osir-lmts_
        if (
            item.is_file()
            and item.name in ('model_data.jsonl', 'dataset_data.jsonl')
            and item.parent.name.startswith('osir-lmts_')
        ):
            data_files.append(item)

    return sorted(data_files)


def main():
    parser = argparse.ArgumentParser(
        description="Fix null values that should be 0 in model_data.jsonl and dataset_data.jsonl. "
        'If a directory is provided, recursively finds all osir-lmts_ folders.'
    )
    parser.add_argument(
        'path', type=Path, help='Path to JSONL file or directory to search'
    )

    args = parser.parse_args()

    data_files = find_data_files(args.path)

    if not data_files:
        print('No model_data.jsonl or dataset_data.jsonl files found in osir-lmts_ directories.')
        return

    print(f'Found {len(data_files)} file(s) to process:\n')

    total_processed = 0
    total_fixed = 0

    for file_path in data_files:
        print(f'Processing: {file_path}')
        try:
            total, fixed = fix_null_values(file_path)
            print(f'  Total lines: {total}, Fixed lines: {fixed}\n')
            total_processed += total
            total_fixed += fixed
        except Exception as e:
            print(f'  Error processing {file_path}: {e}\n')

    print(f'Overall total: Processed {total_processed} lines, Fixed {total_fixed} lines')


if __name__ == '__main__':
    main()