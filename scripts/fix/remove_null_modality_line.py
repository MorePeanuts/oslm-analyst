"""
Remove lines where modality field is null or "null" from model_data.jsonl.
Uses tempfile for safe atomic writes.

If a directory is provided, recursively finds all folders starting with
"osir-lmts_" and processes the model_data.jsonl within them.
"""

import argparse
import tempfile
from pathlib import Path

import jsonlines


def remove_null_modality_lines(file_path: Path) -> tuple[int, int]:
    """
    Remove lines with null or "null" modality from the given JSONL file.

    Args:
        file_path: Path to the model_data.jsonl file

    Returns:
        Tuple of (kept_count, removed_count)
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
            kept_count = 0
            removed_count = 0

            for line in reader:
                modality = line.get('modality')

                # Check if modality is None (null) or the string "null"
                if modality is None or modality == 'null':
                    removed_count += 1
                    continue

                writer.write(line)
                kept_count += 1

    # Atomically replace the original file
    temp_path.replace(file_path)

    return kept_count, removed_count


def find_model_files(root_path: Path) -> list[Path]:
    """
    Find all model_data.jsonl files in osir-lmts_ prefixed directories.

    Args:
        root_path: Root directory to search from

    Returns:
        List of model_data.jsonl file paths
    """
    model_files = []

    # If path is a file, return it directly
    if root_path.is_file():
        return [root_path]

    # If path is a directory, search recursively
    for item in root_path.rglob('*'):
        # Check if item is a file named model_data.jsonl
        # and its parent directory starts with osir-lmts_
        if (
            item.is_file()
            and item.name == 'model_data.jsonl'
            and item.parent.name.startswith('osir-lmts_')
        ):
            model_files.append(item)

    return sorted(model_files)


def main():
    parser = argparse.ArgumentParser(
        description="Remove lines with null or 'null' modality from model_data.jsonl. "
        'If a directory is provided, recursively finds all osir-lmts_ folders.'
    )
    parser.add_argument(
        'path', type=Path, help='Path to model_data.jsonl file or directory to search'
    )

    args = parser.parse_args()

    model_files = find_model_files(args.path)

    if not model_files:
        print('No model_data.jsonl files found in osir-lmts_ directories.')
        return

    print(f'Found {len(model_files)} file(s) to process:\n')

    total_kept = 0
    total_removed = 0

    for file_path in model_files:
        print(f'Processing: {file_path}')
        try:
            kept, removed = remove_null_modality_lines(file_path)
            print(f'  Kept: {kept}, Removed: {removed}\n')
            total_kept += kept
            total_removed += removed
        except Exception as e:
            print(f'  Error processing {file_path}: {e}\n')

    print(f'Overall total: Kept {total_kept}, Removed {total_removed}')


if __name__ == '__main__':
    main()
