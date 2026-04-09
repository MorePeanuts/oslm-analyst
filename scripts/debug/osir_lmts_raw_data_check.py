"""Script to validate raw model/dataset JSONL files from crawled data.

This script checks raw_model_data.jsonl and raw_dataset_data.jsonl files for
validation errors according to predefined rules:

For raw_model_data.jsonl:
- downloads_last_month, likes, discussions, discussion_msg must be integers
- valid field cannot be null
- If valid is true, modality cannot be null or string "null"

For raw_dataset_data.jsonl:
- All the above checks, plus:
- If valid is true, lifecycle cannot be null or string "null"

The script outputs detailed error reports and summary statistics including
the count of lines where valid=True.
"""

import argparse
import json
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class ValidationError:
    """Represents a single validation error found in the data."""
    line_num: int
    field: str
    issue: str
    value: Optional[str] = None


@dataclass
class FileStats:
    """Statistics for a single JSONL file validation."""
    total_lines: int = 0
    valid_lines: int = 0
    invalid_lines: int = 0
    valid_true_lines: int = 0
    errors: list[ValidationError] | None = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def check_integer_field(data: dict, field: str, line_num: int) -> list[ValidationError]:
    """Check that a field exists and is an integer."""
    errors = []
    if field not in data:
        errors.append(ValidationError(line_num, field, 'field missing'))
    else:
        value = data[field]
        if value is None:
            errors.append(ValidationError(line_num, field, 'is null', None))
        elif not isinstance(value, int):
            errors.append(ValidationError(line_num, field, 'not integer', str(value)))
    return errors


def check_valid_field(data: dict, line_num: int) -> list[ValidationError]:
    """Check that valid field exists and is not null."""
    errors = []
    if 'valid' not in data:
        errors.append(ValidationError(line_num, 'valid', 'field missing'))
    else:
        if data['valid'] is None:
            errors.append(ValidationError(line_num, 'valid', 'is null'))
    return errors


def check_modality_or_lifecycle_field(
    data: dict, field: str, line_num: int
) -> list[ValidationError]:
    """Check modality or lifecycle field when valid is true.

    The field must exist, not be null, and not be the string "null".
    """
    errors = []
    # Only check if valid is true
    if data.get('valid') is not True:
        return errors

    if field not in data:
        errors.append(ValidationError(line_num, field, 'field missing (valid is true)'))
    else:
        value = data[field]
        if value is None:
            errors.append(ValidationError(line_num, field, 'is null (valid is true)', None))
        elif isinstance(value, str) and value.lower() == 'null':
            errors.append(
                ValidationError(line_num, field, 'is string "null" (valid is true)', value)
            )
    return errors


def check_model_data(file_path: Path) -> FileStats:
    """Validate raw_model_data.jsonl file.

    Checks:
    - Integer fields: downloads_last_month, likes, discussions, discussion_msg
    - valid field is not null
    - modality is valid when valid is true
    """
    stats = FileStats()

    if not file_path.exists():
        print(f'Warning: {file_path} does not exist')
        return stats

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            stats.total_lines += 1
            line_errors = []

            try:
                data = json.loads(line)

                # Check integer fields
                for field in ['downloads_last_month', 'likes', 'discussions', 'discussion_msg']:
                    line_errors.extend(check_integer_field(data, field, line_num))

                # Check valid field
                line_errors.extend(check_valid_field(data, line_num))

                # Check modality if valid is true
                line_errors.extend(check_modality_or_lifecycle_field(data, 'modality', line_num))

            except json.JSONDecodeError as e:
                line_errors.append(ValidationError(line_num, 'json', f'parse error: {e}'))

            stats.errors.extend(line_errors)
            if line_errors:
                stats.invalid_lines += 1
            else:
                stats.valid_lines += 1

            # Count valid=True lines
            try:
                if data.get('valid') is True:
                    stats.valid_true_lines += 1
            except Exception:
                pass

    return stats


def check_dataset_data(file_path: Path) -> FileStats:
    """Validate raw_dataset_data.jsonl file.

    Checks:
    - Integer fields: downloads_last_month, likes, discussions, discussion_msg
    - valid field is not null
    - modality is valid when valid is true
    - lifecycle is valid when valid is true
    """
    stats = FileStats()

    if not file_path.exists():
        print(f'Warning: {file_path} does not exist')
        return stats

    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            stats.total_lines += 1
            line_errors = []

            try:
                data = json.loads(line)

                # Check integer fields
                for field in ['downloads_last_month', 'likes', 'discussions', 'discussion_msg']:
                    line_errors.extend(check_integer_field(data, field, line_num))

                # Check valid field
                line_errors.extend(check_valid_field(data, line_num))

                # Check modality if valid is true
                line_errors.extend(check_modality_or_lifecycle_field(data, 'modality', line_num))

                # Check lifecycle if valid is true
                line_errors.extend(check_modality_or_lifecycle_field(data, 'lifecycle', line_num))

            except json.JSONDecodeError as e:
                line_errors.append(ValidationError(line_num, 'json', f'parse error: {e}'))

            stats.errors.extend(line_errors)
            if line_errors:
                stats.invalid_lines += 1
            else:
                stats.valid_lines += 1

            # Count valid=True lines
            try:
                if data.get('valid') is True:
                    stats.valid_true_lines += 1
            except Exception:
                pass

    return stats


def print_errors(stats: FileStats, file_name: str):
    """Print validation errors (limited to first 50)."""
    if not stats.errors:
        return

    print(f'\n--- {file_name} Errors ---')
    for err in stats.errors[:50]:  # Limit to first 50 errors to avoid too much output
        value_str = f' (value: {err.value})' if err.value is not None else ''
        print(f'  Line {err.line_num}: {err.field} - {err.issue}{value_str}')

    if len(stats.errors) > 50:
        print(f'  ... and {len(stats.errors) - 50} more errors')


def print_stats(model_stats: FileStats, dataset_stats: FileStats):
    """Print summary statistics for both model and dataset files."""
    print('\n' + '=' * 60)
    print('SUMMARY STATISTICS')
    print('=' * 60)

    print('\n--- raw_model_data.jsonl ---')
    print(f'  Total lines:    {model_stats.total_lines}')
    print(f'  Valid lines:    {model_stats.valid_lines}')
    print(f'  Invalid lines:  {model_stats.invalid_lines}')
    print(f'  valid=True:     {model_stats.valid_true_lines}')
    print(f'  Total errors:   {len(model_stats.errors)}')

    print('\n--- raw_dataset_data.jsonl ---')
    print(f'  Total lines:    {dataset_stats.total_lines}')
    print(f'  Valid lines:    {dataset_stats.valid_lines}')
    print(f'  Invalid lines:  {dataset_stats.invalid_lines}')
    print(f'  valid=True:     {dataset_stats.valid_true_lines}')
    print(f'  Total errors:   {len(dataset_stats.errors)}')

    total_invalid = model_stats.invalid_lines + dataset_stats.invalid_lines
    total_errors = len(model_stats.errors) + len(dataset_stats.errors)
    total_valid_true = model_stats.valid_true_lines + dataset_stats.valid_true_lines
    print(f'\n--- Total ---')
    print(f'  Invalid lines:  {total_invalid}')
    print(f'  valid=True:     {total_valid_true}')
    print(f'  Total errors:   {total_errors}')


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Check raw model/dataset JSONL files for validation errors.'
    )
    parser.add_argument(
        'input_path',
        type=Path,
        help='Path to directory like output/huggingface_2026-04-07'
    )
    args = parser.parse_args()

    input_path = args.input_path

    if not input_path.exists():
        print(f'Error: Path does not exist: {input_path}')
        sys.exit(1)

    if not input_path.is_dir():
        print(f'Error: Path is not a directory: {input_path}')
        sys.exit(1)

    print(f'Checking data in: {input_path}')

    # Check model data
    model_file = input_path / 'raw_model_data.jsonl'
    model_stats = check_model_data(model_file)
    print_errors(model_stats, 'raw_model_data.jsonl')

    # Check dataset data
    dataset_file = input_path / 'raw_dataset_data.jsonl'
    dataset_stats = check_dataset_data(dataset_file)
    print_errors(dataset_stats, 'raw_dataset_data.jsonl')

    # Print summary
    print_stats(model_stats, dataset_stats)


if __name__ == '__main__':
    main()

