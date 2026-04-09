"""OSIR-LMTS data aggregation and processing pipeline."""

from datetime import datetime, timedelta
from typing import Literal

import csv
import json
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path

import jsonlines
import pandas as pd
from loguru import logger
from .osir_lmts_data import (
    RawDataPoint,
    ModelInfo,
    DatasetInfo,
    ModelSummaryRow,
    ModelSummaryTable,
    DatasetSummaryRow,
    DatasetSummaryTable,
    InfraSummaryRow,
    InfraSummaryTable,
    EvalSummaryRow,
    EvalSummaryTable,
    BaseSummaryTable,
)
from ..data_utils import Lifecycle, Modality
from ..utils import OrgInfo
from .osir_lmts_rank import (
    OsirLmtsRankStrategy,
    DefaultRankStrategy,
)


class OsirLmtsProcessor:
    def __init__(
        self,
        target_month: str,
        target_orgs: list[str] | None = None,
        output_root: Path = Path('./output'),
        config_root: Path = Path('./config'),
    ):
        self.output_root = Path(output_root)
        self.config_root = Path(config_root)

        self.target_month = target_month
        self.target_orgs = target_orgs
        self.target_date = datetime.strptime(self.target_month, '%Y-%m')
        self.year = self.target_date.year
        self.month = self.target_date.month
        self.out_dir = self.output_root / f'osir-lmts_{self.target_month}'
        self.out_dir.mkdir(parents=True, exist_ok=True)

        self._org_map: dict[str, str] = {}
        self._org_metadata: dict[str, dict] = {}
        self._model_descendants: dict[str, int] = {}
        self._dataset_descendants: dict[str, int] = {}
        self._model_extra_info: dict[str, dict] = {}
        self._dataset_extra_info: dict[str, dict] = {}
        self._org_list: list[OrgInfo] = []

        self._load_configs()

    def _load_configs(self):
        """Load configuration files."""
        self._org_list = OrgInfo.build_org_info_list_from_yaml(self.config_root / 'orgs.yaml')
        self._org_map = OrgInfo.build_repo_org_map(self._org_list, 'huggingface')
        ms_org_map = OrgInfo.build_repo_org_map(self._org_list, 'modelscope')
        self._org_map.update(ms_org_map)
        self._org_metadata = OrgInfo.build_org_metadata(self._org_list)

        descendants_path = self.config_root / 'model_descendants.jsonl'
        if descendants_path.exists():
            with jsonlines.open(descendants_path) as f:
                for line in f:
                    key = f'{line["repo"]}/{line["name"]}'
                    self._model_descendants[key] = line.get('descendants', 0)

        descendants_path = self.config_root / 'dataset_descendants.jsonl'
        if descendants_path.exists():
            with jsonlines.open(descendants_path) as f:
                for line in f:
                    key = f'{line["repo"]}/{line["name"]}'
                    self._dataset_descendants[key] = line.get('descendants', 0)

        info_path = self.config_root / 'model_info.jsonl'
        if info_path.exists():
            with jsonlines.open(info_path) as f:
                for line in f:
                    key = f'{line["repo"]}/{line["name"]}'
                    self._model_extra_info[key] = line

        info_path = self.config_root / 'dataset_info.jsonl'
        if info_path.exists():
            with jsonlines.open(info_path) as f:
                for line in f:
                    key = f'{line["repo"]}/{line["name"]}'
                    self._dataset_extra_info[key] = line

    def _find_month_directories(self) -> list[Path]:
        """Find all output directories for the target month."""
        dirs = []
        for platform in ['huggingface', 'modelscope', 'baai-datahub']:
            for child in self.output_root.iterdir():
                if not child.is_dir():
                    continue
                if not child.name.startswith(f'{platform}_'):
                    continue
                try:
                    date_str = child.name.split('_', 1)[1]
                    dir_date = datetime.strptime(date_str, '%Y-%m-%d')
                    if dir_date.year == self.year and dir_date.month == self.month:
                        dirs.append(child)
                except (ValueError, IndexError):
                    continue
        return sorted(dirs)

    def _find_previous_month_data(
        self, identifier: str, platform: str, category: str
    ) -> RawDataPoint | None:
        """Find data from the previous month for a specific platform."""
        first_day_of_month = self.target_date.replace(day=1)
        prev_month_date = first_day_of_month - timedelta(days=1)

        for child in self.output_root.iterdir():
            if not child.is_dir():
                continue
            if not child.name.startswith(f'{platform}_'):
                continue
            try:
                date_str = child.name.split('_', 1)[1]
                dir_date = datetime.strptime(date_str, '%Y-%m-%d')
                if (
                    dir_date.year == prev_month_date.year
                    and dir_date.month == prev_month_date.month
                ):
                    data = self._load_raw_data_from_dir(child, category)  # type: ignore
                    if identifier in data:
                        return data[identifier]
            except (ValueError, IndexError):
                continue
        return None

    def _load_raw_data_from_dir(
        self, dir_path: Path, category: Literal['model', 'dataset']
    ) -> dict[str, RawDataPoint]:
        """Load raw data from a single directory."""
        result = {}
        platform = dir_path.name.split('_')[0]
        file_path = dir_path / f'raw_{category}_data.jsonl'

        if not file_path.exists():
            return result

        with jsonlines.open(file_path) as f:
            for line in f:
                if not line.get('valid', True):
                    continue

                repo = line.get('repo', '')
                name = line.get('name', '')
                identifier = f'{repo}/{name}' if repo and name else name or repo

                if not identifier:
                    continue

                downloads_last_month = line.get('downloads_last_month')
                downloads_total = line.get('downloads')

                likes = line.get('likes', 0)
                if likes and likes < 0:
                    likes = 0

                dp = RawDataPoint(
                    identifier=identifier,
                    repo=repo,
                    name=name,
                    platform=platform,
                    date_crawl=line.get('date_crawl', self.target_month),
                    downloads_last_month=downloads_last_month,
                    downloads_total=downloads_total,
                    likes=likes,
                    discussions=line.get('discussions', 0),
                    modality=line.get('modality'),
                    lifecycle=line.get('lifecycle'),
                    valid=line.get('valid', True),
                )

                extra_info = None
                if category == 'model' and identifier in self._model_extra_info:
                    extra_info = self._model_extra_info[identifier]
                elif category == 'dataset' and identifier in self._dataset_extra_info:
                    extra_info = self._dataset_extra_info[identifier]

                if extra_info:
                    if extra_info.get('modality'):
                        dp.modality = extra_info['modality']
                    if extra_info.get('lifecycle'):
                        dp.lifecycle = extra_info['lifecycle']
                    dp.valid = extra_info.get('valid', True)

                result[identifier] = dp

        return result

    def _aggregate_raw_data(
        self, category: Literal['model', 'dataset']
    ) -> dict[str, list[RawDataPoint]]:
        """Aggregate raw data from all sources for the target month."""
        aggregated: dict[str, list[RawDataPoint]] = defaultdict(list)
        dirs = self._find_month_directories()

        for dir_path in dirs:
            data = self._load_raw_data_from_dir(dir_path, category)
            for identifier, dp in data.items():
                aggregated[identifier].append(dp)

        return aggregated

    def _calculate_monthly_downloads(
        self, dp: RawDataPoint, category: Literal['model', 'dataset']
    ) -> int | None:
        """Calculate monthly downloads for a single platform data point."""
        if dp.downloads_last_month is not None:
            return dp.downloads_last_month

        if dp.downloads_total is not None:
            prev_dp = self._find_previous_month_data(dp.identifier, dp.platform, category)
            if prev_dp and prev_dp.downloads_total is not None:
                return max(0, dp.downloads_total - prev_dp.downloads_total)
            return dp.downloads_total

        return None

    def gen_model_data(self) -> list[ModelInfo]:
        """Generate model_data.jsonl."""
        aggregated = self._aggregate_raw_data('model')
        model_infos = []

        for identifier, data_points in aggregated.items():
            # Filter by target_orgs if specified
            org = self._get_org_for_identifier(identifier)
            if self.target_orgs and org not in self.target_orgs:
                continue

            total_downloads = 0
            total_likes = 0
            total_discussions = 0
            modality = None
            date_crawl = self.target_month

            has_valid_data = False
            for dp in data_points:
                if not dp.valid:
                    continue
                has_valid_data = True

                monthly = self._calculate_monthly_downloads(dp, 'model')
                if monthly is not None:
                    total_downloads += monthly
                if dp.likes is not None:
                    total_likes += dp.likes
                if dp.discussions is not None:
                    total_discussions += dp.discussions
                if dp.modality and not modality:
                    modality = dp.modality
                date_crawl = dp.date_crawl

            if not has_valid_data:
                continue

            if identifier in self._model_extra_info:
                extra = self._model_extra_info[identifier]
                if extra.get('modality'):
                    modality = extra['modality']

            descendants = self._model_descendants.get(identifier, 0)

            model_info = ModelInfo(
                identifier=identifier,
                date_crawl=date_crawl,
                downloads_last_month=total_downloads,
                likes=total_likes,
                discussions=total_discussions,
                descendants=descendants,
                modality=modality,
            )
            model_infos.append(model_info)

        with jsonlines.open(self.out_dir / 'model_data.jsonl', 'w') as f:
            for mi in model_infos:
                f.write(mi.to_dict())

        logger.info(f'Generated model_data.jsonl with {len(model_infos)} entries')
        return model_infos

    def gen_dataset_data(self) -> list[DatasetInfo]:
        """Generate dataset_data.jsonl."""
        aggregated = self._aggregate_raw_data('dataset')
        dataset_infos = []

        for identifier, data_points in aggregated.items():
            # Filter by target_orgs if specified
            org = self._get_org_for_identifier(identifier)
            if self.target_orgs and org not in self.target_orgs:
                continue

            total_downloads = 0
            total_likes = 0
            total_discussions = 0
            modality = None
            lifecycle = None
            date_crawl = self.target_month

            has_valid_data = False
            for dp in data_points:
                if not dp.valid:
                    continue
                has_valid_data = True

                monthly = self._calculate_monthly_downloads(dp, 'dataset')
                if monthly is not None:
                    total_downloads += monthly
                if dp.likes is not None:
                    total_likes += dp.likes
                if dp.discussions is not None:
                    total_discussions += dp.discussions
                if dp.modality and not modality:
                    modality = dp.modality
                if dp.lifecycle and not lifecycle:
                    lifecycle = dp.lifecycle
                date_crawl = dp.date_crawl

            if not has_valid_data:
                continue

            if identifier in self._dataset_extra_info:
                extra = self._dataset_extra_info[identifier]
                if extra.get('modality'):
                    modality = extra['modality']
                if extra.get('lifecycle'):
                    lifecycle = extra['lifecycle']

            descendants = self._dataset_descendants.get(identifier, 0)

            dataset_info = DatasetInfo(
                identifier=identifier,
                date_crawl=date_crawl,
                downloads_last_month=total_downloads,
                likes=total_likes,
                discussions=total_discussions,
                descendants=descendants,
                modality=modality,
                lifecycle=lifecycle,
            )
            dataset_infos.append(dataset_info)

        with jsonlines.open(self.out_dir / 'dataset_data.jsonl', 'w') as f:
            for di in dataset_infos:
                f.write(di.to_dict())

        logger.info(f'Generated dataset_data.jsonl with {len(dataset_infos)} entries')
        return dataset_infos

    def _load_previous_acc_data(
        self, category: Literal['model', 'dataset']
    ) -> dict[str, ModelInfo | DatasetInfo]:
        """Load accumulated data from previous month."""
        result = {}
        first_day_of_month = self.target_date.replace(day=1)
        prev_month_date = first_day_of_month - timedelta(days=1)
        prev_month_str = prev_month_date.strftime('%Y-%m')
        prev_dir = self.output_root / f'osir-lmts_{prev_month_str}'
        file_path = prev_dir / f'acc_{category}_data.jsonl'

        if not file_path.exists():
            return result

        if category == 'model':
            info_cls = ModelInfo
        elif category == 'dataset':
            info_cls = DatasetInfo

        with jsonlines.open(file_path) as f:
            for line in f:
                info = info_cls.from_acc_dict(line)
                result[info.identifier] = info

        return result

    def gen_acc_model_data(self, model_infos: list[ModelInfo]) -> list[ModelInfo]:
        """Generate acc_model_data.jsonl with accumulated downloads."""
        prev_acc = self._load_previous_acc_data('model')
        acc_infos: list[ModelInfo] = []

        for mi in model_infos:
            prev = prev_acc.get(mi.identifier)

            acc_downloads = mi.downloads_last_month or 0
            if prev and prev.downloads_last_month is not None:
                acc_downloads += prev.downloads_last_month

            acc_likes = mi.likes
            if prev and prev.likes is not None:
                if acc_likes is None:
                    acc_likes = prev.likes
                else:
                    acc_likes = max(acc_likes, prev.likes)

            acc_discussions = mi.discussions
            if prev and prev.discussions is not None:
                if acc_discussions is None:
                    acc_discussions = prev.discussions
                else:
                    acc_discussions = max(acc_discussions, prev.discussions)

            acc_info = ModelInfo(
                identifier=mi.identifier,
                date_crawl=mi.date_crawl,
                downloads_last_month=acc_downloads if acc_downloads > 0 else None,
                likes=acc_likes,
                discussions=acc_discussions,
                descendants=mi.descendants,
                modality=mi.modality,
            )
            acc_infos.append(acc_info)

        with jsonlines.open(self.out_dir / 'acc_model_data.jsonl', 'w') as f:
            for ai in acc_infos:
                f.write(ai.to_acc_dict())

        logger.info(f'Generated acc_model_data.jsonl with {len(acc_infos)} entries')
        return acc_infos

    def gen_acc_dataset_data(self, dataset_infos: list[DatasetInfo]) -> list[DatasetInfo]:
        """Generate acc_dataset_data.jsonl with accumulated downloads."""
        prev_acc = self._load_previous_acc_data('dataset')
        acc_infos: list[DatasetInfo] = []

        for di in dataset_infos:
            prev = prev_acc.get(di.identifier)

            acc_downloads = di.downloads_last_month or 0
            if prev and prev.downloads_last_month is not None:
                acc_downloads += prev.downloads_last_month

            acc_likes = di.likes
            if prev and prev.likes is not None:
                if acc_likes is None:
                    acc_likes = prev.likes
                else:
                    acc_likes = max(acc_likes, prev.likes)

            acc_discussions = di.discussions
            if prev and prev.discussions is not None:
                if acc_discussions is None:
                    acc_discussions = prev.discussions
                else:
                    acc_discussions = max(acc_discussions, prev.discussions)

            acc_info = DatasetInfo(
                identifier=di.identifier,
                date_crawl=di.date_crawl,
                downloads_last_month=acc_downloads if acc_downloads > 0 else None,
                likes=acc_likes,
                discussions=acc_discussions,
                descendants=di.descendants,
                modality=di.modality,
                lifecycle=di.lifecycle,
            )
            acc_infos.append(acc_info)

        with jsonlines.open(self.out_dir / 'acc_dataset_data.jsonl', 'w') as f:
            for ai in acc_infos:
                f.write(ai.to_acc_dict())

        logger.info(f'Generated acc_dataset_data.jsonl with {len(acc_infos)} entries')
        return acc_infos

    def _get_org_for_identifier(self, identifier: str) -> str:
        """Get organization for an identifier."""
        repo = identifier.split('/')[0] if '/' in identifier else identifier
        return self._org_map.get(repo, repo)

    def _load_other_source_datasets(self) -> list[dict]:
        """Load other source datasets from config."""
        other_path = self.config_root / 'other_source_datasets.jsonl'
        if not other_path.exists():
            return []

        datasets = []
        with jsonlines.open(other_path) as f:
            for line in f:
                datasets.append(line)
        return datasets

    def summary_model_data(
        self,
        model_infos: list[ModelInfo],
        prefix: str = '',
        write_csv: bool = True,
    ) -> ModelSummaryTable:
        """Generate model_summary.csv."""
        org_data = ModelSummaryRow.get_defaultdict()

        for mi in model_infos:
            org = self._get_org_for_identifier(mi.identifier)
            modality = mi.modality or 'Unknown'

            downloads = mi.downloads_last_month or 0
            likes = mi.likes or 0
            discussions = mi.discussions or 0
            descendants = mi.descendants or 0

            org_data[org]['likes'] += likes
            org_data[org]['issue'] += discussions
            org_data[org]['descendants'] += descendants

            modality_key_map = ModelSummaryRow.get_modality_key_map()
            mod_key = modality_key_map.get(modality)
            if mod_key:
                org_data[org][f'downloads_{mod_key}'] += downloads
                org_data[org][f'num_{mod_key}'] += 1

        for org_info in self._org_list:
            org = org_info.org
            metadata = self._org_metadata.get(org, {})
            org_data[org]['num_adapted_chips'] = metadata.get('chips', 0)

        rows = []
        for org_info in self._org_list:
            org = org_info.org
            if org not in org_data:
                logger.warning(f'{org} not found in data, while found in config.')
            else:
                row = ModelSummaryRow(org=org, **org_data[org])
                rows.append(row)

        table = ModelSummaryTable(rows=rows)

        if write_csv:
            filename = f'{prefix}model_summary.csv' if prefix else 'model_summary.csv'
            table.to_csv(self.out_dir / filename, others_as_float=False)
            logger.info(f'Generated {filename} with {len(rows)} rows')

        return table

    def summary_dataset_data(
        self,
        dataset_infos: list[DatasetInfo],
        prefix: str = '',
        write_csv: bool = True,
    ) -> DatasetSummaryTable:
        """Generate dataset_summary.csv using pandas."""
        org_data = DatasetSummaryRow.get_defaultdict()

        for di in dataset_infos:
            org = self._get_org_for_identifier(di.identifier)
            modality = di.modality or 'Unknown'
            lifecycle = di.lifecycle or 'Unknown'

            downloads = di.downloads_last_month or 0
            org_data[org]['dataset_usage'] += 1

            modality_key_map = DatasetSummaryRow.get_modality_key_map()
            mod_key = modality_key_map.get(modality)
            if mod_key:
                org_data[org][f'num_{mod_key}'] += 1
                org_data[org][f'downloads_{mod_key}'] += downloads

            lifecycle_key_map = DatasetSummaryRow.get_lifecycle_key_map()
            lc_key = lifecycle_key_map.get(lifecycle)
            if lc_key:
                org_data[org][f'num_{lc_key}'] += 1
                org_data[org][f'downloads_{lc_key}'] += downloads

        other_datasets = self._load_other_source_datasets()
        for ds in other_datasets:
            org = ds.get('org', 'Unknown')
            modality = ds.get('modality', 'Unknown')
            lifecycle = ds.get('lifecycle', 'Unknown')

            if org not in org_data:
                for org_info in self._org_list:
                    if org_info.org == org:
                        break
                else:
                    continue

            modality_key_map = DatasetSummaryRow.get_modality_key_map()
            mod_key = modality_key_map.get(modality)
            if mod_key:
                org_data[org][f'num_{mod_key}'] += 1

            lifecycle_key_map = DatasetSummaryRow.get_lifecycle_key_map()
            lc_key = lifecycle_key_map.get(lifecycle)
            if lc_key:
                org_data[org][f'num_{lc_key}'] += 1

        for org_info in self._org_list:
            org = org_info.org
            metadata = self._org_metadata.get(org, {})
            org_data[org]['operators'] = metadata.get('dataset_ops', 0)

        rows = []
        for org_info in self._org_list:
            org = org_info.org
            if org not in org_data:
                logger.warning(f'{org} not found in data, while found in config.')
            else:
                row = DatasetSummaryRow(org=org, **org_data[org])
                rows.append(row)

        table = DatasetSummaryTable(rows=rows)

        if write_csv:
            filename = f'{prefix}dataset_summary.csv' if prefix else 'dataset_summary.csv'
            table.to_csv(self.out_dir / filename, others_as_float=False)
            logger.info(f'Generated {filename} with {len(rows)} rows')

        return table

    def summary_infra_data(
        self,
        infra_source_path: Path | None = None,
        write_csv: bool = True,
    ) -> InfraSummaryTable:
        """Process and copy infra_summary.csv from source path."""
        if infra_source_path is None:
            infra_source_path = self.out_dir / 'infra_summary.csv'
            if infra_source_path.exists():
                logger.debug(f'Infra source file found in {infra_source_path}')
                return InfraSummaryTable.from_csv(infra_source_path, raw_csv=False)
            infra_source_path = Path(__file__).parents[3] / 'infra_summary.csv'

        if not infra_source_path.exists():
            logger.error(f'Infra source file {infra_source_path} not found')
            raise RuntimeError(f'Infra source file {infra_source_path} not found')

        table = InfraSummaryTable.from_csv(infra_source_path, raw_csv=True)

        if write_csv:
            table.to_csv(self.out_dir / 'infra_summary.csv', others_as_float=False)
            logger.info(f'Processed infra_summary.csv from {infra_source_path}')
            infra_source_path.unlink(missing_ok=True)

        return table

    def summary_eval_data(
        self,
        eval_source_path: Path | None = None,
        write_csv: bool = True,
    ) -> EvalSummaryTable:
        """Process and copy eval_summary.csv from source path."""
        if eval_source_path is None:
            eval_source_path = self.out_dir / 'eval_summary.csv'
            if eval_source_path.exists():
                logger.debug(f'Eval source file found in {eval_source_path}')
                return EvalSummaryTable.from_csv(eval_source_path, raw_csv=False)
            eval_source_path = Path(__file__).parents[3] / 'eval_summary.csv'

        if not eval_source_path.exists():
            logger.error(f'Eval source file {eval_source_path} not found')
            raise RuntimeError(f'Eval source file {eval_source_path} not found')

        table = EvalSummaryTable.from_csv(eval_source_path, raw_csv=True)

        if write_csv:
            table.to_csv(self.out_dir / 'eval_summary.csv', others_as_float=False)
            logger.info(f'Processed eval_summary.csv from {eval_source_path}')
            eval_source_path.unlink(missing_ok=True)

        return table

    def _load_prev_month_summary(self, filename: str) -> pd.DataFrame | None:
        """Load summary data from previous month."""
        first_day_of_month = self.target_date.replace(day=1)
        prev_month_date = first_day_of_month - timedelta(days=1)
        prev_month_str = prev_month_date.strftime('%Y-%m')
        prev_dir = self.output_root / f'osir-lmts_{prev_month_str}'
        prev_file = prev_dir / filename

        if not prev_file.exists():
            logger.warning(
                f'Previous month summary {prev_file} not found, delta will be same as current'
            )
            return None

        return pd.read_csv(prev_file, index_col='org')

    def delta_model_data(self, model_table: ModelSummaryTable) -> None:
        """Generate delta summary (current month - previous month)."""
        curr_df = model_table.to_dataframe(others_as_float=False)
        prev_df = self._load_prev_month_summary('model_summary.csv')
        if prev_df is None:
            delta_df = pd.DataFrame(index=curr_df.index, columns=curr_df.columns)
        else:
            prev_df = prev_df.reindex(curr_df.index)
            delta_df = curr_df.sub(prev_df)

        delta_df.to_csv(self.out_dir / 'delta_model_summary.csv', na_rep='-')
        logger.info(f'Generated delta_model_summary.csv with {len(delta_df)} rows')

    def delta_dataset_data(self, dataset_table: DatasetSummaryTable) -> None:
        """Generate delta summary (current month - previous month)."""
        curr_df = dataset_table.to_dataframe(others_as_float=False)
        prev_df = self._load_prev_month_summary('dataset_summary.csv')
        if prev_df is None:
            delta_df = pd.DataFrame(index=curr_df.index, columns=curr_df.columns)
        else:
            prev_df = prev_df.reindex(curr_df.index)
            delta_df = curr_df.sub(prev_df)

        delta_df.to_csv(self.out_dir / 'delta_dataset_summary.csv', na_rep='-')
        logger.info(f'Generated delta_dataset_summary.csv with {len(delta_df)} rows')

    def _add_rank_metadata(
        self,
        table: BaseSummaryTable,
        last_month_df: pd.DataFrame | None,
    ) -> BaseSummaryTable:
        """Add delta_rank and last_month_rank to the table."""
        df = table.to_dataframe()

        if last_month_df is not None and 'rank' in last_month_df.columns:
            last_month_df = last_month_df.reindex(df.index)
            df['last_month_rank'] = last_month_df['rank']
            df['delta_rank'] = df['last_month_rank'] - df['rank']
        else:
            df['last_month_rank'] = None
            df['delta_rank'] = None

        return table.from_dataframe(df)

    def gen_rank(
        self,
        strategy: OsirLmtsRankStrategy,
        model_table: ModelSummaryTable,
        dataset_table: DatasetSummaryTable,
        infra_table: InfraSummaryTable,
        eval_table: EvalSummaryTable,
        acc: bool = False,
    ) -> None:
        """Generate rankings using the given strategy."""
        model_rank = strategy.rank_model_dim(model_table, acc=acc)
        dataset_rank = strategy.rank_dataset_dim(dataset_table, acc=acc)
        infra_rank = strategy.rank_infra_dim(infra_table, acc=acc)
        eval_rank = strategy.rank_eval_dim(eval_table, acc=acc)
        overall_rank = strategy.rank_overall(
            model_rank, dataset_rank, infra_rank, eval_rank, acc=acc
        )
        if acc:
            last_month_model_rank_df = self._load_prev_month_summary('acc_model_rank.csv')
            last_month_dataset_rank_df = self._load_prev_month_summary('acc_dataset_rank.csv')
            last_month_overall_rank_df = self._load_prev_month_summary('acc_overall_rank.csv')

            model_rank = self._add_rank_metadata(model_rank, last_month_model_rank_df)
            dataset_rank = self._add_rank_metadata(dataset_rank, last_month_dataset_rank_df)
            overall_rank = self._add_rank_metadata(overall_rank, last_month_overall_rank_df)

            model_rank.to_csv(self.out_dir / 'acc_model_rank.csv')
            dataset_rank.to_csv(self.out_dir / 'acc_dataset_rank.csv')
            overall_rank.to_csv(self.out_dir / 'acc_overall_rank.csv')
        else:
            last_month_model_rank_df = self._load_prev_month_summary('model_rank.csv')
            last_month_dataset_rank_df = self._load_prev_month_summary('dataset_rank.csv')
            last_month_infra_rank_df = self._load_prev_month_summary('infra_rank.csv')
            last_month_eval_rank_df = self._load_prev_month_summary('eval_rank.csv')
            last_month_overall_rank_df = self._load_prev_month_summary('overall_rank.csv')

            model_rank = self._add_rank_metadata(model_rank, last_month_model_rank_df)
            dataset_rank = self._add_rank_metadata(dataset_rank, last_month_dataset_rank_df)
            infra_rank = self._add_rank_metadata(infra_rank, last_month_infra_rank_df)
            eval_rank = self._add_rank_metadata(eval_rank, last_month_eval_rank_df)
            overall_rank = self._add_rank_metadata(overall_rank, last_month_overall_rank_df)

            model_rank.to_csv(self.out_dir / 'model_rank.csv')
            dataset_rank.to_csv(self.out_dir / 'dataset_rank.csv')
            infra_rank.to_csv(self.out_dir / 'infra_rank.csv')
            eval_rank.to_csv(self.out_dir / 'eval_rank.csv')
            overall_rank.to_csv(self.out_dir / 'overall_rank.csv')

    def gen_rank_for_country(
        self,
        strategy: OsirLmtsRankStrategy,
        model_table: ModelSummaryTable,
        dataset_table: DatasetSummaryTable,
        infra_table: InfraSummaryTable,
        eval_table: EvalSummaryTable,
        country: str = 'CN',
        acc: bool = False,
    ) -> None:
        target_orgs = {org_info.org for org_info in self._org_list if org_info.country == country}

        filtered_model_table = ModelSummaryTable()
        for row in model_table.rows:
            if row.org in target_orgs:
                filtered_model_table.rows.append(row)

        filtered_dataset_table = DatasetSummaryTable()
        for row in dataset_table.rows:
            if row.org in target_orgs:
                filtered_dataset_table.rows.append(row)

        filtered_infra_table = InfraSummaryTable()
        for row in infra_table.rows:
            if row.org in target_orgs:
                filtered_infra_table.rows.append(row)

        filtered_eval_table = EvalSummaryTable()
        for row in eval_table.rows:
            if row.org in target_orgs:
                filtered_eval_table.rows.append(row)

        model_rank = strategy.rank_model_dim(filtered_model_table, acc=acc)
        dataset_rank = strategy.rank_dataset_dim(filtered_dataset_table, acc=acc)
        infra_rank = strategy.rank_infra_dim(filtered_infra_table, acc=acc)
        eval_rank = strategy.rank_eval_dim(filtered_eval_table, acc=acc)
        overall_rank = strategy.rank_overall(
            model_rank, dataset_rank, infra_rank, eval_rank, acc=acc
        )
        if acc:
            last_month_overall_rank_df = self._load_prev_month_summary(
                f'{country}_acc_overall_rank.csv'
            )
            overall_rank = self._add_rank_metadata(overall_rank, last_month_overall_rank_df)
            overall_rank.to_csv(self.out_dir / f'{country}_acc_overall_rank.csv')
        else:
            last_month_overall_rank_df = self._load_prev_month_summary(
                f'{country}_overall_rank.csv'
            )
            overall_rank = self._add_rank_metadata(overall_rank, last_month_overall_rank_df)
            overall_rank.to_csv(self.out_dir / f'{country}_overall_rank.csv')

    def run(
        self,
        strategy: OsirLmtsRankStrategy | None = None,
        infra_source_path: Path | None = None,
        eval_source_path: Path | None = None,
    ) -> None:
        """Run the complete OSIR-LMTS pipeline."""
        if strategy is None:
            strategy = DefaultRankStrategy()

        logger.info(f'Starting OSIR-LMTS pipeline for {self.target_month}')

        model_infos = self.gen_model_data()
        dataset_infos = self.gen_dataset_data()

        acc_model_infos = self.gen_acc_model_data(model_infos)
        acc_dataset_infos = self.gen_acc_dataset_data(dataset_infos)

        # Generate summary tables
        model_table = self.summary_model_data(model_infos)
        dataset_table = self.summary_dataset_data(dataset_infos)

        acc_model_table = self.summary_model_data(acc_model_infos, prefix='acc_')
        acc_dataset_table = self.summary_dataset_data(acc_dataset_infos, prefix='acc_')

        self.delta_model_data(model_table)
        self.delta_dataset_data(dataset_table)

        infra_table = self.summary_infra_data(infra_source_path)
        eval_table = self.summary_eval_data(eval_source_path)

        # Generate rankings
        self.gen_rank(strategy, model_table, dataset_table, infra_table, eval_table)
        self.gen_rank(strategy, acc_model_table, acc_dataset_table, infra_table, eval_table, True)

        self.gen_rank_for_country(
            strategy, model_table, dataset_table, infra_table, eval_table, 'CN'
        )
        self.gen_rank_for_country(
            strategy, acc_model_table, acc_dataset_table, infra_table, eval_table, 'CN', True
        )

        logger.info(f'OSIR-LMTS pipeline complete. Output in {self.out_dir}')
