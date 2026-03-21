from datetime import datetime
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from .osir_lmts_data import (
    ModelSummaryTable,
    DatasetSummaryTable,
    InfraSummaryTable,
    EvalSummaryTable,
    OverallSummaryTable,
    BaseSummaryTable,
)


class OsirLmtsRankStrategy(ABC):
    """Abstract base class for ranking strategies."""

    @abstractmethod
    def rank_model_dim(self, model_table: ModelSummaryTable, acc: bool = False) -> ModelSummaryTable:
        pass

    @abstractmethod
    def rank_dataset_dim(self, dataset_table: DatasetSummaryTable, acc: bool = False) -> DatasetSummaryTable:
        pass

    @abstractmethod
    def rank_infra_dim(self, infra_table: InfraSummaryTable, acc: bool = False) -> InfraSummaryTable:
        pass

    @abstractmethod
    def rank_eval_dim(self, eval_table: EvalSummaryTable, acc: bool = False) -> EvalSummaryTable:
        pass

    @abstractmethod
    def rank_overall(
        self,
        model_table: ModelSummaryTable,
        dataset_table: DatasetSummaryTable,
        infra_table: InfraSummaryTable,
        eval_table: EvalSummaryTable,
        acc: bool = False,
    ) -> OverallSummaryTable:
        pass

    def _normalize(self, table: BaseSummaryTable) -> BaseSummaryTable:
        df = table.to_dataframe()
        col_max = df.max()
        df_normalized = df.div(col_max.replace(0, 1))
        return table.from_dataframe(df_normalized)

    def _normalize_overall(
        self,
        model_table: ModelSummaryTable,
        dataset_table: DatasetSummaryTable,
        infra_table: InfraSummaryTable,
        eval_table: EvalSummaryTable,
    ) -> OverallSummaryTable:
        model_df = model_table.to_dataframe()
        dataset_df = dataset_table.to_dataframe()
        infra_df = infra_table.to_dataframe()
        eval_df = eval_table.to_dataframe()

        orgs = list(
            set(model_table.get_orgs())
            | set(dataset_table.get_orgs())
            | set(infra_table.get_orgs())
            | set(eval_table.get_orgs())
        )

        df = pd.DataFrame(index=orgs)  # type: ignore
        df['model_influence'] = 1 / np.log2(model_df['rank'] + 1)
        df['dataset_influence'] = 1 / np.log2(dataset_df['rank'] + 1)
        df['infra_influence'] = 1 / np.log2(infra_df['rank'] + 1)
        df['eval_influence'] = 1 / np.log2(eval_df['rank'] + 1)
        return OverallSummaryTable.from_dataframe(df)


class DefaultRankStrategy(OsirLmtsRankStrategy):
    """Default ranking strategy with average weights."""

    def rank_model_dim(self, model_table: ModelSummaryTable, acc: bool = False) -> ModelSummaryTable:
        table = self._normalize(model_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return ModelSummaryTable.from_dataframe(df)

    def rank_dataset_dim(self, dataset_table: DatasetSummaryTable, acc: bool = False) -> DatasetSummaryTable:
        table = self._normalize(dataset_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return DatasetSummaryTable.from_dataframe(df)

    def rank_infra_dim(self, infra_table: InfraSummaryTable, acc: bool = False) -> InfraSummaryTable:
        table = self._normalize(infra_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return InfraSummaryTable.from_dataframe(df)

    def rank_eval_dim(self, eval_table: EvalSummaryTable, acc: bool = False) -> EvalSummaryTable:
        table = self._normalize(eval_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return EvalSummaryTable.from_dataframe(df)

    def rank_overall(
        self,
        model_table: ModelSummaryTable,
        dataset_table: DatasetSummaryTable,
        infra_table: InfraSummaryTable,
        eval_table: EvalSummaryTable,
        acc: bool = False,
    ) -> OverallSummaryTable:
        table = self._normalize_overall(model_table, dataset_table, infra_table, eval_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return OverallSummaryTable.from_dataframe(df)


class RankStrategyUpdated2603(DefaultRankStrategy):
    def rank_model_dim(self, model_table: ModelSummaryTable, acc: bool = False) -> ModelSummaryTable:
        table = self._normalize(model_table)
        df = table.to_dataframe()

        if acc:
            # Accumulated weights
            weights = {
                'downloads_vision': 0.5 * 0.02,
                'downloads_language': 0.5 * 0.5,
                'downloads_speech': 0.5 * 0.02,
                'downloads_3d': 0.5 * 0.02,
                'downloads_multimodal': 0.5 * 0.2,
                'downloads_protein': 0.5 * 0.02,
                'downloads_vector': 0.5 * 0.2,
                'descendants': 0.5 * 0.02,
                'num_vision': 0.2 * 0.025,
                'num_language': 0.2 * 0.5,
                'num_speech': 0.2 * 0.025,
                'num_3d': 0.2 * 0.025,
                'num_multimodal': 0.2 * 0.2,
                'num_protein': 0.2 * 0.025,
                'num_vector': 0.2 * 0.2,
                'likes': 0.1,
                'issue': 0.1,
                'num_adapted_chips': 0.1,
            }
        else:
            # Non-accumulated weights
            weights = {
                'downloads_vision': 0.6 * 0.02,
                'downloads_language': 0.6 * 0.5,
                'downloads_speech': 0.6 * 0.02,
                'downloads_3d': 0.6 * 0.02,
                'downloads_multimodal': 0.6 * 0.2,
                'downloads_protein': 0.6 * 0.02,
                'downloads_vector': 0.6 * 0.2,
                'descendants': 0.6 * 0.02,
                'num_vision': 0.1 * 0.025,
                'num_language': 0.1 * 0.5,
                'num_speech': 0.1 * 0.025,
                'num_3d': 0.1 * 0.025,
                'num_multimodal': 0.1 * 0.2,
                'num_protein': 0.1 * 0.025,
                'num_vector': 0.1 * 0.2,
                'likes': 0.1,
                'issue': 0.1,
                'num_adapted_chips': 0.1,
            }

        df['score'] = df[list(weights.keys())].mul(pd.Series(weights)).sum(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return ModelSummaryTable.from_dataframe(df)

    def rank_overall(
        self,
        model_table: ModelSummaryTable,
        dataset_table: DatasetSummaryTable,
        infra_table: InfraSummaryTable,
        eval_table: EvalSummaryTable,
        acc: bool = False,
    ) -> OverallSummaryTable:
        table = self._normalize_overall(model_table, dataset_table, infra_table, eval_table)
        df = table.to_dataframe()
        weights = {
            'model_influence': 0.5,
            'dataset_influence': 0.5 / 3,
            'infra_influence': 0.5 / 3,
            'eval_influence': 0.5 / 3,
        }
        df['score'] = df[list(weights.keys())].mul(pd.Series(weights)).sum(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return OverallSummaryTable.from_dataframe(df)


def get_rank_strategy_for_month(target_month: str) -> OsirLmtsRankStrategy:
    """
    Factory method to get the appropriate ranking strategy based on target month.

    Args:
        target_month: Target month in YYYY-MM format.

    Returns:
        Ranking strategy instance:
        - DefaultRankStrategy for 2026-02 and before
        - RankStrategyUpdated2603 for 2026-03 to 2026-12
    """
    target_date = datetime.strptime(target_month, '%Y-%m')

    # 2026-03 and later, before 2027-01: use RankStrategyUpdated2603
    if (target_date.year == 2026 and target_date.month >= 3):
        return RankStrategyUpdated2603()
    # 2026-02 and before: use DefaultRankStrategy
    else:
        return DefaultRankStrategy()
