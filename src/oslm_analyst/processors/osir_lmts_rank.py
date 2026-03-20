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
    def rank_model_dim(self, model_table: ModelSummaryTable) -> ModelSummaryTable:
        pass

    @abstractmethod
    def rank_dataset_dim(self, dataset_table: DatasetSummaryTable) -> DatasetSummaryTable:
        pass

    @abstractmethod
    def rank_infra_dim(self, infra_table: InfraSummaryTable) -> InfraSummaryTable:
        pass

    @abstractmethod
    def rank_eval_dim(self, eval_table: EvalSummaryTable) -> EvalSummaryTable:
        pass

    @abstractmethod
    def rank_overall(
        self,
        model_table: ModelSummaryTable,
        dataset_table: DatasetSummaryTable,
        infra_table: InfraSummaryTable,
        eval_table: EvalSummaryTable,
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

        assert set(model_table.get_orgs()) == set(dataset_table.get_orgs())
        assert set(dataset_table.get_orgs()) == set(infra_table.get_orgs())
        assert set(infra_table.get_orgs()) == set(eval_table.get_orgs())
        orgs = model_table.get_orgs()

        df = pd.DataFrame(index=orgs)  # type: ignore
        df['model_influence'] = 1 / np.log2(model_df['rank'] + 1)
        df['dataset_influence'] = 1 / np.log2(dataset_df['rank'] + 1)
        df['infra_influence'] = 1 / np.log2(infra_df['rank'] + 1)
        df['eval_influence'] = 1 / np.log2(eval_df['rank'] + 1)
        return OverallSummaryTable.from_dataframe(df)


class DefaultRankStrategy(OsirLmtsRankStrategy):
    """Default ranking strategy with average weights."""

    def rank_model_dim(self, model_table: ModelSummaryTable) -> ModelSummaryTable:
        table = self._normalize(model_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return ModelSummaryTable.from_dataframe(df)

    def rank_dataset_dim(self, dataset_table: DatasetSummaryTable) -> DatasetSummaryTable:
        table = self._normalize(dataset_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return DatasetSummaryTable.from_dataframe(df)

    def rank_infra_dim(self, infra_table: InfraSummaryTable) -> InfraSummaryTable:
        table = self._normalize(infra_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return InfraSummaryTable.from_dataframe(df)

    def rank_eval_dim(self, eval_table: EvalSummaryTable) -> EvalSummaryTable:
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
    ) -> OverallSummaryTable:
        table = self._normalize_overall(model_table, dataset_table, infra_table, eval_table)
        df = table.to_dataframe()
        df['score'] = df.mean(axis=1)
        df['rank'] = df['score'].rank(ascending=False, method='dense').astype(int)
        return OverallSummaryTable.from_dataframe(df)
