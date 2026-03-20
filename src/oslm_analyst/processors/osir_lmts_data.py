from itertools import dropwhile
import csv
from collections import defaultdict
from pathlib import Path
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from typing import TypeVar
import pandas as pd

from oslm_analyst.data_utils import Lifecycle, Modality


@dataclass
class ModelInfo:
    identifier: str
    date_crawl: str
    downloads_last_month: int | None = field(default=None)
    likes: int | None = field(default=None)
    discussions: int | None = field(default=None)
    descendants: int | None = field(default=None)
    modality: Modality | None = field(default=None)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'ModelInfo':
        return cls(**d)

    def to_acc_dict(self) -> dict:
        d = asdict(self)
        if 'downloads_last_month' in d:
            d['downloads'] = d.pop('downloads_last_month')
        return d

    @classmethod
    def from_acc_dict(cls, d: dict) -> 'ModelInfo':
        d = d.copy()
        if 'downloads' in d:
            d['downloads_last_month'] = d.pop('downloads')
        return cls.from_dict(d)


@dataclass
class DatasetInfo:
    identifier: str
    date_crawl: str
    downloads_last_month: int | None = field(default=None)
    likes: int | None = field(default=None)
    discussions: int | None = field(default=None)
    descendants: int | None = field(default=None)
    modality: Modality | None = field(default=None)
    lifecycle: Lifecycle | None = field(default=None)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'DatasetInfo':
        return cls(**d)

    def to_acc_dict(self) -> dict:
        d = asdict(self)
        if 'downloads_last_month' in d:
            d['downloads'] = d.pop('downloads_last_month')
        return d

    @classmethod
    def from_acc_dict(cls, d: dict) -> 'DatasetInfo':
        d = d.copy()
        if 'downloads' in d:
            d['downloads_last_month'] = d.pop('downloads')
        return cls.from_dict(d)


@dataclass
class RawDataPoint:
    identifier: str
    repo: str
    name: str
    platform: str
    date_crawl: str
    downloads_last_month: int | None = None
    downloads_total: int | None = None
    likes: int | None = None
    discussions: int | None = None
    modality: Modality | None = None
    lifecycle: Lifecycle | None = None
    valid: bool = True


@dataclass
class ModelSummaryRow:
    """Single row of model dimension summary data."""

    org: str
    downloads_vision: int = 0
    downloads_language: int = 0
    downloads_speech: int = 0
    downloads_3d: int = 0
    downloads_multimodal: int = 0
    downloads_protein: int = 0
    downloads_vector: int = 0
    descendants: int = 0
    num_vision: int = 0
    num_language: int = 0
    num_speech: int = 0
    num_3d: int = 0
    num_multimodal: int = 0
    num_protein: int = 0
    num_vector: int = 0
    likes: int = 0
    issue: int = 0
    num_adapted_chips: int = 0
    score: float | None = None
    rank: int | None = None
    delta_rank: int | None = None
    last_month_rank: int | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        for k, v in d.copy().items():
            if v is None:
                d.pop(k)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'ModelSummaryRow':
        return cls(**d)

    @staticmethod
    def get_defaultdict() -> defaultdict:
        return defaultdict(
            lambda: {
                'downloads_vision': 0,
                'downloads_language': 0,
                'downloads_speech': 0,
                'downloads_3d': 0,
                'downloads_multimodal': 0,
                'downloads_protein': 0,
                'downloads_vector': 0,
                'descendants': 0,
                'num_vision': 0,
                'num_language': 0,
                'num_speech': 0,
                'num_3d': 0,
                'num_multimodal': 0,
                'num_protein': 0,
                'num_vector': 0,
                'likes': 0,
                'issue': 0,
                'num_adapted_chips': 0,
            }
        )

    @staticmethod
    def get_modality_key_map() -> dict:
        return {
            'Vision': 'vision',
            'Language': 'language',
            'Speech': 'speech',
            '3D': '3d',
            'Multimodal': 'multimodal',
            'Protein': 'protein',
            'Vector': 'vector',
            'Embodied': 'multimodal',
        }


@dataclass
class DatasetSummaryRow:
    """Single row of dataset dimension summary data."""

    org: str
    num_language: int = 0
    num_speech: int = 0
    num_vision: int = 0
    num_multimodal: int = 0
    num_embodied: int = 0
    downloads_language: int = 0
    downloads_speech: int = 0
    downloads_vision: int = 0
    downloads_multimodal: int = 0
    downloads_embodied: int = 0
    dataset_usage: int = 0
    num_pretraining: int = 0
    num_finetuning: int = 0
    num_preference: int = 0
    downloads_pretraining: int = 0
    downloads_finetuning: int = 0
    downloads_preference: int = 0
    operators: int = 0
    score: float | None = None
    rank: int | None = None
    delta_rank: int | None = None
    last_month_rank: int | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        for k, v in d.copy().items():
            if v is None:
                d.pop(k)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'DatasetSummaryRow':
        return cls(**d)

    @staticmethod
    def get_defaultdict() -> defaultdict:
        return defaultdict(
            lambda: {
                'num_language': 0,
                'num_speech': 0,
                'num_vision': 0,
                'num_multimodal': 0,
                'num_embodied': 0,
                'downloads_language': 0,
                'downloads_speech': 0,
                'downloads_vision': 0,
                'downloads_multimodal': 0,
                'downloads_embodied': 0,
                'dataset_usage': 0,
                'num_pretraining': 0,
                'num_finetuning': 0,
                'num_preference': 0,
                'downloads_pretraining': 0,
                'downloads_finetuning': 0,
                'downloads_preference': 0,
                'operators': 0,
            }
        )

    @staticmethod
    def get_modality_key_map() -> dict:
        return {
            'Language': 'language',
            'Speech': 'speech',
            'Vision': 'vision',
            'Multimodal': 'multimodal',
            'Embodied': 'embodied',
        }

    @staticmethod
    def get_lifecycle_key_map() -> dict:
        return {
            'Pre-training': 'pretraining',
            'Fine-tuning': 'finetuning',
            'Preference': 'preference',
        }


@dataclass
class InfraSummaryRow:
    """Single row of infrastructure dimension summary data."""

    org: str
    num_operators: int = 0
    num_adapted_chips_operator_lib: int = 0
    num_adapted_frameworks_operator_lib: int = 0
    support_heterogeneous_training_frameworks: int = 0
    num_adapted_chips_frameworks: int = 0
    support_lifecycle: int = 0
    num_adapted_frameworks_ai_compiler: int = 0
    num_adapted_chips_ai_compiler: int = 0
    deep_learning_framework: int = 0
    num_adapted_chips_communication_lib: int = 0
    support_cross_chip_communication: int = 0
    support_heterogeneous_training_communication_lib: int = 0
    score: float | None = None
    rank: int | None = None
    delta_rank: int | None = None
    last_month_rank: int | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        for k, v in d.copy().items():
            if v is None:
                d.pop(k)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'InfraSummaryRow':
        return cls(**d)


@dataclass
class EvalSummaryRow:
    """Single row of evaluation dimension summary data."""

    org: str
    num_leaderboards: int = 0
    num_evaluated_models: int = 0
    num_evaluation_datasets: int = 0
    num_evaluation_methods: int = 0
    num_evaluation_tools: int = 0
    score: float | None = None
    rank: int | None = None
    delta_rank: int | None = None
    last_month_rank: int | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        for k, v in d.copy().items():
            if v is None:
                d.pop(k)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'EvalSummaryRow':
        return cls(**d)


@dataclass
class OverallSummaryRow:
    org: str
    dataset_influence: float = 0
    model_influence: float = 0
    infra_influence: float = 0
    eval_influence: float = 0
    score: float | None = None
    rank: int | None = None
    delta_rank: int | None = None
    last_month_rank: int | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        for k, v in d.copy().items():
            if v is None:
                d.pop(k)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'OverallSummaryRow':
        return cls(**d)


RowType = TypeVar('RowType', ModelSummaryRow, DatasetSummaryRow, InfraSummaryRow, EvalSummaryRow)


class BaseSummaryTable[RowType](ABC):
    """Abstract base class for summary tables."""

    rows: list[RowType]

    @abstractmethod
    def _row_from_dict(self, d: dict) -> RowType:
        """Create a row from a dictionary."""
        pass

    @abstractmethod
    def _get_row_class(self) -> type:
        """Get the row class."""
        pass

    def to_dataframe(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        data = [row.to_dict() for row in self.rows]  # type: ignore
        return pd.DataFrame(data).set_index('org')

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame):
        """Build from DataFrame."""
        table = cls()
        for org, row in df.iterrows():
            table.rows.append(table._row_from_dict({'org': org, **row.to_dict()}))
        return table

    @classmethod
    def from_csv(cls, file_path: Path):
        """Build from csv file."""
        df = pd.read_csv(file_path, index_col='org')
        return cls.from_dataframe(df)

    def to_csv(self, file_path: Path) -> None:
        """Write to CSV file."""
        df = self.to_dataframe()
        df.to_csv(file_path)

    def get_orgs(self) -> list[str]:
        """Get all organizations."""
        return [row.org for row in self.rows]  # type: ignore

    def get_row_for_org(self, org: str) -> RowType | None:
        """Get row for a specific organization."""
        for row in self.rows:
            if row.org == org:  # type: ignore
                return row
        return None


@dataclass
class ModelSummaryTable(BaseSummaryTable[ModelSummaryRow]):
    """Model dimension summary table."""

    rows: list[ModelSummaryRow] = field(default_factory=list)

    def _row_from_dict(self, d: dict) -> ModelSummaryRow:
        return ModelSummaryRow.from_dict(d)

    def _get_row_class(self) -> type:
        return ModelSummaryRow


@dataclass
class DatasetSummaryTable(BaseSummaryTable[DatasetSummaryRow]):
    """Dataset dimension summary table."""

    rows: list[DatasetSummaryRow] = field(default_factory=list)

    def _row_from_dict(self, d: dict) -> DatasetSummaryRow:
        return DatasetSummaryRow.from_dict(d)

    def _get_row_class(self) -> type:
        return DatasetSummaryRow


@dataclass
class InfraSummaryTable(BaseSummaryTable[InfraSummaryRow]):
    """Infrastructure dimension summary table."""

    rows: list[InfraSummaryRow] = field(default_factory=list)

    def _row_from_dict(self, d: dict) -> InfraSummaryRow:
        return InfraSummaryRow.from_dict(d)

    def _get_row_class(self) -> type:
        return InfraSummaryRow

    @classmethod
    def from_csv(cls, file_path: Path, raw_csv: bool = True):
        if not raw_csv:
            return cls.from_dataframe(pd.read_csv(file_path, index_col='org'))
        table = cls()
        with file_path.open('r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            next(csv_reader)

            for row in csv_reader:
                row = list(dropwhile(lambda x: x in ('', None), reversed(row)))[::-1]
                table.rows.append(InfraSummaryRow(row[0], *[int(item) for item in row[1:]]))
        return table


@dataclass
class EvalSummaryTable(BaseSummaryTable[EvalSummaryRow]):
    """Evaluation dimension summary table."""

    rows: list[EvalSummaryRow] = field(default_factory=list)

    def _row_from_dict(self, d: dict) -> EvalSummaryRow:
        return EvalSummaryRow.from_dict(d)

    def _get_row_class(self) -> type:
        return EvalSummaryRow

    @classmethod
    def from_csv(cls, file_path: Path, raw_csv: bool = True):
        if not raw_csv:
            return cls.from_dataframe(pd.read_csv(file_path, index_col='org'))
        table = cls()
        with file_path.open('r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)
            next(csv_reader)

            for row in csv_reader:
                row = list(dropwhile(lambda x: x in ('', None), reversed(row)))[::-1]
                table.rows.append(EvalSummaryRow(row[0], *[int(item) for item in row[1:]]))
        return table


@dataclass
class OverallSummaryTable(BaseSummaryTable[OverallSummaryRow]):
    rows: list[OverallSummaryRow] = field(default_factory=list)

    def _row_from_dict(self, d: dict) -> OverallSummaryRow:
        return OverallSummaryRow.from_dict(d)

    def _get_row_class(self) -> type:
        return OverallSummaryRow
