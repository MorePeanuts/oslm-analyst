""" """

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class ModelInfo:
    identifier: str
    date_crawl: str
    downloads_last_month: int | None = field(default=None)
    likes: int | None = field(default=None)
    discussions: int | None = field(default=None)
    descendants: int | None = field(default=None)
    modality: (
        Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Vector', 'Protein', '3D', 'Embodied']
        | None
    ) = field(default=None)


@dataclass
class DatasetInfo:
    identifier: str
    date_crawl: str
    downloads_last_month: int | None = field(default=None)
    likes: int | None = field(default=None)
    discussions: int | None = field(default=None)
    descendants: int | None = field(default=None)
    modality: (
        Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Vector', 'Protein', '3D', 'Embodied']
        | None
    ) = field(default=None)
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation'] | None = field(
        default=None
    )


@dataclass
class InfraInfo:
    pass


@dataclass
class EvalInfo:
    pass
