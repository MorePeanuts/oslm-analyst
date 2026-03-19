import json
from typing import Literal
from dataclasses import dataclass, asdict, field
from enum import Enum


class Modality(str, Enum):
    Language = 'Language'
    Speech = 'Speech'
    Vision = 'Vision'
    Multimodal = 'Multimodal'
    Vector = 'Vector'
    Protein = 'Protein'
    ThreeDim = '3D'
    Embodied = 'Embodied'


class Lifecycle(str, Enum):
    Pretraining = 'Pre-training'
    Finetuning = 'Fine-tuning'
    Preference = 'Preference'
    Evaluation = 'Evaluation'


@dataclass
class ModelExtraInfo:
    repo: str
    name: str
    modality: Modality
    valid: bool
    link: str

    @classmethod
    def from_dict(cls, obj: dict) -> 'ModelExtraInfo':
        return cls(obj['repo'], obj['name'], obj['modality'], obj['valid'], obj['link'])

    @classmethod
    def from_dataclass(cls, obj) -> 'ModelExtraInfo':
        return cls(obj.repo, obj.name, obj.modality, obj.valid, obj.link)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DatasetExtraInfo:
    repo: str
    name: str
    modality: Modality
    lifecycle: Lifecycle
    valid: bool
    link: str

    @classmethod
    def from_dict(cls, obj: dict) -> 'DatasetExtraInfo':
        return cls(
            obj['repo'], obj['name'], obj['modality'], obj['lifecycle'], obj['valid'], obj['link']
        )

    @classmethod
    def from_dataclass(cls, obj) -> 'DatasetExtraInfo':
        return cls(obj.repo, obj.name, obj.modality, obj.lifecycle, obj.valid, obj.link)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class HfInfo:
    repo: str
    name: str
    category: Literal['model', 'dataset']
    date_crawl: str
    downloads_last_month: int | None = field(default=None)
    likes: int | None = field(default=None)
    discussions: int | None = field(default=None)
    discussion_msg: int | None = field(default=None)
    link: str | None = field(default=None)
    error: str | None = field(default=None)
    modality: Modality | None = field(default=None)
    lifecycle: Lifecycle | None = field(default=None)
    valid: bool | None = field(default=None)

    def format(self) -> str:
        if self.error is not None:
            return json.dumps(
                {
                    'repo': self.repo,
                    'name': self.name,
                    'category': self.category,
                    'date_crawl': self.date_crawl,
                    'error': self.error,
                },
                ensure_ascii=False,
                indent=2,
            )
        else:
            obj = asdict(self)
            obj.pop('error')
            return json.dumps(obj, ensure_ascii=False, indent=2)

    def __repr__(self):
        return self.format()

    def to_dict(self, type: Literal['error', 'output']) -> dict:
        obj = asdict(self)
        if self.category == 'model':
            obj.pop('lifecycle')
        match type:
            case 'error':
                return {
                    'repo': self.repo,
                    'name': self.name,
                    'category': self.category,
                    'date_crawl': self.date_crawl,
                    'error': self.error,
                }
            case 'output':
                obj.pop('error')
        return obj

    def update_from_extra_info(self, conf: dict):
        self.modality = conf.get('modality', None)
        self.lifecycle = conf.get('lifecycle', None)
        self.valid = conf.get('valid', None)


@dataclass
class MsInfo:
    repo: str
    name: str
    category: Literal['model', 'dataset']
    date_crawl: str
    downloads: int | None = field(default=None)
    likes: int | None = field(default=None)
    link: str | None = field(default=None)
    error: str | None = field(default=None)
    modality: Modality | None = field(default=None)
    lifecycle: Lifecycle | None = field(default=None)
    valid: bool | None = field(default=None)

    def format(self) -> str:
        if self.error is not None:
            return json.dumps(
                {
                    'repo': self.repo,
                    'name': self.name,
                    'category': self.category,
                    'date_crawl': self.date_crawl,
                    'error': self.error,
                },
                ensure_ascii=False,
                indent=2,
            )
        else:
            obj = asdict(self)
            obj.pop('error')
            return json.dumps(obj, ensure_ascii=False, indent=2)

    def __repr__(self):
        return self.format()

    def to_dict(self, type: Literal['error', 'output']) -> dict:
        obj = asdict(self)
        if self.category == 'model':
            obj.pop('lifecycle')
        match type:
            case 'error':
                return {
                    'repo': self.repo,
                    'name': self.name,
                    'category': self.category,
                    'date_crawl': self.date_crawl,
                    'error': self.error,
                }
            case 'output':
                obj.pop('error')
        return obj

    def update_from_extra_info(self, conf: dict):
        self.modality = conf.get('modality', None)
        self.lifecycle = conf.get('lifecycle', None)
        self.valid = conf.get('valid', None)


@dataclass
class BAAIDataInfo:
    repo: str = field(init=False, default='BAAI')
    name: str = field()
    downloads: int | None = field()
    likes: int | None = field()
    date_crawl: str = field()
    link: str = field()
    profile: str = field()
    category: str = field(default='dataset')
    modality: Modality | None = field(default=None)
    lifecycle: Lifecycle | None = field(default=None)
    valid: bool | None = field(default=None)

    def to_dict(self):
        return asdict(self)

    def update_from_extra_info(self, conf: dict):
        self.modality = conf.get('modality', None)
        self.lifecycle = conf.get('lifecycle', None)
        self.valid = conf.get('valid', None)
