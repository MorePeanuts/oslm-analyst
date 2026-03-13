from oslm_analyst.crawlers.modelscope import MsCrawler
from oslm_analyst.crawlers.huggingface import HfCrawler
from dataclasses import dataclass, asdict
from httpx import delete
from oslm_analyst.crawlers.crawl_utils import format_identifier_from_dict
import tempfile
import jsonlines
from pathlib import Path
from typing import TypeAlias, Literal
from enum import Enum
from langchain import chat_models


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


class ModalityAIHelper:
    def __init__(self):
        self.hf_crawler = HfCrawler()
        self.ms_crawler = MsCrawler()
        self.model_info_path = Path(__file__).parents[3] / 'config/model_info.jsonl'
        self.dataset_info_path = Path(__file__).parents[3] / 'config/dataset_info.jsonl'

    def update_extra_info(self):
        if self.model_info_path.exists():
            with tempfile.NamedTemporaryFile(
                'w',
                dir=self.model_info_path.parent,
                suffix='.jsonl',
                delete=False,
                encoding='utf-8',
            ) as tf:
                with (
                    jsonlines.open(self.model_info_path, 'r') as reader,
                    jsonlines.Writer(tf) as writer,
                ):
                    for line in reader:
                        readme = ''
                        identifier = format_identifier_from_dict(line)
                        if 'huggingface' in line['link']:
                            readme = self.hf_crawler.fetch_readme_content(identifier, 'model')
                        elif 'modelscope' in line['link']:
                            readme = self.ms_crawler.fetch_readme_content(identifier, 'model')
                        modality = self.gen_modality(identifier, 'model', line['link'], readme)
                        if modality:
                            line['valid'] = True
                            line['modality'] = modality
                        else:
                            line['valid'] = False
                            line['modality'] = None
                        writer.write(line)
                Path(tf.name).replace(self.model_info_path)
        if self.dataset_info_path.exists():
            with tempfile.NamedTemporaryFile(
                'w',
                dir=self.dataset_info_path.parent,
                suffix='.jsonl',
                delete=False,
                encoding='utf-8',
            ) as tf:
                with (
                    jsonlines.open(self.dataset_info_path, 'r') as reader,
                    jsonlines.Writer(tf) as writer,
                ):
                    for line in reader:
                        readme = ''
                        identifier = format_identifier_from_dict(line)
                        if 'huggingface' in line['link']:
                            readme = self.hf_crawler.fetch_readme_content(identifier, 'dataset')
                        elif 'modelscope' in line['link']:
                            readme = self.ms_crawler.fetch_readme_content(identifier, 'dataset')
                        modality = self.gen_modality(identifier, 'dataset', line['link'], readme)
                        lifecycle = self.gen_lifecycle(identifier, 'dataset', line['link'], readme)
                        if modality and lifecycle:
                            line['valid'] = (True,)
                            line['modality'] = modality
                            line['lifecycle'] = lifecycle
                        else:
                            line['valid'] = False
                            line['modality'] = None
                            line['lifecycle'] = None
                        writer.write(line)
                Path(tf.name).replace(self.dataset_info_path)

    def update_raw_data(self, data_path: Path, category: str):
        if category == 'model':
            model_info: dict[str, ModelExtraInfo] = {}
            if self.model_info_path.exists():
                with jsonlines.open(self.model_info_path, 'r') as reader:
                    for line in reader:
                        model_info[format_identifier_from_dict(line)] = ModelExtraInfo.from_dict(
                            line
                        )
            data = []
            with jsonlines.open(data_path, 'r') as reader:
                for line in reader:
                    identifier = format_identifier_from_dict(line)
                    if line['modality'] is not None:
                        continue
                    if identifier in model_info:
                        line['valid'] = model_info[identifier].valid
                        line['modality'] = model_info[identifier].modality
                    else:
                        model_info[identifier] = ModelExtraInfo.from_dict(line)
                    data.append(line)
            with (
                jsonlines.open(self.model_info_path, 'w') as writer1,
                jsonlines.open(data_path, 'w') as writer2,
            ):
                for v in model_info.values():
                    writer1.write(v.to_dict())
                writer2.write_all(data)
        elif category == 'dataset':
            dataset_info: dict[str, DatasetExtraInfo] = {}
            if self.dataset_info_path.exists():
                with jsonlines.open(self.dataset_info_path, 'r') as reader:
                    for line in reader:
                        dataset_info[format_identifier_from_dict(line)] = (
                            DatasetExtraInfo.from_dict(line)
                        )
            data = []
            with jsonlines.open(data_path, 'r') as reader:
                for line in reader:
                    identifier = format_identifier_from_dict(line)
                    if line['modality'] and line['lifecycle']:
                        continue
                    if identifier in dataset_info:
                        line['valid'] = dataset_info[identifier].valid
                        line['modality'] = dataset_info[identifier].modality
                        line['lifecycle'] = dataset_info[identifier].lifecycle
                    else:
                        dataset_info[identifier] = DatasetExtraInfo.from_dict(line)
                    data.append(line)
            with (
                jsonlines.open(self.dataset_info_path, 'w') as writer1,
                jsonlines.open(data_path, 'w') as writer2,
            ):
                for v in dataset_info.values():
                    writer1.write(v.to_dict())
                writer2.write_all(data)

    def gen_modality(self, identifier, category, link, readme) -> Modality | None:
        if readme == '':
            return None
        return Modality.Language

    def gen_lifecycle(self, identifier, category, link, readme) -> Lifecycle | None:
        if category == 'model' or readme == '':
            return None
        return Lifecycle.Pretraining
