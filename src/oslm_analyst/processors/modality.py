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
        return cls(obj['repo'], obj['name'], obj['modality'], obj['lifecycle'], obj['valid'], obj['link'])

    @classmethod
    def from_dataclass(cls, obj) -> 'DatasetExtraInfo':
        return cls(obj.repo, obj.name, obj.modality, obj.lifecycle, obj.valid, obj.link)

    def to_dict(self) -> dict:
        return asdict(self)


class ModalityAIHelper:
    def __init__(self):
        self.conf_dir = Path(__file__).parents[3] / 'config'
        self.hf_config = {}
        self.ms_config = {}
        self.baai_config = {}
        with (
            jsonlines.open(self.conf_dir / 'hf_config.jsonl') as hf_reader,
            jsonlines.open(self.conf_dir / 'ms_config.jsonl') as ms_reader,
            jsonlines.open(self.conf_dir / 'baai_config.jsonl') as baai_reader,
        ):
            for line in hf_reader:
                identifier = format_identifier_from_dict(line)
                self.hf_config[identifier] = line
            for line in ms_reader:
                identifier = format_identifier_from_dict(line)
                self.ms_config[identifier] = line
            for line in baai_reader:
                identifier = format_identifier_from_dict(line)
                self.baai_config[identifier] = line

    def update_config(self, platform: Literal['huggingface', 'modelscope', 'baai-datahub']):
        kwargs = {
            'mode': 'w',
            'dir': Path(__file__).parents[3] / 'config',
            'suffix': '.jsonl',
            'delete': False,
            'encoding': 'utf-8',
        }
        match platform:
            case 'huggingface':
                config = self.hf_config
                other_config = [
                    self.ms_config,
                    self.baai_config,
                ]
                config_file = 'hf_config.jsonl'
                other_config_file = ['ms_config.jsonl', 'baai_config.jsonl']
            case 'modelscope':
                config = self.ms_config
                other_config = [
                    self.hf_config,
                    self.baai_config,
                ]
                config_file = 'ms_config.jsonl'
                other_config_file = ['hf_config.jsonl', 'baai_config.jsonl']
            case 'baai-datahub':
                config = self.baai_config
                other_config = [
                    self.hf_config,
                    self.ms_config,
                ]
                config_file = 'baai_config.jsonl'
                other_config_file = ['hf_config.jsonl', 'ms_config.jsonl']

        with (
            tempfile.NamedTemporaryFile(**kwargs) as tf,  # type: ignore
            tempfile.NamedTemporaryFile(**kwargs) as tf1,  # type: ignore
            tempfile.NamedTemporaryFile(**kwargs) as tf2,  # type: ignore
        ):
            with (
                jsonlines.Writer(tf) as tf_writer,
                jsonlines.Writer(tf1) as tf1_writer,
                jsonlines.Writer(tf2) as tf2_writer,
            ):
                to_write = [None, None]
                for k, v in config.items():
                    try:
                        if v['modality']:
                            if k in other_config[0]:
                                to_write[0] = other_config[k]
                                other_config[0].pop(k)
                                to_write[0]['modality'] = v['modality']
                            if k in other_config[1]:
                                to_write[1] = other_config[k]
                                other_config[1].pop(k)
                                to_write[1]['modality'] = v['modality']
                        else:
                            if k in other_config[0] and other_config[0][k]['modality']:
                                
                    except Exception:
                        pass
                    finally:
                        if to_write[0]:
                            tf_writer.write(to_write[0])
                        if to_write[1]:
                            tf1_writer.write(to_write[1])
                        if to_write[2]:
                            tf2_writer.write(to_write[2])

                for _, v in other_config[0]:
                    tf1_writer.write(v)
                for _, v in other_config[1]:
                    tf2_writer.write(v)

            Path(tf).replace(self.conf_dir / config_file)
            Path(tf1).replace(self.conf_dir / other_config_file[0])
            Path(tf2).replace(self.conf_dir / other_config_file[1])

    def update_config(self, conf_path: Path):
        if not conf_path.exists():
            return
        conf_dir = conf_path.parent
        with tempfile.NamedTemporaryFile(
            'w', dir=conf_dir, suffix='.jsonl', delete=False, encoding='utf-8'
        ) as tf:
            temp_path = Path(tf.name)
            with (
                jsonlines.open(conf_path, 'r') as reader,
                jsonlines.Writer(tf) as writer,
            ):
                for line in reader:
                    identifier = f'{line["repo"]}/{line["name"]}'
                    if 'category' not in line:
                        continue
                    elif line['category'] == 'model':
                        line['modality'] = line.get('modality') or self.gen_modality(
                            identifier, 'model', line.get('link'), line.get('readme')
                        )
                    elif line['category'] == 'dataset':
                        line['modality'] = line.get('modality') or self.gen_modality(
                            identifier, 'dataset', line.get('link'), line.get('readme')
                        )
                        line['lifecycle'] = line.get('lifecycle') or self.gen_lifecycle(
                            identifier, line.get('link'), line.get('readme')
                        )
                    writer.write(line)

            temp_path.replace(conf_path)

    def update_raw_data(self, data_path: Path, platform: str, category: str):
        pass

    def gen_modality(self, identifier, category, link, readme) -> Modality:
        return Modality.Language

    def gen_lifecycle(self, identifier, category, link, readme) -> Lifecycle | None:
        if category == 'model':
            return None
        return Lifecycle.Pretraining
