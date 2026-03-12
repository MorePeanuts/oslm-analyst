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


class ModalityAIHelper:
    def __init__(self):
        root_path = Path(__file__).parents[3] / 'config'
        for config_name in ['hf_config.jsonl', 'ms_config.jsonl', 'baai_config.jsonl']:
            with jsonlines.open(root_path / config_name, 'r') as reader:
                for line in reader:
                    pass
        self.hf_config = {}
        self.ms_config = {}
        self.baai_config = {}

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

    def gen_lifecycle(self, identifier, link, readme) -> Lifecycle:
        return Lifecycle.Pretraining
