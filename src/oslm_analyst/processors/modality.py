import traceback
import tempfile
import os
from pathlib import Path
from typing import Literal, TypedDict

import jsonlines
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from loguru import logger

from oslm_analyst.crawlers.crawl_utils import format_identifier_from_dict
from oslm_analyst.crawlers.huggingface import HfCrawler
from oslm_analyst.crawlers.modelscope import MsCrawler
from oslm_analyst.data_utils import DatasetExtraInfo, Lifecycle, Modality, ModelExtraInfo

# Load environment variables from .env file
load_dotenv()


class ModelClassification(TypedDict):
    valid: bool
    modality: str | None
    reason: str


class DatasetClassification(TypedDict):
    valid: bool
    modality: str | None
    lifecycle: str | None
    reason: str


class ModalityAIHelper:
    def __init__(self, api_key=None, base_url=None, model=None):
        self.hf_crawler = HfCrawler()
        self.ms_crawler = MsCrawler()
        self.model_info_path = Path(__file__).parents[3] / 'config/model_info.jsonl'
        self.dataset_info_path = Path(__file__).parents[3] / 'config/dataset_info.jsonl'

        # Initialize LLM - use provided params first, then env vars
        api_key = api_key or os.getenv('OPENAI_API_KEY', None)
        base_url = base_url or os.getenv('OPENAI_API_BASE', None)
        model = model or os.getenv('OPENAI_MODEL_NAME', 'gpt-5')

        if not api_key:
            logger.warning(
                'OPENAI_API_KEY not found in environment variables. AI classification will be skipped.'
            )
            self.llm = None
        else:
            self.llm = ChatOpenAI(model=model, api_key=api_key, base_url=base_url, temperature=0)  # type: ignore

        # Build chains
        self._build_chains()

    def _build_chains(self):
        """Build LangChain chains for classification."""
        if self.llm is None:
            self.model_chain = None
            self.dataset_chain = None
            return

        # Model classification prompt (validity + modality)
        model_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    'system',
                    'You are an AI assistant that evaluates AI model repositories. '
                    'Available modalities: {modality_options}\n\n'
                    'Validity criteria:\n'
                    '- A valid model is a real AI model (large language model, vision model, speech model, etc.)\n'
                    '- Invalid repositories include: test repos, demos, empty repos, script-only repos, non-AI projects, '
                    "personal notes, or repositories that don't contain actual model weights/code.\n\n"
                    'Respond in JSON format with:\n'
                    '- "valid": boolean (true if this is a valid AI model repository)\n'
                    '- "modality": one of the available modalities (or null if invalid)\n'
                    '- "reason": brief string explaining your decision',
                ),
                (
                    'human',
                    'Identifier: {identifier}\n'
                    'Link: {link}\n'
                    'README content (truncated):\n{readme}\n\n'
                    'Evaluate this model repository.',
                ),
            ]
        )

        # Dataset classification prompt (validity + modality + lifecycle)
        dataset_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    'system',
                    'You are an AI assistant that evaluates AI dataset repositories. '
                    'Available modalities: {modality_options}\n'
                    'Available lifecycle stages: {lifecycle_options}\n\n'
                    'Lifecycle definitions:\n'
                    '- Pre-training: Datasets used for initial model pre-training (large-scale, general data)\n'
                    '- Fine-tuning: Datasets used for task-specific fine-tuning\n'
                    '- Preference: Datasets used for preference learning, RLHF, or alignment\n'
                    '- Evaluation: Datasets used for benchmarking or evaluation\n\n'
                    'Validity criteria:\n'
                    '- A valid dataset is a real dataset used for AI training/evaluation\n'
                    '- Invalid repositories include: test repos, demos, empty repos, script-only repos, non-AI projects, '
                    "personal notes, or repositories that don't contain actual dataset data/documentation.\n\n"
                    'Respond in JSON format with:\n'
                    '- "valid": boolean (true if this is a valid AI dataset repository)\n'
                    '- "modality": one of the available modalities (or null if invalid)\n'
                    '- "lifecycle": one of the available lifecycle stages (or null if invalid)\n'
                    '- "reason": brief string explaining your decision',
                ),
                (
                    'human',
                    'Identifier: {identifier}\n'
                    'Link: {link}\n'
                    'README content (truncated):\n{readme}\n\n'
                    'Evaluate this dataset repository.',
                ),
            ]
        )

        model_parser = JsonOutputParser()
        dataset_parser = JsonOutputParser()

        self.model_chain = model_prompt | self.llm | model_parser
        self.dataset_chain = dataset_prompt | self.llm | dataset_parser

    def _truncate_readme(self, readme: str, max_chars: int = 8000) -> str:
        """Truncate README content to avoid token limit issues."""
        if len(readme) <= max_chars:
            return readme
        # Keep first half and last half to maintain context
        half = max_chars // 2
        return readme[:half] + '\n\n[... truncated ...]\n\n' + readme[-half:]

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
                        identifier = format_identifier_from_dict(line)
                        # Skip if:
                        # 1. valid is False (already marked invalid), OR
                        # 2. valid is True AND modality is not None (already classified)
                        valid_val = line.get('valid')
                        has_modality = line.get('modality') is not None
                        if valid_val is False or (valid_val is True and has_modality):
                            logger.trace(f'Skipping model {identifier} - already classified or invalid')
                            writer.write(line)
                            continue
                        # Need to classify
                        readme = ''
                        if 'huggingface' in line['link']:
                            readme = self.hf_crawler.fetch_readme_content(identifier, 'model')
                        elif 'modelscope' in line['link']:
                            readme = self.ms_crawler.fetch_readme_content(identifier, 'model')
                        classification = self.classify_model(identifier, line['link'], readme)
                        line['valid'] = classification['valid']
                        line['modality'] = classification['modality']
                        logger.info(
                            f'Model {identifier}: valid={line["valid"]}, modality={line["modality"]} ({classification.get("reason", "")})'
                        )
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
                        identifier = format_identifier_from_dict(line)
                        # Skip if:
                        # 1. valid is False (already marked invalid), OR
                        # 2. valid is True AND modality is not None AND lifecycle is not None (already classified)
                        valid_val = line.get('valid')
                        has_modality = line.get('modality') is not None
                        has_lifecycle = line.get('lifecycle') is not None
                        if valid_val is False or (valid_val is True and has_modality and has_lifecycle):
                            logger.trace(f'Skipping dataset {identifier} - already classified or invalid')
                            writer.write(line)
                            continue
                        # Need to classify
                        readme = ''
                        if 'huggingface' in line['link']:
                            readme = self.hf_crawler.fetch_readme_content(identifier, 'dataset')
                        elif 'modelscope' in line['link']:
                            readme = self.ms_crawler.fetch_readme_content(identifier, 'dataset')
                        classification = self.classify_dataset(identifier, line['link'], readme)
                        line['valid'] = classification['valid']
                        line['modality'] = classification['modality']
                        line['lifecycle'] = classification['lifecycle']
                        logger.info(
                            f'Dataset {identifier}: valid={line["valid"]}, modality={line["modality"]}, lifecycle={line["lifecycle"]} ({classification.get("reason", "")})'
                        )
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
                    if line['modality'] is None:
                        if identifier in model_info:
                            line['valid'] = model_info[identifier].valid
                            line['modality'] = model_info[identifier].modality
                        else:
                            model_info[identifier] = ModelExtraInfo.from_dict(line)
                    data.append(line)
            # Write model_info to temp file first
            with tempfile.NamedTemporaryFile(
                'w',
                dir=self.model_info_path.parent,
                suffix='.jsonl',
                delete=False,
                encoding='utf-8',
            ) as tf1:
                with jsonlines.Writer(tf1) as writer1:
                    for v in model_info.values():
                        writer1.write(v.to_dict())
            # Write data to temp file first
            with tempfile.NamedTemporaryFile(
                'w',
                dir=data_path.parent,
                suffix='.jsonl',
                delete=False,
                encoding='utf-8',
            ) as tf2:
                with jsonlines.Writer(tf2) as writer2:
                    writer2.write_all(data)
            # Atomic replace
            Path(tf1.name).replace(self.model_info_path)
            Path(tf2.name).replace(data_path)
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
                    if not (line['modality'] and line['lifecycle']):
                        if identifier in dataset_info:
                            line['valid'] = dataset_info[identifier].valid
                            line['modality'] = dataset_info[identifier].modality
                            line['lifecycle'] = dataset_info[identifier].lifecycle
                        else:
                            dataset_info[identifier] = DatasetExtraInfo.from_dict(line)
                    data.append(line)
            # Write dataset_info to temp file first
            with tempfile.NamedTemporaryFile(
                'w',
                dir=self.dataset_info_path.parent,
                suffix='.jsonl',
                delete=False,
                encoding='utf-8',
            ) as tf1:
                with jsonlines.Writer(tf1) as writer1:
                    for v in dataset_info.values():
                        writer1.write(v.to_dict())
            # Write data to temp file first
            with tempfile.NamedTemporaryFile(
                'w',
                dir=data_path.parent,
                suffix='.jsonl',
                delete=False,
                encoding='utf-8',
            ) as tf2:
                with jsonlines.Writer(tf2) as writer2:
                    writer2.write_all(data)
            # Atomic replace
            Path(tf1.name).replace(self.dataset_info_path)
            Path(tf2.name).replace(data_path)

    def classify_model(self, identifier: str, link: str, readme: str) -> ModelClassification:
        """Classify a model repository: validity + modality."""
        if readme == '':
            return {
                'valid': False,
                'modality': None,
                'reason': 'No README content',
            }

        # If LLM is not available, fall back to default
        if self.model_chain is None:
            logger.warning(f'LLM not available, using default classification for {identifier}')
            return {
                'valid': False,
                'modality': None,
                'reason': 'Default (LLM not available)',
            }

        try:
            modality_options = [m.value for m in Modality]
            result = self.model_chain.invoke(
                {
                    'modality_options': ', '.join(modality_options),
                    'identifier': identifier,
                    'link': link,
                    'readme': self._truncate_readme(readme),
                }
            )

            # Validate and normalize result
            valid = bool(result.get('valid', False))
            modality_str = result.get('modality')
            reason = result.get('reason', '')

            if modality_str not in modality_options:
                modality_str = None

            return {
                'valid': valid,
                'modality': modality_str,
                'reason': reason,
            }
        except Exception:
            error_msg = traceback.format_exc()
            logger.error(f'Failed to classify model {identifier}: {error_msg}')
            return {
                'valid': False,
                'modality': None,
                'reason': f'Fallback (error: {error_msg})',
            }

    def classify_dataset(self, identifier: str, link: str, readme: str) -> DatasetClassification:
        """Classify a dataset repository: validity + modality + lifecycle."""
        if readme == '':
            return {
                'valid': False,
                'modality': None,
                'lifecycle': None,
                'reason': 'No README content',
            }

        # If LLM is not available, fall back to default
        if self.dataset_chain is None:
            logger.warning(f'LLM not available, using default classification for {identifier}')
            return {
                'valid': False,
                'modality': None,
                'lifecycle': None,
                'reason': 'Default (LLM not available)',
            }

        try:
            modality_options = [m.value for m in Modality]
            lifecycle_options = [l.value for l in Lifecycle]
            result = self.dataset_chain.invoke(
                {
                    'modality_options': ', '.join(modality_options),
                    'lifecycle_options': ', '.join(lifecycle_options),
                    'identifier': identifier,
                    'link': link,
                    'readme': self._truncate_readme(readme),
                }
            )

            # Validate and normalize result
            valid = bool(result.get('valid', False))
            modality_str = result.get('modality')
            lifecycle_str = result.get('lifecycle')
            reason = result.get('reason', '')

            if modality_str not in modality_options:
                modality_str = None
            if lifecycle_str not in lifecycle_options:
                lifecycle_str = None

            return {
                'valid': valid,
                'modality': modality_str,
                'lifecycle': lifecycle_str,
                'reason': reason,
            }
        except Exception:
            error_msg = traceback.format_exc()
            logger.error(f'Failed to classify dataset {identifier}: {error_msg}')
            return {
                'valid': False,
                'modality': None,
                'lifecycle': None,
                'reason': f'Fallback (error: {error_msg})',
            }

    def gen_modality(self, identifier, category, link, readme) -> Modality | None:
        """Deprecated: use classify_model or classify_dataset instead."""
        if category == 'model':
            result = self.classify_model(identifier, link, readme)
            return Modality(result['modality']) if result['modality'] else None
        else:
            result = self.classify_dataset(identifier, link, readme)
            return Modality(result['modality']) if result['modality'] else None

    def gen_lifecycle(self, identifier, category, link, readme) -> Lifecycle | None:
        """Deprecated: use classify_dataset instead."""
        if category == 'model':
            return None
        result = self.classify_dataset(identifier, link, readme)
        return Lifecycle(result['lifecycle']) if result['lifecycle'] else None
