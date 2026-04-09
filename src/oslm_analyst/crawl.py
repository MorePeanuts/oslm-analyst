from oslm_analyst.processors.modality import ModelExtraInfo, DatasetExtraInfo
from oslm_analyst.crawlers.crawl_utils import format_identifier_from_dict, format_identifier
from oslm_analyst.crawlers.baai_data import BAAIDataCrawler
from oslm_analyst.crawlers.modelscope import MsCrawler, MsInfo
import jsonlines
from pathlib import Path
from typing import NamedTuple, Literal
from dataclasses import asdict
from loguru import logger
from tqdm import tqdm
from .crawlers.huggingface import HfCrawler, HfInfo
from .utils import Source


def run_hf_crawl_pipeline(
    inp_src: list[Source],
    out_path: Path,
    max_retry: int,
    token: str | None,
    endpoint: str | None,
):
    """
    run
    """
    if len(inp_src) == 0:
        return
    kwargs = {'max_retry': max_retry}
    kwargs['token'] = token
    if endpoint:
        kwargs['endpoint'] = endpoint
    crawler = HfCrawler(**kwargs)  # type: ignore

    outp_path = out_path / f'raw_{inp_src[0].category}_data.jsonl'
    err_path = out_path / f'err_{inp_src[0].category}_data.jsonl'
    logger.info(f'Huggingface pipeline output path: {outp_path}')
    logger.info(f'Huggingface pipeline error path: {err_path}')

    model_info_path = Path(__file__).parents[2] / 'config/model_info.jsonl'
    dataset_info_path = Path(__file__).parents[2] / 'config/dataset_info.jsonl'
    logger.info(f'Loading model extra information from {model_info_path}')
    logger.info(f'Loading dataset extra information from {dataset_info_path}')
    model_info: dict[str, ModelExtraInfo] = {}
    dataset_info: dict[str, DatasetExtraInfo] = {}
    if model_info_path.exists():
        with jsonlines.open(model_info_path, 'r') as reader:
            for line in reader:
                model_info[format_identifier_from_dict(line)] = ModelExtraInfo.from_dict(line)
            logger.info(f'Total ModelExtraInfo: {len(model_info)}')
    if dataset_info_path.exists():
        with jsonlines.open(dataset_info_path, 'r') as reader:
            for line in reader:
                dataset_info[format_identifier_from_dict(line)] = DatasetExtraInfo.from_dict(line)
            logger.info(f'Total DatasetExtraInfo: {len(dataset_info)}')

    total_errors = 0
    logger.info('Calculate total records...')
    total = 0
    for src in inp_src:
        if src.name is not None:
            total += 1
        else:
            total += crawler.fetch_num_of(src.repo, src.category + 's')  # type: ignore
    pbar = tqdm(total=total)

    with (
        jsonlines.open(outp_path, 'a', flush=True) as out_writer,
        jsonlines.open(err_path, 'w', flush=True) as err_writer,
    ):
        for src in inp_src:
            pbar.set_description(f'crawling {src.category} from {src.repo}')
            for info in crawler.fetch(src.repo, src.name, src.category):  # type: ignore
                pbar.update(1)
                logger.trace(f'fetch: {info}')
                if info.error is not None:
                    total_errors += 1
                    pbar.write(f'Error when fetch {info}')
                    err_writer.write(info.to_dict('error'))
                else:
                    identifier = format_identifier(info.repo, info.name)
                    if src.category == 'model':
                        if identifier not in model_info:
                            model_info[identifier] = ModelExtraInfo.from_dataclass(info)
                        else:
                            info.update_from_extra_info(model_info[identifier].to_dict())
                    elif src.category == 'dataset':
                        if identifier not in dataset_info:
                            dataset_info[identifier] = DatasetExtraInfo.from_dataclass(info)
                        else:
                            info.update_from_extra_info(dataset_info[identifier].to_dict())
                    out_writer.write(info.to_dict('output'))

    with (
        jsonlines.open(model_info_path, 'w', flush=True) as model_writer,
        jsonlines.open(dataset_info_path, 'w', flush=True) as dataset_writer,
    ):
        for _, v in model_info.items():
            model_writer.write(v.to_dict())
        for _, v in dataset_info.items():
            dataset_writer.write(v.to_dict())

    pbar.close()
    if total_errors == 0:
        err_path.unlink()


def run_ms_crawl_pipeline(
    inp_src: list[Source],
    out_path: Path,
    max_retry: int,
    endpoint: str | None,
):
    """
    run
    """
    kwargs = {'max_retry': max_retry}
    if endpoint:
        kwargs['endpoint'] = endpoint
    crawler = MsCrawler(**kwargs)

    outp_path = out_path / f'raw_{inp_src[0].category}_data.jsonl'
    err_path = out_path / f'err_{inp_src[0].category}_data.jsonl'
    logger.info(f'Modelscope pipeline output path: {outp_path}')
    logger.info(f'Modelscope pipeline error path: {err_path}')

    model_info_path = Path(__file__).parents[2] / 'config/model_info.jsonl'
    dataset_info_path = Path(__file__).parents[2] / 'config/dataset_info.jsonl'
    logger.info(f'Loading model extra information from {model_info_path}')
    logger.info(f'Loading dataset extra information from {dataset_info_path}')
    model_info: dict[str, ModelExtraInfo] = {}
    dataset_info: dict[str, DatasetExtraInfo] = {}
    if model_info_path.exists():
        with jsonlines.open(model_info_path, 'r') as reader:
            for line in reader:
                model_info[format_identifier_from_dict(line)] = ModelExtraInfo.from_dict(line)
            logger.info(f'Total ModelExtraInfo: {len(model_info)}')
    if dataset_info_path.exists():
        with jsonlines.open(dataset_info_path, 'r') as reader:
            for line in reader:
                dataset_info[format_identifier_from_dict(line)] = DatasetExtraInfo.from_dict(line)
            logger.info(f'Total DatasetExtraInfo: {len(dataset_info)}')

    total_errors = 0
    logger.info('Calculate total records...')
    total = 0
    for src in inp_src:
        if src.name is not None:
            total += 1
        else:
            total += crawler.fetch_num_of(src.repo, src.category + 's')  # type: ignore
    pbar = tqdm(total=total)

    with (
        jsonlines.open(outp_path, 'a', flush=True) as out_writer,
        jsonlines.open(err_path, 'w', flush=True) as err_writer,
    ):
        for src in inp_src:
            pbar.set_description(f'crawling {src.category} data from {src.repo}')
            for info in crawler.fetch(src.repo, src.name, src.category):  # type: ignore
                pbar.update(1)
                logger.trace(f'fetch: {info}')
                if info.error is not None:
                    total_errors += 1
                    pbar.write(f'Error when fetch {info}')
                    err_writer.write(info.to_dict('error'))
                else:
                    identifier = format_identifier(info.repo, info.name)
                    if src.category == 'model':
                        if identifier not in model_info:
                            model_info[identifier] = ModelExtraInfo.from_dataclass(info)
                        else:
                            info.update_from_extra_info(model_info[identifier].to_dict())
                    elif src.category == 'dataset':
                        if identifier not in dataset_info:
                            dataset_info[identifier] = DatasetExtraInfo.from_dataclass(info)
                        else:
                            info.update_from_extra_info(dataset_info[identifier].to_dict())
                    out_writer.write(info.to_dict('output'))

    with (
        jsonlines.open(model_info_path, 'w', flush=True) as model_writer,
        jsonlines.open(dataset_info_path, 'w', flush=True) as dataset_writer,
    ):
        for _, v in model_info.items():
            model_writer.write(v.to_dict())
        for _, v in dataset_info.items():
            dataset_writer.write(v.to_dict())

    pbar.close()
    if total_errors == 0:
        err_path.unlink()


def run_baai_data_pipeline(out_path: Path):
    crawler = BAAIDataCrawler()
    outp_path = out_path / 'raw_dataset_data.jsonl'
    logger.info(f'BAAIData pipeline output path: {outp_path}')

    dataset_info_path = Path(__file__).parents[2] / 'config/dataset_info.jsonl'
    logger.info(f'Loading dataset extra information from {dataset_info_path}')
    dataset_info: dict[str, DatasetExtraInfo] = {}
    if dataset_info_path.exists():
        with jsonlines.open(dataset_info_path, 'r') as reader:
            for line in reader:
                dataset_info[format_identifier_from_dict(line)] = DatasetExtraInfo.from_dict(line)

    with (
        jsonlines.open(outp_path, 'a', flush=True) as out_writer,
    ):
        results = crawler.scrape()
        assert isinstance(results, list)
        for info in results:
            identifier = format_identifier(info.repo, info.name)
            if identifier not in dataset_info:
                dataset_info[identifier] = DatasetExtraInfo.from_dataclass(info)
            else:
                info.update_from_extra_info(dataset_info[identifier].to_dict())
            out_writer.write(info.to_dict())

    with jsonlines.open(dataset_info_path, 'w', flush=True) as writer:
        for _, v in dataset_info.items():
            writer.write(v.to_dict())
