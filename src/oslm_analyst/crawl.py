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
    # TODO: temp solution for persistent information
    conf_path = Path(__file__).parents[2] / 'config/hf_config.jsonl'
    conf = {}
    if conf_path.exists():
        with jsonlines.open(conf_path, 'r') as conf_reader:
            for line in conf_reader:
                conf[f'{line["repo"]}/{line["name"]}'] = line
    logger.info(f'Huggingface pipeline output path: {outp_path}')
    logger.info(f'Huggingface pipeline error path: {err_path}')

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
        jsonlines.open(conf_path, 'w', flush=True) as conf_writer,
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
                    identifier = f'{info.repo}/{info.name}'
                    if identifier in conf:
                        conf[identifier]['readme'] = info.readme
                    else:
                        conf[identifier] = info.to_dict('config')
                    info.update_from_config(conf[identifier])
                    out_writer.write(info.to_dict('output'))
        conf_writer.write_all(list(conf.values()))

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
    # TODO: temp solution for persistent information
    conf_path = Path(__file__).parents[2] / 'config/ms_config.jsonl'
    conf = {}
    if conf_path.exists():
        with jsonlines.open(conf_path, 'r') as conf_reader:
            for line in conf_reader:
                conf[f'{line["repo"]}/{line["name"]}'] = line
    logger.info(f'Modelscope pipeline output path: {outp_path}')
    logger.info(f'Modelscope pipeline error path: {err_path}')

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
        jsonlines.open(conf_path, 'w', flush=True) as conf_writer,
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
                    identifier = f'{info.repo}/{info.name}'
                    if identifier in conf:
                        conf[identifier]['readme'] = info.readme
                    else:
                        conf[identifier] = info.to_dict('config')
                    info.update_from_config(conf[identifier])
                    out_writer.write(info.to_dict('output'))
        conf_writer.write_all(list(conf.values()))

    pbar.close()
    if total_errors == 0:
        err_path.unlink()
