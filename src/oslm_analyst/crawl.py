import jsonlines
from pathlib import Path
from typing import NamedTuple, Literal
from dataclasses import asdict
from loguru import logger
from .crawlers.huggingface import HfCrawler


class Source(NamedTuple):
    platform: str
    org: str
    repo: str
    name: str | None
    category: str


def run_hf_crawl_pipeline(
    inp: list[Source],
    outp: Path,
    max_retry: int,
    token: str | None,
    endpoint: str | None,
):
    """
    run
    """
    # TODO:not complete
    if len(inp) == 0:
        return
    kwargs = {'max_retry': max_retry}
    kwargs['token'] = token
    if endpoint:
        kwargs['endpoint'] = endpoint
    crawler = HfCrawler(**kwargs)

    out_path = outp / f'raw_{inp[0].category}_data.jsonl'
    err_path = outp / f'err_{inp[0].category}_data.jsonl'
    logger.info(f'Huggingface pipeline output path: {out_path}')
    logger.info(f'Huggingface pipeline error path: {err_path}')

    with (
        jsonlines.open(out_path, 'a', flush=True) as out_writer,
        jsonlines.open(err_path, 'w', flush=True) as err_writer,
    ):
        for src in inp:
            for info in crawler.fetch(src.repo, src.name, src.category):
                logger.info(f'fetch: {info}')
                if info.error is not None:
                    err_writer.write(asdict(info))
                else:
                    out_writer.write(asdict(info))
