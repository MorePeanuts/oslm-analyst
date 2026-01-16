import jsonlines
from pathlib import Path
from typing import NamedTuple, Literal
from dataclasses import asdict
from loguru import logger
from .crawlers.huggingface import HfCrawler
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
    # TODO:not complete
    if len(inp_src) == 0:
        return
    kwargs = {'max_retry': max_retry}
    kwargs['token'] = token
    if endpoint:
        kwargs['endpoint'] = endpoint
    crawler = HfCrawler(**kwargs)  # type: ignore

    out_path = out_path / f'raw_{inp_src[0].category}_data.jsonl'
    err_path = out_path / f'err_{inp_src[0].category}_data.jsonl'
    logger.info(f'Huggingface pipeline output path: {out_path}')
    logger.info(f'Huggingface pipeline error path: {err_path}')

    total_errors = 0

    with (
        jsonlines.open(out_path, 'a', flush=True) as out_writer,
        jsonlines.open(err_path, 'w', flush=True) as err_writer,
    ):
        for src in inp_src:
            for info in crawler.fetch(src.repo, src.name, src.category):  # type: ignore
                logger.info(f'fetch: {info}')
                if info.error is not None:
                    total_errors += 1
                    err_writer.write(asdict(info))
                else:
                    out_writer.write(asdict(info))

    if total_errors == 0:
        err_path.unlink()
