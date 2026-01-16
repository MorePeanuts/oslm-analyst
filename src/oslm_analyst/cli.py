import typer
from typer import Argument, Option
from typing import Annotated, Literal
from datetime import datetime
from loguru import logger
from pathlib import Path
from .utils import today, parse_commas_separated_params, OrgInfo
from .crawl import run_hf_crawl_pipeline, Source


app = typer.Typer(name='OSLM-Analyst', help='Open-source large models data analyst.')


@app.command()
def start():
    """
    Launch the interactive application.
    """
    raise NotImplementedError()


@app.command()
def analyze(
    question: Annotated[str, Argument(help='The question you want to ask.')],
):
    """
    Start the data analysis agent to answer a specified question.
    """
    print('User:', question)


@app.command()
def crawl(
    target: Annotated[
        str,
        Argument(
            help='Used to specify path/repository/id. '
            'The path must point to the orgs.yaml configuration file, or the last output path '
            '(the program will automatically search for the last error record file).'
            'repository refers to the account name on the platform.'
            'The ID must start with `dataset:` or `model:`, followed by `{repo}/{name}`.'
        ),
    ],
    platform: Annotated[
        Literal['huggingface', 'modelscope', 'open-datalab', 'baai-datahub'],
        Argument(help='Used to specify the platform from which data is to be crawled.'),
    ] = 'huggingface',
    organization: Annotated[
        str | None,
        Option(
            help='The default value is None, in which case all targets will be crawled; '
            'otherwise, only the parts of the target that belong to the `organization` will be crawled. '
            '(separate by commas)'
        ),
    ] = None,
    skip: Annotated[
        str | None,
        Option(
            help='Skip the target in `skip` (separate by commas). '
            'If the element is an id, it must start with `dataset:` or `model:` followed by the id; '
            'If the element is an organization, it must start with `org:`; '
            'If the element is a repository, no additional characters are needed.'
        ),
    ] = None,
    category: Annotated[
        Literal['model', 'dataset'],
        Option(help='Specify whether to crawl the dataset data or the model data.'),
    ] = 'model',
    output: Annotated[
        str,
        Option(
            help='Path to save the result. If the target is to rerun the failed tasks, then '
            'this parameter is invalid, and the output results will be directly appended to the original task output. '
            'Based on the platform and the current date, a new directory `{platform}_{datetime}` will be automatically created in the output.'
        ),
    ] = '.',
    max_retry: Annotated[int, Option(help='Maximum number of retries on network error')] = 5,
    token: Annotated[
        str | None,
        Option(help='The access token required for using the API to retrieve platform data.'),
    ] = None,
    endpoint: Annotated[str | None, Option(help='Endpoint of the platform.')] = None,
):
    """
    Crawling data such as download counts of data/models on specified platforms.
    """
    if organization:
        required_org = parse_commas_separated_params(organization)
    if skip:
        skip_list = parse_commas_separated_params(skip)
        skip_id, skip_org, skip_repo = [], [], []
        for s in skip_list:
            if s.startswith(('dataset:', 'model:')):
                skip_id.append(s.split(':')[-1])
            elif s.startswith('org:'):
                skip_org.append(s.split(':')[-1])
            else:
                skip_repo.append(s)

    inp: list[Source] = []
    if Path(target).exists():
        target_path = Path(target)
        if target_path.is_dir():
            # TODO: target: recover from HTTP error
            raise NotImplementedError()
        else:
            # target: orgs.yaml -> list
            org_infos = OrgInfo.from_orgs_yaml(target_path)
            for org_info in org_infos:
                if platform == 'huggingface':
                    for repo in org_info.hf_accounts:
                        inp.append(
                            Source(
                                platform=platform,
                                org=org_info.org,
                                repo=repo,
                                name=None,
                                category=category,
                            )
                        )
                elif platform == 'modelscope':
                    raise NotImplementedError()
                else:
                    raise ValueError()
    elif target.startswith(('dataset:', 'model:')):
        # TODO: target: model id or dataset id -> str
        raise NotImplementedError()
    else:
        # TODO: target: repository
        raise NotImplementedError()

    # Filter
    filtered_inp: list[Source] = []
    for src in inp:
        if src.org not in required_org:
            continue
        if src.org in skip_org:
            continue
        if src.repo in skip_repo:
            continue
        if f'{src.repo}/{src.name}' in skip_id:
            continue
        filtered_inp.append(src)

    logger.info(f'Input source:\n{filtered_inp}')

    outp = Path(output) / f'{platform}_{today()}'
    outp.mkdir(parents=True, exist_ok=True)
    logger.info(f'Output path:\n{outp}')

    match platform:
        case 'huggingface':
            run_hf_crawl_pipeline(
                inp=filtered_inp,
                outp=outp,
                max_retry=max_retry,
                token=token,
                endpoint=endpoint,
            )
        case 'modelscope':
            raise NotImplementedError()
        case 'open-datalab':
            raise NotImplementedError()
        case 'baai-datahub':
            raise NotImplementedError()


@app.command(help='Process the raw data obtained from the crawler.')
def process():
    raise NotImplementedError()


@app.command()
def rank(
    date: Annotated[
        str,
        Argument(
            default_factory=today,
            help='Ranking based on data up to date specified by this argument (today by default).',
        ),
    ],
):
    """
    Ranking calculation based on the open-source influence evaluation criteria of large
    model technology for data obtained through web crawling.
    """
    print(date)


def main() -> None:
    print('Hello from oslm-analyst!')
    app()
