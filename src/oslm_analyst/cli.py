import typer
from typer import Argument, Option
from typing import Annotated, Literal
from datetime import datetime
from .utils import today, parse_commas_separated_params


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
        Literal['all', 'huggingface', 'modelscope', 'open-data-lab', 'baai'],
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
    output: Annotated[str, Option(help='Path to save the result.')] = '.',
    max_retry: Annotated[int, Option(help='Maximum number of retries on network error')] = 5,
):
    """
    Crawling data such as download counts of data/models on specified platforms.
    """
    organization = parse_commas_separated_params(organization) if organization else organization  # type: ignore
    skip = parse_commas_separated_params(skip) if skip else skip  # type: ignore
    print(target)
    print(platform)
    print(organization)
    print(skip)
    print(output)
    print(max_retry)
    # 1. target: orgs.yaml -> list
    # 2. target: recover from HTTP error -> list
    # 3. target: organization -> str
    # 4. target: repository -> str
    # 5. target: model id/dataset id -> str


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
