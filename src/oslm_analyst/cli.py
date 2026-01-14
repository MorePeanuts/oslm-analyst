import typer
from typer import Argument, Option
from typing import Annotated, Literal
from datetime import datetime


app = typer.Typer(name='OSLM-Analyst', help='Open-source large models data analyst.')


def today() -> str:
    return str(datetime.today().date())


@app.command(help='Crawling data such as download counts of data/models on specified platforms.')
def crawl(
    platform: Annotated[
        Literal['all', 'huggingface', 'modelscope', 'open-data-lab', 'baai'],
        Argument(help='Used to specify the platform from which data is to be crawled.'),
    ] = 'huggingface',
    max_retry: Annotated[int, Option(help='Maximum number of retries on network error')] = 5,
):
    print(platform)
    print(max_retry)


@app.command(
    help='Ranking calculation based on the open-source influence evaluation criteria of large '
    'model technology for data obtained through web crawling.'
)
def rank(
    date: Annotated[
        str,
        Argument(
            default_factory=today,
            help='Ranking based on data up to date specified by this argument (today by default).',
        ),
    ],
):
    print(date)


def main() -> None:
    print('Hello from oslm-analyst!')
    app()
