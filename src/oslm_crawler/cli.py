import typer
from typer import Argument, Option
from typing import Annotated, Literal
from datetime import datetime

app = typer.Typer()


def today() -> str:
    return str(datetime.today().date())


@app.command()
def crawl(
    platform: Annotated[
        Literal['all', 'huggingface', 'modelscope', 'open-data-lab', 'baai'],
        Argument(help='Used to specify the platform from which data is to be crawled.'),
    ] = 'all',
    max_retry: Annotated[int, Option(help='Maximum number of retries on network error')] = 5,
):
    print(platform)
    print(max_retry)


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
    print(date)


def main() -> None:
    print('Hello from oslm-crawler!')
    app()
