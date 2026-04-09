from datetime import datetime, timedelta
import json
import re
import subprocess
import sys
from oslm_analyst.processors.modality import ModalityAIHelper
from oslm_analyst.processors.osir_lmts import OsirLmtsProcessor
from oslm_analyst.processors.osir_lmts_rank import get_rank_strategy_for_month
from oslm_analyst.database.osir_lmts import OsirLmtsDatabase
import typer
from typer import Argument, Option
from typing import Annotated, Literal
from loguru import logger
from pathlib import Path
from pprint import pformat
from .utils import today, parse_commas_separated_params, OrgInfo, Source
from .crawl import (
    run_hf_crawl_pipeline,
    run_ms_crawl_pipeline,
    run_baai_data_pipeline,
)


app = typer.Typer(name='OSLM-Analyst', help='Open-source large models data analyst.')
process_app = typer.Typer(name='Raw-data Processor', help='Post-process the raw data.')
app.add_typer(process_app, name='process')
db_app = typer.Typer(name='Database', help='OSIR-LMTS database management.')
app.add_typer(db_app, name='db')


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
    platform: Annotated[
        Literal['huggingface', 'modelscope', 'baai-datahub'],
        Argument(help='Used to specify the platform from which data is to be crawled.'),
    ] = 'huggingface',
    target: Annotated[
        str,
        Argument(
            help='Used to specify path/repository/id. '
            'The path must point to the orgs.yaml configuration file, or the last output path '
            '(the program will automatically search for the last error record file).'
            'repository refers to the account name on the platform.'
            'The ID must start with `id:`, followed by `{repo}/{name}`.'
        ),
    ] = './config/orgs.yaml',
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
            'If the element is an id, it must start with `id:` followed by the id; '
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
    ] = './output',
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
    outp = Path(output) / f'{platform}_{today()}'

    # Process the input source.
    if platform == 'huggingface' or platform == 'modelscope':
        required_org = None
        skip_id, skip_org, skip_repo = [], [], []
        if organization:
            required_org = parse_commas_separated_params(organization)
        if skip:
            skip_list = parse_commas_separated_params(skip)
            for s in skip_list:
                if s.startswith('id:'):
                    skip_id.append(s.split(':')[-1])
                elif s.startswith('org:'):
                    skip_org.append(s.split(':')[-1])
                else:
                    skip_repo.append(s)

        inp_src: list[Source] = []
        if Path(target).exists():
            target_path = Path(target)
            if target_path.is_dir():
                # target: recover from HTTP error
                org_infos = OrgInfo.build_org_info_list_from_yaml(
                    Path(__file__).parents[2] / 'config/orgs.yaml'
                )
                repo_org_map = OrgInfo.build_repo_org_map(org_infos, platform)
                inp_src.extend(
                    Source.build_source_list_from_error(
                        target_path, platform, category, repo_org_map
                    )
                )
                outp = Path(target)
            else:
                # target: orgs.yaml -> list
                org_infos = OrgInfo.build_org_info_list_from_yaml(target_path)
                inp_src.extend(
                    Source.build_source_list_from_org_info_list(org_infos, platform, category)
                )
        else:
            try:
                org_infos = OrgInfo.build_org_info_list_from_yaml(
                    Path(__file__).parents[2] / 'config/orgs.yaml'
                )
                repo_org_map = OrgInfo.build_repo_org_map(org_infos, platform)
            except Exception:
                repo_org_map = {}
            if target.startswith('id:'):
                # target: model id or dataset id -> str
                id = target.split(':')[-1]
                repo = id.split('/')[0]
                org = repo_org_map.get(repo, repo)
                inp_src.append(Source.from_id(id, platform, category, org))
            else:
                # target: repository
                org = repo_org_map.get(target, target)
                inp_src.append(Source.from_repo(target, platform, category, org))

        # Filter
        filtered_inp_src: list[Source] = []
        for src in inp_src:
            if required_org and src.org not in required_org:
                continue
            if skip_org and src.org in skip_org:
                continue
            if skip_repo and src.repo in skip_repo:
                continue
            if skip_id and f'{src.repo}/{src.name}' in skip_id:
                continue
            filtered_inp_src.append(src)

        logger.info(f'Input source: (total {len(filtered_inp_src)})\n{pformat(filtered_inp_src)}')

    outp.mkdir(parents=True, exist_ok=True)
    logger.info(f'Output path:\n{outp}')

    match platform:
        case 'huggingface':
            run_hf_crawl_pipeline(
                inp_src=filtered_inp_src,
                out_path=outp,
                max_retry=max_retry,
                token=token,
                endpoint=endpoint,
            )
        case 'modelscope':
            run_ms_crawl_pipeline(
                inp_src=filtered_inp_src,
                out_path=outp,
                max_retry=max_retry,
                endpoint=endpoint,
            )
        case 'baai-datahub':
            run_baai_data_pipeline(out_path=outp)
        case _:
            raise NotImplementedError()


@process_app.command('gen-modality')
def process_modality(
    inp_path: Annotated[
        str | None,
        Argument(
            help='Specify the data source (consistent with the path output by the crawl command), for example, huggingface_2026-01-01'
        ),
    ] = None,
    api_key: Annotated[
        str | None,
        Option(
            help='API key for the LLM service. If not provided, uses OPENAI_API_KEY environment variable.'
        ),
    ] = None,
    base_url: Annotated[
        str | None,
        Option(
            help='Base URL for the LLM service. If not provided, uses OPENAI_API_BASE environment variable.'
        ),
    ] = None,
    model: Annotated[
        str | None,
        Option(
            help='Model name to use. If not provided, uses OPENAI_MODEL_NAME environment variable or defaults to gpt-5.'
        ),
    ] = None,
):
    """
    Generate modal and lifecycle information for all raw data in the specified directory, while updating the configuration file.
    """
    ai_helper = ModalityAIHelper(api_key=api_key, base_url=base_url, model=model)
    ai_helper.update_extra_info()
    if inp_path is None:
        return
    inp_dir = Path(inp_path)
    platform = inp_dir.name.split('_')[0]
    logger.info(
        f'Generate modality and lifecycle information for raw data in {inp_path} ({platform})'
    )
    if (inp_dir / 'raw_dataset_data.jsonl').exists():
        logger.info('Generate modality and lifecycle for dataset data.')
        ai_helper.update_raw_data(inp_dir / 'raw_dataset_data.jsonl', 'dataset')
    if (inp_dir / 'raw_model_data.jsonl').exists():
        logger.info('Generate modality for model data.')
        ai_helper.update_raw_data(inp_dir / 'raw_model_data.jsonl', 'model')


@process_app.command('osir-lmts')
def process_osir_lmts(
    target_month: Annotated[
        str | None,
        Argument(
            help='The target month in YYYY-MM format (e.g., 2026-03). If not provided, uses the previous month.'
        ),
    ] = None,
    output_root: Annotated[
        str,
        Option(help='Root directory containing the crawled output data.'),
    ] = './output',
    config_root: Annotated[
        str,
        Option(
            help='Root directory containing configuration files (orgs.yaml, model_info.jsonl, etc.).'
        ),
    ] = './config',
    infra_source_path: Annotated[
        str | None,
        Option(
            help='Path to the manually curated infra_summary.csv file. '
            'If not provided (None), will try to load from: '
            '1) output/osir-lmts_{YYYY-MM}/infra_summary.csv (if exists), '
            '2) project root/infra_summary.csv. '
            'If neither exists, will raise an error.'
        ),
    ] = None,
    eval_source_path: Annotated[
        str | None,
        Option(
            help='Path to the manually curated eval_summary.csv file. '
            'If not provided (None), will try to load from: '
            '1) output/osir-lmts_{YYYY-MM}/eval_summary.csv (if exists), '
            '2) project root/eval_summary.csv. '
            'If neither exists, will raise an error.'
        ),
    ] = None,
    target_orgs_path: Annotated[
        str | None,
        Option(
            help='Specify the path to the JSON file containing the list of institutions to be included in the ranking.'
        ),
    ] = './config/osir_lmts_orgs.json',
):
    """
    Generate OSIR-LMTS (Open Source AI Resource - Large Model Tracking System) aggregated data.

    This command aggregates data from multiple platforms (HuggingFace, ModelScope, BAAI DataHub)
    for a given month, generates monthly delta and accumulated statistics, summary CSVs by
    organization/modality/lifecycle, and rankings using a configurable weighting strategy.

    Output files:
    - model_data.jsonl / dataset_data.jsonl: Monthly delta data
    - acc_model_data.jsonl / acc_dataset_data.jsonl: Accumulated data since first month
    - model_summary.csv / dataset_summary.csv: Summary by org/modality(/lifecycle) for current month
    - acc_model_summary.csv / acc_dataset_summary.csv: Summary for accumulated data
    - delta_model_summary.csv / delta_dataset_summary.csv: Delta summary (no ranking)
    - infra_summary.csv: Copied from the provided source path (manually curated)
    - eval_summary.csv: Copied from the provided source path (manually curated)
    - model_rank.csv / dataset_rank.csv / infra_rank.csv / eval_rank.csv / overall_rank.csv: Rankings for current month
    - acc_model_rank.csv / acc_dataset_rank.csv / acc_overall_rank.csv: Rankings for accumulated data
    """
    # Resolve target_month: if None, use previous month
    if target_month is None:
        today = datetime.today()
        first_day = today.replace(day=1)
        last_month = first_day - timedelta(days=1)
        resolved_target_month = last_month.strftime('%Y-%m')
    else:
        resolved_target_month = target_month

    strategy = get_rank_strategy_for_month(resolved_target_month)

    if target_orgs_path:
        if Path(target_orgs_path).exists():
            with Path(target_orgs_path).open() as f:
                target_orgs = json.load(f)
        else:
            target_orgs = None
            logger.warning(f'{target_orgs_path} does not exist.')
    else:
        target_orgs = None

    processor = OsirLmtsProcessor(
        target_month=resolved_target_month,
        target_orgs=target_orgs,
        output_root=Path(output_root),
        config_root=Path(config_root),
    )

    processor.run(
        strategy=strategy,
        infra_source_path=Path(infra_source_path) if infra_source_path else None,
        eval_source_path=Path(eval_source_path) if eval_source_path else None,
    )


@app.command()
def dashboard(
    port: Annotated[
        int,
        Option(help="Port to run the Streamlit dashboard on."),
    ] = 8501,
    host: Annotated[
        str,
        Option(help="Host to run the Streamlit dashboard on."),
    ] = "localhost",
):
    """
    Launch the Streamlit dashboard for data visualization.
    """
    dashboard_path = Path(__file__).parent / "ui" / "dashboard.py"

    if not dashboard_path.exists():
        logger.error(f"Dashboard file not found: {dashboard_path}")
        raise typer.Exit(1)

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(dashboard_path),
        "--server.port",
        str(port),
        "--server.address",
        host,
        "--server.headless",
        "true",
    ]

    logger.info(f"Launching dashboard at http://{host}:{port}")
    logger.info(f"Running command: {' '.join(cmd)}")

    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        logger.info("Dashboard stopped by user")
    except subprocess.CalledProcessError as e:
        logger.error(f"Dashboard failed with error: {e}")
        raise typer.Exit(1)


@app.command()
def report():
    """
    Generate data analysis report.
    """
    raise NotImplementedError()


@db_app.command('init')
def init_db(
    db_path: Annotated[
        str,
        Option(help='Path to the SQLite database file.'),
    ] = './output/osir_lmts.db',
    output_root: Annotated[
        str,
        Option(help='Root directory containing the osir-lmts output directories.'),
    ] = './output',
    config_root: Annotated[
        str,
        Option(help='Root directory containing configuration files.'),
    ] = './config',
):
    """
    Initialize the OSIR-LMTS database from all available osir-lmts_xxx directories.

    This command:
    1. Creates a new SQLite database (or overwrites if exists)
    2. Loads organization mappings from config
    3. Imports data from all osir-lmts_YYYY-MM directories in output_root
    4. Applies filtering: only target orgs, exclude Evaluation datasets
    """
    db = OsirLmtsDatabase(db_path)

    # Remove existing database if it exists
    db_path_obj = Path(db_path)
    if db_path_obj.exists():
        logger.warning(f'Database file already exists, will be overwritten: {db_path}')
        db_path_obj.unlink()

    db.initialize(output_root=output_root, config_root=config_root)

    # Show summary
    months = db.get_available_months()
    logger.info(f'Database initialized successfully at {db_path}')
    logger.info(f'Available months: {months}')


@db_app.command('update')
def update_db(
    osir_dir: Annotated[
        str,
        Argument(help='Path to the osir-lmts_YYYY-MM directory to import.'),
    ],
    db_path: Annotated[
        str,
        Option(help='Path to the SQLite database file.'),
    ] = './output/osir_lmts.db',
    config_root: Annotated[
        str,
        Option(help='Root directory containing configuration files.'),
    ] = './config',
):
    """
    Update the database with data from a specific osir-lmts_YYYY-MM directory.

    This command:
    1. Opens an existing database (or creates if doesn't exist)
    2. Loads organization mappings from config
    3. Imports or replaces data for the month from the specified directory
    4. Applies filtering: only target orgs, exclude Evaluation datasets
    """
    db = OsirLmtsDatabase(db_path)

    if not Path(db_path).exists():
        logger.info(f'Creating new database at {db_path}')

    db.update_month(osir_dir=osir_dir, config_root=config_root)

    # Show summary
    months = db.get_available_months()
    logger.info(f'Database updated successfully at {db_path}')
    logger.info(f'Available months: {months}')


@db_app.command('list')
def list_db(
    db_path: Annotated[
        str,
        Option(help='Path to the SQLite database file.'),
    ] = './output/osir_lmts.db',
):
    """
    List available months in the database.
    """
    db = OsirLmtsDatabase(db_path)

    if not Path(db_path).exists():
        logger.error(f'Database not found: {db_path}')
        raise typer.Exit(1)

    months = db.get_available_months()
    logger.info(f'Available months in database:')
    for month in months:
        # Count records for this month
        model_count = sum(1 for _ in db.get_models(month=month))
        dataset_count = sum(1 for _ in db.get_datasets(month=month))
        logger.info(f'  {month}: {model_count} models, {dataset_count} datasets')


def main() -> None:
    print('Hello from oslm-analyst!')
    app()
