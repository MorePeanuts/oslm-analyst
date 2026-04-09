import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import jsonlines
from loguru import logger


@dataclass
class ModelRecord:
    id: str
    org: str
    repo: str
    name: str
    modality: str
    downloads_last_month: int
    likes: int
    discussions: int
    descendants: int
    date_crawl: str
    month: str


@dataclass
class DataRecord:
    id: str
    org: str
    repo: str
    name: str
    modality: str
    lifecycle: str
    downloads_last_month: int
    likes: int
    discussions: int
    descendants: int
    date_crawl: str
    month: str


class OsirLmtsDatabase:
    """SQLite database for OSIR-LMTS data."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self._org_map: dict[str, str] = {}
        self._target_orgs: set[str] | None = None

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self, output_root: Path | str = './output', config_root: Path | str = './config') -> None:
        """Initialize the database and import all available data."""
        output_root = Path(output_root)
        config_root = Path(config_root)

        logger.info(f'Initializing database at {self.db_path}')

        # Load configs
        self._load_configs(config_root)

        # Create tables
        conn = self.connect()
        try:
            self._create_tables(conn)

            # Find all osir-lmts directories
            osir_dirs = sorted([d for d in output_root.iterdir() if d.is_dir() and d.name.startswith('osir-lmts_')])
            logger.info(f'Found {len(osir_dirs)} osir-lmts directories')

            for osir_dir in osir_dirs:
                try:
                    month = osir_dir.name.split('_', 1)[1]
                    self._import_month_data(conn, osir_dir, month)
                except Exception as e:
                    logger.error(f'Failed to import {osir_dir.name}: {e}')

            conn.commit()
            logger.info('Database initialized successfully')
        finally:
            conn.close()

    def update_month(self, osir_dir: Path | str, config_root: Path | str = './config') -> None:
        """Update database with data from a specific osir-lmts directory."""
        osir_dir = Path(osir_dir)
        config_root = Path(config_root)

        if not osir_dir.exists():
            raise ValueError(f'Directory not found: {osir_dir}')

        if not osir_dir.name.startswith('osir-lmts_'):
            raise ValueError(f'Not an osir-lmts directory: {osir_dir.name}')

        month = osir_dir.name.split('_', 1)[1]
        logger.info(f'Updating database with data for {month} from {osir_dir}')

        self._load_configs(config_root)

        conn = self.connect()
        try:
            self._create_tables(conn)
            self._import_month_data(conn, osir_dir, month)
            conn.commit()
            logger.info(f'Successfully updated data for {month}')
        finally:
            conn.close()

    def _load_configs(self, config_root: Path) -> None:
        """Load organization mapping and target orgs from config."""
        # Load orgs.yaml to get repo -> org mapping
        from oslm_analyst.utils import OrgInfo
        org_list = OrgInfo.build_org_info_list_from_yaml(config_root / 'orgs.yaml')
        self._org_map = OrgInfo.build_repo_org_map(org_list, 'huggingface')
        ms_org_map = OrgInfo.build_repo_org_map(org_list, 'modelscope')
        self._org_map.update(ms_org_map)

        # Load target orgs
        target_orgs_path = config_root / 'osir_lmts_orgs.json'
        if target_orgs_path.exists():
            with target_orgs_path.open() as f:
                self._target_orgs = set(json.load(f))
            logger.info(f'Loaded {len(self._target_orgs)} target organizations')

    def _create_tables(self, conn: sqlite3.Connection) -> None:
        """Create database tables if they don't exist."""
        conn.execute('''
            CREATE TABLE IF NOT EXISTS models (
                id TEXT,
                month TEXT,
                org TEXT,
                repo TEXT,
                name TEXT,
                modality TEXT,
                downloads_last_month INTEGER,
                likes INTEGER,
                discussions INTEGER,
                descendants INTEGER,
                date_crawl TEXT,
                PRIMARY KEY (id, month)
            )
        ''')

        conn.execute('''
            CREATE TABLE IF NOT EXISTS datasets (
                id TEXT,
                month TEXT,
                org TEXT,
                repo TEXT,
                name TEXT,
                modality TEXT,
                lifecycle TEXT,
                downloads_last_month INTEGER,
                likes INTEGER,
                discussions INTEGER,
                descendants INTEGER,
                date_crawl TEXT,
                PRIMARY KEY (id, month)
            )
        ''')

        conn.execute('CREATE INDEX IF NOT EXISTS idx_models_month ON models(month)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_models_org ON models(org)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_datasets_month ON datasets(month)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_datasets_org ON datasets(org)')

    def _import_month_data(self, conn: sqlite3.Connection, osir_dir: Path, month: str) -> None:
        """Import data for a specific month."""
        logger.info(f'Importing data for {month}')

        # Import model data
        model_data_path = osir_dir / 'model_data.jsonl'
        if model_data_path.exists():
            count = self._import_model_data(conn, model_data_path, month)
            logger.info(f'  Imported {count} models for {month}')

        # Import dataset data
        dataset_data_path = osir_dir / 'dataset_data.jsonl'
        if dataset_data_path.exists():
            count = self._import_dataset_data(conn, dataset_data_path, month)
            logger.info(f'  Imported {count} datasets for {month}')

    def _import_model_data(self, conn: sqlite3.Connection, file_path: Path, month: str) -> int:
        """Import model data from a JSONL file."""
        count = 0

        with jsonlines.open(file_path) as f:
            for line in f:
                record = self._parse_model_line(line, month)
                if record is None:
                    continue

                # Skip if org not in target orgs
                if self._target_orgs and record.org not in self._target_orgs:
                    continue

                conn.execute('''
                    INSERT OR REPLACE INTO models
                    (id, month, org, repo, name, modality, downloads_last_month, likes, discussions, descendants, date_crawl)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record.id,
                    record.month,
                    record.org,
                    record.repo,
                    record.name,
                    record.modality,
                    record.downloads_last_month,
                    record.likes,
                    record.discussions,
                    record.descendants,
                    record.date_crawl,
                ))
                count += 1

        return count

    def _import_dataset_data(self, conn: sqlite3.Connection, file_path: Path, month: str) -> int:
        """Import dataset data from a JSONL file."""
        count = 0

        with jsonlines.open(file_path) as f:
            for line in f:
                record = self._parse_dataset_line(line, month)
                if record is None:
                    continue

                # Skip if modality is Evaluation
                if record.lifecycle == 'Evaluation':
                    continue

                # Skip if org not in target orgs
                if self._target_orgs and record.org not in self._target_orgs:
                    continue

                conn.execute('''
                    INSERT OR REPLACE INTO datasets
                    (id, month, org, repo, name, modality, lifecycle, downloads_last_month, likes, discussions, descendants, date_crawl)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    record.id,
                    record.month,
                    record.org,
                    record.repo,
                    record.name,
                    record.modality,
                    record.lifecycle,
                    record.downloads_last_month,
                    record.likes,
                    record.discussions,
                    record.descendants,
                    record.date_crawl,
                ))
                count += 1

        return count

    def _parse_model_line(self, line: dict, month: str) -> ModelRecord | None:
        """Parse a JSONL line into a ModelRecord."""
        identifier = line.get('identifier', '')
        if not identifier:
            return None

        repo, name = self._split_identifier(identifier)
        org = self._get_org_for_identifier(identifier)

        return ModelRecord(
            id=identifier,
            org=org,
            repo=repo,
            name=name,
            modality=line.get('modality') or 'Unknown',
            downloads_last_month=line.get('downloads_last_month') or 0,
            likes=line.get('likes') or 0,
            discussions=line.get('discussions') or 0,
            descendants=line.get('descendants') or 0,
            date_crawl=line.get('date_crawl', month),
            month=month,
        )

    def _parse_dataset_line(self, line: dict, month: str) -> DataRecord | None:
        """Parse a JSONL line into a DataRecord."""
        identifier = line.get('identifier', '')
        if not identifier:
            return None

        repo, name = self._split_identifier(identifier)
        org = self._get_org_for_identifier(identifier)

        return DataRecord(
            id=identifier,
            org=org,
            repo=repo,
            name=name,
            modality=line.get('modality') or 'Unknown',
            lifecycle=line.get('lifecycle') or 'Unknown',
            downloads_last_month=line.get('downloads_last_month') or 0,
            likes=line.get('likes') or 0,
            discussions=line.get('discussions') or 0,
            descendants=line.get('descendants') or 0,
            date_crawl=line.get('date_crawl', month),
            month=month,
        )

    def _split_identifier(self, identifier: str) -> tuple[str, str]:
        """Split identifier into repo and name."""
        if '/' in identifier:
            parts = identifier.split('/', 1)
            return parts[0], parts[1]
        return identifier, identifier

    def _get_org_for_identifier(self, identifier: str) -> str:
        """Get organization for an identifier."""
        repo = identifier.split('/')[0] if '/' in identifier else identifier
        return self._org_map.get(repo, repo)

    def get_models(self, month: str | None = None, org: str | None = None) -> Iterator[ModelRecord]:
        """Get model records from the database."""
        conn = self.connect()
        try:
            query = 'SELECT * FROM models WHERE 1=1'
            params: list = []

            if month:
                query += ' AND month = ?'
                params.append(month)

            if org:
                query += ' AND org = ?'
                params.append(org)

            for row in conn.execute(query, params):
                yield ModelRecord(**dict(row))
        finally:
            conn.close()

    def get_datasets(self, month: str | None = None, org: str | None = None) -> Iterator[DataRecord]:
        """Get dataset records from the database."""
        conn = self.connect()
        try:
            query = 'SELECT * FROM datasets WHERE 1=1'
            params: list = []

            if month:
                query += ' AND month = ?'
                params.append(month)

            if org:
                query += ' AND org = ?'
                params.append(org)

            for row in conn.execute(query, params):
                yield DataRecord(**dict(row))
        finally:
            conn.close()

    def get_available_months(self) -> list[str]:
        """Get list of available months in the database."""
        conn = self.connect()
        try:
            months = set()
            for row in conn.execute('SELECT DISTINCT month FROM models UNION SELECT DISTINCT month FROM datasets ORDER BY month'):
                months.add(row[0])
            return sorted(months)
        finally:
            conn.close()
