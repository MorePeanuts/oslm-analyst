import yaml
import jsonlines
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import NamedTuple


def today() -> str:
    return str(datetime.today().date())


def now() -> str:
    return datetime.now().strftime('%Y%M%D_%h%m%s')


def parse_commas_separated_params(param: str) -> list[str]:
    params = param.split(',')
    params = [p.strip() for p in params]
    return params


class Source(NamedTuple):
    """
    Data structure for the data source passed to the Crawler.
    """

    platform: str
    org: str
    repo: str
    name: str | None
    category: str

    @staticmethod
    def build_source_list_from_org_info_list(
        org_infos: list['OrgInfo'], platform: str, category: str
    ) -> list['Source']:
        res = []
        for org_info in org_infos:
            res.extend(org_info.expand_to_source_list(platform, category))
        return res

    @staticmethod
    def build_source_list_from_error(
        dir_path: Path, platform: str, category: str, repo_org_map: dict[str, str]
    ) -> list['Source']:
        res = []
        err_path = dir_path / f'err_{category}_data.jsonl'
        with jsonlines.open(err_path, 'r') as f:
            for line in f:
                res.append(
                    Source(
                        platform, repo_org_map[line['repo']], line['repo'], line['name'], category
                    )
                )
        return res

    @classmethod
    def from_id(cls, id: str, platform: str, category: str, org: str) -> 'Source':
        repo, name = id.split('/')
        return Source(platform, org, repo, name, category)

    @classmethod
    def from_repo(cls, repo: str, platform: str, category: str, org: str) -> 'Source':
        return Source(platform, org, repo, None, category)


@dataclass
class OrgInfo:
    """
    The data structure corresponding to `config/orgs.yaml`.
    """

    org: str
    type: str
    country: str
    focus: list[str] = field(default_factory=list)
    hf_accounts: list[str] = field(default_factory=list)
    ms_accounts: list[str] = field(default_factory=list)

    @staticmethod
    def build_org_info_list_from_yaml(inp_path: str | Path) -> list['OrgInfo']:
        if isinstance(inp_path, str):
            inp_path = Path(inp_path)
        assert inp_path.name.endswith('.yaml'), f'The file must be in yaml format. {inp_path.name}'
        with inp_path.open() as f:
            data: list[dict] = yaml.safe_load(f)
        res = [OrgInfo(**d) for d in data]
        return res

    @staticmethod
    def build_repo_org_map(org_infos: list['OrgInfo'], platform: str) -> dict[str, str]:
        res = {}
        for org_info in org_infos:
            match platform:
                case 'huggingface':
                    accounts = org_info.hf_accounts.copy()
                case 'modelscope':
                    accounts = org_info.ms_accounts.copy()
            for repo in accounts:
                res[repo] = org_info.org
        return res

    def expand_to_source_list(self, platform: str, category: str) -> list[Source]:
        res = []
        match platform:
            case 'huggingface':
                accounts = self.hf_accounts.copy()
            case 'modelscope':
                accounts = self.ms_accounts.copy()

        for repo in accounts:
            res.append(Source(platform, self.org, repo, None, category))
        return res
