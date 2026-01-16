import yaml
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field


def today() -> str:
    return str(datetime.today().date())


def now() -> str:
    return datetime.now().strftime('%Y%M%D_%h%m%s')


def parse_commas_separated_params(param: str) -> list[str]:
    params = param.split(',')
    params = [p.strip() for p in params]
    return params


@dataclass
class OrgInfo:
    org: str
    type: str
    country: str
    focus: list[str] = field(default_factory=list)
    hf_accounts: list[str] = field(default_factory=list)
    ms_accounts: list[str] = field(default_factory=list)

    @staticmethod
    def from_orgs_yaml(inp_path: str | Path) -> list['OrgInfo']:
        if isinstance(inp_path, str):
            inp_path = Path(inp_path)
        assert inp_path.name.endswith('.yaml'), f'The file must be in yaml format. {inp_path.name}'
        with inp_path.open() as f:
            data: list[dict] = yaml.safe_load(f)
        res = [OrgInfo(**d) for d in data]
        return res
