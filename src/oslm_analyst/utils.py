import yaml
from pathlib import Path
from datetime import datetime


def today() -> str:
    return str(datetime.today().date())


def now() -> str:
    return datetime.now().strftime('%Y%M%D_%h%m%s')


def parse_commas_separated_params(param: str) -> list[str]:
    params = param.split(',')
    params = [p.strip() for p in params]
    return params


def parse_orgs_yaml():
    pass


def parse_target(target: str):
    pass
