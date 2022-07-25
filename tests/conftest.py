import os

from typing import Union

import yaml
import dotenv
import dropbox

import pytest


@pytest.fixture(scope="session")
def config() -> dict:
    path_to_config = "./config.yaml"
    if not os.path.isfile(path_to_config):
        # tests run from tests/ directory
        path_to_config = "../config.yaml"

    with open(path_to_config, "r", encoding="utf-8") as config_fp:
        return yaml.safe_load(config_fp)


@pytest.fixture(scope="session")
def dbx() -> Union[dropbox.Dropbox, None]:
    dotenv.load_dotenv()
    return dropbox.Dropbox(os.getenv("APP_TOKEN")) or None