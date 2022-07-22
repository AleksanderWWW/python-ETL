"""
Main runnable module
"""
import os

import yaml
import dropbox

from dotenv import load_dotenv

from etl.src import Extract, Transform, Load
from etl.etl_pipeline import Pipeline

# read config
with open("config.yaml", "r", encoding="utf-8") as config_fp:
    _CONFIG = yaml.safe_load(config_fp)

# read api token
load_dotenv()
_DBX = dropbox.Dropbox(os.getenv("APP_TOKEN")) or None


def main():
    etl_components = [
        (Extract, ("data/expenses.xlsx",)), 
        (Transform, None), 
        (Load, ("db/expenses.db", "expenses"))
        ]
    init_args = (
        _CONFIG["BOC"],
        _DBX,
        )

    etl_pipeline = Pipeline(
        etl_components,
        *init_args
    )

    etl_pipeline.run_pipeline()


if __name__ == "__main__":
    main()
