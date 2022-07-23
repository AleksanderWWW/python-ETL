"""
Main runnable module
"""
import os

import yaml
import dropbox

from dotenv import load_dotenv

from etl.src import Extract, Transform, Load
from etl.pipeline_component import PipelineComponent
from etl.etl_pipeline import Pipeline

# read config
with open("config.yaml", "r", encoding="utf-8") as config_fp:
    _CONFIG = yaml.safe_load(config_fp)

# read api token
load_dotenv()
_DBX = dropbox.Dropbox(os.getenv("APP_TOKEN")) or None


def main():
    etl_components = [
        PipelineComponent(Extract, ("data/expenses.xlsx",)), 
        PipelineComponent(Transform, None), 
        PipelineComponent(Load, (_CONFIG["DB"]["path_to_db"], 
                                _CONFIG["DB"]["table_name"]))
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
