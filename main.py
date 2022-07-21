"""
Main runnable module
"""
import yaml
import dropbox

from etl.src import Extract, Transform, Load
from etl.etl_pipeline import Pipeline

# read config
with open("config.yaml", "r", encoding="utf-8") as config_fp:
    _CONFIG = yaml.safe_load(config_fp)

# read api token
with open("api-token.txt", "r") as fp:
    _API_TOKEN = fp.read()
    _DBX = dropbox.Dropbox(_API_TOKEN)



def main():
    inputs = [
        (Extract, None), 
        (Transform, None), 
        (Load, ("db\expenses.db", "expenses"))
        ]
    init_args = (
        _CONFIG["BOC"]["url"],
        _DBX,
        _CONFIG["BOC"]["startDate"],
        _CONFIG["BOC"]["endDate"]
        )

    etl_pipeline = Pipeline(
        inputs,
        *init_args
    )

    etl_pipeline.run_pipeline()


if __name__ == "__main__":
    main()
