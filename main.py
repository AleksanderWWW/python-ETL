"""
Main runnable module
"""
import yaml
import dropbox

from etl.etl_pipeline import run_pipeline
from db_utils import create_connection

# read config
with open("config.yaml", "r", encoding="utf-8") as config_fp:
    _CONFIG = yaml.safe_load(config_fp)

# read api token
with open("api-token.txt", "r") as fp:
    _API_TOKEN = fp.read()
    _DBX = dropbox.Dropbox(_API_TOKEN)

_DB_CONN = create_connection(_CONFIG["DB"]["path_to_db"])


def main():
    run_pipeline(_CONFIG, _DBX, _DB_CONN)


if __name__ == "__main__":
    main()
