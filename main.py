"""
Main runnable module
"""
import yaml
import dropbox

from etl import run_etl
from db_utils import create_connection

# read config
with open("config.yaml", "r", encoding="utf-8") as config_fp:
    _CONFIG = yaml.safe_load(config_fp)

# read api token
with open("api-token.txt", "r") as fp:
    _API_TOKEN = fp.read()


def main():
    dbx = dropbox.Dropbox(_API_TOKEN)
    conn = create_connection(r"db\expenses.db")
    
    run_etl(_CONFIG, dbx, conn)


if __name__ == "__main__":
    main()
