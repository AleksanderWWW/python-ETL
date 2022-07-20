import sys

from sqlite3 import Connection  # for type hints
from typing import Tuple  # for type hints

import dropbox  # for type hints

from petl import Table  # for type hints

from etl.src import (
    Extract,
    Transform,
    Load
)


def extract(config: dict, dbx: dropbox.Dropbox) -> Tuple[dict, Table]:
    failed: bool = False

    ext = Extract(
        config["BOC"]["url"],
        dbx,
        config["BOC"]["startDate"],
        config["BOC"]["endDate"]
    )

    # extract exchange rate data
    status, boc_data = ext.fetch_data()
    if status != 200:
        failed = True

    ext.save_raw_data(boc_data, failed=failed)

    if failed:
        print("Extraction part failed. Aborting pipeline run.")
        sys.exit()

    # extract expenses
    ext.download_expenses()
    expenses_table = ext.extract_expenses()

    return boc_data, expenses_table


def transform(boc_data: dict, expenses_table: Table) -> Table:

    transformer = Transform(boc_data)
    exchange_rates_table = transformer.create_table()

    return transformer.join_tables(
        expenses_table,
        exchange_rates_table
    )


def load(joined_table: Table, db_conn: Connection):
    loader = Load(joined_table)
    loader.load_to_db(db_conn)


def run_pipeline(config: dict, 
                 dbx: dropbox.Dropbox, 
                 db_connection: Connection) -> None:

    boc_data, expenses_table = extract(config, dbx)

    joined_table = transform(boc_data, expenses_table)

    load(joined_table, db_connection)
