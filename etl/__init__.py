import dropbox

from .src import *


def run_etl(config: dict, dbx: dropbox.Dropbox, db_conn):
    # BOC data
    conn = ApiConnector(
        config["BOC"]["url"],
        config["BOC"]["startDate"],
        "2020-12-30"
    )
    data = conn.fetch_data()
    conn.save_raw_data(data, dbx)

    # Expenses 
    download_expenses(dbx)
    expenses_table = extract_expenses()

    transform = DataTransform(data)

    exchange_rates_table = transform.create_table()

    final_table = transform.join_tables(expenses_table, exchange_rates_table)

    loader = Loader(final_table)

    loader.load_to_db(db_conn)
