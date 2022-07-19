"""
ETL for data coming from Bank of Canada API
"""

import os
import json
import sys

from pathlib import Path
from datetime import date, datetime
from typing import (
    Union, 
    Dict, 
    Any, 
    Tuple, 
    List
)

import petl
import dropbox
import requests

class Extract:
    """
    Class providing methods to connect to Bank of Canada API and extract the FX USD/CAD data.
    This class encapsulates the EXTRACT part of the ETL process.
    """
    _DATE_FMT = "%Y-%m-%d"

    def __init__(self, api_url: str, dbx: dropbox.Dropbox,
                 start_date: date, end_date: Union[date, None] = None):
        self.api_url_template = api_url
        self.dbx = dbx

        try:
            self.start_date = start_date.strftime(self._DATE_FMT)
        except AttributeError as e:
            raise TypeError(f"Expected start date as date, got {type(start_date)} instead.") from e

        if isinstance(end_date, date):
            self.end_date = end_date.strftime(self._DATE_FMT)
        elif isinstance(end_date, str):
            self.end_date = end_date

        self.api_url: str = self._create_api_url()

    def _create_api_url(self) -> str:
        try:
            api_url = self.api_url_template.format(self.start_date, self.end_date)
        
        except AttributeError:
            self.api_url_template = self.api_url_template.split("&")[0]
            api_url = self.api_url_template.format(self.start_date)            
        
        return api_url

    def set_start_date(self, new_start: date) -> None:
        """
        Setter method for start date
        """
        if isinstance(new_start, date):
            new_start_str = new_start.strftime(self._DATE_FMT)
            self.start_date = new_start_str
        else:
            raise TypeError(f"Expected new start date as date, got {type(new_start)} instead.")
        
        # update api url
        self.api_url = self._create_api_url()

    def set_end_date(self, new_end: date) -> None:
        """
        Setter method for end date
        """
        if isinstance(new_end, date):
            new_end_str = new_end.strftime(self._DATE_FMT)
            self.end_date = new_end_str
        else:
            raise TypeError(f"Expected new end date as date, got {type(new_end)} instead.")

        # update api url
        self.api_url = self._create_api_url()


    def fetch_data(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetch fx data based on start and end dates provided in the constructor
        :return: JSON data retrieved from the API
        """
        response = requests.get(self.api_url)
        if response.status_code == 200:
            return 200, response.json()
        else:
            print(response.json())
            print(f">>> ERROR: request returned status code: {response.status_code}")
            return response.status_code, response.json()

    def save_raw_data(self, data: dict, failed=False):
        today = date.today().strftime(self._DATE_FMT)
        if failed:
            root = "/extracted-failed/"
        else:
            root = "/extracted/"

        data = json.dumps(data).encode("utf-8")
        save_name = root + f"FXCADUSD_{today}.json"
        self.dbx.files_upload(data, save_name, mode=dropbox.files.WriteMode.overwrite)

    def download_expenses(self) -> None:
        # TODO logging of metadata
        self.dbx.files_download_to_file("data/expenses.xlsx", "/expenses.xlsx")

    def extract_expenses(
                         self, 
                         path_to_expenses: Union[bytes, str, os.PathLike] = "data/expenses.xlsx"
                         ) -> petl.Table:
        """
        Extracting expenses table from expenses excel file.

        :param path_to_expenses: path identifier to the expenses table source file
        :returns: petl-style data table with expenses data
        :raies FileNotFoundError: if designated file does not exist 
        """

        if not isinstance(path_to_expenses, Path):
            path_to_expenses = Path(path_to_expenses)

        if not path_to_expenses.exists():
            raise FileNotFoundError(f"File {path_to_expenses} does not exist.")
        
        loading_mode = {
            ".xlsx": petl.io.fromxlsx,
            ".xls": petl.io.fromxls,
            ".csv": petl.io.fromcsv
        }[path_to_expenses.suffix]

        return loading_mode(str(path_to_expenses))


class Transform:
    """
    Objects of this type deal with data extracted from BOC servers.
    They will receive raw JSON-styled objects as input.
    As an output they will be able to produce table-like
    dataf structures ready to be loaded into a data wharehouse.

    This class encapsulates the TRANSFORM part of the ETL process.
    """

    def __init__(self, raw_data: dict) -> None:
        self._raw_data = raw_data
        try:
            self._data = raw_data["observations"]
        except KeyError as e:
            raise KeyError("Provided data lacks the appropriate key (observations)") from e

    def set_data(self, new_data: dict) -> None:
        self._raw_data = new_data
        self._data = new_data["observations"]

    def get_data(self) -> List:
        return self._data

    def _parse_data(self) -> Tuple[List]:
        boc_dates = []
        boc_rates = []

        for item in self._data:
            boc_dates.append(
                datetime.strptime(item["d"], "%Y-%m-%d")
            )

            boc_rates.append(
                item["FXUSDCAD"]["v"]
            )

        return boc_dates, boc_rates

    def create_table(self) -> petl.Table:
        dates, rates = self._parse_data()
        table = petl.fromcolumns([dates, rates], header=['date','rate'])

        return table

    def join_tables(self, expenses_table: petl.Table, rate_table: petl.Table) -> petl.Table:
        joined_table = petl.leftjoin(expenses_table, rate_table, key='date')
        return joined_table


class Load:

    def __init__(self, transformed_table: petl.Table) -> None:
        self.table = transformed_table

    def load_to_db(self, conn, table_name: str = "expenses"):
        petl.todb(self.table, conn, table_name)
        conn.close()
