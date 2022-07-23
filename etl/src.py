"""
ETL for data coming from Bank of Canada API
"""

import os
import sys
import json
import sqlite3
import decimal

from pathlib import Path
from datetime import date, datetime, timedelta
from abc import ABC, abstractmethod, abstractproperty
from typing import Union, Any, Tuple 

import petl
import dropbox
import requests


class ProcessStep(ABC):

    @abstractproperty
    def result(self) -> Union[Tuple[Any], None]:
        """
        This property exposes the result of any step of the process pipeline.
        It will be passed on to the next step as input.
        """
        ...

    @abstractmethod
    def run(self) -> None:
        """
        This method will be run after instantiating each process component in the pipeline.
        It implements the entire logic of any given step.
        E.g. Extract subclass will implement data extraction logic in this method.
        It will therefore make use of other methods in the class that implement
        all the steps to extract the data, like make request to API or download
        file from Dropbox.
        """
        ...


class Extract(ProcessStep):
    _DATE_FMT = "%Y-%m-%d"
    _DATETIME_FMT = "%m-%d-%Y-%H:%M:%S"

    def __init__(self,  
                 boc_config_dict: dict,
                 dbx: dropbox.Dropbox,
                 path_to_expenses: Union[bytes, str, os.PathLike]) -> None:
                 
        self.api_url_template = boc_config_dict["url"]
        self.dbx = dbx

        start_date = boc_config_dict["startDate"]
        if start_date.weekday == 6:
            # Saturday
            start_date -= timedelta(days=1)
        elif start_date.weekday == 0:
            # Sunday
            start_date -= timedelta(days=2)
        
        try:
            self.start_date = start_date.strftime(self._DATE_FMT)
        except AttributeError as e:
            raise TypeError(f"Expected start date as date, got {type(start_date)} instead.") from e

        end_date = boc_config_dict["endDate"]
        if isinstance(end_date, date):
            self.end_date = end_date.strftime(self._DATE_FMT)
        elif isinstance(end_date, str):
            self.end_date = end_date

        self.path_to_expenses = path_to_expenses

        self.api_url: str = self._create_api_url()

        self.exchange_rates_dict: dict
        self.api_call_status_code: int

        self.expenses_petl_table: petl.Table

    def _create_api_url(self) -> str:
        try:
            api_url = self.api_url_template.format(self.start_date, self.end_date)
        
        except AttributeError:
            self.api_url_template = self.api_url_template.split("&")[0]
            api_url = self.api_url_template.format(self.start_date)            
        
        return api_url

    def fetch_exchange_rate_data(self) -> None:
        response = requests.get(self.api_url)

        self.exchange_rates_dict = response.json()
        self.api_call_status_code = response.status_code

    def save_raw_data(self) -> None:
        now = datetime.now().strftime(self._DATETIME_FMT)
        failed: bool = (self.api_call_status_code != 200)

        if failed:
            root = "/extracted-failed/"
            # here logging of error
        else:
            root = "/extracted/"

        exchange_rates_dict = json.dumps(self.exchange_rates_dict).encode("utf-8")
        save_name = root + f"FXCADUSD_{now}.json"

        self.dbx.files_upload(exchange_rates_dict, save_name, mode=dropbox.files.WriteMode.overwrite)

    def download_expenses(self) -> None:
        self.dbx.files_download_to_file(self.path_to_expenses, "/expenses.xlsx")

    def extract_expenses(self) -> None:
        path_to_expenses = Path(self.path_to_expenses)

        if not path_to_expenses.exists():
            raise FileNotFoundError(f"File {path_to_expenses} does not exist.")
        
        loading_mode = {
            ".xlsx": petl.io.fromxlsx,
            ".xls": petl.io.fromxls,
            ".csv": petl.io.fromcsv
        }[path_to_expenses.suffix]

        self.expenses_petl_table = loading_mode(str(path_to_expenses))

    def run(self) -> None:
        self.fetch_exchange_rate_data()

        self.save_raw_data()

        if self.api_call_status_code != 200:
            sys.exit()

        self.download_expenses()

        self.extract_expenses()

    def result(self) -> Tuple[dict, petl.Table]:
        return self.exchange_rates_dict, self.expenses_petl_table


class Transform(ProcessStep):

    def __init__(self, exchange_dict: dict, expenses_table: petl.Table) -> None:
        self.exchange_dict = exchange_dict
        self.expenses_petl_table = expenses_table

        # intermediate variables
        self.exchange_petl_table: petl.Table

        # out variable that will flow to loading step
        self.out_table: petl.Table

    def create_exchange_petl_table(self) -> None:
        boc_dates = []
        boc_rates = []
        for item in self.exchange_dict["observations"]:
            boc_dates.append(
                datetime.strptime(item["d"], "%Y-%m-%d")
            )

            boc_rates.append(
                decimal.Decimal(item["FXUSDCAD"]["v"])
            )

        self.exchange_petl_table = petl.fromcolumns([boc_dates, boc_rates], header=['date','rate'])

    def join_tables(self) -> None:
        self.out_table = petl.outerjoin(
            self.expenses_petl_table, 
            self.exchange_petl_table,
            key='date'
            )

    def subset_table(self) -> None:
        self.out_table = petl.filldown(self.out_table, "rate")
        
        self.out_table = petl.select(
            self.out_table,
            lambda rec: rec.USD is not None
            )
    
    def add_cad_field(self) -> None:
        self.out_table = petl.addfield(
            self.out_table, 
            "CAD", 
            lambda rec: decimal.Decimal(rec.USD) * rec.rate if rec.rate is not None else 0 
            )

    def run(self) -> None:
        self.create_exchange_petl_table()
        self.join_tables()
        self.subset_table()
        self.add_cad_field()

    def result(self) -> Tuple[petl.Table]:
        return self.out_table,


class Load(ProcessStep):

    def __init__(self, transformed_table: petl.Table, db_file:str, table_name:str) -> None:
        self.table = transformed_table
        self.db_file = db_file
        self.table_name = table_name

        self.db_conn: sqlite3.Connection


    def create_db_connection(self) -> None:
        """ create a database connection to a SQLite database """
        
        try:
            self.db_conn = sqlite3.connect(self.db_file)
            print("SQLTE3 connection open")
        except sqlite3.Error as e:
            print(e)

    def load_to_db(self) -> None:
        petl.todb(self.table, self.db_conn, self.table_name)

    def run(self) -> None:
        try:
            self.create_db_connection()
            self.load_to_db()
        finally:
            self.db_conn.close()

    def result(self) -> None:
        print("Data successfully loaded")
