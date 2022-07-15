"""
ETL for data coming from Bank of Canada API
"""
import sys


from datetime import date
from typing import Union, Dict, Any

import requests


class ApiConnector:
    """
    Class providing methods to connect to Bank of Canada API and extract the FX USD/CAD data
    """
    _DATE_FMT = "%Y-%m-%d"

    def __init__(self, api_url: str, start_date: date, end_date: Union[date, None] = None):
        self.api_url_template = api_url

        try:
            self.start_date = start_date.strftime(self._DATE_FMT)
        except AttributeError as e:
            raise TypeError(f"Expected start date as date, got {type(start_date)} instead.") from e

        if isinstance(end_date, date):
            self.end_date = end_date.strftime(self._DATE_FMT)

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
            return response.json()
        else:
            print(f">>> ERROR: request returned status code: {response.status_code}")
            print(response.json())
            sys.exit()
