"""
ETL for data coming from Bank of Canada API
"""
import sys

from typing import Union, Dict, Any

import requests


class ApiConnector:
    """
    Class providing methods to connect to Bank of Canada API and extract the FX USD/CAD data
    """
    def __init__(self, api_url: str, start_date: str, end_date: Union[str, None] = None):
        self.api_url_template = api_url
        self.start_date = start_date
        self.end_date = end_date

        if end_date is None:
            self.api_url_template = self.api_url_template.split("&")[0]
            self.api_url = self.api_url_template.format(start_date)

        else:
            self.api_url = self.api_url_template.format(start_date, end_date)

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
