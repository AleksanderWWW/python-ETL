"""
ETL for data coming from Bank of Canada API
"""
from typing import Union

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
