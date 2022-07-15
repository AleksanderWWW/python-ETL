import unittest

from datetime import date, timedelta

import yaml

from boc_etl.src import ApiConnector


class ApiConnectorTest(unittest.TestCase):
    with open("config.yaml", "r") as fp:
        config = yaml.safe_load(fp)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=5)
    url = config["BOC"]["url"]

    def test_api_url_correct(self):
        api_conn = ApiConnector(self.url, self.start_date, self.end_date)
        correct_url = self.url.format(self.start_date, self.end_date)

        self.assertEqual(api_conn.api_url, correct_url)


if __name__ == "__main__":
    unittest.main()