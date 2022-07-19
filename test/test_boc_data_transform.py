import json
import unittest

from etl.src import BOCDataTransform


class TransformTest(unittest.TestCase):
    with open(r"test\sample_data.json", "r") as fp:
        data = json.load(fp)

    def test_data_read_correctly(self):
        transform_obj = BOCDataTransform(self.data)
        self.assertEqual(
            transform_obj.get_data(),
            self.data["observations"]
        )