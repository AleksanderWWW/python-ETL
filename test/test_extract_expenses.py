import unittest

from petl import Table

from etl.src import extract_expenses


class ExpensesExtractionTest(unittest.TestCase):

    def test_reading_existing_file_returns_petl_table(self):
        table = extract_expenses(r"etl\expenses.xlsx")
        self.assertTrue(isinstance(table, Table))

    def test_reading_non_existing_file_raises_FileNotFoundError(self):
        self.assertRaises(FileNotFoundError, lambda: extract_expenses("some_gibberish.xlsx"))

    def test_column_names_correct(self):
        table = extract_expenses(r"etl\expenses.xlsx")
        self.assertEqual(table.header(), ("date", "USD"))


if __name__ == "__main__":
    unittest.main()