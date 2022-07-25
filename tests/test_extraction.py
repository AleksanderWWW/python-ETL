import os

import pytest

from etl.src import Extract


@pytest.fixture(scope="module")
def extract_obj(config, dbx) -> Extract:
    return Extract(config["BOC"], dbx, "data/expenses.xlsx")

@pytest.fixture(scope="module")
def dates(config):
    return config["BOC"]["startDate"], config["BOC"]["endDate"]

# ===============================================================
# TEST CASES
# ===============================================================

def test_create_api_url(extract_obj, config, dates):
    true_url_template = config["BOC"]["url"]
    true_url = true_url_template.format(
        dates[0],
        dates[1]
    )
    assert extract_obj._create_api_url() == true_url


def test_fetch_data_status_code(extract_obj):
    extract_obj.fetch_exchange_rate_data()
    assert extract_obj.api_call_status_code == 200


def test_fetch_data_returns_correct_dict(extract_obj):
    extract_obj.fetch_exchange_rate_data()
    assert "observations" in extract_obj.exchange_rates_dict.keys()


def test_save_raw_data_success(extract_obj, dbx):
    extract_obj.fetch_exchange_rate_data()
    extract_obj.save_raw_data()
    file_name_lower = f"/extracted/fxcadusd_{extract_obj.now}.json"
    files = [f.path_lower for f in dbx.files_list_folder(path="/extracted/").entries]
    assert file_name_lower in files


def test_download_expenses(extract_obj):
    original_path = extract_obj.path_to_expenses

    cwd = os.getcwd()
    if os.path.basename(cwd) == "python-ETL":
        path_to_expenses = "./tests/data/"
    else:
        path_to_expenses = "./data/"

    path_to_expenses += "expenses.xlsx"

    extract_obj.path_to_expenses = path_to_expenses

    extract_obj.download_expenses()

    assert os.path.exists(path_to_expenses)

    os.remove(path_to_expenses)

    extract_obj.path_to_expenses = original_path


def test_extract_expenses_raises_error(extract_obj):
    original_path = extract_obj.path_to_expenses

    extract_obj.path_to_expenses = "non_existent_path"

    with pytest.raises(FileNotFoundError):
        extract_obj.extract_expenses()

    extract_obj.path_to_expenses = original_path
