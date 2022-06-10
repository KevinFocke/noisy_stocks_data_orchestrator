from pathlib import Path
from queue import Empty, SimpleQueue

import numpy as np
import pandas as pd
import pytest
from noisy_stocks_data_orchestrator import __version__, main_flow
from noisy_stocks_data_orchestrator.customdatastructures import TimeSeries
from noisy_stocks_data_orchestrator.ingress import create_folder, folder_exists
from prefect.flows import flow

from tests.conftest import (
    stock_with_duplicate_dates,
    stock_with_negative_closing_price,
    stock_with_unequal_rows,
    stock_with_unordered_dates,
)

# For typechecking use isinstance()
# Start every test with test_

# Prefect testing:
# Testing state:
# state.is_succesful()
# state.result()   # With parenthesis to correclty resolve via function call

# Common troubleshoot:
# If you:
#  getdataset_list: list[str] = [
#  TypeError: 'type' object is not subscriptable
# Then the used Python version is not ^3.10


def test_version():
    assert __version__ == "0.1.0"


def test_sanity():
    """Can it find the module?"""
    assert main_flow.sanity_check() == "Module_Found"


def test_fixture_input_via_conftest(sanity_check_fixture):
    """
    conftest.py is used to supply fixtures accross test files
    """
    assert sanity_check_fixture == "testinput"


def test_create_stock(stock_with_date_nan):
    stock = stock_with_date_nan
    assert isinstance(stock, TimeSeries)
    assert isinstance(stock.name, str)
    assert isinstance(stock.time_series_df, pd.DataFrame)


def test_date_nan_removed(stock_with_date_nan):
    index = stock_with_date_nan.time_series_df.index
    assert len(index) == 4


def test_stock_df_wrong_size_length():
    with pytest.raises(ValueError) as ve:
        stock_with_unequal_rows()

    # When testing exception message, convert it to a string
    assert str(ve.value) == "All arrays must be of the same length"


def test_stock_df_wrong_close_price():
    # with pytest.raises(TypeError) as se:
    stock = stock_with_negative_closing_price()
    closing_price_array = stock.time_series_df["close_price"].values
    expected_result = np.array([1.4])
    assert closing_price_array == expected_result


def test_drop_duplicate_dates():
    stock = stock_with_duplicate_dates()
    closing_price_array = stock.time_series_df["close_price"].values
    expected_result = np.array([1.4, 1.3])  # dates are also reordered
    assert np.array_equal(closing_price_array, expected_result)  # type: ignore


def test_dates_sorted():
    stock = stock_with_unordered_dates()
    timestamp_array = stock.time_series_df["timestamp"].values
    # expected_result in datetime
    expected_result = pd.to_datetime(
        [
            "1976-10-04T00:00:00.000000000",
            "1980-02-05T00:00:00.000000000",
            "1996-10-04T00:00:00.000000000",
        ]
    ).values
    assert np.array_equal(timestamp_array, expected_result)  # type:ignore


@flow
def test_folder_existence(tmp_path):
    assert isinstance(tmp_path, Path)
    assert folder_exists(tmp_path).result()


@flow
def test_folder_creation(tmp_path):
    assert isinstance(tmp_path, Path)
    non_existant_path = tmp_path / "hfdahdasfaeg"
    assert isinstance(non_existant_path, Path)
    # Does not yet exist
    assert not folder_exists(non_existant_path).result()
    # Create and check
    assert create_folder(non_existant_path).result()
    assert folder_exists(non_existant_path).result()


@flow
def test_folder_non_existent(tmp_path):
    assert isinstance(tmp_path, Path)
    non_existant_path = tmp_path / "hfdahdasfaeg"
    assert isinstance(non_existant_path, Path)
    assert folder_exists(tmp_path).result() is False

    # TODO: Create tests for extract_url, path_exists, extract_file,


def test_fixture_csv(temp_ingress_file_csv):
    assert temp_ingress_file_csv.is_file()


def test_fixture_txt(temp_ingress_file_txt):
    assert temp_ingress_file_txt.is_file()


def test_fixture_extraction_fixt_extraction_queue(fixt_extraction_queue):
    assert isinstance(fixt_extraction_queue._queue, SimpleQueue)
    assert fixt_extraction_queue.pop() == 8
    assert fixt_extraction_queue.pop() == 7
    assert fixt_extraction_queue.pop() == "Rose"
    with pytest.raises(Empty):
        return fixt_extraction_queue.pop()
