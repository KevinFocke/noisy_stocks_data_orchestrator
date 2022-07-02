from datetime import datetime
from itertools import combinations
from pathlib import Path

import pandas as pd
import pytest
from freezegun import freeze_time
from noisy_stocks_data_orchestrator import __version__, main_flow
from noisy_stocks_data_orchestrator.customdatastructures import TimeSeries
from noisy_stocks_data_orchestrator.ingress import create_folder, folder_exists
from pandera.errors import SchemaError
from prefect.flows import flow
from prefect.testing.utilities import prefect_test_harness  # for testing flows

from tests.conftest import stock_with_negative_closing_price, stock_with_unequal_rows

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
    assert isinstance(stock.stock_symbol_name, str)
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
    with pytest.raises(SchemaError):
        stock_with_negative_closing_price()


# def test_dates_sorted():
#     stock = stock_with_unordered_dates()
#     timestamp_array = stock.time_series_df.index.values
#     # expected_result in datetime
#     expected_result = pd.to_datetime(
#         [
#             "1976-10-04T00:00:00.000000000",
#             "1980-02-05T00:00:00.000000000",
#             "1996-10-04T00:00:00.000000000",
#         ]
#     ).values
#     assert np.array_equal(timestamp_array, expected_result)  # type:ignore


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


def test_fixture_csv(temp_ingress_file_csv):
    assert temp_ingress_file_csv["file_path"].is_file()
    filename = str(temp_ingress_file_csv["file_path"]).split(r"/")[-1]
    expected_suffix = ".csv"
    lower_bound = len(filename) - len(expected_suffix)
    suffix = filename[lower_bound:]
    assert suffix == expected_suffix


def test_fixture_txt(temp_ingress_file_txt):
    assert temp_ingress_file_txt["file_path"].is_file()
    filename = str(temp_ingress_file_txt["file_path"]).split(r"/")[-1]
    expected_suffix = ".txt"
    lower_bound = len(filename) - len(expected_suffix)
    suffix = filename[lower_bound:]
    assert suffix == expected_suffix


def test_fixt_three_stock_csv_same_folder(
    fixt_three_stock_csv_same_folder,
    stock_sample_data_1,
    stock_sample_data_2,
    stock_sample_data_3,
):
    # Python subtlety:  1 != 2 != 1 will evaluate to true; evaluated right to left
    # Thus using combinations to compare each element
    folder_paths = [
        fixt_three_stock_csv_same_folder[number]["folder_path"]
        for number in range(len(fixt_three_stock_csv_same_folder))
    ]
    file_paths = [
        fixt_three_stock_csv_same_folder[number]["file_path"]
        for number in range(len(fixt_three_stock_csv_same_folder))
    ]
    file_input_data_list = [
        fixt_three_stock_csv_same_folder[number]["file_input_data"]
        for number in range(len(fixt_three_stock_csv_same_folder))
    ]
    file_path_combinations = combinations(file_paths, 2)
    folder_path_combinations = combinations(folder_paths, 2)
    for combination in folder_path_combinations:
        assert combination[0] == combination[1]  # expecting same folders
    for combination in file_path_combinations:
        assert combination[0] != combination[1]  # expecting different files
    # Compare received data to sample data
    stock_sample_data_list = [
        stock_sample_data_1,
        stock_sample_data_2,
        stock_sample_data_3,
    ]
    for index in range(len(file_input_data_list)):
        assert file_input_data_list[index] == stock_sample_data_list[index]


def test_fixt_three_stock_csv_different_folder(
    fixt_three_stock_csv_different_folder,
    stock_sample_data_1,
    stock_sample_data_2,
    stock_sample_data_3,
):

    # Python subtlety:  1 != 2 != 1 will evaluate to true; evaluated right to left
    # Thus using combinations to compare each element
    folder_paths = [
        fixt_three_stock_csv_different_folder[number]["folder_path"]
        for number in range(len(fixt_three_stock_csv_different_folder))
    ]
    file_paths = [
        fixt_three_stock_csv_different_folder[number]["file_path"]
        for number in range(len(fixt_three_stock_csv_different_folder))
    ]
    folder_path_combinations = combinations(folder_paths, 2)
    file_path_combinations = combinations(file_paths, 2)
    for combination in folder_path_combinations:
        assert combination[0] != combination[1]  # expecting different folders
    for combination in file_path_combinations:
        assert combination[0] != combination[1]  # expecting different files
    file_input_data_list = [
        fixt_three_stock_csv_different_folder[number]["file_input_data"]
        for number in range(len(fixt_three_stock_csv_different_folder))
    ]
    # Compare received data to sample data
    stock_sample_data_list = [
        stock_sample_data_1,
        stock_sample_data_2,
        stock_sample_data_3,
    ]
    for index in range(len(file_input_data_list)):
        assert file_input_data_list[index] == stock_sample_data_list[index]


def test_calculate_date_interval_2_days(fixt_database_query):
    date = datetime.strptime("2022-07-01", r"%Y-%m-%d")
    interval_in_days = 2

    assert fixt_database_query.calculate_date_interval(
        date=date, interval_in_days=interval_in_days
    ) == (
        datetime.strptime("2022-06-29", r"%Y-%m-%d"),
        datetime.strptime("2022-07-03", r"%Y-%m-%d"),
    )


def test_calculate_date_interval_same_day(fixt_database_query):
    date = datetime.strptime("2022-07-01", r"%Y-%m-%d")
    interval_in_days = 0

    assert fixt_database_query.calculate_date_interval(
        date=date, interval_in_days=interval_in_days
    ) == (
        datetime.strptime("2022-07-01", r"%Y-%m-%d"),
        datetime.strptime("2022-07-01", r"%Y-%m-%d"),
    )


def test_calculate_stock_query_creation(fixt_database_query_min_args_fakedate):
    assert (
        fixt_database_query_min_args_fakedate.to_sql()
        == r"SELECT timestamp,stock_symbol,price_close FROM stock_timedata WHERE timestamp >= '1992-01-04' and timestamp <= '1992-01-24';"
    )


def test_database_query_output_begin_timestamp(fixt_database_query):
    assert fixt_database_query.output_begin_timestamp() == "2022-06-29"


def test_database_query_output_end_timestamp(fixt_database_query):
    assert fixt_database_query.output_end_timestamp() == "2022-07-03"


def test_freeze_time():
    with freeze_time("2012-01-14"):
        assert datetime.now() == datetime(2012, 1, 14)


def test_database_query_begin_and_timestamp_scenario1(
    fixt_database_query_begin_and_end_timestamp_scenario_1,
):
    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_1.output_begin_timestamp()
        == "2022-06-29"
    )

    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_1.output_end_timestamp()
        == "2022-07-03"
    )


def test_database_query_begin_and_timestamp_scenario2(
    fixt_database_query_begin_and_end_timestamp_scenario_2,
):
    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_2.output_begin_timestamp()
        == "2022-03-05"
    )

    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_2.output_end_timestamp()
        == "2022-03-25"
    )


def test_database_query_begin_and_timestamp_scenario3(
    fixt_database_query_begin_and_end_timestamp_scenario_3,
):
    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_3.output_begin_timestamp()
        == "2008-10-06"
    )

    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_3.output_end_timestamp()
        == "2008-10-20"
    )


def test_database_query_begin_and_timestamp_scenario4(
    fixt_database_query_begin_and_end_timestamp_scenario_4,
):
    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_4.output_begin_timestamp()
        == "1988-10-20"
    )

    assert (
        fixt_database_query_begin_and_end_timestamp_scenario_4.output_end_timestamp()
        == "1988-10-26"
    )


def test_longest_consecutive_days_sequence(fixt_time_series_ordinary):
    dates = ["2002-07-05", "2002-07-06", "2002-07-07", "2002-07-08", "2002-7-09"]
    dates = [pd.Timestamp(el_date, tz=None) for el_date in dates]
    time_series = fixt_time_series_ordinary.calc_longest_consecutive_days_sequence(
        treshold=2
    )  # only 2 stocks contained
    assert time_series == tuple(dates)


def test_longest_consecutive_days_sequence_missing(fixt_time_series_date_missing):
    dates = ["2002-07-07", "2002-07-08", "2002-7-09"]
    dates = [pd.Timestamp(el_date, tz=None) for el_date in dates]
    time_series = fixt_time_series_date_missing.calc_longest_consecutive_days_sequence(
        treshold=2
    )  # only 2 stocks contained
    assert time_series == tuple(dates)
