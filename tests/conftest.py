from datetime import datetime
from os import mkdir
from pathlib import Path

import pandas as pd
import pytest
from freezegun import freeze_time
from noisy_stocks_data_orchestrator.customdatastructures import (
    CorrDatabaseQuery,
    StockTimeSeries,
)

"""
Single source of truth for fixtures across tests
"""

# Scopes of pytest fixtures decide how often they are run.
# Syntax: @pytest.fixture(scope="function")
# Unique polymorphism: You can call the class directly; it returns an object!
# The default scope of a fixture is function; this means:
# The fixture is a wrapper; setup before, teardown after.
# Other possible fixtures are: session, class, module
# You can also make a fixture apply to every test with autouse
# Syntax: @pytest.fixture(autouse=True)


# TODO: Make fixtures flows and tasks + mark?


@pytest.fixture
def sanity_check_fixture():
    return "testinput"


@pytest.fixture
def stock_with_date_nan():
    df = pd.DataFrame(
        {
            "timestamp": [
                "1996-10-04",
                "1980-02-05",
                "1970-02-05",
                "1950-02-07",
                pd.NaT,
            ],
            "close_price": [1.3, 1.4, 8, 1, 5],
        }
    )
    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


def stock_with_unequal_rows():
    df = pd.DataFrame(
        {
            "timestamp": [
                "1996-10-04",
                "1980-02-05",
            ],
            "close_price": [1.3, 1.4, 0],
        }
    )
    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


def stock_with_negative_closing_price():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1980-05-10"],
            "close_price": [-1.3, 1.4, -5],
        }
    )
    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


def stock_with_duplicate_dates():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1996-10-04"],
            "close_price": [1.3, 1.4, 5],
        }
    )
    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


def stock_with_unordered_dates():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1976-10-04"],
            "close_price": [1.3, 1.4, 5],
        }
    )
    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


# Create Path
# Create folder
# Undo any steps; undo creation of folder


@pytest.fixture
def stock_sample_data_1():
    # First 4 lines of aa.us.txt
    return [
        "Date,Open,High,Low,Close,Volume,OpenInt",
        "1970-01-02,2.2972,2.3178,2.2972,2.2972,26437,0",
        "1970-01-05,2.2972,2.2972,2.2544,2.2759,29887,0",
        "1970-01-06,2.2759,2.2759,2.2342,2.2342,106900,0",
    ]


@pytest.fixture
def stock_sample_data_2():
    # First 4 lines of aaap.us.txt
    return [
        "Date,Open,High,Low,Close,Volume,OpenInt",
        "2015-11-11,18.5,25.9,18,24.5,1584600,0",
        "2015-11-12,24.25,27.12,22.5,25,83000,0",
        "2015-11-13,25.47,26.2,24.55,25.26,67300,0",
    ]


@pytest.fixture
def stock_sample_data_3():
    # First 4 lines of aaba.us.txt
    return [
        "Date,Open,High,Low,Close,Volume,OpenInt",
        "1996-04-12,1.05,1.79,1.02,1.38,408720000,0",
        "1996-04-15,1.49,1.5,1.25,1.34,79231200,0",
        "1996-04-16,1.34,1.34,1.17,1.2,48026400,0]",
    ]


@pytest.fixture
def factory_temp_file(tmp_path):
    """Provide temporary file with sample data

    Args:
        suffix (str): The suffix of your filename eg. ".txt" for "tempfile.txt"

    Return:
        If include_folder_path = 0:
        Dict containing Path to file
        If include_folder_path = 1:
        tuple (Dict(file_path: Path to file), Dict(folder_path: Path to folder))
    """

    def _create_temp_file(
        filesuffix=".csv",
        file_input_data=[
            "Date, Open, High, Low, Close, Volume, OpenInt",
            "1999 - 11 - 18, 30.713, 33.754, 27.002, 29.702, 66277506, 0",
            "1999 - 11 - 19, 28.986, 29.027, 26.872, 27.257, 16142920, 0",
            "1999 - 11 - 22, 27.886, 29.702, 27.044, 29.702, 6970266, 0",
        ],
        include_folder_path=0,  # if 1, return the filepath + basepath as tuple
        include_file_input_data=0,  # if 1, include
        foldernumber=None,
        filenumber=1,
        tmp_path=tmp_path,
    ):
        # input_data default based on first three lines of a.us.txt in Stocks
        filename = "testfile" + str(filenumber) + filesuffix
        if foldernumber is not None:
            tmp_path = tmp_path / str(foldernumber)
            mkdir(tmp_path)
            assert isinstance(tmp_path, Path)
            assert tmp_path.is_dir()
        file_path = tmp_path / filename

        # open as file pointer, append to it (implicitly creates file if does not exist)
        # Write the data
        with open(file_path, "a") as fp:
            for line in file_input_data:
                fp.write(line)
                fp.write("\n")

        _validate_temp_file(file_path, file_input_data)
        output_dict = {}
        if include_folder_path == 1:
            output_dict["folder_path"] = tmp_path
        if include_file_input_data == 1:
            output_dict["file_input_data"] = file_input_data
        output_dict["file_path"] = file_path
        return output_dict

    def _validate_temp_file(filepath, file_input_data):
        # Read the data
        read_data = []
        with open(filepath, "r") as fp:
            for line in range(len(file_input_data)):
                read_data.append(fp.readline())

        for index, line in enumerate(read_data):
            read_data[index] = line.strip("\n")
        assert read_data == file_input_data
        assert filepath.is_file()

    return _create_temp_file  # Call subfunction


@pytest.fixture
def temp_ingress_file_csv(factory_temp_file):
    return factory_temp_file(filesuffix=".csv")


@pytest.fixture
def temp_ingress_file_txt(factory_temp_file):
    return factory_temp_file(filesuffix=".txt")


@pytest.fixture
def fixt_folder_extraction_queue():
    filepath = temp_ingress_file_csv  # Path to
    assert filepath.is_file()


@pytest.fixture
def fixt_three_stock_csv_same_folder(
    factory_temp_file, stock_sample_data_1, stock_sample_data_2, stock_sample_data_3
):
    path_list_of_dicts = []
    lower_bound = 0
    upper_bound = 3  # upper bound not inclusive
    stock_sample_data_list = [
        stock_sample_data_1,
        stock_sample_data_2,
        stock_sample_data_3,
    ]

    for number in range(lower_bound, upper_bound):
        path_list_of_dicts.append(
            factory_temp_file(
                file_input_data=stock_sample_data_list[number],
                include_folder_path=1,
                include_file_input_data=1,
                filenumber=number,
            )
        )  # returns dict with file_path, folder_path as keys

    return path_list_of_dicts  # Per file, includes a dict of {}


@pytest.fixture
def fixt_three_stock_csv_different_folder(
    factory_temp_file, stock_sample_data_1, stock_sample_data_2, stock_sample_data_3
):

    path_list_of_dicts = []
    lower_bound = 0
    upper_bound = 3  # upper bound not inclusive
    stock_sample_data_list = [
        stock_sample_data_1,
        stock_sample_data_2,
        stock_sample_data_3,
    ]

    for number in range(lower_bound, upper_bound):
        path_list_of_dicts.append(
            factory_temp_file(
                file_input_data=stock_sample_data_list[number],
                include_folder_path=1,
                include_file_input_data=1,
                filenumber=number,
                foldernumber=number,
            )
        )  # returns dict with file_path, folder_path as keys

    return path_list_of_dicts  # tuples of (filepath, folderpath, data)


@pytest.fixture
def fixt_database_query():
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    date_format = r"%Y-%m-%d"
    begin_date = datetime.strptime("2022-06-29", date_format)
    end_date = datetime.strptime("2022-07-03", date_format)

    return CorrDatabaseQuery(
        select_fields=select_fields,
        from_database=database_name,
        process_begin_and_end_timestamp=(begin_date, end_date),
    )


@pytest.fixture
def fixt_database_query_min_args_fakedate():
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    interval_in_days = 10
    with freeze_time("2012-01-14"):
        return CorrDatabaseQuery(
            select_fields=select_fields,
            from_database=database_name,
            interval_in_days=interval_in_days,
        )


@pytest.fixture
def fixt_database_query_begin_and_end_timestamp_scenario_1():
    # Based on process_begin_and_end_timestamp
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    date_format = r"%Y-%m-%d"
    begin_date = datetime.strptime("2022-06-29", date_format)
    end_date = datetime.strptime("2022-07-03", date_format)

    return CorrDatabaseQuery(
        select_fields=select_fields,
        from_database=database_name,
        process_begin_and_end_timestamp=(begin_date, end_date),
    )


@pytest.fixture
def fixt_database_query_begin_and_end_timestamp_scenario_2():
    # Based on process_begin_and_end_timestamp
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    date_format = r"%Y-%m-%d"
    target_date = datetime.strptime("2022-03-15", date_format)
    interval_in_days = 10

    return CorrDatabaseQuery(
        select_fields=select_fields,
        from_database=database_name,
        target_date=target_date,
        interval_in_days=interval_in_days,
    )


@pytest.fixture
def fixt_database_query_begin_and_end_timestamp_scenario_3():
    # Based on process_begin_and_end_timestamp
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    days_ago = 10
    interval_in_days = 7

    with freeze_time("2008-10-23"):
        return CorrDatabaseQuery(
            select_fields=select_fields,
            from_database=database_name,
            days_ago=days_ago,
            interval_in_days=interval_in_days,
        )


@pytest.fixture
def fixt_database_query_begin_and_end_timestamp_scenario_4():
    # Based on process_begin_and_end_timestamp
    select_fields = ["timestamp", "stock_symbol", "price_close"]
    database_name = "stock_timedata"
    interval_in_days = 3

    with freeze_time("2008-10-23"):
        return CorrDatabaseQuery(
            select_fields=select_fields,
            from_database=database_name,
            interval_in_days=interval_in_days,
        )


@pytest.fixture
def fixt_time_series_ordinary():
    df = pd.DataFrame(
        data=[
            ["2002-07-05", "A", 16.1680],
            ["2002-07-06", "A", 20],
            ["2002-07-07", "A", 21.6170],
            ["2002-07-08", "A", 18.2880],
            ["2002-07-09", "A", 19],
            ["2002-07-05", "B", 20.1680],
            ["2002-07-06", "B", 11],
            ["2002-07-07", "B", 14.6170],
            ["2002-07-08", "B", 12.2880],
            ["2002-07-09", "B", 8],
        ],
        columns=["timestamp", "stock_symbol", "close_price"],
    )

    pd.to_datetime(df.timestamp, format=r"%Y-%m-%d")
    df.set_index("timestamp", inplace=True)

    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


@pytest.fixture
def fixt_time_series_date_missing():
    df = pd.DataFrame(
        data=[
            ["2002-07-05", "A", 16.1680],
            ["2002-07-07", "A", 21.6170],
            ["2002-07-08", "A", 18.2880],
            ["2002-07-09", "A", 19],
            ["2002-07-05", "B", 20.1680],
            ["2002-07-07", "B", 14.6170],
            ["2002-07-08", "B", 12.2880],
            ["2002-07-09", "B", 8],
        ],
        columns=["timestamp", "stock_symbol", "close_price"],
    )

    pd.to_datetime(df.timestamp, format=r"%Y-%m-%d")
    df.set_index("timestamp", inplace=True)

    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


@pytest.fixture
def fixt_time_series_date_missing_filtered():
    """time_series filtered on longest consecutive days"""
    df = pd.DataFrame(
        data=[
            ["2002-07-07", "A", 21.6170],
            ["2002-07-08", "A", 18.2880],
            ["2002-07-09", "A", 19],
            ["2002-07-07", "B", 14.6170],
            ["2002-07-08", "B", 12.2880],
            ["2002-07-09", "B", 8],
        ],
        columns=["timestamp", "stock_symbol", "close_price"],
    )

    pd.to_datetime(df.timestamp, format=r"%Y-%m-%d")
    df.set_index("timestamp", inplace=True)

    return StockTimeSeries(
        numeric_col_name="close_price",
        timestamp_index_name="timestamp",
        time_series_df=df,
    )


@pytest.fixture
def fixt_dataframe_date_missing_filtered_and_pivoted():
    """time_series filtered on longest consecutive days"""
    df = pd.DataFrame(
        data=[
            ["2002-07-07", 21.6170, 14.6170],
            ["2002-07-08", 18.2880, 12.2880],
            ["2002-07-09", 19, 8],
        ],
        columns=["timestamp", "A", "B"],
    )

    df["timestamp"] = pd.to_datetime(df.timestamp, format=r"%Y-%m-%d")
    df.set_index("timestamp", inplace=True)
    df.columns = df.columns.set_names("stock_symbol")  # set col index name
    # clarification: a pivot table includes the original name of the col

    return df
