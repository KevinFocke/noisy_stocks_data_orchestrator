from queue import Empty, SimpleQueue

import pandas as pd
import pandera as pa
import pytest
from noisy_stocks_data_orchestrator.customdatastructures import (
    ExtractionQueue,
    TimeSeries,
)
from pandera.dtypes import Timestamp

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
            "close_price": [1.3, 1.4, 0, 1, 5],
        }
    )
    time_series_df_schema = pa.DataFrameSchema(
        {
            "timestamp": pa.Column(Timestamp, coerce=True),
            "close_price": pa.Column(
                float, checks=pa.Check.greater_than_or_equal_to(0)
            ),
        }
    )
    return TimeSeries(
        name="AAPL", time_series_df=df, resource_schema=time_series_df_schema
    )


# WISHLIST: REFACTOR stocks to use a common df schema
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
    time_series_df_schema = pa.DataFrameSchema(
        {
            "timestamp": pa.Column(Timestamp, coerce=True),
            "close_price": pa.Column(
                float, checks=pa.Check.greater_than_or_equal_to(0)
            ),
        }
    )
    return TimeSeries(
        name="AAPL", time_series_df=df, resource_schema=time_series_df_schema
    )


def stock_with_negative_closing_price():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1980-05-10"],
            "close_price": [-1.3, 1.4, -5],
        }
    )
    time_series_df_schema = pa.DataFrameSchema(
        {
            "timestamp": pa.Column(Timestamp, coerce=True),
            "close_price": pa.Column(
                float, checks=pa.Check.greater_than_or_equal_to(0)
            ),
        }
    )
    return TimeSeries(
        name="AAPL", time_series_df=df, resource_schema=time_series_df_schema
    )


def stock_with_duplicate_dates():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1996-10-04"],
            "close_price": [1.3, 1.4, 5],
        }
    )
    time_series_df_schema = pa.DataFrameSchema(
        {
            "timestamp": pa.Column(Timestamp, coerce=True),
            "close_price": pa.Column(
                float, checks=pa.Check.greater_than_or_equal_to(0)
            ),
        }
    )

    return TimeSeries(
        name="AAPL", time_series_df=df, resource_schema=time_series_df_schema
    )


def stock_with_unordered_dates():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1976-10-04"],
            "close_price": [1.3, 1.4, 5],
        }
    )
    time_series_df_schema = pa.DataFrameSchema(
        {
            "timestamp": pa.Column(Timestamp, coerce=True),
            "close_price": pa.Column(
                float, checks=pa.Check.greater_than_or_equal_to(0)
            ),
        }
    )
    return TimeSeries(
        name="AAPL", time_series_df=df, resource_schema=time_series_df_schema
    )


# Create Path
# Create folder
# Undo any steps; undo creation of folder


@pytest.fixture
def factory_temp_file(tmp_path):
    """Provide temporary file with sample data

    Args:
        suffix (str): The suffix of your filename eg. ".txt" for "tempfile.txt"

    Return:
        path (Path): path to temporary file with sample data;
        Path object created by built-in pathlib
    """

    def _create_temp_file(filesuffix):
        # Based on a.us.txt in Stocks
        filename = "testfile" + filesuffix
        filepath = tmp_path / filename
        input_data = [
            "Date, Open, High, Low, Close, Volume, OpenInt",
            "1999 - 11 - 18, 30.713, 33.754, 27.002, 29.702, 66277506, 0",
            "1999 - 11 - 19, 28.986, 29.027, 26.872, 27.257, 16142920, 0",
            "1999 - 11 - 22, 27.886, 29.702, 27.044, 29.702, 6970266, 0",
        ]
        # open as file pointer, append to it (implicitly creates file if does not exist)
        # Write the data
        with open(filepath, "a") as fp:
            for line in input_data:
                fp.write(line)
                fp.write("\n")

        _validate_temp_file(filepath, input_data)
        return filepath

    def _validate_temp_file(filepath, input_data):
        # Read the data
        read_data = []
        with open(filepath, "r") as fp:
            for line in range(len(input_data)):
                read_data.append(fp.readline())

        for index, line in enumerate(read_data):
            read_data[index] = line.strip("\n")
        assert read_data == input_data
        assert filepath.is_file()

    return _create_temp_file  # Call subfunction


@pytest.fixture
def temp_ingress_file_csv(factory_temp_file):
    return factory_temp_file(filesuffix=".csv")


@pytest.fixture
def temp_ingress_file_txt(factory_temp_file):
    return factory_temp_file(filesuffix=".txt")


@pytest.fixture
def empty_extraction_queue():
    return ExtractionQueue()


@pytest.fixture
def fixt_extraction_queue():
    queue = ExtractionQueue()
    queue.push(8)
    queue.push(7)
    queue.push("Rose")
    return queue
