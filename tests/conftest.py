import pandas as pd
import pytest
from noisy_stocks_data_orchestrator.customdatastructures import TimeSeries

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
    return TimeSeries(name="AAPL", time_series_df=df)


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
    return TimeSeries(name="AAPL", time_series_df=df)


def stock_with_negative_closing_price():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1980-05-10"],
            "close_price": [-1.3, 1.4, -5],
        }
    )
    return TimeSeries(name="AAPL", time_series_df=df)


def stock_with_duplicate_dates():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1996-10-04"],
            "close_price": [1.3, 1.4, 5],
        }
    )
    return TimeSeries(name="AAPL", time_series_df=df)


def stock_with_unordered_dates():
    df = pd.DataFrame(
        {
            "timestamp": ["1996-10-04", "1980-02-05", "1976-10-04"],
            "close_price": [1.3, 1.4, 5],
        }
    )
    return TimeSeries(name="AAPL", time_series_df=df)
