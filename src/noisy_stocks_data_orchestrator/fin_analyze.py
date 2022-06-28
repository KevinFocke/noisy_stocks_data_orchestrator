from datetime import datetime

from pandera.errors import SchemaError
from prefect.flows import flow
from prefect.tasks import task

import customdatastructures

# TODO: Ensure timestamps align

# Correlation is parralellizable! https://docs.dask.org/en/stable/dataframe.html

# TODO: Avoid highest correlation duplicates of the same stock + timerange combination. Append all threads to same olist, then sort, then filter duplicate stocks out of the eventual list.


@flow()
def normalize_time_series():
    # Ensure equal time between timestamps

    # Index timestamp based on first value in time series
    return


@task()
def find_highest_correlation(historical_stock_data):
    # TODO: Skip any timeseries that raises a SchemaError
    return historical_stock_data


@flow()
def select_interesting_stock(stocks, criteria=""):
    return


@flow()
def find_movers_and_shakers(date: datetime, col: str = "closing_price"):
    """On date, check the biggest percentage gain and loss of the col

    Args:
        date (Timestamp): _description_
        col (str, optional): _description_. Defaults to "closing_price".

    Returns:
        Tuple containing Stock: _description_
    """

    stock_data = {
        "stock": "AAPL",
        "change_in_points": "100",
        "market_cap_in_millions": 500,
    }
    return stock_data


@task()
def calculate_percentage_change(Stock):
    pass
