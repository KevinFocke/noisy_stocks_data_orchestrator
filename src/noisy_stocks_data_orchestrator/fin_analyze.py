from datetime import datetime

from prefect import flow, task

import customdatastructures

# TODO: Rewrite program to use data structures
# TODO: Ensure equal time between timestamps


@flow()
def normalize_time_series():
    # Ensure equal time between timestamps

    # Index timestamp based on first value in time series
    return


@task()
def find_highest_correlation(historical_stock_data):
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
