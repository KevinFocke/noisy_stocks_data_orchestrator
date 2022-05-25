from datetime import datetime

from prefect import flow, task

import data_structures

# TODO: Rewrite program to use data structures


@task()
def find_highest_correlation(historical_stock_data):
    return historical_stock_data


@flow()
def select_interesting_stock(stocks, criteria=""):
    return


@flow()
def find_movers_and_shakers(date: int):

    # Create a dict of dicts. Key is stock ticker aka symbol

    stock_data = {
        "stock": "AAPL",
        "change_in_points": "100",
        "market_cap_in_millions": 500,
    }
    return stock_data


@task()
def calculate_percentage_change(Stock):
    pass
