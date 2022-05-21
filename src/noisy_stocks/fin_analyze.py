from prefect import flow, task

import custom_class


@task()
def find_best_fit_regression(historical_stock_data):
    return historical_stock_data


@flow()
def select_interesting_stock(stocks, criteria=""):
    return
