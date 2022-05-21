from prefect import flow
from prefect.task_runners import SequentialTaskRunner

from fin_analyze import find_best_fit_regression, select_interesting_stock
from fin_api import find_movers_and_shakers, get_historical_stock_data
from publish import publish

# Prefect collection for GreatExpectations?
# https://prefecthq.github.io/prefect-great-expectations/

# Convert dates to datetime
# https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html


def sanity_check():
    """Can pytest find the module?

    Returns:
        string: "Module_Found"
    """
    return "Module_Found"


@flow(task_runner=SequentialTaskRunner())
def main_flow():
    # Preanalysis
    stocks = find_movers_and_shakers()

    # TODO: Fix provided class
    interesting_stock = select_interesting_stock(stocks)
    # Get historical stock data

    historical_stock_data = get_historical_stock_data(interesting_stock)
    # Regression analysis
    best_fit = find_best_fit_regression(historical_stock_data)

    # Publish results of analysis
    publish(historical_stock_data, best_fit)


if __name__ == "__main__":
    main_flow()
