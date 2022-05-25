from prefect import flow
from prefect.task_runners import SequentialTaskRunner

from fin_analyze import (
    find_highest_correlation,
    find_movers_and_shakers,
    select_interesting_stock,
)
from publish import publish

# Convert dates to datetime
# https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html


# Worfklows contain tasks but no logic. Tasks do the work.


def sanity_check():
    """Can pytest find the module?

    Returns:
        string: "Module_Found"
    """
    return "Module_Found"


@flow(task_runner=SequentialTaskRunner())
def daily_stock_flow():
    # Preanalysis
    stocks = find_movers_and_shakers(date)

    interesting_stock = select_interesting_stock(stocks)
    # Get historical stock data

    historical_stock_data = get_historical_stock_data(interesting_stock)
    # Regression analysis
    best_fit = find_highest_correlation(historical_stock_data)

    # Publish results of analysis
    publish(historical_stock_data, best_fit)
