from prefect import flow
from prefect.task_runners import SequentialTaskRunner

from egress import publish
from fin_analyze import (
    find_highest_correlation,
    find_movers_and_shakers,
    select_interesting_stock,
)

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
def stock_correlation_flow():

    # TODO: Add date for running the flow; default to today roughly 8 years ago*
    # * 365 * years. Take into account leap days!
    # TODO: Update functions and arguments
    # Preanalysis

    # stocks = find_movers_and_shakers()

    interesting_stock = select_interesting_stock()
    # Get historical stock data

    # Regression analysis
    best_fit = find_highest_correlation()

    # Publish results of analysis
    publish()
