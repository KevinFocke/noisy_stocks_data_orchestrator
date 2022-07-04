from datetime import datetime

import pandas as pd
from pandas import DataFrame
from pandera.dtypes import Timestamp
from pandera.errors import SchemaError
from prefect.flows import flow
from prefect.tasks import task

import customdatastructures

# TODO: Ensure timestamps align

# Correlation is parralellizable! https://docs.dask.org/en/stable/dataframe.html

# Append all threads to same list, then sort, then filter duplicate stocks.


@task()
def find_movers_and_shakers(
    stock_df: DataFrame, start_date: Timestamp, end_date: Timestamp
):
    """calculates the biggest absolute difference between start date and end date"""

    return stock_data
