from pathlib import Path

import pandas as pd
from pandas import DataFrame
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic import validate_arguments
from sqlalchemy import engine

from customdatastructures import TimeSeries, folder_exists

"""Data Inflow Module for data from database
"""


# ETL data into database using Talend + Pandas Exploratory research


class Config_Arbitrary_Types_Allowed:
    arbitrary_types_allowed = True


@flow(task_runner=SequentialTaskRunner())
def fetch_stocks_to_TimeSeries(
    sql_alchemy_stock_engine, stocks_query, numeric_col_name, timeout=60
):
    """thin wrapper for query_database_to_TimeSeries for two reasons:
    1. to apply stock_specific settings
    2. to differentiate the flows"""
    # query stocks
    stocks_time_series = query_database_to_TimeSeries(
        sql_alchemy_engine=sql_alchemy_stock_engine,
        query=stocks_query,
        numeric_col_name=numeric_col_name,
        timeout=timeout,
    ).result()

    return stocks_time_series


@validate_arguments(config=Config_Arbitrary_Types_Allowed)
@task(retries=3, retry_delay_seconds=3)
def query_database(sql_alchemy_engine: engine.base.Engine, query: str) -> DataFrame:
    connection = sql_alchemy_engine.connect()  # Connect to the database
    return pd.read_sql(query, connection)  # Run query and convert into pd DataFrame


@validate_arguments(config=Config_Arbitrary_Types_Allowed)
@task
def normalize_timestamp(df: DataFrame) -> DataFrame:
    """Normalize to UTC; Pandera needs Timezone Unaware"""
    # Set timestamp as index // required by tz_localize
    df.set_index("timestamp", inplace=True)

    # normalize timezone to UTC, then make timezone unaware for pandera validation
    df = df.tz_convert("UTC").tz_localize(None)

    return df


# BUG: Cannot validate engine custom type, thus workaround by
# checking inside Class https://github.com/PrefectHQ/prefect/issues/5663
@flow(task_runner=SequentialTaskRunner())
def query_database_to_TimeSeries(
    sql_alchemy_engine,
    query,
    numeric_col_name,
    timestamp_index_name="timestamp",
    stock_symbol_name=None,
    timeout=10,
):

    # get Prefect Future
    prefect_future = query_database(sql_alchemy_engine=sql_alchemy_engine, query=query)

    # calculate result

    prefect_result_df = prefect_future.result(timeout=timeout)

    # normalize date
    df = normalize_timestamp(df=prefect_result_df).result()

    return TimeSeries(
        stock_symbol_name=stock_symbol_name,
        timestamp_index_name=timestamp_index_name,
        numeric_col_name=numeric_col_name,
        time_series_df=df,
    )


@validate_arguments
@task
def create_folder(folder_url: Path):
    if not folder_exists(folder_url):
        print(f"Folder {folder_url} does not exist, creating it.")
        Path.mkdir(folder_url, parents=True)
        if folder_exists(folder_url):
            return True  # Folder created
        else:
            raise ValueError("Folder should have been created, but was not.")
    else:
        print("Folder already exists")
        return False  # No folder created
