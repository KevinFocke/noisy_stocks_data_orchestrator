import pickle
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from prefect.flows import flow
from prefect.task_runners import SequentialTaskRunner
from prefect.tasks import task
from pydantic import validate_arguments
from sqlalchemy import engine

from customdatastructures import StockTimeSeries, TimeSeries, file_exists, folder_exists

"""Data Inflow Module for data from database
"""


# ETL data into database using Talend + Pandas Exploratory research


class Config_Arbitrary_Types_Allowed:
    arbitrary_types_allowed = True


@flow(task_runner=SequentialTaskRunner())
def load_object_from_file_path(file_path: Path):
    """input: object, folderPath, the filename will be the current datetime"""
    file_exists(file_path)
    with file_path.open("rb") as fp:  # wb to write binary
        loaded_object = pickle.load(fp)
        print(loaded_object)
        return loaded_object


@flow(task_runner=SequentialTaskRunner())
def fetch_stocks_to_TimeSeries(*args, **kwargs):
    """thin wrapper for query_database_to_TimeSeries for two reasons:
    1. to apply stock_specific settings
    2. to differentiate the flows"""
    # query stocks
    # TODO: use args with *args and **kwargs instead
    time_series = query_database_to_TimeSeries(is_stock=True, *args, **kwargs).result()

    return time_series


@flow(task_runner=SequentialTaskRunner())
def fetch_weather_to_TimeSeries(*args, **kwargs):
    """thin wrapper for query_database_to_TimeSeries for two reasons:
    1. to apply weather specific settings
    2. to differentiate the flows"""
    # query weather
    time_series = query_database_to_TimeSeries(*args, **kwargs).result()

    return time_series


@validate_arguments(config=Config_Arbitrary_Types_Allowed)
@task(retries=3, retry_delay_seconds=3)
def query_database(sql_alchemy_engine: engine.base.Engine, query: str) -> DataFrame:
    connection = sql_alchemy_engine.connect()  # Connect to the database
    return pd.read_sql(query, connection)  # Run query and convert into pd DataFrame
    # SPEED, major: parallelize read into sql
    # TODO: Change into dask read_sql (but it doesn't want to connect :/)


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
    timeout=10,
    is_stock: bool = False,  # is it a stock?
):

    # get Prefect Future
    prefect_future = query_database(sql_alchemy_engine=sql_alchemy_engine, query=query)

    # calculate result

    prefect_result_df = prefect_future.result(timeout=timeout)

    # normalize date
    df = normalize_timestamp(df=prefect_result_df).result()

    if is_stock:
        return StockTimeSeries(
            timestamp_index_name=timestamp_index_name,
            numeric_col_name=numeric_col_name,
            time_series_df=df,
        )
    else:
        return TimeSeries(
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
